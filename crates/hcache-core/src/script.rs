//! EVAL: one sandboxed Lua 5.1 state for the node, run on the exclusive
//! side of the execution gate, and SCRIPT KILL.
//!
//! Known limits of the spike: a script stuck inside one long C function
//! (a pathological `string.find`) is not interrupted, because the kill
//! check runs between Lua instructions.

mod functions;
mod libs;

use crate::acl;
use crate::cmd::{Class, Cmd};
use crate::engine::Engine;
use crate::resp::Reply;
use mlua::{
    ChunkMode, Function, HookTriggers, Lua, LuaOptions, MultiValue, StdLib, Table, Value, VmState,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::SeqCst};
use std::sync::Arc;

/// No script is running.
const IDLE: u8 = 0;
/// A script is running and has not written yet: it can be killed.
const PURE: u8 = 1;
/// A script is running and has written: killing it would leave half a
/// transaction behind.
const WROTE: u8 = 2;
/// The running script was told to stop.
const KILL: u8 = 3;

const KILL_CHECK_INSTRUCTIONS: u32 = 1000;
/// A script that leaves the heap fuller than limit / this gets a full
/// collection before the next one runs.
const GC_AFTER_SCRIPT_FRACTION: usize = 4;
const MAX_CACHED_SCRIPTS: usize = 1000;
/// Longer scripts are compiled every time: the cache keeps the source text.
const MAX_CACHED_SCRIPT_LEN: usize = 16 << 10;
const MAX_REPLY_DEPTH: usize = 32;
/// Bounds the conversion of a script's return value; a table that contains
/// itself many times over would otherwise expand without end.
const MAX_REPLY_ITEMS: usize = 1 << 20;
const MAX_ERROR_LEN: usize = 512;
const KILLED: &str = "Script killed by user with SCRIPT KILL.";
/// How long the top level of a library may run while it loads, as in
/// Redis: a library that loops for ever must not hang the node, least of
/// all at start, when nobody is connected to send FUNCTION KILL.
const LOAD_TIMEOUT_MS: u64 = 500;
const LOAD_TIMED_OUT: &str = "FUNCTION LOAD timeout";

/// Milliseconds on a clock that only moves forward, for `LOAD_TIMEOUT_MS`.
fn mono_ms() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(std::time::Instant::now);
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Builds the sandbox. Receives `killed()`, `reclaim()` and the table of
/// Rust-built libraries (`cjson`, `cmsgpack`, `struct`, `bit`), and returns the table
/// that holds the globals scripts can see, the writable `redis` table, and
/// the read-only environment scripts run in.
const SETUP: &str = r#"
local killed, reclaim, libs = ...
local rerror, rpcall, rrawset, rsetmt, rtostring = error, pcall, rawset, setmetatable, tostring
local rtype, rrawget = type, rawget
local ro = rsetmt({}, {__mode = "k"})
local function deny() rerror("Attempt to modify a readonly table", 2) end
local function readonly(t)
  local p = rsetmt({}, {__index = t, __newindex = deny, __metatable = false})
  ro[p] = true
  return p
end

local real = {}
for _, name in ipairs({"assert", "error", "ipairs", "next", "pairs", "rawequal", "rawget", "select",
                       "setmetatable", "getmetatable", "tonumber", "tostring", "type", "unpack", "_VERSION"}) do
  real[name] = _G[name]
end
-- A killed script must not be able to swallow the kill with pcall. A
-- caught error may be an out-of-memory, and Lua 5.1 does not collect when an
-- allocation hits the limit, so the garbage the failed call left behind is
-- freed before the script goes on.
local function check(ok, ...)
  if not ok then
    if killed() then rerror((...), 0) end
    reclaim()
  end
  return ok, ...
end
real.pcall = function(f, ...) return check(rpcall(f, ...)) end
-- The handler runs as an ordinary call after the error is caught: inside
-- Lua's own error handling the kill check does not fire, so a handler that
-- loops could never be stopped.
local function handled(handler, ok, ...)
  if ok then return ok, ... end
  if killed() then rerror((...), 0) end
  reclaim()
  return false, handler(...)
end
real.xpcall = function(f, handler) return handled(handler, rpcall(f)) end
real.rawset = function(t, k, v)
  if ro[t] then rerror("Attempt to modify a readonly table", 2) end
  return rrawset(t, k, v)
end

string.dump = nil
-- Repeating the empty string allocates nothing, so neither the memory limit
-- nor the kill check would ever stop a huge count.
local rrep = string.rep
string.rep = function(s, n)
  if s == "" then return "" end
  return rrep(s, n)
end
getmetatable("").__metatable = false
real.string = readonly(string)
real.table = readonly(table)
real.math = readonly(math)
for name, lib in pairs(libs) do
  real[name] = readonly(lib)
end

local redis = {}
redis.error_reply = function(msg) return {err = msg} end
redis.status_reply = function(msg) return {ok = msg} end
-- The command functions live in Rust and are installed for each run in
-- `hidden`. redis.pcall returns error replies as {err = ...} tables;
-- redis.call raises that same table, so pcall(redis.call, ...) hands the
-- script a table too. An error that reaches the top becomes a string through
-- __tostring, which keeps the message. Every argument becomes a live Lua
-- value on the Rust side, which has room for a few thousand.
local hidden = {}
local rselect = select
redis.pcall = function(...)
  if rselect('#', ...) > 4000 then
    return {err = "Lua redis() command has too many arguments"}
  end
  return hidden.pcall(...)
end
local errmt = {__tostring = function(e) return rtostring(rrawget(e, "err")) end}
redis.call = function(...)
  local r = redis.pcall(...)
  if rtype(r) == "table" and rtype(rrawget(r, "err")) == "string" then
    rerror(rsetmt(r, errmt), 0)
  end
  return r
end
real.redis = readonly(redis)
-- Valkey 8 calls the same table `server`; scripts and libraries use either.
real.server = real.redis

local G = rsetmt({}, {
  __index = function(_, k)
    local v = real[k]
    if v == nil then
      rerror("Script attempted to access nonexistent global variable '" .. rtostring(k) .. "'", 2)
    end
    return v
  end,
  __newindex = function(_, k)
    rerror("Script attempted to create global variable '" .. rtostring(k) .. "'", 2)
  end,
  __metatable = false,
})
ro[G] = true
real._G = G
return real, redis, G, hidden
"#;

struct Vm {
    lua: Lua,
    real: Table,
    /// Where the per-run command functions go; scripts cannot reach it.
    hidden: Table,
    env: Table,
    cache: HashMap<Vec<u8>, Function>,
    /// `cjson` settings, put back to the defaults before every script.
    json_cfg: Arc<libs::JsonCfg>,
    /// Collects `redis.register_function` calls during FUNCTION LOAD.
    registering: functions::Registering,
    /// The functions of the loaded libraries, by name.
    functions: HashMap<String, Function>,
}

/// Scripts callable by hash. Redis keeps every script until SCRIPT FLUSH;
/// here the table is emptied when it is full, and the client's script helper
/// answers the NOSCRIPT that follows by sending the source again.
const MAX_LOADED_SCRIPTS: usize = 10_000;
const MAX_LOADED_SCRIPT_BYTES: usize = 64 << 20;
const SHA_HEX_LEN: usize = 40;
const NOSCRIPT: &str = "NOSCRIPT No matching script. Please use EVAL.";

/// Source text by lowercase SHA1 hex.
#[derive(Default)]
struct Loaded {
    by_sha: HashMap<Vec<u8>, Arc<[u8]>>,
    bytes: usize,
}

pub(crate) struct Scripts {
    vm: Mutex<Option<Vm>>,
    /// Held only for a lookup or an insert, never while a script runs, so
    /// SCRIPT EXISTS and SCRIPT FLUSH answer during a long script.
    loaded: Mutex<Loaded>,
    /// Taken after `vm` when both are needed.
    libraries: Mutex<functions::Libraries>,
    /// Counts the changes to `libraries`, so the data file learns of each.
    library_gen: AtomicU64,
    state: Arc<AtomicU8>,
    /// When the library loading now must have finished (`mono_ms`); 0
    /// while none loads.
    load_until: Arc<AtomicU64>,
    /// Whether the script that runs now came in through EVAL_RO.
    read_only: AtomicBool,
    memory_limit: usize,
}

impl Scripts {
    pub(crate) fn new(memory_limit: usize) -> Scripts {
        Scripts {
            vm: Mutex::new(None),
            loaded: Mutex::new(Loaded::default()),
            libraries: Mutex::new(functions::Libraries::default()),
            library_gen: AtomicU64::new(0),
            state: Arc::new(AtomicU8::new(IDLE)),
            load_until: Arc::new(AtomicU64::new(0)),
            read_only: AtomicBool::new(false),
            memory_limit,
        }
    }

    /// Say whether the next script may write. The caller holds the
    /// exclusive side of the gate, as for running it.
    pub(crate) fn set_read_only(&self, on: bool) {
        self.read_only.store(on, SeqCst);
    }

    /// SCRIPT KILL. Callable from any thread while a script runs.
    pub(crate) fn kill(&self) -> Reply {
        loop {
            match self.state.load(SeqCst) {
                IDLE => return Reply::err("NOTBUSY No scripts in execution right now."),
                WROTE => {
                    return Reply::err(
                        "UNKILLABLE Sorry the script already executed write commands against the dataset.",
                    )
                }
                PURE => {
                    if self.state.compare_exchange(PURE, KILL, SeqCst, SeqCst).is_ok() {
                        return Reply::Ok;
                    }
                }
                _ => return Reply::Ok,
            }
        }
    }

    /// SCRIPT EXISTS.
    pub(crate) fn exists(&self, shas: &[Vec<u8>]) -> Reply {
        let loaded = self.loaded.lock();
        Reply::Array(
            shas.iter()
                .map(|sha| Reply::Int(loaded.by_sha.contains_key(&sha.to_ascii_lowercase()).into()))
                .collect(),
        )
    }

    /// SCRIPT FLUSH. Compiled functions stay cached by source text; only
    /// the hashes are forgotten.
    pub(crate) fn flush(&self) -> Reply {
        let old = std::mem::take(&mut *self.loaded.lock());
        // Freed outside the lock.
        drop(old);
        Reply::Ok
    }

    /// Make `script` callable by hash; returns the lowercase hex SHA1.
    fn register(&self, script: &[u8]) -> Vec<u8> {
        let sha = sha1_smol::Sha1::from(script)
            .digest()
            .to_string()
            .into_bytes();
        let mut loaded = self.loaded.lock();
        if loaded.by_sha.contains_key(&sha) {
            return sha;
        }
        let full = loaded.by_sha.len() >= MAX_LOADED_SCRIPTS
            || loaded.bytes.saturating_add(script.len()) > MAX_LOADED_SCRIPT_BYTES;
        let evicted = if full {
            std::mem::take(&mut *loaded)
        } else {
            Loaded::default()
        };
        loaded.bytes += script.len();
        loaded.by_sha.insert(sha.clone(), script.into());
        drop(loaded);
        // Freed outside the lock.
        drop(evicted);
        sha
    }

    /// The compiled script, starting the Lua state if there is none.
    fn compile(&self, slot: &mut Option<Vm>, script: &[u8]) -> Result<Function, Reply> {
        let vm = match slot {
            Some(vm) => vm,
            None => match Vm::new(
                self.state.clone(),
                self.load_until.clone(),
                self.memory_limit,
            ) {
                Ok(vm) => slot.insert(vm),
                Err(e) => {
                    return Err(Reply::Error(format!(
                        "ERR cannot start Lua: {}",
                        describe(&e)
                    )))
                }
            },
        };
        match vm.compile(script) {
            Ok(func) => Ok(func),
            Err(e) => {
                let reply = Reply::Error(format!(
                    "ERR Error compiling script (new function): {}",
                    describe(&e)
                ));
                if is_memory_error(&e) {
                    *slot = None;
                }
                Err(reply)
            }
        }
    }

    /// SCRIPT LOAD. The caller holds the exclusive side of the gate.
    pub(crate) fn load(&self, script: &[u8]) -> Reply {
        let compiled = self.compile(&mut self.vm.lock(), script);
        match compiled {
            Ok(_) => Reply::Bulk(self.register(script)),
            Err(reply) => reply,
        }
    }

    /// EVALSHA. The caller holds the exclusive side of the gate.
    pub(crate) fn evalsha(
        &self,
        engine: &Engine,
        sha: &[u8],
        keys: Vec<Vec<u8>>,
        args: Vec<Vec<u8>>,
    ) -> Reply {
        if sha.len() != SHA_HEX_LEN {
            return Reply::err(NOSCRIPT);
        }
        let script = self
            .loaded
            .lock()
            .by_sha
            .get(&sha.to_ascii_lowercase())
            .cloned();
        match script {
            Some(script) => self.run(engine, &script, keys, args, false),
            None => Reply::err(NOSCRIPT),
        }
    }

    /// EVAL. The caller holds the exclusive side of the gate.
    pub(crate) fn eval(
        &self,
        engine: &Engine,
        script: &[u8],
        keys: Vec<Vec<u8>>,
        args: Vec<Vec<u8>>,
    ) -> Reply {
        self.run(engine, script, keys, args, true)
    }

    fn run(
        &self,
        engine: &Engine,
        script: &[u8],
        keys: Vec<Vec<u8>>,
        args: Vec<Vec<u8>>,
        register: bool,
    ) -> Reply {
        let mut slot = self.vm.lock();
        let func = match self.compile(&mut slot, script) {
            Ok(func) => func,
            Err(reply) => return reply,
        };
        if register {
            self.register(script);
        }
        let Some(vm) = slot.as_mut() else {
            return Reply::err("ERR internal: no Lua state");
        };
        self.state.store(PURE, SeqCst);
        let result = vm.run(engine, &self.state, &func, &keys, &args, false);
        self.state.store(IDLE, SeqCst);
        // Lua 5.1 does not collect when an allocation hits the limit, so
        // garbage left by one script would fail the next one.
        if vm.lua.used_memory() > self.memory_limit / GC_AFTER_SCRIPT_FRACTION {
            let _ = vm.lua.gc_collect();
        }
        match result {
            Ok(reply) => reply,
            Err(e) => {
                // A state that ran out of memory may hold a half-built
                // sandbox; the next script gets a fresh one.
                if is_memory_error(&e) {
                    *slot = None;
                }
                Reply::Error(format!("ERR Error running script: {}", describe(&e)))
            }
        }
    }
}

impl Vm {
    fn new(
        state: Arc<AtomicU8>,
        load_until: Arc<AtomicU64>,
        memory_limit: usize,
    ) -> mlua::Result<Vm> {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            LuaOptions::default(),
        )?;
        let hook_state = state.clone();
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(KILL_CHECK_INSTRUCTIONS),
            move |_, _| {
                if hook_state.load(SeqCst) == KILL {
                    return Err(mlua::Error::runtime(KILLED));
                }
                let until = load_until.load(SeqCst);
                if until != 0 && mono_ms() > until {
                    return Err(mlua::Error::runtime(LOAD_TIMED_OUT));
                }
                Ok(VmState::Continue)
            },
        );
        let killed = lua.create_function(move |_, ()| Ok(state.load(SeqCst) == KILL))?;
        let reclaim = lua.create_function(move |lua, ()| {
            if memory_limit != 0 && lua.used_memory() > memory_limit / GC_AFTER_SCRIPT_FRACTION {
                lua.gc_collect()?;
            }
            Ok(())
        })?;
        let (libs, json_cfg) = libs::install(&lua, memory_limit)?;
        let (real, redis, env, hidden): (Table, Table, Table, Table) = lua
            .load(SETUP)
            .set_name("=sandbox")
            .call((killed, reclaim, libs))?;
        libs::install_redis(&lua, &redis)?;
        let registering = functions::install(&lua, &redis)?;
        // Set last, so the sandbox itself always fits.
        lua.set_memory_limit(memory_limit)?;
        Ok(Vm {
            lua,
            real,
            hidden,
            env,
            cache: HashMap::new(),
            json_cfg,
            registering,
            functions: HashMap::new(),
        })
    }

    fn compile(&mut self, script: &[u8]) -> mlua::Result<Function> {
        if let Some(func) = self.cache.get(script) {
            return Ok(func.clone());
        }
        // Precompiled chunks bypass the bytecode verifier Lua 5.1 lacks.
        if script.first() == Some(&0x1b) {
            return Err(mlua::Error::runtime("bytecode is not allowed"));
        }
        let func = self
            .lua
            .load(script)
            .set_name("=user_script")
            .set_mode(ChunkMode::Text)
            .into_function()?;
        func.set_environment(self.env.clone())?;
        if script.len() <= MAX_CACHED_SCRIPT_LEN {
            if self.cache.len() >= MAX_CACHED_SCRIPTS {
                self.cache.clear();
            }
            self.cache.insert(script.to_vec(), func.clone());
        }
        Ok(func)
    }

    fn run(
        &self,
        engine: &Engine,
        state: &AtomicU8,
        func: &Function,
        keys: &[Vec<u8>],
        args: &[Vec<u8>],
        // A library function gets the keys and arguments as parameters, a
        // script as the globals KEYS and ARGV.
        as_function: bool,
    ) -> mlua::Result<Reply> {
        let (keys, args) = (self.strings(keys)?, self.strings(args)?);
        // A function sees nothing of the script that ran before it.
        let globals = (!as_function).then(|| (keys.clone(), args.clone()));
        self.real
            .raw_set("KEYS", globals.as_ref().map(|g| g.0.clone()))?;
        self.real.raw_set("ARGV", globals.map(|g| g.1))?;
        self.json_cfg.reset();
        let value = self.lua.scope(|scope| {
            let pcall = scope.create_function(|lua, args: MultiValue| {
                match redis_call(engine, state, args) {
                    Ok(reply) => to_lua(lua, reply),
                    // A kill is not an error the script may handle.
                    Err(e) if state.load(SeqCst) == KILL => Err(e),
                    Err(e) => to_lua(lua, Reply::Error(describe(&e))),
                }
            })?;
            self.hidden.raw_set("pcall", pcall)?;
            if as_function {
                func.call::<Value>((keys, args))
            } else {
                func.call::<Value>(())
            }
        })?;
        let mut budget = MAX_REPLY_ITEMS;
        from_lua(value, 0, &mut budget)
    }

    fn strings(&self, items: &[Vec<u8>]) -> mlua::Result<Table> {
        let table = self.lua.create_table_with_capacity(items.len(), 0)?;
        for (i, item) in items.iter().enumerate() {
            table.raw_set(i + 1, self.lua.create_string(item)?)?;
        }
        Ok(table)
    }
}

/// `redis.call`: run one keyspace command for the running script.
fn redis_call(engine: &Engine, state: &AtomicU8, args: MultiValue) -> mlua::Result<Reply> {
    let mut argv = Vec::with_capacity(args.len());
    for arg in args {
        argv.push(match arg {
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Integer(i) => i.to_string().into_bytes(),
            Value::Number(n) => format_number(n).into_bytes(),
            _ => {
                return Err(mlua::Error::runtime(
                    "Lua redis() command arguments must be strings or integers",
                ))
            }
        });
    }
    if argv.is_empty() {
        return Err(mlua::Error::runtime(
            "Please specify at least one argument for redis.call()",
        ));
    }
    // A script may do what the user who runs it may.
    let named = acl::caller_restricted().then(|| (argv[0].clone(), argv.get(1).cloned()));
    let cmd = match Cmd::parse(argv) {
        Ok(cmd) => cmd,
        Err(reply) => return Ok(reply),
    };
    if let Some((name, sub)) = &named {
        if let Err(denied) = acl::caller_check(name, sub.as_deref(), &cmd) {
            return Ok(denied);
        }
    }
    // Valkey counts what may replicate as a write here: PUBLISH, and
    // PFCOUNT, which can rewrite the cache in its key.
    let may_replicate = matches!(cmd, Cmd::Publish(..) | Cmd::SPublish(..))
        || matches!(&cmd, Cmd::Multi(op) if matches!(**op, crate::ops::MultiOp::PfCount(_)));
    if may_replicate && engine.scripts.read_only.load(SeqCst) {
        return Ok(Reply::err(
            "ERR Write commands are not allowed from read-only scripts.",
        ));
    }
    if matches!(
        cmd,
        Cmd::Ping(_) | Cmd::Echo(_) | Cmd::Dbsize | Cmd::Publish(..) | Cmd::SPublish(..)
    ) {
        return Ok(engine.exec_conn(&cmd));
    }
    // A script has the node to itself, which is all SORT with patterns asks.
    let sort = matches!(&cmd, Cmd::Multi(op) if matches!(**op, crate::ops::MultiOp::Sort { .. }));
    if cmd.class() != Class::Shared && !sort {
        return Ok(Reply::err(
            "ERR This Redis command is not allowed from script",
        ));
    }
    if cmd.is_write() {
        if engine.scripts.read_only.load(SeqCst) {
            return Ok(Reply::err(
                "ERR Write commands are not allowed from read-only scripts.",
            ));
        }
        if engine.is_replica() {
            return Ok(Reply::err(crate::cmd::READONLY));
        }
        // From here on the script cannot be killed; if the kill came first,
        // the write must not happen.
        match state.compare_exchange(PURE, WROTE, SeqCst, SeqCst) {
            Ok(_) | Err(WROTE) => {}
            Err(_) => return Err(mlua::Error::runtime(KILLED)),
        }
    }
    Ok(engine.apply_excl(cmd))
}

fn format_number(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 9.0e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

fn to_lua(lua: &Lua, reply: Reply) -> mlua::Result<Value> {
    Ok(match reply {
        Reply::Nil | Reply::NilArray => Value::Boolean(false),
        Reply::Int(n) => Value::Number(n as f64),
        Reply::Bulk(bytes) => Value::String(lua.create_string(&bytes)?),
        Reply::Ok => Value::Table(tagged(lua, "ok", "OK")?),
        Reply::Simple(text) => Value::Table(tagged(lua, "ok", &text)?),
        Reply::Error(text) => Value::Table(tagged(lua, "err", &text)?),
        Reply::Array(items) | Reply::Map(items) => {
            let table = lua.create_table_with_capacity(items.len(), 0)?;
            for (i, item) in items.into_iter().enumerate() {
                table.raw_set(i + 1, to_lua(lua, item)?)?;
            }
            Value::Table(table)
        }
    })
}

fn tagged(lua: &Lua, field: &str, text: &str) -> mlua::Result<Table> {
    let table = lua.create_table_with_capacity(0, 1)?;
    table.raw_set(field, text)?;
    Ok(table)
}

/// A script's return value as a reply, by the Redis conversion rules.
fn from_lua(value: Value, depth: usize, budget: &mut usize) -> mlua::Result<Reply> {
    if *budget == 0 {
        return Err(mlua::Error::runtime("script result is too large"));
    }
    *budget -= 1;
    Ok(match value {
        Value::Integer(i) => Reply::Int(i),
        // Saturating, NaN becomes 0.
        Value::Number(n) => Reply::Int(n as i64),
        Value::String(s) => Reply::Bulk(s.as_bytes().to_vec()),
        Value::Boolean(true) => Reply::Int(1),
        Value::Table(table) => {
            if depth >= MAX_REPLY_DEPTH {
                return Err(mlua::Error::runtime("reached lua stack limit"));
            }
            if let Value::String(msg) = table.raw_get::<Value>("err")? {
                return Ok(Reply::Error(msg.to_string_lossy()));
            }
            if let Value::String(msg) = table.raw_get::<Value>("ok")? {
                return Ok(Reply::Simple(msg.to_string_lossy()));
            }
            let mut items = Vec::new();
            for i in 1.. {
                match table.raw_get::<Value>(i)? {
                    Value::Nil => break,
                    item => items.push(from_lua(item, depth + 1, budget)?),
                }
            }
            Reply::Array(items)
        }
        _ => Reply::Nil,
    })
}

fn root_cause(mut e: &mlua::Error) -> &mlua::Error {
    loop {
        match e {
            mlua::Error::CallbackError { cause, .. } => e = cause,
            mlua::Error::WithContext { cause, .. } => e = cause,
            _ => return e,
        }
    }
}

fn is_memory_error(e: &mlua::Error) -> bool {
    matches!(root_cause(e), mlua::Error::MemoryError(_))
}

/// First line of the underlying error, bounded in length.
fn describe(e: &mlua::Error) -> String {
    let text = match root_cause(e) {
        mlua::Error::RuntimeError(msg) | mlua::Error::MemoryError(msg) => msg.clone(),
        mlua::Error::SyntaxError { message, .. } => message.clone(),
        other => other.to_string(),
    };
    let line = text.lines().next().unwrap_or_default();
    let mut end = line.len().min(MAX_ERROR_LEN);
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end].to_string()
}
