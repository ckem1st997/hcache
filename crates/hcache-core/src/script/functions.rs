//! FUNCTION and FCALL: libraries of named Lua functions.
//!
//! A library is Lua source that starts with `#!lua name=<library>` and
//! calls `redis.register_function` for each function it offers. The
//! source is kept, so a Lua state that had to be thrown away gets its
//! libraries back the next time one of their functions is called.
//! A database with a data file keeps its libraries there (`persist.rs`).

use super::{
    describe, is_memory_error, mono_ms, Scripts, Vm, GC_AFTER_SCRIPT_FRACTION, IDLE,
    LOAD_TIMEOUT_MS, PURE,
};
use crate::cmd::Bytes;
use crate::engine::Engine;
use crate::glob::glob_match;
use crate::resp::Reply;
use mlua::{ChunkMode, Function, Lua, MultiValue, Table, Value};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

const MAX_LIBRARIES: usize = 1000;
const MAX_LIBRARY_BYTES: usize = 1 << 20;
const MAX_FUNCTIONS_PER_LIBRARY: usize = 1000;
const MAX_NAME: usize = 256;
const FLAGS: [&str; 5] = [
    "no-writes",
    "allow-oom",
    "allow-stale",
    "no-cluster",
    "allow-cross-slot-keys",
];
const NOT_FOUND: &str = "ERR Function not found";

/// What one `redis.register_function` call asked for.
pub(super) struct Registered {
    name: String,
    func: Function,
    flags: Vec<String>,
    description: Option<String>,
}

/// Where `redis.register_function` puts its calls; `None` outside
/// FUNCTION LOAD.
pub(super) type Registering = Arc<Mutex<Option<Vec<Registered>>>>;

struct Meta {
    name: String,
    flags: Vec<String>,
    description: Option<String>,
}

impl Meta {
    fn no_writes(&self) -> bool {
        self.flags.iter().any(|f| f == "no-writes")
    }
}

struct Library {
    code: Arc<[u8]>,
    functions: Vec<Meta>,
}

#[derive(Default)]
pub(super) struct Libraries {
    by_name: BTreeMap<String, Library>,
    /// Function name to the library that has it.
    owner: HashMap<String, String>,
}

fn name_ok(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn text(value: Value, what: &str) -> mlua::Result<String> {
    match value {
        Value::String(s) => Ok(s.to_string_lossy()),
        _ => Err(mlua::Error::runtime(format!(
            "{what} argument must be a string"
        ))),
    }
}

fn registration(args: MultiValue) -> mlua::Result<Registered> {
    let mut args = args.into_iter();
    let wrong = || mlua::Error::runtime("wrong number of arguments to redis.register_function");
    let (mut name, mut func, mut flags, mut description) = (None, None, Vec::new(), None);
    match (args.next(), args.next(), args.next()) {
        (Some(Value::Table(named)), None, None) => {
            for pair in named.pairs::<Value, Value>() {
                let (key, value) = pair?;
                match text(key, "named")?.as_str() {
                    "function_name" => name = Some(text(value, "function_name")?),
                    "description" => description = Some(text(value, "description")?),
                    "callback" => match value {
                        Value::Function(f) => func = Some(f),
                        _ => {
                            return Err(mlua::Error::runtime(
                                "callback argument must be a function",
                            ))
                        }
                    },
                    "flags" => {
                        let Value::Table(list) = value else {
                            return Err(mlua::Error::runtime("flags argument must be a table"));
                        };
                        for flag in list.sequence_values::<Value>() {
                            let flag = text(flag?, "flags")?;
                            if !FLAGS.contains(&flag.as_str()) {
                                return Err(mlua::Error::runtime("unknown flag given"));
                            }
                            flags.push(flag);
                        }
                    }
                    _ => {
                        return Err(mlua::Error::runtime(
                            "unknown argument given to redis.register_function",
                        ))
                    }
                }
            }
        }
        (Some(first), Some(Value::Function(f)), None) => {
            name = Some(text(first, "function_name")?);
            func = Some(f);
        }
        _ => return Err(wrong()),
    }
    let (Some(name), Some(func)) = (name, func) else {
        return Err(mlua::Error::runtime(
            "redis.register_function must get a function name and a callback",
        ));
    };
    if !name_ok(&name) {
        return Err(mlua::Error::runtime(
            "Function names can only contain letters, numbers, or underscores(_) and must be \
             at least one character long",
        ));
    }
    Ok(Registered {
        name,
        func,
        flags,
        description,
    })
}

/// Give the sandbox its `redis.register_function`.
pub(super) fn install(lua: &Lua, redis: &Table) -> mlua::Result<Registering> {
    let registering: Registering = Arc::new(Mutex::new(None));
    let slot = registering.clone();
    let register = lua.create_function(move |_, args: MultiValue| {
        let registered = registration(args)?;
        let mut slot = slot.lock();
        let Some(list) = slot.as_mut() else {
            return Err(mlua::Error::runtime(
                "redis.register_function can only be called on FUNCTION LOAD command",
            ));
        };
        if list.iter().any(|r| r.name == registered.name) {
            return Err(mlua::Error::runtime(
                "Function already exists in the library",
            ));
        }
        if list.len() >= MAX_FUNCTIONS_PER_LIBRARY {
            return Err(mlua::Error::runtime("too many functions in the library"));
        }
        list.push(registered);
        Ok(())
    })?;
    redis.raw_set("register_function", register)?;
    Ok(registering)
}

/// The library name in the `#!lua name=<library>` line.
fn library_name(code: &[u8]) -> Result<String, Reply> {
    let line = code.split(|&b| b == b'\n').next().unwrap_or_default();
    let line = std::str::from_utf8(line).unwrap_or_default().trim_end();
    let Some(meta) = line.strip_prefix("#!") else {
        return Err(Reply::err("ERR Missing library metadata"));
    };
    let mut words = meta.split_ascii_whitespace();
    match words.next() {
        Some("lua") => {}
        Some(engine) => {
            let shown: String = engine.chars().take(64).collect();
            return Err(Reply::Error(format!("ERR Engine '{shown}' not found")));
        }
        None => return Err(Reply::err("ERR Missing library metadata")),
    }
    let mut name = None;
    for word in words {
        match word.strip_prefix("name=") {
            Some(value) if name.is_none() => name = Some(value),
            _ => {
                let shown: String = word.chars().take(64).collect();
                return Err(Reply::Error(format!(
                    "ERR Invalid metadata value given: {shown}"
                )));
            }
        }
    }
    match name {
        Some(name) if name_ok(name) => Ok(name.to_string()),
        Some(_) => Err(Reply::err(
            "ERR Library names can only contain letters, numbers, or underscores(_) and must \
             be at least one character long",
        )),
        None => Err(Reply::err("ERR Library name was not given")),
    }
}

impl Vm {
    /// Run a library's source and collect what it registers.
    fn register(&mut self, code: &[u8]) -> mlua::Result<Vec<Registered>> {
        // The `#!` line is not Lua: it becomes a comment, so that line
        // numbers in errors stay right.
        let mut chunk = Vec::with_capacity(code.len() + 2);
        chunk.extend_from_slice(b"--");
        chunk.extend_from_slice(code);
        let func = self
            .lua
            .load(&chunk[..])
            .set_name("=user_function")
            .set_mode(ChunkMode::Text)
            .into_function()?;
        func.set_environment(self.env.clone())?;
        // No commands while a library loads, and nothing of the last script.
        self.hidden.raw_set("pcall", Value::Nil)?;
        self.real.raw_set("KEYS", Value::Nil)?;
        self.real.raw_set("ARGV", Value::Nil)?;
        *self.registering.lock() = Some(Vec::new());
        let result = func.call::<()>(());
        let registered = self.registering.lock().take().unwrap_or_default();
        result.map(|()| registered)
    }
}

impl Scripts {
    fn started<'a>(&self, slot: &'a mut Option<Vm>) -> Result<&'a mut Vm, Reply> {
        match slot {
            Some(vm) => Ok(vm),
            None => match Vm::new(
                self.state.clone(),
                self.load_until.clone(),
                self.memory_limit,
            ) {
                Ok(vm) => Ok(slot.insert(vm)),
                Err(e) => Err(Reply::Error(format!(
                    "ERR cannot start Lua: {}",
                    describe(&e)
                ))),
            },
        }
    }

    /// FUNCTION with all its arguments, "FUNCTION" included. The caller
    /// holds the exclusive side of the gate.
    pub(crate) fn function(&self, args: &[Bytes]) -> Reply {
        let sub = args.get(1).map(|s| s.to_ascii_uppercase());
        let arity = |sub: &str| {
            Reply::Error(format!(
                "ERR wrong number of arguments for 'function|{sub}' command"
            ))
        };
        match (
            sub.as_deref().unwrap_or_default(),
            &args[2.min(args.len())..],
        ) {
            (b"LOAD", [code]) => self.function_load(code, false),
            (b"LOAD", [replace, code]) if replace.eq_ignore_ascii_case(b"REPLACE") => {
                self.function_load(code, true)
            }
            (b"LOAD", _) => arity("load"),
            (b"DELETE", [name]) => self.function_delete(name),
            (b"DELETE", _) => arity("delete"),
            (b"FLUSH", []) => self.function_flush(),
            (b"FLUSH", [mode])
                if mode.eq_ignore_ascii_case(b"ASYNC") || mode.eq_ignore_ascii_case(b"SYNC") =>
            {
                self.function_flush()
            }
            (b"FLUSH", _) => Reply::err("ERR FUNCTION FLUSH only supports SYNC|ASYNC option"),
            (b"LIST", rest) => self.function_list(rest),
            (b"STATS", []) => self.function_stats(),
            (b"STATS", _) => arity("stats"),
            (b"KILL", []) => self.kill(),
            (b"KILL", _) => arity("kill"),
            (b"DUMP" | b"RESTORE", _) => {
                Reply::err("ERR FUNCTION DUMP and FUNCTION RESTORE are not supported")
            }
            (b"HELP", _) => Reply::Array(
                [
                    "FUNCTION LOAD [REPLACE] <code>",
                    "FUNCTION DELETE <library>",
                    "FUNCTION FLUSH [ASYNC|SYNC]",
                    "FUNCTION LIST [LIBRARYNAME <pattern>] [WITHCODE]",
                    "FUNCTION STATS",
                    "FUNCTION KILL",
                ]
                .into_iter()
                .map(Reply::bulk)
                .collect(),
            ),
            _ => Reply::err("ERR unknown subcommand or wrong number of arguments for 'function'"),
        }
    }

    fn function_load(&self, code: &[u8], replace: bool) -> Reply {
        let name = match library_name(code) {
            Ok(name) => name,
            Err(reply) => return reply,
        };
        if code.len() > MAX_LIBRARY_BYTES {
            return Reply::err("ERR library code is too large");
        }
        {
            let libs = self.libraries.lock();
            let exists = libs.by_name.contains_key(&name);
            if exists && !replace {
                return Reply::Error(format!("ERR Library '{name}' already exists"));
            }
            if !exists && libs.by_name.len() >= MAX_LIBRARIES {
                return Reply::err("ERR too many libraries");
            }
        }
        let mut slot = self.vm.lock();
        let vm = match self.started(&mut slot) {
            Ok(vm) => vm,
            Err(reply) => return reply,
        };
        self.state.store(PURE, SeqCst);
        self.load_until
            .store(mono_ms().saturating_add(LOAD_TIMEOUT_MS), SeqCst);
        let result = vm.register(code);
        self.load_until.store(0, SeqCst);
        self.state.store(IDLE, SeqCst);
        let registered = match result {
            Ok(registered) => registered,
            Err(e) => {
                let reply =
                    Reply::Error(format!("ERR Error registering functions: {}", describe(&e)));
                if is_memory_error(&e) {
                    *slot = None;
                }
                return reply;
            }
        };
        if registered.is_empty() {
            return Reply::err("ERR No functions registered");
        }
        let mut libs = self.libraries.lock();
        let taken = registered
            .iter()
            .find(|r| libs.owner.get(&r.name).is_some_and(|owner| *owner != name));
        if let Some(taken) = taken {
            return Reply::Error(format!("ERR Function {} already exists", taken.name));
        }
        if let Some(old) = libs.by_name.remove(&name) {
            for meta in old.functions {
                libs.owner.remove(&meta.name);
                vm.functions.remove(&meta.name);
            }
        }
        let mut functions = Vec::with_capacity(registered.len());
        for r in registered {
            libs.owner.insert(r.name.clone(), name.clone());
            vm.functions.insert(r.name.clone(), r.func);
            functions.push(Meta {
                name: r.name,
                flags: r.flags,
                description: r.description,
            });
        }
        libs.by_name.insert(
            name.clone(),
            Library {
                code: code.into(),
                functions,
            },
        );
        self.library_gen.fetch_add(1, SeqCst);
        Reply::Bulk(name.into_bytes())
    }

    /// Changes whenever a library is loaded, replaced or removed.
    pub(crate) fn library_gen(&self) -> u64 {
        self.library_gen.load(SeqCst)
    }

    /// The source of every library, by name.
    pub(crate) fn library_sources(&self) -> HashMap<String, Arc<[u8]>> {
        let libs = self.libraries.lock();
        libs.by_name
            .iter()
            .map(|(name, library)| (name.clone(), library.code.clone()))
            .collect()
    }

    /// A library of an RDB file, which may not replace one of the same name:
    /// its name.
    pub(crate) fn add_library(&self, code: &[u8]) -> Result<String, String> {
        match self.function_load(code, false) {
            Reply::Bulk(name) => Ok(String::from_utf8_lossy(&name).into_owned()),
            Reply::Error(e) => Err(format!("a library of functions does not load: {e}")),
            _ => Err("a library of functions does not load".into()),
        }
    }

    /// Recovery: load a library the data file holds.
    pub(crate) fn restore_library(&self, code: &[u8]) -> Result<(), String> {
        match self.function_load(code, true) {
            Reply::Error(e) => Err(format!("a library of functions does not load: {e}")),
            _ => Ok(()),
        }
    }

    /// Recovery: the data file says this library was deleted.
    pub(crate) fn forget_library(&self, name: &[u8]) {
        // Not there any more is what the record asks for.
        let _ = self.function_delete(name);
    }

    fn function_delete(&self, name: &[u8]) -> Reply {
        let mut slot = self.vm.lock();
        let mut libs = self.libraries.lock();
        let old = std::str::from_utf8(name)
            .ok()
            .and_then(|name| libs.by_name.remove(name));
        let Some(old) = old else {
            return Reply::err("ERR Library not found");
        };
        for meta in old.functions {
            libs.owner.remove(&meta.name);
            if let Some(vm) = slot.as_mut() {
                vm.functions.remove(&meta.name);
            }
        }
        self.library_gen.fetch_add(1, SeqCst);
        Reply::Ok
    }

    fn function_flush(&self) -> Reply {
        let mut slot = self.vm.lock();
        let old = std::mem::take(&mut *self.libraries.lock());
        if let Some(vm) = slot.as_mut() {
            vm.functions.clear();
        }
        drop(old);
        self.library_gen.fetch_add(1, SeqCst);
        Reply::Ok
    }

    fn function_list(&self, mut rest: &[Bytes]) -> Reply {
        let mut pattern = None;
        let mut with_code = false;
        while let Some(option) = rest.first() {
            if option.eq_ignore_ascii_case(b"WITHCODE") {
                with_code = true;
                rest = &rest[1..];
            } else if option.eq_ignore_ascii_case(b"LIBRARYNAME") && rest.len() >= 2 {
                pattern = Some(&rest[1]);
                rest = &rest[2..];
            } else {
                return Reply::err("ERR syntax error");
            }
        }
        let libs = self.libraries.lock();
        let mut out = Vec::new();
        for (name, library) in &libs.by_name {
            if pattern.is_some_and(|p| !glob_match(p, name.as_bytes())) {
                continue;
            }
            let functions = library.functions.iter().map(|meta| {
                Reply::Map(vec![
                    Reply::bulk("name"),
                    Reply::bulk(meta.name.as_str()),
                    Reply::bulk("description"),
                    meta.description.as_deref().map_or(Reply::Nil, Reply::bulk),
                    Reply::bulk("flags"),
                    Reply::Array(meta.flags.iter().map(|f| Reply::bulk(f.as_str())).collect()),
                ])
            });
            let mut item = vec![
                Reply::bulk("library_name"),
                Reply::bulk(name.as_str()),
                Reply::bulk("engine"),
                Reply::bulk("LUA"),
                Reply::bulk("functions"),
                Reply::Array(functions.collect()),
            ];
            if with_code {
                item.push(Reply::bulk("library_code"));
                item.push(Reply::Bulk(library.code.to_vec()));
            }
            out.push(Reply::Map(item));
        }
        Reply::Array(out)
    }

    fn function_stats(&self) -> Reply {
        let libs = self.libraries.lock();
        let counts = Reply::Map(vec![
            Reply::bulk("libraries_count"),
            Reply::Int(libs.by_name.len() as i64),
            Reply::bulk("functions_count"),
            Reply::Int(libs.owner.len() as i64),
        ]);
        Reply::Map(vec![
            Reply::bulk("running_script"),
            Reply::Nil,
            Reply::bulk("engines"),
            Reply::Map(vec![Reply::bulk("LUA"), counts]),
        ])
    }

    /// FCALL and FCALL_RO. The caller holds the exclusive side of the gate.
    pub(crate) fn fcall(
        &self,
        engine: &Engine,
        name: &[u8],
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        read_only: bool,
    ) -> Reply {
        let Ok(name) = std::str::from_utf8(name) else {
            return Reply::err(NOT_FOUND);
        };
        let mut slot = self.vm.lock();
        let (code, names, no_writes) = {
            let libs = self.libraries.lock();
            let library = libs.owner.get(name).and_then(|lib| libs.by_name.get(lib));
            let meta = library.and_then(|lib| {
                let meta = lib.functions.iter().find(|meta| meta.name == name)?;
                let names: Vec<String> = lib.functions.iter().map(|m| m.name.clone()).collect();
                Some((lib.code.clone(), names, meta.no_writes()))
            });
            match meta {
                Some(meta) => meta,
                None => return Reply::err(NOT_FOUND),
            }
        };
        if read_only && !no_writes {
            return Reply::err("ERR Can not execute a script with write flag using *_ro command.");
        }
        self.set_read_only(read_only || no_writes);
        let vm = match self.started(&mut slot) {
            Ok(vm) => vm,
            Err(reply) => return reply,
        };
        if !vm.functions.contains_key(name) {
            // A new Lua state: the library is loaded into it again.
            self.state.store(PURE, SeqCst);
            let result = vm.register(&code);
            self.state.store(IDLE, SeqCst);
            match result {
                // The names were checked when the library was loaded; a
                // library that registers others now would take what is
                // not its own.
                Ok(registered)
                    if registered.len() == names.len()
                        && registered.iter().all(|r| names.contains(&r.name)) =>
                {
                    for r in registered {
                        vm.functions.insert(r.name, r.func);
                    }
                }
                Ok(_) => {
                    return Reply::err(
                        "ERR Error loading library: it registers other functions than before",
                    );
                }
                Err(e) => {
                    let reply =
                        Reply::Error(format!("ERR Error loading library: {}", describe(&e)));
                    if is_memory_error(&e) {
                        *slot = None;
                    }
                    return reply;
                }
            }
        }
        let Some(func) = vm.functions.get(name).cloned() else {
            return Reply::err(NOT_FOUND);
        };
        self.state.store(PURE, SeqCst);
        let result = vm.run(engine, &self.state, &func, &keys, &args, true);
        self.state.store(IDLE, SeqCst);
        if vm.lua.used_memory() > self.memory_limit / GC_AFTER_SCRIPT_FRACTION {
            let _ = vm.lua.gc_collect();
        }
        match result {
            Ok(reply) => reply,
            Err(e) => {
                if is_memory_error(&e) {
                    *slot = None;
                }
                Reply::Error(format!("ERR Error running function: {}", describe(&e)))
            }
        }
    }
}
