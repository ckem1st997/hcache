//! The Lua libraries Redis puts in its script state (`cjson`, `cmsgpack`,
//! `struct`, `bit`) and the `redis.*` helpers that are not commands. Queue
//! frameworks (BullMQ, Laravel, Sidekiq) call them from their own scripts.
//!
//! They are written in Rust against Redis 7 behaviour (lua-cjson 2.1.0,
//! lua-cmsgpack, Roberto Ierusalimschy's `struct`, LuaBitOp 1.0). The tables
//! are installed before the sandbox is locked, so scripts see them through the
//! same read-only proxies as `string` and `table`.
//!
//! Output is built in Rust memory the Lua allocator does not count, so every
//! encoder stops at the room the Lua heap has left; decoders build Lua values
//! and are stopped by the heap limit itself. No decoder or encoder recurses
//! deeper than a fixed bound, so hostile nesting cannot overflow the Rust stack.

mod json;
mod msgpack;

use mlua::{BorrowedBytes, Function, IntoLuaMulti, Lua, MultiValue, Table, Value};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

/// Output cap when the Lua heap has no limit.
const UNLIMITED_OUTPUT_CAP: usize = 1 << 30;
/// Values one call may return. mlua keeps every live Lua value in a slot of
/// a fixed pool (about 8000), and running out of slots panics, so the values
/// a decoder holds at once stay far below that.
const MAX_RESULTS: usize = 4000;
/// Arguments one library call may receive; they all become live values.
const MAX_ARGS: usize = 4000;

const CJSON_DEFAULT_ENCODE_DEPTH: usize = 1000;
const CJSON_DEFAULT_DECODE_DEPTH: usize = 1000;
/// A decoder keeps two live values per open container, so the configured
/// decode depth is capped here.
const CJSON_DECODE_DEPTH_CEILING: usize = 1500;
/// The encoder recurses once per nesting level, so the configured depth is
/// capped here to keep the Rust stack safe.
const CJSON_ENCODE_DEPTH_CEILING: usize = 1000;
const CJSON_DEFAULT_SPARSE_RATIO: usize = 2;
const CJSON_DEFAULT_SPARSE_SAFE: usize = 10;
const INT_MAX: usize = i32::MAX as usize;
const INVALID_NUMBERS_ERROR: u8 = 0;
const INVALID_NUMBERS_ALLOW: u8 = 1;
const INVALID_NUMBERS_NULL: u8 = 2;

/// lua-cmsgpack stops encoding tables below this depth and writes nil.
const MSGPACK_MAX_NESTING: usize = 16;

const STRUCT_MAX_INT_SIZE: usize = 32;
const STRUCT_MAX_ALIGN: usize = 8;
const SIZEOF_SHORT: usize = 2;
const SIZEOF_INT: usize = 4;
/// Redis runs on LP64 systems.
const SIZEOF_LONG: usize = 8;
const SIZEOF_SIZE_T: usize = 8;

const REDIS_VERSION: &str = "7.4.0";
const REDIS_VERSION_NUM: f64 = 0x0007_0400 as f64;

type LuaResult<T> = mlua::Result<T>;

fn rt(msg: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(msg)
}

fn oom() -> mlua::Error {
    rt("not enough memory")
}

/// What `luaL_argerror` would say.
fn bad_arg(n: usize, func: &str, msg: &str) -> mlua::Error {
    rt(format!("bad argument #{n} to '{func}' ({msg})"))
}

fn lua_type_name(v: Option<&Value>) -> &'static str {
    match v {
        None => "no value",
        Some(Value::Nil) => "nil",
        Some(Value::Boolean(_)) => "boolean",
        Some(Value::Integer(_) | Value::Number(_)) => "number",
        Some(Value::String(_)) => "string",
        Some(Value::Table(_)) => "table",
        Some(Value::Function(_)) => "function",
        Some(Value::Thread(_)) => "thread",
        Some(_) => "userdata",
    }
}

/// `lua_tonumber` on a string: surrounding blanks, decimal or hexadecimal.
fn parse_number(bytes: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(bytes)
        .ok()?
        .trim_matches(|c: char| c.is_ascii_whitespace() || c == '\x0b');
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if let Some(hex) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
        let value = u64::from_str_radix(hex, 16).ok()? as f64;
        return Some(if negative { -value } else { value });
    }
    text.parse::<f64>().ok()
}

fn number_of(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Number(n) => Some(*n),
        Value::String(s) => parse_number(&s.as_bytes()),
        _ => None,
    }
}

/// C's `%.14g`, which is how Lua 5.1 prints a number and how lua-cjson
/// writes one.
fn format_g14(n: f64) -> String {
    if n.is_nan() {
        return "nan".into();
    }
    if n.is_infinite() {
        return if n < 0.0 { "-inf" } else { "inf" }.into();
    }
    if n == 0.0 {
        return if n.is_sign_negative() { "-0" } else { "0" }.into();
    }
    let scientific = format!("{n:.13e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    if (-4..14).contains(&exponent) {
        let fixed = format!("{:.*}", (13 - exponent) as usize, n);
        trim_fraction(&fixed).to_string()
    } else {
        let sign = if exponent < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim_fraction(mantissa), exponent.abs())
    }
}

fn trim_fraction(text: &str) -> &str {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        text
    }
}

/// A string argument: Lua strings are borrowed, numbers are converted.
enum Bytes<'a> {
    Lua(BorrowedBytes<'a>),
    Owned(Vec<u8>),
}

impl Deref for Bytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Bytes::Lua(b) => b,
            Bytes::Owned(v) => v,
        }
    }
}

fn check_bytes<'a>(args: &'a MultiValue, i: usize, func: &str) -> LuaResult<Bytes<'a>> {
    match args.get(i) {
        Some(Value::String(s)) => Ok(Bytes::Lua(s.as_bytes())),
        Some(v @ (Value::Integer(_) | Value::Number(_))) => {
            let n = number_of(v).unwrap_or(0.0);
            Ok(Bytes::Owned(format_g14(n).into_bytes()))
        }
        other => Err(bad_arg(
            i + 1,
            func,
            &format!("string expected, got {}", lua_type_name(other)),
        )),
    }
}

fn check_number(args: &MultiValue, i: usize, func: &str) -> LuaResult<f64> {
    let arg = args.get(i);
    arg.and_then(number_of).ok_or_else(|| {
        bad_arg(
            i + 1,
            func,
            &format!("number expected, got {}", lua_type_name(arg)),
        )
    })
}

/// `luaL_optinteger`: absent or nil gives `None`.
fn opt_integer(args: &MultiValue, i: usize, func: &str) -> LuaResult<Option<i64>> {
    match args.get(i) {
        None | Some(Value::Nil) => Ok(None),
        Some(_) => Ok(Some(check_number(args, i, func)? as i64)),
    }
}

fn string_value(lua: &Lua, bytes: &[u8]) -> LuaResult<Value> {
    Ok(Value::String(lua.create_string(bytes)?))
}

/// Room the Lua heap has left for a result built outside it.
fn output_cap(lua: &Lua, memory_limit: usize) -> usize {
    if memory_limit == 0 {
        UNLIMITED_OUTPUT_CAP
    } else {
        memory_limit.saturating_sub(lua.used_memory())
    }
}

/// Settings of `cjson.*` that scripts can change; reset before every script
/// so one script cannot change how the next one encodes.
pub(super) struct JsonCfg {
    encode_depth: AtomicUsize,
    decode_depth: AtomicUsize,
    sparse_convert: AtomicBool,
    sparse_ratio: AtomicUsize,
    sparse_safe: AtomicUsize,
    invalid_numbers: AtomicU8,
}

impl JsonCfg {
    fn new() -> JsonCfg {
        JsonCfg {
            encode_depth: AtomicUsize::new(CJSON_DEFAULT_ENCODE_DEPTH),
            decode_depth: AtomicUsize::new(CJSON_DEFAULT_DECODE_DEPTH),
            sparse_convert: AtomicBool::new(false),
            sparse_ratio: AtomicUsize::new(CJSON_DEFAULT_SPARSE_RATIO),
            sparse_safe: AtomicUsize::new(CJSON_DEFAULT_SPARSE_SAFE),
            invalid_numbers: AtomicU8::new(INVALID_NUMBERS_ERROR),
        }
    }

    pub(super) fn reset(&self) {
        self.encode_depth.store(CJSON_DEFAULT_ENCODE_DEPTH, Relaxed);
        self.decode_depth.store(CJSON_DEFAULT_DECODE_DEPTH, Relaxed);
        self.sparse_convert.store(false, Relaxed);
        self.sparse_ratio.store(CJSON_DEFAULT_SPARSE_RATIO, Relaxed);
        self.sparse_safe.store(CJSON_DEFAULT_SPARSE_SAFE, Relaxed);
        self.invalid_numbers.store(INVALID_NUMBERS_ERROR, Relaxed);
    }
}

/// Wraps the Rust functions of the libraries. A Rust callback can only fail
/// with an error object, but scripts expect the string `error()` would give:
/// `pcall(cjson.decode, text)` is commonly followed by a string match. So the
/// callback returns `true, results...` or `false, message`, and a small Lua
/// function turns the second form into a string error.
struct Reg<'a> {
    lua: &'a Lua,
    wrap: Function,
}

const WRAP: &str = r#"
local rerror, rselect = error, select
local function check(ok, ...)
  if ok then return ... end
  rerror((...), 0)
end
return function(f, max_args)
  return function(...)
    if rselect('#', ...) > max_args then rerror('too many arguments', 0) end
    return check(f(...))
  end
end
"#;

impl<'a> Reg<'a> {
    fn new(lua: &'a Lua) -> LuaResult<Reg<'a>> {
        let wrap = lua.load(WRAP).set_name("=libs").call(())?;
        Ok(Reg { lua, wrap })
    }

    fn wrap_fn<R: IntoLuaMulti>(
        &self,
        f: impl Fn(&Lua, MultiValue) -> LuaResult<R> + Send + 'static,
    ) -> LuaResult<Function> {
        let inner = self
            .lua
            .create_function(move |lua, args: MultiValue| match f(lua, args) {
                Ok(results) => {
                    let mut out = results.into_lua_multi(lua)?;
                    out.push_front(Value::Boolean(true));
                    Ok(out)
                }
                // A failed allocation must stay a memory error: the node
                // replaces a Lua state that ran out of memory.
                Err(e @ mlua::Error::MemoryError(_)) => Err(e),
                Err(e) => Ok(MultiValue::from_iter([
                    Value::Boolean(false),
                    Value::String(lua.create_string(error_message(&e))?),
                ])),
            })?;
        self.wrap.call((inner, MAX_ARGS))
    }
}

fn error_message(e: &mlua::Error) -> String {
    match e {
        mlua::Error::RuntimeError(msg) => msg.clone(),
        mlua::Error::CallbackError { cause, .. } => error_message(cause),
        other => other.to_string(),
    }
}

/// The library tables, by global name, and the settings handle.
pub(super) fn install(lua: &Lua, memory_limit: usize) -> LuaResult<(Table, Arc<JsonCfg>)> {
    let reg = Reg::new(lua)?;
    let libs = lua.create_table()?;
    let cfg = Arc::new(JsonCfg::new());
    libs.raw_set("cjson", json::build(lua, &reg, memory_limit, &cfg)?)?;
    libs.raw_set("cmsgpack", msgpack::build(lua, &reg, memory_limit)?)?;
    libs.raw_set("struct", struct_lib(lua, &reg, memory_limit)?)?;
    libs.raw_set("bit", bit_lib(lua, &reg)?)?;
    Ok((libs, cfg))
}

// ---------------------------------------------------------------------
// bit (LuaBitOp)
// ---------------------------------------------------------------------

/// LuaBitOp's conversion: round to nearest, then wrap modulo 2^32.
fn to_bit(n: f64) -> i32 {
    if !n.is_finite() {
        return 0;
    }
    let wrapped = n.round_ties_even().rem_euclid(4_294_967_296.0);
    wrapped as u64 as u32 as i32
}

fn bit_arg(args: &MultiValue, i: usize, func: &str) -> LuaResult<i32> {
    Ok(to_bit(check_number(args, i, func)?))
}

type Unary = fn(i32) -> i32;
type Fold = fn(i32, i32) -> i32;
type Shift = fn(i32, u32) -> i32;

fn bit_lib(lua: &Lua, reg: &Reg) -> LuaResult<Table> {
    let bit = lua.create_table()?;
    let unary: [(&'static str, Unary); 3] = [
        ("tobit", |x| x),
        ("bnot", |x| !x),
        ("bswap", i32::swap_bytes),
    ];
    for (name, op) in unary {
        bit.raw_set(
            name,
            reg.wrap_fn(move |_, args: MultiValue| {
                Ok(Value::Number(f64::from(op(bit_arg(&args, 0, name)?))))
            })?,
        )?;
    }
    let folds: [(&'static str, Fold); 3] = [
        ("band", |a, b| a & b),
        ("bor", |a, b| a | b),
        ("bxor", |a, b| a ^ b),
    ];
    for (name, op) in folds {
        bit.raw_set(
            name,
            reg.wrap_fn(move |_, args: MultiValue| {
                let mut acc = bit_arg(&args, 0, name)?;
                for i in 1..args.len() {
                    acc = op(acc, bit_arg(&args, i, name)?);
                }
                Ok(Value::Number(f64::from(acc)))
            })?,
        )?;
    }
    let shifts: [(&'static str, Shift); 5] = [
        ("lshift", |x, n| ((x as u32) << n) as i32),
        ("rshift", |x, n| ((x as u32) >> n) as i32),
        ("arshift", |x, n| x >> n),
        ("rol", |x, n| (x as u32).rotate_left(n) as i32),
        ("ror", |x, n| (x as u32).rotate_right(n) as i32),
    ];
    for (name, op) in shifts {
        bit.raw_set(
            name,
            reg.wrap_fn(move |_, args: MultiValue| {
                let x = bit_arg(&args, 0, name)?;
                let n = bit_arg(&args, 1, name)? as u32 & 31;
                Ok(Value::Number(f64::from(op(x, n))))
            })?,
        )?;
    }
    bit.raw_set(
        "tohex",
        reg.wrap_fn(|lua, args: MultiValue| {
            let x = bit_arg(&args, 0, "tohex")? as u32;
            let mut n = if args.len() > 1 {
                bit_arg(&args, 1, "tohex")?
            } else {
                8
            };
            let upper = n < 0;
            if n == i32::MIN {
                n = i32::MIN + 1;
            }
            let digits = n.unsigned_abs().min(8) as usize;
            let full = if upper {
                format!("{x:08X}")
            } else {
                format!("{x:08x}")
            };
            string_value(lua, &full.as_bytes()[8 - digits..])
        })?,
    )?;
    Ok(bit)
}

// ---------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------

struct StructHeader {
    little: bool,
    align: usize,
}

fn struct_number(fmt: &[u8], pos: &mut usize, default: usize) -> usize {
    if !fmt.get(*pos).is_some_and(u8::is_ascii_digit) {
        return default;
    }
    let mut value = 0usize;
    while let Some(d) = fmt.get(*pos).filter(|b| b.is_ascii_digit()) {
        value = value
            .saturating_mul(10)
            .saturating_add(usize::from(d - b'0'));
        *pos += 1;
    }
    value
}

/// Size of an option, reading its numeric suffix; 0 for options that take no
/// space (the format controls).
fn struct_optsize(fmt: &[u8], pos: &mut usize, opt: u8) -> LuaResult<usize> {
    Ok(match opt {
        b'B' | b'b' | b'x' => 1,
        b'H' | b'h' => SIZEOF_SHORT,
        b'L' | b'l' => SIZEOF_LONG,
        b'T' => SIZEOF_SIZE_T,
        b'f' => 4,
        b'd' => 8,
        b'c' => struct_number(fmt, pos, 1),
        b'i' | b'I' => {
            let size = struct_number(fmt, pos, SIZEOF_INT);
            if size > STRUCT_MAX_INT_SIZE {
                return Err(rt(format!(
                    "integral size {size} is larger than limit of {STRUCT_MAX_INT_SIZE}"
                )));
            }
            size
        }
        _ => 0,
    })
}

fn struct_toalign(len: usize, header: &StructHeader, opt: u8, size: usize) -> usize {
    if size == 0 || opt == b'c' {
        return 0;
    }
    let size = size.min(header.align);
    size.wrapping_sub(len & size.wrapping_sub(1)) & size.wrapping_sub(1)
}

fn struct_control(
    opt: u8,
    fmt: &[u8],
    pos: &mut usize,
    header: &mut StructHeader,
    func: &str,
) -> LuaResult<()> {
    match opt {
        b' ' => {}
        b'>' => header.little = false,
        b'<' => header.little = true,
        b'!' => {
            let align = struct_number(fmt, pos, STRUCT_MAX_ALIGN);
            if align == 0 || !align.is_power_of_two() {
                return Err(rt(format!("alignment {align} is not a power of 2")));
            }
            header.align = align;
        }
        _ => {
            return Err(bad_arg(
                1,
                func,
                &format!("invalid format option '{}'", char::from(opt)),
            ))
        }
    }
    Ok(())
}

fn struct_put_int(out: &mut Vec<u8>, n: f64, little: bool, size: usize) {
    let mut value: i64 = if n < 0.0 { n as i64 } else { n as u64 as i64 };
    let start = out.len();
    out.resize(start + size, 0);
    let buf = &mut out[start..];
    let mut put = |byte: &mut u8| {
        *byte = (value & 0xff) as u8;
        value >>= 8;
    };
    if little {
        buf.iter_mut().for_each(&mut put);
    } else {
        buf.iter_mut().rev().for_each(&mut put);
    }
}

fn struct_get_int(buf: &[u8], little: bool, signed: bool, size: usize) -> f64 {
    let mut acc = 0u64;
    let mut take = |b: &u8| acc = (acc << 8) | u64::from(*b);
    if little {
        buf.iter().rev().for_each(&mut take);
    } else {
        buf.iter().for_each(&mut take);
    }
    if !signed {
        return acc as f64;
    }
    if size == 0 {
        return 0.0;
    }
    if size >= 8 {
        return acc as i64 as f64;
    }
    let mask = u64::MAX << (size * 8 - 1);
    if acc & mask != 0 {
        acc |= mask;
    }
    acc as i64 as f64
}

fn c_str(fmt: &[u8]) -> &[u8] {
    fmt.iter().position(|&b| b == 0).map_or(fmt, |p| &fmt[..p])
}

fn struct_pack(lua: &Lua, args: &MultiValue, cap: usize) -> LuaResult<Value> {
    const FUNC: &str = "pack";
    let fmt = check_bytes(args, 0, FUNC)?;
    let fmt = c_str(&fmt);
    let mut header = StructHeader {
        little: true,
        align: 1,
    };
    let mut arg = 1usize;
    let mut total = 0usize;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        let mut size = struct_optsize(fmt, &mut i, opt)?;
        let pad = struct_toalign(total, &header, opt, size);
        total += pad;
        out.resize(out.len() + pad, 0);
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let n = check_number(args, arg, FUNC)?;
                arg += 1;
                struct_put_int(&mut out, n, header.little, size);
            }
            b'x' => out.push(0),
            b'f' => {
                let f = check_number(args, arg, FUNC)? as f32;
                arg += 1;
                out.extend_from_slice(&if header.little {
                    f.to_le_bytes()
                } else {
                    f.to_be_bytes()
                });
            }
            b'd' => {
                let d = check_number(args, arg, FUNC)?;
                arg += 1;
                out.extend_from_slice(&if header.little {
                    d.to_le_bytes()
                } else {
                    d.to_be_bytes()
                });
            }
            b'c' | b's' => {
                let s = check_bytes(args, arg, FUNC)?;
                arg += 1;
                if size == 0 {
                    size = s.len();
                }
                if s.len() < size {
                    return Err(bad_arg(arg + 1, FUNC, "string too short"));
                }
                out.extend_from_slice(&s[..size]);
                if opt == b's' {
                    out.push(0);
                    size += 1;
                }
            }
            _ => struct_control(opt, fmt, &mut i, &mut header, FUNC)?,
        }
        total = total.saturating_add(size);
        if out.len() > cap {
            return Err(oom());
        }
    }
    string_value(lua, &out)
}

fn struct_unpack(lua: &Lua, args: &MultiValue) -> LuaResult<MultiValue> {
    const FUNC: &str = "unpack";
    let fmt = check_bytes(args, 0, FUNC)?;
    let fmt = c_str(&fmt);
    let data = check_bytes(args, 1, FUNC)?;
    let start = opt_integer(args, 2, FUNC)?.unwrap_or(1);
    if start < 1 {
        return Err(bad_arg(3, FUNC, "offset must be 1 or greater"));
    }
    let mut pos = (start - 1) as usize;
    if pos > data.len() {
        return Err(bad_arg(
            3,
            FUNC,
            "offset must be less than or equal to string length",
        ));
    }
    let too_short = || bad_arg(2, FUNC, "data string too short");
    let mut header = StructHeader {
        little: true,
        align: 1,
    };
    let mut results: Vec<Value> = Vec::new();
    let mut i = 0usize;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        let mut size = struct_optsize(fmt, &mut i, opt)?;
        pos = pos.saturating_add(struct_toalign(pos, &header, opt, size));
        if pos.checked_add(size).is_none_or(|end| end > data.len()) {
            return Err(too_short());
        }
        if results.len() >= MAX_RESULTS {
            return Err(rt("too many results"));
        }
        match opt {
            b'b' | b'B' | b'h' | b'H' | b'l' | b'L' | b'T' | b'i' | b'I' => {
                let signed = opt.is_ascii_lowercase();
                let n = struct_get_int(&data[pos..pos + size], header.little, signed, size);
                results.push(Value::Number(n));
            }
            b'x' => {}
            b'f' => {
                let mut raw = [0u8; 4];
                raw.copy_from_slice(&data[pos..pos + 4]);
                let f = if header.little {
                    f32::from_le_bytes(raw)
                } else {
                    f32::from_be_bytes(raw)
                };
                results.push(Value::Number(f64::from(f)));
            }
            b'd' => {
                let mut raw = [0u8; 8];
                raw.copy_from_slice(&data[pos..pos + 8]);
                let d = if header.little {
                    f64::from_le_bytes(raw)
                } else {
                    f64::from_be_bytes(raw)
                };
                results.push(Value::Number(d));
            }
            b'c' => {
                if size == 0 {
                    let Some(n) = results.last().and_then(number_of) else {
                        return Err(rt("format `c0' needs a previous size"));
                    };
                    results.pop();
                    if n.is_nan() || n < 0.0 {
                        return Err(too_short());
                    }
                    size = n as usize;
                    if pos.checked_add(size).is_none_or(|end| end > data.len()) {
                        return Err(too_short());
                    }
                }
                results.push(string_value(lua, &data[pos..pos + size])?);
            }
            b's' => {
                let Some(end) = data[pos..].iter().position(|&b| b == 0) else {
                    return Err(rt("unfinished string in data"));
                };
                size = end + 1;
                results.push(string_value(lua, &data[pos..pos + end])?);
            }
            _ => struct_control(opt, fmt, &mut i, &mut header, FUNC)?,
        }
        pos += size;
    }
    results.push(Value::Number((pos + 1) as f64));
    Ok(results.into_iter().collect())
}

fn struct_size(args: &MultiValue) -> LuaResult<Value> {
    const FUNC: &str = "size";
    let fmt = check_bytes(args, 0, FUNC)?;
    let fmt = c_str(&fmt);
    let mut header = StructHeader {
        little: true,
        align: 1,
    };
    let mut pos = 0usize;
    let mut i = 0usize;
    while i < fmt.len() {
        let opt = fmt[i];
        i += 1;
        let size = struct_optsize(fmt, &mut i, opt)?;
        pos = pos.saturating_add(struct_toalign(pos, &header, opt, size));
        if opt == b's' {
            return Err(bad_arg(1, FUNC, "option 's' has no fixed size"));
        } else if opt == b'c' && size == 0 {
            return Err(bad_arg(1, FUNC, "option 'c0' has no fixed size"));
        }
        if !opt.is_ascii_alphanumeric() {
            struct_control(opt, fmt, &mut i, &mut header, FUNC)?;
        }
        pos = pos.saturating_add(size);
    }
    Ok(Value::Number(pos as f64))
}

fn struct_lib(lua: &Lua, reg: &Reg, memory_limit: usize) -> LuaResult<Table> {
    let lib = lua.create_table()?;
    lib.raw_set(
        "pack",
        reg.wrap_fn(move |lua, args: MultiValue| {
            struct_pack(lua, &args, output_cap(lua, memory_limit))
        })?,
    )?;
    lib.raw_set(
        "unpack",
        reg.wrap_fn(|lua, args: MultiValue| struct_unpack(lua, &args))?,
    )?;
    lib.raw_set(
        "size",
        reg.wrap_fn(|_, args: MultiValue| struct_size(&args))?,
    )?;
    Ok(lib)
}

// ---------------------------------------------------------------------
// redis.* helpers that are not commands
// ---------------------------------------------------------------------

const LOG_LEVELS: [(&str, f64); 4] = [
    ("LOG_DEBUG", 0.0),
    ("LOG_VERBOSE", 1.0),
    ("LOG_NOTICE", 2.0),
    ("LOG_WARNING", 3.0),
];
const REPL_FLAGS: [(&str, f64); 5] = [
    ("REPL_NONE", 0.0),
    ("REPL_AOF", 1.0),
    ("REPL_SLAVE", 2.0),
    ("REPL_REPLICA", 2.0),
    ("REPL_ALL", 3.0),
];

/// Adds `sha1hex`, `log`, `setresp`, the version constants and the
/// replication no-ops to the (writable) `redis` table.
pub(super) fn install_redis(lua: &Lua, redis: &Table) -> LuaResult<()> {
    let reg = Reg::new(lua)?;
    redis.raw_set(
        "sha1hex",
        reg.wrap_fn(|lua, args: MultiValue| {
            if args.len() != 1 {
                return Err(rt("wrong number of arguments"));
            }
            let data = check_bytes(&args, 0, "sha1hex")?;
            let digest = sha1_smol::Sha1::from(&*data).digest().to_string();
            string_value(lua, digest.as_bytes())
        })?,
    )?;
    // Nothing reads a log in this server: arguments are checked like Redis
    // does, then dropped.
    redis.raw_set(
        "log",
        reg.wrap_fn(|_, args: MultiValue| {
            if args.len() < 2 {
                return Err(rt("redis.log() requires two arguments or more."));
            }
            let Some(level) = args.front().and_then(|v| match v {
                Value::Integer(_) | Value::Number(_) => number_of(v),
                _ => None,
            }) else {
                return Err(rt("First argument must be a number (log level)."));
            };
            if !(0.0..=3.0).contains(&level) {
                return Err(rt("Invalid debug level."));
            }
            Ok(())
        })?,
    )?;
    for (name, level) in LOG_LEVELS {
        redis.raw_set(name, level)?;
    }
    // The answer to the script is still converted with RESP2 rules whatever
    // the version asked for.
    redis.raw_set(
        "setresp",
        reg.wrap_fn(|_, args: MultiValue| {
            if args.len() != 1 {
                return Err(rt("redis.setresp() requires one argument."));
            }
            match args.front().and_then(number_of) {
                Some(v) if v == 2.0 || v == 3.0 => Ok(()),
                _ => Err(rt("RESP version must be 2 or 3.")),
            }
        })?,
    )?;
    redis.raw_set("REDIS_VERSION", REDIS_VERSION)?;
    redis.raw_set("REDIS_VERSION_NUM", REDIS_VERSION_NUM)?;
    redis.raw_set("breakpoint", lua.create_function(|_, ()| Ok(false))?)?;
    redis.raw_set("debug", lua.create_function(|_, _: MultiValue| Ok(()))?)?;
    redis.raw_set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    redis.raw_set(
        "set_repl",
        reg.wrap_fn(|_, args: MultiValue| {
            if args.len() != 1 {
                return Err(rt("redis.set_repl() requires one argument."));
            }
            match args.front().and_then(number_of) {
                Some(flags) if (0.0..=3.0).contains(&flags) && flags.fract() == 0.0 => Ok(()),
                _ => Err(rt(
                    "Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE.",
                )),
            }
        })?,
    )?;
    for (name, flags) in REPL_FLAGS {
        redis.raw_set(name, flags)?;
    }
    Ok(())
}
