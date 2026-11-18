//! `cjson`: lua-cjson 2.1.0 as bundled with Redis. Error texts match the C
//! library, including its `T_*` token names.

use super::{
    bad_arg, check_bytes, format_g14, lua_type_name, number_of, oom, output_cap, rt, string_value,
    JsonCfg, LuaResult, Reg, CJSON_DECODE_DEPTH_CEILING, CJSON_ENCODE_DEPTH_CEILING, INT_MAX,
    INVALID_NUMBERS_ALLOW, INVALID_NUMBERS_ERROR, INVALID_NUMBERS_NULL,
};
use mlua::{LightUserData, Lua, MultiValue, Table, Value};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

/// Bytes of input checked against the output cap at a time while escaping.
const ESCAPE_CHUNK: usize = 1 << 16;

pub(super) fn build(
    lua: &Lua,
    reg: &Reg,
    memory_limit: usize,
    cfg: &Arc<JsonCfg>,
) -> LuaResult<Table> {
    let lib = lua.create_table()?;
    lib.raw_set(
        "null",
        Value::LightUserData(LightUserData(std::ptr::null_mut())),
    )?;
    lib.raw_set("_NAME", "cjson")?;
    lib.raw_set("_VERSION", "2.1.0")?;

    let encode_cfg = cfg.clone();
    lib.raw_set(
        "encode",
        reg.wrap_fn(move |lua, args: MultiValue| {
            if args.len() != 1 {
                return Err(bad_arg(1, "encode", "expected 1 argument"));
            }
            let mut encoder = Encoder::new(&encode_cfg, output_cap(lua, memory_limit));
            encoder.value(&args[0], 0)?;
            string_value(lua, &encoder.out)
        })?,
    )?;

    let decode_cfg = cfg.clone();
    lib.raw_set(
        "decode",
        reg.wrap_fn(move |lua, args: MultiValue| {
            if args.len() != 1 {
                return Err(bad_arg(1, "decode", "expected 1 argument"));
            }
            let data = check_bytes(&args, 0, "decode")?;
            decode(
                lua,
                &data,
                decode_cfg
                    .decode_depth
                    .load(Relaxed)
                    .min(CJSON_DECODE_DEPTH_CEILING),
            )
        })?,
    )?;

    let c = cfg.clone();
    lib.raw_set(
        "encode_max_depth",
        reg.wrap_fn(move |_, args: MultiValue| {
            if let Some(n) = int_option(&args, 0, "encode_max_depth", 1, INT_MAX)? {
                c.encode_depth.store(n, Relaxed);
            }
            Ok(c.encode_depth.load(Relaxed) as f64)
        })?,
    )?;
    let c = cfg.clone();
    lib.raw_set(
        "decode_max_depth",
        reg.wrap_fn(move |_, args: MultiValue| {
            if let Some(n) = int_option(&args, 0, "decode_max_depth", 1, INT_MAX)? {
                c.decode_depth.store(n, Relaxed);
            }
            Ok(c.decode_depth.load(Relaxed) as f64)
        })?,
    )?;
    let c = cfg.clone();
    lib.raw_set(
        "encode_sparse_array",
        reg.wrap_fn(move |_, args: MultiValue| {
            match args.front() {
                None | Some(Value::Nil) => {}
                Some(Value::Boolean(b)) => c.sparse_convert.store(*b, Relaxed),
                Some(_) => return Err(bad_arg(1, "encode_sparse_array", "invalid option")),
            }
            if let Some(n) = int_option(&args, 1, "encode_sparse_array", 0, INT_MAX)? {
                c.sparse_ratio.store(n, Relaxed);
            }
            if let Some(n) = int_option(&args, 2, "encode_sparse_array", 0, INT_MAX)? {
                c.sparse_safe.store(n, Relaxed);
            }
            Ok(MultiValue::from_iter([
                Value::Boolean(c.sparse_convert.load(Relaxed)),
                Value::Number(c.sparse_ratio.load(Relaxed) as f64),
                Value::Number(c.sparse_safe.load(Relaxed) as f64),
            ]))
        })?,
    )?;
    let c = cfg.clone();
    lib.raw_set(
        "encode_invalid_numbers",
        reg.wrap_fn(move |lua, args: MultiValue| {
            match args.front() {
                None | Some(Value::Nil) => {}
                Some(Value::Boolean(true)) => {
                    c.invalid_numbers.store(INVALID_NUMBERS_ALLOW, Relaxed)
                }
                Some(Value::Boolean(false)) => {
                    c.invalid_numbers.store(INVALID_NUMBERS_ERROR, Relaxed)
                }
                Some(Value::String(s)) => match &*s.as_bytes() {
                    b"on" => c.invalid_numbers.store(INVALID_NUMBERS_ALLOW, Relaxed),
                    b"off" => c.invalid_numbers.store(INVALID_NUMBERS_ERROR, Relaxed),
                    b"null" => c.invalid_numbers.store(INVALID_NUMBERS_NULL, Relaxed),
                    _ => return Err(bad_arg(1, "encode_invalid_numbers", "invalid option")),
                },
                Some(_) => return Err(bad_arg(1, "encode_invalid_numbers", "invalid option")),
            }
            match c.invalid_numbers.load(Relaxed) {
                INVALID_NUMBERS_ALLOW => Ok(Value::Boolean(true)),
                INVALID_NUMBERS_NULL => string_value(lua, b"null"),
                _ => Ok(Value::Boolean(false)),
            }
        })?,
    )?;
    // Accepted for compatibility: the output buffer is not reused, numbers
    // always print with 14 digits, and decoding is always strict.
    lib.raw_set(
        "encode_keep_buffer",
        reg.wrap_fn(|_, args: MultiValue| {
            Ok(!matches!(args.front(), Some(Value::Boolean(false))))
        })?,
    )?;
    lib.raw_set(
        "decode_invalid_numbers",
        reg.wrap_fn(|_, args: MultiValue| {
            Ok(!matches!(args.front(), Some(Value::Boolean(false))))
        })?,
    )?;
    lib.raw_set(
        "encode_number_precision",
        reg.wrap_fn(|_, args: MultiValue| {
            let n = int_option(&args, 0, "encode_number_precision", 1, 14)?;
            Ok(n.unwrap_or(14) as f64)
        })?,
    )?;
    Ok(lib)
}

/// An optional integer argument within `min..=max`.
fn int_option(
    args: &MultiValue,
    i: usize,
    func: &str,
    min: usize,
    max: usize,
) -> LuaResult<Option<usize>> {
    let Some(v) = args.get(i).filter(|v| !matches!(v, Value::Nil)) else {
        return Ok(None);
    };
    let range_error = || {
        bad_arg(
            i + 1,
            func,
            &format!("expected integer between {min} and {max}"),
        )
    };
    let n = number_of(v).ok_or_else(range_error)?;
    if n.fract() != 0.0 || n < min as f64 || n > max as f64 {
        return Err(range_error());
    }
    Ok(Some(n as usize))
}

// ---------------------------------------------------------------------
// encode
// ---------------------------------------------------------------------

struct Encoder {
    out: Vec<u8>,
    cap: usize,
    max_depth: usize,
    sparse_convert: bool,
    sparse_ratio: usize,
    sparse_safe: usize,
    invalid_numbers: u8,
}

impl Encoder {
    fn new(cfg: &JsonCfg, cap: usize) -> Encoder {
        Encoder {
            out: Vec::new(),
            cap,
            max_depth: cfg
                .encode_depth
                .load(Relaxed)
                .min(CJSON_ENCODE_DEPTH_CEILING),
            sparse_convert: cfg.sparse_convert.load(Relaxed),
            sparse_ratio: cfg.sparse_ratio.load(Relaxed),
            sparse_safe: cfg.sparse_safe.load(Relaxed),
            invalid_numbers: cfg.invalid_numbers.load(Relaxed),
        }
    }

    fn check(&self) -> LuaResult<()> {
        if self.out.len() > self.cap {
            return Err(oom());
        }
        Ok(())
    }

    /// `depth` is the number of tables around `v`.
    fn value(&mut self, v: &Value, depth: usize) -> LuaResult<()> {
        match v {
            Value::Nil => self.out.extend_from_slice(b"null"),
            Value::Boolean(true) => self.out.extend_from_slice(b"true"),
            Value::Boolean(false) => self.out.extend_from_slice(b"false"),
            Value::Integer(i) => self.number(*i as f64)?,
            Value::Number(n) => self.number(*n)?,
            Value::String(s) => self.string(&s.as_bytes())?,
            Value::Table(t) => self.table(t, depth + 1)?,
            Value::LightUserData(p) if p.0.is_null() => self.out.extend_from_slice(b"null"),
            other => {
                return Err(rt(format!(
                    "Cannot serialise {}: type not supported",
                    lua_type_name(Some(other))
                )))
            }
        }
        self.check()
    }

    fn number(&mut self, n: f64) -> LuaResult<()> {
        if !n.is_finite() {
            match self.invalid_numbers {
                INVALID_NUMBERS_ERROR => {
                    return Err(rt("Cannot serialise number: must not be NaN or Inf"))
                }
                INVALID_NUMBERS_NULL => {
                    self.out.extend_from_slice(b"null");
                    return Ok(());
                }
                _ => {}
            }
        }
        self.out.extend_from_slice(format_g14(n).as_bytes());
        Ok(())
    }

    fn string(&mut self, s: &[u8]) -> LuaResult<()> {
        self.out.push(b'"');
        for chunk in s.chunks(ESCAPE_CHUNK) {
            for &b in chunk {
                match b {
                    b'"' => self.out.extend_from_slice(b"\\\""),
                    b'\\' => self.out.extend_from_slice(b"\\\\"),
                    b'/' => self.out.extend_from_slice(b"\\/"),
                    0x08 => self.out.extend_from_slice(b"\\b"),
                    0x09 => self.out.extend_from_slice(b"\\t"),
                    0x0a => self.out.extend_from_slice(b"\\n"),
                    0x0c => self.out.extend_from_slice(b"\\f"),
                    0x0d => self.out.extend_from_slice(b"\\r"),
                    0x00..=0x1f | 0x7f => {
                        self.out.extend_from_slice(format!("\\u{b:04x}").as_bytes())
                    }
                    _ => self.out.push(b),
                }
            }
            self.check()?;
        }
        self.out.push(b'"');
        Ok(())
    }

    /// Length of the array the table encodes as, or `None` for an object.
    /// Like lua-cjson: every key a positive integer; a table too sparse for
    /// its item count is an error unless conversion is on.
    fn array_len(&self, t: &Table) -> LuaResult<Option<usize>> {
        let mut max = 0f64;
        let mut items = 0usize;
        for pair in t.pairs::<Value, Value>() {
            let (key, _) = pair?;
            let k = match key {
                Value::Integer(i) => i as f64,
                Value::Number(n) => n,
                _ => return Ok(None),
            };
            if k.floor() != k || k < 1.0 {
                return Ok(None);
            }
            max = max.max(k);
            items += 1;
        }
        if self.sparse_ratio > 0
            && max > (items as f64) * (self.sparse_ratio as f64)
            && max > self.sparse_safe as f64
        {
            if !self.sparse_convert {
                return Err(rt("Cannot serialise table: excessively sparse array"));
            }
            return Ok(None);
        }
        Ok(Some(max as usize))
    }

    fn table(&mut self, t: &Table, depth: usize) -> LuaResult<()> {
        if depth > self.max_depth {
            return Err(rt(format!("Cannot serialise, excessive nesting ({depth})")));
        }
        match self.array_len(t)? {
            Some(len) if len > 0 => {
                self.out.push(b'[');
                for i in 1..=len {
                    if i > 1 {
                        self.out.push(b',');
                    }
                    let item: Value = t.raw_get(i as i64)?;
                    self.value(&item, depth)?;
                }
                self.out.push(b']');
            }
            _ => {
                self.out.push(b'{');
                let mut first = true;
                for pair in t.pairs::<Value, Value>() {
                    let (key, item) = pair?;
                    if !first {
                        self.out.push(b',');
                    }
                    first = false;
                    match &key {
                        Value::Integer(_) | Value::Number(_) => {
                            self.out.push(b'"');
                            self.value(&key, depth)?;
                            self.out.extend_from_slice(b"\":");
                        }
                        Value::String(s) => {
                            self.string(&s.as_bytes())?;
                            self.out.push(b':');
                        }
                        other => {
                            return Err(rt(format!(
                                "Cannot serialise {}: table key must be a number or string",
                                lua_type_name(Some(other))
                            )))
                        }
                    }
                    self.value(&item, depth)?;
                }
                self.out.push(b'}');
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------
// decode
// ---------------------------------------------------------------------

enum Kind {
    ObjBegin,
    ObjEnd,
    ArrBegin,
    ArrEnd,
    Str(Vec<u8>),
    Num(f64),
    Bool(bool),
    Null,
    Colon,
    Comma,
    End,
    Error(&'static str),
}

impl Kind {
    /// The names lua-cjson prints in its messages.
    fn name(&self) -> &'static str {
        match self {
            Kind::ObjBegin => "T_OBJ_BEGIN",
            Kind::ObjEnd => "T_OBJ_END",
            Kind::ArrBegin => "T_ARR_BEGIN",
            Kind::ArrEnd => "T_ARR_END",
            Kind::Str(_) => "T_STRING",
            Kind::Num(_) => "T_NUMBER",
            Kind::Bool(_) => "T_BOOLEAN",
            Kind::Null => "T_NULL",
            Kind::Colon => "T_COLON",
            Kind::Comma => "T_COMMA",
            Kind::End => "T_END",
            Kind::Error(text) => text,
        }
    }
}

struct Token {
    kind: Kind,
    /// Zero-based offset of the token in the input.
    index: usize,
}

fn expected(what: &str, token: &Token) -> mlua::Error {
    rt(format!(
        "Expected {what} but found {} at character {}",
        token.kind.name(),
        token.index + 1
    ))
}

struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn next(&mut self) -> Token {
        while matches!(self.data.get(self.pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
        let index = self.pos;
        // A NUL ends the input, as in the C parser.
        let ch = self.data.get(self.pos).copied().unwrap_or(0);
        let simple = |parser: &mut Parser, kind: Kind| {
            parser.pos += 1;
            kind
        };
        let kind = match ch {
            0 => Kind::End,
            b'{' => simple(self, Kind::ObjBegin),
            b'}' => simple(self, Kind::ObjEnd),
            b'[' => simple(self, Kind::ArrBegin),
            b']' => simple(self, Kind::ArrEnd),
            b':' => simple(self, Kind::Colon),
            b',' => simple(self, Kind::Comma),
            b'"' => match self.string() {
                Ok(s) => Kind::Str(s),
                Err((message, at)) => {
                    return Token {
                        kind: Kind::Error(message),
                        index: at,
                    }
                }
            },
            b'-' | b'0'..=b'9' => match self.number() {
                Some(n) => Kind::Num(n),
                None => Kind::Error("invalid number"),
            },
            b't' if self.data[index..].starts_with(b"true") => {
                self.pos += 4;
                Kind::Bool(true)
            }
            b'f' if self.data[index..].starts_with(b"false") => {
                self.pos += 5;
                Kind::Bool(false)
            }
            b'n' if self.data[index..].starts_with(b"null") => {
                self.pos += 4;
                Kind::Null
            }
            _ => Kind::Error("invalid token"),
        };
        Token { kind, index }
    }

    /// A number like strtod would read it, minus hex, leading zeros, inf and
    /// nan, which are rejected.
    fn number(&mut self) -> Option<f64> {
        let d = self.data;
        let start = self.pos;
        let mut p = start;
        if d.get(p) == Some(&b'-') {
            p += 1;
        }
        match d.get(p) {
            Some(b'0') if matches!(d.get(p + 1), Some(b'0'..=b'9' | b'x' | b'X')) => return None,
            Some(b'0'..=b'9') => {}
            _ => return None,
        }
        let digits = |mut at: usize| {
            while d.get(at).is_some_and(u8::is_ascii_digit) {
                at += 1;
            }
            at
        };
        p = digits(p);
        if d.get(p) == Some(&b'.') {
            p = digits(p + 1);
        }
        if matches!(d.get(p), Some(b'e' | b'E')) {
            let mut q = p + 1;
            if matches!(d.get(q), Some(b'+' | b'-')) {
                q += 1;
            }
            if d.get(q).is_some_and(u8::is_ascii_digit) {
                p = digits(q);
            }
        }
        let n = std::str::from_utf8(&d[start..p])
            .ok()?
            .parse::<f64>()
            .ok()?;
        self.pos = p;
        Some(n)
    }

    /// The string at `pos` (a quote); on failure the message and the offset
    /// the C parser would report.
    fn string(&mut self) -> Result<Vec<u8>, (&'static str, usize)> {
        let d = self.data;
        self.pos += 1;
        let mut out = Vec::new();
        loop {
            let ch = d.get(self.pos).copied().unwrap_or(0);
            match ch {
                b'"' => {
                    self.pos += 1;
                    return Ok(out);
                }
                0 => return Err(("unexpected end of string", self.pos)),
                b'\\' => {
                    let escaped = match d.get(self.pos + 1).copied().unwrap_or(0) {
                        b'"' => b'"',
                        b'\\' => b'\\',
                        b'/' => b'/',
                        b'b' => 0x08,
                        b'f' => 0x0c,
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'u' => {
                            self.unicode_escape(&mut out)?;
                            continue;
                        }
                        _ => return Err(("invalid escape code", self.pos)),
                    };
                    out.push(escaped);
                    self.pos += 2;
                }
                _ => {
                    let run = d[self.pos..]
                        .iter()
                        .position(|&b| b == b'"' || b == b'\\' || b == 0)
                        .unwrap_or(d.len() - self.pos);
                    out.extend_from_slice(&d[self.pos..self.pos + run]);
                    self.pos += run;
                }
            }
        }
    }

    /// `\uXXXX`, or a surrogate pair of two, at `pos`; appended as UTF-8.
    fn unicode_escape(&mut self, out: &mut Vec<u8>) -> Result<(), (&'static str, usize)> {
        const BAD: &str = "invalid unicode escape code";
        let d = self.data;
        let at = self.pos;
        let hex4 = |from: usize| -> Option<u32> {
            d.get(from..from + 4)?.iter().try_fold(0u32, |acc, &b| {
                char::from(b).to_digit(16).map(|v| (acc << 4) | v)
            })
        };
        let mut code = hex4(at + 2).ok_or((BAD, at))?;
        let mut len = 6;
        if code & 0xF800 == 0xD800 {
            // A low surrogate may not come first, and a high one needs a
            // low one right after it.
            if code & 0x400 != 0 || d.get(at + 6) != Some(&b'\\') || d.get(at + 7) != Some(&b'u') {
                return Err((BAD, at));
            }
            let low = hex4(at + 8).ok_or((BAD, at))?;
            if low & 0xFC00 != 0xDC00 {
                return Err((BAD, at));
            }
            code = (((code & 0x3FF) << 10) | (low & 0x3FF)) + 0x10000;
            len = 12;
        }
        let ch = char::from_u32(code).ok_or((BAD, at))?;
        let mut buf = [0u8; 4];
        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
        self.pos += len;
        Ok(())
    }

    /// Reads the colon after an object key.
    fn key(&mut self, key: Token) -> LuaResult<Vec<u8>> {
        let Kind::Str(text) = key.kind else {
            return Err(expected("object key string", &key));
        };
        let colon = self.next();
        if !matches!(colon.kind, Kind::Colon) {
            return Err(expected("colon", &colon));
        }
        Ok(text)
    }
}

enum Frame {
    Array { table: Table, next: i64 },
    Object { table: Table, key: mlua::String },
}

/// Parses with an explicit stack: nesting depth is limited by the configured
/// maximum, not by the Rust stack.
fn decode(lua: &Lua, data: &[u8], max_depth: usize) -> LuaResult<Value> {
    // Two NULs in the first bytes would be UTF-16 or UTF-32.
    if data.len() >= 2 && (data[0] == 0 || data[1] == 0) {
        return Err(rt("JSON parser does not support UTF-16 or UTF-32"));
    }
    let mut parser = Parser { data, pos: 0 };
    let mut stack: Vec<Frame> = Vec::new();
    let mut token = parser.next();
    'values: loop {
        let mut done = match token.kind {
            Kind::Str(s) => Value::String(lua.create_string(&s)?),
            Kind::Num(n) => Value::Number(n),
            Kind::Bool(b) => Value::Boolean(b),
            Kind::Null => Value::LightUserData(LightUserData(std::ptr::null_mut())),
            Kind::ObjBegin | Kind::ArrBegin => {
                let depth = stack.len() + 1;
                if depth > max_depth {
                    return Err(rt(format!(
                        "Found too many nested data structures ({depth}) at character {}",
                        parser.pos
                    )));
                }
                let table = lua.create_table()?;
                let first = parser.next();
                if matches!(token.kind, Kind::ObjBegin) {
                    if matches!(first.kind, Kind::ObjEnd) {
                        Value::Table(table)
                    } else {
                        let key = parser.key(first)?;
                        let key = lua.create_string(&key)?;
                        stack.push(Frame::Object { table, key });
                        token = parser.next();
                        continue 'values;
                    }
                } else if matches!(first.kind, Kind::ArrEnd) {
                    Value::Table(table)
                } else {
                    stack.push(Frame::Array { table, next: 1 });
                    token = first;
                    continue 'values;
                }
            }
            _ => return Err(expected("value", &token)),
        };
        // A value is complete: attach it to its container, closing every
        // container that it was the last item of.
        loop {
            let Some(frame) = stack.last_mut() else {
                let end = parser.next();
                if !matches!(end.kind, Kind::End) {
                    return Err(expected("the end", &end));
                }
                return Ok(done);
            };
            match frame {
                Frame::Array { table, next } => {
                    table.raw_set(*next, done)?;
                    let after = parser.next();
                    match after.kind {
                        Kind::ArrEnd => {
                            done = Value::Table(table.clone());
                            stack.pop();
                        }
                        Kind::Comma => {
                            *next += 1;
                            token = parser.next();
                            continue 'values;
                        }
                        _ => return Err(expected("comma or array end", &after)),
                    }
                }
                Frame::Object { table, key } => {
                    table.raw_set(key.clone(), done)?;
                    let after = parser.next();
                    match after.kind {
                        Kind::ObjEnd => {
                            done = Value::Table(table.clone());
                            stack.pop();
                        }
                        Kind::Comma => {
                            let next_key = parser.next();
                            let text = parser.key(next_key)?;
                            *key = lua.create_string(&text)?;
                            token = parser.next();
                            continue 'values;
                        }
                        _ => return Err(expected("comma or object end", &after)),
                    }
                }
            }
        }
    }
}
