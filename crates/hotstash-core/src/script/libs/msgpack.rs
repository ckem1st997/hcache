//! `cmsgpack`: lua-cmsgpack as bundled with Redis.

use super::{
    check_bytes, check_number, oom, opt_integer, output_cap, rt, string_value, LuaResult, Reg,
    MAX_RESULTS, MSGPACK_MAX_NESTING,
};
use mlua::{Lua, MultiValue, Table, Value};

/// Open containers the decoder keeps live at once (it holds two Lua values
/// for each, and mlua has a fixed pool of them).
const MAX_DECODE_DEPTH: usize = 1000;
/// Doubles at or above this are not 64-bit integers.
const INT64_LIMIT: f64 = 9_223_372_036_854_775_808.0;

pub(super) fn build(lua: &Lua, reg: &Reg, memory_limit: usize) -> LuaResult<Table> {
    let lib = lua.create_table()?;
    lib.raw_set(
        "pack",
        reg.wrap_fn(move |lua, args: MultiValue| {
            if args.is_empty() {
                return Err(rt("MessagePack pack needs input."));
            }
            let mut packer = Packer {
                out: Vec::new(),
                cap: output_cap(lua, memory_limit),
            };
            for arg in &args {
                packer.value(arg, 0)?;
            }
            string_value(lua, &packer.out)
        })?,
    )?;
    lib.raw_set(
        "unpack",
        reg.wrap_fn(|lua, args: MultiValue| {
            let data = check_bytes(&args, 0, "unpack")?;
            unpack(lua, &data, 0, 0)
        })?,
    )?;
    lib.raw_set(
        "unpack_one",
        reg.wrap_fn(|lua, args: MultiValue| {
            let data = check_bytes(&args, 0, "unpack_one")?;
            let offset = opt_integer(&args, 1, "unpack_one")?.unwrap_or(0);
            unpack(lua, &data, 1, offset)
        })?,
    )?;
    lib.raw_set(
        "unpack_limit",
        reg.wrap_fn(|lua, args: MultiValue| {
            let data = check_bytes(&args, 0, "unpack_limit")?;
            let limit = check_number(&args, 1, "unpack_limit")? as i64;
            let offset = opt_integer(&args, 2, "unpack_limit")?.unwrap_or(0);
            unpack(lua, &data, limit, offset)
        })?,
    )?;
    Ok(lib)
}

// ---------------------------------------------------------------------
// pack
// ---------------------------------------------------------------------

struct Packer {
    out: Vec<u8>,
    cap: usize,
}

impl Packer {
    fn check(&self) -> LuaResult<()> {
        if self.out.len() > self.cap {
            return Err(oom());
        }
        Ok(())
    }

    /// `level` counts the tables around `v`; below the nesting bound a table
    /// becomes nil, which also stops cycles.
    fn value(&mut self, v: &Value, level: usize) -> LuaResult<()> {
        match v {
            Value::Boolean(true) => self.out.push(0xc3),
            Value::Boolean(false) => self.out.push(0xc2),
            Value::Integer(i) => self.number(*i as f64),
            Value::Number(n) => self.number(*n),
            Value::String(s) => self.bytes(&s.as_bytes()),
            Value::Table(t) if level < MSGPACK_MAX_NESTING => self.table(t, level)?,
            // nil, functions, userdata, and tables nested too deep.
            _ => self.out.push(0xc0),
        }
        self.check()
    }

    /// A double that holds an integer goes out as the smallest integer, any
    /// other as a float32 when that loses nothing, else as a float64.
    fn number(&mut self, n: f64) {
        if n.fract() == 0.0 && (-INT64_LIMIT..INT64_LIMIT).contains(&n) {
            self.int(n as i64);
            return;
        }
        let narrow = n as f32;
        if n == f64::from(narrow) {
            self.out.push(0xca);
            self.out.extend_from_slice(&narrow.to_be_bytes());
        } else {
            self.out.push(0xcb);
            self.out.extend_from_slice(&n.to_be_bytes());
        }
    }

    fn int(&mut self, n: i64) {
        if n >= 0 {
            match n {
                0..=0x7f => self.out.push(n as u8),
                0x80..=0xff => self.out.extend_from_slice(&[0xcc, n as u8]),
                0x100..=0xffff => {
                    self.out.push(0xcd);
                    self.out.extend_from_slice(&(n as u16).to_be_bytes());
                }
                0x1_0000..=0xffff_ffff => {
                    self.out.push(0xce);
                    self.out.extend_from_slice(&(n as u32).to_be_bytes());
                }
                _ => {
                    self.out.push(0xcf);
                    self.out.extend_from_slice(&(n as u64).to_be_bytes());
                }
            }
        } else if n >= -32 {
            self.out.push(n as i8 as u8);
        } else if n >= i64::from(i8::MIN) {
            self.out.extend_from_slice(&[0xd0, n as i8 as u8]);
        } else if n >= i64::from(i16::MIN) {
            self.out.push(0xd1);
            self.out.extend_from_slice(&(n as i16).to_be_bytes());
        } else if n >= i64::from(i32::MIN) {
            self.out.push(0xd2);
            self.out.extend_from_slice(&(n as i32).to_be_bytes());
        } else {
            self.out.push(0xd3);
            self.out.extend_from_slice(&n.to_be_bytes());
        }
    }

    fn bytes(&mut self, s: &[u8]) {
        let len = s.len();
        match len {
            0..=31 => self.out.push(0xa0 | len as u8),
            32..=0xff => self.out.extend_from_slice(&[0xd9, len as u8]),
            0x100..=0xffff => {
                self.out.push(0xda);
                self.out.extend_from_slice(&(len as u16).to_be_bytes());
            }
            _ => {
                self.out.push(0xdb);
                self.out.extend_from_slice(&(len as u32).to_be_bytes());
            }
        }
        self.out.extend_from_slice(s);
    }

    fn array_header(&mut self, len: usize) {
        match len {
            0..=15 => self.out.push(0x90 | len as u8),
            16..=0xffff => {
                self.out.push(0xdc);
                self.out.extend_from_slice(&(len as u16).to_be_bytes());
            }
            _ => {
                self.out.push(0xdd);
                self.out.extend_from_slice(&(len as u32).to_be_bytes());
            }
        }
    }

    fn map_header(&mut self, len: usize) {
        match len {
            0..=15 => self.out.push(0x80 | len as u8),
            16..=0xffff => {
                self.out.push(0xde);
                self.out.extend_from_slice(&(len as u16).to_be_bytes());
            }
            _ => {
                self.out.push(0xdf);
                self.out.extend_from_slice(&(len as u32).to_be_bytes());
            }
        }
    }

    /// A table is an array when its keys are exactly 1..=n (an empty table
    /// too), otherwise a map.
    fn table(&mut self, t: &Table, level: usize) -> LuaResult<()> {
        let mut count = 0i64;
        let mut max = 0i64;
        let mut is_array = true;
        for pair in t.pairs::<Value, Value>() {
            let (key, _) = pair?;
            let k = match key {
                Value::Integer(i) => i as f64,
                Value::Number(n) => n,
                _ => {
                    is_array = false;
                    break;
                }
            };
            if k <= 0.0 || k != f64::from(k as i32) {
                is_array = false;
                break;
            }
            max = max.max(k as i64);
            count += 1;
        }
        if is_array && max == count {
            self.array_header(count as usize);
            for i in 1..=count {
                let item: Value = t.raw_get(i)?;
                self.value(&item, level + 1)?;
            }
            return Ok(());
        }
        let mut entries = 0usize;
        for pair in t.pairs::<Value, Value>() {
            pair?;
            entries += 1;
        }
        self.map_header(entries);
        for pair in t.pairs::<Value, Value>() {
            let (key, item) = pair?;
            self.value(&key, level + 1)?;
            self.value(&item, level + 1)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------
// unpack
// ---------------------------------------------------------------------

enum Fail {
    Eof,
    Bad,
    Lua(mlua::Error),
}

impl From<mlua::Error> for Fail {
    fn from(e: mlua::Error) -> Fail {
        Fail::Lua(e)
    }
}

impl From<Fail> for mlua::Error {
    fn from(f: Fail) -> mlua::Error {
        match f {
            Fail::Eof => rt("Missing bytes in input."),
            Fail::Bad => rt("Bad data format in input."),
            Fail::Lua(e) => e,
        }
    }
}

enum Item {
    Val(Value),
    Array(usize),
    Map(usize),
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn left(&self) -> usize {
        self.data.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Fail> {
        if n > self.left() {
            return Err(Fail::Eof);
        }
        let bytes = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }

    fn be(&mut self, n: usize) -> Result<u64, Fail> {
        Ok(self
            .take(n)?
            .iter()
            .fold(0u64, |acc, &b| (acc << 8) | u64::from(b)))
    }

    fn string(&mut self, lua: &Lua, len: usize) -> Result<Item, Fail> {
        let bytes = self.take(len)?;
        Ok(Item::Val(Value::String(lua.create_string(bytes)?)))
    }

    /// One header and, for scalars, the whole value.
    fn item(&mut self, lua: &Lua) -> Result<Item, Fail> {
        let b = self.take(1)?[0];
        let num = |n: f64| Ok(Item::Val(Value::Number(n)));
        match b {
            0x00..=0x7f => num(f64::from(b)),
            0xe0..=0xff => num(f64::from(b as i8)),
            0xa0..=0xbf => self.string(lua, usize::from(b & 0x1f)),
            0x90..=0x9f => Ok(Item::Array(usize::from(b & 0x0f))),
            0x80..=0x8f => Ok(Item::Map(usize::from(b & 0x0f))),
            0xc0 => Ok(Item::Val(Value::Nil)),
            0xc2 => Ok(Item::Val(Value::Boolean(false))),
            0xc3 => Ok(Item::Val(Value::Boolean(true))),
            // str8 and bin8, 16, 32 come out as plain strings.
            0xc4 | 0xd9 => {
                let len = self.be(1)? as usize;
                self.string(lua, len)
            }
            0xc5 | 0xda => {
                let len = self.be(2)? as usize;
                self.string(lua, len)
            }
            0xc6 | 0xdb => {
                let len = self.be(4)? as usize;
                self.string(lua, len)
            }
            0xca => {
                let bits = self.be(4)? as u32;
                num(f64::from(f32::from_bits(bits)))
            }
            0xcb => {
                let bits = self.be(8)?;
                num(f64::from_bits(bits))
            }
            0xcc => num(self.be(1)? as f64),
            0xcd => num(self.be(2)? as f64),
            0xce => num(self.be(4)? as f64),
            0xcf => num(self.be(8)? as f64),
            0xd0 => num(f64::from(self.be(1)? as u8 as i8)),
            0xd1 => num(f64::from(self.be(2)? as u16 as i16)),
            0xd2 => num(f64::from(self.be(4)? as u32 as i32)),
            0xd3 => num(self.be(8)? as i64 as f64),
            0xdc => Ok(Item::Array(self.be(2)? as usize)),
            0xdd => Ok(Item::Array(self.be(4)? as usize)),
            0xde => Ok(Item::Map(self.be(2)? as usize)),
            0xdf => Ok(Item::Map(self.be(4)? as usize)),
            // Extensions and the reserved byte.
            _ => Err(Fail::Bad),
        }
    }
}

enum Frame {
    Array {
        table: Table,
        left: usize,
        next: i64,
    },
    Map {
        table: Table,
        left: usize,
        key: Option<Value>,
    },
}

/// Decodes one object with an explicit stack, so nesting is bounded by the
/// input and not by the Rust stack. Tables grow as items arrive: a header
/// that promises more than the input holds costs nothing.
fn decode_one(lua: &Lua, cur: &mut Cursor) -> Result<Value, Fail> {
    let mut stack: Vec<Frame> = Vec::new();
    loop {
        let mut done = match cur.item(lua)? {
            Item::Val(v) => v,
            Item::Array(0) | Item::Map(0) => Value::Table(lua.create_table()?),
            Item::Array(_) | Item::Map(_) if stack.len() >= MAX_DECODE_DEPTH => {
                return Err(Fail::Lua(rt("Too many nested data structures in input.")))
            }
            Item::Array(left) => {
                let table = lua.create_table()?;
                stack.push(Frame::Array {
                    table,
                    left,
                    next: 1,
                });
                continue;
            }
            Item::Map(left) => {
                let table = lua.create_table()?;
                stack.push(Frame::Map {
                    table,
                    left,
                    key: None,
                });
                continue;
            }
        };
        loop {
            let Some(frame) = stack.last_mut() else {
                return Ok(done);
            };
            match frame {
                Frame::Array { table, left, next } => {
                    table.raw_set(*next, done)?;
                    *next += 1;
                    *left -= 1;
                    if *left > 0 {
                        break;
                    }
                    done = Value::Table(table.clone());
                    stack.pop();
                }
                Frame::Map { table, left, key } => match key.take() {
                    None => {
                        match &done {
                            Value::Nil => return Err(rt("table index is nil").into()),
                            Value::Number(n) if n.is_nan() => {
                                return Err(rt("table index is NaN").into())
                            }
                            _ => {}
                        }
                        *key = Some(done);
                        break;
                    }
                    Some(k) => {
                        table.raw_set(k, done)?;
                        *left -= 1;
                        if *left > 0 {
                            break;
                        }
                        done = Value::Table(table.clone());
                        stack.pop();
                    }
                },
            }
        }
    }
}

/// `unpack` (`limit` and `offset` 0: everything), `unpack_one` and
/// `unpack_limit`. With a limit or an offset the first result is the offset
/// of the next unread byte, or -1 when the input is used up.
fn unpack(lua: &Lua, data: &[u8], limit: i64, offset: i64) -> LuaResult<MultiValue> {
    let decode_all = limit == 0 && offset == 0;
    if offset < 0 || limit < 0 {
        return Err(rt(format!(
            "Invalid request to unpack with offset of {offset} and limit of {limit}."
        )));
    }
    if offset as u64 > data.len() as u64 {
        return Err(rt(format!(
            "Start offset {offset} greater than input length {}.",
            data.len()
        )));
    }
    let limit = if decode_all { i64::MAX } else { limit };
    let mut cur = Cursor {
        data,
        pos: offset as usize,
    };
    let mut results: Vec<Value> = Vec::new();
    let mut count = 0i64;
    while cur.left() > 0 && count != limit {
        if results.len() >= MAX_RESULTS {
            return Err(rt("too many results to unpack"));
        }
        results.push(decode_one(lua, &mut cur)?);
        count += 1;
    }
    if !decode_all {
        let next = if cur.left() == 0 {
            -1.0
        } else {
            cur.pos as f64
        };
        results.insert(0, Value::Number(next));
    }
    Ok(results.into_iter().collect())
}
