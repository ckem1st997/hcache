//! JSON documents (JSON.*), as valkey-json 1.0 keeps them: the same
//! commands, paths, replies and errors. `docs/json-path.md` lists the path
//! syntax it takes and where it differs.
//!
//! A command selects values by a path (`path.rs`), then reads them or
//! changes them. A write first looks at everything it needs, then changes
//! the document by the paths it noted, so a refused command leaves the
//! document as it was.

mod node;
mod path;

pub use node::Node;

use crate::cmd::Bytes;
use crate::rdb::{self, Input};
use crate::resp::{parse_i64, Reply};
use node::{Style, MAX_NESTING};
use path::{Mode, Selector, Step};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// The module type valkey-json registers, as an RDB module ID: the name
/// `ReJSON-RL`, then encoding version 3.
pub(crate) const MODULE_ID: u64 = crate::bloom::module_id(b"ReJSON-RL", 3);
// Module values in RDB are a run of typed primitives, ended by EOF.
const OP_EOF: u64 = 0;
const OP_STRING: u64 = 5;

const DEFAULT_PATH_LIMIT: u64 = 128;

/// What a JSON command can fail with: valkey-json's `JsonUtilCode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Parse,
    InvalidPath,
    InvalidMemberName,
    InvalidNumber,
    InvalidIdentifier,
    InvalidDotSequence,
    EmptyExpr,
    IndexNotNumber,
    StepZero,
    InvalidWildcard,
    PathNotExist,
    KeyNotFound,
    NotADocument,
    NotNumber,
    NotBool,
    NotString,
    NotObject,
    NotArray,
    ValueNotNumber,
    ValueNotString,
    ValueNotInteger,
    CommandSyntax,
    MultOverflow,
    AddOverflow,
    OutOfBounds,
    DocumentSize,
    PathLimit,
    RecursionDepth,
    DescentTokens,
    QuerySize,
    SearchLimit,
    ReplySize,
    CannotInsert,
    DollarNotRoot,
    NewKeyNotRoot,
}

impl Error {
    /// A mistake in the path itself rather than in what it found: a
    /// JSONPath stops only for these.
    pub fn is_syntax(self) -> bool {
        matches!(
            self,
            Error::InvalidPath
                | Error::InvalidMemberName
                | Error::InvalidNumber
                | Error::InvalidIdentifier
                | Error::InvalidDotSequence
                | Error::EmptyExpr
                | Error::IndexNotNumber
                | Error::StepZero
                | Error::RecursionDepth
                | Error::DescentTokens
                | Error::QuerySize
                | Error::SearchLimit
        )
    }

    pub fn message(self) -> &'static str {
        match self {
            Error::Parse => "SYNTAXERR Failed to parse JSON string due to syntax error",
            Error::InvalidPath => "SYNTAXERR Invalid JSON path",
            Error::InvalidMemberName => "SYNTAXERR Invalid object member name",
            Error::InvalidNumber => "SYNTAXERR Invalid number",
            Error::InvalidIdentifier => "SYNTAXERR Invalid identifier",
            Error::InvalidDotSequence => "SYNTAXERR Invalid dot sequence",
            Error::EmptyExpr => "SYNTAXERR Expression token cannot be empty",
            Error::IndexNotNumber => "SYNTAXERR Array index is not a number",
            Error::StepZero => "SYNTAXERR Step in the slice cannot be zero",
            Error::InvalidWildcard => "ERR Invalid use of wildcard",
            Error::PathNotExist => "NONEXISTENT JSON path does not exist",
            Error::KeyNotFound => "NONEXISTENT Document key does not exist",
            Error::NotADocument => "WRONGTYPE Not a JSON document key",
            Error::NotNumber => "WRONGTYPE JSON element is not a number",
            Error::NotBool => "WRONGTYPE JSON element is not a bool",
            Error::NotString => "WRONGTYPE JSON element is not a string",
            Error::NotObject => "WRONGTYPE JSON element is not an object",
            Error::NotArray => "WRONGTYPE JSON element is not an array",
            Error::ValueNotNumber => "WRONGTYPE Value is not a number",
            Error::ValueNotString => "WRONGTYPE Value is not a string",
            Error::ValueNotInteger => "WRONGTYPE Value is not an integer",
            Error::CommandSyntax => "SYNTAXERR Command syntax error",
            Error::MultOverflow => "OVERFLOW Multiplication would overflow",
            Error::AddOverflow => "OVERFLOW Addition would overflow",
            Error::OutOfBounds => "OUTOFBOUNDARIES Array index is out of bounds",
            Error::DocumentSize => "LIMIT Document size limit is exceeded",
            Error::PathLimit => "LIMIT Document path nesting limit is exceeded",
            Error::RecursionDepth => "LIMIT Parser recursion depth is exceeded",
            Error::DescentTokens => {
                "LIMIT Total number of recursive descent tokens in the query string exceeds the limit"
            }
            Error::QuerySize => "LIMIT Query string size limit is exceeded",
            Error::SearchLimit => "LIMIT Path search work limit is exceeded",
            Error::ReplySize => "LIMIT Reply size limit is exceeded",
            Error::CannotInsert => "ERROR Cannot insert a member into a non-object value",
            Error::DollarNotRoot => "SYNTAXERR Dollar sign cannot apply to non-root element",
            Error::NewKeyNotRoot => "SYNTAXERR A new Valkey key's path must be root",
        }
    }

    pub fn reply(self) -> Reply {
        Reply::err(self.message())
    }
}

/// Append the body of the RDB module value valkey-json saves: the
/// document as compact JSON.
pub(crate) fn write_rdb(doc: &Node, out: &mut Vec<u8>) {
    rdb::put_len(out, OP_STRING);
    rdb::put_raw(out, &node::to_json(doc));
    rdb::put_len(out, OP_EOF);
}

/// The value `write_rdb` writes, its module ID already read.
pub(crate) fn read_rdb<I: Input + ?Sized>(r: &mut I) -> Option<Node> {
    (r.len()? == OP_STRING).then_some(())?;
    let text = r.string()?;
    let (doc, _) = node::parse(&text, MAX_NESTING).ok()?;
    (r.len()? == OP_EOF).then_some(doc)
}

/// A document as JSON text, as persistence keeps it.
pub fn to_json(doc: &Node) -> Vec<u8> {
    node::to_json(doc)
}

/// A document from what `to_json` wrote.
pub fn from_json(text: &[u8]) -> Option<Node> {
    node::parse(text, MAX_NESTING).ok().map(|(doc, _)| doc)
}

/// The `json.*` settings a command runs with, read when it arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// `json.max-document-size` in bytes; 0 for none.
    pub max_size: u64,
    /// `json.max-path-limit`: how deep a document may nest.
    pub path_limit: u64,
    /// Whether the size limit holds. Not for what a primary sends: the
    /// primary checked it, and a replica that refused would differ.
    pub enforce: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            max_size: 0,
            path_limit: DEFAULT_PATH_LIMIT,
            enforce: true,
        }
    }
}

impl Settings {
    /// How deep a value a command parses may nest.
    fn depth(&self) -> usize {
        usize::try_from(self.path_limit).unwrap_or(usize::MAX)
    }

    /// Whether values `depth` deep placed `at` deep would nest too deep.
    /// Past what the parser takes, never: every walk over a document
    /// recurses.
    fn too_deep(&self, at: usize, depth: usize) -> bool {
        (at + depth) as u64 > self.path_limit.min(MAX_NESTING as u64)
    }

    /// Whether a document would be over the size limit; `size` is only
    /// worked out when there is one.
    fn too_big(&self, size: impl FnOnce() -> u64) -> bool {
        self.enforce && self.max_size > 0 && size() > self.max_size
    }

    /// Whether `copies` copies of `each` bytes added to `doc` are too
    /// much: over the size limit, or, limit or not, over [`MAX_WRITE`].
    fn too_big_write(&self, doc: &Node, copies: u64, each: u64) -> bool {
        let added = copies.saturating_mul(each);
        (self.enforce && added > MAX_WRITE) || self.too_big(|| doc.size().saturating_add(added))
    }
}

/// The most one write may add to a document, all copies together, with or
/// without `json.max-document-size`: a value set at every match of a path
/// is copied once per match.
const MAX_WRITE: u64 = 512 << 20;

/// The `json.*` settings of a server, for CONFIG GET and CONFIG SET.
pub struct Config {
    max_size: AtomicU64,
    path_limit: AtomicU64,
}

impl Default for Config {
    fn default() -> Self {
        let s = Settings::default();
        Config {
            max_size: AtomicU64::new(s.max_size),
            path_limit: AtomicU64::new(s.path_limit),
        }
    }
}

/// One `json.*` setting CONFIG SET checked, to apply once all of them are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    MaxSize(u64),
    PathLimit(u64),
}

impl Config {
    pub fn settings(&self, enforce: bool) -> Settings {
        Settings {
            max_size: self.max_size.load(Ordering::Relaxed),
            path_limit: self.path_limit.load(Ordering::Relaxed),
            enforce,
        }
    }

    /// Every setting with its value, as CONFIG GET shows it.
    pub fn shown(&self) -> [(&'static str, String); 2] {
        [
            (
                "json.max-document-size",
                self.max_size.load(Ordering::Relaxed).to_string(),
            ),
            (
                "json.max-path-limit",
                self.path_limit.load(Ordering::Relaxed).to_string(),
            ),
        ]
    }

    /// The change CONFIG SET of `name` to `value` makes; `None` when the
    /// name is not one of these, the reason when the value is refused.
    pub fn check(name: &[u8], value: &[u8]) -> Option<Result<Change, String>> {
        let between = |max: i64| format!("argument must be between 0 and {max} inclusive");
        Some(match name {
            b"json.max-document-size" => match crate::engine::parse_bytes(value) {
                Some(n) if i64::try_from(n).is_ok() => Ok(Change::MaxSize(n as u64)),
                Some(_) => Err(between(i64::MAX)),
                None => Err("argument must be a memory value".to_string()),
            },
            b"json.max-path-limit" => match String::from_utf8_lossy(value).parse::<i64>() {
                Ok(n) if (0..=i64::from(i32::MAX)).contains(&n) => Ok(Change::PathLimit(n as u64)),
                Ok(_) => Err(between(i32::MAX.into())),
                Err(_) => Err("argument couldn't be parsed into an integer".to_string()),
            },
            _ => return None,
        })
    }

    pub fn apply(&self, change: Change) {
        match change {
            Change::MaxSize(n) => self.max_size.store(n, Ordering::Relaxed),
            Change::PathLimit(n) => self.path_limit.store(n, Ordering::Relaxed),
        }
    }
}

/// The output options of JSON.GET.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Format {
    pub indent: Bytes,
    pub newline: Bytes,
    pub space: Bytes,
}

impl Format {
    fn style(&self) -> Style<'_> {
        Style {
            indent: &self.indent,
            newline: &self.newline,
            space: &self.space,
        }
    }
}

/// A JSON.* command that only reads its key.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadOp {
    Get {
        paths: Vec<Bytes>,
        format: Format,
    },
    StrLen(Bytes),
    ObjLen(Bytes),
    ArrLen(Bytes),
    ObjKeys(Bytes),
    Type(Bytes),
    Resp(Bytes),
    ArrIndex {
        path: Bytes,
        value: Bytes,
        start: i64,
        stop: i64,
    },
    /// JSON.DEBUG MEMORY; without a path, the whole document.
    Memory(Option<Bytes>),
    /// JSON.DEBUG FIELDS.
    Fields(Bytes),
    /// JSON.DEBUG DEPTH.
    Depth,
}

/// A JSON.* command that may change its key.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOp {
    Set {
        path: Bytes,
        json: Bytes,
        nx: bool,
        xx: bool,
        /// One key of JSON.MSET, which the engine checked first.
        mset: bool,
    },
    Merge {
        path: Bytes,
        json: Bytes,
    },
    Del(Bytes),
    NumIncrBy(Bytes, Node),
    NumMultBy(Bytes, Node),
    Toggle(Bytes),
    Clear(Bytes),
    StrAppend(Bytes, Bytes),
    ArrAppend(Bytes, Vec<Bytes>),
    ArrPop(Bytes, i64),
    ArrInsert(Bytes, i64, Vec<Bytes>),
    ArrTrim(Bytes, i64, i64),
}

impl WriteOp {
    /// The keyspace event a change sends.
    pub fn event(&self) -> &'static str {
        match self {
            WriteOp::Set { mset: true, .. } => "json.mset",
            WriteOp::Set { .. } => "json.set",
            WriteOp::Merge { .. } => "json.merge",
            WriteOp::Del(_) => "json.del",
            WriteOp::NumIncrBy(..) => "json.numincrby",
            WriteOp::NumMultBy(..) => "json.nummultby",
            WriteOp::Toggle(_) => "json.toggle",
            WriteOp::Clear(_) => "json.clear",
            WriteOp::StrAppend(..) => "json.strappend",
            WriteOp::ArrAppend(..) => "json.arrappend",
            WriteOp::ArrPop(..) => "json.arrpop",
            WriteOp::ArrInsert(..) => "json.arrinsert",
            WriteOp::ArrTrim(..) => "json.arrtrim",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonOp {
    Read(ReadOp),
    Write(WriteOp),
}

/// A JSON.* command on one key, with the settings it runs under, which
/// the engine fills in when the command reaches it.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonCmd {
    pub op: JsonOp,
    pub settings: Settings,
    /// A write as it was sent, in RESP, for replicas.
    pub repl: Vec<u8>,
}

impl JsonCmd {
    pub fn is_write(&self) -> bool {
        matches!(self.op, JsonOp::Write(_))
    }

    /// Whether it is refused when memory is full (`deny-oom`).
    pub fn may_grow(&self) -> bool {
        matches!(
            self.op,
            JsonOp::Write(
                WriteOp::Set { .. }
                    | WriteOp::Merge { .. }
                    | WriteOp::StrAppend(..)
                    | WriteOp::Toggle(_)
                    | WriteOp::ArrAppend(..)
                    | WriteOp::ArrInsert(..)
            )
        )
    }
}

/// How many values `doc` holds, itself included, counting no further than
/// `limit`: what freeing it costs.
pub fn count_up_to(doc: &Node, limit: usize) -> usize {
    let mut stack = vec![doc];
    let mut n = 0;
    while let Some(v) = stack.pop() {
        n += 1;
        if n >= limit {
            break;
        }
        match v {
            Node::Arr(items) => stack.extend(items.iter().take(limit)),
            Node::Obj(members) => stack.extend(members.iter().take(limit).map(|(_, v)| v)),
            _ => {}
        }
    }
    n
}

/// What a JSON.* command parses to.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Parsed {
    Key(Bytes, JsonCmd),
    /// JSON.MGET: the keys and the path.
    MGet(Vec<Bytes>, Bytes),
    /// JSON.MSET: key, path and value, in order.
    MSet(Vec<(Bytes, Bytes, Bytes)>),
    /// The reply it gives when it runs, whatever the keys hold: an error
    /// in the arguments, or JSON.DEBUG HELP.
    Reply(Reply),
}

const DEBUG_HELP: [&str; 4] = [
    "JSON.DEBUG MEMORY <key> [path] - report memory size (bytes). Without path reports \
     document size without keys. With path reports size including keys",
    "JSON.DEBUG DEPTH <key> - report the maximum path depth of the JSON document.",
    "JSON.DEBUG FIELDS <key> [path] - report number of fields in the JSON element. Path \
     defaults to root if not provided.",
    "JSON.DEBUG HELP - print help message.",
];

fn arity(name: &str) -> Reply {
    Reply::Error(format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}

/// The text of an argument as valkey-json reads it, a C string: up to the
/// first NUL byte.
fn cstr(s: &[u8]) -> &[u8] {
    &s[..s.iter().position(|&c| c == 0).unwrap_or(s.len())]
}

fn is_root(path: &[u8]) -> bool {
    matches!(cstr(path), b"." | b"$")
}

/// A JSON.* command. `name` is upper case. An `Err` is refused before
/// it runs, as the server's own arity check refuses it; an error in the
/// arguments valkey-json finds is the `Parsed::Reply` the command gives.
pub(crate) fn parse(name: &str, args: &mut Vec<Bytes>) -> Result<Parsed, Reply> {
    let n = args.len();
    let fits = match name {
        "JSON.SET" | "JSON.MSET" | "JSON.ARRAPPEND" | "JSON.ARRINDEX" => n >= 4,
        "JSON.MERGE" | "JSON.NUMINCRBY" | "JSON.NUMMULTBY" => n == 4,
        "JSON.STRAPPEND" | "JSON.MGET" => n >= 3,
        "JSON.ARRINSERT" => n >= 5,
        "JSON.ARRTRIM" => n == 5,
        _ => n >= 2,
    };
    if !fits {
        return Err(arity(name));
    }
    if name == "JSON.DEBUG" {
        return parse_debug(args);
    }
    Ok(parse_command(name, args).unwrap_or_else(Parsed::Reply))
}

fn parse_debug(args: &mut [Bytes]) -> Result<Parsed, Reply> {
    let n = args.len();
    let sub = &args[1];
    let (full, fits) = if sub.eq_ignore_ascii_case(b"MEMORY") {
        ("JSON.DEBUG|MEMORY", n >= 3)
    } else if sub.eq_ignore_ascii_case(b"FIELDS") {
        ("JSON.DEBUG|FIELDS", n >= 3)
    } else if sub.eq_ignore_ascii_case(b"DEPTH") {
        ("JSON.DEBUG|DEPTH", n == 3)
    } else if sub.eq_ignore_ascii_case(b"HELP") {
        ("JSON.DEBUG|HELP", n == 2)
    } else {
        let shown = cstr(sub);
        return Err(Reply::Error(format!(
            "ERR unknown subcommand '{}'. Try JSON.DEBUG HELP.",
            String::from_utf8_lossy(&shown[..shown.len().min(128)])
        )));
    };
    if !fits {
        return Err(arity(full));
    }
    let op = match full {
        "JSON.DEBUG|HELP" => {
            let lines = DEBUG_HELP.iter().map(|s| Reply::Simple(s.to_string()));
            return Ok(Parsed::Reply(Reply::Array(lines.collect())));
        }
        // The module takes at most a path after the key.
        _ if n > 4 => return Ok(Parsed::Reply(arity(full))),
        "JSON.DEBUG|DEPTH" => ReadOp::Depth,
        "JSON.DEBUG|MEMORY" => ReadOp::Memory(args.get_mut(3).map(std::mem::take)),
        _ => ReadOp::Fields(
            args.get_mut(3)
                .map_or_else(|| b".".to_vec(), std::mem::take),
        ),
    };
    let cmd = JsonCmd {
        op: JsonOp::Read(op),
        settings: Settings::default(),
        repl: Vec::new(),
    };
    Ok(Parsed::Key(std::mem::take(&mut args[2]), cmd))
}

fn take(args: &mut [Bytes], i: usize) -> Bytes {
    std::mem::take(&mut args[i])
}

/// The path argument at `i`; the root where the command leaves it out.
fn path_at(args: &mut [Bytes], i: usize) -> Bytes {
    args.get_mut(i)
        .map_or_else(|| b".".to_vec(), std::mem::take)
}

fn integer(raw: &[u8]) -> Result<i64, Reply> {
    parse_i64(raw).ok_or_else(|| Error::ValueNotInteger.reply())
}

fn key_cmd(args: &mut [Bytes], op: JsonOp, repl: Vec<u8>) -> Parsed {
    let cmd = JsonCmd {
        op,
        settings: Settings::default(),
        repl,
    };
    Parsed::Key(take(args, 1), cmd)
}

fn parse_command(name: &str, args: &mut Vec<Bytes>) -> Result<Parsed, Reply> {
    let n = args.len();
    let fits = match name {
        "JSON.SET" => n <= 5,
        "JSON.MSET" => (n - 1).is_multiple_of(3),
        "JSON.STRAPPEND" | "JSON.ARRPOP" => n <= 4,
        "JSON.ARRINDEX" => n <= 6,
        "JSON.GET" | "JSON.MGET" | "JSON.ARRAPPEND" | "JSON.ARRINSERT" | "JSON.MERGE"
        | "JSON.NUMINCRBY" | "JSON.NUMMULTBY" | "JSON.ARRTRIM" => true,
        _ => n <= 3,
    };
    if !fits {
        return Err(arity(name));
    }
    let op = match name {
        "JSON.MGET" => {
            let path = args.pop().unwrap_or_default();
            return Ok(Parsed::MGet(args.drain(1..).collect(), path));
        }
        "JSON.MSET" => {
            let mut triples = Vec::with_capacity((n - 1) / 3);
            let mut rest = args.drain(1..);
            while let (Some(k), Some(p), Some(v)) = (rest.next(), rest.next(), rest.next()) {
                triples.push((k, p, v));
            }
            return Ok(Parsed::MSet(triples));
        }
        "JSON.GET" => parse_get(args).map_err(Error::reply)?,
        "JSON.STRLEN" => ReadOp::StrLen(path_at(args, 2)),
        "JSON.OBJLEN" => ReadOp::ObjLen(path_at(args, 2)),
        "JSON.ARRLEN" => ReadOp::ArrLen(path_at(args, 2)),
        "JSON.OBJKEYS" => ReadOp::ObjKeys(path_at(args, 2)),
        "JSON.TYPE" => ReadOp::Type(path_at(args, 2)),
        "JSON.RESP" => ReadOp::Resp(path_at(args, 2)),
        "JSON.ARRINDEX" => {
            let start = args.get(4).map_or(Ok(0), |raw| integer(raw))?;
            let stop = args.get(5).map_or(Ok(0), |raw| integer(raw))?;
            ReadOp::ArrIndex {
                path: take(args, 2),
                value: take(args, 3),
                start,
                stop,
            }
        }
        _ => {
            // Replicas get the command as it came.
            let mut repl = Vec::new();
            let all: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
            crate::repl::command(&mut repl, &all);
            let op = parse_write(name, args)?;
            return Ok(key_cmd(args, JsonOp::Write(op), repl));
        }
    };
    Ok(key_cmd(args, JsonOp::Read(op), Vec::new()))
}

fn parse_write(name: &str, args: &mut Vec<Bytes>) -> Result<WriteOp, Reply> {
    Ok(match name {
        "JSON.SET" => {
            let (nx, xx) = match args.get(4).map(|w| cstr(w)) {
                None => (false, false),
                Some(w) if w.eq_ignore_ascii_case(b"NX") => (true, false),
                Some(w) if w.eq_ignore_ascii_case(b"XX") => (false, true),
                Some(_) => return Err(Error::CommandSyntax.reply()),
            };
            WriteOp::Set {
                path: take(args, 2),
                json: take(args, 3),
                nx,
                xx,
                mset: false,
            }
        }
        "JSON.MERGE" => WriteOp::Merge {
            path: take(args, 2),
            json: take(args, 3),
        },
        "JSON.DEL" | "JSON.FORGET" => WriteOp::Del(path_at(args, 2)),
        "JSON.NUMINCRBY" | "JSON.NUMMULTBY" => {
            let by = match node::parse(&args[3], MAX_NESTING) {
                Ok((by @ (Node::Int(_) | Node::Num(_)), _)) => by,
                _ => return Err(Error::ValueNotNumber.reply()),
            };
            let path = take(args, 2);
            if name == "JSON.NUMINCRBY" {
                WriteOp::NumIncrBy(path, by)
            } else {
                WriteOp::NumMultBy(path, by)
            }
        }
        "JSON.TOGGLE" => WriteOp::Toggle(path_at(args, 2)),
        "JSON.CLEAR" => WriteOp::Clear(path_at(args, 2)),
        "JSON.STRAPPEND" if args.len() == 3 => WriteOp::StrAppend(b".".to_vec(), take(args, 2)),
        "JSON.STRAPPEND" => WriteOp::StrAppend(take(args, 2), take(args, 3)),
        "JSON.ARRAPPEND" => {
            let values = args.split_off(3);
            WriteOp::ArrAppend(take(args, 2), values)
        }
        "JSON.ARRPOP" => {
            let index = args.get(3).map_or(Ok(-1), |raw| integer(raw))?;
            WriteOp::ArrPop(path_at(args, 2), index)
        }
        "JSON.ARRINSERT" => {
            let index = integer(&args[3])?;
            let values = args.split_off(4);
            WriteOp::ArrInsert(take(args, 2), index, values)
        }
        "JSON.ARRTRIM" => {
            let (start, stop) = (integer(&args[3])?, integer(&args[4])?);
            WriteOp::ArrTrim(take(args, 2), start, stop)
        }
        _ => return Err(Reply::Error(format!("ERR unknown command '{name}'"))),
    })
}

fn parse_get(args: &mut [Bytes]) -> Result<ReadOp, Error> {
    let mut format = Format::default();
    // Every argument that is not an option is a path, wherever it stands.
    let mut paths = Vec::new();
    let mut i = 2;
    while i < args.len() {
        let word = cstr(&args[i]);
        let is = |w: &str| word.eq_ignore_ascii_case(w.as_bytes());
        if is("NEWLINE") || is("SPACE") || is("INDENT") {
            let value = cstr(args.get(i + 1).ok_or(Error::CommandSyntax)?).to_vec();
            match word[0].to_ascii_uppercase() {
                b'N' => format.newline = value,
                b'S' => format.space = value,
                _ => format.indent = value,
            }
            i += 1;
        } else if !is("NOESCAPE") {
            paths.push(i);
        }
        i += 1;
    }
    let paths = paths.into_iter().map(|at| take(args, at)).collect();
    Ok(ReadOp::Get { paths, format })
}

/// One step of a path, kept after the selection that found it is gone.
/// Indexes order before names, and by number.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum At {
    Index(usize),
    Key(Box<[u8]>),
}

fn owned(steps: &[Step]) -> Vec<At> {
    steps
        .iter()
        .map(|step| match step {
            Step::Key(name) => At::Key((*name).into()),
            Step::Index(i) => At::Index(*i),
        })
        .collect()
}

fn find_mut<'n>(mut node: &'n mut Node, path: &[At]) -> Option<&'n mut Node> {
    for at in path {
        node = match (node, at) {
            (Node::Obj(members), At::Key(name)) => members
                .iter_mut()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v)?,
            (Node::Arr(items), At::Index(i)) => items.get_mut(*i)?,
            _ => return None,
        };
    }
    Some(node)
}

/// Remove the value at `path`; false when there is none (or it is the
/// root, which a path cannot remove).
fn remove_at(root: &mut Node, path: &[At]) -> bool {
    let Some((last, parent)) = path.split_last() else {
        return false;
    };
    match (find_mut(root, parent), last) {
        (Some(Node::Obj(members)), At::Key(name)) => {
            match members.iter().position(|(k, _)| k == name) {
                Some(i) => {
                    members.remove(i);
                    true
                }
                None => false,
            }
        }
        (Some(Node::Arr(items)), At::Index(i)) if *i < items.len() => {
            items.remove(*i);
            true
        }
        _ => false,
    }
}

/// Set member `name` of the object at `parent` to `value`.
fn put_member(root: &mut Node, parent: &[At], name: &[u8], value: Node) {
    if let Some(Node::Obj(members)) = find_mut(root, parent) {
        match members.iter_mut().find(|(k, _)| **k == *name) {
            Some((_, v)) => *v = value,
            None => members.push((name.into(), value)),
        }
    }
}

/// The order valkey-json changes the values it found in: deepest first,
/// so that a change to an array leaves the paths still to change valid.
fn deepest_first(paths: &[Option<Vec<At>>]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..paths.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(paths[i].as_ref().map_or(0, Vec::len)));
    order
}

/// A read's selection: a legacy path stops at any error, a JSONPath only
/// at a syntax error, keeping what it found before.
fn read_sel<'a>(doc: &'a Node, path: &'a [u8]) -> Result<Selector<'a>, Error> {
    let mut sel = Selector::new(doc, false);
    match sel.get(path) {
        Err(e) if !sel.v2 || e.is_syntax() => Err(e),
        _ => Ok(sel),
    }
}

/// A selection that stops at any error.
fn strict<'a>(doc: &'a Node, path: &'a [u8], mode: Mode) -> Result<Selector<'a>, Error> {
    let mut sel = Selector::new(doc, false);
    sel.select(path, mode)?;
    Ok(sel)
}

/// A legacy path must find something, and something `fits`.
fn legacy(sel: &Selector, wrong: Error, fits: impl Fn(&Node) -> bool) -> Result<(), Error> {
    if sel.v2 {
        return Ok(());
    }
    if sel.results.is_empty() {
        return Err(Error::PathNotExist);
    }
    if !sel.results.iter().any(|(v, _)| fits(v)) {
        return Err(wrong);
    }
    Ok(())
}

/// The unique results, each with its path when it `fits`.
fn targets(sel: &Selector, fits: impl Fn(&Node) -> bool) -> Vec<Option<Vec<At>>> {
    sel.unique()
        .into_iter()
        .map(|(v, p)| fits(v).then(|| owned(p)))
        .collect()
}

fn is_array(v: &Node) -> bool {
    matches!(v, Node::Arr(_))
}

fn parse_value(json: &[u8], s: &Settings) -> Result<(Node, usize), Error> {
    node::parse(json, s.depth()).map_err(path::parse_error)
}

/// A value for a whole document, held to the size limit.
fn parse_doc(json: &[u8], s: &Settings) -> Result<Node, Error> {
    let (doc, _) = parse_value(json, s)?;
    if s.too_big(|| doc.size()) {
        return Err(Error::DocumentSize);
    }
    Ok(doc)
}

fn int(n: usize) -> Reply {
    Reply::Int(i64::try_from(n).unwrap_or(i64::MAX))
}

/// The reply of the array commands: a legacy path's first result, every
/// result for a JSONPath, `Nil` where a value did not fit.
fn first_or_all(v2: bool, results: &[Option<usize>]) -> Reply {
    if v2 {
        Reply::Array(results.iter().map(|r| r.map_or(Reply::Nil, int)).collect())
    } else {
        results
            .iter()
            .flatten()
            .next()
            .map_or(Reply::Nil, |&n| int(n))
    }
}

/// Run a command that only reads; `doc` is `None` for a missing key.
pub fn read(op: &ReadOp, doc: Option<&Node>, s: &Settings) -> Reply {
    let Some(doc) = doc else {
        return match op {
            ReadOp::ArrIndex { .. } => Error::KeyNotFound.reply(),
            _ => Reply::Nil,
        };
    };
    let reply = match op {
        ReadOp::Get { paths, format } => get(doc, paths, format),
        ReadOp::StrLen(path) => lengths(doc, path, Error::NotString, |v| match v {
            Node::Str(s) => Some(s.len()),
            _ => None,
        }),
        ReadOp::ObjLen(path) => lengths(doc, path, Error::NotObject, |v| match v {
            Node::Obj(members) => Some(members.len()),
            _ => None,
        }),
        ReadOp::ArrLen(path) => lengths(doc, path, Error::NotArray, |v| match v {
            Node::Arr(items) => Some(items.len()),
            _ => None,
        }),
        ReadOp::ObjKeys(path) => obj_keys(doc, path),
        ReadOp::Type(path) => types(doc, path),
        ReadOp::Resp(path) => resp(doc, path),
        ReadOp::ArrIndex {
            path,
            value,
            start,
            stop,
        } => arr_index(doc, path, value, *start, *stop, s),
        ReadOp::Memory(None) => Ok(Reply::Int(doc.size() as i64)),
        ReadOp::Memory(Some(path)) => counts(doc, path, Node::size),
        ReadOp::Fields(path) => counts(doc, path, |v| fields(v) - u64::from(v.is_container())),
        ReadOp::Depth => Ok(Reply::Int(path_depth(doc) as i64)),
    };
    reply.unwrap_or_else(Error::reply)
}

fn get(doc: &Node, paths: &[Bytes], format: &Format) -> Result<Reply, Error> {
    let style = format.style();
    let mut out = Vec::new();
    match paths {
        [] => fetch(&mut out, doc, b".", &style)?,
        [path] => fetch(&mut out, doc, path, &style)?,
        _ => fetch_many(&mut out, doc, paths, &style)?,
    }
    too_long(&out)?;
    Ok(Reply::Bulk(out))
}

/// A reply past [`node::MAX_TEXT`]: many values, or values repeated (a
/// JSONPath finds a value and what holds it), or a long INDENT.
fn too_long(out: &[u8]) -> Result<(), Error> {
    if out.len() > node::MAX_TEXT {
        return Err(Error::ReplySize);
    }
    Ok(())
}

/// Append the values at one path as JSON: the first for a legacy path,
/// all of them in an array for a JSONPath.
fn fetch(out: &mut Vec<u8>, doc: &Node, path: &[u8], style: &Style) -> Result<(), Error> {
    let sel = read_sel(doc, path)?;
    if !sel.v2 {
        let (first, _) = sel.results.first().ok_or(Error::PathNotExist)?;
        node::write(out, first, style, 0);
        return Ok(());
    }
    out.push(b'[');
    if !sel.results.is_empty() {
        out.extend_from_slice(style.newline);
        for (i, (v, _)) in sel.results.iter().enumerate() {
            too_long(out)?;
            out.extend_from_slice(style.indent);
            node::write(out, v, style, 1);
            if i + 1 < sel.results.len() {
                out.push(b',');
            }
            out.extend_from_slice(style.newline);
        }
    }
    out.push(b']');
    Ok(())
}

/// JSON.GET of several paths: an object with each path as a name. Any
/// path that is a JSONPath makes all of them answer as one.
fn fetch_many(out: &mut Vec<u8>, doc: &Node, paths: &[Bytes], style: &Style) -> Result<(), Error> {
    let v2 = paths.iter().any(|p| p.first() == Some(&b'$'));
    let mut sel = Selector::new(doc, v2);
    out.push(b'{');
    out.extend_from_slice(style.newline);
    for (i, path) in paths.iter().enumerate() {
        too_long(out)?;
        let path = cstr(path);
        match sel.get(path) {
            Err(e) if !v2 || e.is_syntax() => return Err(e),
            _ => {}
        }
        out.extend_from_slice(style.indent);
        node::write_str(out, path);
        out.push(b':');
        out.extend_from_slice(style.space);
        if v2 {
            out.push(b'[');
            out.extend_from_slice(style.newline);
            for (j, (v, _)) in sel.results.iter().enumerate() {
                too_long(out)?;
                style.indent(out, 2);
                node::write(out, v, style, 2);
                if j + 1 < sel.results.len() {
                    out.push(b',');
                }
                out.extend_from_slice(style.newline);
            }
            out.extend_from_slice(style.indent);
            out.push(b']');
        } else {
            let (first, _) = sel.results.first().ok_or(Error::PathNotExist)?;
            node::write(out, first, style, 1);
        }
        if i + 1 < paths.len() {
            out.push(b',');
        }
        out.extend_from_slice(style.newline);
    }
    out.push(b'}');
    Ok(())
}

/// JSON.MGET's answer for one document: its values at `path` as JSON, or
/// `Nil` when the path is bad or finds nothing. `Err` fails the command.
pub fn mget_one(doc: &Node, path: &[u8]) -> Result<Reply, Reply> {
    let mut out = Vec::new();
    match fetch(&mut out, doc, path, &Style::default()) {
        Ok(()) => Ok(Reply::Bulk(out)),
        Err(Error::InvalidPath | Error::PathNotExist) => Ok(Reply::Nil),
        Err(e) => Err(e.reply()),
    }
}

/// STRLEN, OBJLEN and ARRLEN: `len` of what fits, for every result.
fn lengths(
    doc: &Node,
    path: &[u8],
    wrong: Error,
    len: impl Fn(&Node) -> Option<usize>,
) -> Result<Reply, Error> {
    let sel = read_sel(doc, path)?;
    legacy(&sel, wrong, |v| len(v).is_some())?;
    let all: Vec<Option<usize>> = sel.results.iter().map(|(v, _)| len(v)).collect();
    Ok(first_or_all(sel.v2, &all))
}

/// JSON.DEBUG MEMORY and FIELDS with a path.
fn counts(doc: &Node, path: &[u8], count: impl Fn(&Node) -> u64) -> Result<Reply, Error> {
    let sel = read_sel(doc, path)?;
    if !sel.v2 && sel.results.is_empty() {
        return Err(Error::PathNotExist);
    }
    let mut all = sel.results.iter().map(|(v, _)| Reply::Int(count(v) as i64));
    Ok(if sel.v2 {
        Reply::Array(all.collect())
    } else {
        all.next().unwrap_or(Reply::Nil)
    })
}

/// How many values a value holds, itself included.
fn fields(v: &Node) -> u64 {
    1 + match v {
        Node::Arr(items) => items.iter().map(fields).sum(),
        Node::Obj(members) => members.iter().map(|(_, v)| fields(v)).sum(),
        _ => 0,
    }
}

/// How deep a document nests: 0 for a scalar or an empty container.
fn path_depth(v: &Node) -> usize {
    match v {
        Node::Arr(items) => items.iter().map(|c| 1 + path_depth(c)).max().unwrap_or(0),
        Node::Obj(members) => members
            .iter()
            .map(|(_, c)| 1 + path_depth(c))
            .max()
            .unwrap_or(0),
        _ => 0,
    }
}

fn obj_keys(doc: &Node, path: &[u8]) -> Result<Reply, Error> {
    let keys = |v: &Node| match v {
        Node::Obj(members) => members
            .iter()
            .map(|(k, _)| Reply::Bulk(k.to_vec()))
            .collect(),
        _ => Vec::new(),
    };
    let sel = match read_sel(doc, path).and_then(|sel| {
        legacy(&sel, Error::NotObject, |v| matches!(v, Node::Obj(_)))?;
        Ok(sel)
    }) {
        Ok(sel) => sel,
        Err(Error::InvalidPath | Error::PathNotExist) => return Ok(Reply::Nil),
        Err(e) => return Err(e),
    };
    let mut all = sel.results.iter().map(|(v, _)| keys(v));
    Ok(if sel.v2 {
        Reply::Array(all.map(Reply::Array).collect())
    } else {
        Reply::Array(all.find(|k| !k.is_empty()).unwrap_or_default())
    })
}

fn type_name(v: &Node) -> &'static str {
    match v {
        Node::Null => "null",
        Node::Bool(_) => "boolean",
        Node::Str(_) => "string",
        Node::Num(_) => "number",
        Node::Int(_) => "integer",
        Node::Obj(_) => "object",
        Node::Arr(_) => "array",
    }
}

fn types(doc: &Node, path: &[u8]) -> Result<Reply, Error> {
    let sel = match read_sel(doc, path) {
        Ok(sel) if !sel.v2 && sel.results.is_empty() => return Ok(Reply::Nil),
        Ok(sel) => sel,
        Err(Error::InvalidPath | Error::PathNotExist) => return Ok(Reply::Nil),
        Err(e) => return Err(e),
    };
    let mut all = sel
        .results
        .iter()
        .map(|(v, _)| Reply::Simple(type_name(v).to_string()));
    Ok(if sel.v2 {
        Reply::Array(all.collect())
    } else {
        all.next().unwrap_or(Reply::Nil)
    })
}

/// A value as JSON.RESP gives it.
fn resp_value(v: &Node) -> Reply {
    match v {
        Node::Null => Reply::Nil,
        Node::Bool(b) => Reply::Simple(b.to_string()),
        Node::Int(n) => Reply::Int(*n),
        Node::Num(text) => Reply::bulk(node::dtoa(node::num_value(text))),
        Node::Str(s) => Reply::Bulk(s.to_vec()),
        Node::Arr(items) => Reply::Array(
            std::iter::once(Reply::Simple("[".into()))
                .chain(items.iter().map(resp_value))
                .collect(),
        ),
        Node::Obj(members) => Reply::Array(
            std::iter::once(Reply::Simple("{".into()))
                .chain(
                    members
                        .iter()
                        .map(|(k, v)| Reply::Array(vec![Reply::Bulk(k.to_vec()), resp_value(v)])),
                )
                .collect(),
        ),
    }
}

fn resp(doc: &Node, path: &[u8]) -> Result<Reply, Error> {
    let sel = read_sel(doc, path)?;
    let mut all = sel.results.iter().map(|(v, _)| resp_value(v));
    if sel.v2 {
        return Ok(Reply::Array(all.collect()));
    }
    all.next().ok_or(Error::PathNotExist)
}

fn arr_index(
    doc: &Node,
    path: &[u8],
    value: &[u8],
    start: i64,
    stop: i64,
    s: &Settings,
) -> Result<Reply, Error> {
    let sel = read_sel(doc, path)?;
    legacy(&sel, Error::NotArray, is_array)?;
    let start = start.max(0);
    let (value, _) = parse_value(value, s)?;
    let find = |items: &[Node]| -> i64 {
        let size = items.len() as i64;
        if size == 0 {
            return -1;
        }
        let stop = if stop == 0 || stop == -1 {
            size
        } else {
            stop.min(size)
        };
        if start > stop {
            return -1;
        }
        (start..stop)
            .find(|&i| items[i as usize].same(&value))
            .unwrap_or(-1)
    };
    let all = sel.results.iter().map(|(v, _)| match v {
        Node::Arr(items) => Some(find(items)),
        _ => None,
    });
    Ok(if sel.v2 {
        Reply::Array(all.map(|r| r.map_or(Reply::Nil, Reply::Int)).collect())
    } else {
        all.flatten().next().map_or(Reply::Nil, Reply::Int)
    })
}

/// What a write did to the key.
#[derive(Debug)]
pub enum Effect {
    /// Nothing: the command failed, or a condition stopped it. Nothing
    /// goes to replicas.
    Kept,
    /// The document changed where it is, or the command counts as a
    /// change though it found nothing to do.
    Changed,
    /// The key now holds this document, whatever it held; its deadline
    /// goes.
    Replaced(Box<Node>),
    /// The key went.
    Deleted,
}

/// What a write did, for the shard to keep.
#[derive(Debug)]
pub struct Done {
    pub reply: Reply,
    pub effect: Effect,
}

impl Done {
    fn kept(reply: Reply) -> Done {
        Done {
            reply,
            effect: Effect::Kept,
        }
    }

    fn changed(reply: Reply) -> Done {
        Done {
            reply,
            effect: Effect::Changed,
        }
    }

    fn replaced(doc: Node) -> Done {
        Done {
            reply: Reply::Ok,
            effect: Effect::Replaced(Box::new(doc)),
        }
    }
}

/// Run a command that may write; `doc` is `None` for a missing key.
pub fn write(op: WriteOp, doc: Option<&mut Node>, s: &Settings) -> Done {
    run(op, doc, s).unwrap_or_else(|e| Done::kept(e.reply()))
}

fn run(op: WriteOp, doc: Option<&mut Node>, s: &Settings) -> Result<Done, Error> {
    match op {
        WriteOp::Set {
            path,
            json,
            nx,
            xx,
            mset: _,
        } => return set(doc, &path, &json, nx, xx, s),
        WriteOp::Merge { path, json } => return merge(doc, &path, &json, s),
        WriteOp::Del(path) => return del(doc, &path),
        _ => {}
    }
    let doc = doc.ok_or(Error::KeyNotFound)?;
    let reply = match op {
        WriteOp::NumIncrBy(path, by) => num_op(doc, &path, &by, false)?,
        WriteOp::NumMultBy(path, by) => num_op(doc, &path, &by, true)?,
        WriteOp::Toggle(path) => toggle(doc, &path)?,
        WriteOp::Clear(path) => clear(doc, &path)?,
        WriteOp::StrAppend(path, json) => str_append(doc, &path, &json, s)?,
        WriteOp::ArrAppend(path, values) => arr_insert(doc, &path, None, &values, s)?,
        WriteOp::ArrInsert(path, index, values) => arr_insert(doc, &path, Some(index), &values, s)?,
        WriteOp::ArrPop(path, index) => arr_pop(doc, &path, index)?,
        WriteOp::ArrTrim(path, start, stop) => arr_trim(doc, &path, start, stop)?,
        WriteOp::Set { .. } | WriteOp::Merge { .. } | WriteOp::Del(_) => Reply::Nil,
    };
    Ok(Done::changed(reply))
}

fn set(
    doc: Option<&mut Node>,
    path: &[u8],
    json: &[u8],
    nx: bool,
    xx: bool,
    s: &Settings,
) -> Result<Done, Error> {
    let root = is_root(path);
    let Some(doc) = doc else {
        if !root {
            return Err(Error::NewKeyNotRoot);
        }
        if xx {
            return Ok(Done::kept(Reply::Nil));
        }
        return Ok(Done::replaced(parse_doc(json, s)?));
    };
    if root {
        if nx {
            return Ok(Done::kept(Reply::Nil));
        }
        return Ok(Done::replaced(parse_doc(json, s)?));
    }
    let (updates, inserts, at) = {
        let sel = strict(doc, path, Mode::Write)?;
        if nx && !sel.results.is_empty() || xx && !sel.inserts.is_empty() {
            return Ok(Done::kept(Reply::Nil));
        }
        (targets(&sel, |_| true), owned_inserts(&sel)?, sel.max_depth)
    };
    let copies = updates.iter().flatten().count() + inserts.len();
    let value = checked_value(doc, json, at, copies.max(1) as u64, s)?;
    for path in updates.iter().flatten() {
        if let Some(slot) = find_mut(doc, path) {
            *slot = value.clone();
        }
    }
    for (parent, name) in inserts {
        put_member(doc, &parent, &name, value.clone());
    }
    Ok(Done::changed(Reply::Ok))
}

/// A member a write adds: the path of its object, and its name.
type Insert = (Vec<At>, Vec<u8>);

/// The members a write adds. valkey-json adds each by a JSON Pointer made
/// of its parent's path and the name as written, so in a new name `~1`
/// stands for `/` and `~0` for `~`, and any other `~` makes the path
/// invalid.
fn owned_inserts(sel: &Selector) -> Result<Vec<Insert>, Error> {
    sel.inserts
        .iter()
        .map(|(parent, name)| Ok((owned(parent), pointer_name(name)?)))
        .collect()
}

fn pointer_name(name: &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(name.len());
    let mut bytes = name.iter().copied();
    while let Some(b) = bytes.next() {
        out.push(match b {
            b'~' => match bytes.next() {
                Some(b'0') => b'~',
                Some(b'1') => b'/',
                _ => return Err(Error::InvalidPath),
            },
            b => b,
        });
    }
    Ok(out)
}

/// The value a write places `copies` times `at` deep, held to the path
/// and size limits.
fn checked_value(
    doc: &Node,
    json: &[u8],
    at: usize,
    copies: u64,
    s: &Settings,
) -> Result<Node, Error> {
    let (value, depth) = parse_value(json, s)?;
    if s.too_deep(at, depth) {
        return Err(Error::PathLimit);
    }
    if s.too_big_write(doc, copies, value.size()) {
        return Err(Error::DocumentSize);
    }
    Ok(value)
}

/// JSON.MSET's check of one key before any is written: `doc` is what the
/// key holds, `None` when it is missing.
pub fn mset_check(doc: Option<&Node>, path: &[u8], json: &[u8], s: &Settings) -> Result<(), Reply> {
    let check = || -> Result<(), Error> {
        if is_root(path) {
            return parse_doc(json, s).map(drop);
        }
        let doc = doc.ok_or(Error::CommandSyntax)?;
        let sel = strict(doc, path, Mode::Write)?;
        let copies = sel.unique().len() + sel.inserts.len();
        checked_value(doc, json, sel.max_depth, copies.max(1) as u64, s).map(drop)
    };
    check().map_err(Error::reply)
}

fn merge(doc: Option<&mut Node>, path: &[u8], json: &[u8], s: &Settings) -> Result<Done, Error> {
    let root = is_root(path);
    match doc {
        None if !root => Err(Error::NewKeyNotRoot),
        None => {
            // RFC 7396 on nothing: the patch merged into {}.
            let mut doc = Node::Obj(Vec::new());
            merge_at(&mut doc, b".", json, s)?;
            Ok(Done::replaced(doc))
        }
        Some(doc) => {
            merge_at(doc, if root { b"." } else { path }, json, s)?;
            Ok(Done::changed(Reply::Ok))
        }
    }
}

/// Merge the patch `json` into every value `path` finds, and add it where
/// the path names a missing member. A value found under another one found
/// is left to that one's merge.
fn merge_at(doc: &mut Node, path: &[u8], json: &[u8], s: &Settings) -> Result<(), Error> {
    let (pending, inserts, patch) = {
        let sel = strict(doc, path, Mode::Write)?;
        let (patch, depth) = parse_value(json, s)?;
        let unique = sel.unique();
        let pending: Vec<(Vec<At>, Node)> = unique
            .iter()
            .filter(|(_, p)| {
                !unique
                    .iter()
                    .any(|(_, q)| q.len() < p.len() && p.starts_with(q))
            })
            .map(|(v, p)| (owned(p), merge_values(v, &patch, 0, s.path_limit)))
            .collect();
        let inserts = owned_inserts(&sel)?;
        if !pending.is_empty() || !inserts.is_empty() {
            if s.too_deep(sel.max_depth, depth) {
                return Err(Error::PathLimit);
            }
            let copies = (pending.len() + inserts.len()) as u64;
            if s.too_big_write(doc, copies, patch.size()) {
                return Err(Error::DocumentSize);
            }
        }
        (pending, inserts, patch)
    };
    for (path, merged) in pending {
        if let Some(slot) = find_mut(doc, &path) {
            *slot = merged;
        }
    }
    if !inserts.is_empty() {
        let value = merge_values(&Node::Obj(Vec::new()), &patch, 0, s.path_limit);
        for (parent, name) in inserts {
            put_member(doc, &parent, &name, value.clone());
        }
    }
    Ok(())
}

/// RFC 7396 merge patch: `patch` applied to `existing`. Deeper than
/// `limit`, the patch is taken as it is; the path limit refuses that.
fn merge_values(existing: &Node, patch: &Node, depth: u64, limit: u64) -> Node {
    let Node::Obj(changes) = patch else {
        return patch.clone();
    };
    if depth > limit {
        return patch.clone();
    }
    let empty = Vec::new();
    let old = match existing {
        Node::Obj(members) => members,
        _ => &empty,
    };
    let by_name: HashMap<&[u8], &Node> = changes.iter().map(|(k, v)| (&**k, v)).collect();
    let mut merged = Vec::with_capacity(old.len() + changes.len());
    for (name, value) in old {
        match by_name.get(&**name) {
            Some(Node::Null) => {}
            Some(change) => {
                merged.push((name.clone(), merge_values(value, change, depth + 1, limit)))
            }
            None => merged.push((name.clone(), value.clone())),
        }
    }
    let taken: HashSet<&[u8]> = merged.iter().map(|(k, _)| &**k).collect();
    let added: Vec<(Box<[u8]>, Node)> = changes
        .iter()
        .filter(|(k, v)| !matches!(v, Node::Null) && !taken.contains(&**k))
        .map(|(k, v)| {
            (
                k.clone(),
                merge_values(&Node::Obj(Vec::new()), v, depth + 1, limit),
            )
        })
        .collect();
    merged.extend(added);
    Node::Obj(merged)
}

fn del(doc: Option<&mut Node>, path: &[u8]) -> Result<Done, Error> {
    let Some(doc) = doc else {
        return Ok(Done::kept(Reply::Int(0)));
    };
    if is_root(path) {
        return Ok(Done {
            reply: Reply::Int(1),
            effect: Effect::Deleted,
        });
    }
    let none = || Ok(Done::kept(Reply::Int(0)));
    let mut paths = {
        let sel = match strict(doc, path, Mode::Delete) {
            Ok(sel) => sel,
            Err(Error::InvalidPath | Error::PathNotExist) => return none(),
            Err(e) => return Err(e),
        };
        if sel.results.is_empty() {
            match (sel.v2, sel.error()) {
                (false, _) | (true, Some(Error::InvalidPath | Error::PathNotExist)) => {
                    return none()
                }
                (true, Some(e)) => return Err(e),
                (true, None) => {}
            }
        }
        sel.results
            .iter()
            .map(|(_, p)| owned(p))
            .collect::<Vec<_>>()
    };
    // Deepest first, then the higher index or name first, so that each
    // removal leaves the paths still to remove where they were.
    paths.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| b.cmp(a)));
    paths.dedup();
    let removed = paths.iter().filter(|p| remove_at(doc, p)).count();
    Ok(Done::changed(int(removed)))
}

fn is_int64(x: f64) -> bool {
    const LIMIT: f64 = 9_223_372_036_854_775_808.0;
    (-LIMIT..LIMIT).contains(&x) && x == x.trunc()
}

/// NUMINCRBY and NUMMULTBY. Every result is worked out before any is
/// written, so an overflow leaves the document as it was.
fn num_op(doc: &mut Node, path: &[u8], by: &Node, mult: bool) -> Result<Reply, Error> {
    let by_value = by.number().unwrap_or(f64::NAN);
    let (v2, results) = {
        let sel = strict(doc, path, Mode::Read)?;
        legacy(&sel, Error::NotNumber, |v| v.number().is_some())?;
        let mut results = Vec::new();
        for (v, p) in sel.unique() {
            let Some(x) = v.number() else {
                results.push(None);
                continue;
            };
            let (new, out) = match (v, by) {
                (Node::Int(a), Node::Int(b)) if !mult && a.checked_add(*b).is_some() => {
                    let sum = a + b;
                    (Node::Int(sum), sum as f64)
                }
                _ if mult => {
                    let res = x * by_value;
                    if !res.is_finite() {
                        return Err(Error::MultOverflow);
                    }
                    let new = if is_int64(res) {
                        Node::Int(res as i64)
                    } else {
                        Node::Num(node::g17(res).into())
                    };
                    (new, res)
                }
                _ => {
                    let res = x + by_value;
                    if !res.is_finite() {
                        return Err(Error::AddOverflow);
                    }
                    (Node::Num(node::g17(res).into()), res)
                }
            };
            results.push(Some((owned(p), new, out)));
        }
        (sel.v2, results)
    };
    for (path, new, _) in results.iter().flatten() {
        if let Some(slot) = find_mut(doc, path) {
            *slot = new.clone();
        }
    }
    let outs = results.iter().map(|r| r.as_ref().map(|(_, _, out)| *out));
    if v2 {
        let parts: Vec<String> = outs.map(|o| o.map_or("null".into(), node::g17)).collect();
        return Ok(Reply::bulk(format!("[{}]", parts.join(","))));
    }
    let last = outs.flatten().last().unwrap_or(f64::NAN);
    Ok(Reply::bulk(node::g17(last)))
}

fn toggle(doc: &mut Node, path: &[u8]) -> Result<Reply, Error> {
    let (v2, results) = {
        let sel = read_sel(doc, path)?;
        legacy(&sel, Error::NotBool, |v| matches!(v, Node::Bool(_)))?;
        let results: Vec<Option<(Vec<At>, bool)>> = sel
            .unique()
            .into_iter()
            .map(|(v, p)| match v {
                Node::Bool(b) => Some((owned(p), !b)),
                _ => None,
            })
            .collect();
        (sel.v2, results)
    };
    for (path, b) in results.iter().flatten() {
        if let Some(slot) = find_mut(doc, path) {
            *slot = Node::Bool(*b);
        }
    }
    let news = results.iter().map(|r| r.as_ref().map(|(_, b)| *b));
    Ok(if v2 {
        Reply::Array(
            news.map(|b| b.map_or(Reply::Nil, |b| Reply::Int(b.into())))
                .collect(),
        )
    } else {
        news.flatten()
            .next()
            .map_or(Reply::Nil, |b| Reply::bulk(b.to_string()))
    })
}

fn clear(doc: &mut Node, path: &[u8]) -> Result<Reply, Error> {
    let paths = targets(&strict(doc, path, Mode::Read)?, |_| true);
    let mut cleared = 0;
    for i in deepest_first(&paths) {
        let Some(slot) = paths[i].as_ref().and_then(|p| find_mut(doc, p)) else {
            continue;
        };
        let done = match slot {
            Node::Arr(items) if !items.is_empty() => {
                items.clear();
                true
            }
            Node::Obj(members) if !members.is_empty() => {
                members.clear();
                true
            }
            Node::Bool(b) if *b => {
                *b = false;
                true
            }
            Node::Str(s) if !s.is_empty() => {
                *s = Box::default();
                true
            }
            Node::Int(n) if *n != 0 => {
                *n = 0;
                true
            }
            Node::Num(text) if node::num_value(text) != 0.0 => {
                *text = "0.0".into();
                true
            }
            _ => false,
        };
        cleared += usize::from(done);
    }
    Ok(int(cleared))
}

fn str_append(doc: &mut Node, path: &[u8], json: &[u8], s: &Settings) -> Result<Reply, Error> {
    if s.too_big(|| doc.size() + json.len() as u64) {
        return Err(Error::DocumentSize);
    }
    let (v2, results) = {
        let sel = strict(doc, path, Mode::Read)?;
        legacy(&sel, Error::NotString, |v| matches!(v, Node::Str(_)))?;
        let add = match parse_value(json, s)? {
            (Node::Str(add), _) => add,
            _ => return Err(Error::ValueNotString),
        };
        let copies = sel.unique().len() as u64;
        if s.too_big_write(doc, copies, add.len() as u64) {
            return Err(Error::DocumentSize);
        }
        // valkey-json joins the two as C strings: each ends at a NUL.
        let results: Vec<Option<(Vec<At>, Vec<u8>)>> = sel
            .unique()
            .into_iter()
            .map(|(v, p)| match v {
                Node::Str(old) => Some((owned(p), [cstr(old), cstr(&add)].concat())),
                _ => None,
            })
            .collect();
        (sel.v2, results)
    };
    let lens: Vec<Option<usize>> = results
        .iter()
        .map(|r| r.as_ref().map(|(_, new)| new.len()))
        .collect();
    for (path, new) in results.into_iter().flatten() {
        if let Some(slot) = find_mut(doc, &path) {
            *slot = Node::Str(new.into());
        }
    }
    if v2 {
        return Ok(first_or_all(true, &lens));
    }
    Ok(lens.iter().flatten().last().map_or(Reply::Nil, |&n| int(n)))
}

/// ARRAPPEND (`index` `None`) and ARRINSERT. Every index is checked before
/// any array changes.
fn arr_insert(
    doc: &mut Node,
    path: &[u8],
    index: Option<i64>,
    jsons: &[Bytes],
    s: &Settings,
) -> Result<Reply, Error> {
    let (v2, paths, values) = {
        let sel = strict(doc, path, Mode::Read)?;
        legacy(&sel, Error::NotArray, is_array)?;
        let mut values = Vec::with_capacity(jsons.len());
        let mut total = 0;
        for json in jsons {
            let (value, depth) = parse_value(json, s)?;
            if s.too_deep(sel.max_depth, depth) {
                return Err(Error::PathLimit);
            }
            total += value.size();
            values.push(value);
        }
        let paths = targets(&sel, is_array);
        let copies = paths.iter().flatten().count() as u64;
        if s.too_big_write(doc, copies.max(1), total) {
            return Err(Error::DocumentSize);
        }
        (sel.v2, paths, values)
    };
    let mut at = vec![0; paths.len()];
    for (i, path) in paths.iter().enumerate() {
        let Some(Node::Arr(items)) = path.as_ref().and_then(|p| find_mut(doc, p)) else {
            continue;
        };
        let size = items.len() as i64;
        let idx = match index {
            None => size,
            Some(i) if i < 0 && size == 0 => 0,
            Some(i) if i < 0 => size + i,
            Some(i) => i,
        };
        if idx < 0 || idx > size {
            return Err(Error::OutOfBounds);
        }
        at[i] = idx as usize;
    }
    let mut lens = vec![None; paths.len()];
    for i in deepest_first(&paths) {
        let Some(Node::Arr(items)) = paths[i].as_ref().and_then(|p| find_mut(doc, p)) else {
            continue;
        };
        items.splice(at[i]..at[i], values.iter().cloned());
        lens[i] = Some(items.len());
    }
    Ok(first_or_all(v2, &lens))
}

fn arr_pop(doc: &mut Node, path: &[u8], index: i64) -> Result<Reply, Error> {
    let (v2, paths) = {
        let sel = strict(doc, path, Mode::Read)?;
        legacy(&sel, Error::NotArray, is_array)?;
        (sel.v2, targets(&sel, is_array))
    };
    let mut popped: Vec<Option<Vec<u8>>> = vec![None; paths.len()];
    for i in deepest_first(&paths) {
        let Some(Node::Arr(items)) = paths[i].as_ref().and_then(|p| find_mut(doc, p)) else {
            continue;
        };
        if items.is_empty() {
            continue;
        }
        let size = items.len() as i64;
        let at = if index < 0 { size + index } else { index };
        let at = at.clamp(0, size - 1) as usize;
        popped[i] = Some(node::to_json(&items.remove(at)));
    }
    Ok(if v2 {
        Reply::Array(
            popped
                .into_iter()
                .map(|p| p.map_or(Reply::Nil, Reply::Bulk))
                .collect(),
        )
    } else {
        popped
            .into_iter()
            .flatten()
            .next()
            .map_or(Reply::Nil, Reply::Bulk)
    })
}

fn arr_trim(doc: &mut Node, path: &[u8], start: i64, stop: i64) -> Result<Reply, Error> {
    let (v2, paths) = {
        let sel = strict(doc, path, Mode::Read)?;
        legacy(&sel, Error::NotArray, is_array)?;
        (sel.v2, targets(&sel, is_array))
    };
    let mut lens = vec![None; paths.len()];
    for i in deepest_first(&paths) {
        let Some(Node::Arr(items)) = paths[i].as_ref().and_then(|p| find_mut(doc, p)) else {
            continue;
        };
        let size = items.len() as i64;
        let start = start.max(0);
        let stop = stop.min(size - 1);
        if size == 0 || start >= size || start > stop {
            items.clear();
        } else {
            items.truncate(stop as usize + 1);
            items.drain(..start as usize);
        }
        lens[i] = Some(items.len());
    }
    Ok(first_or_all(v2, &lens))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<Bytes> {
        words.iter().map(|w| w.as_bytes().to_vec()).collect()
    }

    /// Run commands on one document, as the shard would.
    struct Doc(Option<Node>, Settings);

    impl Doc {
        fn new() -> Doc {
            Doc(None, Settings::default())
        }

        fn run(&mut self, words: &[&str]) -> Reply {
            let name = words[0].to_ascii_uppercase();
            let parsed = match parse(&name, &mut args(words)) {
                Ok(parsed) => parsed,
                Err(e) => return e,
            };
            let cmd = match parsed {
                Parsed::Key(_, cmd) => cmd,
                Parsed::Reply(reply) => return reply,
                other => panic!("not a one-key command: {other:?}"),
            };
            match cmd.op {
                JsonOp::Read(op) => read(&op, self.0.as_ref(), &self.1),
                JsonOp::Write(op) => {
                    let done = write(op, self.0.as_mut(), &self.1);
                    match done.effect {
                        Effect::Replaced(doc) => self.0 = Some(*doc),
                        Effect::Deleted => self.0 = None,
                        Effect::Kept | Effect::Changed => {}
                    }
                    done.reply
                }
            }
        }

        fn json(&self) -> String {
            String::from_utf8(to_json(self.0.as_ref().expect("a document"))).expect("utf-8")
        }
    }

    fn bulk(s: &str) -> Reply {
        Reply::bulk(s)
    }

    #[test]
    fn set_and_get() {
        let mut d = Doc::new();
        assert_eq!(
            d.run(&["JSON.SET", "k", ".", r#"{"a":1,"b":[1,2]}"#]),
            Reply::Ok
        );
        assert_eq!(d.run(&["JSON.GET", "k"]), bulk(r#"{"a":1,"b":[1,2]}"#));
        assert_eq!(d.run(&["JSON.GET", "k", "$.b[*]"]), bulk("[1,2]"));
        assert_eq!(d.run(&["JSON.GET", "k", ".a"]), bulk("1"));
        assert_eq!(d.run(&["JSON.SET", "k", "$.c", "true"]), Reply::Ok);
        assert_eq!(d.json(), r#"{"a":1,"b":[1,2],"c":true}"#);
        assert_eq!(d.run(&["JSON.SET", "k", "$.c", "1", "NX"]), Reply::Nil);
        assert_eq!(d.run(&["JSON.SET", "k", "$.d", "1", "XX"]), Reply::Nil);
        assert_eq!(
            d.run(&["JSON.SET", "k", ".", "1", "YY"]),
            Error::CommandSyntax.reply()
        );
        assert_eq!(
            d.run(&["JSON.GET", "k", ".nope"]),
            Error::PathNotExist.reply()
        );
        assert_eq!(d.run(&["JSON.GET", "k", "$.nope"]), bulk("[]"));
    }

    #[test]
    fn new_key_needs_root() {
        let mut d = Doc::new();
        assert_eq!(
            d.run(&["JSON.SET", "k", ".a", "1"]),
            Error::NewKeyNotRoot.reply()
        );
        assert_eq!(d.run(&["JSON.SET", "k", "$", "1", "XX"]), Reply::Nil);
        assert_eq!(d.run(&["JSON.GET", "k"]), Reply::Nil);
    }

    #[test]
    fn get_formats_and_several_paths() {
        let mut d = Doc::new();
        d.run(&["JSON.SET", "k", ".", r#"{"a":[1,{"b":2}],"c":"x"}"#]);
        assert_eq!(
            d.run(&["JSON.GET", "k", "INDENT", "  ", "NEWLINE", "\n", "SPACE", " ", ".a"]),
            bulk("[\n  1,\n  {\n    \"b\": 2\n  }\n]")
        );
        assert_eq!(
            d.run(&["JSON.GET", "k", ".c", ".a[0]"]),
            bulk(r#"{".c":"x",".a[0]":1}"#)
        );
        assert_eq!(
            d.run(&["JSON.GET", "k", ".c", "$.a[0]"]),
            bulk(r#"{".c":["x"],"$.a[0]":[1]}"#)
        );
        assert_eq!(
            d.run(&["JSON.GET", "k", "INDENT"]),
            Error::CommandSyntax.reply()
        );
    }

    #[test]
    fn del_counts_and_orders() {
        let mut d = Doc::new();
        d.run(&["JSON.SET", "k", ".", r#"{"a":[1,2,3],"b":{"a":4}}"#]);
        assert_eq!(d.run(&["JSON.DEL", "k", "$.a[0,2]"]), Reply::Int(2));
        assert_eq!(d.json(), r#"{"a":[2],"b":{"a":4}}"#);
        assert_eq!(d.run(&["JSON.DEL", "k", "$..a"]), Reply::Int(2));
        assert_eq!(d.json(), r#"{"b":{}}"#);
        assert_eq!(d.run(&["JSON.DEL", "k", ".nope"]), Reply::Int(0));
        assert_eq!(d.run(&["JSON.FORGET", "k"]), Reply::Int(1));
        assert_eq!(d.run(&["JSON.DEL", "k"]), Reply::Int(0));
    }

    #[test]
    fn numbers() {
        let mut d = Doc::new();
        d.run(&["JSON.SET", "k", ".", r#"{"a":1,"b":1.5,"c":"x"}"#]);
        assert_eq!(d.run(&["JSON.NUMINCRBY", "k", ".a", "2"]), bulk("3"));
        assert_eq!(
            d.run(&["JSON.NUMINCRBY", "k", "$.*", "1"]),
            bulk("[4,2.5,null]")
        );
        assert_eq!(d.run(&["JSON.NUMMULTBY", "k", ".b", "2"]), bulk("5"));
        assert_eq!(d.json(), r#"{"a":4,"b":5,"c":"x"}"#);
        assert_eq!(
            d.run(&["JSON.NUMINCRBY", "k", ".c", "1"]),
            Error::NotNumber.reply()
        );
        assert_eq!(
            d.run(&["JSON.NUMINCRBY", "k", ".a", "x"]),
            Error::ValueNotNumber.reply()
        );
        d.run(&["JSON.SET", "k", ".a", "9223372036854775807"]);
        assert_eq!(
            d.run(&["JSON.NUMINCRBY", "k", ".a", "1"]),
            bulk("9.2233720368547758e+18")
        );
        assert_eq!(
            d.run(&["JSON.NUMMULTBY", "k", "$.b", "1e308"]),
            Error::MultOverflow.reply()
        );
        assert_eq!(d.json(), r#"{"a":9.2233720368547758e+18,"b":5,"c":"x"}"#);
    }

    #[test]
    fn strings_bools_and_lengths() {
        let mut d = Doc::new();
        d.run(&["JSON.SET", "k", ".", r#"{"s":"ab","t":true,"o":{"x":1}}"#]);
        assert_eq!(
            d.run(&["JSON.STRAPPEND", "k", ".s", r#""cd""#]),
            Reply::Int(4)
        );
        assert_eq!(d.run(&["JSON.STRLEN", "k", ".s"]), Reply::Int(4));
        assert_eq!(
            d.run(&["JSON.STRAPPEND", "k", ".s", "1"]),
            Error::ValueNotString.reply()
        );
        assert_eq!(d.run(&["JSON.TOGGLE", "k", ".t"]), bulk("false"));
        assert_eq!(
            d.run(&["JSON.TOGGLE", "k", "$.*"]),
            Reply::Array(vec![Reply::Nil, Reply::Int(1), Reply::Nil])
        );
        assert_eq!(d.run(&["JSON.OBJLEN", "k"]), Reply::Int(3));
        assert_eq!(
            d.run(&["JSON.OBJKEYS", "k", ".o"]),
            Reply::Array(vec![Reply::bulk("x")])
        );
        assert_eq!(
            d.run(&["JSON.TYPE", "k", ".o.x"]),
            Reply::Simple("integer".into())
        );
        assert_eq!(d.run(&["JSON.TYPE", "k", ".nope"]), Reply::Nil);
    }

    #[test]
    fn arrays() {
        let mut d = Doc::new();
        d.run(&["JSON.SET", "k", ".", "[1,2,3]"]);
        assert_eq!(
            d.run(&["JSON.ARRAPPEND", "k", ".", "4", "5"]),
            Reply::Int(5)
        );
        assert_eq!(
            d.run(&["JSON.ARRINSERT", "k", ".", "-1", r#""x""#]),
            Reply::Int(6)
        );
        assert_eq!(d.json(), r#"[1,2,3,4,"x",5]"#);
        assert_eq!(
            d.run(&["JSON.ARRINSERT", "k", ".", "7", "0"]),
            Error::OutOfBounds.reply()
        );
        assert_eq!(d.run(&["JSON.ARRPOP", "k"]), bulk("5"));
        assert_eq!(d.run(&["JSON.ARRPOP", "k", ".", "0"]), bulk("1"));
        assert_eq!(d.run(&["JSON.ARRINDEX", "k", ".", "3"]), Reply::Int(1));
        assert_eq!(d.run(&["JSON.ARRINDEX", "k", ".", "9"]), Reply::Int(-1));
        assert_eq!(d.run(&["JSON.ARRTRIM", "k", ".", "1", "2"]), Reply::Int(2));
        assert_eq!(d.json(), r#"[3,4]"#);
        assert_eq!(d.run(&["JSON.ARRLEN", "k"]), Reply::Int(2));
        assert_eq!(
            d.run(&["JSON.ARRPOP", "k", ".", "x"]),
            Error::ValueNotInteger.reply()
        );
    }

    #[test]
    fn clear_counts_what_changed() {
        let mut d = Doc::new();
        d.run(&[
            "JSON.SET",
            "k",
            ".",
            r#"{"a":[1],"b":{},"c":true,"d":"","e":2,"f":0.5}"#,
        ]);
        assert_eq!(d.run(&["JSON.CLEAR", "k", "$.*"]), Reply::Int(4));
        assert_eq!(
            d.json(),
            r#"{"a":[],"b":{},"c":false,"d":"","e":0,"f":0.0}"#
        );
    }

    #[test]
    fn merge_follows_rfc_7396() {
        let mut d = Doc::new();
        assert_eq!(
            d.run(&["JSON.MERGE", "k", "$", r#"{"a":1,"b":null}"#]),
            Reply::Ok
        );
        assert_eq!(d.json(), r#"{"a":1}"#);
        d.run(&["JSON.MERGE", "k", "$", r#"{"a":null,"c":{"d":1,"e":null}}"#]);
        assert_eq!(d.json(), r#"{"c":{"d":1}}"#);
        d.run(&["JSON.MERGE", "k", "$.c", r#"{"d":[1]}"#]);
        assert_eq!(d.json(), r#"{"c":{"d":[1]}}"#);
        assert_eq!(
            Doc::new().run(&["JSON.MERGE", "n", ".a", "{}"]),
            Error::NewKeyNotRoot.reply()
        );
    }

    #[test]
    fn resp_and_debug() {
        let mut d = Doc::new();
        d.run(&["JSON.SET", "k", ".", r#"{"a":[1,1.5,null,"s"]}"#]);
        assert_eq!(
            d.run(&["JSON.RESP", "k", ".a"]),
            Reply::Array(vec![
                Reply::Simple("[".into()),
                Reply::Int(1),
                Reply::bulk("1.5"),
                Reply::Nil,
                Reply::bulk("s"),
            ])
        );
        assert_eq!(d.run(&["JSON.DEBUG", "DEPTH", "k"]), Reply::Int(2));
        assert_eq!(d.run(&["JSON.DEBUG", "FIELDS", "k"]), Reply::Int(5));
        assert_eq!(d.run(&["JSON.DEBUG", "FIELDS", "k", ".a"]), Reply::Int(4));
        assert!(matches!(d.run(&["JSON.DEBUG", "MEMORY", "k"]), Reply::Int(n) if n > 0));
        assert!(matches!(d.run(&["JSON.DEBUG", "HELP"]), Reply::Array(lines) if lines.len() == 4));
        assert_eq!(
            d.run(&["JSON.DEBUG", "nope"]),
            Reply::err("ERR unknown subcommand 'nope'. Try JSON.DEBUG HELP.")
        );
        assert_eq!(
            d.run(&["JSON.DEBUG", "DEPTH"]),
            Reply::err("ERR wrong number of arguments for 'JSON.DEBUG|DEPTH' command")
        );
    }

    #[test]
    fn arity_and_limits() {
        let mut d = Doc::new();
        assert_eq!(
            d.run(&["JSON.SET", "k", "."]),
            Reply::err("ERR wrong number of arguments for 'JSON.SET' command")
        );
        assert_eq!(
            d.run(&["JSON.SET", "k", ".", "1", "NX", "x"]),
            Reply::err("ERR wrong number of arguments for 'JSON.SET' command")
        );
        d.1.path_limit = 2;
        assert_eq!(d.run(&["JSON.SET", "k", ".", "[[1]]"]), Reply::Ok);
        assert_eq!(
            d.run(&["JSON.SET", "k", ".", "[[[1]]]"]),
            Error::PathLimit.reply()
        );
        assert_eq!(d.run(&["JSON.SET", "k", "[0]", "[1]"]), Reply::Ok);
        assert_eq!(
            d.run(&["JSON.SET", "k", "[0][0]", "[1]"]),
            Error::PathLimit.reply()
        );
        d.1.max_size = 64;
        const LONG: &str = r#"["a string long enough that the document is over sixty-four bytes"]"#;
        assert_eq!(
            d.run(&["JSON.SET", "k", ".", LONG]),
            Error::DocumentSize.reply()
        );
        d.1.enforce = false;
        assert_eq!(d.run(&["JSON.SET", "k", ".", LONG]), Reply::Ok);
    }

    #[test]
    fn config_checks() {
        assert_eq!(
            Config::check(b"json.max-document-size", b"1mb"),
            Some(Ok(Change::MaxSize(1 << 20)))
        );
        assert!(matches!(
            Config::check(b"json.max-path-limit", b"-1"),
            Some(Err(_))
        ));
        assert_eq!(Config::check(b"bf.bloom-capacity", b"1"), None);
    }

    #[test]
    fn rdb_round_trip() {
        let (doc, _) = node::parse(br#"{"a":[1,2.50,"x"],"b":null}"#, MAX_NESTING).expect("json");
        let mut out = Vec::new();
        write_rdb(&doc, &mut out);
        assert_eq!(read_rdb(&mut rdb::Reader::new(&out)), Some(doc));
    }
}
