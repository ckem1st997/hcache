//! RESP2: reply encoding, an incremental request parser with hard limits,
//! and a client-side reply decoder (used by tests and the load generator).

use memchr::memchr;

/// The arguments of one command.
pub type Args = Vec<Vec<u8>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Ok,
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<Reply>),
    /// The null array: an EXEC aborted by WATCH.
    NilArray,
    /// A flat array of field/value pairs that is a map in RESP3, wherever
    /// it sits in the reply.
    Map(Vec<Reply>),
}

/// The RESP3 types of a command's reply, where RESP2 has only strings and
/// flat arrays. The reply itself is the RESP2 form.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Shape {
    #[default]
    Plain,
    /// A flat array of field/value pairs is a map.
    Map,
    /// The array is a set.
    Set,
    /// A string, or each string of an array, is a double.
    Double,
    /// The last element of the array is a double.
    LastDouble,
    /// A flat array of member/score pairs is an array of pairs.
    ScorePairs,
    /// A flat array of field/value pairs is an array of pairs.
    Pairs,
    /// ZMPOP: a key and member/score pairs.
    PopScores,
    /// EXEC: one shape per reply.
    Each(Vec<Shape>),
    /// An array of key/value pairs is a map.
    PairMap,
    /// GEOSEARCH WITHCOORD: in each match, the position is two doubles.
    Coords,
}

fn array_header(out: &mut Vec<u8>, kind: u8, len: usize) {
    out.push(kind);
    push_int(out, len as i64);
    out.extend_from_slice(b"\r\n");
}

impl Reply {
    /// RESP3 encoding of the reply of a command with this shape.
    pub fn encode_as(&self, out: &mut Vec<u8>, shape: &Shape) {
        match (shape, self) {
            (Shape::Map, _) => self.encode3(out, true),
            (Shape::Set, Reply::Array(items)) => {
                array_header(out, b'~', items.len());
                items.iter().for_each(|item| item.encode3(out, false));
            }
            (Shape::Double, Reply::Bulk(text)) => encode_line(out, b',', text),
            (Shape::Double, Reply::Array(items)) => {
                array_header(out, b'*', items.len());
                items
                    .iter()
                    .for_each(|item| item.encode_as(out, &Shape::Double));
            }
            (Shape::LastDouble, Reply::Array(items)) => {
                array_header(out, b'*', items.len());
                for (i, item) in items.iter().enumerate() {
                    if i + 1 == items.len() {
                        item.encode_as(out, &Shape::Double);
                    } else {
                        item.encode3(out, false);
                    }
                }
            }
            (Shape::ScorePairs | Shape::Pairs, Reply::Array(items)) if items.len() % 2 == 0 => {
                array_header(out, b'*', items.len() / 2);
                for pair in items.chunks(2) {
                    array_header(out, b'*', 2);
                    pair[0].encode3(out, false);
                    if *shape == Shape::ScorePairs {
                        pair[1].encode_as(out, &Shape::Double);
                    } else {
                        pair[1].encode3(out, false);
                    }
                }
            }
            (Shape::PopScores, Reply::Array(items)) => {
                array_header(out, b'*', items.len());
                for item in items {
                    match item {
                        Reply::Array(pairs) => {
                            array_header(out, b'*', pairs.len());
                            for pair in pairs {
                                pair.encode_as(out, &Shape::LastDouble);
                            }
                        }
                        key => key.encode3(out, false),
                    }
                }
            }
            (Shape::PairMap, Reply::Array(items)) => {
                array_header(out, b'%', items.len());
                for item in items {
                    match item {
                        Reply::Array(pair) => pair.iter().for_each(|part| part.encode3(out, false)),
                        other => other.encode3(out, false),
                    }
                }
            }
            (Shape::Coords, Reply::Array(items)) => {
                array_header(out, b'*', items.len());
                for item in items {
                    match item {
                        Reply::Array(parts) => {
                            array_header(out, b'*', parts.len());
                            for part in parts {
                                match part {
                                    Reply::Array(_) => part.encode_as(out, &Shape::Double),
                                    other => other.encode3(out, false),
                                }
                            }
                        }
                        other => other.encode3(out, false),
                    }
                }
            }
            (Shape::Each(shapes), Reply::Array(items)) if shapes.len() == items.len() => {
                array_header(out, b'*', items.len());
                for (item, shape) in items.iter().zip(shapes) {
                    item.encode_as(out, shape);
                }
            }
            _ => self.encode3(out, false),
        }
    }

    pub fn err(msg: impl Into<String>) -> Reply {
        Reply::Error(msg.into())
    }

    pub fn bulk(b: impl Into<Vec<u8>>) -> Reply {
        Reply::Bulk(b.into())
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Reply::Error(_))
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Reply::Ok => out.extend_from_slice(b"+OK\r\n"),
            Reply::Simple(s) => encode_line(out, b'+', s.as_bytes()),
            Reply::Error(s) => encode_line(out, b'-', s.as_bytes()),
            Reply::Int(n) => {
                out.push(b':');
                push_int(out, *n);
                out.extend_from_slice(b"\r\n");
            }
            Reply::Bulk(b) => encode_bulk(out, b),
            Reply::Nil => out.extend_from_slice(b"$-1\r\n"),
            Reply::NilArray => out.extend_from_slice(b"*-1\r\n"),
            Reply::Array(items) | Reply::Map(items) => {
                out.push(b'*');
                push_int(out, items.len() as i64);
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(out);
                }
            }
        }
    }

    /// RESP3 encoding: nil is the null type at any depth, and with `as_map`
    /// a flat array of field/value pairs is sent as a map.
    pub fn encode3(&self, out: &mut Vec<u8>, as_map: bool) {
        match self {
            Reply::Nil | Reply::NilArray => out.extend_from_slice(b"_\r\n"),
            Reply::Array(items) | Reply::Map(items) => {
                let as_map = as_map || matches!(self, Reply::Map(_));
                if as_map && items.len() % 2 == 0 {
                    out.push(b'%');
                    push_int(out, (items.len() / 2) as i64);
                } else {
                    out.push(b'*');
                    push_int(out, items.len() as i64);
                }
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode3(out, false);
                }
            }
            other => other.encode(out),
        }
    }
}

/// `Reply::Bulk(bytes)` as it goes out, the same in RESP2 and RESP3, for a
/// caller that has the bytes but not a `Vec` of its own.
pub fn encode_bulk(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(b'$');
    push_int(out, bytes.len() as i64);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
}

/// A `*<n>\r\n` or `$<n>\r\n` line: `n` and what follows it.
fn header(buf: &[u8], kind: u8) -> Option<(usize, &[u8])> {
    let (&first, rest) = buf.split_first()?;
    if first != kind {
        return None;
    }
    let end = memchr(b'\r', rest)?;
    let n = usize::try_from(parse_i64(&rest[..end])?).ok()?;
    Some((n, rest[end..].strip_prefix(b"\r\n")?))
}

/// A GET or SET that `read_fast` read in place.
#[derive(Debug, PartialEq, Eq)]
pub struct FastCmd<'a> {
    /// Bytes of the input it takes.
    pub used: usize,
    args: [&'a [u8]; 3],
    argc: usize,
}

impl<'a> FastCmd<'a> {
    /// The name as sent, the key and, for SET, the value.
    pub fn args(&self) -> &[&'a [u8]] {
        &self.args[..self.argc]
    }
}

/// A whole GET, or SET with no options, at the start of `buf`, read in
/// place. Most of a cache's traffic is these two, and this way they skip
/// the parser's vectors and the command table. Anything else, a command
/// the parser would refuse under `limits` included, is `None` and goes the
/// usual way.
pub fn read_fast<'a>(buf: &'a [u8], limits: &Limits) -> Option<FastCmd<'a>> {
    let (argc, mut rest) = header(buf, b'*')?;
    if !(2..=3).contains(&argc) || argc > limits.max_args {
        return None;
    }
    let mut args = [&[][..]; 3];
    for arg in &mut args[..argc] {
        let (len, after) = header(rest, b'$')?;
        if len > limits.max_bulk {
            return None;
        }
        rest = after.get(len..)?.strip_prefix(b"\r\n")?;
        *arg = &after[..len];
    }
    let used = buf.len() - rest.len();
    let name: &[u8] = if argc == 2 { b"GET" } else { b"SET" };
    (args[0].eq_ignore_ascii_case(name) && used <= limits.max_request).then_some(FastCmd {
        used,
        args,
        argc,
    })
}

/// The second argument, the key of most commands, of the complete commands
/// at the start of `buf`, at most as many commands as `keys` holds. A
/// command with fewer arguments has none. Returns how many commands were
/// looked at and how many keys were found.
///
/// Only for reading ahead, to know which keys are coming: it stops at
/// anything but a well-formed array of bulk strings and checks no limits,
/// which the parser does when the command's turn comes.
pub fn peek_keys<'a>(mut buf: &'a [u8], keys: &mut [&'a [u8]]) -> (usize, usize) {
    let (mut seen, mut found) = (0, 0);
    while seen < keys.len() {
        let Some((argc, mut rest)) = header(buf, b'*') else {
            break;
        };
        let mut key = None;
        for i in 0..argc {
            let Some((len, after)) = header(rest, b'$') else {
                return (seen, found);
            };
            let Some(next) = after.get(len..).and_then(|tail| tail.strip_prefix(b"\r\n")) else {
                return (seen, found);
            };
            if i == 1 {
                key = Some(&after[..len]);
            }
            rest = next;
        }
        if let Some(key) = key {
            keys[found] = key;
            found += 1;
        }
        seen += 1;
        buf = rest;
    }
    (seen, found)
}

/// A status or error line must stay one line: a CR or LF coming from user
/// data (a Lua error message, a key echoed in an error) would otherwise let
/// the sender forge extra replies.
fn encode_line(out: &mut Vec<u8>, kind: u8, text: &[u8]) {
    out.push(kind);
    out.extend(
        text.iter()
            .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
    );
    out.extend_from_slice(b"\r\n");
}

fn push_int(out: &mut Vec<u8>, n: i64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut v = n.unsigned_abs();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if n < 0 {
        out.push(b'-');
    }
    out.extend_from_slice(&buf[i..]);
}

/// Encode a command the way a client sends it (array of bulk strings).
pub fn encode_command<A: AsRef<[u8]>>(args: &[A], out: &mut Vec<u8>) {
    out.push(b'*');
    push_int(out, args.len() as i64);
    out.extend_from_slice(b"\r\n");
    for a in args {
        let a = a.as_ref();
        out.push(b'$');
        push_int(out, a.len() as i64);
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

/// Strict decimal i64, as Redis parses it: optional '-', digits only, no
/// '+', no spaces, no empty string, no leading zeros, no "-0", no overflow.
pub fn parse_i64(s: &[u8]) -> Option<i64> {
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, s),
    };
    if digits.is_empty() || digits.len() > 19 {
        return None;
    }
    if digits[0] == b'0' && (neg || digits.len() > 1) {
        return None;
    }
    let mut v: u64 = 0;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (d - b'0') as u64;
    }
    if neg {
        if v > i64::MAX as u64 + 1 {
            return None;
        }
        Some((v as i64).wrapping_neg())
    } else {
        i64::try_from(v).ok()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum number of arguments in one command.
    pub max_args: usize,
    /// Maximum size of one bulk argument.
    pub max_bulk: usize,
    /// Maximum length of an inline command line.
    pub max_inline: usize,
    /// Maximum total size of one command on the wire.
    pub max_request: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_args: 1024 * 1024,
            max_bulk: 64 << 20,
            max_inline: 64 * 1024,
            max_request: 256 << 20,
        }
    }
}

/// Header lines ("*<n>\r\n", "$<n>\r\n") never need more than this.
const MAX_HEADER: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtoError(pub String);

/// Incremental request parser. It consumes only complete elements (a header
/// line, a whole bulk), so a caller that feeds the unconsumed tail again
/// after more bytes arrive never re-scans arguments it already parsed, and
/// nothing is allocated from a declared length before the bytes exist.
#[derive(Debug)]
pub struct Parser {
    limits: Limits,
    args: Vec<Vec<u8>>,
    /// Arguments still missing from the multibulk command being read.
    remaining: usize,
    /// Bulk length read from a "$<n>" header whose payload is not complete.
    pending_bulk: Option<usize>,
    /// Bytes consumed so far for the command being read.
    request_bytes: usize,
}

impl Parser {
    pub fn new(limits: Limits) -> Self {
        Parser {
            limits,
            args: Vec::new(),
            remaining: 0,
            pending_bulk: None,
            request_bytes: 0,
        }
    }

    /// The limits for the commands read from now on.
    pub fn set_limits(&mut self, limits: Limits) {
        self.limits = limits;
    }

    /// True when a command has been started but not finished.
    pub fn mid_command(&self) -> bool {
        self.remaining > 0
    }

    /// Parse from the start of `buf`. Returns how many bytes were consumed
    /// and, when a whole command is available, its arguments. `(n, None)`
    /// means more bytes are needed; the caller keeps `buf[n..]`.
    pub fn feed(&mut self, buf: &[u8]) -> Result<(usize, Option<Args>), ProtoError> {
        let mut pos = 0;
        loop {
            if self.remaining == 0 {
                // Between commands.
                let rest = &buf[pos..];
                let Some(&first) = rest.first() else {
                    return Ok((pos, None));
                };
                if first == b'*' {
                    let Some((line, used)) = header_line(rest, "multibulk length")? else {
                        return Ok((pos, None));
                    };
                    let n = parse_i64(line).ok_or_else(|| proto("invalid multibulk length"))?;
                    pos += used;
                    if n <= 0 {
                        // "*0" and "*-1" are empty commands; skip them.
                        continue;
                    }
                    if n as u64 > self.limits.max_args as u64 {
                        return Err(proto("invalid multibulk length"));
                    }
                    self.remaining = n as usize;
                    self.request_bytes = used;
                    self.args = Vec::with_capacity((n as usize).min(16));
                } else {
                    // The limit covers the line's content, not its CRLF.
                    let max = self.limits.max_inline;
                    let window = &rest[..rest.len().min(max.saturating_add(2))];
                    let Some(end) = memchr(b'\n', window) else {
                        // More than `max` bytes are only fine as `max` + CR.
                        if rest.len() > max && rest.get(max..) != Some(b"\r") {
                            return Err(proto("too big inline request"));
                        }
                        return Ok((pos, None));
                    };
                    let line = rest[..end].strip_suffix(b"\r").unwrap_or(&rest[..end]);
                    if line.len() > max {
                        return Err(proto("too big inline request"));
                    }
                    pos += end + 1;
                    let args =
                        split_inline(line).ok_or_else(|| proto("unbalanced quotes in request"))?;
                    if args.is_empty() {
                        continue;
                    }
                    return Ok((pos, Some(args)));
                }
            }

            // Inside a multibulk command.
            let len = match self.pending_bulk {
                Some(len) => len,
                None => {
                    let rest = &buf[pos..];
                    let Some(&first) = rest.first() else {
                        return Ok((pos, None));
                    };
                    if first != b'$' {
                        return Err(proto(format!("expected '$', got '{}'", printable(first))));
                    }
                    let Some((line, used)) = header_line(rest, "bulk length")? else {
                        return Ok((pos, None));
                    };
                    let n = parse_i64(line).ok_or_else(|| proto("invalid bulk length"))?;
                    if n < 0 || n as u64 > self.limits.max_bulk as u64 {
                        return Err(proto("invalid bulk length"));
                    }
                    let len = n as usize;
                    pos += used;
                    self.request_bytes += used;
                    if self.request_bytes.saturating_add(len).saturating_add(2)
                        > self.limits.max_request
                    {
                        return Err(proto("request too large"));
                    }
                    self.pending_bulk = Some(len);
                    len
                }
            };
            let rest = &buf[pos..];
            if rest.len() < len + 2 {
                return Ok((pos, None));
            }
            if &rest[len..len + 2] != b"\r\n" {
                return Err(proto("bulk string not terminated by CRLF"));
            }
            self.args.push(rest[..len].to_vec());
            pos += len + 2;
            self.request_bytes += len + 2;
            self.pending_bulk = None;
            self.remaining -= 1;
            if self.remaining == 0 {
                self.request_bytes = 0;
                return Ok((pos, Some(std::mem::take(&mut self.args))));
            }
        }
    }
}

/// What C's `isspace` accepts.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Split an inline command the way Redis does (`sdssplitargs`): words
/// separated by blanks, "double quotes" with backslash escapes, 'single
/// quotes' with only \' escaped. `None` on unbalanced quotes or a closing
/// quote glued to the next word.
fn split_inline(line: &[u8]) -> Option<Args> {
    let mut args = Vec::new();
    let mut i = 0;
    loop {
        while line.get(i).is_some_and(|&b| is_space(b)) {
            i += 1;
        }
        if i == line.len() {
            return Some(args);
        }
        let mut arg = Vec::new();
        while let Some(&b) = line.get(i) {
            if is_space(b) {
                break;
            }
            i += 1;
            match b {
                b'"' => {
                    i = double_quoted(line, i, &mut arg)?;
                    break;
                }
                b'\'' => {
                    i = single_quoted(line, i, &mut arg)?;
                    break;
                }
                other => arg.push(other),
            }
        }
        args.push(arg);
    }
}

/// Read up to the closing double quote; `i` is just past the opening one.
/// Returns the position after the closing quote.
fn double_quoted(line: &[u8], mut i: usize, arg: &mut Vec<u8>) -> Option<usize> {
    let hex = |b: &u8| (*b as char).to_digit(16).map(|d| d as u8);
    loop {
        match *line.get(i)? {
            b'\\' => {
                let escaped = *line.get(i + 1)?;
                let byte = line
                    .get(i + 2)
                    .and_then(hex)
                    .zip(line.get(i + 3).and_then(hex));
                match (escaped, byte) {
                    (b'x', Some((high, low))) => {
                        arg.push(high * 16 + low);
                        i += 2;
                    }
                    (b'n', _) => arg.push(b'\n'),
                    (b'r', _) => arg.push(b'\r'),
                    (b't', _) => arg.push(b'\t'),
                    (b'b', _) => arg.push(0x08),
                    (b'a', _) => arg.push(0x07),
                    (other, _) => arg.push(other),
                }
                i += 2;
            }
            b'"' => return after_closing_quote(line, i + 1),
            other => {
                arg.push(other);
                i += 1;
            }
        }
    }
}

fn single_quoted(line: &[u8], mut i: usize, arg: &mut Vec<u8>) -> Option<usize> {
    loop {
        match *line.get(i)? {
            b'\\' if line.get(i + 1) == Some(&b'\'') => {
                arg.push(b'\'');
                i += 2;
            }
            b'\'' => return after_closing_quote(line, i + 1),
            other => {
                arg.push(other);
                i += 1;
            }
        }
    }
}

/// A closing quote must be followed by a blank or the end of the line.
fn after_closing_quote(line: &[u8], i: usize) -> Option<usize> {
    line.get(i).is_none_or(|&b| is_space(b)).then_some(i)
}

fn proto(msg: impl Into<String>) -> ProtoError {
    ProtoError(format!("Protocol error: {}", msg.into()))
}

fn printable(b: u8) -> char {
    if b.is_ascii_graphic() {
        b as char
    } else {
        '?'
    }
}

/// Read a "<type><digits>\r\n" header. Returns the digits and the bytes
/// used, `None` when the line is not complete yet.
fn header_line<'a>(buf: &'a [u8], what: &str) -> Result<Option<(&'a [u8], usize)>, ProtoError> {
    let window = &buf[..buf.len().min(MAX_HEADER)];
    match memchr(b'\n', window) {
        Some(nl) => {
            if nl < 2 || buf[nl - 1] != b'\r' {
                return Err(proto(format!("invalid {what}")));
            }
            Ok(Some((&buf[1..nl - 1], nl + 1)))
        }
        None if buf.len() >= MAX_HEADER => Err(proto(format!("too big {what} string"))),
        None => Ok(None),
    }
}

const MAX_REPLY_DEPTH: usize = 32;

/// Decode one reply from the start of `buf`. `Ok(None)` means incomplete.
pub fn decode_reply(buf: &[u8]) -> Result<Option<(Reply, usize)>, String> {
    decode_at(buf, 0)
}

fn decode_at(buf: &[u8], depth: usize) -> Result<Option<(Reply, usize)>, String> {
    if depth > MAX_REPLY_DEPTH {
        return Err("reply nested too deeply".into());
    }
    let Some(&kind) = buf.first() else {
        return Ok(None);
    };
    let Some(nl) = memchr(b'\n', buf) else {
        return Ok(None);
    };
    if nl < 2 || buf[nl - 1] != b'\r' {
        return Err("reply line not terminated by CRLF".into());
    }
    let line = &buf[1..nl - 1];
    let used = nl + 1;
    let text = || String::from_utf8_lossy(line).into_owned();
    match kind {
        b'+' => Ok(Some((
            if line == b"OK" {
                Reply::Ok
            } else {
                Reply::Simple(text())
            },
            used,
        ))),
        b'-' => Ok(Some((Reply::Error(text()), used))),
        b':' => {
            let n = parse_i64(line).ok_or("bad integer reply")?;
            Ok(Some((Reply::Int(n), used)))
        }
        b'$' => {
            let n = parse_i64(line).ok_or("bad bulk length")?;
            if n < 0 {
                return Ok(Some((Reply::Nil, used)));
            }
            let len = n as usize;
            let rest = &buf[used..];
            if rest.len() < len.saturating_add(2) {
                return Ok(None);
            }
            if &rest[len..len + 2] != b"\r\n" {
                return Err("bulk reply not terminated by CRLF".into());
            }
            Ok(Some((Reply::Bulk(rest[..len].to_vec()), used + len + 2)))
        }
        b'*' => {
            let n = parse_i64(line).ok_or("bad array length")?;
            if n < 0 {
                return Ok(Some((Reply::NilArray, used)));
            }
            let mut items = Vec::with_capacity((n as usize).min(1024));
            let mut pos = used;
            for _ in 0..n {
                match decode_at(&buf[pos..], depth + 1)? {
                    Some((item, u)) => {
                        items.push(item);
                        pos += u;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((Reply::Array(items), pos)))
        }
        other => Err(format!("unknown reply type byte {other:#04x}")),
    }
}
