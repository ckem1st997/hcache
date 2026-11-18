//! A JSON value as valkey-json keeps it, with its parser and writers. They
//! follow the RapidJSON fork valkey-json is built on: what it accepts, how
//! it fails, how it nests, and how it prints numbers back.

use std::fmt::Write as _;

/// The deepest nesting Hotstash parses or builds whatever
/// `json.max-path-limit` says: much deeper would risk the stack, as the
/// writers and the path walks recurse.
pub const MAX_NESTING: usize = 1024;
/// The longest reply JSON.GET builds, as `proto-max-bulk-len`: a pretty
/// print stops growing past it, and the command refuses what it built.
pub const MAX_TEXT: usize = 512 << 20;

#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Null,
    Bool(bool),
    Int(i64),
    /// A number with a fraction or an exponent, or too big for an `i64`,
    /// kept as it was written: valkey-json prints it back so.
    Num(Box<str>),
    Str(Box<[u8]>),
    Arr(Vec<Node>),
    /// Members in the order they were added; names are unique.
    Obj(Vec<(Box<[u8]>, Node)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    Syntax,
    /// Nested deeper than the limit.
    Depth,
}

impl Node {
    pub fn is_container(&self) -> bool {
        matches!(self, Node::Arr(_) | Node::Obj(_))
    }

    /// The value of a number, as a double.
    pub fn number(&self) -> Option<f64> {
        match self {
            Node::Int(n) => Some(*n as f64),
            Node::Num(text) => Some(num_value(text)),
            _ => None,
        }
    }

    pub fn member(&self, name: &[u8]) -> Option<&Node> {
        match self {
            Node::Obj(members) => members.iter().find(|(k, _)| **k == *name).map(|(_, v)| v),
            _ => None,
        }
    }

    /// How deep containers nest in it: 0 for a scalar, 1 for `[]`.
    pub fn depth(&self) -> usize {
        match self {
            Node::Arr(items) => 1 + items.iter().map(Node::depth).max().unwrap_or(0),
            Node::Obj(members) => 1 + members.iter().map(|(_, v)| v.depth()).max().unwrap_or(0),
            _ => 0,
        }
    }

    /// About how many bytes it takes: the node, then what it owns.
    pub fn size(&self) -> u64 {
        const NODE: u64 = std::mem::size_of::<Node>() as u64;
        const NAME: u64 = std::mem::size_of::<Box<[u8]>>() as u64;
        NODE + match self {
            Node::Num(text) => text.len() as u64,
            Node::Str(s) => s.len() as u64,
            Node::Arr(items) => items.iter().map(Node::size).sum(),
            Node::Obj(members) => members
                .iter()
                .map(|(k, v)| NAME + k.len() as u64 + v.size())
                .sum(),
            _ => 0,
        }
    }

    /// Whether two values are equal as RapidJSON compares them: numbers by
    /// value, objects whatever the order of their members.
    pub fn same(&self, other: &Node) -> bool {
        match (self, other) {
            (Node::Null, Node::Null) => true,
            (Node::Bool(a), Node::Bool(b)) => a == b,
            (Node::Int(a), Node::Int(b)) => a == b,
            (Node::Int(_) | Node::Num(_), Node::Int(_) | Node::Num(_)) => {
                let (a, b) = (self.number(), other.number());
                a == b
            }
            (Node::Str(a), Node::Str(b)) => a == b,
            (Node::Arr(a), Node::Arr(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.same(y))
            }
            (Node::Obj(a), Node::Obj(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| other.member(k).is_some_and(|w| v.same(w)))
            }
            _ => false,
        }
    }
}

/// The value of a number's text, which the parser checked.
pub fn num_value(text: &str) -> f64 {
    text.parse().unwrap_or(f64::NAN)
}

/// Parse a whole JSON text nested at most `limit` deep: the value and how
/// deep it nests. As RapidJSON reads a C string, a NUL byte ends the text.
pub fn parse(text: &[u8], limit: usize) -> Result<(Node, usize), ParseError> {
    let mut p = Parser { s: text, i: 0 };
    p.skip_ws();
    if p.peek() == 0 {
        return Err(ParseError::Syntax);
    }
    let limit = limit.min(MAX_NESTING);
    // The containers being filled, innermost last; an object's with the
    // name of the member being read.
    let mut open: Vec<Open> = Vec::new();
    let mut deepest = 0;
    loop {
        let mut value = match p.peek() {
            b'[' | b'{' => {
                let array = p.peek() == b'[';
                p.i += 1;
                if open.len() + 1 > limit {
                    return Err(ParseError::Depth);
                }
                deepest = deepest.max(open.len() + 1);
                p.skip_ws();
                if array {
                    if p.peek() == b']' {
                        p.i += 1;
                        Node::Arr(Vec::new())
                    } else {
                        open.push(Open::Arr(Vec::new()));
                        continue;
                    }
                } else if p.peek() == b'}' {
                    p.i += 1;
                    Node::Obj(Vec::new())
                } else {
                    let name = p.name()?;
                    open.push(Open::Obj(Vec::new(), name));
                    continue;
                }
            }
            b'n' => p.literal(b"null", Node::Null)?,
            b't' => p.literal(b"true", Node::Bool(true))?,
            b'f' => p.literal(b"false", Node::Bool(false))?,
            b'"' => Node::Str(p.string()?.into()),
            _ => p.number()?,
        };
        // Put the value where it goes, closing what it ends.
        loop {
            p.skip_ws();
            match open.last_mut() {
                None => {
                    return if p.peek() == 0 {
                        Ok((value, deepest))
                    } else {
                        Err(ParseError::Syntax)
                    };
                }
                Some(Open::Arr(items)) => {
                    items.push(value);
                    match p.peek() {
                        b',' => {
                            p.i += 1;
                            p.skip_ws();
                            break;
                        }
                        b']' => {
                            p.i += 1;
                            let Some(Open::Arr(items)) = open.pop() else {
                                unreachable!("the last container is an array");
                            };
                            value = Node::Arr(items);
                        }
                        _ => return Err(ParseError::Syntax),
                    }
                }
                Some(Open::Obj(members, name)) => {
                    members.push((std::mem::take(name).into(), value));
                    match p.peek() {
                        b',' => {
                            p.i += 1;
                            p.skip_ws();
                            *name = p.name()?;
                            break;
                        }
                        b'}' => {
                            p.i += 1;
                            let Some(Open::Obj(members, _)) = open.pop() else {
                                unreachable!("the last container is an object");
                            };
                            value = Node::Obj(unique(members));
                        }
                        _ => return Err(ParseError::Syntax),
                    }
                }
            }
        }
    }
}

enum Open {
    Arr(Vec<Node>),
    Obj(Vec<(Box<[u8]>, Node)>, Vec<u8>),
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn peek(&self) -> u8 {
        self.s.get(self.i).copied().unwrap_or(0)
    }

    fn digit(&self) -> Option<u64> {
        let c = self.peek();
        c.is_ascii_digit().then(|| u64::from(c - b'0'))
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), b' ' | b'\n' | b'\r' | b'\t') {
            self.i += 1;
        }
    }

    fn literal(&mut self, word: &[u8], node: Node) -> Result<Node, ParseError> {
        if self.s[self.i..].starts_with(word) {
            self.i += word.len();
            Ok(node)
        } else {
            Err(ParseError::Syntax)
        }
    }

    /// A member's name, then the colon after it.
    fn name(&mut self) -> Result<Vec<u8>, ParseError> {
        if self.peek() != b'"' {
            return Err(ParseError::Syntax);
        }
        let name = self.string()?;
        self.skip_ws();
        if self.peek() != b':' {
            return Err(ParseError::Syntax);
        }
        self.i += 1;
        self.skip_ws();
        Ok(name)
    }

    /// A string, from its opening quote. Bytes are kept as they are: UTF-8
    /// is not checked.
    fn string(&mut self) -> Result<Vec<u8>, ParseError> {
        self.i += 1;
        let mut out = Vec::new();
        loop {
            let rest = &self.s[self.i.min(self.s.len())..];
            let plain = rest
                .iter()
                .position(|&c| c == b'"' || c == b'\\' || c < 0x20)
                .unwrap_or(rest.len());
            out.extend_from_slice(&rest[..plain]);
            self.i += plain;
            match self.peek() {
                b'"' => {
                    self.i += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.i += 1;
                    let c = self.peek();
                    self.i += 1;
                    match c {
                        b'"' | b'\\' | b'/' => out.push(c),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let mut code = self.hex4()?;
                            if (0xD800..=0xDBFF).contains(&code) {
                                if self.peek() != b'\\' || self.s.get(self.i + 1) != Some(&b'u') {
                                    return Err(ParseError::Syntax);
                                }
                                self.i += 2;
                                let low = self.hex4()?;
                                if !(0xDC00..=0xDFFF).contains(&low) {
                                    return Err(ParseError::Syntax);
                                }
                                code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                            }
                            let c = char::from_u32(code).ok_or(ParseError::Syntax)?;
                            out.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
                        }
                        _ => return Err(ParseError::Syntax),
                    }
                }
                // A control byte, a NUL, or the end of the text.
                _ => return Err(ParseError::Syntax),
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, ParseError> {
        let mut code = 0;
        for _ in 0..4 {
            let digit = (self.peek() as char)
                .to_digit(16)
                .ok_or(ParseError::Syntax)?;
            code = code << 4 | digit;
            self.i += 1;
        }
        Ok(code)
    }

    /// A number as RapidJSON reads one: an integer while it fits an `i64`,
    /// otherwise a double, kept as text. Only the exponent can make it too
    /// big.
    fn number(&mut self) -> Result<Node, ParseError> {
        const SWITCH: u64 = 0x0CCC_CCCC_CCCC_CCCC;
        const FAST: u64 = (1 << 53) - 1;
        let start = self.i;
        let minus = self.peek() == b'-';
        if minus {
            self.i += 1;
        }
        let mut int = 0u64;
        // Significant digits after the first, as RapidJSON counts them.
        let mut digits = 0;
        let mut double = false;
        match self.peek() {
            b'0' => self.i += 1,
            b'1'..=b'9' => {
                int = u64::from(self.peek() - b'0');
                self.i += 1;
                let last = if minus { b'8' } else { b'7' };
                while let Some(d) = self.digit() {
                    if int >= SWITCH && (int != SWITCH || self.peek() > last) {
                        double = true;
                        break;
                    }
                    int = int * 10 + d;
                    self.i += 1;
                    digits += 1;
                }
            }
            _ => return Err(ParseError::Syntax),
        }
        if double {
            while self.digit().is_some() {
                self.i += 1;
            }
        }
        let mut exp_frac: i64 = 0;
        if self.peek() == b'.' {
            self.i += 1;
            self.digit().ok_or(ParseError::Syntax)?;
            if !double {
                while let Some(d) = self.digit() {
                    if int > FAST {
                        break;
                    }
                    int = int * 10 + d;
                    self.i += 1;
                    exp_frac -= 1;
                    if int != 0 {
                        digits += 1;
                    }
                }
                double = true;
            }
            while self.digit().is_some() {
                self.i += 1;
                if digits < 17 {
                    exp_frac -= 1;
                    digits += 1;
                }
            }
        }
        if matches!(self.peek(), b'e' | b'E') {
            double = true;
            self.i += 1;
            let minus_exp = match self.peek() {
                b'+' => {
                    self.i += 1;
                    false
                }
                b'-' => {
                    self.i += 1;
                    true
                }
                _ => false,
            };
            let first = self.digit().ok_or(ParseError::Syntax)?;
            self.i += 1;
            let max = 308 - exp_frac;
            let mut exp = first as i64;
            while let Some(d) = self.digit() {
                self.i += 1;
                if !minus_exp {
                    exp = exp * 10 + d as i64;
                    if exp > max {
                        return Err(ParseError::Syntax);
                    }
                }
            }
        }
        Ok(if double {
            let text: Box<str> = self.s[start..self.i].iter().map(|&c| c as char).collect();
            if num_value(&text).is_infinite() {
                return Err(ParseError::Syntax);
            }
            Node::Num(text)
        } else if minus {
            Node::Int((!int).wrapping_add(1) as i64)
        } else {
            Node::Int(int as i64)
        })
    }
}

/// An object's members with each name once: the last value given for a
/// name, where the name first came.
fn unique(mut members: Vec<(Box<[u8]>, Node)>) -> Vec<(Box<[u8]>, Node)> {
    const PAIRWISE: usize = 16;
    let n = members.len();
    // For each member: the one whose value it takes, or that it is dropped.
    let mut takes: Vec<Option<usize>> = vec![None; n];
    let mut dropped = vec![false; n];
    let mut any = false;
    if n <= PAIRWISE {
        for i in 0..n {
            if dropped[i] {
                continue;
            }
            for j in i + 1..n {
                if members[i].0 == members[j].0 {
                    dropped[j] = true;
                    takes[i] = Some(j);
                    any = true;
                }
            }
        }
    } else {
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&a, &b| members[a].0.cmp(&members[b].0));
        for run in order.chunk_by(|&a, &b| members[a].0 == members[b].0) {
            if let [first, .., last] = run {
                takes[*first] = Some(*last);
                for &i in &run[1..] {
                    dropped[i] = true;
                }
                any = true;
            }
        }
    }
    if !any {
        return members;
    }
    for i in 0..n {
        if let Some(j) = takes[i] {
            members[i].1 = std::mem::replace(&mut members[j].1, Node::Null);
        }
    }
    members
        .into_iter()
        .zip(dropped)
        .filter_map(|(m, gone)| (!gone).then_some(m))
        .collect()
}

/// How a value is printed: compact when all three are empty.
#[derive(Debug, Clone, Copy, Default)]
pub struct Style<'a> {
    pub indent: &'a [u8],
    pub newline: &'a [u8],
    pub space: &'a [u8],
}

impl Style<'_> {
    fn compact(&self) -> bool {
        self.indent.is_empty() && self.newline.is_empty() && self.space.is_empty()
    }

    pub fn indent(&self, out: &mut Vec<u8>, level: usize) {
        for _ in 0..level {
            if out.len() > MAX_TEXT {
                return;
            }
            out.extend_from_slice(self.indent);
        }
    }
}

/// Append `node` as JSON, as if it began `level` deep.
pub fn write(out: &mut Vec<u8>, node: &Node, style: &Style, level: usize) {
    if style.compact() {
        write_compact(out, node);
    } else {
        write_pretty(out, node, style, level);
    }
}

pub fn to_json(node: &Node) -> Vec<u8> {
    let mut out = Vec::new();
    write_compact(&mut out, node);
    out
}

fn write_compact(out: &mut Vec<u8>, node: &Node) {
    match node {
        Node::Arr(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_compact(out, item);
            }
            out.push(b']');
        }
        Node::Obj(members) => {
            out.push(b'{');
            for (i, (name, value)) in members.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_str(out, name);
                out.push(b':');
                write_compact(out, value);
            }
            out.push(b'}');
        }
        scalar => write_scalar(out, scalar),
    }
}

fn write_pretty(out: &mut Vec<u8>, node: &Node, style: &Style, level: usize) {
    match node {
        Node::Arr(items) if !items.is_empty() => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if out.len() > MAX_TEXT {
                    return;
                }
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(style.newline);
                style.indent(out, level + 1);
                write_pretty(out, item, style, level + 1);
            }
            out.extend_from_slice(style.newline);
            style.indent(out, level);
            out.push(b']');
        }
        Node::Obj(members) if !members.is_empty() => {
            out.push(b'{');
            for (i, (name, value)) in members.iter().enumerate() {
                if out.len() > MAX_TEXT {
                    return;
                }
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(style.newline);
                style.indent(out, level + 1);
                write_str(out, name);
                out.push(b':');
                out.extend_from_slice(style.space);
                write_pretty(out, value, style, level + 1);
            }
            out.extend_from_slice(style.newline);
            style.indent(out, level);
            out.push(b'}');
        }
        other => write_compact(out, other),
    }
}

fn write_scalar(out: &mut Vec<u8>, node: &Node) {
    match node {
        Node::Null => out.extend_from_slice(b"null"),
        Node::Bool(true) => out.extend_from_slice(b"true"),
        Node::Bool(false) => out.extend_from_slice(b"false"),
        Node::Int(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Node::Num(text) => out.extend_from_slice(text.as_bytes()),
        Node::Str(s) => write_str(out, s),
        Node::Arr(_) | Node::Obj(_) => write_compact(out, node),
    }
}

/// Append a JSON string: quotes, backslashes and control bytes escaped,
/// everything else as it is.
pub fn write_str(out: &mut Vec<u8>, s: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push(b'"');
    for &c in s {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            8 => out.extend_from_slice(b"\\b"),
            12 => out.extend_from_slice(b"\\f"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            c if c < 0x20 => {
                out.extend_from_slice(b"\\u00");
                out.push(HEX[usize::from(c >> 4)]);
                out.push(HEX[usize::from(c & 15)]);
            }
            c => out.push(c),
        }
    }
    out.push(b'"');
}

/// A double as C's `%.17g` prints it.
pub fn g17(x: f64) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0" } else { "0" }.into();
    }
    if x.is_nan() {
        return "nan".into();
    }
    if x.is_infinite() {
        return if x < 0.0 { "-inf" } else { "inf" }.into();
    }
    let sci = format!("{x:.16e}");
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    if !(-4..17).contains(&exp) {
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{}e{sign}{:02}", trim_zeros(mantissa), exp.unsigned_abs())
    } else {
        trim_zeros(&format!("{x:.*}", (16 - exp) as usize)).to_string()
    }
}

fn trim_zeros(s: &str) -> &str {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.')
    } else {
        s
    }
}

/// A double as RapidJSON's writer prints it: the shortest digits that read
/// back the same, in fixed notation from 1e-6 up to 1e21.
pub fn dtoa(x: f64) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    if x.is_nan() {
        return "NaN".into();
    }
    if x.is_infinite() {
        return if x < 0.0 { "-Infinity" } else { "Infinity" }.into();
    }
    let mut out = String::new();
    if x < 0.0 {
        out.push('-');
    }
    let sci = format!("{:e}", x.abs());
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let len = digits.len() as i32;
    let kk = exp.parse::<i32>().unwrap_or(0) + 1;
    let k = kk - len;
    if 0 <= k && kk <= 21 {
        out.push_str(&digits);
        out.extend(std::iter::repeat_n('0', k as usize));
        out.push_str(".0");
    } else if 0 < kk && kk <= 21 {
        out.push_str(&digits[..kk as usize]);
        out.push('.');
        out.push_str(&digits[kk as usize..]);
    } else if -6 < kk && kk <= 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-kk) as usize));
        out.push_str(&digits);
    } else {
        out.push_str(&digits[..1]);
        if len > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let _ = write!(out, "e{}", kk - 1);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(text: &str) -> String {
        let (node, _) = parse(text.as_bytes(), 128).expect(text);
        String::from_utf8(to_json(&node)).unwrap()
    }

    fn bad(text: &[u8]) -> ParseError {
        parse(text, 128).expect_err(&String::from_utf8_lossy(text))
    }

    #[test]
    fn parses_and_prints_back() {
        assert_eq!(
            json(" {\"a\" : [1, 2.50, -0, true, null, \"x\"]} "),
            r#"{"a":[1,2.50,0,true,null,"x"]}"#
        );
    }

    #[test]
    fn numbers_switch_to_double_past_i64() {
        assert!(matches!(
            parse(b"9223372036854775807", 9).unwrap().0,
            Node::Int(i64::MAX)
        ));
        assert!(matches!(
            parse(b"-9223372036854775808", 9).unwrap().0,
            Node::Int(i64::MIN)
        ));
        assert!(matches!(
            parse(b"9223372036854775808", 9).unwrap().0,
            Node::Num(_)
        ));
        assert!(matches!(parse(b"-0", 9).unwrap().0, Node::Int(0)));
        assert_eq!(
            json("123456789012345678901234567890"),
            "123456789012345678901234567890"
        );
    }

    #[test]
    fn exponents_past_308_are_too_big() {
        assert_eq!(bad(b"1e309"), ParseError::Syntax);
        assert_eq!(json("1e308"), "1e308");
        assert_eq!(json("0.1e309"), "0.1e309");
        assert_eq!(json("1e-99999"), "1e-99999");
        assert_eq!(bad(b"1."), ParseError::Syntax);
        assert_eq!(bad(b"1e"), ParseError::Syntax);
        assert_eq!(bad(b"01"), ParseError::Syntax);
        assert_eq!(bad(b"+1"), ParseError::Syntax);
    }

    #[test]
    fn strings_unescape_and_escape() {
        assert_eq!(json(r#""a\/bé😀\n\u0001""#), "\"a/bé😀\\n\\u0001\"");
        assert_eq!(bad(br#""\ud83d""#), ParseError::Syntax);
        assert_eq!(bad(br#""\ude00""#), ParseError::Syntax);
        assert_eq!(bad(b"\"a\tb\""), ParseError::Syntax);
        assert_eq!(bad(br#""\x""#), ParseError::Syntax);
        assert_eq!(bad(b"\"abc"), ParseError::Syntax);
    }

    #[test]
    fn a_nul_ends_the_text() {
        assert!(matches!(parse(b"1\0xyz", 9).unwrap().0, Node::Int(1)));
        assert_eq!(bad(b"\0"), ParseError::Syntax);
        assert_eq!(bad(b"  "), ParseError::Syntax);
        assert_eq!(bad(b"1 2"), ParseError::Syntax);
        assert_eq!(bad(b"[1,]"), ParseError::Syntax);
        assert_eq!(bad(b"{\"a\":1,}"), ParseError::Syntax);
        assert_eq!(bad(b"nul"), ParseError::Syntax);
    }

    #[test]
    fn depth_is_counted_and_limited() {
        assert_eq!(parse(b"1", 0).unwrap().1, 0);
        assert_eq!(parse(b"[]", 1).unwrap().1, 1);
        assert_eq!(parse(b"[[1],{\"a\":[]}]", 3).unwrap().1, 3);
        assert_eq!(parse(b"[[]]", 1).unwrap_err(), ParseError::Depth);
        let deep = format!("{}{}", "[".repeat(MAX_NESTING), "]".repeat(MAX_NESTING));
        let (node, depth) = parse(deep.as_bytes(), usize::MAX).unwrap();
        assert_eq!(depth, MAX_NESTING);
        assert_eq!(node.depth(), MAX_NESTING);
        assert_eq!(to_json(&node), deep.as_bytes());
        let deeper = format!("[{deep}]");
        assert_eq!(
            parse(deeper.as_bytes(), usize::MAX).unwrap_err(),
            ParseError::Depth
        );
    }

    #[test]
    fn duplicate_names_keep_the_last_value_first() {
        assert_eq!(json(r#"{"a":1,"b":2,"a":3}"#), r#"{"a":3,"b":2}"#);
        let many: Vec<String> = (0..40).map(|i| format!("\"k{}\":{i}", i % 20)).collect();
        let text = format!("{{{}}}", many.join(","));
        let expect: Vec<String> = (0..20).map(|i| format!("\"k{i}\":{}", i + 20)).collect();
        assert_eq!(json(&text), format!("{{{}}}", expect.join(",")));
    }

    #[test]
    fn pretty_printing() {
        let (node, _) = parse(br#"{"a":"1","b":[],"c":[1,{}]}"#, 9).unwrap();
        let mut out = Vec::new();
        let style = Style {
            indent: b"  ",
            newline: b"\n",
            space: b" ",
        };
        write(&mut out, &node, &style, 0);
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\n  \"a\": \"1\",\n  \"b\": [],\n  \"c\": [\n    1,\n    {}\n  ]\n}"
        );
        let mut out = Vec::new();
        let (node, _) = parse(br#"{"a":"1","b":"2","c":"3"}"#, 9).unwrap();
        write(
            &mut out,
            &node,
            &Style {
                indent: b" ",
                ..Style::default()
            },
            0,
        );
        assert_eq!(out, br#"{ "a":"1", "b":"2", "c":"3"}"#);
    }

    #[test]
    fn equality_compares_numbers_by_value() {
        let p = |t: &str| parse(t.as_bytes(), 9).unwrap().0;
        assert!(p("1").same(&p("1.0")));
        assert!(p(r#"{"a":1,"b":[2]}"#).same(&p(r#"{"b":[2.0],"a":1}"#)));
        assert!(!p("true").same(&p("false")));
        assert!(!p("[1]").same(&p("[1,2]")));
    }

    #[test]
    fn g17_matches_printf() {
        for (x, s) in [
            (0.1, "0.10000000000000001"),
            (1.0, "1"),
            (-0.0, "-0"),
            (1e17, "1e+17"),
            (1e16, "10000000000000000"),
            (123.456, "123.456"),
            (1e-5, "1.0000000000000001e-05"),
            (0.0001, "0.0001"),
            (3.5, "3.5"),
            (1.5e300, "1.5000000000000001e+300"),
        ] {
            assert_eq!(g17(x), s);
        }
    }

    #[test]
    fn dtoa_matches_rapidjson() {
        for (x, s) in [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (1.5, "1.5"),
            (1e21, "1e21"),
            (1e20, "100000000000000000000.0"),
            (123e-8, "0.00000123"),
            (1.23e-7, "1.23e-7"),
            (-2.5e30, "-2.5e30"),
            (0.1, "0.1"),
        ] {
            assert_eq!(dtoa(x), s);
        }
    }
}
