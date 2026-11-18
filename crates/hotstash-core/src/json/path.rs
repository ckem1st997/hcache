//! valkey-json's path selector, ported from its `selector.cc`: the legacy
//! paths (`.a.b[0]`) and the JSONPath subset (`$..a[?(@.b>1)]`) it
//! implements, with the same grammar, errors, limits and order of results.
//!
//! A selection only reads the document. It yields the values found with
//! where they are, and for a write the members to add; the command then
//! applies the change by those paths.

use super::node::{self, Node, ParseError, MAX_NESTING};
use super::Error;
use std::cell::Cell;
use std::collections::HashSet;
use std::ptr;
use std::thread::LocalKey;

/// How deep the parser may recurse through filters and fan-outs.
const MAX_RECURSION: usize = 200;
/// How many `..` one path may hold.
const MAX_DESCENTS: usize = 20;
/// The longest path, in bytes.
const MAX_QUERY: usize = 128 * 1024;
/// How many times one selection, nested ones included, may run the rest of
/// a path from a value, on top of four per value in the document. A `..`
/// runs the rest from every value under it, so `..` after `..` multiplies:
/// `$..a..a..a` on a deep document would otherwise run for hours.
/// valkey-json has no such limit.
const MAX_EVALS: usize = 1 << 20;
/// How deep the `..` walks on a thread may go together: one walk goes as
/// deep as the document, a `$..` in a filter starts another from the root.
const MAX_WALK_DEPTH: usize = 2 * MAX_NESTING;

/// One step down a document.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Step<'a> {
    Key(&'a [u8]),
    Index(usize),
}

/// What the selection is for: a write may add a missing last member; a
/// write or a delete fails on a member of a value that is not an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Read,
    Write,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tok {
    End,
    Dollar,
    Dot,
    DotDot,
    Wildcard,
    Colon,
    Comma,
    Question,
    At,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Single,
    Double,
    Plus,
    Minus,
    Space,
    And,
    Or,
    Eq,
    Ne,
    Ge,
    Gt,
    Le,
    Lt,
    Digit,
    Alpha,
    /// Any other byte, those not ASCII included.
    Other,
}

impl Tok {
    fn compares(self) -> bool {
        matches!(
            self,
            Tok::Eq | Tok::Ne | Tok::Lt | Tok::Le | Tok::Gt | Tok::Ge
        )
    }

    /// The operator with its sides swapped: `1 < @` is `@ > 1`.
    fn swapped(self) -> Tok {
        match self {
            Tok::Gt => Tok::Lt,
            Tok::Lt => Tok::Gt,
            Tok::Ge => Tok::Le,
            Tok::Le => Tok::Ge,
            other => other,
        }
    }
}

/// The token read last, and where it starts. At the end of the path the
/// start stays where the token before began, as there.
#[derive(Debug, Clone, Copy)]
struct Token {
    tok: Tok,
    start: usize,
}

#[derive(Debug, Clone, Copy)]
struct Lexer<'a> {
    path: &'a [u8],
    /// Where the next token begins.
    p: usize,
    next: Token,
    /// How many `..` were read.
    descents: usize,
}

impl<'a> Lexer<'a> {
    fn new(path: &'a [u8]) -> Self {
        Lexer {
            path,
            p: 0,
            next: Token {
                tok: Tok::Other,
                start: 0,
            },
            descents: 0,
        }
    }

    fn at(&self, i: usize) -> u8 {
        self.path.get(i).copied().unwrap_or(0)
    }

    fn cur(&self) -> Tok {
        self.next.tok
    }

    /// What the bytes at `p` begin, without reading them.
    fn peek(&self) -> Tok {
        let second = self.at(self.p + 1);
        match self.at(self.p) {
            0 => Tok::End,
            b'$' => Tok::Dollar,
            b'.' if second == b'.' => Tok::DotDot,
            b'.' => Tok::Dot,
            b'*' => Tok::Wildcard,
            b':' => Tok::Colon,
            b',' => Tok::Comma,
            b'?' => Tok::Question,
            b'@' => Tok::At,
            b'[' => Tok::LBracket,
            b']' => Tok::RBracket,
            b'(' => Tok::LParen,
            b')' => Tok::RParen,
            b'\'' => Tok::Single,
            b'"' => Tok::Double,
            b'+' => Tok::Plus,
            b'-' => Tok::Minus,
            b' ' => Tok::Space,
            b'&' if second == b'&' => Tok::And,
            b'|' if second == b'|' => Tok::Or,
            b'=' if second == b'=' => Tok::Eq,
            b'!' if second == b'=' => Tok::Ne,
            b'>' if second == b'=' => Tok::Ge,
            b'>' => Tok::Gt,
            b'<' if second == b'=' => Tok::Le,
            b'<' => Tok::Lt,
            c if c.is_ascii_digit() => Tok::Digit,
            c if c.is_ascii_alphabetic() => Tok::Alpha,
            _ => Tok::Other,
        }
    }

    fn next_token(&mut self, skip_space: bool) {
        let tok = self.peek();
        self.next.tok = tok;
        match tok {
            Tok::End => {}
            Tok::Space if skip_space => {
                self.skip_raw_spaces();
                self.next_token(false);
            }
            Tok::DotDot | Tok::Ne | Tok::Ge | Tok::Le | Tok::Eq | Tok::And | Tok::Or => {
                if tok == Tok::DotDot {
                    self.descents += 1;
                }
                self.next.start = self.p;
                self.p += 2;
            }
            _ => {
                self.next.start = self.p;
                self.p += 1;
            }
        }
    }

    /// Read past the current token if it is `tok`.
    fn eat(&mut self, tok: Tok, skip_space: bool) -> bool {
        if skip_space && self.next.tok == Tok::Space {
            self.skip_raw_spaces();
            self.next_token(false);
            return self.eat(tok, false);
        }
        if self.next.tok == tok {
            self.next_token(skip_space);
            return true;
        }
        false
    }

    fn skip_raw_spaces(&mut self) {
        while self.at(self.p) == b' ' {
            self.p += 1;
        }
    }

    /// Skip a space token, or else the spaces after the current token.
    fn skip_spaces(&mut self) {
        if self.next.tok == Tok::Space {
            self.next_token(true);
        } else {
            self.skip_raw_spaces();
        }
    }

    /// The current token through the bytes after it that `keep` accepts.
    fn run(&mut self, keep: impl Fn(u8) -> bool) -> &'a [u8] {
        let start = self.next.start;
        while self.p < self.path.len() && keep(self.path[self.p]) {
            self.p += 1;
        }
        let end = self.p.max(start + 1).min(self.path.len());
        self.next_token(false);
        &self.path[start.min(end)..end]
    }

    fn integer(&mut self) -> Result<i64, Error> {
        if !matches!(self.cur(), Tok::Digit | Tok::Plus | Tok::Minus) {
            return Err(Error::ValueNotNumber);
        }
        let negative = self.cur() == Tok::Minus;
        if self.cur() != Tok::Digit {
            self.next_token(false);
            if self.cur() != Tok::Digit {
                return Err(Error::ValueNotNumber);
            }
        }
        let limit = if negative {
            1u64 << 63
        } else {
            i64::MAX as u64
        };
        let mut val = u64::from(self.at(self.next.start) - b'0');
        while self.at(self.p).is_ascii_digit() {
            let digit = u64::from(self.at(self.p) - b'0');
            if val > (limit - digit) / 10 {
                return Err(Error::InvalidNumber);
            }
            val = val * 10 + digit;
            self.p += 1;
        }
        self.next_token(false);
        Ok(if negative {
            (val as i64).wrapping_neg()
        } else {
            val as i64
        })
    }

    fn unquoted_name(&mut self) -> Result<&'a [u8], Error> {
        const STOP: &[u8] = b".[]()<>=!'\" |&";
        let first = self.at(self.next.start);
        if first == 0 || STOP.contains(&first) {
            return Err(Error::InvalidMemberName);
        }
        Ok(self.run(|c| !STOP.contains(&c)))
    }

    /// A number in a filter, to parse as JSON.
    fn number(&mut self) -> Result<&'a [u8], Error> {
        const NUMBER: &[u8] = b"+-0123456789.Ee";
        let first = self.at(self.next.start);
        if first != 0 && !NUMBER.contains(&first) {
            return Err(Error::InvalidNumber);
        }
        Ok(self.run(|c| NUMBER.contains(&c)))
    }

    fn identifier(&mut self) -> Result<&'a [u8], Error> {
        if !self.at(self.next.start).is_ascii_alphanumeric() {
            return Err(Error::InvalidIdentifier);
        }
        Ok(self.run(|c| c.is_ascii_alphanumeric()))
    }

    /// A path from `$` in a filter, up to what ends it outside brackets.
    fn path_value(&mut self) -> Result<&'a [u8], Error> {
        const STOP: &[u8] = b"]()<>=!'\" |&";
        let start = self.next.start;
        let mut quote = b'"';
        let mut in_brackets = false;
        let mut in_quotes = false;
        while self.at(self.p) != 0 {
            let c = self.at(self.p);
            if !in_brackets {
                if c == b'[' {
                    in_brackets = true;
                } else if STOP.contains(&c) {
                    break;
                }
            } else if !in_quotes {
                if c == b'"' || c == b'\'' {
                    in_quotes = true;
                    quote = c;
                } else if c == b']' {
                    in_brackets = false;
                } else if !b"-+0123456789".contains(&c) {
                    return Err(Error::InvalidPath);
                }
            } else if c == b'\\' && self.at(self.p + 1) == quote {
                self.p += 1;
            } else if c == quote {
                in_quotes = false;
            }
            self.p += 1;
        }
        let end = self.p;
        self.next_token(false);
        Ok(&self.path[start..end])
    }

    /// Scan from an opening quote to the first `quote` not right after a
    /// backslash: the bytes between.
    fn quoted(&mut self, quote: u8) -> (usize, usize) {
        let start = self.p;
        let mut end = start;
        while self.at(self.p) != 0 {
            let c = self.at(self.p);
            if c == quote && (self.p == start || self.at(self.p - 1) != b'\\') {
                self.p += 1;
                return (start, end);
            }
            self.p += 1;
            end = self.p;
        }
        (start, end)
    }

    /// A double-quoted string, read as JSON.
    fn double_quoted(&mut self) -> Result<Vec<u8>, Error> {
        let open = self.next.start;
        let _ = self.quoted(b'"');
        let text = &self.path[open..self.p];
        let (value, _) = node::parse(text, MAX_NESTING).map_err(parse_error)?;
        self.next_token(false);
        match value {
            Node::Str(s) => Ok(s.into_vec()),
            _ => Err(Error::Parse),
        }
    }

    /// A single-quoted member name, with `\\ \t \b \f \n \r \'` read only
    /// when it holds a backslash.
    fn single_quoted(&mut self) -> Vec<u8> {
        const SECOND: &[u8] = b"\\tbfnr'";
        const MEANS: &[u8] = b"\\\t\x08\x0c\n\r'";
        let (start, end) = self.quoted(b'\'');
        let name = &self.path[start..end];
        let mut out = Vec::with_capacity(name.len());
        if !name.contains(&b'\\') {
            out.extend_from_slice(name);
        } else {
            let mut i = 0;
            while i < name.len() {
                let c = name[i];
                match name
                    .get(i + 1)
                    .and_then(|n| SECOND.iter().position(|s| s == n))
                {
                    Some(at) if c == b'\\' => {
                        out.push(MEANS[at]);
                        i += 1;
                    }
                    _ => out.push(c),
                }
                i += 1;
            }
        }
        self.next_token(false);
        out
    }

    /// A single-quoted value made a double-quoted JSON string: `"` escaped,
    /// a backslash before `'` dropped.
    fn single_as_double(&mut self) -> Vec<u8> {
        let (start, end) = self.quoted(b'\'');
        let mut out = vec![b'"'];
        for i in start..end {
            match self.path[i] {
                b'"' => out.extend_from_slice(b"\\\""),
                b'\\' if self.at(i + 1) == b'\'' => {}
                c => out.push(c),
            }
        }
        out.push(b'"');
        self.next_token(false);
        out
    }
}

pub(super) fn parse_error(e: ParseError) -> Error {
    match e {
        ParseError::Syntax => Error::Parse,
        ParseError::Depth => Error::PathLimit,
    }
}

thread_local! {
    /// How deep the selectors on this thread recurse, nested ones included.
    static RECURSION: Cell<usize> = const { Cell::new(0) };
    /// How deep the `..` walks on this thread go together.
    static WALK_DEPTH: Cell<usize> = const { Cell::new(0) };
    /// The values the current selection ran the rest of a path from, and
    /// how many it may (0 until the document is counted).
    static EVALS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

struct Deeper(&'static LocalKey<Cell<usize>>);

impl Drop for Deeper {
    fn drop(&mut self) {
        self.0.with(|d| d.set(d.get() - 1));
    }
}

fn deeper() -> Result<Deeper, Error> {
    deeper_in(&RECURSION, MAX_RECURSION)
}

fn deeper_in(key: &'static LocalKey<Cell<usize>>, max: usize) -> Result<Deeper, Error> {
    let depth = key.with(|d| {
        d.set(d.get() + 1);
        d.get()
    });
    let guard = Deeper(key);
    if depth > max {
        return Err(Error::RecursionDepth);
    }
    Ok(guard)
}

/// Count one more run of the rest of a path against the budget of the
/// selection under way.
fn spend(root: &Node) -> Result<(), Error> {
    EVALS.with(|e| {
        let (done, mut limit) = e.get();
        let done = done + 1;
        if done > MAX_EVALS && limit == 0 {
            limit = MAX_EVALS.saturating_add(values_in(root).saturating_mul(4));
        }
        e.set((done, limit));
        if limit != 0 && done > limit {
            return Err(Error::SearchLimit);
        }
        Ok(())
    })
}

/// The values in a document, itself included.
fn values_in(root: &Node) -> usize {
    let mut stack = vec![root];
    let mut n = 0;
    while let Some(v) = stack.pop() {
        n += 1;
        match v {
            Node::Obj(members) => stack.extend(members.iter().map(|(_, child)| child)),
            Node::Arr(items) => stack.extend(items),
            _ => {}
        }
    }
    n
}

#[derive(Clone)]
struct State<'a> {
    node: Option<&'a Node>,
    path_len: usize,
    lex: Lexer<'a>,
    depth: usize,
}

pub struct Selector<'a> {
    root: &'a Node,
    node: Option<&'a Node>,
    path: Vec<Step<'a>>,
    lex: Lexer<'a>,
    /// Whether a path began with `$`; it stays so for later paths.
    pub v2: bool,
    mode: Mode,
    depth: usize,
    /// The deepest a selected value or added member sits.
    pub max_depth: usize,
    /// The values found, in order, with where each sits.
    pub results: Vec<(&'a Node, Vec<Step<'a>>)>,
    /// The members a write adds: the object's path and the name.
    pub inserts: Vec<(Vec<Step<'a>>, Vec<u8>)>,
    insert_set: HashSet<(Vec<Step<'a>>, Vec<u8>)>,
    recursive: bool,
    parsed: bool,
    empty_fanout: bool,
    error: Option<Error>,
}

impl<'a> Selector<'a> {
    pub fn new(root: &'a Node, v2: bool) -> Self {
        Selector {
            root,
            node: None,
            path: Vec::new(),
            lex: Lexer::new(&[]),
            v2,
            mode: Mode::Read,
            depth: 0,
            max_depth: 0,
            results: Vec::new(),
            inserts: Vec::new(),
            insert_set: HashSet::new(),
            recursive: false,
            parsed: false,
            empty_fanout: false,
            error: None,
        }
    }

    /// Select by `path` for a read.
    pub fn get(&mut self, path: &'a [u8]) -> Result<(), Error> {
        self.select(path, Mode::Read)
    }

    pub fn select(&mut self, path: &'a [u8], mode: Mode) -> Result<(), Error> {
        let path = &path[..path.iter().position(|&c| c == 0).unwrap_or(path.len())];
        if path.len() > MAX_QUERY {
            return Err(Error::QuerySize);
        }
        // A selection a filter starts shares the budget of the one it is in.
        if RECURSION.with(Cell::get) == 0 {
            EVALS.with(|e| e.set((0, 0)));
        }
        self.mode = mode;
        self.node = Some(self.root);
        self.path.clear();
        self.lex = Lexer::new(path);
        self.results.clear();
        self.inserts.clear();
        self.insert_set.clear();
        self.max_depth = 0;
        self.depth = 0;
        self.error = None;
        self.parsed = false;
        self.empty_fanout = false;
        self.lex.next_token(false);
        self.eval()
    }

    /// The error a write saw on the way that did not stop it.
    pub fn error(&self) -> Option<Error> {
        self.error
    }

    /// The results without repeats of the same value, in order.
    pub fn unique(&self) -> Vec<(&'a Node, &[Step<'a>])> {
        let mut seen = HashSet::new();
        self.results
            .iter()
            .filter(|(n, _)| seen.insert(ptr::from_ref::<Node>(*n)))
            .map(|(n, p)| (*n, p.as_slice()))
            .collect()
    }

    fn snapshot(&self) -> State<'a> {
        State {
            node: self.node,
            path_len: self.path.len(),
            lex: self.lex,
            depth: self.depth,
        }
    }

    fn restore(&mut self, state: &State<'a>) {
        self.node = state.node;
        self.path.truncate(state.path_len);
        self.lex = state.lex;
        self.depth = state.depth;
    }

    fn deeper_path(&mut self) {
        self.depth += 1;
        self.max_depth = self.max_depth.max(self.depth);
    }

    /// Whether the rest of the path is still read when nothing is left to
    /// walk: only to check it after a fan-out over nothing.
    fn check_rest(&self) -> bool {
        self.v2 && self.empty_fanout && !self.parsed
    }

    fn eval(&mut self) -> Result<(), Error> {
        let _guard = deeper()?;
        spend(self.root)?;
        self.supported_path()?;
        if let Some(node) = self.node {
            self.results.push((node, self.path.clone()));
        }
        Ok(())
    }

    fn supported_path(&mut self) -> Result<(), Error> {
        if self.node.is_none() && !self.check_rest() {
            return Ok(());
        }
        if self.lex.eat(Tok::End, false) {
            self.parsed = true;
            return Ok(());
        }
        if self.lex.eat(Tok::Dollar, false) {
            if !self.node.is_some_and(|n| ptr::eq(n, self.root)) {
                return Err(Error::DollarNotRoot);
            }
            self.v2 = true;
        } else {
            self.lex.eat(Tok::Dot, false);
        }
        self.relative_path()
    }

    /// The rest of a path, one element after another.
    fn relative_path(&mut self) -> Result<(), Error> {
        loop {
            if self.node.is_none() && !self.check_rest() {
                return Ok(());
            }
            if self.lex.eat(Tok::End, false) {
                self.parsed = true;
                return Ok(());
            }
            match self.lex.cur() {
                Tok::DotDot => return self.recursive_path(),
                Tok::Dot => {
                    self.lex.eat(Tok::Dot, false);
                    self.qualified_element()?;
                }
                Tok::LBracket => self.bracket_element()?,
                _ => self.qualified_element()?,
            }
        }
    }

    fn qualified_element(&mut self) -> Result<(), Error> {
        if self.lex.cur() == Tok::LBracket {
            self.bracket_element()
        } else {
            self.key()
        }
    }

    fn recursive_path(&mut self) -> Result<(), Error> {
        self.recursive = true;
        self.lex.eat(Tok::DotDot, false);
        if self.lex.descents > MAX_DESCENTS {
            return Err(Error::DescentTokens);
        }
        if matches!(self.lex.cur(), Tok::DotDot | Tok::Dot) {
            return Err(Error::InvalidDotSequence);
        }
        if let Some(node) = self.node {
            self.recursive_search(node)?;
            self.dedupe();
        }
        match self.supported_path() {
            Err(e) if e.is_syntax() => Err(e),
            _ => Ok(()),
        }
    }

    /// Run the rest of the path from `v` and from every value under it.
    fn recursive_search(&mut self, v: &'a Node) -> Result<(), Error> {
        if !v.is_container() {
            self.node = None;
            return Ok(());
        }
        let _walk = deeper_in(&WALK_DEPTH, MAX_WALK_DEPTH)?;
        let state = self.snapshot();
        self.node = Some(v);
        let rc = self.eval();
        self.restore(&state);
        syntax_only(rc)?;
        let len = self.path.len();
        match v {
            Node::Obj(members) => {
                for (name, child) in members {
                    self.path.push(Step::Key(name));
                    self.deeper_path();
                    let rc = self.recursive_search(child);
                    self.depth -= 1;
                    syntax_only(rc)?;
                    self.path.truncate(len);
                }
            }
            Node::Arr(items) => {
                for (i, child) in items.iter().enumerate() {
                    self.path.push(Step::Index(i));
                    self.deeper_path();
                    let rc = self.recursive_search(child);
                    self.depth -= 1;
                    syntax_only(rc)?;
                    self.path.truncate(len);
                }
            }
            _ => {}
        }
        self.node = None;
        Ok(())
    }

    fn dedupe(&mut self) {
        if self.results.len() > 1 {
            let mut seen = HashSet::new();
            self.results
                .retain(|(n, _)| seen.insert(ptr::from_ref::<Node>(*n)));
        }
    }

    fn bracket_element(&mut self) -> Result<(), Error> {
        if !self.lex.eat(Tok::LBracket, true) {
            return Err(Error::InvalidPath);
        }
        match self.lex.cur() {
            Tok::Wildcard => {
                if !self.lex.eat(Tok::Wildcard, true) || !self.lex.eat(Tok::RBracket, true) {
                    return Err(Error::InvalidPath);
                }
                self.wildcard()?;
            }
            Tok::Single | Tok::Double => self.names_in_brackets()?,
            _ => self.index_expr()?,
        }
        self.lex.skip_spaces();
        Ok(())
    }

    fn names_in_brackets(&mut self) -> Result<(), Error> {
        let mut names = vec![self.quoted_name()?];
        while self.lex.eat(Tok::Comma, true) {
            self.lex.skip_spaces();
            names.push(self.quoted_name()?);
        }
        if !self.lex.eat(Tok::RBracket, true) {
            return Err(Error::InvalidPath);
        }
        let Some(node) = self.node else {
            return Ok(());
        };
        if let [name] = names.as_slice() {
            return self.enter_member(name);
        }
        let Node::Obj(members) = node else {
            if self.mode != Mode::Read {
                return Err(Error::CannotInsert);
            }
            self.node = None;
            return Ok(());
        };
        for name in &names {
            if let Some((key, value)) = members.iter().find(|(k, _)| **k == **name) {
                syntax_only(self.object_member(key, value))?;
            }
        }
        self.node = None;
        Ok(())
    }

    fn quoted_name(&mut self) -> Result<Vec<u8>, Error> {
        match self.lex.cur() {
            Tok::Double => self.lex.double_quoted(),
            Tok::Single => Ok(self.lex.single_quoted()),
            _ => Err(Error::InvalidPath),
        }
    }

    fn key(&mut self) -> Result<(), Error> {
        if self.lex.eat(Tok::Wildcard, false) {
            if self.lex.cur() == Tok::Dot {
                self.lex.next_token(false);
            }
            return self.wildcard();
        }
        let name = self.lex.unquoted_name()?;
        self.enter_member(name)
    }

    fn wildcard(&mut self) -> Result<(), Error> {
        let Some(node) = self.node else {
            return Ok(());
        };
        match node {
            Node::Obj(members) => {
                if members.is_empty() {
                    self.empty_fanout = true;
                }
                for (name, value) in members {
                    let state = self.snapshot();
                    let rc = self.object_member(name, value);
                    self.restore(&state);
                    syntax_only(rc)?;
                }
            }
            Node::Arr(items) => {
                if items.is_empty() {
                    self.empty_fanout = true;
                }
                for i in 0..items.len() {
                    syntax_only(self.array_member(i as i64))?;
                }
            }
            _ if self.v2 => return Err(Error::InvalidWildcard),
            _ => return Err(Error::InvalidPath),
        }
        self.node = None;
        Ok(())
    }

    /// Run the rest of the path from a member of the current object.
    fn object_member(&mut self, name: &'a [u8], value: &'a Node) -> Result<(), Error> {
        if !matches!(self.node, Some(Node::Obj(_))) {
            return Err(Error::NotObject);
        }
        let state = self.snapshot();
        self.path.push(Step::Key(name));
        let rc = self.member(value);
        self.restore(&state);
        rc
    }

    /// Run the rest of the path from an element of the current array.
    fn array_member(&mut self, i: i64) -> Result<(), Error> {
        let Some(Node::Arr(items)) = self.node else {
            return Err(Error::NotArray);
        };
        let Some(item) = usize::try_from(i).ok().and_then(|i| items.get(i)) else {
            return Err(Error::OutOfBounds);
        };
        let state = self.snapshot();
        self.path.push(Step::Index(i as usize));
        let rc = self.member(item);
        self.restore(&state);
        rc
    }

    fn member(&mut self, value: &'a Node) -> Result<(), Error> {
        let _guard = deeper()?;
        self.deeper_path();
        self.node = Some(value);
        self.eval()
    }

    fn enter_member(&mut self, name: &[u8]) -> Result<(), Error> {
        let Some(node) = self.node else {
            return Ok(());
        };
        let Node::Obj(members) = node else {
            if self.mode != Mode::Read {
                return Err(Error::CannotInsert);
            }
            self.node = None;
            return Ok(());
        };
        match members.iter().find(|(k, _)| **k == *name) {
            Some((key, value)) => {
                self.path.push(Step::Key(key));
                self.node = Some(value);
                self.deeper_path();
            }
            None => {
                if self.mode == Mode::Write && !self.recursive {
                    if self.lex.peek() != Tok::End {
                        self.error = Some(Error::PathNotExist);
                        return Err(Error::PathNotExist);
                    }
                    let insert = (self.path.clone(), name.to_vec());
                    if self.insert_set.insert(insert.clone()) {
                        self.inserts.push(insert);
                    }
                    self.deeper_path();
                }
                self.node = None;
            }
        }
        Ok(())
    }

    fn enter_index(&mut self, i: i64) -> Result<(), Error> {
        let Some(node) = self.node else {
            return Ok(());
        };
        let Node::Arr(items) = node else {
            self.node = None;
            return Ok(());
        };
        let i = if i < 0 { i + items.len() as i64 } else { i };
        let Some(item) = usize::try_from(i).ok().and_then(|i| items.get(i)) else {
            return Err(Error::OutOfBounds);
        };
        self.path.push(Step::Index(i as usize));
        self.node = Some(item);
        self.deeper_path();
        Ok(())
    }

    fn index_expr(&mut self) -> Result<(), Error> {
        match self.lex.cur() {
            Tok::End => Err(Error::EmptyExpr),
            Tok::Question => self.filter(),
            Tok::Colon => {
                let size = self.array_len()?;
                self.lex.next_token(true);
                match self.lex.cur() {
                    Tok::RBracket => self.slice(0, size, 1),
                    Tok::Colon => {
                        self.lex.next_token(true);
                        self.step(0, size)
                    }
                    _ => self.end_and_step(0),
                }
            }
            Tok::Comma => Err(Error::InvalidPath),
            _ => {
                let start = self.index()?;
                self.lex.skip_spaces();
                match self.lex.cur() {
                    Tok::Colon => {
                        let size = self.array_len()?;
                        self.lex.next_token(true);
                        self.lex.skip_spaces();
                        match self.lex.cur() {
                            Tok::RBracket => self.slice(start, size, 1),
                            Tok::Colon => {
                                self.lex.next_token(false);
                                self.step(start, size)
                            }
                            _ => self.end_and_step(start),
                        }
                    }
                    Tok::Comma => self.union_of_indexes(start),
                    _ => {
                        if !self.lex.eat(Tok::RBracket, true) {
                            return Err(Error::InvalidPath);
                        }
                        match self.node {
                            Some(Node::Arr(_)) | None => self.enter_index(start),
                            Some(_) => Err(Error::NotArray),
                        }
                    }
                }
            }
        }
    }

    /// The size of the array a slice reads: 0 when there is none.
    fn array_len(&self) -> Result<i64, Error> {
        match self.node {
            None => Ok(0),
            Some(Node::Arr(items)) => Ok(items.len() as i64),
            Some(_) => Err(Error::NotArray),
        }
    }

    fn index(&mut self) -> Result<i64, Error> {
        self.lex.integer().map_err(|e| match e {
            Error::ValueNotNumber => Error::IndexNotNumber,
            e => e,
        })
    }

    fn end_and_step(&mut self, start: i64) -> Result<(), Error> {
        let end = self.index()?;
        self.lex.skip_spaces();
        if self.lex.cur() == Tok::Colon {
            self.lex.next_token(false);
            self.step(start, end)
        } else {
            self.slice(start, end, 1)
        }
    }

    fn step(&mut self, start: i64, end: i64) -> Result<(), Error> {
        self.lex.skip_spaces();
        if self.lex.cur() == Tok::RBracket {
            return self.slice(start, end, 1);
        }
        let step = self.index()?;
        self.slice(start, end, step)
    }

    fn slice(&mut self, start: i64, end: i64, step: i64) -> Result<(), Error> {
        self.array_len()?;
        if !self.lex.eat(Tok::RBracket, true) {
            return Err(Error::InvalidPath);
        }
        if step == 0 {
            return Err(Error::StepZero);
        }
        let Some(Node::Arr(items)) = self.node else {
            return Ok(());
        };
        let size = items.len() as i64;
        let from_end = |i: i64| if i < 0 { i + size } else { i };
        let start = from_end(start).clamp(0, size);
        let end = from_end(end).clamp(0, size);
        let mut i = start;
        if step > 0 {
            while i < end {
                syntax_only(self.array_member(i))?;
                i += step.min(end - i);
            }
        } else {
            while i > end {
                syntax_only(self.array_member(i))?;
                i += step.max(end - i);
            }
        }
        self.node = None;
        Ok(())
    }

    fn union_of_indexes(&mut self, first: i64) -> Result<(), Error> {
        self.array_len()?;
        let mut indexes = vec![first];
        let mut comma = false;
        self.lex.skip_spaces();
        while self.lex.cur() != Tok::RBracket {
            if self.lex.cur() == Tok::Comma {
                if comma {
                    return Err(Error::InvalidPath);
                }
                comma = true;
                self.lex.next_token(true);
            } else {
                if !comma {
                    return Err(Error::InvalidPath);
                }
                comma = false;
                self.lex.skip_spaces();
                indexes.push(self.index()?);
            }
            self.lex.skip_spaces();
        }
        if comma || !self.lex.eat(Tok::RBracket, true) {
            return Err(Error::InvalidPath);
        }
        let Some(Node::Arr(items)) = self.node else {
            return Ok(());
        };
        let size = items.len() as i64;
        // The last index as valkey-json works it out, in 32 bits: on an
        // empty array it wraps, and an index then fails instead.
        let last = i64::from((size - 1) as u32);
        for i in indexes {
            let i = if i < 0 { i + size } else { i };
            if i < 0 || i > last {
                continue;
            }
            self.array_member(i)?;
        }
        self.node = None;
        Ok(())
    }

    fn filter(&mut self) -> Result<(), Error> {
        self.lex.next_token(false);
        if !self.lex.eat(Tok::LParen, false) {
            return Err(Error::InvalidPath);
        }
        let mut chosen = Vec::new();
        self.filter_expr(&mut chosen)?;
        if !self.lex.eat(Tok::RParen, false) || !self.lex.eat(Tok::RBracket, true) {
            return Err(Error::InvalidPath);
        }
        let Some(node) = self.node else {
            return Ok(());
        };
        if chosen.is_empty() {
            self.empty_fanout = true;
        }
        chosen.sort_unstable();
        match node {
            Node::Arr(_) => {
                for i in chosen {
                    syntax_only(self.array_member(i))?;
                }
            }
            Node::Obj(members) => {
                let mut want = chosen.iter().peekable();
                for (ordinal, (name, value)) in members.iter().enumerate() {
                    let Some(&&next) = want.peek() else {
                        break;
                    };
                    if next == ordinal as i64 {
                        syntax_only(self.object_member(name, value))?;
                        want.next();
                    }
                }
            }
            _ if !chosen.is_empty() => syntax_only(self.member(node))?,
            _ => {}
        }
        self.node = None;
        Ok(())
    }

    /// Terms joined by `||`: the indexes or ordinals any of them picks.
    fn filter_expr(&mut self, chosen: &mut Vec<i64>) -> Result<(), Error> {
        let _guard = deeper()?;
        self.lex.skip_spaces();
        self.term(chosen)?;
        let mut seen: Option<HashSet<i64>> = None;
        while self.lex.eat(Tok::Or, true) {
            let mut more = Vec::new();
            self.term(&mut more)?;
            let seen = seen.get_or_insert_with(|| chosen.iter().copied().collect());
            for e in more {
                if seen.insert(e) {
                    chosen.push(e);
                }
            }
        }
        self.lex.skip_spaces();
        Ok(())
    }

    /// Factors joined by `&&`: what all of them pick, in the first's order.
    fn term(&mut self, chosen: &mut Vec<i64>) -> Result<(), Error> {
        let _guard = deeper()?;
        self.factor(chosen)?;
        while self.lex.eat(Tok::And, true) {
            let left = std::mem::take(chosen);
            let mut right = Vec::new();
            self.factor(&mut right)?;
            let right: HashSet<i64> = right.into_iter().collect();
            chosen.extend(left.into_iter().filter(|e| right.contains(e)));
        }
        Ok(())
    }

    fn factor(&mut self, chosen: &mut Vec<i64>) -> Result<(), Error> {
        let _guard = deeper()?;
        self.lex.skip_spaces();
        if self.lex.cur() == Tok::LParen {
            self.lex.next_token(true);
            self.filter_expr(chosen)?;
            if !self.lex.eat(Tok::RParen, true) {
                return Err(Error::InvalidPath);
            }
            return Ok(());
        }
        if self.lex.eat(Tok::At, false) {
            if !matches!(self.lex.cur(), Tok::Dot | Tok::LBracket) {
                let op = self.comparison_op()?;
                let value = self.comparison_value()?;
                self.compare(Pick::Itself, op, &value, chosen);
                return Ok(());
            }
            let name = self.member_name()?;
            self.lex.skip_spaces();
            let tok = self.lex.cur();
            if tok.compares() {
                let op = self.comparison_op()?;
                let value = self.comparison_value()?;
                self.compare(Pick::Member(&name), op, &value, chosen);
            } else if tok == Tok::LBracket {
                self.lex.next_token(true);
                if self.lex.cur() == Tok::Question {
                    self.lex.next_token(true);
                    if !self.lex.eat(Tok::LParen, false) {
                        return Err(Error::InvalidPath);
                    }
                    let (op, value) = if self.lex.cur() == Tok::At {
                        self.lex.next_token(true);
                        let op = self.comparison_op()?;
                        (op, self.comparison_value()?)
                    } else {
                        let value = self.comparison_value()?;
                        let op = self.comparison_op()?.swapped();
                        if !self.lex.eat(Tok::At, false) {
                            return Err(Error::InvalidPath);
                        }
                        (op, value)
                    };
                    self.compare(Pick::AnyElement(&name), op, &value, chosen);
                    if !self.lex.eat(Tok::RParen, true) || !self.lex.eat(Tok::RBracket, false) {
                        return Err(Error::InvalidPath);
                    }
                } else {
                    let i = self.index()?;
                    if !self.lex.eat(Tok::RBracket, false) {
                        return Err(Error::InvalidPath);
                    }
                    let op = self.comparison_op()?;
                    let value = self.comparison_value()?;
                    self.compare(Pick::Element(&name, i), op, &value, chosen);
                }
            } else {
                self.compare(Pick::Has(&name), Tok::Eq, &Node::Null, chosen);
            }
            return Ok(());
        }
        // A value first: `1 < @.a` is `@.a > 1`.
        let value = self.comparison_value()?;
        self.lex.skip_spaces();
        let op = self.comparison_op()?.swapped();
        if !self.lex.eat(Tok::At, false) {
            return Err(Error::InvalidPath);
        }
        self.lex.skip_spaces();
        if matches!(self.lex.cur(), Tok::RParen | Tok::And | Tok::Or) {
            self.compare(Pick::Itself, op, &value, chosen);
            return Ok(());
        }
        let name = self.member_name()?;
        if self.lex.cur() == Tok::LBracket {
            self.lex.next_token(true);
            let i = self.index()?;
            if !self.lex.eat(Tok::RBracket, false) {
                return Err(Error::InvalidPath);
            }
            self.compare(Pick::Element(&name, i), op, &value, chosen);
        } else {
            self.compare(Pick::Member(&name), op, &value, chosen);
        }
        Ok(())
    }

    fn member_name(&mut self) -> Result<Vec<u8>, Error> {
        let bracketed = if self.lex.eat(Tok::Dot, false) {
            self.lex.eat(Tok::LBracket, false)
        } else if self.lex.eat(Tok::LBracket, false) {
            true
        } else {
            return Err(Error::InvalidPath);
        };
        if !bracketed {
            return Ok(self.lex.unquoted_name()?.to_vec());
        }
        self.lex.skip_spaces();
        let name = self.quoted_name()?;
        if !self.lex.eat(Tok::RBracket, true) {
            return Err(Error::InvalidPath);
        }
        Ok(name)
    }

    fn comparison_op(&mut self) -> Result<Tok, Error> {
        self.lex.skip_spaces();
        let op = self.lex.cur();
        if !op.compares() {
            return Err(Error::InvalidPath);
        }
        self.lex.skip_spaces();
        self.lex.next_token(true);
        Ok(op)
    }

    fn comparison_value(&mut self) -> Result<Node, Error> {
        let _guard = deeper()?;
        let token = self.lex.next;
        let first = self.lex.at(token.start);
        let text = match token.tok {
            Tok::Dollar => {
                let path = self.lex.path_value()?;
                let mut selector = Selector::new(self.root, false);
                selector.get(path)?;
                return match selector.results.as_slice() {
                    [(value, _)] if !value.is_container() => Ok((*value).clone()),
                    _ => Err(Error::InvalidPath),
                };
            }
            Tok::Double => return Ok(Node::Str(self.lex.double_quoted()?.into())),
            Tok::Single => self.lex.single_as_double(),
            Tok::Alpha if first == b'n' => {
                let word = self.lex.identifier()?;
                if word != b"null" {
                    return Err(Error::InvalidIdentifier);
                }
                word.to_vec()
            }
            Tok::Alpha if first == b't' || first == b'f' => {
                let word = self.lex.identifier()?;
                if word != b"true" && word != b"false" {
                    return Err(Error::InvalidIdentifier);
                }
                word.to_vec()
            }
            _ => self.lex.number()?.to_vec(),
        };
        node::parse(&text, MAX_NESTING)
            .map(|(value, _)| value)
            .map_err(parse_error)
    }

    /// Add the indexes, or ordinals of members, of the current container's
    /// values that `pick` and `op` accept.
    fn compare(&self, pick: Pick, op: Tok, value: &Node, chosen: &mut Vec<i64>) {
        let Some(node) = self.node else {
            return;
        };
        let accepts = |candidate: &Node| match pick {
            Pick::Itself => compare(candidate, op, value),
            Pick::Member(name) => candidate
                .member(name)
                .is_some_and(|m| compare(m, op, value)),
            Pick::Has(name) => candidate.member(name).is_some(),
            Pick::Element(name, i) => match candidate.member(name) {
                Some(Node::Arr(items)) => {
                    let i = if i < 0 { i + items.len() as i64 } else { i };
                    usize::try_from(i)
                        .ok()
                        .and_then(|i| items.get(i))
                        .is_some_and(|e| compare(e, op, value))
                }
                _ => false,
            },
            Pick::AnyElement(name) => match candidate.member(name) {
                Some(Node::Arr(items)) => items.iter().any(|e| compare(e, op, value)),
                _ => false,
            },
        };
        match node {
            Node::Arr(items) => chosen.extend(
                (0..)
                    .zip(items)
                    .filter_map(|(i, item)| accepts(item).then_some(i)),
            ),
            Node::Obj(members) => chosen.extend(
                (0..)
                    .zip(members)
                    .filter_map(|(i, (_, v))| accepts(v).then_some(i)),
            ),
            scalar => {
                if matches!(pick, Pick::Itself) && compare(scalar, op, value) {
                    chosen.push(0);
                }
            }
        }
    }
}

/// What a filter tests in each candidate.
#[derive(Clone, Copy)]
enum Pick<'n> {
    Itself,
    Member(&'n [u8]),
    /// That it has the member.
    Has(&'n [u8]),
    Element(&'n [u8], i64),
    /// Any element of the member, an array.
    AnyElement(&'n [u8]),
}

/// A filter's comparison: false between values of different types (the
/// two booleans are one type), and for arrays and objects.
fn compare(v: &Node, op: Tok, value: &Node) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let order = match (v, value) {
        (Node::Null, Node::Null) => return op == Tok::Eq,
        (Node::Bool(a), Node::Bool(b)) => a.partial_cmp(b),
        (Node::Str(a), Node::Str(b)) => a.partial_cmp(b),
        (Node::Int(a), Node::Int(b)) => a.partial_cmp(b),
        (Node::Int(_) | Node::Num(_), Node::Int(_) | Node::Num(_)) => {
            v.number().partial_cmp(&value.number())
        }
        _ => return false,
    };
    matches!(
        (op, order),
        (Tok::Eq, Some(Equal))
            | (Tok::Ne, Some(Less | Greater))
            | (Tok::Lt, Some(Less))
            | (Tok::Le, Some(Less | Equal))
            | (Tok::Gt, Some(Greater))
            | (Tok::Ge, Some(Greater | Equal))
    )
}

/// Pass on syntax errors only: the others end one branch of a fan-out.
fn syntax_only(rc: Result<(), Error>) -> Result<(), Error> {
    match rc {
        Err(e) if e.is_syntax() => Err(e),
        _ => Ok(()),
    }
}
