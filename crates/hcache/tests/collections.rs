//! Lists, sets, sorted sets and the newer string/hash/key commands, hunted
//! for bugs: a randomized differential test against a model written with
//! std collections, tables of exact replies (Redis 7 semantics), concurrent
//! hammering with conservation invariants, and big collections.
//!
//! Where a test below fails, the model or table follows Redis' documented
//! behaviour; the implementation is what is wrong.

mod support;

use hcache::{Config, Db, Reply, Session};
// Valkey adds INCRBYFLOAT increments as long doubles; the core's own type,
// pinned to Valkey's output by its unit tests, stands in for them here.
use hcache_core::float::{LongDouble, LongDoubleError};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering as AtomicOrd};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use support::{array, brief, cfg, guarded, int, open, session, text};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const NOT_FLOAT: &str = "ERR value is not a valid float";
const SYNTAX: &str = "ERR syntax error";
const OUT_OF_RANGE: &str = "ERR value is out of range, must be positive";
const NAN_SCORE: &str = "ERR resulting score is not a number (NaN)";
const NO_KEY: &str = "ERR no such key";
const BAD_INDEX: &str = "ERR index out of range";
const NAN_OR_INF: &str = "ERR increment would produce NaN or Infinity";
const BAD_BOUND: &str = "ERR min or max is not a float";
const LIMIT_RANK: &str =
    "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX";

fn conf(shards: usize) -> Config {
    Config {
        shards,
        ..Config::default()
    }
}

// ---------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------

/// xorshift64*, fixed seeds.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        (((self.next() >> 11) as u128 * n as u128) >> 53) as u64
    }

    fn chance(&mut self, one_in: u64) -> bool {
        self.below(one_in) == 0
    }

    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[self.below(xs.len() as u64) as usize]
    }
}

fn sv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| p.to_string()).collect()
}

fn show(r: &Reply) -> String {
    match r {
        Reply::Ok => "OK".into(),
        Reply::Simple(s) => format!("+{s}"),
        Reply::Error(e) => format!("-{e}"),
        Reply::Int(n) => format!(":{n}"),
        Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        Reply::Nil => "nil".into(),
        Reply::NilArray => "nilarray".into(),
        Reply::Array(items) | Reply::Map(items) => {
            format!("[{}]", items.iter().map(show).collect::<Vec<_>>().join(" "))
        }
    }
}

/// `show`, but a top-level array comes out sorted (unordered replies).
fn show_sorted(r: &Reply) -> String {
    match r {
        Reply::Array(items) => {
            let mut parts: Vec<String> = items.iter().map(show).collect();
            parts.sort();
            format!("[{}]", parts.join(" "))
        }
        other => show(other),
    }
}

fn x(s: &Session, line: &str) -> String {
    let args: Vec<&str> = line.split(' ').collect();
    show(&s.execute(&args))
}

fn xs(s: &Session, line: &str) -> String {
    let args: Vec<&str> = line.split(' ').collect();
    show_sorted(&s.execute(&args))
}

fn table(s: &Session, cases: &[(&str, &str)]) {
    for (line, want) in cases {
        assert_eq!(x(s, line), *want, "{line}");
    }
}

/// Like `table`, but arrays come out sorted on both sides.
fn table_sorted(s: &Session, cases: &[(&str, &str)]) {
    for (line, want) in cases {
        let mut want_items: Vec<&str> = Vec::new();
        let want_sorted = if want.starts_with('[') && want.len() > 2 {
            want_items.extend(want[1..want.len() - 1].split(' '));
            want_items.sort_unstable();
            format!("[{}]", want_items.join(" "))
        } else {
            (*want).to_string()
        };
        assert_eq!(xs(s, line), want_sorted, "{line}");
    }
}

fn pttl(s: &Session, key: &str) -> i64 {
    int(&s.execute(&["PTTL", key]))
}

fn words(r: Reply) -> Vec<String> {
    array(r).iter().map(text).collect()
}

fn ints(r: Reply) -> Vec<String> {
    array(r).iter().map(|v| int(v).to_string()).collect()
}

fn sorted_words(r: Reply) -> Vec<String> {
    let mut w = words(r);
    w.sort();
    w
}

fn pairs_of(r: Reply) -> Vec<(String, String)> {
    let w = words(r);
    assert_eq!(w.len() % 2, 0, "odd pair list");
    let mut out: Vec<_> = w.chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------
// Reference model
// ---------------------------------------------------------------------

type B = Vec<u8>;

fn b(s: &str) -> B {
    s.as_bytes().to_vec()
}

#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ok,
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(B),
    Nil,
    NilArray,
    Arr(Vec<Rep>),
}

fn rep(r: &Reply) -> Rep {
    match r {
        Reply::Ok => Rep::Ok,
        Reply::Simple(s) => Rep::Simple(s.clone()),
        Reply::Error(e) => Rep::Err(e.clone()),
        Reply::Int(n) => Rep::Int(*n),
        Reply::Bulk(x) => Rep::Bulk(x.clone()),
        Reply::Nil => Rep::Nil,
        Reply::NilArray => Rep::NilArray,
        Reply::Array(items) | Reply::Map(items) => Rep::Arr(items.iter().map(rep).collect()),
    }
}

fn rerr(s: &str) -> Rep {
    Rep::Err(s.to_string())
}

fn wt() -> Rep {
    rerr(WRONGTYPE)
}

fn ri(n: usize) -> Rep {
    Rep::Int(n as i64)
}

fn rb(x: &[u8]) -> Rep {
    Rep::Bulk(x.to_vec())
}

fn rbulks<'a>(items: impl IntoIterator<Item = &'a B>) -> Rep {
    Rep::Arr(items.into_iter().map(|i| rb(i)).collect())
}

/// A score as Valkey prints it. The rules (integers up to 2^62, Grisu2
/// past that) are pinned to Valkey's output by the core's unit tests.
fn fscore(s: f64) -> B {
    hcache_core::zset::format_score(s)
}

fn pfloat(s: &str) -> Option<f64> {
    let v = match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        t => {
            if t.is_empty()
                || !t
                    .bytes()
                    .all(|c| c.is_ascii_digit() || b".-+e".contains(&c))
            {
                return None;
            }
            t.parse::<f64>().ok()?
        }
    };
    (!v.is_nan()).then_some(v)
}

fn pf(s: &str) -> Result<f64, Rep> {
    pfloat(s).ok_or_else(|| rerr(NOT_FLOAT))
}

fn pint(s: &str) -> Result<i64, Rep> {
    s.parse::<i64>().map_err(|_| rerr(NOT_INT))
}

fn pcount(s: &str) -> Result<usize, Rep> {
    let n = pint(s)?;
    usize::try_from(n).map_err(|_| rerr(OUT_OF_RANGE))
}

/// `(value, exclusive)`
type Bound = (f64, bool);

fn pbound(s: &str) -> Result<Bound, Rep> {
    let (excl, digits) = match s.strip_prefix('(') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    // Valkey 9.1 reads an empty bound as zero.
    let value = if digits.is_empty() {
        Some(0.0)
    } else {
        pfloat(digits)
    };
    value.map(|v| (v, excl)).ok_or_else(|| rerr(BAD_BOUND))
}

fn in_range(score: f64, lo: Bound, hi: Bound) -> bool {
    (if lo.1 { score > lo.0 } else { score >= lo.0 })
        && (if hi.1 { score < hi.0 } else { score <= hi.0 })
}

fn glob(p: &[u8], s: &[u8]) -> bool {
    match p.split_first() {
        None => s.is_empty(),
        Some((b'*', rest)) => (0..=s.len()).any(|i| glob(rest, &s[i..])),
        Some((b'?', rest)) => !s.is_empty() && glob(rest, &s[1..]),
        Some((c, rest)) => s.first() == Some(c) && glob(rest, &s[1..]),
    }
}

/// Redis' normalization of an inclusive index range over `len` items.
fn lrange_bounds(start: i64, end: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut s = if start < 0 {
        start.saturating_add(len)
    } else {
        start
    };
    let mut e = if end < 0 {
        end.saturating_add(len)
    } else {
        end
    };
    if s < 0 {
        s = 0;
    }
    if s > e || s >= len {
        return None;
    }
    if e >= len {
        e = len - 1;
    }
    Some((s as usize, e as usize))
}

fn zlt(a: f64, am: &[u8], c: f64, cm: &[u8]) -> bool {
    a < c || (a == c && am < cm)
}

fn zfind(z: &[(f64, B)], m: &[u8]) -> Option<f64> {
    z.iter().find(|e| e.1 == m).map(|e| e.0)
}

fn zput(z: &mut Vec<(f64, B)>, m: &[u8], s: f64) {
    z.retain(|e| e.1 != m);
    let pos = z.partition_point(|e| zlt(e.0, &e.1, s, m));
    z.insert(pos, (s, m.to_vec()));
}

#[derive(Clone, Debug, PartialEq)]
enum V {
    Str(B),
    Hash(BTreeMap<B, B>),
    List(VecDeque<B>),
    Set(BTreeSet<B>),
    Z(Vec<(f64, B)>),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Str,
    Hash,
    List,
    Set,
    Z,
}

impl Kind {
    fn empty(self) -> V {
        match self {
            Kind::Str => V::Str(Vec::new()),
            Kind::Hash => V::Hash(BTreeMap::new()),
            Kind::List => V::List(VecDeque::new()),
            Kind::Set => V::Set(BTreeSet::new()),
            Kind::Z => V::Z(Vec::new()),
        }
    }
}

impl V {
    fn kind(&self) -> Kind {
        match self {
            V::Str(_) => Kind::Str,
            V::Hash(_) => Kind::Hash,
            V::List(_) => Kind::List,
            V::Set(_) => Kind::Set,
            V::Z(_) => Kind::Z,
        }
    }

    fn type_name(&self) -> &'static str {
        match self {
            V::Str(_) => "string",
            V::Hash(_) => "hash",
            V::List(_) => "list",
            V::Set(_) => "set",
            V::Z(_) => "zset",
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            V::Str(_) => false,
            V::Hash(h) => h.is_empty(),
            V::List(l) => l.is_empty(),
            V::Set(s) => s.is_empty(),
            V::Z(z) => z.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match self {
            V::Str(s) => s.len(),
            V::Hash(h) => h.len(),
            V::List(l) => l.len(),
            V::Set(s) => s.len(),
            V::Z(z) => z.len(),
        }
    }

    fn list(&mut self) -> &mut VecDeque<B> {
        match self {
            V::List(l) => l,
            _ => unreachable!("not a list"),
        }
    }

    fn set(&mut self) -> &mut BTreeSet<B> {
        match self {
            V::Set(s) => s,
            _ => unreachable!("not a set"),
        }
    }

    fn zs(&mut self) -> &mut Vec<(f64, B)> {
        match self {
            V::Z(z) => z,
            _ => unreachable!("not a zset"),
        }
    }

    fn hash(&mut self) -> &mut BTreeMap<B, B> {
        match self {
            V::Hash(h) => h,
            _ => unreachable!("not a hash"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Entry {
    val: V,
    /// Virtual milliseconds.
    deadline: Option<i64>,
}

/// What the model expects of one reply.
enum Out {
    Exact(Rep),
    /// An array of bulks in any order.
    Sorted(Vec<B>),
    /// A flat array of pairs, the pairs in any order.
    Pairs(Vec<(B, B)>),
    /// A whole SSCAN / ZSCAN iteration, already followed to its end.
    Scan(Vec<B>, bool),
    /// PTTL: the real clock moves too, so a little less is fine.
    Pttl(i64),
    /// A bulk, whatever it says.
    AnyBulk,
    Skip,
    Bad(String),
}

use Out::Exact as E;

struct ZReq {
    rev: bool,
    by_score: bool,
    limit: Option<(i64, i64)>,
    with: bool,
}

fn parse_zrange(name: &str, a: &[String]) -> Result<ZReq, Rep> {
    let mut by_score = name.ends_with("BYSCORE");
    let mut rev = name.starts_with("ZREV");
    let (mut with, mut limit) = (false, None);
    let mut i = 4;
    while i < a.len() {
        let opt = a[i].to_ascii_uppercase();
        if opt == "WITHSCORES" {
            with = true;
            i += 1;
        } else if opt == "LIMIT" && i + 2 < a.len() {
            limit = Some((pint(&a[i + 1])?, pint(&a[i + 2])?));
            i += 3;
        } else if opt == "BYSCORE" && name == "ZRANGE" {
            by_score = true;
            i += 1;
        } else if opt == "REV" && name == "ZRANGE" {
            rev = true;
            i += 1;
        } else {
            return Err(rerr(SYNTAX));
        }
    }
    if limit.is_some() && !by_score {
        return Err(rerr(LIMIT_RANK));
    }
    Ok(ZReq {
        rev,
        by_score,
        limit,
        with,
    })
}

fn zrows(z: &[(f64, B)], r: &ZReq, a: &[String]) -> Result<Vec<(f64, B)>, Rep> {
    let mut rows: Vec<(f64, B)> = z.to_vec();
    if r.by_score {
        let (lo, hi) = if r.rev {
            (&a[3], &a[2])
        } else {
            (&a[2], &a[3])
        };
        let (lo, hi) = (pbound(lo)?, pbound(hi)?);
        rows.retain(|e| in_range(e.0, lo, hi));
        if r.rev {
            rows.reverse();
        }
    } else {
        let (s, e) = (pint(&a[2])?, pint(&a[3])?);
        if r.rev {
            rows.reverse();
        }
        rows = match lrange_bounds(s, e, rows.len()) {
            Some((s, e)) => rows[s..=e].to_vec(),
            None => Vec::new(),
        };
    }
    if let Some((off, cnt)) = r.limit {
        if off < 0 {
            rows.clear();
        } else {
            rows = rows.into_iter().skip(off as usize).collect();
            if cnt >= 0 {
                rows.truncate(cnt as usize);
            }
        }
    }
    Ok(rows)
}

fn side(s: &str) -> Result<bool, Rep> {
    match s.to_ascii_uppercase().as_str() {
        "LEFT" => Ok(true),
        "RIGHT" => Ok(false),
        _ => Err(rerr(SYNTAX)),
    }
}

/// Check a reply to SPOP / SRANDMEMBER / HRANDFIELD against the members.
fn check_sample(
    reply: &Rep,
    uni: &BTreeMap<B, B>,
    count: Option<i64>,
    values: bool,
) -> Result<Vec<B>, String> {
    let Some(c) = count else {
        return match reply {
            Rep::Nil if uni.is_empty() => Ok(Vec::new()),
            Rep::Bulk(m) if uni.contains_key(m) => Ok(vec![m.clone()]),
            other => Err(format!(
                "single sample was {other:?} for {} members",
                uni.len()
            )),
        };
    };
    let Rep::Arr(items) = reply else {
        return Err(format!("sample was {reply:?}, not an array"));
    };
    let stride = if values { 2 } else { 1 };
    let want = if c >= 0 {
        uni.len().min(c as usize)
    } else if uni.is_empty() {
        0
    } else {
        c.unsigned_abs() as usize
    };
    if items.len() != want * stride {
        return Err(format!(
            "asked for {c} of {} members, got {} items",
            uni.len(),
            items.len()
        ));
    }
    let mut out = Vec::new();
    for chunk in items.chunks(stride) {
        let Rep::Bulk(m) = &chunk[0] else {
            return Err(format!("sample item {:?}", chunk[0]));
        };
        let Some(v) = uni.get(m) else {
            return Err(format!("sample member {:?} is not in the collection", m));
        };
        if values && chunk[1] != Rep::Bulk(v.clone()) {
            return Err(format!("sample value {:?} for {:?}", chunk[1], m));
        }
        out.push(m.clone());
    }
    if c >= 0 && out.iter().collect::<HashSet<_>>().len() != out.len() {
        return Err("a positive count returned a member twice".into());
    }
    Ok(out)
}

#[derive(Clone, Default)]
struct Model {
    now: i64,
    keys: BTreeMap<String, Entry>,
}

impl Model {
    fn purge(&mut self) {
        let now = self.now;
        self.keys.retain(|_, e| e.deadline.is_none_or(|d| d > now));
    }

    fn kind_of(&self, key: &str) -> Option<Kind> {
        self.keys.get(key).map(|e| e.val.kind())
    }

    fn len_of(&self, key: &str) -> usize {
        self.keys.get(key).map_or(0, |e| e.val.len())
    }

    fn has_ttl(&self, key: &str) -> bool {
        self.keys.get(key).is_some_and(|e| e.deadline.is_some())
    }

    fn peek(&self, key: &str, kind: Kind) -> Result<Option<&V>, Rep> {
        match self.keys.get(key) {
            None => Ok(None),
            Some(e) if e.val.kind() == kind => Ok(Some(&e.val)),
            Some(_) => Err(wt()),
        }
    }

    /// Change a typed value in place; a value left empty disappears, deadline
    /// and all.
    fn edit(
        &mut self,
        key: &str,
        kind: Kind,
        create: bool,
        missing: Rep,
        f: impl FnOnce(&mut V) -> Rep,
    ) -> Result<Rep, Rep> {
        if self.peek(key, kind)?.is_none() {
            if !create {
                return Ok(missing);
            }
            let val = kind.empty();
            self.keys.insert(
                key.to_string(),
                Entry {
                    val,
                    deadline: None,
                },
            );
        }
        let e = self.keys.get_mut(key).expect("just checked");
        let r = f(&mut e.val);
        if e.val.is_empty() {
            self.keys.remove(key);
        }
        Ok(r)
    }

    fn members(&self, key: &str, kind: Kind) -> Result<BTreeMap<B, B>, Rep> {
        Ok(match self.peek(key, kind)? {
            Some(V::Set(s)) => s.iter().map(|m| (m.clone(), Vec::new())).collect(),
            Some(V::Hash(h)) => h.clone(),
            Some(V::Z(z)) => z.iter().map(|e| (e.1.clone(), fscore(e.0))).collect(),
            _ => BTreeMap::new(),
        })
    }

    fn apply(&mut self, args: &[String], got: &Reply) -> Out {
        self.purge();
        match self.run(args, got) {
            Ok(out) => out,
            Err(r) => E(r),
        }
    }

    fn run(&mut self, a: &[String], got: &Reply) -> Result<Out, Rep> {
        let name = a[0].to_ascii_uppercase();
        let key = a.get(1).map_or("", String::as_str);
        let now = self.now;
        Ok(match name.as_str() {
            // ---- keys and strings ----
            "SET" => {
                let val = V::Str(b(&a[2]));
                self.keys.insert(
                    key.into(),
                    Entry {
                        val,
                        deadline: None,
                    },
                );
                E(Rep::Ok)
            }
            "GET" => match self.peek(key, Kind::Str)? {
                Some(V::Str(v)) => E(rb(v)),
                _ => E(Rep::Nil),
            },
            "DEL" => E(Rep::Int(self.keys.remove(key).is_some() as i64)),
            "EXISTS" => E(Rep::Int(self.keys.contains_key(key) as i64)),
            "TYPE" => E(Rep::Simple(
                self.keys
                    .get(key)
                    .map_or("none", |e| e.val.type_name())
                    .into(),
            )),
            "TOUCH" => E(ri(a[1..]
                .iter()
                .filter(|k| self.keys.contains_key(k.as_str()))
                .count())),
            "PEXPIRE" => {
                let ms = pint(&a[2])?;
                match self.keys.get_mut(key) {
                    None => E(Rep::Int(0)),
                    Some(e) => {
                        e.deadline = Some(now + ms);
                        E(Rep::Int(1))
                    }
                }
            }
            "PERSIST" => match self.keys.get_mut(key) {
                None => E(Rep::Int(0)),
                Some(e) => E(Rep::Int(e.deadline.take().is_some() as i64)),
            },
            "PTTL" => match self.keys.get(key) {
                None => E(Rep::Int(-2)),
                Some(Entry { deadline: None, .. }) => E(Rep::Int(-1)),
                Some(Entry {
                    deadline: Some(d), ..
                }) => Out::Pttl(d - now),
            },
            "INCRBYFLOAT" => {
                // The key's type and value are checked before the increment.
                let cur = match self.peek(key, Kind::Str)? {
                    None => Ok(LongDouble::ZERO),
                    Some(V::Str(s)) => LongDouble::parse(s),
                    Some(_) => unreachable!(),
                };
                let next = match (cur, LongDouble::parse(a[2].as_bytes())) {
                    (Err(LongDoubleError::NotFloat), _) | (_, Err(LongDoubleError::NotFloat)) => {
                        return Err(rerr(NOT_FLOAT))
                    }
                    (Ok(cur), Ok(by)) => cur.checked_add(by),
                    _ => None,
                };
                let text = next.ok_or_else(|| rerr(NAN_OR_INF))?.to_human();
                let deadline = self.keys.get(key).and_then(|e| e.deadline);
                let val = V::Str(text.clone());
                self.keys.insert(key.into(), Entry { val, deadline });
                E(Rep::Bulk(text))
            }
            "GETRANGE" | "SUBSTR" => {
                let (s, e) = (pint(&a[2])?, pint(&a[3])?);
                match self.peek(key, Kind::Str)? {
                    Some(V::Str(v)) => {
                        let len = v.len() as i64;
                        // Valkey's t_string.c: an end before the first byte
                        // is clamped to it, unlike LRANGE.
                        if len == 0 || (s < 0 && e < 0 && s > e) {
                            return Ok(E(rb(b"")));
                        }
                        let s = if s < 0 { (s + len).max(0) } else { s };
                        let e = if e < 0 {
                            (e + len).max(0)
                        } else {
                            e.min(len - 1)
                        };
                        if s > e {
                            E(rb(b""))
                        } else {
                            E(rb(&v[s as usize..=e as usize]))
                        }
                    }
                    _ => E(rb(b"")),
                }
            }
            "SETRANGE" => {
                let off = pint(&a[2])?;
                if off < 0 {
                    return Err(rerr("ERR offset is out of range"));
                }
                let (off, patch) = (off as usize, b(&a[3]));
                if self.peek(key, Kind::Str)?.is_none() {
                    if patch.is_empty() {
                        return Ok(E(Rep::Int(0)));
                    }
                    let mut v = vec![0; off + patch.len()];
                    v[off..].copy_from_slice(&patch);
                    let n = v.len();
                    let val = V::Str(v);
                    self.keys.insert(
                        key.into(),
                        Entry {
                            val,
                            deadline: None,
                        },
                    );
                    return Ok(E(ri(n)));
                }
                let Some(Entry { val: V::Str(v), .. }) = self.keys.get_mut(key) else {
                    unreachable!()
                };
                if !patch.is_empty() {
                    if v.len() < off + patch.len() {
                        v.resize(off + patch.len(), 0);
                    }
                    v[off..off + patch.len()].copy_from_slice(&patch);
                }
                E(ri(v.len()))
            }
            "MSETNX" => {
                if a[1..].chunks(2).any(|p| self.keys.contains_key(&p[0])) {
                    return Ok(E(Rep::Int(0)));
                }
                for p in a[1..].chunks(2) {
                    let val = V::Str(b(&p[1]));
                    self.keys.insert(
                        p[0].clone(),
                        Entry {
                            val,
                            deadline: None,
                        },
                    );
                }
                E(Rep::Int(1))
            }
            "RENAME" | "RENAMENX" => {
                let nx = name == "RENAMENX";
                let dst = a[2].as_str();
                if !self.keys.contains_key(key) {
                    return Err(rerr(NO_KEY));
                }
                if nx && self.keys.contains_key(dst) {
                    return Ok(E(Rep::Int(0)));
                }
                if key != dst {
                    let e = self.keys.remove(key).expect("checked");
                    self.keys.insert(dst.to_string(), e);
                }
                E(if nx { Rep::Int(1) } else { Rep::Ok })
            }
            "COPY" => {
                let dst = a[2].as_str();
                if key == dst {
                    return Ok(Out::Skip);
                }
                let replace = a.get(3).is_some_and(|o| o.eq_ignore_ascii_case("REPLACE"));
                let Some(e) = self.keys.get(key).cloned() else {
                    return Ok(E(Rep::Int(0)));
                };
                if !replace && self.keys.contains_key(dst) {
                    return Ok(E(Rep::Int(0)));
                }
                self.keys.insert(dst.to_string(), e);
                E(Rep::Int(1))
            }
            "RANDOMKEY" => match rep(got) {
                Rep::Nil if self.keys.is_empty() => E(Rep::Nil),
                Rep::Bulk(k) if self.keys.contains_key(&*String::from_utf8_lossy(&k)) => {
                    E(Rep::Bulk(k))
                }
                other => Out::Bad(format!(
                    "RANDOMKEY gave {other:?} with {} live keys",
                    self.keys.len()
                )),
            },
            "OBJECT" => match self.keys.get(a[2].as_str()).map(|e| &e.val) {
                None => E(Rep::Nil),
                Some(V::Str(v)) => {
                    let text = std::str::from_utf8(v).ok();
                    E(rb(if text.is_some_and(|t| t.parse::<i64>().is_ok()) {
                        b"int"
                    } else if v.len() <= 44 {
                        b"embstr"
                    } else {
                        b"raw"
                    }))
                }
                Some(_) => Out::AnyBulk,
            },

            // ---- hashes ----
            "HSET" => {
                let pairs: Vec<(B, B)> = a[2..].chunks(2).map(|p| (b(&p[0]), b(&p[1]))).collect();
                E(self.edit(key, Kind::Hash, true, Rep::Nil, |v| {
                    let h = v.hash();
                    ri(pairs
                        .into_iter()
                        .filter(|(f, val)| h.insert(f.clone(), val.clone()).is_none())
                        .count())
                })?)
            }
            "HGET" => match self.peek(key, Kind::Hash)? {
                Some(V::Hash(h)) => E(h.get(a[2].as_bytes()).map_or(Rep::Nil, |v| rb(v))),
                _ => E(Rep::Nil),
            },
            "HDEL" => {
                let fields: Vec<B> = a[2..].iter().map(|f| b(f)).collect();
                E(self.edit(key, Kind::Hash, false, Rep::Int(0), |v| {
                    let h = v.hash();
                    ri(fields.iter().filter(|f| h.remove(*f).is_some()).count())
                })?)
            }
            "HLEN" => E(ri(self.peek(key, Kind::Hash)?.map_or(0, V::len))),
            "HKEYS" | "HVALS" => {
                let h = self.members(key, Kind::Hash)?;
                Out::Sorted(if name == "HKEYS" {
                    h.keys().cloned().collect()
                } else {
                    h.values().cloned().collect()
                })
            }
            "HGETALL" => Out::Pairs(self.members(key, Kind::Hash)?.into_iter().collect()),
            "HSTRLEN" => match self.peek(key, Kind::Hash)? {
                Some(V::Hash(h)) => E(ri(h.get(a[2].as_bytes()).map_or(0, Vec::len))),
                _ => E(Rep::Int(0)),
            },
            "HINCRBYFLOAT" => {
                // The increment is checked before the key's type.
                let by = match LongDouble::parse(a[3].as_bytes()) {
                    Ok(by) => by,
                    Err(LongDoubleError::NotFloat) => return Err(rerr(NOT_FLOAT)),
                    Err(LongDoubleError::Infinite) => {
                        return Err(rerr("ERR value is NaN or Infinity"))
                    }
                };
                let field = b(&a[2]);
                E(self.edit(key, Kind::Hash, true, Rep::Nil, |v| {
                    let h = v.hash();
                    let cur = match h.get(&field) {
                        None => Ok(LongDouble::ZERO),
                        Some(old) => LongDouble::parse(old),
                    };
                    let next = match cur {
                        Ok(cur) => cur.checked_add(by),
                        Err(LongDoubleError::Infinite) => None,
                        Err(LongDoubleError::NotFloat) => {
                            return rerr("ERR hash value is not a float")
                        }
                    };
                    let Some(next) = next else {
                        return rerr(NAN_OR_INF);
                    };
                    let text = next.to_human();
                    h.insert(field, text.clone());
                    Rep::Bulk(text)
                })?)
            }
            "HRANDFIELD" => {
                let count = a.get(2).map(|c| pint(c)).transpose()?;
                let values = match a.get(3) {
                    Some(o) if o.eq_ignore_ascii_case("WITHVALUES") => true,
                    Some(_) => return Err(rerr(SYNTAX)),
                    None => false,
                };
                let uni = self.members(key, Kind::Hash)?;
                match check_sample(&rep(got), &uni, count, values) {
                    Ok(_) => E(rep(got)),
                    Err(m) => Out::Bad(format!("HRANDFIELD: {m}")),
                }
            }

            // ---- lists ----
            "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" => {
                let left = name.starts_with('L');
                let items: Vec<B> = a[2..].iter().map(|i| b(i)).collect();
                E(
                    self.edit(key, Kind::List, !name.ends_with('X'), Rep::Int(0), |v| {
                        let l = v.list();
                        for it in items {
                            if left {
                                l.push_front(it);
                            } else {
                                l.push_back(it);
                            }
                        }
                        ri(l.len())
                    })?,
                )
            }
            "LPOP" | "RPOP" => {
                let left = name == "LPOP";
                let count = a.get(2).map(|c| pcount(c)).transpose()?;
                let missing = if count.is_some() {
                    Rep::NilArray
                } else {
                    Rep::Nil
                };
                E(self.edit(key, Kind::List, false, missing, |v| {
                    let l = v.list();
                    let mut pop = || if left { l.pop_front() } else { l.pop_back() };
                    match count {
                        None => pop().map_or(Rep::Nil, Rep::Bulk),
                        Some(n) => {
                            let mut out = Vec::new();
                            for _ in 0..n {
                                match pop() {
                                    Some(x) => out.push(Rep::Bulk(x)),
                                    None => break,
                                }
                            }
                            Rep::Arr(out)
                        }
                    }
                })?)
            }
            "LLEN" => E(ri(self.peek(key, Kind::List)?.map_or(0, V::len))),
            "LRANGE" => {
                let (s, e) = (pint(&a[2])?, pint(&a[3])?);
                match self.peek(key, Kind::List)? {
                    Some(V::List(l)) => match lrange_bounds(s, e, l.len()) {
                        Some((s, e)) => E(rbulks(l.range(s..=e))),
                        None => E(Rep::Arr(Vec::new())),
                    },
                    _ => E(Rep::Arr(Vec::new())),
                }
            }
            "LINDEX" => {
                let i = pint(&a[2])?;
                match self.peek(key, Kind::List)? {
                    Some(V::List(l)) => {
                        let i = if i < 0 { i + l.len() as i64 } else { i };
                        E(usize::try_from(i)
                            .ok()
                            .and_then(|i| l.get(i))
                            .map_or(Rep::Nil, |v| rb(v)))
                    }
                    _ => E(Rep::Nil),
                }
            }
            "LSET" => {
                let i = pint(&a[2])?;
                let item = b(&a[3]);
                E(self.edit(key, Kind::List, false, rerr(NO_KEY), |v| {
                    let l = v.list();
                    let i = if i < 0 { i + l.len() as i64 } else { i };
                    match usize::try_from(i).ok().filter(|&i| i < l.len()) {
                        Some(i) => {
                            l[i] = item;
                            Rep::Ok
                        }
                        None => rerr(BAD_INDEX),
                    }
                })?)
            }
            "LREM" => {
                let count = pint(&a[2])?;
                let item = b(&a[3]);
                E(self.edit(key, Kind::List, false, Rep::Int(0), |v| {
                    let l = v.list();
                    let before = l.len();
                    let mut left = if count == 0 {
                        usize::MAX
                    } else {
                        count.unsigned_abs() as usize
                    };
                    let order: Vec<usize> = if count >= 0 {
                        (0..before).collect()
                    } else {
                        (0..before).rev().collect()
                    };
                    let mut drop = HashSet::new();
                    for i in order {
                        if left > 0 && l[i] == item {
                            drop.insert(i);
                            left -= 1;
                        }
                    }
                    let mut i = 0;
                    l.retain(|_| {
                        i += 1;
                        !drop.contains(&(i - 1))
                    });
                    ri(before - l.len())
                })?)
            }
            "LTRIM" => {
                let (s, e) = (pint(&a[2])?, pint(&a[3])?);
                E(self.edit(key, Kind::List, false, Rep::Ok, |v| {
                    let l = v.list();
                    match lrange_bounds(s, e, l.len()) {
                        Some((s, e)) => {
                            l.truncate(e + 1);
                            l.drain(..s);
                        }
                        None => l.clear(),
                    }
                    Rep::Ok
                })?)
            }
            "LINSERT" => {
                let before = match a[2].to_ascii_uppercase().as_str() {
                    "BEFORE" => true,
                    "AFTER" => false,
                    _ => return Err(rerr(SYNTAX)),
                };
                let (pivot, item) = (b(&a[3]), b(&a[4]));
                E(self.edit(key, Kind::List, false, Rep::Int(0), |v| {
                    let l = v.list();
                    match l.iter().position(|e| *e == pivot) {
                        Some(i) => {
                            l.insert(if before { i } else { i + 1 }, item);
                            ri(l.len())
                        }
                        None => Rep::Int(-1),
                    }
                })?)
            }
            "LPOS" => {
                let (mut rank, mut count, mut maxlen) = (1i64, None, 0usize);
                let mut i = 3;
                while i < a.len() {
                    let v = a.get(i + 1).ok_or_else(|| rerr(SYNTAX))?;
                    match a[i].to_ascii_uppercase().as_str() {
                        "RANK" => rank = pint(v)?,
                        "COUNT" => count = Some(pcount(v)?),
                        "MAXLEN" => maxlen = pcount(v)?,
                        _ => return Err(rerr(SYNTAX)),
                    }
                    i += 2;
                }
                let l = match self.peek(key, Kind::List)? {
                    Some(V::List(l)) => l.clone(),
                    _ => VecDeque::new(),
                };
                let n = l.len();
                let limit = if maxlen == 0 { n } else { maxlen.min(n) };
                let idxs: Vec<usize> = if rank > 0 {
                    (0..limit).collect()
                } else {
                    (n - limit..n).rev().collect()
                };
                let want = match count {
                    None => 1,
                    Some(0) => usize::MAX,
                    Some(c) => c,
                };
                let hits: Vec<usize> = idxs
                    .into_iter()
                    .filter(|&i| l[i] == a[2].as_bytes())
                    .skip(rank.unsigned_abs() as usize - 1)
                    .take(want)
                    .collect();
                match count {
                    None => E(hits.first().map_or(Rep::Nil, |&i| ri(i))),
                    Some(_) => E(Rep::Arr(hits.into_iter().map(ri).collect())),
                }
            }
            "LMOVE" | "RPOPLPUSH" => {
                let (from_left, to_left) = if name == "RPOPLPUSH" {
                    (false, true)
                } else {
                    (side(&a[3])?, side(&a[4])?)
                };
                let dst = a[2].as_str();
                if self.peek(key, Kind::List)?.is_none() {
                    return Ok(E(Rep::Nil));
                }
                self.peek(dst, Kind::List)?;
                let src_list = self.keys.get_mut(key).expect("checked").val.list();
                let item = if from_left {
                    src_list.pop_front()
                } else {
                    src_list.pop_back()
                }
                .expect("lists are never empty");
                let src_empty = src_list.is_empty();
                let target = self.keys.entry(dst.to_string()).or_insert_with(|| Entry {
                    val: V::List(VecDeque::new()),
                    deadline: None,
                });
                let l = target.val.list();
                if to_left {
                    l.push_front(item.clone());
                } else {
                    l.push_back(item.clone());
                }
                if key != dst && src_empty {
                    self.keys.remove(key);
                }
                E(Rep::Bulk(item))
            }

            // ---- sets ----
            "SADD" => {
                let ms: Vec<B> = a[2..].iter().map(|m| b(m)).collect();
                E(self.edit(key, Kind::Set, true, Rep::Nil, |v| {
                    let s = v.set();
                    ri(ms.into_iter().filter(|m| s.insert(m.clone())).count())
                })?)
            }
            "SREM" => {
                let ms: Vec<B> = a[2..].iter().map(|m| b(m)).collect();
                E(self.edit(key, Kind::Set, false, Rep::Int(0), |v| {
                    let s = v.set();
                    ri(ms.iter().filter(|m| s.remove(*m)).count())
                })?)
            }
            "SISMEMBER" | "SMISMEMBER" => {
                let s = match self.peek(key, Kind::Set)? {
                    Some(V::Set(s)) => s.clone(),
                    _ => BTreeSet::new(),
                };
                let has = |m: &String| Rep::Int(s.contains(m.as_bytes()) as i64);
                if name == "SISMEMBER" {
                    E(has(&a[2]))
                } else {
                    E(Rep::Arr(a[2..].iter().map(has).collect()))
                }
            }
            "SMEMBERS" => Out::Sorted(self.members(key, Kind::Set)?.into_keys().collect()),
            "SCARD" => E(ri(self.peek(key, Kind::Set)?.map_or(0, V::len))),
            "SPOP" => {
                let count = a.get(2).map(|c| pcount(c)).transpose()?;
                let uni = self.members(key, Kind::Set)?;
                let c = count.map(|c| c as i64);
                match check_sample(&rep(got), &uni, c, false) {
                    Err(m) => Out::Bad(format!("SPOP: {m}")),
                    Ok(picked) => {
                        if !picked.is_empty() {
                            let e = self.keys.get_mut(key).expect("non-empty set");
                            for m in &picked {
                                e.val.set().remove(m);
                            }
                            if e.val.is_empty() {
                                self.keys.remove(key);
                            }
                        }
                        E(rep(got))
                    }
                }
            }
            "SRANDMEMBER" => {
                let count = a.get(2).map(|c| pint(c)).transpose()?;
                let uni = self.members(key, Kind::Set)?;
                match check_sample(&rep(got), &uni, count, false) {
                    Ok(_) => E(rep(got)),
                    Err(m) => Out::Bad(format!("SRANDMEMBER: {m}")),
                }
            }
            "SSCAN" | "ZSCAN" => {
                let z = name == "ZSCAN";
                let mut pattern: Option<B> = None;
                let mut i = 3;
                while i + 1 < a.len() {
                    if a[i].eq_ignore_ascii_case("MATCH") {
                        pattern = Some(b(&a[i + 1]));
                    }
                    i += 2;
                }
                let uni = self.members(key, if z { Kind::Z } else { Kind::Set })?;
                let mut items = Vec::new();
                for (m, s) in uni {
                    if pattern.as_ref().is_none_or(|p| glob(p, &m)) {
                        items.push(m);
                        if z {
                            items.push(s);
                        }
                    }
                }
                Out::Scan(items, z)
            }
            "SMOVE" => {
                let dst = a[2].as_str();
                if self.peek(key, Kind::Set)?.is_none() {
                    return Ok(E(Rep::Int(0)));
                }
                self.peek(dst, Kind::Set)?;
                let member = b(&a[3]);
                let src = self.keys.get_mut(key).expect("checked").val.set();
                if key == dst {
                    return Ok(E(Rep::Int(src.contains(&member) as i64)));
                }
                if !src.remove(&member) {
                    return Ok(E(Rep::Int(0)));
                }
                if src.is_empty() {
                    self.keys.remove(key);
                }
                let target = self.keys.entry(dst.to_string()).or_insert_with(|| Entry {
                    val: V::Set(BTreeSet::new()),
                    deadline: None,
                });
                target.val.set().insert(member);
                E(Rep::Int(1))
            }
            "SINTER" | "SUNION" | "SDIFF" | "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE" => {
                let store = name.ends_with("STORE");
                let srcs = &a[if store { 2 } else { 1 }..];
                let mut sets = Vec::new();
                for k in srcs {
                    sets.push(
                        self.members(k, Kind::Set)?
                            .into_keys()
                            .collect::<BTreeSet<B>>(),
                    );
                }
                let result = set_op(&name[..4], &sets);
                if store {
                    self.keys.remove(key);
                    let n = result.len();
                    if n > 0 {
                        let val = V::Set(result);
                        self.keys.insert(
                            key.into(),
                            Entry {
                                val,
                                deadline: None,
                            },
                        );
                    }
                    E(ri(n))
                } else {
                    Out::Sorted(result.into_iter().collect())
                }
            }
            "SINTERCARD" => {
                let n = pint(&a[1])? as usize;
                let mut sets = Vec::new();
                for k in &a[2..2 + n] {
                    sets.push(
                        self.members(k, Kind::Set)?
                            .into_keys()
                            .collect::<BTreeSet<B>>(),
                    );
                }
                let limit = match a.get(2 + n + 1) {
                    Some(l) => match l.parse::<usize>().expect("limit") {
                        0 => usize::MAX,
                        l => l,
                    },
                    None => usize::MAX,
                };
                E(ri(set_op("SINT", &sets).len().min(limit)))
            }

            // ---- sorted sets ----
            "ZADD" => return self.zadd(a),
            "ZREM" => {
                let ms: Vec<B> = a[2..].iter().map(|m| b(m)).collect();
                E(self.edit(key, Kind::Z, false, Rep::Int(0), |v| {
                    let z = v.zs();
                    let uniq: BTreeSet<&B> = ms.iter().collect();
                    let n = uniq.iter().filter(|m| zfind(z, m).is_some()).count();
                    z.retain(|e| !ms.contains(&e.1));
                    ri(n)
                })?)
            }
            "ZSCORE" | "ZMSCORE" => {
                let z = match self.peek(key, Kind::Z)? {
                    Some(V::Z(z)) => z.clone(),
                    _ => Vec::new(),
                };
                let one =
                    |m: &String| zfind(&z, m.as_bytes()).map_or(Rep::Nil, |s| Rep::Bulk(fscore(s)));
                if name == "ZSCORE" {
                    E(one(&a[2]))
                } else {
                    E(Rep::Arr(a[2..].iter().map(one).collect()))
                }
            }
            "ZINCRBY" => {
                let by = pf(&a[2])?;
                let m = b(&a[3]);
                E(self.edit(key, Kind::Z, true, Rep::Nil, |v| {
                    let z = v.zs();
                    let next = zfind(z, &m).map_or(by, |cur| cur + by);
                    if next.is_nan() {
                        return rerr(NAN_SCORE);
                    }
                    zput(z, &m, next);
                    Rep::Bulk(fscore(next))
                })?)
            }
            "ZCARD" => E(ri(self.peek(key, Kind::Z)?.map_or(0, V::len))),
            "ZCOUNT" => {
                let (lo, hi) = (pbound(&a[2])?, pbound(&a[3])?);
                match self.peek(key, Kind::Z)? {
                    Some(V::Z(z)) => E(ri(z.iter().filter(|e| in_range(e.0, lo, hi)).count())),
                    _ => E(Rep::Int(0)),
                }
            }
            "ZRANK" | "ZREVRANK" => {
                let with = match a.get(3) {
                    Some(o) if o.eq_ignore_ascii_case("WITHSCORE") => true,
                    Some(_) => return Err(rerr(SYNTAX)),
                    None => false,
                };
                let found = match self.peek(key, Kind::Z)? {
                    Some(V::Z(z)) => z.iter().position(|e| e.1 == a[2].as_bytes()).map(|p| {
                        let r = if name == "ZREVRANK" {
                            z.len() - 1 - p
                        } else {
                            p
                        };
                        (r, z[p].0)
                    }),
                    _ => None,
                };
                E(match (found, with) {
                    (None, true) => Rep::NilArray,
                    (None, false) => Rep::Nil,
                    (Some((r, s)), true) => Rep::Arr(vec![ri(r), Rep::Bulk(fscore(s))]),
                    (Some((r, _)), false) => ri(r),
                })
            }
            "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" => {
                let r = parse_zrange(&name, a)?;
                zrows(&[], &r, a)?;
                let z = match self.peek(key, Kind::Z)? {
                    Some(V::Z(z)) => z.clone(),
                    _ => Vec::new(),
                };
                let rows = zrows(&z, &r, a)?;
                let mut out = Vec::new();
                for (s, m) in rows {
                    out.push(Rep::Bulk(m));
                    if r.with {
                        out.push(Rep::Bulk(fscore(s)));
                    }
                }
                E(Rep::Arr(out))
            }
            "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" => {
                let by_rank = name.ends_with("RANK");
                let (s, e) = if by_rank {
                    (pint(&a[2])?, pint(&a[3])?)
                } else {
                    (0, 0)
                };
                let bounds = if by_rank {
                    ((0.0, false), (0.0, false))
                } else {
                    (pbound(&a[2])?, pbound(&a[3])?)
                };
                E(self.edit(key, Kind::Z, false, Rep::Int(0), |v| {
                    let z = v.zs();
                    let before = z.len();
                    if by_rank {
                        if let Some((s, e)) = lrange_bounds(s, e, z.len()) {
                            z.drain(s..=e);
                        }
                    } else {
                        z.retain(|e| !in_range(e.0, bounds.0, bounds.1));
                    }
                    ri(before - z.len())
                })?)
            }
            "ZPOPMIN" | "ZPOPMAX" => {
                let count = a.get(2).map(|c| pcount(c)).transpose()?.unwrap_or(1);
                let max = name == "ZPOPMAX";
                E(self.edit(key, Kind::Z, false, Rep::Arr(Vec::new()), |v| {
                    let z = v.zs();
                    let n = count.min(z.len());
                    let gone: Vec<(f64, B)> = if max {
                        let at = z.len() - n;
                        z.drain(at..).rev().collect()
                    } else {
                        z.drain(..n).collect()
                    };
                    Rep::Arr(
                        gone.into_iter()
                            .flat_map(|(s, m)| [Rep::Bulk(m), Rep::Bulk(fscore(s))])
                            .collect(),
                    )
                })?)
            }
            "ZUNIONSTORE" | "ZINTERSTORE" => return self.zstore(&name, a),
            other => panic!("model: unknown command {other}"),
        })
    }

    fn zadd(&mut self, a: &[String]) -> Result<Out, Rep> {
        let key = a[1].as_str();
        let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
            (false, false, false, false, false, false);
        let mut i = 2;
        while i < a.len() {
            match a[i].to_ascii_uppercase().as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "GT" => gt = true,
                "LT" => lt = true,
                "CH" => ch = true,
                "INCR" => incr = true,
                _ => break,
            }
            i += 1;
        }
        let rest = &a[i..];
        if rest.is_empty() || !rest.len().is_multiple_of(2) {
            return Err(rerr(SYNTAX));
        }
        if nx && xx {
            return Err(rerr(
                "ERR XX and NX options at the same time are not compatible",
            ));
        }
        if (gt && lt) || (nx && (gt || lt)) {
            return Err(rerr(
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            ));
        }
        if incr && rest.len() != 2 {
            return Err(rerr(
                "ERR INCR option supports a single increment-element pair",
            ));
        }
        let mut pairs = Vec::new();
        for p in rest.chunks(2) {
            pairs.push((pf(&p[0])?, b(&p[1])));
        }
        let r = self.edit(key, Kind::Z, true, Rep::Nil, |v| {
            let z = v.zs();
            let (mut added, mut updated) = (0usize, 0usize);
            let mut last = None;
            for (given, m) in pairs {
                let cur = zfind(z, &m);
                let next = match cur {
                    None if xx => continue,
                    None => given,
                    Some(_) if nx => continue,
                    Some(c) => {
                        let n = if incr { c + given } else { given };
                        if n.is_nan() {
                            return rerr(NAN_SCORE);
                        }
                        if (gt && n <= c) || (lt && n >= c) {
                            continue;
                        }
                        n
                    }
                };
                last = Some(next);
                match cur {
                    None => added += 1,
                    Some(c) if c != next => updated += 1,
                    Some(_) => continue,
                }
                zput(z, &m, next);
            }
            if incr {
                last.map_or(Rep::Nil, |s| Rep::Bulk(fscore(s)))
            } else {
                ri(if ch { added + updated } else { added })
            }
        })?;
        Ok(E(r))
    }

    fn zstore(&mut self, name: &str, a: &[String]) -> Result<Out, Rep> {
        let dst = a[1].as_str();
        let n = pint(&a[2])?;
        if n < 1 {
            return Err(rerr(&format!(
                "ERR at least 1 input key is needed for '{}' command",
                name.to_lowercase()
            )));
        }
        let n = n as usize;
        if n > a.len() - 3 {
            return Err(rerr(SYNTAX));
        }
        let keys = &a[3..3 + n];
        let mut weights = vec![1.0; n];
        let mut agg = "SUM".to_string();
        let mut i = 3 + n;
        while i < a.len() {
            let opt = a[i].to_ascii_uppercase();
            if opt == "WEIGHTS" && a.len() - i > n {
                for (w, raw) in weights.iter_mut().zip(&a[i + 1..]) {
                    *w = pfloat(raw).ok_or_else(|| rerr("ERR weight value is not a float"))?;
                }
                i += 1 + n;
            } else if opt == "AGGREGATE" && a.len() - i >= 2 {
                agg = a[i + 1].to_ascii_uppercase();
                if !["SUM", "MIN", "MAX"].contains(&agg.as_str()) {
                    return Err(rerr(SYNTAX));
                }
                i += 2;
            } else {
                return Err(rerr(SYNTAX));
            }
        }
        let mut acc: BTreeMap<B, (f64, usize)> = BTreeMap::new();
        for (k, w) in keys.iter().zip(&weights) {
            let entries: Vec<(B, f64)> = match self.keys.get(k.as_str()).map(|e| &e.val) {
                None => Vec::new(),
                Some(V::Z(z)) => z.iter().map(|e| (e.1.clone(), e.0)).collect(),
                Some(V::Set(s)) => s.iter().map(|m| (m.clone(), 1.0)).collect(),
                Some(_) => return Err(wt()),
            };
            for (m, s) in entries {
                let s = match s * w {
                    v if v.is_nan() => 0.0,
                    v => v,
                };
                acc.entry(m)
                    .and_modify(|(cur, seen)| {
                        *seen += 1;
                        *cur = match agg.as_str() {
                            "SUM" if (*cur + s).is_nan() => 0.0,
                            "SUM" => *cur + s,
                            "MIN" if *cur > s => s,
                            "MAX" if *cur < s => s,
                            _ => *cur,
                        };
                    })
                    .or_insert((s, 1));
            }
        }
        let inter = name == "ZINTERSTORE";
        let mut z: Vec<(f64, B)> = Vec::new();
        for (m, (s, seen)) in acc {
            if !inter || seen == n {
                zput(&mut z, &m, s);
            }
        }
        self.keys.remove(dst);
        let count = z.len();
        if count > 0 {
            let val = V::Z(z);
            self.keys.insert(
                dst.into(),
                Entry {
                    val,
                    deadline: None,
                },
            );
        }
        Ok(E(ri(count)))
    }
}

fn set_op(op: &str, sets: &[BTreeSet<B>]) -> BTreeSet<B> {
    let mut it = sets.iter();
    let first = it.next().cloned().unwrap_or_default();
    match op {
        "SINT" => it.fold(first, |acc, s| acc.intersection(s).cloned().collect()),
        "SUNI" => it.fold(first, |acc, s| acc.union(s).cloned().collect()),
        _ => it.fold(first, |acc, s| acc.difference(s).cloned().collect()),
    }
}

fn verify(out: &Out, got: &Reply, slack: i64) -> Result<(), String> {
    let r = rep(got);
    let flat = |r: &Rep| -> Option<Vec<B>> {
        match r {
            Rep::Arr(items) => items
                .iter()
                .map(|i| match i {
                    Rep::Bulk(x) => Some(x.clone()),
                    _ => None,
                })
                .collect(),
            _ => None,
        }
    };
    let pairs = |v: Vec<B>| -> Option<Vec<(B, B)>> {
        v.len().is_multiple_of(2).then(|| {
            let mut p: Vec<_> = v.chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect();
            p.sort();
            p
        })
    };
    match out {
        Out::Exact(want) => (r == *want)
            .then_some(())
            .ok_or_else(|| format!("model says {want:?}")),
        Out::Sorted(want) => {
            let mut want = want.clone();
            want.sort();
            let mut have = flat(&r).ok_or("reply is not an array of bulks")?;
            have.sort();
            (have == want)
                .then_some(())
                .ok_or_else(|| format!("model says (sorted) {want:?}"))
        }
        Out::Pairs(want) => {
            let mut want = want.clone();
            want.sort();
            let have = pairs(flat(&r).ok_or("reply is not an array of bulks")?)
                .ok_or("odd number of items")?;
            (have == want)
                .then_some(())
                .ok_or_else(|| format!("model says (sorted) {want:?}"))
        }
        Out::Scan(items, as_pairs) => {
            let Rep::Arr(parts) = &r else {
                return Err("SCAN reply is not an array".into());
            };
            if parts.len() != 2 || parts[0] != rb(b"0") {
                return Err("SCAN did not end with cursor 0".into());
            }
            let inner = flat(&parts[1]).ok_or("SCAN items are not bulks")?;
            if *as_pairs {
                let mut want = pairs(items.clone()).expect("pairs");
                want.sort();
                let have = pairs(inner).ok_or("odd number of items")?;
                (have == want)
                    .then_some(())
                    .ok_or_else(|| format!("model says (sorted) {want:?}"))
            } else {
                let (mut want, mut have) = (items.clone(), inner);
                want.sort();
                have.sort();
                (have == want)
                    .then_some(())
                    .ok_or_else(|| format!("model says (sorted) {want:?}"))
            }
        }
        Out::Pttl(want) => match got {
            Reply::Int(n) if (want - slack..=*want).contains(n) => Ok(()),
            _ => Err(format!("PTTL about {want}")),
        },
        Out::AnyBulk => matches!(got, Reply::Bulk(v) if !v.is_empty())
            .then_some(())
            .ok_or_else(|| "expected a bulk".to_string()),
        Out::Skip => Ok(()),
        Out::Bad(m) => Err(m.clone()),
    }
}

// ---------------------------------------------------------------------
// Command generator
// ---------------------------------------------------------------------

const LIST_KEYS: u64 = 0;
const HASH_KEYS: u64 = 1;
const SET_KEYS: u64 = 2;
const STR_KEYS: u64 = 3;
const ZSET_KEYS: u64 = 4;

const NUMS: [&str; 9] = ["-2", "-1", "0", "0.5", "1", "1.5", "2", "3", "10"];
const SCORES: [&str; 14] = [
    "0", "1", "1", "2", "2", "3", "-1", "-2", "0.5", "1.5", "10", "-inf", "+inf", "inf",
];

fn key_in(rng: &mut Rng, base: u64) -> String {
    if rng.chance(12) {
        format!("k{}", rng.below(6))
    } else {
        format!("k{}", (base + rng.below(3)) % 6)
    }
}

fn idx(rng: &mut Rng) -> String {
    match rng.below(24) {
        0 => i64::MAX.to_string(),
        1 => i64::MIN.to_string(),
        _ => (rng.below(15) as i64 - 7).to_string(),
    }
}

fn small_idx(rng: &mut Rng) -> String {
    (rng.below(15) as i64 - 7).to_string()
}

fn bad_token(rng: &mut Rng) -> String {
    rng.pick(&["x", "1.5", "", "9223372036854775808", "nan"])
        .to_string()
}

fn item(rng: &mut Rng) -> String {
    format!("v{}", rng.below(4))
}

fn member(rng: &mut Rng) -> String {
    format!("m{}", rng.below(6))
}

fn bound(rng: &mut Rng) -> String {
    let base = if rng.chance(6) {
        rng.pick(&["-inf", "+inf", "inf"]).to_string()
    } else {
        rng.pick(&NUMS).to_string()
    };
    if rng.chance(2) {
        format!("({base}")
    } else {
        base
    }
}

fn shuffle<T>(rng: &mut Rng, v: &mut [T]) {
    for i in (1..v.len()).rev() {
        v.swap(i, rng.below(i as u64 + 1) as usize);
    }
}

/// A destination for LMOVE / SMOVE that keeps clear of the known
/// divergences (see the dedicated tests): never a wrong-typed destination
/// for a missing source, never the source itself when a single member under
/// a deadline would be popped and pushed back.
fn pick_dst(rng: &mut Rng, m: &Model, src: &str, base: u64, kind: Kind) -> Option<String> {
    let mut dst = if rng.chance(3) {
        src.to_string()
    } else {
        key_in(rng, base)
    };
    let wrong = |k: &str| m.kind_of(k).is_some_and(|c| c != kind);
    if m.kind_of(src).is_none() && wrong(&dst) {
        dst = src.to_string();
    }
    if dst == src && m.has_ttl(src) && m.kind_of(src) == Some(kind) && m.len_of(src) < 2 {
        return (0..6)
            .map(|i| format!("k{i}"))
            .find(|k| k != src && !wrong(k));
    }
    Some(dst)
}

fn ttl(rng: &mut Rng) -> String {
    (rng.below(4) as i64 * 10_000 + 5_000).to_string()
}

fn gen_list(rng: &mut Rng, m: &Model, bad: bool) -> Vec<String> {
    let key = key_in(rng, LIST_KEYS);
    if m.kind_of(&key) == Some(Kind::List) && m.len_of(&key) > 40 && rng.chance(2) {
        return sv(&["LTRIM", &key, "0", "15"]);
    }
    match rng.below(24) {
        0..=3 => {
            let mut v = sv(&[if rng.chance(2) { "LPUSH" } else { "RPUSH" }, &key]);
            for _ in 0..1 + rng.below(3) {
                v.push(item(rng));
            }
            v
        }
        4 => {
            let mut v = sv(&[rng.pick(&["LPUSHX", "RPUSHX", "lpushx"]), &key]);
            v.push(item(rng));
            v
        }
        5 | 6 => {
            let mut v = sv(&[if rng.chance(2) { "LPOP" } else { "RPOP" }, &key]);
            if rng.chance(2) {
                v.push(rng.pick(&["0", "1", "2", "3", "7"]).to_string());
            } else if bad && rng.chance(8) {
                v.push("-1".into());
            }
            v
        }
        7 => sv(&["LLEN", &key]),
        8 | 9 => sv(&["LRANGE", &key, &idx(rng), &idx(rng)]),
        10 => sv(&["LINDEX", &key, &idx(rng)]),
        11 => sv(&["LSET", &key, &idx(rng), &item(rng)]),
        12 => sv(&["LREM", &key, &small_idx(rng), &item(rng)]),
        13 => {
            if rng.chance(3) {
                sv(&["LTRIM", &key, &idx(rng), &idx(rng)])
            } else {
                sv(&[
                    "LTRIM",
                    &key,
                    &rng.below(2).to_string(),
                    &format!("-{}", 1 + rng.below(3)),
                ])
            }
        }
        14 => sv(&[
            "LINSERT",
            &key,
            rng.pick(&["BEFORE", "AFTER", "before"]),
            &item(rng),
            &item(rng),
        ]),
        15 => {
            let mut v = sv(&["LPOS", &key, &item(rng)]);
            let mut opts = vec![];
            if rng.chance(2) {
                opts.push(sv(&["RANK", rng.pick(&["1", "2", "3", "-1", "-2"])]));
            }
            if rng.chance(2) {
                opts.push(sv(&["COUNT", rng.pick(&["0", "1", "2", "5"])]));
            }
            if rng.chance(3) {
                opts.push(sv(&["MAXLEN", rng.pick(&["0", "1", "3", "100"])]));
            }
            shuffle(rng, &mut opts);
            v.extend(opts.into_iter().flatten());
            v
        }
        16..=19 => {
            let Some(dst) = pick_dst(rng, m, &key, LIST_KEYS, Kind::List) else {
                return sv(&["LLEN", &key]);
            };
            if rng.chance(4) {
                sv(&["RPOPLPUSH", &key, &dst])
            } else {
                sv(&[
                    "LMOVE",
                    &key,
                    &dst,
                    rng.pick(&["LEFT", "RIGHT", "left"]),
                    rng.pick(&["LEFT", "RIGHT", "right"]),
                ])
            }
        }
        20 => sv(&["LRANGE", &key, "0", "-1"]),
        21 if bad => match rng.below(5) {
            0 => sv(&["LRANGE", &key, &bad_token(rng), "1"]),
            1 => sv(&["LINDEX", &key, &bad_token(rng)]),
            2 => sv(&["LSET", &key, &bad_token(rng), "a"]),
            3 => sv(&["LTRIM", &key, "0", &bad_token(rng)]),
            _ => sv(&["LINSERT", &key, "MIDDLE", "a", "b"]),
        },
        _ => sv(&["LRANGE", &key, &idx(rng), &idx(rng)]),
    }
}

fn gen_set(rng: &mut Rng, m: &Model, det: bool, bad: bool) -> Vec<String> {
    let key = key_in(rng, SET_KEYS);
    let many = |rng: &mut Rng, n: u64, base: &[&str]| {
        let mut v = sv(base);
        for _ in 0..n {
            v.push(member(rng));
        }
        v
    };
    match rng.below(26) {
        0..=3 => {
            let n = 1 + rng.below(3);
            many(rng, n, &["SADD", &key])
        }
        4 | 5 => {
            let n = 1 + rng.below(3);
            many(rng, n, &["SREM", &key])
        }
        6 => sv(&["SISMEMBER", &key, &member(rng)]),
        7 => {
            let n = 1 + rng.below(3);
            many(rng, n, &["SMISMEMBER", &key])
        }
        8 => sv(&["SMEMBERS", &key]),
        9 => sv(&["SCARD", &key]),
        10 if !det => {
            if rng.chance(2) {
                sv(&["SPOP", &key])
            } else {
                let c = if bad && rng.chance(8) {
                    "-1"
                } else {
                    rng.pick(&["0", "1", "2", "3", "9"])
                };
                sv(&["SPOP", &key, c])
            }
        }
        11 if !det => {
            if rng.chance(3) {
                sv(&["SRANDMEMBER", &key])
            } else {
                sv(&[
                    "SRANDMEMBER",
                    &key,
                    rng.pick(&["0", "1", "2", "4", "9", "-1", "-3", "-9"]),
                ])
            }
        }
        12 if !det => {
            let mut v = sv(&["SSCAN", &key, "0"]);
            if rng.chance(2) {
                v.extend(sv(&["MATCH", rng.pick(&["m*", "*1", "m?", "x*", "*"])]));
            }
            if rng.chance(3) {
                v.extend(sv(&["COUNT", rng.pick(&["1", "2", "100"])]));
            }
            v
        }
        13..=15 => {
            let Some(dst) = pick_dst(rng, m, &key, SET_KEYS, Kind::Set) else {
                return sv(&["SCARD", &key]);
            };
            sv(&["SMOVE", &key, &dst, &member(rng)])
        }
        16..=21 => {
            let op = rng.pick(&[
                "SINTER",
                "SUNION",
                "SDIFF",
                "SINTERSTORE",
                "SUNIONSTORE",
                "SDIFFSTORE",
            ]);
            let mut v = sv(&[op]);
            if op.ends_with("STORE") {
                v.push(key_in(rng, SET_KEYS));
            }
            for _ in 0..1 + rng.below(3) {
                v.push(key_in(rng, SET_KEYS));
            }
            v
        }
        22 | 23 => {
            let n = 1 + rng.below(3);
            let mut v = sv(&["SINTERCARD", &n.to_string()]);
            for _ in 0..n {
                v.push(key_in(rng, SET_KEYS));
            }
            if rng.chance(2) {
                v.extend(sv(&["LIMIT", rng.pick(&["0", "1", "2", "5"])]));
            }
            v
        }
        24 if bad => sv(&["SADD", &key])
            .into_iter()
            .chain([member(rng)])
            .collect(),
        _ => sv(&["SMEMBERS", &key]),
    }
}

fn gen_zrange(rng: &mut Rng, key: &str, bad: bool) -> Vec<String> {
    let with = rng.chance(2);
    let limit = |rng: &mut Rng| {
        sv(&[
            "LIMIT",
            rng.pick(&["-1", "0", "0", "1", "2", "5"]),
            rng.pick(&["-1", "0", "1", "2", "3", "100"]),
        ])
    };
    match rng.below(9) {
        0 | 1 => {
            let mut v = sv(&["ZRANGE", key, &idx(rng), &idx(rng)]);
            let mut opts = vec![];
            if rng.chance(2) {
                opts.push(sv(&["REV"]));
            }
            if with {
                opts.push(sv(&["WITHSCORES"]));
            }
            shuffle(rng, &mut opts);
            v.extend(opts.into_iter().flatten());
            v
        }
        2 => {
            let mut v = sv(&["ZREVRANGE", key, &idx(rng), &idx(rng)]);
            if with {
                v.push("WITHSCORES".into());
            }
            v
        }
        3 | 4 => {
            let mut v = sv(&["ZRANGE", key, &bound(rng), &bound(rng)]);
            let mut opts = vec![sv(&["BYSCORE"])];
            if rng.chance(2) {
                opts.push(sv(&["REV"]));
            }
            if rng.chance(2) {
                opts.push(limit(rng));
            }
            if with {
                opts.push(sv(&["WITHSCORES"]));
            }
            shuffle(rng, &mut opts);
            v.extend(opts.into_iter().flatten());
            v
        }
        5 | 6 => {
            let rev = rng.chance(2);
            let mut v = sv(&[
                if rev {
                    "ZREVRANGEBYSCORE"
                } else {
                    "ZRANGEBYSCORE"
                },
                key,
                &bound(rng),
                &bound(rng),
            ]);
            let mut opts = vec![];
            if rng.chance(2) {
                opts.push(limit(rng));
            }
            if with {
                opts.push(sv(&["WITHSCORES"]));
            }
            shuffle(rng, &mut opts);
            v.extend(opts.into_iter().flatten());
            v
        }
        7 if bad => match rng.below(3) {
            0 => sv(&["ZRANGE", key, "0", "-1", "LIMIT", "0", "1"]),
            1 => sv(&["ZRANGEBYSCORE", key, &bad_token(rng), "1"]),
            _ => sv(&["ZRANGE", key, "0", &bad_token(rng)]),
        },
        _ => sv(&["ZRANGE", key, "0", "-1", "WITHSCORES"]),
    }
}

fn gen_zset(rng: &mut Rng, det: bool, bad: bool) -> Vec<String> {
    let key = key_in(rng, ZSET_KEYS);
    match rng.below(34) {
        0..=6 => {
            let mut v = sv(&["ZADD", &key]);
            let flags: &[&str] = match rng.below(16) {
                0..=3 => &[],
                4 => &["NX"],
                5 => &["XX"],
                6 => &["GT"],
                7 => &["LT"],
                8 => &["CH"],
                9 => &["NX", "CH"],
                10 => &["XX", "CH"],
                11 => &["GT", "CH"],
                12 => &["LT", "CH"],
                13 => &["xx", "ch"],
                _ if bad => rng.pick_flags(),
                _ => &[],
            };
            let incr = rng.chance(5) && !flags.iter().any(|f| f.eq_ignore_ascii_case("CH"));
            v.extend(flags.iter().map(|f| f.to_string()));
            if incr {
                v.push("INCR".into());
            }
            let pairs = if incr { 1 } else { 1 + rng.below(3) };
            for _ in 0..pairs {
                v.push(rng.pick(&SCORES).to_string());
                v.push(member(rng));
            }
            v
        }
        7 | 8 => {
            let mut v = sv(&["ZREM", &key]);
            for _ in 0..1 + rng.below(2) {
                v.push(member(rng));
            }
            v
        }
        9 => sv(&["ZSCORE", &key, &member(rng)]),
        10 => {
            let mut v = sv(&["ZMSCORE", &key]);
            for _ in 0..1 + rng.below(3) {
                v.push(member(rng));
            }
            v
        }
        11 | 12 => {
            let by = if bad && rng.chance(8) {
                bad_token(rng)
            } else {
                rng.pick(&["1", "-1", "0.5", "2", "-3", "inf", "-inf", "0"])
                    .to_string()
            };
            sv(&["ZINCRBY", &key, &by, &member(rng)])
        }
        13 => sv(&["ZCARD", &key]),
        14 => sv(&["ZCOUNT", &key, &bound(rng), &bound(rng)]),
        15 | 16 => {
            let mut v = sv(&[rng.pick(&["ZRANK", "ZREVRANK"]), &key, &member(rng)]);
            if rng.chance(2) {
                v.push("WITHSCORE".into());
            }
            v
        }
        17..=21 => gen_zrange(rng, &key, bad),
        22 => sv(&["ZREMRANGEBYRANK", &key, &idx(rng), &idx(rng)]),
        23 => sv(&["ZREMRANGEBYSCORE", &key, &bound(rng), &bound(rng)]),
        24 | 25 => {
            let mut v = sv(&[if rng.chance(2) { "ZPOPMIN" } else { "ZPOPMAX" }, &key]);
            if rng.chance(2) {
                v.push(rng.pick(&["0", "1", "2", "9"]).to_string());
            } else if bad && rng.chance(8) {
                v.push("-1".into());
            }
            v
        }
        26 if !det => {
            let mut v = sv(&["ZSCAN", &key, "0"]);
            if rng.chance(2) {
                v.extend(sv(&["MATCH", rng.pick(&["m*", "*1", "m?", "x*", "*"])]));
            }
            v
        }
        27..=31 => {
            let op = if rng.chance(2) {
                "ZUNIONSTORE"
            } else {
                "ZINTERSTORE"
            };
            let n = 1 + rng.below(3) as usize;
            let mut v = sv(&[op, &key_in(rng, ZSET_KEYS), &n.to_string()]);
            for _ in 0..n {
                // Plain sets are valid sources too.
                let base = if rng.chance(5) { SET_KEYS } else { ZSET_KEYS };
                v.push(key_in(rng, base));
            }
            let mut opts = vec![];
            if rng.chance(2) {
                let mut w = sv(&["WEIGHTS"]);
                for _ in 0..n {
                    w.push(rng.pick(&["1", "2", "0.5", "3", "1", "inf"]).to_string());
                }
                opts.push(w);
            }
            if rng.chance(2) {
                opts.push(sv(&["AGGREGATE", rng.pick(&["SUM", "MIN", "MAX", "max"])]));
            }
            shuffle(rng, &mut opts);
            v.extend(opts.into_iter().flatten());
            v
        }
        _ => sv(&["ZRANGE", &key, "0", "-1", "WITHSCORES"]),
    }
}

impl Rng {
    fn pick_flags(&mut self) -> &'static [&'static str] {
        const BAD: [&[&str]; 3] = [&["NX", "XX"], &["GT", "LT"], &["NX", "GT"]];
        BAD[self.below(3) as usize]
    }
}

fn gen_misc(rng: &mut Rng, m: &Model, det: bool, bad: bool) -> Vec<String> {
    let skey = key_in(rng, STR_KEYS);
    let hkey = key_in(rng, HASH_KEYS);
    let any = format!("k{}", rng.below(6));
    let field = format!("f{}", rng.below(4));
    let val = rng
        .pick(&["10", "1.5", "-2", "abc", "0", "3", "hello", ""])
        .to_string();
    match rng.below(40) {
        0 | 1 => sv(&["SET", &skey, &val]),
        2 => sv(&["GET", &skey]),
        3 | 4 => {
            let by = if bad && rng.chance(6) {
                bad_token(rng)
            } else {
                rng.pick(&["1", "-1", "0.5", "1.5", "100", "-100", "1e1", ".5", "inf"])
                    .to_string()
            };
            sv(&["INCRBYFLOAT", &skey, &by])
        }
        5 | 6 => sv(&[
            rng.pick(&["GETRANGE", "SUBSTR"]),
            &skey,
            &small_idx(rng),
            &small_idx(rng),
        ]),
        7 | 8 => sv(&[
            "SETRANGE",
            &skey,
            &rng.below(9).to_string(),
            rng.pick(&["xy", "", "abc", "7"]),
        ]),
        9..=11 => {
            let mut v = sv(&["HSET", &hkey, &field, &val]);
            if rng.chance(3) {
                v.push(format!("f{}", rng.below(4)));
                v.push(rng.pick(&["1", "2.5", "z"]).to_string());
            }
            v
        }
        12 => sv(&["HGET", &hkey, &field]),
        13 | 14 => {
            let mut v = sv(&["HDEL", &hkey, &field]);
            if rng.chance(3) {
                v.push(format!("f{}", rng.below(4)));
            }
            v
        }
        15 => sv(&["HLEN", &hkey]),
        16 => sv(&[rng.pick(&["HKEYS", "HVALS", "HGETALL"]), &hkey]),
        17 => sv(&["HSTRLEN", &hkey, &field]),
        18 | 19 => {
            let by = rng.pick(&["1", "-1", "0.5", "1.5", "2", "inf"]);
            sv(&["HINCRBYFLOAT", &hkey, &field, by])
        }
        20 if !det => {
            let mut v = sv(&["HRANDFIELD", &hkey]);
            if rng.chance(4) {
                return v;
            }
            v.push(rng.pick(&["0", "1", "2", "9", "-1", "-5"]).to_string());
            if rng.chance(2) {
                v.push("WITHVALUES".into());
            }
            v
        }
        21 => sv(&["DEL", &any]),
        22 => sv(&["EXISTS", &any]),
        23 => sv(&["TYPE", &any]),
        24 => {
            let mut v = sv(&["TOUCH"]);
            for _ in 0..1 + rng.below(3) {
                v.push(format!("k{}", rng.below(6)));
            }
            v
        }
        25 | 26 => {
            let dst = format!("k{}", rng.below(6));
            sv(&[rng.pick(&["RENAME", "RENAME", "RENAMENX"]), &any, &dst])
        }
        27 | 28 => {
            let mut dst = format!("k{}", rng.below(6));
            if dst == any {
                dst = format!(
                    "k{}",
                    (rng.below(5) + 1 + any[1..].parse::<u64>().unwrap_or(0)) % 6
                );
            }
            let mut v = sv(&["COPY", &any, &dst]);
            if rng.chance(2) {
                v.push("REPLACE".into());
            }
            v
        }
        29 => {
            let mut v = sv(&["MSETNX"]);
            for _ in 0..1 + rng.below(2) {
                v.push(format!("k{}", rng.below(6)));
                v.push(val.clone());
            }
            v
        }
        30 if !det => sv(&["RANDOMKEY"]),
        31 => sv(&["OBJECT", rng.pick(&["ENCODING", "encoding"]), &any]),
        32..=34 => sv(&["PEXPIRE", &any, &ttl(rng)]),
        35 => sv(&["PERSIST", &any]),
        36 | 37 => sv(&["PTTL", &any]),
        38 => {
            // Collections would be left to rot otherwise.
            let k = m.keys.keys().next().cloned().unwrap_or_else(|| any.clone());
            sv(&["PEXPIRE", &k, &ttl(rng)])
        }
        _ => sv(&["SET", &skey, &val]),
    }
}

fn gen_op(rng: &mut Rng, m: &Model, det: bool, bad: bool) -> Vec<String> {
    match rng.below(100) {
        0..=24 => gen_list(rng, m, bad),
        25..=46 => gen_set(rng, m, det, bad),
        47..=76 => gen_zset(rng, det, bad),
        _ => gen_misc(rng, m, det, bad),
    }
}

// ---------------------------------------------------------------------
// The differential run
// ---------------------------------------------------------------------

/// SSCAN / ZSCAN followed to the end, as one synthetic reply.
fn scan_all(s: &Session, args: &[String]) -> Reply {
    let mut cursor = "0".to_string();
    let mut items = Vec::new();
    for _ in 0..10_000 {
        let mut call = args.to_vec();
        call[2] = cursor.clone();
        match s.execute(&call) {
            Reply::Array(mut parts) if parts.len() == 2 => {
                let Reply::Array(chunk) = parts.pop().expect("items") else {
                    return Reply::Error("SCAN items not an array".into());
                };
                cursor = text(&parts[0]);
                items.extend(chunk);
                if cursor == "0" {
                    return Reply::Array(vec![Reply::bulk("0"), Reply::Array(items)]);
                }
            }
            other => return other,
        }
    }
    Reply::Error("SCAN did not end".into())
}

fn exec(s: &Session, args: &[String]) -> Reply {
    if matches!(args[0].as_str(), "SSCAN" | "ZSCAN") {
        scan_all(s, args)
    } else {
        s.execute(args)
    }
}

fn check(ctx: &str, step: usize, args: &[String], got: &Reply, out: &Out, slack: i64) {
    if let Err(why) = verify(out, got, slack) {
        panic!("{ctx} step {step}: {args:?} gave {}; {why}", brief(got));
    }
}

fn verify_state(ctx: &str, step: usize, s: &Session, m: &mut Model, slack: i64) {
    m.purge();
    let want: BTreeSet<String> = m.keys.keys().cloned().collect();
    let keys: BTreeSet<String> = words(s.execute(&["KEYS", "*"])).into_iter().collect();
    assert_eq!(keys, want, "{ctx} step {step}: KEYS");
    assert_eq!(
        int(&s.execute(&["DBSIZE"])),
        want.len() as i64,
        "{ctx} step {step}: DBSIZE"
    );
    for (k, e) in m.keys.clone() {
        let probes: Vec<Vec<String>> = vec![match &e.val {
            V::Str(_) => sv(&["GET", &k]),
            V::Hash(_) => sv(&["HGETALL", &k]),
            V::List(_) => sv(&["LRANGE", &k, "0", "-1"]),
            V::Set(_) => sv(&["SMEMBERS", &k]),
            V::Z(_) => sv(&["ZRANGE", &k, "0", "-1", "WITHSCORES"]),
        }];
        for args in probes {
            let got = s.execute(&args);
            let want = match &e.val {
                V::Str(v) => Out::Exact(rb(v)),
                V::Hash(h) => Out::Pairs(h.iter().map(|(f, v)| (f.clone(), v.clone())).collect()),
                V::List(l) => Out::Exact(rbulks(l)),
                V::Set(set) => Out::Sorted(set.iter().cloned().collect()),
                V::Z(z) => Out::Exact(Rep::Arr(
                    z.iter()
                        .flat_map(|(sc, m)| [Rep::Bulk(m.clone()), Rep::Bulk(fscore(*sc))])
                        .collect(),
                )),
            };
            check(ctx, step, &args, &got, &want, slack);
        }
        let want_len = e.val.len();
        let len_cmd = match &e.val {
            V::Str(_) => "STRLEN",
            V::Hash(_) => "HLEN",
            V::List(_) => "LLEN",
            V::Set(_) => "SCARD",
            V::Z(_) => "ZCARD",
        };
        assert_eq!(
            int(&s.execute(&[len_cmd, &k])),
            want_len as i64,
            "{ctx} step {step}: {len_cmd} {k}"
        );
        let want_ttl = match e.deadline {
            None => Out::Exact(Rep::Int(-1)),
            Some(d) => Out::Pttl(d - m.now),
        };
        let args = sv(&["PTTL", &k]);
        check(ctx, step, &args, &s.execute(&args), &want_ttl, slack);
    }
}

/// Returns how many commands ran.
fn model_run(seed: u64, shards: usize, steps: usize) -> usize {
    let ctx = format!("seed {seed} shards {shards}");
    let db = open(cfg(shards, 4, 2));
    let sessions = [session(&db), session(&db)];
    let mut model = Model::default();
    let mut rng = Rng::new(seed);
    let started = Instant::now();
    let mut executed = 0;
    for step in 0..steps {
        // Past this much real time the 5 s margin of `ttl` would be gone.
        if started.elapsed().as_millis() > 4_000 {
            break;
        }
        let slack = started.elapsed().as_millis() as i64 + 5;
        let s = &sessions[rng.below(2) as usize];
        match rng.below(60) {
            0 => {
                let ms = (1 + rng.below(3)) * 10_000;
                db.advance_clock(ms);
                model.now += ms as i64;
                model.purge();
                if rng.chance(3) {
                    db.sweep_expired();
                }
                if rng.chance(3) {
                    db.expire_tick();
                }
            }
            1..=3 => {
                // A transaction of commands whose replies the model can
                // predict; a scratch copy of the model keeps the generator
                // in step with what the earlier commands will have done.
                let mut scratch = model.clone();
                let mut ops = Vec::new();
                for _ in 0..1 + rng.below(5) {
                    let args = gen_op(&mut rng, &scratch, true, false);
                    scratch.apply(&args, &Reply::Nil);
                    ops.push(args);
                }
                assert_eq!(s.execute(&["MULTI"]), Reply::Ok);
                for args in &ops {
                    assert_eq!(
                        s.execute(args),
                        Reply::Simple("QUEUED".into()),
                        "{ctx} step {step}: {args:?}"
                    );
                }
                let replies = array(s.execute(&["EXEC"]));
                assert_eq!(replies.len(), ops.len(), "{ctx} step {step}");
                for (args, got) in ops.iter().zip(&replies) {
                    let out = model.apply(args, got);
                    check(&ctx, step, args, got, &out, slack);
                    executed += 1;
                }
            }
            _ => {
                let args = gen_op(&mut rng, &model, false, true);
                let got = exec(s, &args);
                let out = model.apply(&args, &got);
                check(&ctx, step, &args, &got, &out, slack);
                executed += 1;
            }
        }
        if step % 400 == 0 {
            verify_state(&ctx, step, &sessions[0], &mut model, slack);
        }
    }
    let slack = started.elapsed().as_millis() as i64 + 5;
    verify_state(&ctx, steps, &sessions[0], &mut model, slack);
    executed
}

#[test]
fn random_commands_match_the_model() {
    guarded("collections-model", 240, || {
        let mut total = 0;
        for seed in 1..=5u64 {
            for shards in [1, 5, 64] {
                total += model_run(seed * 7919 + shards as u64, shards, 22_000);
            }
        }
        assert!(total > 100_000, "only {total} commands ran");
    });
}

// ---------------------------------------------------------------------
// Edge-case tables
// ---------------------------------------------------------------------

fn fresh() -> (Db, Session) {
    let db = open(conf(8));
    let s = session(&db);
    (db, s)
}

#[test]
fn list_edge_cases() {
    guarded("list-edges", 60, || {
        let (_db, s) = fresh();
        table(
            &s,
            &[
                ("RPUSH l a b c", ":3"),
                ("LRANGE l 0 -1", "[a b c]"),
                ("LRANGE l -100 100", "[a b c]"),
                ("LRANGE l 5 10", "[]"),
                ("LRANGE l -1 -100", "[]"),
                ("LRANGE l 0 -4", "[]"),
                ("LRANGE l 1 -2", "[b]"),
                ("LRANGE l -3 -3", "[a]"),
                ("LRANGE l 2 1", "[]"),
                ("LRANGE l 0 9223372036854775807", "[a b c]"),
                ("LRANGE l -9223372036854775808 -1", "[a b c]"),
                (
                    "LRANGE l 0 x",
                    "-ERR value is not an integer or out of range",
                ),
                (
                    "LRANGE l 0 9223372036854775808",
                    "-ERR value is not an integer or out of range",
                ),
                ("LRANGE nokey 0 -1", "[]"),
                ("LINDEX l 3", "nil"),
                ("LINDEX l -4", "nil"),
                ("LINDEX l -3", "a"),
                ("LINDEX l 9223372036854775807", "nil"),
                ("LINDEX l -9223372036854775808", "nil"),
                ("LINDEX nokey 0", "nil"),
                ("LINDEX l x", "-ERR value is not an integer or out of range"),
                ("LSET l -1 z", "OK"),
                ("LSET l -4 q", "-ERR index out of range"),
                ("LSET l 3 q", "-ERR index out of range"),
                ("LSET l 0 y", "OK"),
                ("LRANGE l 0 -1", "[y b z]"),
                ("LSET nokey 0 a", "-ERR no such key"),
                ("LSET l x a", "-ERR value is not an integer or out of range"),
                ("LLEN nokey", ":0"),
                ("LLEN l", ":3"),
                // Pops
                ("LPOP l 0", "[]"),
                ("RPOP l 0", "[]"),
                ("LPOP nokey 0", "nilarray"),
                ("RPOP nokey", "nil"),
                ("LPOP nokey 3", "nilarray"),
                ("LPOP l -1", "-ERR value is out of range, must be positive"),
                ("LPOP l abc", "-ERR value is not an integer or out of range"),
                ("LPOP l 2", "[y b]"),
                ("RPOP l 5", "[z]"),
                ("EXISTS l", ":0"),
                ("LPOP l 1", "nilarray"),
                ("LPOP l", "nil"),
                // Push order and the X variants
                ("LPUSH q 1 2 3", ":3"),
                ("RPUSH q 4 5", ":5"),
                ("LRANGE q 0 -1", "[3 2 1 4 5]"),
                ("LPUSHX q 0", ":6"),
                ("RPUSHX q 6", ":7"),
                ("LPUSHX nokey 0", ":0"),
                ("RPUSHX nokey 0", ":0"),
                ("EXISTS nokey", ":0"),
                // LREM
                ("RPUSH r a b a c a", ":5"),
                ("LREM r 2 a", ":2"),
                ("LRANGE r 0 -1", "[b c a]"),
                ("RPUSH r a b a", ":6"),
                ("LREM r -2 a", ":2"),
                ("LRANGE r 0 -1", "[b c a b]"),
                ("LREM r 0 b", ":2"),
                ("LRANGE r 0 -1", "[c a]"),
                ("LREM r 5 zz", ":0"),
                ("LREM nokey 0 a", ":0"),
                ("LREM r x a", "-ERR value is not an integer or out of range"),
                ("LREM r -9223372036854775808 c", ":1"),
                ("LREM r 0 a", ":1"),
                ("EXISTS r", ":0"),
                // LTRIM
                ("RPUSH t 1 2 3 4 5", ":5"),
                ("LTRIM t 1 -2", "OK"),
                ("LRANGE t 0 -1", "[2 3 4]"),
                ("LTRIM t -100 100", "OK"),
                ("LRANGE t 0 -1", "[2 3 4]"),
                ("LTRIM t 1 1", "OK"),
                ("LRANGE t 0 -1", "[3]"),
                ("LTRIM t 1 0", "OK"),
                ("EXISTS t", ":0"),
                ("LTRIM nokey 0 -1", "OK"),
                ("RPUSH t 1 2 3", ":3"),
                ("LTRIM t 5 10", "OK"),
                ("EXISTS t", ":0"),
                ("RPUSH t 1 2 3", ":3"),
                ("LTRIM t 0 -4", "OK"),
                ("EXISTS t", ":0"),
                ("RPUSH t 1 2 3", ":3"),
                (
                    "LTRIM t x 1",
                    "-ERR value is not an integer or out of range",
                ),
                ("LTRIM t -9223372036854775808 9223372036854775807", "OK"),
                ("LRANGE t 0 -1", "[1 2 3]"),
                // LINSERT
                ("RPUSH i a b c", ":3"),
                ("LINSERT i BEFORE b X", ":4"),
                ("LINSERT i AFTER c Y", ":5"),
                ("LINSERT i before a Z", ":6"),
                ("LRANGE i 0 -1", "[Z a X b c Y]"),
                ("LINSERT i AFTER nope Q", ":-1"),
                ("LINSERT i MIDDLE a Q", "-ERR syntax error"),
                ("LINSERT nokey BEFORE a b", ":0"),
                // LPOS
                ("RPUSH p a b c a b a", ":6"),
                ("LPOS p a", ":0"),
                ("LPOS p a RANK 2", ":3"),
                ("LPOS p a RANK -1", ":5"),
                ("LPOS p a RANK -3", ":0"),
                ("LPOS p a RANK 4", "nil"),
                ("LPOS p a COUNT 0", "[:0 :3 :5]"),
                ("LPOS p a COUNT 2", "[:0 :3]"),
                ("LPOS p a RANK 2 COUNT 0", "[:3 :5]"),
                ("LPOS p a MAXLEN 3 COUNT 0", "[:0]"),
                ("LPOS p a RANK -1 MAXLEN 2 COUNT 0", "[:5]"),
                ("LPOS p zz", "nil"),
                ("LPOS p zz COUNT 0", "[]"),
                ("LPOS nokey a", "nil"),
                ("LPOS nokey a COUNT 0", "[]"),
                ("LPOS p a COUNT -1", "-ERR COUNT can't be negative"),
                ("LPOS p a MAXLEN -1", "-ERR MAXLEN can't be negative"),
                ("LPOS p a BOGUS 1", "-ERR syntax error"),
                ("LPOS p a RANK", "-ERR syntax error"),
                (
                    "LPOS p a RANK x",
                    "-ERR value is not an integer or out of range",
                ),
            ],
        );
        let rank_zero = s.execute(&["LPOS", "p", "a", "RANK", "0"]);
        assert!(
            matches!(&rank_zero, Reply::Error(e) if e.starts_with("ERR RANK can't be zero")),
            "{}",
            brief(&rank_zero)
        );
    });
}

#[test]
fn lmove_edge_cases() {
    guarded("lmove-edges", 60, || {
        let (_db, s) = fresh();
        table(
            &s,
            &[
                ("RPUSH src 1 2 3", ":3"),
                ("LMOVE src dst LEFT RIGHT", "1"),
                ("LMOVE src dst RIGHT LEFT", "3"),
                ("LRANGE dst 0 -1", "[3 1]"),
                ("LRANGE src 0 -1", "[2]"),
                ("LMOVE src dst bogus left", "-ERR syntax error"),
                ("LMOVE src dst left bogus", "-ERR syntax error"),
                ("LRANGE src 0 -1", "[2]"),
                ("LMOVE nokey dst LEFT LEFT", "nil"),
                ("RPOPLPUSH nokey dst", "nil"),
                ("RPOPLPUSH dst src", "1"),
                ("LRANGE src 0 -1", "[1 2]"),
                ("LRANGE dst 0 -1", "[3]"),
                // Moving the last element deletes the source.
                ("LMOVE dst src LEFT RIGHT", "3"),
                ("EXISTS dst", ":0"),
                ("TYPE dst", "+none"),
                // A wrong-typed destination stops the move before the pop.
                ("SET str x", "OK"),
                ("LMOVE src str LEFT LEFT", &format!("-{WRONGTYPE}")),
                ("LRANGE src 0 -1", "[1 2 3]"),
                ("LMOVE str src LEFT LEFT", &format!("-{WRONGTYPE}")),
                // Rotation in place.
                ("LMOVE src src LEFT RIGHT", "1"),
                ("LRANGE src 0 -1", "[2 3 1]"),
                ("LMOVE src src RIGHT LEFT", "1"),
                ("LRANGE src 0 -1", "[1 2 3]"),
                ("LMOVE src src LEFT LEFT", "1"),
                ("LRANGE src 0 -1", "[1 2 3]"),
                ("RPOPLPUSH src src", "3"),
                ("LRANGE src 0 -1", "[3 1 2]"),
                ("RPUSH one x", ":1"),
                ("LMOVE one one LEFT RIGHT", "x"),
                ("LRANGE one 0 -1", "[x]"),
                ("RPOPLPUSH one one", "x"),
                ("LRANGE one 0 -1", "[x]"),
            ],
        );
    });
}

#[test]
fn move_in_place_keeps_the_deadline() {
    guarded("move-in-place-ttl", 30, || {
        let (_db, s) = fresh();
        s.execute(&["RPUSH", "l", "x"]);
        s.execute(&["PEXPIRE", "l", "100000"]);
        assert_eq!(x(&s, "LMOVE l l LEFT RIGHT"), "x");
        assert_eq!(x(&s, "EXISTS l"), ":1");
        assert!(
            pttl(&s, "l") > 0,
            "LMOVE l l dropped the deadline of a one-element list"
        );
        assert_eq!(x(&s, "RPOPLPUSH l l"), "x");
        assert!(pttl(&s, "l") > 0, "RPOPLPUSH l l dropped the deadline");

        s.execute(&["SADD", "s", "m"]);
        s.execute(&["PEXPIRE", "s", "100000"]);
        assert_eq!(x(&s, "SMOVE s s m"), ":1");
        assert_eq!(x(&s, "SISMEMBER s m"), ":1");
        assert!(
            pttl(&s, "s") > 0,
            "SMOVE s s m dropped the deadline of a one-member set"
        );
    });
}

#[test]
fn move_with_a_missing_source_ignores_the_destination_type() {
    guarded("move-missing-src", 30, || {
        let (_db, s) = fresh();
        s.execute(&["SET", "str", "x"]);
        // Redis looks at the source first: nothing to move, nothing to say
        // about the destination.
        assert_eq!(x(&s, "LMOVE nokey str LEFT LEFT"), "nil");
        assert_eq!(x(&s, "RPOPLPUSH nokey str"), "nil");
        assert_eq!(x(&s, "SMOVE nokey str a"), ":0");
        // A destination of the wrong type does matter once there is a source.
        s.execute(&["SADD", "s", "a"]);
        assert_eq!(x(&s, "SMOVE s str a"), format!("-{WRONGTYPE}"));
        assert_eq!(x(&s, "SMOVE s str zz"), format!("-{WRONGTYPE}"));
        assert_eq!(x(&s, "SISMEMBER s a"), ":1");
    });
}

#[test]
fn set_edge_cases() {
    guarded("set-edges", 60, || {
        let (_db, s) = fresh();
        table_sorted(
            &s,
            &[
                ("SADD s a a b", ":2"),
                ("SADD s a", ":0"),
                ("SREM s a a x", ":1"),
                ("SMEMBERS s", "[b]"),
                ("SADD s c d e", ":3"),
                ("SPOP s 0", "[]"),
                ("SPOP nokey 0", "[]"),
                ("SPOP nokey", "nil"),
                ("SPOP nokey 3", "[]"),
                ("SPOP s -1", "-ERR value is out of range, must be positive"),
                ("SPOP s x", "-ERR value is not an integer or out of range"),
                ("SRANDMEMBER nokey", "nil"),
                ("SRANDMEMBER nokey 5", "[]"),
                ("SRANDMEMBER nokey -5", "[]"),
                ("SRANDMEMBER s 0", "[]"),
                (
                    "SRANDMEMBER s x",
                    "-ERR value is not an integer or out of range",
                ),
                ("SCARD s", ":4"),
                ("SCARD nokey", ":0"),
                ("SISMEMBER s b", ":1"),
                ("SISMEMBER nokey a", ":0"),
                ("SMISMEMBER nokey a b", "[:0 :0]"),
                ("SMISMEMBER s b q b", "[:1 :0 :1]"),
                ("SSCAN s 0 MATCH x*", "[0 []]"),
                ("SSCAN s abc", "-ERR invalid cursor"),
                ("SSCAN s 0 COUNT 0", "-ERR syntax error"),
                (
                    "SSCAN s 0 COUNT x",
                    "-ERR value is not an integer or out of range",
                ),
                ("SSCAN s 0 BOGUS 1", "-ERR syntax error"),
                ("SSCAN nokey 0", "[0 []]"),
                // Several sets
                ("SADD a 1 2 3", ":3"),
                ("SADD b 2 3 4", ":3"),
                ("SADD c 3 4 5", ":3"),
                ("SINTER a b", "[2 3]"),
                ("SINTER a b c", "[3]"),
                ("SUNION a b c", "[1 2 3 4 5]"),
                ("SDIFF a b", "[1]"),
                ("SDIFF a b c", "[1]"),
                ("SDIFF b a c", "[]"),
                ("SINTER a nokey", "[]"),
                ("SUNION a nokey", "[1 2 3]"),
                ("SDIFF nokey a", "[]"),
                ("SDIFF a nokey", "[1 2 3]"),
                ("SINTER a", "[1 2 3]"),
                ("SDIFF a a", "[]"),
                ("SINTER a a", "[1 2 3]"),
                ("SUNION a a", "[1 2 3]"),
                ("SINTERSTORE d a b", ":2"),
                ("SMEMBERS d", "[2 3]"),
                ("SINTERSTORE d a nokey", ":0"),
                ("EXISTS d", ":0"),
                ("SUNIONSTORE d a b c", ":5"),
                ("SDIFFSTORE d a a", ":0"),
                ("EXISTS d", ":0"),
                ("SINTERSTORE a a b", ":2"),
                ("SMEMBERS a", "[2 3]"),
                ("SUNIONSTORE a a c", ":4"),
                ("SMEMBERS a", "[2 3 4 5]"),
                ("SDIFFSTORE a a b", ":1"),
                ("SMEMBERS a", "[5]"),
                ("SDIFFSTORE a a nokey", ":1"),
                ("SET str x", "OK"),
                ("SUNIONSTORE str a", ":1"),
                ("TYPE str", "+set"),
                // SINTERCARD
                ("SADD x 1 2 3 4 5", ":5"),
                ("SADD y 4 5 6", ":3"),
                ("SINTERCARD 2 x y", ":2"),
                ("SINTERCARD 2 x y LIMIT 1", ":1"),
                ("SINTERCARD 2 x y LIMIT 0", ":2"),
                ("SINTERCARD 2 x y LIMIT 5", ":2"),
                ("SINTERCARD 1 x", ":5"),
                ("SINTERCARD 2 x nokey", ":0"),
                ("SINTERCARD 0 x", "-ERR numkeys should be greater than 0"),
                (
                    "SINTERCARD 3 x y",
                    "-ERR Number of keys can't be greater than number of args",
                ),
                ("SINTERCARD 2 x y BOGUS 1", "-ERR syntax error"),
                ("SINTERCARD 2 x y LIMIT", "-ERR syntax error"),
                // SMOVE
                ("SADD m1 a b", ":2"),
                ("SMOVE m1 m2 a", ":1"),
                ("SMOVE m1 m2 a", ":0"),
                ("SMOVE m1 m1 b", ":1"),
                ("SMOVE m1 m1 zz", ":0"),
                ("SMEMBERS m1", "[b]"),
                ("SMOVE m1 m3 b", ":1"),
                ("EXISTS m1", ":0"),
                ("SMEMBERS m3", "[b]"),
                ("SMOVE m2 m3 b", ":0"),
                ("SMOVE m2 m3 a", ":1"),
                ("SMEMBERS m3", "[a b]"),
            ],
        );
        assert_eq!(
            x(&s, "SINTERCARD 2 x y LIMIT -1"),
            "-ERR LIMIT can't be negative"
        );
    });
}

#[test]
fn zset_flag_and_infinity_edge_cases() {
    guarded("zset-flags", 60, || {
        let (_db, s) = fresh();
        table(
            &s,
            &[
                ("ZADD za 1 a 2 b", ":2"),
                ("ZADD za NX 5 a 5 c", ":1"),
                ("ZADD za XX 9 a 9 d", ":0"),
                ("ZSCORE za a", "9"),
                ("ZSCORE za d", "nil"),
                ("ZADD za XX CH 10 a 10 zz", ":1"),
                ("ZADD za GT 5 a", ":0"),
                ("ZSCORE za a", "10"),
                ("ZADD za GT CH 11 a 1 b", ":1"),
                ("ZADD za LT CH 0 a 5 b", ":1"),
                ("ZRANGE za 0 -1 WITHSCORES", "[a 0 b 2 c 5]"),
                ("ZADD za LT 7 n1", ":1"),
                ("ZADD za GT 7 n2", ":1"),
                ("ZADD za INCR 1 a", "1"),
                ("ZADD za INCR 1 nobody", "1"),
                ("ZADD za NX INCR 1 a", "nil"),
                ("ZADD za XX INCR 1 ghost", "nil"),
                ("EXISTS ghost", ":0"),
                ("ZADD za GT INCR -1 a", "nil"),
                ("ZADD za LT INCR -1 a", "0"),
                ("ZADD za LT INCR 1 a", "nil"),
                ("ZADD za GT INCR 1 a", "1"),
                ("ZADD za INCR 0 a", "1"),
                (
                    "ZADD za INCR 1 a 2 b",
                    "-ERR INCR option supports a single increment-element pair",
                ),
                (
                    "ZADD za NX XX 1 a",
                    "-ERR XX and NX options at the same time are not compatible",
                ),
                (
                    "ZADD za GT LT 1 a",
                    "-ERR GT, LT, and/or NX options at the same time are not compatible",
                ),
                (
                    "ZADD za NX GT 1 a",
                    "-ERR GT, LT, and/or NX options at the same time are not compatible",
                ),
                (
                    "ZADD za NX LT 1 a",
                    "-ERR GT, LT, and/or NX options at the same time are not compatible",
                ),
                (
                    "ZADD za 1",
                    "-ERR wrong number of arguments for 'zadd' command",
                ),
                ("ZADD za 1 a 2", "-ERR syntax error"),
                ("ZADD za nan a", "-ERR value is not a valid float"),
                ("ZADD za abc a", "-ERR value is not a valid float"),
                ("ZADD za 5 x abc y", "-ERR value is not a valid float"),
                ("ZSCORE za x", "nil"),
                ("ZADD za 1e400 big", "-ERR value is not a valid float"),
                ("ZSCORE za big", "nil"),
                // Duplicates inside one command
                ("ZADD dup 1 a 2 a", ":1"),
                ("ZSCORE dup a", "2"),
                ("ZADD dup CH 3 a 4 a", ":2"),
                ("ZADD dup CH 4 a 4 a", ":0"),
                ("ZREM dup a a", ":1"),
                ("EXISTS dup", ":0"),
                // Spelling of numbers
                ("ZADD fz 1e1 a .5 b 5. c +3 d -.5 e", ":5"),
                ("ZRANGE fz 0 -1 WITHSCORES", "[e -0.5 b 0.5 d 3 c 5 a 10]"),
                // Infinities
                ("ZADD zi -inf a +inf b inf c 0 d", ":4"),
                ("ZRANGE zi 0 -1 WITHSCORES", "[a -inf d 0 b inf c inf]"),
                ("ZSCORE zi a", "-inf"),
                ("ZSCORE zi b", "inf"),
                ("ZRANGEBYSCORE zi -inf +inf", "[a d b c]"),
                ("ZRANGEBYSCORE zi (-inf +inf", "[d b c]"),
                ("ZRANGEBYSCORE zi -inf (+inf", "[a d]"),
                ("ZRANGEBYSCORE zi (-inf (+inf", "[d]"),
                ("ZRANGEBYSCORE zi +inf +inf", "[b c]"),
                ("ZRANGEBYSCORE zi -inf -inf", "[a]"),
                ("ZRANGEBYSCORE zi (inf inf", "[]"),
                ("ZCOUNT zi -inf +inf", ":4"),
                ("ZCOUNT zi (inf +inf", ":0"),
                ("ZCOUNT zi (-inf (inf", ":1"),
                ("ZINCRBY zi -inf b", &format!("-{NAN_SCORE}")),
                ("ZINCRBY zi inf a", &format!("-{NAN_SCORE}")),
                ("ZADD zi INCR -inf b", &format!("-{NAN_SCORE}")),
                ("ZSCORE zi b", "inf"),
                ("ZINCRBY zi 1 b", "inf"),
                ("ZINCRBY zi -inf d", "-inf"),
                ("ZINCRBY zi abc d", "-ERR value is not a valid float"),
                ("ZRANGE zi 0 -1 WITHSCORES", "[a -inf d -inf b inf c inf]"),
                ("ZINCRBY newz 1.5 m", "1.5"),
                ("ZINCRBY newz 1.5 m", "3"),
                ("ZINCRBY newz -3 m", "0"),
                ("ZSCORE newz m", "0"),
            ],
        );
        assert_eq!(
            s.execute(&["ZADD", "za", "", "e"]),
            Reply::Error(NOT_FLOAT.into())
        );
    });
}

#[test]
fn zset_range_edge_cases() {
    guarded("zset-ranges", 60, || {
        let (_db, s) = fresh();
        table(
            &s,
            &[
                ("ZADD zr 1 a 2 b 3 c 4 d 5 e", ":5"),
                ("ZRANGE zr 0 -1", "[a b c d e]"),
                ("ZRANGE zr -2 -1", "[d e]"),
                ("ZRANGE zr 1 100", "[b c d e]"),
                ("ZRANGE zr 3 1", "[]"),
                ("ZRANGE zr -100 1", "[a b]"),
                ("ZRANGE zr 0 -6", "[]"),
                ("ZRANGE zr 5 10", "[]"),
                ("ZRANGE zr 0 9223372036854775807", "[a b c d e]"),
                ("ZRANGE zr -9223372036854775808 -1", "[a b c d e]"),
                ("ZRANGE zr 0 -1 REV", "[e d c b a]"),
                ("ZRANGE zr 0 1 REV", "[e d]"),
                ("ZRANGE zr -1 -1 REV", "[a]"),
                ("ZRANGE zr 0 1 REV WITHSCORES", "[e 5 d 4]"),
                ("ZREVRANGE zr 0 1", "[e d]"),
                ("ZREVRANGE zr 1 0", "[]"),
                ("ZREVRANGE zr -2 -1 WITHSCORES", "[b 2 a 1]"),
                ("ZRANGE nokey 0 -1", "[]"),
                ("ZRANGE zr 2 4 BYSCORE", "[b c d]"),
                ("ZRANGE zr (2 4 BYSCORE", "[c d]"),
                ("ZRANGE zr 2 (4 BYSCORE", "[b c]"),
                ("ZRANGE zr (2 (4 BYSCORE", "[c]"),
                ("ZRANGE zr 4 2 BYSCORE", "[]"),
                ("ZRANGE zr 4 2 BYSCORE REV", "[d c b]"),
                ("ZRANGE zr (4 (2 BYSCORE REV", "[c]"),
                ("ZRANGE zr +inf -inf BYSCORE REV", "[e d c b a]"),
                ("ZRANGE zr -inf +inf BYSCORE LIMIT 1 2", "[b c]"),
                ("ZRANGE zr +inf -inf BYSCORE REV LIMIT 1 2", "[d c]"),
                ("ZRANGE zr -inf +inf BYSCORE LIMIT 0 0", "[]"),
                ("ZRANGE zr -inf +inf BYSCORE LIMIT 10 5", "[]"),
                ("ZRANGE zr -inf +inf BYSCORE LIMIT 3 -1", "[d e]"),
                ("ZRANGE zr -inf +inf BYSCORE LIMIT -1 5", "[]"),
                ("ZRANGE zr -inf +inf BYSCORE LIMIT 0 2 WITHSCORES", "[a 1 b 2]"),
                ("ZRANGE zr -inf +inf WITHSCORES LIMIT 0 2 BYSCORE", "[a 1 b 2]"),
                ("ZRANGEBYSCORE zr 2 4", "[b c d]"),
                ("ZRANGEBYSCORE zr 2 4 WITHSCORES", "[b 2 c 3 d 4]"),
                ("ZRANGEBYSCORE zr (1 (5", "[b c d]"),
                ("ZRANGEBYSCORE zr -inf +inf LIMIT 1 1", "[b]"),
                ("ZREVRANGEBYSCORE zr 4 2", "[d c b]"),
                ("ZREVRANGEBYSCORE zr +inf (3", "[e d]"),
                ("ZREVRANGEBYSCORE zr +inf -inf LIMIT 2 2 WITHSCORES", "[c 3 b 2]"),
                ("ZREVRANGEBYSCORE zr 2 4", "[]"),
                ("ZRANGEBYSCORE zr 2 4 LIMIT 1", "-ERR syntax error"),
                ("ZRANGEBYSCORE zr 2 4 LIMIT x 1", "-ERR value is not an integer or out of range"),
                ("ZRANGEBYSCORE zr a 4", "-ERR min or max is not a float"),
                // Valkey 9.1 reads a bare "(" as an exclusive zero.
                ("ZRANGEBYSCORE zr ( 4", "[a b c d]"),
                ("ZRANGEBYSCORE zr 2 4 BOGUS", "-ERR syntax error"),
                (
                    "ZRANGE zr 0 -1 LIMIT 0 1",
                    "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
                ),
                ("ZRANGE zr 0 -1 BOGUS", "-ERR syntax error"),
                ("ZRANGE zr a b", "-ERR value is not an integer or out of range"),
                ("ZCOUNT zr 2 4", ":3"),
                ("ZCOUNT zr (2 (4", ":1"),
                ("ZCOUNT zr 4 2", ":0"),
                ("ZCOUNT zr -inf +inf", ":5"),
                ("ZCOUNT nokey 0 1", ":0"),
                ("ZCOUNT zr x 1", "-ERR min or max is not a float"),
                ("ZRANK zr a", ":0"),
                ("ZRANK zr e", ":4"),
                ("ZREVRANK zr e", ":0"),
                ("ZREVRANK zr a", ":4"),
                ("ZRANK zr x", "nil"),
                ("ZRANK nokey a", "nil"),
                ("ZRANK zr c WITHSCORE", "[:2 3]"),
                ("ZREVRANK zr c WITHSCORE", "[:2 3]"),
                ("ZRANK zr x WITHSCORE", "nilarray"),
                ("ZRANK zr c BOGUS", "-ERR syntax error"),
                ("ZMSCORE zr a x e", "[1 nil 5]"),
                ("ZMSCORE nokey a", "[nil]"),
                // Removal
                ("ZADD zd 1 a 2 b 3 c 4 d 5 e", ":5"),
                ("ZREMRANGEBYRANK zd 1 0", ":0"),
                ("ZREMRANGEBYRANK zd 10 20", ":0"),
                ("ZREMRANGEBYRANK zd -2 -1", ":2"),
                ("ZREMRANGEBYRANK zd 0 0", ":1"),
                ("ZRANGE zd 0 -1", "[b c]"),
                ("ZREMRANGEBYSCORE zd (2 3", ":1"),
                ("ZREMRANGEBYSCORE zd 5 1", ":0"),
                ("ZREMRANGEBYSCORE zd -inf +inf", ":1"),
                ("EXISTS zd", ":0"),
                ("ZREMRANGEBYRANK nokey 0 -1", ":0"),
                ("ZREMRANGEBYSCORE zd x 1", "-ERR min or max is not a float"),
                // Pops
                ("ZADD zp 1 a 2 b 3 c", ":3"),
                ("ZPOPMIN zp 0", "[]"),
                ("ZPOPMIN nokey", "[]"),
                ("ZPOPMAX nokey 3", "[]"),
                ("ZPOPMIN zp -1", "-ERR value is out of range, must be positive"),
                ("ZPOPMIN zp x", "-ERR value is not an integer or out of range"),
                ("ZPOPMAX zp", "[c 3]"),
                ("ZPOPMIN zp 5", "[a 1 b 2]"),
                ("EXISTS zp", ":0"),
            ],
        );
    });
}

#[test]
fn zset_store_edge_cases() {
    guarded("zset-store", 60, || {
        let (_db, s) = fresh();
        table(
            &s,
            &[
                ("ZADD s1 1 a 2 b 3 c", ":3"),
                ("ZADD s2 10 b 20 c 30 d", ":3"),
                ("ZUNIONSTORE u 2 s1 s2", ":4"),
                ("ZRANGE u 0 -1 WITHSCORES", "[a 1 b 12 c 23 d 30]"),
                ("ZINTERSTORE i 2 s1 s2", ":2"),
                ("ZRANGE i 0 -1 WITHSCORES", "[b 12 c 23]"),
                ("ZUNIONSTORE u 2 s1 s2 WEIGHTS 2 3", ":4"),
                ("ZRANGE u 0 -1 WITHSCORES", "[a 2 b 34 c 66 d 90]"),
                ("ZUNIONSTORE u 2 s1 s2 AGGREGATE MIN", ":4"),
                ("ZRANGE u 0 -1 WITHSCORES", "[a 1 b 2 c 3 d 30]"),
                ("ZUNIONSTORE u 2 s1 s2 aggregate max", ":4"),
                ("ZRANGE u 0 -1 WITHSCORES", "[a 1 b 10 c 20 d 30]"),
                ("ZINTERSTORE i 2 s1 s2 WEIGHTS 1 0.5 AGGREGATE SUM", ":2"),
                ("ZRANGE i 0 -1 WITHSCORES", "[b 7 c 13]"),
                ("ZUNIONSTORE u 2 s1 nokey", ":3"),
                ("ZINTERSTORE i 2 s1 nokey", ":0"),
                ("EXISTS i", ":0"),
                ("ZUNIONSTORE u 1 s1", ":3"),
                ("ZRANGE u 0 -1 WITHSCORES", "[a 1 b 2 c 3]"),
                ("ZUNIONSTORE u 2 s1 s1", ":3"),
                ("ZRANGE u 0 -1 WITHSCORES", "[a 2 b 4 c 6]"),
                ("ZINTERSTORE u 2 s1 s1", ":3"),
                ("ZRANGE u 0 -1 WITHSCORES", "[a 2 b 4 c 6]"),
                // Destination is a source
                ("ZUNIONSTORE s1 2 s1 s2", ":4"),
                ("ZRANGE s1 0 -1 WITHSCORES", "[a 1 b 12 c 23 d 30]"),
                ("ZINTERSTORE s1 1 s1", ":4"),
                ("ZINTERSTORE s1 2 s1 s2 WEIGHTS 0 1", ":3"),
                ("ZRANGE s1 0 -1 WITHSCORES", "[b 10 c 20 d 30]"),
                // Plain sets count as scores of 1
                ("SADD ps x y", ":2"),
                ("ZUNIONSTORE u 2 ps s2", ":5"),
                ("ZRANGE u 0 -1 WITHSCORES", "[x 1 y 1 b 10 c 20 d 30]"),
                ("ZINTERSTORE u 2 ps s2", ":0"),
                // Options and errors
                ("ZUNIONSTORE u 2 s1", "-ERR syntax error"),
                (
                    "ZUNIONSTORE u 0 s1",
                    "-ERR at least 1 input key is needed for 'zunionstore' command",
                ),
                (
                    "ZINTERSTORE u 0 s1",
                    "-ERR at least 1 input key is needed for 'zinterstore' command",
                ),
                ("ZUNIONSTORE u 2 s1 s2 WEIGHTS 1", "-ERR syntax error"),
                (
                    "ZUNIONSTORE u 2 s1 s2 WEIGHTS 1 x",
                    "-ERR weight value is not a float",
                ),
                ("ZUNIONSTORE u 2 s1 s2 AGGREGATE BOGUS", "-ERR syntax error"),
                ("ZUNIONSTORE u 2 s1 s2 BOGUS", "-ERR syntax error"),
                (
                    "ZUNIONSTORE u x s1",
                    "-ERR value is not an integer or out of range",
                ),
                ("SET str v", "OK"),
                ("ZUNIONSTORE u 2 s1 str", &format!("-{WRONGTYPE}")),
                // Zero weights, infinities
                ("ZADD zz +inf a 1 b", ":2"),
                ("ZUNIONSTORE o 1 zz WEIGHTS 0", ":2"),
                ("ZRANGE o 0 -1 WITHSCORES", "[a 0 b 0]"),
                ("ZADD q1 inf a", ":1"),
                ("ZADD q2 -inf a", ":1"),
                ("ZUNIONSTORE o 2 q1 q2", ":1"),
                ("ZRANGE o 0 -1 WITHSCORES", "[a 0]"),
                ("ZUNIONSTORE o 2 q1 q2 AGGREGATE MAX", ":1"),
                ("ZRANGE o 0 -1 WITHSCORES", "[a inf]"),
                ("ZUNIONSTORE o 2 q1 q2 AGGREGATE MIN", ":1"),
                ("ZRANGE o 0 -1 WITHSCORES", "[a -inf]"),
                // A destination of another type is replaced
                ("SADD ds a", ":1"),
                ("ZUNIONSTORE ds 1 q1", ":1"),
                ("TYPE ds", "+zset"),
            ],
        );
        // A store replaces the deadline too.
        s.execute(&["ZUNIONSTORE", "u", "1", "s2"]);
        s.execute(&["PEXPIRE", "u", "100000"]);
        assert!(pttl(&s, "u") > 0);
        assert_eq!(x(&s, "ZUNIONSTORE u 1 s2"), ":3");
        assert_eq!(pttl(&s, "u"), -1);
        s.execute(&["PEXPIRE", "u", "100000"]);
        assert_eq!(x(&s, "ZINTERSTORE u 2 s2 nokey"), ":0");
        assert_eq!(x(&s, "EXISTS u"), ":0");
    });
}

/// Captured from Valkey 9.1.3. A set of up to 128 members, none over 64
/// bytes, is a listpack there: -0 is stored as 0, and a member range is
/// found by walking the entries, which differs from a search when the
/// scores differ.
#[test]
fn small_sorted_sets_answer_as_valkey_listpacks() {
    guarded("zset-listpack", 60, || {
        let (_db, s) = fresh();
        let long = "m".repeat(65);
        table(
            &s,
            &[
                ("ZADD z -0 a", ":1"),
                ("ZSCORE z a", "0"),
                ("ZINCRBY z2 -0 b", "-0"),
                ("ZSCORE z2 b", "0"),
                (&format!("ZADD z3 -0 a 1 {long}"), ":2"),
                ("ZSCORE z3 a", "-0"),
                ("ZADD z4 -0 a", ":1"),
                (&format!("ZADD z4 1 {long}"), ":1"),
                ("ZADD z4 -0 b", ":1"),
                ("ZRANGE z4 0 -1 WITHSCORES", &format!("[a 0 b -0 {long} 1]")),
                ("ZADD z5 1 a", ":1"),
                ("ZADD z5 -0 x 2 y", ":2"),
                ("ZUNIONSTORE z6 1 z5 WEIGHTS -0", ":3"),
                ("ZRANGE z6 0 -1 WITHSCORES", "[a 0 x 0 y 0]"),
                ("ZADD m 3 c", ":1"),
                ("ZADD m inf a", ":1"),
                ("ZREMRANGEBYLEX m [a [a", ":0"),
                ("ZLEXCOUNT m [a [a", ":0"),
                ("ZRANGEBYLEX m [a [a", "[]"),
                ("ZREVRANGEBYLEX m [a [a", "[]"),
                ("ZRANGE m [a [a BYLEX REV", "[]"),
                ("ZRANGEBYLEX m [a [c", "[c a]"),
                ("ZRANGEBYLEX m - +", "[c a]"),
                ("ZLEXCOUNT m [b [z", ":0"),
                ("ZADD n 1 b 2 a", ":2"),
                ("ZRANGEBYLEX n [b [c", "[]"),
                ("ZREMRANGEBYLEX n [b [c", ":0"),
                ("ZRANGEBYLEX n [a [z", "[b a]"),
                ("ZREVRANGEBYLEX n [z [a", "[a b]"),
                ("ZREVRANGEBYLEX n [b [a", "[a b]"),
                ("ZRANGEBYLEX n [a [b LIMIT 1 1", "[a]"),
                ("ZRANGESTORE n2 n [a [z BYLEX", ":2"),
                ("ZRANGE n2 0 -1 WITHSCORES", "[b 1 a 2]"),
            ],
        );
    });
}

#[test]
fn string_hash_and_key_edge_cases() {
    guarded("str-hash-key-edges", 60, || {
        let (_db, s) = fresh();
        table(
            &s,
            &[
                ("SET n 10.5", "OK"),
                ("INCRBYFLOAT n 0.1", "10.6"),
                ("INCRBYFLOAT n -10.6", "0"),
                ("INCRBYFLOAT n inf", &format!("-{NAN_OR_INF}")),
                ("INCRBYFLOAT n -inf", &format!("-{NAN_OR_INF}")),
                ("INCRBYFLOAT n abc", &format!("-{NOT_FLOAT}")),
                ("INCRBYFLOAT n nan", &format!("-{NOT_FLOAT}")),
                ("INCRBYFLOAT n 1e1", "10"),
                ("INCRBYFLOAT n .5", "10.5"),
                ("INCRBYFLOAT n 5.", "15.5"),
                ("INCRBYFLOAT n +1", "16.5"),
                ("INCRBYFLOAT fresh 3", "3"),
                ("GET fresh", "3"),
                ("SET w abc", "OK"),
                ("INCRBYFLOAT w 1", &format!("-{NOT_FLOAT}")),
                ("RPUSH li a", ":1"),
                ("INCRBYFLOAT li 1", &format!("-{WRONGTYPE}")),
                ("INCRBYFLOAT li abc", &format!("-{WRONGTYPE}")),
                ("INCRBYFLOAT w abc", &format!("-{NOT_FLOAT}")),
                ("SET hx 0x10", "OK"),
                ("INCRBYFLOAT hx 0x10", "32"),
                ("INCRBYFLOAT dec 0.1", "0.1"),
                ("INCRBYFLOAT dec 0.2", "0.3"),
                ("INCRBYFLOAT big 9223372036854775807", "9223372036854775807"),
                ("INCRBYFLOAT tiny 0x1p-1074", "0"),
                ("HINCRBYFLOAT hf a 0.1", "0.1"),
                ("HINCRBYFLOAT hf a 0.2", "0.3"),
                ("HINCRBYFLOAT hf a inf", "-ERR value is NaN or Infinity"),
                ("HSET hf b inf", ":1"),
                ("HINCRBYFLOAT hf b 1", &format!("-{NAN_OR_INF}")),
                ("ZINCRBY nz -0 m", "-0"),
                ("ZADD nz INCR 1e300 m", "1e+300"),
                ("ZADD nz 0x10 h", ":1"),
                ("ZSCORE nz h", "16"),
                // GETRANGE
                ("SET g Hello_World", "OK"),
                ("GETRANGE g 0 4", "Hello"),
                ("GETRANGE g -5 -1", "World"),
                ("GETRANGE g 0 -1", "Hello_World"),
                ("GETRANGE g 5 3", ""),
                ("GETRANGE g 100 200", ""),
                ("GETRANGE g -100 4", "Hello"),
                ("GETRANGE g 0 100", "Hello_World"),
                ("GETRANGE g -1 -5", ""),
                ("GETRANGE g 3 3", "l"),
                ("GETRANGE g -1 -1", "d"),
                ("GETRANGE g 0 -100", "H"),
                ("GETRANGE g -100 -100", "H"),
                ("GETRANGE g -100 -200", ""),
                ("SUBSTR g 1 2", "el"),
                ("GETRANGE nokey 0 -1", ""),
                ("GETRANGE li 0 -1", &format!("-{WRONGTYPE}")),
                (
                    "GETRANGE g x 1",
                    "-ERR value is not an integer or out of range",
                ),
                ("GETRANGE g 0 9223372036854775807", "Hello_World"),
                ("GETRANGE g -9223372036854775808 4", "Hello"),
                // SETRANGE
                ("SETRANGE sr 0 ", ":0"),
                ("EXISTS sr", ":0"),
                ("SETRANGE g 6 w", ":11"),
                ("GET g", "Hello_world"),
                ("SETRANGE g 11 !!", ":13"),
                ("GET g", "Hello_world!!"),
                ("SETRANGE g -1 x", "-ERR offset is out of range"),
                (
                    "SETRANGE g x x",
                    "-ERR value is not an integer or out of range",
                ),
                ("SETRANGE li 0 x", &format!("-{WRONGTYPE}")),
                (
                    "SETRANGE g 536870912 x",
                    "-ERR string exceeds maximum allowed size (proto-max-bulk-len)",
                ),
                ("SETRANGE g 0 ", ":13"),
                // Hashes
                ("HSET h a 1 b xy", ":2"),
                ("HSTRLEN h b", ":2"),
                ("HSTRLEN h nofield", ":0"),
                ("HSTRLEN nokey a", ":0"),
                ("HSTRLEN g a", &format!("-{WRONGTYPE}")),
                ("HINCRBYFLOAT h a 1.5", "2.5"),
                ("HINCRBYFLOAT h a 1.5", "4"),
                ("HINCRBYFLOAT h b 1", "-ERR hash value is not a float"),
                ("HINCRBYFLOAT h c 1", "1"),
                ("HINCRBYFLOAT h c inf", "-ERR value is NaN or Infinity"),
                ("HINCRBYFLOAT h c abc", &format!("-{NOT_FLOAT}")),
                ("HINCRBYFLOAT g a 1", &format!("-{WRONGTYPE}")),
                ("HINCRBYFLOAT newh f 0.25", "0.25"),
                ("HGET newh f", "0.25"),
                ("HINCRBYFLOAT newh f nan", &format!("-{NOT_FLOAT}")),
                ("HRANDFIELD nokey", "nil"),
                ("HRANDFIELD nokey 5", "[]"),
                ("HRANDFIELD nokey -5 WITHVALUES", "[]"),
                ("HRANDFIELD h 0", "[]"),
                ("HRANDFIELD h 2 BOGUS", "-ERR syntax error"),
                (
                    "HRANDFIELD h WITHVALUES",
                    "-ERR value is not an integer or out of range",
                ),
                ("HRANDFIELD g", &format!("-{WRONGTYPE}")),
                (
                    "HRANDFIELD h x",
                    "-ERR value is not an integer or out of range",
                ),
            ],
        );
        table_sorted(&s, &[("HKEYS h", "[a b c]"), ("HVALS h", "[4 xy 1]")]);
        assert_eq!(s.execute(&["HKEYS", "nokey"]), Reply::Array(vec![]));
        assert_eq!(s.execute(&["HVALS", "g"]), Reply::Error(WRONGTYPE.into()));

        // SETRANGE pads with zero bytes.
        assert_eq!(s.execute(&["SETRANGE", "pad", "5", "x"]), Reply::Int(6));
        assert_eq!(
            s.execute(&["GET", "pad"]),
            Reply::bulk(b"\0\0\0\0\0x".to_vec())
        );

        // MSETNX, TOUCH, OBJECT
        table(
            &s,
            &[
                ("MSETNX a1 1 a2 2", ":1"),
                ("MSETNX a2 9 a3 3", ":0"),
                ("EXISTS a3", ":0"),
                ("MSETNX a3 1 a3 2", ":1"),
                ("GET a3", "2"),
                ("MSETNX li 1 a4 1", ":0"),
                ("EXISTS a4", ":0"),
                (
                    "MSETNX a5 1 a6",
                    "-ERR wrong number of arguments for 'msetnx' command",
                ),
                ("TOUCH a1 a2 nokey", ":2"),
                ("TOUCH a1 a1", ":2"),
                ("TOUCH nokey", ":0"),
                ("OBJECT ENCODING nokey", "nil"),
                ("SET oe 123", "OK"),
                ("OBJECT ENCODING oe", "int"),
                ("SET oe abc", "OK"),
                ("OBJECT ENCODING oe", "embstr"),
                ("OBJECT ENCODING li", "quicklist"),
                (
                    "OBJECT ENCODING oe extra",
                    "-ERR wrong number of arguments for 'object|encoding' command",
                ),
            ],
        );
        let long = "x".repeat(60);
        s.execute(&["SET", "oe", &long]);
        assert_eq!(x(&s, "OBJECT ENCODING oe"), "raw");
    });
}

#[test]
fn rename_and_copy_edge_cases() {
    guarded("rename-copy", 60, || {
        let (_db, s) = fresh();
        table(
            &s,
            &[
                ("RPUSH src a b", ":2"),
                ("PEXPIRE src 100000", ":1"),
                ("SET dst x", "OK"),
                ("RENAME src dst", "OK"),
                ("EXISTS src", ":0"),
                ("TYPE dst", "+list"),
                ("LRANGE dst 0 -1", "[a b]"),
                ("RENAME nokey x", "-ERR no such key"),
                ("RENAMENX nokey x", "-ERR no such key"),
                ("SET other 1", "OK"),
                ("RENAMENX dst other", ":0"),
                ("RENAMENX dst fresh", ":1"),
                ("RENAME fresh fresh", "OK"),
                ("RENAMENX fresh fresh", ":0"),
                ("RENAME nokey nokey", "-ERR no such key"),
            ],
        );
        assert!(pttl(&s, "fresh") > 0, "RENAME lost the deadline");
        // The deadline of an overwritten destination is gone.
        s.execute(&["SET", "d", "x", "PX", "100000"]);
        s.execute(&["RPUSH", "n", "a"]);
        assert_eq!(x(&s, "RENAME n d"), "OK");
        assert_eq!(pttl(&s, "d"), -1);
        // RENAME k k keeps the deadline.
        s.execute(&["PEXPIRE", "d", "100000"]);
        assert_eq!(x(&s, "RENAME d d"), "OK");
        assert!(pttl(&s, "d") > 0);

        // COPY of every type, independent of the original, with its deadline.
        for (kind, setup) in [
            ("string", vec!["SET", "c0", "v"]),
            ("hash", vec!["HSET", "c0", "f", "v"]),
            ("list", vec!["RPUSH", "c0", "a", "b"]),
            ("set", vec!["SADD", "c0", "a", "b"]),
            ("zset", vec!["ZADD", "c0", "1", "a", "2", "b"]),
        ] {
            s.execute(&["DEL", "c0", "c1"]);
            s.execute(&setup);
            s.execute(&["PEXPIRE", "c0", "100000"]);
            assert_eq!(x(&s, "COPY c0 c1"), ":1", "{kind}");
            assert_eq!(x(&s, "TYPE c1"), format!("+{kind}"));
            assert!(pttl(&s, "c1") > 0, "{kind}: COPY lost the deadline");
            assert_eq!(x(&s, "COPY c0 c1"), ":0", "{kind}");
            s.execute(&["PERSIST", "c0"]);
            assert_eq!(x(&s, "COPY c0 c1 REPLACE"), ":1", "{kind}");
            assert_eq!(pttl(&s, "c1"), -1, "{kind}: REPLACE kept the old deadline");
            // Independent copies.
            s.execute(&["DEL", "c0"]);
            assert_eq!(x(&s, "TYPE c1"), format!("+{kind}"), "{kind}");
        }
        s.execute(&["RPUSH", "c0", "a"]);
        s.execute(&["COPY", "c0", "c2"]);
        s.execute(&["RPUSH", "c2", "b"]);
        s.execute(&["LPOP", "c0"]);
        assert_eq!(x(&s, "LRANGE c2 0 -1"), "[a b]");
        s.execute(&["SADD", "cs", "a"]);
        s.execute(&["COPY", "cs", "cs2"]);
        s.execute(&["SADD", "cs2", "b"]);
        assert_eq!(x(&s, "SCARD cs"), ":1");
        s.execute(&["ZADD", "cz", "1", "a"]);
        s.execute(&["COPY", "cz", "cz2"]);
        s.execute(&["ZINCRBY", "cz2", "5", "a"]);
        assert_eq!(x(&s, "ZSCORE cz a"), "1");
        table(
            &s,
            &[
                ("COPY nokey x", ":0"),
                (
                    "COPY c2 c2",
                    "-ERR source and destination objects are the same",
                ),
                ("COPY c2 c3 BOGUS", "-ERR syntax error"),
                ("COPY c2 c3 REPLACE", ":1"),
                ("COPY cs c3", ":0"),
                ("COPY cs c3 REPLACE", ":1"),
                ("TYPE c3", "+set"),
            ],
        );
    });
}

#[test]
fn expiry_and_collections() {
    guarded("collection-ttl", 60, || {
        let db = open(conf(4));
        let s = session(&db);
        let arm = |setup: &[&str]| {
            s.execute(&["FLUSHALL"]);
            s.execute(setup);
            s.execute(&["PEXPIRE", "k", "1000"]);
        };
        let setups: Vec<(&str, Vec<&str>)> = vec![
            ("list", vec!["RPUSH", "k", "a", "b"]),
            ("set", vec!["SADD", "k", "a", "b"]),
            ("zset", vec!["ZADD", "k", "1", "a", "2", "b"]),
            ("hash", vec!["HSET", "k", "a", "1"]),
        ];
        for (kind, setup) in &setups {
            // Readers see nothing.
            for line in [
                "LLEN k",
                "LRANGE k 0 -1",
                "LINDEX k 0",
                "LPOS k a",
                "SCARD k",
                "SMEMBERS k",
                "SISMEMBER k a",
                "ZCARD k",
                "ZRANGE k 0 -1",
                "ZSCORE k a",
                "ZRANK k a",
                "HLEN k",
                "HKEYS k",
                "HSTRLEN k a",
                "SINTER k",
                "SUNION k",
                "SDIFF k",
                "TYPE k",
                "EXISTS k",
                "OBJECT ENCODING k",
                "GETRANGE k 0 -1",
                "COPY k x",
                "ZCOUNT k 0 5",
                "RANDOMKEY",
            ] {
                arm(setup);
                db.advance_clock(1000);
                let got = x(&s, line);
                let want = match line {
                    "LLEN k" | "SCARD k" | "ZCARD k" | "HLEN k" | "HSTRLEN k a" | "EXISTS k"
                    | "COPY k x" | "ZCOUNT k 0 5" | "SISMEMBER k a" => ":0",
                    "LRANGE k 0 -1" | "SMEMBERS k" | "ZRANGE k 0 -1" | "HKEYS k" | "SINTER k"
                    | "SUNION k" | "SDIFF k" | "LPOS k a COUNT 0" => "[]",
                    "TYPE k" => "+none",
                    "GETRANGE k 0 -1" => "",
                    _ => "nil",
                };
                assert_eq!(got, want, "{kind}: {line}");
            }
            // Writers start from nothing and leave no deadline behind.
            let writers: Vec<(&str, &str, &str)> = match *kind {
                "list" => vec![
                    ("LPUSH k z", ":1", "[z]"),
                    ("RPUSH k z", ":1", "[z]"),
                    ("LMOVE s k LEFT LEFT", "nil", "[]"),
                ],
                "set" => vec![("SADD k z", ":1", "[z]"), ("SUNIONSTORE k k", ":0", "[]")],
                "zset" => vec![("ZADD k 1 z", ":1", "[z]"), ("ZINCRBY k 1 z", "1", "[z]")],
                _ => vec![
                    ("HSET k z 1", ":1", "[z]"),
                    ("HINCRBYFLOAT k z 1", "1", "[z]"),
                ],
            };
            for (cmd, reply, probe) in writers {
                arm(setup);
                db.advance_clock(1000);
                assert_eq!(x(&s, cmd), reply, "{kind}: {cmd}");
                assert_eq!(
                    x(&s, "TTL k"),
                    if reply == ":0" || reply == "nil" {
                        ":-2"
                    } else {
                        ":-1"
                    },
                    "{kind}: {cmd} kept the old deadline"
                );
                let _ = probe;
            }
            // Pops and removals of an expired key do nothing and say so.
            arm(setup);
            db.advance_clock(1000);
            for (cmd, want) in [
                ("LPOP k", "nil"),
                ("SPOP k", "nil"),
                ("SREM k a", ":0"),
                ("LREM k 0 a", ":0"),
                ("ZREM k a", ":0"),
                ("ZPOPMIN k", "[]"),
                ("HDEL k a", ":0"),
                ("LSET k 0 x", "-ERR no such key"),
                ("RENAME k z", "-ERR no such key"),
                ("LTRIM k 0 0", "OK"),
                ("LINSERT k BEFORE a b", ":0"),
            ] {
                // Wrong-typed commands error on a live key, not on a dead one.
                assert_eq!(x(&s, cmd), want, "{kind}: {cmd}");
            }
        }
        // Expired sources and destinations of the multi-key commands.
        s.execute(&["FLUSHALL"]);
        s.execute(&["RPUSH", "ls", "a"]);
        s.execute(&["PEXPIRE", "ls", "1000"]);
        s.execute(&["SET", "str", "x"]);
        s.execute(&["PEXPIRE", "str", "1000"]);
        s.execute(&["SADD", "ss", "a"]);
        s.execute(&["PEXPIRE", "ss", "1000"]);
        db.advance_clock(1000);
        table(
            &s,
            &[
                ("LMOVE ls dst LEFT LEFT", "nil"),
                ("RPUSH live 1", ":1"),
                // An expired destination of another type counts as missing.
                ("LMOVE live str LEFT LEFT", "1"),
                ("TYPE str", "+list"),
                ("TTL str", ":-1"),
                ("SMOVE ss live2 a", ":0"),
                ("SUNIONSTORE out ss", ":0"),
                ("SINTERCARD 1 ss", ":0"),
                ("ZUNIONSTORE zout 1 ss", ":0"),
                ("EXISTS out zout", ":0"),
            ],
        );
    });
}

#[test]
fn keys_that_become_empty_disappear() {
    guarded("empty-vanish", 60, || {
        let (_db, s) = fresh();
        let cases: Vec<(Vec<&str>, Vec<&str>)> = vec![
            (vec!["RPUSH", "k", "a"], vec!["LPOP", "k"]),
            (vec!["RPUSH", "k", "a", "b"], vec!["RPOP", "k", "5"]),
            (vec!["RPUSH", "k", "a", "a"], vec!["LREM", "k", "0", "a"]),
            (vec!["RPUSH", "k", "a"], vec!["LTRIM", "k", "1", "0"]),
            (
                vec!["RPUSH", "k", "a"],
                vec!["LMOVE", "k", "other", "LEFT", "LEFT"],
            ),
            (vec!["SADD", "k", "a"], vec!["SREM", "k", "a"]),
            (vec!["SADD", "k", "a"], vec!["SPOP", "k"]),
            (vec!["SADD", "k", "a", "b"], vec!["SPOP", "k", "9"]),
            (vec!["SADD", "k", "a"], vec!["SMOVE", "k", "other", "a"]),
            (vec!["SADD", "k", "a"], vec!["SDIFFSTORE", "k", "k", "k"]),
            (
                vec!["SADD", "k", "a"],
                vec!["SINTERSTORE", "k", "k", "nokey"],
            ),
            (vec!["ZADD", "k", "1", "a"], vec!["ZREM", "k", "a"]),
            (
                vec!["ZADD", "k", "1", "a", "2", "b"],
                vec!["ZPOPMAX", "k", "9"],
            ),
            (
                vec!["ZADD", "k", "1", "a", "2", "b"],
                vec!["ZREMRANGEBYRANK", "k", "0", "-1"],
            ),
            (
                vec!["ZADD", "k", "1", "a", "2", "b"],
                vec!["ZREMRANGEBYSCORE", "k", "-inf", "+inf"],
            ),
            (
                vec!["ZADD", "k", "1", "a"],
                vec!["ZINTERSTORE", "k", "2", "k", "nokey"],
            ),
            (vec!["HSET", "k", "f", "1"], vec!["HDEL", "k", "f"]),
        ];
        for (setup, last) in cases {
            s.execute(&["FLUSHALL"]);
            s.execute(&setup);
            s.execute(&["PEXPIRE", "k", "100000"]);
            s.execute(&last);
            assert_eq!(x(&s, "EXISTS k"), ":0", "{last:?}");
            assert_eq!(x(&s, "TYPE k"), "+none", "{last:?}");
            // The next key under that name starts clean.
            s.execute(&["SET", "k", "v"]);
            assert_eq!(x(&s, "TTL k"), ":-1", "{last:?}");
            assert_eq!(x(&s, "KEYS k"), "[k]", "{last:?}");
        }
        // A command that fails leaves no empty shell behind.
        for (line, _) in [
            ("ZADD nokey XX 1 a", ""),
            ("ZADD nokey NX XX 1 a", ""),
            ("LPUSHX nokey a", ""),
            ("LREM nokey 0 a", ""),
            ("SREM nokey a", ""),
            ("HDEL nokey a", ""),
            ("SPOP nokey 3", ""),
            ("ZPOPMIN nokey 3", ""),
            ("LPOP nokey 3", ""),
            ("SINTERSTORE nokey nokey2", ""),
            ("ZUNIONSTORE nokey 1 nokey2", ""),
            ("LRANGE nokey 0 -1", ""),
            ("SETRANGE nokey 0 ", ""),
        ] {
            s.execute(&line.split(' ').collect::<Vec<_>>());
            assert_eq!(x(&s, "EXISTS nokey"), ":0", "{line}");
        }
        assert_eq!(x(&s, "ZADD nokey 1 a"), ":1");
        assert_eq!(x(&s, "ZINCRBY nokey inf a"), "inf");
        assert_eq!(x(&s, "ZINCRBY nokey -inf a"), format!("-{NAN_SCORE}"));
        assert_eq!(x(&s, "ZSCORE nokey a"), "inf");
    });
}

#[test]
fn wrong_type_matrix() {
    guarded("wrongtype", 60, || {
        let (_db, s) = fresh();
        let makers: [(&str, &[&str]); 5] = [
            ("string", &["SET", "k", "v"]),
            ("hash", &["HSET", "k", "f", "v"]),
            ("list", &["RPUSH", "k", "a"]),
            ("set", &["SADD", "k", "a"]),
            ("zset", &["ZADD", "k", "1", "a"]),
        ];
        let commands: [(&str, &[&str]); 5] = [
            (
                "string",
                &[
                    "INCRBYFLOAT k 1",
                    "GETRANGE k 0 1",
                    "SUBSTR k 0 1",
                    "SETRANGE k 0 x",
                ],
            ),
            (
                "hash",
                &[
                    "HKEYS k",
                    "HVALS k",
                    "HSTRLEN k f",
                    "HINCRBYFLOAT k f 1",
                    "HRANDFIELD k",
                    "HRANDFIELD k 2",
                ],
            ),
            (
                "list",
                &[
                    "LPUSH k x",
                    "RPUSH k x",
                    "LPUSHX k x",
                    "RPUSHX k x",
                    "LPOP k",
                    "RPOP k",
                    "LPOP k 2",
                    "LLEN k",
                    "LRANGE k 0 -1",
                    "LINDEX k 0",
                    "LSET k 0 x",
                    "LREM k 0 x",
                    "LTRIM k 0 1",
                    "LINSERT k BEFORE a b",
                    "LPOS k a",
                ],
            ),
            (
                "set",
                &[
                    "SADD k x",
                    "SREM k x",
                    "SISMEMBER k x",
                    "SMISMEMBER k x",
                    "SMEMBERS k",
                    "SCARD k",
                    "SPOP k",
                    "SPOP k 2",
                    "SRANDMEMBER k",
                    "SRANDMEMBER k 2",
                    "SSCAN k 0",
                ],
            ),
            (
                "zset",
                &[
                    "ZADD k 1 x",
                    "ZREM k x",
                    "ZSCORE k x",
                    "ZMSCORE k x",
                    "ZINCRBY k 1 x",
                    "ZCARD k",
                    "ZCOUNT k 0 1",
                    "ZRANK k x",
                    "ZREVRANK k x",
                    "ZRANGE k 0 -1",
                    "ZREVRANGE k 0 -1",
                    "ZRANGEBYSCORE k 0 1",
                    "ZREVRANGEBYSCORE k 1 0",
                    "ZREMRANGEBYRANK k 0 1",
                    "ZREMRANGEBYSCORE k 0 1",
                    "ZPOPMIN k",
                    "ZPOPMAX k",
                    "ZSCAN k 0",
                ],
            ),
        ];
        let err = format!("-{WRONGTYPE}");
        for (have, maker) in makers {
            for (want, lines) in commands {
                if have == want {
                    continue;
                }
                for line in lines {
                    s.execute(&["FLUSHALL"]);
                    s.execute(maker);
                    s.execute(&["PEXPIRE", "k", "100000"]);
                    let before = show_sorted(&s.execute(&["TYPE", "k"]));
                    assert_eq!(x(&s, line), err, "{line} on a {have}");
                    assert_eq!(
                        show_sorted(&s.execute(&["TYPE", "k"])),
                        before,
                        "{line} on a {have}"
                    );
                    assert!(pttl(&s, "k") > 0, "{line} on a {have} dropped the deadline");
                }
            }
        }
        // Multi-key commands: any wrong-typed source is an error and nothing
        // is stored.
        s.execute(&["FLUSHALL"]);
        s.execute(&["SADD", "set1", "a", "b"]);
        s.execute(&["RPUSH", "lst", "a"]);
        s.execute(&["SET", "dst", "keep"]);
        for line in [
            "SINTER set1 lst",
            "SUNION lst set1",
            "SDIFF set1 lst",
            "SINTERSTORE dst set1 lst",
            "SUNIONSTORE dst lst",
            "SDIFFSTORE dst set1 lst",
            "SINTERCARD 2 set1 lst",
            "ZUNIONSTORE dst 2 set1 lst",
            "ZINTERSTORE dst 1 lst",
            "SMOVE lst set1 a",
            "SMOVE set1 lst a",
            "LMOVE set1 lst LEFT LEFT",
            "LMOVE lst set1 LEFT LEFT",
            "RPOPLPUSH set1 lst",
            "RPOPLPUSH lst set1",
        ] {
            assert_eq!(x(&s, line), err, "{line}");
            assert_eq!(x(&s, "GET dst"), "keep", "{line} touched dst");
            assert_eq!(
                x(&s, "SMEMBERS set1").len(),
                "[a b]".len(),
                "{line} changed set1"
            );
            assert_eq!(x(&s, "LRANGE lst 0 -1"), "[a]", "{line} changed lst");
        }
    });
}

#[test]
fn same_source_and_destination() {
    guarded("same-src-dst", 60, || {
        let (_db, s) = fresh();
        table_sorted(
            &s,
            &[
                ("SADD a 1 2 3", ":3"),
                ("SINTERSTORE a a a", ":3"),
                ("SMEMBERS a", "[1 2 3]"),
                ("SUNIONSTORE a a a", ":3"),
                ("SDIFFSTORE a a a", ":0"),
                ("EXISTS a", ":0"),
                ("SADD a 1 2 3", ":3"),
                ("SMOVE a a 1", ":1"),
                ("SMOVE a a 9", ":0"),
                ("SMEMBERS a", "[1 2 3]"),
                ("ZADD z 1 a 2 b", ":2"),
                ("ZINTERSTORE z 2 z z", ":2"),
                ("ZRANGE z 0 -1 WITHSCORES", "[a 2 b 4]"),
                ("ZUNIONSTORE z 2 z z", ":2"),
                ("ZRANGE z 0 -1 WITHSCORES", "[a 4 b 8]"),
                ("ZUNIONSTORE z 1 z WEIGHTS 0.5", ":2"),
                ("ZRANGE z 0 -1 WITHSCORES", "[a 2 b 4]"),
                ("RPUSH l 1 2 3", ":3"),
                (
                    "COPY l l",
                    "-ERR source and destination objects are the same",
                ),
                ("RENAME l l", "OK"),
                ("RENAMENX l l", ":0"),
                ("LRANGE l 0 -1", "[1 2 3]"),
            ],
        );
    });
}

/// Random replies are checked for what must hold, over sets whose tables
/// have been emptied to a fraction of their size.
#[test]
fn random_member_commands_return_what_was_asked() {
    guarded("random-members", 120, || {
        for shards in [1, 16] {
            let db = open(conf(shards));
            let s = session(&db);
            let mut rng = Rng::new(7 + shards as u64);
            for round in 0..30 {
                s.execute(&["FLUSHALL"]);
                let size = 20 + rng.below(1500) as usize;
                for chunk in (0..size).collect::<Vec<_>>().chunks(100) {
                    let mut set = sv(&["SADD", "s"]);
                    let mut hash = sv(&["HSET", "h"]);
                    for i in chunk {
                        set.push(format!("m{i}"));
                        hash.push(format!("m{i}"));
                        hash.push(format!("v{i}"));
                    }
                    s.execute(&set);
                    s.execute(&hash);
                }
                // Keep only a few of them, so the tables are sparse.
                let keep = 4 + rng.below(80) as usize;
                let mut alive: BTreeSet<String> = (0..size).map(|i| format!("m{i}")).collect();
                let mut order: Vec<String> = alive.iter().cloned().collect();
                shuffle(&mut rng, &mut order);
                for gone in order.iter().skip(keep) {
                    s.execute(&["SREM", "s", gone]);
                    s.execute(&["HDEL", "h", gone]);
                    alive.remove(gone);
                }
                let live = alive.len();
                for count in [1usize, 2, 3, live / 4, live / 3, live / 2, live, live + 5] {
                    if count == 0 {
                        continue;
                    }
                    let c = count.to_string();
                    let got = sorted_words(s.execute(&["SRANDMEMBER", "s", &c]));
                    let distinct: BTreeSet<_> = got.iter().cloned().collect();
                    assert_eq!(
                        got.len(),
                        count.min(live),
                        "round {round}: SRANDMEMBER s {c} of {live}"
                    );
                    assert_eq!(
                        distinct.len(),
                        got.len(),
                        "round {round}: SRANDMEMBER duplicates"
                    );
                    assert!(
                        distinct.is_subset(&alive),
                        "round {round}: SRANDMEMBER foreign member"
                    );
                    let got = pairs_of(s.execute(&["HRANDFIELD", "h", &c, "WITHVALUES"]));
                    assert_eq!(
                        got.len(),
                        count.min(live),
                        "round {round}: HRANDFIELD h {c} of {live}"
                    );
                    for (f, v) in &got {
                        assert!(
                            alive.contains(f) && *v == f.replacen('m', "v", 1),
                            "round {round}: {f} {v}"
                        );
                    }
                    let neg = format!("-{count}");
                    assert_eq!(array(s.execute(&["SRANDMEMBER", "s", &neg])).len(), count);
                    assert_eq!(array(s.execute(&["HRANDFIELD", "h", &neg])).len(), count);
                }
                // One at a time: never nil while there is a member.
                for _ in 0..20 {
                    assert!(
                        matches!(s.execute(&["SRANDMEMBER", "s"]), Reply::Bulk(_)),
                        "round {round}"
                    );
                    assert!(
                        matches!(s.execute(&["HRANDFIELD", "h"]), Reply::Bulk(_)),
                        "round {round}"
                    );
                }
                // SPOP with a count takes exactly that many, or all.
                let mut left = alive.clone();
                while !left.is_empty() {
                    let ask = 1 + rng.below((left.len() as u64 / 3).max(1)) as usize;
                    let c = ask.to_string();
                    let got = sorted_words(s.execute(&["SPOP", "s", &c]));
                    assert_eq!(
                        got.len(),
                        ask.min(left.len()),
                        "round {round}: SPOP s {c} of {}",
                        left.len()
                    );
                    for m in &got {
                        assert!(
                            left.remove(m),
                            "round {round}: SPOP returned {m} twice or from nowhere"
                        );
                    }
                    assert_eq!(
                        int(&s.execute(&["SCARD", "s"])),
                        left.len() as i64,
                        "round {round}"
                    );
                    if rng.chance(3) && !left.is_empty() {
                        // And one by one.
                        let Reply::Bulk(m) = s.execute(&["SPOP", "s"]) else {
                            panic!(
                                "round {round}: SPOP returned nil for {} members",
                                left.len()
                            );
                        };
                        assert!(left.remove(&String::from_utf8_lossy(&m).into_owned()));
                    }
                }
                assert_eq!(x(&s, "EXISTS s"), ":0");
            }
        }
    });
}

#[test]
fn randomkey_never_misses_a_live_key() {
    guarded("randomkey", 60, || {
        for shards in [1, 4, 64] {
            let db = open(conf(shards));
            let s = session(&db);
            assert_eq!(s.execute(&["RANDOMKEY"]), Reply::Nil);
            // Expired keys among live ones: RANDOMKEY must keep looking.
            s.execute(&["SET", "live", "v"]);
            for i in 0..10 {
                s.execute(&["SET", &format!("dead{i}"), "v", "PX", "100"]);
            }
            db.advance_clock(100);
            for n in 0..3_000 {
                assert_eq!(
                    s.execute(&["RANDOMKEY"]),
                    Reply::bulk("live"),
                    "{shards} shards, call {n}: RANDOMKEY with one live key"
                );
            }
            // Only dead keys: nil.
            s.execute(&["DEL", "live"]);
            assert_eq!(s.execute(&["RANDOMKEY"]), Reply::Nil, "{shards} shards");
            // Spread over many keys, of every type.
            s.execute(&["FLUSHALL"]);
            s.execute(&["RPUSH", "l", "a"]);
            s.execute(&["SADD", "s", "a"]);
            s.execute(&["ZADD", "z", "1", "a"]);
            let mut seen = BTreeSet::new();
            for _ in 0..500 {
                seen.insert(text(&s.execute(&["RANDOMKEY"])));
            }
            assert_eq!(
                seen,
                ["l", "s", "z"].iter().map(|k| k.to_string()).collect()
            );
        }
    });
}

/// Redis refuses counts beyond LONG_MAX/2 instead of building an
/// astronomically long reply.
#[test]
fn huge_negative_counts_are_rejected() {
    guarded("huge-negative-count", 10, || {
        let (_db, s) = fresh();
        s.execute(&["SADD", "s", "a"]);
        s.execute(&["HSET", "h", "f", "v"]);
        for count in ["-9223372036854775808", "-4611686018427387905"] {
            for cmd in ["SRANDMEMBER s", "HRANDFIELD h"] {
                let got = s.execute(&[
                    cmd.split(' ').next().unwrap(),
                    cmd.split(' ').nth(1).unwrap(),
                    count,
                ]);
                assert!(
                    matches!(got, Reply::Error(_)),
                    "{cmd} {count}: {}",
                    brief(&got)
                );
            }
        }
    });
}

// ---------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------

const SHARD_LAYOUTS: [usize; 4] = [1, 4, 64, 512];

#[test]
fn concurrent_list_moves_conserve_every_element() {
    for shards in SHARD_LAYOUTS {
        guarded("list-conservation", 120, move || {
            let db = open(conf(shards));
            let s = session(&db);
            const N: usize = 1_500;
            for chunk in (0..N).collect::<Vec<_>>().chunks(100) {
                let mut cmd = sv(&["RPUSH", "A"]);
                cmd.extend(chunk.iter().map(|i| format!("e{i}")));
                s.execute(&cmd);
            }
            let stop = Arc::new(AtomicBool::new(false));
            let pushed = Arc::new(AtomicI64::new(0));
            let popped = Arc::new(AtomicI64::new(0));
            let observer = {
                let (db, stop) = (db.clone(), stop.clone());
                thread::spawn(move || {
                    let s = session(&db);
                    let mut looks = 0;
                    while !stop.load(AtomicOrd::Relaxed) {
                        s.execute(&["MULTI"]);
                        s.execute(&["LLEN", "A"]);
                        s.execute(&["LLEN", "B"]);
                        let seen = array(s.execute(&["EXEC"]));
                        let total = int(&seen[0]) + int(&seen[1]);
                        assert_eq!(total, N as i64, "a MULTI saw {seen:?}: elements in flight");
                        looks += 1;
                    }
                    looks
                })
            };
            let workers: Vec<_> = (0..6u64)
                .map(|t| {
                    let (db, pushed, popped) = (db.clone(), pushed.clone(), popped.clone());
                    thread::spawn(move || {
                        let s = session(&db);
                        let mut rng = Rng::new(100 + t);
                        for i in 0..8_000 {
                            let reply = match rng.below(9) {
                                0 => s.execute(&["LMOVE", "A", "B", "LEFT", "RIGHT"]),
                                1 => s.execute(&["LMOVE", "B", "A", "RIGHT", "LEFT"]),
                                2 => s.execute(&["RPOPLPUSH", "A", "B"]),
                                3 => s.execute(&["LMOVE", "A", "A", "RIGHT", "LEFT"]),
                                4 => s.execute(&["LMOVE", "B", "B", "LEFT", "RIGHT"]),
                                5 => s.execute(&["RPOPLPUSH", "B", "A"]),
                                6 => s.execute(&["LMOVE", "A", "B", "RIGHT", "RIGHT"]),
                                7 => {
                                    assert!(matches!(
                                        s.execute(&["LPUSH", "P", &format!("p{t}:{i}")]),
                                        Reply::Int(_)
                                    ));
                                    pushed.fetch_add(1, AtomicOrd::Relaxed);
                                    continue;
                                }
                                _ => {
                                    if matches!(s.execute(&["RPOP", "P"]), Reply::Bulk(_)) {
                                        popped.fetch_add(1, AtomicOrd::Relaxed);
                                    }
                                    continue;
                                }
                            };
                            assert!(
                                matches!(reply, Reply::Bulk(_) | Reply::Nil),
                                "{}",
                                brief(&reply)
                            );
                        }
                    })
                })
                .collect();
            for w in workers {
                w.join().expect("worker");
            }
            stop.store(true, AtomicOrd::Relaxed);
            assert!(observer.join().expect("observer") > 0);
            let a = words(s.execute(&["LRANGE", "A", "0", "-1"]));
            let b = words(s.execute(&["LRANGE", "B", "0", "-1"]));
            assert_eq!(
                a.len() + b.len(),
                N,
                "{shards} shards: elements lost or invented"
            );
            let mut all: Vec<String> = a.into_iter().chain(b).collect();
            all.sort();
            let mut want: Vec<String> = (0..N).map(|i| format!("e{i}")).collect();
            want.sort();
            assert_eq!(all, want, "{shards} shards");
            assert_eq!(
                int(&s.execute(&["LLEN", "P"])),
                pushed.load(AtomicOrd::Relaxed) - popped.load(AtomicOrd::Relaxed),
                "{shards} shards: LPUSH/RPOP bookkeeping"
            );
        });
    }
}

#[test]
fn concurrent_set_operations_on_private_members_stay_exact() {
    for shards in SHARD_LAYOUTS {
        guarded("set-private", 120, move || {
            let db = open(conf(shards));
            let stop_observer = Arc::new(AtomicBool::new(false));
            let observer = {
                let (db, stop) = (db.clone(), stop_observer.clone());
                thread::spawn(move || {
                    let s = session(&db);
                    while !stop.load(AtomicOrd::Relaxed) {
                        // Every member lives in S1, S2 or both; the union can
                        // only ever hold the members its owners put there.
                        let u = array(s.execute(&["SUNION", "S1", "S2"]));
                        let n = u.len();
                        assert!(n <= 6 * 12, "union of {n}");
                    }
                })
            };
            let workers: Vec<_> = (0..6u64)
                .map(|t| {
                    let db = db.clone();
                    thread::spawn(move || {
                        let s = session(&db);
                        let mut rng = Rng::new(500 + t);
                        let mut in1 = [false; 12];
                        let mut in2 = [false; 12];
                        let m = |i: usize| format!("t{t}:{i}");
                        for step in 0..10_000 {
                            let i = rng.below(12) as usize;
                            let j = rng.below(12) as usize;
                            match rng.below(8) {
                                0 => {
                                    let r = s.execute(&["SADD", "S1", &m(i), &m(j), &m(i)]);
                                    let new = !in1[i] as i64 + (j != i && !in1[j]) as i64;
                                    in1[i] = true;
                                    in1[j] = true;
                                    assert_eq!(r, Reply::Int(new), "t{t} step {step}: SADD S1");
                                }
                                1 => {
                                    let r = s.execute(&["SADD", "S2", &m(i)]);
                                    assert_eq!(
                                        r,
                                        Reply::Int(!in2[i] as i64),
                                        "t{t} step {step}: SADD S2"
                                    );
                                    in2[i] = true;
                                }
                                2 => {
                                    let r = s.execute(&["SREM", "S1", &m(i), &m(j)]);
                                    let gone = in1[i] as i64 + (j != i && in1[j]) as i64;
                                    in1[i] = false;
                                    in1[j] = false;
                                    assert_eq!(r, Reply::Int(gone), "t{t} step {step}: SREM S1");
                                }
                                3 => {
                                    let r = s.execute(&["SREM", "S2", &m(i)]);
                                    assert_eq!(
                                        r,
                                        Reply::Int(in2[i] as i64),
                                        "t{t} step {step}: SREM S2"
                                    );
                                    in2[i] = false;
                                }
                                4 => {
                                    let r = s.execute(&["SMOVE", "S1", "S2", &m(i)]);
                                    assert_eq!(
                                        r,
                                        Reply::Int(in1[i] as i64),
                                        "t{t} step {step}: SMOVE S1 S2"
                                    );
                                    if in1[i] {
                                        in1[i] = false;
                                        in2[i] = true;
                                    }
                                }
                                5 => {
                                    let r = s.execute(&["SMOVE", "S2", "S1", &m(i)]);
                                    assert_eq!(
                                        r,
                                        Reply::Int(in2[i] as i64),
                                        "t{t} step {step}: SMOVE S2 S1"
                                    );
                                    if in2[i] {
                                        in2[i] = false;
                                        in1[i] = true;
                                    }
                                }
                                6 => {
                                    let r = s.execute(&["SMISMEMBER", "S1", &m(i), &m(j)]);
                                    assert_eq!(
                                        r,
                                        Reply::Array(vec![
                                            Reply::Int(in1[i] as i64),
                                            Reply::Int(in1[j] as i64)
                                        ]),
                                        "t{t} step {step}: SMISMEMBER"
                                    );
                                }
                                _ => {
                                    let r = s.execute(&["SISMEMBER", "S2", &m(i)]);
                                    assert_eq!(
                                        r,
                                        Reply::Int(in2[i] as i64),
                                        "t{t} step {step}: SISMEMBER"
                                    );
                                }
                            }
                        }
                        (t, in1, in2)
                    })
                })
                .collect();
            let results: Vec<_> = workers
                .into_iter()
                .map(|w| w.join().expect("worker"))
                .collect();
            stop_observer.store(true, AtomicOrd::Relaxed);
            observer.join().expect("observer");
            let s = session(&db);
            let (mut want1, mut want2) = (Vec::new(), Vec::new());
            for (t, in1, in2) in results {
                for i in 0..12 {
                    if in1[i] {
                        want1.push(format!("t{t}:{i}"));
                    }
                    if in2[i] {
                        want2.push(format!("t{t}:{i}"));
                    }
                }
            }
            want1.sort();
            want2.sort();
            assert_eq!(
                sorted_words(s.execute(&["SMEMBERS", "S1"])),
                want1,
                "{shards} shards: S1"
            );
            assert_eq!(
                sorted_words(s.execute(&["SMEMBERS", "S2"])),
                want2,
                "{shards} shards: S2"
            );
            assert_eq!(int(&s.execute(&["SCARD", "S1"])), want1.len() as i64);
            assert_eq!(int(&s.execute(&["SCARD", "S2"])), want2.len() as i64);
            let both: BTreeSet<_> = want1.iter().chain(&want2).cloned().collect();
            assert_eq!(
                int(&s.execute(&["SUNIONSTORE", "U", "S1", "S2"])),
                both.len() as i64
            );
            assert_eq!(
                int(&s.execute(&["SINTERCARD", "2", "S1", "S2"])),
                want1.iter().filter(|m| want2.contains(m)).count() as i64
            );
        });
    }
}

#[test]
fn concurrent_zset_updates_agree_with_per_thread_models() {
    for shards in SHARD_LAYOUTS {
        guarded("zset-private", 120, move || {
            let db = open(conf(shards));
            let stop_observer = Arc::new(AtomicBool::new(false));
            let observer = {
                let (db, stop) = (db.clone(), stop_observer.clone());
                thread::spawn(move || {
                    let s = session(&db);
                    let mut looks = 0;
                    while !stop.load(AtomicOrd::Relaxed) {
                        let rows =
                            pairs_in_order(s.execute(&["ZRANGE", "Z", "0", "-1", "WITHSCORES"]));
                        let mut seen = BTreeSet::new();
                        for w in rows.windows(2) {
                            let ((m1, s1), (m2, s2)) = (&w[0], &w[1]);
                            assert!(
                                s1 < s2 || (s1 == s2 && m1 < m2),
                                "ZRANGE out of order: {m1} {s1} before {m2} {s2}"
                            );
                        }
                        for (m, _) in &rows {
                            assert!(seen.insert(m.clone()), "{m} listed twice");
                        }
                        looks += 1;
                    }
                    looks
                })
            };
            let workers: Vec<_> = (0..6u64)
                .map(|t| {
                    let db = db.clone();
                    thread::spawn(move || {
                        let s = session(&db);
                        let mut rng = Rng::new(900 + t);
                        let mut mine: HashMap<usize, i64> = HashMap::new();
                        let (mut incs, mut hi, mut lo) = (0i64, i64::MIN, i64::MAX);
                        let m = |i: usize| format!("t{t}:{i}");
                        for step in 0..8_000 {
                            let i = rng.below(20) as usize;
                            match rng.below(9) {
                                0 | 1 => {
                                    let sc = rng.below(50) as i64 - 25;
                                    let r = s.execute(&["ZADD", "Z", &sc.to_string(), &m(i)]);
                                    assert_eq!(
                                        r,
                                        Reply::Int(!mine.contains_key(&i) as i64),
                                        "t{t} step {step}"
                                    );
                                    mine.insert(i, sc);
                                }
                                2 | 3 => {
                                    let d = rng.below(11) as i64 - 5;
                                    let r = s.execute(&["ZINCRBY", "Z", &d.to_string(), &m(i)]);
                                    let new = mine.get(&i).copied().unwrap_or(0) + d;
                                    mine.insert(i, new);
                                    assert_eq!(
                                        r,
                                        Reply::bulk(new.to_string()),
                                        "t{t} step {step}: ZINCRBY"
                                    );
                                }
                                4 => {
                                    let r = s.execute(&["ZREM", "Z", &m(i)]);
                                    assert_eq!(
                                        r,
                                        Reply::Int(mine.remove(&i).is_some() as i64),
                                        "t{t} step {step}"
                                    );
                                }
                                5 => {
                                    let gt = rng.chance(2);
                                    let sc = rng.below(50) as i64 - 25;
                                    let r = s.execute(&[
                                        "ZADD",
                                        "Z",
                                        if gt { "GT" } else { "LT" },
                                        "CH",
                                        &sc.to_string(),
                                        &m(i),
                                    ]);
                                    let changed = match mine.get(&i) {
                                        None => true,
                                        Some(&c) => (gt && sc > c) || (!gt && sc < c),
                                    };
                                    if changed {
                                        mine.insert(i, sc);
                                    }
                                    assert_eq!(
                                        r,
                                        Reply::Int(changed as i64),
                                        "t{t} step {step}: ZADD GT/LT"
                                    );
                                }
                                6 => {
                                    let r = s.execute(&["ZINCRBY", "Z", "1", "ctr"]);
                                    assert!(matches!(r, Reply::Bulk(_)));
                                    incs += 1;
                                }
                                7 => {
                                    let v = rng.below(1_000_000) as i64;
                                    s.execute(&["ZADD", "Z", "GT", &v.to_string(), "hi"]);
                                    hi = hi.max(v);
                                    let w = rng.below(1_000_000) as i64;
                                    s.execute(&["ZADD", "Z", "LT", &w.to_string(), "lo"]);
                                    lo = lo.min(w);
                                }
                                _ => {
                                    let r = s.execute(&["ZSCORE", "Z", &m(i)]);
                                    let want = mine
                                        .get(&i)
                                        .map_or(Reply::Nil, |v| Reply::bulk(v.to_string()));
                                    assert_eq!(r, want, "t{t} step {step}: ZSCORE");
                                    let rank = s.execute(&["ZRANK", "Z", &m(i)]);
                                    assert_eq!(
                                        matches!(rank, Reply::Int(_)),
                                        mine.contains_key(&i),
                                        "t{t} step {step}: ZRANK"
                                    );
                                }
                            }
                        }
                        (t, mine, incs, hi, lo)
                    })
                })
                .collect();
            let results: Vec<_> = workers
                .into_iter()
                .map(|w| w.join().expect("worker"))
                .collect();
            stop_observer.store(true, AtomicOrd::Relaxed);
            assert!(observer.join().expect("observer") > 0);
            let s = session(&db);
            let (mut incs, mut hi, mut lo, mut members) = (0, i64::MIN, i64::MAX, 0);
            for (t, mine, n, h, l) in &results {
                incs += n;
                hi = hi.max(*h);
                lo = lo.min(*l);
                members += mine.len();
                for (i, sc) in mine {
                    assert_eq!(
                        s.execute(&["ZSCORE", "Z", &format!("t{t}:{i}")]),
                        Reply::bulk(sc.to_string()),
                        "{shards} shards: t{t}:{i}"
                    );
                }
            }
            assert_eq!(
                s.execute(&["ZSCORE", "Z", "ctr"]),
                Reply::bulk(incs.to_string()),
                "{shards} shards: ctr"
            );
            assert_eq!(
                s.execute(&["ZSCORE", "Z", "hi"]),
                Reply::bulk(hi.to_string()),
                "{shards} shards: hi"
            );
            assert_eq!(
                s.execute(&["ZSCORE", "Z", "lo"]),
                Reply::bulk(lo.to_string()),
                "{shards} shards: lo"
            );
            let card = int(&s.execute(&["ZCARD", "Z"]));
            assert_eq!(card, members as i64 + 3, "{shards} shards: ZCARD");
            // Every rank agrees with the position in the full listing.
            let rows = pairs_in_order(s.execute(&["ZRANGE", "Z", "0", "-1", "WITHSCORES"]));
            assert_eq!(rows.len() as i64, card);
            for (pos, (m, _)) in rows.iter().enumerate() {
                assert_eq!(
                    s.execute(&["ZRANK", "Z", m]),
                    Reply::Int(pos as i64),
                    "{shards} shards: ZRANK {m}"
                );
                assert_eq!(
                    s.execute(&["ZREVRANK", "Z", m]),
                    Reply::Int((rows.len() - 1 - pos) as i64),
                    "{shards} shards: ZREVRANK {m}"
                );
            }
        });
    }
}

fn pairs_in_order(r: Reply) -> Vec<(String, f64)> {
    let w = words(r);
    w.chunks(2)
        .map(|c| {
            let score = match c[1].as_str() {
                "inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                other => other.parse().expect("score"),
            };
            (c[0].clone(), score)
        })
        .collect()
}

#[test]
fn transactions_across_types_are_atomic() {
    for shards in SHARD_LAYOUTS {
        guarded("txn-atomic", 120, move || {
            let db = open(conf(shards));
            let stop = Arc::new(AtomicBool::new(false));
            let writers: Vec<_> = (0..4u64)
                .map(|w| {
                    let db = db.clone();
                    thread::spawn(move || {
                        let s = session(&db);
                        let mut rng = Rng::new(40 + w);
                        s.execute(&["SET", "STR", "not a list"]);
                        let mut committed = 0i64;
                        for i in 0..2_500 {
                            let who = format!("w{w}:{i}");
                            s.execute(&["MULTI"]);
                            s.execute(&["RPUSH", "L", &who]);
                            s.execute(&["SADD", "S", &who]);
                            if rng.chance(3) {
                                // A command that fails at run time does not stop the rest.
                                s.execute(&["LPUSH", "STR", "x"]);
                            }
                            s.execute(&["ZADD", "Z", &i.to_string(), &who]);
                            s.execute(&["HSET", "H", &who, "v"]);
                            s.execute(&["INCR", "CNT"]);
                            let replies = array(s.execute(&["EXEC"]));
                            for r in &replies {
                                if !matches!(r, Reply::Int(_)) {
                                    assert_eq!(*r, Reply::Error(WRONGTYPE.into()));
                                }
                            }
                            assert_eq!(replies.len(), if replies.len() == 6 { 6 } else { 5 });
                            committed += 1;
                        }
                        committed
                    })
                })
                .collect();
            let observers: Vec<_> = (0..2)
                .map(|_| {
                    let (db, stop) = (db.clone(), stop.clone());
                    thread::spawn(move || {
                        let s = session(&db);
                        let mut looks = 0;
                        while !stop.load(AtomicOrd::Relaxed) {
                            s.execute(&["MULTI"]);
                            s.execute(&["LLEN", "L"]);
                            s.execute(&["SCARD", "S"]);
                            s.execute(&["ZCARD", "Z"]);
                            s.execute(&["HLEN", "H"]);
                            s.execute(&["GET", "CNT"]);
                            let seen = array(s.execute(&["EXEC"]));
                            let nums: Vec<i64> = seen
                                .iter()
                                .map(|r| match r {
                                    Reply::Int(n) => *n,
                                    Reply::Nil => 0,
                                    other => text(other).parse().expect("counter"),
                                })
                                .collect();
                            assert!(
                                nums.windows(2).all(|p| p[0] == p[1]),
                                "a MULTI saw a torn state: {nums:?}"
                            );
                            looks += 1;
                        }
                        looks
                    })
                })
                .collect();
            let committed: i64 = writers.into_iter().map(|w| w.join().expect("writer")).sum();
            stop.store(true, AtomicOrd::Relaxed);
            for o in observers {
                assert!(o.join().expect("observer") > 0);
            }
            let s = session(&db);
            for (cmd, key) in [("LLEN", "L"), ("SCARD", "S"), ("ZCARD", "Z"), ("HLEN", "H")] {
                assert_eq!(
                    int(&s.execute(&[cmd, key])),
                    committed,
                    "{shards} shards: {cmd} {key}"
                );
            }
            assert_eq!(
                s.execute(&["GET", "CNT"]),
                Reply::bulk(committed.to_string())
            );
            assert_eq!(s.execute(&["GET", "STR"]), Reply::bulk("not a list"));
        });
    }
}

#[test]
fn transfers_between_collections_neither_lose_nor_duplicate() {
    for shards in SHARD_LAYOUTS {
        guarded("transfers", 120, move || {
            let db = open(conf(shards));
            let s = session(&db);
            const M: usize = 240;
            for chunk in (0..M).collect::<Vec<_>>().chunks(60) {
                let mut set = sv(&["SADD", "X"]);
                let mut list = sv(&["RPUSH", "LA"]);
                let mut z = sv(&["ZADD", "ZA"]);
                for i in chunk {
                    set.push(format!("e{i}"));
                    list.push(format!("e{i}"));
                    z.push(i.to_string());
                    z.push(format!("e{i}"));
                }
                s.execute(&set);
                s.execute(&list);
                s.execute(&z);
            }
            let stop = Arc::new(AtomicBool::new(false));
            let snapshot_observer = {
                let (db, stop) = (db.clone(), stop.clone());
                thread::spawn(move || {
                    let s = session(&db);
                    let mut looks = 0;
                    while !stop.load(AtomicOrd::Relaxed) {
                        s.execute(&["MULTI"]);
                        s.execute(&["SCARD", "X"]);
                        s.execute(&["SCARD", "Y"]);
                        s.execute(&["LLEN", "LA"]);
                        s.execute(&["LLEN", "LB"]);
                        s.execute(&["ZCARD", "ZA"]);
                        s.execute(&["ZCARD", "ZB"]);
                        let n: Vec<i64> = array(s.execute(&["EXEC"])).iter().map(int).collect();
                        assert_eq!(n[0] + n[1], M as i64, "sets in flight: {n:?}");
                        assert_eq!(n[2] + n[3], M as i64, "lists in flight: {n:?}");
                        assert_eq!(n[4] + n[5], M as i64, "zsets in flight: {n:?}");
                        looks += 1;
                    }
                    looks
                })
            };
            // A plain multi-key read has to be a snapshot too.
            let union_observer = {
                let (db, stop) = (db.clone(), stop.clone());
                thread::spawn(move || {
                    let s = session(&db);
                    let mut looks = 0;
                    while !stop.load(AtomicOrd::Relaxed) {
                        let u = array(s.execute(&["SUNION", "X", "Y"])).len();
                        assert_eq!(u, M, "SUNION X Y saw {u} members: one is in flight");
                        looks += 1;
                    }
                    looks
                })
            };
            let workers: Vec<_> = (0..6u64)
                .map(|t| {
                    let db = db.clone();
                    thread::spawn(move || {
                        let s = session(&db);
                        let mut rng = Rng::new(70 + t);
                        // Thread t owns the members e{i} with i % 6 == t.
                        for _ in 0..4_000 {
                            let i = (rng.below((M / 6) as u64) as usize) * 6 + t as usize;
                            let who = format!("e{i}");
                            match rng.below(5) {
                                0 => {
                                    s.execute(&["SMOVE", "X", "Y", &who]);
                                }
                                1 => {
                                    s.execute(&["SMOVE", "Y", "X", &who]);
                                }
                                2 => {
                                    // Atomic in a transaction: both or neither.
                                    s.execute(&["MULTI"]);
                                    s.execute(&["LMOVE", "LA", "LB", "LEFT", "RIGHT"]);
                                    s.execute(&["LMOVE", "LB", "LA", "RIGHT", "RIGHT"]);
                                    s.execute(&["SMOVE", "X", "Y", &who]);
                                    s.execute(&["SMOVE", "Y", "X", &who]);
                                    s.execute(&["EXEC"]);
                                }
                                3 => {
                                    s.execute(&["LMOVE", "LA", "LB", "LEFT", "LEFT"]);
                                    s.execute(&["LMOVE", "LB", "LA", "RIGHT", "LEFT"]);
                                }
                                _ => {
                                    // Move a zset member between ZA and ZB with its score.
                                    s.execute(&["MULTI"]);
                                    s.execute(&["ZSCORE", "ZA", &who]);
                                    s.execute(&["ZREM", "ZA", &who]);
                                    s.execute(&["ZADD", "ZB", &i.to_string(), &who]);
                                    let r = array(s.execute(&["EXEC"]));
                                    if r[0] == Reply::Nil {
                                        // It was in ZB: move it back.
                                        s.execute(&["MULTI"]);
                                        s.execute(&["ZREM", "ZB", &who]);
                                        s.execute(&["ZADD", "ZA", &i.to_string(), &who]);
                                        s.execute(&["EXEC"]);
                                    } else {
                                        // It was in ZA; ZADD put it in ZB (new).
                                        assert_eq!(r[1], Reply::Int(1));
                                    }
                                }
                            }
                        }
                    })
                })
                .collect();
            for w in workers {
                w.join().expect("worker");
            }
            stop.store(true, AtomicOrd::Relaxed);
            assert!(snapshot_observer.join().expect("observer") > 0);
            assert!(union_observer.join().expect("observer") > 0);
            let want: Vec<String> = {
                let mut v: Vec<String> = (0..M).map(|i| format!("e{i}")).collect();
                v.sort();
                v
            };
            let mut sets = sorted_words(s.execute(&["SMEMBERS", "X"]));
            sets.extend(sorted_words(s.execute(&["SMEMBERS", "Y"])));
            sets.sort();
            assert_eq!(sets, want, "{shards} shards: sets");
            let mut lists = words(s.execute(&["LRANGE", "LA", "0", "-1"]));
            lists.extend(words(s.execute(&["LRANGE", "LB", "0", "-1"])));
            lists.sort();
            assert_eq!(lists, want, "{shards} shards: lists");
            let mut zs: Vec<String> =
                pairs_in_order(s.execute(&["ZRANGE", "ZA", "0", "-1", "WITHSCORES"]))
                    .into_iter()
                    .chain(pairs_in_order(s.execute(&[
                        "ZRANGE",
                        "ZB",
                        "0",
                        "-1",
                        "WITHSCORES",
                    ])))
                    .map(|(m, sc)| {
                        assert_eq!(
                            format!("e{sc}"),
                            m,
                            "a zset member lost its score in transit"
                        );
                        m
                    })
                    .collect();
            zs.sort();
            assert_eq!(zs, want, "{shards} shards: zsets");
        });
    }
}

// ---------------------------------------------------------------------
// Big collections
// ---------------------------------------------------------------------

#[test]
fn big_sorted_set_ranks_and_ranges() {
    guarded("big-zset", 240, || {
        let db = open(conf(8));
        let s = session(&db);
        const N: usize = 200_000;
        let mut model: Vec<(i64, String)> = Vec::with_capacity(N);
        let mut batch = sv(&["ZADD", "z"]);
        for i in 0..N {
            let score = (i as i64 * 7_919) % 100_003;
            let m = format!("m{i}");
            batch.push(score.to_string());
            batch.push(m.clone());
            model.push((score, m));
            if batch.len() >= 2 + 2 * 500 {
                assert_eq!(int(&s.execute(&batch)), 500);
                batch.truncate(2);
            }
        }
        model.sort();
        let mut rng = Rng::new(31);

        let check_all = |model: &Vec<(i64, String)>, rng: &mut Rng, label: &str| {
            let n = model.len();
            assert_eq!(int(&s.execute(&["ZCARD", "z"])), n as i64, "{label}");
            // Ranks and scores of random members, and the extremes.
            for step in 0..400 {
                let pos = if step == 0 {
                    0
                } else if step == 1 {
                    n - 1
                } else {
                    rng.below(n as u64) as usize
                };
                let (score, m) = &model[pos];
                assert_eq!(
                    s.execute(&["ZRANK", "z", m]),
                    Reply::Int(pos as i64),
                    "{label}: ZRANK {m}"
                );
                assert_eq!(
                    s.execute(&["ZREVRANK", "z", m]),
                    Reply::Int((n - 1 - pos) as i64),
                    "{label}: ZREVRANK {m}"
                );
                assert_eq!(
                    s.execute(&["ZSCORE", "z", m]),
                    Reply::bulk(score.to_string()),
                    "{label}: ZSCORE {m}"
                );
            }
            assert_eq!(s.execute(&["ZRANK", "z", "nobody"]), Reply::Nil);
            // Slices, around chunk boundaries too.
            let mut starts: Vec<usize> = (0..60).map(|_| rng.below(n as u64) as usize).collect();
            starts.extend([
                0,
                1,
                255,
                256,
                257,
                511,
                512,
                513,
                1023,
                1024,
                n - 60,
                n - 1,
            ]);
            for start in starts {
                let len = 1 + rng.below(80) as usize;
                let end = (start + len - 1).min(n - 1);
                let want: Vec<String> = model[start..=end].iter().map(|e| e.1.clone()).collect();
                let got = words(s.execute(&["ZRANGE", "z", &start.to_string(), &end.to_string()]));
                assert_eq!(got, want, "{label}: ZRANGE {start} {end}");
                let mut rev = want.clone();
                rev.reverse();
                let (rs, re) = (n - 1 - end, n - 1 - start);
                let got = words(s.execute(&["ZREVRANGE", "z", &rs.to_string(), &re.to_string()]));
                assert_eq!(got, rev, "{label}: ZREVRANGE {rs} {re}");
                let neg = format!("-{}", n - start);
                let got =
                    words(s.execute(&["ZRANGE", "z", &neg, &(end as i64 - n as i64).to_string()]));
                assert_eq!(got, want, "{label}: ZRANGE {neg}");
            }
            // By score, with LIMIT and exclusive bounds.
            for _ in 0..80 {
                let lo = rng.below(100_003) as i64;
                let hi = lo + rng.below(2_000) as i64;
                let (lo_x, hi_x) = (rng.chance(2), rng.chance(2));
                let a = model.partition_point(|e| if lo_x { e.0 <= lo } else { e.0 < lo });
                let b = model.partition_point(|e| if hi_x { e.0 < hi } else { e.0 <= hi });
                let b = b.max(a);
                let lo_s = format!("{}{lo}", if lo_x { "(" } else { "" });
                let hi_s = format!("{}{hi}", if hi_x { "(" } else { "" });
                assert_eq!(
                    int(&s.execute(&["ZCOUNT", "z", &lo_s, &hi_s])),
                    (b - a) as i64,
                    "{label}: ZCOUNT {lo_s} {hi_s}"
                );
                let off = rng.below(50) as usize;
                let cnt = 1 + rng.below(40) as usize;
                let want: Vec<String> = model[a..b]
                    .iter()
                    .skip(off)
                    .take(cnt)
                    .map(|e| e.1.clone())
                    .collect();
                let got = words(s.execute(&[
                    "ZRANGEBYSCORE",
                    "z",
                    &lo_s,
                    &hi_s,
                    "LIMIT",
                    &off.to_string(),
                    &cnt.to_string(),
                ]));
                assert_eq!(
                    got, want,
                    "{label}: ZRANGEBYSCORE {lo_s} {hi_s} LIMIT {off} {cnt}"
                );
                let want: Vec<String> = model[a..b]
                    .iter()
                    .rev()
                    .skip(off)
                    .take(cnt)
                    .map(|e| e.1.clone())
                    .collect();
                let got = words(s.execute(&[
                    "ZREVRANGEBYSCORE",
                    "z",
                    &hi_s,
                    &lo_s,
                    "LIMIT",
                    &off.to_string(),
                    &cnt.to_string(),
                ]));
                assert_eq!(
                    got, want,
                    "{label}: ZREVRANGEBYSCORE {hi_s} {lo_s} LIMIT {off} {cnt}"
                );
            }
        };
        check_all(&model, &mut rng, "built");

        // ZSCAN visits every member once.
        let mut cursor = "0".to_string();
        let mut seen = HashSet::new();
        loop {
            let mut reply = array(s.execute(&["ZSCAN", "z", &cursor, "COUNT", "1000"]));
            let items = words(reply.pop().expect("items").clone_array());
            cursor = text(&reply[0]);
            assert!(items.len() <= 1100, "ZSCAN returned {} items", items.len());
            for pair in items.chunks(2) {
                assert!(seen.insert(pair[0].clone()), "ZSCAN repeated {}", pair[0]);
            }
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen.len(), N);

        // Removals from the middle and the ends.
        assert_eq!(
            int(&s.execute(&["ZREMRANGEBYRANK", "z", "50000", "50999"])),
            1_000
        );
        model.drain(50_000..51_000);
        check_all(&model, &mut rng, "after ZREMRANGEBYRANK");
        let lo = model[70_000].0;
        let hi = model[71_000].0;
        let a = model.partition_point(|e| e.0 < lo);
        let b = model.partition_point(|e| e.0 <= hi);
        assert_eq!(
            int(&s.execute(&["ZREMRANGEBYSCORE", "z", &lo.to_string(), &hi.to_string()])),
            (b - a) as i64
        );
        model.drain(a..b);
        check_all(&model, &mut rng, "after ZREMRANGEBYSCORE");
        let popped = pairs_flat(s.execute(&["ZPOPMIN", "z", "300"]));
        let want: Vec<(String, String)> = model[..300]
            .iter()
            .map(|e| (e.1.clone(), e.0.to_string()))
            .collect();
        assert_eq!(popped, want, "ZPOPMIN");
        model.drain(..300);
        let popped = pairs_flat(s.execute(&["ZPOPMAX", "z", "300"]));
        let n = model.len();
        let want: Vec<(String, String)> = model[n - 300..]
            .iter()
            .rev()
            .map(|e| (e.1.clone(), e.0.to_string()))
            .collect();
        assert_eq!(popped, want, "ZPOPMAX");
        model.truncate(n - 300);
        check_all(&model, &mut rng, "after pops");

        // Score updates move members across chunk boundaries.
        for step in 0..6_000 {
            let pos = rng.below(model.len() as u64) as usize;
            let (old, m) = model.remove(pos);
            let new = if step % 2 == 0 {
                rng.below(100_003) as i64
            } else {
                old + rng.below(21) as i64 - 10
            };
            let reply = if step % 3 == 0 {
                s.execute(&["ZINCRBY", "z", &(new - old).to_string(), &m])
            } else {
                s.execute(&["ZADD", "z", "CH", &new.to_string(), &m]);
                Reply::Nil
            };
            let _ = reply;
            let at = model.partition_point(|e| *e < (new, m.clone()));
            model.insert(at, (new, m));
        }
        check_all(&model, &mut rng, "after updates");
        let all = words(s.execute(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]));
        assert_eq!(all.len(), model.len() * 2);
        for (i, e) in model.iter().enumerate() {
            assert!(
                all[2 * i] == e.1 && all[2 * i + 1] == e.0.to_string(),
                "full listing, position {i}"
            );
        }
    });
}

trait CloneArray {
    fn clone_array(&self) -> Reply;
}

impl CloneArray for Reply {
    fn clone_array(&self) -> Reply {
        self.clone()
    }
}

fn pairs_flat(r: Reply) -> Vec<(String, String)> {
    let w = words(r);
    w.chunks(2).map(|c| (c[0].clone(), c[1].clone())).collect()
}

#[test]
fn big_list_indexes_edits_and_moves() {
    guarded("big-list", 240, || {
        let db = open(conf(8));
        let s = session(&db);
        const N: usize = 200_000;
        let mut model: VecDeque<String> = VecDeque::with_capacity(N);
        let mut batch = sv(&["RPUSH", "L"]);
        for i in 0..N {
            let v = if i % 2_000 == 7 {
                "dup".to_string()
            } else {
                format!("x{i}")
            };
            batch.push(v.clone());
            model.push_back(v);
            if batch.len() >= 2 + 1_000 {
                s.execute(&batch);
                batch.truncate(2);
            }
        }
        let mut rng = Rng::new(77);
        let check = |model: &VecDeque<String>, rng: &mut Rng, label: &str| {
            let n = model.len();
            assert_eq!(int(&s.execute(&["LLEN", "L"])), n as i64, "{label}");
            for _ in 0..300 {
                let i = rng.below(n as u64) as usize;
                assert_eq!(
                    s.execute(&["LINDEX", "L", &i.to_string()]),
                    Reply::bulk(model[i].clone()),
                    "{label}: LINDEX {i}"
                );
                let neg = i as i64 - n as i64;
                assert_eq!(
                    s.execute(&["LINDEX", "L", &neg.to_string()]),
                    Reply::bulk(model[i].clone()),
                    "{label}: LINDEX {neg}"
                );
            }
            for _ in 0..60 {
                let start = rng.below(n as u64) as usize;
                let end = (start + rng.below(300) as usize).min(n - 1);
                let want: Vec<String> = model.range(start..=end).cloned().collect();
                assert_eq!(
                    words(s.execute(&["LRANGE", "L", &start.to_string(), &end.to_string()])),
                    want,
                    "{label}: LRANGE {start} {end}"
                );
            }
        };
        check(&model, &mut rng, "built");

        // LSET and LINSERT in the middle.
        for _ in 0..300 {
            let i = rng.below(model.len() as u64) as usize;
            let v = format!("set{i}");
            assert_eq!(x(&s, &format!("LSET L {i} {v}")), "OK");
            model[i] = v;
        }
        for _ in 0..30 {
            let i = rng.below(model.len() as u64) as usize;
            let pivot = model[i].clone();
            let first = model.iter().position(|e| *e == pivot).expect("pivot");
            let before = rng.chance(2);
            let reply = s.execute(&[
                "LINSERT",
                "L",
                if before { "BEFORE" } else { "AFTER" },
                &pivot,
                "ins",
            ]);
            model.insert(if before { first } else { first + 1 }, "ins".into());
            assert_eq!(reply, Reply::Int(model.len() as i64), "LINSERT");
        }
        check(&model, &mut rng, "after LSET/LINSERT");

        // LPOS and LREM over the repeated value.
        let dups: Vec<usize> = model
            .iter()
            .enumerate()
            .filter(|(_, e)| *e == "dup")
            .map(|(i, _)| i)
            .collect();
        assert!(dups.len() > 50);
        assert_eq!(s.execute(&["LPOS", "L", "dup"]), Reply::Int(dups[0] as i64));
        assert_eq!(
            s.execute(&["LPOS", "L", "dup", "RANK", "-1"]),
            Reply::Int(*dups.last().unwrap() as i64)
        );
        assert_eq!(
            s.execute(&["LPOS", "L", "dup", "RANK", "5"]),
            Reply::Int(dups[4] as i64)
        );
        let got = ints(s.execute(&["LPOS", "L", "dup", "COUNT", "0"]));
        assert_eq!(got, dups.iter().map(|d| d.to_string()).collect::<Vec<_>>());
        let got = ints(s.execute(&["LPOS", "L", "dup", "RANK", "-2", "COUNT", "3"]));
        let want: Vec<String> = dups
            .iter()
            .rev()
            .skip(1)
            .take(3)
            .map(|d| d.to_string())
            .collect();
        assert_eq!(got, want);
        assert_eq!(int(&s.execute(&["LREM", "L", "-20", "dup"])), 20);
        let mut removed = 0;
        let mut i = model.len();
        while i > 0 && removed < 20 {
            i -= 1;
            if model[i] == "dup" {
                model.remove(i);
                removed += 1;
            }
        }
        assert_eq!(int(&s.execute(&["LREM", "L", "10", "dup"])), 10);
        let mut removed = 0;
        let mut i = 0;
        while i < model.len() && removed < 10 {
            if model[i] == "dup" {
                model.remove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }
        check(&model, &mut rng, "after LREM");

        // Pops, pushes and a trim.
        let got = words(s.execute(&["LPOP", "L", "1000"]));
        let want: Vec<String> = model.drain(..1_000).collect();
        assert_eq!(got, want);
        let got = words(s.execute(&["RPOP", "L", "1000"]));
        let n = model.len();
        let mut want: Vec<String> = model.drain(n - 1_000..).collect();
        want.reverse();
        assert_eq!(got, want);
        let mut push = sv(&["LPUSH", "L"]);
        for i in 0..1_000 {
            push.push(format!("front{i}"));
            model.push_front(format!("front{i}"));
        }
        s.execute(&push);
        check(&model, &mut rng, "after pops and pushes");
        assert_eq!(x(&s, "LTRIM L 5000 -5001"), "OK");
        let n = model.len();
        model.truncate(n - 5_000);
        model.drain(..5_000);
        check(&model, &mut rng, "after LTRIM");

        // LMOVE between two big lists, both ends.
        for i in 0..3_000 {
            let (from_left, to_left) = (i % 2 == 0, i % 3 == 0);
            let reply = s.execute(&[
                "LMOVE",
                "L",
                "M",
                if from_left { "LEFT" } else { "RIGHT" },
                if to_left { "LEFT" } else { "RIGHT" },
            ]);
            let item = if from_left {
                model.pop_front()
            } else {
                model.pop_back()
            }
            .unwrap();
            assert_eq!(reply, Reply::bulk(item.clone()));
            let _ = to_left;
            let _ = item;
        }
        assert_eq!(
            int(&s.execute(&["LLEN", "L"])) + int(&s.execute(&["LLEN", "M"])),
            (model.len() + 3_000) as i64
        );
    });
}

#[test]
fn big_set_and_hash_operations() {
    guarded("big-set-hash", 240, || {
        let db = open(conf(8));
        let s = session(&db);
        const N: usize = 100_000;
        let fill = |key: &str, range: std::ops::Range<usize>| {
            for chunk in range.collect::<Vec<_>>().chunks(1_000) {
                let mut cmd = sv(&["SADD", key]);
                cmd.extend(chunk.iter().map(|i| format!("s{i}")));
                assert_eq!(int(&s.execute(&cmd)), chunk.len() as i64);
            }
        };
        fill("A", 0..N);
        fill("B", N / 2..N + N / 2);
        assert_eq!(int(&s.execute(&["SCARD", "A"])), N as i64);
        // SSCAN covers the set once, in small steps.
        let mut cursor = "0".to_string();
        let mut seen = HashSet::new();
        loop {
            let mut reply = array(s.execute(&["SSCAN", "A", &cursor, "COUNT", "5000"]));
            let items = words(reply.pop().expect("items").clone_array());
            cursor = text(&reply[0]);
            assert!(items.len() <= 1100, "SSCAN returned {} items", items.len());
            for m in items {
                assert!(seen.insert(m.clone()), "SSCAN repeated {m}");
            }
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen.len(), N);
        // Set algebra against known sizes.
        assert_eq!(
            int(&s.execute(&["SINTERSTORE", "I", "A", "B"])),
            (N / 2) as i64
        );
        assert_eq!(
            int(&s.execute(&["SUNIONSTORE", "U", "A", "B"])),
            (N + N / 2) as i64
        );
        assert_eq!(
            int(&s.execute(&["SDIFFSTORE", "D", "A", "B"])),
            (N / 2) as i64
        );
        assert_eq!(
            int(&s.execute(&["SINTERCARD", "2", "A", "B"])),
            (N / 2) as i64
        );
        assert_eq!(
            int(&s.execute(&["SINTERCARD", "2", "A", "B", "LIMIT", "1234"])),
            1_234
        );
        for probe in [0usize, N / 2 - 1, N / 2, N - 1, N, N + N / 2 - 1] {
            let m = format!("s{probe}");
            let in_a = probe < N;
            let in_b = probe >= N / 2;
            assert_eq!(
                int(&s.execute(&["SISMEMBER", "I", &m])),
                (in_a && in_b) as i64,
                "I {m}"
            );
            assert_eq!(
                int(&s.execute(&["SISMEMBER", "D", &m])),
                (in_a && !in_b) as i64,
                "D {m}"
            );
            assert_eq!(int(&s.execute(&["SISMEMBER", "U", &m])), 1, "U {m}");
        }
        // Random picks.
        let got = sorted_words(s.execute(&["SRANDMEMBER", "A", "150"]));
        assert_eq!(got.len(), 150);
        assert_eq!(got.iter().collect::<HashSet<_>>().len(), 150);
        assert_eq!(
            array(s.execute(&["SRANDMEMBER", "A", "-2000"])).len(),
            2_000
        );
        // SPOP of a big share, then of the rest.
        let popped = sorted_words(s.execute(&["SPOP", "A", "30000"]));
        assert_eq!(popped.len(), 30_000);
        assert_eq!(popped.iter().collect::<HashSet<_>>().len(), 30_000);
        assert_eq!(int(&s.execute(&["SCARD", "A"])), (N - 30_000) as i64);
        for m in popped.iter().take(200) {
            assert_eq!(int(&s.execute(&["SISMEMBER", "A", m])), 0);
        }
        let rest = array(s.execute(&["SPOP", "A", "1000000"]));
        assert_eq!(rest.len(), N - 30_000);
        assert_eq!(x(&s, "EXISTS A"), ":0");

        // A big hash.
        const H: usize = 50_000;
        for chunk in (0..H).collect::<Vec<_>>().chunks(1_000) {
            let mut cmd = sv(&["HSET", "h"]);
            for i in chunk {
                cmd.push(format!("f{i}"));
                cmd.push(format!("value-{i}"));
            }
            assert_eq!(int(&s.execute(&cmd)), chunk.len() as i64);
        }
        assert_eq!(array(s.execute(&["HKEYS", "h"])).len(), H);
        assert_eq!(array(s.execute(&["HVALS", "h"])).len(), H);
        assert_eq!(
            int(&s.execute(&["HSTRLEN", "h", "f12345"])),
            "value-12345".len() as i64
        );
        assert_eq!(
            s.execute(&["HINCRBYFLOAT", "h", "counter", "1.5"]),
            Reply::bulk("1.5")
        );
        let got = pairs_of(s.execute(&["HRANDFIELD", "h", "300", "WITHVALUES"]));
        assert_eq!(got.len(), 300);
        for (f, v) in &got {
            if f != "counter" {
                assert_eq!(*v, f.replacen('f', "value-", 1));
            }
        }
        assert_eq!(array(s.execute(&["HRANDFIELD", "h", "-1500"])).len(), 1_500);
    });
}
