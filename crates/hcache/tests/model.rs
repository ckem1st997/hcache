//! Single-threaded differential tests: a trivial model of Redis semantics
//! (strings and hashes in a `HashMap`) against the engine, plus targeted
//! edge-case tests for command parsing and replies.

mod support;

use hcache::{Config, Reply};
use hcache_core::engine::Engine;
use hcache_core::testutil::Rng;
use std::collections::{BTreeSet, HashMap};
use support::*;

type B = Vec<u8>;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const OVERFLOW: &str = "ERR increment or decrement would overflow";
const DECR_OVERFLOW: &str = "ERR decrement would overflow";
const STEPS: usize = 150_000;

fn wt() -> Reply {
    err(WRONGTYPE)
}

fn bytes(s: &str) -> B {
    s.as_bytes().to_vec()
}

fn upper(args: &[B]) -> String {
    String::from_utf8_lossy(&args[0]).to_ascii_uppercase()
}

/// Redis' strict integer syntax: canonical decimal only (no '+', no leading
/// zeros, no "-0", no spaces).
fn redis_i64(s: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(s).ok()?;
    let n: i64 = text.parse().ok()?;
    (n.to_string() == text).then_some(n)
}

/// Redis glob matching, written the slow obvious way. Only well-formed
/// patterns are fed to it.
fn glob(p: &[u8], t: &[u8]) -> bool {
    match p.first() {
        None => t.is_empty(),
        Some(b'*') => (0..=t.len()).any(|i| glob(&p[1..], &t[i..])),
        Some(b'?') => !t.is_empty() && glob(&p[1..], &t[1..]),
        Some(b'[') => {
            let Some(&c) = t.first() else { return false };
            let mut i = 1;
            let negate = p.get(i) == Some(&b'^');
            if negate {
                i += 1;
            }
            let mut hit = false;
            while p[i] != b']' {
                if p[i] == b'\\' {
                    hit |= p[i + 1] == c;
                    i += 2;
                } else if p.get(i + 1) == Some(&b'-') && p.get(i + 2).is_some_and(|&h| h != b']') {
                    let (lo, hi) = (p[i].min(p[i + 2]), p[i].max(p[i + 2]));
                    hit |= lo <= c && c <= hi;
                    i += 3;
                } else {
                    hit |= p[i] == c;
                    i += 1;
                }
            }
            hit != negate && glob(&p[i + 1..], &t[1..])
        }
        Some(b'\\') if p.len() >= 2 => !t.is_empty() && p[1] == t[0] && glob(&p[2..], &t[1..]),
        Some(&c) => !t.is_empty() && c == t[0] && glob(&p[1..], &t[1..]),
    }
}

enum Val {
    Str(B),
    Hash(HashMap<B, B>),
}

#[derive(Default)]
struct Model {
    map: HashMap<B, Val>,
}

impl Model {
    /// Reply of a well-formed command.
    fn exec(&mut self, a: &[B]) -> Reply {
        let name = upper(a);
        match name.as_str() {
            "PING" if a.len() == 1 => Reply::Simple("PONG".into()),
            "PING" | "ECHO" => Reply::Bulk(a[1].clone()),
            "DBSIZE" => Reply::Int(self.map.len() as i64),
            "GET" => match self.map.get(&a[1]) {
                None => Reply::Nil,
                Some(Val::Str(v)) => Reply::Bulk(v.clone()),
                Some(Val::Hash(_)) => wt(),
            },
            "SET" => {
                self.map.insert(a[1].clone(), Val::Str(a[2].clone()));
                Reply::Ok
            }
            "DEL" | "UNLINK" => {
                let n = a[1..]
                    .iter()
                    .filter(|k| self.map.remove(*k).is_some())
                    .count();
                Reply::Int(n as i64)
            }
            "EXISTS" => {
                let n = a[1..].iter().filter(|k| self.map.contains_key(*k)).count();
                Reply::Int(n as i64)
            }
            "INCR" | "DECR" | "INCRBY" | "DECRBY" => self.incr(&name, a),
            "MSET" => {
                for pair in a[1..].chunks(2) {
                    self.map.insert(pair[0].clone(), Val::Str(pair[1].clone()));
                }
                Reply::Ok
            }
            "MGET" => Reply::Array(
                a[1..]
                    .iter()
                    .map(|k| match self.map.get(k) {
                        Some(Val::Str(v)) => Reply::Bulk(v.clone()),
                        _ => Reply::Nil,
                    })
                    .collect(),
            ),
            "KEYS" => Reply::Array(
                self.map
                    .keys()
                    .filter(|k| glob(&a[1], k))
                    .map(|k| Reply::Bulk(k.clone()))
                    .collect(),
            ),
            "FLUSHALL" | "FLUSHDB" => {
                self.map.clear();
                Reply::Ok
            }
            "HSET" => {
                if matches!(self.map.get(&a[1]), Some(Val::Str(_))) {
                    return wt();
                }
                let Val::Hash(h) = self
                    .map
                    .entry(a[1].clone())
                    .or_insert_with(|| Val::Hash(HashMap::new()))
                else {
                    unreachable!("checked above")
                };
                let mut added = 0;
                for pair in a[2..].chunks(2) {
                    added += i64::from(h.insert(pair[0].clone(), pair[1].clone()).is_none());
                }
                Reply::Int(added)
            }
            "HGET" => match self.map.get(&a[1]) {
                None => Reply::Nil,
                Some(Val::Str(_)) => wt(),
                Some(Val::Hash(h)) => h.get(&a[2]).map_or(Reply::Nil, |v| Reply::Bulk(v.clone())),
            },
            "HDEL" => match self.map.get_mut(&a[1]) {
                None => Reply::Int(0),
                Some(Val::Str(_)) => wt(),
                Some(Val::Hash(h)) => {
                    let n = a[2..].iter().filter(|f| h.remove(*f).is_some()).count();
                    if h.is_empty() {
                        self.map.remove(&a[1]);
                    }
                    Reply::Int(n as i64)
                }
            },
            "HLEN" => match self.map.get(&a[1]) {
                None => Reply::Int(0),
                Some(Val::Str(_)) => wt(),
                Some(Val::Hash(h)) => Reply::Int(h.len() as i64),
            },
            "HGETALL" => match self.map.get(&a[1]) {
                None => Reply::Array(Vec::new()),
                Some(Val::Str(_)) => wt(),
                Some(Val::Hash(h)) => Reply::Array(
                    h.iter()
                        .flat_map(|(f, v)| [Reply::Bulk(f.clone()), Reply::Bulk(v.clone())])
                        .collect(),
                ),
            },
            other => panic!("model: unsupported command {other}"),
        }
    }

    fn incr(&mut self, name: &str, a: &[B]) -> Reply {
        let by = match name {
            "INCR" => 1,
            "DECR" => -1,
            _ => {
                let Some(n) = redis_i64(&a[2]) else {
                    return err(NOT_INT);
                };
                if name == "DECRBY" {
                    match n.checked_neg() {
                        Some(m) => m,
                        None => return err(DECR_OVERFLOW),
                    }
                } else {
                    n
                }
            }
        };
        let current = match self.map.get(&a[1]) {
            None => 0,
            Some(Val::Str(v)) => match redis_i64(v) {
                Some(n) => n,
                None => return err(NOT_INT),
            },
            Some(Val::Hash(_)) => return wt(),
        };
        let Some(next) = current.checked_add(by) else {
            return err(OVERFLOW);
        };
        self.map
            .insert(a[1].clone(), Val::Str(next.to_string().into_bytes()));
        Reply::Int(next)
    }
}

fn as_bytes(r: &Reply) -> &[u8] {
    match r {
        Reply::Bulk(b) => b,
        other => panic!("expected a bulk reply, got {}", brief(other)),
    }
}

/// Order-insensitive form of the replies whose order is unspecified.
fn norm(name: &str, reply: Reply) -> Reply {
    let Reply::Array(mut items) = reply else {
        return reply;
    };
    match name {
        "KEYS" => {
            items.sort_by(|x, y| as_bytes(x).cmp(as_bytes(y)));
            Reply::Array(items)
        }
        "HGETALL" => {
            assert!(
                items.len() % 2 == 0,
                "HGETALL returned an odd element count"
            );
            let mut pairs: Vec<(B, B)> = items
                .chunks(2)
                .map(|c| (as_bytes(&c[0]).to_vec(), as_bytes(&c[1]).to_vec()))
                .collect();
            pairs.sort();
            Reply::Array(
                pairs
                    .into_iter()
                    .flat_map(|(f, v)| [Reply::Bulk(f), Reply::Bulk(v)])
                    .collect(),
            )
        }
        _ => Reply::Array(items),
    }
}

fn show(args: &[B]) -> String {
    let parts: Vec<String> = args
        .iter()
        .map(|a| {
            let s = String::from_utf8_lossy(a);
            if s.len() > 40 {
                format!(
                    "{:?}...(+{} bytes)",
                    &s[..s.floor_char_boundary(40)],
                    a.len()
                )
            } else {
                format!("{s:?}")
            }
        })
        .collect();
    parts.join(" ")
}

struct Gen {
    rng: Rng,
    keys: Vec<B>,
    fields: Vec<B>,
    vals: Vec<B>,
    bys: Vec<B>,
    tokens: Vec<B>,
    big: B,
}

fn list(items: &[&str]) -> Vec<B> {
    items.iter().map(|s| bytes(s)).collect()
}

fn pick(rng: &mut Rng, items: &[B]) -> B {
    items[rng.below(items.len() as u64) as usize].clone()
}

impl Gen {
    fn new(seed: u64) -> Gen {
        let mut rng = Rng::new(seed);
        let big = rng.bytes(1 << 20);
        let mut keys = list(&[
            "a",
            "b",
            "c",
            "d",
            "",
            "k\r\n",
            "n\0l",
            "{a}x",
            "{a}y",
            "{}",
            "{a",
            "a}",
            "{}a",
            "{b}{a}",
            "x{a}z",
            "long-key-xxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        ]);
        keys.push(vec![0xff, 0xfe, b'\r', 0]);
        Gen {
            rng,
            keys,
            fields: list(&["f1", "f2", "f3", "", "f\r\n", "\0", "f5", "f6"]),
            vals: list(&[
                "0",
                "1",
                "-1",
                "42",
                "9223372036854775807",
                "-9223372036854775808",
                "9223372036854775806",
                "-9223372036854775807",
                "",
                "abc",
                " 1",
                "1 ",
                "+1",
                "1.5",
                "99999999999999999999",
                "12345678901234567890",
                "-",
                "v\r\nv",
                "\0\0",
                "1e3",
                "0x10",
            ]),
            // Leading zeros and "-0" are kept out of the random mix on
            // purpose: `leading_zero_integers_are_not_integers` covers them.
            bys: list(&[
                "0",
                "1",
                "-1",
                "5",
                "-5",
                "9223372036854775807",
                "-9223372036854775808",
                "-9223372036854775807",
                "9223372036854775808",
                "abc",
                "",
                "1.0",
                "+3",
                " 2",
            ]),
            tokens: list(&[
                "*", "?", "a", "b", "c", "d", "x", "y", "z", "{", "}", "k", "n", "l", "[ab]",
                "[^a]", "[a-c]", "[^b-y]", "\\*", "\\?", "\r", "\n", "\0", "f", "1", "-",
            ]),
            big,
        }
    }

    fn key(&mut self) -> B {
        pick(&mut self.rng, &self.keys)
    }

    fn field(&mut self) -> B {
        pick(&mut self.rng, &self.fields)
    }

    fn value(&mut self) -> B {
        match self.rng.below(10) {
            0 => {
                let len = self.rng.below(40) as usize;
                self.rng.bytes(len)
            }
            1 => (self.rng.below(2000) as i64 - 1000)
                .to_string()
                .into_bytes(),
            _ => pick(&mut self.rng, &self.vals),
        }
    }

    fn by(&mut self) -> B {
        if self.rng.chance(4) {
            (self.rng.below(200) as i64 - 100).to_string().into_bytes()
        } else {
            pick(&mut self.rng, &self.bys)
        }
    }

    fn pattern(&mut self) -> B {
        if self.rng.chance(6) {
            return bytes("*");
        }
        let n = 1 + self.rng.below(4);
        let mut out = Vec::new();
        for _ in 0..n {
            out.extend(pick(&mut self.rng, &self.tokens));
        }
        out
    }

    fn count(&mut self, max: u64) -> usize {
        1 + self.rng.below(max) as usize
    }

    fn name(&mut self, name: &str) -> B {
        match self.rng.below(3) {
            0 => name.to_ascii_uppercase().into_bytes(),
            1 => name.to_ascii_lowercase().into_bytes(),
            _ => name
                .bytes()
                .map(|c| {
                    if self.rng.chance(2) {
                        c.to_ascii_lowercase()
                    } else {
                        c.to_ascii_uppercase()
                    }
                })
                .collect(),
        }
    }

    fn command(&mut self, step: usize) -> Vec<B> {
        // A few 1 MB values over the whole run.
        if step % 20_000 == 7 {
            let (n, k, v) = (self.name("SET"), self.key(), self.big.clone());
            return vec![n, k, v];
        }
        if step % 20_000 == 11 {
            let (n, k, f, v) = (
                self.name("HSET"),
                self.key(),
                self.field(),
                self.big.clone(),
            );
            return vec![n, k, f, v];
        }
        if self.rng.chance(3000) {
            let name = if self.rng.chance(2) {
                "FLUSHALL"
            } else {
                "FLUSHDB"
            };
            return vec![self.name(name)];
        }
        let roll = self.rng.below(100);
        let (name, rest): (&str, Vec<B>) = match roll {
            0..=8 => ("GET", vec![self.key()]),
            9..=17 | 96..=97 => ("SET", vec![self.key(), self.value()]),
            18..=23 => {
                let n = self.count(3);
                ("DEL", (0..n).map(|_| self.key()).collect())
            }
            24 => ("UNLINK", vec![self.key()]),
            25..=29 => {
                let n = self.count(4);
                ("EXISTS", (0..n).map(|_| self.key()).collect())
            }
            30..=34 => ("INCR", vec![self.key()]),
            35..=37 => ("DECR", vec![self.key()]),
            38..=43 => ("INCRBY", vec![self.key(), self.by()]),
            44..=47 => ("DECRBY", vec![self.key(), self.by()]),
            48..=57 | 98..=99 => {
                let n = self.count(3);
                let mut rest = vec![self.key()];
                for _ in 0..n {
                    rest.push(self.field());
                    rest.push(self.value());
                }
                ("HSET", rest)
            }
            58..=63 => ("HGET", vec![self.key(), self.field()]),
            64..=69 => {
                let n = self.count(3);
                let mut rest = vec![self.key()];
                rest.extend((0..n).map(|_| self.field()));
                ("HDEL", rest)
            }
            70..=72 => ("HLEN", vec![self.key()]),
            73..=77 => ("HGETALL", vec![self.key()]),
            78..=82 => {
                let n = self.count(3);
                let mut rest = Vec::new();
                for _ in 0..n {
                    rest.push(self.key());
                    rest.push(self.value());
                }
                ("MSET", rest)
            }
            83..=87 => {
                let n = self.count(4);
                ("MGET", (0..n).map(|_| self.key()).collect())
            }
            88..=91 => ("KEYS", vec![self.pattern()]),
            92..=93 => ("DBSIZE", vec![]),
            94 => (
                "PING",
                if self.rng.chance(2) {
                    vec![self.value()]
                } else {
                    vec![]
                },
            ),
            _ => ("ECHO", vec![self.value()]),
        };
        let mut args = vec![self.name(name)];
        args.extend(rest);
        args
    }
}

#[derive(Debug, Default)]
struct Coverage {
    wrongtype: u32,
    not_int: u32,
    overflow: u32,
    hgetall: u32,
    keys: u32,
    hset_new: u32,
    get_hit: u32,
}

fn run_model(seed: u64, shards: usize, scan_chunk: usize, hash_tags: bool, steps: usize) {
    let config = Config {
        shards,
        scan_chunk,
        hash_tags,
        ..Config::default()
    };
    let label = format!("seed={seed} shards={shards} scan_chunk={scan_chunk} tags={hash_tags}");
    let db = open(config);
    let s = session(&db);
    let mut model = Model::default();
    let mut gen = Gen::new(seed);
    let mut cover = Coverage::default();
    for step in 0..steps {
        let args = gen.command(step);
        let name = upper(&args);
        let want = norm(&name, model.exec(&args));
        let got = norm(&name, s.execute(&args));
        assert!(
            want == got,
            "MISMATCH [{label}] step {step}: {}\n  model:  {}\n  engine: {}",
            show(&args),
            brief(&want),
            brief(&got)
        );
        match &got {
            Reply::Error(e) if e.starts_with("WRONGTYPE") => cover.wrongtype += 1,
            Reply::Error(e) if e == NOT_INT => cover.not_int += 1,
            Reply::Error(e) if e == OVERFLOW => cover.overflow += 1,
            Reply::Array(a) if !a.is_empty() && name == "HGETALL" => cover.hgetall += 1,
            Reply::Array(a) if !a.is_empty() && name == "KEYS" => cover.keys += 1,
            Reply::Int(n) if *n > 0 && name == "HSET" => cover.hset_new += 1,
            Reply::Bulk(_) if name == "GET" => cover.get_hit += 1,
            _ => {}
        }
    }
    // The random mix must really hit the interesting branches.
    if steps >= 100_000 {
        eprintln!("coverage [{label}]: {cover:?}");
        assert!(
            cover.wrongtype > 1000,
            "[{label}] too few WRONGTYPE cases: {cover:?}"
        );
        assert!(
            cover.not_int > 300,
            "[{label}] too few INCR-on-non-integer cases: {cover:?}"
        );
        assert!(
            cover.overflow > 5,
            "[{label}] too few overflow cases: {cover:?}"
        );
        assert!(
            cover.hgetall > 300
                && cover.keys > 1000
                && cover.hset_new > 1000
                && cover.get_hit > 1000
        );
    }
    // Whole-state comparison at the end.
    let all = vec![bytes("KEYS"), bytes("*")];
    let want = norm("KEYS", model.exec(&all));
    let got = norm("KEYS", s.execute(&all));
    assert!(want == got, "[{label}] final KEYS differ");
    let keys: Vec<B> = model.map.keys().cloned().collect();
    for key in keys {
        for cmd in ["GET", "HGETALL"] {
            let args = vec![bytes(cmd), key.clone()];
            let want = norm(cmd, model.exec(&args));
            let got = norm(cmd, s.execute(&args));
            assert!(
                want == got,
                "[{label}] final {cmd} of {:?}",
                show(std::slice::from_ref(&key))
            );
        }
    }
    assert_eq!(
        s.execute(&["DBSIZE"]),
        Reply::Int(model.map.len() as i64),
        "[{label}] final DBSIZE"
    );
}

macro_rules! model_test {
    ($name:ident, $seed:expr, $shards:expr, $chunk:expr, $tags:expr) => {
        #[test]
        fn $name() {
            guarded(stringify!($name), 120, || {
                run_model($seed, $shards, $chunk, $tags, STEPS)
            });
        }
    };
}

model_test!(differential_1_shard_chunk_1, 1, 1, 1, false);
model_test!(differential_2_shards_chunk_2, 2, 2, 2, false);
model_test!(differential_64_shards_chunk_3, 3, 64, 3, false);
model_test!(differential_1_shard_default_chunk, 4, 1, 256, false);
model_test!(differential_64_shards_default_chunk, 5, 64, 256, false);
model_test!(differential_hash_tags_64_shards_chunk_1, 6, 64, 1, true);
model_test!(differential_hash_tags_2_shards_chunk_3, 7, 2, 3, true);
model_test!(differential_hash_tags_1_shard_chunk_2, 8, 1, 2, true);

// ---------------------------------------------------------------------
// Targeted tests
// ---------------------------------------------------------------------

fn fresh() -> (hcache::Db, hcache::Session) {
    let db = open(Config::default());
    let s = session(&db);
    (db, s)
}

#[test]
fn arity_errors_for_every_command() {
    // (name, is `n` (counting the name) a valid argument count)
    type Arity = fn(usize) -> bool;
    let table: &[(&str, Arity)] = &[
        ("GET", |n| n == 2),
        ("INCR", |n| n == 2),
        ("DECR", |n| n == 2),
        ("INCRBY", |n| n == 3),
        ("DECRBY", |n| n == 3),
        ("HSET", |n| n >= 4 && n % 2 == 0),
        ("HMSET", |n| n >= 4 && n % 2 == 0),
        ("HGET", |n| n == 3),
        ("HDEL", |n| n >= 3),
        ("HLEN", |n| n == 2),
        ("HGETALL", |n| n == 2),
        ("DEL", |n| n >= 2),
        ("UNLINK", |n| n >= 2),
        ("EXISTS", |n| n >= 2),
        ("MGET", |n| n >= 2),
        ("MSET", |n| n >= 3 && n % 2 == 1),
        ("KEYS", |n| n == 2),
        ("DBSIZE", |n| n == 1),
        ("ECHO", |n| n == 2),
        ("SELECT", |n| n == 2),
        ("CLIENT", |n| n >= 2),
        ("EVAL", |n| n >= 3),
        ("SCRIPT", |n| n >= 2),
        ("PING", |n| n <= 2),
    ];
    guarded("arity", 30, move || {
        let (_db, s) = fresh();
        for (name, valid) in table {
            for spelling in [name.to_string(), name.to_ascii_lowercase()] {
                for n in 1..=8usize {
                    if valid(n) {
                        continue;
                    }
                    let mut args: Vec<&str> = vec![&spelling];
                    args.extend(std::iter::repeat_n("x", n - 1));
                    let want = format!(
                        "ERR wrong number of arguments for '{}' command",
                        name.to_ascii_lowercase()
                    );
                    let got = s.execute(&args);
                    assert!(
                        got == err(&want),
                        "{args:?}: expected {want:?}, got {}",
                        brief(&got)
                    );
                }
            }
        }
        // SET: too few is an arity error, an unknown option a syntax error.
        assert_eq!(
            s.execute(&["SET", "k"]),
            err("ERR wrong number of arguments for 'set' command")
        );
        assert_eq!(
            s.execute(&["SET", "k", "v", "BOGUS"]),
            err("ERR syntax error")
        );
        assert_eq!(s.execute::<&str>(&[]), err("ERR empty command"));
    });
}

#[test]
fn unknown_commands_and_case_insensitivity() {
    guarded("unknown", 30, || {
        let (_db, s) = fresh();
        for name in ["FOO", "GETT", "get\0", "ABCDEFGHIJKLMNOPQ", "", "SETT", "é"] {
            let got = s.execute(&[name, "k"]);
            assert!(
                has_err(&got, "ERR unknown command"),
                "{name:?} -> {}",
                brief(&got)
            );
        }
        let got = s.execute(&[vec![0xffu8, 0xfe], b"k".to_vec()]);
        assert!(has_err(&got, "ERR unknown command"), "{}", brief(&got));
        let got = s.execute(&["x".repeat(1 << 20).as_str(), "k"]);
        assert!(has_err(&got, "ERR unknown command"), "{}", brief(&got));
        // The echoed name is bounded, so a huge name cannot blow up the reply.
        assert!(err_text(&got).len() < 200);

        assert_eq!(s.execute(&["sEt", "k", "v"]), Reply::Ok);
        assert_eq!(s.execute(&["GeT", "k"]), bulk("v"));
        assert_eq!(s.execute(&["iNcR", "n"]), Reply::Int(1));
        assert_eq!(s.execute(&["hSeT", "h", "f", "1"]), Reply::Int(1));
        assert_eq!(
            s.execute(&["hGeTaLl", "h"]),
            Reply::Array(vec![bulk("f"), bulk("1")])
        );
        assert_eq!(s.execute(&["mSeT", "a", "1", "b", "2"]), Reply::Ok);
        assert_eq!(
            s.execute(&["mGeT", "a", "zz", "b"]),
            Reply::Array(vec![bulk("1"), Reply::Nil, bulk("2")])
        );
        assert_eq!(s.execute(&["dBsIzE"]), Reply::Int(5));
        assert_eq!(s.execute(&["eChO", "hi"]), bulk("hi"));
        assert_eq!(s.execute(&["pInG"]), Reply::Simple("PONG".into()));
        assert_eq!(s.execute(&["fLuShAlL"]), Reply::Ok);
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
    });
}

#[test]
fn connection_level_commands() {
    guarded("conn", 30, || {
        let (_db, s) = fresh();
        assert_eq!(s.execute(&["PING"]), Reply::Simple("PONG".into()));
        assert_eq!(s.execute(&["PING", "a b\r\n"]), bulk("a b\r\n"));
        assert_eq!(s.execute(&["ECHO", ""]), bulk(""));
        assert_eq!(s.execute(&["SELECT", "0"]), Reply::Ok);
        assert_eq!(
            s.execute(&["SELECT", "16"]),
            err("ERR DB index is out of range")
        );
        assert_eq!(
            s.execute(&["SELECT", "-1"]),
            err("ERR DB index is out of range")
        );
        assert_eq!(s.execute(&["SELECT", "x"]), err("ERR invalid DB index"));
        assert_eq!(s.execute(&["CLIENT", "SETNAME", "x"]), Reply::Ok);
        assert!(matches!(s.execute(&["COMMAND", "COUNT"]), Reply::Int(n) if n > 150));
        assert_eq!(
            s.execute(&["CONFIG", "GET", "nothing-such"]),
            Reply::Array(vec![])
        );
        assert_eq!(s.execute(&["CONFIG", "SET", "x", "y"]), Reply::Ok);
        assert_eq!(s.execute(&["SCRIPT", "FLUSH"]), Reply::Ok);
        assert!(has_err(
            &s.execute(&["SCRIPT", "LOAD"]),
            "wrong number of arguments for 'script' subcommand"
        ));
        assert!(has_err(
            &s.execute(&["SCRIPT", "NOPE", "x"]),
            "unknown SCRIPT subcommand"
        ));
        assert!(matches!(s.execute(&["INFO"]), Reply::Bulk(_)));
        assert_eq!(s.execute(&["QUIT"]), Reply::Ok);
    });
}

#[test]
fn incr_edge_values() {
    guarded("incr_edges", 30, || {
        let (_db, s) = fresh();
        let max = i64::MAX.to_string();
        let min = i64::MIN.to_string();

        // Values that are not integers by Redis' rules.
        for bad in [
            "",
            " 1",
            "1 ",
            "+1",
            "1.0",
            "1e3",
            "0x10",
            "abc",
            "-",
            "--1",
            "1\n",
            "9223372036854775808",
            "-9223372036854775809",
            "99999999999999999999",
        ] {
            s.execute(&["SET", "k", bad]);
            assert_eq!(s.execute(&["INCR", "k"]), err(NOT_INT), "INCR on {bad:?}");
            assert_eq!(
                s.execute(&["DECRBY", "k", "1"]),
                err(NOT_INT),
                "DECRBY on {bad:?}"
            );
            assert_eq!(
                s.execute(&["GET", "k"]),
                bulk(bad),
                "value changed by a failed INCR"
            );
        }

        // Overflow at both ends, value untouched afterwards.
        s.execute(&["SET", "k", &max]);
        assert_eq!(s.execute(&["INCR", "k"]), err(OVERFLOW));
        assert_eq!(s.execute(&["INCRBY", "k", "1"]), err(OVERFLOW));
        assert_eq!(s.execute(&["GET", "k"]), bulk(&max));
        assert_eq!(s.execute(&["DECR", "k"]), Reply::Int(i64::MAX - 1));
        s.execute(&["SET", "k", &min]);
        assert_eq!(s.execute(&["DECR", "k"]), err(OVERFLOW));
        assert_eq!(s.execute(&["DECRBY", "k", "1"]), err(OVERFLOW));
        assert_eq!(s.execute(&["INCR", "k"]), Reply::Int(i64::MIN + 1));

        // INCRBY with i64::MIN.
        s.execute(&["SET", "k", "0"]);
        assert_eq!(s.execute(&["INCRBY", "k", &min]), Reply::Int(i64::MIN));
        s.execute(&["SET", "k", "-1"]);
        assert_eq!(s.execute(&["INCRBY", "k", &min]), err(OVERFLOW));
        s.execute(&["SET", "k", "1"]);
        assert_eq!(s.execute(&["INCRBY", "k", &min]), Reply::Int(i64::MIN + 1));
        // DECRBY i64::MIN cannot be negated.
        assert_eq!(s.execute(&["DECRBY", "k", &min]), err(DECR_OVERFLOW));
        // k is MIN+1 here: subtracting MIN+1 gives 0, subtracting -MAX again would overflow.
        assert_eq!(
            s.execute(&["DECRBY", "k", &(i64::MIN + 1).to_string()]),
            Reply::Int(0)
        );
        assert_eq!(
            s.execute(&["DECRBY", "k", &(i64::MIN + 1).to_string()]),
            Reply::Int(i64::MAX)
        );
        assert_eq!(
            s.execute(&["DECRBY", "k", &(i64::MIN + 1).to_string()]),
            err(OVERFLOW)
        );
        // Increment argument must be an integer.
        for bad in ["1.5", "", " 1", "+1", "abc", "9223372036854775808"] {
            assert_eq!(
                s.execute(&["INCRBY", "fresh", bad]),
                err(NOT_INT),
                "INCRBY {bad:?}"
            );
            assert_eq!(s.execute(&["EXISTS", "fresh"]), Reply::Int(0));
        }
        // New key.
        assert_eq!(s.execute(&["INCRBY", "new", &max]), Reply::Int(i64::MAX));
        assert_eq!(s.execute(&["DECRBY", "new2", "5"]), Reply::Int(-5));
        // Wrong type is reported before arithmetic, but after argument parsing.
        s.execute(&["HSET", "h", "f", "v"]);
        assert_eq!(s.execute(&["INCR", "h"]), wt());
        assert_eq!(s.execute(&["INCRBY", "h", "x"]), err(NOT_INT));
        assert_eq!(s.execute(&["DECRBY", "h", &min]), err(DECR_OVERFLOW));
        // A stored integer is canonical afterwards.
        s.execute(&["SET", "c", "-5"]);
        assert_eq!(s.execute(&["INCRBY", "c", "5"]), Reply::Int(0));
        assert_eq!(s.execute(&["GET", "c"]), bulk("0"));
    });
}

/// Redis accepts only canonical decimal integers: "007", "00" and "-0" are
/// not integers (`string2ll`), so INCR on them is an error.
#[test]
fn leading_zero_integers_are_not_integers() {
    guarded("leading_zeros", 30, || {
        let (_db, s) = fresh();
        for bad in ["007", "00", "-0", "0000000000000000000001", "-007", "01"] {
            s.execute(&["SET", "k", bad]);
            assert_eq!(
                s.execute(&["INCR", "k"]),
                err(NOT_INT),
                "INCR on stored {bad:?}"
            );
            assert_eq!(s.execute(&["GET", "k"]), bulk(bad));
            assert_eq!(
                s.execute(&["INCRBY", "other", bad]),
                err(NOT_INT),
                "INCRBY by {bad:?}"
            );
            assert_eq!(
                s.execute(&["DECRBY", "other", bad]),
                err(NOT_INT),
                "DECRBY by {bad:?}"
            );
        }
        // The same strictness applies to EVAL's numkeys.
        assert_eq!(s.execute(&["EVAL", "return 1", "01"]), err(NOT_INT));
    });
}

#[test]
fn hset_counts_new_fields_only() {
    guarded("hset", 30, || {
        let (_db, s) = fresh();
        assert_eq!(s.execute(&["HSET", "h", "a", "1", "b", "2"]), Reply::Int(2));
        // Duplicate field inside one call: counted once, last value wins.
        assert_eq!(
            s.execute(&["HSET", "h", "c", "1", "c", "2", "a", "9"]),
            Reply::Int(1)
        );
        assert_eq!(s.execute(&["HGET", "h", "c"]), bulk("2"));
        assert_eq!(s.execute(&["HGET", "h", "a"]), bulk("9"));
        assert_eq!(s.execute(&["HLEN", "h"]), Reply::Int(3));
        assert_eq!(s.execute(&["HSET", "h", "a", "9"]), Reply::Int(0));
        // Empty field and value.
        assert_eq!(s.execute(&["HSET", "h", "", ""]), Reply::Int(1));
        assert_eq!(s.execute(&["HGET", "h", ""]), bulk(""));
        // HDEL with a repeated field counts once; last HDEL removes the key.
        assert_eq!(s.execute(&["HDEL", "h", "a", "a", "nope"]), Reply::Int(1));
        assert_eq!(s.execute(&["HDEL", "h", "b", "c", ""]), Reply::Int(3));
        assert_eq!(s.execute(&["EXISTS", "h"]), Reply::Int(0));
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
        assert_eq!(s.execute(&["HGETALL", "h"]), Reply::Array(vec![]));
        assert_eq!(s.execute(&["HLEN", "h"]), Reply::Int(0));
        assert_eq!(s.execute(&["HDEL", "h", "a"]), Reply::Int(0));
        // The key is a plain string after the hash vanished.
        assert_eq!(s.execute(&["SET", "h", "v"]), Reply::Ok);
        assert_eq!(s.execute(&["HSET", "h", "a", "1"]), wt());
        assert_eq!(s.execute(&["HGET", "h", "a"]), wt());
        assert_eq!(s.execute(&["HDEL", "h", "a"]), wt());
        assert_eq!(s.execute(&["HLEN", "h"]), wt());
        assert_eq!(s.execute(&["HGETALL", "h"]), wt());
        // And SET over a hash replaces it.
        s.execute(&["HSET", "g", "a", "1"]);
        assert_eq!(s.execute(&["GET", "g"]), wt());
        assert_eq!(s.execute(&["SET", "g", "v"]), Reply::Ok);
        assert_eq!(s.execute(&["GET", "g"]), bulk("v"));
    });
}

/// HMSET is the deprecated alias of HSET but, unlike HSET, replies +OK
/// (Redis 7/8). The spike returns HSET's integer.
#[test]
fn hmset_replies_ok() {
    guarded("hmset", 30, || {
        let (_db, s) = fresh();
        assert_eq!(s.execute(&["HMSET", "h", "a", "1", "b", "2"]), Reply::Ok);
        assert_eq!(s.execute(&["HMSET", "h", "a", "3"]), Reply::Ok);
        assert_eq!(s.execute(&["HGET", "h", "a"]), bulk("3"));
    });
}

#[test]
fn del_and_exists_repeated_keys() {
    guarded("del_exists", 30, || {
        let (_db, s) = fresh();
        s.execute(&["SET", "a", "1"]);
        s.execute(&["HSET", "h", "f", "v"]);
        // EXISTS counts every mention, DEL only what it actually removed.
        assert_eq!(
            s.execute(&["EXISTS", "a", "a", "a", "zz", "h"]),
            Reply::Int(4)
        );
        assert_eq!(s.execute(&["DEL", "a", "a", "a", "zz"]), Reply::Int(1));
        assert_eq!(s.execute(&["EXISTS", "a"]), Reply::Int(0));
        assert_eq!(s.execute(&["DEL", "h", "h"]), Reply::Int(1));
        assert_eq!(s.execute(&["UNLINK", "h"]), Reply::Int(0));
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
    });
}

#[test]
fn mset_mget_corner_cases() {
    guarded("mset_mget", 30, || {
        let (_db, s) = fresh();
        // Repeated key: last wins.
        assert_eq!(
            s.execute(&["MSET", "a", "1", "b", "2", "a", "3"]),
            Reply::Ok
        );
        assert_eq!(s.execute(&["GET", "a"]), bulk("3"));
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(2));
        // MGET: hash and missing keys read as nil, repeats allowed.
        s.execute(&["HSET", "h", "f", "v"]);
        assert_eq!(
            s.execute(&["MGET", "a", "h", "zz", "a", "b"]),
            Reply::Array(vec![
                bulk("3"),
                Reply::Nil,
                Reply::Nil,
                bulk("3"),
                bulk("2")
            ])
        );
        // MSET over a hash replaces it.
        s.execute(&["MSET", "h", "str", "x", "y"]);
        assert_eq!(s.execute(&["GET", "h"]), bulk("str"));
        // Empty key and empty value.
        assert_eq!(s.execute(&["MSET", "", "", "e", ""]), Reply::Ok);
        assert_eq!(
            s.execute(&["MGET", "", "e"]),
            Reply::Array(vec![bulk(""), bulk("")])
        );
        assert_eq!(s.execute(&["SET", "", "v"]), Reply::Ok);
        assert_eq!(s.execute(&["GET", ""]), bulk("v"));
        assert_eq!(s.execute(&["EXISTS", ""]), Reply::Int(1));
    });
}

#[test]
fn binary_keys_and_values() {
    guarded("binary", 30, || {
        let (_db, s) = fresh();
        let keys: Vec<Vec<u8>> = vec![
            b"a\r\nb".to_vec(),
            b"\0".to_vec(),
            b"\0\0".to_vec(),
            vec![0xff, 0xfe, 0x00, b'\r', b'\n'],
            b"*1\r\n$3\r\nfoo\r\n".to_vec(),
            Vec::new(),
        ];
        for (i, k) in keys.iter().enumerate() {
            let v = vec![i as u8, 0, b'\r', b'\n', 0xff];
            assert_eq!(
                s.execute(&[b"SET".to_vec(), k.clone(), v.clone()]),
                Reply::Ok
            );
            assert_eq!(s.execute(&[b"GET".to_vec(), k.clone()]), Reply::Bulk(v));
        }
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(keys.len() as i64));
        let found = array(s.execute(&["KEYS", "*"]));
        let found: BTreeSet<Vec<u8>> = found.iter().map(|r| as_bytes(r).to_vec()).collect();
        assert_eq!(found, keys.iter().cloned().collect::<BTreeSet<_>>());
        // Literal CRLF and NUL in patterns.
        let got = array(s.execute(&[b"KEYS".to_vec(), b"a\r\nb".to_vec()]));
        assert_eq!(got, vec![Reply::Bulk(b"a\r\nb".to_vec())]);
        let got = array(s.execute(&[b"KEYS".to_vec(), b"\0".to_vec()]));
        assert_eq!(got, vec![Reply::Bulk(b"\0".to_vec())]);
        let got = array(s.execute(&[b"KEYS".to_vec(), b"?".to_vec()]));
        assert_eq!(got, vec![Reply::Bulk(b"\0".to_vec())]);
        let got = array(s.execute(&[b"KEYS".to_vec(), b"\0?".to_vec()]));
        assert_eq!(got, vec![Reply::Bulk(b"\0\0".to_vec())]);
        // Pattern length limit.
        let long = "a".repeat(1025);
        assert!(has_err(&s.execute(&["KEYS", &long]), "pattern too long"));
        assert!(matches!(
            s.execute(&["KEYS", &"a".repeat(1024)]),
            Reply::Array(_)
        ));
    });
}

#[test]
fn megabyte_values() {
    guarded("megabyte", 60, || {
        let (_db, s) = fresh();
        let mut rng = Rng::new(99);
        let big = rng.bytes(1 << 20);
        let bigkey = rng.bytes(1 << 20);
        assert_eq!(
            s.execute(&[b"SET".to_vec(), b"k".to_vec(), big.clone()]),
            Reply::Ok
        );
        assert_eq!(s.execute(&["GET", "k"]), Reply::Bulk(big.clone()));
        assert_eq!(s.execute(&["INCR", "k"]), err(NOT_INT));
        assert_eq!(
            s.execute(&[b"SET".to_vec(), bigkey.clone(), b"v".to_vec()]),
            Reply::Ok
        );
        assert_eq!(s.execute(&[b"GET".to_vec(), bigkey.clone()]), bulk("v"));
        assert_eq!(
            s.execute(&[
                b"MSET".to_vec(),
                b"a".to_vec(),
                big.clone(),
                b"b".to_vec(),
                big.clone()
            ]),
            Reply::Ok
        );
        assert_eq!(
            s.execute(&["MGET", "a", "b", "k"]),
            Reply::Array(vec![Reply::Bulk(big.clone()); 3])
        );
        assert_eq!(
            s.execute(&[
                b"HSET".to_vec(),
                b"h".to_vec(),
                bigkey.clone(),
                big.clone(),
                b"g".to_vec(),
                big.clone()
            ]),
            Reply::Int(2)
        );
        assert_eq!(
            s.execute(&[b"HGET".to_vec(), b"h".to_vec(), bigkey.clone()]),
            Reply::Bulk(big.clone())
        );
        let all = norm("HGETALL", s.execute(&["HGETALL", "h"]));
        let want = norm(
            "HGETALL",
            Reply::Array(vec![
                Reply::Bulk(bigkey.clone()),
                Reply::Bulk(big.clone()),
                bulk("g"),
                Reply::Bulk(big.clone()),
            ]),
        );
        assert!(all == want, "HGETALL with 1 MB fields differs");
    });
}

#[test]
fn big_hashes_and_keyspace_scans_in_tiny_chunks() {
    guarded("big_hash", 90, || {
        for (shards, chunk) in [(1, 1), (2, 2), (64, 3), (2, 256), (1, 1000)] {
            let db = open(Config {
                shards,
                scan_chunk: chunk,
                ..Config::default()
            });
            let s = session(&db);
            let n = 2500usize;
            let mut rng = Rng::new(shards as u64 * 1000 + chunk as u64);
            let mut want: HashMap<B, B> = HashMap::new();
            let mut i = 0;
            while i < n {
                let calls = 1 + rng.below(7) as usize;
                let mut args: Vec<B> = vec![bytes("HSET"), bytes("big")];
                let mut new = 0;
                for _ in 0..calls {
                    let f = format!("field-{}", rng.below(n as u64)).into_bytes();
                    let len = rng.below(12) as usize;
                    let v = rng.bytes(len);
                    if want.insert(f.clone(), v.clone()).is_none() {
                        new += 1;
                    }
                    args.push(f);
                    args.push(v);
                }
                assert_eq!(s.execute(&args), Reply::Int(new), "HSET new-field count");
                i += calls;
            }
            let check = |want: &HashMap<B, B>, what: &str| {
                let got = array(s.execute(&["HGETALL", "big"]));
                assert_eq!(
                    got.len(),
                    want.len() * 2,
                    "{what}: HGETALL length (shards={shards} chunk={chunk})"
                );
                let got: HashMap<B, B> = got
                    .chunks(2)
                    .map(|c| (as_bytes(&c[0]).to_vec(), as_bytes(&c[1]).to_vec()))
                    .collect();
                assert_eq!(got.len(), want.len(), "{what}: duplicate fields in HGETALL");
                assert!(
                    &got == want,
                    "{what}: HGETALL content differs (shards={shards} chunk={chunk})"
                );
                assert_eq!(s.execute(&["HLEN", "big"]), Reply::Int(want.len() as i64));
            };
            check(&want, "after insert");
            // Delete about half, with repeats and misses in the same call.
            let fields: Vec<B> = want.keys().cloned().collect();
            for pair in fields.chunks(5) {
                if rng.chance(2) {
                    let mut args: Vec<B> =
                        vec![bytes("HDEL"), bytes("big"), bytes("no-such-field")];
                    args.extend(pair.iter().cloned());
                    args.push(pair[0].clone());
                    assert_eq!(s.execute(&args), Reply::Int(pair.len() as i64));
                    for f in pair {
                        want.remove(f);
                    }
                }
            }
            check(&want, "after delete");
            // Re-grow after deletions (free-list reuse in the table).
            for j in 0..n {
                let f = format!("again-{j}").into_bytes();
                s.execute(&[bytes("HSET"), bytes("big"), f.clone(), f.clone()]);
                want.insert(f.clone(), f);
            }
            check(&want, "after regrow");

            // KEYS over many keys.
            let mut keys = BTreeSet::new();
            for j in 0..n {
                let k = format!("key-{j}");
                s.execute(&["SET", &k, "v"]);
                keys.insert(k.into_bytes());
            }
            keys.insert(bytes("big"));
            let got = array(s.execute(&["KEYS", "*"]));
            assert_eq!(
                got.len(),
                keys.len(),
                "KEYS length (shards={shards} chunk={chunk})"
            );
            let got: BTreeSet<B> = got.iter().map(|r| as_bytes(r).to_vec()).collect();
            assert_eq!(got, keys, "KEYS content (shards={shards} chunk={chunk})");
            let got = array(s.execute(&["KEYS", "key-1?"]));
            assert_eq!(got.len(), 10);
            // Deleting a big hash entirely.
            assert_eq!(s.execute(&["DEL", "big"]), Reply::Int(1));
            assert_eq!(s.execute(&["HGETALL", "big"]), Reply::Array(vec![]));
            assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(n as i64));
        }
    });
}

#[test]
fn keys_patterns_against_model() {
    guarded("keys_patterns", 60, || {
        let db = open(Config {
            shards: 4,
            scan_chunk: 2,
            ..Config::default()
        });
        let s = session(&db);
        let names = [
            "", "a", "b", "ab", "abc", "a*b", "a?c", "a[b]c", "a-c", "h\0i", "h\r\ni", "{x}y",
            "x}", "[", "\\", "a\\",
        ];
        for n in names {
            s.execute(&["SET", n, "1"]);
        }
        let patterns = [
            "*", "?", "??", "a*", "*b", "a*b", "a?c", "a\\*b", "a\\?c", "a[b]c", "a[^b]c",
            "a[a-c]c", "[a-c]", "[^a]", "?*?", "*?*", "**", "a**c", "\\a", "\\\\", "{x}*", "*}",
            "\\{*", "h?i", "h*i", "h\0i", "h\r\ni", "a-c", "a[-]c", "[abc]*",
        ];
        for p in patterns {
            let got = array(s.execute(&["KEYS", p]));
            let got: BTreeSet<B> = got.iter().map(|r| as_bytes(r).to_vec()).collect();
            let want: BTreeSet<B> = names
                .iter()
                .filter(|n| glob(p.as_bytes(), n.as_bytes()))
                .map(|n| bytes(n))
                .collect();
            assert_eq!(got, want, "KEYS {p:?}");
        }
    });
}

#[test]
fn hash_tags_route_together() {
    let engine = Engine::new(Config {
        shards: 64,
        hash_tags: true,
        ..Config::default()
    });
    let plain = Engine::new(Config {
        shards: 64,
        hash_tags: false,
        ..Config::default()
    });
    let shard = |e: &Engine, k: &str| e.route(k.as_bytes()).1;
    // Same non-empty tag -> same shard, wherever the braces sit.
    for k in ["{a}x", "{a}y", "pre{a}", "p{a}s", "{a}", "z{a}{b}", "}{a}"] {
        assert_eq!(shard(&engine, k), shard(&engine, "{a}"), "key {k:?}");
    }
    // Only the first `{`..`}` counts; `{{x}` has tag `{x`.
    assert_eq!(shard(&engine, "q{{x}z"), shard(&engine, "{{x}"));
    assert_eq!(shard(&engine, "a}b{c}d"), shard(&engine, "{c}"));
    // No tag (empty, unbalanced): the whole key routes, as without tags.
    for k in ["{}", "{}a", "{}{b}", "{a", "a}", "a{", "}", "{"] {
        assert_eq!(
            shard(&engine, k),
            shard(&plain, k),
            "key {k:?} must not use a tag"
        );
    }
    // Without the feature the tag is ignored for at least some pair.
    let split = (0..200).any(|i| {
        let a = format!("{{t{i}}}a");
        let b = format!("{{t{i}}}b");
        shard(&plain, &a) != shard(&plain, &b)
    });
    assert!(split, "hash tags seem to be applied even when disabled");
    // Every shard index is in range.
    for i in 0..1000 {
        assert!(shard(&engine, &format!("k{i}")) < 64);
    }
}

#[test]
fn flushall_and_dbsize() {
    guarded("flushall", 30, || {
        let (_db, s) = fresh();
        for i in 0..1000 {
            s.execute(&["SET", &format!("k{i}"), "v"]);
            s.execute(&["HSET", &format!("h{i}"), "f", "v"]);
        }
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(2000));
        assert_eq!(s.execute(&["FLUSHALL"]), Reply::Ok);
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
        assert_eq!(s.execute(&["KEYS", "*"]), Reply::Array(vec![]));
        assert_eq!(s.execute(&["GET", "k1"]), Reply::Nil);
        assert_eq!(s.execute(&["HGETALL", "h1"]), Reply::Array(vec![]));
        // Usable afterwards, including a table that has to grow again.
        for i in 0..100 {
            s.execute(&["SET", &format!("n{i}"), "v"]);
        }
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(100));
        assert_eq!(s.execute(&["FLUSHDB"]), Reply::Ok);
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
    });
}

#[test]
fn error_replies_cannot_inject_extra_replies() {
    guarded("injection", 30, || {
        let (_db, s) = fresh();
        // Error text built from client data (the command name) with CRLF.
        for name in ["FOO\r\n+OK", "x\r\n:1\r\n", "\r\n"] {
            let reply = s.execute(&[name]);
            let wire = enc(&reply);
            assert!(wire.starts_with(b"-"), "{wire:?}");
            assert_eq!(
                wire.windows(2).filter(|w| *w == b"\r\n").count(),
                1,
                "an error line must contain exactly one CRLF: {wire:?}"
            );
            assert!(wire.ends_with(b"\r\n"));
        }
    });
}
