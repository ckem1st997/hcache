//! Expiry, SET options, SCAN/HSCAN and MULTI/EXEC/WATCH through the
//! embedded API. The clock is moved by hand (`Db::advance_clock`), so no
//! test waits for real time to pass.

mod support;

use hcache::{Reply, Session};
use hcache_core::testutil::Rng;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use support::*;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";

fn queued() -> Reply {
    Reply::Simple("QUEUED".into())
}

fn bulks(items: &[&str]) -> Reply {
    Reply::Array(items.iter().map(|s| bulk(s)).collect())
}

/// One whole SCAN iteration; panics if it does not end.
fn full_scan(mut exec: impl FnMut(&[&str]) -> Reply, extra: &[&str]) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut out = Vec::new();
    for _ in 0..1_000_000 {
        let mut args = vec!["SCAN", cursor.as_str()];
        args.extend_from_slice(extra);
        let mut reply = array(exec(&args));
        assert_eq!(reply.len(), 2, "SCAN reply shape");
        let items = array(reply.pop().expect("items"));
        cursor = text(&reply[0]);
        out.extend(items.iter().map(text));
        if cursor == "0" {
            return out;
        }
    }
    panic!("SCAN did not finish");
}

// ---------------------------------------------------------------------
// Reference model
// ---------------------------------------------------------------------

#[derive(Clone, PartialEq, Debug)]
enum Val {
    Str(String),
    Hash(BTreeMap<String, String>),
}

#[derive(Clone, PartialEq, Debug)]
struct Entry {
    val: Val,
    /// Virtual milliseconds.
    deadline: Option<i64>,
}

enum Want {
    Exact(Reply),
    /// PTTL: the real clock also moves, so a little less is fine.
    Pttl(i64),
}

#[derive(Default)]
struct Model {
    now: i64,
    keys: BTreeMap<String, Entry>,
}

impl Model {
    fn purge(&mut self) {
        let now = self.now;
        self.keys.retain(|_, e| e.deadline.is_none_or(|d| d > now));
    }

    fn str_of(&self, key: &str) -> Result<Option<&String>, Reply> {
        match self.keys.get(key).map(|e| &e.val) {
            None => Ok(None),
            Some(Val::Str(s)) => Ok(Some(s)),
            Some(Val::Hash(_)) => Err(err(WRONGTYPE)),
        }
    }

    fn apply(&mut self, args: &[String]) -> Want {
        self.purge();
        let reply = match self.run(args) {
            Ok(Want::Pttl(n)) => return Want::Pttl(n),
            Ok(Want::Exact(r)) | Err(r) => r,
        };
        Want::Exact(reply)
    }

    fn run(&mut self, args: &[String]) -> Result<Want, Reply> {
        let key = args[1].clone();
        let now = self.now;
        let opt_bulk = |v: Option<&String>| v.map_or(Reply::Nil, |s| bulk(s));
        let reply = match args[0].as_str() {
            "SET" => {
                let opts: Vec<&str> = args[3..].iter().map(String::as_str).collect();
                let get = opts.contains(&"GET");
                let old = if get {
                    opt_bulk(self.str_of(&key)?)
                } else {
                    Reply::Nil
                };
                let exists = self.keys.contains_key(&key);
                if (opts.contains(&"NX") && exists) || (opts.contains(&"XX") && !exists) {
                    return Ok(Want::Exact(old));
                }
                let deadline = if let Some(i) = opts.iter().position(|o| *o == "PX") {
                    Some(now + opts[i + 1].parse::<i64>().expect("px"))
                } else if opts.contains(&"KEEPTTL") {
                    self.keys.get(&key).and_then(|e| e.deadline)
                } else {
                    None
                };
                let val = Val::Str(args[2].clone());
                self.keys.insert(key, Entry { val, deadline });
                if get {
                    old
                } else {
                    Reply::Ok
                }
            }
            "GET" => opt_bulk(self.str_of(&key)?),
            "STRLEN" => Reply::Int(self.str_of(&key)?.map_or(0, |s| s.len() as i64)),
            "DEL" => Reply::Int(self.keys.remove(&key).is_some() as i64),
            "EXISTS" => Reply::Int(self.keys.contains_key(&key) as i64),
            "TYPE" => Reply::Simple(
                match self.keys.get(&key).map(|e| &e.val) {
                    None => "none",
                    Some(Val::Str(_)) => "string",
                    Some(Val::Hash(_)) => "hash",
                }
                .into(),
            ),
            "PERSIST" => match self.keys.get_mut(&key) {
                Some(e) => Reply::Int(e.deadline.take().is_some() as i64),
                None => Reply::Int(0),
            },
            "PTTL" => {
                return Ok(match self.keys.get(&key) {
                    None => Want::Exact(Reply::Int(-2)),
                    Some(Entry { deadline: None, .. }) => Want::Exact(Reply::Int(-1)),
                    Some(Entry {
                        deadline: Some(d), ..
                    }) => Want::Pttl(d - now),
                })
            }
            "INCR" => {
                let n = match self.str_of(&key)? {
                    None => 0,
                    Some(s) => s.parse::<i64>().map_err(|_| err(NOT_INT))?,
                } + 1;
                let deadline = self.keys.get(&key).and_then(|e| e.deadline);
                let val = Val::Str(n.to_string());
                self.keys.insert(key, Entry { val, deadline });
                Reply::Int(n)
            }
            "APPEND" => {
                let mut s = self.str_of(&key)?.cloned().unwrap_or_default();
                s.push_str(&args[2]);
                let len = s.len() as i64;
                let deadline = self.keys.get(&key).and_then(|e| e.deadline);
                let val = Val::Str(s);
                self.keys.insert(key, Entry { val, deadline });
                Reply::Int(len)
            }
            "GETDEL" => {
                let old = opt_bulk(self.str_of(&key)?);
                self.keys.remove(&key);
                old
            }
            "GETEX" => {
                let old = opt_bulk(self.str_of(&key)?);
                if let Some(e) = self.keys.get_mut(&key) {
                    match args.get(2).map(String::as_str) {
                        Some("PX") => e.deadline = Some(now + args[3].parse::<i64>().expect("px")),
                        Some("PERSIST") => e.deadline = None,
                        _ => {}
                    }
                }
                old
            }
            "SETNX" => {
                if let std::collections::btree_map::Entry::Vacant(slot) = self.keys.entry(key) {
                    let val = Val::Str(args[2].clone());
                    let deadline = None;
                    slot.insert(Entry { val, deadline });
                    Reply::Int(1)
                } else {
                    Reply::Int(0)
                }
            }
            "HSET" => {
                self.str_of(&key).err().map_or(Ok(()), |_| Ok(()))?;
                let entry = self.keys.entry(key).or_insert(Entry {
                    val: Val::Hash(BTreeMap::new()),
                    deadline: None,
                });
                match &mut entry.val {
                    Val::Str(_) => return Err(err(WRONGTYPE)),
                    Val::Hash(h) => {
                        Reply::Int(h.insert(args[2].clone(), args[3].clone()).is_none() as i64)
                    }
                }
            }
            "HGET" => match self.keys.get(&key).map(|e| &e.val) {
                None => Reply::Nil,
                Some(Val::Str(_)) => return Err(err(WRONGTYPE)),
                Some(Val::Hash(h)) => opt_bulk(h.get(&args[2])),
            },
            "HDEL" => {
                let (removed, empty) = match self.keys.get_mut(&key).map(|e| &mut e.val) {
                    None => (0, false),
                    Some(Val::Str(_)) => return Err(err(WRONGTYPE)),
                    Some(Val::Hash(h)) => (h.remove(&args[2]).is_some() as i64, h.is_empty()),
                };
                if empty {
                    self.keys.remove(&key);
                }
                Reply::Int(removed)
            }
            "PEXPIRE" => {
                let ms: i64 = args[2].parse().expect("ms");
                let at = now + ms;
                let Some(entry) = self.keys.get_mut(&key) else {
                    return Ok(Want::Exact(Reply::Int(0)));
                };
                let current = entry.deadline;
                let holds = match args.get(3).map(String::as_str) {
                    None => true,
                    Some("NX") => current.is_none(),
                    Some("XX") => current.is_some(),
                    Some("GT") => current.is_some_and(|c| at > c),
                    Some("LT") => current.is_none_or(|c| at < c),
                    Some(other) => panic!("model: PEXPIRE option {other}"),
                };
                if !holds {
                    Reply::Int(0)
                } else {
                    if ms <= 0 {
                        self.keys.remove(&key);
                    } else {
                        entry.deadline = Some(at);
                    }
                    Reply::Int(1)
                }
            }
            other => panic!("model: unknown command {other}"),
        };
        Ok(Want::Exact(reply))
    }
}

/// TTLs end in 5 s and the clock moves in steps of 10 s, so the few real
/// milliseconds a run takes never decide whether a key has expired.
fn ttl(rng: &mut Rng) -> String {
    (rng.below(4) as i64 * 10_000 + 5_000).to_string()
}

fn gen_op(rng: &mut Rng, model: &Model) -> Vec<String> {
    let key = format!("k{}", rng.below(6));
    let val = ["1", "41", "abc", "", "-7"][rng.below(5) as usize].to_string();
    let field = format!("f{}", rng.below(3));
    let s = |parts: &[&str]| parts.iter().map(|p| p.to_string()).collect::<Vec<_>>();
    match rng.below(26) {
        0 | 1 => s(&["SET", &key, &val]),
        2 | 3 => s(&["SET", &key, &val, "PX", &ttl(rng)]),
        4 => s(&["SET", &key, &val, "NX", "PX", &ttl(rng)]),
        5 => s(&["SET", &key, &val, "XX"]),
        6 => s(&["SET", &key, &val, "KEEPTTL"]),
        7 => s(&["SET", &key, &val, "GET"]),
        8 => s(&["SET", &key, &val, "NX", "GET"]),
        9 => s(&["GET", &key]),
        10 => s(&["DEL", &key]),
        11 => s(&["EXISTS", &key]),
        12 => s(&["PERSIST", &key]),
        13 | 14 => s(&["PTTL", &key]),
        15 => s(&["INCR", &key]),
        16 => s(&["APPEND", &key, &val]),
        17 => s(&["GETDEL", &key]),
        18 => match rng.below(3) {
            0 => s(&["GETEX", &key]),
            1 => s(&["GETEX", &key, "PERSIST"]),
            _ => s(&["GETEX", &key, "PX", &ttl(rng)]),
        },
        19 => s(&["SETNX", &key, &val]),
        20 => s(&["HSET", &key, &field, &val]),
        21 => s(&["HGET", &key, &field]),
        22 => s(&["HDEL", &key, &field]),
        23 => s(&["TYPE", &key]),
        24 => s(&["STRLEN", &key]),
        _ => {
            let ms = if rng.chance(5) {
                ["0", "-5"][rng.below(2) as usize].to_string()
            } else {
                ttl(rng)
            };
            let cond = ["", "", "NX", "XX", "GT", "LT"][rng.below(6) as usize];
            // GT/LT against an equal deadline would hang on real time.
            let at = model.now + ms.parse::<i64>().expect("ms");
            let tie = model
                .keys
                .get(&key)
                .is_some_and(|e| e.deadline.is_some_and(|d| d > model.now && d == at));
            if cond.is_empty() || (tie && matches!(cond, "GT" | "LT")) {
                s(&["PEXPIRE", &key, &ms])
            } else {
                s(&["PEXPIRE", &key, &ms, cond])
            }
        }
    }
}

fn check(seed: u64, step: usize, args: &[String], got: &Reply, want: &Want, slack: i64) {
    let ok = match want {
        Want::Exact(want) => enc(got) == enc(want),
        Want::Pttl(want) => matches!(got, Reply::Int(n) if (want - slack..=*want).contains(n)),
    };
    let shown = match want {
        Want::Exact(r) => brief(r),
        Want::Pttl(n) => format!("PTTL about {n}"),
    };
    assert!(
        ok,
        "seed {seed} step {step}: {args:?} gave {}, model says {shown}",
        brief(got)
    );
}

fn model_run(seed: u64, shards: usize, steps: usize) {
    let db = open(cfg(shards, 4, 2));
    let a = session(&db);
    let b = session(&db);
    let mut model = Model::default();
    let mut rng = Rng::new(seed);
    let started = Instant::now();
    let exec = |s: &Session, args: &[String]| s.execute(args);
    for step in 0..steps {
        // Past this much real time the 5 s margin of `ttl` would be gone.
        if started.elapsed().as_millis() > 3_000 {
            break;
        }
        let slack = started.elapsed().as_millis() as i64 + 5;
        match rng.below(20) {
            0 => {
                let ms = (1 + rng.below(3)) * 10_000;
                db.advance_clock(ms);
                model.now += ms as i64;
                if rng.chance(3) {
                    db.sweep_expired();
                }
                if rng.chance(3) {
                    db.expire_tick();
                }
            }
            1 | 2 => {
                // A transaction, sometimes with a watched key that another
                // session may touch.
                let watched = rng.chance(2).then(|| format!("k{}", rng.below(6)));
                let mut must_abort = false;
                let mut may_abort = false;
                if let Some(key) = &watched {
                    // A write first, so no expired copy of the key lingers.
                    let args = vec!["PERSIST".to_string(), key.clone()];
                    let want = model.apply(&args);
                    check(seed, step, &args, &exec(&a, &args), &want, slack);
                    assert_eq!(a.execute(&["WATCH", key]), Reply::Ok);
                    if rng.chance(2) {
                        let before = model.keys.get(key).cloned();
                        let args = gen_op(&mut rng, &model);
                        let want = model.apply(&args);
                        check(seed, step, &args, &exec(&b, &args), &want, slack);
                        must_abort = model.keys.get(key).cloned() != before;
                        may_abort = true;
                    }
                }
                assert_eq!(a.execute(&["MULTI"]), Reply::Ok);
                let ops: Vec<_> = (0..rng.below(6))
                    .map(|_| gen_op(&mut rng, &model))
                    .collect();
                for args in &ops {
                    assert_eq!(exec(&a, args), queued(), "seed {seed} step {step}");
                }
                let got = a.execute(&["EXEC"]);
                if got == Reply::NilArray {
                    assert!(
                        may_abort,
                        "seed {seed} step {step}: EXEC aborted, nothing touched {watched:?}"
                    );
                    continue;
                }
                assert!(
                    !must_abort,
                    "seed {seed} step {step}: EXEC ran although {watched:?} was modified"
                );
                let replies = array(got);
                assert_eq!(replies.len(), ops.len(), "seed {seed} step {step}");
                for (args, got) in ops.iter().zip(&replies) {
                    let want = model.apply(args);
                    check(seed, step, args, got, &want, slack);
                }
            }
            _ => {
                let args = gen_op(&mut rng, &model);
                let want = model.apply(&args);
                check(seed, step, &args, &exec(&a, &args), &want, slack);
            }
        }
        if step % 64 == 0 {
            model.purge();
            let want: BTreeSet<String> = model.keys.keys().cloned().collect();
            let keys: BTreeSet<String> =
                array(a.execute(&["KEYS", "*"])).iter().map(text).collect();
            assert_eq!(keys, want, "seed {seed} step {step}: KEYS");
            let scanned: BTreeSet<String> = full_scan(|args| a.execute(args), &["COUNT", "2"])
                .into_iter()
                .collect();
            assert_eq!(scanned, want, "seed {seed} step {step}: SCAN");
            let hashes: BTreeSet<String> = model
                .keys
                .iter()
                .filter(|(_, e)| matches!(e.val, Val::Hash(_)))
                .map(|(k, _)| k.clone())
                .collect();
            let scanned: BTreeSet<String> = full_scan(|args| a.execute(args), &["TYPE", "hash"])
                .into_iter()
                .collect();
            assert_eq!(scanned, hashes, "seed {seed} step {step}: SCAN TYPE hash");
        }
    }
}

#[test]
fn random_commands_match_the_model() {
    guarded("expiry-model", 120, || {
        for seed in 1..=12u64 {
            for shards in [1, 3, 64] {
                model_run(seed * 7919 + shards as u64, shards, 4_000);
            }
        }
    });
}

// ---------------------------------------------------------------------
// TTL and SET options
// ---------------------------------------------------------------------

#[test]
fn ttl_edge_values() {
    guarded("ttl-edges", 30, || {
        let db = open(cfg(4, 4, 2));
        let s = session(&db);
        let max = i64::MAX.to_string();
        let min = i64::MIN.to_string();

        assert_eq!(int(&run(&s, &["TTL", "nokey"])), -2);
        assert_eq!(int(&run(&s, &["PTTL", "nokey"])), -2);
        assert_eq!(int(&run(&s, &["EXPIRETIME", "nokey"])), -2);
        run(&s, &["SET", "k", "v"]);
        assert_eq!(int(&run(&s, &["TTL", "k"])), -1);
        assert_eq!(int(&run(&s, &["PEXPIRETIME", "k"])), -1);

        // Not positive, or too large to be a deadline.
        for bad in ["0", "-1", &min, &max] {
            for args in [
                vec!["SET", "k", "v", "EX", bad],
                vec!["SET", "k", "v", "PX", bad],
                vec!["SETEX", "k", bad, "v"],
                vec!["PSETEX", "k", bad, "v"],
                vec!["GETEX", "k", "EX", bad],
            ] {
                let got = run(&s, &args);
                assert!(
                    has_err(&got, "ERR invalid expire time in"),
                    "{args:?}: {}",
                    brief(&got)
                );
            }
        }
        for args in [
            vec!["SET", "k", "v", "EX", "abc"],
            vec!["SET", "k", "v", "PX", "1.5"],
            vec!["SET", "k", "v", "EX", ""],
            vec!["EXPIRE", "k", "soon"],
            vec!["EXPIRE", "k", "99999999999999999999"],
            vec!["SETEX", "k", "x", "v"],
        ] {
            assert_eq!(run(&s, &args), err(NOT_INT), "{args:?}");
        }
        // Nothing above touched the key.
        assert_eq!(run(&s, &["GET", "k"]), bulk("v"));
        assert_eq!(int(&run(&s, &["TTL", "k"])), -1);

        // EXPIRE: overflow is an error, the past deletes.
        for args in [
            vec!["EXPIRE", "k", &max[..]],
            vec!["PEXPIRE", "k", &max[..]],
            vec!["EXPIREAT", "k", &max[..]],
        ] {
            let got = run(&s, &args);
            assert!(
                has_err(&got, "ERR invalid expire time in"),
                "{args:?}: {}",
                brief(&got)
            );
        }
        assert_eq!(run(&s, &["GET", "k"]), bulk("v"));
        for args in [
            vec!["EXPIRE", "k", "0"],
            vec!["EXPIRE", "k", "-100"],
            vec!["PEXPIRE", "k", &min[..]],
            vec!["EXPIREAT", "k", "1"],
            vec!["PEXPIREAT", "k", "-1"],
            vec!["PEXPIREAT", "k", &min[..]],
        ] {
            run(&s, &["SET", "k", "v"]);
            assert_eq!(int(&run(&s, &args)), 1, "{args:?}");
            assert_eq!(int(&run(&s, &["EXISTS", "k"])), 0, "{args:?}");
            assert_eq!(int(&run(&s, &["DBSIZE"])), 0, "{args:?}");
        }
        // A deadline as far away as fits is accepted and kept.
        run(&s, &["SET", "k", "v"]);
        assert_eq!(int(&run(&s, &["PEXPIREAT", "k", &max])), 1);
        assert_eq!(int(&run(&s, &["PEXPIRETIME", "k"])), i64::MAX);
        db.advance_clock(1 << 40);
        assert_eq!(run(&s, &["GET", "k"]), bulk("v"));

        // Seconds and milliseconds.
        run(&s, &["SET", "t", "v", "EX", "100"]);
        let ttl = int(&run(&s, &["TTL", "t"]));
        assert!((99..=100).contains(&ttl), "TTL {ttl}");
        let pttl = int(&run(&s, &["PTTL", "t"]));
        assert!((99_000..=100_000).contains(&pttl), "PTTL {pttl}");
        let at = int(&run(&s, &["PEXPIRETIME", "t"]));
        assert_eq!(int(&run(&s, &["EXPIRETIME", "t"])), (at + 500) / 1000);
        db.advance_clock(99_990);
        assert_eq!(run(&s, &["GET", "t"]), bulk("v"));
        db.advance_clock(20);
        assert_eq!(run(&s, &["GET", "t"]), Reply::Nil);
        assert_eq!(int(&run(&s, &["TTL", "t"])), -2);
    });
}

#[test]
fn set_options_conflicts_and_spelling() {
    guarded("set-options", 30, || {
        let db = open(cfg(4, 4, 2));
        let s = session(&db);
        for args in [
            vec!["SET", "k", "v", "NX", "XX"],
            vec!["SET", "k", "v", "XX", "NX"],
            vec!["SET", "k", "v", "EX", "10", "PX", "10"],
            vec!["SET", "k", "v", "EX", "10", "KEEPTTL"],
            vec!["SET", "k", "v", "KEEPTTL", "PXAT", "99999999999999"],
            vec!["SET", "k", "v", "EX"],
            vec!["SET", "k", "v", "PX"],
            vec!["SET", "k", "v", "10"],
            vec!["SET", "k", "v", "EX", "10", "EX", "10"],
            vec!["SET", "k", "v", "GET", "BOGUS"],
            vec!["GETEX", "k", "PERSIST", "EX", "10"],
            vec!["GETEX", "k", "EX"],
            vec!["GETEX", "k", "KEEPTTL"],
            vec!["EXPIRE", "k", "10", "NX", "XX"],
            vec!["EXPIRE", "k", "10", "GT", "LT"],
        ] {
            let got = run(&s, &args);
            assert!(matches!(got, Reply::Error(_)), "{args:?}: {}", brief(&got));
            assert_eq!(
                int(&run(&s, &["EXISTS", "k"])),
                0,
                "{args:?} created the key"
            );
        }
        assert_eq!(
            run(&s, &["EXPIRE", "k", "10", "SOON"]),
            err("ERR Unsupported option SOON")
        );
        // Options in any case and order.
        assert_eq!(run(&s, &["set", "k", "1", "px", "50000", "nx"]), Reply::Ok);
        assert_eq!(run(&s, &["SET", "k", "2", "Nx"]), Reply::Nil);
        assert_eq!(
            run(&s, &["SET", "k", "3", "xX", "gEt", "KeepTtl"]),
            bulk("1")
        );
        let pttl = int(&run(&s, &["PTTL", "k"]));
        assert!(
            (1..=50_000).contains(&pttl),
            "KEEPTTL lost the deadline: {pttl}"
        );
        // A plain SET drops the deadline, GETSET too.
        assert_eq!(run(&s, &["SET", "k", "4"]), Reply::Ok);
        assert_eq!(int(&run(&s, &["TTL", "k"])), -1);
        run(&s, &["PEXPIRE", "k", "50000"]);
        assert_eq!(run(&s, &["GETSET", "k", "5"]), bulk("4"));
        assert_eq!(int(&run(&s, &["TTL", "k"])), -1);
        // GET on the wrong type fails and leaves the hash alone.
        run(&s, &["HSET", "h", "f", "v"]);
        assert_eq!(run(&s, &["SET", "h", "x", "GET"]), err(WRONGTYPE));
        assert_eq!(run(&s, &["HGET", "h", "f"]), bulk("v"));
        // Without GET a SET replaces any type.
        assert_eq!(run(&s, &["SET", "h", "x"]), Reply::Ok);
        assert_eq!(text(&run(&s, &["TYPE", "h"])), "string");
        // Binary option-looking values are just values.
        assert_eq!(run(&s, &["SET", "NX", "EX"]), Reply::Ok);
        assert_eq!(run(&s, &["GET", "NX"]), bulk("EX"));
    });
}

#[test]
fn an_expired_key_is_gone_for_every_command() {
    guarded("expired-gone", 30, || {
        let db = open(cfg(2, 4, 2));
        let s = session(&db);
        let arm = |kind: &str| {
            run(&s, &["DEL", "k"]);
            match kind {
                "str" => run(&s, &["SET", "k", "10", "PX", "100"]),
                _ => {
                    run(&s, &["HSET", "k", "f", "10"]);
                    run(&s, &["PEXPIRE", "k", "100"])
                }
            };
            db.advance_clock(100);
        };
        for kind in ["str", "hash"] {
            let cases: Vec<(Vec<&str>, Reply)> = vec![
                (vec!["GET", "k"], Reply::Nil),
                (vec!["EXISTS", "k"], Reply::Int(0)),
                (vec!["DEL", "k"], Reply::Int(0)),
                (vec!["UNLINK", "k"], Reply::Int(0)),
                (vec!["TYPE", "k"], Reply::Simple("none".into())),
                (vec!["STRLEN", "k"], Reply::Int(0)),
                (vec!["TTL", "k"], Reply::Int(-2)),
                (vec!["PERSIST", "k"], Reply::Int(0)),
                (vec!["EXPIRE", "k", "100"], Reply::Int(0)),
                (vec!["GETDEL", "k"], Reply::Nil),
                (vec!["GETEX", "k", "PERSIST"], Reply::Nil),
                (vec!["HGET", "k", "f"], Reply::Nil),
                (vec!["HLEN", "k"], Reply::Int(0)),
                (vec!["HEXISTS", "k", "f"], Reply::Int(0)),
                (vec!["HDEL", "k", "f"], Reply::Int(0)),
                (vec!["HGETALL", "k"], Reply::Array(vec![])),
                (vec!["HMGET", "k", "f"], Reply::Array(vec![Reply::Nil])),
                (vec!["MGET", "k"], Reply::Array(vec![Reply::Nil])),
                (vec!["KEYS", "*"], Reply::Array(vec![])),
                (vec!["HSCAN", "k", "0"], bulks_scan_empty()),
                (vec!["SET", "k", "x", "XX"], Reply::Nil),
                (vec!["SET", "k", "x", "GET"], Reply::Nil),
                (
                    vec!["EVAL", "return redis.call('EXISTS', KEYS[1])", "1", "k"],
                    Reply::Int(0),
                ),
            ];
            for (args, want) in cases {
                arm(kind);
                assert_eq!(run(&s, &args), want, "{kind}: {args:?}");
            }
            // SCAN may need more than one call to say so.
            arm(kind);
            assert!(full_scan(|a| s.execute(a), &[]).is_empty(), "{kind}: SCAN");
            // Writers start from nothing, and what they create has no
            // deadline inherited from the dead key.
            let cases: Vec<(Vec<&str>, Reply, Vec<&str>, Reply)> = vec![
                (
                    vec!["INCR", "k"],
                    Reply::Int(1),
                    vec!["GET", "k"],
                    bulk("1"),
                ),
                (
                    vec!["INCRBY", "k", "5"],
                    Reply::Int(5),
                    vec!["GET", "k"],
                    bulk("5"),
                ),
                (
                    vec!["DECR", "k"],
                    Reply::Int(-1),
                    vec!["GET", "k"],
                    bulk("-1"),
                ),
                (
                    vec!["APPEND", "k", "ab"],
                    Reply::Int(2),
                    vec!["GET", "k"],
                    bulk("ab"),
                ),
                (
                    vec!["SETNX", "k", "n"],
                    Reply::Int(1),
                    vec!["GET", "k"],
                    bulk("n"),
                ),
                (
                    vec!["SET", "k", "n", "NX"],
                    Reply::Ok,
                    vec!["GET", "k"],
                    bulk("n"),
                ),
                (
                    vec!["SET", "k", "n", "KEEPTTL"],
                    Reply::Ok,
                    vec!["GET", "k"],
                    bulk("n"),
                ),
                (
                    vec!["GETSET", "k", "n"],
                    Reply::Nil,
                    vec!["GET", "k"],
                    bulk("n"),
                ),
                (
                    vec!["MSET", "k", "n"],
                    Reply::Ok,
                    vec!["GET", "k"],
                    bulk("n"),
                ),
                (
                    vec!["HSET", "k", "g", "1"],
                    Reply::Int(1),
                    vec!["HLEN", "k"],
                    Reply::Int(1),
                ),
                (
                    vec!["HSETNX", "k", "f", "1"],
                    Reply::Int(1),
                    vec!["HGET", "k", "f"],
                    bulk("1"),
                ),
                (
                    vec!["HINCRBY", "k", "f", "3"],
                    Reply::Int(3),
                    vec!["HGET", "k", "f"],
                    bulk("3"),
                ),
                (
                    vec!["EVAL", "return redis.call('INCR', KEYS[1])", "1", "k"],
                    Reply::Int(1),
                    vec!["GET", "k"],
                    bulk("1"),
                ),
            ];
            for (args, want, probe, seen) in cases {
                arm(kind);
                assert_eq!(run(&s, &args), want, "{kind}: {args:?}");
                assert_eq!(
                    int(&run(&s, &["TTL", "k"])),
                    -1,
                    "{kind}: {args:?} kept a deadline"
                );
                db.advance_clock(1_000_000);
                assert_eq!(run(&s, &probe), seen, "{kind}: {args:?} then {probe:?}");
                assert_eq!(int(&run(&s, &["DBSIZE"])), 1, "{kind}: {args:?}");
            }
        }
    });
}

fn bulks_scan_empty() -> Reply {
    Reply::Array(vec![bulk("0"), Reply::Array(vec![])])
}

#[test]
fn writes_keep_or_drop_the_deadline_as_redis_does() {
    guarded("ttl-kept", 30, || {
        let db = open(cfg(2, 4, 2));
        let s = session(&db);
        let has_ttl = |key: &str| int(&run(&s, &["PTTL", key])) > 0;
        run(&s, &["SET", "n", "1", "PX", "60000"]);
        for args in [
            vec!["INCR", "n"],
            vec!["DECRBY", "n", "3"],
            vec!["APPEND", "n", "0"],
            vec!["SET", "n", "7", "KEEPTTL"],
            vec!["SET", "n", "7", "XX", "KEEPTTL", "GET"],
            vec!["GETEX", "n"],
            vec!["EVAL", "return redis.call('INCR', KEYS[1])", "1", "n"],
        ] {
            run(&s, &args);
            assert!(has_ttl("n"), "{args:?} dropped the deadline");
        }
        run(&s, &["HSET", "h", "a", "1"]);
        run(&s, &["EXPIRE", "h", "60"]);
        for args in [
            vec!["HSET", "h", "b", "2"],
            vec!["HSETNX", "h", "c", "3"],
            vec!["HINCRBY", "h", "a", "1"],
            vec!["HDEL", "h", "b"],
            vec!["HMSET", "h", "d", "4"],
        ] {
            run(&s, &args);
            assert!(has_ttl("h"), "{args:?} dropped the deadline");
        }
        // Deleting the last field deletes the key and its deadline; a new
        // hash under the same name starts clean.
        run(&s, &["HDEL", "h", "a", "c", "d"]);
        assert_eq!(int(&run(&s, &["EXISTS", "h"])), 0);
        run(&s, &["HSET", "h", "a", "1"]);
        assert_eq!(int(&run(&s, &["TTL", "h"])), -1);
        // DEL then SET likewise.
        run(&s, &["DEL", "n"]);
        run(&s, &["SET", "n", "1"]);
        db.advance_clock(120_000);
        assert_eq!(run(&s, &["GET", "n"]), bulk("1"));
        assert_eq!(run(&s, &["HGET", "h", "a"]), bulk("1"));
    });
}

#[test]
fn expire_conditions() {
    guarded("expire-cond", 30, || {
        let db = open(cfg(2, 4, 2));
        let s = session(&db);
        run(&s, &["SET", "k", "v"]);
        // No deadline counts as "never": nothing is greater.
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "100", "XX"])), 0);
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "100", "GT"])), 0);
        assert_eq!(int(&run(&s, &["TTL", "k"])), -1);
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "100", "LT"])), 1);
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "200", "NX"])), 0);
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "200", "LT"])), 0);
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "200", "GT"])), 1);
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "50", "GT"])), 0);
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "50", "XX"])), 1);
        let ttl = int(&run(&s, &["TTL", "k"]));
        assert!((49..=50).contains(&ttl), "{ttl}");
        // A deadline in the past under GT does nothing; under LT it deletes.
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "-1", "GT"])), 0);
        assert_eq!(int(&run(&s, &["EXISTS", "k"])), 1);
        assert_eq!(int(&run(&s, &["EXPIRE", "k", "-1", "LT"])), 1);
        assert_eq!(int(&run(&s, &["EXISTS", "k"])), 0);
        assert_eq!(int(&run(&s, &["PERSIST", "k"])), 0);
    });
}

#[test]
fn expired_keys_are_collected_without_being_touched() {
    guarded("active-expiry", 60, || {
        for shards in [1, 16, 300] {
            let db = open(cfg(shards, 4, 2));
            let s = session(&db);
            const KEYS: usize = 20_000;
            for i in 0..KEYS {
                let key = format!("k{i}");
                if i % 2 == 0 {
                    run(&s, &["SET", &key, "v", "PX", "1000"]);
                } else {
                    run(&s, &["SET", &key, "v"]);
                }
            }
            assert_eq!(int(&run(&s, &["DBSIZE"])), KEYS as i64);
            db.advance_clock(1_000);
            // Readers see them gone at once.
            assert_eq!(array(run(&s, &["KEYS", "*"])).len(), KEYS / 2);
            assert_eq!(
                full_scan(|a| s.execute(a), &["COUNT", "1000"]).len(),
                KEYS / 2
            );
            // The background pass removes them, a bounded bit per call.
            let mut ticks = 0;
            while int(&run(&s, &["DBSIZE"])) > KEYS as i64 / 2 {
                db.advance_clock(100);
                db.expire_tick();
                ticks += 1;
                assert!(ticks < 100_000, "{shards} shards: expiry does not converge");
            }
            assert!(
                ticks > 1 || shards > 1,
                "one tick must not sweep a whole big shard"
            );
            assert_eq!(db.sweep_expired(), 0);
            assert_eq!(run(&s, &["GET", "k1"]), bulk("v"));
            assert_eq!(array(run(&s, &["KEYS", "*"])).len(), KEYS / 2);
        }
        // `sweep_expired` does it in one go and reports the count.
        let db = open(cfg(8, 4, 2));
        let s = session(&db);
        for i in 0..5_000 {
            run(
                &s,
                &[
                    "SET",
                    &format!("k{i}"),
                    "v",
                    "PX",
                    &(1 + i % 50).to_string(),
                ],
            );
        }
        db.advance_clock(50);
        assert_eq!(db.sweep_expired(), 5_000);
        assert_eq!(int(&run(&s, &["DBSIZE"])), 0);
    });
}

#[test]
fn expiry_under_concurrent_writers_never_resurrects() {
    guarded("expiry-race", 120, || {
        let db = open(cfg(4, 4, 2));
        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        for t in 0..6u64 {
            let (db, stop) = (db.clone(), stop.clone());
            threads.push(thread::spawn(move || {
                let s = session(&db);
                let mut rng = Rng::new(t + 1);
                let mut ops = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("k{}", rng.below(40));
                    let px = (1 + rng.below(30)).to_string();
                    match rng.below(8) {
                        0..=2 => assert_eq!(run(&s, &["SET", &key, "val", "PX", &px]), Reply::Ok),
                        3 => {
                            let got = run(&s, &["GET", &key]);
                            assert!(got == Reply::Nil || got == bulk("val"), "{}", brief(&got));
                        }
                        4 => {
                            let got = run(&s, &["GETEX", &key, "PX", &px]);
                            assert!(got == Reply::Nil || got == bulk("val"), "{}", brief(&got));
                        }
                        5 => {
                            let n = int(&run(&s, &["PEXPIRE", &key, &px]));
                            assert!(n == 0 || n == 1);
                        }
                        6 => {
                            let n = int(&run(&s, &["PTTL", &key]));
                            assert!(n == -2 || (0..=30).contains(&n), "PTTL {n}");
                        }
                        _ => {
                            if t == 0 {
                                db.advance_clock(rng.below(5));
                            } else {
                                db.expire_tick();
                            }
                        }
                    }
                    ops += 1;
                }
                ops
            }));
        }
        thread::sleep(std::time::Duration::from_millis(1_500));
        stop.store(true, Ordering::Relaxed);
        let ops: u64 = threads.into_iter().map(|t| t.join().expect("worker")).sum();
        assert!(ops > 10_000, "only {ops} operations ran");
        // Every key had a deadline of at most 30 ms.
        let s = session(&db);
        db.advance_clock(31);
        assert_eq!(run(&s, &["KEYS", "*"]), Reply::Array(vec![]));
        db.sweep_expired();
        assert_eq!(int(&run(&s, &["DBSIZE"])), 0);
        assert_eq!(db.sweep_expired(), 0);
    });
}

// ---------------------------------------------------------------------
// SCAN and HSCAN
// ---------------------------------------------------------------------

#[test]
fn scan_returns_every_key_for_any_count_and_shard_layout() {
    guarded("scan-cover", 120, || {
        for shards in [1, 2, 7, 64, 1000] {
            let db = open(cfg(shards, 4, 2));
            let s = session(&db);
            for n in [0usize, 1, 5, 300, 5_000] {
                run(&s, &["FLUSHALL"]);
                let mut want = BTreeSet::new();
                for i in 0..n {
                    let key = format!("key:{i}");
                    if i % 3 == 0 {
                        run(&s, &["HSET", &key, "f", "v"]);
                    } else {
                        run(&s, &["SET", &key, "v"]);
                    }
                    want.insert(key);
                }
                for count in ["1", "2", "10", "97", "100000", "9223372036854775807"] {
                    if n == 5_000 && shards == 1000 && count == "1" {
                        continue;
                    }
                    let got = full_scan(|a| s.execute(a), &["COUNT", count]);
                    assert_eq!(
                        got.len(),
                        n,
                        "{shards} shards, {n} keys, COUNT {count}: duplicates"
                    );
                    let got: BTreeSet<String> = got.into_iter().collect();
                    assert_eq!(got, want, "{shards} shards, {n} keys, COUNT {count}");
                }
                let got = full_scan(|a| s.execute(a), &["MATCH", "key:1*", "COUNT", "50"]);
                let want_m = want.iter().filter(|k| k.starts_with("key:1")).count();
                assert_eq!(got.len(), want_m, "{shards} shards, {n} keys: MATCH");
                let got = full_scan(|a| s.execute(a), &["TYPE", "HASH"]);
                assert_eq!(got.len(), n.div_ceil(3), "{shards} shards, {n} keys: TYPE");
                let got = full_scan(|a| s.execute(a), &["TYPE", "zset"]);
                assert!(got.is_empty());
            }
        }
    });
}

#[test]
fn scan_with_few_keys_in_many_shards_takes_few_calls() {
    guarded("scan-sparse", 30, || {
        let db = open(cfg(4096, 4, 2));
        let s = session(&db);
        for key in ["a", "b", "c"] {
            run(&s, &["SET", key, "v"]);
        }
        let mut calls = 0;
        let got = full_scan(
            |a| {
                calls += 1;
                s.execute(a)
            },
            &["COUNT", "1"],
        );
        assert_eq!(got.len(), 3);
        assert!(calls <= 8, "{calls} SCAN calls for three keys");
    });
}

#[test]
fn scan_rejects_bad_input_and_survives_wild_cursors() {
    guarded("scan-bad", 30, || {
        let db = open(cfg(8, 4, 2));
        let s = session(&db);
        for i in 0..500 {
            run(&s, &["SET", &format!("k{i}"), "v"]);
        }
        run(&s, &["HSET", "h", "f", "v"]);
        for cursor in ["abc", "-1", "", "1.0", "18446744073709551616", " 0", "0x10"] {
            assert_eq!(
                run(&s, &["SCAN", cursor]),
                err("ERR invalid cursor"),
                "{cursor:?}"
            );
            assert_eq!(
                run(&s, &["HSCAN", "h", cursor]),
                err("ERR invalid cursor"),
                "{cursor:?}"
            );
        }
        for args in [
            vec!["SCAN", "0", "COUNT", "0"],
            vec!["SCAN", "0", "COUNT", "-5"],
            vec!["SCAN", "0", "COUNT"],
            vec!["SCAN", "0", "MATCH"],
            vec!["SCAN", "0", "TYPE"],
            vec!["SCAN", "0", "BOGUS", "1"],
            vec!["SCAN", "0", "NOVALUES"],
            vec!["HSCAN", "h", "0", "TYPE", "hash"],
            vec!["HSCAN", "h", "0", "COUNT", "0"],
        ] {
            assert_eq!(run(&s, &args), err("ERR syntax error"), "{args:?}");
        }
        assert_eq!(run(&s, &["SCAN", "0", "COUNT", "x"]), err(NOT_INT));
        assert!(matches!(run(&s, &["SCAN"]), Reply::Error(_)));
        assert!(matches!(run(&s, &["HSCAN", "h"]), Reply::Error(_)));
        // Any number is a cursor: the reply is well formed and following it
        // ends.
        let mut rng = Rng::new(99);
        let mut wild: Vec<u64> = (0..2_000).map(|_| rng.next_u64()).collect();
        wild.extend([
            u64::MAX,
            u64::MAX - 1,
            1 << 63,
            7,
            8,
            9,
            65_535,
            65_536,
            65_537,
        ]);
        for start in wild {
            let mut cursor = start.to_string();
            let mut hops = 0;
            loop {
                let mut reply = array(run(&s, &["SCAN", &cursor, "COUNT", "100"]));
                assert_eq!(reply.len(), 2);
                array(reply.pop().expect("items"));
                cursor = text(&reply[0]);
                if cursor == "0" {
                    break;
                }
                hops += 1;
                assert!(hops < 1_000, "cursor {start} never ends");
            }
            let reply = array(run(&s, &["HSCAN", "h", &start.to_string()]));
            assert_eq!(reply.len(), 2);
        }
    });
}

#[test]
fn scan_keeps_every_stable_key_while_others_come_and_go() {
    guarded("scan-churn", 180, || {
        for shards in [1, 8] {
            let db = open(cfg(shards, 4, 2));
            let s = session(&db);
            const STABLE: usize = 3_000;
            for i in 0..STABLE {
                run(&s, &["SET", &format!("stable:{i}"), "v"]);
            }
            let stop = Arc::new(AtomicBool::new(false));
            let churned = Arc::new(AtomicU64::new(0));
            let mut threads = Vec::new();
            for t in 0..4u64 {
                let (db, stop, churned) = (db.clone(), stop.clone(), churned.clone());
                threads.push(thread::spawn(move || {
                    let s = session(&db);
                    let mut rng = Rng::new(t + 10);
                    while !stop.load(Ordering::Relaxed) {
                        // Grow far past the stable set, then shrink again,
                        // so the tables resize both ways under the scan.
                        let wave = 2_000 + rng.below(20_000);
                        for i in 0..wave {
                            let key = format!("churn:{t}:{i}");
                            if i % 5 == 0 {
                                run(&s, &["SET", &key, "v", "PX", "5"]);
                            } else {
                                run(&s, &["SET", &key, "v"]);
                            }
                        }
                        for i in 0..wave {
                            run(&s, &["DEL", &format!("churn:{t}:{i}")]);
                        }
                        churned.fetch_add(wave, Ordering::Relaxed);
                    }
                }));
            }
            let mut rng = Rng::new(5);
            let mut scans = 0;
            let started = Instant::now();
            while started.elapsed().as_millis() < 4_000 || scans < 5 {
                let count = (1 + rng.below(500)).to_string();
                let extra: &[&str] = if rng.chance(2) {
                    &["COUNT", &count, "MATCH", "stable:*"]
                } else {
                    &["COUNT", &count]
                };
                let got: BTreeSet<String> =
                    full_scan(|a| s.execute(a), extra).into_iter().collect();
                let missing: Vec<String> = (0..STABLE)
                    .map(|i| format!("stable:{i}"))
                    .filter(|k| !got.contains(k))
                    .collect();
                assert!(
                    missing.is_empty(),
                    "{shards} shards, scan {scans} (COUNT {count}) missed {} keys, e.g. {:?}",
                    missing.len(),
                    &missing[..missing.len().min(5)]
                );
                scans += 1;
            }
            stop.store(true, Ordering::Relaxed);
            for t in threads {
                t.join().expect("churn thread");
            }
            assert!(
                churned.load(Ordering::Relaxed) > 10_000,
                "churn was too light"
            );
        }
    });
}

#[test]
fn hscan_covers_big_and_small_hashes() {
    guarded("hscan", 60, || {
        let db = open(cfg(4, 4, 2));
        let s = session(&db);
        assert_eq!(run(&s, &["HSCAN", "nokey", "0"]), bulks_scan_empty());
        assert_eq!(run(&s, &["HSCAN", "nokey", "12345"]), bulks_scan_empty());
        run(&s, &["SET", "str", "v"]);
        assert_eq!(run(&s, &["HSCAN", "str", "0"]), err(WRONGTYPE));
        for n in [1usize, 9, 4_000] {
            run(&s, &["DEL", "h"]);
            for i in 0..n {
                run(&s, &["HSET", "h", &format!("f{i}"), &format!("v{i}")]);
            }
            for (count, novalues) in [("1", false), ("10", true), ("100000", false), ("3", true)] {
                let mut cursor = "0".to_string();
                let mut seen = BTreeMap::new();
                let mut calls = 0;
                loop {
                    let mut args = vec!["HSCAN", "h", cursor.as_str(), "COUNT", count];
                    if novalues {
                        args.push("NOVALUES");
                    }
                    let mut reply = array(run(&s, &args));
                    let items: Vec<String> = array(reply.pop().expect("items"))
                        .iter()
                        .map(text)
                        .collect();
                    cursor = text(&reply[0]);
                    if novalues {
                        for f in items {
                            seen.insert(f, String::new());
                        }
                    } else {
                        assert_eq!(items.len() % 2, 0);
                        for pair in items.chunks(2) {
                            assert_eq!(pair[1], pair[0].replacen('f', "v", 1));
                            seen.insert(pair[0].clone(), pair[1].clone());
                        }
                    }
                    calls += 1;
                    assert!(calls <= 2 * n + 10, "HSCAN does not end");
                    if cursor == "0" {
                        break;
                    }
                }
                assert_eq!(seen.len(), n, "{n} fields, COUNT {count}");
            }
            // COUNT is a hint: one call scans a bounded part of a big hash.
            let mut cursor = "0".to_string();
            let mut got = 0;
            loop {
                let args = [
                    "HSCAN",
                    "h",
                    cursor.as_str(),
                    "MATCH",
                    "f1*",
                    "COUNT",
                    "100000",
                ];
                let reply = array(run(&s, &args));
                got += array(reply[1].clone()).len();
                cursor = text(&reply[0]);
                if cursor == "0" {
                    break;
                }
            }
            let want = (0..n).filter(|i| i.to_string().starts_with('1')).count();
            assert_eq!(got, 2 * want);
        }
    });
}

// ---------------------------------------------------------------------
// MULTI / EXEC / WATCH
// ---------------------------------------------------------------------

#[test]
fn transaction_rules() {
    guarded("txn-rules", 30, || {
        let db = open(cfg(4, 4, 2));
        let s = session(&db);
        let o = session(&db);
        assert_eq!(run(&s, &["EXEC"]), err("ERR EXEC without MULTI"));
        assert_eq!(run(&s, &["DISCARD"]), err("ERR DISCARD without MULTI"));
        assert!(matches!(run(&s, &["MULTI", "x"]), Reply::Error(_)));
        assert!(matches!(run(&s, &["WATCH"]), Reply::Error(_)));
        assert!(matches!(run(&s, &["EXEC", "x"]), Reply::Error(_)));

        // Empty transaction.
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        assert_eq!(run(&s, &["EXEC"]), Reply::Array(vec![]));
        assert_eq!(run(&s, &["EXEC"]), err("ERR EXEC without MULTI"));

        // Nothing runs before EXEC; a runtime error fails only its command.
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        assert_eq!(run(&s, &["SET", "a", "abc"]), queued());
        assert_eq!(run(&s, &["INCR", "a"]), queued());
        assert_eq!(run(&s, &["HSET", "a", "f", "v"]), queued());
        assert_eq!(run(&s, &["APPEND", "a", "d"]), queued());
        assert_eq!(run(&s, &["PING"]), queued());
        assert_eq!(run(&s, &["GET", "a"]), queued());
        assert_eq!(run(&o, &["GET", "a"]), Reply::Nil);
        assert_eq!(
            run(&s, &["EXEC"]),
            Reply::Array(vec![
                Reply::Ok,
                err(NOT_INT),
                err(WRONGTYPE),
                Reply::Int(4),
                Reply::Simple("PONG".into()),
                bulk("abcd"),
            ])
        );
        assert_eq!(run(&o, &["GET", "a"]), bulk("abcd"));

        // A command that cannot be queued poisons the transaction.
        for bad in [
            vec!["NOSUCHCOMMAND"],
            vec!["GET"],
            vec!["SET", "a", "v", "EX", "0"],
            vec!["HELLO", "3"],
            vec!["SCAN", "x"],
        ] {
            assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
            assert_eq!(run(&s, &["SET", "a", "poisoned"]), queued());
            assert!(matches!(run(&s, &bad), Reply::Error(_)), "{bad:?}");
            assert_eq!(run(&s, &["SET", "a", "poisoned"]), queued());
            let got = run(&s, &["EXEC"]);
            assert!(has_err(&got, "EXECABORT"), "{bad:?}: {}", brief(&got));
            assert_eq!(run(&s, &["GET", "a"]), bulk("abcd"), "{bad:?}");
            assert_eq!(run(&s, &["EXEC"]), err("ERR EXEC without MULTI"));
        }

        // Nested MULTI and WATCH inside are errors that do not poison.
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        assert_eq!(
            run(&s, &["MULTI"]),
            err("ERR MULTI calls can not be nested")
        );
        assert_eq!(
            run(&s, &["WATCH", "a"]),
            err("ERR WATCH inside MULTI is not allowed")
        );
        assert_eq!(run(&s, &["UNWATCH"]), queued());
        assert_eq!(run(&s, &["INCR", "n"]), queued());
        assert_eq!(
            run(&s, &["EXEC"]),
            Reply::Array(vec![Reply::Ok, Reply::Int(1)])
        );

        // DISCARD drops the queue.
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        assert_eq!(run(&s, &["INCR", "n"]), queued());
        assert_eq!(run(&s, &["DISCARD"]), Reply::Ok);
        assert_eq!(run(&s, &["GET", "n"]), bulk("1"));
        assert_eq!(run(&s, &["DISCARD"]), err("ERR DISCARD without MULTI"));

        // A session's transaction is its own.
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        assert_eq!(run(&o, &["EXEC"]), err("ERR EXEC without MULTI"));
        assert_eq!(run(&o, &["SET", "x", "1"]), Reply::Ok);
        assert_eq!(run(&s, &["DISCARD"]), Reply::Ok);
    });
}

#[test]
fn every_kind_of_command_works_inside_a_transaction() {
    guarded("txn-kinds", 30, || {
        let db = open(cfg(4, 4, 2));
        let s = session(&db);
        for i in 0..50 {
            run(&s, &["SET", &format!("k{i}"), "v"]);
            run(&s, &["HSET", "h", &format!("f{i}"), "v"]);
        }
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        let queue: Vec<Vec<&str>> = vec![
            vec!["KEYS", "k*"],
            vec!["SCAN", "0", "COUNT", "1000"],
            vec!["HGETALL", "h"],
            vec!["HSCAN", "h", "0", "COUNT", "1000"],
            vec!["MSET", "a", "1", "b", "2"],
            vec!["MGET", "a", "b", "nokey"],
            vec!["EVAL", "return redis.call('INCR', KEYS[1])", "1", "a"],
            vec!["EVAL", "return redis.call('MULTI')", "0"],
            vec!["SCRIPT", "LOAD", "return 7"],
            vec!["DBSIZE"],
            vec!["DEL", "a", "b", "k0", "k1"],
            vec!["SET", "t", "v", "PX", "500"],
            vec!["PTTL", "t"],
            vec!["FLUSHALL"],
            vec!["DBSIZE"],
            vec!["SET", "after", "flush"],
        ];
        for args in &queue {
            assert_eq!(run(&s, args), queued(), "{args:?}");
        }
        let r = array(run(&s, &["EXEC"]));
        assert_eq!(r.len(), queue.len());
        assert_eq!(array(r[0].clone()).len(), 50);
        assert_eq!(text(&array(r[1].clone())[0]), "0");
        assert_eq!(array(array(r[1].clone())[1].clone()).len(), 51);
        // Inside EXEC a map-shaped reply is marked as one.
        match &r[2] {
            Reply::Map(items) => assert_eq!(items.len(), 100),
            other => panic!("HGETALL inside EXEC: {}", brief(other)),
        }
        assert_eq!(array(array(r[3].clone())[1].clone()).len(), 100);
        assert_eq!(r[4], Reply::Ok);
        assert_eq!(r[5], Reply::Array(vec![bulk("1"), bulk("2"), Reply::Nil]));
        assert_eq!(r[6], Reply::Int(2));
        assert!(
            matches!(r[7], Reply::Error(_)),
            "MULTI inside a script: {}",
            brief(&r[7])
        );
        assert!(matches!(r[8], Reply::Bulk(_)));
        assert_eq!(r[9], Reply::Int(53));
        assert_eq!(r[10], Reply::Int(4));
        assert_eq!(r[11], Reply::Ok);
        assert!((1..=500).contains(&int(&r[12])));
        assert_eq!(r[13], Reply::Ok);
        assert_eq!(r[14], Reply::Int(0));
        assert_eq!(r[15], Reply::Ok);
        assert_eq!(run(&s, &["KEYS", "*"]), bulks(&["after"]));
        // Scripts cannot open or close transactions.
        for cmd in ["MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"] {
            let script = format!("return redis.call('{cmd}', 'k')");
            assert!(
                matches!(run(&s, &["EVAL", &script, "0"]), Reply::Error(_)),
                "{cmd}"
            );
        }
        assert_eq!(run(&s, &["EXEC"]), err("ERR EXEC without MULTI"));
    });
}

#[test]
fn watch_aborts_on_any_change_and_only_then() {
    guarded("watch", 30, || {
        let db = open(cfg(4, 4, 2));
        let s = session(&db);
        let o = session(&db);
        let txn = |s: &Session| {
            assert_eq!(run(s, &["MULTI"]), Reply::Ok);
            assert_eq!(run(s, &["INCR", "ran"]), queued());
            run(s, &["EXEC"])
        };
        // Each of these, done by another session after WATCH, must abort.
        let changes: Vec<Vec<&str>> = vec![
            vec!["SET", "w", "x"],
            vec!["SET", "w", "same"],
            vec!["DEL", "w"],
            vec!["UNLINK", "w"],
            vec!["APPEND", "w", "x"],
            vec!["GETDEL", "w"],
            vec!["GETSET", "w", "x"],
            vec!["EXPIRE", "w", "100"],
            vec!["PEXPIRE", "w", "-1"],
            vec!["GETEX", "w", "EX", "100"],
            vec!["MSET", "other", "1", "w", "x"],
            vec!["SET", "w", "x", "XX", "KEEPTTL"],
            vec!["FLUSHALL"],
            vec!["EVAL", "return redis.call('SET', KEYS[1], 'x')", "1", "w"],
            vec!["EVAL", "return redis.call('DEL', KEYS[1])", "1", "w"],
        ];
        for change in &changes {
            run(&o, &["SET", "w", "same"]);
            assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
            run(&o, change);
            assert_eq!(txn(&s), Reply::NilArray, "{change:?} went unnoticed");
            // The watch is spent: the next transaction runs.
            assert!(
                matches!(txn(&s), Reply::Array(_)),
                "{change:?}: watch outlived EXEC"
            );
        }
        // Changes through a hash, and a transaction by the other session.
        run(&o, &["FLUSHALL"]);
        run(&o, &["HSET", "w", "f", "1"]);
        for change in [
            vec!["HSET", "w", "g", "1"],
            vec!["HSET", "w", "f", "1"],
            vec!["HDEL", "w", "f"],
            vec!["HINCRBY", "w", "n", "1"],
            vec!["HSETNX", "w", "new", "1"],
        ] {
            assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
            run(&o, &change);
            assert_eq!(txn(&s), Reply::NilArray, "{change:?} went unnoticed");
        }
        assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
        run(&o, &["MULTI"]);
        run(&o, &["HSET", "w", "z", "1"]);
        run(&o, &["EXEC"]);
        assert_eq!(txn(&s), Reply::NilArray);

        // A key that did not exist and is created; one that is recreated
        // with the same value.
        run(&o, &["DEL", "w"]);
        assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
        run(&o, &["SET", "w", "v"]);
        assert_eq!(txn(&s), Reply::NilArray);
        assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
        run(&o, &["DEL", "w"]);
        run(&o, &["SET", "w", "v"]);
        assert_eq!(txn(&s), Reply::NilArray);

        // Expiry between WATCH and EXEC counts as a change.
        run(&o, &["SET", "w", "v", "PX", "100"]);
        assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
        db.advance_clock(100);
        assert_eq!(txn(&s), Reply::NilArray);
        run(&o, &["SET", "w", "v", "PX", "100"]);
        assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
        db.advance_clock(100);
        db.sweep_expired();
        assert_eq!(txn(&s), Reply::NilArray);

        // Not a change: reads, failed writes, the watcher's own queue,
        // UNWATCH, DISCARD.
        run(&o, &["SET", "w", "v"]);
        let ran = || int(&run(&o, &["INCRBY", "ran", "0"]));
        for harmless in [
            vec!["GET", "w"],
            vec!["TTL", "w"],
            vec!["EXISTS", "w"],
            vec!["STRLEN", "w"],
            vec!["TYPE", "w"],
            vec!["SET", "w", "x", "NX"],
            vec!["SETNX", "w", "x"],
            vec!["HSET", "w", "f", "v"],
            vec!["INCR", "w"],
            vec!["PERSIST", "w"],
            vec!["GETEX", "w"],
            vec!["EXPIRE", "w", "100", "XX"],
        ] {
            assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
            run(&o, &harmless);
            let before = ran();
            assert!(
                matches!(txn(&s), Reply::Array(_)),
                "{harmless:?} aborted the transaction"
            );
            assert_eq!(ran(), before + 1);
        }
        assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        assert_eq!(run(&s, &["SET", "w", "mine"]), queued());
        assert_eq!(run(&s, &["GET", "w"]), queued());
        assert_eq!(
            run(&s, &["EXEC"]),
            Reply::Array(vec![Reply::Ok, bulk("mine")])
        );

        assert_eq!(run(&s, &["WATCH", "w", "w2", "w3"]), Reply::Ok);
        run(&o, &["SET", "w3", "x"]);
        assert_eq!(run(&s, &["UNWATCH"]), Reply::Ok);
        assert!(matches!(txn(&s), Reply::Array(_)));

        assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
        run(&o, &["SET", "w", "x"]);
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        assert_eq!(run(&s, &["DISCARD"]), Reply::Ok);
        assert!(matches!(txn(&s), Reply::Array(_)), "DISCARD kept the watch");

        // A poisoned transaction also spends the watch.
        assert_eq!(run(&s, &["WATCH", "w"]), Reply::Ok);
        run(&o, &["SET", "w", "y"]);
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        assert!(matches!(run(&s, &["NOPE"]), Reply::Error(_)));
        assert!(has_err(&run(&s, &["EXEC"]), "EXECABORT"));
        assert!(matches!(txn(&s), Reply::Array(_)));

        // Any one of many watched keys.
        let keys: Vec<String> = (0..300).map(|i| format!("many{i}")).collect();
        for touched in [0, 150, 299] {
            let mut args = vec!["WATCH"];
            args.extend(keys.iter().map(String::as_str));
            assert_eq!(run(&s, &args), Reply::Ok);
            run(&o, &["SET", &keys[touched], "x"]);
            assert_eq!(txn(&s), Reply::NilArray, "key {touched} of 300");
        }
    });
}

#[test]
fn optimistic_counter_loses_no_update() {
    guarded("watch-cas", 120, || {
        for shards in [1, 16] {
            let db = open(cfg(shards, 4, 2));
            const THREADS: usize = 6;
            const EACH: usize = 300;
            let stop = Arc::new(AtomicBool::new(false));
            let noise = {
                let (db, stop) = (db.clone(), stop.clone());
                thread::spawn(move || {
                    let s = session(&db);
                    let mut i = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        run(&s, &["SET", &format!("noise{}", i % 5_000), "v", "PX", "3"]);
                        if i.is_multiple_of(97) {
                            run(&s, &["GET", "counter"]);
                        }
                        i += 1;
                    }
                })
            };
            let workers: Vec<_> = (0..THREADS)
                .map(|_| {
                    let db = db.clone();
                    thread::spawn(move || {
                        let s = session(&db);
                        let mut retries = 0u64;
                        for _ in 0..EACH {
                            loop {
                                assert_eq!(run(&s, &["WATCH", "counter"]), Reply::Ok);
                                let now = match run(&s, &["GET", "counter"]) {
                                    Reply::Nil => 0,
                                    other => text(&other).parse::<u64>().expect("number"),
                                };
                                assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
                                let next = (now + 1).to_string();
                                assert_eq!(run(&s, &["SET", "counter", &next]), queued());
                                match run(&s, &["EXEC"]) {
                                    Reply::NilArray => retries += 1,
                                    Reply::Array(r) => {
                                        assert_eq!(r, vec![Reply::Ok]);
                                        break;
                                    }
                                    other => panic!("EXEC: {}", brief(&other)),
                                }
                            }
                        }
                        retries
                    })
                })
                .collect();
            let retries: u64 = workers.into_iter().map(|w| w.join().expect("worker")).sum();
            stop.store(true, Ordering::Relaxed);
            noise.join().expect("noise");
            let s = session(&db);
            assert_eq!(
                run(&s, &["GET", "counter"]),
                bulk(&(THREADS * EACH).to_string()),
                "{shards} shards, {retries} retries: an update was lost"
            );
        }
    });
}

#[test]
fn transactions_are_atomic_for_readers() {
    guarded("txn-atomic", 120, || {
        let db = open(cfg(8, 2, 1));
        let s = session(&db);
        const ACCOUNTS: usize = 8;
        for i in 0..ACCOUNTS {
            run(&s, &["SET", &format!("acct{i}"), "1000"]);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let movers: Vec<_> = (0..4u64)
            .map(|t| {
                let (db, stop) = (db.clone(), stop.clone());
                thread::spawn(move || {
                    let s = session(&db);
                    let mut rng = Rng::new(t + 77);
                    let mut done = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let from = format!("acct{}", rng.below(ACCOUNTS as u64));
                        let to = format!("acct{}", rng.below(ACCOUNTS as u64));
                        let amount = rng.below(50).to_string();
                        run(&s, &["MULTI"]);
                        run(&s, &["DECRBY", &from, &amount]);
                        // Fails at run time; the transfer still completes.
                        run(&s, &["HSET", &from, "f", "v"]);
                        run(&s, &["INCRBY", &to, &amount]);
                        let r = array(run(&s, &["EXEC"]));
                        assert_eq!(r.len(), 3);
                        done += 1;
                    }
                    done
                })
            })
            .collect();
        let total = |r: Reply| -> i64 {
            array(r)
                .iter()
                .map(|v| text(v).parse::<i64>().expect("balance"))
                .sum()
        };
        let keys: Vec<String> = (0..ACCOUNTS).map(|i| format!("acct{i}")).collect();
        let mut mget = vec!["MGET"];
        mget.extend(keys.iter().map(String::as_str));
        let started = Instant::now();
        let mut reads = 0;
        while started.elapsed().as_millis() < 2_000 {
            // Three ways to read all balances at one instant.
            assert_eq!(
                total(run(&s, &mget)),
                1000 * ACCOUNTS as i64,
                "MGET saw half a transfer"
            );
            run(&s, &["MULTI"]);
            for key in &keys {
                run(&s, &["GET", key]);
            }
            assert_eq!(
                total(run(&s, &["EXEC"])),
                1000 * ACCOUNTS as i64,
                "EXEC saw half"
            );
            let script =
                "local t = 0 for i = 1, #KEYS do t = t + redis.call('GET', KEYS[i]) end return t";
            let mut eval = vec!["EVAL", script, "8"];
            eval.extend(keys.iter().map(String::as_str));
            assert_eq!(
                int(&run(&s, &eval)),
                1000 * ACCOUNTS as i64,
                "EVAL saw half"
            );
            reads += 1;
        }
        stop.store(true, Ordering::Relaxed);
        let moved: u64 = movers.into_iter().map(|m| m.join().expect("mover")).sum();
        assert!(
            moved > 500 && reads > 50,
            "{moved} transfers, {reads} reads"
        );
        assert_eq!(total(run(&s, &mget)), 1000 * ACCOUNTS as i64);
    });
}

#[test]
fn a_huge_transaction_runs() {
    guarded("txn-huge", 60, || {
        let db = open(cfg(4, 4, 2));
        let s = session(&db);
        const N: usize = 100_000;
        assert_eq!(run(&s, &["MULTI"]), Reply::Ok);
        for i in 0..N {
            assert_eq!(
                run(&s, &["SET", &format!("k{}", i % 1_000), &i.to_string()]),
                queued()
            );
        }
        assert_eq!(run(&s, &["INCR", "count"]), queued());
        let r = array(run(&s, &["EXEC"]));
        assert_eq!(r.len(), N + 1);
        assert!(r[..N].iter().all(|x| *x == Reply::Ok));
        assert_eq!(int(&run(&s, &["DBSIZE"])), 1_001);
        assert_eq!(run(&s, &["GET", "k999"]), bulk(&(N - 1).to_string()));
    });
}
