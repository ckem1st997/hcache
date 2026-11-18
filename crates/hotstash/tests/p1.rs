//! The P1 group: deadlines on hash fields, HyperLogLog, DUMP and RESTORE,
//! keyspace notifications, MEMORY USAGE, LCS and FUNCTION. Written to find
//! mistakes: odd arguments, state that should not survive, damaged input.

mod net;

use hotstash::{Db, PersistOptions, Reply, Session};
use hotstash_core::rdb;
use net::*;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn fresh() -> (Db, Session) {
    let db = Db::open(None, engine_cfg(4)).unwrap();
    let s = db.session().unwrap();
    (db, s)
}

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn ints(xs: &[i64]) -> Reply {
    Reply::Array(xs.iter().map(|&x| Reply::Int(x)).collect())
}

fn int(reply: Reply) -> i64 {
    match reply {
        Reply::Int(n) => n,
        other => panic!("not an integer: {other:?}"),
    }
}

fn bytes(reply: Reply) -> Vec<u8> {
    match reply {
        Reply::Bulk(v) => v,
        other => panic!("not a bulk string: {other:?}"),
    }
}

fn error(reply: Reply) -> String {
    match reply {
        Reply::Error(text) => text,
        other => panic!("not an error: {other:?}"),
    }
}

fn items(reply: Reply) -> Vec<Reply> {
    match reply {
        Reply::Array(items) | Reply::Map(items) => items,
        other => panic!("not an array: {other:?}"),
    }
}

/// A time left in milliseconds: the clock runs while the test does.
fn near(reply: Reply, want: i64) {
    let got = match reply {
        Reply::Array(items) => int(items[0].clone()),
        other => int(other),
    };
    assert!(
        (want - 500..=want).contains(&got),
        "{got} is not about {want}"
    );
}

fn exists(s: &Session, key: &str) -> bool {
    int(s.execute(&["EXISTS", key])) == 1
}

/// A small generator, so that failures repeat.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

// ---- hash fields with a deadline ----

#[test]
fn field_deadlines_are_set_reported_and_kept() {
    let (db, s) = fresh();
    s.execute(&["HSET", "h", "a", "1", "b", "2", "c", "3"]);
    assert_eq!(
        s.execute(&["HEXPIRE", "h", "100", "FIELDS", "3", "a", "b", "zz"]),
        ints(&[1, 1, -2])
    );
    assert_eq!(
        s.execute(&["HTTL", "h", "FIELDS", "3", "a", "c", "zz"]),
        ints(&[100, -1, -2])
    );
    near(s.execute(&["HPTTL", "h", "FIELDS", "1", "a"]), 100_000);
    let at = items(s.execute(&["HPEXPIRETIME", "h", "FIELDS", "2", "a", "c"]));
    assert!(int(at[0].clone()) > 100_000, "{at:?}");
    assert_eq!(at[1], Reply::Int(-1));
    let secs = items(s.execute(&["HEXPIRETIME", "h", "FIELDS", "1", "a"]));
    assert_eq!(int(secs[0].clone()), (int(at[0].clone()) + 500) / 1000);

    // A missing key answers for every field; another type is refused.
    assert_eq!(
        s.execute(&["HEXPIRE", "none", "10", "FIELDS", "2", "a", "b"]),
        ints(&[-2, -2])
    );
    assert_eq!(
        s.execute(&["HTTL", "none", "FIELDS", "1", "a"]),
        ints(&[-2])
    );
    assert_eq!(
        s.execute(&["HPERSIST", "none", "FIELDS", "1", "a"]),
        ints(&[-2])
    );
    assert!(!exists(&s, "none"));
    s.execute(&["SET", "str", "v"]);
    for cmd in [
        vec!["HEXPIRE", "str", "10", "FIELDS", "1", "a"],
        vec!["HTTL", "str", "FIELDS", "1", "a"],
        vec!["HPERSIST", "str", "FIELDS", "1", "a"],
    ] {
        assert!(error(s.execute(&cmd)).starts_with("WRONGTYPE"), "{cmd:?}");
    }

    // Shortly before the deadline the field is there; at it, gone.
    db.advance_clock(99_000);
    assert_eq!(s.execute(&["HGET", "h", "a"]), b("1"));
    assert_eq!(s.execute(&["HLEN", "h"]), Reply::Int(3));
    db.advance_clock(1000);
    assert_eq!(s.execute(&["HGET", "h", "a"]), Reply::Nil);
    assert_eq!(s.execute(&["HEXISTS", "h", "b"]), Reply::Int(0));
    assert_eq!(s.execute(&["HLEN", "h"]), Reply::Int(1));
    assert_eq!(
        s.execute(&["HGETALL", "h"]),
        Reply::Array(vec![b("c"), b("3")])
    );
    assert_eq!(s.execute(&["HKEYS", "h"]), Reply::Array(vec![b("c")]));
    assert_eq!(
        s.execute(&["HMGET", "h", "a", "b", "c"]),
        Reply::Array(vec![Reply::Nil, Reply::Nil, b("3")])
    );
    assert_eq!(
        s.execute(&["HTTL", "h", "FIELDS", "2", "a", "c"]),
        ints(&[-2, -1])
    );
    // A field that came back is a new field: no deadline.
    s.execute(&["HSET", "h", "a", "again"]);
    assert_eq!(s.execute(&["HTTL", "h", "FIELDS", "1", "a"]), ints(&[-1]));
    db.advance_clock(1 << 40);
    assert_eq!(s.execute(&["HGET", "h", "a"]), b("again"));
}

#[test]
fn field_deadline_conditions() {
    let (_db, s) = fresh();
    s.execute(&["HSET", "h", "plain", "1", "timed", "2"]);
    s.execute(&["HEXPIRE", "h", "100", "FIELDS", "1", "timed"]);
    let both = |opts: &[&str]| {
        let mut cmd = vec!["HEXPIRE", "h"];
        cmd.extend_from_slice(opts);
        cmd.extend_from_slice(&["FIELDS", "2", "plain", "timed"]);
        s.execute(&cmd)
    };
    assert_eq!(both(&["50", "XX"]), ints(&[0, 1]));
    assert_eq!(both(&["50", "GT"]), ints(&[0, 0]));
    assert_eq!(both(&["60", "gt"]), ints(&[0, 1]));
    assert_eq!(both(&["60", "LT"]), ints(&[1, 0]));
    assert_eq!(both(&["40", "LT"]), ints(&[1, 1]));
    assert_eq!(both(&["500", "NX"]), ints(&[0, 0]));
    assert_eq!(
        s.execute(&["HTTL", "h", "FIELDS", "2", "plain", "timed"]),
        ints(&[40, 40])
    );
    assert_eq!(
        s.execute(&["HPERSIST", "h", "FIELDS", "3", "plain", "nope", "plain"]),
        ints(&[1, -2, -1])
    );
    assert_eq!(both(&["500", "NX"]), ints(&[1, 0]));
}

#[test]
fn a_deadline_in_the_past_deletes_the_field_and_the_last_field_the_key() {
    let (_db, s) = fresh();
    s.execute(&["HSET", "h", "a", "1", "b", "2", "c", "3"]);
    assert_eq!(
        s.execute(&["HEXPIRE", "h", "0", "FIELDS", "2", "a", "zz"]),
        ints(&[2, -2])
    );
    assert_eq!(s.execute(&["HEXISTS", "h", "a"]), Reply::Int(0));
    assert_eq!(
        s.execute(&["HPEXPIREAT", "h", "1", "FIELDS", "1", "b"]),
        ints(&[2])
    );
    // The condition is looked at first: nothing to compare with, no delete.
    assert_eq!(
        s.execute(&["HEXPIREAT", "h", "1", "XX", "FIELDS", "1", "c"]),
        ints(&[0])
    );
    assert_eq!(s.execute(&["HLEN", "h"]), Reply::Int(1));
    assert_eq!(
        s.execute(&["HEXPIREAT", "h", "1", "FIELDS", "1", "c"]),
        ints(&[2])
    );
    assert!(!exists(&s, "h"));
    assert_eq!(s.execute(&["TYPE", "h"]), Reply::Simple("none".into()));
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
    // The same field twice: deleted once, then missing.
    s.execute(&["HSET", "h", "a", "1", "b", "2"]);
    assert_eq!(
        s.execute(&["HEXPIRE", "h", "0", "FIELDS", "2", "a", "a"]),
        ints(&[2, -2])
    );
}

#[test]
fn a_hash_goes_when_its_last_field_expires() {
    let (db, s) = fresh();
    s.execute(&["HSET", "h", "a", "1", "b", "2"]);
    s.execute(&["HPEXPIRE", "h", "10", "FIELDS", "1", "a"]);
    s.execute(&["HPEXPIRE", "h", "5000", "FIELDS", "1", "b"]);
    db.advance_clock(15);
    assert_eq!(s.execute(&["HLEN", "h"]), Reply::Int(1));
    db.advance_clock(5000);
    assert!(!exists(&s, "h"));
    assert_eq!(s.execute(&["TYPE", "h"]), Reply::Simple("none".into()));
    assert_eq!(s.execute(&["HGETALL", "h"]), Reply::Array(Vec::new()));
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
    // The name is free for anything, and nothing of the old hash is left.
    ok(s.execute(&["SET", "h", "v"]));
    s.execute(&["DEL", "h"]);
    s.execute(&["HSET", "h", "a", "new"]);
    assert_eq!(s.execute(&["HTTL", "h", "FIELDS", "1", "a"]), ints(&[-1]));

    // Nobody touches these: the sweep removes them.
    for i in 0..200 {
        let key = format!("idle{i}");
        s.execute(&["HSET", &key, "f", "v"]);
        s.execute(&["HPEXPIRE", &key, "5", "FIELDS", "1", "f"]);
    }
    s.execute(&["HSET", "half", "stay", "1", "go", "2"]);
    s.execute(&["HPEXPIRE", "half", "5", "FIELDS", "1", "go"]);
    db.advance_clock(10);
    db.sweep_expired();
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(2));
    assert_eq!(s.execute(&["HLEN", "half"]), Reply::Int(1));
    // The background tick gets there too, a few hashes at a time.
    for i in 0..200 {
        let key = format!("tick{i}");
        s.execute(&["HSET", &key, "f", "v"]);
        s.execute(&["HPEXPIRE", &key, "5", "FIELDS", "1", "f"]);
    }
    db.advance_clock(10);
    for _ in 0..2000 {
        db.advance_clock(60);
        db.expire_tick();
    }
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(2));
}

#[test]
fn writes_to_a_field_decide_what_happens_to_its_deadline() {
    let (db, s) = fresh();
    s.execute(&[
        "HSET", "h", "n", "1", "f", "1.5", "s", "x", "d", "x", "nx", "x",
    ]);
    s.execute(&[
        "HEXPIRE", "h", "100", "FIELDS", "5", "n", "f", "s", "d", "nx",
    ]);
    // HINCRBY and HINCRBYFLOAT keep it, HSETNX does not touch the field.
    s.execute(&["HINCRBY", "h", "n", "1"]);
    s.execute(&["HINCRBYFLOAT", "h", "f", "1"]);
    assert_eq!(s.execute(&["HSETNX", "h", "nx", "y"]), Reply::Int(0));
    // HSET and HMSET drop it, HDEL takes it along.
    s.execute(&["HSET", "h", "s", "y"]);
    s.execute(&["HDEL", "h", "d"]);
    s.execute(&["HSET", "h", "d", "back"]);
    assert_eq!(
        s.execute(&["HTTL", "h", "FIELDS", "5", "n", "f", "s", "d", "nx"]),
        ints(&[100, 100, -1, -1, 100])
    );
    db.advance_clock(100_000);
    assert_eq!(
        s.execute(&["HMGET", "h", "n", "f", "s", "d", "nx"]),
        Reply::Array(vec![Reply::Nil, Reply::Nil, b("y"), b("back"), Reply::Nil])
    );
    // A new value at the key is a new key.
    s.execute(&["HEXPIRE", "h", "100", "FIELDS", "1", "s"]);
    ok(s.execute(&["SET", "h", "string"]));
    s.execute(&["DEL", "h"]);
    s.execute(&["HSET", "h", "s", "fresh"]);
    assert_eq!(s.execute(&["HTTL", "h", "FIELDS", "1", "s"]), ints(&[-1]));
    s.execute(&["HEXPIRE", "h", "100", "FIELDS", "1", "s"]);
    ok(s.execute(&["FLUSHALL"]));
    s.execute(&["HSET", "h", "s", "fresh"]);
    assert_eq!(s.execute(&["HTTL", "h", "FIELDS", "1", "s"]), ints(&[-1]));
    db.advance_clock(1 << 40);
    assert_eq!(s.execute(&["HGET", "h", "s"]), b("fresh"));
}

#[test]
fn field_deadlines_follow_the_key_around() {
    let (db, s) = fresh();
    s.execute(&["HSET", "h", "a", "1", "b", "2"]);
    s.execute(&["HPEXPIRE", "h", "1000", "FIELDS", "1", "a"]);
    ok(s.execute(&["RENAME", "h", "moved"]));
    near(s.execute(&["HPTTL", "moved", "FIELDS", "1", "a"]), 1000);
    assert_eq!(
        s.execute(&["HPTTL", "moved", "FIELDS", "1", "b"]),
        ints(&[-1])
    );
    assert_eq!(s.execute(&["COPY", "moved", "copy"]), Reply::Int(1));
    assert_eq!(
        s.execute(&["COPY", "moved", "far", "DB", "3"]),
        Reply::Int(1)
    );
    assert_eq!(s.execute(&["MOVE", "moved", "2"]), Reply::Int(1));
    // The copy is its own key: changing one leaves the other.
    assert_eq!(
        s.execute(&["HPERSIST", "copy", "FIELDS", "1", "a"]),
        ints(&[1])
    );
    ok(s.execute(&["SELECT", "2"]));
    near(s.execute(&["HPTTL", "moved", "FIELDS", "1", "a"]), 1000);
    ok(s.execute(&["SELECT", "3"]));
    near(s.execute(&["HPTTL", "far", "FIELDS", "1", "a"]), 1000);
    db.advance_clock(1000);
    assert_eq!(s.execute(&["HKEYS", "far"]), Reply::Array(vec![b("b")]));
    ok(s.execute(&["SELECT", "2"]));
    assert_eq!(s.execute(&["HKEYS", "moved"]), Reply::Array(vec![b("b")]));
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["HLEN", "copy"]), Reply::Int(2));

    // The key's own deadline wins when it comes first, and takes the
    // field deadlines with it.
    s.execute(&["HSET", "both", "f", "v"]);
    s.execute(&["HEXPIRE", "both", "100", "FIELDS", "1", "f"]);
    s.execute(&["PEXPIRE", "both", "10"]);
    db.advance_clock(10);
    assert!(!exists(&s, "both"));
    s.execute(&["HSET", "both", "f", "v2"]);
    assert_eq!(
        s.execute(&["HTTL", "both", "FIELDS", "1", "f"]),
        ints(&[-1])
    );
    db.advance_clock(1 << 40);
    assert_eq!(s.execute(&["HGET", "both", "f"]), b("v2"));
}

#[test]
fn field_deadline_arguments_are_checked() {
    let (_db, s) = fresh();
    s.execute(&["HSET", "h", "a", "1"]);
    let cases: &[(&[&str], &str)] = &[
        (&["HEXPIRE", "h", "10", "FIELDS", "2", "a"], "numFields"),
        (
            &["HEXPIRE", "h", "10", "FIELDS", "1", "a", "b"],
            "numFields",
        ),
        (&["HEXPIRE", "h", "10", "FIELDS", "0", "a"], "numFields"),
        (&["HEXPIRE", "h", "10", "FIELDS", "-1", "a"], "numFields"),
        (
            &["HEXPIRE", "h", "10", "FIELDS", "x", "a"],
            "not an integer",
        ),
        (
            &["HEXPIRE", "h", "10", "FIELD", "1", "a"],
            "Unsupported option",
        ),
        (&["HEXPIRE", "h", "10", "NX", "1", "a"], "FIELDS"),
        (
            &["HEXPIRE", "h", "10", "NX", "XX", "FIELDS", "1", "a"],
            "FIELDS",
        ),
        (
            &["HEXPIRE", "h", "-1", "FIELDS", "1", "a"],
            "invalid expire",
        ),
        (&["HEXPIRE", "h", "x", "FIELDS", "1", "a"], "not an integer"),
        (
            &["HEXPIRE", "h", "9223372036854775807", "FIELDS", "1", "a"],
            "invalid expire",
        ),
        (
            &["HPEXPIRE", "h", "9223372036854775807", "FIELDS", "1", "a"],
            "invalid expire",
        ),
        (&["HEXPIRE", "h", "10", "FIELDS", "1"], "wrong number"),
        (&["HTTL", "h", "1", "a"], "wrong number"),
        (&["HTTL", "h", "FIELDS", "2", "a"], "numFields"),
        (&["HTTL", "h", "FIELDZ", "1", "a"], "FIELDS"),
        (&["HPERSIST", "h", "FIELDS", "1", "a", "b"], "numFields"),
        (&["HPERSIST", "h"], "wrong number"),
    ];
    for (cmd, want) in cases {
        let got = error(s.execute(cmd));
        assert!(got.contains(want), "{cmd:?}: {got}");
    }
    // Nothing above changed anything.
    assert_eq!(s.execute(&["HTTL", "h", "FIELDS", "1", "a"]), ints(&[-1]));
    // The largest time that fits is taken.
    assert_eq!(
        s.execute(&["HPEXPIREAT", "h", "9223372036854775807", "FIELDS", "1", "a"]),
        ints(&[1])
    );
    assert_eq!(
        s.execute(&["HPEXPIRETIME", "h", "FIELDS", "1", "a"]),
        ints(&[i64::MAX])
    );
}

#[test]
fn transactions_and_scripts_see_expired_fields_as_gone() {
    let (db, s) = fresh();
    s.execute(&["HSET", "h", "a", "1", "b", "2"]);
    s.execute(&["HPEXPIRE", "h", "10", "FIELDS", "1", "a"]);
    db.advance_clock(10);
    ok(s.execute(&["MULTI"]));
    s.execute(&["HGET", "h", "a"]);
    s.execute(&["HLEN", "h"]);
    assert_eq!(
        s.execute(&["EXEC"]),
        Reply::Array(vec![Reply::Nil, Reply::Int(1)])
    );
    s.execute(&["HSET", "h2", "a", "1", "b", "2"]);
    s.execute(&["HPEXPIRE", "h2", "10", "FIELDS", "1", "a"]);
    db.advance_clock(10);
    let script = "return {redis.call('HEXISTS', KEYS[1], 'a'), redis.call('HLEN', KEYS[1]), \
                  redis.call('HTTL', KEYS[1], 'FIELDS', 1, 'b')[1]}";
    assert_eq!(s.execute(&["EVAL", script, "1", "h2"]), ints(&[0, 1, -1]));
    // A script can set them, too.
    let set = "return redis.call('HPEXPIRE', KEYS[1], 50, 'FIELDS', 1, 'b')";
    assert_eq!(s.execute(&["EVAL", set, "1", "h2"]), ints(&[1]));
    db.advance_clock(50);
    assert!(!exists(&s, "h2"));
}

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("hotstash-p1-{}-{name}.hc", std::process::id()));
    remove(&path);
    path
}

fn remove(path: &Path) {
    for suffix in ["", ".lock", ".new", ".tmp"] {
        let mut name = path.to_path_buf().into_os_string();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

fn open_at(path: &Path) -> (Db, Session) {
    let options = PersistOptions {
        interval: Duration::from_millis(5),
        ..PersistOptions::default()
    };
    let db = Db::open_with(Some(path), engine_cfg(4), options).unwrap();
    let s = db.session().unwrap();
    (db, s)
}

#[test]
fn field_deadlines_and_sketches_survive_a_restart() {
    let path = scratch("fields");
    let (db, s) = open_at(&path);
    s.execute(&["HSET", "h", "a", "1", "b", "2", "c", "3"]);
    s.execute(&["HPEXPIRE", "h", "600000", "FIELDS", "1", "a"]);
    s.execute(&["HPEXPIRE", "h", "1", "FIELDS", "1", "c"]);
    s.execute(&["HSET", "plain", "f", "v"]);
    s.execute(&["HSET", "cleared", "f", "v"]);
    s.execute(&["HEXPIRE", "cleared", "100", "FIELDS", "1", "f"]);
    s.execute(&["HPERSIST", "cleared", "FIELDS", "1", "f"]);
    s.execute(&["PFADD", "hll", "a", "b", "c"]);
    ok(s.execute(&["SELECT", "5"]));
    s.execute(&["HSET", "h", "x", "1"]);
    s.execute(&["HEXPIRE", "h", "600", "FIELDS", "1", "x"]);
    db.sync().unwrap();
    drop(s);
    drop(db);

    std::thread::sleep(Duration::from_millis(5));
    let (db, s) = open_at(&path);
    let left = items(s.execute(&["HPTTL", "h", "FIELDS", "2", "a", "b"]));
    let ms = int(left[0].clone());
    assert!(ms > 500_000 && ms <= 600_000, "{ms}");
    assert_eq!(left[1], Reply::Int(-1));
    // `c` was due before the restart: it does not come back.
    assert_eq!(s.execute(&["HKEYS", "h"]).clone(), {
        let mut keys = items(s.execute(&["HKEYS", "h"]));
        keys.sort_by_key(|k| bytes(k.clone()));
        assert_eq!(keys, vec![b("a"), b("b")]);
        s.execute(&["HKEYS", "h"])
    });
    assert_eq!(
        s.execute(&["HTTL", "plain", "FIELDS", "1", "f"]),
        ints(&[-1])
    );
    assert_eq!(
        s.execute(&["HTTL", "cleared", "FIELDS", "1", "f"]),
        ints(&[-1])
    );
    assert_eq!(s.execute(&["PFCOUNT", "hll"]), Reply::Int(3));
    ok(s.execute(&["SELECT", "5"]));
    assert_eq!(s.execute(&["HTTL", "h", "FIELDS", "1", "x"]), ints(&[600]));
    drop(s);
    drop(db);
    remove(&path);
}

#[test]
fn a_large_hash_read_over_tcp_leaves_out_expired_fields() {
    let env = env();
    let mut c = env.client();
    let mut cmd = vec!["HSET".to_string(), "big".to_string()];
    for i in 0..3000 {
        cmd.push(format!("f{i}"));
        cmd.push("v".to_string());
    }
    c.cmd(&cmd);
    let mut expire = vec![
        "HPEXPIRE".to_string(),
        "big".to_string(),
        "10".to_string(),
        "FIELDS".to_string(),
        "1000".to_string(),
    ];
    expire.extend((0..1000).map(|i| format!("f{i}")));
    assert_eq!(c.cmd(&expire), ints(&[1; 1000]));
    env.db.advance_clock(10);
    assert_eq!(items(c.cmd(&["HGETALL", "big"])).len(), 4000);
    assert_eq!(c.cmd(&["HLEN", "big"]), Reply::Int(2000));
}

// ---- HyperLogLog ----

#[test]
fn hyperloglog_counts_merges_and_refuses_what_is_not_one() {
    let (db, s) = fresh();
    assert_eq!(s.execute(&["PFCOUNT", "none"]), Reply::Int(0));
    assert_eq!(s.execute(&["PFADD", "a", "x", "y", "z"]), Reply::Int(1));
    assert_eq!(s.execute(&["PFADD", "a", "x", "y", "z"]), Reply::Int(0));
    assert_eq!(s.execute(&["PFCOUNT", "a"]), Reply::Int(3));
    // Without elements the key is made, once.
    assert_eq!(s.execute(&["PFADD", "empty"]), Reply::Int(1));
    assert_eq!(s.execute(&["PFADD", "empty"]), Reply::Int(0));
    assert_eq!(s.execute(&["PFCOUNT", "empty"]), Reply::Int(0));
    assert_eq!(s.execute(&["TYPE", "a"]), Reply::Simple("string".into()));
    assert!(bytes(s.execute(&["GET", "a"])).starts_with(b"HYLL"));

    s.execute(&["PFADD", "b", "y", "z", "w", "v"]);
    assert_eq!(s.execute(&["PFCOUNT", "a", "b", "none"]), Reply::Int(5));
    // Counting a union changes neither.
    assert_eq!(s.execute(&["PFCOUNT", "a"]), Reply::Int(3));
    s.execute(&["PEXPIRE", "a", "1000"]);
    ok(s.execute(&["PFMERGE", "a", "b", "none"]));
    assert_eq!(s.execute(&["PFCOUNT", "a"]), Reply::Int(5));
    near(s.execute(&["PTTL", "a"]), 1000);
    ok(s.execute(&["PFMERGE", "new", "a", "empty"]));
    assert_eq!(s.execute(&["PFCOUNT", "new"]), Reply::Int(5));
    ok(s.execute(&["PFMERGE", "alone"]));
    assert_eq!(s.execute(&["PFCOUNT", "alone"]), Reply::Int(0));
    assert!(exists(&s, "alone"));
    // Merging into itself is the same sketch.
    ok(s.execute(&["PFMERGE", "a", "a", "a"]));
    assert_eq!(s.execute(&["PFCOUNT", "a"]), Reply::Int(5));
    db.advance_clock(1000);
    assert_eq!(s.execute(&["PFCOUNT", "a"]), Reply::Int(0));

    s.execute(&["RPUSH", "list", "x"]);
    s.execute(&["SET", "text", "not a sketch"]);
    s.execute(&["SET", "short", "HYLL"]);
    for cmd in [
        vec!["PFADD", "list", "x"],
        vec!["PFCOUNT", "list"],
        vec!["PFCOUNT", "b", "list"],
        vec!["PFMERGE", "list", "b"],
        vec!["PFMERGE", "dst", "b", "list"],
    ] {
        assert!(
            error(s.execute(&cmd)).starts_with("WRONGTYPE Operation"),
            "{cmd:?}"
        );
    }
    for cmd in [
        vec!["PFADD", "text", "x"],
        vec!["PFADD", "text"],
        vec!["PFCOUNT", "text"],
        vec!["PFCOUNT", "b", "short"],
        vec!["PFMERGE", "text", "b"],
        vec!["PFMERGE", "dst", "b", "short"],
    ] {
        assert_eq!(
            error(s.execute(&cmd)),
            "WRONGTYPE Key is not a valid HyperLogLog string value.",
            "{cmd:?}"
        );
    }
    // A refused merge wrote nothing.
    assert!(!exists(&s, "dst"));
    assert_eq!(s.execute(&["GET", "text"]), b("not a sketch"));
    assert!(error(s.execute(&["PFADD"])).contains("wrong number"));
    assert!(error(s.execute(&["PFCOUNT"])).contains("wrong number"));
    assert!(error(s.execute(&["PFMERGE"])).contains("wrong number"));
}

/// What Valkey 9.1.3 stored and replied for the same commands.
#[test]
fn hyperloglog_strings_match_valkey() {
    let (_db, s) = fresh();
    let unhex = |h: &str| -> Vec<u8> {
        (0..h.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap())
            .collect()
    };
    let set = |key: &str, value: &[u8]| {
        let cmd: Vec<Vec<u8>> = vec![b"SET".to_vec(), key.as_bytes().to_vec(), value.to_vec()];
        s.execute(&cmd);
    };
    s.execute(&["PFADD", "e"]);
    assert_eq!(
        bytes(s.execute(&["GET", "e"])),
        unhex("48594c4c0100000000000000000000807fff")
    );
    s.execute(&["PFADD", "s", "a", "b", "c"]);
    assert_eq!(
        bytes(s.execute(&["GET", "s"])),
        unhex("48594c4c01000000000000000000008060f38050b1844bfb80425a")
    );
    // PFCOUNT of one key keeps the count in the string.
    assert_eq!(s.execute(&["PFCOUNT", "s"]), Reply::Int(3));
    assert_eq!(
        bytes(s.execute(&["GET", "s"])),
        unhex("48594c4c01000000030000000000000060f38050b1844bfb80425a")
    );

    // One register short, with a fresh cache: the cache answers, a union
    // or a merge reads the registers and finds the damage.
    let mut short = unhex("48594c4c0100000000000000000000007ffe");
    set("c", &short);
    assert_eq!(s.execute(&["PFCOUNT", "c"]), Reply::Int(0));
    let corrupt = "INVALIDOBJ Corrupted HLL object detected";
    assert_eq!(error(s.execute(&["PFCOUNT", "c", "s"])), corrupt);
    assert_eq!(error(s.execute(&["PFMERGE", "m", "s", "c"])), corrupt);
    assert!(!exists(&s, "m"));
    // The destination is read like the sources.
    assert_eq!(error(s.execute(&["PFMERGE", "c", "s"])), corrupt);
    s.execute(&["RPUSH", "list", "x"]);
    assert_eq!(error(s.execute(&["PFCOUNT", "c", "list"])), corrupt);
    assert!(error(s.execute(&["PFCOUNT", "list", "c"])).starts_with("WRONGTYPE Operation"));
    short[HDR_STALE] |= 0x80;
    set("c", &short);
    assert_eq!(error(s.execute(&["PFCOUNT", "c"])), corrupt);
    set("blank", b"");
    for cmd in [
        vec!["PFCOUNT", "blank"],
        vec!["PFADD", "blank", "a"],
        vec!["PFMERGE", "m", "blank"],
    ] {
        assert_eq!(
            error(s.execute(&cmd)),
            "WRONGTYPE Key is not a valid HyperLogLog string value.",
            "{cmd:?}"
        );
    }
}

/// The byte of an HLL header whose top bit marks the cached count stale.
const HDR_STALE: usize = 15;

#[test]
fn hyperloglog_estimates_are_close() {
    let (_db, s) = fresh();
    for (n, key) in [(1_000usize, "k1"), (50_000, "k2")] {
        for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
            let mut cmd = vec!["PFADD".to_string(), key.to_string()];
            cmd.extend(chunk.iter().map(|i| format!("{key}:element:{i}")));
            s.execute(&cmd);
        }
        let got = int(s.execute(&["PFCOUNT", key])) as f64;
        let off = (got - n as f64).abs() / n as f64;
        assert!(off < 0.03, "{n}: {got}");
    }
    // Disjoint sets add up.
    let both = int(s.execute(&["PFCOUNT", "k1", "k2"])) as f64;
    assert!((both - 51_000.0).abs() / 51_000.0 < 0.03, "{both}");
}

#[test]
fn damaged_sketches_are_refused_not_trusted() {
    let (_db, s) = fresh();
    s.execute(&["PFADD", "good", "a", "b", "c"]);
    let good = bytes(s.execute(&["GET", "good"]));
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let set = |value: &[u8]| {
        let cmd: Vec<Vec<u8>> = vec![b"SET".to_vec(), b"k".to_vec(), value.to_vec()];
        s.execute(&cmd);
    };
    for round in 0..3000 {
        let mut value = good.clone();
        match round % 4 {
            0 => value.truncate(rng.below(good.len())),
            1 => {
                for _ in 0..1 + rng.below(8) {
                    let at = rng.below(value.len().min(64));
                    value[at] = rng.next() as u8;
                }
            }
            2 => value.extend((0..1 + rng.below(40)).map(|_| rng.next() as u8)),
            _ => {
                // A sparse header with random opcodes behind it.
                value = b"HYLL\x01\0\0\0\0\0\0\0\0\0\0\0".to_vec();
                value.extend((0..rng.below(64)).map(|_| rng.next() as u8));
            }
        }
        set(&value);
        // Whatever the answers are, they are answers.
        for cmd in [
            vec!["PFCOUNT", "k"],
            vec!["PFADD", "k", "x"],
            vec!["PFCOUNT", "k", "good"],
            vec!["PFMERGE", "out", "k", "good"],
        ] {
            match s.execute(&cmd) {
                Reply::Int(n) => assert!(n >= 0, "{cmd:?}"),
                Reply::Ok => {}
                Reply::Error(e) => assert!(
                    e.contains("HyperLogLog") || e == "INVALIDOBJ Corrupted HLL object detected",
                    "{e}"
                ),
                other => panic!("{cmd:?}: {other:?}"),
            }
        }
    }
    assert_eq!(s.execute(&["PFCOUNT", "good"]), Reply::Int(3));
}

// ---- DUMP and RESTORE ----

fn run(s: &Session, cmd: &[&[u8]]) -> Reply {
    s.execute(cmd)
}

fn restore(s: &Session, key: &str, ttl: &str, payload: &[u8], opts: &[&str]) -> Reply {
    let mut cmd: Vec<&[u8]> = vec![b"RESTORE", key.as_bytes(), ttl.as_bytes(), payload];
    cmd.extend(opts.iter().map(|o| o.as_bytes()));
    run(s, &cmd)
}

/// One key of every kind, and for each a command whose reply shows all of
/// the value.
fn zoo(s: &Session) -> Vec<(&'static str, Vec<&'static str>)> {
    s.execute(&["SET", "str", "hello"]);
    s.execute(&["SET", "num", "-12345"]);
    s.execute(&["SET", "blank", ""]);
    s.execute(&["RPUSH", "list", "a", "b", "", "a"]);
    s.execute(&["SADD", "set", "m1", "m2", ""]);
    s.execute(&[
        "ZADD", "zset", "-1.5", "a", "inf", "b", "0", "c", "-inf", "d",
    ]);
    s.execute(&["HSET", "hash", "f1", "v1", "f2", "", "", "v3"]);
    s.execute(&["HSET", "timed", "f1", "v1", "f2", "v2"]);
    s.execute(&["HPEXPIREAT", "timed", "9000000000000", "FIELDS", "1", "f1"]);
    s.execute(&["XADD", "stream", "1-1", "f", "v"]);
    s.execute(&["XADD", "stream", "2-0", "g", "w", "h", "x"]);
    s.execute(&["XGROUP", "CREATE", "stream", "grp", "0"]);
    s.execute(&[
        "XREADGROUP",
        "GROUP",
        "grp",
        "me",
        "COUNT",
        "1",
        "STREAMS",
        "stream",
        ">",
    ]);
    s.execute(&["PFADD", "hll", "a", "b"]);
    vec![
        ("str", vec!["GET"]),
        ("num", vec!["GET"]),
        ("blank", vec!["GET"]),
        ("list", vec!["LRANGE", "0", "-1"]),
        ("set", vec!["SMEMBERS"]),
        ("zset", vec!["ZRANGE", "0", "-1", "WITHSCORES"]),
        ("hash", vec!["HGETALL"]),
        ("timed", vec!["HGETALL"]),
        ("stream", vec!["XINFO"]),
        ("hll", vec!["GET"]),
    ]
}

fn look(s: &Session, key: &str, how: &[&str]) -> Vec<Reply> {
    let mut cmd = match how[0] {
        "XINFO" => vec!["XINFO", "STREAM", key],
        first => vec![first, key],
    };
    cmd.extend_from_slice(&how[1..]);
    let mut seen = match s.execute(&cmd) {
        Reply::Array(items) | Reply::Map(items) => items,
        other => vec![other],
    };
    // Sets and hashes have no order; pairs stay together as text.
    if matches!(how[0], "SMEMBERS") {
        seen.sort_by_key(|r| format!("{r:?}"));
    }
    if how[0] == "HGETALL" {
        let mut pairs: Vec<String> = seen.chunks(2).map(|p| format!("{p:?}")).collect();
        pairs.sort();
        return pairs
            .into_iter()
            .map(|p| Reply::Bulk(p.into_bytes()))
            .collect();
    }
    seen
}

#[test]
fn every_kind_of_value_comes_back_from_its_dump() {
    let (_db, s) = fresh();
    let kinds = zoo(&s);
    assert_eq!(s.execute(&["DUMP", "none"]), Reply::Nil);
    for (key, how) in &kinds {
        let payload = bytes(s.execute(&["DUMP", key]));
        let copy = format!("copy:{key}");
        ok(restore(&s, &copy, "0", &payload, &[]));
        assert_eq!(look(&s, &copy, how), look(&s, key, how), "{key}");
        assert_eq!(s.execute(&["TYPE", &copy]), s.execute(&["TYPE", key]));
        assert_eq!(s.execute(&["TTL", &copy]), Reply::Int(-1));
        // The same bytes again: a dump is a function of the value.
        assert_eq!(
            bytes(s.execute(&["DUMP", &copy])).len(),
            payload.len(),
            "{key}"
        );
        // Into another database, too.
        ok(s.execute(&["SELECT", "7"]));
        ok(restore(&s, key, "0", &payload, &[]));
        assert_eq!(look(&s, key, how).len(), {
            ok(s.execute(&["SELECT", "0"]));
            look(&s, key, how).len()
        });
    }
    assert_eq!(
        s.execute(&["HPEXPIRETIME", "copy:timed", "FIELDS", "2", "f1", "f2"]),
        ints(&[9_000_000_000_000, -1])
    );
    assert_eq!(s.execute(&["PFCOUNT", "copy:hll"]), Reply::Int(2));
    let groups = items(s.execute(&["XINFO", "GROUPS", "copy:stream"]));
    assert_eq!(groups.len(), 1);
    assert_eq!(s.execute(&["XLEN", "copy:stream"]), Reply::Int(2));
    assert_eq!(
        items(s.execute(&["XPENDING", "copy:stream", "grp"]))[0],
        Reply::Int(1)
    );
    // The key's own deadline is not in the dump.
    s.execute(&["EXPIRE", "str", "100"]);
    let payload = bytes(s.execute(&["DUMP", "str"]));
    ok(restore(&s, "str2", "0", &payload, &[]));
    assert_eq!(s.execute(&["TTL", "str2"]), Reply::Int(-1));
}

#[test]
fn restore_options_and_refusals() {
    let (db, s) = fresh();
    s.execute(&["RPUSH", "list", "a", "b"]);
    let payload = bytes(s.execute(&["DUMP", "list"]));
    assert_eq!(
        error(restore(&s, "list", "0", &payload, &[])),
        "BUSYKEY Target key name already exists."
    );
    s.execute(&["SET", "other", "v"]);
    assert!(error(restore(&s, "other", "0", &payload, &[])).starts_with("BUSYKEY"));
    ok(restore(&s, "other", "0", &payload, &["REPLACE"]));
    assert_eq!(s.execute(&["LLEN", "other"]), Reply::Int(2));
    // A relative and an absolute deadline.
    ok(restore(&s, "rel", "1500", &payload, &["replace"]));
    near(s.execute(&["PTTL", "rel"]), 1500);
    ok(restore(&s, "abs", "9000000000000", &payload, &["ABSTTL"]));
    assert_eq!(
        s.execute(&["PEXPIRETIME", "abs"]),
        Reply::Int(9_000_000_000_000)
    );
    // A deadline that has passed: no key, and with REPLACE the old one goes.
    ok(restore(&s, "past", "1", &payload, &["ABSTTL"]));
    assert!(!exists(&s, "past"));
    ok(restore(&s, "other", "1", &payload, &["ABSTTL", "REPLACE"]));
    assert!(!exists(&s, "other"));
    // REPLACE drops the old key's deadline with the old key.
    s.execute(&["SET", "ttl", "v", "PX", "100"]);
    ok(restore(&s, "ttl", "0", &payload, &["REPLACE"]));
    assert_eq!(s.execute(&["TTL", "ttl"]), Reply::Int(-1));
    db.advance_clock(1000);
    assert_eq!(s.execute(&["LLEN", "ttl"]), Reply::Int(2));
    // An expired key is no obstacle.
    s.execute(&["SET", "gone", "v", "PX", "10"]);
    db.advance_clock(10);
    ok(restore(&s, "gone", "0", &payload, &[]));
    ok(restore(
        &s,
        "opts",
        "0",
        &payload,
        &["IDLETIME", "5", "FREQ", "10"],
    ));

    let refused: &[(&str, &[&str], &str)] = &[
        ("-1", &[], "Invalid TTL"),
        ("x", &[], "not an integer"),
        ("0", &["NOPE"], "syntax"),
        ("0", &["IDLETIME"], "syntax"),
        ("0", &["IDLETIME", "-1"], "IDLETIME"),
        ("0", &["FREQ", "256"], "FREQ"),
        ("0", &["FREQ", "x"], "not an integer"),
    ];
    for (ttl, opts, want) in refused {
        let got = error(restore(&s, "bad", ttl, &payload, opts));
        assert!(got.contains(want), "{ttl} {opts:?}: {got}");
    }
    assert!(!exists(&s, "bad"));
    assert!(error(s.execute(&["RESTORE", "k", "0"])).contains("wrong number"));
    assert!(error(s.execute(&["DUMP"])).contains("wrong number"));
    assert!(error(s.execute(&["DUMP", "a", "b"])).contains("wrong number"));
}

#[test]
fn damaged_payloads_are_refused() {
    let (_db, s) = fresh();
    let kinds = zoo(&s);
    let wrong = "ERR DUMP payload version or checksum are wrong";
    for junk in [&b""[..], b"x", b"0123456789", b"\x00\x03bar\x08\x00"] {
        assert_eq!(error(restore(&s, "bad", "0", junk, &[])), wrong, "{junk:?}");
    }
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    for (key, _) in &kinds {
        let payload = bytes(s.execute(&["DUMP", key]));
        // Any one byte changed, or the end cut off: the checksum says no.
        for at in 0..payload.len() {
            let mut bad = payload.clone();
            bad[at] ^= 1 << rng.below(8);
            assert_eq!(
                error(restore(&s, "bad", "0", &bad, &[])),
                wrong,
                "{key} {at}"
            );
        }
        for len in 0..payload.len() {
            assert_eq!(
                error(restore(&s, "bad", "0", &payload[..len], &[])),
                wrong,
                "{key} {len}"
            );
        }
        // With the checksum made right again the body has to hold up by
        // itself: an error or a usable key, never anything else.
        let body = &payload[..payload.len() - 10];
        for round in 0..400 {
            let mut bad = body.to_vec();
            match round % 3 {
                0 => bad.truncate(rng.below(body.len())),
                1 => {
                    for _ in 0..1 + rng.below(4) {
                        let at = rng.below(bad.len());
                        bad[at] = rng.next() as u8;
                    }
                }
                _ => bad.extend((0..1 + rng.below(16)).map(|_| rng.next() as u8)),
            }
            match restore(&s, "fuzz", "0", &rdb::seal(bad), &["REPLACE"]) {
                Reply::Ok => {
                    // Whatever came out can be read, dumped and deleted.
                    let ty = s.execute(&["TYPE", "fuzz"]);
                    assert_ne!(ty, Reply::Simple("none".into()), "{key} {round}");
                    assert!(matches!(s.execute(&["DUMP", "fuzz"]), Reply::Bulk(_)));
                    assert!(matches!(
                        s.execute(&["MEMORY", "USAGE", "fuzz"]),
                        Reply::Int(_)
                    ));
                    for how in [
                        vec!["LRANGE", "fuzz", "0", "-1"],
                        vec!["HGETALL", "fuzz"],
                        vec!["SMEMBERS", "fuzz"],
                        vec!["ZRANGE", "fuzz", "0", "-1", "WITHSCORES"],
                        vec!["XRANGE", "fuzz", "-", "+"],
                        vec!["XINFO", "STREAM", "fuzz", "FULL"],
                        vec!["HTTL", "fuzz", "FIELDS", "1", "f1"],
                    ] {
                        s.execute(&how);
                    }
                    assert_eq!(s.execute(&["DEL", "fuzz"]), Reply::Int(1));
                }
                Reply::Error(e) => assert_eq!(e, "ERR Bad data format", "{key} {round}"),
                other => panic!("{key} {round}: {other:?}"),
            }
        }
    }
    assert!(!exists(&s, "bad"));

    // Lengths that promise far more than is there cost nothing.
    for body in [
        &b"\x01\x80\xff\xff\xff\xff"[..],
        b"\x01\x81\xff\xff\xff\xff\xff\xff\xff\xff",
        b"\x00\x80\x7f\xff\xff\xff",
        b"\x02\x80\xff\xff\xff\xff\x01a",
        b"\x04\x81\x00\x00\x00\x01\x00\x00\x00\x00",
        b"\x05\x80\xff\xff\xff\xff",
        b"\xe0\xff\xff\xff\xff\xff\xff\xff\xff",
        b"\xe1\xff\xff\xff\xff",
        b"\xe1\x01\x00\x00\x00\xff\xff\xff\xff",
        b"\xe2",
        b"\xff",
        b"\x01\x00",
        b"\x04\x00",
        b"\xe1\x00\x00\x00\x00",
    ] {
        let started = std::time::Instant::now();
        assert_eq!(
            error(restore(&s, "huge", "0", &rdb::seal(body.to_vec()), &[])),
            "ERR Bad data format",
            "{body:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2), "{body:?}");
    }
    // Duplicates would make a set or hash that lies about its size.
    for body in [
        &b"\x02\x02\x01a\x01a"[..],
        b"\x04\x02\x01f\x01v\x01f\x01w",
        b"\xe1\x02\x00\x00\x00\x01\x00\x00\x00f\x00\x00\x00\x00\0\0\0\0\0\0\0\0\
          \x01\x00\x00\x00f\x00\x00\x00\x00\0\0\0\0\0\0\0\0",
    ] {
        assert_eq!(
            error(restore(&s, "dup", "0", &rdb::seal(body.to_vec()), &[])),
            "ERR Bad data format",
            "{body:?}"
        );
    }
    // A payload as Redis 7 writes it (RDB version 11) is taken.
    let mut redis7 = b"\x00\x03bar".to_vec();
    redis7.extend_from_slice(&11u16.to_le_bytes());
    let crc = rdb::seal(b"\x00\x03bar".to_vec());
    assert_ne!(crc[5..7], redis7[5..7]);
    ok(restore(&s, "v8", "0", &crc, &[]));
    assert_eq!(s.execute(&["GET", "v8"]), b("bar"));
}

// ---- keyspace notifications ----

/// The events a subscriber to `__keyevent@*` has received, as
/// "event:key" (or "db/event:key" for a database other than 0), up to the
/// one for key `until`.
fn events(sub: &mut Client, until: &str) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        let message = items(sub.read());
        assert_eq!(message[0], b("pmessage"), "{message:?}");
        let channel = String::from_utf8(bytes(message[2].clone())).unwrap();
        let key = String::from_utf8_lossy(&bytes(message[3].clone())).into_owned();
        let (db, event) = channel
            .strip_prefix("__keyevent@")
            .and_then(|rest| rest.split_once("__:"))
            .unwrap_or_else(|| panic!("{channel}"));
        if key == until {
            return seen;
        }
        seen.push(match db {
            "0" => format!("{event}:{key}"),
            db => format!("{db}/{event}:{key}"),
        });
    }
}

fn notifying(flags: &str) -> (Env, Client, Client) {
    let env = env();
    let mut c = env.client();
    ok(c.cmd(&["CONFIG", "SET", "notify-keyspace-events", flags]));
    let mut sub = env.client();
    sub.cmd(&["PSUBSCRIBE", "__keyevent@*"]);
    (env, c, sub)
}

#[test]
fn the_notification_setting_is_parsed_and_shown() {
    let (_db, s) = fresh();
    let get = || items(s.execute(&["CONFIG", "GET", "notify-keyspace-events"]))[1].clone();
    assert_eq!(get(), b(""));
    for (set, shown) in [
        ("KEA", "AKE"),
        ("Kg$lshzxetd", "AK"),
        ("Ex", "xE"),
        ("Klh", "lhK"),
        ("A", "A"),
        ("", ""),
    ] {
        ok(s.execute(&["CONFIG", "SET", "notify-keyspace-events", set]));
        assert_eq!(get(), b(shown), "{set}");
    }
    for bad in ["KEq", "k", " ", "KE,"] {
        let got = error(s.execute(&["CONFIG", "SET", "notify-keyspace-events", bad]));
        assert!(got.contains("notify-keyspace-events"), "{got}");
        assert_eq!(get(), b(""));
    }
    // Without a server there is nobody to tell; commands run as ever.
    ok(s.execute(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]));
    ok(s.execute(&["SET", "k", "v"]));
    assert_eq!(s.execute(&["DEL", "k"]), Reply::Int(1));
}

#[test]
fn commands_report_what_they_did() {
    let (_env, mut c, mut sub) = notifying("EA");
    let steps: &[(&[&str], &[&str])] = &[
        (&["SET", "s", "1"], &["set:s"]),
        (&["SET", "s", "2", "NX"], &[]),
        (&["SET", "s", "2", "XX", "GET"], &["set:s"]),
        (&["SETNX", "s", "3"], &[]),
        (&["SET", "e", "1", "EX", "100"], &["set:e", "expire:e"]),
        (&["APPEND", "s", "x"], &["append:s"]),
        (&["INCR", "n"], &["incrby:n"]),
        (&["INCR", "s"], &[]),
        (&["INCRBYFLOAT", "n", "1.5"], &["incrbyfloat:n"]),
        (&["SETRANGE", "s", "1", "zz"], &["setrange:s"]),
        (&["SETBIT", "bits", "3", "1"], &["setbit:bits"]),
        (&["MSET", "m1", "1", "m2", "2"], &["set:m1", "set:m2"]),
        (&["MSETNX", "m1", "1", "m3", "2"], &[]),
        (&["GETDEL", "m1"], &["del:m1"]),
        (&["GETDEL", "m1"], &[]),
        (&["EXPIRE", "s", "100"], &["expire:s"]),
        (&["EXPIRE", "s", "100", "NX"], &[]),
        (&["EXPIRE", "nokey", "100"], &[]),
        (&["PERSIST", "s"], &["persist:s"]),
        (&["PERSIST", "s"], &[]),
        (&["GETEX", "s", "EX", "50"], &["expire:s"]),
        (&["GETEX", "s", "PERSIST"], &["persist:s"]),
        (&["GETEX", "s"], &[]),
        (&["RENAME", "s", "t"], &["rename_from:s", "rename_to:t"]),
        (&["RENAME", "nokey", "t"], &[]),
        (&["RENAMENX", "t", "n"], &[]),
        (&["COPY", "t", "t2"], &["copy_to:t2"]),
        (&["COPY", "t", "t2"], &[]),
        (&["DEL", "t", "nokey", "t2"], &["del:t", "del:t2"]),
        (&["EXPIRE", "n", "0"], &["del:n"]),
        (&["LPUSH", "l", "a", "b"], &["lpush:l"]),
        (&["RPUSHX", "nolist", "a"], &[]),
        (&["LSET", "l", "0", "z"], &["lset:l"]),
        (&["LINSERT", "l", "BEFORE", "z", "y"], &["linsert:l"]),
        (&["LINSERT", "l", "BEFORE", "nope", "y"], &[]),
        (&["LREM", "l", "0", "y"], &["lrem:l"]),
        (&["LTRIM", "l", "0", "0"], &["ltrim:l"]),
        (&["LTRIM", "nolist", "0", "0"], &[]),
        (
            &["LMOVE", "l", "l2", "LEFT", "RIGHT"],
            &["lpop:l", "del:l", "rpush:l2"],
        ),
        (&["RPOP", "l2"], &["rpop:l2", "del:l2"]),
        (&["RPOP", "l2"], &[]),
        (&["RPUSH", "l3", "a", "b"], &["rpush:l3"]),
        (&["LTRIM", "l3", "5", "9"], &["ltrim:l3", "del:l3"]),
        (&["RPUSH", "l4", "a"], &["rpush:l4"]),
        (
            &["LMPOP", "2", "nolist", "l4", "LEFT"],
            &["lpop:l4", "del:l4"],
        ),
        (&["SADD", "st", "a", "b"], &["sadd:st"]),
        (&["SADD", "st", "a"], &[]),
        (&["SMOVE", "st", "st2", "a"], &["srem:st", "sadd:st2"]),
        (&["SREM", "st", "b"], &["srem:st", "del:st"]),
        (&["SPOP", "st2"], &["spop:st2", "del:st2"]),
        (&["SADD", "x1", "a", "b"], &["sadd:x1"]),
        (&["SUNIONSTORE", "x2", "x1", "nokey"], &["sunionstore:x2"]),
        (&["ZADD", "z", "1", "a", "2", "b"], &["zadd:z"]),
        (&["ZINCRBY", "z", "1", "a"], &["zincr:z"]),
        (&["ZADD", "z", "INCR", "1", "a"], &["zincr:z"]),
        (&["ZREM", "z", "nope"], &[]),
        (&["ZRANGESTORE", "z2", "z", "0", "-1"], &["zrangestore:z2"]),
        (&["ZUNIONSTORE", "z3", "1", "z"], &["zunionstore:z3"]),
        (
            &["ZREMRANGEBYSCORE", "z", "-inf", "+inf"],
            &["zremrangebyscore:z", "del:z"],
        ),
        (&["ZPOPMIN", "z2"], &["zpopmin:z2"]),
        (&["ZPOPMAX", "z2"], &["zpopmax:z2", "del:z2"]),
        (&["HSET", "h", "f", "v", "g", "1"], &["hset:h"]),
        (&["HSETNX", "h", "f", "w"], &[]),
        (&["HINCRBY", "h", "g", "1"], &["hincrby:h"]),
        (&["HINCRBYFLOAT", "h", "g", "1"], &["hincrbyfloat:h"]),
        (
            &["HEXPIRE", "h", "100", "FIELDS", "2", "f", "nope"],
            &["hexpire:h"],
        ),
        (&["HEXPIRE", "h", "100", "NX", "FIELDS", "1", "f"], &[]),
        (&["HPERSIST", "h", "FIELDS", "1", "f"], &["hpersist:h"]),
        (&["HPERSIST", "h", "FIELDS", "1", "f"], &[]),
        (&["HEXPIRE", "h", "0", "FIELDS", "1", "f"], &["hdel:h"]),
        (&["HDEL", "h", "nope"], &[]),
        (&["HDEL", "h", "g"], &["hdel:h", "del:h"]),
        (&["XADD", "x", "1-1", "a", "1"], &["xadd:x"]),
        (&["XADD", "nox", "NOMKSTREAM", "1-1", "a", "1"], &[]),
        (&["XGROUP", "CREATE", "x", "g", "0"], &["xgroup-create:x"]),
        (&["XDEL", "x", "1-1"], &["xdel:x"]),
        (&["PFADD", "p", "a"], &["pfadd:p"]),
        (&["PFADD", "p", "a"], &[]),
        (&["PFMERGE", "p2", "p"], &["pfadd:p2"]),
        (&["LPUSH", "p", "wrong type"], &[]),
        (&["GET", "p"], &[]),
    ];
    for (i, (cmd, want)) in steps.iter().enumerate() {
        let reply = c.cmd(cmd);
        let marker = format!("marker{i}");
        ok(c.cmd(&["SET", &marker, "1"]));
        assert_eq!(&events(&mut sub, &marker), want, "{cmd:?} -> {reply:?}");
    }
    // RESTORE, with a payload from this server.
    let payload = bytes(c.cmd(&["DUMP", "p"]));
    let cmd: Vec<&[u8]> = vec![b"RESTORE", b"p3", b"0", &payload];
    ok(c.cmd(&cmd));
    ok(c.cmd(&["SET", "end", "1"]));
    assert_eq!(events(&mut sub, "end"), ["restore:p3"]);
}

#[test]
fn notifications_name_the_database_and_follow_the_setting() {
    let env = env();
    let mut c = env.client();
    let mut sub = env.client();
    sub.cmd(&["PSUBSCRIBE", "__key*"]);
    // Off: nothing is said.
    ok(c.cmd(&["SET", "quiet", "1"]));
    assert!(sub.silent_for(150));
    // Both forms, keyspace first.
    ok(c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "KE$"]));
    ok(c.cmd(&["SELECT", "3"]));
    ok(c.cmd(&["SET", "k", "1"]));
    c.cmd(&["LPUSH", "list", "not strings"]);
    let first = items(sub.read());
    assert_eq!(first[2..], [b("__keyspace@3__:k"), b("set")]);
    let second = items(sub.read());
    assert_eq!(second[2..], [b("__keyevent@3__:set"), b("k")]);
    // Classes that are off stay quiet; only the keyspace form now.
    ok(c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "Kl"]));
    ok(c.cmd(&["SET", "k", "2"]));
    c.cmd(&["LPUSH", "list", "a"]);
    let only = items(sub.read());
    assert_eq!(only[2..], [b("__keyspace@3__:list"), b("lpush")]);
    // A key that is not text travels as it is.
    ok(c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "E$"]));
    let cmd: Vec<&[u8]> = vec![b"SET", b"\xff\x00\r\n", b"v"];
    ok(c.cmd(&cmd));
    assert_eq!(items(sub.read())[3], Reply::Bulk(b"\xff\x00\r\n".to_vec()));
    // Classes without K or E, or the reverse, publish nothing.
    for flags in ["A", "KE", ""] {
        ok(c.cmd(&["CONFIG", "SET", "notify-keyspace-events", flags]));
        ok(c.cmd(&["SET", "k", "3"]));
    }
    assert!(sub.silent_for(150));
}

#[test]
fn expiry_is_announced_once() {
    let (env, mut c, mut sub) = notifying("EA");
    ok(c.cmd(&["SET", "lazy", "v", "PX", "10"]));
    ok(c.cmd(&["SET", "swept", "v", "PX", "10"]));
    ok(c.cmd(&["SET", "m0", "1"]));
    events(&mut sub, "m0");
    env.db.advance_clock(10);
    // A write finds the key expired and says so before it says "set".
    ok(c.cmd(&["SET", "lazy", "new"]));
    ok(c.cmd(&["SET", "m1", "1"]));
    assert_eq!(events(&mut sub, "m1"), ["expired:lazy", "set:lazy"]);
    env.db.sweep_expired();
    ok(c.cmd(&["SET", "m2", "1"]));
    assert_eq!(events(&mut sub, "m2"), ["expired:swept"]);
    // Reading an expired key does not delete it; the sweep reports it.
    ok(c.cmd(&["SET", "read", "v", "PX", "10"]));
    env.db.advance_clock(10);
    assert_eq!(c.cmd(&["GET", "read"]), Reply::Nil);
    env.db.sweep_expired();
    ok(c.cmd(&["SET", "m3", "1"]));
    let seen = events(&mut sub, "m3");
    assert_eq!(seen, ["set:read", "expire:read", "expired:read"]);

    // Fields: the hash says "hexpired", and "del" when that was all.
    c.cmd(&["HSET", "h", "a", "1", "b", "2"]);
    c.cmd(&["HPEXPIRE", "h", "10", "FIELDS", "1", "a"]);
    c.cmd(&["HPEXPIRE", "h", "5000", "FIELDS", "1", "b"]);
    ok(c.cmd(&["SET", "m4", "1"]));
    events(&mut sub, "m4");
    env.db.advance_clock(10);
    assert_eq!(c.cmd(&["HLEN", "h"]), Reply::Int(1));
    ok(c.cmd(&["SET", "m5", "1"]));
    assert_eq!(events(&mut sub, "m5"), ["hexpired:h"]);
    env.db.advance_clock(5000);
    env.db.sweep_expired();
    ok(c.cmd(&["SET", "m6", "1"]));
    assert_eq!(events(&mut sub, "m6"), ["hexpired:h", "del:h"]);
    // In another database the channel says which.
    ok(c.cmd(&["SELECT", "2"]));
    ok(c.cmd(&["SET", "far", "v", "PX", "10"]));
    env.db.advance_clock(10);
    env.db.sweep_expired();
    ok(c.cmd(&["SELECT", "0"]));
    ok(c.cmd(&["SET", "m7", "1"]));
    assert_eq!(
        events(&mut sub, "m7"),
        ["2/set:far", "2/expire:far", "2/expired:far"]
    );
}

#[test]
fn transactions_scripts_and_move_notify_too() {
    let (_env, mut c, mut sub) = notifying("EA");
    ok(c.cmd(&["MULTI"]));
    c.cmd(&["SET", "a", "1"]);
    c.cmd(&["LPUSH", "a", "wrong type"]);
    c.cmd(&["DEL", "a"]);
    c.cmd(&["EXEC"]);
    ok(c.cmd(&["SET", "m1", "1"]));
    assert_eq!(events(&mut sub, "m1"), ["set:a", "del:a"]);
    let script = "redis.call('RPUSH', KEYS[1], 'x'); return redis.call('LPOP', KEYS[1])";
    assert_eq!(c.cmd(&["EVAL", script, "1", "l"]), b("x"));
    ok(c.cmd(&["SET", "m2", "1"]));
    assert_eq!(events(&mut sub, "m2"), ["rpush:l", "lpop:l", "del:l"]);
    ok(c.cmd(&["SET", "mv", "1"]));
    assert_eq!(c.cmd(&["MOVE", "mv", "4"]), Reply::Int(1));
    assert_eq!(c.cmd(&["MOVE", "mv", "4"]), Reply::Int(0));
    ok(c.cmd(&["SET", "m3", "1"]));
    assert_eq!(
        events(&mut sub, "m3"),
        ["set:mv", "move_from:mv", "4/move_to:mv"]
    );
    let lib = "#!lua name=n\nredis.register_function('put', function(k, a) \
               return redis.call('SET', k[1], a[1]) end)";
    assert_eq!(c.cmd(&["FUNCTION", "LOAD", lib]), b("n"));
    ok(c.cmd(&["FCALL", "put", "1", "fk", "v"]));
    ok(c.cmd(&["SET", "m4", "1"]));
    assert_eq!(events(&mut sub, "m4"), ["set:fk"]);
}

// ---- MEMORY USAGE and LCS ----

#[test]
fn memory_usage_grows_with_the_value() {
    let (_db, s) = fresh();
    assert_eq!(s.execute(&["MEMORY", "USAGE", "none"]), Reply::Nil);
    s.execute(&["SET", "small", "x"]);
    s.execute(&["SET", "large", &"x".repeat(10_000)]);
    let small = int(s.execute(&["MEMORY", "USAGE", "small"]));
    let large = int(s.execute(&["MEMORY", "USAGE", "large"]));
    assert!(small > 0 && large >= small + 9_999, "{small} {large}");
    let mut hset = vec!["HSET".to_string(), "h".to_string()];
    for i in 0..1000 {
        hset.push(format!("field{i}"));
        hset.push("v".repeat(50));
    }
    s.execute(&hset);
    for samples in ["0", "1", "5", "100000"] {
        let n = int(s.execute(&["MEMORY", "USAGE", "h", "SAMPLES", samples]));
        assert!((50_000..500_000).contains(&n), "{samples}: {n}");
    }
    s.execute(&["RPUSH", "l", "a", "b"]);
    s.execute(&["SADD", "st", "a", "b"]);
    s.execute(&["ZADD", "z", "1", "a"]);
    s.execute(&["XADD", "x", "1-1", "f", "v"]);
    for key in ["l", "st", "z", "x"] {
        assert!(int(s.execute(&["memory", "usage", key])) > 0, "{key}");
    }
    ok(s.execute(&["MEMORY", "PURGE"]));
    assert!(matches!(s.execute(&["MEMORY", "DOCTOR"]), Reply::Bulk(_)));
    for bad in [
        vec!["MEMORY"],
        vec!["MEMORY", "USAGE"],
        vec!["MEMORY", "USAGE", "h", "SAMPLES"],
        vec!["MEMORY", "USAGE", "h", "SAMPLES", "-1"],
        vec!["MEMORY", "USAGE", "h", "SAMPLES", "x"],
        vec!["MEMORY", "USAGE", "h", "NOPE", "1"],
        vec!["MEMORY", "NOPE"],
        vec!["MEMORY", "PURGE", "x"],
    ] {
        error(s.execute(&bad));
    }
}

#[test]
fn lcs_answers_as_redis_does() {
    let (_db, s) = fresh();
    ok(s.execute(&["MSET", "key1", "ohmytext", "key2", "mynewtext"]));
    assert_eq!(s.execute(&["LCS", "key1", "key2"]), b("mytext"));
    assert_eq!(s.execute(&["LCS", "key1", "key2", "LEN"]), Reply::Int(6));
    let pair = |a, b| ints(&[a, b]);
    assert_eq!(
        s.execute(&["LCS", "key1", "key2", "IDX"]),
        Reply::Array(vec![
            b("matches"),
            Reply::Array(vec![
                Reply::Array(vec![pair(4, 7), pair(5, 8)]),
                Reply::Array(vec![pair(2, 3), pair(0, 1)]),
            ]),
            b("len"),
            Reply::Int(6),
        ])
    );
    assert_eq!(
        s.execute(&[
            "LCS",
            "key1",
            "key2",
            "IDX",
            "MINMATCHLEN",
            "4",
            "WITHMATCHLEN"
        ]),
        Reply::Array(vec![
            b("matches"),
            Reply::Array(vec![Reply::Array(vec![
                pair(4, 7),
                pair(5, 8),
                Reply::Int(4)
            ])]),
            b("len"),
            Reply::Int(6),
        ])
    );
    // Missing keys are empty strings.
    assert_eq!(s.execute(&["LCS", "key1", "none"]), b(""));
    assert_eq!(s.execute(&["LCS", "none", "none2", "LEN"]), Reply::Int(0));
    assert_eq!(s.execute(&["LCS", "key1", "key1"]), b("ohmytext"));
    s.execute(&["RPUSH", "list", "a"]);
    assert_eq!(
        error(s.execute(&["LCS", "key1", "list"])),
        "ERR The specified keys must contain string values"
    );
    assert!(error(s.execute(&["LCS", "key1", "key2", "LEN", "IDX"])).contains("IDX"));
    assert!(error(s.execute(&["LCS", "key1", "key2", "NOPE"])).contains("syntax"));
    assert!(error(s.execute(&["LCS", "key1", "key2", "MINMATCHLEN"])).contains("syntax"));
    assert!(error(s.execute(&["LCS", "key1"])).contains("wrong number"));
    // Too much to compare is refused, not attempted.
    let long = "ab".repeat(4000);
    ok(s.execute(&["MSET", "long1", &long, "long2", &long]));
    assert_eq!(
        error(s.execute(&["LCS", "long1", "long2"])),
        "ERR String too long for LCS"
    );
    // Against brute force on short random strings.
    let mut rng = Rng(0xA076_1D64_78BD_642F);
    for _ in 0..300 {
        let word = |rng: &mut Rng| -> String {
            (0..rng.below(9))
                .map(|_| (b'a' + rng.below(3) as u8) as char)
                .collect()
        };
        let (x, y) = (word(&mut rng), word(&mut rng));
        ok(s.execute(&["MSET", "x", &x, "y", &y]));
        let got = bytes(s.execute(&["LCS", "x", "y"]));
        assert_eq!(got.len(), brute_lcs(x.as_bytes(), y.as_bytes()), "{x} {y}");
        assert!(is_subsequence(&got, x.as_bytes()) && is_subsequence(&got, y.as_bytes()));
        assert_eq!(
            s.execute(&["LCS", "x", "y", "LEN"]),
            Reply::Int(got.len() as i64)
        );
    }
}

fn is_subsequence(small: &[u8], big: &[u8]) -> bool {
    let mut rest = big.iter();
    small.iter().all(|c| rest.any(|d| d == c))
}

fn brute_lcs(a: &[u8], b: &[u8]) -> usize {
    (0u32..1 << a.len())
        .filter_map(|mask| {
            let pick: Vec<u8> = (0..a.len())
                .filter(|i| mask >> i & 1 == 1)
                .map(|i| a[i])
                .collect();
            is_subsequence(&pick, b).then_some(pick.len())
        })
        .max()
        .unwrap_or(0)
}

// ---- FUNCTION and FCALL ----

const LIB: &str = "#!lua name=mylib
local function echo(keys, args) return args[1] end
redis.register_function('echo', echo)
redis.register_function('put', function(keys, args)
  return redis.call('SET', keys[1], args[1])
end)
redis.register_function{
  function_name = 'peek',
  callback = function(keys) return redis.call('GET', keys[1]) end,
  flags = {'no-writes'},
  description = 'reads a key',
}
redis.register_function{
  function_name = 'liar',
  callback = function(keys) return redis.pcall('SET', keys[1], 'x') end,
  flags = {'no-writes'},
}
";

#[test]
fn functions_load_run_and_go() {
    let (_db, s) = fresh();
    assert_eq!(s.execute(&["FUNCTION", "LOAD", LIB]), b("mylib"));
    assert_eq!(s.execute(&["FCALL", "echo", "0", "hello"]), b("hello"));
    assert_eq!(s.execute(&["FCALL", "echo", "0"]), Reply::Nil);
    assert_eq!(
        s.execute(&["FCALL", "put", "1", "k", "v"]),
        Reply::Simple("OK".into())
    );
    assert_eq!(s.execute(&["FCALL", "peek", "1", "k"]), b("v"));
    assert_eq!(s.execute(&["FCALL_RO", "peek", "1", "k"]), b("v"));
    assert_eq!(
        s.execute(&["fcall_ro", "PEEK", "1", "k"]),
        Reply::err("ERR Function not found")
    );
    // A function that may not write cannot, whatever it was called with.
    assert!(error(s.execute(&["FCALL_RO", "put", "1", "k", "w"])).contains("write flag"));
    for call in ["FCALL", "FCALL_RO"] {
        assert!(
            error(s.execute(&[call, "liar", "1", "k"])).contains("read-only"),
            "{call}"
        );
    }
    assert_eq!(s.execute(&["GET", "k"]), b("v"));
    // After a read-only function, scripts write again.
    assert_eq!(
        s.execute(&["EVAL", "return redis.call('INCR', KEYS[1])", "1", "n"]),
        Reply::Int(1)
    );
    for bad in [vec!["FCALL", "nope", "0"], vec!["FCALL", "", "0"]] {
        assert_eq!(s.execute(&bad), Reply::err("ERR Function not found"));
    }
    assert!(error(s.execute(&["FCALL", "echo"])).contains("wrong number"));
    assert!(error(s.execute(&["FCALL", "echo", "x"])).contains("not an integer"));
    assert!(error(s.execute(&["FCALL", "echo", "-1"])).contains("negative"));
    assert!(error(s.execute(&["FCALL", "echo", "2", "k"])).contains("greater"));
    // An error in a function is an error reply, and the next call works.
    let lib = "#!lua name=bad\nredis.register_function('boom', function() error('kaput') end)";
    assert_eq!(s.execute(&["FUNCTION", "LOAD", lib]), b("bad"));
    assert!(error(s.execute(&["FCALL", "boom", "0"])).contains("kaput"));
    assert_eq!(s.execute(&["FCALL", "echo", "0", "still"]), b("still"));

    // LIST and STATS.
    let list = items(s.execute(&["FUNCTION", "LIST"]));
    assert_eq!(list.len(), 2);
    let mylib =
        items(items(s.execute(&["FUNCTION", "LIST", "LIBRARYNAME", "my*", "WITHCODE"]))[0].clone());
    assert_eq!(
        mylib[..4],
        [b("library_name"), b("mylib"), b("engine"), b("LUA")]
    );
    let functions = items(mylib[5].clone());
    assert_eq!(functions.len(), 4);
    let peek = functions
        .iter()
        .map(|f| items(f.clone()))
        .find(|f| f[1] == b("peek"))
        .unwrap();
    assert_eq!(peek[3], b("reads a key"));
    assert_eq!(peek[5], Reply::Array(vec![b("no-writes")]));
    assert_eq!(mylib[6..], [b("library_code"), b(LIB)]);
    assert_eq!(
        s.execute(&["FUNCTION", "LIST", "LIBRARYNAME", "zz*"]),
        Reply::Array(Vec::new())
    );
    assert!(error(s.execute(&["FUNCTION", "LIST", "NOPE"])).contains("syntax"));
    let stats = items(s.execute(&["FUNCTION", "STATS"]));
    assert_eq!(stats[1], Reply::Nil);
    let lua = items(items(stats[3].clone())[1].clone());
    assert_eq!(
        lua,
        [
            b("libraries_count"),
            Reply::Int(2),
            b("functions_count"),
            Reply::Int(5)
        ]
    );

    // DELETE and FLUSH.
    ok(s.execute(&["FUNCTION", "DELETE", "bad"]));
    assert_eq!(
        s.execute(&["FCALL", "boom", "0"]),
        Reply::err("ERR Function not found")
    );
    assert_eq!(
        s.execute(&["FUNCTION", "DELETE", "bad"]),
        Reply::err("ERR Library not found")
    );
    ok(s.execute(&["FUNCTION", "FLUSH", "SYNC"]));
    assert_eq!(
        s.execute(&["FCALL", "echo", "0", "x"]),
        Reply::err("ERR Function not found")
    );
    assert_eq!(s.execute(&["FUNCTION", "LIST"]), Reply::Array(Vec::new()));
    assert!(error(s.execute(&["FUNCTION", "FLUSH", "NOW"])).contains("SYNC"));
    assert!(error(s.execute(&["FUNCTION", "NOPE"])).contains("unknown subcommand"));
    assert!(error(s.execute(&["FUNCTION"])).contains("wrong number"));
    assert!(error(s.execute(&["FUNCTION", "DUMP"])).contains("not supported"));
    assert!(error(s.execute(&["FUNCTION", "KILL"])).starts_with("NOTBUSY"));
    // The name is free again.
    assert_eq!(s.execute(&["FUNCTION", "LOAD", LIB]), b("mylib"));
}

#[test]
fn a_library_that_is_wrong_changes_nothing() {
    let (_db, s) = fresh();
    assert_eq!(s.execute(&["FUNCTION", "LOAD", LIB]), b("mylib"));
    let f = "redis.register_function('f', function() return 1 end)";
    let cases: Vec<(String, &str)> =
        vec![
        (f.to_string(), "Missing library metadata"),
        (format!("#!lua\n{f}"), "name was not given"),
        (format!("#!js name=x\n{f}"), "Engine 'js' not found"),
        (format!("#!lua name=x y=1\n{f}"), "Invalid metadata"),
        (format!("#!lua name=x name=y\n{f}"), "Invalid metadata"),
        (format!("#!lua name=bad-name\n{f}"), "Library names"),
        (format!("#!lua name=\n{f}"), "Library names"),
        ("#!lua name=x\n".to_string(), "No functions registered"),
        ("#!lua name=x\nreturn 1".to_string(), "No functions registered"),
        ("#!lua name=x\nthis is not lua".to_string(), "Error registering"),
        ("#!lua name=x\nerror('no')".to_string(), "Error registering"),
        (format!("#!lua name=x\n{f}\n{f}"), "already exists in the library"),
        (
            "#!lua name=x\nredis.register_function('bad name', function() end)".to_string(),
            "Function names",
        ),
        (
            "#!lua name=x\nredis.register_function('', function() end)".to_string(),
            "Function names",
        ),
        ("#!lua name=x\nredis.register_function('f')".to_string(), "Error registering"),
        ("#!lua name=x\nredis.register_function('f', 1)".to_string(), "Error registering"),
        ("#!lua name=x\nredis.register_function()".to_string(), "Error registering"),
        (
            "#!lua name=x\nredis.register_function{function_name='f'}".to_string(),
            "Error registering",
        ),
        (
            "#!lua name=x\nredis.register_function{function_name='f', callback=function() end, \
             flags={'fly'}}"
                .to_string(),
            "unknown flag",
        ),
        (
            "#!lua name=x\nredis.register_function{function_name='f', callback=function() end, \
             extra=1}"
                .to_string(),
            "unknown argument",
        ),
        // Another library's function name is taken.
        (
            "#!lua name=x\nredis.register_function('echo', function() end)".to_string(),
            "Function echo already exists",
        ),
        // A library may not run commands or leave globals behind as it loads.
        (
            format!("#!lua name=x\nredis.call('SET', 'loaded', '1')\n{f}"),
            "Error registering",
        ),
        (format!("#!lua name=x\nleak = 1\n{f}"), "global variable"),
        (
            format!("#!lua name=x\n{f}\nerror('after registering')"),
            "after registering",
        ),
        (LIB.to_string(), "Library 'mylib' already exists"),
    ];
    for (code, want) in &cases {
        let got = error(s.execute(&["FUNCTION", "LOAD", code]));
        assert!(got.contains(want), "{code:?}: {got}");
    }
    assert!(error(s.execute(&["FUNCTION", "LOAD"])).contains("wrong number"));
    assert!(error(s.execute(&["FUNCTION", "LOAD", "REPLACEX", LIB])).contains("wrong number"));
    // Nothing of the failed loads is there.
    assert_eq!(items(s.execute(&["FUNCTION", "LIST"])).len(), 1);
    assert_eq!(
        s.execute(&["FCALL", "f", "0"]),
        Reply::err("ERR Function not found")
    );
    assert_eq!(s.execute(&["EXISTS", "loaded"]), Reply::Int(0));
    assert_eq!(s.execute(&["FCALL", "echo", "0", "ok"]), b("ok"));

    // REPLACE swaps the whole library: functions it no longer has are gone.
    let v2 = "#!lua name=mylib\nredis.register_function('echo', function(k, a) \
              return 'v2:' .. a[1] end)";
    assert_eq!(s.execute(&["FUNCTION", "LOAD", "replace", v2]), b("mylib"));
    assert_eq!(s.execute(&["FCALL", "echo", "0", "x"]), b("v2:x"));
    assert_eq!(
        s.execute(&["FCALL", "put", "1", "k", "v"]),
        Reply::err("ERR Function not found")
    );
    // A REPLACE that fails leaves the old library working.
    let broken = "#!lua name=mylib\nredis.register_function('echo', function() end)\nerror('x')";
    error(s.execute(&["FUNCTION", "LOAD", "REPLACE", broken]));
    assert_eq!(s.execute(&["FCALL", "echo", "0", "x"]), b("v2:x"));

    // Outside FUNCTION LOAD nothing can be registered.
    let sneak = "redis.register_function('sneak', function() return 1 end)";
    assert!(error(s.execute(&["EVAL", sneak, "0"])).contains("FUNCTION LOAD"));
    let inner = "#!lua name=inner\nredis.register_function('outer', function()\n\
                 redis.register_function('late', function() return 1 end)\nend)";
    assert_eq!(s.execute(&["FUNCTION", "LOAD", inner]), b("inner"));
    assert!(error(s.execute(&["FCALL", "outer", "0"])).contains("FUNCTION LOAD"));
    assert_eq!(
        s.execute(&["FCALL", "late", "0"]),
        Reply::err("ERR Function not found")
    );
    // A function sees the sandbox scripts see: no globals to set.
    let global = "#!lua name=g\nredis.register_function('g', function() leak = 1 end)";
    assert_eq!(s.execute(&["FUNCTION", "LOAD", global]), b("g"));
    assert!(error(s.execute(&["FCALL", "g", "0"])).contains("global variable"));
    // Functions are not scripts: SCRIPT FLUSH leaves them.
    ok(s.execute(&["SCRIPT", "FLUSH"]));
    assert_eq!(s.execute(&["FCALL", "echo", "0", "x"]), b("v2:x"));
}

#[test]
fn a_function_may_do_what_its_caller_may() {
    let env = env();
    let mut admin = env.client();
    assert_eq!(admin.cmd(&["FUNCTION", "LOAD", LIB]), b("mylib"));
    ok(admin.cmd(&["SET", "other:1", "secret"]));
    ok(admin.cmd(&[
        "ACL",
        "SETUSER",
        "u",
        "on",
        ">pw",
        "~mine:*",
        "+fcall",
        "+fcall_ro",
        "+get",
    ]));
    let mut user = env.client();
    ok(user.cmd(&["AUTH", "u", "pw"]));
    // Keys it names must be its keys.
    assert!(error(user.cmd(&["FCALL", "peek", "1", "other:1"])).starts_with("NOPERM"));
    assert!(error(user.cmd(&["FCALL_RO", "peek", "1", "other:1"])).starts_with("NOPERM"));
    assert_eq!(user.cmd(&["FCALL", "peek", "1", "mine:1"]), Reply::Nil);
    // Commands inside run as the caller: no SET for this one.
    assert!(error(user.cmd(&["FCALL", "put", "1", "mine:1", "v"])).contains("NOPERM"));
    assert_eq!(admin.cmd(&["EXISTS", "mine:1"]), Reply::Int(0));
    // Managing libraries is its own permission.
    assert!(error(user.cmd(&["FUNCTION", "LIST"])).starts_with("NOPERM"));
    assert!(error(user.cmd(&["FUNCTION", "FLUSH"])).starts_with("NOPERM"));
    ok(admin.cmd(&["ACL", "SETUSER", "u", "+set"]));
    ok(user.cmd(&["FCALL", "put", "1", "mine:1", "v"]));
    assert_eq!(user.cmd(&["FCALL", "peek", "1", "mine:1"]), b("v"));
    // The new commands are in the table ACL checks against.
    ok(admin.cmd(&[
        "ACL",
        "SETUSER",
        "h",
        "on",
        "nopass",
        "~*",
        "+@hyperloglog",
        "+@hash",
    ]));
    let mut h = env.client();
    ok(h.cmd(&["AUTH", "h", "x"]));
    assert_eq!(h.cmd(&["PFADD", "p", "a"]), Reply::Int(1));
    assert_eq!(h.cmd(&["PFCOUNT", "p"]), Reply::Int(1));
    h.cmd(&["HSET", "hh", "f", "v"]);
    assert_eq!(
        h.cmd(&["HEXPIRE", "hh", "10", "FIELDS", "1", "f"]),
        ints(&[1])
    );
    for denied in [
        vec!["DUMP", "p"],
        vec!["MEMORY", "USAGE", "p"],
        vec!["LCS", "p", "p"],
        vec!["FCALL", "echo", "0"],
    ] {
        assert!(error(h.cmd(&denied)).starts_with("NOPERM"), "{denied:?}");
    }
    let info = items(admin.cmd(&["COMMAND", "INFO", "hexpire", "pfmerge", "restore", "fcall"]));
    assert!(
        info.iter().all(|i| matches!(i, Reply::Array(_))),
        "{info:?}"
    );
    assert!(int(admin.cmd(&["COMMAND", "COUNT"])) >= 195);
}

// ---- found by review ----

#[test]
fn a_library_sees_nothing_of_scripts_and_cannot_change_on_reload() {
    let db = Db::open(
        None,
        hotstash::Config {
            lua_memory_limit: 8 << 20,
            ..engine_cfg(4)
        },
    )
    .unwrap();
    let s = db.session().unwrap();
    // What a script was called with is gone when it ends.
    s.execute(&["EVAL", "return 1", "1", "secretkey", "secretarg"]);
    let peek = "#!lua name=peek\nerror(tostring(KEYS and KEYS[1]) .. tostring(ARGV and ARGV[1]))";
    let got = error(s.execute(&["FUNCTION", "LOAD", peek]));
    assert!(!got.contains("secret"), "{got}");
    let lib = "#!lua name=seer\nredis.register_function('seer', function() \
               return tostring(KEYS and KEYS[1]) .. tostring(ARGV and ARGV[1]) end)";
    assert_eq!(s.execute(&["FUNCTION", "LOAD", lib]), b("seer"));
    s.execute(&["EVAL", "return 1", "1", "secretkey", "secretarg"]);
    // The globals are not there at all: reading one is an error.
    let got = error(s.execute(&["FCALL", "seer", "0"]));
    assert!(
        got.contains("global variable") && !got.contains("secret"),
        "{got}"
    );

    // A library that registers something else the second time it runs is
    // refused, not given another library's name.
    let victim = "#!lua name=victim\nredis.register_function('victimfn', function() \
                  return 'victim' end)";
    assert_eq!(s.execute(&["FUNCTION", "LOAD", victim]), b("victim"));
    let evil = "#!lua name=evil\nredis.register_function('mine', function() return 'mine' end)\n\
                local ok, a = pcall(function() return ARGV[1] end)\n\
                if ok and a == 'go' then redis.register_function('victimfn', function() \
                return 'stolen' end) end";
    assert_eq!(s.execute(&["FUNCTION", "LOAD", evil]), b("evil"));
    // Running out of memory throws the Lua state away.
    error(s.execute(&["EVAL", "local s = 'x' while true do s = s .. s end", "0"]));
    s.execute(&["EVAL", "return 1", "0", "go"]);
    s.execute(&["FCALL", "mine", "0"]);
    assert_eq!(s.execute(&["FCALL", "victimfn", "0"]), b("victim"));
}

#[test]
fn a_restored_field_deadline_beyond_the_clock_is_harmless() {
    let (_db, s) = fresh();
    // A field deadline no clock reaches is not a deadline.
    let mut body = vec![0xe1, 1, 0, 0, 0, 1, 0, 0, 0, b'f', 1, 0, 0, 0, b'v'];
    body.extend_from_slice(&u64::MAX.to_le_bytes());
    match restore(&s, "late", "0", &rdb::seal(body), &[]) {
        Reply::Error(e) => assert_eq!(e, "ERR Bad data format"),
        Reply::Ok => {
            for cmd in ["HEXPIRETIME", "HPEXPIRETIME", "HTTL", "HPTTL"] {
                let at = items(s.execute(&[cmd, "late", "FIELDS", "1", "f"]));
                assert!(int(at[0].clone()) > 0, "{cmd}: {at:?}");
            }
        }
        other => panic!("{other:?}"),
    }
}
