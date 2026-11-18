//! SELECT and the commands that tell databases apart: every database is
//! its own keyspace, and the few commands that cross between them say so.

mod net;

use hotstash::{Config, Db, PersistOptions, Reply, Session};
use net::*;
use std::collections::BTreeSet;
use std::time::Duration;

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn fresh() -> (Db, Session) {
    let db = Db::open(None, engine_cfg(4)).unwrap();
    let s = db.session().unwrap();
    (db, s)
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

fn names(reply: Reply) -> BTreeSet<String> {
    match reply {
        Reply::Array(items) => items
            .into_iter()
            .map(|item| match item {
                Reply::Bulk(b) => String::from_utf8(b).unwrap(),
                other => panic!("{other:?}"),
            })
            .collect(),
        other => panic!("{other:?}"),
    }
}

/// Every key SCAN returns, walking the cursor to the end.
fn scan_all(run: &mut dyn FnMut(&[&str]) -> Reply, count: &str) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut seen = Vec::new();
    for _ in 0..100_000 {
        let Reply::Array(mut parts) = run(&["SCAN", &cursor, "COUNT", count]) else {
            panic!("not an array");
        };
        seen.extend(names(parts.pop().unwrap()));
        let Some(Reply::Bulk(next)) = parts.pop() else {
            panic!("no cursor");
        };
        cursor = String::from_utf8(next).unwrap();
        if cursor == "0" {
            return seen;
        }
    }
    panic!("SCAN never came back to 0");
}

#[test]
fn select_checks_its_argument() {
    let (_db, s) = fresh();
    ok(s.execute(&["SELECT", "0"]));
    ok(s.execute(&["SELECT", "15"]));
    for bad in ["16", "-1", "9223372036854775807", "-9223372036854775808"] {
        assert_eq!(
            s.execute(&["SELECT", bad]),
            Reply::err("ERR DB index is out of range"),
            "{bad}"
        );
    }
    for bad in ["x", "", "1.0", " 1", "99999999999999999999"] {
        assert_eq!(
            s.execute(&["SELECT", bad]),
            Reply::err("ERR invalid DB index"),
            "{bad:?}"
        );
    }
    assert_eq!(
        s.execute(&["SELECT"]),
        Reply::err("ERR wrong number of arguments for 'select' command")
    );
    // A refused SELECT leaves the connection where it was.
    s.execute(&["SET", "here", "15"]);
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["GET", "here"]), Reply::Nil);
    ok(s.execute(&["SELECT", "15"]));
    assert_eq!(s.execute(&["GET", "here"]), Reply::bulk("15"));
}

#[test]
fn each_database_is_its_own_keyspace() {
    let (db, s) = fresh();
    // The same names in three databases, as different types.
    ok(s.execute(&["SET", "k", "zero"]));
    s.execute(&["RPUSH", "shared", "a", "b"]);
    ok(s.execute(&["SELECT", "1"]));
    assert_eq!(s.execute(&["GET", "k"]), Reply::Nil);
    assert_eq!(s.execute(&["EXISTS", "k", "shared"]), Reply::Int(0));
    s.execute(&["HSET", "k", "f", "one"]);
    s.execute(&["SADD", "shared", "m"]);
    s.execute(&["ZADD", "z", "1", "a"]);
    s.execute(&["XADD", "x", "1-1", "f", "v"]);
    s.execute(&["SETBIT", "bits", "7", "1"]);
    ok(s.execute(&["SELECT", "2"]));
    ok(s.execute(&["MSET", "k", "two", "other", "2"]));

    let types = |s: &Session, key: &str| s.execute(&["TYPE", key]);
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(types(&s, "k"), Reply::Simple("string".into()));
    assert_eq!(types(&s, "shared"), Reply::Simple("list".into()));
    assert_eq!(types(&s, "z"), Reply::Simple("none".into()));
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(2));
    assert_eq!(
        names(s.execute(&["KEYS", "*"])),
        ["k", "shared"].map(String::from).into()
    );
    ok(s.execute(&["SELECT", "1"]));
    assert_eq!(types(&s, "k"), Reply::Simple("hash".into()));
    assert_eq!(types(&s, "shared"), Reply::Simple("set".into()));
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(5));
    assert_eq!(s.execute(&["XLEN", "x"]), Reply::Int(1));
    ok(s.execute(&["SELECT", "2"]));
    assert_eq!(
        s.execute(&["MGET", "k", "other", "shared"]),
        Reply::Array(vec![Reply::bulk("two"), Reply::bulk("2"), Reply::Nil])
    );
    // Multi-key commands stay inside the database.
    assert_eq!(s.execute(&["DEL", "k", "shared", "z"]), Reply::Int(1));
    ok(s.execute(&["RENAME", "other", "k"]));
    assert_eq!(s.execute(&["COPY", "k", "copy"]), Reply::Int(1));
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["GET", "k"]), Reply::bulk("zero"));
    assert_eq!(s.execute(&["EXISTS", "copy", "other"]), Reply::Int(0));
    ok(s.execute(&["SELECT", "1"]));
    assert_eq!(s.execute(&["HGET", "k", "f"]), Reply::bulk("one"));

    // RANDOMKEY never leaves the database either, however often it is asked.
    ok(s.execute(&["SELECT", "3"]));
    assert_eq!(s.execute(&["RANDOMKEY"]), Reply::Nil);
    s.execute(&["SET", "only", "1"]);
    for _ in 0..200 {
        assert_eq!(s.execute(&["RANDOMKEY"]), Reply::bulk("only"));
    }
    ok(s.execute(&["SELECT", "4"]));
    assert_eq!(s.execute(&["RANDOMKEY"]), Reply::Nil);
    assert_eq!(s.execute(&["KEYS", "*"]), Reply::Array(vec![]));
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));

    // A second session starts in database 0 whatever the first one chose.
    let other = db.session().unwrap();
    assert_eq!(other.execute(&["GET", "k"]), Reply::bulk("zero"));
}

#[test]
fn scan_and_keys_cover_one_database_exactly() {
    let (_db, s) = fresh();
    for db in [0, 7, 15] {
        ok(s.execute(&["SELECT", &db.to_string()]));
        for i in 0..(300 + db * 10) {
            s.execute(&["SET", &format!("db{db}:{i}"), "v"]);
        }
    }
    for db in [0usize, 7, 15] {
        ok(s.execute(&["SELECT", &db.to_string()]));
        let want: BTreeSet<String> = (0..(300 + db * 10))
            .map(|i| format!("db{db}:{i}"))
            .collect();
        for count in ["1", "7", "1000"] {
            let seen = scan_all(&mut |args| s.execute(args), count);
            assert_eq!(seen.len(), want.len(), "db {db} count {count}: duplicates");
            assert_eq!(seen.into_iter().collect::<BTreeSet<_>>(), want);
        }
        assert_eq!(names(s.execute(&["KEYS", "*"])), want);
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(want.len() as i64));
    }
    // A cursor from one database, used in an empty one, ends at once
    // rather than wandering into a neighbour.
    ok(s.execute(&["SELECT", "0"]));
    let Reply::Array(parts) = s.execute(&["SCAN", "0", "COUNT", "5"]) else {
        panic!("not an array");
    };
    let Reply::Bulk(cursor) = &parts[0] else {
        panic!("no cursor");
    };
    let cursor = String::from_utf8(cursor.clone()).unwrap();
    assert_ne!(cursor, "0");
    ok(s.execute(&["SELECT", "8"]));
    let seen = {
        let mut first = Some(cursor);
        scan_all(
            &mut |args| match first.take() {
                Some(c) => s.execute(&["SCAN", &c, "COUNT", "5"]),
                None => s.execute(args),
            },
            "5",
        )
    };
    assert!(seen.is_empty(), "{seen:?}");
    for wild in ["65535", "65536", "4294967296", "4294967295"] {
        let seen = {
            let mut first = Some(wild.to_string());
            scan_all(
                &mut |args| match first.take() {
                    Some(c) => s.execute(&["SCAN", &c]),
                    None => s.execute(args),
                },
                "10",
            )
        };
        assert!(seen.is_empty(), "{wild}: {seen:?}");
    }
}

#[test]
fn flushdb_empties_one_database_and_flushall_every_one() {
    let (_db, s) = fresh();
    for db in 0..4 {
        ok(s.execute(&["SELECT", &db.to_string()]));
        for i in 0..50 {
            s.execute(&["SET", &format!("k{i}"), "v"]);
        }
    }
    ok(s.execute(&["SELECT", "2"]));
    ok(s.execute(&["FLUSHDB"]));
    for (db, want) in [(0, 50), (1, 50), (2, 0), (3, 50)] {
        ok(s.execute(&["SELECT", &db.to_string()]));
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(want), "db {db}");
        assert_eq!(names(s.execute(&["KEYS", "*"])).len(), want as usize);
    }
    // Inside a transaction it still means the transaction's database.
    ok(s.execute(&["SELECT", "1"]));
    ok(s.execute(&["MULTI"]));
    s.execute(&["FLUSHDB"]);
    s.execute(&["SET", "after", "1"]);
    assert_eq!(
        s.execute(&["EXEC"]),
        Reply::Array(vec![Reply::Ok, Reply::Ok])
    );
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(1));
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(50));
    ok(s.execute(&["SELECT", "3"]));
    ok(s.execute(&["FLUSHALL"]));
    for db in 0..16 {
        ok(s.execute(&["SELECT", &db.to_string()]));
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0), "db {db}");
    }
}

#[test]
fn move_takes_a_key_to_another_database() {
    let (db, s) = fresh();
    s.execute(&["RPUSH", "list", "a", "b", "c"]);
    s.execute(&["SET", "timed", "v", "PX", "50000"]);
    s.execute(&["SET", "taken", "from-0"]);
    ok(s.execute(&["SELECT", "5"]));
    s.execute(&["SET", "taken", "in-5"]);
    ok(s.execute(&["SELECT", "0"]));

    assert_eq!(s.execute(&["MOVE", "nothing", "5"]), Reply::Int(0));
    assert_eq!(s.execute(&["MOVE", "taken", "5"]), Reply::Int(0));
    assert_eq!(s.execute(&["GET", "taken"]), Reply::bulk("from-0"));
    assert_eq!(s.execute(&["MOVE", "list", "5"]), Reply::Int(1));
    assert_eq!(s.execute(&["MOVE", "list", "5"]), Reply::Int(0));
    assert_eq!(s.execute(&["MOVE", "timed", "5"]), Reply::Int(1));
    assert_eq!(s.execute(&["EXISTS", "list", "timed"]), Reply::Int(0));
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(1));
    for (args, want) in [
        (
            &["MOVE", "taken", "0"][..],
            "ERR source and destination objects are the same",
        ),
        (&["MOVE", "taken", "16"], "ERR DB index is out of range"),
        (&["MOVE", "taken", "-1"], "ERR DB index is out of range"),
        (
            &["MOVE", "taken", "x"],
            "ERR value is not an integer or out of range",
        ),
        (
            &["MOVE", "taken"],
            "ERR wrong number of arguments for 'move' command",
        ),
    ] {
        assert_eq!(s.execute(args), Reply::err(want), "{args:?}");
    }
    ok(s.execute(&["SELECT", "5"]));
    assert_eq!(
        s.execute(&["LRANGE", "list", "0", "-1"]),
        Reply::Array(vec![Reply::bulk("a"), Reply::bulk("b"), Reply::bulk("c")])
    );
    assert_eq!(s.execute(&["GET", "taken"]), Reply::bulk("in-5"));
    // The deadline travels with the key.
    let Reply::Int(left) = s.execute(&["PTTL", "timed"]) else {
        panic!("no ttl");
    };
    assert!((1..=50_000).contains(&left), "{left}");
    // An expired key is not there to be moved, nor in the way.
    s.execute(&["SET", "gone", "v", "PX", "10"]);
    ok(s.execute(&["SELECT", "0"]));
    s.execute(&["SET", "gone", "mine"]);
    db.advance_clock(100);
    assert_eq!(s.execute(&["MOVE", "gone", "5"]), Reply::Int(1));
    ok(s.execute(&["SELECT", "5"]));
    assert_eq!(s.execute(&["GET", "gone"]), Reply::bulk("mine"));
    assert_eq!(s.execute(&["PTTL", "gone"]), Reply::Int(-1));
}

#[test]
fn copy_reaches_another_database_with_db() {
    let (_db, s) = fresh();
    s.execute(&["HSET", "h", "f", "v"]);
    assert_eq!(s.execute(&["COPY", "h", "h", "DB", "3"]), Reply::Int(1));
    assert_eq!(s.execute(&["COPY", "h", "h", "DB", "3"]), Reply::Int(0));
    assert_eq!(
        s.execute(&["COPY", "h", "h", "DB", "0"]),
        Reply::err("ERR source and destination objects are the same")
    );
    assert_eq!(
        s.execute(&["COPY", "h", "h"]),
        Reply::err("ERR source and destination objects are the same")
    );
    assert_eq!(
        s.execute(&["COPY", "nothing", "x", "DB", "3"]),
        Reply::Int(0)
    );
    s.execute(&["HSET", "h", "f", "changed"]);
    assert_eq!(
        s.execute(&["COPY", "h", "h", "DB", "3", "REPLACE"]),
        Reply::Int(1)
    );
    // The copy is a value of its own.
    s.execute(&["HSET", "h", "f", "later"]);
    ok(s.execute(&["SELECT", "3"]));
    assert_eq!(s.execute(&["HGET", "h", "f"]), Reply::bulk("changed"));
    assert_eq!(s.execute(&["COPY", "h", "back", "DB", "0"]), Reply::Int(1));
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["HGET", "back", "f"]), Reply::bulk("changed"));
    assert_eq!(s.execute(&["HGET", "h", "f"]), Reply::bulk("later"));
    // In a transaction, from the database chosen inside it.
    ok(s.execute(&["MULTI"]));
    s.execute(&["SELECT", "3"]);
    s.execute(&["COPY", "h", "from3", "DB", "9"]);
    assert_eq!(
        s.execute(&["EXEC"]),
        Reply::Array(vec![Reply::Ok, Reply::Int(1)])
    );
    ok(s.execute(&["SELECT", "9"]));
    assert_eq!(s.execute(&["HGET", "from3", "f"]), Reply::bulk("changed"));
}

#[test]
fn a_transaction_follows_the_selects_inside_it() {
    let (db, s) = fresh();
    ok(s.execute(&["SELECT", "1"]));
    ok(s.execute(&["MULTI"]));
    let queued = Reply::Simple("QUEUED".into());
    assert_eq!(s.execute(&["SET", "a", "in-1"]), queued);
    assert_eq!(s.execute(&["SELECT", "2"]), queued);
    assert_eq!(s.execute(&["SET", "a", "in-2"]), queued);
    assert_eq!(s.execute(&["DBSIZE"]), queued);
    assert_eq!(s.execute(&["SELECT", "0"]), queued);
    assert_eq!(s.execute(&["INCR", "n"]), queued);
    assert_eq!(s.execute(&["SELECT", "2"]), queued);
    assert_eq!(s.execute(&["KEYS", "*"]), queued);
    assert_eq!(
        s.execute(&["EXEC"]),
        Reply::Array(vec![
            Reply::Ok,
            Reply::Ok,
            Reply::Ok,
            Reply::Int(1),
            Reply::Ok,
            Reply::Int(1),
            Reply::Ok,
            Reply::Array(vec![Reply::bulk("a")]),
        ])
    );
    // The connection ends up where the last SELECT left it.
    assert_eq!(s.execute(&["GET", "a"]), Reply::bulk("in-2"));
    ok(s.execute(&["SELECT", "1"]));
    assert_eq!(s.execute(&["GET", "a"]), Reply::bulk("in-1"));
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["GET", "n"]), Reply::bulk("1"));

    // DISCARD forgets the SELECT along with the rest.
    ok(s.execute(&["SELECT", "1"]));
    ok(s.execute(&["MULTI"]));
    s.execute(&["SELECT", "2"]);
    s.execute(&["SET", "a", "never"]);
    ok(s.execute(&["DISCARD"]));
    assert_eq!(s.execute(&["GET", "a"]), Reply::bulk("in-1"));
    // A SELECT that cannot work aborts the transaction and moves nothing.
    ok(s.execute(&["MULTI"]));
    s.execute(&["SET", "a", "never"]);
    assert_eq!(
        s.execute(&["SELECT", "16"]),
        Reply::err("ERR DB index is out of range")
    );
    assert_eq!(
        s.execute(&["EXEC"]),
        Reply::err("EXECABORT Transaction discarded because of previous errors.")
    );
    assert_eq!(s.execute(&["GET", "a"]), Reply::bulk("in-1"));

    // WATCH belongs to the database it was given in.
    let other = db.session().unwrap();
    ok(other.execute(&["SELECT", "1"]));
    ok(s.execute(&["WATCH", "a"]));
    ok(s.execute(&["SELECT", "6"]));
    ok(s.execute(&["MULTI"]));
    s.execute(&["SET", "b", "1"]);
    other.execute(&["SET", "a", "changed"]);
    assert_eq!(s.execute(&["EXEC"]), Reply::NilArray);
    assert_eq!(s.execute(&["GET", "b"]), Reply::Nil);
    // A watched key that expires in its own database counts as changed,
    // even when the transaction runs in another one.
    ok(s.execute(&["SELECT", "1"]));
    s.execute(&["SET", "short", "v", "PX", "10"]);
    ok(s.execute(&["WATCH", "short"]));
    ok(s.execute(&["SELECT", "6"]));
    db.advance_clock(100);
    ok(s.execute(&["MULTI"]));
    s.execute(&["SET", "b", "1"]);
    assert_eq!(s.execute(&["EXEC"]), Reply::NilArray);
}

#[test]
fn scripts_run_in_the_callers_database() {
    let (_db, s) = fresh();
    s.execute(&["SET", "k", "zero"]);
    ok(s.execute(&["SELECT", "4"]));
    s.execute(&["SET", "k", "four"]);
    let script = "redis.call('SET', KEYS[2], ARGV[1]) \
                  return {redis.call('GET', KEYS[1]), redis.call('DBSIZE'), \
                          #redis.call('KEYS', '*')}";
    assert_eq!(
        s.execute(&["EVAL", script, "2", "k", "made", "x"]),
        Reply::Array(vec![Reply::bulk("four"), Reply::Int(2), Reply::Int(2)])
    );
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["EXISTS", "made"]), Reply::Int(0));
    assert_eq!(
        s.execute(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]),
        Reply::bulk("zero")
    );
    // A script cannot change the database behind its caller's back.
    for call in [
        "return redis.pcall('SELECT', '1')",
        "return redis.pcall('MOVE', 'k', '1')",
        "return redis.pcall('FLUSHDB')",
        "return redis.pcall('COPY', 'k', 'k2', 'DB', '1')",
    ] {
        assert_eq!(
            s.execute(&["EVAL", call, "0"]),
            Reply::err("ERR This Redis command is not allowed from script"),
            "{call}"
        );
    }
    assert_eq!(s.execute(&["GET", "k"]), Reply::bulk("zero"));
    // Queued in a transaction after a SELECT, a script follows it.
    ok(s.execute(&["MULTI"]));
    s.execute(&["SELECT", "4"]);
    s.execute(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]);
    assert_eq!(
        s.execute(&["EXEC"]),
        Reply::Array(vec![Reply::Ok, Reply::bulk("four")])
    );
}

#[test]
fn expiry_and_sweeping_reach_every_database() {
    let (db, s) = fresh();
    for n in [0, 9, 15] {
        ok(s.execute(&["SELECT", &n.to_string()]));
        for i in 0..40 {
            s.execute(&["SET", &format!("t{i}"), "v", "PX", "50"]);
        }
        s.execute(&["SET", "stays", "v"]);
    }
    db.advance_clock(1000);
    assert_eq!(db.sweep_expired(), 120);
    for n in [0, 9, 15] {
        ok(s.execute(&["SELECT", &n.to_string()]));
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(1), "db {n}");
    }
    // The background tick gets to the last database too.
    ok(s.execute(&["SELECT", "15"]));
    for i in 0..40 {
        s.execute(&["SET", &format!("t{i}"), "v", "PX", "50"]);
    }
    db.advance_clock(1000);
    assert!(wait_until(10, || {
        db.advance_clock(1000);
        db.expire_tick();
        s.execute(&["DBSIZE"]) == Reply::Int(1)
    }));
}

#[test]
fn connections_keep_their_own_database_over_tcp() {
    let env = env();
    let mut a = env.client();
    let mut b = env.client();
    assert_eq!(a.cmd(&["SELECT", "3"]), Reply::Ok);
    a.cmd(&["SET", "k", "a-in-3"]);
    assert_eq!(b.cmd(&["GET", "k"]), Reply::Nil);
    b.cmd(&["SET", "k", "b-in-0"]);
    assert_eq!(a.cmd(&["GET", "k"]), Reply::bulk("a-in-3"));
    assert_eq!(a.cmd(&["DBSIZE"]), Reply::Int(1));
    assert_eq!(a.cmd(&["KEYS", "*"]), Reply::Array(vec![Reply::bulk("k")]));
    let info = |c: &mut Client| match c.cmd(&["CLIENT", "INFO"]) {
        Reply::Bulk(b) => String::from_utf8(b).unwrap(),
        other => panic!("{other:?}"),
    };
    assert!(info(&mut a).contains(" db=3 "), "{}", info(&mut a));
    assert!(info(&mut b).contains(" db=0 "));
    let Reply::Bulk(list) = b.cmd(&["CLIENT", "LIST"]) else {
        panic!("no list");
    };
    let list = String::from_utf8(list).unwrap();
    assert_eq!(list.matches(" db=3 ").count(), 1, "{list}");

    // A pipeline that hops between databases, sent in one write.
    let mut wire = Vec::new();
    for i in 0..300 {
        let db = (i % 5).to_string();
        wire.extend(wire_str(&["SELECT", &db]));
        wire.extend(wire_str(&["RPUSH", "hops", &i.to_string()]));
    }
    a.send_raw(&wire);
    for i in 0..300 {
        assert_eq!(a.read(), Reply::Ok);
        assert_eq!(a.read(), Reply::Int(i / 5 + 1), "push {i}");
    }
    for db in 0..5 {
        b.cmd(&["SELECT", &db.to_string()]);
        assert_eq!(b.cmd(&["LLEN", "hops"]), Reply::Int(60), "db {db}");
        assert_eq!(
            b.cmd(&["LINDEX", "hops", "1"]),
            Reply::bulk((db + 5).to_string())
        );
    }

    // INFO names the databases that hold keys.
    let Reply::Bulk(text) = b.cmd(&["INFO"]) else {
        panic!("no info");
    };
    let text = String::from_utf8(text).unwrap();
    let keyspace = &text[text.find("# Keyspace").unwrap()..];
    assert!(keyspace.contains("db0:keys=2,"), "{keyspace}");
    assert!(keyspace.contains("db3:keys=2,"), "{keyspace}");
    assert!(keyspace.contains("db4:keys=1,"), "{keyspace}");
    assert!(!keyspace.contains("db5:"), "{keyspace}");

    // RESET goes back to database 0; HELLO stays.
    a.cmd(&["SELECT", "3"]);
    a.cmd(&["HELLO", "2"]);
    assert_eq!(a.cmd(&["GET", "k"]), Reply::bulk("a-in-3"));
    assert_eq!(a.cmd(&["RESET"]), Reply::Simple("RESET".into()));
    assert_eq!(a.cmd(&["GET", "k"]), Reply::bulk("b-in-0"));
    assert!(info(&mut a).contains(" db=0 "));

    // Exclusive commands sent over the wire know their database too.
    a.cmd(&["SELECT", "3"]);
    assert_eq!(a.cmd(&["FLUSHDB"]), Reply::Ok);
    assert_eq!(a.cmd(&["DBSIZE"]), Reply::Int(0));
    b.cmd(&["SELECT", "0"]);
    assert_eq!(b.cmd(&["DBSIZE"]), Reply::Int(2));
    assert_eq!(a.cmd(&["MOVE", "k", "0"]), Reply::Int(0));
    assert_eq!(b.cmd(&["MOVE", "k", "3"]), Reply::Int(1));
    assert_eq!(a.cmd(&["GET", "k"]), Reply::bulk("b-in-0"));
    assert_eq!(
        a.cmd(&["EVAL", "return redis.call('GET', 'k')", "0"]),
        Reply::bulk("b-in-0")
    );
    assert_eq!(
        b.cmd(&["EVAL", "return redis.call('GET', 'k')", "0"]),
        Reply::Nil
    );
}

#[test]
fn a_blocked_client_is_served_only_from_its_database() {
    let env = env();
    let mut waiter = env.client();
    let mut pusher = env.client();
    waiter.cmd(&["SELECT", "2"]);
    waiter.send(&["BLPOP", "q", "0"]);
    assert!(wait_until(5, || {
        let Reply::Bulk(info) = pusher.cmd(&["INFO"]) else {
            return false;
        };
        String::from_utf8_lossy(&info).contains("blocked_clients:1")
    }));
    // The same name in other databases is none of its business.
    for db in ["0", "1", "3"] {
        pusher.cmd(&["SELECT", db]);
        assert_eq!(pusher.cmd(&["RPUSH", "q", "wrong"]), Reply::Int(1));
    }
    waiter.set_timeout(Duration::from_millis(300));
    assert!(waiter.try_read().is_err(), "served from another database");
    waiter.set_timeout(IO_TIMEOUT);
    for db in ["0", "1", "3"] {
        pusher.cmd(&["SELECT", db]);
        assert_eq!(pusher.cmd(&["LLEN", "q"]), Reply::Int(1), "db {db}");
    }
    pusher.cmd(&["SELECT", "2"]);
    pusher.cmd(&["RPUSH", "q", "right"]);
    assert_eq!(
        waiter.read(),
        Reply::Array(vec![Reply::bulk("q"), Reply::bulk("right")])
    );
    // MOVE into the database wakes a waiter as a push would.
    waiter.send(&["BLPOP", "q", "0"]);
    assert!(wait_until(5, || {
        let Reply::Bulk(info) = pusher.cmd(&["INFO"]) else {
            return false;
        };
        String::from_utf8_lossy(&info).contains("blocked_clients:1")
    }));
    pusher.cmd(&["SELECT", "3"]);
    assert_eq!(pusher.cmd(&["MOVE", "q", "2"]), Reply::Int(1));
    assert_eq!(
        waiter.read(),
        Reply::Array(vec![Reply::bulk("q"), Reply::bulk("wrong")])
    );
    // And a stream reader, with a transaction feeding it from elsewhere.
    waiter.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]);
    assert!(wait_until(5, || {
        let Reply::Bulk(info) = pusher.cmd(&["INFO"]) else {
            return false;
        };
        String::from_utf8_lossy(&info).contains("blocked_clients:1")
    }));
    pusher.cmd(&["SELECT", "0"]);
    pusher.cmd(&["MULTI"]);
    pusher.cmd(&["XADD", "s", "1-1", "db", "0"]);
    pusher.cmd(&["SELECT", "2"]);
    pusher.cmd(&["XADD", "s", "2-2", "db", "2"]);
    pusher.cmd(&["EXEC"]);
    let Reply::Array(streams) = waiter.read() else {
        panic!("no streams");
    };
    assert!(
        format!("{streams:?}").contains("[50, 45, 50]"),
        "{streams:?}"
    );
}

#[test]
fn databases_stay_apart_under_concurrent_load() {
    let _heavy = heavy();
    let env = env();
    const DBS: usize = 6;
    const ROUNDS: usize = 400;
    let threads: Vec<_> = (0..DBS * 2)
        .map(|t| {
            let addr = env.addr;
            std::thread::spawn(move || {
                let db = t % DBS;
                let mut c = Client::connect(addr);
                c.cmd(&["SELECT", &db.to_string()]);
                for i in 0..ROUNDS {
                    c.cmd(&["INCR", "counter"]);
                    c.cmd(&["RPUSH", "list", &format!("{t}:{i}")]);
                    c.cmd(&["HINCRBY", "hash", "n", "1"]);
                    c.cmd(&["MSET", &format!("a{t}:{i}"), "1", &format!("b{t}:{i}"), "2"]);
                    c.cmd(&["SADD", "set", &i.to_string()]);
                    if i % 50 == 0 {
                        // Transactions that visit a neighbour and come back.
                        let next = ((db + 1) % DBS).to_string();
                        c.cmd(&["MULTI"]);
                        c.cmd(&["SELECT", &next]);
                        c.cmd(&["INCR", "visits"]);
                        c.cmd(&["SELECT", &db.to_string()]);
                        c.cmd(&["INCR", "trips"]);
                        c.cmd(&["EXEC"]);
                        let keys = c.cmd(&["KEYS", "a*"]);
                        assert!(names(keys).iter().all(|k| {
                            let owner: usize = k[1..k.find(':').unwrap()].parse().unwrap();
                            owner % DBS == db
                        }));
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let mut c = env.client();
    for db in 0..DBS {
        c.cmd(&["SELECT", &db.to_string()]);
        let rounds = (2 * ROUNDS).to_string();
        assert_eq!(c.cmd(&["GET", "counter"]), Reply::bulk(rounds.clone()));
        assert_eq!(c.cmd(&["LLEN", "list"]), Reply::Int(2 * ROUNDS as i64));
        assert_eq!(c.cmd(&["HGET", "hash", "n"]), Reply::bulk(rounds));
        assert_eq!(c.cmd(&["SCARD", "set"]), Reply::Int(ROUNDS as i64));
        assert_eq!(c.cmd(&["GET", "visits"]), Reply::bulk("16"));
        assert_eq!(c.cmd(&["GET", "trips"]), Reply::bulk("16"));
        // counter, list, hash, set, visits, trips and four keys a round.
        assert_eq!(
            c.cmd(&["DBSIZE"]),
            Reply::Int(6 + 4 * ROUNDS as i64),
            "db {db}"
        );
        let seen = scan_all(&mut |args| c.cmd(args), "100");
        assert_eq!(seen.len(), 6 + 4 * ROUNDS);
    }
    c.cmd(&["SELECT", &DBS.to_string()]);
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(0));
}

fn open(path: &std::path::Path, databases: usize) -> Result<(Db, Session), hotstash::Error> {
    let db = Db::open_with(
        Some(path),
        Config {
            shards: 4,
            databases,
            ..Config::default()
        },
        PersistOptions {
            interval: Duration::from_millis(5),
            ..PersistOptions::default()
        },
    )?;
    let s = db.session().unwrap();
    Ok((db, s))
}

/// What every database holds, as replies that do not depend on order.
fn picture(s: &Session) -> Vec<(usize, Vec<Reply>)> {
    (0..16)
        .map(|db| {
            ok(s.execute(&["SELECT", &db.to_string()]));
            let mut keys: Vec<String> = names(s.execute(&["KEYS", "*"])).into_iter().collect();
            keys.sort();
            let mut view = vec![s.execute(&["DBSIZE"])];
            for key in keys {
                view.push(Reply::bulk(key.clone()));
                view.push(s.execute(&["TYPE", &key]));
                view.push(match s.execute(&["PTTL", &key]) {
                    Reply::Int(n) => Reply::Int(n.signum()),
                    other => other,
                });
                for read in [
                    &["GET", &key][..],
                    &["LRANGE", &key, "0", "-1"],
                    &["HGETALL", &key],
                    &["XRANGE", &key, "-", "+"],
                ] {
                    let reply = s.execute(read);
                    if !reply.is_error() {
                        view.push(reply);
                    }
                }
            }
            (db, view)
        })
        .collect()
}

#[test]
fn every_database_comes_back_from_the_data_file() {
    let dir = std::env::temp_dir().join(format!("hotstash-databases-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("databases.hc");
    let _ = std::fs::remove_file(&path);
    let before = {
        let (db, s) = open(&path, 16).unwrap();
        for n in [0, 1, 7, 15] {
            ok(s.execute(&["SELECT", &n.to_string()]));
            for i in 0..200 {
                s.execute(&["SET", &format!("k{i}"), &format!("db{n}")]);
            }
            s.execute(&["RPUSH", "list", &n.to_string(), "x"]);
            s.execute(&["HSET", "hash", "db", &n.to_string()]);
            s.execute(&["XADD", "stream", "1-1", "db", &n.to_string()]);
            s.execute(&["SET", "timed", "v", "EX", "1000"]);
        }
        db.sync().unwrap();
        // Changes after the first write: deletions, a flushed database, a
        // key that moves, a copy, and a transaction across two databases.
        ok(s.execute(&["SELECT", "7"]));
        ok(s.execute(&["FLUSHDB"]));
        s.execute(&["SET", "after-flush", "7"]);
        ok(s.execute(&["SELECT", "1"]));
        for i in 0..100 {
            s.execute(&["DEL", &format!("k{i}")]);
        }
        assert_eq!(s.execute(&["MOVE", "list", "9"]), Reply::Int(1));
        assert_eq!(
            s.execute(&["COPY", "hash", "hash-copy", "DB", "10"]),
            Reply::Int(1)
        );
        ok(s.execute(&["MULTI"]));
        s.execute(&["SELECT", "12"]);
        s.execute(&["SET", "from-txn", "12"]);
        s.execute(&["SELECT", "0"]);
        s.execute(&["DEL", "k0"]);
        s.execute(&["EXEC"]);
        db.sync().unwrap();
        picture(&s)
    };
    assert_eq!(before[7].1[0], Reply::Int(1));
    assert_eq!(before[9].1[0], Reply::Int(1));
    assert_eq!(before[1].1[0], Reply::Int(103));
    let after = {
        let (db, s) = open(&path, 16).unwrap();
        let view = picture(&s);
        // A rewrite of the file keeps them apart as well.
        ok(s.execute(&["SELECT", "0"]));
        assert!(!s.execute(&["BGSAVE"]).is_error());
        ok(s.execute(&["SELECT", "15"]));
        s.execute(&["SET", "after-rewrite", "15"]);
        db.sync().unwrap();
        view
    };
    assert_eq!(before, after);
    let mut expected = before;
    {
        let (_db, s) = open(&path, 16).unwrap();
        let view = picture(&s);
        ok(s.execute(&["SELECT", "15"]));
        assert_eq!(s.execute(&["GET", "after-rewrite"]), Reply::bulk("15"));
        s.execute(&["DEL", "after-rewrite"]);
        expected[15] = view[15].clone();
        assert_eq!(view[..15], expected[..15]);
        assert_eq!(view[15].1[0], Reply::Int(205));
    }
    // FLUSHALL reaches the file for every database.
    {
        let (db, s) = open(&path, 16).unwrap();
        ok(s.execute(&["SELECT", "5"]));
        ok(s.execute(&["FLUSHALL"]));
        s.execute(&["SET", "survivor", "5"]);
        db.sync().unwrap();
    }
    {
        let (_db, s) = open(&path, 16).unwrap();
        let view = picture(&s);
        for (db, keys) in &view {
            let want = i64::from(*db == 5);
            assert_eq!(keys[0], Reply::Int(want), "db {db}");
        }
    }
    // Fewer databases than the file needs: refuse, do not fold them away.
    let error = match open(&path, 4) {
        Ok(_) => panic!("opened a file that holds database 5 with 4 databases"),
        Err(e) => e.to_string(),
    };
    assert!(error.contains("but only 4 are configured"), "{error}");
    let _ = std::fs::remove_dir_all(&dir);
}
