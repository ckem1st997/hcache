//! Streams as a client sees them: the key among the other types, XREAD and
//! XREADGROUP with and without BLOCK, consumer groups under load, and the
//! data file.

mod net;

use hotstash::{Config, Db, PersistOptions, Reply, Session};
use net::*;
use std::collections::HashSet;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn env(workers: usize) -> Env {
    Env::new(engine_cfg(8), server_cfg(workers, 64))
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn arr(items: &[Reply]) -> Reply {
    Reply::Array(items.to_vec())
}

fn entry(id: &str, fields: &[&str]) -> Reply {
    arr(&[b(id), Reply::Array(fields.iter().map(|f| b(f)).collect())])
}

fn items(reply: Reply) -> Vec<Reply> {
    match reply {
        Reply::Array(items) => items,
        other => panic!("not an array: {other:?}"),
    }
}

fn text(reply: &Reply) -> String {
    match reply {
        Reply::Bulk(bytes) => String::from_utf8(bytes.clone()).unwrap(),
        Reply::Simple(s) => s.clone(),
        other => panic!("not text: {other:?}"),
    }
}

fn settle() {
    thread::sleep(Duration::from_millis(60));
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

#[test]
fn a_stream_is_a_key_like_any_other() {
    let env = env(2);
    let mut c = env.client();
    assert_eq!(c.cmd(&["XADD", "s", "1-1", "f", "v"]), b("1-1"));
    assert_eq!(c.cmd(&["XADD", "s", "1-2", "f", "w", "g", "x"]), b("1-2"));
    assert_eq!(c.cmd(&["TYPE", "s"]), Reply::Simple("stream".into()));
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "s"]), b("stream"));
    assert_eq!(c.cmd(&["XLEN", "s"]), Reply::Int(2));
    assert_eq!(c.cmd(&["EXISTS", "s"]), Reply::Int(1));
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(1));
    assert_eq!(
        c.cmd(&["XRANGE", "s", "-", "+"]),
        arr(&[
            entry("1-1", &["f", "v"]),
            entry("1-2", &["f", "w", "g", "x"])
        ])
    );
    assert_eq!(
        c.cmd(&["XREVRANGE", "s", "+", "-", "COUNT", "1"]),
        arr(&[entry("1-2", &["f", "w", "g", "x"])])
    );
    // The other types refuse it, and it refuses them.
    for cmd in [
        &["GET", "s"][..],
        &["LPUSH", "s", "x"],
        &["SADD", "s", "x"],
        &["HSET", "s", "f", "v"],
        &["ZADD", "s", "1", "m"],
        &["INCR", "s"],
        &["BLPOP", "s", "0.01"],
    ] {
        assert_eq!(c.cmd(cmd), Reply::err(WRONGTYPE), "{cmd:?}");
    }
    c.cmd(&["SET", "str", "x"]);
    for cmd in [
        &["XADD", "str", "*", "f", "v"][..],
        &["XLEN", "str"],
        &["XRANGE", "str", "-", "+"],
        &["XREAD", "STREAMS", "str", "0"],
        &["XREAD", "STREAMS", "str", "$"],
        &["XGROUP", "CREATE", "str", "g", "$"],
        &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "str", ">"],
        &["XDEL", "str", "1-1"],
        &["XTRIM", "str", "MAXLEN", "0"],
        &["XINFO", "STREAM", "str"],
    ] {
        assert_eq!(c.cmd(cmd), Reply::err(WRONGTYPE), "{cmd:?}");
    }
    assert_eq!(c.cmd(&["GET", "str"]), b("x"));

    // Generic commands.
    assert_eq!(c.cmd(&["COPY", "s", "s2"]), Reply::Int(1));
    assert_eq!(c.cmd(&["XADD", "s2", "2-0", "only", "copy"]), b("2-0"));
    assert_eq!(c.cmd(&["XLEN", "s"]), Reply::Int(2));
    assert_eq!(c.cmd(&["RENAME", "s2", "s3"]), Reply::Ok);
    assert_eq!(c.cmd(&["XLEN", "s3"]), Reply::Int(3));
    assert_eq!(c.cmd(&["PEXPIRE", "s3", "40"]), Reply::Int(1));
    thread::sleep(Duration::from_millis(80));
    assert_eq!(c.cmd(&["XLEN", "s3"]), Reply::Int(0));
    assert_eq!(c.cmd(&["TYPE", "s3"]), Reply::Simple("none".into()));
    // After expiry the name starts over: small IDs are fine again.
    assert_eq!(c.cmd(&["XADD", "s3", "0-1", "f", "v"]), b("0-1"));
    assert_eq!(c.cmd(&["DEL", "s", "s3"]), Reply::Int(2));
    assert_eq!(c.cmd(&["XADD", "s", "0-1", "f", "v"]), b("0-1"));
}

#[test]
fn commands_that_fail_leave_no_key_behind() {
    let env = env(1);
    let mut c = env.client();
    for cmd in [
        &["XADD", "s", "0-0", "f", "v"][..],
        &["XADD", "s", "NOMKSTREAM", "*", "f", "v"],
        &["XADD", "s", "bad", "f", "v"],
        &["XADD", "s", "*", "f"],
        &["XADD", "s", "MAXLEN", "x", "*", "f", "v"],
        &["XGROUP", "CREATE", "s", "g", "$"],
        &["XGROUP", "CREATE", "s", "g", "nope", "MKSTREAM"],
        &["XGROUP", "CREATECONSUMER", "s", "g", "c"],
        &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
        &["XTRIM", "s", "MAXLEN", "0"],
        &["XDEL", "s", "1-1"],
        &["XSETID", "s", "5-5"],
        &["XACK", "s", "g", "1-1"],
        &["XCLAIM", "s", "g", "c", "0", "1-1"],
        &["XAUTOCLAIM", "s", "g", "c", "0", "0"],
        &["XPENDING", "s", "g"],
    ] {
        let reply = c.cmd(cmd);
        assert_eq!(
            c.cmd(&["EXISTS", "s"]),
            Reply::Int(0),
            "{cmd:?} -> {reply:?}"
        );
    }
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(0));
    // A stream emptied entry by entry stays, with its last ID.
    c.cmd(&["XADD", "s", "5-0", "f", "v"]);
    assert_eq!(c.cmd(&["XDEL", "s", "5-0"]), Reply::Int(1));
    assert_eq!(c.cmd(&["XLEN", "s"]), Reply::Int(0));
    assert_eq!(c.cmd(&["EXISTS", "s"]), Reply::Int(1));
    assert_eq!(
        c.cmd(&["XADD", "s", "5-0", "f", "v"]),
        Reply::err(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        )
    );
    // A group made with MKSTREAM is a key with no entries.
    assert_eq!(
        c.cmd(&["XGROUP", "CREATE", "t", "g", "$", "MKSTREAM"]),
        Reply::Ok
    );
    assert_eq!(c.cmd(&["TYPE", "t"]), Reply::Simple("stream".into()));
    assert_eq!(
        c.cmd(&["XGROUP", "CREATE", "t", "g", "$", "MKSTREAM"]),
        Reply::err("BUSYGROUP Consumer Group name already exists")
    );
}

#[test]
fn reads_over_several_streams() {
    let env = env(2);
    let mut c = env.client();
    for (key, id) in [("a", "1-1"), ("a", "2-1"), ("b", "1-5"), ("c", "9-9")] {
        c.cmd(&["XADD", key, id, "k", key]);
    }
    assert_eq!(
        c.cmd(&["XREAD", "STREAMS", "a", "b", "nokey", "c", "1-1", "0", "0", "9-9"]),
        arr(&[
            arr(&[b("a"), arr(&[entry("2-1", &["k", "a"])])]),
            arr(&[b("b"), arr(&[entry("1-5", &["k", "b"])])]),
        ])
    );
    assert_eq!(
        c.cmd(&["XREAD", "COUNT", "1", "STREAMS", "a", "0"]),
        arr(&[arr(&[b("a"), arr(&[entry("1-1", &["k", "a"])])])])
    );
    assert!(!c
        .cmd(&["xread", "count", "0", "streams", "a", "1"])
        .is_error());
    assert_eq!(
        c.cmd(&["XREAD", "STREAMS", "a", "b", "$", "$"]),
        Reply::NilArray
    );
    assert_eq!(c.cmd(&["XREAD", "STREAMS", "a", "2-1"]), Reply::NilArray);
    assert_eq!(
        c.cmd(&[
            "XREAD",
            "STREAMS",
            "a",
            "18446744073709551615-18446744073709551615"
        ]),
        Reply::NilArray
    );
    for (cmd, want) in [
        (&["XREAD", "STREAMS", "a"][..], "ERR wrong number of arguments for 'xread' command"),
        (
            &["XREAD", "STREAMS", "a", "b", "0"],
            "ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.",
        ),
        (&["XREAD", "COUNT", "x", "STREAMS", "a", "0"], "ERR value is not an integer or out of range"),
        (&["XREAD", "BLOCK", "-1", "STREAMS", "a", "0"], "ERR timeout is negative"),
        (
            &["XREAD", "BLOCK", "1.5", "STREAMS", "a", "0"],
            "ERR timeout is not an integer or out of range",
        ),
        (&["XREAD", "NOPE", "1", "STREAMS", "a", "0"], "ERR syntax error"),
        (&["XREAD", "COUNT", "1", "a", "0"], "ERR syntax error"),
        (
            &["XREAD", "STREAMS", "a", "x-y"],
            "ERR Invalid stream ID specified as stream command argument",
        ),
        (
            &["XREAD", "STREAMS", "a", ">"],
            "ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
        ),
        (
            &["XREAD", "NOACK", "STREAMS", "a", "0"],
            "ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.",
        ),
        (
            &["XREADGROUP", "COUNT", "1", "BLOCK", "1", "STREAMS", "a", ">"],
            "ERR Missing GROUP option for XREADGROUP",
        ),
        (
            &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "a", "nokey", ">", ">"],
            "NOGROUP No such key 'a' or consumer group 'g' in XREADGROUP with GROUP option",
        ),
    ] {
        assert_eq!(c.cmd(cmd), Reply::err(want), "{cmd:?}");
    }
    assert!(c
        .cmd(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "a", "$"])
        .is_error());
}

#[test]
fn a_blocked_reader_wakes_for_new_entries_only() {
    let env = env(2);
    let mut reader = env.client();
    let mut writer = env.client();
    writer.cmd(&["XADD", "s", "1-1", "old", "1"]);
    reader.send(&["XREAD", "BLOCK", "0", "STREAMS", "nokey", "s", "$", "$"]);
    settle();
    assert!(reader.silent_for(80));
    // Changes that add nothing keep it waiting.
    writer.cmd(&["XDEL", "s", "1-1"]);
    writer.cmd(&["XGROUP", "CREATE", "s", "g", "0"]);
    writer.cmd(&["XTRIM", "s", "MAXLEN", "0"]);
    assert!(reader.silent_for(80));
    assert_eq!(writer.cmd(&["XADD", "s", "1-2", "new", "1"]), b("1-2"));
    assert_eq!(
        reader.read(),
        arr(&[arr(&[b("s"), arr(&[entry("1-2", &["new", "1"])])])])
    );
    // A stream that did not exist when the reader came: `$` is its start.
    reader.send(&["XREAD", "BLOCK", "0", "COUNT", "1", "STREAMS", "fresh", "$"]);
    settle();
    writer.send(&["MULTI"]);
    writer.send(&["XADD", "fresh", "1-1", "a", "1"]);
    writer.send(&["XADD", "fresh", "1-2", "a", "2"]);
    writer.send(&["EXEC"]);
    for _ in 0..4 {
        writer.read();
    }
    assert_eq!(
        reader.read(),
        arr(&[arr(&[b("fresh"), arr(&[entry("1-1", &["a", "1"])])])])
    );
    // An ID in the future waits for exactly that.
    reader.send(&["XREAD", "BLOCK", "0", "STREAMS", "fresh", "5"]);
    settle();
    writer.cmd(&["XADD", "fresh", "4-0", "too", "early"]);
    assert!(reader.silent_for(80));
    writer.cmd(&["XADD", "fresh", "5-0", "too", "early"]);
    assert!(reader.silent_for(80));
    writer.cmd(&["XADD", "fresh", "5-1", "just", "right"]);
    assert_eq!(
        reader.read(),
        arr(&[arr(&[b("fresh"), arr(&[entry("5-1", &["just", "right"])])])])
    );
}

#[test]
fn blocking_reads_time_out_and_never_wait_inside_a_transaction() {
    let env = env(2);
    let mut c = env.client();
    let start = Instant::now();
    assert_eq!(
        c.cmd(&["XREAD", "BLOCK", "120", "STREAMS", "s", "$"]),
        Reply::NilArray
    );
    let took = start.elapsed();
    assert!(
        took >= Duration::from_millis(110) && took < Duration::from_secs(2),
        "{took:?}"
    );
    c.cmd(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
    let start = Instant::now();
    assert_eq!(
        c.cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "120",
            "STREAMS",
            "s",
            ">"
        ]),
        Reply::NilArray
    );
    assert!(start.elapsed() >= Duration::from_millis(110));
    // History is answered at once, empty or not.
    let start = Instant::now();
    assert_eq!(
        c.cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            "0"
        ]),
        arr(&[arr(&[b("s"), arr(&[])])])
    );
    assert!(start.elapsed() < Duration::from_secs(1));

    c.cmd(&["MULTI"]);
    c.cmd(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]);
    c.cmd(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "BLOCK",
        "0",
        "STREAMS",
        "s",
        ">",
    ]);
    c.cmd(&["XADD", "s", "7-7", "f", "v"]);
    assert_eq!(
        c.cmd(&["EXEC"]),
        arr(&[Reply::NilArray, Reply::NilArray, b("7-7")])
    );
    // In a script too.
    assert_eq!(
        c.cmd(&[
            "EVAL",
            "local r = redis.call('XREAD', 'BLOCK', '0', 'STREAMS', KEYS[1], '$') \
             return {type(r), tostring(r)}",
            "1",
            "s"
        ]),
        arr(&[b("boolean"), b("false")])
    );
}

#[test]
fn a_group_hands_each_entry_to_one_consumer() {
    let env = env(2);
    let mut c = env.client();
    let mut alice = env.client();
    let mut bob = env.client();
    c.cmd(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
    alice.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "BLOCK",
        "0",
        "STREAMS",
        "s",
        ">",
    ]);
    settle();
    bob.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "bob",
        "BLOCK",
        "0",
        "STREAMS",
        "s",
        ">",
    ]);
    settle();
    c.cmd(&["XADD", "s", "1-1", "n", "1"]);
    // First come, first served.
    assert_eq!(
        alice.read(),
        arr(&[arr(&[b("s"), arr(&[entry("1-1", &["n", "1"])])])])
    );
    assert!(bob.silent_for(80));
    c.cmd(&["XADD", "s", "1-2", "n", "2"]);
    assert_eq!(
        bob.read(),
        arr(&[arr(&[b("s"), arr(&[entry("1-2", &["n", "2"])])])])
    );
    assert_eq!(
        c.cmd(&["XPENDING", "s", "g"]),
        arr(&[
            Reply::Int(2),
            b("1-1"),
            b("1-2"),
            arr(&[arr(&[b("alice"), b("1")]), arr(&[b("bob"), b("1")])]),
        ])
    );
    // History is each consumer's own.
    assert_eq!(
        alice.cmd(&["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "0"]),
        arr(&[arr(&[b("s"), arr(&[entry("1-1", &["n", "1"])])])])
    );
    assert_eq!(
        alice.cmd(&["XACK", "s", "g", "1-1", "1-2", "9-9"]),
        Reply::Int(2)
    );
    assert_eq!(alice.cmd(&["XACK", "s", "g", "1-1"]), Reply::Int(0));
    assert_eq!(
        c.cmd(&["XPENDING", "s", "g"]),
        arr(&[Reply::Int(0), Reply::Nil, Reply::Nil, Reply::NilArray])
    );
    // NOACK leaves nothing pending.
    c.cmd(&["XADD", "s", "1-3", "n", "3"]);
    assert_eq!(
        bob.cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "bob",
            "NOACK",
            "STREAMS",
            "s",
            ">"
        ]),
        arr(&[arr(&[b("s"), arr(&[entry("1-3", &["n", "3"])])])])
    );
    assert_eq!(items(c.cmd(&["XPENDING", "s", "g"]))[0], Reply::Int(0));
}

#[test]
fn a_blocked_group_reader_learns_that_its_group_is_gone() {
    let env = env(2);
    let mut c = env.client();
    for how in ["destroy", "del", "flushall", "retype"] {
        c.cmd(&["DEL", "s"]);
        c.cmd(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
        let mut reader = env.client();
        reader.send(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]);
        settle();
        assert!(reader.silent_for(60), "{how}");
        match how {
            "destroy" => assert_eq!(c.cmd(&["XGROUP", "DESTROY", "s", "g"]), Reply::Int(1)),
            "del" => assert_eq!(c.cmd(&["DEL", "s"]), Reply::Int(1)),
            "flushall" => assert_eq!(c.cmd(&["FLUSHALL"]), Reply::Ok),
            _ => {
                c.send(&["MULTI"]);
                c.send(&["DEL", "s"]);
                c.send(&["SET", "s", "x"]);
                c.send(&["EXEC"]);
                for _ in 0..4 {
                    c.read();
                }
            }
        }
        let reply = reader
            .try_read()
            .unwrap_or_else(|e| panic!("{how}: no reply: {e}"));
        assert!(reply.is_error(), "{how}: {reply:?}");
        assert_eq!(reader.cmd(&["PING"]), Reply::Simple("PONG".into()), "{how}");
    }
    assert_eq!(env.wait_conns(1, 5), 1);
}

#[test]
fn producers_and_group_consumers_lose_and_repeat_nothing() {
    const PRODUCERS: usize = 4;
    const PER_PRODUCER: usize = 400;
    const CONSUMERS: usize = 5;
    let _heavy = heavy();
    let env = Arc::new(env(3));
    env.client()
        .cmd(&["XGROUP", "CREATE", "jobs", "g", "$", "MKSTREAM"]);
    let seen = Arc::new(AtomicUsize::new(0));
    let total = PRODUCERS * PER_PRODUCER;

    let consumers: Vec<_> = (0..CONSUMERS)
        .map(|n| {
            let env = env.clone();
            let seen = seen.clone();
            thread::spawn(move || {
                let mut c = env.client();
                let name = format!("c{n}");
                let mut mine = Vec::new();
                while seen.load(SeqCst) < total {
                    let reply = c.cmd(&[
                        "XREADGROUP",
                        "GROUP",
                        "g",
                        &name,
                        "COUNT",
                        "7",
                        "BLOCK",
                        "50",
                        "STREAMS",
                        "jobs",
                        ">",
                    ]);
                    if reply == Reply::NilArray {
                        continue;
                    }
                    let streams = items(reply);
                    let entries = items(items(streams[0].clone())[1].clone());
                    assert!(!entries.is_empty() && entries.len() <= 7);
                    for e in entries {
                        let parts = items(e);
                        let id = text(&parts[0]);
                        let payload = text(&items(parts[1].clone())[1]);
                        assert_eq!(c.cmd(&["XACK", "jobs", "g", &id]), Reply::Int(1));
                        mine.push((id, payload));
                        seen.fetch_add(1, SeqCst);
                    }
                }
                mine
            })
        })
        .collect();
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let env = env.clone();
            thread::spawn(move || {
                let mut c = env.client();
                let mut ids = Vec::new();
                for n in 0..PER_PRODUCER {
                    ids.push(text(&c.cmd(&[
                        "XADD",
                        "jobs",
                        "*",
                        "job",
                        &format!("{p}:{n}"),
                    ])));
                }
                ids
            })
        })
        .collect();

    let mut produced = HashSet::new();
    for p in producers {
        for id in p.join().unwrap() {
            assert!(produced.insert(id.clone()), "ID {id} given out twice");
        }
    }
    let mut delivered = HashSet::new();
    let mut payloads = HashSet::new();
    for c in consumers {
        for (id, payload) in watchdog("consumer", 60, move || c.join().unwrap()) {
            assert!(delivered.insert(id.clone()), "{id} delivered twice");
            assert!(payloads.insert(payload));
        }
    }
    assert_eq!(delivered, produced);
    assert_eq!(payloads.len(), total);
    let mut c = env.client();
    assert_eq!(c.cmd(&["XLEN", "jobs"]), Reply::Int(total as i64));
    assert_eq!(items(c.cmd(&["XPENDING", "jobs", "g"]))[0], Reply::Int(0));
    // IDs in the stream only go up.
    let ids: Vec<(u64, u64)> = items(c.cmd(&["XRANGE", "jobs", "-", "+"]))
        .into_iter()
        .map(|e| {
            let id = text(&items(e)[0]);
            let (ms, seq) = id.split_once('-').unwrap();
            (ms.parse().unwrap(), seq.parse().unwrap())
        })
        .collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn plain_readers_all_see_every_entry() {
    let env = Arc::new(env(2));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let env = env.clone();
            thread::spawn(move || {
                let mut c = env.client();
                let mut last = "0".to_string();
                let mut got = Vec::new();
                while got.len() < 300 {
                    let reply = c.cmd(&[
                        "XREAD", "BLOCK", "0", "COUNT", "11", "STREAMS", "feed", &last,
                    ]);
                    for e in items(items(items(reply)[0].clone())[1].clone()) {
                        let parts = items(e);
                        last = text(&parts[0]);
                        got.push(text(&items(parts[1].clone())[1]));
                    }
                }
                got
            })
        })
        .collect();
    let mut c = env.client();
    for n in 0..300 {
        c.cmd(&["XADD", "feed", "*", "n", &n.to_string()]);
    }
    let want: Vec<String> = (0..300).map(|n| n.to_string()).collect();
    for r in readers {
        assert_eq!(watchdog("reader", 30, move || r.join().unwrap()), want);
    }
}

#[test]
fn a_reader_that_hangs_up_while_blocked_leaves_nothing_behind() {
    let env = env(2);
    let mut c = env.client();
    c.cmd(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
    for _ in 0..20 {
        let mut gone = env.client();
        gone.send(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "gone",
            "BLOCK",
            "0",
            "STREAMS",
            "s",
            ">",
        ]);
        let mut gone2 = env.client();
        gone2.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]);
        thread::sleep(Duration::from_millis(4));
    }
    assert_eq!(env.wait_conns(1, 5), 1);
    let mut stays = env.client();
    stays.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "stays",
        "BLOCK",
        "0",
        "STREAMS",
        "s",
        ">",
    ]);
    settle();
    c.cmd(&["XADD", "s", "1-1", "f", "v"]);
    assert_eq!(
        stays.read(),
        arr(&[arr(&[b("s"), arr(&[entry("1-1", &["f", "v"])])])])
    );
    let info = text(&c.cmd(&["INFO"]));
    assert!(info.contains("blocked_clients:0"), "{info}");
}

#[test]
fn resp3_reads_are_maps() {
    let env = env(1);
    let mut c = env.client();
    c.cmd(&["XADD", "s", "1-1", "f", "v"]);
    c.send(&["HELLO", "3"]);
    c.send(&["XREAD", "STREAMS", "s", "0"]);
    c.send(&["XREAD", "STREAMS", "s", "$"]);
    c.send(&["XINFO", "STREAM", "s"]);
    let want = b"%1\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n_\r\n%";
    let mut seen = Vec::new();
    let mut byte = [0u8; 1];
    while !seen.ends_with(want) {
        c.rd.read_exact(&mut byte).unwrap();
        seen.push(byte[0]);
        assert!(seen.len() < 4096, "{}", String::from_utf8_lossy(&seen));
    }
}

#[test]
fn scripts_and_transactions_use_streams() {
    let env = env(1);
    let mut c = env.client();
    assert_eq!(
        c.cmd(&[
            "EVAL",
            "redis.call('XADD', KEYS[1], '1-1', 'a', '1') \
             redis.call('XADD', KEYS[1], '1-2', 'b', '2') \
             local r = redis.call('XRANGE', KEYS[1], '-', '+') \
             return {#r, r[2][1], r[2][2][1], redis.call('XLEN', KEYS[1]), \
                     redis.call('TYPE', KEYS[1]).ok}",
            "1",
            "s"
        ]),
        arr(&[Reply::Int(2), b("1-2"), b("b"), Reply::Int(2), b("stream")])
    );
    let denied = c.cmd(&[
        "EVAL_RO",
        "return redis.call('XADD', KEYS[1], '*', 'a', '1')",
        "1",
        "s",
    ]);
    assert!(denied.is_error(), "{denied:?}");
    let denied = c.cmd(&[
        "EVAL_RO",
        "return redis.call('XREADGROUP', 'GROUP', 'g', 'c', 'STREAMS', KEYS[1], '>')",
        "1",
        "s",
    ]);
    assert!(denied.is_error(), "{denied:?}");
    assert_eq!(
        c.cmd(&["EVAL_RO", "return redis.call('XLEN', KEYS[1])", "1", "s"]),
        Reply::Int(2)
    );
    assert_eq!(c.cmd(&["XLEN", "s"]), Reply::Int(2));
    // WATCH sees stream writes, group changes included.
    let mut other = env.client();
    for change in [
        &["XADD", "s", "*", "f", "v"][..],
        &["XGROUP", "CREATE", "s", "g", "0"],
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "1",
            "STREAMS",
            "s",
            ">",
        ],
        &["XACK", "s", "g", "1-1"],
        &["XDEL", "s", "1-2"],
        &["XTRIM", "s", "MAXLEN", "0"],
    ] {
        c.cmd(&["WATCH", "s"]);
        assert!(!other.cmd(change).is_error(), "{change:?}");
        c.cmd(&["MULTI"]);
        c.cmd(&["PING"]);
        assert_eq!(c.cmd(&["EXEC"]), Reply::NilArray, "{change:?}");
    }
    // Reads do not.
    c.cmd(&["WATCH", "s"]);
    other.cmd(&["XLEN", "s"]);
    other.cmd(&["XRANGE", "s", "-", "+"]);
    other.cmd(&["XREAD", "STREAMS", "s", "0"]);
    other.cmd(&["XINFO", "STREAM", "s"]);
    other.cmd(&["XPENDING", "s", "g"]);
    c.cmd(&["MULTI"]);
    c.cmd(&["PING"]);
    assert_eq!(c.cmd(&["EXEC"]), arr(&[Reply::Simple("PONG".into())]));
}

fn open(path: &std::path::Path) -> (Db, Session) {
    let db = Db::open_with(
        Some(path),
        Config {
            shards: 4,
            ..Config::default()
        },
        PersistOptions {
            interval: Duration::from_millis(5),
            ..PersistOptions::default()
        },
    )
    .unwrap();
    let s = db.session().unwrap();
    (db, s)
}

#[test]
fn streams_come_back_from_the_data_file() {
    let dir = std::env::temp_dir().join(format!("hotstash-streams-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("streams.hc");
    let _ = std::fs::remove_file(&path);
    let views: &[&[&str]] = &[
        &["XRANGE", "s", "-", "+"],
        &["XLEN", "s"],
        &["XPENDING", "s", "g"],
        &["XPENDING", "s", "g", "-", "+", "10"],
        &["XINFO", "GROUPS", "s"],
        &["XREADGROUP", "GROUP", "g", "alice", "STREAMS", "s", "0"],
        &["XLEN", "emptied"],
        &["TYPE", "emptied"],
        &["TYPE", "onlygroup"],
        &["XINFO", "GROUPS", "onlygroup"],
        &["PTTL", "s"],
        &["DBSIZE"],
    ];
    let before: Vec<Reply> = {
        let (_db, s) = open(&path);
        for n in 1..=50 {
            let id = format!("{n}-0");
            s.execute(&[
                "XADD",
                "s",
                &id,
                "n",
                &n.to_string(),
                "bin",
                "\u{0}\r\n\u{ff}",
            ]);
        }
        s.execute(&["XGROUP", "CREATE", "s", "g", "0"]);
        s.execute(&["XGROUP", "CREATE", "s", "idle", "$"]);
        s.execute(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "alice",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]);
        s.execute(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "bob",
            "COUNT",
            "5",
            "STREAMS",
            "s",
            ">",
        ]);
        s.execute(&["XACK", "s", "g", "1-0", "2-0", "11-0"]);
        s.execute(&["XDEL", "s", "3-0", "40-0"]);
        s.execute(&["XTRIM", "s", "MINID", "3"]);
        s.execute(&["XADD", "emptied", "9-9", "f", "v"]);
        s.execute(&["XDEL", "emptied", "9-9"]);
        s.execute(&["XGROUP", "CREATE", "onlygroup", "g", "$", "MKSTREAM"]);
        s.execute(&["XADD", "gone", "*", "f", "v"]);
        s.execute(&["DEL", "gone"]);
        views.iter().map(|v| s.execute(v)).collect()
    };
    assert_eq!(before[1], Reply::Int(46));
    let (_db, s) = open(&path);
    let idle_first = |reply: &Reply| format!("{reply:?}");
    for (view, want) in views.iter().zip(&before) {
        let got = s.execute(view);
        if view[0] == "XPENDING" && view.len() > 3 {
            // Idle times move on; the rows are the same ones.
            assert_eq!(items(got).len(), items(want.clone()).len(), "{view:?}");
        } else {
            assert_eq!(idle_first(&got), idle_first(want), "{view:?}");
        }
    }
    // The last IDs survived, so old IDs stay refused.
    assert!(s.execute(&["XADD", "s", "50-0", "f", "v"]).is_error());
    assert!(s.execute(&["XADD", "emptied", "9-9", "f", "v"]).is_error());
    assert_eq!(s.execute(&["XADD", "emptied", "9-10", "f", "v"]), b("9-10"));
    assert_eq!(s.execute(&["EXISTS", "gone"]), Reply::Int(0));
    // The group goes on where it stopped.
    assert_eq!(
        items(
            items(
                items(s.execute(&[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "carol",
                    "COUNT",
                    "1",
                    "STREAMS",
                    "s",
                    ">"
                ]))[0]
                    .clone()
            )[1]
            .clone()
        )[0],
        entry("16-0", &["n", "16", "bin", "\u{0}\r\n\u{ff}"])
    );
    drop(s);
    drop(_db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_waiter_that_cannot_use_a_change_does_not_hide_it_from_the_next() {
    let env = env(2);
    let mut c = env.client();
    // First in line: a reader that wants a later ID. Behind it: one that
    // takes anything new, a group reader, and a list popper.
    c.cmd(&["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
    let mut picky = env.client();
    picky.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "5"]);
    settle();
    let mut any = env.client();
    any.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]);
    settle();
    let mut grouped = env.client();
    grouped.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c",
        "BLOCK",
        "0",
        "STREAMS",
        "s",
        ">",
    ]);
    settle();
    let mut any2 = env.client();
    any2.send(&["XREAD", "BLOCK", "0", "STREAMS", "other", "s", "$", "$"]);
    settle();
    c.cmd(&["XADD", "s", "4-0", "f", "v"]);
    let want = arr(&[arr(&[b("s"), arr(&[entry("4-0", &["f", "v"])])])]);
    assert_eq!(any.read(), want);
    assert_eq!(grouped.read(), want);
    assert_eq!(any2.read(), want);
    assert!(picky.silent_for(80));
    c.cmd(&["XADD", "s", "5-1", "f", "v"]);
    assert_eq!(
        picky.read(),
        arr(&[arr(&[b("s"), arr(&[entry("5-1", &["f", "v"])])])])
    );

    // A list popper in front of a sorted-set popper, and the other way.
    let mut list = env.client();
    list.send(&["BLPOP", "k", "0"]);
    settle();
    let mut zset = env.client();
    zset.send(&["BZPOPMIN", "k", "0"]);
    settle();
    let mut reader = env.client();
    reader.send(&["XREAD", "BLOCK", "0", "STREAMS", "k", "$"]);
    settle();
    c.cmd(&["XADD", "k", "1-1", "f", "v"]);
    assert_eq!(
        reader.read(),
        arr(&[arr(&[b("k"), arr(&[entry("1-1", &["f", "v"])])])])
    );
    c.cmd(&["DEL", "k"]);
    c.cmd(&["ZADD", "k", "1", "m"]);
    assert_eq!(zset.read(), arr(&[b("k"), b("m"), b("1")]));
    assert!(list.silent_for(80));
    c.cmd(&["RPUSH", "k", "x"]);
    assert_eq!(list.read(), arr(&[b("k"), b("x")]));
    let info = text(&c.cmd(&["INFO"]));
    assert!(info.contains("blocked_clients:0"), "{info}");
}
