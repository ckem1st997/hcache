//! BLPOP and its relatives over TCP: waiting, waking, timing out, order
//! among waiters, and what happens when waiters vanish.

mod net;

use hotstash::Reply;
use net::*;
use std::collections::HashSet;
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
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

fn strs(items: &[&str]) -> Reply {
    Reply::Array(items.iter().map(|s| b(s)).collect())
}

/// Give a command just sent the time to reach the server and block.
fn settle() {
    thread::sleep(Duration::from_millis(60));
}

#[test]
fn data_already_there_is_returned_at_once() {
    let env = env(2);
    let mut c = env.client();
    c.cmd(&["RPUSH", "l", "a", "b", "c", "d", "e", "f"]);
    c.cmd(&["ZADD", "z", "1", "x", "2", "y", "3", "w", "4", "v"]);
    assert_eq!(c.cmd(&["BLPOP", "l", "0"]), strs(&["l", "a"]));
    assert_eq!(c.cmd(&["BRPOP", "nokey", "l", "0"]), strs(&["l", "f"]));
    assert_eq!(c.cmd(&["BLPOP", "l", "l", "1.5"]), strs(&["l", "b"]));
    assert_eq!(
        c.cmd(&["BZPOPMIN", "nokey", "z", "0"]),
        strs(&["z", "x", "1"])
    );
    assert_eq!(c.cmd(&["BZPOPMAX", "z", "0"]), strs(&["z", "v", "4"]));
    assert_eq!(
        c.cmd(&["BLMPOP", "0", "2", "nokey", "l", "LEFT", "COUNT", "2"]),
        arr(&[b("l"), strs(&["c", "d"])])
    );
    assert_eq!(
        c.cmd(&["BZMPOP", "0.5", "1", "z", "MAX"]),
        arr(&[b("z"), arr(&[strs(&["w", "3"])])])
    );
    assert_eq!(c.cmd(&["BLMOVE", "l", "m", "RIGHT", "LEFT", "0"]), b("e"));
    assert_eq!(c.cmd(&["BRPOPLPUSH", "m", "n", "0"]), b("e"));
    assert_eq!(c.cmd(&["LRANGE", "n", "0", "-1"]), strs(&["e"]));
    assert_eq!(c.cmd(&["EXISTS", "l", "m"]), Reply::Int(0));
}

#[test]
fn bad_arguments_and_wrong_types_answer_at_once() {
    let env = env(1);
    let mut c = env.client();
    c.cmd(&["SET", "s", "v"]);
    for (args, want) in [
        (&["BLPOP", "k", "-1"][..], "ERR timeout is negative"),
        (
            &["BLPOP", "k", "abc"],
            "ERR timeout is not a float or out of range",
        ),
        (
            &["BLPOP", "k", "inf"],
            "ERR timeout is not a float or out of range",
        ),
        (
            &["BLPOP", "k", "nan"],
            "ERR timeout is not a float or out of range",
        ),
        (
            &["BLPOP", "k", "1e30"],
            "ERR timeout is not a float or out of range",
        ),
        (
            &["BLPOP", "0"],
            "ERR wrong number of arguments for 'blpop' command",
        ),
        (
            &["BRPOPLPUSH", "a", "0"],
            "ERR wrong number of arguments for 'brpoplpush' command",
        ),
        (
            &["BLMOVE", "a", "b", "LEFT", "0"],
            "ERR wrong number of arguments for 'blmove' command",
        ),
        (&["BLMOVE", "a", "b", "UP", "LEFT", "0"], "ERR syntax error"),
        (
            &["BLMPOP", "0", "0", "k", "LEFT"],
            "ERR numkeys should be greater than 0",
        ),
        (&["BLMPOP", "0", "1", "k", "MIN"], "ERR syntax error"),
        (
            &["BZMPOP", "-2", "1", "k", "MIN"],
            "ERR timeout is negative",
        ),
        (
            &["BLPOP", "s", "0"],
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        (
            &["BZPOPMIN", "s", "0"],
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        (
            &["BLMOVE", "s", "d", "LEFT", "LEFT", "0"],
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
    ] {
        assert_eq!(c.cmd(args), Reply::err(want), "{args:?}");
    }
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
}

#[test]
fn nothing_arrives_and_the_wait_times_out() {
    let env = env(2);
    let mut c = env.client();
    for args in [
        &["BLPOP", "nokey", "0.25"][..],
        &["BRPOP", "a", "b", "c", "0.25"],
        &["BZPOPMIN", "nokey", "0.25"],
        &["BLMOVE", "nokey", "dst", "LEFT", "LEFT", "0.25"],
        &["BRPOPLPUSH", "nokey", "dst", "0.25"],
        &["BLMPOP", "0.25", "1", "nokey", "LEFT"],
        &["BZMPOP", "0.25", "2", "nokey", "other", "MAX"],
    ] {
        let started = Instant::now();
        assert_eq!(c.cmd(args), Reply::NilArray, "{args:?}");
        let took = started.elapsed();
        assert!(
            took >= Duration::from_millis(240),
            "{args:?} gave up after {took:?}"
        );
        assert!(took < Duration::from_secs(3), "{args:?} took {took:?}");
    }
    // The shortest wait is still a wait, not "forever".
    assert_eq!(c.cmd(&["BLPOP", "nokey", "0.0001"]), Reply::NilArray);
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    assert_eq!(c.cmd(&["EXISTS", "dst"]), Reply::Int(0));
}

#[test]
fn a_resp3_wait_times_out_with_a_null() {
    let env = env(1);
    let mut c = env.client();
    c.send(&["HELLO", "3"]);
    c.send(&["BLPOP", "nokey", "0.05"]);
    c.send(&["BLMOVE", "nokey", "d", "LEFT", "LEFT", "0.05"]);
    let mut all = Vec::new();
    let mut buf = [0u8; 4096];
    while !all.ends_with(b"_\r\n_\r\n") {
        let n = c.rd.read(&mut buf).expect("read");
        assert!(n > 0, "closed after {:?}", String::from_utf8_lossy(&all));
        all.extend_from_slice(&buf[..n]);
    }
}

#[test]
fn every_way_of_adding_data_wakes_a_waiter() {
    let env = env(3);
    let mut other = env.client();
    let session = env.db.session().unwrap();
    type Feed<'a> = Box<dyn FnMut(&mut Client) + 'a>;
    let mut cases: Vec<(&[&str], Feed, Reply)> = vec![
        (
            &["BLPOP", "k1", "0"],
            Box::new(|o| drop(o.cmd(&["RPUSH", "k1", "v"]))),
            strs(&["k1", "v"]),
        ),
        (
            &["BRPOP", "a", "k2", "0"],
            Box::new(|o| drop(o.cmd(&["LPUSH", "k2", "x", "y"]))),
            strs(&["k2", "x"]),
        ),
        (
            &["BLPOP", "k3", "0"],
            Box::new(|_| drop(session.execute(&["RPUSH", "k3", "embedded"]))),
            strs(&["k3", "embedded"]),
        ),
        (
            &["BLPOP", "k4", "0"],
            Box::new(|o| {
                drop(o.cmd(&[
                    "EVAL",
                    "return redis.call('RPUSH', KEYS[1], 'lua')",
                    "1",
                    "k4",
                ]))
            }),
            strs(&["k4", "lua"]),
        ),
        (
            &["BLPOP", "k5", "0"],
            Box::new(|o| {
                o.cmd(&["MULTI"]);
                o.cmd(&["RPUSH", "k5", "txn"]);
                o.cmd(&["EXEC"]);
            }),
            strs(&["k5", "txn"]),
        ),
        (
            &["BLPOP", "k6", "0"],
            Box::new(|o| {
                o.cmd(&["RPUSH", "tmp6", "renamed"]);
                o.cmd(&["RENAME", "tmp6", "k6"]);
            }),
            strs(&["k6", "renamed"]),
        ),
        (
            &["BLPOP", "k7", "0"],
            Box::new(|o| {
                o.cmd(&["RPUSH", "src7", "moved"]);
                o.cmd(&["LMOVE", "src7", "k7", "LEFT", "RIGHT"]);
            }),
            strs(&["k7", "moved"]),
        ),
        (
            &["BZPOPMIN", "z8", "0"],
            Box::new(|o| drop(o.cmd(&["ZADD", "z8", "5", "m"]))),
            strs(&["z8", "m", "5"]),
        ),
        (
            &["BZPOPMAX", "z9", "0"],
            Box::new(|o| drop(o.cmd(&["ZINCRBY", "z9", "2", "m"]))),
            strs(&["z9", "m", "2"]),
        ),
        (
            &["BLMOVE", "k10", "d10", "LEFT", "RIGHT", "0"],
            Box::new(|o| drop(o.cmd(&["RPUSH", "k10", "v"]))),
            b("v"),
        ),
        (
            &["BRPOPLPUSH", "k11", "d11", "0"],
            Box::new(|o| drop(o.cmd(&["RPUSH", "k11", "v", "w"]))),
            b("w"),
        ),
        (
            &["BLMPOP", "0", "2", "a", "k12", "RIGHT", "COUNT", "5"],
            Box::new(|o| drop(o.cmd(&["RPUSH", "k12", "1", "2"]))),
            arr(&[b("k12"), strs(&["2", "1"])]),
        ),
        (
            &["BZMPOP", "0", "1", "z13", "MIN", "COUNT", "2"],
            Box::new(|o| drop(o.cmd(&["ZADD", "z13", "1", "a", "2", "b", "3", "c"]))),
            arr(&[b("z13"), arr(&[strs(&["a", "1"]), strs(&["b", "2"])])]),
        ),
        (
            &["BLPOP", "k14", "0"],
            Box::new(|o| {
                o.cmd(&["RPUSH", "c14", "copied"]);
                o.cmd(&["COPY", "c14", "k14"]);
            }),
            strs(&["k14", "copied"]),
        ),
        (
            &["BZPOPMIN", "z15", "0"],
            Box::new(|o| {
                o.cmd(&["ZADD", "s15", "7", "stored"]);
                o.cmd(&["ZUNIONSTORE", "z15", "1", "s15"]);
            }),
            strs(&["z15", "stored", "7"]),
        ),
    ];
    for (args, feed, want) in cases.iter_mut() {
        let mut waiter = env.client();
        waiter.send(args);
        assert!(waiter.silent_for(80), "{args:?} did not wait");
        feed(&mut other);
        assert_eq!(waiter.read(), *want, "{args:?}");
        assert_eq!(waiter.cmd(&["PING"]), Reply::Simple("PONG".into()));
    }
    assert_eq!(other.cmd(&["LRANGE", "d10", "0", "-1"]), strs(&["v"]));
    assert_eq!(other.cmd(&["LRANGE", "d11", "0", "-1"]), strs(&["w"]));
    assert_eq!(other.cmd(&["LLEN", "k12"]), Reply::Int(0));
    assert_eq!(other.cmd(&["ZCARD", "z13"]), Reply::Int(1));
}

#[test]
fn changes_that_add_nothing_leave_the_waiter_waiting() {
    let env = env(2);
    let mut other = env.client();
    let mut waiter = env.client();
    waiter.send(&["BLPOP", "k", "0"]);
    settle();
    // Another type under the key, a delete, an expiry, an empty transaction.
    other.cmd(&["SET", "k", "string"]);
    other.cmd(&["DEL", "k"]);
    other.cmd(&["SADD", "k", "member"]);
    other.cmd(&["EXPIRE", "k", "100"]);
    other.cmd(&["DEL", "k"]);
    other.cmd(&["FLUSHALL"]);
    assert!(waiter.silent_for(150));
    other.cmd(&["RPUSH", "k", "finally"]);
    assert_eq!(waiter.read(), strs(&["k", "finally"]));
}

#[test]
fn waiters_are_served_in_the_order_they_came() {
    for workers in [1, 4] {
        let env = env(workers);
        let mut waiters: Vec<Client> = (0..6)
            .map(|_| {
                let mut c = env.client();
                c.send(&["BLPOP", "q", "0"]);
                settle();
                c
            })
            .collect();
        let mut pusher = env.client();
        // One command, six values: the turn passes from waiter to waiter.
        assert_eq!(
            pusher.cmd(&["RPUSH", "q", "v0", "v1", "v2", "v3", "v4", "v5"]),
            Reply::Int(6)
        );
        for (i, waiter) in waiters.iter_mut().enumerate() {
            assert_eq!(
                waiter.read(),
                strs(&["q", &format!("v{i}")]),
                "workers {workers}"
            );
        }
        assert_eq!(pusher.cmd(&["LLEN", "q"]), Reply::Int(0));
    }
}

#[test]
fn a_waiter_that_leaves_passes_its_turn_on() {
    let env = env(3);
    let mut pusher = env.client();
    let mut gone = env.client();
    gone.send(&["BLPOP", "q", "0"]);
    settle();
    let mut timed = env.client();
    timed.send(&["BLPOP", "q", "0.2"]);
    settle();
    let mut stays = env.client();
    stays.send(&["BLPOP", "q", "other", "0"]);
    settle();
    assert_eq!(env.wait_conns(4, 5), 4);
    drop(gone);
    assert_eq!(env.wait_conns(3, 5), 3);
    assert_eq!(timed.read(), Reply::NilArray);
    pusher.cmd(&["RPUSH", "q", "one"]);
    assert_eq!(stays.read(), strs(&["q", "one"]));
    // Nobody is left: the value stays.
    pusher.cmd(&["RPUSH", "q", "two"]);
    assert_eq!(pusher.cmd(&["LRANGE", "q", "0", "-1"]), strs(&["two"]));
    assert_eq!(timed.cmd(&["BLPOP", "q", "0"]), strs(&["q", "two"]));
}

#[test]
fn a_waiter_served_from_one_key_leaves_the_lines_of_the_others() {
    let env = env(2);
    let mut pusher = env.client();
    let mut first = env.client();
    first.send(&["BLPOP", "a", "b", "0"]);
    settle();
    let mut second = env.client();
    second.send(&["BLPOP", "b", "0"]);
    settle();
    pusher.cmd(&["RPUSH", "a", "for-first"]);
    assert_eq!(first.read(), strs(&["a", "for-first"]));
    pusher.cmd(&["RPUSH", "b", "for-second"]);
    assert_eq!(second.read(), strs(&["b", "for-second"]));
}

#[test]
fn commands_pipelined_behind_a_wait_run_after_it() {
    let env = env(2);
    let mut c = env.client();
    let mut bytes = wire_str(&["BLPOP", "q", "0"]);
    bytes.extend(wire_str(&["PING"]));
    bytes.extend(wire_str(&["BLPOP", "q", "0"]));
    bytes.extend(wire_str(&["LLEN", "q"]));
    c.send_raw(&bytes);
    assert!(c.silent_for(120));
    let mut pusher = env.client();
    pusher.cmd(&["RPUSH", "q", "1"]);
    assert_eq!(c.read(), strs(&["q", "1"]));
    assert_eq!(c.read(), Reply::Simple("PONG".into()));
    assert!(c.silent_for(120));
    pusher.cmd(&["RPUSH", "q", "2", "3"]);
    assert_eq!(c.read(), strs(&["q", "2"]));
    assert_eq!(c.read(), Reply::Int(1));
}

#[test]
fn inside_transactions_and_scripts_nothing_waits() {
    let env = env(2);
    let mut c = env.client();
    c.cmd(&["RPUSH", "has", "v"]);
    c.cmd(&["MULTI"]);
    for args in [
        &["BLPOP", "nokey", "0"][..],
        &["BLPOP", "has", "0"],
        &["BLMOVE", "nokey", "d", "LEFT", "LEFT", "0"],
        &["BZPOPMIN", "nokey", "0"],
        &["BLMPOP", "0", "1", "nokey", "LEFT"],
    ] {
        assert_eq!(c.cmd(args), Reply::Simple("QUEUED".into()));
    }
    assert_eq!(
        c.cmd(&["EXEC"]),
        arr(&[
            Reply::NilArray,
            strs(&["has", "v"]),
            Reply::Nil,
            Reply::NilArray,
            Reply::NilArray
        ])
    );
    let script = "return {redis.call('BLPOP', KEYS[1], 0) == false, redis.call('RPUSH', KEYS[1], 'x'), redis.call('BLPOP', KEYS[1], 0)}";
    assert_eq!(
        c.cmd(&["EVAL", script, "1", "lk"]),
        arr(&[Reply::Int(1), Reply::Int(1), strs(&["lk", "x"])])
    );
    // The embedded API has no connection to park.
    let session = env.db.session().unwrap();
    assert_eq!(session.execute(&["BLPOP", "nokey", "0"]), Reply::NilArray);
}

#[test]
fn a_blocked_chain_moves_every_item_once() {
    watchdog("blocked chain", 120, || {
        let env = Arc::new(env(4));
        const ITEMS: usize = 3000;
        const STAGES: usize = 4;
        let done = Arc::new(AtomicBool::new(false));
        let mut movers = Vec::new();
        // Stage i moves from s{i} to s{i+1}; keys land on different shards.
        for stage in 0..STAGES {
            for _ in 0..3 {
                let (env, done) = (env.clone(), done.clone());
                movers.push(thread::spawn(move || {
                    let mut c = env.client();
                    let (src, dst) = (format!("s{stage}"), format!("s{}", stage + 1));
                    while !done.load(SeqCst) {
                        let got = c.cmd(&["BLMOVE", &src, &dst, "LEFT", "RIGHT", "0.05"]);
                        assert!(matches!(got, Reply::Bulk(_) | Reply::NilArray), "{got:?}");
                    }
                }));
            }
        }
        let seen = Arc::new(AtomicUsize::new(0));
        let mut sinks = Vec::new();
        for _ in 0..4 {
            let (env, done, seen) = (env.clone(), done.clone(), seen.clone());
            sinks.push(thread::spawn(move || {
                let mut c = env.client();
                let mut mine = Vec::new();
                let last = format!("s{STAGES}");
                while !done.load(SeqCst) {
                    match c.cmd(&["BLPOP", "nothing-here", &last, "0.05"]) {
                        Reply::Array(pair) => {
                            assert_eq!(pair[0], Reply::bulk(last.as_str()));
                            mine.push(pair[1].clone());
                            seen.fetch_add(1, SeqCst);
                        }
                        Reply::NilArray => {}
                        other => panic!("{other:?}"),
                    }
                }
                mine
            }));
        }
        let mut feeders = Vec::new();
        for feeder in 0..3 {
            let env = env.clone();
            feeders.push(thread::spawn(move || {
                let mut c = env.client();
                for i in (feeder..ITEMS).step_by(3) {
                    c.cmd(&["RPUSH", "s0", &format!("item{i}")]);
                    if i % 50 == 0 {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }));
        }
        for f in feeders {
            f.join().unwrap();
        }
        assert!(
            wait_until(60, || seen.load(SeqCst) == ITEMS),
            "only {} of {ITEMS} items arrived",
            seen.load(SeqCst)
        );
        done.store(true, SeqCst);
        for m in movers {
            m.join().unwrap();
        }
        let mut all = HashSet::new();
        for s in sinks {
            for item in s.join().unwrap() {
                assert!(all.insert(format!("{item:?}")), "{item:?} delivered twice");
            }
        }
        assert_eq!(all.len(), ITEMS);
        let mut c = env.client();
        for stage in 0..=STAGES {
            assert_eq!(c.cmd(&["LLEN", &format!("s{stage}")]), Reply::Int(0));
        }
    });
}

#[test]
fn many_waiters_many_keys_nothing_lost_nothing_stuck() {
    watchdog("many waiters", 120, || {
        let env = Arc::new(env(4));
        const WAITERS: usize = 40;
        const ROUNDS: usize = 30;
        let mut handles = Vec::new();
        for w in 0..WAITERS {
            let env = env.clone();
            handles.push(thread::spawn(move || {
                let mut c = env.client();
                let mut got = Vec::new();
                for _ in 0..ROUNDS {
                    // Different key orders, different commands, same pool.
                    let reply = match w % 4 {
                        0 => c.cmd(&["BLPOP", "p0", "p1", "p2", "0"]),
                        1 => c.cmd(&["BRPOP", "p2", "p1", "p0", "0"]),
                        2 => c.cmd(&["BLMPOP", "0", "2", "p1", "p2", "LEFT"]),
                        _ => c.cmd(&["BLPOP", "p0", "0"]),
                    };
                    let item = match reply {
                        Reply::Array(mut parts) => match parts.pop() {
                            Some(Reply::Array(mut inner)) => inner.pop().unwrap(),
                            Some(value) => value,
                            None => panic!("empty reply"),
                        },
                        other => panic!("{other:?}"),
                    };
                    got.push(item);
                }
                got
            }));
        }
        let mut pusher = env.client();
        let total = WAITERS * ROUNDS;
        // Who takes what is up to the race, so some keys end with more
        // than their readers want: keep feeding all three until every
        // waiter has had its rounds.
        let mut pushed = 0;
        while handles.iter().any(|h| !h.is_finished()) {
            pusher.cmd(&["RPUSH", &format!("p{}", pushed % 3), &format!("v{pushed}")]);
            pushed += 1;
            if pushed > total {
                thread::sleep(Duration::from_micros(200));
            }
            assert!(
                pushed < total * 200,
                "waiters are stuck with data under their keys"
            );
        }
        let mut all = HashSet::new();
        for h in handles {
            for item in h.join().unwrap() {
                assert!(all.insert(format!("{item:?}")), "{item:?} delivered twice");
            }
        }
        assert_eq!(all.len(), total);
        let mut left = 0;
        for key in ["p0", "p1", "p2"] {
            let Reply::Array(items) = pusher.cmd(&["LRANGE", key, "0", "-1"]) else {
                panic!("not a list");
            };
            for item in items {
                assert!(
                    all.insert(format!("{item:?}")),
                    "{item:?} delivered and kept"
                );
                left += 1;
            }
        }
        assert_eq!(total + left, pushed);
    });
}

#[test]
fn waiters_come_and_go_while_data_flows() {
    watchdog("churn", 120, || {
        let env = Arc::new(env(3));
        let stop = Arc::new(AtomicBool::new(false));
        let taken = Arc::new(AtomicUsize::new(0));
        let mut threads = Vec::new();
        for t in 0..8 {
            let (env, stop, taken) = (env.clone(), stop.clone(), taken.clone());
            threads.push(thread::spawn(move || {
                let mut n = 0u32;
                while !stop.load(SeqCst) {
                    n += 1;
                    // Windows runs out of client ports when connections are
                    // opened as fast as the loop can go.
                    thread::sleep(Duration::from_millis(4));
                    let mut c = env.client();
                    match (t + n) % 3 {
                        // Blocks, then hangs up without waiting for an answer.
                        0 => {
                            c.send(&["BLPOP", "churn", "0"]);
                            thread::sleep(Duration::from_millis(3));
                        }
                        // Short wait.
                        1 => {
                            if let Reply::Array(_) = c.cmd(&["BLPOP", "churn", "0.01"]) {
                                taken.fetch_add(1, SeqCst);
                            }
                        }
                        // Pipelines behind the wait and leaves.
                        _ => {
                            let mut bytes = wire_str(&["BRPOP", "churn", "0.02"]);
                            bytes.extend(wire_str(&["PING"]));
                            c.send_raw(&bytes);
                            if let Reply::Array(_) = c.read() {
                                taken.fetch_add(1, SeqCst);
                            }
                        }
                    }
                }
            }));
        }
        let mut pusher = env.client();
        let started = Instant::now();
        let mut pushed = 0;
        while started.elapsed() < Duration::from_secs(3) {
            pusher.cmd(&["RPUSH", "churn", "x"]);
            pushed += 1;
            if pushed % 20 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
        }
        stop.store(true, SeqCst);
        for t in threads {
            t.join().unwrap();
        }
        // Whatever a vanished waiter was handed is gone with it, as in
        // Redis; nothing may be handed out twice or invented.
        let left = match pusher.cmd(&["LLEN", "churn"]) {
            Reply::Int(n) => n as usize,
            other => panic!("{other:?}"),
        };
        assert!(
            taken.load(SeqCst) + left <= pushed,
            "{} taken, {left} left, {pushed} pushed",
            taken.load(SeqCst)
        );
        // The server is still healthy and nobody is stuck in line.
        assert_eq!(env.wait_conns(1, 10), 1);
        let mut waiter = env.client();
        pusher.cmd(&["DEL", "churn"]);
        waiter.send(&["BLPOP", "churn", "0"]);
        settle();
        pusher.cmd(&["RPUSH", "churn", "last"]);
        assert_eq!(waiter.read(), strs(&["churn", "last"]));
    });
}

#[test]
fn shutdown_does_not_wait_for_waiters() {
    watchdog("shutdown with waiters", 30, || {
        let env = env(2);
        let mut waiters: Vec<Client> = (0..5)
            .map(|i| {
                let mut c = env.client();
                c.send(&["BLPOP", &format!("k{i}"), "shared", "0"]);
                c
            })
            .collect();
        settle();
        let Env { db, server, .. } = env;
        server.shutdown();
        for waiter in &mut waiters {
            assert!(waiter.try_read().is_err());
        }
        // The engine carries on, and a new server on it works.
        let session = db.session().unwrap();
        assert_eq!(session.execute(&["RPUSH", "shared", "v"]), Reply::Int(1));
        let server = db.listen(server_cfg(1, 64)).unwrap();
        let mut c = Client::connect(server.local_addr());
        assert_eq!(c.cmd(&["BLPOP", "shared", "0"]), strs(&["shared", "v"]));
        c.send(&["BLPOP", "shared", "0"]);
        settle();
        session.execute(&["RPUSH", "shared", "again"]);
        assert_eq!(c.read(), strs(&["shared", "again"]));
    });
}

#[test]
fn a_wait_sees_data_that_arrives_while_the_gate_is_closed() {
    watchdog("wait across exclusive work", 60, || {
        let env = Arc::new(env(2));
        let mut waiter = env.client();
        let stop = Arc::new(AtomicBool::new(false));
        // Scripts keep the exclusive side busy while waiters come and go.
        let noise = {
            let (env, stop) = (env.clone(), stop.clone());
            thread::spawn(move || {
                let mut c = env.client();
                while !stop.load(SeqCst) {
                    c.cmd(&[
                        "EVAL",
                        "local x = 0 for i = 1, 20000 do x = x + i end return x",
                        "0",
                    ]);
                }
            })
        };
        let mut pusher = env.client();
        for i in 0..300 {
            let value = format!("v{i}");
            if i % 2 == 0 {
                waiter.send(&["BLPOP", "g", "0"]);
                pusher.cmd(&["RPUSH", "g", &value]);
            } else {
                pusher.cmd(&["RPUSH", "g", &value]);
                waiter.send(&["BLPOP", "g", "0"]);
            }
            assert_eq!(waiter.read(), strs(&["g", &value]), "round {i}");
        }
        stop.store(true, SeqCst);
        noise.join().unwrap();
    });
}
