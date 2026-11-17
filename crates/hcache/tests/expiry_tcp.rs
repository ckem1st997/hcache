//! Transactions, WATCH and background expiry through the TCP server, in
//! RESP2 and RESP3.

mod net;

use hcache::Reply;
use net::*;
use std::io::Read;
use std::thread;

fn ok() -> Reply {
    Reply::Ok
}

fn queued() -> Reply {
    Reply::Simple("QUEUED".into())
}

fn env(shards: usize, workers: usize, cap: usize) -> Env {
    Env::new(engine_cfg(shards), server_cfg(workers, cap))
}

/// Read raw bytes until they end with `tail`.
fn read_through(c: &mut Client, tail: &[u8]) -> Vec<u8> {
    let mut got = Vec::new();
    let mut byte = [0u8; 1];
    while !got.ends_with(tail) {
        c.rd.read_exact(&mut byte).expect("read");
        got.push(byte[0]);
        assert!(
            got.len() < 1 << 20,
            "no {:?} in the reply",
            String::from_utf8_lossy(tail)
        );
    }
    got
}

fn read_exactly(c: &mut Client, want: &[u8]) {
    let mut got = vec![0u8; want.len()];
    c.rd.read_exact(&mut got).expect("read");
    assert_eq!(String::from_utf8_lossy(&got), String::from_utf8_lossy(want));
}

#[test]
fn transaction_and_watch_over_resp2() {
    watchdog("tcp-txn-resp2", 30, || {
        let env = env(4, 2, 64);
        let mut a = env.client();
        let mut b = env.client();
        assert_eq!(a.cmd(&["MULTI"]), ok());
        assert_eq!(a.cmd(&["SET", "k", "1"]), queued());
        assert_eq!(a.cmd(&["HSET", "h", "f", "v"]), queued());
        assert_eq!(a.cmd(&["HGETALL", "h"]), queued());
        assert_eq!(a.cmd(&["GET", "nokey"]), queued());
        // Nothing is visible before EXEC.
        assert_eq!(b.cmd(&["EXISTS", "k", "h"]), Reply::Int(0));
        assert_eq!(
            a.cmd(&["EXEC"]),
            Reply::Array(vec![
                ok(),
                Reply::Int(1),
                Reply::Array(vec![Reply::bulk("f"), Reply::bulk("v")]),
                Reply::Nil,
            ])
        );
        // WATCH across connections: an aborted EXEC is a null array.
        assert_eq!(a.cmd(&["WATCH", "k"]), ok());
        assert_eq!(b.cmd(&["INCR", "k"]), Reply::Int(2));
        assert_eq!(a.cmd(&["MULTI"]), ok());
        assert_eq!(a.cmd(&["SET", "k", "lost"]), queued());
        a.send(&["EXEC"]);
        read_exactly(&mut a, b"*-1\r\n");
        assert_eq!(b.cmd(&["GET", "k"]), Reply::bulk("2"));
        // Errors keep their place.
        assert_eq!(a.cmd(&["MULTI"]), ok());
        assert!(error_text(&a.cmd(&["NOPE"])).is_some());
        let got = a.cmd(&["EXEC"]);
        assert!(
            error_text(&got).is_some_and(|e| e.starts_with("EXECABORT")),
            "{}",
            short(&got)
        );
        assert_eq!(a.cmd(&["PING"]), Reply::Simple("PONG".into()));
    });
}

#[test]
fn transaction_over_resp3_keeps_maps_and_nulls() {
    watchdog("tcp-txn-resp3", 30, || {
        let env = env(4, 2, 64);
        let mut a = env.client();
        let mut b = env.client();
        a.send(&["HELLO", "3"]);
        a.send(&["PING"]);
        let hello = read_through(&mut a, b"+PONG\r\n");
        assert_eq!(hello[0], b'%', "HELLO 3 must answer with a map");
        a.send(&["MULTI"]);
        a.send(&["HSET", "h", "f", "v"]);
        a.send(&["HGETALL", "h"]);
        a.send(&["GET", "nokey"]);
        a.send(&["HGETALL", "nokey"]);
        a.send(&["EXEC"]);
        read_exactly(
            &mut a,
            b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n\
              *4\r\n:1\r\n%1\r\n$1\r\nf\r\n$1\r\nv\r\n_\r\n%0\r\n",
        );
        // An aborted EXEC is the RESP3 null.
        a.send(&["WATCH", "h"]);
        read_exactly(&mut a, b"+OK\r\n");
        assert_eq!(b.cmd(&["HSET", "h", "g", "1"]), Reply::Int(1));
        a.send(&["MULTI"]);
        a.send(&["DEL", "h"]);
        a.send(&["EXEC"]);
        read_exactly(&mut a, b"+OK\r\n+QUEUED\r\n_\r\n");
        // The connection that stayed on RESP2 is unaffected.
        assert_eq!(b.cmd(&["HLEN", "h"]), Reply::Int(2));
        // Outside a transaction HGETALL is still a map, and the command
        // after EXEC is not mistaken for one.
        a.send(&["DBSIZE"]);
        a.send(&["HGETALL", "nokey"]);
        read_exactly(&mut a, b":1\r\n%0\r\n");
    });
}

#[test]
fn a_dropped_connection_applies_nothing_it_queued() {
    watchdog("tcp-txn-drop", 30, || {
        let env = env(4, 2, 64);
        let mut b = env.client();
        for how in 0..3 {
            let mut a = env.client();
            assert_eq!(a.cmd(&["WATCH", "k"]), ok());
            assert_eq!(a.cmd(&["MULTI"]), ok());
            assert_eq!(a.cmd(&["SET", "k", "ghost"]), queued());
            assert_eq!(a.cmd(&["INCR", "n"]), queued());
            match how {
                0 => drop(a),
                1 => {
                    // QUIT is obeyed even inside MULTI.
                    assert_eq!(a.cmd(&["QUIT"]), ok());
                    assert!(a.read_until_closed().is_empty());
                }
                _ => {
                    // Half of an EXEC, then gone.
                    a.send_raw(b"*1\r\n$4\r\nEX");
                    drop(a);
                }
            }
            assert!(
                wait_until(10, || env.wait_conns(1, 1) == 1),
                "connection not released"
            );
            assert_eq!(b.cmd(&["EXISTS", "k", "n"]), Reply::Int(0), "case {how}");
        }
        // A new connection starts with no transaction.
        let mut a = env.client();
        assert!(error_text(&a.cmd(&["EXEC"])).is_some());
    });
}

#[test]
fn pipelined_transactions_keep_order_past_the_pipeline_cap() {
    watchdog("tcp-txn-pipeline", 120, || {
        for (workers, cap) in [(1, 1), (1, 7), (4, 64)] {
            let env = env(8, workers, cap);
            const ROUNDS: usize = 3_000;
            let writers: Vec<_> = (0..3)
                .map(|t| {
                    let mut c = env.client();
                    thread::spawn(move || {
                        let key = format!("ctr{t}");
                        let mut batch = Vec::new();
                        for _ in 0..ROUNDS {
                            batch.extend(wire_str(&["MULTI"]));
                            batch.extend(wire_str(&["INCR", &key]));
                            batch.extend(wire_str(&["INCR", "shared"]));
                            batch.extend(wire_str(&["INCR", &key]));
                            batch.extend(wire_str(&["EXEC"]));
                            batch.extend(wire_str(&["GET", &key]));
                        }
                        let mut wr = c.rd.get_ref().try_clone().expect("clone");
                        let sender = thread::spawn(move || {
                            use std::io::Write;
                            wr.write_all(&batch).expect("write");
                        });
                        let mut last_shared = 0;
                        for i in 0..ROUNDS as i64 {
                            assert_eq!(c.read(), Reply::Ok);
                            for _ in 0..3 {
                                assert_eq!(c.read(), Reply::Simple("QUEUED".into()));
                            }
                            let Reply::Array(r) = c.read() else {
                                panic!("EXEC {i} of writer {t}")
                            };
                            assert_eq!(r[0], Reply::Int(2 * i + 1));
                            assert_eq!(r[2], Reply::Int(2 * i + 2));
                            let Reply::Int(shared) = r[1] else {
                                panic!("shared counter")
                            };
                            assert!(shared > last_shared);
                            last_shared = shared;
                            assert_eq!(c.read(), Reply::bulk((2 * i + 2).to_string()));
                        }
                        sender.join().expect("sender");
                    })
                })
                .collect();
            for w in writers {
                w.join().expect("writer");
            }
            let mut c = env.client();
            assert_eq!(
                c.cmd(&["GET", "shared"]),
                Reply::bulk((3 * ROUNDS).to_string()),
                "{workers} workers, cap {cap}"
            );
        }
    });
}

#[test]
fn watch_counter_over_tcp_loses_no_update() {
    watchdog("tcp-watch-cas", 120, || {
        let env = env(4, 3, 16);
        const CONNS: usize = 8;
        const EACH: usize = 200;
        let workers: Vec<_> = (0..CONNS)
            .map(|_| {
                let mut c = env.client();
                thread::spawn(move || {
                    for _ in 0..EACH {
                        loop {
                            assert_eq!(c.cmd(&["WATCH", "counter"]), Reply::Ok);
                            let now = match c.cmd(&["GET", "counter"]) {
                                Reply::Nil => 0,
                                Reply::Bulk(b) => String::from_utf8(b)
                                    .expect("utf8")
                                    .parse::<u64>()
                                    .expect("number"),
                                other => panic!("GET: {}", short(&other)),
                            };
                            // MULTI, SET and EXEC in one write.
                            let mut batch = wire_str(&["MULTI"]);
                            batch.extend(wire_str(&["SET", "counter", &(now + 1).to_string()]));
                            batch.extend(wire_str(&["EXEC"]));
                            c.send_raw(&batch);
                            assert_eq!(c.read(), Reply::Ok);
                            assert_eq!(c.read(), Reply::Simple("QUEUED".into()));
                            match c.read() {
                                Reply::NilArray => continue,
                                Reply::Array(r) => {
                                    assert_eq!(r, vec![Reply::Ok]);
                                    break;
                                }
                                other => panic!("EXEC: {}", short(&other)),
                            }
                        }
                    }
                })
            })
            .collect();
        for w in workers {
            w.join().expect("worker");
        }
        let mut c = env.client();
        assert_eq!(
            c.cmd(&["GET", "counter"]),
            Reply::bulk((CONNS * EACH).to_string())
        );
    });
}

#[test]
fn a_listening_server_expires_keys_nobody_touches() {
    watchdog("tcp-bg-expiry", 60, || {
        let env = env(16, 2, 64);
        let mut c = env.client();
        const KEYS: usize = 20_000;
        let mut batch = Vec::new();
        for i in 0..KEYS {
            batch.extend(wire_str(&["SET", &format!("k{i}"), "v", "PX", "300"]));
        }
        batch.extend(wire_str(&["SET", "stays", "v"]));
        batch.extend(wire_str(&["SET", "later", "v", "EX", "1000"]));
        c.send_raw(&batch);
        for _ in 0..KEYS + 2 {
            assert_eq!(c.read(), Reply::Ok);
        }
        let Reply::Int(before) = c.cmd(&["DBSIZE"]) else {
            panic!("DBSIZE")
        };
        assert!(before > 2, "keys expired before they could be counted");
        // DBSIZE reads no key, so only the background pass can shrink it.
        let mut last = before;
        let gone = wait_until(30, || {
            let Reply::Int(n) = c.cmd(&["DBSIZE"]) else {
                panic!("DBSIZE")
            };
            last = n;
            n == 2
        });
        assert!(gone, "{last} keys left after 30 s");
        assert_eq!(c.cmd(&["GET", "stays"]), Reply::bulk("v"));
        let Reply::Int(ttl) = c.cmd(&["TTL", "later"]) else {
            panic!("TTL")
        };
        assert!((990..=1000).contains(&ttl), "TTL {ttl}");
    });
}

#[test]
fn scan_over_tcp_while_other_connections_write() {
    watchdog("tcp-scan", 60, || {
        let env = env(8, 2, 16);
        let mut c = env.client();
        let mut batch = Vec::new();
        for i in 0..2_000 {
            batch.extend(wire_str(&["SET", &format!("stable:{i}"), "v"]));
        }
        c.send_raw(&batch);
        for _ in 0..2_000 {
            assert_eq!(c.read(), Reply::Ok);
        }
        let mut w = env.client();
        let writer = thread::spawn(move || {
            for round in 0..20 {
                let mut batch = Vec::new();
                for i in 0..2_000 {
                    batch.extend(wire_str(&["SET", &format!("churn:{i}"), "v", "PX", "20"]));
                    if round % 2 == 1 {
                        batch.extend(wire_str(&["DEL", &format!("churn:{i}")]));
                    }
                }
                let n = if round % 2 == 1 { 4_000 } else { 2_000 };
                w.send_raw(&batch);
                for _ in 0..n {
                    w.read();
                }
            }
        });
        for count in ["1", "13", "500", "100000"].iter().cycle().take(12) {
            let mut cursor = "0".to_string();
            let mut seen = std::collections::BTreeSet::new();
            loop {
                let args = ["SCAN", cursor.as_str(), "MATCH", "stable:*", "COUNT", count];
                let Reply::Array(mut r) = c.cmd(&args) else {
                    panic!("SCAN")
                };
                let Some(Reply::Array(items)) = r.pop() else {
                    panic!("SCAN items")
                };
                let Some(Reply::Bulk(next)) = r.pop() else {
                    panic!("SCAN cursor")
                };
                for item in items {
                    let Reply::Bulk(key) = item else {
                        panic!("SCAN key")
                    };
                    seen.insert(key);
                }
                cursor = String::from_utf8(next).expect("cursor");
                if cursor == "0" {
                    break;
                }
            }
            assert_eq!(seen.len(), 2_000, "COUNT {count}");
        }
        writer.join().expect("writer");
    });
}
