//! Adversarial tests of the TCP server: protocol abuse, delivery, binary
//! safety, back-pressure, connection lifecycle, fairness, scripts over TCP
//! and shutdown. Every test has a watchdog and every socket a timeout, so a
//! hang is reported as a failure.

mod net;

use hcache::{Config, Reply, ServerConfig};
use hcache_core::resp::{encode_command, Limits};
use hcache_core::testutil::Rng;
use net::*;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

fn x(r: Reply) -> Expect {
    Expect::Exact(r)
}

fn bulk(s: &str) -> Expect {
    x(Reply::bulk(s))
}

fn pong() -> Reply {
    Reply::Simple("PONG".into())
}

fn ping(c: &mut Client) {
    assert_eq!(c.cmd(&["PING"]), pong());
}

fn is_proto_err(r: &Reply) -> bool {
    error_text(r).is_some_and(|t| t.starts_with("ERR Protocol error"))
}

fn limits_env(limits: Limits) -> Env {
    let mut scfg = server_cfg(2, 8);
    scfg.limits = limits;
    Env::new(engine_cfg(4), scfg)
}

fn join_all<T>(handles: Vec<thread::JoinHandle<T>>) -> Vec<T> {
    handles
        .into_iter()
        .map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
        .collect()
}

// ---------------------------------------------------------------------
// Protocol abuse
// ---------------------------------------------------------------------

#[test]
fn garbage_and_mutated_streams_end_with_valid_replies_then_close() {
    watchdog("garbage", 120, || {
        let env = Env::new(engine_cfg(4), server_cfg(2, 8));
        let mut rng = Rng::new(0xBAD_5EED);
        let mut base = Workload::new(7, "g/");
        base.allow_eval = false;
        for round in 0..300 {
            let payload = if round % 2 == 0 {
                let n = 1 + rng.below(2000) as usize;
                rng.bytes(n)
            } else {
                let (mut wire, _) = base.batch(20);
                for _ in 0..1 + rng.below(8) {
                    let i = rng.below(wire.len() as u64) as usize;
                    wire[i] = rng.next_u64() as u8;
                }
                wire
            };
            let mut c = env.client_with(Duration::from_secs(10));
            c.send_raw(&payload);
            c.half_close();
            // Panics when the reply stream is not valid RESP or the
            // connection is not closed.
            c.read_until_closed();
        }
        assert_eq!(env.wait_conns(0, 5), 0, "connections leaked after garbage");
        ping(&mut env.client());
    });
}

#[test]
fn http_requests_are_not_executed_and_the_connection_ends() {
    watchdog("http", 60, || {
        let env = Env::new(engine_cfg(2), server_cfg(1, 8));
        let get = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n";
        let mut c = env.client();
        c.send_raw(get);
        let replies = c.read_until_closed();
        assert!(
            replies.iter().all(Reply::is_error) && replies.len() <= 1,
            "GET / HTTP: {replies:?}"
        );

        let post = b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 6\r\n\r\nPING\r\n";
        let mut c = env.client();
        c.send_raw(post);
        assert_eq!(c.read_until_closed(), vec![], "POST must not run commands");

        let mut c = env.client();
        c.send_raw(b"post / http/1.1\r\n\r\n");
        assert_eq!(c.read_until_closed(), vec![]);

        let mut c = env.client();
        c.send_raw(b"Host: x\r\n");
        assert_eq!(c.read_until_closed(), vec![]);

        let mut c = env.client();
        c.send_raw(b"PING\r\nPOST / HTTP/1.1\r\n\r\n");
        assert_eq!(c.read_until_closed(), vec![pong()]);

        assert_eq!(env.wait_conns(0, 5), 0);
        ping(&mut env.client());
    });
}

#[test]
fn bad_header_is_answered_after_earlier_commands_then_closes() {
    watchdog("bad-header", 60, || {
        let env = Env::new(engine_cfg(2), server_cfg(2, 4));
        let mut too_long = b"*".to_vec();
        too_long.extend(std::iter::repeat_n(b'1', 40));
        let bad: [&[u8]; 9] = [
            b"*abc\r\n",
            b"*3\r\n$x\r\n",
            b"*1\r\nfoo\r\n",
            b"*1\r\n$3\r\nabcde\r\n",
            b"*1\r\n$-5\r\n",
            b"*99999999999999999999\r\n",
            b"*2\r\n$3\r\nGET\r\n:1\r\n",
            b"*1\n$4\r\nPING\r\n",
            &too_long,
        ];
        for header in bad {
            for k in [0usize, 1, 5, 50] {
                let mut payload = Vec::new();
                for i in 0..k {
                    encode_command(&["ECHO", i.to_string().as_str()], &mut payload);
                }
                payload.extend_from_slice(header);
                let mut c = env.client();
                c.send_raw(&payload);
                let replies = c.read_until_closed();
                assert_eq!(replies.len(), k + 1, "header {header:?} k={k}: {replies:?}");
                for (i, r) in replies[..k].iter().enumerate() {
                    assert_eq!(
                        r,
                        &Reply::bulk(i.to_string()),
                        "header {header:?} reply {i}"
                    );
                }
                assert!(
                    is_proto_err(&replies[k]),
                    "header {header:?} k={k}: last reply {:?}",
                    replies[k]
                );
            }
        }
        assert_eq!(env.wait_conns(0, 5), 0);
    });
}

#[test]
fn huge_declared_lengths_are_rejected_or_wait_without_allocating() {
    let _h = heavy();
    watchdog("huge", 60, || {
        let env = Env::new(engine_cfg(4), server_cfg(2, 8));
        let payloads: [&[u8]; 4] = [
            b"*2147483647\r\n",
            b"*1\r\n$536870913\r\n",
            b"*1\r\n$4294967296\r\n",
            b"*1\r\n$9223372036854775807\r\n",
        ];
        for payload in payloads {
            let mut c = env.client();
            c.send_raw(payload);
            let replies = c.read_until_closed();
            assert_eq!(replies.len(), 1, "{payload:?}: {replies:?}");
            assert!(is_proto_err(&replies[0]), "{payload:?}: {replies:?}");
        }
        drop(env);

        // With the limits lifted the declared sizes are legal: the server
        // must just wait, without reserving them.
        let mut scfg = server_cfg(2, 8);
        scfg.limits = Limits {
            max_args: 1 << 31,
            max_bulk: 1 << 30,
            max_inline: 64 * 1024,
            max_request: 1 << 33,
        };
        let env = Env::new(engine_cfg(4), scfg);
        let mem0 = private_bytes();
        let mut waiters = Vec::new();
        for i in 0..8 {
            let mut c = env.client();
            if i % 2 == 0 {
                c.send_raw(b"*2147483647\r\n$3\r\nGET\r\n");
            } else {
                c.send_raw(b"*2\r\n$3\r\nGET\r\n$536870912\r\n");
                c.send_raw(&[b'x'; 1000]);
            }
            waiters.push(c);
        }
        thread::sleep(Duration::from_millis(300));
        let t = Instant::now();
        let mut other = env.client();
        ping(&mut other);
        assert!(
            t.elapsed() < Duration::from_secs(1),
            "slow while others wait"
        );
        if let (Some(a), Some(b)) = (mem0, private_bytes()) {
            let delta = b.saturating_sub(a);
            eprintln!("huge declared lengths: memory delta {} KiB", delta >> 10);
            assert!(delta < 64 << 20, "memory grew by {delta} bytes");
        }
        for w in &mut waiters {
            assert!(
                w.silent_for(200),
                "server answered or closed an incomplete command"
            );
        }
        drop(waiters);
        drop(other);
        assert_eq!(env.wait_conns(0, 5), 0, "waiting connections not released");
    });
}

#[test]
fn limits_are_enforced_exactly_at_the_boundary() {
    watchdog("limits", 60, || {
        let env = limits_env(Limits {
            max_args: 4,
            max_bulk: 16,
            max_inline: 32,
            max_request: 1 << 20,
        });

        // max_args: 4 arguments pass, 5 do not.
        let mut c = env.client();
        assert_eq!(
            c.cmd(&["MGET", "a", "b", "c"]),
            Reply::Array(vec![Reply::Nil; 3])
        );
        ping(&mut c);
        drop(c);
        let mut c = env.client();
        c.send(&["MGET", "a", "b", "c", "d"]);
        let r = c.read_until_closed();
        assert!(r.len() == 1 && is_proto_err(&r[0]), "5 args: {r:?}");

        // max_bulk: 16 bytes pass, 17 do not.
        drop(c);
        let mut c = env.client();
        assert_eq!(
            c.cmd(&["ECHO", "0123456789abcdef"]),
            Reply::bulk("0123456789abcdef")
        );
        drop(c);
        let mut c = env.client();
        c.send(&["ECHO", "0123456789abcdefg"]);
        let r = c.read_until_closed();
        assert!(r.len() == 1 && is_proto_err(&r[0]), "17 byte bulk: {r:?}");

        // max_inline: a 32 byte line passes, 33 does not.
        drop(c);
        let mut c = env.client();
        let mut line = b"ECHO ".to_vec();
        line.extend(std::iter::repeat_n(b'a', 27));
        line.push(b'\n');
        c.send_raw(&line);
        assert_eq!(c.read(), Reply::bulk(vec![b'a'; 27]));
        ping(&mut c);
        drop(c);
        let mut c = env.client();
        let mut line = b"ECHO ".to_vec();
        line.extend(std::iter::repeat_n(b'a', 28));
        line.push(b'\n');
        c.send_raw(&line);
        let r = c.read_until_closed();
        assert!(
            r.len() == 1 && is_proto_err(&r[0]),
            "33 byte inline line: {r:?}"
        );

        // A 32 byte line still waiting for its newline is fine; a 33rd
        // byte without newline is not.
        drop(c);
        let mut c = env.client();
        c.send_raw(&[b'a'; 32]);
        assert!(c.silent_for(300), "reply to an unfinished inline line");
        c.send_raw(b"\n");
        let r = c.read();
        assert!(
            error_text(&r).is_some_and(|t| t.starts_with("ERR unknown command")),
            "{r:?}"
        );
        ping(&mut c);
        drop(c);
        let mut c = env.client();
        c.send_raw(&[b'a'; 33]);
        let r = c.read_until_closed();
        assert!(
            r.len() == 1 && is_proto_err(&r[0]),
            "33 bytes without newline: {r:?}"
        );
        drop(c);
        assert_eq!(env.wait_conns(0, 5), 0);
    });
}

#[test]
fn max_request_limit_counts_the_whole_command_on_the_wire() {
    watchdog("max-request", 60, || {
        let env = limits_env(Limits {
            max_args: 100,
            max_bulk: 1000,
            max_inline: 1000,
            max_request: 100,
        });
        let set = |len: usize| wire(&[b"SET", b"k", &vec![b'v'; len]]);
        let exact = (1..200)
            .find(|&l| set(l).len() == 100)
            .expect("a value length that makes a 100 byte command");
        for len in [exact - 1, exact] {
            let mut c = env.client();
            assert_eq!(set(len).len(), len + 27);
            assert_eq!(
                c.cmd_raw(&set(len)),
                Reply::Ok,
                "command of {} bytes",
                set(len).len()
            );
        }
        // One byte over max_request must be refused.
        let mut c = env.client();
        let over = set(exact + 1);
        assert_eq!(over.len(), 101);
        c.send_raw(&over);
        let r = c.read_until_closed();
        assert!(
            r.len() == 1 && is_proto_err(&r[0]),
            "a 101 byte command under max_request=100 was accepted: {r:?}"
        );
    });
}

#[test]
fn inline_commands_blank_lines_and_empty_multibulk() {
    watchdog("inline", 60, || {
        let env = Env::new(engine_cfg(2), server_cfg(1, 8));
        let mut c = env.client();
        c.send_raw(
            b"PING\r\n\r\n\r\nPING\n   \n  ECHO   hi  \r\n*0\r\n*-1\r\n\t\r\n*1\r\n$4\r\nPING\r\n\r\nECHO\tx\r\n",
        );
        c.read_expecting(
            &[x(pong()), x(pong()), bulk("hi"), x(pong()), bulk("x")],
            "inline",
        );
        assert!(c.silent_for(100), "extra reply");
        // QUIT answers OK, runs nothing behind it, and closes.
        c.send_raw(b"QUIT\r\nPING\r\n");
        assert_eq!(c.read_until_closed(), vec![Reply::Ok]);
    });
}

// ---------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------

#[test]
fn one_byte_at_a_time_on_50_connections() {
    watchdog("trickle", 90, || {
        let env = Env::new(engine_cfg(4), server_cfg(4, 16));
        let addr = env.addr;
        let handles: Vec<_> = (0..50)
            .map(|i| {
                thread::spawn(move || {
                    let mut c = Client::connect(addr);
                    let key = format!("t{i}");
                    let counter = format!("n{i}");
                    let mut payload = Vec::new();
                    encode_command(&["SET", key.as_str(), "hello"], &mut payload);
                    encode_command(&["GET", key.as_str()], &mut payload);
                    encode_command(&["INCR", counter.as_str()], &mut payload);
                    payload.extend_from_slice(b"PING\r\n");
                    encode_command(&["MGET", key.as_str(), "nokey"], &mut payload);
                    encode_command(
                        &[
                            "EVAL",
                            "return redis.call('GET', KEYS[1])",
                            "1",
                            key.as_str(),
                        ],
                        &mut payload,
                    );
                    for (n, b) in payload.iter().enumerate() {
                        c.send_raw(std::slice::from_ref(b));
                        if n % 3 == 0 {
                            thread::sleep(Duration::from_micros(300));
                        }
                    }
                    c.read_expecting(
                        &[
                            x(Reply::Ok),
                            bulk("hello"),
                            x(Reply::Int(1)),
                            x(pong()),
                            x(Reply::Array(vec![Reply::bulk("hello"), Reply::Nil])),
                            bulk("hello"),
                        ],
                        &format!("trickle {i}"),
                    );
                })
            })
            .collect();
        join_all(handles);
    });
}

#[test]
fn random_chunking_and_pauses_match_the_model() {
    watchdog("chunks", 90, || {
        let cfg = Config {
            shards: 2,
            scan_chunk: 4,
            ..Config::default()
        };
        let env = Env::new(cfg, server_cfg(2, 3));
        let mut wl = Workload::new(99, "r/");
        wl.allow_flush = true;
        let (bytes, expects) = wl.batch(4000);
        let mut c = env.client();
        let wr = c.wr.try_clone().expect("clone");
        let writer = thread::spawn(move || -> io::Result<()> {
            let mut wr = wr;
            let mut rng = Rng::new(5);
            let mut pos = 0;
            while pos < bytes.len() {
                let n = if rng.chance(4) {
                    1
                } else {
                    1 + rng.below(700) as usize
                };
                let end = (pos + n).min(bytes.len());
                wr.write_all(&bytes[pos..end])?;
                pos = end;
                if rng.chance(50) {
                    thread::sleep(Duration::from_micros(rng.below(2000)));
                }
            }
            Ok(())
        });
        c.read_expecting(&expects, "random chunks");
        join_writer(writer, "chunks");
    });
}

#[test]
fn million_command_pipeline_is_answered_complete_in_order_exactly_once() {
    let _h = heavy();
    watchdog("million", 150, || {
        let env = Env::new(engine_cfg(2), server_cfg(2, 64));
        let mut c = env.client();
        const CYCLES: u32 = 250_000;
        let wr = c.wr.try_clone().expect("clone");
        let writer = thread::spawn(move || -> io::Result<()> {
            let mut wr = wr;
            let mut buf = Vec::with_capacity(1 << 20);
            for i in 0..CYCLES {
                let key = format!("k{}", i % 1000);
                let val = i.to_string();
                encode_command(&["SET", key.as_str(), val.as_str()], &mut buf);
                encode_command(&["GET", key.as_str()], &mut buf);
                encode_command(&["INCR", "ctr"], &mut buf);
                encode_command(&["ECHO", val.as_str()], &mut buf);
                if buf.len() >= 1 << 19 {
                    wr.write_all(&buf)?;
                    buf.clear();
                }
            }
            wr.write_all(&buf)
        });
        let started = Instant::now();
        for i in 0..CYCLES {
            let val = Reply::bulk(i.to_string());
            let got = [c.read(), c.read(), c.read(), c.read()];
            let want = [Reply::Ok, val.clone(), Reply::Int(i64::from(i) + 1), val];
            assert!(got == want, "cycle {i}: got {got:?}, want {want:?}");
        }
        eprintln!(
            "million pipeline: 1,000,000 commands in {:.2} s",
            started.elapsed().as_secs_f64()
        );
        join_writer(writer, "million");
        assert_eq!(c.cmd(&["GET", "ctr"]), Reply::bulk(CYCLES.to_string()));
        assert!(c.silent_for(100), "unexpected extra reply");
        drop(c);
        assert_eq!(env.wait_conns(0, 5), 0);
    });
}

fn run_matrix(shards: usize, workers: usize, cap: usize, conns: usize, ops: usize) {
    let cfg = Config {
        shards,
        scan_chunk: 4,
        drain_batch: 3,
        multi_escalate_after: 3,
        ..Config::default()
    };
    let env = Env::new(cfg, server_cfg(workers, cap));
    let addr = env.addr;
    let handles: Vec<_> = (0..conns)
        .map(|i| {
            thread::spawn(move || {
                let mut wl = Workload::new(1000 + i as u64, &format!("c{i:02}/"));
                let (bytes, expects) = wl.batch(ops);
                let mut c = Client::connect(addr);
                let writer = spawn_writer(c.wr.try_clone().expect("clone"), bytes, false);
                let who = format!("conn {i} (shards={shards} workers={workers} cap={cap})");
                c.read_expecting(&expects, &who);
                join_writer(writer, &who);
            })
        })
        .collect();
    join_all(handles);
    assert_eq!(
        env.wait_conns(0, 5),
        0,
        "shards={shards} workers={workers} cap={cap}: connections leaked"
    );
}

fn matrix_for_workers(workers: usize) {
    for shards in [1, 2] {
        for cap in [1, 64] {
            run_matrix(shards, workers, cap, 16, 2500);
        }
    }
}

#[test]
fn mixed_class_pipelines_stay_in_order_with_1_worker() {
    watchdog("matrix-1", 180, || matrix_for_workers(1));
}

#[test]
fn mixed_class_pipelines_stay_in_order_with_2_workers() {
    watchdog("matrix-2", 180, || matrix_for_workers(2));
}

#[test]
fn mixed_class_pipelines_stay_in_order_with_8_workers() {
    watchdog("matrix-8", 180, || matrix_for_workers(8));
}

// ---------------------------------------------------------------------
// Binary safety
// ---------------------------------------------------------------------

#[test]
fn binary_safe_keys_and_values() {
    watchdog("binary", 90, || {
        let env = Env::new(engine_cfg(4), server_cfg(2, 16));
        let mut c = env.client();
        let mut rng = Rng::new(42);
        let mut cases: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for b in 0..=255u8 {
            cases.push((vec![b], vec![b; 3]));
        }
        cases.push(((0..=255).collect(), (0..=255).rev().collect()));
        cases.push((b"a\r\nb".to_vec(), b"\r\n".to_vec()));
        cases.push((b"\0\0".to_vec(), b"\0\0\r\n\0".to_vec()));
        cases.push((Vec::new(), b"empty-key".to_vec()));
        cases.push((b"empty-value".to_vec(), Vec::new()));
        cases.push((
            b"inject".to_vec(),
            b"\r\n$3\r\nfoo\r\n*1\r\n$4\r\nPING\r\n+OK\r\n".to_vec(),
        ));
        cases.push((b"crlf-end".to_vec(), b"abc\r\n".to_vec()));
        // Sizes around the server's 64 KiB read chunk and 256 KiB read budget.
        for n in [
            1usize,
            2,
            65_535,
            65_536,
            65_537,
            65_538,
            131_071,
            131_072,
            131_073,
            262_143,
            262_144,
            262_145,
            8 * 1024 * 1024,
        ] {
            let mut v = rng.bytes(n);
            if n >= 2 {
                v[n - 2] = b'\r';
                v[n - 1] = b'\n';
            }
            cases.push((format!("size{n}").into_bytes(), v));
        }
        for (k, v) in &cases {
            let (k, v) = (k.as_slice(), v.as_slice());
            assert_eq!(c.cmd(&[b"SET".as_slice(), k, v]), Reply::Ok, "SET {k:?}");
            assert_eq!(
                c.cmd(&[b"GET".as_slice(), k]),
                Reply::Bulk(v.to_vec()),
                "GET {k:?} (value of {} bytes)",
                v.len()
            );
            assert_eq!(c.cmd(&[b"ECHO".as_slice(), v]), Reply::Bulk(v.to_vec()));
            assert_eq!(c.cmd(&[b"EXISTS".as_slice(), k]), Reply::Int(1));
        }
        // Everything at once.
        let small: Vec<&(Vec<u8>, Vec<u8>)> =
            cases.iter().filter(|(_, v)| v.len() < 1000).collect();
        let mut mset: Vec<&[u8]> = vec![b"MSET"];
        let mut mget: Vec<&[u8]> = vec![b"MGET"];
        for (k, v) in &small {
            mset.push(k);
            mset.push(v);
            mget.push(k);
        }
        assert_eq!(c.cmd(&mset), Reply::Ok);
        let want: Vec<Reply> = small.iter().map(|(_, v)| Reply::Bulk(v.clone())).collect();
        assert_eq!(c.cmd(&mget), Reply::Array(want));
        // Binary hash fields.
        let mut hset: Vec<&[u8]> = vec![b"HSET", b"hash"];
        for (k, v) in &small {
            hset.push(k);
            hset.push(v);
        }
        c.send(&hset);
        assert_eq!(c.read(), Reply::Int(small.len() as i64));
        c.send(&["HGETALL", "hash"]);
        let pairs = small.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let got = c.read();
        assert!(
            Expect::Pairs(pairs).matches(&got),
            "HGETALL: {}",
            short(&got)
        );
        // KEYS * returns every key, whatever bytes they hold.
        c.send(&["KEYS", "*"]);
        let mut keys: Vec<Vec<u8>> = cases.iter().map(|(k, _)| k.clone()).collect();
        keys.push(b"hash".to_vec());
        let got = c.read();
        assert!(
            Expect::Unordered(keys).matches(&got),
            "KEYS: {}",
            short(&got)
        );
    });
}

// ---------------------------------------------------------------------
// Back-pressure
// ---------------------------------------------------------------------

fn big_value() -> Vec<u8> {
    (0..1usize << 20)
        .map(|i| (i.wrapping_mul(7) + i / 251) as u8)
        .collect()
}

fn set_big(env: &Env, big: &[u8]) {
    let s = env.db.session().expect("session");
    let r = s.execute(&[b"SET".as_slice(), b"big".as_slice(), big]);
    assert_eq!(r, Reply::Ok);
}

fn big_pipeline(n: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for i in 0..n {
        encode_command(&["GET", "big"], &mut bytes);
        encode_command(&["ECHO", i.to_string().as_str()], &mut bytes);
    }
    bytes
}

#[test]
fn unread_big_replies_stay_bounded_and_arrive_intact_in_order() {
    let _h = heavy();
    watchdog("backpressure", 120, || {
        const N: usize = 800;
        for workers in [1, 2] {
            let env = Env::new(engine_cfg(8), server_cfg(workers, 64));
            let big = big_value();
            set_big(&env, &big);
            let mem0 = private_bytes();
            let mut slow = env.client();
            let writer = spawn_writer(slow.wr.try_clone().expect("clone"), big_pipeline(N), false);
            let mut other = env.client();
            let mut worst = Duration::ZERO;
            let mut peak = 0usize;
            for _ in 0..15 {
                thread::sleep(Duration::from_millis(100));
                for _ in 0..3 {
                    let t = Instant::now();
                    ping(&mut other);
                    assert_eq!(other.cmd(&["SET", "small", "1"]), Reply::Ok);
                    assert_eq!(other.cmd(&["GET", "small"]), Reply::bulk("1"));
                    worst = worst.max(t.elapsed());
                }
                if let (Some(a), Some(b)) = (mem0, private_bytes()) {
                    peak = peak.max(b.saturating_sub(a));
                }
            }
            eprintln!(
                "back-pressure workers={workers}: peak memory delta {} KiB while {N} MB of replies were unread, worst small round trip {:.1} ms",
                peak >> 10,
                ms(worst)
            );
            assert!(
                worst < Duration::from_millis(500),
                "other client stalled: {worst:?}"
            );
            assert!(
                peak < 200 << 20,
                "server buffered {peak} bytes for a client that does not read"
            );
            for i in 0..N {
                let r = slow.read();
                assert!(
                    matches!(&r, Reply::Bulk(b) if *b == big),
                    "reply {i}: {}",
                    short(&r)
                );
                assert_eq!(slow.read(), Reply::bulk(i.to_string()), "marker {i}");
            }
            join_writer(writer, "backpressure");
            assert!(slow.silent_for(100));
        }
    });
}

#[test]
fn abrupt_disconnect_while_replies_are_unread_releases_the_connection() {
    let _h = heavy();
    watchdog("backpressure-abort", 90, || {
        let env = Env::new(engine_cfg(8), server_cfg(2, 64));
        let big = big_value();
        set_big(&env, &big);
        let mem0 = private_bytes();
        let mut clients = Vec::new();
        let mut writers = Vec::new();
        for _ in 0..6 {
            let c = env.client();
            writers.push(spawn_writer(
                c.wr.try_clone().expect("clone"),
                big_pipeline(400),
                false,
            ));
            clients.push(c);
        }
        thread::sleep(Duration::from_millis(500));
        // One client reads a few replies first, then everybody vanishes.
        for _ in 0..3 {
            assert!(matches!(clients[0].read(), Reply::Bulk(_)));
            assert!(matches!(clients[0].read(), Reply::Bulk(_)));
        }
        drop(clients);
        for w in writers {
            let _ = w.join();
        }
        assert_eq!(
            env.wait_conns(0, 8),
            0,
            "connections with unread replies leaked"
        );
        ping(&mut env.client());
        if let (Some(a), Some(b)) = (mem0, private_bytes()) {
            let delta = b.saturating_sub(a);
            eprintln!("after abrupt disconnects: memory delta {} KiB", delta >> 10);
            assert!(delta < 100 << 20, "memory not released: {delta}");
        }
    });
}

// ---------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------

fn half_close_case(workers: usize, cap: usize, seed: u64, partial_tail: bool) {
    let cfg = Config {
        shards: 2,
        scan_chunk: 8,
        ..Config::default()
    };
    let env = Env::new(cfg, server_cfg(workers, cap));
    let mut wl = Workload::new(seed, "h/");
    let (mut bytes, expects) = wl.batch(10_000);
    if partial_tail {
        bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nk");
    }
    let mut c = env.client();
    let writer = spawn_writer(c.wr.try_clone().expect("clone"), bytes, true);
    let who = format!("half-close workers={workers} cap={cap} tail={partial_tail}");
    c.read_expecting(&expects, &who);
    join_writer(writer, &who);
    assert_eq!(
        c.read_until_closed(),
        vec![],
        "{who}: extra replies before EOF"
    );
    drop(c);
    assert_eq!(env.wait_conns(0, 5), 0, "{who}: connection not released");
}

#[test]
fn half_close_after_10000_command_pipeline_still_gets_every_reply() {
    watchdog("half-close", 120, || {
        half_close_case(2, 64, 11, false);
        half_close_case(1, 1, 12, false);
        half_close_case(8, 3, 13, true);
    });
}

#[test]
fn abrupt_disconnects_while_parked_or_queued_leave_server_consistent() {
    let _h = heavy();
    watchdog("disconnect-storm", 150, || {
        let cfg = Config {
            shards: 2,
            scan_chunk: 8,
            multi_escalate_after: 1,
            drain_batch: 2,
            ..Config::default()
        };
        let env = Env::new(cfg, server_cfg(2, 4));
        {
            let s = env.db.session().expect("session");
            for chunk in 0..10 {
                let mut args: Vec<Vec<u8>> = vec![b"MSET".to_vec()];
                for i in 0..300 {
                    args.push(format!("pre{}", chunk * 300 + i).into_bytes());
                    args.push(b"v".to_vec());
                }
                assert_eq!(s.execute(&args), Reply::Ok);
            }
            let mut args: Vec<Vec<u8>> = vec![b"HSET".to_vec(), b"bigh".to_vec()];
            for i in 0..600 {
                args.push(format!("f{i}").into_bytes());
                args.push(b"v".to_vec());
            }
            assert_eq!(s.execute(&args), Reply::Int(600));
        }
        let addr = env.addr;
        let mut handles = Vec::new();
        for i in 0..4 {
            // Connections that stay: their counters must be exact.
            handles.push(thread::spawn(move || {
                let mut c = Client::connect(addr);
                let key = format!("own{i}");
                let mut n = 0;
                for _ in 0..400 {
                    let mut batch = Vec::new();
                    for _ in 0..50 {
                        encode_command(&["INCR", key.as_str()], &mut batch);
                    }
                    c.send_raw(&batch);
                    for _ in 0..50 {
                        n += 1;
                        assert_eq!(c.read(), Reply::Int(n), "stable client {i}");
                    }
                }
            }));
        }
        for i in 0..4u64 {
            // Connections that vanish at random moments.
            handles.push(thread::spawn(move || {
                let mut rng = Rng::new(500 + i);
                for _ in 0..150 {
                    let mut c = Client::connect(addr);
                    let mut batch = Vec::new();
                    for _ in 0..1 + rng.below(12) {
                        match rng.below(7) {
                            0 => encode_command(&["KEYS", "*"], &mut batch),
                            1 => encode_command(
                                &["EVAL", "for i=1,20000 do end return 1", "0"],
                                &mut batch,
                            ),
                            2 => encode_command(
                                &["MGET", "pre1", "pre500", "pre1000", "pre2999"],
                                &mut batch,
                            ),
                            3 => encode_command(&["HGETALL", "bigh"], &mut batch),
                            4 => encode_command(&["SET", "hot", "x"], &mut batch),
                            5 => encode_command(
                                &["MSET", "pre1", "v", "pre2", "v", "pre3", "v"],
                                &mut batch,
                            ),
                            _ => encode_command(&["GET", "hot"], &mut batch),
                        }
                    }
                    c.send_raw(&batch);
                    thread::sleep(Duration::from_micros(rng.below(3000)));
                    if rng.chance(3) {
                        let _ = c.try_read();
                    }
                    if rng.chance(3) {
                        c.half_close();
                    }
                    drop(c);
                }
            }));
        }
        join_all(handles);
        assert_eq!(
            env.wait_conns(0, 10),
            0,
            "connections leaked after the storm"
        );
        let s = env.db.session().expect("session");
        for i in 0..4 {
            let key = format!("own{i}");
            assert_eq!(s.execute(&["GET", key.as_str()]), Reply::bulk("20000"));
        }
        let mut c = env.client();
        let t = Instant::now();
        ping(&mut c);
        assert_eq!(c.cmd(&["GET", "pre1"]), Reply::bulk("v"));
        assert!(t.elapsed() < Duration::from_secs(1));
    });
}

#[test]
fn disconnect_mid_keys_reply_with_1m_keys() {
    let _h = heavy();
    watchdog("keys-1m", 150, || {
        const KEYS: usize = 1_000_000;
        let env = Env::new(Config::default(), server_cfg(4, 64));
        {
            let s = env.db.session().expect("session");
            for chunk in 0..KEYS / 1000 {
                let mut args: Vec<Vec<u8>> = vec![b"MSET".to_vec()];
                for i in 0..1000 {
                    args.push(format!("k{}", chunk * 1000 + i).into_bytes());
                    args.push(b"v".to_vec());
                }
                assert_eq!(s.execute(&args), Reply::Ok);
            }
        }
        let mut rng = Rng::new(31337);
        for round in 0..6 {
            let mut c = env.client();
            c.send(&["KEYS", "*"]);
            if round % 2 == 1 {
                c.send(&["KEYS", "k1*"]);
            }
            thread::sleep(Duration::from_millis(5 + rng.below(150)));
            if round % 3 == 2 {
                // Take part of the reply, then vanish.
                let mut buf = [0u8; 4096];
                let _ = c.rd.get_mut().read(&mut buf);
            }
            drop(c);
            // One scan at a time: each holds its whole reply in memory.
            assert_eq!(env.wait_conns(0, 30), 0, "round {round}: connection leaked");
        }
        let mut c = env.client();
        assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(KEYS as i64));
        assert_eq!(c.cmd(&["GET", "k999999"]), Reply::bulk("v"));
        c.send(&["KEYS", "*"]);
        match c.read() {
            Reply::Array(items) => assert_eq!(items.len(), KEYS),
            other => panic!("KEYS *: {}", short(&other)),
        }
    });
}

#[test]
fn max_conns_turns_away_extra_clients_and_recovers_slots() {
    watchdog("max-conns", 60, || {
        let mut scfg = server_cfg(2, 8);
        scfg.max_conns = 5;
        let env = Env::new(engine_cfg(2), scfg);
        let mut clients: Vec<Client> = (0..5).map(|_| env.client()).collect();
        for c in &mut clients {
            ping(c);
        }
        assert_eq!(env.server.connections(), 5);
        for round in 0..20 {
            let mut extra = env.client();
            let r = extra.read_until_closed();
            assert_eq!(
                r,
                vec![Reply::err("ERR max number of clients reached")],
                "round {round}"
            );
        }
        assert_eq!(
            env.server.connections(),
            5,
            "rejections leaked counter slots"
        );
        drop(clients.pop());
        assert_eq!(env.wait_conns(4, 5), 4);
        let mut fresh = env.client();
        ping(&mut fresh);
        let mut extra = env.client();
        assert_eq!(
            extra.read_until_closed(),
            vec![Reply::err("ERR max number of clients reached")]
        );
        // The accepted ones keep working.
        for c in &mut clients {
            ping(c);
        }
        drop(clients);
        drop(fresh);
        assert_eq!(env.wait_conns(0, 5), 0);
    });
}

#[test]
fn connection_churn_leaves_no_stuck_worker() {
    watchdog("churn", 150, || {
        let env = Env::new(engine_cfg(4), server_cfg(4, 8));
        let addr = env.addr;
        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                thread::spawn(move || {
                    let mut rng = Rng::new(77 + t);
                    for _ in 0..375 {
                        let s = connect_stream(addr);
                        match rng.below(6) {
                            0 => drop(s),
                            1 => {
                                let mut c = Client::from_stream(s, Duration::from_secs(10));
                                c.send_raw(b"*3\r\n$3\r\nSE");
                            }
                            2 => {
                                let mut c = Client::from_stream(s, Duration::from_secs(10));
                                let mut batch = Vec::new();
                                for _ in 0..100 {
                                    encode_command(&["PING"], &mut batch);
                                }
                                c.send_raw(&batch);
                            }
                            3 => {
                                let mut c = Client::from_stream(s, Duration::from_secs(10));
                                ping(&mut c);
                            }
                            4 => {
                                let mut c = Client::from_stream(s, Duration::from_secs(10));
                                let mut batch = Vec::new();
                                for _ in 0..50 {
                                    encode_command(&["PING"], &mut batch);
                                }
                                c.send_raw(&batch);
                                c.half_close();
                                let replies = c.read_until_closed();
                                assert_eq!(replies, vec![pong(); 50]);
                            }
                            _ => {
                                let mut c = Client::from_stream(s, Duration::from_secs(10));
                                c.send_raw(b"*2\r\n$4\r\nECHO\r\n$10\r\nabc");
                                c.half_close();
                            }
                        }
                    }
                })
            })
            .collect();
        join_all(handles);
        assert_eq!(env.wait_conns(0, 15), 0, "connections leaked after churn");
        // Every worker must still serve (connections go round robin).
        let mut fresh: Vec<Client> = (0..16).map(|_| env.client()).collect();
        for (i, c) in fresh.iter_mut().enumerate() {
            let t = Instant::now();
            ping(c);
            assert!(
                t.elapsed() < Duration::from_secs(1),
                "client {i} waited {:?} for PONG",
                t.elapsed()
            );
        }
    });
}

// ---------------------------------------------------------------------
// Starvation and fairness
// ---------------------------------------------------------------------

#[test]
fn long_scans_do_not_starve_small_commands() {
    let _h = heavy();
    watchdog("fairness", 150, || {
        let env = Env::new(Config::default(), server_cfg(4, 64));
        {
            let s = env.db.session().expect("session");
            for chunk in 0..300 {
                let mut args: Vec<Vec<u8>> = vec![b"MSET".to_vec()];
                for i in 0..1000 {
                    args.push(format!("s{}", chunk * 1000 + i).into_bytes());
                    args.push(b"v".to_vec());
                }
                assert_eq!(s.execute(&args), Reply::Ok);
            }
            for chunk in 0..600 {
                let mut args: Vec<Vec<u8>> = vec![b"HSET".to_vec(), b"bigh".to_vec()];
                for i in 0..500 {
                    args.push(format!("f{}", chunk * 500 + i).into_bytes());
                    args.push(b"v".to_vec());
                }
                s.execute(&args);
            }
        }
        let mut c = env.client();
        let t = Instant::now();
        match c.cmd(&["KEYS", "*"]) {
            Reply::Array(a) => assert_eq!(a.len(), 300_001),
            other => panic!("KEYS: {}", short(&other)),
        }
        let d_keys = t.elapsed();
        let t = Instant::now();
        match c.cmd(&["HGETALL", "bigh"]) {
            Reply::Array(a) => assert_eq!(a.len(), 600_000),
            other => panic!("HGETALL: {}", short(&other)),
        }
        let d_hash = t.elapsed();
        drop(c);

        let stop = Arc::new(AtomicBool::new(false));
        let addr = env.addr;
        let scans = Arc::new(AtomicU64::new(0));
        let mut scanners = Vec::new();
        for cmd in [["KEYS", "*"], ["HGETALL", "bigh"]] {
            let (stop, scans) = (stop.clone(), scans.clone());
            scanners.push(thread::spawn(move || {
                let mut c = Client::connect(addr);
                while !stop.load(SeqCst) {
                    assert!(matches!(c.cmd(&cmd), Reply::Array(_)));
                    scans.fetch_add(1, SeqCst);
                }
            }));
        }
        thread::sleep(Duration::from_millis(300));
        let smalls: Vec<_> = (0..8)
            .map(|i| {
                let stop = stop.clone();
                thread::spawn(move || {
                    let mut c = Client::connect(addr);
                    let key = format!("small{i}");
                    let mut lat = Vec::new();
                    let mut n = 0u64;
                    while !stop.load(SeqCst) {
                        let t = Instant::now();
                        let v = n.to_string();
                        assert_eq!(c.cmd(&["SET", key.as_str(), v.as_str()]), Reply::Ok);
                        lat.push(t.elapsed());
                        let t = Instant::now();
                        assert_eq!(c.cmd(&["GET", key.as_str()]), Reply::bulk(v));
                        lat.push(t.elapsed());
                        n += 1;
                        thread::sleep(Duration::from_micros(200));
                    }
                    lat
                })
            })
            .collect();
        thread::sleep(Duration::from_secs(6));
        stop.store(true, SeqCst);
        let mut lat: Vec<Duration> = join_all(smalls).into_iter().flatten().collect();
        join_all(scanners);
        lat.sort();
        let (p50, p99, max) = (
            percentile(&lat, 0.5),
            percentile(&lat, 0.99),
            *lat.last().expect("latencies"),
        );
        eprintln!(
            "fairness: one KEYS * (300k keys) alone {:.0} ms, HGETALL (300k fields) alone {:.0} ms; {} scans completed under load; {} small ops: p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms",
            ms(d_keys),
            ms(d_hash),
            scans.load(SeqCst),
            lat.len(),
            ms(p50),
            ms(p99),
            ms(max)
        );
        assert!(
            max < Duration::from_secs(1),
            "a small command waited {max:?}"
        );
        assert!(
            p99 * 2 < d_keys.max(Duration::from_millis(100)),
            "p99 {p99:?} is not well below one KEYS ({d_keys:?})"
        );
    });
}

fn worst_ping(c: &mut Client, d: Duration) -> Duration {
    let end = Instant::now() + d;
    let mut worst = Duration::ZERO;
    while Instant::now() < end {
        let t = Instant::now();
        ping(c);
        worst = worst.max(t.elapsed());
        thread::sleep(Duration::from_millis(2));
    }
    worst
}

#[test]
fn endless_pipeline_does_not_starve_another_connection_on_the_same_worker() {
    let _h = heavy();
    watchdog("endless", 90, || {
        let env = Env::new(engine_cfg(2), server_cfg(1, 2));
        let repeated = |n: usize, args: &[&str]| {
            let mut b = Vec::new();
            for _ in 0..n {
                encode_command(args, &mut b);
            }
            b
        };
        let mut set_get = repeated(100, &["SET", "endless", "v"]);
        set_get.extend(repeated(100, &["GET", "endless"]));
        for (name, batch) in [
            ("PING", repeated(200, &["PING"])),
            ("SET/GET", set_get),
            ("EVAL", repeated(50, &["EVAL", "return 1", "0"])),
        ] {
            let endless = flood(env.addr, batch, Duration::from_secs(10));
            thread::sleep(Duration::from_millis(200));
            let mut p = env.client();
            let worst = worst_ping(&mut p, Duration::from_secs(2));
            eprintln!(
                "endless {name} pipeline, workers=1 cap=2: worst PING {:.1} ms",
                ms(worst)
            );
            assert!(
                worst < Duration::from_millis(500),
                "{name}: PING waited {worst:?}"
            );
            drop(p);
            assert_eq!(endless.stop_and_join(), End::Closed, "{name}");
            assert_eq!(env.wait_conns(0, 10), 0, "{name}: flood connection leaked");
        }
    });
}

#[test]
fn client_that_never_reads_does_not_stall_its_worker() {
    let _h = heavy();
    watchdog("never-reads", 60, || {
        let env = Env::new(engine_cfg(2), server_cfg(1, 8));
        let big = big_value();
        set_big(&env, &big);
        let c = env.client();
        let writer = spawn_writer(c.wr.try_clone().expect("clone"), big_pipeline(300), false);
        thread::sleep(Duration::from_millis(500));
        let mut p = env.client();
        let worst = worst_ping(&mut p, Duration::from_secs(1));
        eprintln!(
            "never-reading client on the only worker: worst PING {:.1} ms",
            ms(worst)
        );
        assert!(worst < Duration::from_millis(500), "PING waited {worst:?}");
        drop(c);
        let _ = writer.join();
    });
}

// ---------------------------------------------------------------------
// Scripts over TCP
// ---------------------------------------------------------------------

const KILL: [&str; 2] = ["SCRIPT", "KILL"];
const SPIN: [&str; 3] = ["EVAL", "while true do end", "0"];

fn wait_script_running(env: &Env, base: u64) {
    assert!(
        wait_until(5, || env.db.stats().exclusive > base),
        "the script never started"
    );
    thread::sleep(Duration::from_millis(100));
}

fn script_run(workers: usize) {
    let env = Env::new(engine_cfg(4), server_cfg(workers, 8));
    let _guard = KillScriptsOnDrop(env.db.clone());
    let mut b = env.client();
    assert_eq!(b.cmd(&["SET", "x", "1"]), Reply::Ok);
    let base = env.db.stats().exclusive;
    let mut a = env.client();
    a.send(&SPIN);
    wait_script_running(&env, base);

    b.send(&["GET", "x"]);
    let mut c = env.client();
    let mut worst = Duration::ZERO;
    for _ in 0..50 {
        let t = Instant::now();
        ping(&mut c);
        worst = worst.max(t.elapsed());
    }
    assert_eq!(c.cmd(&["ECHO", "hi"]), Reply::bulk("hi"));
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(1));
    match c.cmd(&["INFO"]) {
        Reply::Bulk(info) => assert!(String::from_utf8_lossy(&info).contains("exclusive:")),
        other => panic!("INFO: {other:?}"),
    }
    assert!(
        b.silent_for(300),
        "GET was answered while a script owns the node"
    );
    let t = Instant::now();
    let mut k = env.client();
    assert_eq!(k.cmd(&KILL), Reply::Ok);
    let reply_a = a.read();
    let kill_latency = t.elapsed();
    assert!(
        error_text(&reply_a).is_some_and(|e| e.contains("killed")),
        "script reply: {reply_a:?}"
    );
    assert_eq!(
        b.read(),
        Reply::bulk("1"),
        "the parked GET was lost or wrong"
    );
    assert!(error_text(&k.cmd(&KILL)).is_some_and(|e| e.starts_with("NOTBUSY")));
    assert_eq!(c.cmd(&["GET", "x"]), Reply::bulk("1"));
    eprintln!(
        "script workers={workers}: worst PING while the script ran {:.2} ms, SCRIPT KILL to the script's error reply {:.2} ms",
        ms(worst),
        ms(kill_latency)
    );
    assert!(worst < Duration::from_millis(500));
}

#[test]
fn running_script_does_not_stall_connection_commands_workers_1() {
    watchdog("script-1", 60, || script_run(1));
}

#[test]
fn running_script_does_not_stall_connection_commands_workers_2() {
    watchdog("script-2", 60, || script_run(2));
}

#[test]
fn running_script_does_not_stall_connection_commands_workers_8() {
    watchdog("script-8", 60, || script_run(8));
}

#[test]
fn script_that_wrote_cannot_be_killed_and_finishes() {
    watchdog("unkillable", 60, || {
        let env = Env::new(engine_cfg(2), server_cfg(2, 8));
        let _guard = KillScriptsOnDrop(env.db.clone());
        let base = env.db.stats().exclusive;
        let mut a = env.client();
        a.send(&[
            "EVAL",
            "redis.call('SET','uk','1') for i=1,60000000 do end return 'fin'",
            "0",
        ]);
        wait_script_running(&env, base);
        let mut k = env.client();
        let r = k.cmd(&KILL);
        assert!(
            error_text(&r).is_some_and(|e| e.starts_with("UNKILLABLE")),
            "SCRIPT KILL after a write: {r:?}"
        );
        assert_eq!(a.read(), Reply::bulk("fin"));
        assert_eq!(k.cmd(&["GET", "uk"]), Reply::bulk("1"));
    });
}

#[test]
fn client_disconnecting_during_a_script_does_not_wedge_the_server() {
    watchdog("script-disconnect", 60, || {
        let env = Env::new(engine_cfg(4), server_cfg(2, 8));
        let _guard = KillScriptsOnDrop(env.db.clone());
        let base = env.db.stats().exclusive;
        let mut a = env.client();
        a.send(&SPIN);
        // Commands queued behind the script on the same connection.
        let mut tail = Vec::new();
        for _ in 0..50 {
            encode_command(&["SET", "after", "1"], &mut tail);
        }
        a.send_raw(&tail);
        wait_script_running(&env, base);
        drop(a);
        thread::sleep(Duration::from_millis(200));
        let mut b = env.client();
        ping(&mut b);
        assert_eq!(b.cmd(&KILL), Reply::Ok);
        let t = Instant::now();
        assert_eq!(b.cmd(&["GET", "nokey"]), Reply::Nil);
        assert!(t.elapsed() < Duration::from_secs(5));
        assert_eq!(b.cmd(&["SET", "k", "v"]), Reply::Ok);
        assert_eq!(env.wait_conns(1, 5), 1, "dead script client not released");
        drop(b);
        assert_eq!(env.wait_conns(0, 5), 0);
    });
}

#[test]
fn kill_storm_never_kills_a_script_without_an_ok() {
    watchdog("kill-storm", 90, || {
        let env = Env::new(engine_cfg(4), server_cfg(4, 8));
        let _guard = KillScriptsOnDrop(env.db.clone());
        let stop = Arc::new(AtomicBool::new(false));
        let (oks, killed, done, notbusy) = (
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        );
        let addr = env.addr;
        let mut handles = Vec::new();
        for _ in 0..16 {
            let (stop, killed, done) = (stop.clone(), killed.clone(), done.clone());
            handles.push(thread::spawn(move || {
                let mut c = Client::connect(addr);
                while !stop.load(SeqCst) {
                    match c.cmd(&["EVAL", "for i=1,30000 do end return 'done'", "0"]) {
                        Reply::Bulk(b) if b == b"done" => done.fetch_add(1, SeqCst),
                        Reply::Error(e) if e.contains("Script killed") => {
                            killed.fetch_add(1, SeqCst)
                        }
                        other => panic!("unexpected EVAL reply {other:?}"),
                    };
                }
            }));
        }
        for _ in 0..16 {
            let (stop, oks, notbusy) = (stop.clone(), oks.clone(), notbusy.clone());
            handles.push(thread::spawn(move || {
                let mut c = Client::connect(addr);
                while !stop.load(SeqCst) {
                    match c.cmd(&KILL) {
                        Reply::Ok => oks.fetch_add(1, SeqCst),
                        Reply::Error(e) if e.starts_with("NOTBUSY") => notbusy.fetch_add(1, SeqCst),
                        other => panic!("unexpected SCRIPT KILL reply {other:?}"),
                    };
                    thread::sleep(Duration::from_micros(50));
                }
            }));
        }
        thread::sleep(Duration::from_secs(4));
        stop.store(true, SeqCst);
        join_all(handles);
        let (oks, killed, done, notbusy) = (
            oks.load(SeqCst),
            killed.load(SeqCst),
            done.load(SeqCst),
            notbusy.load(SeqCst),
        );
        eprintln!(
            "kill storm: {done} scripts completed, {killed} killed, {oks} SCRIPT KILL answered OK, {notbusy} NOTBUSY"
        );
        assert!(done + killed > 0, "no script ran");
        assert!(
            killed <= oks,
            "{killed} scripts were killed but only {oks} kills were acknowledged"
        );
        let mut c = env.client();
        assert!(error_text(&c.cmd(&KILL)).is_some_and(|e| e.starts_with("NOTBUSY")));
        assert_eq!(
            c.cmd(&["EVAL", "for i=1,30000 do end return 'done'", "0"]),
            Reply::bulk("done"),
            "a stale kill hit the next script"
        );
    });
}

// ---------------------------------------------------------------------
// Shutdown and restart
// ---------------------------------------------------------------------

fn start_floods(addr: SocketAddr, n: usize) -> Vec<thread::JoinHandle<End>> {
    (0..n)
        .map(|_| flood(addr, flood_batch(), Duration::from_secs(10)).into_handle())
        .collect()
}

fn listen_again(db: &hcache::Db, addr: SocketAddr) -> hcache_server::Server {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let scfg = ServerConfig {
            addr,
            ..server_cfg(2, 8)
        };
        match db.listen(scfg) {
            Ok(s) => return s,
            Err(e) if Instant::now() < deadline => {
                eprintln!("rebinding {addr}: {e}, retrying");
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("cannot bind {addr} again after shutdown: {e}"),
        }
    }
}

#[test]
fn shutdown_closes_every_client_and_the_port_can_be_reused() {
    watchdog("shutdown", 90, || {
        let db = hcache::Db::open(None, engine_cfg(4)).expect("db");
        let server = db.listen(server_cfg(4, 8)).expect("listen");
        let addr = server.local_addr();
        let s = db.session().expect("session");
        assert_eq!(s.execute(&["SET", "kept", "yes"]), Reply::Ok);
        let floods = start_floods(addr, 32);
        thread::sleep(Duration::from_millis(500));
        let t = Instant::now();
        server.shutdown();
        let took = t.elapsed();
        eprintln!("shutdown with 32 clients mid-pipeline: {:.0} ms", ms(took));
        assert!(took < Duration::from_secs(5), "shutdown took {took:?}");
        for (i, end) in join_all(floods).into_iter().enumerate() {
            assert_eq!(end, End::Closed, "client {i} was not cut off by shutdown");
        }
        assert!(
            std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_err(),
            "the port still accepts connections after shutdown"
        );
        let server = listen_again(&db, addr);
        let mut c = Client::connect(server.local_addr());
        assert_eq!(c.cmd(&["GET", "kept"]), Reply::bulk("yes"));
        c.send(&["KEYS", "*"]);
        assert!(matches!(c.read(), Reply::Array(_)));
        assert_eq!(
            c.cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "kept"]),
            Reply::bulk("yes")
        );
        drop(c);
        drop(server);
    });
}

#[test]
fn dropping_the_server_handle_stops_it_like_shutdown() {
    watchdog("drop-server", 60, || {
        let db = hcache::Db::open(None, engine_cfg(2)).expect("db");
        let server = db.listen(server_cfg(2, 8)).expect("listen");
        let addr = server.local_addr();
        let floods = start_floods(addr, 8);
        thread::sleep(Duration::from_millis(300));
        let t = Instant::now();
        drop(server);
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "drop took {:?}",
            t.elapsed()
        );
        for end in join_all(floods) {
            assert_eq!(end, End::Closed);
        }
        let s = db.session().expect("embedded session after server drop");
        assert_eq!(s.execute(&["SET", "a", "1"]), Reply::Ok);
        assert_eq!(
            s.execute(&["KEYS", "a"]),
            Reply::Array(vec![Reply::bulk("a")])
        );
        let server = db.listen(server_cfg(2, 8)).expect("second server");
        let mut c = Client::connect(server.local_addr());
        assert_eq!(c.cmd(&["GET", "a"]), Reply::bulk("1"));
    });
}

#[test]
fn shutdown_returns_while_a_script_loops_forever() {
    watchdog("shutdown-script", 90, || {
        let db = hcache::Db::open(None, engine_cfg(4)).expect("db");
        let _guard = KillScriptsOnDrop(db.clone());
        let server = db.listen(server_cfg(4, 8)).expect("listen");
        let addr = server.local_addr();
        let floods = start_floods(addr, 32);
        let base = db.stats().exclusive;
        let mut a = Client::connect(addr);
        a.send(&SPIN);
        assert!(
            wait_until(5, || db.stats().exclusive > base),
            "script did not start"
        );
        thread::sleep(Duration::from_millis(200));
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let t = Instant::now();
            server.shutdown();
            let _ = tx.send(t.elapsed());
        });
        let hung = match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(d) => {
                eprintln!("shutdown with a looping script returned in {:.0} ms", ms(d));
                false
            }
            Err(_) => {
                // Free the executor so the run can go on, then report.
                let s = db.session().expect("session");
                assert_eq!(s.execute(&KILL), Reply::Ok);
                rx.recv_timeout(Duration::from_secs(10))
                    .expect("shutdown did not return even after the script was killed");
                true
            }
        };
        for end in join_all(floods) {
            assert_eq!(end, End::Closed);
        }
        assert!(
            !hung,
            "Server::shutdown blocked for more than 10 s because a running script keeps the executor thread busy"
        );
    });
}

#[test]
fn shutdown_does_not_kill_the_script_of_an_embedded_session() {
    watchdog("shutdown-foreign-script", 60, || {
        let db = hcache::Db::open(None, engine_cfg(4)).expect("db");
        let _guard = KillScriptsOnDrop(db.clone());
        let server = db.listen(server_cfg(2, 8)).expect("listen");
        let base = db.stats().exclusive;
        let embedded = {
            let db = db.clone();
            thread::spawn(move || db.session().expect("session").execute(&SPIN))
        };
        assert!(
            wait_until(5, || db.stats().exclusive > base),
            "script did not start"
        );
        // A script of the server's own waits behind the embedded one.
        let mut c = Client::connect(server.local_addr());
        c.send(&["EVAL", "return redis.call('SET', 'ran', '1')", "0"]);
        thread::sleep(Duration::from_millis(200));
        let t = Instant::now();
        server.shutdown();
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "shutdown took {:?}",
            t.elapsed()
        );
        assert!(
            !embedded.is_finished(),
            "Server::shutdown killed a script the server did not start"
        );
        let s = db.session().expect("session");
        assert_eq!(s.execute(&KILL), Reply::Ok);
        let reply = embedded.join().expect("embedded thread");
        assert!(matches!(reply, Reply::Error(_)), "{reply:?}");
        // The queued script must not run once the server is gone.
        thread::sleep(Duration::from_millis(300));
        assert_eq!(s.execute(&["GET", "ran"]), Reply::Nil);
    });
}

#[test]
fn second_server_on_the_same_db_is_refused_or_works() {
    watchdog("two-servers", 60, || {
        let db = hcache::Db::open(None, engine_cfg(4)).expect("db");
        let first = db.listen(server_cfg(2, 8)).expect("first listener");
        let second = db.listen(server_cfg(2, 8));
        let Ok(second) = second else {
            // The design forbids it: fine, as long as the first still works.
            let mut c = Client::connect(first.local_addr());
            assert_eq!(c.cmd(&["SET", "a", "1"]), Reply::Ok);
            return;
        };
        let mut c1 = Client::with_timeout(first.local_addr(), Duration::from_secs(5));
        let mut c2 = Client::with_timeout(second.local_addr(), Duration::from_secs(5));
        for round in 0..5 {
            // KEYS always parks, so its reply travels through the hooks.
            c1.send(&["KEYS", "*"]);
            let r1 = c1.try_read().unwrap_or_else(|e| {
                panic!(
                    "round {round}: KEYS on the first of two servers on one Db got no reply: {e}"
                )
            });
            assert!(matches!(r1, Reply::Array(_)), "round {round}: {r1:?}");
            c2.send(&["KEYS", "*"]);
            let r2 = c2.try_read().unwrap_or_else(|e| {
                panic!("round {round}: KEYS on the second server got no reply: {e}")
            });
            assert!(matches!(r2, Reply::Array(_)), "round {round}: {r2:?}");
        }
        drop(first);
        c2.send(&["KEYS", "*"]);
        let r = c2.try_read().unwrap_or_else(|e| {
            panic!("KEYS on the surviving server hangs after the other server was dropped: {e}")
        });
        assert!(matches!(r, Reply::Array(_)));
    });
}

#[test]
fn stale_work_of_a_dead_server_is_not_delivered_to_a_new_connection() {
    let _h = heavy();
    watchdog("stale-ticket", 150, || {
        let cfg = Config {
            shards: 8,
            scan_chunk: 8,
            ..Config::default()
        };
        let db = hcache::Db::open(None, cfg).expect("db");
        {
            let s = db.session().expect("session");
            for chunk in 0..100 {
                let mut args: Vec<Vec<u8>> = vec![b"MSET".to_vec()];
                for i in 0..1000 {
                    args.push(format!("key{}", chunk * 1000 + i).into_bytes());
                    args.push(b"v".to_vec());
                }
                assert_eq!(s.execute(&args), Reply::Ok);
            }
        }
        let mut rng = Rng::new(2024);
        for attempt in 0..5 {
            let first = db.listen(server_cfg(2, 8)).expect("first");
            // The first command of the first connection: ticket generation 1.
            let mut old = Client::with_timeout(first.local_addr(), Duration::from_secs(10));
            old.send(&["KEYS", "*"]);
            thread::sleep(Duration::from_millis(rng.below(4)));
            first.shutdown();
            let second = db.listen(server_cfg(2, 8)).expect("second");
            // Same worker, same slot, same generation as the old client.
            let mut new = Client::with_timeout(second.local_addr(), Duration::from_secs(60));
            new.send(&["KEYS", "zzz-matches-nothing*"]);
            let reply = new.read();
            assert!(
                reply == Reply::Array(vec![]),
                "attempt {attempt}: the new connection received {} (the reply of a command from the previous server)",
                short(&reply)
            );
            assert!(old.read_until_closed().len() <= 1);
            drop(new);
            drop(second);
            // Let leftover tasks drain without a server.
            let s = db.session().expect("session");
            s.execute(&["KEYS", "zzz-matches-nothing*"]);
        }
    });
}

#[test]
fn half_closed_sockets_are_a_clean_eof_for_the_client() {
    watchdog("eof", 30, || {
        let env = Env::new(engine_cfg(2), server_cfg(1, 8));
        let mut c = env.client();
        c.send(&["PING"]);
        c.half_close();
        assert_eq!(c.read(), pong());
        let mut buf = [0u8; 1];
        match c.rd.get_mut().read(&mut buf) {
            Ok(0) => {}
            Err(e) if is_closed(&e) => {}
            other => panic!("expected EOF, got {other:?}"),
        }
        let _ = c.wr.shutdown(Shutdown::Both);
    });
}

/// Half-close right after a small pipeline of connection-level commands:
/// the server must answer all and then close, whatever the interleaving of
/// data and FIN at the server's socket.
fn half_close_small(workers: usize, cap: usize) {
    watchdog("half-close-small", 120, move || {
        let env = Env::new(engine_cfg(2), server_cfg(workers, cap));
        let addr = env.addr;
        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                thread::spawn(move || {
                    let mut rng = Rng::new(900 + t);
                    let mut stuck = Vec::new();
                    for _ in 0..60 {
                        let n = 1 + rng.below(100) as usize;
                        let mut c = Client::with_timeout(addr, Duration::from_millis(1000));
                        let mut batch = Vec::new();
                        for _ in 0..n {
                            encode_command(&["PING"], &mut batch);
                        }
                        c.send_raw(&batch);
                        if rng.chance(2) {
                            thread::sleep(Duration::from_micros(rng.below(500)));
                        }
                        c.half_close();
                        let mut got = 0;
                        let closed = loop {
                            match c.try_read() {
                                Ok(_) => got += 1,
                                Err(e) if is_closed(&e) => break true,
                                Err(_) => break false,
                            }
                        };
                        if !closed || got != n {
                            stuck.push((n, got, closed));
                        }
                    }
                    stuck
                })
            })
            .collect();
        let stuck: Vec<_> = join_all(handles).into_iter().flatten().collect();
        assert!(
                stuck.is_empty(),
                "workers={workers} cap={cap}: {} of 480 half-closed pipelines were not closed by the server after all replies; (pipeline len, replies, closed): {:?}; server still counts {} connections",
                stuck.len(),
                &stuck[..stuck.len().min(10)],
                env.server.connections()
            );
    });
}

#[test]
fn half_close_after_small_pipeline_always_closes() {
    half_close_small(1, 1);
    half_close_small(1, 8);
    half_close_small(4, 64);
}

#[test]
fn quit_behind_a_parked_command_replies_in_order_then_closes() {
    watchdog("quit", 30, || {
        let env = Env::new(engine_cfg(2), server_cfg(1, 8));
        let mut c = env.client();
        let mut payload = Vec::new();
        encode_command(&["SET", "a", "1"], &mut payload);
        encode_command(&["KEYS", "*"], &mut payload);
        encode_command(&["EVAL", "return 7", "0"], &mut payload);
        encode_command(&["QUIT"], &mut payload);
        encode_command(&["SET", "never", "1"], &mut payload);
        c.send_raw(&payload);
        let replies = c.read_until_closed();
        assert_eq!(
            replies,
            vec![
                Reply::Ok,
                Reply::Array(vec![Reply::bulk("a")]),
                Reply::Int(7),
                Reply::Ok
            ]
        );
        assert_eq!(env.client().cmd(&["GET", "never"]), Reply::Nil);
    });
}
