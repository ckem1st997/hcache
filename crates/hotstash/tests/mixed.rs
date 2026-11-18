//! Embedded sessions and TCP clients on the same `Db` at the same time.

mod net;

use hotstash::{Config, Db, Error, Reply, ServerConfig};
use hotstash_core::engine::Engine;
use hotstash_core::resp::encode_command;
use hotstash_core::testutil::Rng;
use net::*;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

const COUNTERS: usize = 100;
const BALANCES: usize = 20;
const TRANSFER: &str =
    "redis.call('DECRBY', KEYS[1], ARGV[1]) redis.call('INCRBY', KEYS[2], ARGV[1]) return 1";
const SPIN: [&str; 3] = ["EVAL", "while true do end", "0"];
const KILL: [&str; 2] = ["SCRIPT", "KILL"];

fn join_all<T>(handles: Vec<thread::JoinHandle<T>>) -> Vec<T> {
    handles
        .into_iter()
        .map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
        .collect()
}

fn pong() -> Reply {
    Reply::Simple("PONG".into())
}

/// Eight keys, half of them on each of two shards.
fn spread_keys(cfg: &Config) -> Vec<String> {
    let engine = Engine::new(cfg.clone());
    let mut per_shard: Vec<Vec<String>> = vec![Vec::new(); cfg.shards];
    let per = 8 / cfg.shards;
    let mut i = 0;
    while per_shard.iter().any(|v| v.len() < per) {
        let key = format!("m{i}");
        let shard = engine.route(key.as_bytes()).1;
        if per_shard[shard].len() < per {
            per_shard[shard].push(key);
        }
        i += 1;
    }
    per_shard.concat()
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Incr(usize),
    Transfer(usize, usize, i64),
    Mset(u64),
    Mget,
}

fn choose(rng: &mut Rng, who: u64, seq: &mut u64) -> Op {
    match rng.below(100) {
        0..=49 => Op::Incr(rng.below(COUNTERS as u64) as usize),
        50..=64 => {
            let a = rng.below(BALANCES as u64) as usize;
            let b = (a + 1 + rng.below(BALANCES as u64 - 1) as usize) % BALANCES;
            Op::Transfer(a, b, 1 + rng.below(50) as i64)
        }
        65..=79 => {
            *seq += 1;
            Op::Mset(who << 32 | *seq)
        }
        _ => Op::Mget,
    }
}

fn args(op: Op, keys: &[String]) -> Vec<Vec<u8>> {
    let s = |x: &str| x.as_bytes().to_vec();
    match op {
        Op::Incr(j) => vec![s("INCR"), format!("ctr:{j}").into_bytes()],
        Op::Transfer(a, b, n) => vec![
            s("EVAL"),
            s(TRANSFER),
            s("2"),
            format!("bal:{a}").into_bytes(),
            format!("bal:{b}").into_bytes(),
            n.to_string().into_bytes(),
        ],
        Op::Mset(v) => {
            let mut out = vec![s("MSET")];
            for k in keys {
                out.push(s(k));
                out.push(format!("v{v}").into_bytes());
            }
            out
        }
        Op::Mget => {
            let mut out = vec![s("MGET")];
            out.extend(keys.iter().map(|k| s(k)));
            out
        }
    }
}

fn check(op: Op, reply: &Reply, last: &mut [i64], who: &str) {
    match op {
        Op::Incr(j) => {
            let Reply::Int(n) = reply else {
                panic!("{who}: INCR ctr:{j} answered {}", short(reply));
            };
            assert!(
                *n > last[j],
                "{who}: INCR ctr:{j} went from {} to {n}",
                last[j]
            );
            last[j] = *n;
        }
        Op::Transfer(..) => assert_eq!(*reply, Reply::Int(1), "{who}: transfer"),
        Op::Mset(_) => assert_eq!(*reply, Reply::Ok, "{who}: MSET"),
        Op::Mget => {
            let Reply::Array(items) = reply else {
                panic!("{who}: MGET answered {}", short(reply));
            };
            assert_eq!(items.len(), 8, "{who}: MGET length");
            assert!(
                items.iter().all(|i| *i == items[0]),
                "{who}: torn MGET (MSET of 8 keys on 2 shards seen half applied): {items:?}"
            );
        }
    }
}

fn embedded_worker(db: Db, id: u64, keys: Vec<String>, ops: usize) -> Vec<u64> {
    let session = db.session().expect("session");
    let mut rng = Rng::new(id);
    let (mut seq, mut last, mut tally) = (0, vec![0i64; COUNTERS], vec![0u64; COUNTERS]);
    let who = format!("embedded {id}");
    for _ in 0..ops {
        let op = choose(&mut rng, id, &mut seq);
        let reply = session.execute(&args(op, &keys));
        check(op, &reply, &mut last, &who);
        if let Op::Incr(j) = op {
            tally[j] += 1;
        }
    }
    tally
}

fn tcp_worker(addr: std::net::SocketAddr, id: u64, keys: Vec<String>, ops: usize) -> Vec<u64> {
    let mut c = Client::connect(addr);
    let mut rng = Rng::new(id);
    let (mut seq, mut last, mut tally) = (0, vec![0i64; COUNTERS], vec![0u64; COUNTERS]);
    let who = format!("tcp {id}");
    let mut done = 0;
    while done < ops {
        let mut batch = Vec::new();
        let mut sent = Vec::new();
        for _ in 0..8.min(ops - done) {
            let op = choose(&mut rng, id, &mut seq);
            let a = args(op, &keys);
            let refs: Vec<&[u8]> = a.iter().map(Vec::as_slice).collect();
            encode_command(&refs, &mut batch);
            sent.push(op);
        }
        c.send_raw(&batch);
        for op in sent {
            let reply = c.read();
            check(op, &reply, &mut last, &who);
            if let Op::Incr(j) = op {
                tally[j] += 1;
            }
            done += 1;
        }
    }
    tally
}

#[test]
fn embedded_and_tcp_clients_hammer_the_same_counters() {
    let _h = heavy();
    watchdog("mixed-hammer", 150, || {
        let cfg = Config {
            shards: 2,
            multi_escalate_after: 1,
            ..Config::default()
        };
        let keys = spread_keys(&cfg);
        let env = Env::new(cfg, server_cfg(4, 16));
        let db = env.db.clone();
        {
            let s = db.session().expect("session");
            for i in 0..BALANCES {
                let key = format!("bal:{i}");
                assert_eq!(s.execute(&["SET", key.as_str(), "1000"]), Reply::Ok);
            }
        }
        let mut handles = Vec::new();
        for id in 0..64u64 {
            let (db, keys) = (db.clone(), keys.clone());
            handles.push(thread::spawn(move || embedded_worker(db, id, keys, 4000)));
        }
        for id in 64..96u64 {
            let (addr, keys) = (env.addr, keys.clone());
            handles.push(thread::spawn(move || tcp_worker(addr, id, keys, 4000)));
        }
        let mut totals = vec![0u64; COUNTERS];
        for tally in join_all(handles) {
            for (t, n) in totals.iter_mut().zip(tally) {
                *t += n;
            }
        }
        let s = db.session().expect("session");
        for (j, total) in totals.iter().enumerate() {
            let key = format!("ctr:{j}");
            let want = if *total == 0 {
                Reply::Nil
            } else {
                Reply::bulk(total.to_string())
            };
            assert_eq!(s.execute(&["GET", key.as_str()]), want, "ctr:{j} total");
        }
        let mut sum = 0i64;
        for i in 0..BALANCES {
            let key = format!("bal:{i}");
            let Reply::Bulk(b) = s.execute(&["GET", key.as_str()]) else {
                panic!("balance {i} missing");
            };
            sum += String::from_utf8_lossy(&b)
                .parse::<i64>()
                .expect("integer balance");
        }
        assert_eq!(
            sum,
            BALANCES as i64 * 1000,
            "EVAL transfers lost or created money"
        );
        let st = db.stats();
        eprintln!(
            "mixed hammer stats: deferred={} multi_deferrals={} multi_escalations={} exclusive={} kicks={} tasks_run={}",
            st.deferred, st.multi_deferrals, st.multi_escalations, st.exclusive, st.kicks, st.tasks_run
        );
        assert!(
            st.deferred > 0,
            "no single-key command was ever deferred: {st:?}"
        );
        assert!(
            st.multi_escalations > 0,
            "no multi-key command ever escalated: {st:?}"
        );
        assert!(st.exclusive > 0, "no exclusive command ran: {st:?}");
        assert_eq!(env.wait_conns(0, 5), 0);
    });
}

#[test]
fn script_in_an_embedded_thread_and_tcp_clients() {
    watchdog("mixed-script", 90, || {
        let env = Env::new(engine_cfg(4), server_cfg(2, 8));
        let _guard = KillScriptsOnDrop(env.db.clone());
        let db = env.db.clone();
        assert_eq!(
            db.session().expect("session").execute(&["SET", "x", "1"]),
            Reply::Ok
        );

        // An embedded thread runs the infinite script.
        let base = db.stats().exclusive;
        let spinner = {
            let db = db.clone();
            thread::spawn(move || db.session().expect("session").execute(&SPIN))
        };
        assert!(
            wait_until(5, || db.stats().exclusive > base),
            "script did not start"
        );
        thread::sleep(Duration::from_millis(100));

        let mut c = env.client();
        let mut worst = Duration::ZERO;
        for _ in 0..50 {
            let t = Instant::now();
            assert_eq!(c.cmd(&["PING"]), pong());
            worst = worst.max(t.elapsed());
        }
        let session = db.session().expect("session");
        assert_eq!(
            session.execute(&["PING"]),
            pong(),
            "embedded PING during a script"
        );
        let mut g = env.client();
        g.send(&["GET", "x"]);
        assert!(
            g.silent_for(300),
            "TCP GET answered while an embedded script runs"
        );
        let (tx, rx) = mpsc::channel();
        {
            let db = db.clone();
            thread::spawn(move || {
                let r = db.session().expect("session").execute(&["GET", "x"]);
                let _ = tx.send(r);
            });
        }
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "embedded GET answered while a script runs"
        );
        let t = Instant::now();
        let mut k = env.client();
        assert_eq!(
            k.cmd(&KILL),
            Reply::Ok,
            "TCP SCRIPT KILL of an embedded script"
        );
        let killed = spinner.join().expect("spinner");
        let kill_latency = t.elapsed();
        assert!(
            error_text(&killed).is_some_and(|e| e.contains("killed")),
            "embedded script result: {killed:?}"
        );
        assert_eq!(g.read(), Reply::bulk("1"), "parked TCP GET lost");
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("embedded GET never answered"),
            Reply::bulk("1")
        );
        eprintln!(
            "embedded script: worst TCP PING {:.2} ms, TCP SCRIPT KILL to embedded script's error {:.2} ms",
            ms(worst),
            ms(kill_latency)
        );

        // And the other way round: a TCP client's script, killed from here.
        let base = db.stats().exclusive;
        let mut a = env.client();
        a.send(&SPIN);
        assert!(
            wait_until(5, || db.stats().exclusive > base),
            "script did not start"
        );
        thread::sleep(Duration::from_millis(100));
        assert_eq!(session.execute(&["PING"]), pong());
        let t = Instant::now();
        assert_eq!(
            session.execute(&KILL),
            Reply::Ok,
            "embedded SCRIPT KILL of a TCP script"
        );
        let r = a.read();
        eprintln!(
            "embedded SCRIPT KILL to TCP script's error {:.2} ms",
            ms(t.elapsed())
        );
        assert!(
            error_text(&r).is_some_and(|e| e.contains("killed")),
            "{r:?}"
        );
        assert_eq!(session.execute(&["GET", "x"]), Reply::bulk("1"));
        assert_eq!(c.cmd(&["GET", "x"]), Reply::bulk("1"));
    });
}

#[test]
fn session_and_connection_churn_with_exclusive_commands() {
    watchdog("mixed-churn", 120, || {
        let cfg = Config {
            max_slots: 12,
            ..engine_cfg(4)
        };
        let env = Env::new(cfg, server_cfg(4, 8));
        let db = env.db.clone();
        let incrs = Arc::new(AtomicU64::new(0));
        let evals = Arc::new(AtomicU64::new(0));
        let refused = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for t in 0..12u64 {
            let (db, incrs, evals, refused) =
                (db.clone(), incrs.clone(), evals.clone(), refused.clone());
            handles.push(thread::spawn(move || {
                let mut rng = Rng::new(t);
                let mut done = 0;
                while done < 150 {
                    let session = match db.session() {
                        Ok(s) => s,
                        Err(Error::TooManySessions) => {
                            refused.fetch_add(1, SeqCst);
                            thread::sleep(Duration::from_micros(200));
                            continue;
                        }
                        Err(other) => panic!("db.session() failed with {other:?}"),
                    };
                    for _ in 0..1 + rng.below(4) {
                        match rng.below(3) {
                            0 => {
                                assert!(matches!(session.execute(&["INCR", "cnt"]), Reply::Int(_)));
                                incrs.fetch_add(1, SeqCst);
                            }
                            1 => {
                                let r = session.execute(&[
                                    "EVAL",
                                    "return redis.call('INCR', KEYS[1])",
                                    "1",
                                    "ecnt",
                                ]);
                                assert!(matches!(r, Reply::Int(_)), "{r:?}");
                                evals.fetch_add(1, SeqCst);
                            }
                            _ => assert_eq!(session.execute(&["PING"]), pong()),
                        }
                    }
                    drop(session);
                    done += 1;
                }
            }));
        }
        for t in 0..6u64 {
            let (addr, incrs, evals) = (env.addr, incrs.clone(), evals.clone());
            handles.push(thread::spawn(move || {
                let mut rng = Rng::new(100 + t);
                for _ in 0..120 {
                    let mut c = Client::connect(addr);
                    match rng.below(4) {
                        0 => {
                            let r =
                                c.cmd(&["EVAL", "return redis.call('INCR', KEYS[1])", "1", "ecnt"]);
                            assert!(matches!(r, Reply::Int(_)), "{r:?}");
                            evals.fetch_add(1, SeqCst);
                        }
                        1 => {
                            assert!(matches!(c.cmd(&["INCR", "cnt"]), Reply::Int(_)));
                            incrs.fetch_add(1, SeqCst);
                        }
                        2 => {
                            // Leaves a half command behind.
                            c.send_raw(b"*3\r\n$4\r\nINCR\r\n$3\r\ncnt");
                        }
                        _ => {
                            assert_eq!(c.cmd(&["PING"]), pong());
                        }
                    }
                }
            }));
        }
        join_all(handles);
        assert_eq!(env.wait_conns(0, 10), 0);
        eprintln!(
            "session churn: {} refusals with TooManySessions",
            refused.load(SeqCst)
        );
        let s = db.session().expect("session");
        let get = |k: &str| match s.execute(&["GET", k]) {
            Reply::Bulk(b) => String::from_utf8_lossy(&b).parse::<u64>().expect("integer"),
            Reply::Nil => 0,
            other => panic!("GET {k}: {other:?}"),
        };
        assert_eq!(get("cnt"), incrs.load(SeqCst), "INCR total");
        assert_eq!(get("ecnt"), evals.load(SeqCst), "EVAL INCR total");
        // No slot leaked: 12 slots, 4 held by the workers, 1 by `s`.
        let more: Vec<_> = (0..7).map(|_| db.session().expect("slot leaked")).collect();
        assert!(matches!(db.session(), Err(Error::TooManySessions)));
        drop(more);
    });
}

#[test]
fn running_out_of_slots_is_a_clean_error() {
    watchdog("mixed-slots", 30, || {
        let cfg = Config {
            max_slots: 3,
            ..engine_cfg(2)
        };
        let db = Db::open(None, cfg).expect("db");
        let s1 = db.session().expect("first session");
        let s2 = db.session().expect("second session");
        match db.listen(server_cfg(2, 8)) {
            Err(Error::Io(e)) => eprintln!("listen with too few slots: {e}"),
            Err(other) => panic!("wrong error {other:?}"),
            Ok(_) => panic!("a server with 2 workers started with 1 free slot"),
        }
        // A failed start must give its slots back.
        let s3 = db
            .session()
            .expect("slot leaked by the failed Server::start");
        assert!(matches!(db.session(), Err(Error::TooManySessions)));
        drop(s3);
        drop(s2);
        let server = db
            .listen(server_cfg(2, 8))
            .expect("server with the slots now free");
        assert!(matches!(db.session(), Err(Error::TooManySessions)));
        let mut c = Client::connect(server.local_addr());
        assert_eq!(c.cmd(&["SET", "a", "1"]), Reply::Ok);
        assert_eq!(s1.execute(&["GET", "a"]), Reply::bulk("1"));
        server.shutdown();
        let _again = db.session().expect("slots not released by shutdown");
        // Many more workers than slots.
        let wide = Db::open(
            None,
            Config {
                max_slots: 8,
                ..engine_cfg(2)
            },
        )
        .expect("db");
        assert!(matches!(wide.listen(server_cfg(50, 8)), Err(Error::Io(_))));
        let held: Vec<_> = (0..8)
            .map(|_| wide.session().expect("slot leaked"))
            .collect();
        drop(held);
        // An address in use is an error, not a panic, and costs no slots.
        let taken = TcpListener::bind("127.0.0.1:0").expect("bind");
        let scfg = ServerConfig {
            addr: taken.local_addr().expect("addr"),
            ..server_cfg(2, 8)
        };
        assert!(matches!(wide.listen(scfg), Err(Error::Io(_))));
        let held: Vec<_> = (0..8)
            .map(|_| wide.session().expect("slot leaked by bind failure"))
            .collect();
        drop(held);
    });
}

#[test]
fn embedded_threads_keep_working_across_server_restarts() {
    let _h = heavy();
    watchdog("mixed-restart", 150, || {
        let cfg = Config {
            shards: 2,
            multi_escalate_after: 1,
            ..Config::default()
        };
        let keys = spread_keys(&cfg);
        let db = Db::open(None, cfg).expect("db");
        {
            let s = db.session().expect("session");
            for i in 0..BALANCES {
                let key = format!("bal:{i}");
                assert_eq!(s.execute(&["SET", key.as_str(), "1000"]), Reply::Ok);
            }
        }
        let stop = Arc::new(AtomicBool::new(false));
        let embedded: Vec<_> = (0..8u64)
            .map(|id| {
                let (db, keys, stop) = (db.clone(), keys.clone(), stop.clone());
                thread::spawn(move || {
                    let session = db.session().expect("session");
                    let mut rng = Rng::new(id);
                    let (mut seq, mut last, mut tally) =
                        (0, vec![0i64; COUNTERS], vec![0u64; COUNTERS]);
                    let who = format!("embedded {id}");
                    while !stop.load(SeqCst) {
                        let op = choose(&mut rng, id, &mut seq);
                        let reply = session.execute(&args(op, &keys));
                        check(op, &reply, &mut last, &who);
                        if let Op::Incr(j) = op {
                            tally[j] += 1;
                        }
                    }
                    tally
                })
            })
            .collect();

        for cycle in 0..10u64 {
            let server = db.listen(server_cfg(2, 4)).expect("listen");
            let addr = server.local_addr();
            let clients: Vec<_> = (0..6u64)
                .map(|i| {
                    thread::spawn(move || {
                        let mut c = Client::with_timeout(addr, Duration::from_secs(10));
                        let key = format!("own:{cycle}:{i}");
                        let mut n = 0i64;
                        let mut marker = 0u64;
                        loop {
                            // Replies are known exactly: any reply that is
                            // not ours (or arrives twice) shows up here.
                            let mut batch = Vec::new();
                            let mut expect = Vec::new();
                            for _ in 0..6 {
                                encode_command(&["INCR", key.as_str()], &mut batch);
                                n += 1;
                                expect.push(Reply::Int(n));
                            }
                            encode_command(&["KEYS", "zzz-matches-nothing*"], &mut batch);
                            expect.push(Reply::Array(vec![]));
                            marker += 1;
                            let m = format!("{i}-{marker}");
                            encode_command(&["ECHO", m.as_str()], &mut batch);
                            expect.push(Reply::bulk(m));
                            if c.try_send_raw(&batch).is_err() {
                                return (key, n - 6);
                            }
                            for (k, want) in expect.iter().enumerate() {
                                match c.try_read() {
                                    Ok(got) => assert!(
                                        got == *want,
                                        "cycle {cycle} client {i}: reply {k} of a batch is {} instead of {}",
                                        short(&got),
                                        short(want)
                                    ),
                                    Err(e) if is_closed(&e) => {
                                        return (key, n - 6 + k.min(6) as i64);
                                    }
                                    Err(e) => panic!("cycle {cycle} client {i}: {e}"),
                                }
                            }
                        }
                    })
                })
                .collect();
            thread::sleep(Duration::from_millis(40 + 10 * cycle));
            let t = Instant::now();
            server.shutdown();
            assert!(
                t.elapsed() < Duration::from_secs(5),
                "shutdown took {:?}",
                t.elapsed()
            );
            let s = db.session().expect("session");
            for (key, acked) in join_all(clients) {
                let got = match s.execute(&["GET", key.as_str()]) {
                    Reply::Bulk(b) => String::from_utf8_lossy(&b).parse::<i64>().expect("int"),
                    Reply::Nil => 0,
                    other => panic!("GET {key}: {other:?}"),
                };
                assert!(
                    got >= acked,
                    "{key}: server holds {got}, client saw {acked} acknowledged"
                );
            }
        }
        stop.store(true, SeqCst);
        let mut totals = vec![0u64; COUNTERS];
        for tally in join_all(embedded) {
            for (t, n) in totals.iter_mut().zip(tally) {
                *t += n;
            }
        }
        let s = db.session().expect("session");
        let server = db.listen(server_cfg(2, 4)).expect("final server");
        let mut c = Client::connect(server.local_addr());
        for (j, total) in totals.iter().enumerate() {
            let key = format!("ctr:{j}");
            let want = if *total == 0 {
                Reply::Nil
            } else {
                Reply::bulk(total.to_string())
            };
            assert_eq!(
                s.execute(&["GET", key.as_str()]),
                want,
                "embedded view of ctr:{j}"
            );
            assert_eq!(c.cmd(&["GET", key.as_str()]), want, "TCP view of ctr:{j}");
        }
    });
}
