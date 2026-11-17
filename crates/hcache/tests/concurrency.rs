//! Multi-threaded adversarial tests of the engine through the embedded API
//! (and, for the deferral/escalation paths that sessions never take, through
//! `Engine::submit` with a small in-process `Hooks` implementation).
//!
//! Every test runs under a watchdog: a deadlock fails the test.

mod support;

use hcache::{Config, Db, Error, Reply, Session};
use hcache_core::cmd::{Class, Cmd};
use hcache_core::engine::{Engine, Hooks, Submitted, Ticket};
use hcache_core::resp::decode_reply;
use hcache_core::testutil::Rng;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use support::*;

/// (shards, drain_batch, multi_escalate_after)
const MATRIX: &[(usize, usize, u32)] = &[(1, 0, 0), (2, 1, 1), (4, 2, 2), (4, 1, 0)];

/// The tests here saturate every core and some measure latency, so they take
/// turns; the watchdog clock starts only once a test has its turn.
fn serial_guarded<T: Send + 'static>(
    name: &str,
    secs: u64,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    static TURN: Mutex<()> = Mutex::new(());
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    guarded(name, secs, f)
}

/// Run `f(0..n)` on `n` threads and collect the results in thread order.
fn par<T: Send, F: Fn(usize) -> T + Sync>(n: usize, f: F) -> Vec<T> {
    let f = &f;
    thread::scope(|sc| {
        let handles: Vec<_> = (0..n).map(|i| sc.spawn(move || f(i))).collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .collect()
    })
}

/// Counts a finished worker even when it panics, so the threads that wait for
/// "all writers done" cannot spin forever and hide the panic.
struct Done<'a>(&'a AtomicUsize);

impl Drop for Done<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, SeqCst);
    }
}

/// Raises a stop flag on drop (normal end or panic).
struct StopOnDrop<'a>(&'a AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, SeqCst);
    }
}

/// `n` keys with the given prefix that cover as many shards as possible.
fn spread_keys(config: &Config, n: usize, prefix: &str) -> Vec<String> {
    let engine = Engine::new(config.clone());
    let shards = config.shards.clamp(1, 1 << 16);
    let mut chosen: Vec<String> = Vec::new();
    let mut used = HashSet::new();
    let mut i = 0;
    while chosen.len() < n {
        let key = format!("{prefix}{i}");
        i += 1;
        let shard = engine.route(key.as_bytes()).1;
        if used.len() < shards.min(n) && !used.insert(shard) {
            continue;
        }
        chosen.push(key);
    }
    let distinct: HashSet<_> = chosen
        .iter()
        .map(|k| engine.route(k.as_bytes()).1)
        .collect();
    assert!(
        distinct.len() >= shards.min(n),
        "could not spread keys over shards"
    );
    chosen
}

fn strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

fn db_stats_line(db: &Db) -> String {
    format!("{:?}", db.stats())
}

// ---------------------------------------------------------------------
// INCR: no lost updates
// ---------------------------------------------------------------------

fn incr_totals(config: Config, threads: usize, per: usize) {
    let db = open(config);
    let keys = ["c0", "c1", "c2", "c3", "c4"];
    let results: Vec<Vec<(usize, i64)>> = par(threads, |t| {
        let s = session(&db);
        (0..per)
            .map(|i| {
                let k = (t + i) % keys.len();
                (k, int(&s.execute(&["INCR", keys[k]])))
            })
            .collect()
    });
    let s = session(&db);
    for (k, key) in keys.iter().enumerate() {
        let mut returned: Vec<i64> = results
            .iter()
            .flatten()
            .filter(|(kk, _)| *kk == k)
            .map(|(_, v)| *v)
            .collect();
        returned.sort_unstable();
        let total = returned.len() as i64;
        // Every INCR saw a distinct value: 1..=total, nothing skipped or repeated.
        assert!(
            returned.iter().copied().eq(1..=total),
            "{key}: INCR results are not a permutation of 1..={total}"
        );
        assert_eq!(s.execute(&["GET", key]), bulk(&total.to_string()), "{key}");
    }
}

#[test]
fn incr_no_lost_updates_16_threads() {
    serial_guarded("incr16", 120, || {
        for &(shards, drain, esc) in MATRIX {
            incr_totals(cfg(shards, drain, esc), 16, 3000);
        }
    });
}

#[test]
fn incr_no_lost_updates_64_threads() {
    serial_guarded("incr64", 120, || {
        for &(shards, drain, esc) in &MATRIX[..3] {
            incr_totals(cfg(shards, drain, esc), 64, 600);
        }
    });
}

#[test]
fn incrby_decrby_signed_totals() {
    serial_guarded("incrby", 120, || {
        for &(shards, drain, esc) in MATRIX {
            let db = open(cfg(shards, drain, esc));
            let threads = 16usize;
            let per = 1500usize;
            par(threads, |t| {
                let s = session(&db);
                let mut rng = Rng::new(t as u64 + 1);
                for _ in 0..per {
                    let key = format!("n{}", rng.below(3));
                    let by = (t as i64 + 1) * 3;
                    let cmd = if t % 2 == 0 { "INCRBY" } else { "DECRBY" };
                    assert!(!s.execute(&[cmd, key.as_str(), &by.to_string()]).is_error());
                }
            });
            // Even threads add, odd threads subtract; the split across the
            // three keys is random but the grand total is exact.
            let s = session(&db);
            let total: i64 = (0..3)
                .map(|k| match s.execute(&["GET", &format!("n{k}")]) {
                    Reply::Bulk(b) => String::from_utf8(b).unwrap().parse::<i64>().unwrap(),
                    Reply::Nil => 0,
                    other => panic!("{}", brief(&other)),
                })
                .sum();
            let want: i64 = (0..threads as i64)
                .map(|t| {
                    let by = (t + 1) * 3 * per as i64;
                    if t % 2 == 0 {
                        by
                    } else {
                        -by
                    }
                })
                .sum();
            assert_eq!(total, want, "shards={shards}");
        }
    });
}

// ---------------------------------------------------------------------
// MSET atomicity against MGET
// ---------------------------------------------------------------------

fn mset_args(keys: &[String], value: &str, variant: usize, rng: &mut Rng) -> Vec<String> {
    let mut order: Vec<usize> = (0..keys.len()).collect();
    match variant {
        0 => {}
        1 => order.reverse(),
        2 => order.push(0), // a key repeated in one command, same value
        _ => {
            for i in (1..order.len()).rev() {
                order.swap(i, rng.below(i as u64 + 1) as usize);
            }
            order.push(rng.below(keys.len() as u64) as usize);
            order.insert(0, rng.below(keys.len() as u64) as usize);
        }
    }
    let mut args = vec!["MSET".to_string()];
    for i in order {
        args.push(keys[i].clone());
        args.push(value.to_string());
    }
    args
}

fn mget_args(keys: &[String], variant: usize) -> Vec<String> {
    let mut order: Vec<usize> = (0..keys.len()).collect();
    match variant % 3 {
        0 => {}
        1 => order.reverse(),
        _ => order.extend([0, 1, 0]),
    }
    let mut args = vec!["MGET".to_string()];
    args.extend(order.into_iter().map(|i| keys[i].clone()));
    args
}

fn mset_atomicity(config: Config, writers: usize, readers: usize, iters: usize) {
    let keys = spread_keys(&config, 8, "m");
    let db = open(config);
    {
        let s = session(&db);
        let mut args = vec!["MSET".to_string()];
        for k in &keys {
            args.push(k.clone());
            args.push("init".into());
        }
        assert_eq!(s.execute(&args), Reply::Ok);
    }
    let done = AtomicUsize::new(0);
    let observations = AtomicU64::new(0);
    let keys = &keys;
    let (done, observations, db) = (&done, &observations, &db);
    par(writers + readers, |t| {
        let s = session(db);
        let mut rng = Rng::new(t as u64 * 7 + 1);
        if t < writers {
            let _done = Done(done);
            for i in 0..iters {
                let args = mset_args(keys, &format!("w{t}-{i}"), t % 4, &mut rng);
                assert_eq!(s.execute(&args), Reply::Ok);
            }
        } else {
            let mut n = 0u64;
            while done.load(SeqCst) < writers {
                let args = mget_args(keys, t);
                let reply = array(s.execute(&args));
                let first = reply[0].clone();
                assert!(
                    matches!(first, Reply::Bulk(_)),
                    "MGET returned {}",
                    brief(&first)
                );
                for r in &reply {
                    assert!(
                        *r == first,
                        "TORN MSET: MGET saw {} and {}",
                        brief(&first),
                        brief(r)
                    );
                }
                n += 1;
            }
            observations.fetch_add(n, SeqCst);
        }
    });
    let s = session(db);
    let last = array(s.execute(&mget_args(keys, 0)));
    assert!(last.iter().all(|r| *r == last[0]), "final state is torn");
    assert!(
        observations.load(SeqCst) > 0,
        "readers never observed anything"
    );
}

#[test]
fn mset_is_atomic_for_mget_readers() {
    serial_guarded("mset_atomic", 120, || {
        for &(shards, drain, esc) in MATRIX {
            mset_atomicity(cfg(shards, drain, esc), 8, 8, 1500);
        }
    });
}

#[test]
fn mset_is_atomic_with_many_threads() {
    serial_guarded("mset_atomic_64", 120, || {
        mset_atomicity(cfg(4, 1, 1), 32, 32, 300);
    });
}

#[test]
fn opposite_order_multi_key_commands_do_not_deadlock() {
    serial_guarded("deadlock_bait", 60, || {
        // Two overlapping key sets given in opposite orders, with DEL and
        // EXISTS (multi-shard readers/writers) mixed in.
        let config = cfg(4, 1, 1);
        let keys = spread_keys(&config, 8, "d");
        let db = open(config);
        par(16, |t| {
            let s = session(&db);
            let mut order: Vec<&String> = keys.iter().collect();
            if t % 2 == 1 {
                order.reverse();
            }
            for i in 0..1500 {
                let mut args: Vec<String> = Vec::new();
                match i % 4 {
                    0 => {
                        args.push("MSET".into());
                        for k in &order {
                            args.push((*k).clone());
                            args.push(format!("{t}"));
                        }
                    }
                    1 => {
                        args.push("MGET".into());
                        args.extend(order.iter().map(|k| (*k).clone()));
                    }
                    2 => {
                        args.push("EXISTS".into());
                        args.extend(order.iter().map(|k| (*k).clone()));
                    }
                    _ => {
                        args.push("DEL".into());
                        args.extend(order.iter().map(|k| (*k).clone()));
                    }
                }
                let reply = s.execute(&args);
                assert!(!reply.is_error(), "{}", brief(&reply));
            }
        });
    });
}

// ---------------------------------------------------------------------
// Bank transfer by EVAL
// ---------------------------------------------------------------------

const TRANSFER: &str = "local bal = tonumber(redis.call('GET', KEYS[1]))\n\
local amt = tonumber(ARGV[1])\n\
if bal < amt then return 0 end\n\
redis.call('DECRBY', KEYS[1], amt)\n\
redis.call('INCRBY', KEYS[2], amt)\n\
return 1";

fn bank(config: Config) {
    let accounts = spread_keys(&config, 8, "acct");
    let db = open(config);
    let per_account = 1000i64;
    {
        let s = session(&db);
        for a in &accounts {
            s.execute(&["SET", a, &per_account.to_string()]);
        }
    }
    let total = per_account * accounts.len() as i64;
    let transfers = 4usize;
    let observers = 4usize;
    let done = AtomicUsize::new(0);
    let evals = AtomicU64::new(0);
    let (done, evals, db, accounts) = (&done, &evals, &db, &accounts);
    let sums: Vec<u64> = par(transfers + observers, |t| {
        let s = session(db);
        let mut rng = Rng::new(t as u64 + 100);
        if t < transfers {
            let _done = Done(done);
            for _ in 0..300 {
                let from = rng.below(accounts.len() as u64) as usize;
                let to =
                    (from + 1 + rng.below(accounts.len() as u64 - 1) as usize) % accounts.len();
                let amt = (1 + rng.below(400)).to_string();
                let reply = eval(
                    &s,
                    TRANSFER,
                    &[accounts[from].as_str(), accounts[to].as_str()],
                    &[amt.as_str()],
                );
                assert!(
                    matches!(reply, Reply::Int(0 | 1)),
                    "transfer: {}",
                    brief(&reply)
                );
                evals.fetch_add(1, SeqCst);
            }
            0
        } else {
            let mut args = vec!["MGET"];
            args.extend(strs(accounts));
            let mut observed = 0;
            while done.load(SeqCst) < transfers {
                let balances = array(s.execute(&args));
                let sum: i64 = balances
                    .iter()
                    .map(|r| text(r).parse::<i64>().unwrap())
                    .inspect(|b| assert!(*b >= 0, "negative balance {b}"))
                    .sum();
                assert_eq!(sum, total, "MGET observed money being created/destroyed");
                observed += 1;
                // A read-only script must see a consistent state too.
                let reply = eval(
                    &s,
                    "local s=0 for i=1,#KEYS do s = s + tonumber(redis.call('GET', KEYS[i])) end return s",
                    &strs(accounts),
                    &[],
                );
                assert_eq!(reply, Reply::Int(total));
            }
            observed
        }
    });
    let s = session(db);
    let mut args = vec!["MGET"];
    args.extend(strs(accounts));
    let sum: i64 = array(s.execute(&args))
        .iter()
        .map(|r| text(r).parse::<i64>().unwrap())
        .sum();
    assert_eq!(sum, total, "final sum");
    assert!(
        sums.iter().sum::<u64>() > 0,
        "observers never ran during transfers"
    );
    let stats = db.stats();
    assert!(
        stats.exclusive >= evals.load(SeqCst),
        "every EVAL must take the exclusive side: {stats:?}"
    );
}

#[test]
fn bank_transfer_by_eval_keeps_sum_constant() {
    serial_guarded("bank", 120, || {
        for &(shards, drain, esc) in MATRIX {
            bank(cfg(shards, drain, esc));
        }
    });
}

// ---------------------------------------------------------------------
// FLUSHALL racing everyone
// ---------------------------------------------------------------------

fn flush_race(config: Config) {
    let db = open(config);
    let writers = 6usize;
    let readers = 4usize;
    let done = AtomicUsize::new(0);
    let (done, db) = (&done, &db);
    par(writers + readers + 1, |t| {
        let s = session(db);
        let mut rng = Rng::new(t as u64 + 500);
        if t < writers {
            let _done = Done(done);
            for i in 0..3000 {
                let k = format!("p{}", rng.below(24));
                match rng.below(7) {
                    0 => assert_eq!(s.execute(&["SET", &k, "v"]), Reply::Ok),
                    1 => assert!(matches!(
                        s.execute(&["INCR", &k]),
                        Reply::Int(_) | Reply::Error(_)
                    )),
                    2 => assert!(matches!(
                        s.execute(&["HSET", &k, "f", "v"]),
                        Reply::Int(_) | Reply::Error(_)
                    )),
                    3 => {
                        s.execute(&["DEL", &k, "p1", "p2"]);
                    }
                    _ => {
                        // A pair that must always be seen both set or both unset.
                        let v = format!("{t}-{i}");
                        assert_eq!(s.execute(&["MSET", "q1", &v, "q2", &v]), Reply::Ok);
                    }
                }
            }
        } else if t < writers + readers {
            while done.load(SeqCst) < writers {
                let k = format!("p{}", rng.below(24));
                let reply = s.execute(&["GET", &k]);
                assert!(matches!(
                    reply,
                    Reply::Bulk(_) | Reply::Nil | Reply::Error(_)
                ));
                let pair = array(s.execute(&["MGET", "q1", "q2"]));
                assert!(
                    pair[0] == pair[1],
                    "pair torn by FLUSHALL: {}",
                    brief(&Reply::Array(pair))
                );
                array(s.execute(&["KEYS", "p*"]));
                let all = s.execute(&["HGETALL", &k]);
                assert!(
                    matches!(&all, Reply::Array(a) if a.len() % 2 == 0)
                        || has_err(&all, "WRONGTYPE")
                );
                let n = int(&s.execute(&["DBSIZE"]));
                assert!(n >= 0);
            }
        } else {
            let mut flushes = 0;
            while done.load(SeqCst) < writers {
                assert_eq!(s.execute(&["FLUSHALL"]), Reply::Ok);
                flushes += 1;
                thread::sleep(Duration::from_micros(rng.below(2000)));
            }
            assert!(flushes > 0);
        }
    });
    // Quiescent: the views must agree with each other.
    let s = session(db);
    let keys = array(s.execute(&["KEYS", "*"]));
    let names: BTreeMap<String, ()> = keys.iter().map(|r| (text(r), ())).collect();
    assert_eq!(names.len(), keys.len(), "duplicate keys in KEYS");
    assert_eq!(
        int(&s.execute(&["DBSIZE"])) as usize,
        keys.len(),
        "DBSIZE vs KEYS"
    );
    for name in names.keys() {
        assert_eq!(
            s.execute(&["EXISTS", name]),
            Reply::Int(1),
            "{name} listed but missing"
        );
    }
    let candidates: Vec<String> = (0..24)
        .map(|i| format!("p{i}"))
        .chain(["q1".into(), "q2".into()])
        .collect();
    for c in candidates {
        let exists = int(&s.execute(&["EXISTS", &c])) == 1;
        assert_eq!(exists, names.contains_key(&c), "{c}: EXISTS vs KEYS");
    }
    assert!(s.execute(&["FLUSHALL"]) == Reply::Ok);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
    assert_eq!(s.execute(&["KEYS", "*"]), Reply::Array(vec![]));
}

#[test]
fn flushall_racing_writers_and_readers() {
    serial_guarded("flush_race", 120, || {
        for &(shards, drain, esc) in MATRIX {
            flush_race(cfg(shards, drain, esc));
        }
    });
}

// ---------------------------------------------------------------------
// KEYS / HGETALL on big collections while others mutate
// ---------------------------------------------------------------------

fn parse_index(s: &str, prefix: char) -> Option<usize> {
    s.strip_prefix(prefix)?.parse().ok()
}

struct ScanSetup {
    shards: usize,
    chunk: usize,
    fields: usize,
    stable_keys: usize,
    rounds: usize,
}

fn scan_during_mutation(p: ScanSetup) -> (Duration, Duration, u64) {
    let db = open(Config {
        shards: p.shards,
        scan_chunk: p.chunk,
        ..Config::default()
    });
    {
        let s = session(&db);
        for start in (0..p.fields).step_by(1000) {
            let mut args = vec!["HSET".to_string(), "big".to_string()];
            for i in start..(start + 1000).min(p.fields) {
                args.push(format!("f{i}"));
                args.push(format!("v{i}"));
            }
            assert_eq!(int(&s.execute(&args)), (args.len() as i64 - 2) / 2);
        }
        for start in (0..p.stable_keys).step_by(1000) {
            let mut args = vec!["MSET".to_string()];
            for i in start..(start + 1000).min(p.stable_keys) {
                args.push(format!("s{i}"));
                args.push(format!("sv{i}"));
            }
            assert_eq!(s.execute(&args), Reply::Ok);
        }
    }
    let stop = AtomicBool::new(false);
    let latency_max_us = AtomicU64::new(0);
    let get_max_us = AtomicU64::new(0);
    let slow_reads = AtomicU64::new(0);
    let gets = AtomicU64::new(0);
    let scan_time = Mutex::new((Duration::ZERO, Duration::ZERO));
    let burst = p.fields / 2;
    let (stop, latency_max_us, gets, scan_time, db) =
        (&stop, &latency_max_us, &gets, &scan_time, &db);
    let (get_max_us, slow_reads) = (&get_max_us, &slow_reads);
    let p = &p;
    par(5, |t| {
        let s = session(db);
        match t {
            // Field churn inside the scanned hash: grows past the next
            // power of two and shrinks back, over and over.
            0 => {
                let mut round = 0usize;
                while !stop.load(SeqCst) {
                    for start in (0..burst).step_by(250) {
                        let mut args = vec!["HSET".to_string(), "big".into()];
                        for i in start..(start + 250).min(burst) {
                            args.push(format!("x{round}-{i}"));
                            args.push("1".into());
                        }
                        s.execute(&args);
                    }
                    for start in (0..burst).step_by(250) {
                        let mut args = vec!["HDEL".to_string(), "big".into()];
                        for i in start..(start + 250).min(burst) {
                            args.push(format!("x{round}-{i}"));
                        }
                        s.execute(&args);
                    }
                    round += 1;
                }
            }
            // Churn of other keys.
            1 => {
                let mut n = 0usize;
                while !stop.load(SeqCst) {
                    let mut args = vec!["MSET".to_string()];
                    for i in 0..200 {
                        args.push(format!("o{}", n + i));
                        args.push("1".into());
                    }
                    s.execute(&args);
                    let mut args = vec!["DEL".to_string()];
                    args.extend((0..200).map(|i| format!("o{}", n + i)));
                    s.execute(&args);
                    s.execute(&["HSET", "other", &format!("g{n}"), "1"]);
                    n += 200;
                }
            }
            // Single-key readers: they must keep completing during scans.
            2 | 3 => {
                let mut rng = Rng::new(t as u64);
                while !stop.load(SeqCst) {
                    let i = rng.below(p.stable_keys as u64);
                    let start = Instant::now();
                    let plain_get = rng.chance(2);
                    let reply = if plain_get {
                        s.execute(&["GET", &format!("s{i}")])
                    } else {
                        s.execute(&["HGET", "big", &format!("f{}", rng.below(p.fields as u64))])
                    };
                    let took = start.elapsed().as_micros() as u64;
                    latency_max_us.fetch_max(took, SeqCst);
                    if plain_get {
                        get_max_us.fetch_max(took, SeqCst);
                    }
                    if took > 100_000 {
                        slow_reads.fetch_add(1, SeqCst);
                    }
                    gets.fetch_add(1, SeqCst);
                    assert!(
                        matches!(reply, Reply::Bulk(_)),
                        "stable key vanished: {}",
                        brief(&reply)
                    );
                }
            }
            _ => {
                let _stop = StopOnDrop(stop);
                let mut hg = Duration::ZERO;
                let mut ks = Duration::ZERO;
                for round in 0..p.rounds {
                    let start = Instant::now();
                    let reply = array(s.execute(&["HGETALL", "big"]));
                    hg = hg.max(start.elapsed());
                    assert!(reply.len().is_multiple_of(2));
                    let mut seen = vec![0u8; p.fields];
                    let mut extra = HashSet::new();
                    for pair in reply.chunks(2) {
                        let field = text(&pair[0]);
                        if let Some(i) = parse_index(&field, 'f') {
                            assert!(i < p.fields, "field {field} never existed");
                            assert_eq!(text(&pair[1]), format!("v{i}"), "wrong value for {field}");
                            seen[i] += 1;
                            assert!(seen[i] == 1, "round {round}: duplicate field {field}");
                        } else {
                            assert!(field.starts_with('x'), "field {field} never existed");
                            assert!(
                                extra.insert(field.clone()),
                                "round {round}: duplicate field {field}"
                            );
                        }
                    }
                    let missing = seen.iter().filter(|c| **c == 0).count();
                    assert_eq!(
                        missing, 0,
                        "round {round}: {missing} stable fields missing from HGETALL"
                    );

                    let start = Instant::now();
                    let reply = array(s.execute(&["KEYS", "*"]));
                    ks = ks.max(start.elapsed());
                    let mut seen = vec![0u8; p.stable_keys];
                    let mut other = HashSet::new();
                    let (mut big, mut oth) = (0, 0);
                    for key in &reply {
                        let key = text(key);
                        if let Some(i) = parse_index(&key, 's') {
                            assert!(i < p.stable_keys, "key {key} never existed");
                            seen[i] += 1;
                            assert!(seen[i] == 1, "round {round}: duplicate key {key}");
                        } else if key == "big" {
                            big += 1;
                        } else if key == "other" {
                            oth += 1;
                        } else {
                            assert!(parse_index(&key, 'o').is_some(), "key {key} never existed");
                            assert!(
                                other.insert(key.clone()),
                                "round {round}: duplicate key {key}"
                            );
                        }
                    }
                    assert_eq!(big, 1, "big missing/duplicated in KEYS");
                    assert!(oth <= 1);
                    let missing = seen.iter().filter(|c| **c == 0).count();
                    assert_eq!(
                        missing, 0,
                        "round {round}: {missing} stable keys missing from KEYS"
                    );
                }
                *scan_time.lock().unwrap() = (hg, ks);
            }
        }
    });
    let (hg, ks) = *scan_time.lock().unwrap();
    let worst = Duration::from_micros(latency_max_us.load(SeqCst));
    let n = gets.load(SeqCst);
    eprintln!(
        "METRIC scan detail: worst plain GET {:.2} ms, reads slower than 100 ms: {}",
        ms(Duration::from_micros(get_max_us.load(SeqCst))),
        slow_reads.load(SeqCst)
    );
    eprintln!(
        "METRIC scan shards={} chunk={} fields={}: slowest HGETALL {:.0} ms, slowest KEYS {:.0} ms, \
         {} single-key reads completed during the scans, worst read latency {:.2} ms",
        p.shards,
        p.chunk,
        p.fields,
        ms(hg),
        ms(ks),
        n,
        ms(worst)
    );
    (worst, hg.max(ks), n)
}

#[test]
fn scans_of_200k_fields_while_others_mutate() {
    serial_guarded("scan200k", 180, || {
        let (worst, _, reads) = scan_during_mutation(ScanSetup {
            shards: 4,
            chunk: 256,
            fields: 200_000,
            stable_keys: 50_000,
            rounds: 4,
        });
        assert!(
            reads > 0,
            "single-key reads made no progress while scanning"
        );
        assert!(
            worst < Duration::from_secs(2),
            "a GET stalled for {worst:?} during a scan"
        );
    });
}

#[test]
fn scans_in_tiny_chunks_while_others_mutate() {
    serial_guarded("scan_tiny", 180, || {
        for (shards, chunk) in [(1, 1), (2, 3), (4, 2)] {
            let (worst, _, reads) = scan_during_mutation(ScanSetup {
                shards,
                chunk,
                fields: 12_000,
                stable_keys: 4_000,
                rounds: 6,
            });
            assert!(reads > 0);
            assert!(
                worst < Duration::from_secs(2),
                "a GET stalled for {worst:?}"
            );
        }
    });
}

// ---------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------

#[test]
fn session_churn_while_exclusive_commands_run() {
    serial_guarded("session_churn", 120, || {
        let db = open(Config {
            shards: 4,
            max_slots: 64,
            ..Config::default()
        });
        let stop = AtomicBool::new(false);
        let (stop, db) = (&stop, &db);
        thread::scope(|sc| {
            let _stop = StopOnDrop(stop);
            for e in 0..2 {
                sc.spawn(move || {
                    let s = session(db);
                    while !stop.load(SeqCst) {
                        assert_eq!(s.execute(&["EVAL", "return 1", "0"]), Reply::Int(1));
                        if e == 0 {
                            assert_eq!(s.execute(&["FLUSHALL"]), Reply::Ok);
                        }
                    }
                });
            }
            let churners: Vec<_> = (0..16)
                .map(|t| {
                    sc.spawn(move || {
                        for i in 0..2500 {
                            // 18 live sessions at most, 64 slots: never refused.
                            let s = match db.session() {
                                Ok(s) => s,
                                Err(e) => panic!("session refused with 18 of 64 slots used: {e}"),
                            };
                            let k = format!("k{t}");
                            match i % 5 {
                                0 => assert_eq!(s.execute(&["SET", &k, "v"]), Reply::Ok),
                                1 => assert!(!s.execute(&["GET", &k]).is_error()),
                                2 => assert!(!s.execute(&["MGET", &k, "x"]).is_error()),
                                3 => {
                                    assert_eq!(s.execute(&["EVAL", "return 2", "0"]), Reply::Int(2))
                                }
                                _ => {}
                            }
                            drop(s);
                        }
                    })
                })
                .collect();
            for c in churners {
                c.join().unwrap_or_else(|e| std::panic::resume_unwind(e));
            }
            stop.store(true, SeqCst);
        });
    });
}

#[test]
fn max_slots_is_enforced_and_slots_are_reusable() {
    serial_guarded("max_slots", 60, || {
        let db = open(Config {
            max_slots: 4,
            ..Config::default()
        });
        let mut held: Vec<Session> = (0..4).map(|_| session(&db)).collect();
        assert!(matches!(db.session(), Err(Error::TooManySessions)));
        assert!(matches!(db.clone().session(), Err(Error::TooManySessions)));
        // All four slots work at once, including exclusive commands.
        for (i, s) in held.iter().enumerate() {
            assert_eq!(s.execute(&["SET", &format!("k{i}"), "v"]), Reply::Ok);
        }
        assert_eq!(held[0].execute(&["EVAL", "return 1", "0"]), Reply::Int(1));
        // A freed slot is handed out again, over and over.
        held.pop();
        for _ in 0..10_000 {
            let s = session(&db);
            assert!(matches!(db.session(), Err(Error::TooManySessions)));
            drop(s);
        }
        // Concurrent churn on 4 slots: never more than 4 live sessions.
        held.clear();
        let live = AtomicUsize::new(0);
        let rejected = AtomicUsize::new(0);
        let (live, rejected, db) = (&live, &rejected, &db);
        par(12, |t| {
            for i in 0..1500 {
                match db.session() {
                    Ok(s) => {
                        let now = live.fetch_add(1, SeqCst) + 1;
                        assert!(now <= 4, "{now} sessions alive with max_slots = 4");
                        if i % 3 == 0 {
                            s.execute(&["EVAL", "return 1", "0"]);
                        } else {
                            s.execute(&["SET", &format!("t{t}"), "v"]);
                        }
                        live.fetch_sub(1, SeqCst);
                        drop(s);
                    }
                    Err(Error::TooManySessions) => {
                        rejected.fetch_add(1, SeqCst);
                        thread::yield_now();
                    }
                    Err(other) => panic!("unexpected error {other}"),
                }
            }
        });
        eprintln!("METRIC max_slots churn: {} refusals", rejected.load(SeqCst));
        // After all drops every slot is free again.
        let again: Vec<Session> = (0..4).map(|_| session(db)).collect();
        assert!(matches!(db.session(), Err(Error::TooManySessions)));
        drop(again);

        let none = open(Config {
            max_slots: 0,
            ..Config::default()
        });
        assert!(matches!(none.session(), Err(Error::TooManySessions)));
        let one = open(Config {
            max_slots: 1,
            ..Config::default()
        });
        let s = session(&one);
        assert_eq!(s.execute(&["EVAL", "return 1", "0"]), Reply::Int(1));
        assert_eq!(s.execute(&["FLUSHALL"]), Reply::Ok);
    });
}

#[test]
fn session_moves_between_threads() {
    serial_guarded("session_move", 60, || {
        let db = open(cfg(2, 1, 1));
        let mut s = session(&db);
        for round in 0..300 {
            s = thread::spawn(move || {
                assert_eq!(s.execute(&["INCR", "n"]), Reply::Int(round + 1));
                if round % 10 == 0 {
                    assert_eq!(
                        s.execute(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "n"]),
                        bulk(&(round + 1).to_string())
                    );
                }
                s
            })
            .join()
            .unwrap_or_else(|e| std::panic::resume_unwind(e));
        }
        assert_eq!(s.execute(&["GET", "n"]), bulk("300"));

        // A ring of threads passing sessions around while all of them also
        // use their own session.
        let (txs, rxs): (Vec<_>, Vec<_>) =
            (0..4).map(|_| mpsc::channel::<(Session, u32)>()).unzip();
        let mut rxs: Vec<_> = rxs.into_iter().map(Some).collect();
        let hops = 2000u32;
        let finished = AtomicUsize::new(0);
        let (db, finished) = (&db, &finished);
        thread::scope(|sc| {
            for t in 0..4usize {
                let rx = rxs[t].take().unwrap();
                let next = txs[(t + 1) % 4].clone();
                sc.spawn(move || {
                    let mine = session(db);
                    loop {
                        match rx.recv_timeout(Duration::from_millis(50)) {
                            Ok((s, left)) => {
                                assert!(!s.execute(&["INCR", "ring"]).is_error());
                                assert!(!mine.execute(&["INCR", "ring"]).is_error());
                                if left == 0 {
                                    finished.fetch_add(1, SeqCst);
                                } else {
                                    next.send((s, left - 1)).expect("ring peer alive");
                                }
                            }
                            Err(_) if finished.load(SeqCst) >= 2 => break,
                            Err(_) => {}
                        }
                    }
                });
            }
            for tx in &txs[..2] {
                tx.send((session(db), hops)).unwrap();
            }
        });
        let s = session(db);
        let ring: i64 = text(&s.execute(&["GET", "ring"])).parse().unwrap();
        assert!(ring >= 2 * i64::from(hops), "ring counter {ring}");
    });
}

#[test]
fn db_dropped_while_sessions_are_alive() {
    serial_guarded("db_drop", 60, || {
        let db = open(cfg(4, 1, 1));
        {
            let s = session(&db);
            for i in 0..2000 {
                s.execute(&["SET", &format!("k{i}"), "v"]);
            }
        }
        let sessions: Vec<Session> = (0..4).map(|_| session(&db)).collect();
        let clone = db.clone();
        drop(db);
        let handles: Vec<_> = sessions
            .into_iter()
            .enumerate()
            .map(|(t, s)| {
                thread::spawn(move || {
                    for i in 0..1500 {
                        match (t, i % 6) {
                            (0, 5) => assert_eq!(s.execute(&["FLUSHALL"]), Reply::Ok),
                            (1, 5) => {
                                assert_eq!(s.execute(&["EVAL", "return 7", "0"]), Reply::Int(7))
                            }
                            (_, 4) => {
                                array(s.execute(&["KEYS", "k1*"]));
                            }
                            _ => {
                                s.execute(&["SET", &format!("k{i}"), "v"]);
                            }
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap_or_else(|e| std::panic::resume_unwind(e));
        }
        // The clone still works, and a new session can still be created.
        let s = session(&clone);
        assert!(!s.execute(&["DBSIZE"]).is_error());
        drop(clone);
        // Last reference gone with a live session: the session keeps it alive.
        assert!(!s.execute(&["PING"]).is_error());
    });
}

// ---------------------------------------------------------------------
// Fairness of the execution gate
// ---------------------------------------------------------------------

fn exclusive_storm(command: &[&str], exclusive_threads: usize) {
    let db = open(cfg(4, 1, 1));
    {
        let s = session(&db);
        s.execute(&["SET", "probe", "v"]);
    }
    let stop = AtomicBool::new(false);
    let gets = AtomicU64::new(0);
    let worst_us = AtomicU64::new(0);
    let excl_ops = AtomicU64::new(0);
    let (stop, gets, worst_us, excl_ops, db) = (&stop, &gets, &worst_us, &excl_ops, &db);
    par(exclusive_threads + 1, |t| {
        let s = session(db);
        if t < exclusive_threads {
            while !stop.load(SeqCst) {
                assert!(!s.execute(command).is_error());
                excl_ops.fetch_add(1, SeqCst);
            }
        } else {
            let _stop = StopOnDrop(stop);
            let end = Instant::now() + Duration::from_millis(1500);
            while Instant::now() < end {
                let start = Instant::now();
                assert!(!s.execute(&["GET", "probe"]).is_error());
                worst_us.fetch_max(start.elapsed().as_micros() as u64, SeqCst);
                gets.fetch_add(1, SeqCst);
            }
        }
    });
    let worst = Duration::from_micros(worst_us.load(SeqCst));
    eprintln!(
        "METRIC exclusive storm {:?} x{}: {} exclusive ops, {} plain GETs completed in 1.5 s, worst GET {:.2} ms",
        command[0],
        exclusive_threads,
        excl_ops.load(SeqCst),
        gets.load(SeqCst),
        ms(worst)
    );
    assert!(gets.load(SeqCst) > 0, "GET starved by exclusive commands");
    assert!(
        worst < Duration::from_secs(2),
        "GET stalled {worst:?} behind a storm of {command:?}"
    );
}

#[test]
fn plain_commands_are_not_starved_by_eval_storm() {
    serial_guarded("storm_eval", 60, || {
        exclusive_storm(&["EVAL", "return 1", "0"], 6)
    });
}

#[test]
fn plain_commands_are_not_starved_by_flushall_storm() {
    serial_guarded("storm_flush", 60, || exclusive_storm(&["FLUSHALL"], 4));
}

#[test]
fn exclusive_command_is_not_starved_by_busy_readers() {
    serial_guarded("busy_readers", 60, || {
        let db = open(cfg(4, 1, 1));
        {
            let s = session(&db);
            s.execute(&["SET", "probe", "v"]);
        }
        let stop = AtomicBool::new(false);
        let worst_us = AtomicU64::new(0);
        let (stop, worst_us, db) = (&stop, &worst_us, &db);
        par(33, |t| {
            let s = session(db);
            if t < 32 {
                while !stop.load(SeqCst) {
                    s.execute(&["GET", "probe"]);
                    s.execute(&["MGET", "probe", "a", "b", "c"]);
                }
            } else {
                let _stop = StopOnDrop(stop);
                for _ in 0..40 {
                    let start = Instant::now();
                    assert_eq!(s.execute(&["EVAL", "return 1", "0"]), Reply::Int(1));
                    worst_us.fetch_max(start.elapsed().as_micros() as u64, SeqCst);
                }
            }
        });
        let worst = Duration::from_micros(worst_us.load(SeqCst));
        eprintln!(
            "METRIC EVAL among 32 busy readers: worst latency {:.2} ms",
            ms(worst)
        );
        assert!(worst < Duration::from_secs(2), "EVAL starved {worst:?}");
    });
}

#[test]
fn stats_show_which_paths_embedded_sessions_take() {
    serial_guarded("stats", 60, || {
        let db = open(cfg(2, 1, 1));
        let db = &db;
        par(8, |t| {
            let s = session(db);
            for i in 0..500 {
                s.execute(&["MSET", "a", "1", "b", "2", "c", "3"]);
                s.execute(&["MGET", "a", "b", "c"]);
                s.execute(&["INCR", &format!("n{}", (t + i) % 3)]);
                if i % 50 == 0 {
                    s.execute(&["EVAL", "return 1", "0"]);
                }
            }
        });
        let stats = db.stats();
        eprintln!(
            "METRIC embedded stats after contention: {}",
            db_stats_line(db)
        );
        assert!(
            stats.exclusive >= 80,
            "exclusive path not exercised: {stats:?}"
        );
        // By design: sessions are application threads that may sleep on a
        // lock, so `execute_blocking` never defers, escalates or queues. The
        // deferral machinery is reachable only through `Engine::submit`
        // (event loops) and is covered by the `submit_path_*` tests below.
        assert_eq!(stats.deferred, 0);
        assert_eq!(stats.multi_deferrals, 0);
        assert_eq!(stats.multi_escalations, 0);
        assert_eq!(stats.tasks_run, 0);
    });
}

// ---------------------------------------------------------------------
// The event-loop path: `Engine::submit` + queues + escalation, driven by
// worker threads of this test and a minimal `Hooks`.
// ---------------------------------------------------------------------

struct Park {
    done: Mutex<HashMap<Ticket, Reply>>,
    escalate: Mutex<mpsc::Sender<Option<(Cmd, Ticket)>>>,
}

impl Hooks for Park {
    fn complete(&self, ticket: Ticket, reply: Reply) {
        self.done.lock().unwrap().insert(ticket, reply);
    }
    fn wake_any(&self) {}
    fn gate_opened(&self) {}
    fn escalate(&self, cmd: Cmd, ticket: Ticket) {
        self.escalate
            .lock()
            .unwrap()
            .send(Some((cmd, ticket)))
            .unwrap();
    }
}

struct Node {
    engine: Arc<Engine>,
    park: Arc<Park>,
    executor: Option<thread::JoinHandle<()>>,
}

impl Node {
    fn new(config: Config) -> Node {
        let engine = Arc::new(Engine::new(config));
        let (tx, rx) = mpsc::channel::<Option<(Cmd, Ticket)>>();
        let park = Arc::new(Park {
            done: Mutex::new(HashMap::new()),
            escalate: Mutex::new(tx),
        });
        engine.set_hooks(Some(park.clone()));
        let (e, p) = (engine.clone(), park.clone());
        let executor = thread::spawn(move || {
            while let Ok(Some((cmd, ticket))) = rx.recv() {
                let reply = {
                    let mut exclusive = e.exclusive();
                    exclusive.run(cmd)
                };
                p.complete(ticket, reply);
            }
        });
        Node {
            engine,
            park,
            executor: Some(executor),
        }
    }

    fn worker(&self, id: u64) -> Worker {
        Worker {
            engine: self.engine.clone(),
            park: self.park.clone(),
            slot: self.engine.gate().register().expect("gate slot"),
            id,
            seq: 0,
        }
    }

    fn shutdown(mut self) -> hcache::StatsSnapshot {
        self.park.escalate.lock().unwrap().send(None).unwrap();
        if let Some(h) = self.executor.take() {
            h.join().unwrap();
        }
        self.engine.set_hooks(None);
        self.engine.stats()
    }
}

/// An event-loop style caller: enters the shared side of the gate, submits,
/// leaves, and waits for parked commands outside the gate while servicing
/// the shard queues.
struct Worker {
    engine: Arc<Engine>,
    park: Arc<Park>,
    slot: usize,
    id: u64,
    seq: u64,
}

impl Worker {
    fn exec<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Reply {
        let argv: Vec<Vec<u8>> = args.iter().map(|a| a.as_ref().to_vec()).collect();
        let cmd = match Cmd::parse(argv) {
            Ok(cmd) => cmd,
            Err(reply) => return reply,
        };
        match cmd.class() {
            Class::Conn => self.engine.exec_conn(&cmd),
            Class::Exclusive => {
                let mut exclusive = self.engine.exclusive();
                exclusive.run(cmd)
            }
            Class::Shared => {
                self.seq += 1;
                let ticket = Ticket::from((self.id << 40) | self.seq);
                let gate = self.engine.gate();
                if !gate.try_enter(self.slot) {
                    gate.mark_starved();
                    while !gate.try_enter(self.slot) {
                        thread::yield_now();
                    }
                    gate.clear_starved();
                }
                // Every other command takes the way of a caller that wants
                // RESP: both ways must answer alike.
                let first = if self.seq.is_multiple_of(2) {
                    let mut out = Vec::new();
                    match self.engine.submit_into(cmd, ticket, &mut out) {
                        Submitted::Reply(reply) => Some(reply),
                        Submitted::Written => {
                            let (reply, used) = decode_reply(&out)
                                .expect("well-formed reply")
                                .expect("a whole reply");
                            assert_eq!(used, out.len());
                            Some(reply)
                        }
                        Submitted::Parked => None,
                    }
                } else {
                    self.engine.submit(cmd, ticket)
                };
                gate.leave(self.slot);
                if let Some(reply) = first {
                    return reply;
                }
                loop {
                    if let Some(reply) = self.park.done.lock().unwrap().remove(&ticket) {
                        return reply;
                    }
                    if self.engine.has_service() && gate.try_enter(self.slot) {
                        self.engine.service();
                        gate.leave(self.slot);
                    } else {
                        thread::yield_now();
                    }
                }
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.engine.gate().unregister(self.slot);
    }
}

fn submit_path_mset(shards: usize, drain: usize, esc: u32) -> hcache::StatsSnapshot {
    let config = cfg(shards, drain, esc);
    let keys = spread_keys(&config, 8, "sm");
    let node = Node::new(config);
    {
        let mut w = node.worker(99);
        let mut args = vec!["MSET".to_string()];
        for k in &keys {
            args.push(k.clone());
            args.push("init".into());
        }
        assert_eq!(w.exec(&args), Reply::Ok);
    }
    let workers = 8usize;
    let iters = 1200usize;
    let node_ref = &node;
    let keys = &keys;
    par(workers, |t| {
        let mut w = node_ref.worker(t as u64);
        let mut rng = Rng::new(t as u64 + 31);
        for i in 0..iters {
            if t % 2 == 0 {
                let args = mset_args(keys, &format!("w{t}-{i}"), (t / 2 + i) % 4, &mut rng);
                assert_eq!(w.exec(&args), Reply::Ok);
            } else {
                let reply = array(w.exec(&mget_args(keys, t + i)));
                assert!(
                    reply.iter().all(|r| *r == reply[0]) && matches!(reply[0], Reply::Bulk(_)),
                    "TORN MSET on the submit path: {}",
                    brief(&Reply::Array(reply))
                );
            }
            assert!(!w.exec(&["INCR", "counter"]).is_error());
        }
    });
    let mut w = node.worker(98);
    assert_eq!(
        w.exec(&["GET", "counter"]),
        bulk(&(workers * iters).to_string()),
        "lost INCR on the submit path (shards={shards} drain={drain} esc={esc})"
    );
    let last = array(w.exec(&mget_args(keys, 0)));
    assert!(last.iter().all(|r| *r == last[0]));
    drop(w);
    node.shutdown()
}

#[test]
fn submit_path_mset_atomic_with_deferral_and_escalation() {
    serial_guarded("submit_mset", 120, || {
        let mut total = hcache::StatsSnapshot::default();
        for (shards, drain, esc) in [
            (2, 1, 0),
            (2, 1, 1),
            (4, 2, 2),
            (8, 1, 3),
            (1, 1, 1),
            (4, 1, 1000),
        ] {
            let stats = submit_path_mset(shards, drain, esc);
            eprintln!("METRIC submit path shards={shards} drain={drain} esc={esc}: {stats:?}");
            total.deferred += stats.deferred;
            total.multi_deferrals += stats.multi_deferrals;
            total.multi_escalations += stats.multi_escalations;
            total.tasks_run += stats.tasks_run;
        }
        assert!(
            total.deferred > 0,
            "no single-key command was ever queued: {total:?}"
        );
        assert!(
            total.multi_deferrals > 0,
            "no multi-key command was ever deferred: {total:?}"
        );
        assert!(
            total.multi_escalations > 0,
            "no multi-key command ever escalated: {total:?}"
        );
        assert!(total.tasks_run > 0);
    });
}

#[test]
fn submit_path_scans_are_exact_under_writes() {
    serial_guarded("submit_scans", 120, || {
        for (shards, chunk, drain) in [(1, 1, 1), (4, 3, 2), (2, 256, 1)] {
            let node = Node::new(Config {
                shards,
                scan_chunk: chunk,
                drain_batch: drain,
                multi_escalate_after: 2,
                ..Config::default()
            });
            let stable_keys = 600usize;
            let fields = 3000usize;
            {
                let mut w = node.worker(97);
                for start in (0..fields).step_by(500) {
                    let mut args = vec!["HSET".to_string(), "big".into()];
                    for i in start..(start + 500).min(fields) {
                        args.push(format!("f{i}"));
                        args.push(format!("v{i}"));
                    }
                    w.exec(&args);
                }
                for i in 0..stable_keys {
                    w.exec(&["SET", &format!("s{i}"), "v"]);
                }
            }
            let stop = AtomicBool::new(false);
            let (stop, node_ref) = (&stop, &node);
            par(6, |t| {
                let mut w = node_ref.worker(t as u64);
                let mut rng = Rng::new(t as u64 + 7);
                match t {
                    0 | 1 => {
                        let mut n = 0u64;
                        while !stop.load(SeqCst) {
                            let k = format!("o{t}-{}", n % 300);
                            w.exec(&["SET", &k, "1"]);
                            w.exec(&["HSET", "big", &format!("x{t}-{}", n % 800), "1"]);
                            if n.is_multiple_of(3) {
                                w.exec(&["DEL", &k, "nothing"]);
                                w.exec(&["HDEL", "big", &format!("x{t}-{}", (n + 400) % 800)]);
                            }
                            if n.is_multiple_of(7) {
                                w.exec(&["MSET", &k, "2", "o-a", "3", "o-b", "4"]);
                            }
                            n += 1;
                        }
                    }
                    2 | 3 => {
                        while !stop.load(SeqCst) {
                            let i = rng.below(stable_keys as u64);
                            assert_eq!(w.exec(&["GET", &format!("s{i}")]), bulk("v"));
                        }
                    }
                    _ => {
                        let _stop = StopOnDrop(stop);
                        for round in 0..25 {
                            let reply = array(w.exec(&["HGETALL", "big"]));
                            let mut seen = vec![0u8; fields];
                            let mut extra = HashSet::new();
                            for pair in reply.chunks(2) {
                                let f = text(&pair[0]);
                                if let Some(i) = parse_index(&f, 'f') {
                                    assert!(i < fields);
                                    seen[i] += 1;
                                    assert_eq!(seen[i], 1, "round {round}: duplicate {f}");
                                } else {
                                    assert!(
                                        extra.insert(f.clone()),
                                        "round {round}: duplicate {f}"
                                    );
                                }
                            }
                            assert!(
                                seen.iter().all(|c| *c == 1),
                                "round {round}: HGETALL lost a stable field"
                            );
                            let reply = array(w.exec(&["KEYS", "*"]));
                            let mut seen = vec![0u8; stable_keys];
                            let mut others = HashSet::new();
                            for key in &reply {
                                let k = text(key);
                                if let Some(i) = parse_index(&k, 's') {
                                    seen[i] += 1;
                                    assert_eq!(seen[i], 1, "round {round}: duplicate {k}");
                                } else {
                                    assert!(
                                        others.insert(k.clone()),
                                        "round {round}: duplicate {k}"
                                    );
                                }
                            }
                            assert!(
                                seen.iter().all(|c| *c == 1),
                                "round {round}: KEYS lost a stable key"
                            );
                            assert!(others.contains("big"));
                        }
                    }
                }
            });
            let stats = node.shutdown();
            eprintln!("METRIC submit scans shards={shards} chunk={chunk}: {stats:?}");
        }
    });
}

/// A pending exclusive command closes the gate and waits for every command
/// already inside it. Long scans (KEYS, HGETALL) stay inside the shared side
/// for their whole duration, so one EVAL queued behind a scan holds back all
/// other commands for as long as the scan takes. This test measures that
/// effect; it asserts only a loose bound.
#[test]
fn eval_queued_behind_a_long_scan_stalls_other_commands() {
    serial_guarded("scan_plus_eval", 180, || {
        let db = open(cfg(4, 1, 1));
        {
            let s = session(&db);
            for start in (0..300_000usize).step_by(1000) {
                let mut args = vec!["MSET".to_string()];
                for i in start..start + 1000 {
                    args.push(format!("key{i}"));
                    args.push("v".into());
                }
                assert_eq!(s.execute(&args), Reply::Ok);
            }
            s.execute(&["SET", "probe", "v"]);
        }
        let stop = AtomicBool::new(false);
        let worst_us = AtomicU64::new(0);
        let worst_scan_us = AtomicU64::new(0);
        let evals = AtomicU64::new(0);
        let (stop, worst_us, worst_scan_us, evals, db) =
            (&stop, &worst_us, &worst_scan_us, &evals, &db);
        par(4, |t| {
            let s = session(db);
            match t {
                0 => {
                    let _stop = StopOnDrop(stop);
                    for _ in 0..6 {
                        let start = Instant::now();
                        assert_eq!(array(s.execute(&["KEYS", "*"])).len(), 300_001);
                        worst_scan_us.fetch_max(start.elapsed().as_micros() as u64, SeqCst);
                    }
                }
                1 => {
                    while !stop.load(SeqCst) {
                        assert_eq!(s.execute(&["EVAL", "return 1", "0"]), Reply::Int(1));
                        evals.fetch_add(1, SeqCst);
                        thread::sleep(Duration::from_millis(20));
                    }
                }
                _ => {
                    while !stop.load(SeqCst) {
                        let start = Instant::now();
                        assert_eq!(s.execute(&["GET", "probe"]), bulk("v"));
                        worst_us.fetch_max(start.elapsed().as_micros() as u64, SeqCst);
                        thread::sleep(Duration::from_micros(200));
                    }
                }
            }
        });
        let worst = Duration::from_micros(worst_us.load(SeqCst));
        eprintln!(
            "METRIC KEYS over 300k keys takes up to {:.0} ms; with an EVAL queued every 20 ms, worst single-key GET latency {:.0} ms ({} EVALs ran)",
            ms(Duration::from_micros(worst_scan_us.load(SeqCst))),
            ms(worst),
            evals.load(SeqCst)
        );
        assert!(
            worst < Duration::from_secs(5),
            "GET stalled {worst:?} behind scan + EVAL"
        );
    });
}
