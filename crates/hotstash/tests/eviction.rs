//! `maxmemory`: the process stays near the limit, the policy decides who
//! goes, and `noeviction` refuses instead of evicting.
//!
//! The tests share one process and one heap counter, so they run one at a
//! time behind a lock, each against a limit set relative to current use.

use hotstash::{Config, CountingAlloc, Db, Policy, Reply, Session};
use std::sync::Mutex;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

const VALUE: usize = 1000;
const HEADROOM: usize = 2 << 20;

fn open(policy: Policy, shards: usize) -> (Db, Session) {
    let db = Db::open(
        None,
        Config {
            shards,
            maxmemory_policy: policy,
            ..Config::default()
        },
    )
    .unwrap();
    let s = db.session().unwrap();
    (db, s)
}

fn limit(s: &Session, bytes: usize) {
    let reply = s.execute(&["CONFIG", "SET", "maxmemory", &bytes.to_string()]);
    assert_eq!(reply, Reply::Ok);
}

fn int(reply: Reply) -> i64 {
    match reply {
        Reply::Int(n) => n,
        other => panic!("not an integer: {other:?}"),
    }
}

fn exists(s: &Session, key: &str) -> bool {
    int(s.execute(&["EXISTS", key])) == 1
}

#[test]
fn noeviction_refuses_writes_that_grow_and_allows_the_rest() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (db, s) = open(Policy::NoEviction, 4);
    let value = "x".repeat(VALUE);
    limit(&s, hotstash::memory_used() + HEADROOM);
    let mut stored = 0;
    let mut refused = None;
    for i in 0..10_000 {
        match s.execute(&["SET", &format!("k{i}"), &value]) {
            Reply::Ok => stored += 1,
            other => {
                refused = Some(other);
                break;
            }
        }
    }
    assert_eq!(
        refused,
        Some(Reply::err(
            "OOM command not allowed when used memory > 'maxmemory'."
        ))
    );
    // About 2 MiB of 1 kB values fit, give or take bookkeeping.
    assert!((500..2100).contains(&stored), "stored {stored}");
    assert_eq!(int(s.execute(&["DBSIZE"])), stored);
    // Reads, deletes and expiry still work, and deleting makes room.
    assert_eq!(s.execute(&["GET", "k0"]), Reply::bulk(value.as_str()));
    assert!(s.execute(&["RPUSH", "list", "a"]).is_error());
    assert!(s
        .execute(&["EVAL", "return redis.call('set', 'e', 'v')", "0"])
        .is_error());
    assert_eq!(int(s.execute(&["EXPIRE", "k1", "100"])), 1);
    for i in 0..stored / 2 {
        assert_eq!(int(s.execute(&["DEL", &format!("k{i}")])), 1);
    }
    assert_eq!(s.execute(&["SET", "again", &value]), Reply::Ok);
    assert_eq!(db.stats().evicted, 0);
    limit(&s, 0);
}

#[test]
fn allkeys_lru_keeps_memory_bounded_and_spares_keys_in_use() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    // One shard: recency is tracked per shard, so this makes it exact.
    let (db, s) = open(Policy::AllKeysLru, 1);
    let value = "x".repeat(VALUE);
    for i in 0..50 {
        assert_eq!(s.execute(&["SET", &format!("hot{i}"), &value]), Reply::Ok);
    }
    let cap = hotstash::memory_used() + HEADROOM;
    limit(&s, cap);
    let mut peak = 0;
    for i in 0..20_000 {
        assert_eq!(s.execute(&["SET", &format!("cold{i}"), &value]), Reply::Ok);
        for h in 0..50 {
            if (i + h) % 10 == 0 {
                s.execute(&["GET", &format!("hot{h}")]);
            }
        }
        peak = peak.max(hotstash::memory_used());
    }
    // 20 MB were written into 2 MiB of room.
    assert!(peak < cap + (256 << 10), "peak {peak} cap {cap}");
    assert!(db.stats().evicted > 15_000, "{:?}", db.stats());
    let hot_left = (0..50).filter(|h| exists(&s, &format!("hot{h}"))).count();
    assert!(hot_left >= 45, "only {hot_left} of 50 hot keys survived");
    assert!(exists(&s, "cold19999"));
    assert!(!exists(&s, "cold0"));
    limit(&s, 0);
}

#[test]
fn allkeys_lfu_spares_keys_used_often_even_when_not_lately() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (db, s) = open(Policy::AllKeysLfu, 1);
    let value = "x".repeat(VALUE);
    for i in 0..50 {
        assert_eq!(s.execute(&["SET", &format!("hot{i}"), &value]), Reply::Ok);
    }
    // Used a great deal, and then not at all while memory fills: by
    // recency these would be the first to go.
    for _ in 0..400 {
        for h in 0..50 {
            s.execute(&["GET", &format!("hot{h}")]);
        }
    }
    let hot = int(s.execute(&["OBJECT", "FREQ", "hot0"]));
    assert_eq!(s.execute(&["SET", "fresh", &value]), Reply::Ok);
    assert!(hot > int(s.execute(&["OBJECT", "FREQ", "fresh"])), "{hot}");
    let cap = hotstash::memory_used() + HEADROOM;
    limit(&s, cap);
    let mut peak = 0;
    for i in 0..20_000 {
        assert_eq!(s.execute(&["SET", &format!("cold{i}"), &value]), Reply::Ok);
        peak = peak.max(hotstash::memory_used());
    }
    assert!(peak < cap + (256 << 10), "peak {peak} cap {cap}");
    assert!(db.stats().evicted > 15_000, "{:?}", db.stats());
    let hot_left = (0..50).filter(|h| exists(&s, &format!("hot{h}"))).count();
    assert!(hot_left >= 45, "only {hot_left} of 50 hot keys survived");
    assert!(!exists(&s, "cold0"));
    limit(&s, 0);
}

#[test]
fn volatile_lfu_only_touches_keys_with_a_deadline() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (db, s) = open(Policy::VolatileLfu, 4);
    let value = "x".repeat(VALUE);
    for i in 0..200 {
        assert_eq!(s.execute(&["SET", &format!("keep{i}"), &value]), Reply::Ok);
    }
    limit(&s, hotstash::memory_used() + HEADROOM);
    let mut refused = 0;
    for i in 0..10_000 {
        match s.execute(&["SET", &format!("tmp{i}"), &value, "EX", "1000"]) {
            Reply::Ok => {}
            Reply::Error(e) if e.starts_with("OOM") => refused += 1,
            other => panic!("{other:?}"),
        }
    }
    assert!(refused < 100, "{refused} refused");
    assert!(db.stats().evicted > 5_000, "{:?}", db.stats());
    for i in 0..200 {
        assert!(exists(&s, &format!("keep{i}")), "lost keep{i}");
    }
    limit(&s, 0);
}

#[test]
fn volatile_policies_only_touch_keys_with_a_deadline() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    for policy in [
        Policy::VolatileLru,
        Policy::VolatileRandom,
        Policy::VolatileTtl,
    ] {
        let (db, s) = open(policy, 8);
        let value = "x".repeat(VALUE);
        for i in 0..200 {
            assert_eq!(s.execute(&["SET", &format!("keep{i}"), &value]), Reply::Ok);
        }
        limit(&s, hotstash::memory_used() + HEADROOM);
        let mut refused = 0;
        for i in 0..10_000 {
            let ttl = (1000 + i).to_string();
            match s.execute(&["SET", &format!("tmp{i}"), &value, "EX", &ttl]) {
                Reply::Ok => {}
                Reply::Error(e) if e.starts_with("OOM") => refused += 1,
                other => panic!("{other:?}"),
            }
        }
        // A shard may briefly hold no volatile key; refusals stay rare.
        assert!(refused < 100, "{policy:?}: {refused} refused");
        assert!(db.stats().evicted > 5_000, "{policy:?}: {:?}", db.stats());
        for i in 0..200 {
            assert!(exists(&s, &format!("keep{i}")), "{policy:?} lost keep{i}");
        }
        if policy == Policy::VolatileTtl {
            // The ones closest to their deadline went first.
            let early = (0..1000).filter(|i| exists(&s, &format!("tmp{i}"))).count();
            let late = (9000..10_000)
                .filter(|i| exists(&s, &format!("tmp{i}")))
                .count();
            assert!(early < late, "early {early} late {late}");
        }
        limit(&s, 0);
    }
}

#[test]
fn eviction_under_concurrent_writers_over_tcp_like_load() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (db, s) = open(Policy::AllKeysRandom, 16);
    let cap = hotstash::memory_used() + 2 * HEADROOM;
    limit(&s, cap);
    let threads: Vec<_> = (0..6)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                let s = db.session().unwrap();
                let value = "y".repeat(VALUE);
                let mut worst = 0;
                for i in 0..20_000 {
                    let key = format!("t{t}:{i}");
                    let reply = match i % 4 {
                        0 => s.execute(&["SET", &key, &value]),
                        1 => s.execute(&["RPUSH", &format!("l{t}:{}", i % 50), &value]),
                        2 => s.execute(&["HSET", &format!("h{t}:{}", i % 50), &key, &value]),
                        _ => s.execute(&["MSET", &key, &value, &format!("{key}b"), &value]),
                    };
                    assert!(!reply.is_error(), "{reply:?}");
                    worst = worst.max(hotstash::memory_used());
                }
                worst
            })
        })
        .collect();
    let peak = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .max()
        .unwrap();
    // Lists and hashes are evicted whole, so the slack is a few of those.
    assert!(peak < cap + (4 << 20), "peak {peak} cap {cap}");
    assert!(db.stats().evicted > 0);
    limit(&s, 0);
}

fn big_hash(s: &Session, key: &str, fields: usize) {
    let mut args = vec!["HSET".to_string(), key.to_string()];
    for f in 0..fields {
        args.push(format!("f{f}"));
        args.push("v".into());
    }
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    assert_eq!(int(s.execute(&args)), fields as i64);
}

#[test]
fn a_value_freed_lazily_ends_the_eviction_round() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (_db, s) = open(Policy::AllKeysRandom, 1);
    const KEYS: i64 = 20;
    for k in 0..KEYS {
        big_hash(&s, &format!("h{k}"), 5000);
    }
    // Over by a byte: the next write needs one eviction.
    limit(&s, hotstash::memory_used() - 1);
    assert_eq!(s.execute(&["SET", "new", "v"]), Reply::Ok);
    // One hash went to the freeing thread, not one per round until the
    // thread caught up.
    assert_eq!(int(s.execute(&["DBSIZE"])), KEYS);
    limit(&s, 0);
}

#[test]
fn a_write_waits_briefly_for_memory_being_freed() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (_db, s) = open(Policy::NoEviction, 1);
    big_hash(&s, "h", 2000);
    limit(&s, hotstash::memory_used() - 1);
    assert!(s.execute(&["SET", "k", "v"]).is_error());
    assert_eq!(int(s.execute(&["UNLINK", "h"])), 1);
    assert_eq!(s.execute(&["SET", "k", "v"]), Reply::Ok);
    limit(&s, 0);
}

#[test]
fn config_get_and_set_validate_their_input() {
    let _guard = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (_db, s) = open(Policy::NoEviction, 2);
    let pair = |k: &str, v: &str| Reply::Array(vec![Reply::bulk(k), Reply::bulk(v)]);
    assert_eq!(
        s.execute(&["CONFIG", "GET", "maxmemory"]),
        pair("maxmemory", "0")
    );
    assert_eq!(
        s.execute(&["CONFIG", "SET", "maxmemory", "64mb"]),
        Reply::Ok
    );
    assert_eq!(
        s.execute(&["CONFIG", "GET", "MAXMEMORY"]),
        pair("maxmemory", "67108864")
    );
    assert!(s
        .execute(&["CONFIG", "SET", "maxmemory", "lots"])
        .is_error());
    assert!(s.execute(&["CONFIG", "SET", "maxmemory", "-1"]).is_error());
    assert!(s
        .execute(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lfru"])
        .is_error());
    // A refused setting leaves the others of the same command alone.
    assert!(s
        .execute(&[
            "CONFIG",
            "SET",
            "maxmemory",
            "1",
            "maxmemory-policy",
            "bogus"
        ])
        .is_error());
    assert_eq!(
        s.execute(&["CONFIG", "GET", "maxmemory"]),
        pair("maxmemory", "67108864")
    );
    assert_eq!(
        s.execute(&["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"]),
        Reply::Ok
    );
    match s.execute(&["CONFIG", "GET", "maxmemory*"]) {
        Reply::Array(items) => assert_eq!(items.len(), 4),
        other => panic!("{other:?}"),
    }
    assert!(s.execute(&["CONFIG", "GET"]).is_error());
    assert!(s.execute(&["CONFIG", "SET", "maxmemory"]).is_error());
    limit(&s, 0);
}
