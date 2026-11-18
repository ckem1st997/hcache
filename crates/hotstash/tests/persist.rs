//! The data file: what comes back after a restart, after a crash at any
//! byte, after damage, across rewrites, and while writers keep going.

use hotstash::{Config, Db, PersistOptions, Reply, Session};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A fresh path under the test's own temporary directory.
fn fresh(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("persist");
    fs::create_dir_all(&dir).unwrap();
    let n = NEXT.fetch_add(1, Ordering::SeqCst);
    let path = dir.join(format!("{name}-{}-{n}.hc", std::process::id()));
    for p in [
        path.clone(),
        sibling(&path, ".lock"),
        sibling(&path, ".rewrite"),
    ] {
        match fs::remove_file(&p) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("cannot clear {}: {e}", p.display()),
        }
    }
    path
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn fast() -> PersistOptions {
    PersistOptions {
        interval: Duration::from_millis(5),
        ..PersistOptions::default()
    }
}

fn open_with(path: &Path, options: PersistOptions) -> (Db, Session) {
    let db = Db::open_with(
        Some(path),
        Config {
            shards: 8,
            ..Config::default()
        },
        options,
    )
    .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let s = db.session().unwrap();
    (db, s)
}

fn open(path: &Path) -> (Db, Session) {
    open_with(path, fast())
}

fn try_open(path: &Path) -> Result<Db, hotstash::Error> {
    Db::open_with(Some(path), Config::default(), fast())
}

fn int(reply: Reply) -> i64 {
    match reply {
        Reply::Int(n) => n,
        other => panic!("not an integer: {other:?}"),
    }
}

fn items(reply: Reply) -> Vec<Reply> {
    match reply {
        Reply::Array(items) => items,
        other => panic!("not an array: {other:?}"),
    }
}

fn text(reply: &Reply) -> String {
    match reply {
        Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        Reply::Simple(s) => s.clone(),
        other => panic!("not text: {other:?}"),
    }
}

/// Everything in the database, in a form two databases can be compared by.
fn snapshot(s: &Session) -> Vec<(String, String, bool)> {
    let mut out = Vec::new();
    for key in items(s.execute(&["KEYS", "*"])) {
        let key = text(&key);
        let kind = text(&s.execute(&["TYPE", &key]));
        let mut content = match kind.as_str() {
            "string" => vec![s.execute(&["GET", &key])],
            "hash" => {
                let flat = items(s.execute(&["HGETALL", &key]));
                let mut pairs: Vec<Reply> =
                    flat.chunks(2).map(|p| Reply::Array(p.to_vec())).collect();
                pairs.sort_by_key(|p| format!("{p:?}"));
                pairs
            }
            "list" => items(s.execute(&["LRANGE", &key, "0", "-1"])),
            "set" => {
                let mut members = items(s.execute(&["SMEMBERS", &key]));
                members.sort_by_key(|m| format!("{m:?}"));
                members
            }
            "zset" => items(s.execute(&["ZRANGE", &key, "0", "-1", "WITHSCORES"])),
            // Expired since KEYS listed it.
            "none" => continue,
            other => panic!("{key}: unexpected type {other}"),
        };
        let has_ttl = int(s.execute(&["PTTL", &key])) > 0;
        out.push((
            key,
            format!("{kind}:{:?}", std::mem::take(&mut content)),
            has_ttl,
        ));
    }
    out.sort();
    out
}

fn copy_of(path: &Path, name: &str) -> PathBuf {
    let to = fresh(name);
    // A rewrite may be swapping the file in at this very moment.
    for _ in 0..200 {
        if fs::copy(path, &to).is_ok() {
            return to;
        }
        thread::sleep(Duration::from_millis(2));
    }
    panic!("cannot copy {}", path.display());
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One random write over a small key space, any type, sometimes the wrong
/// one for the key (which must then change nothing).
fn random_write(s: &Session, rng: &mut Rng) {
    let key = format!("k{}", rng.below(40));
    let a = format!("m{}", rng.below(12));
    let b = format!("v{}", rng.next());
    let n = rng.below(7).to_string();
    let score = format!("{}.5", rng.below(1000) as i64 - 500);
    let big = "x".repeat(rng.below(3000) as usize);
    let _ = match rng.below(26) {
        0 => s.execute(&["SET", &key, &b]),
        1 => s.execute(&["SET", &key, &big, "PX", "600000"]),
        2 => s.execute(&["APPEND", &key, &b]),
        3 => s.execute(&["INCR", &key]),
        4 => s.execute(&["DEL", &key]),
        5 => s.execute(&["HSET", &key, &a, &b]),
        6 => s.execute(&["HDEL", &key, &a]),
        7 => s.execute(&["HINCRBY", &key, &a, "3"]),
        8 => s.execute(&["RPUSH", &key, &a, &b]),
        9 => s.execute(&["LPUSH", &key, &b]),
        10 => s.execute(&["LPOP", &key]),
        11 => s.execute(&["LSET", &key, "0", &b]),
        12 => s.execute(&["LTRIM", &key, "0", &n]),
        13 => s.execute(&["SADD", &key, &a, &b]),
        14 => s.execute(&["SREM", &key, &a]),
        15 => s.execute(&["SPOP", &key]),
        16 => s.execute(&["ZADD", &key, &score, &a]),
        17 => s.execute(&["ZINCRBY", &key, "1.25", &a]),
        18 => s.execute(&["ZREM", &key, &a]),
        19 => s.execute(&["ZPOPMIN", &key]),
        20 => s.execute(&["PEXPIRE", &key, "900000"]),
        21 => s.execute(&["PERSIST", &key]),
        22 => s.execute(&["RENAME", &key, &format!("k{}", rng.below(40))]),
        23 => s.execute(&["COPY", &key, &format!("k{}", rng.below(40)), "REPLACE"]),
        24 => s.execute(&["MSET", &key, &b, &format!("k{}", rng.below(40)), &a]),
        _ => s.execute(&[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('DEL', KEYS[2])",
            "2",
            &key,
            &format!("k{}", rng.below(40)),
            &b,
        ]),
    };
}

#[test]
fn every_type_and_ttl_comes_back_after_a_restart() {
    let path = fresh("roundtrip");
    let before = {
        let (db, s) = open(&path);
        assert_eq!(db.load_report(), Some(Default::default()));
        s.execute(&["SET", "str", "value"]);
        s.execute(&[b"SET".as_slice(), b"bin", b"\x00\xff\r\n"]);
        s.execute(&["SET", "", "empty key"]);
        s.execute(&["SET", "empty value", ""]);
        s.execute(&["SET", "ttl", "v", "PX", "600000"]);
        s.execute(&["SET", "soon", "v", "PX", "40"]);
        s.execute(&["HSET", "hash", "f1", "v1", "f2", "", "", "v3"]);
        s.execute(&["RPUSH", "list", "a", "b", "a", ""]);
        s.execute(&["SADD", "set", "x", "y", ""]);
        s.execute(&[
            "ZADD", "zset", "-inf", "low", "1.5", "mid", "1.5", "mid2", "inf", "high",
        ]);
        s.execute(&[
            "ZADD", "zset", "0.1", "tenth", "-0", "zero", "1e300", "huge",
        ]);
        s.execute(&["SET", "gone", "v"]);
        s.execute(&["DEL", "gone"]);
        s.execute(&["RPUSH", "emptied", "a"]);
        s.execute(&["LPOP", "emptied"]);
        s.execute(&["SET", "retyped", "v"]);
        s.execute(&["DEL", "retyped"]);
        s.execute(&["SADD", "retyped", "now a set"]);
        s.execute(&["SET", "unttl", "v", "PX", "600000"]);
        s.execute(&["PERSIST", "unttl"]);
        let big = "z".repeat(3 << 20);
        s.execute(&["SET", "big", &big]);
        for i in 0..5000 {
            s.execute(&["HSET", "wide", &format!("f{i}"), &i.to_string()]);
        }
        let mut snap = snapshot(&s);
        snap.retain(|(key, _, _)| key != "soon");
        snap
    };
    thread::sleep(Duration::from_millis(60));
    let (db, s) = open(&path);
    assert_eq!(snapshot(&s), before);
    assert_eq!(
        s.execute(&["EXISTS", "soon", "gone", "emptied"]),
        Reply::Int(0)
    );
    assert_eq!(s.execute(&["ZSCORE", "zset", "tenth"]), Reply::bulk("0.1"));
    let ttl = int(s.execute(&["PTTL", "ttl"]));
    assert!((500_000..=600_000).contains(&ttl), "ttl {ttl}");
    assert_eq!(s.execute(&["PTTL", "unttl"]), Reply::Int(-1));
    let report = db.load_report().unwrap();
    assert!(report.frames > 0 && report.dropped_bytes == 0, "{report:?}");
    // Loading marks nothing as changed: a second restart finds the same.
    drop((db, s));
    let (_db, s) = open(&path);
    assert_eq!(snapshot(&s), before);
}

#[test]
fn flushall_is_remembered_in_order() {
    let path = fresh("flush");
    {
        let (db, s) = open(&path);
        for i in 0..500 {
            s.execute(&["SET", &format!("old{i}"), "v"]);
        }
        db.sync().unwrap();
        s.execute(&["SET", "same-round", "v"]);
        assert_eq!(s.execute(&["FLUSHALL"]), Reply::Ok);
        s.execute(&["SET", "after", "1"]);
        db.sync().unwrap();
        s.execute(&["FLUSHALL"]);
        s.execute(&["FLUSHALL"]);
        s.execute(&["RPUSH", "last", "x"]);
    }
    let (_db, s) = open(&path);
    assert_eq!(
        s.execute(&["KEYS", "*"]),
        Reply::Array(vec![Reply::bulk("last")])
    );
    s.execute(&["FLUSHALL"]);
    drop((_db, s));
    let (_db, s) = open(&path);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
}

#[test]
fn random_history_survives_many_restarts() {
    let path = fresh("history");
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut before = Vec::new();
    for round in 0..25 {
        let (db, s) = open(&path);
        assert_eq!(
            snapshot(&s),
            before,
            "round {round}: reopened state differs"
        );
        for _ in 0..400 {
            random_write(&s, &mut rng);
            if rng.below(97) == 0 {
                db.sync().unwrap();
            }
            if rng.below(301) == 0 {
                s.execute(&["BGSAVE"]);
            }
        }
        if round % 9 == 8 {
            s.execute(&["FLUSHALL"]);
            random_write(&s, &mut rng);
        }
        before = snapshot(&s);
    }
    let (_db, s) = open(&path);
    assert_eq!(snapshot(&s), before);
}

#[test]
fn a_crash_at_any_byte_keeps_everything_before_the_torn_frame() {
    let path = fresh("torn");
    let (db, s) = open_with(
        &path,
        PersistOptions {
            // Only explicit syncs write, so the frames are known.
            interval: Duration::from_secs(3600),
            ..PersistOptions::default()
        },
    );
    let mut sizes = vec![fs::metadata(&path).unwrap().len()];
    for frame in 0..6 {
        for i in 0..20 {
            s.execute(&["RPUSH", &format!("list{i}"), &format!("frame{frame}")]);
        }
        s.execute(&["SET", "frames", &(frame + 1).to_string()]);
        db.sync().unwrap();
        sizes.push(fs::metadata(&path).unwrap().len());
    }
    let whole = fs::read(&path).unwrap();
    assert_eq!(whole.len() as u64, sizes[6]);
    // Cut the file at every length from the header to the full size.
    let step = (whole.len() / 400).max(1);
    let mut cuts: Vec<usize> = (sizes[0] as usize..whole.len()).step_by(step).collect();
    for &edge in &sizes {
        for d in [-1i64, 0, 1, 15, 16, 17] {
            let at = edge as i64 + d;
            if at >= sizes[0] as i64 && at <= whole.len() as i64 {
                cuts.push(at as usize);
            }
        }
    }
    for cut in cuts {
        let copy = fresh("torn-copy");
        fs::write(&copy, &whole[..cut]).unwrap();
        let complete = sizes.iter().filter(|&&edge| edge <= cut as u64).count() - 1;
        let (cdb, cs) = open(&copy);
        let report = cdb.load_report().unwrap();
        assert_eq!(report.frames as usize, complete, "cut at {cut}");
        assert_eq!(
            report.dropped_bytes,
            cut as u64 - sizes[complete],
            "cut at {cut}"
        );
        if complete == 0 {
            assert_eq!(cs.execute(&["DBSIZE"]), Reply::Int(0));
        } else {
            assert_eq!(
                cs.execute(&["GET", "frames"]),
                Reply::bulk(complete.to_string())
            );
            assert_eq!(cs.execute(&["LLEN", "list7"]), Reply::Int(complete as i64));
        }
        // The tail is gone from the file and new writes follow the good part.
        cs.execute(&["SET", "after-crash", "v"]);
        drop((cdb, cs));
        assert!(fs::metadata(&copy).unwrap().len() > sizes[complete]);
        let (cdb, cs) = open(&copy);
        assert_eq!(cdb.load_report().unwrap().dropped_bytes, 0, "cut at {cut}");
        assert_eq!(cs.execute(&["GET", "after-crash"]), Reply::bulk("v"));
        drop((cdb, cs));
        for p in [copy.clone(), sibling(&copy, ".lock")] {
            fs::remove_file(p).unwrap();
        }
    }
}

#[test]
fn damage_is_refused_not_guessed_at() {
    let path = fresh("damage");
    {
        let (db, s) = open(&path);
        for frame in 0..4 {
            s.execute(&["SET", &format!("k{frame}"), &"v".repeat(200)]);
            db.sync().unwrap();
        }
    }
    let whole = fs::read(&path).unwrap();

    // A flipped bit in the last frame is a torn write: dropped.
    let last = fresh("damage-last");
    let mut bytes = whole.clone();
    let n = bytes.len();
    bytes[n - 10] ^= 0x01;
    fs::write(&last, &bytes).unwrap();
    let (db, s) = open(&last);
    assert!(db.load_report().unwrap().dropped_bytes > 0);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(3));
    drop((db, s));

    // The same in an earlier frame is damage: opening fails and the file
    // is left exactly as it was.
    for at in [8 + 16 + 5, whole.len() / 2] {
        let middle = fresh("damage-middle");
        let mut bytes = whole.clone();
        bytes[at] ^= 0x80;
        fs::write(&middle, &bytes).unwrap();
        let error = try_open(&middle).err().expect("opened a damaged file");
        assert!(error.to_string().contains("corrupt frame"), "{error}");
        assert_eq!(fs::read(&middle).unwrap(), bytes);
    }

    // A damaged length is damage too, not a reason to cut the file there.
    for bit in [0x01u8, 0x7f] {
        let long = fresh("damage-length");
        let mut bytes = whole.clone();
        bytes[8 + 6] ^= bit;
        fs::write(&long, &bytes).unwrap();
        let error = try_open(&long)
            .err()
            .expect("opened a file with a damaged length");
        assert!(
            error.to_string().contains("corrupt frame header"),
            "{error}"
        );
        assert_eq!(fs::read(&long).unwrap(), bytes);
    }

    // Not our file at all.
    for content in [
        &b"REDIS0011\xfa\tredis-ver"[..],
        b"HCACHE1\n",
        b"HCACHE9\nxxxx",
        b"x",
    ] {
        let foreign = fresh("damage-foreign");
        fs::write(&foreign, content).unwrap();
        assert!(try_open(&foreign).is_err(), "opened {content:?}");
        assert_eq!(fs::read(&foreign).unwrap(), content);
    }

    // A frame with a good checksum over nonsense is refused, not skipped.
    let nonsense = fresh("damage-record");
    let payload = [2u8, 0xff, 0xff, 0xff, 0xff, 1, 2, 3];
    let mut bytes = b"HCACHE2\n".to_vec();
    let len = (payload.len() as u64).to_le_bytes();
    bytes.extend_from_slice(&len);
    bytes.extend_from_slice(&xxhash_rust::xxh3::xxh3_64(&payload).to_le_bytes());
    bytes.extend_from_slice(&xxhash_rust::xxh3::xxh3_64(&len).to_le_bytes());
    bytes.extend_from_slice(&payload);
    fs::write(&nonsense, &bytes).unwrap();
    let error = try_open(&nonsense).err().expect("opened nonsense");
    assert!(error.to_string().contains("unreadable record"), "{error}");
}

#[test]
fn one_file_one_database() {
    let path = fresh("lock");
    let (db, s) = open(&path);
    s.execute(&["SET", "k", "v"]);
    let error = try_open(&path).err().expect("opened the same file twice");
    assert!(error.to_string().contains("in use"), "{error}");
    // A session keeps the database, and so the file, open.
    drop(db);
    assert!(try_open(&path).is_err());
    assert_eq!(s.execute(&["SET", "k2", "v"]), Reply::Ok);
    drop(s);
    let (_db, s) = open(&path);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(2));
}

#[test]
fn rewrites_keep_the_file_small_and_the_data_whole() {
    let path = fresh("rewrite");
    let options = PersistOptions {
        interval: Duration::from_millis(2),
        rewrite_factor: 2,
        rewrite_min_bytes: 64 << 10,
        ..PersistOptions::default()
    };
    let (db, s) = open_with(&path, options.clone());
    let value = "v".repeat(1000);
    // 20 MB of writes over 200 keys: 200 KB of live data.
    for i in 0..20_000 {
        s.execute(&["SET", &format!("k{}", i % 200), &format!("{i}:{value}")]);
        if i % 500 == 0 {
            db.sync().unwrap();
        }
    }
    db.sync().unwrap();
    thread::sleep(Duration::from_millis(50));
    let size = fs::metadata(&path).unwrap().len();
    assert!(size < 2 << 20, "the file was never rewritten: {size} bytes");
    assert!(!sibling(&path, ".rewrite").exists());
    let before = snapshot(&s);
    assert_eq!(before.len(), 200);

    // BGSAVE compacts on request, also with nothing new to write.
    s.execute(&["DEL", "k0", "k1", "k2"]);
    let last = int(s.execute(&["LASTSAVE"]));
    assert!(last > 1_600_000_000);
    assert_eq!(
        s.execute(&["BGSAVE"]),
        Reply::Simple("Background saving started".into())
    );
    let mut small = 0;
    for _ in 0..500 {
        small = fs::metadata(&path).map(|m| m.len()).unwrap_or(u64::MAX);
        if small < 230_000 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(small < 230_000, "BGSAVE did not compact: {small} bytes");
    let before = snapshot(&s);
    drop((db, s));
    let (db, s) = open_with(&path, options);
    assert_eq!(snapshot(&s), before);
    assert_eq!(before.len(), 197);
    let info = text(&s.execute(&["INFO"]));
    assert!(
        info.contains("persistence:1") && info.contains("last_write_error:\r\n"),
        "{info}"
    );
    drop((db, s));

    let memory = Db::open(None, Config::default()).unwrap();
    let s = memory.session().unwrap();
    assert!(s.execute(&["BGSAVE"]).is_error());
    assert!(s.execute(&["SAVE"]).is_error());
    assert!(memory.sync().is_ok() && memory.load_report().is_none());
}

/// Writers never stop while the file is synced, rewritten and copied. Each
/// copy is what a crash at that moment would leave behind: it must open,
/// hold no half transaction, and never go back in time.
#[test]
fn copies_taken_under_load_are_consistent_databases() {
    const WRITERS: usize = 4;
    let path = fresh("load");
    let (db, s) = open_with(
        &path,
        PersistOptions {
            interval: Duration::from_millis(1),
            rewrite_factor: 2,
            rewrite_min_bytes: 32 << 10,
            ..PersistOptions::default()
        },
    );
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let (db, stop) = (db.clone(), stop.clone());
            thread::spawn(move || {
                let s = db.session().unwrap();
                let pad = "p".repeat(300);
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    n += 1;
                    // Two counters on different shards, moved together.
                    match n % 3 {
                        0 => {
                            assert_eq!(s.execute(&["MULTI"]), Reply::Ok);
                            s.execute(&["INCR", &format!("a{w}")]);
                            s.execute(&["INCR", &format!("b{w}")]);
                            s.execute(&["SET", &format!("pad{w}:{}", n % 50), &pad]);
                            assert_eq!(items(s.execute(&["EXEC"])).len(), 3);
                        }
                        1 => {
                            let script = "redis.call('INCR', KEYS[1]); return redis.call('INCR', KEYS[2])";
                            let (a, b) = (format!("a{w}"), format!("b{w}"));
                            int(s.execute(&["EVAL", script, "2", &a, &b]));
                        }
                        _ => {
                            let (a, b) = (format!("a{w}"), format!("b{w}"));
                            let script = "redis.call('INCR', KEYS[1]); redis.call('RPUSH', KEYS[3], ARGV[1]); \
                                          redis.call('LTRIM', KEYS[3], -20, -1); return redis.call('INCR', KEYS[2])";
                            int(s.execute(&["EVAL", script, "3", &a, &b, &format!("log{w}"), &n.to_string()]));
                        }
                    }
                }
                n
            })
        })
        .collect();

    let mut floor = [0i64; WRITERS];
    let mut checked = 0;
    let mut loaded = 0;
    for round in 0..60 {
        thread::sleep(Duration::from_millis(15));
        if round % 10 == 9 {
            s.execute(&["BGSAVE"]);
        }
        let copy = copy_of(&path, "load-copy");
        let (cdb, cs) = open(&copy);
        loaded += cdb.load_report().unwrap().frames;
        for (w, floor) in floor.iter_mut().enumerate() {
            let a = s_int(&cs, &format!("a{w}"));
            let b = s_int(&cs, &format!("b{w}"));
            assert_eq!(
                a, b,
                "round {round}: writer {w} has half a transaction on disk"
            );
            assert!(
                a >= *floor,
                "round {round}: writer {w} went back from {floor} to {a}"
            );
            *floor = a;
            checked += 1;
        }
        drop((cdb, cs));
        for p in [copy.clone(), sibling(&copy, ".lock")] {
            fs::remove_file(p).unwrap();
        }
    }
    stop.store(true, Ordering::Relaxed);
    let totals: Vec<u64> = writers
        .into_iter()
        .map(|t| t.join().expect("writer"))
        .collect();
    assert!(checked > 0 && loaded > 0);
    assert!(
        floor.iter().all(|&n| n > 0),
        "the copies never saw progress: {floor:?}"
    );
    let before = snapshot(&s);
    drop((db, s));
    let (_db, s) = open(&path);
    assert_eq!(snapshot(&s), before);
    for (w, total) in totals.iter().enumerate() {
        assert_eq!(s_int(&s, &format!("a{w}")), *total as i64, "writer {w}");
    }
}

fn s_int(s: &Session, key: &str) -> i64 {
    match s.execute(&["GET", key]) {
        Reply::Nil => 0,
        reply => text(&reply).parse().unwrap(),
    }
}

/// Without an explicit sync, what was written reaches the disk within a
/// few intervals, and a crash loses nothing older.
#[test]
fn changes_reach_the_disk_on_their_own() {
    let path = fresh("interval");
    let (_db, s) = open_with(
        &path,
        PersistOptions {
            interval: Duration::from_millis(20),
            ..PersistOptions::default()
        },
    );
    for i in 0..300 {
        s.execute(&["SADD", "set", &i.to_string()]);
    }
    thread::sleep(Duration::from_millis(400));
    s.execute(&["SET", "too late", "maybe"]);
    let copy = copy_of(&path, "interval-copy");
    let (_cdb, cs) = open(&copy);
    assert_eq!(cs.execute(&["SCARD", "set"]), Reply::Int(300));
}

/// After a power cut a file system may show the end of the file as zeros,
/// or as a frame whose first sectors made it and whose rest is zeros.
#[test]
fn a_zero_filled_tail_is_a_crash_not_damage() {
    let path = fresh("zeros");
    let mut sizes = Vec::new();
    {
        let (db, s) = open(&path);
        for frame in 0..3 {
            s.execute(&["SET", &format!("k{frame}"), &"v".repeat(5000)]);
            db.sync().unwrap();
            sizes.push(fs::metadata(&path).unwrap().len() as usize);
        }
    }
    let whole = fs::read(&path).unwrap();
    let mut cases: Vec<(Vec<u8>, i64)> = Vec::new();
    for zeros in [1usize, 16, 23, 24, 25, 4096, 100_000] {
        let mut bytes = whole.clone();
        bytes.resize(whole.len() + zeros, 0);
        cases.push((bytes, 3));
    }
    // The last frame partly written: header only, half the header, half
    // the payload; each with and without zeros to its full length.
    for kept in [8usize, 24, 24 + 2500] {
        for pad in [false, true] {
            let mut bytes = whole[..sizes[1] + kept].to_vec();
            if pad {
                bytes.resize(whole.len(), 0);
            }
            cases.push((bytes, 2));
        }
    }
    // A database that never got past its first bytes.
    for start in [&b""[..], b"HCA", b"\0\0\0\0\0"] {
        cases.push((start.to_vec(), 0));
    }
    for (n, (bytes, keys)) in cases.into_iter().enumerate() {
        let copy = fresh("zeros-copy");
        fs::write(&copy, &bytes).unwrap();
        let (db, s) = open(&copy);
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(keys), "case {n}");
        s.execute(&["SET", "after", "v"]);
        drop((db, s));
        let (db, s) = open(&copy);
        assert_eq!(db.load_report().unwrap().dropped_bytes, 0, "case {n}");
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(keys + 1), "case {n}");
    }
    // What was cut off is kept beside the file when it was not just zeros.
    let copy = fresh("zeros-kept");
    fs::write(&copy, &whole[..sizes[1] + 3000]).unwrap();
    drop(open(&copy));
    assert_eq!(
        fs::read(sibling(&copy, ".torn")).unwrap(),
        &whole[sizes[1]..sizes[1] + 3000]
    );
}

/// A server started from the database keeps writing to disk after the
/// last `Db` and `Session` are gone, and syncing all the time does not
/// keep the file from being rewritten.
#[test]
fn the_listener_keeps_the_file_alive_and_syncs_do_not_starve_rewrites() {
    use std::io::{Read, Write};
    let path = fresh("listener");
    let (db, s) = open(&path);
    let server = db.listen_on("127.0.0.1:0".parse().unwrap()).unwrap();
    drop((db, s));
    let mut c = std::net::TcpStream::connect(server.local_addr()).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    c.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*1\r\n$6\r\nBGSAVE\r\n")
        .unwrap();
    let want = b"+OK\r\n+Background saving started\r\n";
    let mut reply = [0u8; 33];
    c.read_exact(&mut reply).unwrap();
    assert_eq!(&reply[..], &want[..]);
    thread::sleep(Duration::from_millis(100));
    let copy = copy_of(&path, "listener-copy");
    let (_cdb, cs) = open(&copy);
    assert_eq!(cs.execute(&["GET", "k"]), Reply::bulk("v"));
    drop(server);
    // The file is free again once the server is gone.
    let (_db, s) = open(&path);
    assert_eq!(s.execute(&["GET", "k"]), Reply::bulk("v"));
    drop((_db, s));

    let path = fresh("sync-storm");
    let (db, s) = open_with(
        &path,
        PersistOptions {
            interval: Duration::from_millis(20),
            rewrite_factor: 2,
            rewrite_min_bytes: 16 << 10,
            ..PersistOptions::default()
        },
    );
    let value = "v".repeat(2000);
    let until = std::time::Instant::now() + Duration::from_millis(600);
    let mut n = 0u64;
    while std::time::Instant::now() < until {
        n += 1;
        s.execute(&["SET", "same", &format!("{n}:{value}")]);
        db.sync().unwrap();
    }
    let size = fs::metadata(&path).unwrap().len();
    assert!(
        n > 20 && size < 200_000,
        "{n} synced writes left a file of {size} bytes"
    );
}
