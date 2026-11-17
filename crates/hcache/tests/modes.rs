//! How the server does its work, chosen per call or at start: large
//! values freed on another thread (UNLINK, FLUSHALL ASYNC, the
//! `lazyfree-lazy-*` settings), WAIT and WAITAOF, and databases that are
//! kept in memory only.

mod net;

use hcache::{Config, Db, Fsync, PersistOptions, Reply, Session};
use net::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn fresh(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("modes");
    fs::create_dir_all(&dir).unwrap();
    let n = NEXT.fetch_add(1, Ordering::SeqCst);
    let path = dir.join(format!("{name}-{}-{n}.hc", std::process::id()));
    for suffix in ["", ".lock", ".rewrite"] {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        match fs::remove_file(PathBuf::from(p)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("cannot clear {}: {e}", path.display()),
        }
    }
    path
}

/// The background writer all but never runs on its own.
fn still(fsync: Fsync) -> PersistOptions {
    PersistOptions {
        interval: Duration::from_secs(3600),
        fsync,
        ..PersistOptions::default()
    }
}

fn open(path: &Path, cfg: Config, options: PersistOptions) -> (Db, Session) {
    let db = Db::open_with(Some(path), cfg, options)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let s = db.session().unwrap();
    (db, s)
}

/// What a process that starts now from the bytes on disk would see.
fn snapshot(path: &Path) -> (Db, Session) {
    let copy = fresh("snapshot");
    fs::copy(path, &copy).unwrap();
    open(&copy, engine_cfg(4), still(Fsync::Interval))
}

fn memory() -> Session {
    Db::open(None, engine_cfg(4)).unwrap().session().unwrap()
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn pair(local: i64, replicas: i64) -> Reply {
    Reply::Array(vec![Reply::Int(local), Reply::Int(replicas)])
}

fn err(reply: Reply) -> String {
    match reply {
        Reply::Error(e) => e,
        other => panic!("expected an error, got {other:?}"),
    }
}

fn info_num(s: &Session, field: &str) -> u64 {
    let Reply::Bulk(text) = s.execute(&["INFO"]) else {
        panic!("INFO is not a bulk string");
    };
    let text = String::from_utf8(text).unwrap();
    let prefix = format!("{field}:");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("no {field} in INFO"))
        .trim()
        .parse()
        .unwrap()
}

fn freed(s: &Session) -> u64 {
    info_num(s, "lazyfreed_objects")
}

/// Wait until the freeing thread has freed `n` objects and has none left.
fn freed_reaches(s: &Session, n: u64) {
    assert!(
        wait_until(10, || freed(s) >= n
            && info_num(s, "lazyfree_pending_objects") == 0),
        "freed {} of {n}",
        freed(s)
    );
}

fn big_hash(s: &Session, key: &str, fields: usize) {
    let mut args = vec!["HSET".to_string(), key.to_string()];
    for i in 0..fields {
        args.push(format!("f{i}"));
        args.push("v".into());
    }
    assert_eq!(s.execute(&args), Reply::Int(fields as i64));
}

// ---------------------------------------------------------------------
// Freeing on another thread
// ---------------------------------------------------------------------

#[test]
fn unlink_frees_a_large_value_on_another_thread_and_a_small_one_here() {
    let s = memory();
    big_hash(&s, "h", 1000);
    s.execute(&["SET", "s", "x"]);
    s.execute(&["RPUSH", "l", "a", "b", "c"]);
    assert_eq!(
        s.execute(&["UNLINK", "h", "s", "l", "missing"]),
        Reply::Int(3)
    );
    assert_eq!(s.execute(&["EXISTS", "h", "s", "l"]), Reply::Int(0));
    freed_reaches(&s, 1);
    assert_eq!(freed(&s), 1, "only the hash is large");
    assert!(err(s.execute(&["UNLINK"])).contains("wrong number of arguments"));
}

#[test]
fn del_frees_as_lazyfree_lazy_user_del_says() {
    let s = memory();
    assert_eq!(
        s.execute(&["CONFIG", "GET", "lazyfree-lazy-user-del"]),
        Reply::Array(vec![b("lazyfree-lazy-user-del"), b("yes")])
    );
    assert_eq!(
        s.execute(&["CONFIG", "SET", "lazyfree-lazy-user-del", "no"]),
        Reply::Ok
    );
    big_hash(&s, "h1", 200);
    assert_eq!(s.execute(&["DEL", "h1"]), Reply::Int(1));
    assert_eq!(freed(&s), 0, "freed on the spot");
    assert_eq!(
        s.execute(&["CONFIG", "SET", "lazyfree-lazy-user-del", "YES"]),
        Reply::Ok
    );
    big_hash(&s, "h2", 200);
    assert_eq!(s.execute(&["DEL", "h2"]), Reply::Int(1));
    freed_reaches(&s, 1);
    assert!(
        err(s.execute(&["CONFIG", "SET", "lazyfree-lazy-user-del", "maybe"]))
            .contains("lazyfree-lazy-user-del")
    );
}

#[test]
fn every_setting_is_shown_and_on_by_default() {
    let s = memory();
    let Reply::Array(all) = s.execute(&["CONFIG", "GET", "lazyfree-*"]) else {
        panic!("CONFIG GET is not an array");
    };
    let names: Vec<Reply> = all.iter().step_by(2).cloned().collect();
    for name in [
        "lazyfree-lazy-eviction",
        "lazyfree-lazy-expire",
        "lazyfree-lazy-server-del",
        "lazyfree-lazy-user-del",
        "lazyfree-lazy-user-flush",
    ] {
        assert!(names.contains(&b(name)), "{name}");
    }
    assert!(all.iter().skip(1).step_by(2).all(|v| *v == b("yes")));
}

#[test]
fn replacing_or_expiring_a_large_value_follows_its_own_setting() {
    let s = memory();
    // SET over a hash: a side effect of the command.
    s.execute(&["CONFIG", "SET", "lazyfree-lazy-server-del", "no"]);
    big_hash(&s, "h", 300);
    assert_eq!(s.execute(&["SET", "h", "now a string"]), Reply::Ok);
    assert_eq!(freed(&s), 0);
    s.execute(&["CONFIG", "SET", "lazyfree-lazy-server-del", "yes"]);
    big_hash(&s, "h2", 300);
    assert_eq!(s.execute(&["SET", "h2", "now a string"]), Reply::Ok);
    freed_reaches(&s, 1);
    // A deadline in the past deletes at once.
    s.execute(&["CONFIG", "SET", "lazyfree-lazy-expire", "no"]);
    big_hash(&s, "e1", 300);
    assert_eq!(s.execute(&["EXPIRE", "e1", "-1"]), Reply::Int(1));
    assert_eq!(freed(&s), 1);
    s.execute(&["CONFIG", "SET", "lazyfree-lazy-expire", "yes"]);
    big_hash(&s, "e2", 300);
    assert_eq!(s.execute(&["EXPIRE", "e2", "-1"]), Reply::Int(1));
    freed_reaches(&s, 2);
    assert_eq!(s.execute(&["EXISTS", "h", "e1", "e2"]), Reply::Int(1));
}

#[test]
fn flushall_and_flushdb_take_async_and_sync() {
    let s = memory();
    for bad in [
        vec!["FLUSHALL", "NOW"],
        vec!["FLUSHALL", "ASYNC", "SYNC"],
        vec!["FLUSHDB", "LATER"],
    ] {
        assert_eq!(err(s.execute(&bad)), "ERR syntax error", "{bad:?}");
    }
    big_hash(&s, "zero", 100);
    assert_eq!(s.execute(&["SELECT", "1"]), Reply::Ok);
    big_hash(&s, "one", 100);
    assert_eq!(s.execute(&["FLUSHDB", "sync"]), Reply::Ok);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
    assert_eq!(freed(&s), 0);
    assert_eq!(s.execute(&["SELECT", "0"]), Reply::Ok);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(1));
    assert_eq!(s.execute(&["SET", "small", "1"]), Reply::Ok);
    assert_eq!(s.execute(&["FLUSHALL", "ASYNC"]), Reply::Ok);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
    // A flushed keyspace counts its keys.
    freed_reaches(&s, 2);
    // Without an option, lazyfree-lazy-user-flush decides.
    s.execute(&["CONFIG", "SET", "lazyfree-lazy-user-flush", "no"]);
    big_hash(&s, "again", 100);
    assert_eq!(s.execute(&["FLUSHALL"]), Reply::Ok);
    assert_eq!(freed(&s), 2);
    s.execute(&["CONFIG", "SET", "lazyfree-lazy-user-flush", "yes"]);
    big_hash(&s, "again", 100);
    assert_eq!(s.execute(&["FLUSHDB"]), Reply::Ok);
    freed_reaches(&s, 3);
    // Writes right after a lazy flush see an empty keyspace.
    assert_eq!(s.execute(&["HLEN", "again"]), Reply::Int(0));
    big_hash(&s, "again", 10);
}

#[test]
fn unlink_and_async_flush_work_over_the_network() {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let mut c = env.client();
    let mut args = vec!["RPUSH".to_string(), "list".into()];
    args.extend((0..500).map(|i| i.to_string()));
    assert_eq!(c.cmd(&args), Reply::Int(500));
    assert_eq!(c.cmd(&["UNLINK", "list"]), Reply::Int(1));
    assert_eq!(c.cmd(&["SET", "k", "v"]), Reply::Ok);
    assert_eq!(c.cmd(&["FLUSHALL", "ASYNC"]), Reply::Ok);
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(0));
    // UNLINK keeps its keys for ACL and tracking as DEL does.
    assert_eq!(
        c.cmd(&["COMMAND", "GETKEYS", "UNLINK", "a", "b"]),
        Reply::Array(vec![b("a"), b("b")])
    );
}

// ---------------------------------------------------------------------
// WAIT and WAITAOF
// ---------------------------------------------------------------------

#[test]
fn wait_answers_at_once_since_no_replica_can_acknowledge() {
    let s = memory();
    assert_eq!(s.execute(&["WAIT", "0", "0"]), Reply::Int(0));
    assert_eq!(s.execute(&["WAIT", "3", "100"]), Reply::Int(0));
    assert_eq!(
        err(s.execute(&["WAIT", "x", "0"])),
        "ERR value is not an integer or out of range"
    );
    assert_eq!(
        err(s.execute(&["WAIT", "0", "-1"])),
        "ERR timeout is negative"
    );
    assert_eq!(
        err(s.execute(&["WAIT", "0", "1.5"])),
        "ERR timeout is not an integer or out of range"
    );
    assert_eq!(
        err(s.execute(&["WAIT", "0", "9223372036854775807"])),
        "ERR timeout is out of range"
    );
    assert!(err(s.execute(&["WAIT", "0"])).contains("wrong number of arguments for 'wait'"));
}

#[test]
fn waitaof_without_a_data_file_has_nothing_to_wait_for() {
    let s = memory();
    assert_eq!(s.execute(&["WAITAOF", "0", "0", "0"]), pair(0, 0));
    assert_eq!(
        err(s.execute(&["WAITAOF", "1", "0", "0"])),
        "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled."
    );
    assert_eq!(
        err(s.execute(&["WAITAOF", "2", "0", "0"])),
        "ERR value is out of range, value must between 0 and 1"
    );
    assert_eq!(
        err(s.execute(&["WAITAOF", "0", "-1", "0"])),
        "ERR value is out of range, must be positive"
    );
    assert_eq!(
        err(s.execute(&["WAITAOF", "0", "0", "-5"])),
        "ERR timeout is negative"
    );
    assert_eq!(
        err(s.execute(&["WAITAOF", "x", "0", "0"])),
        "ERR value is not an integer or out of range"
    );
    assert!(err(s.execute(&["WAITAOF", "0", "0"])).contains("wrong number of arguments"));
}

#[test]
fn waitaof_puts_the_writes_before_it_on_disk() {
    let path = fresh("waitaof");
    let (db, s) = open(&path, engine_cfg(4), still(Fsync::Interval));
    assert_eq!(s.execute(&["SET", "k", "v"]), Reply::Ok);
    // numlocal 0 waits for nothing and syncs nothing.
    assert_eq!(s.execute(&["WAITAOF", "0", "0", "0"]), pair(0, 0));
    {
        let (_copy, c) = snapshot(&path);
        assert_eq!(c.execute(&["GET", "k"]), Reply::Nil, "not written yet");
    }
    assert_eq!(s.execute(&["WAITAOF", "1", "0", "0"]), pair(1, 0));
    {
        let (_copy, c) = snapshot(&path);
        assert_eq!(c.execute(&["GET", "k"]), b("v"));
    }
    // Over the network: the reply waits for the disk as a write does under
    // `appendfsync always`.
    let server = db.listen(server_cfg(2, 64)).unwrap();
    let mut c = Client::connect(server.local_addr());
    assert_eq!(c.cmd(&["SET", "net", "1"]), Reply::Ok);
    assert_eq!(c.cmd(&["WAITAOF", "1", "0", "100"]), pair(1, 0));
    {
        let (_copy, snap) = snapshot(&path);
        assert_eq!(snap.execute(&["GET", "net"]), b("1"));
    }
    assert_eq!(c.cmd(&["SET", "later", "1"]), Reply::Ok);
    assert_eq!(c.cmd(&["WAITAOF", "0", "0", "0"]), pair(0, 0));
    {
        let (_copy, snap) = snapshot(&path);
        assert_eq!(snap.execute(&["GET", "later"]), Reply::Nil);
    }
    // A transaction cannot wait: it tells how things stand.
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    c.cmd(&["SET", "t", "1"]);
    c.cmd(&["WAITAOF", "0", "0", "0"]);
    assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![Reply::Ok, pair(0, 0)]));
    // Under `always` the reply of EXEC itself waits for the disk.
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "appendfsync", "always"]),
        Reply::Ok
    );
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    c.cmd(&["SET", "t", "2"]);
    c.cmd(&["WAITAOF", "1", "0", "0"]);
    assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![Reply::Ok, pair(1, 0)]));
    let (_copy, snap) = snapshot(&path);
    assert_eq!(snap.execute(&["GET", "t"]), b("2"));
}

#[test]
fn waitaof_is_not_for_scripts() {
    let s = memory();
    let reply = s.execute(&["EVAL", "return redis.call('WAITAOF', 0, 0, 0)", "0"]);
    assert!(matches!(reply, Reply::Error(_)), "{reply:?}");
}

// ---------------------------------------------------------------------
// Databases kept in memory only
// ---------------------------------------------------------------------

fn with_memory_dbs(dbs: &[usize]) -> Config {
    Config {
        memory_dbs: dbs.to_vec(),
        ..engine_cfg(4)
    }
}

fn holds(path: &Path, needle: &[u8]) -> bool {
    fs::read(path)
        .unwrap()
        .windows(needle.len())
        .any(|w| w == needle)
}

#[test]
fn a_memory_database_never_reaches_the_data_file() {
    let path = fresh("memory-db");
    {
        let quick = PersistOptions {
            interval: Duration::from_millis(5),
            ..PersistOptions::default()
        };
        let (_db, s) = open(&path, with_memory_dbs(&[1]), quick);
        assert_eq!(
            s.execute(&["CONFIG", "GET", "memory-dbs"]),
            Reply::Array(vec![b("memory-dbs"), b("1")])
        );
        assert!(err(s.execute(&["CONFIG", "SET", "memory-dbs", "2"])).contains("immutable"));
        assert!(err(s.execute(&["CONFIG", "SET", "databases", "4"])).contains("immutable"));
        assert_eq!(s.execute(&["SET", "kept", "0"]), Reply::Ok);
        assert_eq!(s.execute(&["SELECT", "1"]), Reply::Ok);
        assert_eq!(s.execute(&["SET", "cached-key", "1"]), Reply::Ok);
        big_hash(&s, "cached-hash", 10);
        // Moving a key out of the cache keeps it.
        assert_eq!(s.execute(&["SET", "promoted", "1"]), Reply::Ok);
        assert_eq!(s.execute(&["MOVE", "promoted", "0"]), Reply::Int(1));
        // Rewrites of one key make the file long, so that a rewrite of the
        // whole file shows: it gets short.
        assert_eq!(s.execute(&["SELECT", "0"]), Reply::Ok);
        for i in 0..50 {
            s.execute(&["SET", "churn", &i.to_string()]);
            assert_eq!(s.execute(&["SAVE"]), Reply::Ok);
        }
        assert!(!holds(&path, b"cached-"));
        let long = fs::metadata(&path).unwrap().len();
        assert_eq!(
            s.execute(&["BGSAVE"]),
            Reply::Simple("Background saving started".into())
        );
        assert!(wait_until(10, || fs::metadata(&path)
            .map_or(0, |m| m.len())
            < long / 2));
        assert!(!holds(&path, b"cached-"));
        assert!(holds(&path, b"promoted"));
    }
    let (_db, s) = open(&path, with_memory_dbs(&[1]), still(Fsync::Interval));
    assert_eq!(s.execute(&["GET", "kept"]), b("0"));
    assert_eq!(s.execute(&["GET", "promoted"]), b("1"));
    assert_eq!(s.execute(&["SELECT", "1"]), Reply::Ok);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
}

#[test]
fn swapdb_with_a_memory_database_stores_what_the_kept_side_now_holds() {
    let path = fresh("memory-swap");
    {
        let (_db, s) = open(&path, with_memory_dbs(&[1]), still(Fsync::Interval));
        assert_eq!(s.execute(&["SET", "was-kept", "0"]), Reply::Ok);
        assert_eq!(s.execute(&["SELECT", "1"]), Reply::Ok);
        assert_eq!(s.execute(&["SET", "was-cached", "1"]), Reply::Ok);
        assert_eq!(s.execute(&["SWAPDB", "0", "1"]), Reply::Ok);
        assert_eq!(s.execute(&["SAVE"]), Reply::Ok);
    }
    let (_db, s) = open(&path, with_memory_dbs(&[1]), still(Fsync::Interval));
    assert_eq!(s.execute(&["GET", "was-cached"]), b("1"));
    assert_eq!(s.execute(&["EXISTS", "was-kept"]), Reply::Int(0));
}

#[test]
fn a_database_made_memory_only_loses_its_stored_keys_for_good() {
    let path = fresh("memory-turned");
    {
        let (_db, s) = open(&path, with_memory_dbs(&[]), still(Fsync::Interval));
        assert_eq!(s.execute(&["SET", "zero", "0"]), Reply::Ok);
        assert_eq!(s.execute(&["SELECT", "1"]), Reply::Ok);
        assert_eq!(s.execute(&["SET", "old", "1"]), Reply::Ok);
        assert_eq!(s.execute(&["SAVE"]), Reply::Ok);
    }
    {
        let (_db, s) = open(&path, with_memory_dbs(&[1]), still(Fsync::Interval));
        assert_eq!(s.execute(&["SELECT", "1"]), Reply::Ok);
        assert_eq!(s.execute(&["EXISTS", "old"]), Reply::Int(0));
        assert_eq!(s.execute(&["SET", "new", "1"]), Reply::Ok);
    }
    // Kept again: neither the key from before nor the one from the
    // memory-only start comes back.
    let (_db, s) = open(&path, with_memory_dbs(&[]), still(Fsync::Interval));
    assert_eq!(s.execute(&["GET", "zero"]), b("0"));
    assert_eq!(s.execute(&["SELECT", "1"]), Reply::Ok);
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(0));
}
