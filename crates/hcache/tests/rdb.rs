//! RDB files: what `Db::save_rdb` writes comes back whole through
//! `Db::load_rdb`, and a damaged or foreign file loads nothing.

mod net;

use hcache::{Config, Db, Reply};
use hcache_core::testutil::Rng;
use net::{engine_cfg, server_cfg, wait_until, Env};
use std::path::PathBuf;

const LIBRARY: &str = "#!lua name=lib\nredis.register_function('one', function() return 1 end)";

fn db(databases: usize) -> Db {
    Db::open(
        None,
        Config {
            databases,
            ..Config::default()
        },
    )
    .unwrap()
}

fn run(db: &Db, args: &[&str]) -> Reply {
    db.session().unwrap().execute(args)
}

fn in_db(db: &Db, n: usize, args: &[&str]) -> Reply {
    let s = db.session().unwrap();
    assert_eq!(s.execute(&["SELECT", &n.to_string()]), Reply::Ok);
    s.execute(args)
}

fn bulk(reply: Reply) -> Vec<u8> {
    match reply {
        Reply::Bulk(b) => b,
        other => panic!("not a bulk string: {other:?}"),
    }
}

/// Every key of database `n`, with its DUMP payload and deadline.
fn contents(db: &Db, n: usize) -> Vec<(Vec<u8>, Vec<u8>, Reply)> {
    let Reply::Array(keys) = in_db(db, n, &["KEYS", "*"]) else {
        panic!("KEYS")
    };
    let mut all: Vec<_> = keys
        .into_iter()
        .map(|k| {
            let key = bulk(k);
            let name = String::from_utf8(key.clone()).unwrap();
            let dump = bulk(in_db(db, n, &["DUMP", &name]));
            let at = in_db(db, n, &["PEXPIRETIME", &name]);
            (key, dump, at)
        })
        .collect();
    all.sort_by(|a, b| a.0.cmp(&b.0));
    all
}

/// `contents` of database `n` of both, told key by key.
fn assert_same(got: &Db, want: &Db, n: usize) {
    let (got, want) = (contents(got, n), contents(want, n));
    let names = |all: &[(Vec<u8>, Vec<u8>, Reply)]| {
        let names = all
            .iter()
            .map(|(k, ..)| String::from_utf8_lossy(k).into_owned());
        names.collect::<Vec<_>>()
    };
    assert_eq!(names(&got), names(&want), "keys of database {n}");
    for (g, w) in got.iter().zip(&want) {
        let name = String::from_utf8_lossy(&g.0);
        let at = g.1.iter().zip(&w.1).position(|(a, b)| a != b);
        assert!(
            g.1 == w.1,
            "{name} in database {n}: payloads differ from byte {at:?}"
        );
        assert_eq!(g.2, w.2, "deadline of {name} in database {n}");
    }
}

fn filled() -> Db {
    let db = db(4);
    let s = db.session().unwrap();
    let ok = |args: &[&str]| {
        let reply = s.execute(args);
        assert!(!matches!(reply, Reply::Error(_)), "{args:?}: {reply:?}");
    };
    ok(&["SET", "str", "hello"]);
    ok(&["SET", "int", "12345"]);
    ok(&["SET", "big", &"x".repeat(70_000)]);
    ok(&["SET", "timed", "v", "PXAT", "4102444800000"]);
    ok(&["RPUSH", "list", "a", "1", "b", "-7"]);
    ok(&["SADD", "set", "x", "y", "42"]);
    ok(&["ZADD", "zset", "1.5", "a", "-inf", "b", "3", "c"]);
    ok(&["HSET", "hash", "f", "v", "n", "9"]);
    ok(&["XADD", "stream", "1-1", "a", "1", "b", "2"]);
    ok(&["XADD", "stream", "1-2", "a", "3", "b", "4"]);
    ok(&["XADD", "stream", "2-0", "other", "5"]);
    for i in 3..250 {
        ok(&["XADD", "stream", &format!("{i}-0"), "a", &i.to_string()]);
    }
    ok(&["XGROUP", "CREATE", "stream", "g", "0"]);
    ok(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "alice",
        "COUNT",
        "3",
        "STREAMS",
        "stream",
        ">",
    ]);
    ok(&["XGROUP", "CREATECONSUMER", "stream", "g", "bob"]);
    ok(&["XGROUP", "CREATE", "stream", "late", "$"]);
    ok(&["XDEL", "stream", "1-2"]);
    ok(&["XADD", "empty-stream", "5-5", "f", "v"]);
    ok(&["XDEL", "empty-stream", "5-5"]);
    ok(&["FUNCTION", "LOAD", LIBRARY]);
    ok(&["SELECT", "2"]);
    ok(&["SET", "elsewhere", "2"]);
    ok(&["SET", "str", "same name, other database"]);
    db
}

#[test]
fn every_type_comes_back_as_it_was() {
    let source = filled();
    let file = source.save_rdb();
    assert_eq!(&file[..9], b"REDIS0011");
    let copy = db(4);
    let stats = copy.load_rdb(&file[..]).unwrap();
    assert_eq!(stats.version, 11);
    assert_eq!(stats.keys, 12);
    assert_eq!(stats.libraries, 1);
    for n in 0..4 {
        assert_same(&copy, &source, n);
    }
    assert_eq!(run(&copy, &["FCALL", "one", "0"]), Reply::Int(1));
    assert_eq!(
        run(&copy, &["XINFO", "STREAM", "stream", "FULL"]),
        run(&source, &["XINFO", "STREAM", "stream", "FULL"])
    );
}

#[test]
fn fields_with_deadlines_need_rdb_80() {
    let source = db(1);
    run(&source, &["HSET", "h", "kept", "1"]);
    let reply = run(
        &source,
        &[
            "HSETEX",
            "h",
            "PXAT",
            "4102444800000",
            "FIELDS",
            "1",
            "f",
            "v",
        ],
    );
    assert_eq!(reply, Reply::Int(1));
    let file = source.save_rdb();
    assert_eq!(&file[..9], b"VALKEY080");
    let copy = db(1);
    assert_eq!(copy.load_rdb(&file[..]).unwrap().version, 80);
    let at = |db: &Db, field| run(db, &["HPEXPIRETIME", "h", "FIELDS", "1", field]);
    assert_eq!(
        at(&copy, "f"),
        Reply::Array(vec![Reply::Int(4_102_444_800_000)])
    );
    assert_eq!(at(&copy, "kept"), Reply::Array(vec![Reply::Int(-1)]));
    assert_same(&copy, &source, 0);
}

#[test]
fn keys_past_their_deadline_are_left_out() {
    let source = db(1);
    run(&source, &["SET", "soon", "v", "PX", "100"]);
    run(&source, &["SET", "later", "v", "PX", "100000"]);
    let file = source.save_rdb();
    let copy = db(1);
    copy.advance_clock(1_000);
    let stats = copy.load_rdb(&file[..]).unwrap();
    assert_eq!((stats.keys, stats.expired), (1, 1));
    assert_eq!(run(&copy, &["DBSIZE"]), Reply::Int(1));
}

#[test]
fn a_database_with_keys_takes_no_file() {
    let file = filled().save_rdb();
    let copy = db(4);
    run(&copy, &["SET", "mine", "1"]);
    let e = copy.load_rdb(&file[..]).unwrap_err();
    assert!(e.contains("not empty"), "{e}");
    assert_eq!(run(&copy, &["GET", "mine"]), Reply::bulk("1"));
}

#[test]
fn a_file_with_more_databases_than_here_is_refused() {
    let file = filled().save_rdb();
    let copy = db(2);
    let e = copy.load_rdb(&file[..]).unwrap_err();
    assert!(e.contains("database 2"), "{e}");
    assert_eq!(run(&copy, &["DBSIZE"]), Reply::Int(0));
}

/// A file of `body` (after the magic), with no checksum.
fn unchecked(magic: &[u8], body: &[u8]) -> Vec<u8> {
    let mut file = magic.to_vec();
    file.extend_from_slice(body);
    file.extend_from_slice(&[0; 8]);
    file
}

/// A library as a file holds it.
fn library_body(code: &str) -> Vec<u8> {
    let n = code.len();
    assert!(n < 1 << 14);
    let mut body = vec![245, 0x40 | (n >> 8) as u8, n as u8];
    body.extend_from_slice(code.as_bytes());
    body
}

#[test]
fn libraries_of_a_file_load_in_time_never_replace_and_never_stay_half() {
    let spin = "#!lua name=spin\nwhile true do end\nredis.register_function('s', function() end)";
    let copy = db(1);
    // At start nobody could send FUNCTION KILL: the load gives up itself.
    let mut body = library_body(LIBRARY);
    body.extend_from_slice(&library_body(spin));
    body.push(255);
    let started = std::time::Instant::now();
    let e = copy
        .load_rdb(&unchecked(b"REDIS0011", &body)[..])
        .unwrap_err();
    assert!(e.contains("FUNCTION LOAD timeout"), "{e}");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    // The library that loaded first went again with the file.
    assert_eq!(run(&copy, &["FUNCTION", "LIST"]), Reply::Array(Vec::new()));
    let e = run(&copy, &["FUNCTION", "LOAD", spin]);
    assert!(matches!(&e, Reply::Error(e) if e.contains("FUNCTION LOAD timeout")));
    // A library of the same name as one here is an error, not a swap.
    let mine = "#!lua name=lib\nredis.register_function('two', function() return 2 end)";
    assert_eq!(run(&copy, &["FUNCTION", "LOAD", mine]), Reply::bulk("lib"));
    let mut body = library_body(LIBRARY);
    body.push(255);
    let e = copy
        .load_rdb(&unchecked(b"REDIS0011", &body)[..])
        .unwrap_err();
    assert!(e.contains("already exists"), "{e}");
    assert_eq!(run(&copy, &["FCALL", "two", "0"]), Reply::Int(2));
}

#[test]
fn a_key_twice_is_an_error() {
    let key = [0, 1, b'k', 1, b'v'];
    let mut body = vec![254, 0];
    body.extend_from_slice(&key);
    let copy = db(1);
    let once = unchecked(b"REDIS0011", &[&body[..], &[255]].concat());
    assert_eq!(copy.load_rdb(&once[..]).unwrap().keys, 1);
    run(&copy, &["FLUSHALL"]);
    body.extend_from_slice(&key);
    let twice = unchecked(b"REDIS0011", &[&body[..], &[255]].concat());
    let e = copy.load_rdb(&twice[..]).unwrap_err();
    assert!(e.contains("twice"), "{e}");
    assert_eq!(run(&copy, &["DBSIZE"]), Reply::Int(0));
}

#[test]
fn foreign_versions_types_and_modules_are_refused() {
    let copy = db(1);
    let refused = |file: Vec<u8>, why: &str| {
        let e = copy.load_rdb(&file[..]).unwrap_err();
        assert!(e.contains(why), "{e} lacks {why}");
    };
    refused(unchecked(b"REDIS0012", &[255]), "version 12");
    refused(unchecked(b"VALKEY011", &[255]), "version 11");
    refused(unchecked(b"VALKEY081", &[255]), "version 81");
    refused(unchecked(b"MEMCACHE1", &[255]), "not an RDB file");
    refused(unchecked(b"REDIS00x1", &[255]), "not an RDB file");
    // Type 22 is a hash with field deadlines only in Valkey's files.
    refused(unchecked(b"REDIS0011", &[22, 1, b'h', 0]), "fork");
    refused(unchecked(b"VALKEY080", &[247, 0]), "module");
    refused(unchecked(b"VALKEY080", &[7, 1, b'm', 0]), "type 7");
}

#[test]
fn a_damaged_file_leaves_nothing_behind() {
    let file = filled().save_rdb();
    let copy = db(4);
    let mut flipped = file.clone();
    let at = flipped.len() - 20;
    flipped[at] ^= 0x40;
    assert!(copy.load_rdb(&flipped[..]).is_err());
    assert_eq!(in_db(&copy, 0, &["DBSIZE"]), Reply::Int(0));
    assert_eq!(in_db(&copy, 2, &["DBSIZE"]), Reply::Int(0));
    let e = copy.load_rdb(&file[..file.len() - 3]).unwrap_err();
    assert!(e.contains("ends early"), "{e}");
    let mut longer = file.clone();
    longer.push(0);
    let e = copy.load_rdb(&longer[..]).unwrap_err();
    assert!(e.contains("after the end"), "{e}");
    assert_eq!(copy.load_rdb(&file[..]).unwrap().keys, 12);
}

#[test]
fn random_damage_never_panics_and_never_half_loads() {
    let file = filled().save_rdb();
    let mut rng = Rng::new(0x5eed);
    let copy = db(4);
    for round in 0..300 {
        let mut bad = file.clone();
        if round % 3 == 0 {
            bad.truncate(rng.below(file.len() as u64) as usize);
        } else {
            for _ in 0..1 + rng.below(4) {
                let at = 9 + rng.below(file.len() as u64 - 9) as usize;
                bad[at] = rng.next_u64() as u8;
            }
        }
        // The checksum would catch most of it: take it away.
        let n = bad.len();
        if n > 8 && round % 2 == 0 {
            bad[n - 8..].fill(0);
        }
        match copy.load_rdb(&bad[..]) {
            Ok(_) => {
                run(&copy, &["FLUSHALL"]);
            }
            Err(_) => {
                for n in 0..4 {
                    assert_eq!(in_db(&copy, n, &["DBSIZE"]), Reply::Int(0), "round {round}");
                }
            }
        }
        run(&copy, &["FUNCTION", "FLUSH"]);
    }
}

/// Written by Valkey 9.1.3 (`DEBUG DIGEST` 6f449c06fed00cf068069650ecda2e08c9f16164)
/// from the commands in the comments below: every encoding Valkey picks for
/// small and large collections, LZF strings, deadlines on keys and on hash
/// fields, a stream with groups, and a library.
const VALKEY_FILE: &[u8] = include_bytes!("fixtures/valkey-9.1.3.rdb");

fn strings(reply: Reply) -> Vec<String> {
    match reply {
        Reply::Array(items) => items
            .into_iter()
            .map(|r| String::from_utf8(bulk(r)).unwrap())
            .collect(),
        other => panic!("not an array: {other:?}"),
    }
}

#[test]
fn a_file_of_valkey_9_loads_whole() {
    let copy = db(4);
    let stats = copy.load_rdb(VALKEY_FILE).unwrap();
    assert_eq!(stats.version, 80);
    assert_eq!((stats.keys, stats.expired, stats.libraries), (19, 0, 1));
    let get = |args: &[&str]| run(&copy, args);
    let text = |args: &[&str]| String::from_utf8(bulk(get(args))).unwrap();
    assert_eq!(text(&["GET", "str"]), "hello");
    assert_eq!(text(&["GET", "int"]), "12345");
    assert_eq!(text(&["GET", "neg"]), "-77");
    assert_eq!(text(&["GET", "long"]), "abcdefghij".repeat(300));
    assert_eq!(
        get(&["PEXPIRETIME", "timed"]),
        Reply::Int(4_102_444_800_000)
    );
    assert_eq!(
        strings(get(&["LRANGE", "list", "0", "-1"])),
        ["a", "1", "b", "-7", "ccc"]
    );
    assert_eq!(get(&["LLEN", "biglist"]), Reply::Int(200));
    assert_eq!(text(&["LINDEX", "biglist", "199"]), "item-200");
    let mut ints = strings(get(&["SMEMBERS", "intset"]));
    ints.sort();
    assert_eq!(ints, ["1", "1000000", "2", "3"]);
    assert_eq!(get(&["SCARD", "bigset"]), Reply::Int(200));
    assert_eq!(get(&["SISMEMBER", "set", "y"]), Reply::Int(1));
    let zset = strings(get(&["ZRANGE", "zset", "0", "-1", "WITHSCORES"]));
    assert_eq!(zset, ["b", "-inf", "a", "1.5", "c", "3"]);
    assert_eq!(text(&["ZSCORE", "bigzset", "z77"]), "77");
    assert_eq!(get(&["ZCARD", "bigzset"]), Reply::Int(200));
    assert_eq!(text(&["HGET", "hash", "n"]), "9");
    assert_eq!(text(&["HGET", "bighash", "f150"]), "v150");
    assert_eq!(get(&["HLEN", "bighash"]), Reply::Int(200));
    let at = get(&["HPEXPIRETIME", "thash", "FIELDS", "3", "kept", "f1", "f2"]);
    let far = Reply::Int(4_102_444_800_000);
    assert_eq!(at, Reply::Array(vec![Reply::Int(-1), far.clone(), far]));
    assert_eq!(get(&["XLEN", "stream"]), Reply::Int(250));
    assert_eq!(get(&["XLEN", "empty"]), Reply::Int(0));
    let pending = get(&["XPENDING", "stream", "g"]);
    let want = Reply::Array(vec![
        Reply::Int(3),
        Reply::bulk("1-1"),
        Reply::bulk("2-0"),
        Reply::Array(vec![Reply::Array(vec![
            Reply::bulk("alice"),
            Reply::bulk("3"),
        ])]),
    ]);
    assert_eq!(pending, want);
    let (Reply::Array(info) | Reply::Map(info)) = get(&["XINFO", "STREAM", "stream"]) else {
        panic!("XINFO")
    };
    let field = |name: &str| {
        let at = info.iter().position(|r| *r == Reply::bulk(name)).unwrap();
        info[at + 1].clone()
    };
    assert_eq!(field("last-generated-id"), Reply::bulk("250-0"));
    assert_eq!(field("max-deleted-entry-id"), Reply::bulk("1-2"));
    assert_eq!(field("entries-added"), Reply::Int(251));
    assert_eq!(field("groups"), Reply::Int(2));
    assert_eq!(get(&["FCALL", "one", "0"]), Reply::Int(1));
    assert_eq!(in_db(&copy, 3, &["GET", "other-db"]), Reply::bulk("yes"));
    let other = in_db(&copy, 3, &["GET", "str"]);
    assert_eq!(other, Reply::bulk("same name, other database"));
    // What HCache writes of it reads back the same here.
    let again = db(4);
    again.load_rdb(&copy.save_rdb()[..]).unwrap();
    for n in 0..4 {
        assert_same(&again, &copy, n);
    }
}

/// For a script that checks HCache's files with Valkey (`HCACHE_RDB_DIR`):
/// what HCache makes of Valkey's file, and `filled` as RDB 11.
#[test]
#[ignore]
fn write_files_for_valkey() {
    let dir = std::path::PathBuf::from(std::env::var("HCACHE_RDB_DIR").expect("HCACHE_RDB_DIR"));
    let copy = db(4);
    copy.load_rdb(VALKEY_FILE).unwrap();
    std::fs::write(dir.join("fixture-back.rdb"), copy.save_rdb()).unwrap();
    std::fs::write(dir.join("hcache11.rdb"), filled().save_rdb()).unwrap();
}

/// The other half: Valkey loaded `hcache11.rdb` and saved it again as
/// `valkey-of-hcache11.rdb`; both read the same here.
#[test]
#[ignore]
fn read_what_valkey_made_of_ours() {
    let dir = std::path::PathBuf::from(std::env::var("HCACHE_RDB_DIR").expect("HCACHE_RDB_DIR"));
    let load = |name: &str| {
        let db = db(4);
        db.load_rdb(&std::fs::read(dir.join(name)).unwrap()[..])
            .unwrap();
        db
    };
    let (ours, theirs) = (load("hcache11.rdb"), load("valkey-of-hcache11.rdb"));
    let full = |db: &Db| format!("{:?}", run(db, &["XINFO", "STREAM", "stream", "FULL"]));
    assert_eq!(full(&theirs), full(&ours));
    for n in 0..4 {
        assert_same(&theirs, &ours, n);
    }
}

/// A directory of its own for one test's RDB file.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("hcache-rdb-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn info_field(info: &Reply, name: &str) -> String {
    let text = String::from_utf8(match info {
        Reply::Bulk(b) => b.clone(),
        other => panic!("INFO: {other:?}"),
    })
    .unwrap();
    let line = text.lines().find(|l| l.starts_with(&format!("{name}:")));
    line.unwrap_or_else(|| panic!("no {name} in INFO"))[name.len() + 1..].to_string()
}

#[test]
fn save_and_bgsave_write_the_rdb_file_over_the_network() {
    let dir = scratch("server");
    let file = dir.join("dump.rdb");
    let cfg = Config {
        rdb_file: Some(file.clone()),
        ..engine_cfg(4)
    };
    let env = Env::new(cfg, server_cfg(2, 64));
    let mut c = env.client();
    assert_eq!(c.cmd(&["SET", "k", "1"]), Reply::Ok);
    assert_eq!(c.cmd(&["SAVE"]), Reply::Ok);
    let saved = std::fs::read(&file).unwrap();
    assert_eq!(&saved[..9], b"REDIS0011");
    assert!(matches!(c.cmd(&["LASTSAVE"]), Reply::Int(t) if t > 0));
    assert_eq!(c.cmd(&["SET", "k", "2"]), Reply::Ok);
    let started = Reply::Simple("Background saving started".into());
    assert_eq!(c.cmd(&["BGSAVE"]), started);
    assert!(wait_until(10, || {
        info_field(&c.cmd(&["INFO", "persistence"]), "rdb_bgsave_in_progress") == "0"
    }));
    let info = c.cmd(&["INFO", "persistence"]);
    assert_eq!(info_field(&info, "rdb_last_bgsave_status"), "ok");
    let copy = db(16);
    copy.load_rdb(std::fs::File::open(&file).unwrap()).unwrap();
    assert_eq!(run(&copy, &["GET", "k"]), Reply::bulk("2"));
    // In a transaction the gate is held already: still written.
    assert_eq!(c.cmd(&["SET", "k", "3"]), Reply::Ok);
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    assert_eq!(c.cmd(&["BGSAVE"]), Reply::Simple("QUEUED".into()));
    assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![started]));
    // Taken in the transaction, written by a thread of its own.
    assert!(wait_until(10, || {
        info_field(&c.cmd(&["INFO", "persistence"]), "rdb_bgsave_in_progress") == "0"
    }));
    let copy = db(16);
    copy.load_rdb(std::fs::File::open(&file).unwrap()).unwrap();
    assert_eq!(run(&copy, &["GET", "k"]), Reply::bulk("3"));
    let where_to = c.cmd(&["CONFIG", "GET", "dbfilename"]);
    assert_eq!(
        where_to,
        Reply::Array(vec![Reply::bulk("dbfilename"), Reply::bulk("dump.rdb")])
    );
    let Reply::Array(got) = c.cmd(&["CONFIG", "GET", "dir"]) else {
        panic!("CONFIG GET dir")
    };
    let shown = PathBuf::from(String::from_utf8(bulk(got[1].clone())).unwrap());
    assert_eq!(shown.canonicalize().unwrap(), dir.canonicalize().unwrap());
    for name in ["dir", "dbfilename"] {
        let refused = c.cmd(&["CONFIG", "SET", name, "/tmp"]);
        assert!(matches!(&refused, Reply::Error(e) if e.contains("protected config")));
    }
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_file_that_cannot_be_written_is_an_error_and_shows_in_info() {
    let dir = scratch("unwritable");
    let cfg = Config {
        rdb_file: Some(dir.join("missing").join("dump.rdb")),
        ..Config::default()
    };
    let env = Env::new(cfg, server_cfg(1, 64));
    let mut c = env.client();
    assert!(matches!(c.cmd(&["SAVE"]), Reply::Error(e) if e.contains("dump.rdb")));
    let info = c.cmd(&["INFO", "persistence"]);
    assert_eq!(info_field(&info, "rdb_last_bgsave_status"), "err");
    assert_eq!(info_field(&info, "rdb_bgsave_in_progress"), "0");
    // Nothing half written is left behind.
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_embedded_session_saves_the_rdb_file_too() {
    let dir = scratch("embedded");
    let file = dir.join("dump.rdb");
    let db = Db::open(
        None,
        Config {
            rdb_file: Some(file.clone()),
            ..Config::default()
        },
    )
    .unwrap();
    run(&db, &["SET", "k", "v"]);
    assert_eq!(run(&db, &["SAVE"]), Reply::Ok);
    let copy = self::db(16);
    copy.load_rdb(std::fs::File::open(&file).unwrap()).unwrap();
    assert_eq!(run(&copy, &["GET", "k"]), Reply::bulk("v"));
    let started = Reply::Simple("Background saving started".into());
    assert_eq!(run(&db, &["BGSAVE"]), started);
    let _ = std::fs::remove_dir_all(&dir);
}
