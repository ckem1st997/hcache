//! What a reply promises about the disk (`appendfsync`, SAVE), functions
//! and users that outlive a restart, CLIENT KILL, and the connections a
//! server gives up on: silent ones and ones that do not read.

mod net;

use hotstash::{Db, Fsync, PersistOptions, Reply, ServerConfig, Session};
use net::*;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn fresh(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("durability");
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

/// The background writer all but never runs on its own: what reaches the
/// file reaches it because something asked.
fn lazy(fsync: Fsync) -> PersistOptions {
    PersistOptions {
        interval: Duration::from_secs(3600),
        fsync,
        ..PersistOptions::default()
    }
}

fn quick() -> PersistOptions {
    PersistOptions {
        interval: Duration::from_millis(5),
        ..PersistOptions::default()
    }
}

fn open(path: &Path, options: PersistOptions) -> (Db, Session) {
    let db = Db::open_with(Some(path), engine_cfg(4), options)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let s = db.session().unwrap();
    (db, s)
}

/// What a process that starts now from the bytes on disk would see, while
/// the database stays open.
fn snapshot(path: &Path) -> (Db, Session) {
    let copy = fresh("snapshot");
    fs::copy(path, &copy).unwrap();
    open(&copy, quick())
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn int(reply: Reply) -> i64 {
    match reply {
        Reply::Int(n) => n,
        other => panic!("not an integer: {other:?}"),
    }
}

fn error(reply: Reply) -> String {
    match reply {
        Reply::Error(e) => e,
        other => panic!("not an error: {other:?}"),
    }
}

fn text(reply: Reply) -> String {
    match reply {
        Reply::Bulk(v) => String::from_utf8(v).expect("utf-8"),
        Reply::Simple(s) => s,
        other => panic!("not text: {other:?}"),
    }
}

fn lib(name: &str, func: &str, body: &str) -> String {
    format!("#!lua name={name}\nredis.register_function('{func}', function(keys, args) {body} end)")
}

// ---------------------------------------------------------------------
// appendfsync and SAVE
// ---------------------------------------------------------------------

#[test]
fn always_puts_a_write_on_disk_before_it_is_answered() {
    let path = fresh("always");
    let (_db, s) = open(&path, lazy(Fsync::Always));
    ok(s.execute(&["SET", "k", "v"]));
    assert_eq!(s.execute(&["RPUSH", "l", "a", "b"]), Reply::Int(2));
    ok(s.execute(&["MULTI"]));
    s.execute(&["INCR", "n"]);
    s.execute(&["HSET", "h", "f", "1"]);
    s.execute(&["EXEC"]);
    s.execute(&["EVAL", "return redis.call('SET', KEYS[1], 'lua')", "1", "e"]);
    let (_copy, c) = snapshot(&path);
    assert_eq!(c.execute(&["GET", "k"]), b("v"));
    assert_eq!(c.execute(&["LLEN", "l"]), Reply::Int(2));
    assert_eq!(c.execute(&["GET", "n"]), b("1"));
    assert_eq!(c.execute(&["HGET", "h", "f"]), b("1"));
    assert_eq!(c.execute(&["GET", "e"]), b("lua"));
}

#[test]
fn without_always_a_write_waits_for_its_turn_and_save_does_not() {
    for fsync in [Fsync::Interval, Fsync::Never] {
        let path = fresh("lazy");
        let (_db, s) = open(&path, lazy(fsync));
        ok(s.execute(&["SET", "k", "v"]));
        {
            let (_copy, c) = snapshot(&path);
            assert_eq!(c.execute(&["GET", "k"]), Reply::Nil, "{fsync:?}");
        }
        ok(s.execute(&["SAVE"]));
        let (_copy, c) = snapshot(&path);
        assert_eq!(c.execute(&["GET", "k"]), b("v"), "{fsync:?}");
    }
}

#[test]
fn appendfsync_is_a_setting_that_can_be_read_and_changed() {
    let path = fresh("config");
    let (_db, s) = open(&path, lazy(Fsync::Interval));
    let get = |s: &Session| s.execute(&["CONFIG", "GET", "appendfsync"]);
    assert_eq!(get(&s), Reply::Array(vec![b("appendfsync"), b("everysec")]));
    ok(s.execute(&["SET", "before", "1"]));
    ok(s.execute(&["CONFIG", "SET", "appendfsync", "always"]));
    assert_eq!(get(&s), Reply::Array(vec![b("appendfsync"), b("always")]));
    ok(s.execute(&["SET", "after", "1"]));
    {
        // One sync brings everything, not only the last write.
        let (_copy, c) = snapshot(&path);
        assert_eq!(c.execute(&["EXISTS", "before", "after"]), Reply::Int(2));
    }
    ok(s.execute(&["CONFIG", "SET", "appendfsync", "NO"]));
    assert_eq!(get(&s), Reply::Array(vec![b("appendfsync"), b("no")]));
    ok(s.execute(&["SET", "later", "1"]));
    {
        let (_copy, c) = snapshot(&path);
        assert_eq!(c.execute(&["EXISTS", "later"]), Reply::Int(0));
    }
    for bad in ["", "sometimes", "always ", "1"] {
        let e = error(s.execute(&["CONFIG", "SET", "appendfsync", bad]));
        assert!(e.starts_with("ERR"), "{bad:?}: {e}");
    }
    assert_eq!(get(&s), Reply::Array(vec![b("appendfsync"), b("no")]));
}

#[test]
fn save_without_a_data_file_says_so() {
    let db = Db::open(None, engine_cfg(2)).unwrap();
    let s = db.session().unwrap();
    assert_eq!(
        s.execute(&["SAVE"]),
        Reply::err("ERR this database was opened without a data file")
    );
    assert!(error(s.execute(&["SAVE", "x"])).starts_with("ERR"));
    // The setting is kept even though nothing is written.
    ok(s.execute(&["CONFIG", "SET", "appendfsync", "always"]));
    ok(s.execute(&["SET", "k", "v"]));
    let server = db.listen(server_cfg(1, 16)).unwrap();
    let mut c = Client::connect(server.local_addr());
    assert_eq!(
        c.cmd(&["SAVE"]),
        Reply::err("ERR this database was opened without a data file")
    );
    ok(c.cmd(&["SET", "k2", "v"]));
}

#[test]
fn save_inside_a_transaction_or_a_script_does_not_hang() {
    watchdog("save-in-multi", 30, || {
        let path = fresh("save-multi");
        let (db, s) = open(&path, lazy(Fsync::Interval));
        ok(s.execute(&["MULTI"]));
        let queued = s.execute(&["SAVE"]);
        let done = s.execute(&["EXEC"]);
        // Refused when queued: never stuck.
        assert_eq!(
            queued,
            Reply::err("ERR Command not allowed inside a transaction")
        );
        assert!(error(done).starts_with("EXECABORT"));
        let r = s.execute(&["EVAL", "return redis.pcall('SAVE')", "0"]);
        assert!(matches!(r, Reply::Error(_) | Reply::Ok), "{r:?}");
        let server = db.listen(server_cfg(2, 16)).unwrap();
        let mut c = Client::connect(server.local_addr());
        ok(c.cmd(&["MULTI"]));
        let queued = c.cmd(&["SAVE"]);
        let done = c.cmd(&["EXEC"]);
        assert!(matches!(queued, Reply::Error(_)), "{queued:?}");
        assert!(error(done).starts_with("EXECABORT"));
        // CLIENT KILL runs inside EXEC (Sentinel sends it there) and skips the
        // caller.
        ok(c.cmd(&["MULTI"]));
        assert_eq!(
            c.cmd(&["CLIENT", "KILL", "TYPE", "normal"]),
            Reply::Simple("QUEUED".into())
        );
        assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![Reply::Int(0)]));
        let r = c.cmd(&["EVAL", "return redis.pcall('SAVE')", "0"]);
        assert!(matches!(r, Reply::Error(_) | Reply::Ok), "{r:?}");
        assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    });
}

#[test]
fn always_holds_tcp_replies_until_the_disk_has_the_change() {
    watchdog("always-tcp", 120, || {
        let path = fresh("always-tcp");
        let (db, _s) = open(&path, lazy(Fsync::Always));
        let server = db.listen(server_cfg(3, 64)).unwrap();
        let addr = server.local_addr();
        let mut c = Client::connect(addr);
        ok(c.cmd(&["SET", "one", "1"]));
        {
            let (_copy, s) = snapshot(&path);
            assert_eq!(s.execute(&["GET", "one"]), b("1"));
        }
        // Reads are answered without the disk, and stay in order with the
        // writes around them.
        let mut pipeline = Vec::new();
        for i in 0..500 {
            pipeline.extend(wire_str(&["SET", &format!("p{i}"), &i.to_string()]));
            pipeline.extend(wire_str(&["GET", &format!("p{i}")]));
        }
        c.send_raw(&pipeline);
        for i in 0..500 {
            ok(c.read());
            assert_eq!(c.read(), b(&i.to_string()));
        }
        // Many connections at once: every answered write is in the file.
        let writers: Vec<_> = (0..8)
            .map(|t| {
                thread::spawn(move || {
                    let mut c = Client::connect(addr);
                    for i in 0..150 {
                        ok(c.cmd(&["SET", &format!("t{t}:{i}"), "x"]));
                        assert!(matches!(c.cmd(&["INCR", "shared"]), Reply::Int(_)));
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        // A waiter's reply is a write too.
        let mut waiter = Client::connect(addr);
        waiter.send(&["BLPOP", "queue", "0"]);
        assert!(waiter.silent_for(150));
        assert_eq!(c.cmd(&["RPUSH", "queue", "job"]), Reply::Int(1));
        assert_eq!(waiter.read(), Reply::Array(vec![b("queue"), b("job")]));
        ok(c.cmd(&["MULTI"]));
        c.cmd(&["SET", "in-txn", "1"]);
        c.cmd(&["EXEC"]);
        assert_eq!(
            c.cmd(&["EVAL", "return redis.call('INCR', KEYS[1])", "1", "lua"]),
            Reply::Int(1)
        );
        let (_copy, s) = snapshot(&path);
        assert_eq!(s.execute(&["GET", "shared"]), b("1200"));
        assert_eq!(s.execute(&["LLEN", "queue"]), Reply::Int(0));
        assert_eq!(s.execute(&["GET", "in-txn"]), b("1"));
        assert_eq!(s.execute(&["GET", "lua"]), b("1"));
        assert_eq!(s.execute(&["GET", "p499"]), b("499"));
        let mut missing = 0;
        for t in 0..8 {
            for i in 0..150 {
                missing += 1 - int(s.execute(&["EXISTS", &format!("t{t}:{i}")]));
            }
        }
        assert_eq!(missing, 0);
    });
}

#[test]
fn save_and_a_runtime_switch_to_always_work_over_tcp() {
    watchdog("save-tcp", 60, || {
        let path = fresh("save-tcp");
        let (db, _s) = open(&path, lazy(Fsync::Interval));
        let server = db.listen(server_cfg(2, 64)).unwrap();
        let mut c = Client::connect(server.local_addr());
        ok(c.cmd(&["SET", "a", "1"]));
        {
            let (_copy, s) = snapshot(&path);
            assert_eq!(s.execute(&["EXISTS", "a"]), Reply::Int(0));
        }
        // In a pipeline, SAVE answers in its place.
        let mut pipeline = wire_str(&["SET", "b", "2"]);
        pipeline.extend(wire_str(&["SAVE"]));
        pipeline.extend(wire_str(&["GET", "b"]));
        c.send_raw(&pipeline);
        ok(c.read());
        ok(c.read());
        assert_eq!(c.read(), b("2"));
        {
            let (_copy, s) = snapshot(&path);
            assert_eq!(s.execute(&["EXISTS", "a", "b"]), Reply::Int(2));
        }
        ok(c.cmd(&["CONFIG", "SET", "appendfsync", "always"]));
        ok(c.cmd(&["SET", "c", "3"]));
        let (_copy, s) = snapshot(&path);
        assert_eq!(s.execute(&["GET", "c"]), b("3"));
    });
}

// ---------------------------------------------------------------------
// Functions and users in the data file
// ---------------------------------------------------------------------

#[test]
fn functions_come_back_after_a_restart() {
    let path = fresh("functions");
    {
        let (_db, s) = open(&path, quick());
        assert_eq!(
            s.execute(&["FUNCTION", "LOAD", &lib("one", "f1", "return 1")]),
            b("one")
        );
        assert_eq!(
            s.execute(&["FUNCTION", "LOAD", &lib("two", "f2", "return args[1]")]),
            b("two")
        );
        assert_eq!(
            s.execute(&["FUNCTION", "LOAD", &lib("gone", "f3", "return 3")]),
            b("gone")
        );
        ok(s.execute(&["FUNCTION", "DELETE", "gone"]));
        assert_eq!(
            s.execute(&[
                "FUNCTION",
                "LOAD",
                "REPLACE",
                &lib("one", "f1", "return redis.call('INCR', keys[1])")
            ]),
            b("one")
        );
    }
    {
        let (_db, s) = open(&path, quick());
        assert_eq!(s.execute(&["FCALL", "f1", "1", "n"]), Reply::Int(1));
        assert_eq!(s.execute(&["FCALL", "f2", "0", "hi"]), b("hi"));
        assert!(error(s.execute(&["FCALL", "f3", "0"])).contains("Function not found"));
        let listed = format!("{:?}", s.execute(&["FUNCTION", "LIST"]));
        assert!(!listed.contains("gone"), "{listed}");
        // A name that is taken stays taken.
        assert!(
            error(s.execute(&["FUNCTION", "LOAD", &lib("two", "f9", "return 9")]))
                .contains("already exists")
        );
        ok(s.execute(&["FUNCTION", "FLUSH"]));
        assert_eq!(
            s.execute(&["FUNCTION", "LOAD", &lib("late", "f4", "return 4")]),
            b("late")
        );
    }
    let (_db, s) = open(&path, quick());
    assert!(error(s.execute(&["FCALL", "f1", "1", "n"])).contains("Function not found"));
    assert_eq!(s.execute(&["FCALL", "f4", "0"]), Reply::Int(4));
    assert_eq!(s.execute(&["GET", "n"]), b("1"));
}

#[test]
fn an_odd_source_comes_back_byte_for_byte() {
    let path = fresh("functions-dump");
    let odd = "#!lua name=odd\nlocal odd = '\u{1F980} \\r\\t \"quotes\" \\\\ more'\r\nredis.register_function{function_name='odd', callback=function() return 'ok' end, flags={'no-writes'}}";
    let listing = |s: &Session| s.execute(&["FUNCTION", "LIST", "WITHCODE", "LIBRARYNAME", "odd"]);
    let before;
    {
        let (_db, s) = open(&path, quick());
        assert_eq!(s.execute(&["FUNCTION", "LOAD", odd]), b("odd"));
        before = listing(&s);
    }
    let (_db, s) = open(&path, quick());
    assert_eq!(s.execute(&["FCALL_RO", "odd", "0"]), b("ok"));
    // The source comes back byte for byte.
    assert_eq!(listing(&s), before);
    assert!(matches!(&before, Reply::Array(libs) if libs.len() == 1));
}

fn acl_list(s: &Session) -> Vec<String> {
    match s.execute(&["ACL", "LIST"]) {
        Reply::Array(items) => {
            let mut lines: Vec<String> = items.into_iter().map(text).collect();
            lines.sort();
            lines
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn users_come_back_after_a_restart() {
    let path = fresh("users");
    let before;
    {
        let (_db, s) = open(&path, quick());
        ok(s.execute(&[
            "ACL",
            "SETUSER",
            "alice",
            "on",
            ">wonder",
            ">second",
            "~app:*",
            "%R~ro:*",
            "&chan:*",
            "+@read",
            "+set",
            "-keys",
            "+client|id",
        ]));
        ok(s.execute(&[
            "ACL", "SETUSER", "off-user", "off", "nopass", "allkeys", "+@all",
        ]));
        ok(
            s.execute(&["ACL", "SETUSER", "weird name?", "on", ">p w", "~*", "+get"])
                .or_ok(),
        );
        ok(s.execute(&["ACL", "SETUSER", "temp", "on", ">x", "+@all"]));
        assert_eq!(s.execute(&["ACL", "DELUSER", "temp"]), Reply::Int(1));
        ok(s.execute(&[
            "ACL",
            "SETUSER",
            "sel",
            "on",
            ">s",
            "+get",
            "~a*",
            "(+set ~b*)",
        ])
        .or_ok());
        ok(s.execute(&["CONFIG", "SET", "requirepass", "front-door"]));
        before = acl_list(&s);
        assert!(before.iter().any(|l| l.starts_with("user alice on")));
    }
    let (db, s) = open(&path, quick());
    assert_eq!(acl_list(&s), before);
    // The file holds hashes; the passwords still open the door.
    let raw = fs::read(&path).unwrap();
    for secret in ["wonder", "second", "front-door"] {
        assert!(
            !raw.windows(secret.len()).any(|w| w == secret.as_bytes()),
            "{secret} is in the data file in the clear"
        );
    }
    let server = db.listen(server_cfg(2, 16)).unwrap();
    let mut c = Client::connect(server.local_addr());
    assert!(error(c.cmd(&["GET", "app:1"])).starts_with("NOAUTH"));
    assert!(error(c.cmd(&["AUTH", "wrong"])).starts_with("WRONGPASS"));
    assert!(error(c.cmd(&["AUTH", "temp", "x"])).starts_with("WRONGPASS"));
    assert!(error(c.cmd(&["AUTH", "off-user", "any"])).starts_with("WRONGPASS"));
    ok(c.cmd(&["AUTH", "alice", "second"]));
    ok(c.cmd(&["SET", "app:1", "v"]));
    assert_eq!(c.cmd(&["GET", "app:1"]), b("v"));
    assert!(error(c.cmd(&["SET", "other", "v"])).starts_with("NOPERM"));
    assert!(error(c.cmd(&["KEYS", "*"])).starts_with("NOPERM"));
    assert!(error(c.cmd(&["DEL", "app:1"])).starts_with("NOPERM"));
    ok(c.cmd(&["AUTH", "front-door"]));
    assert_eq!(c.cmd(&["DEL", "app:1"]), Reply::Int(1));
    // Taking the password away again is kept as well.
    ok(c.cmd(&["CONFIG", "SET", "requirepass", ""]));
    ok(c.cmd(&["ACL", "SETUSER", "alice", "resetpass", ">only"]));
    drop(c);
    drop(server);
    drop(s);
    drop(db);
    let (db, _s) = open(&path, quick());
    let server = db.listen(server_cfg(1, 16)).unwrap();
    let mut c = Client::connect(server.local_addr());
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    assert!(error(c.cmd(&["AUTH", "alice", "second"])).starts_with("WRONGPASS"));
    ok(c.cmd(&["AUTH", "alice", "only"]));
}

trait OrOk {
    fn or_ok(self) -> Reply;
}

impl OrOk for Reply {
    /// For rules this server may refuse: the test goes on either way, and
    /// what was accepted has to survive.
    fn or_ok(self) -> Reply {
        match self {
            Reply::Error(_) => Reply::Ok,
            other => other,
        }
    }
}

#[test]
fn functions_and_users_survive_rewrites_of_the_file() {
    let path = fresh("meta-rewrite");
    let options = PersistOptions {
        interval: Duration::from_millis(2),
        rewrite_factor: 2,
        rewrite_min_bytes: 32 << 10,
        ..PersistOptions::default()
    };
    let before;
    {
        let (db, s) = open(&path, options.clone());
        s.execute(&["FUNCTION", "LOAD", &lib("keep", "kept", "return 'kept'")]);
        s.execute(&["FUNCTION", "LOAD", &lib("drop", "dropped", "return 0")]);
        ok(s.execute(&["ACL", "SETUSER", "bob", "on", ">pw", "~b:*", "+@write"]));
        ok(s.execute(&["ACL", "SETUSER", "tmp", "on", "nopass", "+ping"]));
        let value = "v".repeat(1000);
        for round in 0..6 {
            for i in 0..2_000 {
                s.execute(&[
                    "SET",
                    &format!("k{}", i % 50),
                    &format!("{round}:{i}:{value}"),
                ]);
            }
            db.sync().unwrap();
            if round == 2 {
                // Changed between rewrites: the next one writes the new
                // state, not the old.
                ok(s.execute(&["FUNCTION", "DELETE", "drop"]));
                assert_eq!(s.execute(&["ACL", "DELUSER", "tmp"]), Reply::Int(1));
                ok(s.execute(&["ACL", "SETUSER", "bob", "+get"]));
            }
        }
        db.sync().unwrap();
        let len = fs::metadata(&path).unwrap().len();
        assert!(len < 2 << 20, "the file was never rewritten: {len} bytes");
        before = acl_list(&s);
    }
    let (_db, s) = open(&path, options);
    assert_eq!(s.execute(&["FCALL", "kept", "0"]), b("kept"));
    assert!(error(s.execute(&["FCALL", "dropped", "0"])).contains("Function not found"));
    assert_eq!(acl_list(&s), before);
    assert!(before.iter().any(|l| l.starts_with("user bob on")));
    assert!(!before.iter().any(|l| l.starts_with("user tmp ")));
    assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(50));
}

#[test]
fn a_file_cut_short_keeps_the_users_and_functions_written_before_the_cut() {
    let path = fresh("meta-torn");
    {
        let (db, s) = open(&path, quick());
        s.execute(&["FUNCTION", "LOAD", &lib("early", "early", "return 1")]);
        ok(s.execute(&["ACL", "SETUSER", "early", "on", ">pw", "+@all", "~*"]));
        db.sync().unwrap();
    }
    let whole = fs::metadata(&path).unwrap().len();
    {
        let (db, s) = open(&path, quick());
        s.execute(&["FUNCTION", "LOAD", &lib("late", "late", "return 2")]);
        ok(s.execute(&["ACL", "SETUSER", "late", "on", ">pw", "+@all", "~*"]));
        db.sync().unwrap();
    }
    let bytes = fs::read(&path).unwrap();
    // Every length between the two states opens, and holds the early
    // state at least; nothing half-read is taken for a user.
    for cut in (whole as usize..bytes.len()).step_by(7) {
        let copy = fresh("meta-torn-copy");
        fs::write(&copy, &bytes[..cut]).unwrap();
        let (_db, s) = open(&copy, quick());
        assert_eq!(
            s.execute(&["FCALL", "early", "0"]),
            Reply::Int(1),
            "cut {cut}"
        );
        let users = acl_list(&s);
        assert!(
            users.iter().any(|l| l.starts_with("user early on")),
            "cut {cut}"
        );
        for line in &users {
            assert!(
                ["user default ", "user early ", "user late "]
                    .iter()
                    .any(|p| line.starts_with(p)),
                "cut {cut}: {line}"
            );
        }
    }
}

// ---------------------------------------------------------------------
// CLIENT KILL
// ---------------------------------------------------------------------

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(3, 64))
}

fn client_id(c: &mut Client) -> String {
    int(c.cmd(&["CLIENT", "ID"])).to_string()
}

/// A field of this connection's own CLIENT INFO line.
fn info_field(c: &mut Client, field: &str) -> String {
    let line = text(c.cmd(&["CLIENT", "INFO"]));
    let prefix = format!("{field}=");
    line.split_whitespace()
        .find_map(|part| part.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("no {field} in {line}"))
        .to_string()
}

fn closed(c: &mut Client) -> bool {
    c.set_timeout(Duration::from_secs(5));
    match c.try_read() {
        Err(e) => is_closed(&e),
        Ok(reply) => panic!("expected a closed connection, got {reply:?}"),
    }
}

#[test]
fn client_kill_by_id_addr_and_the_old_form() {
    let env = env();
    let mut admin = env.client();
    let mut a = env.client();
    let mut b2 = env.client();
    let mut c = env.client();
    let (ida, addr_b, addr_c) = (
        client_id(&mut a),
        info_field(&mut b2, "addr"),
        info_field(&mut c, "addr"),
    );
    assert_eq!(admin.cmd(&["CLIENT", "KILL", "ID", &ida]), Reply::Int(1));
    assert!(closed(&mut a));
    assert_eq!(admin.cmd(&["CLIENT", "KILL", "ID", &ida]), Reply::Int(0));
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "ADDR", &addr_b]),
        Reply::Int(1)
    );
    assert!(closed(&mut b2));
    ok(admin.cmd(&["CLIENT", "KILL", &addr_c]));
    assert!(closed(&mut c));
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", &addr_c]),
        Reply::err("ERR No such client")
    );
    assert_eq!(env.wait_conns(1, 5), 1);
    // Filters combine: all of them have to fit.
    let mut d = env.client();
    let (idd, addr_d) = (client_id(&mut d), info_field(&mut d, "addr"));
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "ID", &idd, "ADDR", "1.2.3.4:5"]),
        Reply::Int(0)
    );
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "ID", &idd, "USER", "nobody"]),
        Reply::Int(0)
    );
    assert_eq!(d.cmd(&["PING"]), Reply::Simple("PONG".into()));
    assert_eq!(
        admin.cmd(&["client", "kill", "id", &idd, "addr", &addr_d, "user", "default"]),
        Reply::Int(1)
    );
    assert!(closed(&mut d));
    for bad in [
        &["CLIENT", "KILL", "ID", "abc"][..],
        &["CLIENT", "KILL", "ID", "-1"],
        &["CLIENT", "KILL", "TYPE", "alien"],
        &["CLIENT", "KILL", "SKIPME", "maybe"],
        &["CLIENT", "KILL", "MAXAGE", "-1"],
        &["CLIENT", "KILL", "NOPE", "1"],
        &["CLIENT", "KILL"],
    ] {
        assert!(error(admin.cmd(bad)).starts_with("ERR"), "{bad:?}");
    }
    assert_eq!(admin.cmd(&["PING"]), Reply::Simple("PONG".into()));
}

#[test]
fn client_kill_spares_the_caller_unless_told_otherwise() {
    let env = env();
    let mut admin = env.client();
    let mut others: Vec<Client> = (0..5).map(|_| env.client()).collect();
    for c in &mut others {
        c.cmd(&["PING"]);
    }
    let laddr = info_field(&mut admin, "laddr");
    // Everyone on this listener but the caller.
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "LADDR", &laddr]),
        Reply::Int(5)
    );
    for c in &mut others {
        assert!(closed(c));
    }
    assert_eq!(admin.cmd(&["PING"]), Reply::Simple("PONG".into()));
    let me = client_id(&mut admin);
    assert_eq!(admin.cmd(&["CLIENT", "KILL", "ID", &me]), Reply::Int(0));
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "ID", &me, "SKIPME", "yes"]),
        Reply::Int(0)
    );
    // The reply comes first, then the end, and nothing after it runs.
    let mut pipeline = wire_str(&["CLIENT", "KILL", "ID", &me, "SKIPME", "no"]);
    pipeline.extend(wire_str(&["SET", "after-death", "1"]));
    admin.send_raw(&pipeline);
    assert_eq!(admin.read(), Reply::Int(1));
    assert!(closed(&mut admin));
    assert_eq!(env.wait_conns(0, 5), 0);
    let mut check = env.client();
    assert_eq!(check.cmd(&["EXISTS", "after-death"]), Reply::Int(0));
    // The old form does not spare the caller.
    let addr = info_field(&mut check, "addr");
    check.send(&["CLIENT", "KILL", &addr]);
    ok(check.read());
    assert!(closed(&mut check));
}

#[test]
fn client_kill_by_user_type_and_age() {
    let env = env();
    let mut admin = env.client();
    ok(admin.cmd(&["ACL", "SETUSER", "worker", "on", ">pw", "+@all", "~*", "&*"]));
    let mut w1 = env.client();
    let mut w2 = env.client();
    ok(w1.cmd(&["AUTH", "worker", "pw"]));
    ok(w2.cmd(&["AUTH", "worker", "pw"]));
    let mut sub = env.client();
    sub.cmd(&["SUBSCRIBE", "news"]);
    let mut psub = env.client();
    psub.cmd(&["PSUBSCRIBE", "n*"]);
    let mut was_sub = env.client();
    was_sub.cmd(&["SUBSCRIBE", "news"]);
    was_sub.cmd(&["UNSUBSCRIBE", "news"]);
    assert_eq!(was_sub.cmd(&["PING"]), Reply::Simple("PONG".into()));

    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "TYPE", "master"]),
        Reply::Int(0)
    );
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "TYPE", "replica"]),
        Reply::Int(0)
    );
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "TYPE", "PUBSUB"]),
        Reply::Int(2)
    );
    assert!(closed(&mut sub));
    assert!(closed(&mut psub));
    assert_eq!(admin.cmd(&["PUBLISH", "news", "x"]), Reply::Int(0));
    assert_eq!(was_sub.cmd(&["PING"]), Reply::Simple("PONG".into()));

    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "USER", "worker"]),
        Reply::Int(2)
    );
    assert!(closed(&mut w1));
    assert!(closed(&mut w2));
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "USER", "worker"]),
        Reply::Int(0)
    );

    // Only the old: a connection made after the pause stays.
    thread::sleep(Duration::from_millis(2100));
    let mut young = env.client();
    young.cmd(&["PING"]);
    assert_eq!(admin.cmd(&["CLIENT", "KILL", "MAXAGE", "2"]), Reply::Int(1));
    assert!(closed(&mut was_sub));
    assert_eq!(young.cmd(&["PING"]), Reply::Simple("PONG".into()));
    assert_eq!(
        admin.cmd(&["CLIENT", "KILL", "TYPE", "normal", "MAXAGE", "0"]),
        Reply::Int(1)
    );
    assert!(closed(&mut young));
    assert_eq!(env.wait_conns(1, 5), 1);
}

#[test]
fn a_killed_client_leaves_nothing_behind() {
    watchdog("kill-cleanup", 60, || {
        let env = env();
        let mut admin = env.client();
        // Waiting for a list.
        let mut waiter = env.client();
        let id = client_id(&mut waiter);
        waiter.send(&["BLPOP", "jobs", "0"]);
        assert!(waiter.silent_for(150));
        assert_eq!(admin.cmd(&["CLIENT", "KILL", "ID", &id]), Reply::Int(1));
        assert!(closed(&mut waiter));
        assert_eq!(admin.cmd(&["RPUSH", "jobs", "j1"]), Reply::Int(1));
        assert_eq!(admin.cmd(&["LLEN", "jobs"]), Reply::Int(1));
        // In the middle of a transaction: nothing of it runs.
        let mut txn = env.client();
        let id = client_id(&mut txn);
        ok(txn.cmd(&["MULTI"]));
        txn.cmd(&["SET", "half", "1"]);
        assert_eq!(admin.cmd(&["CLIENT", "KILL", "ID", &id]), Reply::Int(1));
        assert!(closed(&mut txn));
        assert_eq!(admin.cmd(&["EXISTS", "half"]), Reply::Int(0));
        // A subscriber and a monitor.
        let mut sub = env.client();
        let id = client_id(&mut sub);
        sub.cmd(&["SUBSCRIBE", "c"]);
        assert_eq!(admin.cmd(&["CLIENT", "KILL", "ID", &id]), Reply::Int(1));
        assert!(closed(&mut sub));
        assert_eq!(admin.cmd(&["PUBLISH", "c", "m"]), Reply::Int(0));
        let mut mon = env.client();
        let id = client_id(&mut mon);
        ok(mon.cmd(&["MONITOR"]));
        assert_eq!(admin.cmd(&["CLIENT", "KILL", "ID", &id]), Reply::Int(1));
        // Lines it was sent before the end may still arrive.
        mon.read_until_closed();
        // A client with megabytes of unread output.
        ok(admin.cmd(&["SET", "big", &"x".repeat(1 << 20)]));
        let mut hog = env.client();
        let id = client_id(&mut hog);
        let mut pipeline = Vec::new();
        for _ in 0..64 {
            pipeline.extend(wire_str(&["GET", "big"]));
        }
        hog.send_raw(&pipeline);
        thread::sleep(Duration::from_millis(200));
        assert_eq!(admin.cmd(&["CLIENT", "KILL", "ID", &id]), Reply::Int(1));
        assert_eq!(env.wait_conns(1, 10), 1);
        let listed = text(admin.cmd(&["CLIENT", "LIST"]));
        assert_eq!(listed.lines().count(), 1, "{listed}");
    });
}

#[test]
fn client_kill_needs_permission_and_races_cleanly() {
    watchdog("kill-race", 120, || {
        let env = env();
        let mut admin = env.client();
        ok(admin.cmd(&[
            "ACL",
            "SETUSER",
            "app",
            "on",
            ">pw",
            "+@all",
            "-@dangerous",
            "~*",
        ]));
        let mut app = env.client();
        ok(app.cmd(&["AUTH", "app", "pw"]));
        let victim = client_id(&mut admin);
        assert!(error(app.cmd(&["CLIENT", "KILL", "ID", &victim])).starts_with("NOPERM"));
        assert_eq!(admin.cmd(&["PING"]), Reply::Simple("PONG".into()));
        drop(app);
        // Connections come and go while two killers shoot at everything:
        // no hang, no crash, and the count comes back to the survivors.
        let addr = env.addr;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let churn: Vec<_> = (0..4)
            .map(|_| {
                let stop = stop.clone();
                thread::spawn(move || {
                    while !stop.load(Ordering::SeqCst) {
                        let mut c = Client::with_timeout(addr, Duration::from_secs(5));
                        for _ in 0..20 {
                            c.send(&["INCR", "n"]);
                            if c.try_read().is_err() {
                                break;
                            }
                        }
                    }
                })
            })
            .collect();
        let killers: Vec<_> = (0..2)
            .map(|_| {
                thread::spawn(move || {
                    let mut k = Client::connect(addr);
                    let mut killed = 0;
                    let end = Instant::now() + Duration::from_secs(3);
                    while Instant::now() < end {
                        killed += int(k.cmd(&["CLIENT", "KILL", "TYPE", "normal"]));
                    }
                    killed
                })
            })
            .collect();
        let mut total = 0;
        for k in killers {
            // A killer may be shot by the other one; that is a panic in
            // its thread and fine, as long as someone killed something.
            total += k.join().unwrap_or(1);
        }
        stop.store(true, Ordering::SeqCst);
        for c in churn {
            c.join().unwrap();
        }
        assert!(total > 0);
        assert_eq!(env.wait_conns(0, 10), 0);
        let mut c = env.client();
        assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    });
}

// ---------------------------------------------------------------------
// Connections the server gives up on
// ---------------------------------------------------------------------

fn timeouts(idle: Option<u64>, stall: u64) -> ServerConfig {
    ServerConfig {
        idle_timeout: idle.map(Duration::from_secs),
        stall_timeout: Duration::from_secs(stall),
        ..server_cfg(2, 64)
    }
}

#[test]
fn a_silent_connection_is_closed_after_the_timeout() {
    watchdog("idle", 60, || {
        let env = Env::new(engine_cfg(2), timeouts(Some(1), 60));
        let mut idle = env.client();
        assert_eq!(
            idle.cmd(&["CONFIG", "GET", "timeout"]),
            Reply::Array(vec![b("timeout"), b("1")])
        );
        let mut busy = env.client();
        let mut sub = env.client();
        sub.cmd(&["SUBSCRIBE", "c"]);
        let mut waiter = env.client();
        waiter.send(&["BLPOP", "never", "0"]);
        let mut monitor = env.client();
        ok(monitor.cmd(&["MONITOR"]));
        let mut in_txn = env.client();
        ok(in_txn.cmd(&["MULTI"]));
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(3500) {
            assert_eq!(busy.cmd(&["PING"]), Reply::Simple("PONG".into()));
            thread::sleep(Duration::from_millis(200));
        }
        assert!(closed(&mut idle));
        // A transaction left open is no excuse.
        assert!(closed(&mut in_txn));
        // Those that are silent by design stay.
        assert_eq!(busy.cmd(&["PUBLISH", "c", "hello"]), Reply::Int(1));
        assert_eq!(
            sub.read(),
            Reply::Array(vec![b("message"), b("c"), b("hello")])
        );
        assert_eq!(busy.cmd(&["RPUSH", "never", "x"]), Reply::Int(1));
        assert_eq!(waiter.read(), Reply::Array(vec![b("never"), b("x")]));
        assert!(matches!(monitor.read(), Reply::Simple(_)));
        // Once it has its answer, the waiter is an ordinary connection.
        thread::sleep(Duration::from_millis(100));
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(3500) {
            assert_eq!(busy.cmd(&["PING"]), Reply::Simple("PONG".into()));
            thread::sleep(Duration::from_millis(200));
        }
        assert!(closed(&mut waiter));
    });
}

#[test]
fn the_timeout_can_be_changed_and_switched_off() {
    watchdog("idle-config", 60, || {
        let env = Env::new(engine_cfg(2), timeouts(None, 60));
        let mut c = env.client();
        assert_eq!(
            c.cmd(&["CONFIG", "GET", "timeout"]),
            Reply::Array(vec![b("timeout"), b("0")])
        );
        thread::sleep(Duration::from_millis(2500));
        assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
        for bad in ["-1", "x", "", "1.5"] {
            assert!(error(c.cmd(&["CONFIG", "SET", "timeout", bad])).starts_with("ERR"));
        }
        ok(c.cmd(&["CONFIG", "SET", "timeout", "1"]));
        let mut other = env.client();
        thread::sleep(Duration::from_millis(3200));
        assert!(closed(&mut c));
        assert!(closed(&mut other));
        let mut c = env.client();
        ok(c.cmd(&["CONFIG", "SET", "timeout", "0"]));
        thread::sleep(Duration::from_millis(2500));
        assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    });
}

#[test]
fn a_client_that_does_not_read_is_closed() {
    watchdog("stall", 90, || {
        let env = Env::new(engine_cfg(2), timeouts(None, 2));
        let mut admin = env.client();
        ok(admin.cmd(&["SET", "big", &"x".repeat(256 << 10)]));
        // Asks for far more than the socket buffers hold, reads nothing.
        let hog = connect_stream(env.addr);
        let mut pipeline = Vec::new();
        for _ in 0..400 {
            pipeline.extend(wire_str(&["GET", "big"]));
        }
        let mut wr = hog.try_clone().unwrap();
        let writer = thread::spawn(move || {
            let _ = wr.write_all(&pipeline);
        });
        // One that reads slowly, but reads: it gets everything.
        let mut slow = env.client();
        let mut pipeline = Vec::new();
        for _ in 0..40 {
            pipeline.extend(wire_str(&["GET", "big"]));
        }
        slow.send_raw(&pipeline);
        for _ in 0..40 {
            thread::sleep(Duration::from_millis(150));
            match slow.read() {
                Reply::Bulk(v) => assert_eq!(v.len(), 256 << 10),
                other => panic!("{}", short(&other)),
            }
        }
        assert_eq!(env.wait_conns(2, 15), 2, "the hog is still connected");
        drop(hog);
        writer.join().unwrap();
        assert_eq!(admin.cmd(&["PING"]), Reply::Simple("PONG".into()));
        assert_eq!(slow.cmd(&["PING"]), Reply::Simple("PONG".into()));
    });
}

#[test]
fn a_subscriber_that_does_not_read_is_closed_too() {
    watchdog("stall-sub", 90, || {
        let env = Env::new(engine_cfg(2), timeouts(None, 2));
        let mut publisher = env.client();
        let mut sub = env.client();
        sub.cmd(&["SUBSCRIBE", "flood"]);
        let payload = "p".repeat(64 << 10);
        // About 3 MB a second until the subscriber is gone. However much
        // the kernel buffers take first (some Windows hosts take more than
        // 13 MB), output then waits in the server for the 2 s stall timeout
        // long before it reaches the 32 MB that drops a subscriber outright.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let mut receivers = Reply::Int(1);
            for _ in 0..16 {
                receivers = publisher.cmd(&["PUBLISH", "flood", &payload]);
            }
            if receivers == Reply::Int(0) {
                break;
            }
            assert_eq!(receivers, Reply::Int(1));
            assert!(Instant::now() < deadline, "the subscriber is never closed");
            thread::sleep(Duration::from_millis(300));
        }
        assert_eq!(
            env.wait_conns(1, 15),
            1,
            "the subscriber is still connected"
        );
        assert_eq!(publisher.cmd(&["PUBLISH", "flood", "x"]), Reply::Int(0));
    });
}

// ---------------------------------------------------------------------
// Found by review
// ---------------------------------------------------------------------

#[test]
fn a_reply_that_waits_for_the_disk_does_not_freeze_its_worker() {
    watchdog("always-script-kill", 60, || {
        let path = fresh("always-kill");
        let (db, _s) = open(&path, lazy(Fsync::Always));
        // One worker: whatever stops it stops everyone.
        let server = db.listen(server_cfg(1, 64)).unwrap();
        let addr = server.local_addr();
        let mut script = Client::connect(addr);
        let mut writer = Client::connect(addr);
        let mut admin = Client::connect(addr);
        assert_eq!(admin.cmd(&["PING"]), Reply::Simple("PONG".into()));
        // A script that has written cannot be killed, so this one does
        // not write, and that it runs shows in what follows: a reply
        // that is held back.
        let forever = "while true do end";
        script.send(&["EVAL", forever, "0"]);
        thread::sleep(Duration::from_millis(300));
        // A change whose reply has to wait for the file, which cannot be
        // written while the script runs.
        writer.send(&["ACL", "SETUSER", "u", "on", "nopass"]);
        assert!(writer.silent_for(300));
        // The way out still works, on the same worker.
        assert_eq!(admin.cmd(&["PING"]), Reply::Simple("PONG".into()));
        ok(admin.cmd(&["SCRIPT", "KILL"]));
        assert!(error(script.read()).contains("killed"));
        ok(writer.read());
        ok(admin.cmd(&["SET", "after", "1"]));
        // And a server with a script running and replies waiting stops.
        script.send(&["EVAL", forever, "0"]);
        thread::sleep(Duration::from_millis(300));
        writer.send(&["ACL", "SETUSER", "u2", "on", "nopass"]);
        assert!(writer.silent_for(300));
        drop(server);
    });
}

#[test]
fn always_switched_on_inside_a_pipeline_covers_what_follows_it() {
    watchdog("always-mid-pipeline", 60, || {
        let path = fresh("always-mid");
        let (db, _s) = open(&path, lazy(Fsync::Interval));
        let server = db.listen(server_cfg(2, 64)).unwrap();
        let mut c = Client::connect(server.local_addr());
        let mut pipeline = wire_str(&["SET", "early", "1"]);
        pipeline.extend(wire_str(&["CONFIG", "SET", "appendfsync", "always"]));
        pipeline.extend(wire_str(&["SET", "late", "1"]));
        c.send_raw(&pipeline);
        for _ in 0..3 {
            ok(c.read());
        }
        let (_copy, s) = snapshot(&path);
        assert_eq!(s.execute(&["EXISTS", "early", "late"]), Reply::Int(2));
    });
}

#[test]
fn a_pattern_for_every_key_does_not_come_back_as_more() {
    let path = fresh("users-rw");
    // What each user may and may not do, as the server answers it.
    let probe = |db: &Db| {
        let server = db.listen(server_cfg(1, 16)).unwrap();
        let mut c = Client::connect(server.local_addr());
        let mut got = Vec::new();
        for user in ["rw", "w"] {
            ok(c.cmd(&["AUTH", user, "pw"]));
            for cmd in [
                &["SORT", "list", "BY", "weight_*"][..],
                &["LPUSH", "w:1", "v"],
                &["LRANGE", "w:1", "0", "-1"],
                &["LPUSH", "r:1", "v"],
                &["LRANGE", "r:1", "0", "-1"],
                &["CLIENT", "TRACKING", "ON", "BCAST"],
                &["CLIENT", "TRACKING", "OFF"],
            ] {
                got.push(format!("{user} {cmd:?} -> {:?}", c.cmd(cmd)));
            }
        }
        got
    };
    let (before, answers);
    {
        let (db, s) = open(&path, quick());
        ok(s.execute(&["ACL", "SETUSER", "rw", "on", ">pw", "%RW~*", "+@all", "&*"]));
        ok(s.execute(&[
            "ACL", "SETUSER", "w", "on", ">pw", "%W~w:*", "%R~r:*", "+@all",
        ]));
        s.execute(&["RPUSH", "list", "1", "2"]);
        before = acl_list(&s);
        answers = probe(&db);
        assert!(
            answers.iter().any(|line| line.contains("NOPERM")),
            "{answers:#?}"
        );
        s.execute(&["DEL", "w:1", "r:1"]);
    }
    let (db, s) = open(&path, quick());
    assert_eq!(acl_list(&s), before);
    assert_eq!(probe(&db), answers);
}

#[test]
fn client_kill_is_not_for_scripts() {
    let env = env();
    let mut c = env.client();
    let mut other = env.client();
    let r = c.cmd(&[
        "EVAL",
        "return redis.pcall('CLIENT', 'KILL', 'TYPE', 'normal', 'SKIPME', 'no')",
        "0",
    ]);
    assert!(error(r).contains("not allowed from script"));
    assert_eq!(other.cmd(&["PING"]), Reply::Simple("PONG".into()));
}
