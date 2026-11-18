//! JSON documents (JSON.*) as a key type of the server: the type, the
//! errors on other types, deadlines, transactions, DUMP, RESTORE, RDB
//! files, the data file, settings, key positions, replicas that end with
//! the same documents as their primary, and the keyspace events. What the
//! commands answer is checked against valkey-json's own tests in
//! `json_upstream.rs`.

mod net;

use hotstash::{Config, Db, PersistOptions, Reply, Session};
use net::{engine_cfg, server_cfg, wait_until, Client, Env};
use std::time::{Duration, Instant};

fn fresh() -> (Db, Session) {
    let db = Db::open(
        None,
        Config {
            shards: 4,
            ..Config::default()
        },
    )
    .unwrap();
    let s = db.session().unwrap();
    (db, s)
}

fn int(n: i64) -> Reply {
    Reply::Int(n)
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn err(s: &str) -> Reply {
    Reply::Error(s.into())
}

fn bytes(reply: Reply) -> Vec<u8> {
    match reply {
        Reply::Bulk(b) => b,
        other => panic!("not a bulk string: {other:?}"),
    }
}

fn items(reply: Reply) -> Vec<Reply> {
    match reply {
        Reply::Array(items) => items,
        other => panic!("not an array: {other:?}"),
    }
}

/// Run `(arguments, expected reply)` in order.
fn check(s: &Session, vectors: &[(&[&str], Reply)]) {
    for (args, want) in vectors {
        assert_eq!(&s.execute(args), want, "{args:?}");
    }
}

const NOT_A_DOCUMENT: &str = "WRONGTYPE Not a JSON document key";
const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

#[test]
fn a_document_is_a_key_of_its_own_type() {
    let (_db, s) = fresh();
    check(
        &s,
        &[
            (
                &["JSON.SET", "doc", ".", r#"{"a":[1,2],"b":"x"}"#],
                Reply::Ok,
            ),
            (&["TYPE", "doc"], Reply::Simple("ReJSON-RL".into())),
            (&["OBJECT", "ENCODING", "doc"], b("raw")),
            (&["LPUSH", "doc", "x"], err(WRONGTYPE)),
            (&["SET", "s", "v"], Reply::Ok),
            (&["JSON.GET", "s"], err(NOT_A_DOCUMENT)),
            (&["JSON.SET", "s", ".", "1"], err(NOT_A_DOCUMENT)),
            (&["JSON.DEL", "s"], err(NOT_A_DOCUMENT)),
            (&["JSON.MGET", "doc", "s", "."], err(NOT_A_DOCUMENT)),
            (
                &["JSON.MGET", "doc", "none", "$.b"],
                Reply::Array(vec![b(r#"["x"]"#), Reply::Nil]),
            ),
            (
                &["JSON.MSET", "doc", "$.c", "1", "s", ".", "1"],
                err(NOT_A_DOCUMENT),
            ),
            // The check of every key comes before any is written.
            (&["JSON.GET", "doc", "$.c"], b("[]")),
            (&["JSON.GET", "none"], Reply::Nil),
            (
                &["JSON.ARRINDEX", "none", ".", "1"],
                err("NONEXISTENT Document key does not exist"),
            ),
            (
                &["JSON.ARRAPPEND", "none", ".", "1"],
                err("NONEXISTENT Document key does not exist"),
            ),
            (&["JSON.DEL", "doc", "$.a[0]"], int(1)),
            (&["JSON.GET", "doc"], b(r#"{"a":[2],"b":"x"}"#)),
            (&["JSON.DEL", "doc"], int(1)),
            (&["EXISTS", "doc"], int(0)),
        ],
    );
}

#[test]
fn setting_the_root_drops_the_deadline_and_a_path_keeps_it() {
    let (_db, s) = fresh();
    check(
        &s,
        &[
            (&["JSON.SET", "k", "$", r#"{"a":1}"#], Reply::Ok),
            (&["EXPIRE", "k", "100"], int(1)),
            (&["JSON.SET", "k", "$.a", "2"], Reply::Ok),
            (&["JSON.NUMINCRBY", "k", "$.a", "1"], b("[3]")),
            (&["TTL", "k"], int(100)),
            (&["JSON.SET", "k", ".", r#"{"a":0}"#], Reply::Ok),
            (&["TTL", "k"], int(-1)),
            (&["EXPIRE", "k", "100"], int(1)),
            (&["JSON.MERGE", "k", "$", r#"{"b":1}"#], Reply::Ok),
            (&["TTL", "k"], int(100)),
            // MSET at the root replaces the document too.
            (&["JSON.MSET", "k", "$", "[]"], Reply::Ok),
            (&["TTL", "k"], int(-1)),
        ],
    );
}

#[test]
fn errors_in_the_arguments_come_when_the_transaction_runs() {
    let (_db, s) = fresh();
    // Too few arguments for the server: refused when queued.
    check(
        &s,
        &[
            (&["MULTI"], Reply::Ok),
            (
                &["JSON.SET", "k", "."],
                err("ERR wrong number of arguments for 'JSON.SET' command"),
            ),
            (
                &["EXEC"],
                err("EXECABORT Transaction discarded because of previous errors."),
            ),
        ],
    );
    // What the module itself refuses: queued, then the error in EXEC.
    let queued = Reply::Simple("QUEUED".into());
    check(
        &s,
        &[
            (&["MULTI"], Reply::Ok),
            (&["JSON.SET", "k", ".", "[1]"], queued.clone()),
            (&["JSON.SET", "k", ".", "1", "NX", "extra"], queued.clone()),
            (&["JSON.ARRPOP", "k", ".", "x"], queued.clone()),
            (&["JSON.DEBUG", "HELP"], queued.clone()),
            (&["JSON.ARRAPPEND", "k", ".", "2"], queued),
        ],
    );
    let reply = items(s.execute(&["EXEC"]));
    assert_eq!(reply[0], Reply::Ok);
    assert_eq!(
        reply[1],
        err("ERR wrong number of arguments for 'JSON.SET' command")
    );
    assert_eq!(reply[2], err("WRONGTYPE Value is not an integer"));
    assert_eq!(items(reply[3].clone()).len(), 4);
    assert_eq!(reply[4], int(2));
    assert_eq!(
        s.execute(&["JSON.DEBUG", "DEPTH"]),
        err("ERR wrong number of arguments for 'JSON.DEBUG|DEPTH' command")
    );
}

/// Documents of every kind of value, one with a deadline.
fn filled(s: &Session) {
    let docs = [
        ("scalar", "3.25"),
        ("string", r#""a \"quoted\" é string""#),
        ("array", r#"[1,-2.5e-3,true,false,null,"x",[],{}]"#),
        (
            "nested",
            r#"{"store":{"book":[{"title":"A","price":8.95},{"title":"B","price":12.99}],"open":true}}"#,
        ),
        ("timed", r#"{"n":1}"#),
    ];
    for (key, json) in docs {
        assert_eq!(s.execute(&["JSON.SET", key, ".", json]), Reply::Ok, "{key}");
    }
    assert_eq!(s.execute(&["PEXPIREAT", "timed", "4102444800000"]), int(1));
}

/// What every document of `filled` answers.
fn views(s: &Session) -> Vec<Reply> {
    let mut out = Vec::new();
    for key in ["scalar", "string", "array", "nested", "timed"] {
        out.push(s.execute(&["DUMP", key]));
        out.push(s.execute(&["JSON.GET", key]));
        out.push(s.execute(&["JSON.TYPE", key, "$..*"]));
    }
    out.push(s.execute(&["PEXPIRETIME", "timed"]));
    out
}

#[test]
fn dump_restore_and_rdb_files_keep_every_document() {
    let (source, s) = fresh();
    filled(&s);
    let before = views(&s);
    // A DUMP payload: module type 7, then valkey-json's module ID
    // (`ReJSON-RL`, encoding version 3), then the document as one string.
    let payload = bytes(s.execute(&["DUMP", "nested"]));
    assert_eq!(payload[0], 7);
    assert_eq!(payload[1], 0x81);
    assert_eq!(payload[2..10], 0x45e2_5238_df91_2c03u64.to_be_bytes());
    let json = bytes(s.execute(&["JSON.GET", "nested"]));
    assert!(payload.windows(json.len()).any(|w| w == json.as_slice()));
    let args: [&[u8]; 4] = [b"RESTORE", b"copy", b"0", &payload];
    assert_eq!(s.execute(&args), Reply::Ok);
    assert_eq!(bytes(s.execute(&["DUMP", "copy"])), payload);
    assert_eq!(
        s.execute(&["JSON.GET", "copy", "$..price"]),
        b("[8.95,12.99]")
    );
    // A payload cut short is refused.
    let args: [&[u8]; 4] = [b"RESTORE", b"cut", b"0", &payload[..payload.len() - 12]];
    assert!(matches!(s.execute(&args), Reply::Error(_)));

    let file = source.save_rdb();
    let (copy, t) = fresh();
    copy.load_rdb(&file[..]).unwrap();
    assert_eq!(views(&t), before);
}

#[test]
fn the_data_file_keeps_every_document() {
    let dir = std::env::temp_dir().join(format!("hotstash-json-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("json.hc");
    let _ = std::fs::remove_file(&path);
    let open = || {
        let db = Db::open_with(
            Some(&path),
            Config {
                shards: 4,
                ..Config::default()
            },
            PersistOptions {
                interval: Duration::from_millis(5),
                ..PersistOptions::default()
            },
        )
        .unwrap();
        let s = db.session().unwrap();
        (db, s)
    };
    let before = {
        let (_db, s) = open();
        filled(&s);
        assert_eq!(s.execute(&["JSON.ARRAPPEND", "array", ".", "9"]), int(9));
        views(&s)
    };
    let (_db, s) = open();
    assert_eq!(views(&s), before);
    drop((_db, s));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn settings_are_shown_checked_and_used() {
    let (_db, s) = fresh();
    let shown = |s: &Session| s.execute(&["CONFIG", "GET", "json.*"]);
    assert_eq!(
        shown(&s),
        Reply::Array(vec![
            b("json.max-document-size"),
            b("0"),
            b("json.max-path-limit"),
            b("128"),
        ])
    );
    let refused = |name: &str, why: &str| {
        err(&format!(
            "ERR CONFIG SET failed (possibly related to argument '{name}') - {why}"
        ))
    };
    check(
        &s,
        &[
            (
                &["CONFIG", "SET", "json.max-path-limit", "-1"],
                refused(
                    "json.max-path-limit",
                    "argument must be between 0 and 2147483647 inclusive",
                ),
            ),
            (
                &["CONFIG", "SET", "json.max-path-limit", "x"],
                refused(
                    "json.max-path-limit",
                    "argument couldn't be parsed into an integer",
                ),
            ),
            (
                &["CONFIG", "SET", "json.max-document-size", "lots"],
                refused("json.max-document-size", "argument must be a memory value"),
            ),
            // None is set when one is refused.
            (
                &[
                    "CONFIG",
                    "SET",
                    "json.max-path-limit",
                    "3",
                    "json.max-document-size",
                    "x",
                ],
                refused("json.max-document-size", "argument must be a memory value"),
            ),
            (&["CONFIG", "GET", "json.max-path-limit"], {
                Reply::Array(vec![b("json.max-path-limit"), b("128")])
            }),
            (
                &[
                    "CONFIG",
                    "SET",
                    "json.max-path-limit",
                    "3",
                    "json.max-document-size",
                    "1kb",
                ],
                Reply::Ok,
            ),
            (&["CONFIG", "GET", "json.max-document-size"], {
                Reply::Array(vec![b("json.max-document-size"), b("1024")])
            }),
            (&["JSON.SET", "k", ".", "[[[1]]]"], Reply::Ok),
            (
                &["JSON.SET", "k", ".", "[[[[1]]]]"],
                err("LIMIT Document path nesting limit is exceeded"),
            ),
            (
                &["JSON.SET", "k", "[0][0][0]", "[1]"],
                err("LIMIT Document path nesting limit is exceeded"),
            ),
            (
                &["JSON.ARRAPPEND", "k", "[0]", "[[[1]]]"],
                err("LIMIT Document path nesting limit is exceeded"),
            ),
            (&["JSON.GET", "k"], b("[[[1]]]")),
        ],
    );
    let big = format!("[\"{}\"]", "x".repeat(2000));
    check(
        &s,
        &[
            (
                &["JSON.SET", "big", ".", &big],
                err("LIMIT Document size limit is exceeded"),
            ),
            (&["EXISTS", "big"], int(0)),
            (&["CONFIG", "SET", "json.max-document-size", "0"], Reply::Ok),
            (&["JSON.SET", "big", ".", &big], Reply::Ok),
        ],
    );
}

#[test]
fn key_positions_and_categories_are_valkey_jsons() {
    let (_db, s) = fresh();
    let keys = |args: &[&str]| {
        let mut all = vec!["COMMAND", "GETKEYS"];
        all.extend_from_slice(args);
        s.execute(&all)
    };
    assert_eq!(
        keys(&["JSON.MSET", "a", "$", "1", "b", ".", "2"]),
        Reply::Array(vec![b("a"), b("b")])
    );
    assert_eq!(
        keys(&["JSON.MGET", "a", "b", "c", "$"]),
        Reply::Array(vec![b("a"), b("b"), b("c")])
    );
    assert_eq!(
        keys(&["JSON.GET", "a", "$", "$.x"]),
        Reply::Array(vec![b("a")])
    );
    let info = items(s.execute(&["COMMAND", "INFO", "json.mset"]));
    let info = items(info[0].clone());
    assert_eq!(
        info[1..6],
        [int(-4), info[2].clone(), int(1), int(-3), int(3)]
    );
    let Reply::Array(cats) = s.execute(&["ACL", "CAT", "json"]) else {
        panic!("ACL CAT json")
    };
    assert_eq!(cats.len(), 24);
    assert!(cats.contains(&b("json.set")));
    assert!(cats.contains(&b("json.debug")));
}

// ---- over the network ----

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn field(c: &mut Client, section: &str, name: &str) -> Option<String> {
    let info = String::from_utf8(bytes(c.cmd(&["INFO", section]))).unwrap();
    let prefix = format!("{name}:");
    info.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()).map(str::to_string))
}

fn offset(c: &mut Client) -> u64 {
    field(c, "replication", "master_repl_offset")
        .and_then(|n| n.parse().ok())
        .expect("master_repl_offset")
}

#[test]
fn a_replica_ends_with_the_same_documents() {
    let primary = env();
    let replica = env();
    let mut p = primary.client();
    let mut r = replica.client();
    // The size limit holds on the primary only: the replica takes what it
    // is sent.
    assert_eq!(
        r.cmd(&["CONFIG", "SET", "json.max-document-size", "10"]),
        Reply::Ok
    );
    let port = primary.addr.port().to_string();
    assert_eq!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]), Reply::Ok);
    let up = || field(&mut replica.client(), "replication", "master_link_status");
    assert!(wait_until(15, || up().as_deref() == Some("up")), "no link");
    let commands: &[&[&str]] = &[
        &[
            "JSON.SET",
            "a",
            ".",
            r#"{"n":1,"s":"x","l":[1,2,3],"o":{"b":true}}"#,
        ],
        &["JSON.NUMINCRBY", "a", "$..n", "2.5"],
        &["JSON.NUMMULTBY", "a", ".n", "4"],
        &["JSON.STRAPPEND", "a", ".s", r#""yz""#],
        &["JSON.TOGGLE", "a", "$.o.b"],
        &["JSON.ARRAPPEND", "a", "$.l", "4", r#"{"k":[]}"#],
        &["JSON.ARRINSERT", "a", ".l", "0", "0"],
        &["JSON.ARRPOP", "a", ".l", "2"],
        &["JSON.ARRTRIM", "a", ".l", "1", "3"],
        &[
            "JSON.MERGE",
            "a",
            "$",
            r#"{"o":{"c":null,"d":[1]},"s":null}"#,
        ],
        &["JSON.SET", "a", "$.new", r#""v""#],
        &["JSON.CLEAR", "a", "$.o"],
        &[
            "JSON.MSET",
            "b",
            "$",
            "[]",
            "a",
            "$.m",
            "1",
            "c",
            ".",
            r#"{"x":1}"#,
        ],
        &["JSON.DEL", "c", "$.x"],
        &["JSON.SET", "gone", ".", "1"],
        &["JSON.FORGET", "gone"],
        // Refused or doing nothing: never sent.
        &["JSON.SET", "a", "$.n", "1", "NX"],
        &["JSON.NUMMULTBY", "a", "$.n", "1e308"],
        &["JSON.ARRINSERT", "a", ".l", "99", "1"],
        &["JSON.DEL", "a", ".missing"],
    ];
    for args in commands {
        p.cmd(args);
    }
    // In a transaction and in a script too.
    assert_eq!(p.cmd(&["MULTI"]), Reply::Ok);
    p.cmd(&["JSON.SET", "tx", ".", "[]"]);
    p.cmd(&["JSON.ARRAPPEND", "tx", ".", "1", "2"]);
    p.cmd(&["JSON.ARRPOP", "tx", ".", "x"]);
    assert!(matches!(p.cmd(&["EXEC"]), Reply::Array(r) if r.len() == 3));
    let script = "redis.call('JSON.SET', KEYS[1], '.', '{}'); \
                  return redis.call('JSON.SET', KEYS[1], '$.k', '1')";
    assert_eq!(p.cmd(&["EVAL", script, "1", "lua"]), Reply::Ok);
    let synced = wait_until(15, || offset(&mut p) == offset(&mut r));
    assert!(synced, "the replica did not catch up");
    for key in ["a", "b", "c", "tx", "lua"] {
        let dump = |c: &mut Client| bytes(c.cmd(&["DUMP", key]));
        assert_eq!(dump(&mut p), dump(&mut r), "{key}");
    }
    assert_eq!(r.cmd(&["EXISTS", "gone"]), int(0));
    assert_eq!(r.cmd(&["JSON.GET", "tx"]), b("[1,2]"));
    assert!(matches!(
        r.cmd(&["JSON.SET", "a", "$.w", "1"]),
        Reply::Error(e) if e.starts_with("READONLY")
    ));
}

#[test]
fn changes_raise_their_events_and_nothing_else_does() {
    let env = env();
    let mut c = env.client();
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        Reply::Ok
    );
    let mut sub = env.client();
    sub.cmd(&["PSUBSCRIBE", "__keyevent@*"]);
    let commands: &[&[&str]] = &[
        &["JSON.SET", "n1", ".", r#"{"a":1,"l":[1]}"#],
        &["JSON.SET", "n1", ".a", "2", "NX"],
        &["JSON.NUMINCRBY", "n1", ".a", "1"],
        &["JSON.NUMINCRBY", "n1", ".l", "1"],
        &["JSON.DEL", "n1", ".zz"],
        &["JSON.MSET", "n1", ".a", "5", "n2", ".", "[]"],
        &["JSON.ARRAPPEND", "n2", ".", "1"],
        &["JSON.ARRPOP", "n1", ".l"],
        &["JSON.STRAPPEND", "n1", ".a", r#""x""#],
        &["JSON.MERGE", "n3", ".", r#"{"x":1}"#],
        &["JSON.TOGGLE", "n3", "$.x"],
        &["JSON.CLEAR", "n3"],
        &["JSON.GET", "n1"],
        &["JSON.DEL", "n2"],
        &["SET", "end", "1"],
    ];
    for args in commands {
        c.cmd(args);
    }
    let mut seen = Vec::new();
    loop {
        let message = items(sub.read());
        let channel = String::from_utf8(bytes(message[2].clone())).unwrap();
        let key = String::from_utf8(bytes(message[3].clone())).unwrap();
        let event = channel.rsplit_once("__:").unwrap().1.to_string();
        if key == "end" {
            break;
        }
        seen.push(format!("{event}:{key}"));
    }
    assert_eq!(
        seen,
        [
            "json.set:n1",
            "json.numincrby:n1",
            "json.mset:n1",
            "json.mset:n2",
            "json.arrappend:n2",
            "json.arrpop:n1",
            "json.merge:n3",
            "json.toggle:n3",
            "json.clear:n3",
            "json.del:n2",
        ]
    );
}

/// What the review of wave 3 found: a path that walks for hours, a pretty
/// print too big for memory, numbers that become NaN (the data file could
/// not read them back), and one value written at every match of a path.
#[test]
fn costly_paths_huge_replies_nan_and_copies_are_refused() {
    let (_db, s) = fresh();
    // 100 objects deep: each `..` walks again from every value the one
    // before it found, so eight of them would take hours.
    let deep = format!("{}1{}", r#"{"a":"#.repeat(100), "}".repeat(100));
    assert_eq!(s.execute(&["JSON.SET", "deep", "$", &deep]), Reply::Ok);
    let started = Instant::now();
    let limit = err("LIMIT Path search work limit is exceeded");
    assert_eq!(
        s.execute(&["JSON.GET", "deep", "$..a..a..a..a..a..a..a..a"]),
        limit
    );
    // A `$..` in a filter shares the budget of the path it is in.
    assert_eq!(
        s.execute(&["JSON.GET", "deep", "$..[?(@.a==$..a..a..a..a..a..a..a)]"]),
        limit
    );
    assert!(started.elapsed() < Duration::from_secs(60));
    // Paths that cost what the document is worth still run.
    assert_eq!(items(s.execute(&["JSON.TYPE", "deep", "$..a"])).len(), 100);
    assert!(!bytes(s.execute(&["JSON.GET", "deep", "$..a..a"])).is_empty());

    // An INDENT of 1 MiB, repeated per level for every value.
    let list = format!("[{}[0]]", "[0],".repeat(299));
    assert_eq!(s.execute(&["JSON.SET", "list", "$", &list]), Reply::Ok);
    let indent = " ".repeat(1 << 20);
    assert_eq!(
        s.execute(&["JSON.GET", "list", "INDENT", &indent, "NEWLINE", "\n", "$"]),
        err("LIMIT Reply size limit is exceeded")
    );
    assert_eq!(
        bytes(s.execute(&["JSON.GET", "list", "INDENT", "  ", "NEWLINE", "\n", "$[0]"])),
        b"[\n  [\n    0\n  ]\n]".to_vec()
    );

    // RapidJSON refuses a number past the largest double; taken as
    // infinity, 0 times it was NaN, saved as `nan` and unreadable.
    let parse = err("SYNTAXERR Failed to parse JSON string due to syntax error");
    assert_eq!(
        s.execute(&["JSON.SET", "n", "$", r#"{"x":9.9e309}"#]),
        parse
    );
    let huge = format!("1{}", "0".repeat(400));
    assert_eq!(s.execute(&["JSON.SET", "n", "$", &huge]), parse);
    assert_eq!(s.execute(&["JSON.SET", "n", "$", r#"{"x":0}"#]), Reply::Ok);
    assert_eq!(
        s.execute(&["JSON.NUMMULTBY", "n", "$.x", "9.9e309"]),
        err("WRONGTYPE Value is not a number")
    );
    assert_eq!(s.execute(&["JSON.SET", "n", "$.x", "1e308"]), Reply::Ok);
    assert_eq!(
        s.execute(&["JSON.NUMMULTBY", "n", "$.x", "10"]),
        err("OVERFLOW Multiplication would overflow")
    );
    assert_eq!(s.execute(&["JSON.GET", "n"]), b(r#"{"x":1e308}"#));

    // One value at every match counts once per match against the limit...
    let size = err("LIMIT Document size limit is exceeded");
    let twenty = format!("[{}0]", "0,".repeat(19));
    let hundred = format!("\"{}\"", "x".repeat(100));
    assert_eq!(s.execute(&["JSON.SET", "c", "$", &twenty]), Reply::Ok);
    check(
        &s,
        &[
            (
                &["CONFIG", "SET", "json.max-document-size", "2000"],
                Reply::Ok,
            ),
            (&["JSON.SET", "c", "$[*]", &hundred], size.clone()),
            (&["JSON.SET", "c", "$[0]", &hundred], Reply::Ok),
            (&["JSON.SET", "c", "$[1:]", "[]"], Reply::Ok),
            (&["JSON.ARRAPPEND", "c", "$[1:]", &hundred], size.clone()),
            (
                &["JSON.STRAPPEND", "c", "$[0]", &hundred],
                Reply::Array(vec![int(200)]),
            ),
            (&["CONFIG", "SET", "json.max-document-size", "0"], Reply::Ok),
        ],
    );
    // ... and, limit or not, one write adds at most 512 MiB.
    let many = format!("[{}0]", "0,".repeat(599));
    let mib = format!("\"{}\"", "x".repeat(1 << 20));
    assert_eq!(s.execute(&["JSON.SET", "c", "$", &many]), Reply::Ok);
    assert_eq!(s.execute(&["JSON.SET", "c", "$[*]", &mib]), size);
    assert_eq!(
        s.execute(&["JSON.ARRLEN", "c", "$"]),
        Reply::Array(vec![int(600)])
    );
}
