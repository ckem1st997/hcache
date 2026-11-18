//! What real client libraries send before the first command, and what
//! their script helpers rely on: HELLO with RESP3, EVALSHA, SCRIPT LOAD,
//! SCRIPT EXISTS, SCRIPT FLUSH. RESP3 replies are compared byte for byte.

mod net;

use hotstash::{Config, Db, Reply};
use net::*;
use std::io::{BufRead, Read};

const SHA_RETURN_1: &str = "e0e1f9fabfc9d4800c877a703b823ac0578ff8db";
const SHA_RETURN_2: &str = "7f923f79fe76194c868d7e1d0820de36700eb649";
const SHA_KEYS_ARGV: &str = "bfbf458525d6a0b19200bfd6db3af481156b367b";
const SHA_UNKNOWN: &str = "ffffffffffffffffffffffffffffffffffffffff";

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 8))
}

/// Send one command and require exactly these bytes back.
fn expect_bytes(c: &mut Client, args: &[&str], want: &[u8]) {
    c.send(args);
    let mut got = vec![0u8; want.len()];
    c.rd.read_exact(&mut got)
        .unwrap_or_else(|e| panic!("{args:?}: reading {} bytes: {e}", want.len()));
    assert_eq!(
        String::from_utf8_lossy(&got),
        String::from_utf8_lossy(want),
        "{args:?}"
    );
}

fn is_err(r: &Reply, prefix: &str) -> bool {
    error_text(r).is_some_and(|t| t.starts_with(prefix))
}

const HELLO3: &[u8] = b"%7\r\n$6\r\nserver\r\n$8\r\nhotstash\r\n$7\r\nversion\r\n$5\r\n7.4.0\r\n\
$5\r\nproto\r\n:3\r\n$2\r\nid\r\n:0\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n\
$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n";

#[test]
fn hello_without_version_reports_resp2() {
    watchdog("hello2", 30, || {
        let env = env();
        let mut c = env.client();
        for args in [
            &["HELLO"][..],
            &["hello", "2"],
            &["HELLO", "2", "SETNAME", "x"],
        ] {
            let Reply::Array(items) = c.cmd(args) else {
                panic!("{args:?}: not an array");
            };
            assert_eq!(items.len(), 14, "{args:?}");
            assert_eq!(items[4], Reply::bulk("proto"));
            assert_eq!(items[5], Reply::Int(2));
        }
        assert_eq!(c.cmd(&["GET", "missing"]), Reply::Nil);
    });
}

#[test]
fn hello_rejects_bad_versions_and_keeps_the_protocol() {
    watchdog("hello-bad", 30, || {
        let env = env();
        let mut c = env.client();
        for v in ["1", "4", "0", "-3"] {
            assert!(is_err(&c.cmd(&["HELLO", v]), "NOPROTO"), "HELLO {v}");
        }
        // As Redis: a version that is not an i64 is not a protocol at all.
        for v in ["abc", "99999999999999999999", "3.0", ""] {
            assert!(
                is_err(&c.cmd(&["HELLO", v]), "ERR Protocol version"),
                "HELLO {v}"
            );
        }
        assert!(is_err(&c.cmd(&["HELLO", "3", "BOGUS"]), "ERR"));
        assert!(is_err(&c.cmd(&["HELLO", "3", "SETNAME"]), "ERR"));
        assert!(is_err(
            &c.cmd(&["HELLO", "3", "AUTH", "someone", "pw"]),
            "WRONGPASS"
        ));
        // None of the failed HELLOs switched the connection.
        assert_eq!(c.cmd(&["GET", "missing"]), Reply::Nil);
        assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    });
}

#[test]
fn hello_3_switches_this_connection_only() {
    watchdog("hello3", 30, || {
        let env = env();
        let mut c = env.client();
        let mut other = env.client();
        expect_bytes(&mut c, &["HELLO", "3"], HELLO3);
        expect_bytes(&mut c, &["GET", "missing"], b"_\r\n");
        expect_bytes(&mut c, &["HGET", "nohash", "f"], b"_\r\n");
        expect_bytes(&mut c, &["SET", "s", "v"], b"+OK\r\n");
        expect_bytes(&mut c, &["GET", "s"], b"$1\r\nv\r\n");
        expect_bytes(&mut c, &["MGET", "missing", "s"], b"*2\r\n_\r\n$1\r\nv\r\n");
        expect_bytes(&mut c, &["HSET", "h", "a", "1"], b":1\r\n");
        expect_bytes(&mut c, &["HGETALL", "h"], b"%1\r\n$1\r\na\r\n$1\r\n1\r\n");
        expect_bytes(&mut c, &["HGETALL", "nohash"], b"%0\r\n");
        expect_bytes(&mut c, &["HGETALL", "s"], b"-WRONGTYPE");
        c.rd.read_line(&mut String::new())
            .expect("rest of the error");
        expect_bytes(&mut c, &["KEYS", "nothing*"], b"*0\r\n");
        expect_bytes(&mut c, &["CONFIG", "GET", "nothing-such"], b"%0\r\n");
        expect_bytes(
            &mut c,
            &["EVAL", "return {1, false, {false}}", "0"],
            b"*3\r\n:1\r\n_\r\n*1\r\n_\r\n",
        );
        expect_bytes(&mut c, &["EVAL", "return false", "0"], b"_\r\n");
        // A script sees RESP2 conversions whatever the connection speaks.
        expect_bytes(
            &mut c,
            &["EVAL", "return redis.call('HGETALL', 'h')", "0"],
            b"*2\r\n$1\r\na\r\n$1\r\n1\r\n",
        );
        expect_bytes(&mut c, &["HELLO"], HELLO3);
        expect_bytes(
            &mut c,
            &["HELLO", "3", "AUTH", "default", "pw", "SETNAME", "n"],
            HELLO3,
        );

        assert_eq!(other.cmd(&["GET", "missing"]), Reply::Nil);
        assert_eq!(
            other.cmd(&["HGETALL", "h"]),
            Reply::Array(vec![Reply::bulk("a"), Reply::bulk("1")])
        );

        // And back again.
        let Reply::Array(items) = c.cmd(&["HELLO", "2"]) else {
            panic!("HELLO 2 after HELLO 3 is not an array");
        };
        assert_eq!(items[5], Reply::Int(2));
        assert_eq!(c.cmd(&["GET", "missing"]), Reply::Nil);
    });
}

#[test]
fn hello_3_applies_to_commands_pipelined_behind_it() {
    watchdog("hello3-pipeline", 30, || {
        let env = env();
        let mut c = env.client();
        let mut batch = wire_str(&["GET", "missing"]);
        batch.extend(wire_str(&["HELLO", "3"]));
        batch.extend(wire_str(&["GET", "missing"]));
        batch.extend(wire_str(&["HGETALL", "nohash"]));
        batch.extend(wire_str(&["HELLO", "2"]));
        batch.extend(wire_str(&["GET", "missing"]));
        c.send_raw(&batch);
        let mut want = b"$-1\r\n".to_vec();
        want.extend_from_slice(HELLO3);
        want.extend_from_slice(b"_\r\n%0\r\n*14\r\n");
        let mut got = vec![0u8; want.len()];
        c.rd.read_exact(&mut got).expect("pipelined replies");
        assert_eq!(
            String::from_utf8_lossy(&got),
            String::from_utf8_lossy(&want)
        );
        let mut rest = Vec::new();
        for _ in 0..14 {
            rest.push(c.read());
        }
        assert_eq!(rest[5], Reply::Int(2));
        assert_eq!(c.read(), Reply::Nil);
    });
}

#[test]
fn resp3_hgetall_of_a_hash_scanned_in_chunks_is_one_map() {
    watchdog("hello3-big-hash", 60, || {
        let env = env();
        let mut c = env.client();
        const FIELDS: usize = 5000;
        for chunk in 0..FIELDS / 100 {
            let mut args = vec!["HSET".to_string(), "big".to_string()];
            for i in 0..100 {
                args.push(format!("f{}", chunk * 100 + i));
                args.push("v".to_string());
            }
            assert_eq!(c.cmd(&args), Reply::Int(100));
        }
        // Writers on other connections make the scan park and resume.
        let mut noise = env.client();
        let mut batch = Vec::new();
        for i in 0..2000 {
            batch.extend(wire_str(&["SET", &format!("n{i}"), "x"]));
        }
        noise.send_raw(&batch);
        expect_bytes(&mut c, &["HELLO", "3"], HELLO3);
        expect_bytes(
            &mut c,
            &["HGETALL", "big"],
            format!("%{FIELDS}\r\n").as_bytes(),
        );
        // The pairs decode as RESP2 values: bulk strings only.
        for _ in 0..FIELDS * 2 {
            assert!(matches!(c.read(), Reply::Bulk(_)));
        }
        expect_bytes(&mut c, &["PING"], b"+PONG\r\n");
        for _ in 0..2000 {
            assert_eq!(noise.read(), Reply::Ok);
        }
    });
}

fn fresh() -> (Db, hotstash::Session) {
    let db = Db::open(None, Config::default()).expect("open db");
    let s = db.session().expect("session");
    (db, s)
}

#[test]
fn script_load_then_evalsha() {
    let (_db, s) = fresh();
    assert_eq!(
        s.execute(&["SCRIPT", "LOAD", "return 1"]),
        Reply::bulk(SHA_RETURN_1)
    );
    assert_eq!(s.execute(&["EVALSHA", SHA_RETURN_1, "0"]), Reply::Int(1));
    assert_eq!(
        s.execute(&["evalsha", &SHA_RETURN_1.to_uppercase(), "0"]),
        Reply::Int(1)
    );
    assert_eq!(
        s.execute(&["SCRIPT", "LOAD", "return {KEYS[1],ARGV[1]}"]),
        Reply::bulk(SHA_KEYS_ARGV)
    );
    assert_eq!(
        s.execute(&["EVALSHA", SHA_KEYS_ARGV, "1", "k", "a"]),
        Reply::Array(vec![Reply::bulk("k"), Reply::bulk("a")])
    );
    // Loading twice is harmless.
    assert_eq!(
        s.execute(&["SCRIPT", "LOAD", "return 1"]),
        Reply::bulk(SHA_RETURN_1)
    );
    assert_eq!(s.execute(&["EVALSHA", SHA_RETURN_1, "0"]), Reply::Int(1));
}

#[test]
fn evalsha_of_an_unknown_script_is_noscript() {
    let (_db, s) = fresh();
    for sha in [
        SHA_UNKNOWN,
        SHA_RETURN_1,
        "",
        "abc",
        "return 1",
        &"f".repeat(41),
    ] {
        let r = s.execute(&["EVALSHA", sha, "0"]);
        assert!(is_err(&r, "NOSCRIPT"), "{sha:?}: {r:?}");
    }
    // The helper in every client library retries with the source.
    assert_eq!(s.execute(&["EVAL", "return 1", "0"]), Reply::Int(1));
    assert_eq!(s.execute(&["EVALSHA", SHA_RETURN_1, "0"]), Reply::Int(1));
}

#[test]
fn eval_makes_the_script_callable_by_hash() {
    let (_db, s) = fresh();
    assert_eq!(s.execute(&["EVAL", "return 2", "0"]), Reply::Int(2));
    assert_eq!(s.execute(&["EVALSHA", SHA_RETURN_2, "0"]), Reply::Int(2));
    // A script that fails to compile is not registered.
    let bad = "this is not lua";
    assert!(s.execute(&["EVAL", bad, "0"]).is_error());
    let Reply::Error(e) = s.execute(&["SCRIPT", "LOAD", bad]) else {
        panic!("SCRIPT LOAD of invalid Lua did not fail");
    };
    assert!(e.starts_with("ERR Error compiling script"), "{e}");
    assert_eq!(
        s.execute(&["SCRIPT", "EXISTS", SHA_RETURN_2, SHA_RETURN_1]),
        Reply::Array(vec![Reply::Int(1), Reply::Int(0)])
    );
}

#[test]
fn script_exists_and_flush() {
    let (_db, s) = fresh();
    s.execute(&["SCRIPT", "LOAD", "return 1"]);
    assert_eq!(
        s.execute(&[
            "SCRIPT",
            "EXISTS",
            SHA_RETURN_1,
            SHA_UNKNOWN,
            "junk",
            SHA_RETURN_1
        ]),
        Reply::Array(vec![
            Reply::Int(1),
            Reply::Int(0),
            Reply::Int(0),
            Reply::Int(1)
        ])
    );
    assert!(s.execute(&["SCRIPT", "EXISTS"]).is_error());
    assert!(s.execute(&["SCRIPT", "LOAD"]).is_error());
    assert!(s.execute(&["SCRIPT", "LOAD", "a", "b"]).is_error());
    assert!(s.execute(&["SCRIPT", "NOPE"]).is_error());
    for flush in [
        &["SCRIPT", "FLUSH"][..],
        &["SCRIPT", "FLUSH", "ASYNC"],
        &["script", "flush", "sync"],
    ] {
        s.execute(&["SCRIPT", "LOAD", "return 1"]);
        assert_eq!(s.execute(flush), Reply::Ok, "{flush:?}");
        assert_eq!(
            s.execute(&["SCRIPT", "EXISTS", SHA_RETURN_1]),
            Reply::Array(vec![Reply::Int(0)])
        );
        assert!(is_err(
            &s.execute(&["EVALSHA", SHA_RETURN_1, "0"]),
            "NOSCRIPT"
        ));
    }
    assert!(s.execute(&["SCRIPT", "FLUSH", "BOGUS"]).is_error());
}

#[test]
fn evalsha_argument_errors_match_eval() {
    let (_db, s) = fresh();
    s.execute(&["SCRIPT", "LOAD", "return 1"]);
    assert!(is_err(
        &s.execute(&["EVALSHA", SHA_RETURN_1]),
        "ERR wrong number"
    ));
    assert!(is_err(
        &s.execute(&["EVALSHA", SHA_RETURN_1, "x"]),
        "ERR value is not an integer"
    ));
    assert!(is_err(
        &s.execute(&["EVALSHA", SHA_RETURN_1, "-1"]),
        "ERR Number of keys"
    ));
    assert!(is_err(
        &s.execute(&["EVALSHA", SHA_RETURN_1, "2", "k"]),
        "ERR Number of keys"
    ));
}

#[test]
fn scripts_cannot_call_script_commands() {
    let (_db, s) = fresh();
    s.execute(&["SCRIPT", "LOAD", "return 1"]);
    for inner in [
        format!("return redis.call('EVALSHA', '{SHA_RETURN_1}', 0)"),
        "return redis.call('SCRIPT', 'LOAD', 'return 1')".to_string(),
        "return redis.call('SCRIPT', 'FLUSH')".to_string(),
        "return redis.call('HELLO', 3)".to_string(),
    ] {
        let r = s.execute(&["EVAL", &inner, "0"]);
        assert!(r.is_error(), "{inner}: {r:?}");
    }
    assert_eq!(s.execute(&["EVALSHA", SHA_RETURN_1, "0"]), Reply::Int(1));
}

#[test]
fn a_script_too_long_for_the_compile_cache_still_runs_by_hash() {
    let (_db, s) = fresh();
    let script = format!("-- {}\nreturn 7", "x".repeat(40 << 10));
    let Reply::Bulk(sha) = s.execute(&["SCRIPT", "LOAD", &script]) else {
        panic!("SCRIPT LOAD failed");
    };
    let sha = String::from_utf8(sha).expect("hex");
    assert_eq!(sha.len(), 40);
    for _ in 0..3 {
        assert_eq!(s.execute(&["EVALSHA", &sha, "0"]), Reply::Int(7));
    }
}

#[test]
fn loading_more_scripts_than_the_table_holds_keeps_the_newest() {
    let (_db, s) = fresh();
    // More than the 10 000 hashes the table keeps.
    const LOADS: i64 = 10_500;
    let mut first = String::new();
    let mut last = String::new();
    for i in 0..LOADS {
        let Reply::Bulk(sha) = s.execute(&["SCRIPT", "LOAD", &format!("return {i}")]) else {
            panic!("SCRIPT LOAD {i} failed");
        };
        last = String::from_utf8(sha).expect("hex");
        if i == 0 {
            first.clone_from(&last);
        }
        assert_eq!(s.execute(&["EVALSHA", &last, "0"]), Reply::Int(i));
    }
    assert_eq!(s.execute(&["EVALSHA", &last, "0"]), Reply::Int(LOADS - 1));
    // The full table was emptied, so the oldest hash is gone; a client's
    // script helper sees NOSCRIPT and sends the source again.
    assert!(matches!(
        s.execute(&["EVALSHA", &first, "0"]),
        Reply::Error(e) if e.starts_with("NOSCRIPT")
    ));
    assert_eq!(s.execute(&["EVAL", "return 0", "0"]), Reply::Int(0));
    assert_eq!(s.execute(&["EVALSHA", &first, "0"]), Reply::Int(0));
}

#[test]
fn evalsha_over_tcp_from_two_connections() {
    watchdog("evalsha-tcp", 30, || {
        let env = env();
        let mut a = env.client();
        let mut b = env.client();
        assert!(is_err(&a.cmd(&["EVALSHA", SHA_RETURN_1, "0"]), "NOSCRIPT"));
        assert_eq!(
            a.cmd(&["SCRIPT", "LOAD", "return 1"]),
            Reply::bulk(SHA_RETURN_1)
        );
        assert_eq!(b.cmd(&["EVALSHA", SHA_RETURN_1, "0"]), Reply::Int(1));
        assert_eq!(
            b.cmd(&["SCRIPT", "EXISTS", SHA_RETURN_1, SHA_UNKNOWN]),
            Reply::Array(vec![Reply::Int(1), Reply::Int(0)])
        );
        // The connection survives NOSCRIPT, as the client helpers assume.
        assert_eq!(a.cmd(&["PING"]), Reply::Simple("PONG".into()));
    });
}
