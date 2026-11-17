//! Adversarial tests of EVAL (Lua 5.1 sandbox, conversions, resource limits)
//! and SCRIPT KILL, through the embedded API. Reference behaviour: Redis 7/8.

mod support;

use hcache::{Config, Db, Reply};
use hcache_core::resp::decode_reply;
use hcache_core::testutil::Rng;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use support::*;

const KILL_BOUND: Duration = Duration::from_secs(2);

fn fresh() -> (Db, hcache::Session) {
    let db = open(Config::default());
    let s = session(&db);
    (db, s)
}

fn ev(s: &hcache::Session, script: &str) -> Reply {
    eval(s, script, &[], &[])
}

/// Replies compared by their wire form (`Ok` and `Simple("OK")` are the same).
fn same(a: &Reply, b: &Reply) -> bool {
    enc(a) == enc(b)
}

fn ints(v: &[i64]) -> Reply {
    Reply::Array(v.iter().map(|n| Reply::Int(*n)).collect())
}

const CANARY: &str = r#"
local ok = redis.call('SET', KEYS[1], 'v')
assert(ok.ok == 'OK')
assert(string.rep('a', 3) == 'aaa')
assert(type(table.insert) == 'function')
assert(math.floor(2.5) == 2)
assert(#KEYS == 1 and #ARGV == 1 and KEYS[1] == 'k' and ARGV[1] == 'a')
assert(type(redis.pcall) == 'function' and type(redis.error_reply) == 'function')
assert(type(redis.call) == 'function' and type(redis.status_reply) == 'function')
assert(tostring(12) == '12' and _VERSION == 'Lua 5.1')
assert(pcall(function() return 1 end))
assert(select('#', unpack({1, 2, 3})) == 3)
assert(type(string.format) == 'function' and type(table.concat) == 'function')
assert(KEYS.stash == nil and ARGV.stash == nil and KEYS.evil == nil)
return 'canary-ok'
"#;

fn canary(s: &hcache::Session) -> Reply {
    eval(s, CANARY, &["k"], &["a"])
}

fn assert_canary(s: &hcache::Session, context: &str) {
    let r = canary(s);
    assert!(
        r == bulk("canary-ok"),
        "after {context}: canary returned {}",
        brief(&r)
    );
}

// ---------------------------------------------------------------------
// Return-value conversion
// ---------------------------------------------------------------------

#[test]
fn return_value_conversion_table() {
    guarded("conversion", 60, || {
        let (_db, s) = fresh();
        let arr = Reply::Array;
        let cases: Vec<(&str, Reply)> = vec![
            ("return 3", Reply::Int(3)),
            ("return -3", Reply::Int(-3)),
            ("return 3.99", Reply::Int(3)),
            ("return -3.99", Reply::Int(-3)),
            ("return 0.5", Reply::Int(0)),
            ("return -0.5", Reply::Int(0)),
            ("return 1e10", Reply::Int(10_000_000_000)),
            ("return 1e15", Reply::Int(1_000_000_000_000_000)),
            ("return 2^53", Reply::Int(9_007_199_254_740_992)),
            ("return -2^63", Reply::Int(i64::MIN)),
            ("return true", Reply::Int(1)),
            ("return false", Reply::Nil),
            ("return nil", Reply::Nil),
            ("", Reply::Nil),
            ("local x = 1", Reply::Nil),
            ("return 'hello'", bulk("hello")),
            ("return ''", bulk("")),
            ("return 'a\\0b\\r\\nc'", Reply::Bulk(b"a\0b\r\nc".to_vec())),
            ("return tostring(12)", bulk("12")),
            ("return 12 .. ''", bulk("12")),
            ("return {1,2,3}", ints(&[1, 2, 3])),
            ("return {1,2,nil,4}", ints(&[1, 2])),
            ("return {nil,2,3}", arr(vec![])),
            ("return {}", arr(vec![])),
            ("return {{}}", arr(vec![arr(vec![])])),
            (
                "return {1,{2,{3}}}",
                arr(vec![Reply::Int(1), arr(vec![Reply::Int(2), ints(&[3])])]),
            ),
            (
                "return {1,'a',true,false,3.7}",
                arr(vec![
                    Reply::Int(1),
                    bulk("a"),
                    Reply::Int(1),
                    Reply::Nil,
                    Reply::Int(3),
                ]),
            ),
            (
                "return {1,false,3}",
                arr(vec![Reply::Int(1), Reply::Nil, Reply::Int(3)]),
            ),
            ("return {[1]=1,[2]=2,[4]=4}", ints(&[1, 2])),
            ("return {[2]=2}", arr(vec![])),
            ("return {n=1}", arr(vec![])),
            ("return {ok='x'}", Reply::Simple("x".into())),
            ("return {ok='OK'}", Reply::Ok),
            ("return {err='x'}", err("x")),
            ("return {err='a', ok='b'}", err("a")),
            ("return {ok='x', 1, 2}", Reply::Simple("x".into())),
            ("return {err='x', 1, 2}", err("x")),
            ("return {ok=5}", arr(vec![])),
            ("return {err=5}", arr(vec![])),
            ("return {{ok='n'}}", arr(vec![Reply::Simple("n".into())])),
            ("return {{err='n'}, 1}", arr(vec![err("n"), Reply::Int(1)])),
            (
                "return redis.status_reply('FINE')",
                Reply::Simple("FINE".into()),
            ),
            ("return redis.error_reply('MYERR boom')", err("MYERR boom")),
            ("return redis.error_reply('')", err("")),
            (
                "return redis.status_reply('')",
                Reply::Simple(String::new()),
            ),
            ("return function() end", Reply::Nil),
            ("return print == nil", Reply::Nil),
        ];
        for (script, want) in cases {
            let got = ev(&s, script);
            // `print == nil` raises (undeclared global): not part of the table.
            if script == "return print == nil" {
                assert!(got.is_error());
                continue;
            }
            assert!(
                same(&got, &want),
                "script {script:?}\n  want {}\n  got  {}",
                brief(&want),
                brief(&got)
            );
        }
        assert_canary(&s, "conversion table");
    });
}

#[test]
fn return_numbers_outside_i64_and_special_floats() {
    guarded("number_edges", 30, || {
        let (_db, s) = fresh();
        for script in [
            "return 1e30",
            "return -1e30",
            "return 2^63",
            "return -2^63",
            "return 2^64",
            "return 0/0",
            "return 1/0",
            "return -1/0",
            "return {1e30, 0/0, 1/0}",
        ] {
            let got = ev(&s, script);
            // Redis casts the double to long long (undefined for out-of-range
            // values); all that is required here is a sane reply, no panic.
            let ok = matches!(&got, Reply::Int(_))
                || matches!(&got, Reply::Array(a) if a.iter().all(|r| matches!(r, Reply::Int(_))));
            assert!(ok, "{script}: {}", brief(&got));
            eprintln!("INFO {script:?} -> {}", brief(&got));
        }
        assert_canary(&s, "number edges");
    });
}

#[test]
fn self_referential_and_deeply_nested_return_values() {
    guarded("cycles", 60, || {
        let (_db, s) = fresh();
        for script in [
            "local t = {} t[1] = t return t",
            // Exponential expansion of a DAG/cycle: must stop at the item budget.
            "local t = {} t[1] = t t[2] = t return t",
            "local t = {} for i = 1, 50 do t[i] = t end return t",
            "local a = {} local b = {a} a[1] = b return a",
            "local t = {} for i = 1, 10000 do t = {t} end return t",
            "local t = {} for i = 1, 100000 do t = {t} end return t",
        ] {
            let start = Instant::now();
            let got = ev(&s, script);
            assert!(
                has_err(&got, "ERR Error running script"),
                "{script}: {}",
                brief(&got)
            );
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "{script} took {:?}",
                start.elapsed()
            );
        }
        // `{ok = <table>}` is not a status: it is a plain (empty) table.
        assert!(same(
            &ev(&s, "local t = {} t.ok = t return t"),
            &Reply::Array(vec![])
        ));
        // Moderately deep nesting is fine.
        let nested = |n: usize| format!("local t = {{7}} for i = 1, {n} do t = {{t}} end return t");
        let mut got = ev(&s, &nested(20));
        for _ in 0..20 {
            got = array(got).remove(0);
        }
        assert_eq!(got, ints(&[7]));
        for depth in [31usize, 40, 100, 1000] {
            let r = ev(&s, &nested(depth));
            eprintln!(
                "INFO nesting depth {depth}: {}",
                if r.is_error() { brief(&r) } else { "ok".into() }
            );
            if depth <= 31 {
                assert!(!r.is_error(), "depth {depth}: {}", brief(&r));
            }
        }
        assert_canary(&s, "cycles");
    });
}

// ---------------------------------------------------------------------
// redis.call / redis.pcall
// ---------------------------------------------------------------------

#[test]
fn redis_call_reply_conversion() {
    guarded("call_conv", 60, || {
        let (_db, s) = fresh();
        let r = ev(
            &s,
            "local v = redis.call('GET', 'nope') return {type(v), v == false and 1 or 0}",
        );
        assert_eq!(r, Reply::Array(vec![bulk("boolean"), Reply::Int(1)]));
        let r = ev(&s, "return type(redis.call('INCR', 'n'))");
        assert_eq!(r, bulk("number"));
        let r = ev(
            &s,
            "local r = redis.call('SET', 'k', 'v') return {type(r), r.ok}",
        );
        assert_eq!(r, Reply::Array(vec![bulk("table"), bulk("OK")]));
        let r = ev(
            &s,
            "redis.call('SET','a','1') redis.call('SET','b','2') \
             local r = redis.call('MGET','a','zz','b') \
             return {type(r), #r, r[1], tostring(r[2]), r[3]}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![
                bulk("table"),
                Reply::Int(3),
                bulk("1"),
                bulk("false"),
                bulk("2")
            ])
        );
        // HGETALL: flat table of strings; HSET: number.
        let r = ev(
            &s,
            "local n = redis.call('HSET', 'h', 'f', 'v') local r = redis.call('HGETALL', 'h') \
             return {type(n), n, type(r), #r, r[1], r[2]}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![
                bulk("number"),
                Reply::Int(1),
                bulk("table"),
                Reply::Int(2),
                bulk("f"),
                bulk("v")
            ])
        );
        // Empty array stays a table; wrong-type MGET element is false.
        let r = ev(
            &s,
            "local r = redis.call('KEYS', 'nomatch*') return {type(r), #r}",
        );
        assert_eq!(r, Reply::Array(vec![bulk("table"), Reply::Int(0)]));
        let r = ev(&s, "return redis.call('MGET', 'h')");
        assert_eq!(r, Reply::Array(vec![Reply::Nil]));
        // Binary-safe both ways.
        let bin = vec![0u8, 255, b'\r', b'\n', 0];
        let r = s.execute(&[
            b"EVAL".to_vec(),
            b"redis.call('SET', KEYS[1], ARGV[1]) return redis.call('GET', KEYS[1])".to_vec(),
            b"1".to_vec(),
            b"k\0\r\n".to_vec(),
            bin.clone(),
        ]);
        assert_eq!(r, Reply::Bulk(bin));
        // A status reply passes through unchanged.
        let r = ev(&s, "return redis.call('SET', 'x', 'y')");
        assert!(same(&r, &Reply::Ok), "{}", brief(&r));
        // Errors from redis.call() abort; the integer from INCR is a number.
        let r = ev(&s, "return redis.call('INCR', 'n') + 1");
        assert_eq!(r, Reply::Int(3));
    });
}

#[test]
fn redis_call_errors_abort_and_pcall_returns_error_tables() {
    guarded("call_errors", 60, || {
        let (_db, s) = fresh();
        s.execute(&["HSET", "h", "f", "v"]);
        // Aborting error; effects before it stay (no rollback, as in Redis).
        let r = ev(
            &s,
            "redis.call('SET', 'a', '1') redis.call('INCR', 'h') redis.call('SET', 'b', '1') return 'unreachable'",
        );
        assert!(has_err(&r, "WRONGTYPE"), "{}", brief(&r));
        assert_eq!(s.execute(&["GET", "a"]), bulk("1"));
        assert_eq!(s.execute(&["GET", "b"]), Reply::Nil);

        let r = ev(&s, "return redis.call('NOSUCHCOMMAND', 'x')");
        assert!(has_err(&r, "nknown"), "{}", brief(&r));
        let r = ev(&s, "return redis.call('GET')");
        assert!(has_err(&r, "wrong number"), "{}", brief(&r));
        let r = ev(&s, "return redis.call('INCR', 'n', 'extra')");
        assert!(has_err(&r, "wrong number"), "{}", brief(&r));

        // pcall: error table, script continues.
        let r = ev(
            &s,
            "local r = redis.pcall('INCR', 'h') return {type(r), r.err}",
        );
        let items = array(r);
        assert_eq!(items[0], bulk("table"));
        assert!(has_err(&Reply::Error(text(&items[1])), "WRONGTYPE"));
        let r = ev(&s, "local r = redis.pcall('NOSUCHCOMMAND') return type(r) .. ':' .. tostring(r.err ~= nil)");
        assert_eq!(r, bulk("table:true"));
        let r = ev(
            &s,
            "local r = redis.pcall('INCR', 'h') return redis.call('GET', 'a')",
        );
        assert_eq!(r, bulk("1"));
        // And returning the error table from pcall returns an error reply.
        let r = ev(&s, "return redis.pcall('INCR', 'h')");
        assert!(has_err(&r, "WRONGTYPE"), "{}", brief(&r));
        assert!(
            !has_err(&r, "Error running script"),
            "pcall error table must pass through: {}",
            brief(&r)
        );
        // pcall of a good command behaves like call.
        assert_eq!(ev(&s, "return redis.pcall('INCR', 'cnt')"), Reply::Int(1));

        // Argument handling.
        let r = ev(&s, "return redis.call()");
        assert!(has_err(&r, "at least one argument"), "{}", brief(&r));
        let r = ev(
            &s,
            "local r = redis.pcall() return type(r) .. tostring(r.err ~= nil)",
        );
        assert_eq!(r, bulk("tabletrue"), "{}", brief(&r));
        for bad in [
            "redis.call('SET', {}, 'v')",
            "redis.call('SET', 'k', true)",
            "redis.call('SET', 'k', false)",
            "redis.call('SET', nil, 'v')",
            "redis.call('GET', 'k', nil)",
            "redis.call('SET', 'k', function() end)",
        ] {
            let r = ev(&s, bad);
            assert!(
                has_err(&r, "must be strings or integers"),
                "{bad}: {}",
                brief(&r)
            );
            let r = ev(&s, &bad.replace("redis.call", "redis.pcall").to_string());
            assert!(
                !has_err(&r, "Error running script"),
                "{bad} via pcall: {}",
                brief(&r)
            );
        }
        let mut leftovers = array(s.execute(&["KEYS", "k"]));
        leftovers.clear();
        assert_canary(&s, "call errors");
    });
}

#[test]
fn redis_call_converts_numbers_to_strings() {
    guarded("call_numbers", 30, || {
        let (_db, s) = fresh();
        for (lua, want) in [
            ("10", "10"),
            ("-5", "-5"),
            ("0", "0"),
            ("3.5", "3.5"),
            ("-0.25", "-0.25"),
            ("1e15", "1000000000000000"),
            ("2^53", "9007199254740992"),
            ("100 / 4", "25"),
            ("7 % 3", "1"),
        ] {
            let script = format!("redis.call('SET', 'k', {lua}) return redis.call('GET', 'k')");
            assert_eq!(ev(&s, &script), bulk(want), "SET k {lua}");
        }
        // Numbers as keys, as command names' arguments, as hash fields.
        ev(&s, "redis.call('SET', 123, 456)");
        assert_eq!(s.execute(&["GET", "123"]), bulk("456"));
        ev(&s, "redis.call('HSET', 'h', 1, 2)");
        assert_eq!(s.execute(&["HGET", "h", "1"]), bulk("2"));
        assert_eq!(ev(&s, "return redis.call('INCRBY', 'c', 5)"), Reply::Int(5));
        assert_eq!(ev(&s, "return redis.call('DECRBY', 'c', 2)"), Reply::Int(3));
        // Redis formats doubles with %.17g; the spike prints the shortest form.
        let r = ev(
            &s,
            "redis.call('SET', 'k', 0.1) return redis.call('GET', 'k')",
        );
        eprintln!(
            "INFO redis.call('SET','k',0.1) stores {} (Redis: 0.10000000000000001)",
            brief(&r)
        );
    });
}

/// PING, ECHO and DBSIZE are callable from scripts in Redis. The spike
/// classifies them as connection-level commands and refuses them.
#[test]
fn ping_echo_dbsize_callable_from_scripts() {
    guarded("conn_in_script", 30, || {
        let (_db, s) = fresh();
        s.execute(&["SET", "a", "1"]);
        let r = ev(&s, "return redis.call('PING')");
        assert!(
            same(&r, &Reply::Simple("PONG".into())),
            "PING: {}",
            brief(&r)
        );
        let r = ev(&s, "return redis.call('ECHO', 'hi')");
        assert_eq!(r, bulk("hi"), "ECHO");
        let r = ev(&s, "return redis.call('DBSIZE')");
        assert_eq!(r, Reply::Int(1), "DBSIZE");
    });
}

#[test]
fn commands_refused_inside_scripts() {
    guarded("refused", 30, || {
        let (_db, s) = fresh();
        // Nested EVAL must never run (it would need the gate the caller holds).
        let r = ev(&s, "return redis.call('EVAL', 'return 1', 0)");
        assert!(has_err(&r, "not allowed"), "nested EVAL: {}", brief(&r));
        let r = ev(&s, "local r = redis.pcall('EVAL', 'return redis.call(\"EVAL\", \"return 1\", 0)', 0) return r.err");
        assert!(
            matches!(r, Reply::Bulk(_)),
            "nested pcall EVAL: {}",
            brief(&r)
        );
        let r = ev(&s, "return redis.call('eval', 'return 1', '0')");
        assert!(
            has_err(&r, "not allowed"),
            "lowercase nested EVAL: {}",
            brief(&r)
        );
        let r = ev(&s, "return redis.call('SCRIPT', 'KILL')");
        assert!(
            has_err(&r, "not allowed") || has_err(&r, "nknown"),
            "SCRIPT KILL in script: {}",
            brief(&r)
        );
        let r = ev(
            &s,
            "redis.call('SET','a','1') return redis.call('FLUSHALL')",
        );
        eprintln!(
            "INFO FLUSHALL inside a script -> {} (Redis allows it)",
            brief(&r)
        );
        assert_canary(&s, "refused commands");
    });
}

// ---------------------------------------------------------------------
// KEYS / ARGV
// ---------------------------------------------------------------------

#[test]
fn keys_and_argv_handling() {
    guarded("keys_argv", 60, || {
        let (_db, s) = fresh();
        let not_int = err("ERR value is not an integer or out of range");
        assert_eq!(
            s.execute(&["EVAL", "return 1", "-1"]),
            err("ERR Number of keys can't be negative")
        );
        assert_eq!(
            s.execute(&["EVAL", "return 1", "-9223372036854775808"]),
            err("ERR Number of keys can't be negative")
        );
        for bad in [
            "abc",
            "1.5",
            "",
            " 1",
            "+1",
            "1 ",
            "0x1",
            "9223372036854775808",
        ] {
            assert_eq!(
                s.execute(&["EVAL", "return 1", bad]),
                not_int,
                "numkeys {bad:?}"
            );
        }
        let too_many = err("ERR Number of keys can't be greater than number of args");
        assert_eq!(s.execute(&["EVAL", "return 1", "1"]), too_many);
        assert_eq!(s.execute(&["EVAL", "return 1", "3", "a", "b"]), too_many);
        assert_eq!(
            s.execute(&["EVAL", "return 1", "9223372036854775807", "a"]),
            too_many
        );
        assert_eq!(
            s.execute(&["EVAL", "return 1"]),
            err("ERR wrong number of arguments for 'eval' command")
        );
        assert_eq!(
            s.execute(&["EVAL"]),
            err("ERR wrong number of arguments for 'eval' command")
        );
        // Zero keys with extra args: they are ARGV.
        let r = s.execute(&[
            "EVAL",
            "return {#KEYS, #ARGV, ARGV[1], ARGV[2]}",
            "0",
            "a",
            "b",
        ]);
        assert_eq!(
            r,
            Reply::Array(vec![Reply::Int(0), Reply::Int(2), bulk("a"), bulk("b")])
        );
        // All keys, no args.
        let r = s.execute(&["EVAL", "return {#KEYS, #ARGV, KEYS[2]}", "2", "a", "b"]);
        assert_eq!(
            r,
            Reply::Array(vec![Reply::Int(2), Reply::Int(0), bulk("b")])
        );
        // Empty strings.
        let r = s.execute(&[
            "EVAL",
            "return {#KEYS, #ARGV, KEYS[1], ARGV[1], #KEYS[1]}",
            "1",
            "",
            "",
        ]);
        assert_eq!(
            r,
            Reply::Array(vec![
                Reply::Int(1),
                Reply::Int(1),
                bulk(""),
                bulk(""),
                Reply::Int(0)
            ])
        );
        // Binary keys and arguments.
        let key = b"a\0b\r\n\xff".to_vec();
        let arg = b"\xff\0\r\n".to_vec();
        let r = s.execute(&[
            b"EVAL".to_vec(),
            b"return {KEYS[1], ARGV[1], #KEYS[1], #ARGV[1]}".to_vec(),
            b"1".to_vec(),
            key.clone(),
            arg.clone(),
        ]);
        assert_eq!(
            r,
            Reply::Array(vec![
                Reply::Bulk(key),
                Reply::Bulk(arg),
                Reply::Int(6),
                Reply::Int(4)
            ])
        );
        // Many.
        let mut argv: Vec<String> = vec![
            "EVAL".into(),
            "return {#KEYS, #ARGV, KEYS[5000], ARGV[5000]}".into(),
            "5000".into(),
        ];
        argv.extend((1..=5000).map(|i| format!("k{i}")));
        argv.extend((1..=5000).map(|i| format!("a{i}")));
        let r = s.execute(&argv);
        assert_eq!(
            r,
            Reply::Array(vec![
                Reply::Int(5000),
                Reply::Int(5000),
                bulk("k5000"),
                bulk("a5000")
            ])
        );
        // KEYS and ARGV are rebuilt for every script: nothing leaks through them.
        let r = s.execute(&[
            "EVAL",
            "KEYS[1] = 'x' table.insert(ARGV, 'y') KEYS.stash = redis.call ARGV.stash = string.rep return #ARGV",
            "1",
            "k",
            "a",
        ]);
        assert_eq!(r, Reply::Int(2));
        let r = s.execute(&[
            "EVAL",
            "return {KEYS[1], #ARGV, type(KEYS.stash), type(ARGV.stash)}",
            "1",
            "k",
            "a",
        ]);
        assert_eq!(
            r,
            Reply::Array(vec![bulk("k"), Reply::Int(1), bulk("nil"), bulk("nil")])
        );
        // KEYS/ARGV are returned as arrays.
        let r = s.execute(&["EVAL", "return KEYS", "2", "x", "y", "z"]);
        assert_eq!(r, Reply::Array(vec![bulk("x"), bulk("y")]));
        let r = s.execute(&["EVAL", "return ARGV", "1", "x", "y", "z"]);
        assert_eq!(r, Reply::Array(vec![bulk("y"), bulk("z")]));
    });
}

// ---------------------------------------------------------------------
// Sandbox
// ---------------------------------------------------------------------

#[test]
fn sandbox_reachable_globals() {
    guarded("globals", 60, || {
        let (_db, s) = fresh();
        let candidates = [
            "os",
            "io",
            "require",
            "dofile",
            "loadfile",
            "package",
            "debug",
            "load",
            "loadstring",
            "getfenv",
            "setfenv",
            "module",
            "newproxy",
            "gcinfo",
            "collectgarbage",
            "print",
            "coroutine",
            "string",
            "table",
            "math",
            "redis",
            "KEYS",
            "ARGV",
            "assert",
            "error",
            "ipairs",
            "pairs",
            "next",
            "pcall",
            "xpcall",
            "rawequal",
            "rawget",
            "rawset",
            "select",
            "setmetatable",
            "getmetatable",
            "tonumber",
            "tostring",
            "type",
            "unpack",
            "_G",
            "_VERSION",
            "_ENV",
            "arg",
            "jit",
            "bit",
            "cjson",
            "cmsgpack",
            "struct",
            "unsafe_dummy",
        ];
        let mut reachable = Vec::new();
        for name in candidates {
            if !ev(&s, &format!("return type({name})")).is_error() {
                reachable.push(name);
            }
        }
        eprintln!("INFO reachable globals: {reachable:?}");
        for must_be_gone in [
            "os",
            "io",
            "require",
            "dofile",
            "loadfile",
            "package",
            "debug",
            "load",
            "loadstring",
            "getfenv",
            "setfenv",
            "module",
            "newproxy",
            "jit",
            "arg",
        ] {
            assert!(
                !reachable.contains(&must_be_gone),
                "`{must_be_gone}` is reachable from scripts"
            );
        }
        for must_work in [
            "string", "table", "math", "redis", "KEYS", "ARGV", "assert", "error", "ipairs",
            "pairs", "next", "pcall", "xpcall", "select", "tonumber", "tostring", "type", "unpack",
            "_G", "_VERSION",
        ] {
            assert!(reachable.contains(&must_work), "`{must_work}` is missing");
        }
        // Members.
        assert_eq!(ev(&s, "return type(string.dump)"), bulk("nil"));
        let members = |table: &str, names: &[&str]| -> Vec<String> {
            names
                .iter()
                .filter(|n| text(&ev(&s, &format!("return type({table}.{n})"))) != "nil")
                .map(|n| n.to_string())
                .collect()
        };
        eprintln!(
            "INFO redis.*: {:?}",
            members(
                "redis",
                &[
                    "call",
                    "pcall",
                    "error_reply",
                    "status_reply",
                    "sha1hex",
                    "log",
                    "setresp",
                    "replicate_commands",
                    "set_repl",
                    "breakpoint",
                    "debug",
                    "LOG_DEBUG",
                    "LOG_WARNING",
                    "REDIS_VERSION",
                    "REDIS_VERSION_NUM",
                    "REPL_ALL",
                    "register_function"
                ]
            )
        );
        eprintln!(
            "INFO string.*: {:?}",
            members(
                "string",
                &[
                    "byte", "char", "dump", "find", "format", "gmatch", "gsub", "len", "lower",
                    "match", "rep", "reverse", "sub", "upper"
                ]
            )
        );
        eprintln!(
            "INFO table.*: {:?}",
            members(
                "table",
                &[
                    "concat", "insert", "maxn", "remove", "sort", "getn", "foreach", "foreachi",
                    "setn"
                ]
            )
        );
        eprintln!(
            "INFO math.*: {:?}",
            members(
                "math",
                &[
                    "abs",
                    "ceil",
                    "floor",
                    "max",
                    "min",
                    "random",
                    "randomseed",
                    "sqrt",
                    "huge",
                    "pi",
                    "fmod",
                    "modf",
                    "pow",
                    "log"
                ]
            )
        );
        for needed in ["call", "pcall", "error_reply", "status_reply"] {
            assert!(
                !members("redis", &[needed]).is_empty(),
                "redis.{needed} missing"
            );
        }
        // `_G` shows no names to iterate.
        let r = ev(
            &s,
            "local n = 0 for k in pairs(_G) do n = n + 1 end return n",
        );
        eprintln!("INFO pairs(_G) yields {} entries", brief(&r));
        assert_eq!(
            ev(&s, "return rawget(_G, 'os') == nil and 1 or 0"),
            Reply::Int(1)
        );
        assert_eq!(ev(&s, "return _VERSION"), bulk("Lua 5.1"));
    });
}

#[test]
fn bytecode_is_never_executed() {
    guarded("bytecode", 30, || {
        let (_db, s) = fresh();
        s.execute(&["SET", "touched", "no"]);
        // Binary chunk signatures: a header of Lua 5.1 bytecode followed by garbage.
        let header: &[u8] = b"\x1bLuaQ\x00\x01\x04\x08\x04\x08\x00";
        for script in [
            header.to_vec(),
            [header, &[0u8; 64][..]].concat(),
            b"\x1bLua".to_vec(),
            b"\x1b".to_vec(),
            [b" ".as_slice(), header].concat(),
            [b"--\n".as_slice(), header].concat(),
        ] {
            let r = s.execute(&[b"EVAL".to_vec(), script.clone(), b"0".to_vec()]);
            assert!(
                has_err(&r, "ERR Error compiling script") || r.is_error(),
                "{script:?} -> {}",
                brief(&r)
            );
        }
        for call in [
            "return loadstring('return 1')()",
            "return load('return 1')()",
            "return string.dump(function() end)",
            "return (loadstring or load)(string.char(27) .. 'Lua')",
        ] {
            let r = ev(&s, call);
            assert!(r.is_error(), "{call} -> {}", brief(&r));
        }
        assert_eq!(s.execute(&["GET", "touched"]), bulk("no"));
        assert_canary(&s, "bytecode attempts");
    });
}

#[test]
fn undeclared_globals_fail() {
    guarded("undeclared", 30, || {
        let (_db, s) = fresh();
        let r = ev(&s, "return foo");
        assert!(
            has_err(&r, "nonexistent global variable 'foo'"),
            "{}",
            brief(&r)
        );
        let r = ev(&s, "foo = 1");
        assert!(has_err(&r, "global variable 'foo'"), "{}", brief(&r));
        let r = ev(&s, "function foo() end");
        assert!(r.is_error(), "{}", brief(&r));
        let r = ev(&s, "local foo = 1 return foo");
        assert_eq!(r, Reply::Int(1));
        let r = ev(&s, "_G.foo = 1");
        assert!(r.is_error(), "{}", brief(&r));
        let r = ev(&s, "return _G.foo");
        assert!(r.is_error(), "{}", brief(&r));
        let r = ev(&s, "KEYS = {}");
        assert!(r.is_error(), "{}", brief(&r));
        // pcall can catch it, the script goes on.
        let r = ev(
            &s,
            "local ok, e = pcall(function() return nothere end) return {tostring(ok), type(e)}",
        );
        assert_eq!(r, Reply::Array(vec![bulk("false"), bulk("string")]));
        assert_canary(&s, "undeclared globals");
    });
}

#[test]
fn global_tampering_does_not_leak_into_later_scripts() {
    guarded("tamper", 120, || {
        let (_db, s) = fresh();
        // (script, must it raise?)
        let attacks: &[(&str, Option<bool>)] = &[
            ("redis.call = nil", Some(true)),
            ("redis.call = function() return 'pwned' end", Some(true)),
            ("redis.pcall = nil", Some(true)),
            ("redis.error_reply = nil", Some(true)),
            ("string.rep = nil", Some(true)),
            ("string.rep = function() return 'pwned' end", Some(true)),
            ("string.format = nil", Some(true)),
            ("table.insert = nil", Some(true)),
            ("table = nil", Some(true)),
            ("string = nil", Some(true)),
            ("redis = nil", Some(true)),
            ("_G = nil", Some(true)),
            ("_G.string = nil", Some(true)),
            ("KEYS = nil", Some(true)),
            ("ARGV = nil", Some(true)),
            ("rawset(_G, 'string', nil)", Some(true)),
            ("rawset(_G, 'x', 1)", Some(true)),
            ("rawset(string, 'rep', nil)", Some(true)),
            ("rawset(table, 'insert', 1)", Some(true)),
            ("rawset(redis, 'call', nil)", Some(true)),
            ("rawset(math, 'floor', nil)", Some(true)),
            ("getmetatable('').__index = nil", Some(true)),
            ("getmetatable('').__index.rep = nil", Some(true)),
            ("getmetatable('').__metatable = nil", Some(true)),
            ("setmetatable(_G, nil)", Some(true)),
            ("setmetatable(_G, {__index = function() return 1 end})", Some(true)),
            ("setmetatable(string, nil)", Some(true)),
            ("setmetatable(redis, {})", Some(true)),
            ("setmetatable(math, {})", Some(true)),
            ("getfenv(1).string = nil", Some(true)),
            ("setfenv(1, {})", Some(true)),
            ("local mt = getmetatable(_G) if mt then mt.__index = nil end", None),
            ("local mt = getmetatable(redis) if mt then mt.__newindex = nil mt.__index = nil end", None),
            ("rawequal = nil", Some(true)),
            ("pcall = nil", Some(true)),
            ("error = nil", Some(true)),
            ("tostring = nil", Some(true)),
            ("math.floor = nil", Some(true)),
            ("math.huge = 1", Some(true)),
            ("_VERSION = 'x'", Some(true)),
            ("unpack = nil", Some(true)),
            ("select = nil", Some(true)),
            ("local s = ('x') s.y = 1", Some(true)),
            ("debug = {}", Some(true)),
            ("os = {}", Some(true)),
            ("coroutine = {}", Some(true)),
            ("KEYS[1] = 'zz' ARGV[1] = 'zz' table.insert(KEYS, 'q') table.insert(ARGV, 'q')", None),
            ("KEYS.evil = redis.call KEYS.stash = string.rep ARGV.stash = redis.call", None),
            ("local k = KEYS k.evil = redis.pcall k.n = string.rep", None),
            ("setmetatable(KEYS, {__index = function() return 'pwned' end})", None),
            ("setmetatable(ARGV, {__newindex = function() end, __index = function() return 'pwned' end})", None),
            ("local t = setmetatable({}, {__index = string}) t.rep = nil", None),
            ("math.randomseed(1)", None),
            ("local s = string local r = s.rep s = nil", None),
        ];
        for (attack, must_raise) in attacks {
            let r = eval(&s, attack, &["k"], &["a"]);
            if let Some(true) = must_raise {
                assert!(r.is_error(), "attack {attack:?} succeeded: {}", brief(&r));
            }
            assert_canary(&s, attack);
            // Twice, so a cached compiled function is also covered.
            let r2 = eval(&s, attack, &["k"], &["a"]);
            assert_eq!(
                r2.is_error(),
                r.is_error(),
                "attack {attack:?} behaves differently the second time"
            );
            assert_canary(&s, attack);
        }
        // Other sessions are affected by nothing either.
        let (_db2, s2) = fresh();
        assert_canary(&s2, "attacks (fresh db)");
    });
}

#[test]
fn math_random_state_across_scripts() {
    guarded("math_random", 30, || {
        let (_db, s) = fresh();
        let a = text(&ev(
            &s,
            "math.randomseed(42) return tostring(math.random(1000000))",
        ));
        let b = text(&ev(&s, "return tostring(math.random(1000000))"));
        let c = text(&ev(
            &s,
            "math.randomseed(42) return tostring(math.random(1000000))",
        ));
        let d = text(&ev(&s, "return tostring(math.random(1000000))"));
        assert_eq!(a, c);
        assert_eq!(b, d);
        // The generator state is shared by all scripts of the node: a script can
        // make a later script's math.random() predictable (documented, not asserted).
        eprintln!(
            "INFO math.random state persists across scripts: next value after seed(42) is {b}"
        );
    });
}

#[test]
fn errors_do_not_poison_later_scripts() {
    guarded("poison", 60, || {
        let (_db, s) = fresh();
        let script = "if tonumber(ARGV[1]) == 1 then error('boom') end return 'fine'";
        for i in 0..200 {
            let arg = if i % 2 == 0 { "1" } else { "2" };
            let r = eval(&s, script, &[], &[arg]);
            if arg == "1" {
                assert!(has_err(&r, "boom"), "{}", brief(&r));
            } else {
                assert_eq!(r, bulk("fine"));
            }
        }
        // Syntax errors.
        for bad in [
            "return (",
            "x = = 1",
            "return 'unterminated",
            "if then",
            "for",
            "return 1 1",
            "\0",
            "return }",
            "local function",
        ] {
            for _ in 0..2 {
                let r = ev(&s, bad);
                assert!(
                    has_err(&r, "ERR Error compiling script"),
                    "{bad:?}: {}",
                    brief(&r)
                );
            }
            assert_eq!(ev(&s, "return 1"), Reply::Int(1));
        }
        // Random garbage never panics.
        let mut rng = Rng::new(5);
        for _ in 0..300 {
            let len = rng.below(60) as usize;
            let junk = rng.bytes(len);
            let r = s.execute(&[b"EVAL".to_vec(), junk, b"0".to_vec()]);
            assert!(
                r.is_error() || matches!(r, Reply::Nil | Reply::Int(_)),
                "{}",
                brief(&r)
            );
        }
        assert_canary(&s, "error storm");
    });
}

#[test]
fn error_objects_of_every_kind() {
    guarded("error_objects", 60, || {
        let (_db, s) = fresh();
        let cases = [
            "error('msg')",
            "error('msg', 0)",
            "error('msg', 2)",
            "error({})",
            "error({err = 'MYERR custom'})",
            "error()",
            "error(nil)",
            "error(12)",
            "error(true)",
            "error(function() end)",
            "error(redis.error_reply('X custom'))",
            "error(setmetatable({}, {__tostring = function() return 'boom' end}))",
            "error(setmetatable({}, {__tostring = function() error('inner') end}))",
            "error(setmetatable({}, {__tostring = function() return {} end}))",
            "error(string.rep('x', 100000))",
            "error('a\\nb')",
            "error('a\\r\\nb')",
            "local ok, e = pcall(error) return tostring(ok)",
            "assert(false)",
            "assert(nil, 'assertion text')",
            "assert(false, {})",
            "return tostring(nil) .. nil",
            "return #nil",
            "return nil + 1",
            "return ('x')()",
            "local t = nil t.x = 1",
            "return string.rep()",
            "return string.rep('x', -1)",
            "return ('x'):bad()",
            "return unpack({}, 1, 1e8)",
            "return table.concat({{}})",
            "return string.format('%d', 'x')",
            "return string.format('%999999999d', 1)",
            "return setmetatable({}, {__index = function(t, k) return t[k] end}).x",
            "local function f() return f() + 1 end return f()",
            "local t = setmetatable({}, {__index = function(t, k) return t[k .. 'x'] end}) return t.a",
        ];
        for script in cases {
            let r = ev(&s, script);
            match &r {
                Reply::Error(e) => {
                    assert!(
                        e.starts_with("ERR"),
                        "{script}: error does not start with a code: {e:?}"
                    );
                    assert!(e.len() < 700, "{script}: error text is {} bytes", e.len());
                    assert!(!e.contains('\n'), "{script}: newline in error text");
                    eprintln!(
                        "INFO {script:<70} -> {}",
                        e.chars().take(110).collect::<String>()
                    );
                }
                other => eprintln!("INFO {script:<70} -> (no error) {}", brief(other)),
            }
        }
        assert_canary(&s, "error objects");
    });
}

#[test]
fn replies_cannot_forge_extra_replies() {
    guarded("forge", 30, || {
        let (_db, s) = fresh();
        let scripts = [
            "return redis.error_reply('ERR a\\r\\n+INJECTED\\r\\n')",
            "return redis.error_reply('a\\n:1')",
            "return redis.status_reply('OK\\r\\n:1\\r\\n')",
            "return redis.status_reply('a\\rb')",
            "return {err = 'x\\r\\n$3\\r\\nfoo'}",
            "return {ok = 'x\\r\\n$3\\r\\nfoo'}",
            "return {{err = 'x\\r\\n:1'}, {ok = 'y\\r\\n:2'}}",
            "error('a\\rb')",
            "error('a\\r\\n+OK')",
            "return redis.call('FOO\\r\\n+OK')",
            "return redis.pcall('FOO\\r\\n+OK')",
            "local r = redis.pcall('FOO\\r\\n+OK') return r.err",
        ];
        for script in scripts {
            let r = ev(&s, script);
            let wire = enc(&r);
            let (decoded, used) = decode_reply(&wire)
                .unwrap_or_else(|e| panic!("{script}: undecodable wire {wire:?}: {e}"))
                .unwrap_or_else(|| panic!("{script}: incomplete wire {wire:?}"));
            assert_eq!(
                used,
                wire.len(),
                "{script}: trailing bytes after one reply: {wire:?}"
            );
            let lines = wire.windows(2).filter(|w| *w == b"\r\n").count();
            // One CRLF per line-based element; bulks and arrays are checked by decode.
            if matches!(decoded, Reply::Error(_) | Reply::Simple(_)) {
                assert_eq!(lines, 1, "{script}: {wire:?}");
            }
            if let Reply::Array(items) = &decoded {
                assert_eq!(lines, 1 + items.len(), "{script}: {wire:?}");
            }
        }
        // Via the direct API too.
        let r = s.execute(&["NOSUCH\r\n+OK"]);
        let wire = enc(&r);
        assert_eq!(
            wire.windows(2).filter(|w| *w == b"\r\n").count(),
            1,
            "{wire:?}"
        );
    });
}

// ---------------------------------------------------------------------
// Resource abuse
// ---------------------------------------------------------------------

#[test]
fn memory_bombs_end_in_errors_and_engine_recovers() {
    guarded("bombs", 120, || {
        let db = open(Config {
            lua_memory_limit: 8 << 20,
            ..Config::default()
        });
        let s = session(&db);
        let bombs = [
            "return string.rep('x', 1e9)",
            "return #string.rep('abc', 1e9)",
            "local t = {} for i = 1, 1e9 do t[i] = i end return #t",
            "local t = {} while true do t[#t + 1] = {} end",
            "local s = 'x' while true do s = s .. s end",
            "local t = {} for i = 1, 1e9 do t[i] = tostring(i) .. 'padpadpadpad' end return #t",
            "local t = {} for i = 1, 1e9 do t['k' .. i] = i end return 1",
            "return table.concat({string.rep('x', 4e6), string.rep('y', 4e6), string.rep('z', 4e6)})",
        ];
        let normal =
            "local t = {} for i = 1, 50000 do t[i] = i end local s = string.rep('a', 1000000) \
                      return {#t, #s, redis.call('INCR', 'ok')}";
        for round in 0..50 {
            // The string.rep bomb 50 times, the whole set every 5th round.
            let set = if round % 5 == 0 {
                &bombs[..]
            } else {
                &bombs[..1]
            };
            for bomb in set {
                let r = ev(&s, bomb);
                assert!(
                    has_err(&r, "memory"),
                    "round {round}: {bomb} -> {}",
                    brief(&r)
                );
            }
            // The limit is not permanently used up: a script that needs
            // several MB still runs right after the bombs.
            let r = ev(&s, normal);
            assert_eq!(
                r,
                Reply::Array(vec![
                    Reply::Int(50_000),
                    Reply::Int(1_000_000),
                    Reply::Int(round + 1)
                ]),
                "round {round}"
            );
        }
        // Memory errors caught by the script itself.
        for _ in 0..20 {
            let r = ev(
                &s,
                "local ok, e = pcall(string.rep, 'x', 1e9) local ok2 = pcall(function() local t = {} while true do t[#t+1] = 'x' .. #t end end) \
                 return {tostring(ok), tostring(ok2), 'alive'}",
            );
            assert_eq!(
                r,
                Reply::Array(vec![bulk("false"), bulk("false"), bulk("alive")])
            );
            // (The first script after a swallowed OOM may fail once: see
            // `heap_filled_and_swallowed_by_pcall_does_not_poison_the_node`.)
            assert!(
                (0..3).any(|_| !ev(&s, normal).is_error()),
                "node stays out of memory"
            );
        }
        assert_canary(&s, "memory bombs");
        // State written before a bomb survives it (the Lua state is replaced, the keyspace is not).
        s.execute(&["SET", "survivor", "yes"]);
        assert!(has_err(&ev(&s, bombs[0]), "memory"));
        assert_eq!(s.execute(&["GET", "survivor"]), bulk("yes"));
        // Writes made before the out-of-memory error are kept (no rollback).
        let r = ev(
            &s,
            "redis.call('SET', 'partial', '1') local s = string.rep('x', 1e9) return 1",
        );
        assert!(has_err(&r, "memory"));
        assert_eq!(s.execute(&["GET", "partial"]), bulk("1"));
    });
}

#[test]
fn lua_memory_limit_edge_values() {
    guarded("limit_edges", 60, || {
        for limit in [0usize, 1, 1000, 100_000, 1 << 20] {
            let db = open(Config {
                lua_memory_limit: limit,
                ..Config::default()
            });
            let s = session(&db);
            for script in [
                "return 1",
                "return string.rep('x', 10)",
                "return {1,2,3}",
                "local t = {} for i=1,100 do t[i]=i end return #t",
            ] {
                let r = ev(&s, script);
                // Either it fits or it fails cleanly; never a panic or a hang.
                assert!(matches!(
                    r,
                    Reply::Int(_) | Reply::Bulk(_) | Reply::Array(_) | Reply::Error(_)
                ));
            }
            eprintln!(
                "INFO lua_memory_limit={limit}: `return 1` -> {}",
                brief(&ev(&s, "return 1"))
            );
        }
    });
}

#[test]
fn large_values_pass_through_scripts() {
    guarded("large_values", 90, || {
        let (_db, s) = fresh();
        let mut rng = Rng::new(3);
        let big = rng.bytes(10 << 20);
        assert_eq!(
            s.execute(&[b"SET".to_vec(), b"big".to_vec(), big.clone()]),
            Reply::Ok
        );
        // A 10 MB value read inside a script and returned (default limit 64 MB).
        let r = s.execute(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "big"]);
        assert!(
            r == Reply::Bulk(big.clone()),
            "10 MB GET inside a script: {}",
            brief(&r)
        );
        // A 5 MB argument written from a script.
        let arg = rng.bytes(5 << 20);
        let r = s.execute(&[
            b"EVAL".to_vec(),
            b"return redis.call('SET', KEYS[1], ARGV[1])".to_vec(),
            b"1".to_vec(),
            b"k5".to_vec(),
            arg.clone(),
        ]);
        assert!(same(&r, &Reply::Ok), "{}", brief(&r));
        assert_eq!(s.execute(&["GET", "k5"]), Reply::Bulk(arg));
        // A script source of 1 MB.
        let src = format!("-- {}\nreturn 42", "x".repeat(1 << 20));
        assert_eq!(ev(&s, &src), Reply::Int(42));
    });
}

#[test]
fn million_element_results() {
    guarded("million", 120, || {
        let db = open(Config {
            lua_memory_limit: 256 << 20,
            ..Config::default()
        });
        let s = session(&db);
        let r = ev(
            &s,
            "local t = {} for i = 1, 1000000 do t[i] = i end return t",
        );
        let items = array(r);
        assert_eq!(items.len(), 1_000_000);
        assert_eq!(items[0], Reply::Int(1));
        assert_eq!(items[999_999], Reply::Int(1_000_000));
        // Past the internal item budget the reply is an error (Redis would
        // return all of it); only "no crash, bounded time" is required.
        let start = Instant::now();
        let r = ev(
            &s,
            "local t = {} for i = 1, 2000000 do t[i] = i end return t",
        );
        eprintln!(
            "INFO 2,000,000-element table -> {} in {:?}",
            if r.is_error() { brief(&r) } else { "ok".into() },
            start.elapsed()
        );
        // Lots of tiny tables.
        let r = ev(
            &s,
            "local t = {} for i = 1, 200000 do t[i] = {i, 'x'} end return t",
        );
        assert_eq!(array(r).len(), 200_000);
        // Big keyspace read from inside a script.
        let r = ev(
            &s,
            "for i = 1, 100000 do redis.call('HSET', 'big', 'f' .. i, i) redis.call('SET', 'k' .. i, i) end return 1",
        );
        assert_eq!(r, Reply::Int(1));
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(100_001));
        let r = ev(&s, "local h = redis.call('HGETALL', 'big') local k = redis.call('KEYS', '*') return {#h, #k}");
        assert_eq!(r, ints(&[200_000, 100_001]));
        let r = ev(&s, "return redis.call('HGETALL', 'big')");
        assert_eq!(array(r).len(), 200_000);
        let r = ev(&s, "return redis.call('MGET', unpack({'k1', 'k2', 'k3'}))");
        assert_eq!(array(r).len(), 3);
    });
}

#[test]
fn stack_abuse_is_an_error_not_a_crash() {
    guarded("stack", 60, || {
        let (_db, s) = fresh();
        for script in [
            "local function f(n) return 1 + f(n + 1) end return f(1)",
            "local t = {} for i = 1, 100000 do t = {t} end return 1",
            "return unpack({}, 1, 1e7)",
            "local t = {} for i = 1, 1e5 do t[i] = 'k' .. i end return redis.call('MSET', unpack(t))",
            "return string.rep('x', 1e6):gsub('x', 'y'):len()",
            "return #string.rep('x', 1e6):rep(2)",
            "return select('#', string.byte(string.rep('x', 1e5), 1, -1))",
            "local s = string.rep('(', 1000) return string.find('x', s)",
        ] {
            let r = ev(&s, script);
            assert!(matches!(r, Reply::Error(_) | Reply::Int(_) | Reply::Bulk(_) | Reply::Nil), "{script}: {}", brief(&r));
            eprintln!("INFO {script:<80} -> {}", brief(&r).chars().take(100).collect::<String>());
        }
        assert_canary(&s, "stack abuse");
    });
}

// ---------------------------------------------------------------------
// Scripts as transactions, cache
// ---------------------------------------------------------------------

#[test]
fn concurrent_read_modify_write_scripts_are_isolated() {
    guarded("isolation", 120, || {
        let db = open(Config::default());
        let threads = 8;
        let per = 400;
        thread::scope(|sc| {
            for t in 0..threads {
                let db = &db;
                sc.spawn(move || {
                    let s = session(db);
                    for i in 0..per {
                        let script = "local v = tonumber(redis.call('GET', KEYS[1]) or '0') \
                                      redis.call('SET', KEYS[1], v + 1) \
                                      local w = tonumber(redis.call('GET', KEYS[2]) or '0') \
                                      redis.call('SET', KEYS[2], w + 2) return v";
                        let r = eval(&s, script, &["ca", "cb"], &[]);
                        assert!(matches!(r, Reply::Int(_)), "{}", brief(&r));
                        if (t + i) % 7 == 0 {
                            // Plain commands interleave without breaking the totals.
                            assert!(!s.execute(&["MGET", "ca", "cb"]).is_error());
                        }
                    }
                });
            }
        });
        let s = session(&db);
        assert_eq!(
            s.execute(&["GET", "ca"]),
            bulk(&(threads * per).to_string())
        );
        assert_eq!(
            s.execute(&["GET", "cb"]),
            bulk(&(2 * threads * per).to_string())
        );
    });
}

#[test]
fn script_cache_churn() {
    guarded("cache", 120, || {
        let (_db, s) = fresh();
        // More distinct scripts than the cache holds, interleaved with reuse.
        for i in 0..2600 {
            let r = ev(&s, &format!("return {i}"));
            assert_eq!(r, Reply::Int(i), "script {i}");
            if i % 3 == 0 {
                assert_eq!(ev(&s, "return 'hot'"), bulk("hot"));
            }
            if i % 500 == 0 {
                assert_eq!(ev(&s, &format!("return {}", i / 2)), Reply::Int(i / 2));
            }
        }
        for i in (0..2600).step_by(97) {
            assert_eq!(ev(&s, &format!("return {i}")), Reply::Int(i));
        }
        assert_canary(&s, "cache churn");
    });
}

#[test]
fn script_writes_are_visible_and_counted() {
    guarded("writes", 60, || {
        let (_db, s) = fresh();
        let r = ev(&s, "for i = 1, 1000 do redis.call('SET', 'k' .. i, i) end redis.call('HSET', 'h', 'a', 1) return redis.call('DBSIZE')");
        // DBSIZE inside a script: see `ping_echo_dbsize_callable_from_scripts`.
        let _ = r;
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(1001));
        let r = ev(
            &s,
            "redis.call('DEL', 'h', 'k1', 'k2') return redis.call('EXISTS', 'k1', 'k3', 'k3')",
        );
        assert_eq!(r, Reply::Int(2));
        assert_eq!(s.execute(&["DBSIZE"]), Reply::Int(998));
        assert_eq!(
            ev(&s, "return redis.call('KEYS', 'k99?')").clone(),
            Reply::Array(array(s.execute(&["KEYS", "k99?"])))
        );
    });
}

// ---------------------------------------------------------------------
// SCRIPT KILL
// ---------------------------------------------------------------------

struct Running {
    rx: mpsc::Receiver<(Reply, Instant)>,
    handle: thread::JoinHandle<()>,
}

fn spawn_script(db: &Db, script: &str) -> Running {
    let s = session(db);
    let script = script.to_string();
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let r = eval(&s, &script, &[], &[]);
        let _ = tx.send((r, Instant::now()));
    });
    Running { rx, handle }
}

impl Running {
    fn finish(self, within: Duration) -> (Reply, Instant) {
        let got = self.rx.recv_timeout(within).unwrap_or_else(|_| {
            panic!("script still running {within:?} after it should have ended")
        });
        self.handle.join().unwrap();
        got
    }
}

/// Retry SCRIPT KILL until a running script acknowledges it.
fn kill_when_running(db: &Db) -> Instant {
    let s = session(db);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match s.execute(&["SCRIPT", "KILL"]) {
            Reply::Ok => return Instant::now(),
            r if has_err(&r, "NOTBUSY") => {}
            other => panic!("SCRIPT KILL -> {}", brief(&other)),
        }
        assert!(Instant::now() < deadline, "script never started");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn script_kill_with_nothing_running() {
    guarded("notbusy", 30, || {
        let (_db, s) = fresh();
        let r = s.execute(&["SCRIPT", "KILL"]);
        assert!(has_err(&r, "NOTBUSY"), "{}", brief(&r));
        assert_eq!(err_text(&r), "NOTBUSY No scripts in execution right now.");
        assert!(has_err(&s.execute(&["script", "kill"]), "NOTBUSY"));
        assert_eq!(ev(&s, "return 1"), Reply::Int(1));
        assert!(has_err(&s.execute(&["SCRIPT", "KILL"]), "NOTBUSY"));
        assert!(has_err(&s.execute(&["SCRIPT"]), "wrong number"));
        assert!(has_err(
            &s.execute(&["SCRIPT", "KILL", "x"]),
            "wrong number"
        ));
        assert!(has_err(
            &s.execute(&["SCRIPT", "NOPE"]),
            "unknown SCRIPT subcommand"
        ));
    });
}

#[test]
fn kill_stops_an_infinite_loop_promptly() {
    guarded("kill_loop", 60, || {
        let (db, s) = fresh();
        for script in [
            "while true do end",
            "repeat until false",
            "local i = 0 while true do i = i + 1 end",
            "for i = 1, 1e300 do end",
            "while true do redis.call('GET', 'k') end",
            "while true do redis.pcall('GET', 'k') end",
            "local t = {} while true do t[#t % 100 + 1] = string.rep('x', 100) end",
            "local function f() while true do end end f()",
            "table.sort({3, 2, 1}, function(a, b) while true do end end)",
            "return string.gsub('x', 'x', function() while true do end end)",
        ] {
            let running = spawn_script(&db, script);
            let acked = kill_when_running(&db);
            let (reply, ended) = running.finish(KILL_BOUND);
            assert!(has_err(&reply, "killed"), "{script}: {}", brief(&reply));
            let took = ended.saturating_duration_since(acked);
            assert!(took < KILL_BOUND, "{script}: died {took:?} after the kill");
            eprintln!("INFO killed {script:<60} within {:.2} ms", ms(took));
            assert_eq!(ev(&s, "return 'after'"), bulk("after"));
            assert!(has_err(&s.execute(&["SCRIPT", "KILL"]), "NOTBUSY"));
        }
    });
}

#[test]
fn kill_defeats_scripts_that_swallow_errors() {
    guarded("kill_swallow", 60, || {
        let (db, s) = fresh();
        for script in [
            "while true do pcall(function() while true do end end) end",
            "while true do xpcall(function() while true do end end, function(e) return e end) end",
            "while true do pcall(function() error('x') end) end",
            "while true do pcall(pcall, function() while true do end end) end",
            "local function f() while true do pcall(f) end end f()",
            "while true do pcall(function() pcall(function() pcall(function() while true do end end) end) end) end",
            "while true do local ok, e = pcall(function() while true do end end) if not ok then e = nil end end",
            "while true do pcall(redis.call, 'GET', 'h') end",
            "while true do pcall(string.rep, 'x', 1e3) end",
            "local t = setmetatable({}, {__index = function() while true do pcall(function() while true do end end) end end}) return t.x",
        ] {
            eprintln!("INFO trying to kill: {script}");
            let running = spawn_script(&db, script);
            let acked = kill_when_running(&db);
            let (reply, ended) = running.finish(KILL_BOUND);
            assert!(reply.is_error(), "{script}: {}", brief(&reply));
            let took = ended.saturating_duration_since(acked);
            assert!(took < KILL_BOUND, "{script}: died {took:?} after the kill");
            assert_eq!(ev(&s, "return 'after'"), bulk("after"), "{script}");
        }
    });
}

/// Once a script has written, SCRIPT KILL refuses (UNKILLABLE). The spike has
/// no SHUTDOWN NOSAVE and no other way in: such a script cannot be stopped
/// until it ends by itself, so this one is bounded by an iteration count.
#[test]
fn kill_after_a_write_is_refused_and_script_finishes_by_itself() {
    guarded("unkillable", 120, || {
        let (db, s) = fresh();
        let running = spawn_script(
            &db,
            "redis.call('SET', 'w', '1') for i = 1, 1e8 do end return 'done'",
        );
        // DBSIZE is a connection-level command: it answers while the script
        // holds the node, and its count is updated by the script's writes.
        let deadline = Instant::now() + Duration::from_secs(20);
        while s.execute(&["DBSIZE"]) != Reply::Int(1) {
            assert!(Instant::now() < deadline, "the write never became visible");
            thread::sleep(Duration::from_micros(200));
        }
        let first = s.execute(&["SCRIPT", "KILL"]);
        assert!(has_err(&first, "UNKILLABLE"), "{}", brief(&first));
        assert!(
            running.rx.try_recv().is_err(),
            "script was already over; the loop is too short for this test"
        );
        let again = s.execute(&["SCRIPT", "KILL"]);
        assert!(has_err(&again, "UNKILLABLE"), "{}", brief(&again));
        let started = Instant::now();
        let (reply, _) = running.finish(Duration::from_secs(60));
        eprintln!(
            "INFO unkillable script ended by itself {:.0} ms after the refused kill",
            ms(started.elapsed())
        );
        assert_eq!(reply, bulk("done"));
        assert_eq!(s.execute(&["GET", "w"]), bulk("1"));
        assert!(has_err(&s.execute(&["SCRIPT", "KILL"]), "NOTBUSY"));
        assert_eq!(ev(&s, "return 1"), Reply::Int(1));
    });
}

#[test]
fn kill_does_not_leak_into_the_next_script() {
    guarded("kill_leak", 120, || {
        let (db, s) = fresh();
        for round in 0..15 {
            let running = spawn_script(&db, "while true do end");
            kill_when_running(&db);
            let (reply, _) = running.finish(KILL_BOUND);
            assert!(has_err(&reply, "killed"), "{}", brief(&reply));
            for i in 0..1000 {
                let r = ev(&s, "local x = 0 for i = 1, 10 do x = x + i end return x");
                assert_eq!(
                    r,
                    Reply::Int(55),
                    "round {round}, script {i} was affected by the earlier kill"
                );
            }
            assert!(has_err(&s.execute(&["SCRIPT", "KILL"]), "NOTBUSY"));
        }
        // Scripts long enough to cross the instruction-count hook many times.
        let r = ev(&s, "local x = 0 for i = 1, 1e6 do x = x + 1 end return x");
        assert_eq!(r, Reply::Int(1_000_000));
    });
}

/// SCRIPT KILL racing the natural end of short scripts. A script that ended
/// can only have been killed if some SCRIPT KILL call overlapped it in time;
/// a kill that is "left over" from before it started is a leak.
#[test]
fn kill_racing_the_natural_end_never_hits_an_innocent_script() {
    guarded("kill_race", 120, || {
        let db = open(Config::default());
        let base = Instant::now();
        let stop = AtomicBool::new(false);
        let acked = AtomicU64::new(0);
        let (db, stop, acked) = (&db, &stop, &acked);
        let (kills, runs) = thread::scope(|sc| {
            let killer = sc.spawn(move || {
                let s = session(db);
                let mut rng = Rng::new(77);
                let mut kills: Vec<(u64, u64)> = Vec::new();
                while !stop.load(SeqCst) {
                    for _ in 0..rng.below(3000) {
                        std::hint::spin_loop();
                    }
                    let start = base.elapsed().as_nanos() as u64;
                    let r = s.execute(&["SCRIPT", "KILL"]);
                    let end = base.elapsed().as_nanos() as u64;
                    match r {
                        Reply::Ok => {
                            acked.fetch_add(1, SeqCst);
                            kills.push((start, end));
                        }
                        r if has_err(&r, "NOTBUSY") => kills.push((start, end)),
                        other => panic!("SCRIPT KILL -> {}", brief(&other)),
                    }
                }
                kills
            });
            let runner = sc.spawn(move || {
                let s = session(db);
                let mut runs: Vec<(u64, u64, bool)> = Vec::new();
                for _ in 0..20_000 {
                    let start = base.elapsed().as_nanos() as u64;
                    let r = ev(&s, "local x = 0 for i = 1, 50 do x = x + i end return x");
                    let end = base.elapsed().as_nanos() as u64;
                    let killed = match &r {
                        Reply::Int(1275) => false,
                        r if has_err(r, "killed") => true,
                        other => panic!("unexpected reply {}", brief(other)),
                    };
                    runs.push((start, end, killed));
                }
                stop.store(true, SeqCst);
                runs
            });
            let runs = runner
                .join()
                .unwrap_or_else(|e| std::panic::resume_unwind(e));
            stop.store(true, SeqCst);
            (
                killer
                    .join()
                    .unwrap_or_else(|e| std::panic::resume_unwind(e)),
                runs,
            )
        });
        let killed: Vec<&(u64, u64, bool)> = runs.iter().filter(|r| r.2).collect();
        eprintln!(
            "INFO kill race: {} kill calls ({} acknowledged), {} of {} short scripts were killed",
            kills.len(),
            acked.load(SeqCst),
            killed.len(),
            runs.len()
        );
        for (start, end, _) in &killed {
            let overlapped = kills.iter().any(|(ks, ke)| ks <= end && ke >= start);
            assert!(overlapped, "a script ({start}..{end} ns) was killed while no SCRIPT KILL call was in flight: leaked kill");
        }
        let s = session(db);
        assert_eq!(ev(&s, "return 5"), Reply::Int(5));
        assert!(has_err(&s.execute(&["SCRIPT", "KILL"]), "NOTBUSY"));
    });
}

#[test]
fn kill_storm_with_many_killers_and_runners() {
    guarded("kill_storm", 120, || {
        let db = open(Config::default());
        let stop = AtomicBool::new(false);
        let finished = AtomicU64::new(0);
        let (db, stop, finished) = (&db, &stop, &finished);
        thread::scope(|sc| {
            let runners: Vec<_> = (0..3)
                .map(|r| {
                    sc.spawn(move || {
                        let s = session(db);
                        let mut killed = 0u32;
                        for i in 0..4000 {
                            let script = if i % 50 == 0 {
                                // longer, killable
                                "local x = 0 for i = 1, 20000 do x = x + 1 end return 'long'"
                            } else if r == 0 && i % 5 == 0 {
                                "redis.call('INCR', 'storm') return 'wrote'"
                            } else {
                                "return 'short'"
                            };
                            let reply = ev(&s, script);
                            match &reply {
                                Reply::Bulk(b)
                                    if b == b"long" || b == b"short" || b == b"wrote" => {}
                                r if has_err(r, "killed") => killed += 1,
                                other => panic!("unexpected reply {}", brief(other)),
                            }
                        }
                        finished.fetch_add(1, SeqCst);
                        killed
                    })
                })
                .collect();
            for k in 0..4 {
                sc.spawn(move || {
                    let s = session(db);
                    let mut rng = Rng::new(k);
                    while !stop.load(SeqCst) {
                        let r = s.execute(&["SCRIPT", "KILL"]);
                        assert!(
                            r == Reply::Ok || has_err(&r, "NOTBUSY") || has_err(&r, "UNKILLABLE"),
                            "SCRIPT KILL -> {}",
                            brief(&r)
                        );
                        for _ in 0..rng.below(500) {
                            std::hint::spin_loop();
                        }
                    }
                });
            }
            let mut total_killed = 0;
            for r in runners {
                total_killed += r.join().unwrap_or_else(|e| std::panic::resume_unwind(e));
            }
            stop.store(true, SeqCst);
            eprintln!("INFO kill storm: {total_killed} scripts killed, all runners finished");
        });
        let s = session(db);
        assert_eq!(finished.load(SeqCst), 3);
        assert_eq!(ev(&s, "return 1"), Reply::Int(1));
    });
}

#[test]
fn commands_during_a_running_script() {
    guarded("during_script", 120, || {
        let (db, s) = fresh();
        s.execute(&["SET", "pre", "1"]);
        let before = db.stats().exclusive;
        let running = spawn_script(&db, "while true do end");
        let deadline = Instant::now() + Duration::from_secs(20);
        while db.stats().exclusive == before {
            assert!(Instant::now() < deadline, "script did not start");
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(100));

        // Plain keyspace commands from other sessions wait for the script.
        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::new();
        let cmds: Vec<Vec<&str>> = vec![
            vec!["SET", "during", "v"],
            vec!["GET", "pre"],
            vec!["MGET", "pre", "during"],
            vec!["DEL", "nothing", "pre2"],
            vec!["KEYS", "*"],
            vec!["HSET", "h", "f", "v"],
            vec!["EXISTS", "pre"],
        ];
        for (i, cmd) in cmds.iter().cloned().enumerate() {
            let sess = session(&db);
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                let r = sess.execute(&cmd);
                let _ = tx.send((i, r));
            }));
        }
        let queued_eval = {
            let sess = session(&db);
            let tx = tx.clone();
            thread::spawn(move || {
                let r = eval(&sess, "return redis.call('GET', 'during')", &[], &[]);
                let _ = tx.send((100, r));
            })
        };
        thread::sleep(Duration::from_millis(300));
        assert!(
            rx.try_recv().is_err(),
            "a keyspace command completed while a script was holding the node"
        );

        // Connection-level commands answer immediately.
        let t0 = Instant::now();
        assert_eq!(s.execute(&["PING"]), Reply::Simple("PONG".into()));
        assert_eq!(s.execute(&["PING", "x"]), bulk("x"));
        assert_eq!(s.execute(&["ECHO", "e"]), bulk("e"));
        assert!(matches!(s.execute(&["DBSIZE"]), Reply::Int(_)));
        assert!(matches!(s.execute(&["INFO"]), Reply::Bulk(_)));
        assert_eq!(s.execute(&["CLIENT", "SETNAME", "x"]), Reply::Ok);
        assert_eq!(s.execute(&["SELECT", "0"]), Reply::Ok);
        assert!(matches!(s.execute(&["COMMAND", "COUNT"]), Reply::Int(n) if n > 150));
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "connection-level commands took {:?}",
            t0.elapsed()
        );
        eprintln!("INFO PING/ECHO/DBSIZE/INFO/CLIENT/SELECT/COMMAND during a script: {:.2} ms total (Redis would answer BUSY)", ms(t0.elapsed()));
        assert!(rx.try_recv().is_err());

        // Kill: everything completes, correctly and in a consistent order.
        kill_when_running(&db);
        let (reply, _) = running.finish(KILL_BOUND);
        assert!(has_err(&reply, "killed"));
        let mut results = std::collections::BTreeMap::new();
        for _ in 0..cmds.len() + 1 {
            let (i, r) = rx
                .recv_timeout(Duration::from_secs(10))
                .expect("blocked command never completed after the kill");
            results.insert(i, r);
        }
        for h in handles {
            h.join().unwrap();
        }
        queued_eval.join().unwrap();
        assert_eq!(results[&0], Reply::Ok);
        assert_eq!(results[&1], bulk("1"));
        assert_eq!(results[&3], Reply::Int(0));
        assert_eq!(results[&5], Reply::Int(1));
        assert_eq!(results[&6], Reply::Int(1));
        let keys = array(results.remove(&4).unwrap());
        assert!(keys.iter().any(|k| *k == bulk("pre")));
        // The queued EVAL and the MGET ran after the SET, in some serial order:
        // both saw either nothing or "v" for `during`, never an error.
        assert!(matches!(results[&100], Reply::Bulk(_) | Reply::Nil));
        assert_eq!(s.execute(&["GET", "during"]), bulk("v"));
        assert_eq!(ev(&s, "return 1"), Reply::Int(1));
    });
}

// ---------------------------------------------------------------------
// Scripts that can take the whole process down or never end. Each runs in
// a child process (this test binary, re-invoked) so a crash or a runaway
// script fails one test instead of aborting or wedging the whole run.
// ---------------------------------------------------------------------

const CHILD_ENV: &str = "HCACHE_SCRIPT_CHILD";

/// (what, script, must the script be killable by SCRIPT KILL)
const CHILD_CASES: &[(&str, &str, bool)] = &[
    (
        "pcall tail recursion, results accumulate",
        "local function f() return pcall(f) end return f()",
        false,
    ),
    (
        "pcall recursion keeping the result",
        "local function f() local ok = pcall(f) return ok end return f()",
        false,
    ),
    (
        "xpcall recursion",
        "local function f() return xpcall(f, function(e) return e end) end return f()",
        false,
    ),
    (
        "error handler that loops, after error()",
        "while true do xpcall(function() error('x') end, function(e) while true do end end) end",
        true,
    ),
    (
        "error handler that loops, after a runtime error",
        "xpcall(function() local x = nil + 1 end, function() while true do end end)",
        true,
    ),
    (
        "error handler that loops, xpcall(error, ...)",
        "xpcall(error, function() while true do end end)",
        true,
    ),
];

#[test]
fn child_case() {
    let Ok(index) = std::env::var(CHILD_ENV) else {
        return; // only does something when re-invoked by `dangerous_scripts_*`
    };
    let (what, script, killable) = CHILD_CASES[index.parse::<usize>().unwrap()];
    let (db, s) = fresh();
    if killable {
        let running = spawn_script(&db, script);
        let acked = kill_when_running(&db);
        match running.rx.recv_timeout(Duration::from_secs(3)) {
            Ok((reply, ended)) => {
                assert!(reply.is_error(), "{what}: {}", brief(&reply));
                eprintln!(
                    "{what}: killed {:.1} ms after the kill",
                    ms(ended.saturating_duration_since(acked))
                );
            }
            Err(_) => {
                eprintln!(
                    "NOT KILLABLE: `{script}` still running 3 s after SCRIPT KILL was acknowledged"
                );
                std::process::exit(3);
            }
        }
    } else {
        let r = ev(&s, script);
        eprintln!("{what}: {}", brief(&r));
    }
    assert_canary(&s, what);
}

fn run_child(index: usize) -> Result<(), String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    let exe = std::env::current_exe().expect("current exe");
    let mut child = Command::new(exe)
        .args(["--exact", "child_case", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, index.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child");
    let deadline = Instant::now() + Duration::from_secs(40);
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait") {
            break Some(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut output = String::new();
    if let Some(mut e) = child.stderr.take() {
        let _ = e.read_to_string(&mut output);
    }
    if let Some(mut o) = child.stdout.take() {
        let _ = o.read_to_string(&mut output);
    }
    let tail: String = output
        .lines()
        .filter(|l| !l.starts_with("running"))
        .take(8)
        .collect::<Vec<_>>()
        .join(" | ");
    match status {
        Some(st) if st.success() => Ok(()),
        Some(st) => Err(format!("child exited with {st} :: {tail}")),
        None => Err(format!("child still running after 40 s (killed) :: {tail}")),
    }
}

fn dangerous_scripts(killable: bool) {
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let mut failures = Vec::new();
    for (i, (what, script, k)) in CHILD_CASES.iter().enumerate() {
        if *k != killable {
            continue;
        }
        if let Err(e) = run_child(i) {
            failures.push(format!("{what}: `{script}`\n    {e}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) failed:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// Recursion through pcall piles up results until Lua's C-function stack
/// check trips. Lua is built with API checks in debug profiles, where the
/// failed assertion aborts the process; in release it is silently tolerated.
#[test]
fn dangerous_scripts_must_not_abort_the_process() {
    guarded("child_abort", 200, || dangerous_scripts(false));
}

/// A script that loops inside an `xpcall` error handler: the instruction hook
/// that implements SCRIPT KILL is switched off by Lua while it unwinds an
/// error raised by the hook itself, so the loop can never be interrupted.
#[test]
fn error_handler_loops_must_be_killable() {
    guarded("child_kill", 200, || dangerous_scripts(true));
}

/// A script that fills the Lua heap to the limit and swallows the
/// out-of-memory error with pcall leaves the heap full of garbage. The next
/// scripts must still be able to run (the collector has to reclaim it).
#[test]
fn heap_filled_and_swallowed_by_pcall_does_not_poison_the_node() {
    guarded("swallowed_oom", 60, || {
        let db = open(Config {
            lua_memory_limit: 8 << 20,
            ..Config::default()
        });
        let s = session(&db);
        let fill = "local ok = pcall(function() local t = {} while true do t[#t + 1] = 'x' .. #t end end) return tostring(ok)";
        let small = "local t = {} for i = 1, 50000 do t[i] = i end return #t";
        let mut failures_in_a_row = 0;
        let mut first_failure_text = String::new();
        for round in 0..10 {
            assert_eq!(
                ev(&s, fill),
                bulk("false"),
                "round {round}: the fill script itself"
            );
            for attempt in 0..30 {
                let r = ev(&s, small);
                if r == Reply::Int(50_000) {
                    break;
                }
                failures_in_a_row += 1;
                if first_failure_text.is_empty() {
                    first_failure_text = brief(&r);
                }
                assert!(
                    attempt < 29,
                    "round {round}: still `{}` after 30 attempts: the node is poisoned",
                    brief(&r)
                );
            }
        }
        eprintln!("INFO failures between a swallowed OOM and the first working script: {failures_in_a_row} ({first_failure_text})");
        assert_eq!(
            failures_in_a_row, 0,
            "scripts failed ({first_failure_text}) right after another script's swallowed out-of-memory"
        );
    });
}

#[test]
fn repeating_the_empty_string_returns_at_once() {
    guarded("string.rep empty", 20, || {
        let (_db, s) = fresh();
        let t = Instant::now();
        assert_eq!(ev(&s, "return #string.rep('', 2^50)"), Reply::Int(0));
        assert_eq!(ev(&s, "return #(''):rep(1e15)"), Reply::Int(0));
        assert_eq!(ev(&s, "return string.rep('ab', 3)"), bulk("ababab"));
        assert!(t.elapsed() < KILL_BOUND, "took {:?}", t.elapsed());
        assert_canary(&s, "after string.rep of the empty string");
    });
}

#[test]
fn scripts_too_long_to_cache_still_run_every_time() {
    guarded("long script", 60, || {
        let (_db, s) = fresh();
        let padding = "-- padding\n".repeat(4000);
        for round in 0..3 {
            let script = format!("{padding}return redis.call('INCR', 'n')");
            assert_eq!(ev(&s, &script), Reply::Int(round + 1), "round {round}");
        }
        assert_canary(&s, "after long scripts");
    });
}

#[test]
fn server_is_another_name_for_redis_as_in_valkey() {
    let (_db, s) = fresh();
    assert!(same(
        &ev(&s, "return server.call('SET', 'k', 'v')"),
        &Reply::Ok
    ));
    assert_eq!(ev(&s, "return server.pcall('GET', 'k')"), Reply::bulk("v"));
    assert_eq!(ev(&s, "return rawequal(server, redis)"), Reply::Int(1));
    let lib = "#!lua name=vk\nserver.register_function('get_k', function() return server.call('GET', 'k') end)";
    assert_eq!(s.execute(&["FUNCTION", "LOAD", lib]), Reply::bulk("vk"));
    assert_eq!(s.execute(&["FCALL", "get_k", "0"]), Reply::bulk("v"));
}
