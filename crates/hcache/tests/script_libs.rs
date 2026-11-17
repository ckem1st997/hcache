//! Tests of the Lua libraries scripts get from Redis (`cjson`, `cmsgpack`,
//! `struct`, `bit`) and of the `redis.*` helpers, through the embedded API.
//! Reference behaviour: Redis 7 (lua-cjson 2.1.0, lua-cmsgpack, `struct`,
//! LuaBitOp).

mod support;

use hcache::{Config, Db, Reply, Session};
use hcache_core::testutil::Rng;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use support::*;

const KILL_BOUND: Duration = Duration::from_secs(2);

/// Helpers every script below can use: `hex(s)`, `err(f, ...)` (the text of
/// the error `f` raises, or NOERR), `eq(a, b)` (deep equality).
const PRELUDE: &str = r#"
local function hex(s)
  return (string.gsub(s, '.', function(c) return string.format('%02x', string.byte(c)) end))
end
local function err(f, ...)
  local ok, e = pcall(f, ...)
  if ok then return 'NOERR' end
  return tostring(e)
end
local function eq(a, b)
  if type(a) ~= type(b) then return false end
  if type(a) ~= 'table' then return a == b end
  for k, v in pairs(a) do if not eq(v, b[k]) then return false end end
  for k in pairs(b) do if a[k] == nil then return false end end
  return true
end
"#;

fn fresh() -> (Db, Session) {
    let db = open(Config::default());
    let s = session(&db);
    (db, s)
}

fn ev(s: &Session, script: &str) -> Reply {
    eval(
        s,
        &format!(
            "{PRELUDE}
{script}"
        ),
        &[],
        &[],
    )
}

/// Run `return <expr>` with the prelude and give the string it returns.
fn run_text(s: &Session, expr: &str) -> String {
    let r = ev(s, &format!("{PRELUDE}\nreturn {expr}"));
    match r {
        Reply::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("{expr}: {}", brief(&other)),
    }
}

fn check_texts(s: &Session, cases: &[(&str, &str)]) {
    for (expr, want) in cases {
        assert_eq!(&run_text(s, expr), want, "{expr}");
    }
}

/// `(expr, needle)`: the expression evaluates to an error text that must
/// contain the needle.
fn check_errors(s: &Session, cases: &[(&str, &str)]) {
    for (expr, needle) in cases {
        let got = run_text(s, expr);
        assert!(
            got.contains(needle),
            "{expr}\n  want an error containing {needle:?}\n  got {got:?}"
        );
    }
}

fn check_numbers(s: &Session, cases: &[(&str, i64)]) {
    for (expr, want) in cases {
        let r = ev(s, &format!("{PRELUDE}\nreturn {expr}"));
        assert_eq!(r, Reply::Int(*want), "{expr}: {}", brief(&r));
    }
}

const CANARY: &str = r#"
assert(type(cjson.encode) == 'function' and type(cjson.decode) == 'function')
assert(type(cmsgpack.pack) == 'function' and type(cmsgpack.unpack) == 'function')
assert(type(struct.pack) == 'function' and type(bit.band) == 'function')
assert(type(redis.sha1hex) == 'function' and type(redis.call) == 'function')
assert(cjson.encode({1, 2}) == '[1,2]' and cjson.null ~= nil)
assert(bit.band(7, 3) == 3 and struct.size('i') == 4)
assert(redis.REDIS_VERSION == '7.4.0')
return 'canary-ok'
"#;

fn assert_canary(s: &Session, context: &str) {
    let r = ev(s, CANARY);
    assert!(
        r == bulk("canary-ok"),
        "after {context}: canary returned {}",
        brief(&r)
    );
}

// ---------------------------------------------------------------------
// bit
// ---------------------------------------------------------------------

#[test]
fn bit_known_answers() {
    guarded("bit_known", 60, || {
        let (_db, s) = fresh();
        check_numbers(
            &s,
            &[
                ("bit.tobit(0xffffffff)", -1),
                ("bit.tobit(-1)", -1),
                ("bit.tobit(2^32)", 0),
                ("bit.tobit(2^32 + 5)", 5),
                ("bit.tobit(2^31)", -2_147_483_648),
                ("bit.tobit(2^31 - 1)", 2_147_483_647),
                ("bit.tobit(-2^31 - 1)", 2_147_483_647),
                ("bit.tobit(1e10)", 1_410_065_408),
                ("bit.tobit(2^53)", 0),
                ("bit.tobit(2.5)", 2),
                ("bit.tobit(3.5)", 4),
                ("bit.tobit(-2.5)", -2),
                ("bit.tobit(0.4)", 0),
                ("bit.tobit('255')", 255),
                ("bit.band(0xff00, 0x0ff0)", 0x0f00),
                ("bit.band(-1, 0xff)", 255),
                ("bit.band(-1)", -1),
                ("bit.band(0xffffffff, 0xffffffff)", -1),
                ("bit.band(2^32 + 6, 3)", 2),
                ("bit.band(7, 6, 5, 4)", 4),
                ("bit.bor(1, 2, 4)", 7),
                ("bit.bor(0x80000000, 1)", -2_147_483_647),
                ("bit.bxor(5, 3)", 6),
                ("bit.bxor(-1, 0xffffffff)", 0),
                ("bit.bxor(1, 2, 4, 7)", 0),
                ("bit.bnot(0)", -1),
                ("bit.bnot(-1)", 0),
                ("bit.bnot(0x7fffffff)", -2_147_483_648),
                ("bit.lshift(1, 31)", -2_147_483_648),
                ("bit.lshift(1, 32)", 1),
                ("bit.lshift(1, 33)", 2),
                ("bit.lshift(-1, 1)", -2),
                ("bit.rshift(-1, 28)", 15),
                ("bit.rshift(-1, 0)", -1),
                ("bit.rshift(0x80000000, 31)", 1),
                ("bit.rshift(256, -28)", 16),
                ("bit.arshift(-256, 4)", -16),
                ("bit.arshift(0x80000000, 31)", -1),
                ("bit.arshift(0x7fffffff, 31)", 0),
                ("bit.rol(0x12345678, 8)", 878_082_066),
                ("bit.ror(0x12345678, 8)", 2_014_458_966),
                ("bit.rol(1, 32)", 1),
                ("bit.rol(0x80000000, 1)", 1),
                ("bit.ror(1, 1)", -2_147_483_648),
                ("bit.bswap(0x12345678)", 2_018_915_346),
                ("bit.bswap(1)", 16_777_216),
                ("bit.bswap(-1)", -1),
            ],
        );
        check_texts(
            &s,
            &[
                ("bit.tohex(255)", "000000ff"),
                ("bit.tohex(-1)", "ffffffff"),
                ("bit.tohex(255, 2)", "ff"),
                ("bit.tohex(255, -4)", "00FF"),
                ("bit.tohex(0x12345678, 3)", "678"),
                ("bit.tohex(0x12345678, -8)", "12345678"),
                ("bit.tohex(1, 0)", ""),
                ("bit.tohex(1, 12)", "00000001"),
                ("bit.tohex(0xabcdef01, -12)", "ABCDEF01"),
                ("bit.tohex(2^32 + 0xa)", "0000000a"),
            ],
        );
        check_errors(
            &s,
            &[
                (
                    "err(bit.band)",
                    "bad argument #1 to 'band' (number expected, got no value)",
                ),
                (
                    "err(bit.band, {}, 1)",
                    "bad argument #1 to 'band' (number expected, got table)",
                ),
                (
                    "err(bit.bor, 1, 'x')",
                    "bad argument #2 to 'bor' (number expected, got string)",
                ),
                (
                    "err(bit.lshift, 1)",
                    "bad argument #2 to 'lshift' (number expected, got no value)",
                ),
                ("err(bit.tobit, nil)", "number expected, got nil"),
                ("err(bit.tohex, 1, {})", "bad argument #2 to 'tohex'"),
            ],
        );
        // Non-finite input is undefined in LuaBitOp; it must not crash.
        check_numbers(
            &s,
            &[
                ("bit.tobit(1/0) * 0", 0),
                ("bit.tobit(0/0) * 0", 0),
                ("bit.band(1/0, -1) * 0", 0),
            ],
        );
        assert_canary(&s, "bit");
    });
}

// ---------------------------------------------------------------------
// redis.*
// ---------------------------------------------------------------------

#[test]
fn sha1hex_known_answers() {
    guarded("sha1hex", 30, || {
        let (_db, s) = fresh();
        check_texts(
            &s,
            &[
                (
                    "redis.sha1hex('')",
                    "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                ),
                (
                    "redis.sha1hex('abc')",
                    "a9993e364706816aba3e25717850c26c9cd0d89d",
                ),
                (
                    "redis.sha1hex(1)",
                    "356a192b7913b04c54574d18c28d46e6395428ab",
                ),
                (
                    "redis.sha1hex('The quick brown fox jumps over the lazy dog')",
                    "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12",
                ),
            ],
        );
        // Binary-safe: a NUL byte is part of the data.
        let want = {
            let a = text(&ev(&s, "return redis.sha1hex('a\\0b')"));
            let b = text(&ev(&s, "return redis.sha1hex('a')"));
            assert_ne!(a, b);
            a
        };
        assert_eq!(want.len(), 40);
        check_errors(
            &s,
            &[
                ("err(redis.sha1hex)", "wrong number of arguments"),
                ("err(redis.sha1hex, 'a', 'b')", "wrong number of arguments"),
                ("err(redis.sha1hex, {})", "string expected, got table"),
            ],
        );
        // The same hash EVALSHA uses.
        let sha = text(&ev(&s, "return redis.sha1hex('return 1')"));
        assert_eq!(s.execute(&["SCRIPT", "LOAD", "return 1"]), bulk(&sha));
    });
}

#[test]
fn redis_helper_functions_and_constants() {
    guarded("redis_helpers", 60, || {
        let (_db, s) = fresh();
        check_numbers(
            &s,
            &[
                ("redis.LOG_DEBUG", 0),
                ("redis.LOG_VERBOSE", 1),
                ("redis.LOG_NOTICE", 2),
                ("redis.LOG_WARNING", 3),
                ("redis.REDIS_VERSION_NUM == 0x00070400 and 1 or 0", 1),
                ("redis.REPL_NONE", 0),
                ("redis.REPL_AOF", 1),
                ("redis.REPL_SLAVE", 2),
                ("redis.REPL_REPLICA", 2),
                ("redis.REPL_ALL", 3),
            ],
        );
        check_texts(
            &s,
            &[
                ("redis.REDIS_VERSION", "7.4.0"),
                ("type(redis.REDIS_VERSION_NUM)", "number"),
                ("tostring(redis.breakpoint())", "false"),
                ("tostring(redis.replicate_commands())", "true"),
                ("tostring((redis.set_repl(redis.REPL_ALL)))", "nil"),
                ("tostring((redis.set_repl(redis.REPL_NONE)))", "nil"),
                ("tostring((redis.debug('x', 1)))", "nil"),
                ("tostring((redis.setresp(2)))", "nil"),
                ("tostring((redis.setresp(3)))", "nil"),
                (
                    "tostring((redis.log(redis.LOG_WARNING, 'a', 'b', 3)))",
                    "nil",
                ),
                ("tostring((redis.log(0, 'x')))", "nil"),
                ("tostring(select('#', redis.log(3, 'x')))", "0"),
            ],
        );
        check_errors(
            &s,
            &[
                ("err(redis.log)", "requires two arguments or more"),
                (
                    "err(redis.log, redis.LOG_DEBUG)",
                    "requires two arguments or more",
                ),
                (
                    "err(redis.log, 'x', 'y')",
                    "First argument must be a number",
                ),
                ("err(redis.log, 9, 'y')", "Invalid debug level"),
                ("err(redis.log, -1, 'y')", "Invalid debug level"),
                ("err(redis.setresp)", "requires one argument"),
                ("err(redis.setresp, 1)", "RESP version must be 2 or 3"),
                ("err(redis.setresp, 4)", "RESP version must be 2 or 3"),
                ("err(redis.setresp, 'x')", "RESP version must be 2 or 3"),
                ("err(redis.set_repl)", "requires one argument"),
                ("err(redis.set_repl, 99)", "Invalid replication flags"),
            ],
        );
        // Table conversions of script results.
        assert_eq!(
            ev(&s, "return {err = 'MYERR boom'}"),
            Reply::Error("MYERR boom".into())
        );
        assert_eq!(
            ev(&s, "return redis.error_reply('MYERR again')"),
            Reply::Error("MYERR again".into())
        );
        assert_eq!(ev(&s, "return {ok = 'FINE'}"), Reply::Simple("FINE".into()));
        assert_eq!(
            ev(&s, "return redis.status_reply('QUEUED')"),
            Reply::Simple("QUEUED".into())
        );
        assert_canary(&s, "redis helpers");
    });
}

#[test]
fn pcall_of_redis_call_returns_an_error_table() {
    guarded("pcall_call", 60, || {
        let (_db, s) = fresh();
        s.execute(&["HSET", "h", "f", "v"]);
        s.execute(&["SET", "a", "1"]);
        let r = ev(
            &s,
            "local ok, e = pcall(redis.call, 'INCR', 'h') \
             return {tostring(ok), type(e), string.sub(e.err, 1, 9)}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![bulk("false"), bulk("table"), bulk("WRONGTYPE")]),
            "{}",
            brief(&r)
        );
        let r = ev(
            &s,
            "local ok, e = pcall(redis.call) return {tostring(ok), type(e), tostring(e.err)}",
        );
        let items = array(r);
        assert_eq!(items[0], bulk("false"));
        assert_eq!(items[1], bulk("table"));
        assert!(text(&items[2]).contains("at least one argument"));
        // The table is the error reply when returned.
        let r = ev(&s, "local ok, e = pcall(redis.call, 'INCR', 'h') return e");
        assert!(has_err(&r, "WRONGTYPE"), "{}", brief(&r));
        assert!(!has_err(&r, "Error running script"), "{}", brief(&r));
        // Raised again, it is an error of the script that keeps its text.
        let r = ev(&s, "local ok, e = pcall(redis.call, 'INCR', 'h') error(e)");
        assert!(has_err(&r, "WRONGTYPE"), "{}", brief(&r));
        // An uncaught failure of redis.call keeps its text.
        let r = ev(&s, "return redis.call('INCR', 'h')");
        assert!(has_err(&r, "WRONGTYPE"), "{}", brief(&r));
        // The script goes on after a caught error.
        let r = ev(
            &s,
            "pcall(redis.call, 'INCR', 'h') local ok = pcall(redis.call, 'NOPE') \
             return redis.call('GET', 'a')",
        );
        assert_eq!(r, bulk("1"));
        // xpcall hands the table to the handler.
        let r = ev(
            &s,
            "local ok, e = xpcall(function() return redis.call('INCR', 'h') end, \
             function(e) return type(e) .. ':' .. string.sub(e.err, 1, 9) end) return e",
        );
        assert_eq!(r, bulk("table:WRONGTYPE"));
        // Successful calls are unchanged.
        assert_eq!(
            ev(
                &s,
                "local ok, v = pcall(redis.call, 'GET', 'a') return {tostring(ok), v}"
            ),
            Reply::Array(vec![bulk("true"), bulk("1")])
        );
        assert_eq!(
            ev(
                &s,
                "local ok, v = pcall(redis.call, 'GET', 'nokey') return tostring(v)"
            ),
            bulk("false")
        );
        assert_canary(&s, "pcall(redis.call)");
    });
}

// ---------------------------------------------------------------------
// struct
// ---------------------------------------------------------------------

#[test]
fn struct_pack_known_answers() {
    guarded("struct_pack", 60, || {
        let (_db, s) = fresh();
        check_texts(
            &s,
            &[
                ("hex(struct.pack('b', -1))", "ff"),
                ("hex(struct.pack('b', 127))", "7f"),
                ("hex(struct.pack('B', 255))", "ff"),
                ("hex(struct.pack('h', -2))", "feff"),
                ("hex(struct.pack('>h', -2))", "fffe"),
                ("hex(struct.pack('<h', 258))", "0201"),
                ("hex(struct.pack('H', 258))", "0201"),
                ("hex(struct.pack('>H', 258))", "0102"),
                ("hex(struct.pack('H', 65535))", "ffff"),
                ("hex(struct.pack('i', 1))", "01000000"),
                ("hex(struct.pack('>i', 1))", "00000001"),
                ("hex(struct.pack('i', -2))", "feffffff"),
                ("hex(struct.pack('>i', -2))", "fffffffe"),
                ("hex(struct.pack('I', 3735928559))", "efbeadde"),
                ("hex(struct.pack('>I', 3735928559))", "deadbeef"),
                ("hex(struct.pack('I', -1))", "ffffffff"),
                ("hex(struct.pack('I', 2^32))", "00000000"),
                ("hex(struct.pack('l', -1))", "ffffffffffffffff"),
                ("hex(struct.pack('L', 1))", "0100000000000000"),
                ("hex(struct.pack('>L', 1))", "0000000000000001"),
                ("hex(struct.pack('T', 1))", "0100000000000000"),
                ("hex(struct.pack('>T', 258))", "0000000000000102"),
                ("hex(struct.pack('i3', -2))", "feffff"),
                ("hex(struct.pack('>i3', 1))", "000001"),
                ("hex(struct.pack('i1', 5))", "05"),
                ("hex(struct.pack('I2', 65537))", "0100"),
                ("hex(struct.pack('i8', -2))", "feffffffffffffff"),
                ("hex(struct.pack('>i8', 1))", "0000000000000001"),
                (
                    "hex(struct.pack('i16', -1))",
                    "ffffffffffffffffffffffffffffffff",
                ),
                (
                    "hex(struct.pack('>I16', 1))",
                    "00000000000000000000000000000001",
                ),
                ("hex(struct.pack('f', 1.5))", "0000c03f"),
                ("hex(struct.pack('>f', 1.5))", "3fc00000"),
                ("hex(struct.pack('d', 1.5))", "000000000000f83f"),
                ("hex(struct.pack('>d', 1.5))", "3ff8000000000000"),
                ("hex(struct.pack('d', -0.1))", "9a9999999999b9bf"),
                ("hex(struct.pack('s', 'ab'))", "616200"),
                ("hex(struct.pack('s', ''))", "00"),
                ("hex(struct.pack('c3', 'abc'))", "616263"),
                ("hex(struct.pack('c2', 'abc'))", "6162"),
                ("hex(struct.pack('c0', 'abc'))", "616263"),
                ("hex(struct.pack('c', 'a'))", "61"),
                ("hex(struct.pack('x'))", "00"),
                ("hex(struct.pack('bxb', 1, 2))", "010002"),
                ("hex(struct.pack('b b', 1, 2))", "0102"),
                ("hex(struct.pack('<i>i', 1, 1))", "0100000000000001"),
                ("hex(struct.pack('>h<h', 1, 1))", "00010100"),
                ("hex(struct.pack(''))", ""),
                ("hex(struct.pack('!bi', 1, 2))", "0100000002000000"),
                ("hex(struct.pack('!2bh', 1, 2))", "01000200"),
                ("hex(struct.pack('!4bi', 1, 2))", "0100000002000000"),
                (
                    "hex(struct.pack('!bd', 1, 0))",
                    "01000000000000000000000000000000",
                ),
                ("hex(struct.pack('!1bi', 1, 2))", "0102000000"),
                ("hex(struct.pack('bc3', 1, 'abc'))", "01616263"),
                ("hex(struct.pack('!c1i', 'a', 1))", "6100000001000000"),
                ("hex(struct.pack('i', '7'))", "07000000"),
                ("hex(struct.pack('s', 12))", "313200"),
                ("hex(struct.pack('b', 1.9))", "01"),
                ("hex(struct.pack('b', -1.9))", "ff"),
            ],
        );
        // A NUL ends the format string.
        check_texts(&s, &[("hex(struct.pack('b\\0b', 1, 2))", "01")]);
        // Binary payloads.
        check_texts(
            &s,
            &[
                ("hex(struct.pack('s', 'a\\0b'))", "61006200"),
                ("hex(struct.pack('c3', 'a\\0b'))", "610062"),
            ],
        );
        assert_canary(&s, "struct.pack");
    });
}

#[test]
fn struct_unpack_size_and_errors() {
    guarded("struct_unpack", 60, || {
        let (_db, s) = fresh();
        // (script, expected results as an array of numbers or strings)
        let r = ev(
            &s,
            "local a, b, c = struct.unpack('bB', '\\255\\255') return {a, b, c}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![Reply::Int(-1), Reply::Int(255), Reply::Int(3)])
        );
        let r = ev(&s, "return {struct.unpack('>I2', '\\1\\2')}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(258), Reply::Int(3)]));
        let r = ev(&s, "return {struct.unpack('<I2', '\\1\\2')}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(513), Reply::Int(3)]));
        let r = ev(&s, "return {struct.unpack('b', '\\1\\2\\3', 2)}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(2), Reply::Int(3)]));
        let r = ev(&s, "return {struct.unpack('s', 'ab\\0cd')}");
        assert_eq!(r, Reply::Array(vec![bulk("ab"), Reply::Int(4)]));
        let r = ev(&s, "return {struct.unpack('bc0', '\\3abcd')}");
        assert_eq!(r, Reply::Array(vec![bulk("abc"), Reply::Int(5)]));
        let r = ev(&s, "return {struct.unpack('c2c1', 'xyz')}");
        assert_eq!(r, Reply::Array(vec![bulk("xy"), bulk("z"), Reply::Int(4)]));
        let r = ev(&s, "return {struct.unpack('i3', '\\254\\255\\255')}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(-2), Reply::Int(4)]));
        let r = ev(&s, "return {struct.unpack('I3', '\\254\\255\\255')}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(16_777_214), Reply::Int(4)]));
        let r = ev(&s, "return {struct.unpack('>i3', '\\255\\255\\254')}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(-2), Reply::Int(4)]));
        let r = ev(&s, "return {struct.unpack('x b', '\\0\\9')}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(9), Reply::Int(3)]));
        let r = ev(&s, "return {struct.unpack('', 'abc')}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(1)]));
        let r = ev(
            &s,
            "return {struct.unpack('!bi', '\\1\\0\\0\\0\\2\\0\\0\\0')}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![Reply::Int(1), Reply::Int(2), Reply::Int(9)])
        );
        // Round trips, including values beyond 2^32.
        let r = ev(
            &s,
            "local vals = {0, 1, -1, 255, 256, 65535, 65536, 2^31 - 1, -2^31, 2^32 - 1, 2^40, -2^40, 2^52, -2^52} \
             for _, fmt in ipairs({'<i8', '>i8', '<l', '>l'}) do \
               for _, v in ipairs(vals) do \
                 local got = struct.unpack(fmt, struct.pack(fmt, v)) \
                 if got ~= v then return fmt .. ' ' .. v .. ' -> ' .. got end \
               end \
             end \
             for _, v in ipairs({0, 1, 2^32 - 1, 2^40, 2^53}) do \
               for _, fmt in ipairs({'<I8', '>I8', '<L', '>T'}) do \
                 local got = struct.unpack(fmt, struct.pack(fmt, v)) \
                 if got ~= v then return fmt .. ' ' .. v .. ' -> ' .. got end \
               end \
             end \
             return 'ok'",
        );
        assert_eq!(r, bulk("ok"), "{}", brief(&r));
        let r = ev(
            &s,
            "for _, fmt in ipairs({'<d', '>d'}) do for _, v in ipairs({0, 1.5, -0.1, 1e300, 1e-10, 2^53 + 2}) do \
               if struct.unpack(fmt, struct.pack(fmt, v)) ~= v then return 'bad ' .. fmt .. v end end end \
             for _, fmt in ipairs({'<f', '>f'}) do for _, v in ipairs({0, 0.5, -2.25, 1e10}) do \
               if struct.unpack(fmt, struct.pack(fmt, v)) ~= v then return 'bad ' .. fmt .. v end end end \
             local x = struct.unpack('f', struct.pack('f', 0.1)) \
             if math.abs(x - 0.1) > 1e-7 or x == 0.1 then return 'float32 precision' end \
             local inf = struct.unpack('<d', struct.pack('<d', 1/0)) \
             return inf == 1/0 and 'ok' or 'inf'",
        );
        assert_eq!(r, bulk("ok"), "{}", brief(&r));

        check_numbers(
            &s,
            &[
                ("struct.size('bhi')", 7),
                ("struct.size('!bi')", 8),
                ("struct.size('!bd')", 16),
                ("struct.size('!2bi')", 6),
                ("struct.size('b!2bi')", 6),
                ("struct.size('l')", 8),
                ("struct.size('L')", 8),
                ("struct.size('T')", 8),
                ("struct.size('f')", 4),
                ("struct.size('d')", 8),
                ("struct.size('c10')", 10),
                ("struct.size('c')", 1),
                ("struct.size('i16')", 16),
                ("struct.size('I3')", 3),
                ("struct.size('')", 0),
                ("struct.size('xxx')", 3),
                ("struct.size('>b<h')", 3),
                ("struct.size(' b  b ')", 2),
                ("struct.size('z')", 0),
            ],
        );
        check_errors(
            &s,
            &[
                ("err(struct.size, 's')", "option 's' has no fixed size"),
                ("err(struct.size, 'c0')", "option 'c0' has no fixed size"),
                ("err(struct.size, '!3')", "alignment 3 is not a power of 2"),
                ("err(struct.size, '!0')", "alignment 0 is not a power of 2"),
                (
                    "err(struct.size, 'i33')",
                    "integral size 33 is larger than limit of 32",
                ),
                ("err(struct.size, '?')", "invalid format option '?'"),
                ("err(struct.size)", "bad argument #1 to 'size'"),
                ("err(struct.pack, 'c5', 'ab')", "string too short"),
                (
                    "err(struct.pack, 'b')",
                    "bad argument #2 to 'pack' (number expected, got no value)",
                ),
                ("err(struct.pack, 'b', 'x')", "number expected, got string"),
                ("err(struct.pack, 's', {})", "string expected, got table"),
                ("err(struct.pack, 'q', 1)", "invalid format option 'q'"),
                (
                    "err(struct.pack, 'i40', 1)",
                    "integral size 40 is larger than limit of 32",
                ),
                (
                    "err(struct.pack, 'i99999999999999999999', 1)",
                    "larger than limit",
                ),
                ("err(struct.pack)", "bad argument #1 to 'pack'"),
                ("err(struct.unpack, 'i', 'abc')", "data string too short"),
                (
                    "err(struct.unpack, 'd', '1234567')",
                    "data string too short",
                ),
                ("err(struct.unpack, 'b', '')", "data string too short"),
                ("err(struct.unpack, 'c5', 'abcd')", "data string too short"),
                (
                    "err(struct.unpack, 's', 'abc')",
                    "unfinished string in data",
                ),
                (
                    "err(struct.unpack, 'c0', 'x')",
                    "format `c0' needs a previous size",
                ),
                ("err(struct.unpack, 'bc0', '\\9x')", "data string too short"),
                (
                    "err(struct.unpack, 'b', 'x', 0)",
                    "offset must be 1 or greater",
                ),
                (
                    "err(struct.unpack, 'b', 'x', -5)",
                    "offset must be 1 or greater",
                ),
                (
                    "err(struct.unpack, 'b', 'x', 3)",
                    "offset must be less than or equal to string length",
                ),
                ("err(struct.unpack, 'z', 'x')", "invalid format option 'z'"),
                ("err(struct.unpack, 'b')", "bad argument #2 to 'unpack'"),
                (
                    "err(struct.unpack, 'b', 'x', 'y')",
                    "bad argument #3 to 'unpack'",
                ),
            ],
        );
        // 'b' at the offset one past the end finds nothing, but 'x' is fine.
        let r = ev(&s, "return {struct.unpack('', 'abc', 4)}");
        assert_eq!(r, Reply::Array(vec![Reply::Int(4)]));
        // Huge counts fail cleanly instead of overflowing.
        check_errors(
            &s,
            &[
                (
                    "err(struct.unpack, 'c99999999999999999999', 'abc')",
                    "data string too short",
                ),
                (
                    "err(struct.pack, 'c99999999999999999999', 'abc')",
                    "string too short",
                ),
            ],
        );
        // Too many results is an error, not a crash.
        let r = ev(
            &s,
            "return err(struct.unpack, string.rep('b', 100000), string.rep('x', 100000))",
        );
        eprintln!("INFO 100000 results -> {}", brief(&r));
        assert!(matches!(r, Reply::Bulk(_)), "{}", brief(&r));
        assert_canary(&s, "struct.unpack");
    });
}

// ---------------------------------------------------------------------
// cmsgpack
// ---------------------------------------------------------------------

#[test]
fn msgpack_pack_known_answers() {
    guarded("msgpack_pack", 60, || {
        let (_db, s) = fresh();
        let pack = |v: &str| format!("hex(cmsgpack.pack({v}))");
        let cases: Vec<(String, &str)> = vec![
            (pack("0"), "00"),
            (pack("1"), "01"),
            (pack("127"), "7f"),
            (pack("128"), "cc80"),
            (pack("255"), "ccff"),
            (pack("256"), "cd0100"),
            (pack("65535"), "cdffff"),
            (pack("65536"), "ce00010000"),
            (pack("2^31 - 1"), "ce7fffffff"),
            (pack("2^31"), "ce80000000"),
            (pack("2^32 - 1"), "ceffffffff"),
            (pack("2^32"), "cf0000000100000000"),
            (pack("2^53"), "cf0020000000000000"),
            (pack("-1"), "ff"),
            (pack("-32"), "e0"),
            (pack("-33"), "d0df"),
            (pack("-128"), "d080"),
            (pack("-129"), "d1ff7f"),
            (pack("-32768"), "d18000"),
            (pack("-32769"), "d2ffff7fff"),
            (pack("-2^31"), "d280000000"),
            (pack("-2^31 - 1"), "d3ffffffff7fffffff"),
            (pack("-2^53"), "d3ffe0000000000000"),
            (pack("-0.0"), "00"),
            (pack("3.0"), "03"),
            (pack("1e3"), "cd03e8"),
            (pack("nil"), "c0"),
            (pack("true"), "c3"),
            (pack("false"), "c2"),
            (pack("0.5"), "ca3f000000"),
            (pack("1.5"), "ca3fc00000"),
            (pack("-2.25"), "cac0100000"),
            (pack("0.1"), "cb3fb999999999999a"),
            (pack("1/3"), "cb3fd5555555555555"),
            (pack("1e300"), "cb7e37e43c8800759c"),
            (pack("2^63"), "ca5f000000"),
            (pack("1/0"), "ca7f800000"),
            (pack("-1/0"), "caff800000"),
            (pack("function() end"), "c0"),
            (pack("''"), "a0"),
            (pack("'a'"), "a161"),
            (pack("'\\0'"), "a100"),
            (pack("1, 2, 3"), "010203"),
            (pack("1, 'a', nil, true"), "01a161c0c3"),
            (pack("{1, 2, 3}"), "93010203"),
            (pack("{}"), "90"),
            (pack("{{}}"), "9190"),
            (pack("{a = 1}"), "81a16101"),
            (pack("{[0] = 1}"), "810001"),
            (pack("{[1.5] = 1}"), "81ca3fc0000001"),
            (pack("{[2^31] = 1}"), "81ce8000000001"),
            (pack("{[-1] = 1}"), "81ff01"),
            (pack("{[true] = 1}"), "81c301"),
        ];
        for (expr, want) in &cases {
            assert_eq!(&run_text(&s, expr), want, "{expr}");
        }
        assert_eq!(
            run_text(&s, "hex(cmsgpack.pack(string.rep('a', 31)))"),
            format!("bf{}", "61".repeat(31))
        );
        // Lengths of strings: header formats.
        let header = |len: usize, take: usize| {
            run_text(
                &s,
                &format!("string.sub(hex(cmsgpack.pack(string.rep('x', {len}))), 1, {take})"),
            )
        };
        assert_eq!(header(31, 2), "bf");
        assert_eq!(header(32, 4), "d920");
        assert_eq!(header(255, 4), "d9ff");
        assert_eq!(header(256, 6), "da0100");
        assert_eq!(header(65535, 6), "daffff");
        assert_eq!(header(65536, 10), "db00010000");
        // Arrays and maps: header formats.
        let r = |expr: &str| run_text(&s, expr);
        assert_eq!(
            r("string.sub(hex(cmsgpack.pack((function() local t = {} for i = 1, 15 do t[i] = 1 end return t end)())), 1, 2)"),
            "9f"
        );
        assert_eq!(
            r("string.sub(hex(cmsgpack.pack((function() local t = {} for i = 1, 16 do t[i] = 1 end return t end)())), 1, 6)"),
            "dc0010"
        );
        assert_eq!(
            r("string.sub(hex(cmsgpack.pack((function() local t = {} for i = 1, 65535 do t[i] = 1 end return t end)())), 1, 6)"),
            "dcffff"
        );
        assert_eq!(
            r("string.sub(hex(cmsgpack.pack((function() local t = {} for i = 1, 65536 do t[i] = 1 end return t end)())), 1, 10)"),
            "dd00010000"
        );
        assert_eq!(
            r("string.sub(hex(cmsgpack.pack((function() local t = {} for i = 1, 15 do t['k' .. i] = 1 end return t end)())), 1, 2)"),
            "8f"
        );
        assert_eq!(
            r("string.sub(hex(cmsgpack.pack((function() local t = {} for i = 1, 16 do t['k' .. i] = 1 end return t end)())), 1, 6)"),
            "de0010"
        );
        // Mixed tables are maps; sparse arrays too.
        assert_eq!(
            r("string.sub(hex(cmsgpack.pack({1, 2, x = 3})), 1, 2)"),
            "83"
        );
        assert_eq!(
            r("string.sub(hex(cmsgpack.pack({[1] = 1, [3] = 3})), 1, 2)"),
            "82"
        );
        // Nesting: the 17th table down becomes nil, as does a cycle.
        let deep = format!("{}c0", "91".repeat(16));
        assert_eq!(
            r("hex(cmsgpack.pack((function() local t = {} for i = 1, 16 do t = {t} end return t end)()))"),
            deep
        );
        assert_eq!(
            r("hex(cmsgpack.pack((function() local t = {} for i = 1, 15 do t = {t} end return t end)()))"),
            format!("{}90", "91".repeat(15))
        );
        assert_eq!(
            r("hex(cmsgpack.pack((function() local t = {} t[1] = t return t end)()))"),
            deep
        );
        // Errors.
        check_errors(
            &s,
            &[
                ("err(cmsgpack.pack)", "MessagePack pack needs input."),
                ("err(cmsgpack.unpack)", "bad argument #1 to 'unpack'"),
                ("err(cmsgpack.unpack, {})", "string expected, got table"),
            ],
        );
        assert_canary(&s, "cmsgpack.pack");
    });
}

#[test]
fn msgpack_unpack_forms_and_offsets() {
    guarded("msgpack_unpack", 60, || {
        let (_db, s) = fresh();
        // Decoding the str/bin family.
        check_texts(
            &s,
            &[
                ("cmsgpack.unpack('\\217\\003abc')", "abc"),
                ("cmsgpack.unpack('\\196\\003abc')", "abc"),
                ("cmsgpack.unpack('\\197\\000\\003abc')", "abc"),
                ("cmsgpack.unpack('\\198\\000\\000\\000\\003abc')", "abc"),
                ("cmsgpack.unpack('\\218\\000\\003abc')", "abc"),
                ("cmsgpack.unpack('\\219\\000\\000\\000\\003abc')", "abc"),
                ("cmsgpack.unpack('\\163abc')", "abc"),
                ("cmsgpack.unpack('\\160')", ""),
                ("hex(cmsgpack.unpack('\\162\\0\\255'))", "00ff"),
                ("tostring(cmsgpack.unpack('\\192'))", "nil"),
                ("tostring(cmsgpack.unpack('\\194'))", "false"),
                ("tostring(cmsgpack.unpack('\\195'))", "true"),
                ("tostring(select('#', cmsgpack.unpack('')))", "0"),
                ("tostring(select('#', cmsgpack.unpack('\\1\\2\\3')))", "3"),
            ],
        );
        check_numbers(
            &s,
            &[
                ("cmsgpack.unpack('\\127')", 127),
                ("cmsgpack.unpack('\\204\\128')", 128),
                ("cmsgpack.unpack('\\205\\255\\255')", 65535),
                (
                    "cmsgpack.unpack('\\206\\255\\255\\255\\255')",
                    4_294_967_295,
                ),
                (
                    "cmsgpack.unpack('\\207\\000\\000\\000\\001\\000\\000\\000\\000')",
                    4_294_967_296,
                ),
                ("cmsgpack.unpack('\\224')", -32),
                ("cmsgpack.unpack('\\208\\128')", -128),
                ("cmsgpack.unpack('\\209\\128\\000')", -32768),
                (
                    "cmsgpack.unpack('\\210\\128\\000\\000\\000')",
                    -2_147_483_648,
                ),
                (
                    "cmsgpack.unpack('\\211\\255\\255\\255\\255\\127\\255\\255\\255')",
                    -2_147_483_649,
                ),
                ("cmsgpack.unpack('\\202\\063\\000\\000\\000') * 2", 1),
                (
                    "cmsgpack.unpack('\\203\\063\\224\\000\\000\\000\\000\\000\\000') * 2",
                    1,
                ),
                ("#cmsgpack.unpack('\\147\\1\\2\\3')", 3),
                ("cmsgpack.unpack('\\220\\000\\002\\1\\2')[2]", 2),
                ("cmsgpack.unpack('\\221\\000\\000\\000\\002\\1\\2')[2]", 2),
                ("cmsgpack.unpack('\\129\\161k\\007').k", 7),
                ("cmsgpack.unpack('\\222\\000\\001\\161k\\007').k", 7),
                (
                    "cmsgpack.unpack('\\223\\000\\000\\000\\001\\161k\\007').k",
                    7,
                ),
                ("cmsgpack.unpack('\\129\\001\\007')[1]", 7),
                ("#cmsgpack.unpack('\\144') + #cmsgpack.unpack('\\128')", 0),
            ],
        );
        // unpack_one and unpack_limit return the next offset first.
        let r = ev(&s, "return {cmsgpack.unpack_one(cmsgpack.pack(1, 2, 3))}");
        assert_eq!(r, ints(&[1, 1]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_one(cmsgpack.pack(1, 2, 3), 1)}",
        );
        assert_eq!(r, ints(&[2, 2]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_one(cmsgpack.pack(1, 2, 3), 2)}",
        );
        assert_eq!(r, ints(&[-1, 3]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_one(cmsgpack.pack(1, 2, 3), 3)}",
        );
        assert_eq!(r, ints(&[-1]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_limit(cmsgpack.pack(1, 2, 3), 2)}",
        );
        assert_eq!(r, ints(&[2, 1, 2]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_limit(cmsgpack.pack(1, 2, 3), 2, 1)}",
        );
        assert_eq!(r, ints(&[-1, 2, 3]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_limit(cmsgpack.pack(1, 2, 3), 10, 0)}",
        );
        assert_eq!(r, ints(&[-1, 1, 2, 3]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_limit(cmsgpack.pack(1, 2, 3), 0, 0)}",
        );
        assert_eq!(r, ints(&[1, 2, 3]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_limit(cmsgpack.pack(1, 2, 3), 0, 1)}",
        );
        assert_eq!(r, ints(&[1]));
        let r = ev(
            &s,
            "return {cmsgpack.unpack_one(cmsgpack.pack('abc', {1}), 4)}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![Reply::Int(-1), Reply::Array(vec![Reply::Int(1)])])
        );
        check_errors(
            &s,
            &[
                (
                    "err(cmsgpack.unpack_one, cmsgpack.pack(1, 2, 3), -1)",
                    "Invalid request to unpack with offset of -1 and limit of 1.",
                ),
                (
                    "err(cmsgpack.unpack_limit, cmsgpack.pack(1, 2, 3), -1, 0)",
                    "Invalid request to unpack with offset of 0 and limit of -1.",
                ),
                (
                    "err(cmsgpack.unpack_one, cmsgpack.pack(1, 2, 3), 4)",
                    "Start offset 4 greater than input length 3.",
                ),
                (
                    "err(cmsgpack.unpack_limit, 'x')",
                    "bad argument #2 to 'unpack_limit'",
                ),
                (
                    "err(cmsgpack.unpack_one)",
                    "bad argument #1 to 'unpack_one'",
                ),
            ],
        );
        // A map whose key is nil or NaN cannot be built.
        check_errors(
            &s,
            &[
                ("err(cmsgpack.unpack, '\\129\\192\\001')", "table index is nil"),
                ("err(cmsgpack.unpack, '\\129\\203\\127\\248\\000\\000\\000\\000\\000\\000\\001')", "table index is NaN"),
            ],
        );
        // Duplicate keys: the last one wins.
        check_numbers(
            &s,
            &[("cmsgpack.unpack('\\130\\161k\\001\\161k\\002').k", 2)],
        );
        // nil items leave holes.
        check_numbers(&s, &[("cmsgpack.unpack('\\147\\1\\192\\3')[3]", 3)]);
        assert_canary(&s, "cmsgpack.unpack");
    });
}

fn ints(v: &[i64]) -> Reply {
    Reply::Array(v.iter().map(|n| Reply::Int(*n)).collect())
}

#[test]
fn msgpack_malformed_input_never_panics_or_hangs() {
    guarded("msgpack_malformed", 120, || {
        let (_db, s) = fresh();
        // Every proper prefix of a valid single object is "Missing bytes".
        let r = ev(
            &s,
            r#"
            local msg = cmsgpack.pack({1, -200, 70000, 2^40, -2^40, 0.1, 0.5, 'abc', string.rep('x', 40),
              string.rep('y', 300), {nested = {true, false, {}}}, {a = 1, b = {2, 3}}, 'tail'})
            local bad = 0
            for k = 1, #msg - 1 do
              local e = err(cmsgpack.unpack, string.sub(msg, 1, k))
              if not string.find(e, 'Missing bytes in input.', 1, true) then
                bad = bad + 1
                if bad < 3 then return 'prefix ' .. k .. ': ' .. e end
              end
            end
            if #cmsgpack.unpack(msg) ~= 13 then return 'full message failed' end
            return 'ok ' .. #msg
            "#,
        );
        assert!(
            matches!(&r, Reply::Bulk(b) if b.starts_with(b"ok ")),
            "{}",
            brief(&r)
        );
        // Every proper prefix of a stream of objects either decodes fewer
        // objects or says "Missing bytes".
        let r = ev(
            &s,
            r#"
            local msg = cmsgpack.pack(1, 'abc', {1, 2}, {a = 'b'}, 300, 'zz')
            for k = 1, #msg - 1 do
              local ok, e = pcall(cmsgpack.unpack, string.sub(msg, 1, k))
              if not ok and not string.find(tostring(e), 'Missing bytes', 1, true) then return tostring(e) end
            end
            return 'ok'
            "#,
        );
        assert_eq!(r, bulk("ok"), "{}", brief(&r));
        check_errors(
            &s,
            &[
                ("err(cmsgpack.unpack, '\\193')", "Bad data format in input."),
                (
                    "err(cmsgpack.unpack, '\\212\\001\\000')",
                    "Bad data format in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\199\\001\\001\\000')",
                    "Bad data format in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\145\\193')",
                    "Bad data format in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\1\\193')",
                    "Bad data format in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\221\\255\\255\\255\\255')",
                    "Missing bytes in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\223\\255\\255\\255\\255')",
                    "Missing bytes in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\219\\255\\255\\255\\255abc')",
                    "Missing bytes in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\207\\0\\0')",
                    "Missing bytes in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\146\\1')",
                    "Missing bytes in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\129\\1')",
                    "Missing bytes in input.",
                ),
                (
                    "err(cmsgpack.unpack, '\\1\\204')",
                    "Missing bytes in input.",
                ),
            ],
        );
        // Deep nesting in the input is not recursed into on the Rust stack.
        let r = ev(
            &s,
            r#"
            local depth = 100000
            local t = cmsgpack.unpack(string.rep('\145', depth) .. '\192')
            local n = 0
            while type(t) == 'table' do n = n + 1 t = t[1] end
            return n
            "#,
        );
        assert!(has_err(&r, "Too many nested"), "{}", brief(&r));
        let r = ev(
            &s,
            "return err(cmsgpack.unpack, string.rep('\\129\\001', 100000))",
        );
        assert!(
            matches!(&r, Reply::Bulk(b) if String::from_utf8_lossy(b).contains("Too many nested")),
            "{}",
            brief(&r)
        );
        // Random garbage, through ARGV so every byte value is covered.
        let mut rng = Rng::new(11);
        let start = Instant::now();
        for round in 0..400 {
            let len = rng.below(48) as usize;
            let junk = rng.bytes(len);
            let r = s.execute(&[
                b"EVAL".to_vec(),
                b"local ok, e = pcall(cmsgpack.unpack, ARGV[1]) \
                  local ok2, e2 = pcall(cmsgpack.unpack_one, ARGV[1], 0) \
                  local ok3, e3 = pcall(cmsgpack.unpack_limit, ARGV[1], 3, 0) return 1"
                    .to_vec(),
                b"0".to_vec(),
                junk.clone(),
            ]);
            assert_eq!(
                r,
                Reply::Int(1),
                "round {round}, bytes {junk:?}: {}",
                brief(&r)
            );
        }
        assert!(start.elapsed() < Duration::from_secs(60));
        // Mutations of a valid message.
        let msg = [
            0x93u8, 0x01, 0xa3, b'a', b'b', b'c', 0x82, 0xa1, b'k', 0xcd, 0x01, 0x00, 0xa1, b'z',
            0x90,
        ];
        for pos in 0..msg.len() {
            for byte in 0..=255u8 {
                let mut m = msg.to_vec();
                m[pos] = byte;
                let r = s.execute(&[
                    b"EVAL".to_vec(),
                    b"pcall(cmsgpack.unpack, ARGV[1]) return 1".to_vec(),
                    b"0".to_vec(),
                    m,
                ]);
                assert_eq!(r, Reply::Int(1), "pos {pos} byte {byte}");
            }
        }
        assert_canary(&s, "malformed msgpack");
    });
}

// ---------------------------------------------------------------------
// cjson
// ---------------------------------------------------------------------

#[test]
fn cjson_encode_known_answers() {
    guarded("cjson_encode", 60, || {
        let (_db, s) = fresh();
        check_texts(
            &s,
            &[
                ("cjson.encode({1, 2, 3})", "[1,2,3]"),
                ("cjson.encode({})", "{}"),
                ("cjson.encode({{}, {{}}})", "[{},[{}]]"),
                ("cjson.encode({a = {}})", r#"{"a":{}}"#),
                ("cjson.encode({a = 1})", r#"{"a":1}"#),
                ("cjson.encode({'a', 'b'})", r#"["a","b"]"#),
                ("cjson.encode({true, false})", "[true,false]"),
                ("cjson.encode(true)", "true"),
                ("cjson.encode(false)", "false"),
                ("cjson.encode(nil)", "null"),
                ("cjson.encode(cjson.null)", "null"),
                ("cjson.encode({cjson.null})", "[null]"),
                ("cjson.encode({a = cjson.null})", r#"{"a":null}"#),
                ("cjson.encode({1, nil, 3})", "[1,null,3]"),
                ("cjson.encode({[1] = 1, [2] = 2, [4] = 4})", "[1,2,null,4]"),
                ("cjson.encode({[2] = 'x'})", r#"[null,"x"]"#),
                ("cjson.encode({[0] = 1})", r#"{"0":1}"#),
                ("cjson.encode({[-1] = 1})", r#"{"-1":1}"#),
                ("cjson.encode({[1.5] = 1})", r#"{"1.5":1}"#),
                ("cjson.encode('')", r#""""#),
                ("cjson.encode('a/b')", r#""a\/b""#),
                ("cjson.encode('\\0')", r#""\u0000""#),
                ("cjson.encode('\\1\\31')", r#""\u0001\u001f""#),
                ("cjson.encode('\\127')", r#""\u007f""#),
                ("cjson.encode('\\n\\t\\r\\b\\f')", r#""\n\t\r\b\f""#),
                ("cjson.encode('\"\\\\')", r#""\"\\""#),
                ("cjson.encode('é')", "\"é\""),
                ("cjson.encode(1)", "1"),
                ("cjson.encode(-1)", "-1"),
                ("cjson.encode(0)", "0"),
                ("cjson.encode(-0.5)", "-0.5"),
                ("cjson.encode(1.5)", "1.5"),
                ("cjson.encode(0.1)", "0.1"),
                ("cjson.encode(100)", "100"),
                ("cjson.encode(1e308)", "1e+308"),
                ("cjson.encode(-1e308)", "-1e+308"),
                ("cjson.encode(2^53)", "9.007199254741e+15"),
                ("cjson.encode(123456789012)", "123456789012"),
                ("cjson.encode(99999999999999)", "99999999999999"),
                ("cjson.encode(100000000000000)", "1e+14"),
                ("cjson.encode(1e15)", "1e+15"),
                ("cjson.encode(12345678901234567)", "1.2345678901235e+16"),
                ("cjson.encode(1e-5)", "1e-05"),
                ("cjson.encode(0.0001)", "0.0001"),
                ("cjson.encode(1/3)", "0.33333333333333"),
                ("cjson.encode(3.14159265358979)", "3.1415926535898"),
                ("cjson.encode(123456.789)", "123456.789"),
                ("cjson.encode(1e-10)", "1e-10"),
                ("cjson.encode(2^31)", "2147483648"),
                ("cjson.encode(-2^63)", "-9.2233720368548e+18"),
                ("cjson.encode({1.5, 2})", "[1.5,2]"),
            ],
        );
        assert_canary(&s, "cjson.encode known answers");
    });
}

#[test]
fn cjson_encode_edge_cases() {
    guarded("cjson_encode_edges", 90, || {
        let (_db, s) = fresh();
        // Bytes that are not UTF-8 pass through, and are not escaped.
        check_numbers(
            &s,
            &[
                ("#cjson.encode('\\255')", 3),
                ("#cjson.encode('\\192\\128')", 4),
                ("#cjson.encode('\\237\\160\\128')", 5),
                ("#cjson.encode(string.rep('\\255', 1000))", 1002),
                ("#cjson.encode(string.rep('/', 1000))", 2002),
                ("#cjson.encode(string.rep('\\0', 1000))", 6002),
            ],
        );
        check_texts(&s, &[("hex(cjson.encode('\\255\\254'))", "22fffe22")]);
        // Mixed and duplicate-key tables.
        let r = ev(
            &s,
            "local t = cjson.decode(cjson.encode({1, 2, x = 3})) \
             return {type(t), t.x, t['1'], t['2'], tostring(t[1])}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![
                bulk("table"),
                Reply::Int(3),
                Reply::Int(1),
                Reply::Int(2),
                bulk("nil")
            ])
        );
        // Sparse arrays: too sparse is an error, conversion is a setting.
        check_errors(
            &s,
            &[
                (
                    "err(cjson.encode, {[1] = 1, [20] = 2})",
                    "Cannot serialise table: excessively sparse array",
                ),
                ("err(cjson.encode, {[100] = 1})", "excessively sparse array"),
            ],
        );
        check_texts(
            &s,
            &[
                // max 10 is not above the safe limit: still an array.
                ("cjson.encode({[10] = 1})", "[null,null,null,null,null,null,null,null,null,1]"),
                ("(function() cjson.encode_sparse_array(true) return cjson.encode({[100] = 1}) end)()", r#"{"100":1}"#),
                ("(function() cjson.encode_sparse_array(false, 0) return cjson.encode({[1] = 1, [50] = 2}):sub(1, 3) end)()", "[1,"),
                ("(function() cjson.encode_sparse_array(false, 10, 100) return #cjson.encode({[1] = 1, [50] = 2}) end)() > 100 and 'long' or 'short'", "long"),
            ],
        );
        // Settings return their value; wrong values are refused.
        check_texts(
            &s,
            &[
                ("tostring(cjson.encode_max_depth())", "1000"),
                ("tostring(cjson.decode_max_depth())", "1000"),
                ("tostring(cjson.encode_max_depth(5))", "5"),
                ("tostring(cjson.decode_max_depth(7))", "7"),
                ("tostring(cjson.encode_keep_buffer(false))", "false"),
                ("tostring(cjson.encode_keep_buffer())", "true"),
                ("tostring(cjson.encode_invalid_numbers())", "false"),
                ("tostring(cjson.encode_invalid_numbers(true))", "true"),
                ("tostring(cjson.encode_invalid_numbers('null'))", "null"),
                ("tostring(select(2, cjson.encode_sparse_array()))", "2"),
                ("tostring(select(3, cjson.encode_sparse_array()))", "10"),
                ("tostring(cjson.encode_number_precision(14))", "14"),
            ],
        );
        check_errors(
            &s,
            &[
                (
                    "err(cjson.encode_max_depth, 0)",
                    "expected integer between 1 and",
                ),
                (
                    "err(cjson.encode_max_depth, -3)",
                    "expected integer between 1 and",
                ),
                (
                    "err(cjson.encode_max_depth, 1.5)",
                    "expected integer between 1 and",
                ),
                (
                    "err(cjson.decode_max_depth, 'x')",
                    "expected integer between 1 and",
                ),
                ("err(cjson.encode_sparse_array, 5)", "invalid option"),
                (
                    "err(cjson.encode_invalid_numbers, 'maybe')",
                    "invalid option",
                ),
            ],
        );
        // NaN and infinity are refused unless the script asks otherwise.
        check_errors(
            &s,
            &[
                (
                    "err(cjson.encode, 0/0)",
                    "Cannot serialise number: must not be NaN or Inf",
                ),
                (
                    "err(cjson.encode, 1/0)",
                    "Cannot serialise number: must not be NaN or Inf",
                ),
                (
                    "err(cjson.encode, -1/0)",
                    "Cannot serialise number: must not be NaN or Inf",
                ),
                ("err(cjson.encode, {1, 2, 1/0})", "must not be NaN or Inf"),
                ("err(cjson.encode, {a = 0/0})", "must not be NaN or Inf"),
                ("err(cjson.encode, {[1/0] = 1})", "excessively sparse array"),
                (
                    "err(cjson.encode, function() end)",
                    "Cannot serialise function: type not supported",
                ),
                (
                    "err(cjson.encode, {type})",
                    "Cannot serialise function: type not supported",
                ),
                (
                    "err(cjson.encode, {[true] = 1})",
                    "Cannot serialise boolean: table key must be a number or string",
                ),
                (
                    "err(cjson.encode, {[{}] = 1})",
                    "Cannot serialise table: table key must be a number or string",
                ),
                (
                    "err(cjson.encode)",
                    "bad argument #1 to 'encode' (expected 1 argument)",
                ),
                (
                    "err(cjson.encode, 1, 2)",
                    "bad argument #1 to 'encode' (expected 1 argument)",
                ),
            ],
        );
        check_texts(
            &s,
            &[
                ("(function() cjson.encode_invalid_numbers(true) return cjson.encode({1/0, -1/0}) end)()", "[inf,-inf]"),
                ("(function() cjson.encode_invalid_numbers('null') return cjson.encode({1/0, 0/0, 1}) end)()", "[null,null,1]"),
            ],
        );
        // Depth: 1000 tables deep is fine, 1001 is not; a cycle ends there too.
        let nest = |n: usize| {
            format!("local t = {{}} for i = 1, {n} do t = {{t}} end return pcall(cjson.encode, t)")
        };
        let r = ev(
            &s,
            &format!(
                "local ok, e = (function() {} end)() return tostring(ok)",
                nest(999)
            ),
        );
        assert_eq!(r, bulk("true"), "{}", brief(&r));
        let r = ev(
            &s,
            "local t = {} for i = 1, 1000 do t = {t} end local ok, e = pcall(cjson.encode, t) \
             return tostring(ok) .. ':' .. tostring(e):sub(1, 60)",
        );
        assert_eq!(r, bulk("false:Cannot serialise, excessive nesting (1001)"));
        let r = ev(
            &s,
            "local t = {} for i = 1, 999 do t = {t} end local ok, e = pcall(cjson.encode, t) \
             return tostring(ok) .. ':' .. #e",
        );
        assert_eq!(r, bulk("true:2000"), "{}", brief(&r));
        check_errors(
            &s,
            &[
                (
                    "err(cjson.encode, (function() local t = {} t[1] = t return t end)())",
                    "excessive nesting (1001)",
                ),
                (
                    "err(cjson.encode, (function() local t = {} t.a = t return t end)())",
                    "excessive nesting (1001)",
                ),
                (
                    "(function() cjson.encode_max_depth(2) return err(cjson.encode, {{{}}}) end)()",
                    "excessive nesting (3)",
                ),
                (
                    "(function() cjson.encode_max_depth(3) return cjson.encode({{{}}}) end)()",
                    "[[{}]]",
                ),
            ],
        );
        // Absurdly high depth settings do not make the encoder recurse on the Rust stack.
        let r = ev(
            &s,
            "cjson.encode_max_depth(2000000000) local t = {} for i = 1, 100000 do t = {t} end \
             local ok, e = pcall(cjson.encode, t) return tostring(ok)",
        );
        assert_eq!(r, bulk("false"), "{}", brief(&r));
        // A DAG that expands exponentially is stopped by the output cap.
        let r = ev(
            &s,
            "local t = {1} for i = 1, 60 do t = {t, t} end local ok, e = pcall(cjson.encode, t) \
             return tostring(ok) .. ':' .. tostring(e)",
        );
        let got = text(&r);
        assert!(got.starts_with("false:") && got.contains("memory"), "{got}");
        assert_canary(&s, "cjson.encode edges");
    });
}

#[test]
fn cjson_settings_do_not_leak_between_scripts() {
    guarded("cjson_cfg_leak", 60, || {
        let (_db, s) = fresh();
        let r = ev(
            &s,
            "cjson.encode_max_depth(1) cjson.decode_max_depth(1) cjson.encode_sparse_array(true, 1, 1) \
             cjson.encode_invalid_numbers(true) return 1",
        );
        assert_eq!(r, Reply::Int(1));
        let r = ev(
            &s,
            "return {cjson.encode({{{}}}), tostring(select(2, pcall(cjson.encode, 1/0))):sub(1, 22), \
             #cjson.decode('[[[1]]]'), tostring(cjson.decode_max_depth()), tostring(cjson.encode_max_depth()), \
             tostring(pcall(cjson.encode, {[1] = 1, [30] = 2}))}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![
                bulk("[[{}]]"),
                bulk("Cannot serialise numbe"),
                Reply::Int(1),
                bulk("1000"),
                bulk("1000"),
                bulk("false"),
            ]),
            "{}",
            brief(&r)
        );
        assert_canary(&s, "cjson settings");
    });
}

#[test]
fn cjson_decode_known_answers() {
    guarded("cjson_decode", 60, || {
        let (_db, s) = fresh();
        check_texts(
            &s,
            &[
                ("cjson.decode('\"abc\"')", "abc"),
                ("cjson.decode('\"\"')", ""),
                ("cjson.decode('\"a\\\\/b\"')", "a/b"),
                (
                    "cjson.decode('\"\\\\n\\\\t\\\\r\\\\b\\\\f\\\\\\\\\\\\\"\"')",
                    "\n\t\r\u{8}\u{c}\\\"",
                ),
                ("hex(cjson.decode('\"\\\\u0041\"'))", "41"),
                ("hex(cjson.decode('\"\\\\u0000\"'))", "00"),
                ("hex(cjson.decode('\"\\\\u00e9\"'))", "c3a9"),
                ("hex(cjson.decode('\"\\\\u20ac\"'))", "e282ac"),
                ("hex(cjson.decode('\"\\\\u20AC\"'))", "e282ac"),
                ("hex(cjson.decode('\"\\\\ud83d\\\\ude00\"'))", "f09f9880"),
                ("hex(cjson.decode('\"\\\\uD834\\\\uDD1E\"'))", "f09d849e"),
                ("hex(cjson.decode('\"\\\\u007f\"'))", "7f"),
                ("hex(cjson.decode('\"\\255\\254\"'))", "fffe"),
                ("hex(cjson.decode('\"a\\nb\"'))", "610a62"),
                ("type(cjson.decode('true'))", "boolean"),
                ("tostring(cjson.decode('true'))", "true"),
                ("tostring(cjson.decode('false'))", "false"),
                ("tostring(cjson.decode('null') == cjson.null)", "true"),
                ("type(cjson.decode('null'))", "userdata"),
                (
                    "tostring(cjson.decode(' \\t\\r\\n null \\n') == cjson.null)",
                    "true",
                ),
                ("type(cjson.decode('[]'))", "table"),
                ("type(cjson.decode('{}'))", "table"),
                ("tostring(next(cjson.decode('{}')))", "nil"),
                ("tostring(next(cjson.decode('[]')))", "nil"),
                ("tostring(cjson.decode('[null]')[1] == cjson.null)", "true"),
                (
                    "tostring(cjson.decode('{\"a\":null}').a == cjson.null)",
                    "true",
                ),
                ("tostring(cjson.decode('[[],[[]]]')[2][1] ~= nil)", "true"),
                (
                    "cjson.decode('{\"a\":{\"b\":{\"c\":\"deep\"}}}').a.b.c",
                    "deep",
                ),
                ("cjson.decode('{\"\":\"empty key\"}')['']", "empty key"),
                ("tostring(cjson.decode('1'))", "1"),
                ("tostring(cjson.decode('-1'))", "-1"),
                ("tostring(cjson.decode('1.5'))", "1.5"),
                ("tostring(cjson.decode('-0.5'))", "-0.5"),
                ("tostring(cjson.decode('1e3'))", "1000"),
                ("tostring(cjson.decode('1E3'))", "1000"),
                ("tostring(cjson.decode('1e+3'))", "1000"),
                ("tostring(cjson.decode('1e-2'))", "0.01"),
                ("tostring(cjson.decode('0'))", "0"),
                ("tostring(cjson.decode('0.5'))", "0.5"),
                ("tostring(cjson.decode('-0'))", "-0"),
                (
                    "tostring(cjson.decode('123456789012345'))",
                    "1.2345678901234e+14",
                ),
                (
                    "string.format('%.0f', cjson.decode('9007199254740993'))",
                    "9007199254740992",
                ),
                (
                    "string.format('%.0f', cjson.decode('9223372036854775807'))",
                    "9223372036854775808",
                ),
                ("tostring(cjson.decode('1e999'))", "inf"),
                ("tostring(cjson.decode('1.'))", "1"),
                ("tostring(cjson.decode('  42  '))", "42"),
                ("tostring(#cjson.decode('[1,2,3]'))", "3"),
                ("tostring(#cjson.decode(' [ 1 , 2 , 3 ] '))", "3"),
                ("tostring(cjson.decode('{\"a\":1,\"a\":2}').a)", "2"),
                ("tostring(cjson.decode('[1,[2,[3,[4]]]]')[2][2][2][1])", "4"),
                ("tostring(cjson.decode(5))", "5"),
            ],
        );
        assert_canary(&s, "cjson.decode known answers");
    });
}

#[test]
fn cjson_decode_errors() {
    guarded("cjson_decode_errors", 90, || {
        let (_db, s) = fresh();
        let e = |input: &str| format!("err(cjson.decode, {input})");
        let cases: Vec<(String, &str)> = vec![
            (e("''"), "Expected value but found T_END at character 1"),
            (e("'   '"), "Expected value but found T_END at character 4"),
            (
                e("'{'"),
                "Expected object key string but found T_END at character 2",
            ),
            (e("'['"), "Expected value but found T_END at character 2"),
            (
                e("'[1,2'"),
                "Expected comma or array end but found T_END at character 5",
            ),
            (
                e("'[1,]'"),
                "Expected value but found T_ARR_END at character 4",
            ),
            (
                e("'[,1]'"),
                "Expected value but found T_COMMA at character 2",
            ),
            (
                e("'[1 2]'"),
                "Expected comma or array end but found T_NUMBER at character 4",
            ),
            (
                e("'{\"a\" 1}'"),
                "Expected colon but found T_NUMBER at character 6",
            ),
            (
                e("'{\"a\"}'"),
                "Expected colon but found T_OBJ_END at character 5",
            ),
            (
                e("'{\"a\":}'"),
                "Expected value but found T_OBJ_END at character 6",
            ),
            (
                e("'{\"a\":1,}'"),
                "Expected object key string but found T_OBJ_END at character 8",
            ),
            (
                e("'{\"a\":1 \"b\":2}'"),
                "Expected comma or object end but found T_STRING at character 8",
            ),
            (
                e("'{1:2}'"),
                "Expected object key string but found T_NUMBER at character 2",
            ),
            (
                e("'{a:1}'"),
                "Expected object key string but found invalid token at character 2",
            ),
            (
                e("'{\"a\":1'"),
                "Expected comma or object end but found T_END at character 7",
            ),
            (
                e("'1 2'"),
                "Expected the end but found T_NUMBER at character 3",
            ),
            (
                e("'{} x'"),
                "Expected the end but found invalid token at character 4",
            ),
            (
                e("'[] []'"),
                "Expected the end but found T_ARR_BEGIN at character 4",
            ),
            (
                e("'1 // c'"),
                "Expected the end but found invalid token at character 3",
            ),
            (
                e("'1.5.3'"),
                "Expected the end but found invalid token at character 4",
            ),
            (
                e("'2e'"),
                "Expected the end but found invalid token at character 2",
            ),
            (
                e("'truefalse'"),
                "Expected the end but found T_BOOLEAN at character 5",
            ),
            (
                e("'nul'"),
                "Expected value but found invalid token at character 1",
            ),
            (
                e("'tru'"),
                "Expected value but found invalid token at character 1",
            ),
            (
                e("'True'"),
                "Expected value but found invalid token at character 1",
            ),
            (
                e("'NaN'"),
                "Expected value but found invalid token at character 1",
            ),
            (
                e("'Infinity'"),
                "Expected value but found invalid token at character 1",
            ),
            (
                e("'nan'"),
                "Expected value but found invalid token at character 1",
            ),
            (
                e("'-Infinity'"),
                "Expected value but found invalid number at character 1",
            ),
            (
                e("'-'"),
                "Expected value but found invalid number at character 1",
            ),
            (
                e("'-x'"),
                "Expected value but found invalid number at character 1",
            ),
            (
                e("'01'"),
                "Expected value but found invalid number at character 1",
            ),
            (
                e("'-01'"),
                "Expected value but found invalid number at character 1",
            ),
            (
                e("'0x10'"),
                "Expected value but found invalid number at character 1",
            ),
            (
                e("'[0x10]'"),
                "Expected value but found invalid number at character 2",
            ),
            (
                e("'\\'x\\''"),
                "Expected value but found invalid token at character 1",
            ),
            (
                e("'\"abc'"),
                "Expected value but found unexpected end of string at character 5",
            ),
            (
                e("'\"abc\\\\'"),
                "Expected value but found invalid escape code at character 5",
            ),
            (
                e("'\"\\\\x\"'"),
                "Expected value but found invalid escape code at character 2",
            ),
            (
                e("'\"\\\\u12\"'"),
                "Expected value but found invalid unicode escape code at character 2",
            ),
            (
                e("'\"\\\\u12G4\"'"),
                "Expected value but found invalid unicode escape code at character 2",
            ),
            (e("'\"\\\\ud800\"'"), "invalid unicode escape code"),
            (e("'\"\\\\ud800\\\\u0041\"'"), "invalid unicode escape code"),
            (e("'\"\\\\ud800x\"'"), "invalid unicode escape code"),
            (e("'\"\\\\udc00\"'"), "invalid unicode escape code"),
            (e("'\"\\\\ud83d\\\\ude0\"'"), "invalid unicode escape code"),
            (
                e("'{\"a\\\\q\":1}'"),
                "Expected object key string but found invalid escape code at character 4",
            ),
            (e("'\\0a'"), "JSON parser does not support UTF-16 or UTF-32"),
            (e("'a\\0'"), "JSON parser does not support UTF-16 or UTF-32"),
            (
                e("'1\\0 2'"),
                "JSON parser does not support UTF-16 or UTF-32",
            ),
            (
                "err(cjson.decode)".into(),
                "bad argument #1 to 'decode' (expected 1 argument)",
            ),
            (
                "err(cjson.decode, '1', '2')".into(),
                "bad argument #1 to 'decode' (expected 1 argument)",
            ),
            (
                "err(cjson.decode, {})".into(),
                "bad argument #1 to 'decode' (string expected, got table)",
            ),
            ("err(cjson.decode, nil)".into(), "string expected, got nil"),
        ];
        for (expr, needle) in &cases {
            let got = run_text(&s, expr);
            assert!(
                got.contains(needle),
                "{expr}\n  want {needle:?}\n  got  {got:?}"
            );
        }
        // A NUL in the input ends it, as in C: nothing after it is read.
        let r = run_text(&s, "tostring(cjson.decode('1 \\0'))");
        assert_eq!(r, "1");
        // Depth.
        let ok = |n: usize| {
            format!(
                "(function() local ok, e = pcall(cjson.decode, string.rep('[', {n}) .. string.rep(']', {n})) return tostring(ok) end)()"
            )
        };
        assert_eq!(run_text(&s, &ok(1000)), "true");
        let too_deep =
            |n: usize| format!("err(cjson.decode, string.rep('[', {n}) .. string.rep(']', {n}))");
        for n in [1001usize, 1002, 5000, 10_000, 100_000] {
            let got = run_text(&s, &too_deep(n));
            assert!(
                got.contains("Found too many nested data structures (1001) at character 1001"),
                "{n}: {got}"
            );
        }
        let got = run_text(&s, "err(cjson.decode, string.rep('[', 10000))");
        assert!(
            got.contains("Found too many nested data structures (1001)"),
            "{got}"
        );
        let got = run_text(&s, "err(cjson.decode, string.rep('{\"a\":', 10000))");
        assert!(
            got.contains("Found too many nested data structures (1001) at character"),
            "{got}"
        );
        let got = run_text(
            &s,
            "err(cjson.decode, string.rep('[', 500) .. string.rep('{\"a\":', 501) .. '1')",
        );
        assert!(got.contains("(1001)"), "{got}");
        assert!(run_text(
            &s,
            "(function() cjson.decode_max_depth(3) return err(cjson.decode, '[[[[1]]]]') end)()"
        )
        .contains("Found too many nested data structures (4) at character 4"));
        assert_eq!(
            run_text(&s, "(function() cjson.decode_max_depth(3) return tostring(#cjson.decode('[[[1]]]')) end)()"),
            "1"
        );
        // A million-deep input is cut off at the depth limit, not the Rust stack.
        let start = Instant::now();
        let got = run_text(&s, "err(cjson.decode, string.rep('[', 1000000))");
        assert!(got.contains("(1001)"), "{got}");
        assert!(start.elapsed() < Duration::from_secs(10));
        // A raised limit is honoured without recursion.
        let r = ev(
            &s,
            "cjson.decode_max_depth(200000) local t = cjson.decode(string.rep('[', 100000) .. string.rep(']', 100000)) \
             local n = 0 while type(t) == 'table' and t[1] do n = n + 1 t = t[1] end return n + 1",
        );
        assert!(has_err(&r, "too many nested"), "{}", brief(&r));
        // Errors can be caught and the script goes on.
        let r = ev(
            &s,
            "local ok = pcall(cjson.decode, '{') local ok2, v = pcall(cjson.decode, '[1]') return {tostring(ok), tostring(ok2), v[1]}",
        );
        assert_eq!(
            r,
            Reply::Array(vec![bulk("false"), bulk("true"), Reply::Int(1)])
        );
        // An uncaught one is an error reply.
        let r = ev(&s, "return cjson.decode('{')");
        assert!(has_err(&r, "Expected object key string"), "{}", brief(&r));
        assert_canary(&s, "cjson.decode errors");
    });
}

#[test]
fn cjson_truncation_and_mutation_never_panic() {
    guarded("cjson_fuzz", 120, || {
        let (_db, s) = fresh();
        let r = ev(
            &s,
            r#"
            local doc = cjson.encode({a = {1, 2.5, 'x\ny', true, cjson.null}, b = {c = {d = {}}}, e = 'é\0'})
            local bad = 0
            for k = 1, #doc - 1 do
              local ok, v = pcall(cjson.decode, string.sub(doc, 1, k))
              if ok and type(v) ~= 'number' then bad = bad + 1 end
            end
            -- a truncated object or array can never decode
            return bad
            "#,
        );
        assert_eq!(r, Reply::Int(0), "{}", brief(&r));
        let mut rng = Rng::new(21);
        for round in 0..300 {
            let len = rng.below(60) as usize;
            let junk = rng.bytes(len);
            let r = s.execute(&[
                b"EVAL".to_vec(),
                b"pcall(cjson.decode, ARGV[1]) pcall(cmsgpack.unpack, ARGV[1]) \
                  pcall(struct.unpack, 'bhi', ARGV[1]) pcall(struct.unpack, ARGV[1], 'abcdefgh') return 1"
                    .to_vec(),
                b"0".to_vec(),
                junk.clone(),
            ]);
            assert_eq!(r, Reply::Int(1), "round {round}: {junk:?} -> {}", brief(&r));
        }
        // Mutated valid documents.
        let doc = br#"{"a":[1,2.5,"x\ny",true,null],"b":{"c":"\u00e9\ud83d\ude00"}}"#;
        for pos in 0..doc.len() {
            for byte in [
                0u8, b'"', b'\\', b'{', b'}', b'[', b']', b',', b':', b'0', b'-', b'e', 255,
            ] {
                let mut m = doc.to_vec();
                m[pos] = byte;
                let r = s.execute(&[
                    b"EVAL".to_vec(),
                    b"pcall(cjson.decode, ARGV[1]) return 1".to_vec(),
                    b"0".to_vec(),
                    m,
                ]);
                assert_eq!(r, Reply::Int(1), "pos {pos} byte {byte}");
            }
        }
        assert_canary(&s, "cjson fuzz");
    });
}

// ---------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------

#[test]
fn round_trips_of_every_type() {
    guarded("round_trips", 120, || {
        let (_db, s) = fresh();
        let r = ev(
            &s,
            &format!(
                r#"{PRELUDE}
                local seed = 12345
                local function rnd(n) seed = (seed * 16807) % 2147483647 return seed % n end
                local function rstr()
                  local n = rnd(6) == 0 and rnd(400) or rnd(12)
                  local t = {{}}
                  for i = 1, n do t[i] = string.char(rnd(256)) end
                  return table.concat(t)
                end
                local function rnum()
                  local k = rnd(6)
                  if k == 0 then return rnd(200) - 100 end
                  if k == 1 then return (rnd(2000000) - 1000000) * 4096 end
                  if k == 2 then return rnd(100000) / 8 end
                  if k == 3 then return rnd(65536) end
                  if k == 4 then return -(rnd(70000)) end
                  return rnd(1000000) * 1000
                end
                local function rval(depth)
                  local k = rnd(depth > 3 and 4 or 7)
                  if k == 0 then return rnum() end
                  if k == 1 then return rstr() end
                  if k == 2 then return rnd(2) == 0 end
                  if k == 3 then return rnum() end
                  local t = {{}}
                  local n = rnd(5)
                  if k == 4 or k == 5 then
                    for i = 1, n do t[i] = rval(depth + 1) end
                  else
                    for i = 1, n do t[rstr()] = rval(depth + 1) end
                  end
                  return t
                end
                local function sanitize_msgpack(v) return v end
                for i = 1, 400 do
                  local v = rval(0)
                  local packed = cmsgpack.pack(v)
                  local back = cmsgpack.unpack(packed)
                  if not eq(v, back) then return 'msgpack mismatch at ' .. i .. ' ' .. hex(packed) end
                  local json = cjson.encode(v)
                  local jb = cjson.decode(json)
                  if not eq(v, jb) then return 'cjson mismatch at ' .. i .. ' ' .. json end
                end
                return 'ok'
                "#
            ),
        );
        assert_eq!(r, bulk("ok"), "{}", brief(&r));
        // msgpack of every boundary integer round-trips exactly.
        let r = ev(
            &s,
            &format!(
                r#"{PRELUDE}
                local vals = {{0, 1, 127, 128, 255, 256, 65535, 65536, 2^31 - 1, 2^31, 2^32 - 1, 2^32, 2^53,
                  -1, -32, -33, -128, -129, -32768, -32769, -2^31, -2^31 - 1, -2^53, 0.5, -0.5, 0.1, 1/3, 1e300, -1e-10}}
                for _, v in ipairs(vals) do
                  local back = cmsgpack.unpack(cmsgpack.pack(v))
                  if back ~= v then return 'int ' .. tostring(v) .. ' -> ' .. tostring(back) end
                end
                for _, n in ipairs({{0, 1, 31, 32, 255, 256, 65535, 65536, 100000}}) do
                  local s = string.rep('z', n)
                  if cmsgpack.unpack(cmsgpack.pack(s)) ~= s then return 'string ' .. n end
                  if cjson.decode(cjson.encode(s)) ~= s then return 'json string ' .. n end
                end
                for _, n in ipairs({{0, 1, 15, 16, 17, 65535, 65536, 70000}}) do
                  local t = {{}}
                  for i = 1, n do t[i] = i end
                  local back = cmsgpack.unpack(cmsgpack.pack(t))
                  if #back ~= n or (n > 0 and back[n] ~= n) then return 'array ' .. n end
                  local m = {{}}
                  for i = 1, n do m['k' .. i] = i end
                  local back2 = cmsgpack.unpack(cmsgpack.pack(m))
                  local c = 0 for _ in pairs(back2) do c = c + 1 end
                  if c ~= n then return 'map ' .. n end
                end
                -- every byte value in strings
                local all = {{}}
                for i = 0, 255 do all[#all + 1] = string.char(i) end
                all = table.concat(all)
                if cmsgpack.unpack(cmsgpack.pack(all)) ~= all then return 'bytes msgpack' end
                if cjson.decode(cjson.encode(all)) ~= all then return 'bytes json' end
                -- stream of several objects
                local a, b, c = cmsgpack.unpack(cmsgpack.pack(1, 'two', {{3}}))
                if a ~= 1 or b ~= 'two' or c[1] ~= 3 then return 'stream' end
                return 'ok'
                "#
            ),
        );
        assert_eq!(r, bulk("ok"), "{}", brief(&r));
        assert_canary(&s, "round trips");
    });
}

// ---------------------------------------------------------------------
// Framework-style scripts
// ---------------------------------------------------------------------

#[test]
fn bullmq_style_scripts() {
    guarded("bullmq", 60, || {
        let (_db, s) = fresh();
        // BullMQ ships job options as a msgpack map in ARGV: {name, delay, data, attempts}.
        let mut opts: Vec<u8> = vec![0x84];
        for (k, v) in [("name", "send-mail"), ("data", "{\"to\":\"a@b.c\"}")] {
            opts.push(0xa0 | k.len() as u8);
            opts.extend_from_slice(k.as_bytes());
            if v.len() < 32 {
                opts.push(0xa0 | v.len() as u8);
            } else {
                opts.extend_from_slice(&[0xd9, v.len() as u8]);
            }
            opts.extend_from_slice(v.as_bytes());
        }
        opts.extend_from_slice(&[0xa5]);
        opts.extend_from_slice(b"delay");
        opts.extend_from_slice(&[0xcd, 0x13, 0x88]); // 5000
        opts.extend_from_slice(&[0xa8]);
        opts.extend_from_slice(b"attempts");
        opts.push(3);
        let script = b"local opts = cmsgpack.unpack(ARGV[1]) \
            redis.call('HSET', KEYS[1], 'name', opts.name, 'data', opts.data, 'delay', opts.delay, 'attempts', opts.attempts) \
            local packed = cmsgpack.pack({opts.name, opts.delay}) \
            redis.call('SET', KEYS[2], packed) \
            redis.call('RPUSH', KEYS[3], cjson.encode({id = 1, name = opts.name, opts = {attempts = opts.attempts, delay = opts.delay}})) \
            return opts.delay";
        let r = s.execute(&[
            b"EVAL".to_vec(),
            script.to_vec(),
            b"3".to_vec(),
            b"bull:q:1".to_vec(),
            b"bull:q:packed".to_vec(),
            b"bull:q:wait".to_vec(),
            opts,
        ]);
        assert_eq!(r, Reply::Int(5000), "{}", brief(&r));
        assert_eq!(s.execute(&["HGET", "bull:q:1", "name"]), bulk("send-mail"));
        assert_eq!(
            s.execute(&["HGET", "bull:q:1", "data"]),
            bulk("{\"to\":\"a@b.c\"}")
        );
        assert_eq!(s.execute(&["HGET", "bull:q:1", "delay"]), bulk("5000"));
        assert_eq!(s.execute(&["HGET", "bull:q:1", "attempts"]), bulk("3"));
        // "send-mail" is 9 bytes: fixstr; 5000 is uint16.
        assert_eq!(
            s.execute(&["GET", "bull:q:packed"]),
            Reply::Bulk(b"\x92\xa9send-mail\xcd\x13\x88".to_vec())
        );
        let pushed = text(&s.execute(&["LINDEX", "bull:q:wait", "0"]));
        assert_eq!(
            pushed.len(),
            r#"{"id":1,"name":"send-mail","opts":{"attempts":3,"delay":5000}}"#.len()
        );
        assert!(pushed.contains(r#""name":"send-mail""#), "{pushed}");
        assert!(pushed.contains(r#""attempts":3"#), "{pushed}");
        // Next step of a BullMQ flow: read the list entry back and update a field.
        let r = ev(
            &s,
            "local job = cjson.decode(redis.call('LPOP', KEYS[1])) \
             job.opts.attempts = job.opts.attempts - 1 \
             redis.call('HSET', KEYS[2], 'opts', cjson.encode(job.opts)) \
             return redis.call('HGET', KEYS[2], 'opts')",
        );
        let _ = r;
        let r = eval(
            &s,
            "local job = cjson.decode(redis.call('LPOP', KEYS[1])) \
             job.opts.attempts = job.opts.attempts - 1 \
             redis.call('HSET', KEYS[2], 'opts', cjson.encode(job.opts)) \
             return cjson.decode(redis.call('HGET', KEYS[2], 'opts')).attempts",
            &["bull:q:wait", "bull:q:2"],
            &[],
        );
        // The list was already emptied by the first run: LPOP gives false.
        assert!(
            r.is_error() || r == Reply::Int(2) || r == Reply::Nil,
            "{}",
            brief(&r)
        );
        // A fresh entry, then the update.
        s.execute(&["RPUSH", "bull:q:wait", r#"{"id":2,"opts":{"attempts":3}}"#]);
        let r = eval(
            &s,
            "local job = cjson.decode(redis.call('LPOP', KEYS[1])) \
             job.opts.attempts = job.opts.attempts - 1 \
             redis.call('HSET', KEYS[2], 'opts', cjson.encode(job.opts)) \
             return cjson.decode(redis.call('HGET', KEYS[2], 'opts')).attempts",
            &["bull:q:wait", "bull:q:2"],
            &[],
        );
        assert_eq!(r, Reply::Int(2), "{}", brief(&r));
        assert_eq!(
            s.execute(&["HGET", "bull:q:2", "opts"]),
            bulk(r#"{"attempts":2}"#)
        );
        // Unpacking several packed values passed one after the other.
        let r = s.execute(&[
            b"EVAL".to_vec(),
            b"local a, b, c = cmsgpack.unpack(ARGV[1]) return {a, b, c[1]}".to_vec(),
            b"0".to_vec(),
            b"\x01\xa3abc\x91\x07".to_vec(),
        ]);
        assert_eq!(
            r,
            Reply::Array(vec![Reply::Int(1), bulk("abc"), Reply::Int(7)])
        );
        assert_canary(&s, "bullmq");
    });
}

#[test]
fn laravel_style_scripts() {
    guarded("laravel", 60, || {
        let (_db, s) = fresh();
        let payload = r#"{"uuid":"4e0a6a30-6a0b-4b4e-9d8c-1c8d3d3f1a11","displayName":"App\\Jobs\\SendEmail","job":"Illuminate\\Queue\\CallQueuedHandler@call","maxTries":null,"attempts":0,"data":{"commandName":"App\\Jobs\\SendEmail","command":"O:19:\"App\\Jobs\\SendEmail\":1:{s:2:\"id\";i:5;}","title":"caf\u00e9 / na\u00efve \ud83d\ude00"},"tags":[],"id":"abc"}"#;
        s.execute(&["RPUSH", "queues:default", payload]);
        // Laravel's pop script: move the job to the reserved set with attempts + 1.
        let pop = "local job = redis.call('lpop', KEYS[1]) \
                   local reserved = false \
                   if(job ~= false) then \
                     reserved = cjson.decode(job) \
                     reserved['attempts'] = reserved['attempts'] + 1 \
                     reserved = cjson.encode(reserved) \
                     redis.call('zadd', KEYS[2], ARGV[1], reserved) \
                   end \
                   return {job, reserved}";
        let r = eval(
            &s,
            pop,
            &["queues:default", "queues:default:reserved"],
            &["1700000060"],
        );
        let items = array(r);
        assert_eq!(items.len(), 2);
        assert_eq!(text(&items[0]), payload);
        let reserved = text(&items[1]);
        assert!(reserved.contains(r#""attempts":1"#), "{reserved}");
        assert!(!reserved.contains(r#""attempts":0"#), "{reserved}");
        // Slashes are escaped by cjson, other content is untouched.
        assert!(
            reserved.contains(r#""job":"Illuminate\\Queue\\CallQueuedHandler@call""#),
            "{reserved}"
        );
        assert!(
            reserved.contains("caf\u{e9} \\/ na\u{ef}ve \u{1F600}"),
            "{reserved}"
        );
        assert!(
            reserved.contains(r#""tags":{}"#),
            "empty arrays come out as {{}}: {reserved}"
        );
        assert!(reserved.contains(r#""maxTries":null"#), "{reserved}");
        // The stored member decodes to the same content.
        let r = eval(
            &s,
            "local m = redis.call('zrange', KEYS[1], 0, -1)[1] local j = cjson.decode(m) \
             return {j.attempts, j.uuid, j.data.title, tostring(j.maxTries == cjson.null), j.data.command}",
            &["queues:default:reserved"],
            &[],
        );
        let items = array(r);
        assert_eq!(items[0], Reply::Int(1));
        assert_eq!(text(&items[1]), "4e0a6a30-6a0b-4b4e-9d8c-1c8d3d3f1a11");
        assert_eq!(text(&items[2]), "caf\u{e9} / na\u{ef}ve \u{1F600}");
        assert_eq!(text(&items[3]), "true");
        assert_eq!(
            text(&items[4]),
            "O:19:\"App\\Jobs\\SendEmail\":1:{s:2:\"id\";i:5;}"
        );
        // Release: remove from reserved, decode, bump attempts again, push back.
        let release = "local job = redis.call('zrange', KEYS[1], 0, 0)[1] \
                       redis.call('zrem', KEYS[1], job) \
                       local d = cjson.decode(job) d.attempts = d.attempts + 1 \
                       redis.call('rpush', KEYS[2], cjson.encode(d)) \
                       return d.attempts";
        let r = eval(
            &s,
            release,
            &["queues:default:reserved", "queues:default"],
            &[],
        );
        assert_eq!(r, Reply::Int(2), "{}", brief(&r));
        // Sidekiq-ish: sha1hex used as a lock key.
        let r = ev(&s, "return redis.sha1hex(cjson.encode({1, 2, 3}))");
        assert_eq!(r, bulk(&run_text(&s, "redis.sha1hex('[1,2,3]')")));
        assert_canary(&s, "laravel");
    });
}

// ---------------------------------------------------------------------
// Sandbox, limits, kill
// ---------------------------------------------------------------------

#[test]
fn libraries_cannot_be_replaced_or_modified() {
    guarded("libs_tamper", 120, || {
        let (_db, s) = fresh();
        let attacks = [
            "cjson.encode = nil",
            "cjson.encode = function() return 'pwned' end",
            "cjson.decode = nil",
            "cjson.null = 1",
            "cjson.evil = 1",
            "cjson = nil",
            "cjson = {}",
            "cmsgpack.pack = nil",
            "cmsgpack.pack = function() return 'pwned' end",
            "cmsgpack = nil",
            "struct.pack = nil",
            "struct.size = function() return 0 end",
            "struct = nil",
            "bit.band = nil",
            "bit.band = function() return 0 end",
            "bit = nil",
            "redis.sha1hex = nil",
            "redis.sha1hex = function() return 'pwned' end",
            "redis.setresp = nil",
            "redis.log = nil",
            "redis.REDIS_VERSION = 'x'",
            "redis.LOG_DEBUG = 9",
            "rawset(cjson, 'encode', nil)",
            "rawset(cmsgpack, 'pack', nil)",
            "rawset(struct, 'pack', nil)",
            "rawset(bit, 'band', nil)",
            "rawset(redis, 'sha1hex', nil)",
            "setmetatable(cjson, {})",
            "setmetatable(bit, nil)",
            "setmetatable(redis, {__index = function() return 1 end})",
            "getmetatable(cjson).__index = nil",
            "getmetatable(cjson).__newindex = nil",
            "getmetatable(struct).__index = {}",
            "_G.cjson = nil",
            "_G.bit = {}",
            "rawset(_G, 'cjson', nil)",
            "getfenv(1).cjson = nil",
        ];
        for attack in attacks {
            let r = ev(&s, attack);
            assert!(r.is_error(), "attack {attack:?} succeeded: {}", brief(&r));
            assert_canary(&s, attack);
            let again = ev(&s, attack);
            assert!(again.is_error(), "attack {attack:?} the second time");
            assert_canary(&s, attack);
        }
        // Stashing values in the tables of results is harmless.
        let r = ev(
            &s,
            "local t = cjson.decode('{\"a\":1}') t.encode = 'x' local u = cmsgpack.unpack('\\129\\161k\\001') u.k = 2 return 1",
        );
        assert_eq!(r, Reply::Int(1));
        assert_canary(&s, "stash in results");
    });
}

#[test]
fn memory_limit_applies_inside_library_calls() {
    guarded("libs_memory", 180, || {
        let db = open(Config {
            lua_memory_limit: 8 << 20,
            ..Config::default()
        });
        let s = session(&db);
        let bombs = [
            // Doubling through cjson.encode.
            "local s = 'x' while true do s = cjson.encode({s, s}) end",
            // Escapes six times the size of the input.
            "local s = string.rep('\\0', 3e6) local r = cjson.encode(s) return #r",
            "local s = string.rep('\\0', 1.5e6) local t = {} for i = 1, 100 do t[i] = cjson.encode(s) end return #t",
            // A DAG that expands exponentially.
            "local t = {1} for i = 1, 60 do t = {t, t} end return #cjson.encode(t)",
            // Decoding into a huge table.
            "local s = '[' .. string.rep('1,', 2e6) .. '1]' return #cjson.decode(s)",
            "local s = '{' .. string.rep('\"a\":1,', 1) .. '\"b\":[' .. string.rep('[],', 1.5e6) .. '[]]}' return cjson.decode(s)",
            "local s = string.rep('\\145', 1e6) .. '\\192' return cmsgpack.unpack(s)",
            "local s = '\\221\\000\\032\\000\\000' .. string.rep('\\192', 2e6) return #cmsgpack.unpack(s)",
            // pack of big strings in loops.
            "local s = string.rep('y', 3e6) local t = {} for i = 1, 100 do t[i] = cmsgpack.pack(s) end return #t",
            "local s = string.rep('y', 3e6) local t = {} for i = 1, 100 do t[i] = struct.pack('s', s) end return #t",
            "local s = 'x' while true do s = struct.pack('s', s) .. s end",
            "local s = 'x' while true do s = cmsgpack.pack(s, s) end",
        ];
        for bomb in bombs {
            let start = Instant::now();
            let r = ev(&s, bomb);
            assert!(
                has_err(&r, "memory") || has_err(&r, "Too many nested"),
                "{bomb}: {}",
                brief(&r)
            );
            assert!(
                start.elapsed() < Duration::from_secs(20),
                "{bomb} took {:?}",
                start.elapsed()
            );
            // The node recovers.
            let ok = ev(
                &s,
                "local t = cmsgpack.unpack(cmsgpack.pack({1, 2, 3})) return #t + #cjson.encode({1})",
            );
            assert_eq!(ok, Reply::Int(6), "after {bomb}: {}", brief(&ok));
        }
        // A caught out-of-memory does not hurt the next script.
        for _ in 0..5 {
            let r = ev(
                &s,
                "local ok = pcall(function() local s = 'x' while true do s = cjson.encode({s, s}) end end) return tostring(ok)",
            );
            assert!(r == bulk("false") || r.is_error(), "{}", brief(&r));
            assert!((0..3).any(|_| !ev(&s, "return cjson.encode({1})").is_error()));
        }
        assert_canary(&s, "memory bombs in libs");
        // A big but legal encode within the limit works.
        let r = ev(
            &s,
            "local s = string.rep('a', 1e6) return #cjson.encode({s}) + #cmsgpack.pack(s)",
        );
        assert_eq!(r, Reply::Int(1_000_004 + 1_000_005), "{}", brief(&r));
    });
}

#[test]
fn deep_inputs_end_in_errors_not_stack_overflows() {
    guarded("libs_deep", 180, || {
        let db = open(Config {
            lua_memory_limit: 256 << 20,
            ..Config::default()
        });
        let s = session(&db);
        // Deeply nested JSON text (10000 levels) must be an error.
        for script in [
            "return err(cjson.decode, string.rep('[', 10000) .. string.rep(']', 10000))",
            "return err(cjson.decode, string.rep('{\"a\":', 10000) .. '1' .. string.rep('}', 10000))",
            "return err(cjson.decode, string.rep('[{\"a\":', 5000) .. '1' .. string.rep('}]', 5000))",
        ] {
            let r = ev(&s, &format!("{PRELUDE}\n{script}"));
            let t = text(&r);
            assert!(t.contains("Found too many nested data structures"), "{script}: {t}");
        }
        // Deeply nested tables on the encoding side (100000 levels).
        for script in [
            "local t = {} for i = 1, 100000 do t = {t} end return err(cjson.encode, t)",
            "local t = {} for i = 1, 100000 do t = {a = t} end return err(cjson.encode, t)",
            "local t = {} for i = 1, 100000 do t = {t} end return tostring(#cmsgpack.pack(t) > 0)",
        ] {
            let r = ev(&s, &format!("{PRELUDE}\n{script}"));
            let t = text(&r);
            assert!(
                t.contains("excessive nesting") || t == "true",
                "{script}: {t}"
            );
        }
        // Deep msgpack input decodes iteratively.
        let r = ev(
            &s,
            "local n = 0 local t = cmsgpack.unpack(string.rep('\\145', 500000) .. '\\192') \
             while type(t) == 'table' do n = n + 1 t = t[1] end return n",
        );
        assert!(has_err(&r, "Too many nested"), "{}", brief(&r));
        // Maps nested deeply.
        let r = ev(
            &s,
            "local n = 0 local t = cmsgpack.unpack(string.rep('\\129\\001', 200000) .. '\\192') \
             while type(t) == 'table' do n = n + 1 t = t[1] end return n",
        );
        assert!(has_err(&r, "Too many nested"), "{}", brief(&r));
        assert_canary(&s, "deep inputs");
    });
}

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
        self.handle.join().expect("script thread");
        got
    }
}

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
fn script_kill_stops_loops_that_use_the_libraries() {
    guarded("libs_kill", 120, || {
        let (db, s) = fresh();
        for script in [
            "while true do cjson.decode(cjson.encode({1, 2, {a = 'x'}})) end",
            "while true do cmsgpack.unpack(cmsgpack.pack({1, 'a', {b = 2}})) end",
            "while true do struct.unpack('>I4', struct.pack('>I4', 7)) end",
            "while true do bit.bxor(bit.rol(1, 3), bit.bswap(5)) end",
            "while true do redis.sha1hex('abc') end",
            "while true do pcall(cjson.decode, '{') end",
            "while true do pcall(cmsgpack.unpack, '\\221\\255\\255\\255\\255') end",
            "local big = string.rep('x', 1e6) while true do cjson.encode(big) end",
            "local big = cmsgpack.pack(string.rep('x', 1e6)) while true do cmsgpack.unpack(big) end",
            "while true do pcall(redis.call, 'GET', 'k') cjson.encode({}) end",
        ] {
            let running = spawn_script(&db, script);
            let acked = kill_when_running(&db);
            let (reply, ended) = running.finish(KILL_BOUND);
            // A pcall that fails at the moment of the kill re-raises its own
            // error: the script still stops, with that error.
            if script.contains("pcall") {
                assert!(reply.is_error(), "{script}: {}", brief(&reply));
            } else {
                assert!(has_err(&reply, "killed"), "{script}: {}", brief(&reply));
            }
            let took = ended.saturating_duration_since(acked);
            assert!(took < KILL_BOUND, "{script}: died {took:?} after the kill");
            assert_canary(&s, script);
        }
    });
}

#[test]
fn libraries_work_through_evalsha_and_across_scripts() {
    guarded("libs_evalsha", 60, || {
        let (_db, s) = fresh();
        let body = "return cjson.encode({a = cmsgpack.unpack(ARGV[1]), b = bit.band(6, 3), c = struct.size('i')})";
        let sha = text(&s.execute(&["SCRIPT", "LOAD", body]));
        for _ in 0..3 {
            let r = s.execute(&[
                b"EVALSHA".to_vec(),
                sha.clone().into_bytes(),
                b"0".to_vec(),
                b"\x05".to_vec(),
            ]);
            let t = text(&r);
            assert!(
                t.contains(r#""a":5"#) && t.contains(r#""b":2"#) && t.contains(r#""c":4"#),
                "{t}"
            );
        }
        // 1000 scripts in a row: nothing accumulates in the shared tables.
        for i in 0..1000 {
            let r = ev(
                &s,
                &format!("return bit.bxor({i}, 1) + #cjson.encode({{{i}}})"),
            );
            assert_eq!(
                r,
                Reply::Int((i ^ 1) + 2 + i.to_string().len() as i64),
                "{i}"
            );
        }
        assert_canary(&s, "many scripts");
    });
}
