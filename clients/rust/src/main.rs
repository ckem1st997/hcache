//! HCache spike against redis-rs (sync API). Every check runs; nothing aborts the run.
//!
//! Checks named x_* use Redis features outside the spike's scope; they show
//! what a real application would hit, they are not expected to pass.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use redis::{
    Commands, Connection, CopyOptions, Direction, LposOptions, RedisError, RedisResult, ScanOptions,
    SortedSetAddOptions, UpdateCheck, Value,
};

const REDIS_RS_VERSION: &str = "1.8.2"; // pinned in Cargo.toml with `=`
const TIMEOUT: Duration = Duration::from_secs(30);
const P: &str = "rs:";

type R = Result<(), String>;

fn port() -> String {
    std::env::var("HCACHE_PORT").unwrap_or_else(|_| "6400".to_string())
}

fn proto() -> Option<String> {
    std::env::var("HCACHE_PROTO").ok().filter(|s| !s.is_empty())
}

fn url() -> String {
    let base = format!("redis://127.0.0.1:{}/", port());
    match proto().as_deref() {
        Some("2") => format!("{base}?protocol=resp2"),
        Some("3") => format!("{base}?protocol=resp3"),
        _ => base,
    }
}

fn connect() -> Result<Connection, String> {
    let client = redis::Client::open(url()).map_err(|e| format!("open: {e}"))?;
    let conn = client
        .get_connection_with_timeout(TIMEOUT)
        .map_err(|e| format!("connect: {e}"))?;
    conn.set_read_timeout(Some(TIMEOUT))
        .map_err(|e| format!("set_read_timeout: {e}"))?;
    conn.set_write_timeout(Some(TIMEOUT))
        .map_err(|e| format!("set_write_timeout: {e}"))?;
    Ok(conn)
}

fn q<T>(res: RedisResult<T>) -> Result<T, String> {
    res.map_err(|e| format!("{e} [code={:?}]", e.code()))
}

fn trunc(s: String, n: usize) -> String {
    s.chars().take(n).collect()
}

fn eq<T: PartialEq + Debug>(got: T, want: T) -> R {
    if got == want {
        return Ok(());
    }
    Err(format!(
        "got {}, want {}",
        trunc(format!("{got:?}"), 200),
        trunc(format!("{want:?}"), 200)
    ))
}

fn is_server_error(e: &RedisError) -> bool {
    e.code().is_some()
}

/// Mirror of the Python `raises`: only a server error reply counts; anything
/// else (I/O, parse, timeout) propagates as a failure.
fn raises<T: Debug>(res: RedisResult<T>, text: &str) -> R {
    match res {
        Err(e) if is_server_error(&e) => {
            let msg = format!(
                "{} {} {}",
                e.code().unwrap_or(""),
                e.detail().unwrap_or(""),
                e
            );
            if msg.to_lowercase().contains(&text.to_lowercase()) {
                Ok(())
            } else {
                Err(format!("error {msg} lacks {text:?}"))
            }
        }
        Err(e) => Err(format!("non-server error: {e} [kind={:?}]", e.kind())),
        Ok(got) => Err(format!("no error, got {}", trunc(format!("{got:?}"), 100))),
    }
}

fn k(s: &str) -> String {
    format!("{P}{s}")
}

fn kb(suffix: &[u8]) -> Vec<u8> {
    let mut v = P.as_bytes().to_vec();
    v.extend_from_slice(suffix);
    v
}

fn bs(b: &[u8]) -> Value {
    Value::BulkString(b.to_vec())
}

fn random_bytes(n: usize) -> Vec<u8> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

fn ping(c: &mut Connection) -> Result<String, String> {
    q(redis::cmd("PING").query::<String>(c))
}

fn connect_ping() -> R {
    let mut c = connect()?;
    eq(ping(&mut c)?, "PONG".to_string())?;
    eq(
        q(redis::cmd("ECHO").arg("hi").query::<Vec<u8>>(&mut c))?,
        b"hi".to_vec(),
    )
}

fn set_cmd(
    c: &mut Connection,
    key: impl redis::ToRedisArgs,
    v: impl redis::ToRedisArgs,
) -> Result<bool, String> {
    q(redis::cmd("SET").arg(key).arg(v).query::<bool>(c))
}

fn get(c: &mut Connection, key: impl redis::ToRedisArgs) -> Result<Option<Vec<u8>>, String> {
    q(redis::cmd("GET").arg(key).query::<Option<Vec<u8>>>(c))
}

fn del(c: &mut Connection, keys: &[String]) -> Result<i64, String> {
    q(redis::cmd("DEL").arg(keys).query::<i64>(c))
}

fn set_get() -> R {
    let mut c = connect()?;
    eq(set_cmd(&mut c, k("s"), "hello")?, true)?;
    eq(get(&mut c, k("s"))?, Some(b"hello".to_vec()))?;
    eq(get(&mut c, k("missing"))?, None)
}

fn binary_key_and_value() -> R {
    let mut c = connect()?;
    let v: Vec<u8> = (0..=255u8).cycle().take(256 * 4).collect();
    let key = kb(b"\x00\r\n\xff bin");
    set_cmd(&mut c, key.clone(), v.clone())?;
    eq(get(&mut c, key)?, Some(v))?;
    set_cmd(&mut c, k("empty"), Vec::<u8>::new())?;
    eq(get(&mut c, k("empty"))?, Some(Vec::new()))?;
    set_cmd(&mut c, k("uni"), "xin chào 🌏")?;
    let got = get(&mut c, k("uni"))?.ok_or("uni missing")?;
    eq(
        String::from_utf8_lossy(&got).to_string(),
        "xin chào 🌏".to_string(),
    )
}

fn big_value_8mib() -> R {
    let mut c = connect()?;
    let v = random_bytes(8 << 20);
    set_cmd(&mut c, k("big"), v.clone())?;
    eq(get(&mut c, k("big"))?.as_deref() == Some(&v[..]), true)?;
    del(&mut c, &[k("big")])?;
    Ok(())
}

fn incr(c: &mut Connection, key: &str) -> RedisResult<i64> {
    redis::cmd("INCR").arg(key).query(c)
}

fn counters() -> R {
    let mut c = connect()?;
    let n = k("n");
    del(&mut c, &[n.clone()])?;
    eq(q(incr(&mut c, &n))?, 1)?;
    eq(q(redis::cmd("INCRBY").arg(&n).arg(41).query::<i64>(&mut c))?, 42)?;
    eq(q(redis::cmd("DECR").arg(&n).query::<i64>(&mut c))?, 41)?;
    eq(q(redis::cmd("DECRBY").arg(&n).arg(50).query::<i64>(&mut c))?, -9)?;
    eq(get(&mut c, &n)?, Some(b"-9".to_vec()))
}

fn counter_errors() -> R {
    let mut c = connect()?;
    set_cmd(&mut c, k("abc"), "abc")?;
    raises(incr(&mut c, &k("abc")), "not an integer")?;
    set_cmd(&mut c, k("max"), i64::MAX.to_string())?;
    raises(incr(&mut c, &k("max")), "overflow")?;
    eq(get(&mut c, k("max"))?, Some(i64::MAX.to_string().into_bytes()))
}

fn hashes() -> R {
    let mut c = connect()?;
    let h = k("h");
    del(&mut c, &[h.clone()])?;
    // redis-py's hset(mapping=...) sends HSET key f v f v ...
    eq(
        q(redis::cmd("HSET")
            .arg(&h)
            .arg("a")
            .arg("1")
            .arg("b")
            .arg("2")
            .arg("c")
            .arg("3")
            .query::<i64>(&mut c))?,
        3,
    )?;
    eq(q(redis::cmd("HSET").arg(&h).arg("a").arg("9").query::<i64>(&mut c))?, 0)?;
    eq(
        q(redis::cmd("HGET").arg(&h).arg("a").query::<Option<Vec<u8>>>(&mut c))?,
        Some(b"9".to_vec()),
    )?;
    eq(
        q(redis::cmd("HGET").arg(&h).arg("zz").query::<Option<Vec<u8>>>(&mut c))?,
        None,
    )?;
    let want: HashMap<Vec<u8>, Vec<u8>> = [(b"a", b"9"), (b"b", b"2"), (b"c", b"3")]
        .iter()
        .map(|(a, b)| (a.to_vec(), b.to_vec()))
        .collect();
    eq(
        q(redis::cmd("HGETALL").arg(&h).query::<HashMap<Vec<u8>, Vec<u8>>>(&mut c))?,
        want,
    )?;
    eq(q(redis::cmd("HLEN").arg(&h).query::<i64>(&mut c))?, 3)?;
    eq(q(redis::cmd("HDEL").arg(&h).arg("a").arg("zz").query::<i64>(&mut c))?, 1)?;
    eq(
        q(redis::cmd("HGETALL")
            .arg(k("nohash"))
            .query::<HashMap<Vec<u8>, Vec<u8>>>(&mut c))?,
        HashMap::new(),
    )
}

fn wrong_type() -> R {
    let mut c = connect()?;
    set_cmd(&mut c, k("str"), "x")?;
    raises(
        redis::cmd("HGET").arg(k("str")).arg("f").query::<Value>(&mut c),
        "WRONGTYPE",
    )?;
    del(&mut c, &[k("h2")])?;
    q(redis::cmd("HSET").arg(k("h2")).arg("f").arg("v").query::<i64>(&mut c))?;
    raises(redis::cmd("GET").arg(k("h2")).query::<Value>(&mut c), "WRONGTYPE")?;
    raises(incr(&mut c, &k("h2")), "WRONGTYPE")
}

fn mset(c: &mut Connection, pairs: &[(String, &str)]) -> Result<bool, String> {
    let mut cmd = redis::cmd("MSET");
    for (a, b) in pairs {
        cmd.arg(a).arg(*b);
    }
    q(cmd.query::<bool>(c))
}

fn mset_mget() -> R {
    let mut c = connect()?;
    eq(mset(&mut c, &[(k("m1"), "1"), (k("m2"), "2")])?, true)?;
    eq(
        q(redis::cmd("MGET")
            .arg(k("m1"))
            .arg(k("nope"))
            .arg(k("m2"))
            .query::<Vec<Option<Vec<u8>>>>(&mut c))?,
        vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())],
    )
}

fn del_exists_unlink() -> R {
    let mut c = connect()?;
    mset(&mut c, &[(k("d1"), "1"), (k("d2"), "2"), (k("d3"), "3")])?;
    eq(
        q(redis::cmd("EXISTS")
            .arg(k("d1"))
            .arg(k("d2"))
            .arg(k("nope"))
            .arg(k("d1"))
            .query::<i64>(&mut c))?,
        3,
    )?;
    eq(del(&mut c, &[k("d1"), k("nope")])?, 1)?;
    eq(
        q(redis::cmd("UNLINK").arg(k("d2")).arg(k("d3")).query::<i64>(&mut c))?,
        2,
    )?;
    eq(
        q(redis::cmd("EXISTS")
            .arg(k("d1"))
            .arg(k("d2"))
            .arg(k("d3"))
            .query::<i64>(&mut c))?,
        0,
    )
}

fn keys(c: &mut Connection, pat: &str) -> Result<Vec<Vec<u8>>, String> {
    q(redis::cmd("KEYS").arg(pat).query::<Vec<Vec<u8>>>(c))
}

fn keys_pattern() -> R {
    let mut c = connect()?;
    mset(&mut c, &[(k("k:1"), "a"), (k("k:2"), "b"), (k("k:x["), "c")])?;
    let mut got = keys(&mut c, &k("k:?"))?;
    got.sort();
    eq(got, vec![kb(b"k:1"), kb(b"k:2")])?;
    eq(keys(&mut c, &k("k:x\\["))?, vec![kb(b"k:x[")])?;
    eq(keys(&mut c, &k("nothing*"))?, Vec::<Vec<u8>>::new())
}

fn eval(c: &mut Connection, script: &str, keys: &[&[u8]], args: &[&[u8]]) -> RedisResult<Value> {
    let mut cmd = redis::cmd("EVAL");
    cmd.arg(script).arg(keys.len());
    for a in keys.iter().chain(args.iter()) {
        cmd.arg(*a);
    }
    cmd.query(c)
}

fn eval_values() -> R {
    let mut c = connect()?;
    let key = k("k");
    let v = q(eval(
        &mut c,
        "return {KEYS[1], ARGV[1], 7, {1, 2}}",
        &[key.as_bytes()],
        &[b"a"],
    ))?;
    eq(
        v,
        Value::Array(vec![
            bs(key.as_bytes()),
            bs(b"a"),
            Value::Int(7),
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
        ]),
    )?;
    eq(q(eval(&mut c, "return false", &[], &[]))?, Value::Nil)?;
    // redis-py yields b"FINE" for a status reply; compare as bytes so a
    // SimpleString/BulkString framing difference does not matter.
    let st = q(eval(&mut c, "return redis.status_reply('FINE')", &[], &[]))?;
    eq(redis::from_redis_value::<String>(st).map_err(|e| e.to_string())?, "FINE".to_string())?;
    eq(q(eval(&mut c, "return 3.99", &[], &[]))?, Value::Int(3))?;
    eq(
        q(eval(&mut c, "return ARGV[1]", &[], &[b"\x00\xff"]))?,
        bs(b"\x00\xff"),
    )
}

fn eval_calls_redis() -> R {
    let mut c = connect()?;
    let script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    eq(
        q(eval(&mut c, script, &[k("ev").as_bytes()], &[b"from-lua"]))?,
        bs(b"from-lua"),
    )?;
    eq(get(&mut c, k("ev"))?, Some(b"from-lua".to_vec()))?;
    eq(
        q(eval(
            &mut c,
            "return redis.call('INCRBY', KEYS[1], 5)",
            &[k("evn").as_bytes()],
            &[],
        ))?,
        Value::Int(5),
    )
}

fn eval_errors() -> R {
    let mut c = connect()?;
    raises(eval(&mut c, "return redis.call('NOSUCH')", &[], &[]), "unknown command")?;
    raises(eval(&mut c, "return redis.error_reply('boom')", &[], &[]), "boom")?;
    raises(eval(&mut c, "this is not lua", &[], &[]), "ERR")?;
    raises(eval(&mut c, "error('thrown')", &[], &[]), "thrown")?;
    eq(ping(&mut c)?, "PONG".to_string())
}

fn script_helper_evalsha() -> R {
    let mut c = connect()?;
    let s = redis::Script::new("return ARGV[1]");
    eq(q(s.arg("x").invoke::<Vec<u8>>(&mut c))?, b"x".to_vec())?;
    eq(q(s.arg("y").invoke::<Vec<u8>>(&mut c))?, b"y".to_vec())
}

fn pipeline_plain_10000() -> R {
    let mut c = connect()?;
    del(&mut c, &[k("pn")])?;
    let mut p = redis::pipe();
    for _ in 0..10000 {
        p.cmd("INCR").arg(k("pn"));
    }
    let out: Vec<i64> = q(p.query(&mut c))?;
    eq(out, (1..=10000).collect::<Vec<i64>>())
}

fn pipeline_with_error_in_middle() -> R {
    let mut c = connect()?;
    set_cmd(&mut c, k("pe"), "abc")?;
    let mut p = redis::pipe();
    p.cmd("SET").arg(k("pa")).arg("1");
    p.cmd("INCR").arg(k("pe"));
    p.cmd("GET").arg(k("pa"));
    // ignore_errors: per-command server errors come back as Value::ServerError
    // instead of failing the whole pipeline (like redis-py raise_on_error=False).
    p.ignore_errors();
    let out: Vec<Value> = q(p.query(&mut c))?;
    eq(out.len(), 3)?;
    eq(redis::from_redis_value::<bool>(out[0].clone()).map_err(|e| e.to_string())?, true)?;
    if !matches!(out[1], Value::ServerError(_)) {
        return Err(format!("out[1] is not a server error: {:?}", out[1]));
    }
    eq(out[2].clone(), bs(b"1"))
}

fn x_pipeline_transaction_multi_exec() -> R {
    let mut c = connect()?;
    let mut p = redis::pipe();
    p.atomic();
    p.cmd("SET").arg(k("tx")).arg("1");
    p.cmd("GET").arg(k("tx"));
    let out: (bool, Vec<u8>) = q(p.query(&mut c))?;
    eq(out, (true, b"1".to_vec()))
}

/// SET key value EX 60 through the library's SetOptions builder.
fn set_ex_60(c: &mut Connection, key: &str, v: &str) -> Result<bool, String> {
    use redis::{Commands, SetExpiry, SetOptions};
    let opts = SetOptions::default().with_expiration(SetExpiry::EX(60));
    q(c.set_options::<_, _, bool>(key, v, opts))
}

fn x_set_with_expiry_and_nx() -> R {
    let mut c = connect()?;
    eq(set_ex_60(&mut c, &k("ex"), "v")?, true)?;
    let ttl = q(redis::cmd("TTL").arg(k("ex")).query::<i64>(&mut c))?;
    if !(0 < ttl && ttl <= 60) {
        return Err(format!("ttl {ttl} not in (0, 60]"));
    }
    let nx = q(redis::cmd("SET")
        .arg(k("ex"))
        .arg("other")
        .arg("NX")
        .query::<Option<bool>>(&mut c))?;
    eq(nx, None)
}

fn concurrency_16_threads() -> R {
    let mut c = connect()?;
    del(&mut c, &[k("c")])?;
    let handles: Vec<_> = (0..16)
        .map(|_| {
            std::thread::spawn(|| -> R {
                let mut c = connect()?;
                for _ in 0..500 {
                    q(incr(&mut c, &k("c")))?;
                }
                Ok(())
            })
        })
        .collect();
    let mut errors = Vec::new();
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errors.push(e),
            Err(_) => errors.push("thread panicked".to_string()),
        }
    }
    eq(errors, Vec::<String>::new())?;
    eq(get(&mut c, k("c"))?, Some(b"8000".to_vec()))
}

fn unknown_command_keeps_connection() -> R {
    let mut c = connect()?;
    raises(
        redis::cmd("NOSUCHCMD").arg("a").query::<Value>(&mut c),
        "unknown command",
    )?;
    raises(
        redis::cmd("GET").query::<Value>(&mut c),
        "wrong number of arguments",
    )?;
    eq(ping(&mut c)?, "PONG".to_string())
}

fn hgetall_100k_fields() -> R {
    let mut c = connect()?;
    let h = k("bigh");
    del(&mut c, &[h.clone()])?;
    let mut p = redis::pipe();
    for i in (0..100000).step_by(1000) {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(&h);
        for j in i..i + 1000 {
            cmd.arg(format!("f{j}")).arg(format!("v{j}"));
        }
        p.add_command(cmd);
    }
    let out: Vec<i64> = q(p.query(&mut c))?;
    eq(out.iter().sum::<i64>(), 100000)?;
    let got: HashMap<Vec<u8>, Vec<u8>> = q(redis::cmd("HGETALL").arg(&h).query(&mut c))?;
    eq(got.len(), 100000)?;
    eq(got.get(b"f99999".as_slice()).cloned(), Some(b"v99999".to_vec()))?;
    eq(del(&mut c, &[h])?, 1)
}

fn client_side_commands() -> R {
    let mut c = connect()?;
    eq(
        q(redis::cmd("CLIENT").arg("SETNAME").arg("rs-test").query::<bool>(&mut c))?,
        true,
    )?;
    let info = q(redis::cmd("INFO").query::<String>(&mut c))?;
    if info.trim().is_empty() {
        return Err("INFO returned an empty string".to_string());
    }
    eq(q(redis::cmd("SELECT").arg(0).query::<bool>(&mut c))?, true)?;
    raises(
        redis::cmd("SELECT").arg("16").query::<Value>(&mut c),
        "out of range",
    )
}

fn flushall_dbsize() -> R {
    let mut c = connect()?;
    set_cmd(&mut c, k("last"), "1")?;
    let n = q(redis::cmd("DBSIZE").query::<i64>(&mut c))?;
    if n < 1 {
        return Err(format!("dbsize {n} < 1"));
    }
    eq(q(redis::cmd("FLUSHALL").query::<bool>(&mut c))?, true)?;
    eq(q(redis::cmd("DBSIZE").query::<i64>(&mut c))?, 0)?;
    eq(get(&mut c, k("last"))?, None)
}

// ---- lists, sets, sorted sets, keys, pub/sub, transactions, Lua, config ----

fn sv(xs: &[&str]) -> Vec<String> {
    xs.iter().map(|s| s.to_string()).collect()
}

fn hs(xs: &[&str]) -> HashSet<String> {
    xs.iter().map(|s| s.to_string()).collect()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn some(s: &str) -> Option<String> {
    Some(s.to_string())
}

/// Member -> score map built from (member, score) pairs.
fn zmap(pairs: &[(&str, f64)]) -> BTreeMap<String, f64> {
    pairs.iter().map(|(m, s)| (m.to_string(), *s)).collect()
}

/// Collect a SCAN-family iterator, failing on the first reply that does not parse.
fn drain<T, I: Iterator<Item = RedisResult<T>>>(it: RedisResult<I>) -> Result<Vec<T>, String> {
    q(it.and_then(|i| i.collect::<RedisResult<Vec<T>>>()))
}

fn conv<T: redis::FromRedisValue>(v: RedisResult<Value>) -> Result<T, String> {
    redis::from_redis_value(q(v)?).map_err(|e| e.to_string())
}

/// TYPE reply as the lowercase name Redis uses, via the library's ValueType.
fn type_name(c: &mut Connection, key: impl redis::ToSingleRedisArg) -> Result<String, String> {
    let t: redis::ValueType = q(c.key_type(key))?;
    Ok(format!("{t:?}").to_lowercase())
}

fn lists() -> R {
    let mut c = connect()?;
    let (k1, k2) = (k("l"), k("l2"));
    del(&mut c, &[k1.clone(), k2.clone()])?;
    eq(q(c.rpush(&k1, &["a", "b", "c"][..]))?, 3usize)?;
    eq(q(c.lpush(&k1, &["z", "y"][..]))?, 5usize)?; // y z a b c
    eq(q(c.llen(&k1))?, 5usize)?;
    eq(q(c.lrange(&k1, 0, -1))?, sv(&["y", "z", "a", "b", "c"]))?;
    eq(q(c.lrange(&k1, -2, -1))?, sv(&["b", "c"]))?;
    eq(q(c.lrange(&k1, 10, 20))?, Vec::<String>::new())?;
    eq(q(c.lindex(&k1, 0))?, some("y"))?;
    eq(q(c.lindex(&k1, -1))?, some("c"))?;
    eq(q(c.lindex(&k1, 99))?, None::<String>)?;
    q::<()>(c.lset(&k1, 1, "Z"))?;
    raises(c.lset::<_, _, ()>(&k1, 99, "x"), "out of range")?;
    eq(q(c.lpop(&k1, None))?, some("y"))?;
    eq(q(c.rpop(&k1, None))?, some("c"))?;
    eq(q(c.lpop(&k1, NonZeroUsize::new(2)))?, Some(sv(&["Z", "a"])))?;
    eq(q(c.rpop(&k1, NonZeroUsize::new(5)))?, Some(sv(&["b"])))?;
    eq(q(c.lpop(&k1, None))?, None::<String>)?;
    eq(q(c.lpop(&k1, NonZeroUsize::new(2)))?, None::<Vec<String>>)?;
    eq(q(c.exists(&k1))?, false)?;
    q::<usize>(c.rpush(&k1, &["a", "b", "a", "c", "a"][..]))?;
    eq(q(c.lrem(&k1, 2, "a"))?, 2usize)?;
    eq(q(c.lrange(&k1, 0, -1))?, sv(&["b", "c", "a"]))?;
    eq(q(c.lrem(&k1, -1, "a"))?, 1usize)?;
    eq(q(c.lrem(&k1, 0, "nope"))?, 0usize)?;
    q::<usize>(c.rpush(&k1, &["d", "e", "f"][..]))?; // b c d e f
    q::<()>(c.ltrim(&k1, 1, -2))?;
    eq(q(c.lrange(&k1, 0, -1))?, sv(&["c", "d", "e"]))?;
    eq(q(c.linsert_before(&k1, "d", "cd"))?, 4isize)?;
    eq(q(c.linsert_after(&k1, "e", "ef"))?, 5isize)?;
    eq(q(c.linsert_before(&k1, "missing", "x"))?, -1isize)?;
    eq(q(c.linsert_before(k("nolist"), "a", "x"))?, 0isize)?;
    eq(q(c.lrange(&k1, 0, -1))?, sv(&["c", "cd", "d", "e", "ef"]))?;
    q::<usize>(c.rpush(&k1, "d"))?;
    eq(q(c.lpos(&k1, "d", LposOptions::default()))?, Some(2usize))?;
    eq(q(c.lpos(&k1, "d", LposOptions::default().rank(2)))?, Some(5usize))?;
    eq(q(c.lpos(&k1, "d", LposOptions::default().count(0)))?, vec![2usize, 5])?;
    eq(q(c.lpos(&k1, "nope", LposOptions::default()))?, None::<usize>)?;
    eq(q(c.lmove(&k1, &k2, Direction::Left, Direction::Right))?, "c".to_string())?;
    eq(q(c.lmove(&k1, &k2, Direction::Right, Direction::Left))?, "d".to_string())?;
    eq(q(c.lrange(&k2, 0, -1))?, sv(&["d", "c"]))?;
    eq(q(c.rpoplpush(&k1, &k2))?, some("ef"))?;
    eq(q(c.lrange(&k2, 0, -1))?, sv(&["ef", "d", "c"]))?;
    eq(
        q(redis::cmd("LMOVE").arg(k("nolist")).arg(&k2).arg("LEFT").arg("LEFT").query::<Option<String>>(&mut c))?,
        None,
    )?;
    eq(type_name(&mut c, &k1)?, "list".to_string())?;
    raises(c.get::<_, Value>(&k1), "WRONGTYPE")?;
    set_cmd(&mut c, k("lstr"), "x")?;
    raises(c.lpush::<_, _, Value>(k("lstr"), "x"), "WRONGTYPE")?;
    eq(del(&mut c, &[k1, k2, k("lstr")])?, 3)
}

fn sets() -> R {
    let mut c = connect()?;
    let (a, b, d, e) = (k("seta"), k("setb"), k("setc"), k("setd"));
    del(&mut c, &[a.clone(), b.clone(), d.clone(), e.clone()])?;
    eq(q(c.sadd(&a, &["1", "2", "3", "4", "4"][..]))?, 4usize)?;
    eq(q(c.sadd(&a, "1"))?, 0usize)?;
    eq(q(c.scard(&a))?, 4usize)?;
    eq(q(c.sismember(&a, "2"))?, true)?;
    eq(q(c.sismember(&a, "9"))?, false)?;
    eq(q(c.smismember(&a, &["1", "9", "4"][..]))?, vec![true, false, true])?;
    eq(q(c.smembers(&a))?, hs(&["1", "2", "3", "4"]))?;
    eq(q(c.srem(&a, &["4", "9"][..]))?, 1usize)?;
    q::<usize>(c.sadd(&b, &["2", "3", "5"][..]))?;
    q::<usize>(c.sadd(&d, &["3", "6"][..]))?;
    eq(q(c.sinter(&[&a, &b][..]))?, hs(&["2", "3"]))?;
    eq(q(c.sinter(&[&a, &b, &d][..]))?, hs(&["3"]))?;
    eq(q(c.sunion(&[&a, &b, &d][..]))?, hs(&["1", "2", "3", "5", "6"]))?;
    eq(q(c.sdiff(&[&a, &b][..]))?, hs(&["1"]))?;
    eq(q(c.sdiff(&[&a, &b, &d][..]))?, hs(&["1"]))?;
    eq(q(c.sinterstore(&e, &[&a, &b][..]))?, 2usize)?;
    eq(q(c.smembers(&e))?, hs(&["2", "3"]))?;
    eq(q(c.sunionstore(&e, &[&a, &b][..]))?, 4usize)?;
    eq(q(c.sdiffstore(&e, &[&b, &a][..]))?, 1usize)?;
    eq(q(c.smembers(&e))?, hs(&["5"]))?;
    // no typed SINTERCARD in redis-rs
    let card = |c: &mut Connection, limit: Option<i64>| -> Result<i64, String> {
        let mut cmd = redis::cmd("SINTERCARD");
        cmd.arg(2).arg(&a).arg(&b);
        if let Some(l) = limit {
            cmd.arg("LIMIT").arg(l);
        }
        q(cmd.query::<i64>(c))
    };
    eq(card(&mut c, None)?, 2)?;
    eq(card(&mut c, Some(1))?, 1)?;
    eq(q(c.smove(&a, &d, "1"))?, true)?;
    eq(q(c.smove(&a, &d, "nope"))?, false)?;
    eq(q(c.smembers(&d))?, hs(&["1", "3", "6"]))?;
    let one: Option<String> = q(c.srandmember(&d))?;
    if !one.as_deref().is_some_and(|s| ["1", "3", "6"].contains(&s)) {
        return Err(format!("srandmember outside set: {one:?}"));
    }
    let two: Vec<String> = q(c.srandmember_multiple(&d, 2))?;
    eq(two.iter().collect::<HashSet<_>>().len(), 2)?;
    eq(q::<Vec<String>>(c.srandmember_multiple(&d, 10))?.len(), 3usize)?;
    eq(q::<Vec<String>>(c.srandmember_multiple(&d, -5))?.len(), 5usize)?;
    eq(q(c.srandmember(k("noset")))?, None::<String>)?;
    let p: Option<String> = q(c.spop(&d))?;
    if !p.as_deref().is_some_and(|s| ["1", "3", "6"].contains(&s)) {
        return Err(format!("spop outside set: {p:?}"));
    }
    let rest: Vec<String> = q(redis::cmd("SPOP").arg(&d).arg(5).query(&mut c))?;
    eq(rest.len(), 2)?;
    eq(q(c.exists(&d))?, false)?;
    eq(q(c.spop(&d))?, None::<String>)?;
    let big = k("setbig");
    del(&mut c, &[big.clone()])?;
    let members: Vec<String> = (0..300).map(|i| format!("m{i}")).collect();
    q::<usize>(c.sadd(&big, &members[..]))?;
    let seen: HashSet<String> = drain(c.sscan::<_, String>(&big))?.into_iter().collect();
    eq(seen.len(), 300)?;
    eq(type_name(&mut c, &big)?, "set".to_string())?;
    raises(c.get::<_, Value>(&big), "WRONGTYPE")?;
    del(&mut c, &[big, a, b, d, e])?;
    Ok(())
}

fn zadd_m(c: &mut Connection, key: &str, items: &[(f64, &str)]) -> Result<usize, String> {
    q(c.zadd_multiple(key, items))
}

fn zsets_basic() -> R {
    // membership and order only; score replies are checked in zset_scores
    let mut c = connect()?;
    let key = k("z");
    del(&mut c, &[key.clone()])?;
    eq(zadd_m(&mut c, &key, &[(1.0, "a"), (2.5, "b"), (1.5, "c")])?, 3)?; // a c b
    eq(q(c.zadd(&key, "a", 1))?, 0usize)?;
    let ch = SortedSetAddOptions::add_or_update(None).include_changed_count();
    eq(q(c.zadd_options(&key, "a", 5, &ch))?, 1usize)?; // c b a
    eq(q(c.zadd_multiple_options(&key, &[(7, "d"), (5, "a")], &ch))?, 1usize)?; // c b a d
    eq(q(c.zadd_options(&key, "a", 9, &SortedSetAddOptions::add_only()))?, 0usize)?;
    eq(q(c.zrange(&key, 0, -1))?, sv(&["c", "b", "a", "d"]))?;
    eq(q(c.zadd_options(&key, "zz", 1, &SortedSetAddOptions::update_only(None)))?, 0usize)?;
    eq(q(c.zcard(&key))?, 4usize)?;
    let gt = SortedSetAddOptions::add_or_update(Some(UpdateCheck::GT)).include_changed_count();
    let lt = SortedSetAddOptions::add_or_update(Some(UpdateCheck::LT)).include_changed_count();
    eq(q(c.zadd_options(&key, "a", 3, &gt))?, 0usize)?;
    eq(q(c.zadd_options(&key, "a", 8, &gt))?, 1usize)?; // c b d a
    eq(q(c.zrange(&key, 0, -1))?, sv(&["c", "b", "d", "a"]))?;
    eq(q(c.zadd_options(&key, "a", 10, &lt))?, 0usize)?;
    eq(q(c.zadd_options(&key, "a", 6, &lt))?, 1usize)?; // c b a d
    eq(q(c.zrange(&key, 0, -1))?, sv(&["c", "b", "a", "d"]))?;
    let nx_incr = SortedSetAddOptions::add_only().increment_score();
    let xx_incr = SortedSetAddOptions::update_only(None).increment_score();
    eq(q(c.zadd_options(&key, "a", 1, &nx_incr))?, None::<f64>)?;
    eq(q(c.zadd_options(&key, "nope", 1, &xx_incr))?, None::<f64>)?;
    eq(q(c.zrank(&key, "c"))?, Some(0usize))?;
    eq(q(c.zrank(&key, "d"))?, Some(3usize))?;
    eq(q(c.zrevrank(&key, "d"))?, Some(0usize))?;
    eq(q(c.zrevrank(&key, "c"))?, Some(3usize))?;
    eq(q(c.zrank(&key, "nope"))?, None::<usize>)?;
    eq(q(c.zrevrank(&key, "nope"))?, None::<usize>)?;
    eq(q(c.zcount(&key, 2, 6))?, 2usize)?;
    eq(q(c.zcount(&key, "(2.5", 7))?, 2usize)?;
    eq(q(c.zcount(&key, "(1.5", "(7"))?, 2usize)?;
    eq(q(c.zcount(&key, "-inf", "+inf"))?, 4usize)?;
    eq(q(c.zcount(&key, 100, 200))?, 0usize)?;
    eq(q(c.zrem(&key, &["a", "nope"][..]))?, 1usize)?;
    eq(q(c.zrange(&key, 0, -1))?, sv(&["c", "b", "d"]))?;
    eq(type_name(&mut c, &key)?, "zset".to_string())?;
    raises(c.get::<_, Value>(&key), "WRONGTYPE")?;
    raises(redis::cmd("ZADD").arg(&key).arg("notanumber").arg("a").query::<Value>(&mut c), "not a valid float")?;
    raises(
        redis::cmd("ZADD").arg(&key).arg("NX").arg("XX").arg(1).arg("a").query::<Value>(&mut c),
        "not compatible",
    )?;
    eq(del(&mut c, &[key])?, 1)
}

fn zset_scores() -> R {
    let mut c = connect()?;
    let key = k("zsc");
    let (inf, ninf) = (f64::INFINITY, f64::NEG_INFINITY);
    del(&mut c, &[key.clone()])?;
    zadd_m(&mut c, &key, &[(1.0, "a"), (2.5, "b"), (1.5, "c")])?;
    eq(q(c.zscore(&key, "a"))?, Some(1.0f64))?;
    eq(q(c.zscore(&key, "c"))?, Some(1.5f64))?;
    eq(q(c.zscore(&key, "nope"))?, None::<f64>)?;
    eq(
        q(redis::cmd("ZMSCORE").arg(&key).arg("a").arg("nope").arg("c").query::<Vec<Option<f64>>>(&mut c))?,
        vec![Some(1.0), None, Some(1.5)],
    )?;
    let incr = SortedSetAddOptions::add_or_update(None).increment_score();
    eq(q(c.zadd_options(&key, "a", 4.25, &incr))?, Some(5.25f64))?;
    let xx_incr = SortedSetAddOptions::update_only(None).increment_score();
    eq(q(c.zadd_options(&key, "a", -0.25, &xx_incr))?, Some(5.0f64))?;
    eq(q(c.zincr(&key, "c", 0.5))?, 2.0f64)?;
    eq(q(c.zincr(&key, "new", 1))?, 1.0f64)?; // new c b a
    eq(
        q(redis::cmd("ZRANK").arg(&key).arg("c").arg("WITHSCORE").query::<(i64, f64)>(&mut c))?,
        (1, 2.0),
    )?;
    eq(
        q(redis::cmd("ZREVRANK").arg(&key).arg("c").arg("WITHSCORE").query::<(i64, f64)>(&mut c))?,
        (2, 2.0),
    )?;
    eq(
        q(redis::cmd("ZRANK").arg(&key).arg("nope").arg("WITHSCORE").query::<Option<(i64, f64)>>(&mut c))?,
        None,
    )?;
    eq(
        q(c.zrange_withscores(&key, 0, -1))?,
        vec![("new".to_string(), 1.0), ("c".to_string(), 2.0), ("b".to_string(), 2.5), ("a".to_string(), 5.0)],
    )?;
    eq(
        q(redis::cmd("ZRANGE").arg(&key).arg(0).arg(1).arg("REV").arg("WITHSCORES").query::<Vec<(String, f64)>>(&mut c))?,
        vec![("a".to_string(), 5.0), ("b".to_string(), 2.5)],
    )?;
    eq(
        q(c.zrangebyscore_withscores(&key, 2, 3))?,
        vec![("c".to_string(), 2.0f64), ("b".to_string(), 2.5)],
    )?;
    eq(q(c.zrevrange_withscores(&key, 0, 0))?, vec![("a".to_string(), 5.0f64)])?;
    eq(
        q(c.zrevrangebyscore_withscores(&key, 5, 2.5))?,
        vec![("a".to_string(), 5.0f64), ("b".to_string(), 2.5)],
    )?;
    eq(
        q(redis::cmd("ZRANGE")
            .arg(&key)
            .arg(2)
            .arg(5)
            .arg("BYSCORE")
            .arg("LIMIT")
            .arg(1)
            .arg(1)
            .arg("WITHSCORES")
            .query::<Vec<(String, f64)>>(&mut c))?,
        vec![("b".to_string(), 2.5)],
    )?;
    del(&mut c, &[key.clone()])?;
    zadd_m(&mut c, &key, &[(ninf, "lo"), (inf, "hi"), (1.5, "mid"), (-0.1, "frac")])?;
    eq(q(c.zrange(&key, 0, -1))?, sv(&["lo", "frac", "mid", "hi"]))?;
    eq(
        q(c.zrange_withscores(&key, 0, -1))?,
        vec![("lo".to_string(), ninf), ("frac".to_string(), -0.1), ("mid".to_string(), 1.5), ("hi".to_string(), inf)],
    )?;
    eq(q(c.zscore(&key, "hi"))?, Some(inf))?;
    eq(q(c.zscore(&key, "lo"))?, Some(ninf))?;
    eq(q(c.zrangebyscore(&key, "-inf", "+inf"))?, sv(&["lo", "frac", "mid", "hi"]))?;
    eq(q(c.zrangebyscore(&key, "(-inf", "(inf"))?, sv(&["frac", "mid"]))?;
    eq(q(c.zcount(&key, "-inf", 0))?, 2usize)?;
    eq(q(c.zincr(&key, "hi", 5))?, inf)?;
    let got: BTreeMap<String, f64> = drain(c.zscan::<_, (String, f64)>(&key))?.into_iter().collect();
    eq(got, zmap(&[("lo", ninf), ("frac", -0.1), ("mid", 1.5), ("hi", inf)]))?;
    del(&mut c, &[key])?;
    Ok(())
}

fn zset_pop() -> R {
    let mut c = connect()?;
    let key = k("zp");
    del(&mut c, &[key.clone()])?;
    zadd_m(&mut c, &key, &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.5, "d"), (5.0, "e")])?;
    let pair = |m: &str, s: f64| (m.to_string(), s);
    eq(q(c.zpopmin(&key, 1))?, vec![pair("a", 1.0)])?;
    eq(q(c.zpopmax(&key, 1))?, vec![pair("e", 5.0)])?;
    eq(q(c.zpopmin(&key, 2))?, vec![pair("b", 2.0), pair("c", 3.0)])?;
    eq(q(c.zpopmax(&key, 5))?, vec![pair("d", 4.5)])?;
    eq(q(c.zpopmin(&key, 1))?, Vec::<(String, f64)>::new())?;
    eq(q(c.zpopmax(&key, 3))?, Vec::<(String, f64)>::new())?;
    eq(q(c.exists(&key))?, false)
}

fn zsets_ranges() -> R {
    let mut c = connect()?;
    let key = k("zr");
    del(&mut c, &[key.clone()])?;
    zadd_m(&mut c, &key, &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d"), (5.0, "e")])?;
    let zr = |c: &mut Connection, args: &[&str]| -> Result<Vec<String>, String> {
        let mut cmd = redis::cmd("ZRANGE");
        cmd.arg(&key);
        for a in args {
            cmd.arg(*a);
        }
        q(cmd.query::<Vec<String>>(c))
    };
    eq(q(c.zrange(&key, 0, 1))?, sv(&["a", "b"]))?;
    eq(q(c.zrange(&key, -2, -1))?, sv(&["d", "e"]))?;
    eq(zr(&mut c, &["0", "1", "REV"])?, sv(&["e", "d"]))?;
    eq(zr(&mut c, &["2", "3", "BYSCORE"])?, sv(&["b", "c"]))?;
    eq(zr(&mut c, &["(1", "+inf", "BYSCORE", "LIMIT", "1", "2"])?, sv(&["c", "d"]))?;
    eq(zr(&mut c, &["+inf", "(2", "BYSCORE", "REV", "LIMIT", "0", "2"])?, sv(&["e", "d"]))?;
    eq(q(c.zrangebyscore(&key, "-inf", 3))?, sv(&["a", "b", "c"]))?;
    eq(q(c.zrangebyscore(&key, "(1", "(4"))?, sv(&["b", "c"]))?;
    eq(q(c.zrangebyscore_limit(&key, 1, 5, 1, 2))?, sv(&["b", "c"]))?;
    eq(q(c.zrevrangebyscore(&key, 5, 4))?, sv(&["e", "d"]))?;
    eq(q(c.zrevrangebyscore_limit(&key, "+inf", "-inf", 0, 1))?, sv(&["e"]))?;
    eq(q(c.zrevrange(&key, 0, 1))?, sv(&["e", "d"]))?;
    eq(q(c.zrange(&key, 10, 20))?, Vec::<String>::new())?;
    eq(q(c.zrange(k("nozset"), 0, -1))?, Vec::<String>::new())?;
    eq(q(c.zremrangebyrank(&key, 0, 1))?, 2usize)?; // c d e
    eq(q(c.zrembyscore(&key, "(3", 4))?, 1usize)?; // c e
    eq(q(c.zrange(&key, 0, -1))?, sv(&["c", "e"]))?;
    eq(q(c.zrembyscore(&key, "-inf", "+inf"))?, 2usize)?;
    eq(q(c.exists(&key))?, false)?;
    let big = k("zbig");
    del(&mut c, &[big.clone()])?;
    let items: Vec<(f64, String)> = (0..300).map(|i| (i as f64, format!("m{i}"))).collect();
    q::<usize>(c.zadd_multiple(&big, &items[..]))?;
    let seen: BTreeMap<String, f64> = drain(c.zscan::<_, (String, f64)>(&big))?.into_iter().collect();
    eq(seen.len(), 300)?;
    eq(seen.get("m299").copied(), Some(299.0))?;
    del(&mut c, &[big])?;
    Ok(())
}

fn scores(c: &mut Connection, key: &str) -> Result<BTreeMap<String, f64>, String> {
    Ok(drain(c.zscan::<_, (String, f64)>(key))?.into_iter().collect())
}

fn zsets_store() -> R {
    let mut c = connect()?;
    let (a, b, d) = (k("zsa"), k("zsb"), k("zsd"));
    del(&mut c, &[a.clone(), b.clone(), d.clone(), k("zss")])?;
    zadd_m(&mut c, &a, &[(1.0, "x"), (2.0, "y"), (3.0, "z")])?;
    zadd_m(&mut c, &b, &[(10.0, "y"), (20.0, "z"), (0.5, "w")])?;
    eq(q(c.zunionstore(&d, &[&a, &b][..]))?, 4usize)?;
    eq(q(c.zrange(&d, 0, -1))?, sv(&["w", "x", "y", "z"]))?;
    eq(q(c.zunionstore_weights(&d, &[(&a, 2.0), (&b, 0.5)]))?, 4usize)?;
    eq(scores(&mut c, &d)?, zmap(&[("w", 0.25), ("x", 2.0), ("y", 9.0), ("z", 16.0)]))?;
    eq(q(c.zunionstore_max(&d, &[&a, &b][..]))?, 4usize)?;
    eq(scores(&mut c, &d)?["z"], 20.0)?;
    eq(q(c.zinterstore(&d, &[&a, &b][..]))?, 2usize)?;
    eq(scores(&mut c, &d)?, zmap(&[("y", 12.0), ("z", 23.0)]))?;
    eq(q(c.zinterstore_weights(&d, &[(&a, 3.0), (&b, 2.0)]))?, 2usize)?;
    eq(scores(&mut c, &d)?, zmap(&[("y", 26.0), ("z", 49.0)]))?;
    eq(q(c.zinterstore_min(&d, &[&a, &b][..]))?, 2usize)?;
    eq(scores(&mut c, &d)?["y"], 2.0)?;
    q::<usize>(c.sadd(k("zss"), &["y", "q"][..]))?;
    eq(q(c.zinterstore(&d, &[a.clone(), k("zss")][..]))?, 1usize)?; // sets count as score 1
    eq(scores(&mut c, &d)?["y"], 3.0)?;
    eq(q(c.zinterstore(&d, &[a.clone(), k("nozset")][..]))?, 0usize)?;
    eq(q(c.exists(&d))?, false)?;
    del(&mut c, &[a, b, d, k("zss")])?;
    Ok(())
}

fn string_key_extras() -> R {
    let mut c = connect()?;
    let h = k("hx");
    let all = [k("f"), k("gr"), k("grnew"), k("abc"), h.clone()];
    del(&mut c, &all)?;
    eq(q(c.incr(k("f"), 1.5f64))?, 1.5f64)?;
    eq(q(c.incr(k("f"), 2.0f64))?, 3.5f64)?;
    eq(q(c.incr(k("f"), -0.25f64))?, 3.25f64)?;
    eq(get(&mut c, k("f"))?, Some(b"3.25".to_vec()))?;
    set_cmd(&mut c, k("abc"), "abc")?;
    raises(c.incr::<_, _, Value>(k("abc"), 1.0f64), "not a valid float")?;
    set_cmd(&mut c, k("gr"), "Hello World")?;
    eq(q(c.getrange(k("gr"), 0, 4))?, "Hello".to_string())?;
    eq(q(c.getrange(k("gr"), -5, -1))?, "World".to_string())?;
    eq(q(c.getrange(k("gr"), 20, 30))?, String::new())?;
    eq(q(c.setrange(k("gr"), 6, "Redis"))?, 11usize)?;
    eq(get(&mut c, k("gr"))?, Some(b"Hello Redis".to_vec()))?;
    eq(q(c.setrange(k("gr"), 13, "!"))?, 14usize)?;
    eq(get(&mut c, k("gr"))?, Some(b"Hello Redis\0\0!".to_vec()))?;
    eq(q(c.setrange(k("grnew"), 2, "x"))?, 3usize)?;
    eq(get(&mut c, k("grnew"))?, Some(b"\0\0x".to_vec()))?;
    q::<()>(c.hset_multiple(&h, &[("a", "1"), ("b", "hello"), ("c", "2.5")]))?;
    eq(sorted(q(c.hkeys(&h))?), sv(&["a", "b", "c"]))?;
    eq(sorted(q(c.hvals(&h))?), sv(&["1", "2.5", "hello"]))?;
    let hstrlen = |c: &mut Connection, f: &str| q(redis::cmd("HSTRLEN").arg(&h).arg(f).query::<i64>(c));
    eq(hstrlen(&mut c, "b")?, 5)?;
    eq(hstrlen(&mut c, "nope")?, 0)?;
    eq(q(c.hincr(&h, "c", 0.5f64))?, 3.0f64)?;
    eq(q(c.hincr(&h, "new", 1.5f64))?, 1.5f64)?;
    eq(q(c.hincr(&h, "a", 4))?, 5i64)?;
    raises(c.hincr::<_, _, _, Value>(&h, "b", 1.0f64), "not a float")?;
    let rf = |c: &mut Connection, args: &[&str]| -> RedisResult<Value> {
        let mut cmd = redis::cmd("HRANDFIELD");
        cmd.arg(&h);
        for a in args {
            cmd.arg(*a);
        }
        cmd.query(c)
    };
    let one: Option<String> = conv(rf(&mut c, &[]))?;
    if !one.as_deref().is_some_and(|s| ["a", "b", "c", "new"].contains(&s)) {
        return Err(format!("hrandfield outside hash: {one:?}"));
    }
    eq(conv::<Vec<String>>(rf(&mut c, &["2"]))?.len(), 2)?;
    eq(conv::<Vec<String>>(rf(&mut c, &["-6"]))?.len(), 6)?;
    eq(
        q(redis::cmd("HRANDFIELD").arg(k("nohash")).query::<Option<String>>(&mut c))?,
        None,
    )?;
    eq(
        q(redis::cmd("HRANDFIELD").arg(k("nohash")).arg(3).query::<Vec<String>>(&mut c))?,
        Vec::<String>::new(),
    )?;
    eq(q(c.hexists(&h, "a"))?, true)?;
    eq(q(c.hget(&h, "zz"))?, None::<String>)?;
    eq(q(c.hset_nx(&h, "a", "x"))?, false)?;
    del(&mut c, &all)?;
    Ok(())
}

fn hrandfield_values() -> R {
    let mut c = connect()?;
    let h = k("hrf");
    del(&mut c, &[h.clone()])?;
    q::<()>(c.hset_multiple(&h, &[("a", "1"), ("b", "hello"), ("c", "3")]))?;
    let got: Vec<(String, String)> =
        q(redis::cmd("HRANDFIELD").arg(&h).arg(3).arg("WITHVALUES").query(&mut c))?;
    let want: BTreeMap<String, String> = [("a", "1"), ("b", "hello"), ("c", "3")]
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
    eq(got.len(), 3)?;
    eq(got.into_iter().collect::<BTreeMap<_, _>>(), want)?;
    let neg: Vec<(String, String)> =
        q(redis::cmd("HRANDFIELD").arg(&h).arg(-5).arg("WITHVALUES").query(&mut c))?;
    eq(neg.len(), 5)?;
    del(&mut c, &[h])?;
    Ok(())
}

fn keys_misc() -> R {
    let mut c = connect()?;
    let names = [
        "r1", "r2", "r3", "cp1", "cp2", "ms1", "ms2", "ms3", "ty:s", "ty:h", "ty:l", "ty:set", "ty:z", "ty:int",
    ];
    let all: Vec<String> = names.iter().map(|n| k(n)).collect();
    del(&mut c, &all)?;
    set_cmd(&mut c, k("r1"), "v")?;
    q::<()>(c.rename(k("r1"), k("r2")))?;
    eq(q(c.exists(&[k("r1"), k("r2")][..]).map(|n: usize| n))?, 1usize)?;
    raises(c.rename::<_, _, ()>(k("nokey"), k("x")), "no such key")?;
    set_cmd(&mut c, k("r1"), "other")?;
    eq(q(c.rename_nx(k("r1"), k("r2")))?, false)?;
    eq(get(&mut c, k("r2"))?, Some(b"v".to_vec()))?;
    eq(q(c.rename_nx(k("r1"), k("r3")))?, true)?;
    eq(get(&mut c, k("r3"))?, Some(b"other".to_vec()))?;
    q::<usize>(c.rpush(k("cp1"), &["a", "b"][..]))?;
    // CopyOptions always carries a DB option here only when set; default sends plain COPY src dst
    eq(q(c.copy(k("cp1"), k("cp2"), CopyOptions::default()))?, true)?;
    eq(q(c.copy(k("cp1"), k("cp2"), CopyOptions::default()))?, false)?;
    eq(q(c.copy(k("cp1"), k("cp2"), CopyOptions::default().replace(true)))?, true)?;
    eq(q(c.lrange(k("cp2"), 0, -1))?, sv(&["a", "b"]))?;
    q::<usize>(c.rpush(k("cp1"), "c"))?;
    eq(q(c.llen(k("cp2")))?, 2usize)?; // a copy, not an alias
    eq(q(c.copy(k("nokey"), k("cp3"), CopyOptions::default()))?, false)?;
    eq(q(c.mset_nx(&[(k("ms1"), "1"), (k("ms2"), "2")]))?, true)?;
    eq(q(c.mset_nx(&[(k("ms1"), "x"), (k("ms3"), "3")]))?, false)?;
    eq(q(c.exists(k("ms3")))?, false)?;
    eq(get(&mut c, k("ms1"))?, Some(b"1".to_vec()))?;
    eq(
        q(redis::cmd("TOUCH").arg(k("r2")).arg(k("cp1")).arg(k("nokey")).query::<i64>(&mut c))?,
        2,
    )?;
    set_cmd(&mut c, k("ty:s"), "x")?;
    q::<usize>(c.hset(k("ty:h"), "f", "v"))?;
    q::<usize>(c.rpush(k("ty:l"), "x"))?;
    q::<usize>(c.sadd(k("ty:set"), "x"))?;
    q::<usize>(c.zadd(k("ty:z"), "x", 1))?;
    let mut types = Vec::new();
    for t in ["s", "h", "l", "set", "z"] {
        types.push(type_name(&mut c, k(&format!("ty:{t}")))?);
    }
    eq(types, sv(&["string", "hash", "list", "set", "zset"]))?;
    eq(type_name(&mut c, k("nokey"))?, "none".to_string())?;
    for n in ["ty:s", "ty:h", "ty:l", "ty:set", "ty:z"] {
        let enc: Option<String> = q(c.object_encoding(k(n)))?;
        if enc.as_deref().unwrap_or("").is_empty() {
            return Err(format!("encoding of {n}: {enc:?}"));
        }
    }
    eq(q(c.object_encoding(k("nokey")))?, None::<String>)?;
    let senc: Option<String> = q(c.object_encoding(k("ty:s")))?;
    if !matches!(senc.as_deref(), Some("embstr" | "raw")) {
        return Err(format!("string encoding {senc:?}"));
    }
    set_cmd(&mut c, k("ty:int"), "12345")?;
    eq(q(c.object_encoding(k("ty:int")))?, some("int"))?;
    let scan_type = |c: &mut Connection, t: &str| -> Result<Vec<String>, String> {
        let opts = ScanOptions::default().with_pattern(format!("{P}ty:*")).with_count(100).with_type(t);
        let mut got: Vec<String> = drain(c.scan_options::<String>(opts))?;
        got.sort();
        got.dedup();
        Ok(got)
    };
    eq(scan_type(&mut c, "string")?, vec![k("ty:int"), k("ty:s")])?;
    eq(scan_type(&mut c, "hash")?, vec![k("ty:h")])?;
    eq(scan_type(&mut c, "list")?, vec![k("ty:l")])?;
    eq(scan_type(&mut c, "set")?, vec![k("ty:set")])?;
    eq(scan_type(&mut c, "zset")?, vec![k("ty:z")])?;
    let rk: Option<String> = q(redis::cmd("RANDOMKEY").query(&mut c))?;
    let rk = rk.ok_or("RANDOMKEY returned nil")?;
    eq(q(c.exists(&rk))?, true)?;
    del(&mut c, &all)?;
    Ok(())
}

/// Poll until `cond` holds; a missing subscription fails the test instead of racing it.
fn wait_for(what: &str, mut cond: impl FnMut() -> Result<bool, String>) -> R {
    let end = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if cond()? {
            return Ok(());
        }
        if std::time::Instant::now() > end {
            return Err(format!("timed out waiting for {what}"));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn numsub(c: &mut Connection, chan: &str) -> Result<i64, String> {
    let v: Vec<(String, i64)> = q(redis::cmd("PUBSUB").arg("NUMSUB").arg(chan).query(c))?;
    Ok(v.first().map(|e| e.1).unwrap_or(-1))
}

fn numpat(c: &mut Connection) -> Result<i64, String> {
    q(redis::cmd("PUBSUB").arg("NUMPAT").query::<i64>(c))
}

fn pubsub_roundtrip() -> R {
    let mut sc = connect()?;
    let mut pc = connect()?;
    let (ch, ch2, pat, both) = (k("chan"), k("chan2"), k("pat:*"), k("pat:both"));
    let mut ps = sc.as_pubsub();
    ps.set_read_timeout(Some(Duration::from_secs(5))).map_err(|e| e.to_string())?;
    let next = |ps: &mut redis::PubSub| -> Result<(String, Option<String>, String), String> {
        let m = ps.get_message().map_err(|e| format!("no pub/sub message: {e}"))?;
        let pattern = if m.from_pattern() { Some(q(m.get_pattern::<String>())?) } else { None };
        Ok((m.get_channel_name().to_string(), pattern, q(m.get_payload::<String>())?))
    };
    q(ps.subscribe(&ch))?;
    q(ps.psubscribe(&pat))?;
    wait_for("subscribe", || Ok(numsub(&mut pc, &ch)? == 1 && numpat(&mut pc)? == 1))?;
    eq(q(pc.publish(&ch, "hello"))?, 1usize)?;
    eq(next(&mut ps)?, (ch.clone(), None, "hello".to_string()))?;
    eq(q(pc.publish(k("pat:one"), "p1"))?, 1usize)?;
    eq(next(&mut ps)?, (k("pat:one"), Some(pat.clone()), "p1".to_string()))?;
    eq(q(pc.publish(k("other"), "x"))?, 0usize)?;
    eq(q(pc.publish(&ch2, "x"))?, 0usize)?;
    q(ps.subscribe(&both))?;
    q(ps.psubscribe(k("pat:b*")))?;
    wait_for("second subscribe", || Ok(numsub(&mut pc, &both)? == 1 && numpat(&mut pc)? == 2))?;
    eq(q(pc.publish(&both, "twice"))?, 3usize)?;
    let mut kinds = Vec::new();
    for _ in 0..3 {
        let (_, pattern, payload) = next(&mut ps)?;
        eq(payload, "twice".to_string())?;
        kinds.push(pattern.is_some());
    }
    kinds.sort();
    eq(kinds, vec![false, true, true])?;
    eq(numsub(&mut pc, &ch)?, 1)?;
    eq(numsub(&mut pc, &ch2)?, 0)?;
    eq(numpat(&mut pc)?, 2)?;
    let mut chans: Vec<String> = q(redis::cmd("PUBSUB").arg("CHANNELS").arg(k("*")).query(&mut pc))?;
    chans.sort();
    eq(chans, sorted(vec![ch.clone(), both.clone()]))?;
    q(ps.punsubscribe(k("pat:b*")))?;
    q(ps.unsubscribe(&ch))?;
    wait_for("unsubscribe", || Ok(numsub(&mut pc, &ch)? == 0 && numpat(&mut pc)? == 1))?;
    eq(q(pc.publish(&ch, "gone"))?, 0usize)?;
    q(ps.punsubscribe(&pat))?;
    wait_for("punsubscribe", || Ok(numpat(&mut pc)? == 0))?;
    eq(q(pc.publish(k("pat:one"), "gone"))?, 0usize)?;
    q(ps.unsubscribe(&both))?;
    wait_for("last unsubscribe", || Ok(numsub(&mut pc, &both)? == 0))?;
    Ok(())
}

fn tx_new_types() -> R {
    let mut c = connect()?;
    let all = [k("txl"), k("txz"), k("txs"), k("w"), k("txstr")];
    del(&mut c, &all)?;
    let mut p = redis::pipe();
    p.atomic();
    p.cmd("RPUSH").arg(k("txl")).arg("a").arg("b");
    p.cmd("ZADD").arg(k("txz")).arg(1.5).arg("m").arg(2).arg("n");
    p.cmd("SADD").arg(k("txs")).arg("x").arg("y");
    p.cmd("LRANGE").arg(k("txl")).arg(0).arg(-1);
    p.cmd("ZRANGE").arg(k("txz")).arg(0).arg(-1);
    p.cmd("SCARD").arg(k("txs"));
    let out: (i64, i64, i64, Vec<String>, Vec<String>, i64) = q(p.query(&mut c))?;
    eq(out, (2, 2, 2, sv(&["a", "b"]), sv(&["m", "n"]), 2))?;
    let mut p = redis::pipe();
    p.atomic();
    p.cmd("LPOP").arg(k("txl"));
    p.cmd("ZREM").arg(k("txz")).arg("m");
    p.cmd("SPOP").arg(k("txs")).arg(2);
    let out: (String, i64, Vec<String>) = q(p.query(&mut c))?;
    eq((out.0, out.1, sorted(out.2)), ("a".to_string(), 1, sv(&["x", "y"])))?;
    set_cmd(&mut c, k("txstr"), "s")?;
    // an error inside EXEC does not roll back the other commands
    let mut p = redis::pipe();
    p.atomic();
    p.cmd("RPUSH").arg(k("txl")).arg("q");
    p.cmd("LPUSH").arg(k("txstr")).arg("bad");
    p.cmd("LLEN").arg(k("txl"));
    p.ignore_errors();
    let out: Vec<Value> = q(p.query(&mut c))?;
    eq(out.len(), 3)?;
    eq(out[0].clone(), Value::Int(2))?;
    if !matches!(&out[1], Value::ServerError(e) if format!("{e:?}").contains("WRONGTYPE")) {
        return Err(format!("out[1] is not a WRONGTYPE error: {:?}", out[1]));
    }
    eq(out[2].clone(), Value::Int(2))?;
    // WATCH aborts EXEC when the key changed
    set_cmd(&mut c, k("w"), "1")?;
    q::<String>(redis::cmd("WATCH").arg(k("w")).query(&mut c))?;
    let mut other = connect()?;
    set_cmd(&mut other, k("w"), "2")?;
    let mut p = redis::pipe();
    p.atomic();
    p.cmd("ZADD").arg(k("txz")).arg(1).arg("w");
    let out: Option<Vec<i64>> = q(p.query(&mut c))?;
    eq(out, None)?;
    eq(q(c.zscore(k("txz"), "w"))?, None::<f64>)?;
    del(&mut c, &all)?;
    Ok(())
}

fn lua_new_types() -> R {
    let mut c = connect()?;
    let (z, l, s) = (k("lz"), k("ll"), k("ls"));
    del(&mut c, &[z.clone(), l.clone(), s.clone()])?;
    let script = "
redis.call('ZADD', KEYS[1], 1.5, 'a', 2, 'b', 3, 'c')
redis.call('RPUSH', KEYS[2], 'x', 'y', 'z')
redis.call('SADD', KEYS[3], 'm', 'n')
return {redis.call('ZRANGE', KEYS[1], 0, -1), redis.call('LRANGE', KEYS[2], 0, -1),
        redis.call('SCARD', KEYS[3]), redis.call('ZSCORE', KEYS[1], 'a'),
        redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES'), redis.call('TYPE', KEYS[2])['ok']}";
    let arr = |xs: &[&str]| Value::Array(xs.iter().map(|x| bs(x.as_bytes())).collect());
    eq(
        q(eval(&mut c, script, &[z.as_bytes(), l.as_bytes(), s.as_bytes()], &[]))?,
        Value::Array(vec![
            arr(&["a", "b", "c"]),
            arr(&["x", "y", "z"]),
            Value::Int(2),
            bs(b"1.5"),
            arr(&["a", "1.5"]),
            bs(b"list"),
        ]),
    )?;
    eq(q(c.zrange(&z, 0, -1))?, sv(&["a", "b", "c"]))?;
    let ev = |c: &mut Connection, body: &str, key: &str| q(eval(c, body, &[key.as_bytes()], &[]));
    eq(ev(&mut c, "return redis.call('ZPOPMIN', KEYS[1])", &z)?, arr(&["a", "1.5"]))?;
    eq(ev(&mut c, "return redis.call('LPOP', KEYS[1], 2)", &l)?, arr(&["x", "y"]))?;
    let members: HashSet<String> =
        redis::from_redis_value(ev(&mut c, "return redis.call('SMEMBERS', KEYS[1])", &s)?).map_err(|e| e.to_string())?;
    eq(members, hs(&["m", "n"]))?;
    eq(ev(&mut c, "return redis.call('ZINCRBY', KEYS[1], 0.5, 'b')", &z)?, bs(b"2.5"))?;
    eq(ev(&mut c, "return redis.call('EXISTS', KEYS[1])", &k("none"))?, Value::Int(0))?;
    raises(eval(&mut c, "return redis.call('LPUSH', KEYS[1], 'x')", &[z.as_bytes()], &[]), "WRONGTYPE")?;
    eq(
        ev(&mut c, "return redis.pcall('LPUSH', KEYS[1], 'x')['err'] ~= nil", &z)?,
        Value::Int(1),
    )?;
    let sc = redis::Script::new("return redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')");
    eq(
        q(sc.key(&z).arg("-inf").arg("+inf").invoke::<Vec<String>>(&mut c))?,
        sv(&["b", "2.5", "c", "3"]),
    )?;
    del(&mut c, &[z, l, s])?;
    Ok(())
}

fn config_info() -> R {
    let mut c = connect()?;
    let get_cfg = |c: &mut Connection, name: &str| -> Result<HashMap<String, String>, String> {
        q(redis::cmd("CONFIG").arg("GET").arg(name).query(c))
    };
    let mm = get_cfg(&mut c, "maxmemory")?;
    let v = mm.get("maxmemory").ok_or(format!("CONFIG GET maxmemory: {mm:?}"))?;
    v.parse::<i64>().map_err(|e| format!("maxmemory {v:?}: {e}"))?;
    let pol = get_cfg(&mut c, "maxmemory-policy")?;
    if pol.get("maxmemory-policy").map_or(true, |s| s.is_empty()) {
        return Err(format!("CONFIG GET maxmemory-policy: {pol:?}"));
    }
    let both = get_cfg(&mut c, "maxmemory*")?;
    if !(both.contains_key("maxmemory") && both.contains_key("maxmemory-policy")) {
        return Err(format!("CONFIG GET maxmemory*: {both:?}"));
    }
    let info: redis::InfoDict = q(redis::cmd("INFO").query(&mut c))?;
    if info.is_empty() {
        return Err("INFO parsed to no key:value pairs".to_string());
    }
    for sec in ["memory", "keyspace"] {
        q::<redis::InfoDict>(redis::cmd("INFO").arg(sec).query(&mut c))?;
    }
    Ok(())
}

fn main() {
    let checks: Vec<(&str, fn() -> R)> = vec![
        ("connect_ping", connect_ping),
        ("set_get", set_get),
        ("binary_key_and_value", binary_key_and_value),
        ("big_value_8mib", big_value_8mib),
        ("counters", counters),
        ("counter_errors", counter_errors),
        ("hashes", hashes),
        ("wrong_type", wrong_type),
        ("mset_mget", mset_mget),
        ("del_exists_unlink", del_exists_unlink),
        ("keys_pattern", keys_pattern),
        ("eval_values", eval_values),
        ("eval_calls_redis", eval_calls_redis),
        ("eval_errors", eval_errors),
        ("script_helper_evalsha", script_helper_evalsha),
        ("pipeline_plain_10000", pipeline_plain_10000),
        ("pipeline_with_error_in_middle", pipeline_with_error_in_middle),
        ("x_pipeline_transaction_multi_exec", x_pipeline_transaction_multi_exec),
        ("x_set_with_expiry_and_nx", x_set_with_expiry_and_nx),
        ("concurrency_16_threads", concurrency_16_threads),
        ("unknown_command_keeps_connection", unknown_command_keeps_connection),
        ("hgetall_100k_fields", hgetall_100k_fields),
        ("client_side_commands", client_side_commands),
        ("lists", lists),
        ("sets", sets),
        ("zsets_basic", zsets_basic),
        ("zset_scores", zset_scores),
        ("zset_pop", zset_pop),
        ("zsets_ranges", zsets_ranges),
        ("zsets_store", zsets_store),
        ("string_key_extras", string_key_extras),
        ("hrandfield_values", hrandfield_values),
        ("keys_misc", keys_misc),
        ("pubsub_roundtrip", pubsub_roundtrip),
        ("tx_new_types", tx_new_types),
        ("lua_new_types", lua_new_types),
        ("config_info", config_info),
        ("flushall_dbsize", flushall_dbsize),
    ];
    let total = checks.len();
    let mut failed: Vec<&str> = Vec::new();
    println!(
        "== rust redis-rs {REDIS_RS_VERSION} protocol={}",
        proto().as_deref().unwrap_or("default")
    );
    for (name, f) in checks {
        let res = match catch_unwind(AssertUnwindSafe(f)) {
            Ok(r) => r,
            Err(p) => {
                let m = p
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "unknown panic".to_string());
                Err(format!("panic: {m}"))
            }
        };
        match res {
            Ok(()) => println!("PASS {name}"),
            Err(m) => {
                failed.push(name);
                println!("FAIL {name}: {}", trunc(m, 300));
            }
        }
    }
    println!(
        "== rust: {}/{} passed; failed: {:?}",
        total - failed.len(),
        total,
        failed
    );
    std::process::exit(if failed.is_empty() { 0 } else { 1 });
}
