//! First contact with lists, sets, sorted sets and the newer key commands.

use hotstash::{Config, Db, Reply};

fn run(s: &hotstash::Session, line: &str) -> String {
    let args: Vec<&str> = line.split(' ').collect();
    show(&s.execute(&args))
}

fn show(r: &Reply) -> String {
    match r {
        Reply::Ok => "OK".into(),
        Reply::Simple(s) => format!("+{s}"),
        Reply::Error(e) => format!("-{e}"),
        Reply::Int(n) => format!(":{n}"),
        Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        Reply::Nil => "nil".into(),
        Reply::NilArray => "nilarray".into(),
        Reply::Array(items) | Reply::Map(items) => {
            format!("[{}]", items.iter().map(show).collect::<Vec<_>>().join(" "))
        }
    }
}

#[test]
fn lists_sets_zsets_and_key_commands() {
    let db = Db::open(None, Config::default()).unwrap();
    let s = db.session().unwrap();
    let cases: &[(&str, &str)] = &[
        ("RPUSH l a b c", ":3"),
        ("LPUSH l z y", ":5"),
        ("LRANGE l 0 -1", "[y z a b c]"),
        ("LRANGE l -2 100", "[b c]"),
        ("LRANGE l 3 1", "[]"),
        ("LINDEX l -1", "c"),
        ("LINDEX l 9", "nil"),
        ("LSET l 1 Z", "OK"),
        ("LSET l 9 Z", "-ERR index out of range"),
        ("LSET nokey 0 Z", "-ERR no such key"),
        ("LINSERT l BEFORE a x", ":6"),
        ("LINSERT l AFTER nope x", ":-1"),
        ("LRANGE l 0 -1", "[y Z x a b c]"),
        ("LPOP l", "y"),
        ("RPOP l 2", "[c b]"),
        ("LPOP nokey", "nil"),
        ("LPOP nokey 2", "nilarray"),
        ("LLEN l", ":3"),
        ("RPUSH l a a", ":5"),
        ("LREM l -1 a", ":1"),
        ("LRANGE l 0 -1", "[Z x a a]"),
        ("LPOS l a", ":2"),
        ("LPOS l a RANK -1", ":3"),
        ("LPOS l a COUNT 0", "[:2 :3]"),
        ("LTRIM l 1 2", "OK"),
        ("LRANGE l 0 -1", "[x a]"),
        ("LMOVE l l2 LEFT RIGHT", "x"),
        ("RPOPLPUSH l l2", "a"),
        ("EXISTS l", ":0"),
        ("LRANGE l2 0 -1", "[a x]"),
        ("LPUSHX nokey v", ":0"),
        ("TYPE l2", "+list"),
        (
            "GET l2",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        ("SADD s a b c", ":3"),
        ("SADD s a d", ":1"),
        ("SCARD s", ":4"),
        ("SISMEMBER s a", ":1"),
        ("SMISMEMBER s a q", "[:1 :0]"),
        ("SREM s a q", ":1"),
        ("SADD t b x", ":2"),
        ("SINTER s t", "[b]"),
        ("SINTERCARD 2 s t", ":1"),
        ("SDIFFSTORE u s t", ":2"),
        ("SUNIONSTORE u2 s t", ":4"),
        ("SMOVE s t c", ":1"),
        ("SISMEMBER t c", ":1"),
        ("SMOVE s t zzz", ":0"),
        ("SPOP nokey", "nil"),
        ("TYPE s", "+set"),
        ("ZADD z 1 a 2 b 3 c", ":3"),
        ("ZADD z XX CH 5 a 9 q", ":1"),
        ("ZADD z NX 7 a", ":0"),
        ("ZADD z GT 4 a", ":0"),
        ("ZADD z INCR 1.5 b", "3.5"),
        ("ZRANGE z 0 -1 WITHSCORES", "[c 3 b 3.5 a 5]"),
        ("ZRANGE z 0 0 REV", "[a]"),
        ("ZRANGEBYSCORE z (3 +inf", "[b a]"),
        ("ZRANGEBYSCORE z -inf +inf LIMIT 1 1", "[b]"),
        ("ZREVRANGEBYSCORE z +inf -inf LIMIT 1 5", "[b c]"),
        ("ZRANGE z 5 3 BYSCORE REV", "[a b c]"),
        ("ZRANK z a", ":2"),
        ("ZREVRANK z a WITHSCORE", "[:0 5]"),
        ("ZRANK z nope", "nil"),
        ("ZSCORE z b", "3.5"),
        ("ZMSCORE z a nope", "[5 nil]"),
        ("ZCOUNT z 3 (5", ":2"),
        ("ZINCRBY z 10 c", "13"),
        ("ZPOPMAX z", "[c 13]"),
        ("ZPOPMIN z 5", "[b 3.5 a 5]"),
        ("EXISTS z", ":0"),
        ("ZADD z 1 a 2 b 3 c 4 d", ":4"),
        ("ZREMRANGEBYRANK z 0 0", ":1"),
        ("ZREMRANGEBYSCORE z (2 3", ":1"),
        ("ZRANGE z 0 -1", "[b d]"),
        ("ZADD z2 10 b 10 x", ":2"),
        ("ZUNIONSTORE zu 2 z z2 WEIGHTS 2 1", ":3"),
        ("ZRANGE zu 0 -1 WITHSCORES", "[d 8 x 10 b 14]"),
        ("ZINTERSTORE zi 2 z z2 AGGREGATE MAX", ":1"),
        ("ZRANGE zi 0 -1 WITHSCORES", "[b 10]"),
        ("ZADD z nan a", "-ERR value is not a valid float"),
        ("SET str 10.5", "OK"),
        ("INCRBYFLOAT str 0.1", "10.6"),
        ("INCRBYFLOAT str abc", "-ERR value is not a valid float"),
        ("SET str hello", "OK"),
        ("GETRANGE str 1 -2", "ell"),
        ("SETRANGE str 7 !", ":8"),
        ("STRLEN str", ":8"),
        ("HSET h a 1 b xy", ":2"),
        ("HSTRLEN h b", ":2"),
        ("HINCRBYFLOAT h a 1.5", "2.5"),
        ("RENAME str str2", "OK"),
        ("RENAME nokey x", "-ERR no such key"),
        ("RENAMENX str2 h", ":0"),
        ("COPY h h2", ":1"),
        ("COPY h h2", ":0"),
        ("HGET h2 b", "xy"),
        ("MSETNX k1 1 k2 2", ":1"),
        ("MSETNX k2 9 k3 3", ":0"),
        ("EXISTS k3", ":0"),
        ("TOUCH k1 k2 k3", ":2"),
        ("OBJECT ENCODING k1", "int"),
        ("LLEN", "-ERR wrong number of arguments for 'llen' command"),
        (
            "ZADD z 1",
            "-ERR wrong number of arguments for 'zadd' command",
        ),
    ];
    for (line, want) in cases {
        assert_eq!(run(&s, line), *want, "{line}");
    }
    assert_ne!(run(&s, "RANDOMKEY"), "nil");
    assert_eq!(run(&s, "SRANDMEMBER t 10").split(' ').count(), 3);
    assert_eq!(run(&s, "HRANDFIELD h -5").split(' ').count(), 5);
    assert_eq!(run(&s, "SPOP t 10").split(' ').count(), 3);
    assert_eq!(run(&s, "EXISTS t"), ":0");
    let scanned = run(&s, "SSCAN u 0");
    assert!(scanned.starts_with("[0 ["), "{scanned}");
    let script =
        "redis.call('rpush', KEYS[1], 'a', 'b'); return redis.call('lrange', KEYS[1], 0, -1)";
    assert_eq!(show(&s.execute(&["EVAL", script, "1", "lua"])), "[a b]");
    assert_eq!(run(&s, "MULTI"), "OK");
    assert_eq!(run(&s, "SADD tx a"), "+QUEUED");
    assert_eq!(run(&s, "ZADD tz 1 a"), "+QUEUED");
    assert_eq!(run(&s, "EXEC"), "[:1 :1]");
}
