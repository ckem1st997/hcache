//! The commands added after the survey of other caches: set algebra on
//! sorted sets, ranges by member, pops over several keys, bitmaps, TIME.

use hcache::{Config, Db, Reply};

fn run(s: &hcache::Session, line: &str) -> String {
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

fn check(cases: &[(&str, &str)]) {
    let db = Db::open(None, Config::default()).unwrap();
    let s = db.session().unwrap();
    for (line, want) in cases {
        assert_eq!(run(&s, line), *want, "{line}");
    }
}

#[test]
fn sorted_set_algebra() {
    check(&[
        ("ZADD a 1 x 2 y 3 z", ":3"),
        ("ZADD b 10 y 20 z 30 w", ":3"),
        ("SET str v", "OK"),
        ("ZUNION 2 a b", "[x y z w]"),
        ("ZUNION 2 a b WITHSCORES", "[x 1 y 12 z 23 w 30]"),
        (
            "ZUNION 2 a b WEIGHTS 2 1 AGGREGATE MAX WITHSCORES",
            "[x 2 y 10 z 20 w 30]",
        ),
        ("ZINTER 2 a b WITHSCORES", "[y 12 z 23]"),
        ("ZINTER 2 a b AGGREGATE MIN WITHSCORES", "[y 2 z 3]"),
        ("ZINTER 2 a nokey", "[]"),
        ("ZDIFF 2 a b WITHSCORES", "[x 1]"),
        ("ZDIFF 2 b a", "[w]"),
        ("ZDIFF 1 a", "[x y z]"),
        ("ZDIFF 2 nokey a", "[]"),
        ("ZDIFF 2 a b WEIGHTS 1 1", "-ERR syntax error"),
        ("ZDIFFSTORE d 2 a b", ":1"),
        ("ZRANGE d 0 -1 WITHSCORES", "[x 1]"),
        ("ZDIFFSTORE d 2 a a", ":0"),
        ("EXISTS d", ":0"),
        ("ZINTERCARD 2 a b", ":2"),
        ("ZINTERCARD 2 a b LIMIT 1", ":1"),
        ("ZINTERCARD 2 a b LIMIT 0", ":2"),
        ("ZINTERCARD 2 a b LIMIT -1", "-ERR LIMIT can't be negative"),
        (
            "ZINTERCARD 0 a",
            "-ERR at least 1 input key is needed for 'zintercard' command",
        ),
        ("ZINTERCARD 1 a LIMIT abc", "-ERR LIMIT can't be negative"),
        ("ZINTERCARD 2 a b LIMIT 1 LIMIT 0", ":2"),
        ("ZINTERCARD 3 a b", "-ERR syntax error"),
        (
            "ZUNION 0 a",
            "-ERR at least 1 input key is needed for 'zunion' command",
        ),
        ("ZUNION 3 a b", "-ERR syntax error"),
        (
            "ZUNION 2 a str",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        ("ZUNION 2 a b WITHSCORES WITHSCORES", "[x 1 y 12 z 23 w 30]"),
        ("ZUNIONSTORE u 2 a b WITHSCORES", "-ERR syntax error"),
        // A plain set counts as scores of one.
        ("SADD s x q", ":2"),
        ("ZUNION 2 a s WITHSCORES", "[q 1 x 2 y 2 z 3]"),
        // Ties sort by member.
        ("ZADD t 0 b 0 a 0 c", ":3"),
        ("ZUNION 1 t", "[a b c]"),
    ]);
}

#[test]
fn ranges_by_member() {
    check(&[
        ("ZADD z 0 a 0 b 0 c 0 d 0 e", ":5"),
        ("ZRANGEBYLEX z - +", "[a b c d e]"),
        ("ZRANGEBYLEX z [b (d", "[b c]"),
        ("ZRANGEBYLEX z (b [d", "[c d]"),
        ("ZRANGEBYLEX z [bb +", "[c d e]"),
        ("ZRANGEBYLEX z + -", "[]"),
        ("ZRANGEBYLEX z [d [b", "[]"),
        ("ZRANGEBYLEX z (c (c", "[]"),
        ("ZRANGEBYLEX z [c [c", "[c]"),
        ("ZRANGEBYLEX z - + LIMIT 1 2", "[b c]"),
        ("ZRANGEBYLEX z - + LIMIT 1 -1", "[b c d e]"),
        (
            "ZRANGEBYLEX z b d",
            "-ERR min or max not valid string range item",
        ),
        ("ZRANGEBYLEX z - + WITHSCORES", "-ERR syntax error"),
        ("ZREVRANGEBYLEX z + -", "[e d c b a]"),
        ("ZREVRANGEBYLEX z [d (b", "[d c]"),
        ("ZREVRANGEBYLEX z + - LIMIT 0 2", "[e d]"),
        ("ZRANGE z [b [d BYLEX", "[b c d]"),
        ("ZRANGE z [d [b BYLEX REV", "[d c b]"),
        ("ZRANGE z - + BYLEX LIMIT 3 9", "[d e]"),
        ("ZRANGE z - + BYLEX BYSCORE", "-ERR syntax error"),
        ("ZLEXCOUNT z - +", ":5"),
        ("ZLEXCOUNT z [b [d", ":3"),
        ("ZLEXCOUNT z (b (d", ":1"),
        ("ZLEXCOUNT z + -", ":0"),
        ("ZLEXCOUNT nokey - +", ":0"),
        (
            "ZLEXCOUNT z b d",
            "-ERR min or max not valid string range item",
        ),
        ("ZREMRANGEBYLEX z [b (d", ":2"),
        ("ZRANGE z 0 -1", "[a d e]"),
        ("ZREMRANGEBYLEX z - +", ":3"),
        ("EXISTS z", ":0"),
        ("ZREMRANGEBYLEX z - +", ":0"),
    ]);
}

#[test]
fn range_store_and_random_members() {
    check(&[
        ("ZADD z 1 a 2 b 3 c 4 d", ":4"),
        ("ZRANGESTORE dst z 1 2", ":2"),
        ("ZRANGE dst 0 -1 WITHSCORES", "[b 2 c 3]"),
        ("ZRANGESTORE dst z 2 4 BYSCORE REV", ":0"),
        ("ZRANGESTORE dst z 4 2 BYSCORE REV LIMIT 1 2", ":2"),
        ("ZRANGE dst 0 -1", "[b c]"),
        ("ZRANGESTORE dst z 9 10", ":0"),
        ("EXISTS dst", ":0"),
        ("ZRANGESTORE dst nokey 0 -1", ":0"),
        ("ZRANGESTORE dst z 0 -1 WITHSCORES", "-ERR syntax error"),
        ("ZRANGESTORE z z 0 0", ":1"),
        ("ZRANGE z 0 -1 WITHSCORES", "[a 1]"),
        ("ZRANDMEMBER z", "a"),
        ("ZRANDMEMBER z 5", "[a]"),
        ("ZRANDMEMBER z -3", "[a a a]"),
        ("ZRANDMEMBER z -2 WITHSCORES", "[a 1 a 1]"),
        ("ZRANDMEMBER z 0", "[]"),
        ("ZRANDMEMBER nokey", "nil"),
        ("ZRANDMEMBER nokey 3", "[]"),
        ("ZRANDMEMBER z 1 NOPE", "-ERR syntax error"),
        (
            "ZRANDMEMBER z -9223372036854775808",
            "-ERR value is out of range",
        ),
        ("SET str v", "OK"),
        (
            "ZRANDMEMBER str",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
    ]);
}

#[test]
fn random_members_are_distinct_and_cover_the_set() {
    let db = Db::open(None, Config::default()).unwrap();
    let s = db.session().unwrap();
    for i in 0..50 {
        run(&s, &format!("ZADD z {i} m{i}"));
    }
    let Reply::Array(got) = s.execute(&["ZRANDMEMBER", "z", "20"]) else {
        panic!("not an array");
    };
    let distinct: std::collections::HashSet<_> = got.iter().map(show).collect();
    assert_eq!((got.len(), distinct.len()), (20, 20));
    let Reply::Array(all) = s.execute(&["ZRANDMEMBER", "z", "500"]) else {
        panic!("not an array");
    };
    assert_eq!(all.len(), 50);
    let Reply::Array(pairs) = s.execute(&["ZRANDMEMBER", "z", "-200", "WITHSCORES"]) else {
        panic!("not an array");
    };
    assert_eq!(pairs.len(), 400);
    for pair in pairs.chunks(2) {
        assert_eq!(show(&pair[0]), format!("m{}", show(&pair[1])));
    }
}

#[test]
fn pops_over_several_keys() {
    check(&[
        ("RPUSH l2 a b c", ":3"),
        ("LMPOP 2 l1 l2 LEFT", "[l2 [a]]"),
        ("LMPOP 2 l1 l2 RIGHT COUNT 5", "[l2 [c b]]"),
        ("EXISTS l2", ":0"),
        ("LMPOP 2 l1 l2 LEFT", "nilarray"),
        ("LMPOP 0 l1 LEFT", "-ERR numkeys should be greater than 0"),
        ("LMPOP 2 l1 LEFT", "-ERR syntax error"),
        ("LMPOP 1 l1 UP", "-ERR syntax error"),
        (
            "LMPOP 1 l1 LEFT COUNT 0",
            "-ERR count should be greater than 0",
        ),
        ("LMPOP 1 l1 LEFT COUNT", "-ERR syntax error"),
        ("LMPOP 1 l1 MIN", "-ERR syntax error"),
        ("ZADD z 1 a 2 b 3 c", ":3"),
        ("ZMPOP 2 nokey z MIN", "[z [[a 1]]]"),
        ("ZMPOP 1 z MAX COUNT 9", "[z [[c 3] [b 2]]]"),
        ("ZMPOP 1 z MAX", "nilarray"),
        ("SET str v", "OK"),
        (
            "LMPOP 2 str l1 LEFT",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        (
            "ZMPOP 1 str MIN",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        // The first key with anything wins even when a later one is the wrong type.
        ("RPUSH l1 x", ":1"),
        ("LMPOP 2 l1 str LEFT", "[l1 [x]]"),
    ]);
}

#[test]
fn bitmaps() {
    check(&[
        ("SETBIT b 7 1", ":0"),
        ("GET b", "\u{1}"),
        ("SETBIT b 7 1", ":1"),
        ("GETBIT b 7", ":1"),
        ("GETBIT b 6", ":0"),
        ("GETBIT b 1000", ":0"),
        ("GETBIT nokey 0", ":0"),
        ("SETBIT b 7 0", ":1"),
        ("STRLEN b", ":1"),
        ("SETBIT b 100 1", ":0"),
        ("STRLEN b", ":13"),
        ("BITCOUNT b", ":1"),
        ("SETBIT b 2 2", "-ERR bit is not an integer or out of range"),
        (
            "SETBIT b -1 1",
            "-ERR bit offset is not an integer or out of range",
        ),
        (
            "SETBIT b 4294967296 1",
            "-ERR bit offset is not an integer or out of range",
        ),
        (
            "SETBIT b x 1",
            "-ERR bit offset is not an integer or out of range",
        ),
        (
            "GETBIT b -1",
            "-ERR bit offset is not an integer or out of range",
        ),
        ("SET s foobar", "OK"),
        ("BITCOUNT s", ":26"),
        ("BITCOUNT s 0 0", ":4"),
        ("BITCOUNT s 1 1", ":6"),
        ("BITCOUNT s 1 1 BYTE", ":6"),
        ("BITCOUNT s 5 30 BIT", ":17"),
        ("BITCOUNT s -1 -1", ":4"),
        ("BITCOUNT s 3 1", ":0"),
        ("BITCOUNT s 0 -1 BIT", ":26"),
        ("BITCOUNT s 0", ":26"),
        ("BITCOUNT s 1", ":22"),
        ("BITCOUNT s 0 1 BIT x", "-ERR syntax error"),
        ("BITCOUNT s 0 1 NOPE", "-ERR syntax error"),
        ("BITCOUNT nokey", ":0"),
        (
            "BITCOUNT nokey 0 x",
            "-ERR value is not an integer or out of range",
        ),
        ("LPUSH l a", ":1"),
        (
            "BITCOUNT l",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        (
            "SETBIT l 0 1",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
        (
            "GETBIT l 0",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
    ]);
}

#[test]
fn bit_positions_and_operations() {
    let db = Db::open(None, Config::default()).unwrap();
    let s = db.session().unwrap();
    let set = |key: &str, bytes: &[u8]| s.execute(&[b"SET".as_slice(), key.as_bytes(), bytes]);
    set("k", b"\xff\xf0\x00");
    set("z", b"\x00\x00\x00");
    set("o", b"\xff\xff\xff");
    set("a", b"abc");
    set("b", b"ab");
    set("one", b"1");
    for (line, want) in [
        ("BITPOS k 0", ":12"),
        ("BITPOS k 1", ":0"),
        ("BITPOS k 1 2", ":-1"),
        ("BITPOS k 0 2", ":16"),
        ("BITPOS k 1 0 -1", ":0"),
        ("BITPOS k 0 0 0", ":-1"),
        ("BITPOS k 1 7 15 BIT", ":7"),
        ("BITPOS k 0 7 15 BIT", ":12"),
        ("BITPOS k 1 12 20 BIT", ":-1"),
        ("BITPOS z 1", ":-1"),
        ("BITPOS z 0", ":0"),
        // Past the end counts as zeros only when no end was named.
        ("BITPOS o 0", ":24"),
        ("BITPOS o 0 0", ":24"),
        ("BITPOS o 0 0 -1", ":-1"),
        ("BITPOS o 0 5", ":-1"),
        ("BITPOS nokey 0", ":0"),
        ("BITPOS nokey 1", ":-1"),
        // Captured from Valkey 9.1.3: both ends clamp to the first byte;
        // only BITCOUNT treats two negative ends the wrong way round as
        // nothing.
        ("BITPOS one 0 -2 -100", ":0"),
        ("BITPOS one 1 -2 -100", ":2"),
        ("BITPOS one 0 -100 -2 BIT", ":0"),
        ("BITCOUNT one -1 -2", ":0"),
        ("BITCOUNT one -2 -100 BIT", ":0"),
        ("BITPOS k 2", "-ERR The bit argument must be 1 or 0."),
        ("BITPOS k 1 0 1 NOPE", "-ERR syntax error"),
        ("BITOP AND d a b", ":3"),
        ("BITOP OR d a b", ":3"),
        ("GET d", "abc"),
        ("BITOP XOR d a a", ":3"),
        ("BITCOUNT d", ":0"),
        ("STRLEN d", ":3"),
        ("BITOP NOT d z", ":3"),
        ("BITCOUNT d", ":24"),
        (
            "BITOP NOT d a b",
            "-ERR BITOP NOT must be called with a single source key.",
        ),
        ("BITOP NAND d a b", "-ERR syntax error"),
        ("BITOP AND d nokey nokey2", ":0"),
        ("EXISTS d", ":0"),
        ("BITOP OR a a nokey", ":3"),
        ("GET a", "abc"),
        ("LPUSH l x", ":1"),
        (
            "BITOP OR d a l",
            "-WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
    ] {
        assert_eq!(run(&s, line), want, "{line}");
    }
    assert_eq!(s.execute(&["BITOP", "AND", "d", "a", "b"]), Reply::Int(3));
    assert_eq!(s.execute(&["GET", "d"]), Reply::Bulk(b"ab\0".to_vec()));
}

/// Bit commands against a model that keeps one bool per bit.
#[test]
fn bits_match_a_model() {
    let db = Db::open(None, Config::default()).unwrap();
    let s = db.session().unwrap();
    let mut model: Vec<bool> = Vec::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut rand = |n: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % n
    };
    for step in 0..4000 {
        let at = rand(300) as usize;
        let bit = rand(2) == 1;
        let old = model.get(at).copied().unwrap_or(false);
        if model.len() <= at {
            model.resize((at / 8 + 1) * 8, false);
        }
        model[at] = bit;
        let got = run(&s, &format!("SETBIT m {at} {}", u8::from(bit)));
        assert_eq!(got, format!(":{}", u8::from(old)), "step {step}");

        let len = model.len() as i64;
        let (a, b) = (rand(700) as i64 - 350, rand(700) as i64 - 350);
        let clamp = |i: i64| if i < 0 { (len + i).max(0) } else { i };
        let (lo, hi) = (clamp(a), clamp(b).min(len - 1));
        // BITPOS only clamps; BITCOUNT also answers 0 for two negative
        // ends the wrong way round.
        let range = if lo > hi {
            0..0
        } else {
            lo as usize..hi as usize + 1
        };
        let counted = if a < 0 && b < 0 && a > b {
            0..0
        } else {
            range.clone()
        };
        let ones = model[counted].iter().filter(|&&x| x).count();
        assert_eq!(
            run(&s, &format!("BITCOUNT m {a} {b} BIT")),
            format!(":{ones}"),
            "step {step}: {a} {b}"
        );
        for want in [false, true] {
            let pos = range
                .clone()
                .find(|&i| model[i] == want)
                .map_or(-1, |i| i as i64);
            let got = run(&s, &format!("BITPOS m {} {a} {b} BIT", u8::from(want)));
            assert_eq!(got, format!(":{pos}"), "step {step}: {want} {a} {b}");
        }
        let probe = rand(400) as usize;
        let expect = model.get(probe).copied().unwrap_or(false);
        assert_eq!(
            run(&s, &format!("GETBIT m {probe}")),
            format!(":{}", u8::from(expect))
        );
    }
}

#[test]
fn time_and_read_only_scripts() {
    let db = Db::open(None, Config::default()).unwrap();
    let s = db.session().unwrap();
    let Reply::Array(now) = s.execute(&["TIME"]) else {
        panic!("not an array");
    };
    let secs: u64 = show(&now[0]).parse().unwrap();
    let micros: u64 = show(&now[1]).parse().unwrap();
    assert!(secs > 1_700_000_000 && micros < 1_000_000);
    assert_eq!(
        run(&s, "TIME x"),
        "-ERR wrong number of arguments for 'time' command"
    );
    assert_eq!(run(&s, "SET k v"), "OK");
    assert_eq!(
        s.execute(&["EVAL_RO", "return redis.call('GET', KEYS[1])", "1", "k"]),
        Reply::Bulk(b"v".to_vec())
    );
    let denied = show(&s.execute(&[
        "EVAL_RO",
        "return redis.call('SET', KEYS[1], 'x')",
        "1",
        "k",
    ]));
    assert!(denied.starts_with('-'), "{denied}");
    assert_eq!(run(&s, "GET k"), "v");
    // What may replicate counts as a write too, as in Valkey 9.1.
    for call in [
        "redis.call('PFCOUNT', 'nokey')",
        "redis.call('PUBLISH', 'ch', 'x')",
        "redis.call('SPUBLISH', 'ch', 'x')",
    ] {
        let denied = show(&s.execute(&["EVAL_RO", &format!("return {call}"), "0"]));
        assert!(
            denied.starts_with('-')
                && denied.contains("Write commands are not allowed from read-only scripts."),
            "{call}: {denied}"
        );
    }
    assert_eq!(
        show(&s.execute(&["EVAL", "return redis.call('PFCOUNT', 'nokey')", "0"])),
        ":0"
    );
}

/// The new multi-key commands inside transactions and across shards.
#[test]
fn multi_key_commands_are_atomic_across_shards() {
    let db = Db::open(None, Config::default()).unwrap();
    let s = db.session().unwrap();
    for i in 0..64 {
        run(&s, &format!("ZADD src{i} 1 a 2 b 3 c"));
        assert_eq!(run(&s, &format!("ZRANGESTORE dst{i} src{i} 0 1")), ":2");
        assert_eq!(
            run(&s, &format!("ZDIFFSTORE diff{i} 2 src{i} dst{i}")),
            ":1"
        );
        assert_eq!(run(&s, &format!("ZRANGE diff{i} 0 -1 WITHSCORES")), "[c 3]");
        assert_eq!(run(&s, &format!("TYPE dst{i}")), "+zset");
    }
    assert_eq!(run(&s, "MULTI"), "OK");
    assert_eq!(run(&s, "ZMPOP 2 nokey src0 MAX"), "+QUEUED");
    assert_eq!(run(&s, "BITOP OR bits src0"), "+QUEUED");
    assert_eq!(run(&s, "ZINTER 2 src1 dst1"), "+QUEUED");
    assert_eq!(
        run(&s, "EXEC"),
        "[[src0 [[c 3]]] -WRONGTYPE Operation against a key holding the wrong kind of value [a b]]"
    );
}

/// Results that replace a key drop its time to live, like any overwrite.
#[test]
fn stored_results_drop_the_old_time_to_live() {
    check(&[
        ("SET d old EX 100", "OK"),
        ("ZADD z 1 a", ":1"),
        ("ZRANGESTORE d z 0 -1", ":1"),
        ("TTL d", ":-1"),
        ("SET e old EX 100", "OK"),
        ("SET a x", "OK"),
        ("BITOP OR e a", ":1"),
        ("TTL e", ":-1"),
        ("SET f v EX 100", "OK"),
        ("SETBIT f 0 1", ":0"),
        ("TTL f", ":100"),
    ]);
}
