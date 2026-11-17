//! The RESP3 types of replies, byte for byte: doubles, sets, maps and
//! arrays of pairs where RESP2 has strings and flat arrays.

mod net;

use net::*;
use std::io::Read;

fn exchange(c: &mut Client, line: &str, want: &str) {
    let args: Vec<&str> = line.split(' ').collect();
    c.send(&args);
    let want = want.replace(' ', "\r\n");
    let mut got = vec![0u8; want.len()];
    c.rd.read_exact(&mut got)
        .unwrap_or_else(|e| panic!("{line}: {e}"));
    assert_eq!(String::from_utf8_lossy(&got), want, "{line}");
}

fn setup(c: &mut Client) {
    for line in [
        "ZADD z 1 a 2.5 b",
        "SADD s x",
        "SADD s2 x y",
        "HSET h f v",
        "RPUSH l a",
    ] {
        let args: Vec<&str> = line.split(' ').collect();
        c.cmd(&args);
    }
}

/// (command, RESP2 reply, RESP3 reply), spaces standing for CRLF.
const CASES: &[(&str, &str, &str)] = &[
    ("ZSCORE z b", "$3 2.5 ", ",2.5 "),
    ("ZSCORE z nope", "$-1 ", "_ "),
    ("ZINCRBY z 0 a", "$1 1 ", ",1 "),
    ("ZADD z INCR 0 a", "$1 1 ", ",1 "),
    ("ZADD z NX INCR 5 a", "$-1 ", "_ "),
    ("ZADD inf inf m", ":1 ", ":1 "),
    ("ZSCORE inf m", "$3 inf ", ",inf "),
    ("ZMSCORE z a nope b", "*3 $1 1 $-1 $3 2.5 ", "*3 ,1 _ ,2.5 "),
    ("ZRANK z b", ":1 ", ":1 "),
    ("ZRANK z b WITHSCORE", "*2 :1 $3 2.5 ", "*2 :1 ,2.5 "),
    ("ZRANK z nope WITHSCORE", "*-1 ", "_ "),
    ("ZRANGE z 0 -1", "*2 $1 a $1 b ", "*2 $1 a $1 b "),
    (
        "ZRANGE z 0 -1 WITHSCORES",
        "*4 $1 a $1 1 $1 b $3 2.5 ",
        "*2 *2 $1 a ,1 *2 $1 b ,2.5 ",
    ),
    (
        "ZREVRANGE z 0 0 WITHSCORES",
        "*2 $1 b $3 2.5 ",
        "*1 *2 $1 b ,2.5 ",
    ),
    (
        "ZRANGEBYSCORE z (1 +inf WITHSCORES",
        "*2 $1 b $3 2.5 ",
        "*1 *2 $1 b ,2.5 ",
    ),
    ("ZRANGE nokey 0 -1 WITHSCORES", "*0 ", "*0 "),
    (
        "ZRANDMEMBER inf 1 WITHSCORES",
        "*2 $1 m $3 inf ",
        "*1 *2 $1 m ,inf ",
    ),
    ("ZRANDMEMBER inf -2", "*2 $1 m $1 m ", "*2 $1 m $1 m "),
    ("ZUNION 2 z inf", "*3 $1 a $1 b $1 m ", "*3 $1 a $1 b $1 m "),
    (
        "ZINTER 2 z z WITHSCORES",
        "*4 $1 a $1 2 $1 b $1 5 ",
        "*2 *2 $1 a ,2 *2 $1 b ,5 ",
    ),
    (
        "ZDIFF 2 z inf WITHSCORES",
        "*4 $1 a $1 1 $1 b $3 2.5 ",
        "*2 *2 $1 a ,1 *2 $1 b ,2.5 ",
    ),
    ("SMEMBERS s", "*1 $1 x ", "~1 $1 x "),
    ("SMEMBERS nokey", "*0 ", "~0 "),
    ("SINTER s s2", "*1 $1 x ", "~1 $1 x "),
    ("SUNION s s", "*1 $1 x ", "~1 $1 x "),
    ("SDIFF s s2", "*0 ", "~0 "),
    ("SINTERCARD 2 s s2", ":1 ", ":1 "),
    ("SRANDMEMBER s 1", "*1 $1 x ", "*1 $1 x "),
    ("SMISMEMBER s x q", "*2 :1 :0 ", "*2 :1 :0 "),
    (
        "HRANDFIELD h -2 WITHVALUES",
        "*4 $1 f $1 v $1 f $1 v ",
        "*2 *2 $1 f $1 v *2 $1 f $1 v ",
    ),
    ("HRANDFIELD h 1", "*1 $1 f ", "*1 $1 f "),
    ("HGETALL h", "*2 $1 f $1 v ", "%1 $1 f $1 v "),
    ("HINCRBYFLOAT h n 1.5", "$3 1.5 ", "$3 1.5 "),
    ("INCRBYFLOAT fl 1.5", "$3 1.5 ", "$3 1.5 "),
    ("BLPOP l 0", "*2 $1 l $1 a ", "*2 $1 l $1 a "),
    ("BLPOP l 0.01", "*-1 ", "_ "),
    ("ZPOPMIN nokey", "*0 ", "*0 "),
    ("ZMPOP 1 nokey MIN", "*-1 ", "_ "),
    ("LMPOP 1 nokey LEFT", "*-1 ", "_ "),
    // From here on the sorted set is taken apart.
    ("ZADD p 1 a 2 b 3 c 4 d 5 e 6 f", ":6 ", ":6 "),
    ("ZPOPMIN p", "*2 $1 a $1 1 ", "*2 $1 a ,1 "),
    ("ZPOPMAX p 1", "*2 $1 f $1 6 ", "*1 *2 $1 f ,6 "),
    ("ZPOPMIN p 0", "*0 ", "*0 "),
    ("BZPOPMIN p 0", "*3 $1 p $1 b $1 2 ", "*3 $1 p $1 b ,2 "),
    (
        "ZMPOP 1 p MAX",
        "*2 $1 p *1 *2 $1 e $1 5 ",
        "*2 $1 p *1 *2 $1 e ,5 ",
    ),
    (
        "BZMPOP 0 1 p MIN COUNT 9",
        "*2 $1 p *2 *2 $1 c $1 3 *2 $1 d $1 4 ",
        "*2 $1 p *2 *2 $1 c ,3 *2 $1 d ,4 ",
    ),
    ("SPOP s2 5", "*2 $1 ", "~2 $1 "),
];

#[test]
fn replies_are_typed_in_resp3_and_flat_in_resp2() {
    for proto in [2, 3] {
        let env = Env::new(engine_cfg(4), server_cfg(2, 64));
        let mut c = env.client();
        setup(&mut c);
        if proto == 3 {
            c.send(&["HELLO", "3"]);
            // The map HELLO answers with has no fixed length: read up to
            // the reply of the command behind it.
            c.send(&["PING"]);
            let mut seen = Vec::new();
            let mut byte = [0u8; 1];
            while !seen.ends_with(b"+PONG\r\n") {
                c.rd.read_exact(&mut byte).unwrap();
                seen.push(byte[0]);
            }
        }
        for (line, resp2, resp3) in CASES {
            exchange(&mut c, line, if proto == 3 { resp3 } else { resp2 });
            // SPOP's order is random: skip the rest of that reply.
            if line.starts_with("SPOP") {
                break;
            }
        }
    }
}

#[test]
fn a_transaction_types_each_reply() {
    let env = Env::new(engine_cfg(4), server_cfg(1, 64));
    let mut c = env.client();
    setup(&mut c);
    for (line, want) in [
        ("HELLO 3 SETNAME x", ""),
        ("MULTI", "+OK "),
        ("ZSCORE z b", "+QUEUED "),
        ("SMEMBERS s", "+QUEUED "),
        ("ZRANGE z 0 0 WITHSCORES", "+QUEUED "),
        ("HGETALL h", "+QUEUED "),
        ("GET nokey", "+QUEUED "),
        ("ZPOPMIN z", "+QUEUED "),
        ("LPUSH z x", "+QUEUED "),
        (
            "EXEC",
            "*7 ,2.5 ~1 $1 x *1 *2 $1 a ,1 %1 $1 f $1 v _ *2 $1 a ,1 \
             -WRONGTYPE Operation against a key holding the wrong kind of value ",
        ),
    ] {
        if want.is_empty() {
            let args: Vec<&str> = line.split(' ').collect();
            c.send(&args);
            c.send(&["ECHO", "marker"]);
            let mut seen = Vec::new();
            let mut byte = [0u8; 1];
            while !seen.ends_with(b"$6\r\nmarker\r\n") {
                c.rd.read_exact(&mut byte).unwrap();
                seen.push(byte[0]);
            }
            continue;
        }
        // The error line has spaces of its own.
        let args: Vec<&str> = line.split(' ').collect();
        c.send(&args);
        let (head, tail) = match want.split_once("-WRONGTYPE") {
            Some((head, tail)) => (
                head.replace(' ', "\r\n"),
                format!("-WRONGTYPE{}\r\n", tail.trim_end()),
            ),
            None => (want.replace(' ', "\r\n"), String::new()),
        };
        let want = format!("{head}{tail}");
        let mut got = vec![0u8; want.len()];
        c.rd.read_exact(&mut got).unwrap();
        assert_eq!(String::from_utf8_lossy(&got), want, "{line}");
    }
}

#[test]
fn copy_accepts_the_only_database() {
    let env = Env::new(engine_cfg(4), server_cfg(1, 64));
    let mut c = env.client();
    c.cmd(&["SET", "a", "1"]);
    for (line, want) in [
        ("COPY a b DB 0", ":1 "),
        ("COPY a b DB 0", ":0 "),
        ("COPY a b DB 0 REPLACE", ":1 "),
        ("COPY a b REPLACE DB 0", ":1 "),
        ("COPY a c db 0", ":1 "),
        ("COPY a d DB 16", "-ERR DB index is out of range "),
        ("COPY a d DB -1", "-ERR DB index is out of range "),
        ("COPY a d DB", "-ERR syntax error "),
        (
            "COPY a d DB x",
            "-ERR value is not an integer or out of range ",
        ),
        ("COPY a d NOPE", "-ERR syntax error "),
        ("EXISTS d", ":0 "),
    ] {
        let args: Vec<&str> = line.split(' ').collect();
        c.send(&args);
        let want = format!("{}\r\n", want.trim_end());
        let mut got = vec![0u8; want.len()];
        c.rd.read_exact(&mut got).unwrap();
        assert_eq!(String::from_utf8_lossy(&got), want, "{line}");
    }
}
