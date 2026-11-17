//! Inputs that cost a server far more than they cost the client that sends
//! them, and connection states nothing would ever leave.

mod net;

use hcache::Reply;
use net::*;
use std::time::{Duration, Instant};

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn info_field(c: &mut Client, name: &str) -> u64 {
    let Reply::Bulk(info) = c.cmd(&["INFO"]) else {
        panic!("no info");
    };
    let info = String::from_utf8(info).unwrap();
    info.lines()
        .find_map(|line| line.strip_prefix(&format!("{name}:")))
        .unwrap_or_else(|| panic!("no {name} in {info}"))
        .parse()
        .unwrap()
}

fn bulks(reply: Reply) -> Vec<Vec<u8>> {
    match reply {
        Reply::Array(items) => items
            .into_iter()
            .map(|item| match item {
                Reply::Bulk(b) => b,
                other => panic!("{other:?}"),
            })
            .collect(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_blocking_command_at_the_end_of_a_full_turn_still_times_out() {
    let env = env();
    // The pipeline cap is 64: the blocking command is the last one a turn
    // takes, at every offset around it.
    for pings in [62, 63, 64, 65, 127, 128] {
        let mut c = env.client();
        let mut wire = Vec::new();
        for _ in 0..pings {
            wire.extend(wire_str(&["PING"]));
        }
        wire.extend(wire_str(&["BLPOP", "nothing", "0.3"]));
        wire.extend(wire_str(&["ECHO", "after"]));
        let started = Instant::now();
        c.send_raw(&wire);
        for _ in 0..pings {
            assert_eq!(c.read(), Reply::Simple("PONG".into()));
        }
        assert_eq!(c.read(), Reply::NilArray, "{pings} pings");
        assert_eq!(c.read(), Reply::bulk("after"));
        assert!(started.elapsed() < Duration::from_secs(5), "{pings} pings");
    }
    // The same place in the turn, woken by data instead of by the clock.
    let mut c = env.client();
    let mut other = env.client();
    let mut wire = Vec::new();
    for _ in 0..63 {
        wire.extend(wire_str(&["PING"]));
    }
    wire.extend(wire_str(&["BLPOP", "q", "0"]));
    c.send_raw(&wire);
    for _ in 0..63 {
        c.read();
    }
    assert!(wait_until(5, || info_field(&mut other, "blocked_clients") == 1));
    other.cmd(&["RPUSH", "q", "x"]);
    assert_eq!(
        c.read(),
        Reply::Array(vec![Reply::bulk("q"), Reply::bulk("x")])
    );
}

#[test]
fn a_key_named_twice_is_one_place_in_line() {
    let env = env();
    let mut ctl = env.client();
    let mut stream = env.client();
    let mut list = env.client();
    let mut behind = env.client();
    stream.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "s", "$", "$"]);
    list.send(&["BLPOP", "k", "k", "other", "k", "0"]);
    assert!(wait_until(5, || info_field(&mut ctl, "blocked_clients") == 2));
    behind.send(&["BLPOP", "k", "0"]);
    assert!(wait_until(5, || info_field(&mut ctl, "blocked_clients") == 3));
    // A value of the wrong type is no use to either list reader: they
    // look, pass the turn on, and wait again. Nobody keeps looking.
    ctl.cmd(&["SET", "k", "not-a-list"]);
    std::thread::sleep(Duration::from_millis(200));
    let settled = info_field(&mut ctl, "blocked_retries");
    std::thread::sleep(Duration::from_millis(400));
    let later = info_field(&mut ctl, "blocked_retries");
    assert_eq!(settled, later, "waiters keep retrying with nothing new");
    assert!(settled < 50, "{settled} retries for one change");
    assert_eq!(info_field(&mut ctl, "blocked_clients"), 3);

    // Each is served once when its data comes.
    ctl.cmd(&["DEL", "k"]);
    ctl.cmd(&["RPUSH", "k", "one", "two", "three"]);
    assert_eq!(
        list.read(),
        Reply::Array(vec![Reply::bulk("k"), Reply::bulk("one")])
    );
    assert_eq!(
        behind.read(),
        Reply::Array(vec![Reply::bulk("k"), Reply::bulk("two")])
    );
    assert_eq!(ctl.cmd(&["LLEN", "k"]), Reply::Int(1));
    ctl.cmd(&["XADD", "s", "5-1", "f", "v"]);
    let Reply::Array(streams) = stream.read() else {
        panic!("no streams");
    };
    // Named twice, answered twice, as the command asked.
    assert_eq!(streams.len(), 2, "{streams:?}");
    assert_eq!(info_field(&mut ctl, "blocked_clients"), 0);
    let quiet = info_field(&mut ctl, "blocked_retries");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(info_field(&mut ctl, "blocked_retries"), quiet);
}

#[test]
fn a_waiting_client_that_hangs_up_behind_its_own_input_is_noticed() {
    let env = env();
    let mut ctl = env.client();
    for pings in [10usize, 20_000, 60_000] {
        // About 14 bytes a PING: under the read chunk, over it, and near
        // the most a waiting connection may send ahead.
        let mut c = env.client();
        let mut wire = wire_str(&["BLPOP", "gone", "0"]);
        for _ in 0..pings {
            wire.extend(wire_str(&["PING"]));
        }
        c.send_raw(&wire);
        assert!(wait_until(5, || info_field(&mut ctl, "blocked_clients") == 1));
        drop(c);
        assert_eq!(env.wait_conns(1, 10), 1, "{pings} pings ahead");
        assert_eq!(info_field(&mut ctl, "blocked_clients"), 0);
    }
    // Nobody is left to take what arrives later.
    ctl.cmd(&["RPUSH", "gone", "kept"]);
    assert_eq!(ctl.cmd(&["LLEN", "gone"]), Reply::Int(1));

    // Far more than that ahead: the connection is closed while it waits,
    // rather than read without end or left unread.
    let mut c = env.client();
    let mut wire = wire_str(&["BLPOP", "flood", "0"]);
    let ping = wire_str(&["PING"]);
    while wire.len() < 8 << 20 {
        wire.extend(&ping);
    }
    let sent = c.try_send_raw(&wire);
    assert_eq!(env.wait_conns(1, 10), 1);
    assert!(sent.is_err() || c.try_read().is_err());
    // A waiting connection that sends a little ahead is left alone, and
    // what it sent runs once it is served.
    let mut c = env.client();
    let mut wire = wire_str(&["BLPOP", "served", "0"]);
    for _ in 0..5000 {
        wire.extend(wire_str(&["INCR", "ran"]));
    }
    c.send_raw(&wire);
    assert!(wait_until(5, || info_field(&mut ctl, "blocked_clients") == 1));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(env.wait_conns(2, 1), 2);
    ctl.cmd(&["RPUSH", "served", "x"]);
    assert_eq!(
        c.read(),
        Reply::Array(vec![Reply::bulk("served"), Reply::bulk("x")])
    );
    for n in 1..=5000 {
        assert_eq!(c.read(), Reply::Int(n));
    }
}

#[test]
fn counts_with_repeats_are_bounded() {
    let env = env();
    let mut c = env.client();
    c.cmd(&["SADD", "s", "a"]);
    c.cmd(&["HSET", "h", "f", "v"]);
    c.cmd(&["ZADD", "z", "1", "m"]);
    for count in [
        "-1048577",
        "-100000000",
        "-4611686018427387903",
        "-9223372036854775808",
    ] {
        for args in [
            &["SRANDMEMBER", "s", count][..],
            &["HRANDFIELD", "h", count],
            &["HRANDFIELD", "h", count, "WITHVALUES"],
            &["ZRANDMEMBER", "z", count],
            &["ZRANDMEMBER", "z", count, "WITHSCORES"],
            // Refused before the key is looked at.
            &["SRANDMEMBER", "nokey", count],
        ] {
            assert_eq!(
                c.cmd(args),
                Reply::err("ERR value is out of range"),
                "{args:?}"
            );
        }
        let script = format!("return redis.pcall('SRANDMEMBER', 's', '{count}')");
        assert_eq!(
            c.cmd(&["EVAL", &script, "0"]),
            Reply::err("ERR value is out of range")
        );
    }
    // The largest count allowed still answers in full.
    assert_eq!(
        bulks(c.cmd(&["SRANDMEMBER", "s", "-1048576"])).len(),
        1 << 20
    );
    assert_eq!(bulks(c.cmd(&["SRANDMEMBER", "s", "-3"])).len(), 3);
    // A positive count never repeats, so the set bounds it.
    assert_eq!(
        bulks(c.cmd(&["SRANDMEMBER", "s", "9223372036854775807"])).len(),
        1
    );
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
}

fn bitop_model(op: &str, sources: &[&[u8]]) -> Vec<u8> {
    let len = sources.iter().map(|s| s.len()).max().unwrap_or(0);
    (0..len)
        .map(|at| {
            let mut bytes = sources.iter().map(|s| s.get(at).copied().unwrap_or(0));
            let first = bytes.next().unwrap_or(0);
            match op {
                "NOT" => !first,
                "AND" => bytes.fold(first, |x, y| x & y),
                "OR" => bytes.fold(first, |x, y| x | y),
                _ => bytes.fold(first, |x, y| x ^ y),
            }
        })
        .collect()
}

#[test]
fn repeated_source_keys_give_the_same_answers() {
    let env = env();
    let mut c = env.client();
    let values: [(&str, &[u8]); 4] = [
        ("a", b"\xff\x0f\xf0\x55\xaa"),
        ("b", b"\x0f\xff"),
        ("c", b"\x81\x00\x7e\x00\x00\x00\x01"),
        ("missing", b""),
    ];
    for (key, value) in values.iter().filter(|(_, v)| !v.is_empty()) {
        c.cmd(&[b"SET".to_vec(), key.as_bytes().to_vec(), value.to_vec()]);
    }
    let value_of = |key: &str| values.iter().find(|(k, _)| *k == key).unwrap().1;
    let lists: &[&[&str]] = &[
        &["a"],
        &["a", "a"],
        &["a", "a", "a"],
        &["b", "a", "b", "a", "b"],
        &["missing", "a"],
        &["a", "missing", "a"],
        &["c", "b", "a", "c"],
        &["b", "c"],
        &["missing", "missing"],
    ];
    for op in ["AND", "OR", "XOR"] {
        for keys in lists {
            let mut args = vec!["BITOP", op, "dst"];
            args.extend(keys.iter());
            let sources: Vec<&[u8]> = keys.iter().map(|k| value_of(k)).collect();
            let want = bitop_model(op, &sources);
            assert_eq!(c.cmd(&args), Reply::Int(want.len() as i64), "{args:?}");
            let got = match c.cmd(&["GET", "dst"]) {
                Reply::Bulk(b) => b,
                Reply::Nil => Vec::new(),
                other => panic!("{other:?}"),
            };
            assert_eq!(got, want, "{args:?}");
        }
    }
    assert_eq!(c.cmd(&["BITOP", "NOT", "dst", "a"]), Reply::Int(5));
    assert_eq!(
        c.cmd(&["GET", "dst"]),
        Reply::Bulk(bitop_model("NOT", &[value_of("a")]))
    );
    // The destination may be one of the sources, any number of times.
    assert_eq!(c.cmd(&["BITOP", "XOR", "a", "a", "b", "a"]), Reply::Int(5));
    assert_eq!(
        c.cmd(&["GET", "a"]),
        Reply::Bulk(b"\x0f\xff\x00\x00\x00".to_vec())
    );
    c.cmd(&["RPUSH", "list", "x"]);
    assert!(c.cmd(&["BITOP", "OR", "dst", "b", "list", "b"]).is_error());

    // Many names for one large value cost one value, not one each.
    let big = vec![0x5au8; 4 << 20];
    c.cmd(&[b"SET".to_vec(), b"big".to_vec(), big.clone()]);
    let mut args: Vec<Vec<u8>> = vec![b"BITOP".to_vec(), b"AND".to_vec(), b"dst".to_vec()];
    args.extend(std::iter::repeat_n(b"big".to_vec(), 2000));
    let before = private_bytes();
    assert_eq!(c.cmd(&args), Reply::Int(4 << 20));
    assert_eq!(c.cmd(&["STRLEN", "dst"]), Reply::Int(4 << 20));
    assert_eq!(c.cmd(&["BITCOUNT", "dst"]), Reply::Int(4 * (4 << 20)));
    if let (Some(before), Some(after)) = (before, private_bytes()) {
        assert!(
            after < before + (256 << 20),
            "memory grew from {before} to {after}"
        );
    }

    // Sets: the same members whatever is repeated.
    c.cmd(&["SADD", "s1", "a", "b", "c", "d"]);
    c.cmd(&["SADD", "s2", "c", "d", "e"]);
    c.cmd(&["SADD", "s3", "d", "x"]);
    let sorted = |reply: Reply| {
        let mut members = bulks(reply);
        members.sort();
        members
            .into_iter()
            .map(|m| String::from_utf8(m).unwrap())
            .collect::<Vec<_>>()
            .join("")
    };
    for (args, want) in [
        (&["SINTER", "s1", "s1"][..], "abcd"),
        (&["SINTER", "s1", "s2", "s1", "s2", "s2"], "cd"),
        (&["SINTER", "s2", "s1", "s3", "s1"], "d"),
        (&["SINTER", "s1", "nokey", "s1"], ""),
        (&["SUNION", "s1", "s1", "s2", "s2", "s1"], "abcde"),
        (&["SUNION", "nokey", "s3", "nokey", "s3"], "dx"),
        (&["SDIFF", "s1", "s2", "s2"], "ab"),
        (&["SDIFF", "s1", "s2", "s3", "s2", "s3"], "ab"),
        (&["SDIFF", "s1", "s1"], ""),
        (&["SDIFF", "s1", "s2", "s1"], ""),
        (&["SDIFF", "s1", "nokey", "nokey"], "abcd"),
        (&["SDIFF", "s1"], "abcd"),
    ] {
        assert_eq!(sorted(c.cmd(args)), want, "{args:?}");
    }
    assert_eq!(
        c.cmd(&["SINTERCARD", "4", "s1", "s2", "s1", "s2"]),
        Reply::Int(2)
    );
    assert_eq!(
        c.cmd(&["SUNIONSTORE", "s1", "s1", "s2", "s1"]),
        Reply::Int(5)
    );
    assert_eq!(c.cmd(&["SDIFFSTORE", "s1", "s1", "s1"]), Reply::Int(0));
    assert_eq!(c.cmd(&["EXISTS", "s1"]), Reply::Int(0));
    assert!(c.cmd(&["SUNION", "s2", "list", "s2"]).is_error());
}

#[test]
fn client_names_are_short() {
    let env = env();
    let mut c = env.client();
    let fits = "n".repeat(256);
    let long = "n".repeat(257);
    assert_eq!(c.cmd(&["CLIENT", "SETNAME", &fits]), Reply::Ok);
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]), Reply::bulk(fits.clone()));
    assert_eq!(
        c.cmd(&["CLIENT", "SETNAME", &long]),
        Reply::err("ERR Client names cannot be longer than 256 bytes")
    );
    assert_eq!(c.cmd(&["CLIENT", "SETINFO", "LIB-NAME", &fits]), Reply::Ok);
    for option in ["LIB-NAME", "lib-ver"] {
        assert_eq!(
            c.cmd(&["CLIENT", "SETINFO", option, &long]),
            Reply::err("ERR lib-name and lib-ver cannot be longer than 256 bytes")
        );
    }
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]), Reply::bulk(fits));
}
