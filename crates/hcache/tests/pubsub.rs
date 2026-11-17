//! Pub/sub over TCP: delivery, patterns, RESP2 and RESP3 framing, the
//! restricted command set of a subscribed RESP2 connection, cleanup when
//! subscribers vanish, and order under load.

mod net;

use hcache::Reply;
use net::*;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn env(workers: usize) -> Env {
    Env::new(engine_cfg(8), server_cfg(workers, 64))
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn arr(items: &[Reply]) -> Reply {
    Reply::Array(items.to_vec())
}

fn read_exactly(c: &mut Client, want: &[u8]) {
    let mut got = vec![0u8; want.len()];
    c.rd.read_exact(&mut got).expect("read");
    assert_eq!(String::from_utf8_lossy(&got), String::from_utf8_lossy(want));
}

#[test]
fn messages_reach_channel_and_pattern_subscribers() {
    let env = env(3);
    let mut sub = env.client();
    let mut psub = env.client();
    let mut publisher = env.client();
    assert_eq!(
        sub.cmd(&["SUBSCRIBE", "news", "sport"]),
        arr(&[b("subscribe"), b("news"), Reply::Int(1)])
    );
    assert_eq!(
        sub.read(),
        arr(&[b("subscribe"), b("sport"), Reply::Int(2)])
    );
    assert_eq!(
        psub.cmd(&["PSUBSCRIBE", "n*"]),
        arr(&[b("psubscribe"), b("n*"), Reply::Int(1)])
    );
    assert_eq!(publisher.cmd(&["PUBLISH", "news", "hello"]), Reply::Int(2));
    assert_eq!(publisher.cmd(&["PUBLISH", "sport", "goal"]), Reply::Int(1));
    assert_eq!(publisher.cmd(&["PUBLISH", "nobody", "x"]), Reply::Int(1));
    assert_eq!(publisher.cmd(&["PUBLISH", "void", "x"]), Reply::Int(0));
    assert_eq!(sub.read(), arr(&[b("message"), b("news"), b("hello")]));
    assert_eq!(sub.read(), arr(&[b("message"), b("sport"), b("goal")]));
    assert_eq!(
        psub.read(),
        arr(&[b("pmessage"), b("n*"), b("news"), b("hello")])
    );
    assert_eq!(
        psub.read(),
        arr(&[b("pmessage"), b("n*"), b("nobody"), b("x")])
    );

    // Introspection.
    assert_eq!(
        publisher.cmd(&["PUBSUB", "NUMSUB", "news", "void"]),
        arr(&[b("news"), Reply::Int(1), b("void"), Reply::Int(0)])
    );
    assert_eq!(publisher.cmd(&["PUBSUB", "NUMPAT"]), Reply::Int(1));
    assert_eq!(
        publisher.cmd(&["PUBSUB", "CHANNELS", "s*"]),
        arr(&[b("sport")])
    );

    // A subscribed RESP2 connection speaks only the subscription commands.
    let refused = sub.cmd(&["GET", "k"]);
    assert!(
        error_text(&refused).is_some_and(|e| e.starts_with("ERR Can't execute 'get'")),
        "{refused:?}"
    );
    assert_eq!(sub.cmd(&["PING"]), arr(&[b("pong"), b("")]));
    assert_eq!(
        sub.cmd(&["UNSUBSCRIBE", "news"]),
        arr(&[b("unsubscribe"), b("news"), Reply::Int(1)])
    );
    assert_eq!(publisher.cmd(&["PUBLISH", "news", "again"]), Reply::Int(1));
    // Unsubscribing from everything gives the full command set back.
    assert_eq!(
        sub.cmd(&["UNSUBSCRIBE"]),
        arr(&[b("unsubscribe"), b("sport"), Reply::Int(0)])
    );
    assert_eq!(sub.cmd(&["SET", "k", "v"]), Reply::Ok);
    assert_eq!(
        sub.cmd(&["UNSUBSCRIBE"]),
        arr(&[b("unsubscribe"), Reply::Nil, Reply::Int(0)])
    );
    assert_eq!(publisher.cmd(&["PUBLISH", "sport", "x"]), Reply::Int(0));
}

#[test]
fn resp3_subscribers_get_push_frames_and_keep_all_commands() {
    let env = env(2);
    let mut c = env.client();
    c.send(&["HELLO", "3"]);
    let mut seen = Vec::new();
    let mut byte = [0u8; 1];
    while !seen.ends_with(b"modules\r\n*0\r\n") {
        c.rd.read_exact(&mut byte).expect("hello");
        seen.push(byte[0]);
    }
    c.send(&["SUBSCRIBE", "ch"]);
    read_exactly(&mut c, b">3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n");
    c.send(&["SET", "k", "v"]);
    read_exactly(&mut c, b"+OK\r\n");
    let mut publisher = env.client();
    assert_eq!(publisher.cmd(&["PUBLISH", "ch", "payload"]), Reply::Int(1));
    read_exactly(
        &mut c,
        b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$7\r\npayload\r\n",
    );
    c.send(&["PING"]);
    read_exactly(&mut c, b"+PONG\r\n");
    c.send(&["UNSUBSCRIBE", "other"]);
    read_exactly(&mut c, b">3\r\n$11\r\nunsubscribe\r\n$5\r\nother\r\n:1\r\n");
}

#[test]
fn a_vanished_subscriber_is_forgotten() {
    let env = env(2);
    let mut publisher = env.client();
    for round in 0..50 {
        let mut sub = env.client();
        sub.cmd(&["SUBSCRIBE", "ch"]);
        sub.cmd(&["PSUBSCRIBE", "c*"]);
        assert_eq!(publisher.cmd(&["PUBLISH", "ch", "x"]), Reply::Int(2));
        drop(sub);
        let gone = wait_until(5, || {
            publisher.cmd(&["PUBSUB", "NUMSUB", "ch"]) == arr(&[b("ch"), Reply::Int(0)])
                && publisher.cmd(&["PUBSUB", "NUMPAT"]) == Reply::Int(0)
        });
        assert!(gone, "round {round}: subscription outlived its connection");
        assert_eq!(publisher.cmd(&["PUBLISH", "ch", "x"]), Reply::Int(0));
    }
}

#[test]
fn every_subscriber_sees_every_message_in_publish_order() {
    const PUBLISHERS: usize = 4;
    const PER_PUBLISHER: usize = 3000;
    const SUBSCRIBERS: usize = 6;
    let env = env(4);
    let mut subs: Vec<Client> = (0..SUBSCRIBERS)
        .map(|i| {
            let mut c = env.client_with(Duration::from_secs(20));
            if i % 2 == 0 {
                c.cmd(&["SUBSCRIBE", "stream"]);
            } else {
                c.cmd(&["PSUBSCRIBE", "str?am"]);
            }
            c
        })
        .collect();
    let publishers: Vec<_> = (0..PUBLISHERS)
        .map(|p| {
            let addr = env.addr;
            thread::spawn(move || {
                let mut c = Client::connect(addr);
                for i in 0..PER_PUBLISHER {
                    // Pipelined in bursts, replies read afterwards.
                    c.send(&["PUBLISH", "stream", &format!("{p}:{i}")]);
                    if i % 100 == 99 {
                        for _ in 0..100 {
                            assert_eq!(c.read(), Reply::Int(SUBSCRIBERS as i64));
                        }
                    }
                }
            })
        })
        .collect();
    let readers: Vec<_> = subs
        .drain(..)
        .enumerate()
        .map(|(n, mut c)| {
            thread::spawn(move || {
                let mut next = [0usize; PUBLISHERS];
                for _ in 0..PUBLISHERS * PER_PUBLISHER {
                    let Reply::Array(parts) = c.read() else {
                        panic!("subscriber {n}: not a message");
                    };
                    let Some(Reply::Bulk(payload)) = parts.last() else {
                        panic!("subscriber {n}: no payload");
                    };
                    let text = String::from_utf8_lossy(payload);
                    let (p, i) = text.split_once(':').expect("payload shape");
                    let (p, i): (usize, usize) = (p.parse().unwrap(), i.parse().unwrap());
                    assert_eq!(i, next[p], "subscriber {n}: publisher {p} out of order");
                    next[p] += 1;
                }
                assert!(c.silent_for(100), "subscriber {n}: extra message");
            })
        })
        .collect();
    for t in publishers {
        t.join().expect("publisher");
    }
    for t in readers {
        t.join().expect("subscriber");
    }
}

#[test]
fn publish_from_scripts_transactions_and_embedded_sessions() {
    let env = env(2);
    let mut sub = env.client();
    sub.cmd(&["SUBSCRIBE", "ch"]);
    let mut c = env.client();
    assert_eq!(
        c.cmd(&["EVAL", "return redis.call('publish', 'ch', 'lua')", "0"]),
        Reply::Int(1)
    );
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    c.cmd(&["PUBLISH", "ch", "tx"]);
    assert!(c.cmd(&["SUBSCRIBE", "ch"]).is_error());
    assert!(c.cmd(&["EXEC"]).is_error());
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    c.cmd(&["PUBLISH", "ch", "tx"]);
    assert_eq!(c.cmd(&["EXEC"]), arr(&[Reply::Int(1)]));
    let session = env.db.session().unwrap();
    assert_eq!(
        session.execute(&["PUBLISH", "ch", "embedded"]),
        Reply::Int(1)
    );
    assert!(session.execute(&["SUBSCRIBE", "ch"]).is_error());
    for want in ["lua", "tx", "embedded"] {
        assert_eq!(sub.read(), arr(&[b("message"), b("ch"), b(want)]));
    }
}

#[test]
fn a_subscriber_that_never_reads_is_cut_off_and_others_are_not_held_up() {
    let env = env(2);
    let mut stuck = env.client();
    stuck.cmd(&["SUBSCRIBE", "flood"]);
    let mut healthy = env.client_with(Duration::from_secs(30));
    healthy.cmd(&["SUBSCRIBE", "flood"]);
    let got = Arc::new(AtomicUsize::new(0));
    let reader = thread::spawn({
        let got = got.clone();
        move || {
            while got.load(Ordering::Relaxed) < 600 {
                let Reply::Array(parts) = healthy.read() else {
                    panic!("not a message");
                };
                assert_eq!(parts.len(), 3);
                got.fetch_add(1, Ordering::Relaxed);
            }
            healthy
        }
    });
    let mut publisher = env.client_with(Duration::from_secs(30));
    let payload = "p".repeat(100_000);
    let mut receivers_at_end = 0;
    // 60 MB: past the 32 MB a subscriber may fall behind.
    for sent in 0..600usize {
        // The healthy reader stays within 5 MB, however slow the host is.
        let caught_up = wait_until(30, || {
            sent.saturating_sub(got.load(Ordering::Relaxed)) <= 50
        });
        assert!(caught_up, "the healthy subscriber stopped reading");
        match publisher.cmd(&["PUBLISH", "flood", &payload]) {
            Reply::Int(n) => receivers_at_end = n,
            other => panic!("{other:?}"),
        }
    }
    let _healthy = reader.join().expect("healthy subscriber");
    let dropped = wait_until(10, || {
        publisher.cmd(&["PUBSUB", "NUMSUB", "flood"]) == arr(&[b("flood"), Reply::Int(1)])
    });
    assert!(dropped, "the stuck subscriber is still registered");
    assert!(receivers_at_end >= 1);
    drop(stuck);
}
