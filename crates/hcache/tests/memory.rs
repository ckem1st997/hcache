//! Process-wide memory checks. They live in their own test binary so no
//! other test allocates while they measure.

mod net;

use hcache::Reply;
use net::*;
use std::io::{BufRead, Read};
use std::thread;
use std::time::{Duration, Instant};

fn ping(c: &mut Client) {
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
}

#[test]
fn connection_gives_back_the_memory_of_a_huge_request_and_reply() {
    let _h = heavy();
    watchdog("buffer release", 120, || {
        const HUGE: usize = 48 << 20;
        let env = Env::new(engine_cfg(8), server_cfg(1, 64));
        let mut c = env.client();
        ping(&mut c);
        let Some(before) = private_bytes() else {
            eprintln!("skipped: no memory counter on this platform");
            return;
        };
        let value: Vec<u8> = (0..HUGE).map(|i| (i % 251) as u8).collect();
        assert_eq!(
            c.cmd(&[b"SET".as_slice(), b"huge".as_slice(), &value]),
            Reply::Ok
        );
        // Read the reply in small bites so the server sends it in many pieces.
        c.send(&["GET", "huge"]);
        let mut header = Vec::new();
        c.rd.read_until(b'\n', &mut header).expect("header");
        assert_eq!(header, format!("${HUGE}\r\n").into_bytes());
        let mut got = vec![0u8; HUGE + 2];
        let t = Instant::now();
        for (i, chunk) in got.chunks_mut(64 << 10).enumerate() {
            c.rd.read_exact(chunk).expect("body");
            if i % 64 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
        }
        eprintln!("slow read of {} MiB took {:?}", HUGE >> 20, t.elapsed());
        assert!(got[..HUGE] == value[..], "value came back damaged");
        assert_eq!(&got[HUGE..], b"\r\n");
        assert_eq!(c.cmd(&["DEL", "huge"]), Reply::Int(1));
        drop((value, got));
        ping(&mut c);
        let kept = wait_until(5, || {
            private_bytes().is_some_and(|now| now.saturating_sub(before) < HUGE / 2)
        });
        assert!(
            kept,
            "the idle connection still holds {} MiB after a {} MiB request and reply",
            private_bytes().map_or(0, |now| now.saturating_sub(before)) >> 20,
            HUGE >> 20
        );
    });
}
