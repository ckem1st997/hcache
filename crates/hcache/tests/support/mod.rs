//! Helpers shared by the adversarial engine tests (`model`, `concurrency`,
//! `script`). Each test binary uses a different subset.
#![allow(dead_code)]

use hcache::{Config, Db, Reply, Session};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Run `f` on its own thread and fail the test, instead of hanging the run,
/// when it does not finish within `secs`. A panic inside `f` is re-raised.
pub fn guarded<T: Send + 'static>(
    name: &str,
    secs: u64,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = mpsc::channel();
    let handle = thread::Builder::new()
        .name(format!("guarded-{name}"))
        .spawn(move || {
            let _ = tx.send(f());
        })
        .expect("spawn guarded thread");
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(value) => {
            let _ = handle.join();
            value
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("HANG: `{name}` did not finish within {secs} s (deadlock or livelock)")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => match handle.join() {
            Err(payload) => std::panic::resume_unwind(payload),
            Ok(()) => panic!("`{name}`: worker ended without a result"),
        },
    }
}

pub fn cfg(shards: usize, drain_batch: usize, multi_escalate_after: u32) -> Config {
    Config {
        shards,
        drain_batch,
        multi_escalate_after,
        ..Config::default()
    }
}

pub fn open(config: Config) -> Db {
    Db::open(None, config).expect("open in-memory db")
}

pub fn session(db: &Db) -> Session {
    db.session().expect("session slot")
}

pub fn run(s: &Session, args: &[&str]) -> Reply {
    s.execute(args)
}

pub fn bulk(text: &str) -> Reply {
    Reply::bulk(text)
}

pub fn err(text: &str) -> Reply {
    Reply::Error(text.to_string())
}

/// RESP bytes of a reply.
pub fn enc(reply: &Reply) -> Vec<u8> {
    let mut out = Vec::new();
    reply.encode(&mut out);
    out
}

pub fn err_text(reply: &Reply) -> String {
    match reply {
        Reply::Error(text) => text.clone(),
        other => panic!("expected an error reply, got {}", brief(other)),
    }
}

pub fn has_err(reply: &Reply, needle: &str) -> bool {
    matches!(reply, Reply::Error(text) if text.contains(needle))
}

pub fn int(reply: &Reply) -> i64 {
    match reply {
        Reply::Int(n) => *n,
        other => panic!("expected an integer reply, got {}", brief(other)),
    }
}

pub fn text(reply: &Reply) -> String {
    match reply {
        Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        Reply::Simple(s) => s.clone(),
        other => panic!("expected a string reply, got {}", brief(other)),
    }
}

pub fn array(reply: Reply) -> Vec<Reply> {
    match reply {
        Reply::Array(items) => items,
        other => panic!("expected an array reply, got {}", brief(&other)),
    }
}

/// `Debug` output cut to a readable size (values can be megabytes).
pub fn brief(reply: &Reply) -> String {
    let mut s = format!("{reply:?}");
    if s.len() > 300 {
        let mut end = 300;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
        s.push_str("...");
    }
    s
}

pub fn eval(s: &Session, script: &str, keys: &[&str], args: &[&str]) -> Reply {
    let numkeys = keys.len().to_string();
    let mut argv: Vec<&str> = vec!["EVAL", script, &numkeys];
    argv.extend_from_slice(keys);
    argv.extend_from_slice(args);
    s.execute(&argv)
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
