//! Valkey 9's new commands (HGETDEL, HGETEX, HSETEX, DELIFEQ, MSETEX, SET
//! IFEQ and sharded pub/sub) against docs/valkey9-wave1-spec.md, whose vectors were
//! captured from a live Valkey 9.1.3. The transcripts there run as they
//! stand: the `> command` lines, the replies as valkey-cli prints them, the
//! keyspace events, and the bytes on the wire. Where HCache answers
//! otherwise on purpose, `DIFFERS` says so and why.

mod net;

use hcache::{Db, Reply};
use net::*;
use std::collections::HashMap;
use std::io::Read;
use std::thread;
use std::time::Duration;

const SPEC: &str = include_str!("../../../docs/valkey9-wave1-spec.md");

/// A field or key given `PX 1` is waited out for this long, as the capture
/// did with `DEBUG SLEEP 0.05`.
const EXPIRED: Duration = Duration::from_millis(50);

/// Replies HCache gives on purpose where Valkey's differ: vector, command,
/// and HCache's reply as valkey-cli would print it.
const DIFFERS: &[(&str, &str, &[&str])] = &[
    // The capture turned active expiry off, so an expired field stayed in
    // the hash. HCache always reaps it, as Valkey does moments later.
    ("HD-12", "HLEN h", &["(integer) 0"]),
    ("HD-12", "EXISTS h", &["(integer) 0"]),
    ("HG-16", "HLEN h", &["(integer) 1"]),
    // The field reaped and set again comes last; a hash keeps no order.
    (
        "HS-18",
        "HGETALL h",
        &["1) \"k\"", "2) \"1\"", "3) \"e\"", "4) \"new\""],
    ),
];

/// Arguments the capture printed joined by spaces, so that splitting them
/// again would be wrong.
const ARGS: &[(&str, &[&str])] = &[
    ("SET b a b", &["SET", "b", "a b"]),
    ("DELIFEQ b a b", &["DELIFEQ", "b", "a b"]),
];

/// Blocks a runner below cannot take as they stand; each has a test of its
/// own further down.
const BY_HAND: &[&str] = &[
    "EV-HSETEX-overwrite-expired",
    "EV-active-field-expiry",
    "EV-active-expiry-wait",
    // The capture lost a subscriber between steps: B's PSUBSCRIBE in
    // SP-04, one of three in SP-16.
    "SP-04",
    "SP-13",
    // HCache refuses SUBSCRIBE inside MULTI.
    "SP-14",
    "SP-15",
    "SP-16",
    "SP-17",
];

struct Vector {
    id: String,
    title: String,
    lines: Vec<String>,
}

/// Every vector of the spec: the ```text block under a `**ID** title` line.
fn vectors() -> Vec<Vector> {
    let mut out = Vec::new();
    let mut named: Option<(String, String)> = None;
    let mut block: Option<Vec<String>> = None;
    for line in SPEC.lines() {
        if let Some(lines) = &mut block {
            if line.starts_with("```") {
                let (id, title) = named.take().expect("a block has a name");
                out.push(Vector {
                    id,
                    title,
                    lines: block.take().unwrap_or_default(),
                });
            } else {
                lines.push(line.to_string());
            }
        } else if line.starts_with('#') {
            named = None;
        } else if let Some((id, title)) = line
            .strip_prefix("**")
            .and_then(|rest| rest.split_once("**"))
        {
            named = Some((id.to_string(), title.trim().to_string()));
        } else if line.starts_with("```text") && named.is_some() {
            block = Some(Vec::new());
        }
    }
    out
}

fn of_kind<'a>(all: &'a [Vector], prefixes: &[&str]) -> Vec<&'a Vector> {
    let picked: Vec<_> = all
        .iter()
        .filter(|v| prefixes.iter().any(|p| v.id.starts_with(p)))
        .filter(|v| !BY_HAND.iter().any(|h| v.id.starts_with(h)))
        .collect();
    assert!(!picked.is_empty(), "no vectors for {prefixes:?}");
    picked
}

fn args_of(line: &str) -> Vec<String> {
    if let Some((_, args)) = ARGS.iter().find(|(l, _)| *l == line) {
        return args.iter().map(|a| a.to_string()).collect();
    }
    line.split(' ').map(str::to_string).collect()
}

/// After `PX 1` the capture waited for the field or key to expire.
fn waits(args: &[String]) -> bool {
    args.windows(2)
        .any(|w| w[0].eq_ignore_ascii_case("PX") && w[1] == "1")
}

// ---- transcripts, embedded ----

/// A reply as `valkey-cli --no-raw` prints it.
fn render(reply: &Reply) -> Vec<String> {
    match reply {
        Reply::Array(items) | Reply::Map(items) if items.is_empty() => {
            vec!["(empty array)".into()]
        }
        Reply::Array(items) | Reply::Map(items) => items
            .iter()
            .enumerate()
            .map(|(i, item)| format!("{}) {}", i + 1, one(item)))
            .collect(),
        other => vec![one(other)],
    }
}

fn one(reply: &Reply) -> String {
    match reply {
        Reply::Ok => "OK".into(),
        Reply::Simple(s) => s.clone(),
        Reply::Error(e) => format!("(error) {e}"),
        Reply::Int(n) => format!("(integer) {n}"),
        Reply::Bulk(b) => format!("{:?}", String::from_utf8_lossy(b)),
        Reply::Nil | Reply::NilArray => "(nil)".into(),
        nested => format!("{nested:?}"),
    }
}

/// Relative TTLs may read a little lower than what was set (spec 0).
fn close_enough(cmd: &str, got: &str, want: &str) -> bool {
    let slack = match cmd.to_ascii_uppercase().as_str() {
        "TTL" | "HTTL" => 1,
        "PTTL" | "HPTTL" => 50,
        _ => return false,
    };
    let number = |line: &str| line.rsplit(' ').next()?.parse::<i64>().ok();
    match (number(got), number(want)) {
        (Some(g), Some(w)) => g >= 0 && w >= 0 && (g - w).abs() <= slack,
        _ => false,
    }
}

fn run_transcript(v: &Vector, failures: &mut Vec<String>) {
    let db = Db::open(None, engine_cfg(4)).unwrap();
    let s = db.session().unwrap();
    let mut i = 0;
    while i < v.lines.len() {
        let Some(line) = v.lines[i].strip_prefix("> ") else {
            failures.push(format!("{}: stray line {:?}", v.id, v.lines[i]));
            i += 1;
            continue;
        };
        let want: Vec<&str> = v.lines[i + 1..]
            .iter()
            .take_while(|l| !l.starts_with("> "))
            .map(String::as_str)
            .collect();
        i += 1 + want.len();
        let args = args_of(line);
        let got = render(&s.execute(&args));
        if waits(&args) {
            thread::sleep(EXPIRED);
        }
        let want: Vec<&str> = DIFFERS
            .iter()
            .find(|(id, cmd, _)| *id == v.id && *cmd == line)
            .map_or(want, |(_, _, ours)| ours.to_vec());
        let same = got.len() == want.len()
            && got
                .iter()
                .zip(&want)
                .all(|(g, w)| g == w || close_enough(&args[0], g, w));
        if !same {
            failures.push(format!(
                "{} `{line}`\n    want {want:?}\n    got  {got:?}",
                v.id
            ));
        }
    }
}

#[test]
fn hash_and_string_commands_match_the_captured_transcripts() {
    let all = vectors();
    let mut failures = Vec::new();
    let picked = of_kind(&all, &["HD-", "HG-", "HS-", "DE-", "MS-", "SI-"]);
    for v in &picked {
        run_transcript(v, &mut failures);
    }
    assert!(
        failures.is_empty(),
        "{} of {} vectors' lines differ:\n{}",
        failures.len(),
        picked.len(),
        failures.join("\n")
    );
}

// ---- keyspace events and wire bytes, over TCP ----

/// The bytes of a Python literal as the capture printed them: '..' or "..".
fn unrepr(literal: &str) -> Vec<u8> {
    let inner = &literal[1..literal.len() - 1];
    let mut out = Vec::new();
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            let mut buf = [0; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            continue;
        }
        match chars.next() {
            Some('r') => out.push(b'\r'),
            Some('n') => out.push(b'\n'),
            Some('t') => out.push(b'\t'),
            Some('x') => {
                let hex: String = chars.by_ref().take(2).collect();
                out.push(u8::from_str_radix(&hex, 16).expect("\\x escape"));
            }
            Some(other) => out.push(other as u8),
            None => panic!("dangling escape in {literal}"),
        }
    }
    out
}

fn encoded(reply: &Reply) -> Vec<u8> {
    let mut out = Vec::new();
    reply.encode(&mut out);
    out
}

/// The events a subscriber to `__keyevent@0__:*` has received, as
/// "event key", up to the one for key `until`.
fn events(sub: &mut Client, until: &str) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        let Reply::Array(message) = sub.read() else {
            panic!("not a message");
        };
        let text = |r: &Reply| match r {
            Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("{other:?}"),
        };
        let key = text(&message[3]);
        if key == until {
            return seen;
        }
        let channel = text(&message[2]);
        let event = channel.rsplit(':').next().unwrap_or_default().to_string();
        seen.push(format!("{event} {key}"));
    }
}

fn notifying() -> (Env, Client, Client) {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let mut c = env.client();
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "EA"]),
        Reply::Ok
    );
    let mut sub = env.client();
    sub.cmd(&["PSUBSCRIBE", "__keyevent@0__:*"]);
    (env, c, sub)
}

fn run_events(v: &Vector, failures: &mut Vec<String>) {
    let (env, mut c, mut sub) = notifying();
    let mut want = Vec::new();
    let mut lines = v.lines.iter();
    let mut set_up = false;
    while let Some(line) = lines.next() {
        if let Some(cmd) = line.strip_prefix("(setup) > ") {
            c.cmd(&args_of(cmd));
            continue;
        }
        if !set_up {
            // What the setup did is not part of the trace.
            assert_eq!(c.cmd(&["SET", "setup-done", "1"]), Reply::Ok);
            events(&mut sub, "setup-done");
            set_up = true;
        }
        let cmd = line.strip_prefix("> ").expect("a command");
        let reply = lines.next().and_then(|l| l.trim().strip_prefix("reply: "));
        let heard = lines.next().and_then(|l| l.trim().strip_prefix("events: "));
        let (Some(reply), Some(heard)) = (reply, heard) else {
            panic!("{}: `{cmd}` lacks its reply or events", v.id);
        };
        let args = args_of(cmd);
        let got = encoded(&c.cmd(&args));
        if waits(&args) {
            thread::sleep(EXPIRED);
        }
        if got != unrepr(reply) {
            failures.push(format!(
                "{} `{cmd}`\n    want {reply}\n    got  {:?}",
                v.id,
                String::from_utf8_lossy(&got)
            ));
        }
        if heard != "(none)" {
            want.extend(heard.split(", ").map(str::to_string));
        }
    }
    if want.iter().any(|e| e.starts_with("expired ")) {
        // A read that finds a key expired leaves its removal, and the
        // event, to the active expiry cycle; run it now.
        env.db.sweep_expired();
    }
    assert_eq!(c.cmd(&["SET", "vector-done", "1"]), Reply::Ok);
    let got = events(&mut sub, "vector-done");
    if got != want {
        failures.push(format!(
            "{} events\n    want {want:?}\n    got  {got:?}",
            v.id
        ));
    }
}

#[test]
fn keyspace_events_match_the_captured_traces() {
    let all = vectors();
    let mut failures = Vec::new();
    let picked = of_kind(&all, &["EV-"]);
    for v in &picked {
        run_events(v, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// HELLO 3, its reply read past.
fn resp3(c: &mut Client) {
    c.send(&["HELLO", "3"]);
    c.send(&["PING"]);
    let mut all = Vec::new();
    let mut buf = [0u8; 4096];
    while !all.ends_with(b"+PONG\r\n") {
        let n = c.rd.read(&mut buf).expect("read");
        assert!(n > 0, "closed");
        all.extend_from_slice(&buf[..n]);
    }
}

fn read_bytes(c: &mut Client, n: usize) -> Vec<u8> {
    let mut got = vec![0; n];
    let mut have = 0;
    while have < n {
        match c.rd.read(&mut got[have..]) {
            Ok(0) | Err(_) => break,
            Ok(k) => have += k,
        }
    }
    got.truncate(have);
    got
}

/// Commands whose frames come in no set order.
fn unordered(cmd: &str) -> bool {
    cmd == "SUNSUBSCRIBE" || cmd.starts_with("PUBSUB SHARDCHANNELS")
}

fn run_pubsub(v: &Vector, failures: &mut Vec<String>) {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let mut clients: HashMap<String, Client> = HashMap::new();
    let mut lines = v.lines.iter();
    while let Some(line) = lines.next() {
        let (name, rest) = line
            .strip_prefix('[')
            .and_then(|l| l.split_once("] "))
            .unwrap_or_else(|| panic!("{}: {line}", v.id));
        let c = clients.entry(name.to_string()).or_insert_with(|| {
            let mut c = env.client_with(Duration::from_secs(2));
            if v.title.contains("RESP3") || name.ends_with('3') {
                resp3(&mut c);
            }
            c
        });
        let (cmd, want) = if let Some(want) = rest.strip_prefix("(async) <- ") {
            (None, want)
        } else {
            let cmd = rest.strip_prefix("> ").expect("a command");
            let want = lines
                .next()
                .and_then(|l| l.trim().strip_prefix("<- "))
                .unwrap_or_else(|| panic!("{}: `{cmd}` lacks its reply", v.id));
            c.send(&args_of(cmd));
            (Some(cmd), want)
        };
        let what = cmd.unwrap_or("(async)");
        if want == "(nothing)" {
            if !c.silent_for(150) {
                failures.push(format!("{} [{name}] heard something", v.id));
            }
            continue;
        }
        let want = unrepr(want);
        let got = read_bytes(c, want.len());
        let sorted = |b: &[u8]| {
            let mut b = b.to_vec();
            b.sort_unstable();
            b
        };
        let same = got == want || cmd.is_some_and(unordered) && sorted(&got) == sorted(&want);
        if !same {
            failures.push(format!(
                "{} [{name}] `{what}`\n    want {:?}\n    got  {:?}",
                v.id,
                String::from_utf8_lossy(&want),
                String::from_utf8_lossy(&got)
            ));
        }
    }
}

#[test]
fn sharded_pubsub_matches_the_captured_bytes() {
    let all = vectors();
    let mut failures = Vec::new();
    let picked = of_kind(&all, &["SP-"]);
    for v in &picked {
        run_pubsub(v, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ---- the blocks run by hand ----

#[test]
fn overwriting_an_expired_field_reports_it_expired_first() {
    let (_env, mut c, mut sub) = notifying();
    // Long enough not to expire before the setup is told, even under load.
    c.cmd(&["HSETEX", "h", "PX", "300", "FIELDS", "1", "e", "v"]);
    c.cmd(&["HSET", "h", "k", "1"]);
    assert_eq!(c.cmd(&["SET", "setup-done", "1"]), Reply::Ok);
    events(&mut sub, "setup-done");
    thread::sleep(Duration::from_millis(600));
    // Whether the field was reaped while waiting or is found expired now,
    // its expiry is told before the new value.
    assert_eq!(
        c.cmd(&["HSETEX", "h", "FIELDS", "1", "e", "new"]),
        Reply::Int(1)
    );
    assert_eq!(c.cmd(&["HSET", "h", "e", "x"]), Reply::Int(0));
    assert_eq!(c.cmd(&["SET", "done", "1"]), Reply::Ok);
    assert_eq!(events(&mut sub, "done"), ["hexpired h", "hset h", "hset h"]);
}

#[test]
fn fields_expired_by_time_are_reaped_and_told() {
    let (_env, mut c, mut sub) = notifying();
    assert_eq!(
        c.cmd(&["HSETEX", "h", "PX", "100", "FIELDS", "2", "a", "1", "b", "2"]),
        Reply::Int(1)
    );
    thread::sleep(Duration::from_millis(800));
    assert_eq!(c.cmd(&["SET", "done", "1"]), Reply::Ok);
    assert_eq!(
        events(&mut sub, "done"),
        ["hset h", "hexpire h", "hexpired h", "del h"]
    );
    assert_eq!(c.cmd(&["EXISTS", "h"]), Reply::Int(0));
}

fn frame(kind: &str, channel: &[u8], count: i64) -> Vec<u8> {
    let mut out = format!("*3\r\n${}\r\n{kind}\r\n", kind.len()).into_bytes();
    out.extend(format!("${}\r\n", channel.len()).bytes());
    out.extend_from_slice(channel);
    out.extend(format!("\r\n:{count}\r\n").bytes());
    out
}

fn smessage(channel: &[u8], payload: &[u8]) -> Vec<u8> {
    encoded(&Reply::Array(vec![
        Reply::Bulk(b"smessage".to_vec()),
        Reply::Bulk(channel.to_vec()),
        Reply::Bulk(payload.to_vec()),
    ]))
}

#[test]
fn reset_and_disconnect_drop_shard_subscriptions() {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let mut a = env.client();
    let mut c = env.client();
    let subscribed = frame("ssubscribe", b"ch", 1);
    a.send(&["SSUBSCRIBE", "ch"]);
    assert_eq!(read_bytes(&mut a, subscribed.len()), subscribed);
    assert_eq!(c.cmd(&["SPUBLISH", "ch", "m"]), Reply::Int(1));
    let want = smessage(b"ch", b"m");
    assert_eq!(read_bytes(&mut a, want.len()), want);
    assert_eq!(a.cmd(&["RESET"]), Reply::Simple("RESET".into()));
    assert_eq!(c.cmd(&["SPUBLISH", "ch", "m"]), Reply::Int(0));
    assert_eq!(
        c.cmd(&["PUBSUB", "SHARDNUMSUB", "ch"]),
        Reply::Array(vec![Reply::Bulk(b"ch".to_vec()), Reply::Int(0)])
    );
    // A second subscriber, gone with its connection.
    let mut a2 = env.client();
    a2.send(&["SSUBSCRIBE", "ch"]);
    assert_eq!(read_bytes(&mut a2, subscribed.len()), subscribed);
    drop(a2);
    let none = Reply::Array(Vec::new());
    assert!(soon(|| c.cmd(&["PUBSUB", "SHARDCHANNELS"]) == none));
}

/// Whether `check` holds within a second; a closed connection is noticed
/// by the server a little later.
fn soon(mut check: impl FnMut() -> bool) -> bool {
    (0..100).any(|_| {
        let held = check();
        if !held {
            thread::sleep(Duration::from_millis(10));
        }
        held
    })
}

#[test]
fn plain_and_shard_messages_never_cross() {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let (mut a, mut b, mut c, mut d) = (env.client(), env.client(), env.client(), env.client());
    a.send(&["SUBSCRIBE", "ch"]);
    let want = frame("subscribe", b"ch", 1);
    assert_eq!(read_bytes(&mut a, want.len()), want);
    assert_eq!(c.cmd(&["SPUBLISH", "ch", "y"]), Reply::Int(0));
    b.send(&["PSUBSCRIBE", "*"]);
    let want = frame("psubscribe", b"*", 1);
    assert_eq!(read_bytes(&mut b, want.len()), want);
    assert_eq!(c.cmd(&["SPUBLISH", "ch", "y"]), Reply::Int(0));
    assert!(a.silent_for(150) && b.silent_for(50));
    d.send(&["SSUBSCRIBE", "ch2"]);
    let want = frame("ssubscribe", b"ch2", 1);
    assert_eq!(read_bytes(&mut d, want.len()), want);
    // The pattern hears it; the shard subscriber does not.
    assert_eq!(c.cmd(&["PUBLISH", "ch2", "z"]), Reply::Int(1));
    assert_eq!(
        b.read(),
        Reply::Array(
            ["pmessage", "*", "ch2", "z"]
                .map(|s| Reply::Bulk(s.into()))
                .to_vec()
        )
    );
    assert!(d.silent_for(150));
}

#[test]
fn ssubscribe_queued_in_multi_is_refused_when_exec_runs() {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let mut a = env.client();
    assert_eq!(a.cmd(&["MULTI"]), Reply::Ok);
    let queued = Reply::Simple("QUEUED".into());
    assert_eq!(a.cmd(&["SSUBSCRIBE", "ch"]), queued);
    assert_eq!(a.cmd(&["SPUBLISH", "ch", "m"]), queued);
    assert_eq!(
        a.cmd(&["EXEC"]),
        Reply::Array(vec![
            Reply::err("ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client"),
            Reply::Int(0),
        ])
    );
    // Nothing was subscribed, so every command still runs.
    assert_eq!(a.cmd(&["GET", "x"]), Reply::Nil);
    assert_eq!(
        a.cmd(&["PUBSUB", "SHARDCHANNELS"]),
        Reply::Array(Vec::new())
    );
}

#[test]
fn a_shard_message_reaches_every_subscriber_still_there() {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let mut subs = [env.client(), env.client(), env.client()];
    let mut p = env.client();
    let want = frame("ssubscribe", b"ch", 1);
    for s in &mut subs {
        s.send(&["SSUBSCRIBE", "ch"]);
        assert_eq!(read_bytes(s, want.len()), want);
    }
    assert_eq!(p.cmd(&["SPUBLISH", "ch", "m"]), Reply::Int(3));
    let message = smessage(b"ch", b"m");
    for s in &mut subs {
        assert_eq!(read_bytes(s, message.len()), message);
    }
    let [_a, _b, c] = subs;
    drop(c);
    assert!(soon(|| p.cmd(&["SPUBLISH", "ch", "m2"]) == Reply::Int(2)));
}

#[test]
fn shard_channels_and_messages_are_binary_safe() {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let mut a = env.client();
    let mut b = env.client();
    a.send(&["SSUBSCRIBE", ""]);
    let want = frame("ssubscribe", b"", 1);
    assert_eq!(read_bytes(&mut a, want.len()), want);
    assert_eq!(b.cmd(&["SPUBLISH", "", ""]), Reply::Int(1));
    let want = smessage(b"", b"");
    assert_eq!(read_bytes(&mut a, want.len()), want);
    a.send(&["SSUBSCRIBE", "a b"]);
    let want = frame("ssubscribe", b"a b", 2);
    assert_eq!(read_bytes(&mut a, want.len()), want);
    assert_eq!(b.cmd(&["SPUBLISH", "a b", "x\r\ny"]), Reply::Int(1));
    let want = smessage(b"a b", b"x\r\ny");
    assert_eq!(read_bytes(&mut a, want.len()), want);
}

#[test]
fn info_and_client_list_count_shard_channels() {
    let env = Env::new(engine_cfg(4), server_cfg(2, 64));
    let mut a = env.client();
    let mut c = env.client();
    a.send(&["SSUBSCRIBE", "s1", "s2"]);
    a.send(&["SUBSCRIBE", "c1"]);
    let mut want = frame("ssubscribe", b"s1", 1);
    want.extend(frame("ssubscribe", b"s2", 2));
    want.extend(frame("subscribe", b"c1", 1));
    assert_eq!(read_bytes(&mut a, want.len()), want);
    let Reply::Bulk(info) = c.cmd(&["INFO", "stats"]) else {
        panic!("INFO");
    };
    let info = String::from_utf8(info).unwrap();
    for line in [
        "pubsub_channels:1",
        "pubsub_patterns:0",
        "pubsubshard_channels:2",
    ] {
        assert!(info.lines().any(|l| l == line), "{line} in {info}");
    }
    let Reply::Bulk(list) = c.cmd(&["CLIENT", "LIST"]) else {
        panic!("CLIENT LIST");
    };
    let list = String::from_utf8(list).unwrap();
    let row = list
        .lines()
        .find(|row| row.contains(" ssub=2"))
        .unwrap_or_else(|| panic!("{list}"));
    for field in ["sub=1", "psub=0", "ssub=2", "flags=P"] {
        assert!(row.split(' ').any(|f| f == field), "{field} in {row}");
    }
}
