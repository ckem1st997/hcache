//! Replication between HCache nodes: a replica gets the data and every
//! write after it, goes on from where it was when its link breaks, refuses
//! writes of its own, and takes them again once it is the primary.

mod net;

use hcache::Reply;
use net::{engine_cfg, read_reply, server_cfg, wait_until, Client, Env};
use std::io::{BufRead, Read};

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn text(reply: Reply) -> String {
    match reply {
        Reply::Bulk(b) => String::from_utf8(b).expect("utf-8"),
        other => panic!("not a bulk string: {other:?}"),
    }
}

/// One field of INFO.
fn field(c: &mut Client, section: &str, name: &str) -> Option<String> {
    let info = text(c.cmd(&["INFO", section]));
    let prefix = format!("{name}:");
    info.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()).map(str::to_string))
}

fn offset(c: &mut Client) -> u64 {
    field(c, "replication", "master_repl_offset")
        .and_then(|n| n.parse().ok())
        .expect("master_repl_offset")
}

fn link_up(c: &mut Client) -> bool {
    field(c, "replication", "master_link_status").as_deref() == Some("up")
}

fn follow(replica: &mut Client, primary: &Env) {
    let port = primary.addr.port().to_string();
    assert_eq!(replica.cmd(&["REPLICAOF", "127.0.0.1", &port]), Reply::Ok);
    assert!(
        wait_until(15, || link_up(replica)),
        "the link never came up"
    );
}

/// The replica has everything the primary has sent.
fn caught_up(primary: &mut Client, replica: &mut Client) {
    let synced = wait_until(15, || offset(primary) == offset(replica));
    assert!(synced, "the replica did not catch up");
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

#[test]
fn a_replica_gets_the_data_and_every_write_after_it() {
    let (primary, replica) = (env(), env());
    let mut p = primary.client();
    ok(p.cmd(&["SET", "before", "1"]));
    p.cmd(&["HSET", "h", "a", "1", "b", "2"]);
    ok(p.cmd(&["SELECT", "2"]));
    ok(p.cmd(&["SET", "in2", "x"]));
    ok(p.cmd(&["SELECT", "0"]));
    let mut r = replica.client();
    ok(r.cmd(&["SET", "mine", "gone after the sync"]));
    follow(&mut r, &primary);

    ok(p.cmd(&["SET", "after", "2"]));
    p.cmd(&["INCR", "n"]);
    p.cmd(&["RPUSH", "l", "a", "b", "c"]);
    p.cmd(&["EXPIRE", "before", "1000"]);
    ok(p.cmd(&["SET", "short", "v", "PX", "50"]));
    p.cmd(&["EVAL", "redis.call('SET', KEYS[1], 'lua')", "1", "scripted"]);
    caught_up(&mut p, &mut r);
    assert_eq!(r.cmd(&["GET", "before"]), Reply::bulk("1"));
    assert_eq!(r.cmd(&["GET", "after"]), Reply::bulk("2"));
    assert_eq!(r.cmd(&["GET", "n"]), Reply::bulk("1"));
    assert_eq!(r.cmd(&["HGET", "h", "b"]), Reply::bulk("2"));
    assert_eq!(r.cmd(&["LLEN", "l"]), Reply::Int(3));
    assert_eq!(r.cmd(&["GET", "scripted"]), Reply::bulk("lua"));
    assert_eq!(r.cmd(&["EXISTS", "mine"]), Reply::Int(0));
    assert!(matches!(r.cmd(&["TTL", "before"]), Reply::Int(t) if t > 900));
    ok(r.cmd(&["SELECT", "2"]));
    assert_eq!(r.cmd(&["GET", "in2"]), Reply::bulk("x"));
    ok(r.cmd(&["SELECT", "0"]));
    // The primary deletes what expires, and the replica follows.
    let same = wait_until(10, || p.cmd(&["DBSIZE"]) == r.cmd(&["DBSIZE"]));
    assert!(same, "the expired key stayed on the replica");

    // A replica takes no writes of its own.
    let refused = r.cmd(&["SET", "x", "1"]);
    assert!(
        matches!(&refused, Reply::Error(e) if e.starts_with("READONLY")),
        "{refused:?}"
    );
    let script = r.cmd(&["EVAL", "return redis.call('SET', 'x', '1')", "0"]);
    assert!(matches!(script, Reply::Error(_)), "{script:?}");
    assert_eq!(
        r.cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "after"]),
        Reply::bulk("2")
    );

    // What each side says of the other.
    assert_eq!(
        field(&mut p, "replication", "role").as_deref(),
        Some("master")
    );
    assert_eq!(
        field(&mut p, "replication", "connected_slaves").as_deref(),
        Some("1")
    );
    assert_eq!(
        field(&mut r, "replication", "role").as_deref(),
        Some("slave")
    );
    let port = primary.addr.port().to_string();
    assert_eq!(
        field(&mut r, "replication", "master_port").as_deref(),
        Some(port.as_str())
    );
    let Reply::Array(role) = r.cmd(&["ROLE"]) else {
        panic!("ROLE")
    };
    assert_eq!(role[0], Reply::bulk("slave"));
    assert_eq!(role[3], Reply::bulk("connected"));
    let Reply::Array(role) = p.cmd(&["ROLE"]) else {
        panic!("ROLE")
    };
    assert_eq!(role[0], Reply::bulk("master"));
    let Reply::Array(replicas) = &role[2] else {
        panic!("ROLE replicas")
    };
    let mine = replica.addr.port().to_string();
    assert!(
        matches!(&replicas[..], [Reply::Array(r)] if r[1] == Reply::bulk(mine.as_str())),
        "{replicas:?}"
    );

    // WAIT counts the replica once it has the write.
    ok(p.cmd(&["SET", "w", "1"]));
    assert_eq!(p.cmd(&["WAIT", "1", "5000"]), Reply::Int(1));
    assert_eq!(p.cmd(&["WAIT", "0", "0"]), Reply::Int(1));
    // More than there are: it waits out its time.
    assert_eq!(p.cmd(&["WAIT", "2", "100"]), Reply::Int(1));
    let refused = r.cmd(&["WAIT", "1", "10"]);
    assert!(matches!(refused, Reply::Error(_)), "{refused:?}");

    // Promoted, it takes writes again.
    ok(r.cmd(&["REPLICAOF", "NO", "ONE"]));
    ok(r.cmd(&["SET", "x", "1"]));
    assert_eq!(
        field(&mut r, "replication", "role").as_deref(),
        Some("master")
    );
    let gone = wait_until(10, || {
        field(&mut p, "replication", "connected_slaves").as_deref() == Some("0")
    });
    assert!(gone, "the primary still lists the replica");
}

#[test]
fn a_broken_link_goes_on_from_where_it_was() {
    let (primary, replica) = (env(), env());
    let mut p = primary.client();
    let mut r = replica.client();
    follow(&mut r, &primary);
    for i in 0..100 {
        ok(p.cmd(&["SET", &format!("k{i}"), "v"]));
    }
    caught_up(&mut p, &mut r);
    assert_eq!(r.cmd(&["CLIENT", "KILL", "TYPE", "master"]), Reply::Int(1));
    ok(p.cmd(&["SET", "meanwhile", "1"]));
    assert!(
        wait_until(15, || link_up(&mut r)),
        "the link did not come back"
    );
    caught_up(&mut p, &mut r);
    assert_eq!(r.cmd(&["GET", "meanwhile"]), Reply::bulk("1"));
    assert_eq!(r.cmd(&["DBSIZE"]), Reply::Int(101));
    let partial = wait_until(15, || {
        field(&mut p, "stats", "sync_partial_ok").as_deref() == Some("1")
    });
    assert!(partial, "the replica did not go on from where it was");
    assert_eq!(field(&mut p, "stats", "sync_full").as_deref(), Some("1"));
    // The primary's side of the link can go too.
    assert_eq!(p.cmd(&["CLIENT", "KILL", "TYPE", "replica"]), Reply::Int(1));
    ok(p.cmd(&["SET", "again", "1"]));
    assert!(
        wait_until(15, || link_up(&mut r)),
        "the link did not come back"
    );
    caught_up(&mut p, &mut r);
    assert_eq!(r.cmd(&["GET", "again"]), Reply::bulk("1"));
}

#[test]
fn a_replica_of_a_replica_follows_the_first() {
    let (a, b, c) = (env(), env(), env());
    let (mut ca, mut cb, mut cc) = (a.client(), b.client(), c.client());
    ok(ca.cmd(&["SET", "first", "1"]));
    follow(&mut cb, &a);
    follow(&mut cc, &b);
    ok(ca.cmd(&["SET", "second", "2"]));
    caught_up(&mut ca, &mut cb);
    caught_up(&mut cb, &mut cc);
    assert_eq!(cc.cmd(&["GET", "first"]), Reply::bulk("1"));
    assert_eq!(cc.cmd(&["GET", "second"]), Reply::bulk("2"));
    // The middle one is promoted: the last goes on with it.
    ok(cb.cmd(&["REPLICAOF", "NO", "ONE"]));
    ok(cb.cmd(&["SET", "third", "3"]));
    caught_up(&mut cb, &mut cc);
    assert_eq!(cc.cmd(&["GET", "third"]), Reply::bulk("3"));
}

#[test]
fn a_replica_logs_in_at_a_primary_with_a_password() {
    let (primary, replica) = (env(), env());
    let mut p = primary.client();
    ok(p.cmd(&["CONFIG", "SET", "requirepass", "s3cret"]));
    ok(p.cmd(&["AUTH", "s3cret"]));
    ok(p.cmd(&["SET", "k", "v"]));
    let mut r = replica.client();
    ok(r.cmd(&["CONFIG", "SET", "masterauth", "s3cret"]));
    follow(&mut r, &primary);
    caught_up(&mut p, &mut r);
    assert_eq!(r.cmd(&["GET", "k"]), Reply::bulk("v"));
    // The password is never shown.
    assert_eq!(
        r.cmd(&["CONFIG", "GET", "masterauth"]),
        Reply::Array(vec![Reply::bulk("masterauth"), Reply::bulk("")])
    );
}

/// `$<length>\r\n` and the file that follows.
fn read_file(c: &mut Client) -> Vec<u8> {
    let mut line = Vec::new();
    // A primary may send empty lines while it makes the file.
    while line.is_empty() || line == b"\n" {
        line.clear();
        c.rd.read_until(b'\n', &mut line).expect("header");
    }
    let len: usize = std::str::from_utf8(&line[1..line.len() - 2])
        .expect("utf-8")
        .parse()
        .expect("length");
    assert_eq!(line[0], b'$');
    let mut file = vec![0; len];
    c.rd.read_exact(&mut file).expect("the file");
    file
}

#[test]
fn sync_hands_out_the_file_and_then_the_stream() {
    let primary = env();
    let mut p = primary.client();
    ok(p.cmd(&["SET", "k", "v"]));

    // REPLCONF rdb-only: the file, then the end of the link.
    let mut once = primary.client();
    ok(once.cmd(&["REPLCONF", "rdb-only", "1"]));
    once.send(&["SYNC"]);
    let file = read_file(&mut once);
    assert!(file.starts_with(b"REDIS"), "{:?}", &file[..9]);
    assert!(once.read_until_closed().is_empty());

    let mut sync = primary.client();
    sync.send(&["SYNC"]);
    let file = read_file(&mut sync);
    assert!(file.starts_with(b"REDIS"));
    ok(p.cmd(&["SET", "later", "1"]));
    // SELECT, then the write as it was sent.
    let mut got = Vec::new();
    while !got.contains(&Reply::bulk("later")) {
        let Reply::Array(args) = read_reply(&mut sync.rd).expect("the stream") else {
            panic!("not a command")
        };
        got = args;
    }
    assert_eq!(got[0], Reply::bulk("SET"));

    // What a replica says of itself, and what is not understood.
    let mut c = primary.client();
    ok(c.cmd(&[
        "REPLCONF",
        "listening-port",
        "6380",
        "capa",
        "eof",
        "capa",
        "psync2",
    ]));
    let refused = c.cmd(&["REPLCONF", "nonsense", "1"]);
    assert!(
        matches!(&refused, Reply::Error(e) if e.contains("Unrecognized REPLCONF option")),
        "{refused:?}"
    );
    assert!(matches!(c.cmd(&["REPLCONF", "capa"]), Reply::Error(_)));
    // ACK from a connection that is no replica has no reply.
    c.send(&["REPLCONF", "ACK", "10"]);
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
}

#[test]
fn psync_failover_makes_the_replica_the_primary() {
    let (primary, replica) = (env(), env());
    let mut p = primary.client();
    let mut r = replica.client();
    follow(&mut r, &primary);
    ok(p.cmd(&["SET", "k", "v"]));
    caught_up(&mut p, &mut r);
    let replid = field(&mut r, "replication", "master_replid").expect("replid");
    let next = (offset(&mut r) + 1).to_string();
    // Only for the replica's own history.
    let refused = r.cmd(&["PSYNC", &"0".repeat(40), &next, "FAILOVER"]);
    assert!(
        matches!(&refused, Reply::Error(e) if e.contains("must match my replid")),
        "{refused:?}"
    );
    let mut old = replica.client();
    old.send(&["PSYNC", &replid, &next, "FAILOVER"]);
    let Reply::Simple(answer) = old.read() else {
        panic!("PSYNC FAILOVER")
    };
    assert!(answer.starts_with("CONTINUE "), "{answer}");
    assert_eq!(
        field(&mut r, "replication", "role").as_deref(),
        Some("master")
    );
    ok(r.cmd(&["SET", "now", "mine"]));
    let refused = p.cmd(&["PSYNC", &replid, &next, "FAILOVER"]);
    assert!(
        matches!(&refused, Reply::Error(e) if e.contains("can't be sent to a master")),
        "{refused:?}"
    );
}

fn refused(reply: Reply, says: &str) {
    assert!(
        matches!(&reply, Reply::Error(e) if e.contains(says)),
        "{reply:?}"
    );
}

fn role(c: &mut Client) -> Option<String> {
    field(c, "replication", "role")
}

fn failover_state(c: &mut Client) -> Option<String> {
    field(c, "replication", "master_failover_state")
}

#[test]
fn failover_hands_the_primary_role_to_a_replica() {
    let (primary, replica, alone) = (env(), env(), env());
    let mut p = primary.client();
    let mut r = replica.client();
    // Checked as Valkey checks it.
    refused(
        alone.client().cmd(&["FAILOVER"]),
        "requires connected replicas",
    );
    refused(p.cmd(&["FAILOVER", "ABORT"]), "No failover in progress");
    refused(p.cmd(&["FAILOVER", "TIMEOUT", "0"]), "greater than 0");
    refused(p.cmd(&["FAILOVER", "TIMEOUT", "x"]), "not an integer");
    refused(p.cmd(&["FAILOVER", "ABORT", "FORCE"]), "syntax error");
    follow(&mut r, &primary);
    refused(r.cmd(&["FAILOVER"]), "not valid when server is a replica");
    refused(
        p.cmd(&["FAILOVER", "TO", "127.0.0.1", "1"]),
        "is not a replica",
    );
    refused(
        p.cmd(&["FAILOVER", "FORCE"]),
        "requires both a timeout and target",
    );
    for i in 0..100 {
        ok(p.cmd(&["SET", &format!("k{i}"), "v"]));
    }
    let mut blocked = primary.client();
    blocked.send(&["BLPOP", "nothing", "0"]);
    let waits = wait_until(15, || {
        field(&mut p, "clients", "blocked_clients").as_deref() == Some("1")
    });
    assert!(waits, "BLPOP did not block");

    let port = replica.addr.port().to_string();
    ok(p.cmd(&["FAILOVER", "TO", "127.0.0.1", &port]));
    let swapped = wait_until(15, || {
        role(&mut r).as_deref() == Some("master") && role(&mut p).as_deref() == Some("slave")
    });
    assert!(swapped, "the roles did not change");
    // A connection waiting to write is told it no longer can.
    let Reply::Error(e) = blocked.read() else {
        panic!("BLPOP was not released")
    };
    assert!(e.starts_with("UNBLOCKED"), "{e}");
    assert!(
        wait_until(15, || link_up(&mut p)),
        "no link to the new primary"
    );
    assert_eq!(failover_state(&mut p).as_deref(), Some("no-failover"));
    refused(p.cmd(&["SET", "k", "v"]), "READONLY");
    ok(r.cmd(&["SET", "after", "1"]));
    caught_up(&mut r, &mut p);
    assert_eq!(p.cmd(&["GET", "after"]), Reply::bulk("1"));
    assert_eq!(p.cmd(&["DBSIZE"]), Reply::Int(101));
    // The new primary went on from its old primary's history.
    assert_eq!(
        field(&mut r, "stats", "sync_partial_ok").as_deref(),
        Some("1")
    );
}

#[test]
fn a_failover_waits_for_its_replica_and_can_be_called_off() {
    let primary = env();
    let mut p = primary.client();
    ok(p.cmd(&["SET", "k", "v"]));
    // A replica that never says how far it got.
    let mut mute = primary.client();
    ok(mute.cmd(&["REPLCONF", "listening-port", "1"]));
    mute.send(&["SYNC"]);
    read_file(&mut mute);
    let online = wait_until(15, || {
        field(&mut p, "replication", "slave0").is_some_and(|s| s.contains("state=online"))
    });
    assert!(online, "the replica is not online");

    ok(p.cmd(&["FAILOVER"]));
    assert_eq!(failover_state(&mut p).as_deref(), Some("waiting-for-sync"));
    refused(p.cmd(&["FAILOVER"]), "already in progress");
    refused(
        p.cmd(&["REPLICAOF", "NO", "ONE"]),
        "not allowed while failing over",
    );
    // Writes wait; reads do not.
    let mut writer = primary.client();
    writer.send(&["SET", "k", "w"]);
    assert_eq!(p.cmd(&["GET", "k"]), Reply::bulk("v"));
    ok(p.cmd(&["FAILOVER", "ABORT"]));
    assert_eq!(writer.read(), Reply::Ok);
    assert_eq!(p.cmd(&["GET", "k"]), Reply::bulk("w"));
    assert_eq!(failover_state(&mut p).as_deref(), Some("no-failover"));

    // Without FORCE, running out of time calls it off.
    ok(p.cmd(&["FAILOVER", "TIMEOUT", "200"]));
    let off = wait_until(15, || {
        failover_state(&mut p).as_deref() == Some("no-failover")
    });
    assert!(off, "the failover did not time out");
    assert_eq!(role(&mut p).as_deref(), Some("master"));
    ok(p.cmd(&["SET", "k", "x"]));

    // With FORCE it hands over anyway: here to a node that is not there,
    // until it is called off.
    ok(p.cmd(&[
        "FAILOVER",
        "TO",
        "127.0.0.1",
        "1",
        "TIMEOUT",
        "200",
        "FORCE",
    ]));
    let handing = wait_until(15, || {
        failover_state(&mut p).as_deref() == Some("failover-in-progress")
    });
    assert!(handing, "FORCE did not hand over");
    assert_eq!(role(&mut p).as_deref(), Some("slave"));
    ok(p.cmd(&["FAILOVER", "ABORT"]));
    let back = wait_until(15, || role(&mut p).as_deref() == Some("master"));
    assert!(back, "the primary did not take its role back");
    ok(p.cmd(&["SET", "k", "y"]));
    assert_eq!(failover_state(&mut p).as_deref(), Some("no-failover"));
}
