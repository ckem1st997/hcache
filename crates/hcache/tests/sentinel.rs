//! What a Sentinel needs of the nodes it watches: the role change it sends
//! as one transaction (REPLICAOF, CONFIG REWRITE, CLIENT KILL), the
//! coordinated failover (CLIENT PAUSE and FAILOVER in one), INFO with a run
//! ID and by section, the settings it reads, and CLIENT PAUSE.

mod net;

use hcache::Reply;
use net::{engine_cfg, server_cfg, wait_until, Client, Env};
use std::time::{Duration, Instant};

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn text(reply: Reply) -> String {
    match reply {
        Reply::Bulk(b) => String::from_utf8(b).expect("utf-8"),
        other => panic!("not a bulk string: {other:?}"),
    }
}

fn field(info: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}:");
    info.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()).map(str::to_string))
}

fn info_field(c: &mut Client, section: &str, name: &str) -> Option<String> {
    field(&text(c.cmd(&["INFO", section])), name)
}

fn link_up(c: &mut Client) -> bool {
    info_field(c, "replication", "master_link_status").as_deref() == Some("up")
}

fn follow(replica: &mut Client, primary: &Env) {
    let port = primary.addr.port().to_string();
    assert_eq!(replica.cmd(&["REPLICAOF", "127.0.0.1", &port]), Reply::Ok);
    assert!(
        wait_until(15, || link_up(replica)),
        "the link never came up"
    );
}

fn role(c: &mut Client) -> Reply {
    let Reply::Array(role) = c.cmd(&["ROLE"]) else {
        panic!("ROLE")
    };
    role[0].clone()
}

fn error(reply: Reply) -> String {
    match reply {
        Reply::Error(e) => e,
        other => panic!("not an error: {other:?}"),
    }
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

fn queued() -> Reply {
    Reply::Simple("QUEUED".into())
}

const NO_CONFIG_FILE: &str = "ERR The server is running without a config file";

/// The block Sentinel sends to change a node's role (`sentinelSendReplicaOf`).
fn role_change(c: &mut Client, target: &[&str]) -> Reply {
    ok(c.cmd(&["MULTI"]));
    let mut slaveof = vec!["SLAVEOF"];
    slaveof.extend_from_slice(target);
    for args in [
        &slaveof[..],
        &["CONFIG", "REWRITE"],
        &["CLIENT", "KILL", "TYPE", "normal"],
        &["CLIENT", "KILL", "TYPE", "pubsub"],
    ] {
        assert_eq!(c.cmd(args), queued(), "{args:?}");
    }
    c.cmd(&["EXEC"])
}

#[test]
fn a_sentinels_role_change_runs_inside_a_transaction() {
    let (primary, replica) = (env(), env());
    let mut p = primary.client();
    let mut r = replica.client();
    ok(p.cmd(&["SET", "k", "1"]));
    follow(&mut r, &primary);
    // Clients of the replica the change closes: one plain, one listening.
    let mut plain = replica.client();
    assert_eq!(plain.cmd(&["PING"]), Reply::Simple("PONG".into()));
    let mut listening = replica.client();
    listening.cmd(&["SUBSCRIBE", "news"]);

    // Promote the replica.
    assert_eq!(
        role_change(&mut r, &["NO", "ONE"]),
        Reply::Array(vec![
            Reply::Ok,
            Reply::err(NO_CONFIG_FILE),
            Reply::Int(1),
            Reply::Int(1),
        ])
    );
    assert_eq!(role(&mut r), Reply::bulk("master"));
    ok(r.cmd(&["SET", "k", "2"]));
    assert!(plain.read_until_closed().is_empty());
    assert!(listening.read_until_closed().is_empty());

    // Point the old primary at the new one.
    let port = replica.addr.port().to_string();
    assert_eq!(
        role_change(&mut p, &["127.0.0.1", &port]),
        Reply::Array(vec![
            Reply::Ok,
            Reply::err(NO_CONFIG_FILE),
            Reply::Int(0),
            Reply::Int(0),
        ])
    );
    assert_eq!(role(&mut p), Reply::bulk("slave"));
    assert!(wait_until(15, || link_up(&mut p)), "the link never came up");
    assert!(wait_until(15, || p.cmd(&["GET", "k"]) == Reply::bulk("2")));
    assert!(error(p.cmd(&["SET", "k", "3"])).starts_with("READONLY"));

    // A primary told to stay one answers at once.
    ok(r.cmd(&["MULTI"]));
    assert_eq!(r.cmd(&["SLAVEOF", "NO", "ONE"]), queued());
    assert_eq!(r.cmd(&["EXEC"]), Reply::Array(vec![Reply::Ok]));
}

/// What Sentinel sends both nodes once a coordinated failover has swapped
/// them (`sentinelKillClients`).
fn after_role_change(c: &mut Client) -> Reply {
    ok(c.cmd(&["MULTI"]));
    for args in [
        &["CONFIG", "REWRITE"][..],
        &["CLIENT", "KILL", "TYPE", "normal"],
        &["CLIENT", "KILL", "TYPE", "pubsub"],
        &["CLIENT", "UNPAUSE"],
    ] {
        assert_eq!(c.cmd(args), queued(), "{args:?}");
    }
    c.cmd(&["EXEC"])
}

#[test]
fn a_coordinated_failover_hands_over_inside_a_transaction() {
    let (primary, replica) = (env(), env());
    let mut p = primary.client();
    let mut r = replica.client();
    ok(p.cmd(&["SET", "k", "1"]));
    follow(&mut r, &primary);

    // What `sentinelFailoverTo` sends the primary.
    let port = replica.addr.port().to_string();
    ok(p.cmd(&["MULTI"]));
    assert_eq!(p.cmd(&["CLIENT", "PAUSE", "10000", "WRITE"]), queued());
    assert_eq!(
        p.cmd(&["FAILOVER", "TO", "127.0.0.1", &port, "TIMEOUT", "10000"]),
        queued()
    );
    assert_eq!(p.cmd(&["EXEC"]), Reply::Array(vec![Reply::Ok, Reply::Ok]));
    // PUBLISH waits while the pause lasts, whatever the role by then.
    let mut publisher = primary.client();
    publisher.send(&["PUBLISH", "ch", "m"]);
    assert!(publisher.silent_for(200), "PUBLISH ran during the pause");
    let swapped = wait_until(15, || {
        role(&mut r) == Reply::bulk("master") && role(&mut p) == Reply::bulk("slave")
    });
    assert!(swapped, "the roles did not change");
    // As in Valkey, a replica refuses a write before a pause could hold it.
    assert!(error(p.cmd(&["SET", "k", "2"])).starts_with("READONLY"));

    assert_eq!(
        after_role_change(&mut r),
        Reply::Array(vec![
            Reply::err(NO_CONFIG_FILE),
            Reply::Int(0),
            Reply::Int(0),
            Reply::Ok,
        ])
    );
    // The client the pause held is closed before the pause ends (its
    // worker may still answer it in between).
    assert_eq!(
        after_role_change(&mut p),
        Reply::Array(vec![
            Reply::err(NO_CONFIG_FILE),
            Reply::Int(1),
            Reply::Int(0),
            Reply::Ok,
        ])
    );
    let held = publisher.read_until_closed();
    assert!(held.iter().all(|reply| *reply == Reply::Int(0)), "{held:?}");
    assert!(error(p.cmd(&["SET", "k", "3"])).starts_with("READONLY"));
    ok(r.cmd(&["SET", "k", "4"]));
    assert!(wait_until(15, || p.cmd(&["GET", "k"]) == Reply::bulk("4")));
}

#[test]
fn client_commands_in_a_transaction_act_in_their_place() {
    let s = env();
    let mut c = s.client();
    let mut other = s.client();
    ok(c.cmd(&["MULTI"]));
    for args in [
        &["CLIENT", "SETNAME", "first"][..],
        &["SET", "a", "1"],
        &["CLIENT", "GETNAME"],
        &["CLIENT", "SETNAME", "second"],
        &["GET", "a"],
    ] {
        assert_eq!(c.cmd(args), queued(), "{args:?}");
    }
    assert_eq!(
        c.cmd(&["EXEC"]),
        Reply::Array(vec![
            Reply::Ok,
            Reply::Ok,
            Reply::bulk("first"),
            Reply::Ok,
            Reply::bulk("1"),
        ])
    );
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]), Reply::bulk("second"));

    // A watched key that changed: nothing runs, not even the CLIENT part.
    ok(c.cmd(&["WATCH", "w"]));
    ok(other.cmd(&["SET", "w", "1"]));
    ok(c.cmd(&["MULTI"]));
    assert_eq!(c.cmd(&["CLIENT", "SETNAME", "third"]), queued());
    assert_eq!(c.cmd(&["CLIENT", "KILL", "TYPE", "normal"]), queued());
    assert_eq!(c.cmd(&["EXEC"]), Reply::NilArray);
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]), Reply::bulk("second"));
    assert_eq!(other.cmd(&["PING"]), Reply::Simple("PONG".into()));

    // CLIENT KILL leaves the caller alone unless told otherwise.
    ok(c.cmd(&["MULTI"]));
    assert_eq!(c.cmd(&["CLIENT", "KILL", "TYPE", "normal"]), queued());
    assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![Reply::Int(1)]));
    assert!(other.read_until_closed().is_empty());
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));

    // Commands a transaction cannot hold are still refused when queued.
    ok(c.cmd(&["MULTI"]));
    assert!(
        error(c.cmd(&["CLIENT", "TRACKING", "ON"])).contains("not allowed inside a transaction")
    );
    assert!(error(c.cmd(&["EXEC"])).starts_with("EXECABORT"));
}

#[test]
fn info_has_a_run_id_and_gives_the_sections_asked_for() {
    let (s, t) = (env(), env());
    let mut c = s.client();
    let info = text(c.cmd(&["INFO"]));
    let run_id = field(&info, "run_id").expect("run_id");
    assert_eq!(run_id.len(), 40);
    assert!(run_id.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(info_field(&mut c, "server", "run_id"), Some(run_id.clone()));
    assert_ne!(
        info_field(&mut t.client(), "server", "run_id"),
        Some(run_id)
    );
    assert_eq!(info.matches("# Memory\r\n").count(), 1);
    for section in [
        "# Server",
        "# Replication",
        "# Clients",
        "# Stats",
        "# Memory",
        "# Keyspace",
    ] {
        assert!(info.contains(section), "{section}");
    }

    let server = text(c.cmd(&["INFO", "SERVER"]));
    assert!(server.starts_with("# Server\r\n"));
    assert!(!server.contains("# Replication"));
    let two = text(c.cmd(&["INFO", "replication", "memory"]));
    assert!(two.starts_with("# Replication\r\n"));
    assert!(two.contains("\r\n# Memory\r\nused_memory:"));
    assert!(two.contains("used_memory_human:"));
    assert!(!two.contains("# Server"));
    assert_eq!(text(c.cmd(&["INFO", "nosuch"])), "");
    for whole in ["default", "all", "everything"] {
        let got = text(c.cmd(&["INFO", whole]));
        assert!(
            got.contains("# Server") && got.contains("# Keyspace"),
            "{whole}"
        );
    }
}

#[test]
fn a_sentinel_reads_the_replica_settings() {
    let (primary, replica) = (env(), env());
    let mut r = replica.client();
    assert_eq!(
        r.cmd(&["CONFIG", "GET", "*priority"]),
        Reply::Array(vec![
            Reply::bulk("replica-priority"),
            Reply::bulk("100"),
            Reply::bulk("slave-priority"),
            Reply::bulk("100"),
        ])
    );
    ok(r.cmd(&["CONFIG", "SET", "slave-priority", "0"]));
    ok(r.cmd(&["CONFIG", "SET", "replica-announced", "no"]));
    assert_eq!(
        r.cmd(&["CONFIG", "GET", "replica-priority"]),
        Reply::Array(vec![Reply::bulk("replica-priority"), Reply::bulk("0")])
    );
    assert_eq!(
        r.cmd(&["CONFIG", "GET", "replica-announced"]),
        Reply::Array(vec![Reply::bulk("replica-announced"), Reply::bulk("no")])
    );
    for (name, bad) in [
        ("replica-priority", "-1"),
        ("replica-priority", "2147483648"),
        ("replica-priority", "x"),
        ("replica-announced", "maybe"),
    ] {
        assert!(error(r.cmd(&["CONFIG", "SET", name, bad])).starts_with("ERR CONFIG SET failed"));
    }
    follow(&mut r, &primary);
    assert_eq!(
        info_field(&mut r, "replication", "slave_priority").as_deref(),
        Some("0")
    );
    assert_eq!(
        info_field(&mut r, "replication", "replica_announced").as_deref(),
        Some("0")
    );
}

#[test]
fn client_pause_holds_writes_or_everything_until_it_ends() {
    let s = env();
    let mut admin = s.client();
    let mut writer = s.client();
    let mut reader = s.client();
    let mut publisher = s.client();

    ok(admin.cmd(&["CLIENT", "PAUSE", "100000", "WRITE"]));
    writer.send(&["SET", "k", "v"]);
    assert!(writer.silent_for(300), "a write ran during the pause");
    // PUBLISH waits as well: the replicas get it.
    publisher.send(&["PUBLISH", "ch", "m"]);
    assert!(publisher.silent_for(100), "PUBLISH ran during the pause");
    assert_eq!(reader.cmd(&["GET", "k"]), Reply::Nil);
    ok(admin.cmd(&["CLIENT", "UNPAUSE"]));
    assert_eq!(writer.read(), Reply::Ok);
    assert_eq!(publisher.read(), Reply::Int(0));

    // A pause ends by itself.
    ok(admin.cmd(&["CLIENT", "PAUSE", "300", "WRITE"]));
    let started = Instant::now();
    ok(writer.cmd(&["SET", "k", "w"]));
    assert!(started.elapsed() >= Duration::from_millis(200));

    // ALL, the default, holds reads as well; CLIENT UNPAUSE still runs.
    ok(admin.cmd(&["CLIENT", "PAUSE", "100000"]));
    reader.send(&["GET", "k"]);
    assert!(reader.silent_for(300), "a read ran during the pause");
    ok(admin.cmd(&["CLIENT", "UNPAUSE"]));
    assert_eq!(reader.read(), Reply::bulk("w"));

    for (args, error) in [
        (
            &["CLIENT", "PAUSE", "x"][..],
            "ERR timeout is not an integer or out of range",
        ),
        (&["CLIENT", "PAUSE", "-1"], "ERR timeout is negative"),
        (
            &["CLIENT", "PAUSE", "10", "SOME"],
            "ERR CLIENT PAUSE mode must be WRITE or ALL",
        ),
        (&["CLIENT", "PAUSE", "10", "WRITE", "x"], "ERR syntax error"),
        (
            &["CLIENT", "PAUSE"],
            "ERR wrong number of arguments for 'client|pause' command",
        ),
    ] {
        assert_eq!(admin.cmd(args), Reply::err(error), "{args:?}");
    }
    ok(writer.cmd(&["SET", "k", "x"]));
}

#[test]
fn config_rewrite_says_there_is_no_file() {
    let s = env();
    let mut c = s.client();
    assert_eq!(c.cmd(&["CONFIG", "REWRITE"]), Reply::err(NO_CONFIG_FILE));
}
