//! Commands about the connection itself: CLIENT, RESET, and the parts of
//! INFO a server adds.

mod net;

use hcache::Reply;
use net::*;

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn text(reply: Reply) -> String {
    match reply {
        Reply::Bulk(b) => String::from_utf8(b).unwrap(),
        other => panic!("{other:?}"),
    }
}

fn field(info: &str, name: &str) -> String {
    info.lines()
        .find_map(|line| line.strip_prefix(&format!("{name}:")))
        .unwrap_or_else(|| panic!("no {name} in {info}"))
        .to_string()
}

#[test]
fn clients_have_ids_names_and_a_list() {
    let env = env();
    let mut a = env.client();
    let mut b = env.client();
    let Reply::Int(id_a) = a.cmd(&["CLIENT", "ID"]) else {
        panic!("no id");
    };
    let Reply::Int(id_b) = b.cmd(&["CLIENT", "ID"]) else {
        panic!("no id");
    };
    assert!(id_a > 0 && id_b > id_a);
    // A later connection has a larger id, whichever worker serves it.
    let mut later: Vec<Client> = (0..16).map(|_| env.client()).collect();
    let ids: Vec<i64> = later
        .iter_mut()
        .map(|c| match c.cmd(&["CLIENT", "ID"]) {
            Reply::Int(id) => id,
            other => panic!("{other:?}"),
        })
        .collect();
    assert!(
        id_b < ids[0] && ids.windows(2).all(|w| w[0] < w[1]),
        "{ids:?}"
    );
    drop(later);
    assert_eq!(env.wait_conns(2, 5), 2);
    assert_eq!(a.cmd(&["CLIENT", "GETNAME"]), Reply::Nil);
    assert_eq!(a.cmd(&["CLIENT", "SETNAME", "worker-1"]), Reply::Ok);
    assert_eq!(a.cmd(&["client", "getname"]), Reply::bulk("worker-1"));
    assert_eq!(b.cmd(&["CLIENT", "GETNAME"]), Reply::Nil);
    assert_eq!(
        a.cmd(&["CLIENT", "SETINFO", "LIB-NAME", "redis-py"]),
        Reply::Ok
    );
    assert_eq!(a.cmd(&["CLIENT", "SETINFO", "lib-ver", "8.1.0"]), Reply::Ok);

    let info = text(a.cmd(&["CLIENT", "INFO"]));
    assert!(
        info.starts_with(&format!("id={id_a} addr=127.0.0.1:")),
        "{info}"
    );
    for part in [
        " name=worker-1 ",
        " resp=2 ",
        " lib-name=redis-py ",
        " lib-ver=8.1.0\n",
        " db=0 ",
    ] {
        assert!(info.contains(part), "{part:?} not in {info}");
    }
    let list = text(b.cmd(&["CLIENT", "LIST"]));
    assert_eq!(list.lines().count(), 2, "{list}");
    assert!(list
        .lines()
        .next()
        .unwrap()
        .starts_with(&format!("id={id_a} ")));
    assert!(list.lines().nth(1).unwrap().contains(" name= "));

    drop(a);
    assert_eq!(env.wait_conns(1, 5), 1);
    let list = text(b.cmd(&["CLIENT", "LIST"]));
    assert_eq!(list.lines().count(), 1, "{list}");
    // An empty name takes the name away.
    b.cmd(&["CLIENT", "SETNAME", "x"]);
    assert_eq!(b.cmd(&["CLIENT", "SETNAME", ""]), Reply::Ok);
    assert_eq!(b.cmd(&["CLIENT", "GETNAME"]), Reply::Nil);
}

#[test]
fn client_subcommands_are_checked() {
    let env = env();
    let mut c = env.client();
    for (args, want) in [
        (
            &["CLIENT", "SETNAME", "two words"][..],
            "ERR Client names cannot contain spaces, newlines or special characters.",
        ),
        (
            &["CLIENT", "SETNAME", "line\nbreak"],
            "ERR Client names cannot contain spaces, newlines or special characters.",
        ),
        (
            &["CLIENT", "SETNAME"],
            "ERR wrong number of arguments for 'client|setname' command",
        ),
        (
            &["CLIENT", "ID", "x"],
            "ERR wrong number of arguments for 'client|id' command",
        ),
        (
            &["CLIENT", "SETINFO", "LIB-NOPE", "x"],
            "ERR Unrecognized option 'LIB-NOPE'",
        ),
        (
            &["CLIENT", "SETINFO", "LIB-NAME", "a b"],
            "ERR lib-name and lib-ver cannot contain spaces, newlines or special characters.",
        ),
        (
            &["CLIENT", "NOPE"],
            "ERR unknown subcommand 'NOPE'. Try CLIENT HELP.",
        ),
        (
            &["CLIENT", "REPLY", "OFF"],
            "ERR CLIENT REPLY is not supported in this form",
        ),
        (
            &["CLIENT", "KILL", "ID", "0"],
            "ERR client-id should be greater than 0",
        ),
        (&["CLIENT", "KILL", "ID"], "ERR No such client"),
        (&["CLIENT", "KILL", "ID", "1", "TYPE"], "ERR syntax error"),
        (&["CLIENT", "KILL", "127.0.0.1:1"], "ERR No such client"),
        (
            &["CLIENT"],
            "ERR wrong number of arguments for 'client' command",
        ),
        (
            &["RESET", "x"],
            "ERR wrong number of arguments for 'reset' command",
        ),
    ] {
        assert_eq!(c.cmd(args), Reply::err(want), "{args:?}");
    }
    for args in [
        &["CLIENT", "NO-EVICT", "ON"][..],
        &["CLIENT", "NO-TOUCH", "off"],
        &["CLIENT", "REPLY", "ON"],
        &["CLIENT", "TRACKING", "OFF"],
        &["CLIENT", "UNPAUSE"],
    ] {
        assert_eq!(c.cmd(args), Reply::Ok, "{args:?}");
    }
    assert_eq!(c.cmd(&["CLIENT", "GETREDIR"]), Reply::Int(-1));
    // A name cannot smuggle a second line into CLIENT LIST.
    assert_eq!(text(c.cmd(&["CLIENT", "LIST"])).lines().count(), 1);
}

#[test]
fn reset_returns_a_connection_to_its_first_state() {
    let env = env();
    let mut c = env.client();
    let mut other = env.client();
    c.cmd(&["CLIENT", "SETNAME", "named"]);
    c.cmd(&["SET", "watched", "1"]);
    c.cmd(&["WATCH", "watched"]);
    c.cmd(&["MULTI"]);
    assert_eq!(
        c.cmd(&["SET", "k", "queued"]),
        Reply::Simple("QUEUED".into())
    );
    assert_eq!(c.cmd(&["RESET"]), Reply::Simple("RESET".into()));
    assert_eq!(c.cmd(&["EXEC"]), Reply::err("ERR EXEC without MULTI"));
    assert_eq!(c.cmd(&["GET", "k"]), Reply::Nil);
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]), Reply::Nil);
    // The watch is gone too: a change by someone else no longer aborts.
    other.cmd(&["SET", "watched", "2"]);
    c.cmd(&["MULTI"]);
    c.cmd(&["SET", "k", "v"]);
    assert_eq!(c.cmd(&["EXEC"]), Reply::Array(vec![Reply::Ok]));

    // Out of subscriber mode, without confirmations.
    c.cmd(&["SUBSCRIBE", "news"]);
    c.cmd(&["PSUBSCRIBE", "n*"]);
    assert_eq!(c.cmd(&["GET", "k"]), Reply::err(
        "ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
    ));
    assert_eq!(c.cmd(&["RESET"]), Reply::Simple("RESET".into()));
    assert_eq!(c.cmd(&["GET", "k"]), Reply::bulk("v"));
    assert_eq!(other.cmd(&["PUBLISH", "news", "x"]), Reply::Int(0));
    assert_eq!(other.cmd(&["PUBSUB", "NUMPAT"]), Reply::Int(0));

    // Back from RESP3 to RESP2: a missing key is `$-1` again.
    c.send(&["HELLO", "3"]);
    c.send(&["RESET"]);
    c.send(&["GET", "nokey"]);
    let mut seen = Vec::new();
    let mut byte = [0u8; 1];
    while !seen.ends_with(b"+RESET\r\n$-1\r\n") {
        std::io::Read::read_exact(&mut c.rd, &mut byte).unwrap();
        seen.push(byte[0]);
        assert!(seen.len() < 4096, "{}", String::from_utf8_lossy(&seen));
    }
}

#[test]
fn info_reports_what_frameworks_read() {
    let env = env();
    let mut c = env.client();
    let mut blocked = env.client();
    blocked.send(&["BLPOP", "nokey", "0"]);
    // A busy machine can take a while to park the BLPOP.
    assert!(wait_until(10, || {
        field(&text(c.cmd(&["INFO"])), "blocked_clients") == "1"
    }));
    for _ in 0..10 {
        c.cmd(&["PING"]);
    }
    let info = text(c.cmd(&["INFO"]));
    let version = field(&info, "redis_version");
    let major: u32 = version.split('.').next().unwrap().parse().unwrap();
    assert!(major >= 7, "{version}");
    assert_eq!(field(&info, "redis_mode"), "standalone");
    assert_eq!(field(&info, "role"), "master");
    assert_eq!(field(&info, "connected_clients"), "2");
    assert_eq!(field(&info, "blocked_clients"), "1");
    assert_eq!(field(&info, "total_connections_received"), "2");
    assert!(
        field(&info, "total_commands_processed")
            .parse::<u64>()
            .unwrap()
            >= 11
    );
    assert!(field(&info, "uptime_in_seconds").parse::<u64>().is_ok());
    assert!(field(&info, "used_memory").parse::<u64>().is_ok());
    assert!(field(&info, "used_memory_human").ends_with('M'));
    assert!(text(c.cmd(&["INFO", "server"])).contains("redis_version:"));
    c.cmd(&["RPUSH", "nokey", "x"]);
    assert_eq!(
        blocked.read(),
        Reply::Array(vec![Reply::bulk("nokey"), Reply::bulk("x")])
    );
    assert_eq!(field(&text(c.cmd(&["INFO"])), "blocked_clients"), "0");
    // Embedded: the engine's own sections, no server ones.
    let session = env.db.session().unwrap();
    let Reply::Bulk(own) = session.execute(&["INFO"]) else {
        panic!("not a bulk");
    };
    assert!(String::from_utf8(own).unwrap().contains("redis_version:"));
    assert_eq!(session.execute(&["CLIENT", "SETNAME", "x"]), Reply::Ok);
    assert_eq!(session.execute(&["CLIENT", "GETNAME"]), Reply::Nil);
    assert_eq!(session.execute(&["RESET"]), Reply::Simple("RESET".into()));
}
