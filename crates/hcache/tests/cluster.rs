//! The one-node cluster (`Config::cluster`) and the server commands of
//! Valkey 9 beside it: CLUSTER, CROSSSLOT, ASKING, READONLY, ROLE,
//! COMMANDLOG, DEBUG, SHUTDOWN and LOLWUT.

mod net;

use hcache::{Config, Reply};
use hcache_core::engine::Protected;
use hcache_server::OnShutdown;
use net::*;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const CROSSSLOT: &str = "CROSSSLOT Keys in request don't hash to the same slot";
const DISABLED: &str = "ERR This instance has cluster support disabled";

fn clustered() -> Env {
    Env::new(
        Config {
            cluster: true,
            databases: 1,
            ..engine_cfg(8)
        },
        server_cfg(2, 64),
    )
}

fn bulk_text(reply: &Reply) -> String {
    match reply {
        Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("not a bulk string: {other:?}"),
    }
}

/// Every bulk string in a reply, however deep.
fn strings(reply: &Reply, out: &mut Vec<String>) {
    match reply {
        Reply::Bulk(_) => out.push(bulk_text(reply)),
        Reply::Array(items) => items.iter().for_each(|item| strings(item, out)),
        _ => {}
    }
}

fn err(text: &str) -> Reply {
    Reply::Error(text.into())
}

#[test]
fn keys_of_one_command_must_share_a_slot() {
    let env = clustered();
    let mut c = env.client();
    assert_eq!(c.cmd(&["MSET", "foo", "1", "bar", "2"]), err(CROSSSLOT));
    assert_eq!(c.cmd(&["MSET", "{u}a", "1", "{u}b", "2"]), Reply::Ok);
    assert_eq!(
        c.cmd(&["MGET", "{u}a", "{u}b"]),
        Reply::Array(vec![Reply::Bulk(b"1".to_vec()), Reply::Bulk(b"2".to_vec())])
    );
    assert_eq!(c.cmd(&["DEL", "foo", "bar"]), err(CROSSSLOT));
    assert_eq!(c.cmd(&["SUNIONSTORE", "{u}d", "{u}s", "x"]), err(CROSSSLOT));
    assert_eq!(
        c.cmd(&["EVAL", "return 1", "2", "foo", "bar"]),
        err(CROSSSLOT)
    );
    assert_eq!(c.cmd(&["WATCH", "foo", "bar"]), err(CROSSSLOT));
    assert_eq!(c.cmd(&["SSUBSCRIBE", "foo", "bar"]), err(CROSSSLOT));
    // Keyless commands go anywhere.
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(2));
}

#[test]
fn a_transaction_must_keep_to_one_slot() {
    let env = clustered();
    let mut c = env.client();
    let queued = Reply::Simple("QUEUED".into());
    // Each command alone is fine; together they are not.
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    assert_eq!(c.cmd(&["SET", "foo", "1"]), queued);
    assert_eq!(c.cmd(&["PING"]), queued);
    assert_eq!(c.cmd(&["SET", "bar", "1"]), queued);
    assert_eq!(c.cmd(&["EXEC"]), err(CROSSSLOT));
    assert_eq!(c.cmd(&["EXISTS", "foo"]), Reply::Int(0));
    // A command refused on its own spoils the transaction.
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    assert_eq!(c.cmd(&["MGET", "foo", "bar"]), err(CROSSSLOT));
    assert_eq!(
        c.cmd(&["EXEC"]),
        err("EXECABORT Transaction discarded because of previous errors.")
    );
    // One slot throughout runs.
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    assert_eq!(c.cmd(&["SET", "{t}1", "a"]), queued);
    assert_eq!(c.cmd(&["GET", "{t}1"]), queued);
    assert_eq!(
        c.cmd(&["EXEC"]),
        Reply::Array(vec![Reply::Ok, Reply::Bulk(b"a".to_vec())])
    );
}

#[test]
fn a_cluster_has_one_database_and_no_swapdb() {
    let env = clustered();
    let mut c = env.client();
    assert_eq!(c.cmd(&["SELECT", "0"]), Reply::Ok);
    assert_eq!(c.cmd(&["SELECT", "1"]), err("ERR DB index is out of range"));
    assert_eq!(
        c.cmd(&["SWAPDB", "0", "0"]),
        err("ERR SWAPDB is not allowed in cluster mode")
    );
    assert_eq!(c.cmd(&["SET", "k", "v"]), Reply::Ok);
    assert_eq!(
        c.cmd(&["MOVE", "k", "1"]),
        err("ERR DB index is out of range")
    );
}

#[test]
fn cluster_commands_describe_one_node_with_every_slot() {
    let env = clustered();
    let mut c = env.client();
    let id = bulk_text(&c.cmd(&["CLUSTER", "MYID"]));
    assert_eq!(id.len(), 40);
    assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));
    let shard = bulk_text(&c.cmd(&["CLUSTER", "MYSHARDID"]));
    assert_eq!(shard.len(), 40);
    assert_ne!(id, shard);

    let info = bulk_text(&c.cmd(&["CLUSTER", "INFO"]));
    for line in [
        "cluster_state:ok",
        "cluster_slots_assigned:16384",
        "cluster_slots_ok:16384",
        "cluster_known_nodes:1",
        "cluster_size:1",
    ] {
        assert!(info.contains(&format!("{line}\r\n")), "{line} in {info}");
    }

    let port = env.addr.port();
    assert_eq!(
        bulk_text(&c.cmd(&["CLUSTER", "NODES"])),
        format!(
            "{id} :{port}@{} myself,master - 0 0 0 connected 0-16383\n",
            u32::from(port) + 10000
        )
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "SLOTS"]),
        Reply::Array(vec![Reply::Array(vec![
            Reply::Int(0),
            Reply::Int(16383),
            Reply::Array(vec![
                Reply::Bulk(Vec::new()),
                Reply::Int(port.into()),
                Reply::Bulk(id.clone().into_bytes()),
                Reply::Array(Vec::new()),
            ]),
        ])])
    );
    let Reply::Array(shards) = c.cmd(&["CLUSTER", "SHARDS"]) else {
        panic!("SHARDS is an array");
    };
    assert_eq!(shards.len(), 1);
    let mut texts = Vec::new();
    strings(&shards[0], &mut texts);
    assert!(texts.contains(&id) && texts.contains(&shard), "{texts:?}");

    assert_eq!(c.cmd(&["CLUSTER", "KEYSLOT", "foo"]), Reply::Int(12182));
    assert_eq!(
        c.cmd(&["CLUSTER", "KEYSLOT", "{user1000}.following"]),
        c.cmd(&["CLUSTER", "KEYSLOT", "user1000"])
    );
    assert_eq!(c.cmd(&["CLUSTER", "LINKS"]), Reply::Array(Vec::new()));
    assert_eq!(
        c.cmd(&["CLUSTER", "REPLICAS", &id]),
        Reply::Array(Vec::new())
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "REPLICAS", "nope"]),
        err("ERR Unknown node nope")
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "ADDSLOTS", "5"]),
        err("ERR Slot 5 is already busy")
    );
    let Reply::Array(help) = c.cmd(&["CLUSTER", "HELP"]) else {
        panic!("HELP is an array");
    };
    assert_eq!(
        help.first(),
        Some(&Reply::Simple(
            "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".into()
        ))
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "KEYSLOT"]),
        err("ERR wrong number of arguments for 'cluster|keyslot' command")
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "NOPE"]),
        err("ERR unknown subcommand 'NOPE'. Try CLUSTER HELP.")
    );
}

#[test]
fn keys_are_counted_and_listed_by_slot() {
    let env = clustered();
    let mut c = env.client();
    for i in 0..30 {
        let key = format!("{{tag}}{i}");
        assert_eq!(c.cmd(&["SET", key.as_str(), "v"]), Reply::Ok);
    }
    assert_eq!(c.cmd(&["SET", "foo", "v"]), Reply::Ok);
    let slot = match c.cmd(&["CLUSTER", "KEYSLOT", "tag"]) {
        Reply::Int(slot) => slot.to_string(),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        c.cmd(&["CLUSTER", "COUNTKEYSINSLOT", slot.as_str()]),
        Reply::Int(30)
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "COUNTKEYSINSLOT", "12182"]),
        Reply::Int(1)
    );
    assert_eq!(c.cmd(&["CLUSTER", "COUNTKEYSINSLOT", "0"]), Reply::Int(0));
    let Reply::Array(keys) = c.cmd(&["CLUSTER", "GETKEYSINSLOT", slot.as_str(), "7"]) else {
        panic!("GETKEYSINSLOT is an array");
    };
    assert_eq!(keys.len(), 7);
    for key in &keys {
        assert!(bulk_text(key).starts_with("{tag}"), "{key:?}");
    }
    assert_eq!(
        c.cmd(&["CLUSTER", "GETKEYSINSLOT", "12182", "10"]),
        Reply::Array(vec![Reply::Bulk(b"foo".to_vec())])
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "COUNTKEYSINSLOT", "16384"]),
        err("ERR Invalid slot")
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "GETKEYSINSLOT", "1", "-1"]),
        err("ERR Invalid slot or number of keys")
    );
    assert_eq!(
        c.cmd(&["CLUSTER", "COUNTKEYSINSLOT", "x"]),
        err("ERR value is not an integer or out of range")
    );
}

#[test]
fn the_node_says_it_is_a_cluster() {
    let env = clustered();
    let mut c = env.client();
    let info = bulk_text(&c.cmd(&["INFO"]));
    assert!(info.contains("redis_mode:cluster\r\n"), "{info}");
    assert!(info.contains("cluster_enabled:1\r\n"), "{info}");
    let Reply::Array(hello) = c.cmd(&["HELLO", "2"]) else {
        panic!("HELLO is an array");
    };
    let mode = hello
        .chunks(2)
        .find(|pair| pair[0] == Reply::Bulk(b"mode".to_vec()))
        .map(|pair| pair[1].clone());
    assert_eq!(mode, Some(Reply::Bulk(b"cluster".to_vec())));
    assert_eq!(c.cmd(&["ASKING"]), Reply::Ok);
    assert_eq!(c.cmd(&["READONLY"]), Reply::Ok);
    assert_eq!(c.cmd(&["READWRITE"]), Reply::Ok);
}

#[test]
fn without_cluster_mode_cluster_commands_are_refused() {
    let env = Env::new(engine_cfg(4), server_cfg(1, 64));
    let mut c = env.client();
    assert_eq!(c.cmd(&["CLUSTER", "INFO"]), err(DISABLED));
    assert_eq!(c.cmd(&["CLUSTER", "KEYSLOT", "foo"]), err(DISABLED));
    assert_eq!(c.cmd(&["CLUSTER", "COUNTKEYSINSLOT", "x"]), err(DISABLED));
    assert_eq!(c.cmd(&["ASKING"]), err(DISABLED));
    // Argument counts are checked first, as Valkey does.
    assert_eq!(
        c.cmd(&["CLUSTER", "KEYSLOT"]),
        err("ERR wrong number of arguments for 'cluster|keyslot' command")
    );
    assert_eq!(c.cmd(&["READONLY"]), Reply::Ok);
    assert_eq!(c.cmd(&["MSET", "foo", "1", "bar", "2"]), Reply::Ok);
    let info = bulk_text(&c.cmd(&["INFO"]));
    assert!(info.contains("cluster_enabled:0\r\n"), "{info}");
}

#[test]
fn role_is_a_primary_without_replicas() {
    let env = Env::new(engine_cfg(4), server_cfg(1, 64));
    let mut c = env.client();
    assert_eq!(
        c.cmd(&["ROLE"]),
        Reply::Array(vec![
            Reply::Bulk(b"master".to_vec()),
            Reply::Int(0),
            Reply::Array(Vec::new())
        ])
    );
}

#[test]
fn commandlog_keeps_large_requests_and_replies() {
    let env = Env::new(engine_cfg(4), server_cfg(1, 64));
    let mut c = env.client();
    for (setting, value) in [
        ("commandlog-request-larger-than", "100"),
        ("commandlog-reply-larger-than", "100"),
        ("slowlog-log-slower-than", "-1"),
    ] {
        assert_eq!(c.cmd(&["CONFIG", "SET", setting, value]), Reply::Ok);
    }
    let big = "x".repeat(200);
    assert_eq!(c.cmd(&["SET", "big", big.as_str()]), Reply::Ok);
    assert_eq!(c.cmd(&["SET", "small", "v"]), Reply::Ok);
    assert_eq!(
        c.cmd(&["GET", "big"]),
        Reply::Bulk(big.clone().into_bytes())
    );
    assert_eq!(
        c.cmd(&["COMMANDLOG", "LEN", "large-request"]),
        Reply::Int(1)
    );
    assert_eq!(c.cmd(&["COMMANDLOG", "LEN", "large-reply"]), Reply::Int(1));
    assert_eq!(c.cmd(&["COMMANDLOG", "LEN", "slow"]), Reply::Int(0));
    let Reply::Array(entries) = c.cmd(&["COMMANDLOG", "GET", "-1", "large-request"]) else {
        panic!("GET is an array");
    };
    let Reply::Array(entry) = &entries[0] else {
        panic!("an entry is an array");
    };
    assert_eq!(entry.len(), 6);
    assert!(
        matches!(entry[2], Reply::Int(bytes) if bytes > 200),
        "{entry:?}"
    );
    let Reply::Array(args) = &entry[3] else {
        panic!("the arguments are an array");
    };
    assert_eq!(args[0], Reply::Bulk(b"SET".to_vec()));
    let Reply::Array(entries) = c.cmd(&["COMMANDLOG", "GET", "1", "large-reply"]) else {
        panic!("GET is an array");
    };
    let Reply::Array(entry) = &entries[0] else {
        panic!("an entry is an array");
    };
    assert!(
        matches!(entry[2], Reply::Int(bytes) if bytes > 200),
        "{entry:?}"
    );
    assert_eq!(c.cmd(&["COMMANDLOG", "RESET", "large-reply"]), Reply::Ok);
    assert_eq!(c.cmd(&["COMMANDLOG", "LEN", "large-reply"]), Reply::Int(0));
    assert_eq!(
        c.cmd(&["COMMANDLOG", "LEN", "nope"]),
        err("ERR type should be one of the following: slow, large-request, large-reply")
    );
    assert_eq!(
        c.cmd(&["COMMANDLOG", "GET", "-2", "slow"]),
        err("ERR count should be greater than or equal to -1")
    );
    assert_eq!(
        c.cmd(&["COMMANDLOG", "GET", "1"]),
        err("ERR wrong number of arguments for 'commandlog|get' command")
    );
    // The slow log is COMMANDLOG's first; Valkey's names reach it too.
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "commandlog-execution-slower-than"]),
        Reply::Array(vec![
            Reply::Bulk(b"commandlog-execution-slower-than".to_vec()),
            Reply::Bulk(b"-1".to_vec())
        ])
    );
}

#[test]
fn debug_is_refused_unless_enabled() {
    let env = Env::new(engine_cfg(4), server_cfg(1, 64));
    let mut c = env.client();
    let Reply::Error(e) = c.cmd(&["DEBUG", "SLEEP", "0"]) else {
        panic!("DEBUG must be refused");
    };
    assert!(e.starts_with("ERR DEBUG command not allowed."), "{e}");
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    assert!(matches!(c.cmd(&["DEBUG", "SLEEP", "0"]), Reply::Error(_)));
    assert!(matches!(c.cmd(&["EXEC"]), Reply::Error(e) if e.starts_with("EXECABORT")));

    let env = Env::new(
        Config {
            debug_command: Protected::Local,
            ..engine_cfg(4)
        },
        server_cfg(1, 64),
    );
    let mut c = env.client();
    assert_eq!(c.cmd(&["DEBUG", "SLEEP", "0.01"]), Reply::Ok);
    assert_eq!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "0"]), Reply::Ok);
    assert_eq!(c.cmd(&["SET", "gone", "v", "PX", "1"]), Reply::Ok);
    std::thread::sleep(Duration::from_millis(300));
    // Not swept: still counted, though a read would see it gone.
    assert_eq!(c.cmd(&["DBSIZE"]), Reply::Int(1));
    assert_eq!(c.cmd(&["DEBUG", "SET-ACTIVE-EXPIRE", "1"]), Reply::Ok);
    assert!(wait_until(5, || c.cmd(&["DBSIZE"]) == Reply::Int(0)));
    assert!(matches!(c.cmd(&["DEBUG", "NOPE"]), Reply::Error(e) if e.contains("DEBUG HELP")));
}

#[test]
fn shutdown_stops_through_the_hook_and_closes_the_connection() {
    let (tx, rx) = mpsc::channel();
    let tx = std::sync::Mutex::new(tx);
    let mut scfg = server_cfg(1, 64);
    scfg.on_shutdown = Some(OnShutdown(Arc::new(move || {
        let _ = tx.lock().map(|tx| tx.send(()));
    })));
    let env = Env::new(engine_cfg(4), scfg);
    let mut c = env.client();
    assert_eq!(
        c.cmd(&["SHUTDOWN", "NOSAVE", "SAVE"]),
        err("ERR syntax error")
    );
    assert_eq!(
        c.cmd(&["SHUTDOWN", "ABORT", "NOW"]),
        err("ERR syntax error")
    );
    assert_eq!(
        c.cmd(&["SHUTDOWN", "ABORT"]),
        err("ERR No shutdown in progress.")
    );
    assert_eq!(
        c.cmd(&["SHUTDOWN", "FAILOVER"]),
        err("ERR SHUTDOWN FAILOVER is only supported in cluster mode.")
    );
    assert_eq!(c.cmd(&["MULTI"]), Reply::Ok);
    assert_eq!(
        c.cmd(&["SHUTDOWN"]),
        err("ERR Command not allowed inside a transaction")
    );
    assert!(matches!(c.cmd(&["DISCARD"]), Reply::Ok));
    assert!(rx.try_recv().is_err());
    c.send(&["SHUTDOWN", "NOW"]);
    assert!(c.read_until_closed().is_empty());
    assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok());

    // Without a hook there is nothing to stop.
    let env = Env::new(engine_cfg(4), server_cfg(1, 64));
    let mut c = env.client();
    assert_eq!(
        c.cmd(&["SHUTDOWN"]),
        err("ERR Errors trying to SHUTDOWN. Check logs.")
    );
}

#[test]
fn lolwut_shows_the_version() {
    let env = Env::new(engine_cfg(4), server_cfg(1, 64));
    let mut c = env.client();
    let text = bulk_text(&c.cmd(&["LOLWUT"]));
    assert!(
        text.starts_with("HCache ver. ") && text.ends_with('\n'),
        "{text}"
    );
    assert_eq!(
        bulk_text(&c.cmd(&["LOLWUT", "VERSION", "5", "1", "2"])),
        text
    );
    assert_eq!(
        c.cmd(&["LOLWUT", "VERSION", "x"]),
        err("ERR value is not an integer or out of range")
    );
}
