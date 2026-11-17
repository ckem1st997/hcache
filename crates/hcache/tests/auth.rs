//! AUTH, ACL and COMMAND over TCP: what a connection may do before it has
//! logged in, what a user with limits may do, and the ways around both
//! that must stay shut (transactions, scripts, blocking commands, a user
//! changed or deleted under a live connection).

mod net;

use hcache::{Db, Reply};
use net::*;

const PASS: &str = "correct horse battery staple";

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

#[track_caller]
fn refused(reply: Reply, start: &str) {
    match error_text(&reply) {
        Some(text) if text.starts_with(start) => {}
        _ => panic!("expected an error starting with {start:?}, got {reply:?}"),
    }
}

fn text(reply: Reply) -> String {
    match reply {
        Reply::Bulk(b) => String::from_utf8(b).unwrap(),
        Reply::Simple(s) => s,
        other => panic!("not text: {other:?}"),
    }
}

/// An admin connection on a server whose default user needs `PASS`.
fn locked() -> (Env, Client) {
    let env = env();
    let mut admin = env.client();
    ok(admin.cmd(&["CONFIG", "SET", "requirepass", PASS]));
    // The password holds for everyone from now on, its setter included.
    refused(admin.cmd(&["PING"]), "NOAUTH");
    ok(admin.cmd(&["AUTH", PASS]));
    (env, admin)
}

/// A server with the user "alice", and a connection logged in as her.
fn with_alice(rules: &[&str]) -> (Env, Client, Client) {
    let env = env();
    let mut admin = env.client();
    let mut setuser = vec!["ACL", "SETUSER", "alice", "on", ">alice-pw"];
    setuser.extend_from_slice(rules);
    ok(admin.cmd(&setuser));
    let mut alice = env.client();
    ok(alice.cmd(&["AUTH", "alice", "alice-pw"]));
    (env, admin, alice)
}

#[test]
fn a_server_without_a_password_is_open() {
    let env = env();
    let mut c = env.client();
    ok(c.cmd(&["SET", "k", "v"]));
    refused(
        c.cmd(&["AUTH", "anything"]),
        "ERR AUTH <password> called without any password configured",
    );
    // "default" takes any password while it has none, as in Redis.
    ok(c.cmd(&["AUTH", "default", "anything"]));
    refused(c.cmd(&["AUTH", "nobody", "anything"]), "WRONGPASS");
    refused(c.cmd(&["AUTH"]), "ERR wrong number of arguments");
    refused(
        c.cmd(&["AUTH", "a", "b", "c"]),
        "ERR wrong number of arguments",
    );
    assert_eq!(c.cmd(&["GET", "k"]), Reply::bulk("v"));
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "databases"]),
        Reply::Array(vec![Reply::bulk("databases"), Reply::bulk("16")])
    );
}

#[test]
fn nothing_runs_before_auth() {
    let (env, mut admin) = locked();
    ok(admin.cmd(&["SET", "k", "v"]));

    let mut c = env.client();
    let script = "return redis.call('GET', 'k')";
    let attempts: &[&[&str]] = &[
        &["GET", "k"],
        &["get", "k"],
        &["PING"],
        &["MULTI"],
        &["EXEC"],
        &["SUBSCRIBE", "ch"],
        &["EVAL", script, "0"],
        &["CONFIG", "SET", "requirepass", ""],
        &["CONFIG", "GET", "*"],
        &["ACL", "SETUSER", "me", "on", "nopass", "+@all", "~*"],
        &["ACL", "WHOAMI"],
        &["CLIENT", "LIST"],
        &["COMMAND"],
        &["INFO"],
        &["SELECT", "1"],
        &["FLUSHALL"],
        &["BLPOP", "q", "0"],
        &["NOSUCHCOMMAND"],
        &["AUTHX", PASS],
    ];
    for attempt in attempts {
        refused(c.cmd(attempt), "NOAUTH Authentication required.");
    }
    // The same in one write, where a parser slip would show.
    let mut wire = Vec::new();
    for attempt in attempts {
        wire.extend(wire_str(attempt));
    }
    c.send_raw(&wire);
    for _ in attempts {
        refused(c.read(), "NOAUTH");
    }
    refused(c.cmd(&["HELLO", "3"]), "NOAUTH HELLO must be called");
    refused(c.cmd(&["HELLO"]), "NOAUTH HELLO must be called");

    refused(c.cmd(&["AUTH", "wrong"]), "WRONGPASS");
    refused(c.cmd(&["AUTH", ""]), "WRONGPASS");
    refused(c.cmd(&["AUTH", "default", "wrong"]), "WRONGPASS");
    refused(c.cmd(&["AUTH", "nobody", PASS]), "WRONGPASS");
    // A prefix or an extension of the password is not the password.
    refused(c.cmd(&["AUTH", &PASS[..PASS.len() - 1]]), "WRONGPASS");
    refused(c.cmd(&["AUTH", &format!("{PASS}x")]), "WRONGPASS");
    refused(
        c.cmd(&["HELLO", "3", "AUTH", "default", "wrong"]),
        "WRONGPASS",
    );
    // Still nobody, and still RESP2: the failed HELLO changed nothing.
    refused(c.cmd(&["GET", "k"]), "NOAUTH");

    ok(c.cmd(&["AUTH", PASS]));
    assert_eq!(c.cmd(&["GET", "k"]), Reply::bulk("v"));

    // RESET makes it a new connection: logged out again.
    assert_eq!(c.cmd(&["RESET"]), Reply::Simple("RESET".into()));
    refused(c.cmd(&["GET", "k"]), "NOAUTH");
    assert!(matches!(
        c.cmd(&["HELLO", "2", "AUTH", "default", PASS]),
        Reply::Array(_)
    ));
    assert_eq!(c.cmd(&["GET", "k"]), Reply::bulk("v"));

    // QUIT needs no login.
    let mut q = env.client();
    ok(q.cmd(&["QUIT"]));
    assert!(q.read_until_closed().is_empty());

    // The password is not given back.
    assert_eq!(
        admin.cmd(&["CONFIG", "GET", "requirepass"]),
        Reply::Array(vec![Reply::bulk("requirepass"), Reply::bulk("")])
    );
    let users = format!("{:?}", admin.cmd(&["ACL", "LIST"]));
    assert!(!users.contains(PASS), "{users}");
    let default = format!("{:?}", admin.cmd(&["ACL", "GETUSER", "default"]));
    assert!(!default.contains(PASS), "{default}");
}

#[test]
fn the_password_can_change_and_go_while_connections_live() {
    let (env, mut admin) = locked();
    let mut waiting = env.client();
    refused(waiting.cmd(&["PING"]), "NOAUTH");

    ok(admin.cmd(&["CONFIG", "SET", "requirepass", "second"]));
    refused(waiting.cmd(&["AUTH", PASS]), "WRONGPASS");
    // Logged in before the change: stays logged in, as in Redis.
    ok(admin.cmd(&["SET", "k", "v"]));
    ok(waiting.cmd(&["AUTH", "second"]));

    let mut late = env.client();
    refused(late.cmd(&["PING"]), "NOAUTH");
    ok(admin.cmd(&["CONFIG", "SET", "requirepass", ""]));
    // No password any more: the connection that never logged in is in.
    assert_eq!(late.cmd(&["GET", "k"]), Reply::bulk("v"));
    assert_eq!(env.client().cmd(&["GET", "k"]), Reply::bulk("v"));
}

#[test]
fn a_user_runs_only_its_commands_on_its_keys() {
    let (_env, mut admin, mut alice) = with_alice(&[
        "~app:*",
        "+@read",
        "+set",
        "+copy",
        "+blpop",
        "+acl|whoami",
        "+client|info",
    ]);
    ok(admin.cmd(&["SET", "other", "secret"]));
    ok(admin.cmd(&["RPUSH", "otherq", "secret"]).pipe_int());

    assert_eq!(text(alice.cmd(&["ACL", "WHOAMI"])), "alice");
    assert!(text(alice.cmd(&["CLIENT", "INFO"])).contains(" user=alice "));
    ok(alice.cmd(&["SET", "app:x", "1"]));
    assert_eq!(alice.cmd(&["GET", "app:x"]), Reply::bulk("1"));

    let key = "NOPERM No permissions to access the 'other' key";
    refused(alice.cmd(&["GET", "other"]), key);
    refused(alice.cmd(&["gEt", "other"]), key);
    refused(alice.cmd(&["SET", "other", "mine"]), key);
    refused(alice.cmd(&["MGET", "app:x", "other"]), key);
    refused(alice.cmd(&["COPY", "other", "app:stolen"]), key);
    refused(alice.cmd(&["COPY", "app:x", "other"]), key);
    refused(alice.cmd(&["STRLEN", "other"]), key);
    refused(
        alice.cmd(&["BLPOP", "app:q", "otherq", "0"]),
        "NOPERM No permissions to access the 'otherq' key",
    );
    refused(
        alice.cmd(&["BLPOP", "otherq", "1"]),
        "NOPERM No permissions to access the 'otherq' key",
    );

    let command = "NOPERM User alice has no permissions to run the";
    refused(alice.cmd(&["DEL", "app:x"]), command);
    refused(alice.cmd(&["FLUSHALL"]), command);
    refused(alice.cmd(&["FLUSHDB"]), command);
    refused(alice.cmd(&["CONFIG", "SET", "requirepass", "x"]), command);
    refused(alice.cmd(&["CONFIG", "GET", "*"]), command);
    refused(
        alice.cmd(&["ACL", "SETUSER", "alice", "+@all", "~*"]),
        command,
    );
    refused(alice.cmd(&["ACL", "LIST"]), command);
    refused(alice.cmd(&["ACL", "DELUSER", "alice"]), command);
    refused(alice.cmd(&["CLIENT", "LIST"]), command);
    refused(alice.cmd(&["EVAL", "return 1", "0"]), command);
    refused(alice.cmd(&["MULTI"]), command);
    refused(alice.cmd(&["SUBSCRIBE", "ch"]), command);
    refused(alice.cmd(&["MOVE", "app:x", "1"]), command);
    refused(alice.cmd(&["RENAME", "app:x", "app:y"]), command);

    // Nothing leaked and nothing changed.
    assert_eq!(admin.cmd(&["GET", "other"]), Reply::bulk("secret"));
    assert_eq!(admin.cmd(&["LLEN", "otherq"]), Reply::Int(1));
    assert_eq!(admin.cmd(&["EXISTS", "app:stolen"]), Reply::Int(0));
    assert_eq!(admin.cmd(&["GET", "app:x"]), Reply::bulk("1"));

    // She can always become someone else, or try to.
    refused(alice.cmd(&["AUTH", "alice", "wrong"]), "WRONGPASS");
    assert_eq!(text(alice.cmd(&["ACL", "WHOAMI"])), "alice");
    ok(alice.cmd(&["AUTH", "default", "x"]));
    assert_eq!(alice.cmd(&["GET", "other"]), Reply::bulk("secret"));
}

trait PipeInt {
    fn pipe_int(self) -> Reply;
}

impl PipeInt for Reply {
    /// An integer reply as the `OK` the helper `ok` expects.
    fn pipe_int(self) -> Reply {
        match self {
            Reply::Int(_) => Reply::Ok,
            other => other,
        }
    }
}

#[test]
fn a_transaction_is_no_way_around_the_limits() {
    let (_env, mut admin, mut alice) = with_alice(&[
        "~app:*", "+get", "+set", "+multi", "+exec", "+discard", "+watch",
    ]);
    ok(admin.cmd(&["SET", "other", "secret"]));

    ok(alice.cmd(&["MULTI"]));
    assert_eq!(
        alice.cmd(&["SET", "app:a", "1"]),
        Reply::Simple("QUEUED".into())
    );
    refused(
        alice.cmd(&["GET", "other"]),
        "NOPERM No permissions to access the 'other' key",
    );
    refused(alice.cmd(&["EXEC"]), "EXECABORT");
    assert_eq!(admin.cmd(&["EXISTS", "app:a"]), Reply::Int(0));

    ok(alice.cmd(&["MULTI"]));
    refused(alice.cmd(&["DEL", "app:a"]), "NOPERM User alice");
    refused(alice.cmd(&["EXEC"]), "EXECABORT");

    refused(
        alice.cmd(&["WATCH", "other"]),
        "NOPERM No permissions to access the 'other' key",
    );
    ok(alice.cmd(&["WATCH", "app:a"]));
    ok(alice.cmd(&["MULTI"]));
    assert_eq!(
        alice.cmd(&["SET", "app:a", "2"]),
        Reply::Simple("QUEUED".into())
    );
    assert_eq!(alice.cmd(&["EXEC"]), Reply::Array(vec![Reply::Ok]));
    assert_eq!(admin.cmd(&["GET", "app:a"]), Reply::bulk("2"));
    assert_eq!(admin.cmd(&["GET", "other"]), Reply::bulk("secret"));
}

#[test]
fn a_script_may_do_what_its_caller_may() {
    let (_env, mut admin, mut alice) = with_alice(&[
        "~app:*", "&news:*", "+get", "+set", "+eval", "+evalsha", "+publish", "+multi", "+exec",
    ]);
    ok(admin.cmd(&["SET", "other", "secret"]));
    ok(alice.cmd(&["SET", "app:x", "1"]));

    let eval = |c: &mut Client, script: &str, keys: &[&str]| {
        let n = keys.len().to_string();
        let mut args = vec!["EVAL", script, &n];
        args.extend_from_slice(keys);
        c.cmd(&args)
    };
    assert_eq!(eval(&mut alice, S_GET, &["app:x"]), Reply::bulk("1"));
    // A key she may not touch, declared.
    refused(
        eval(&mut alice, S_GET, &["other"]),
        "NOPERM No permissions to access the 'other' key",
    );
    // The same key, not declared: only the script knows it.
    let reply = eval(&mut alice, "return redis.call('GET', 'other')", &[]);
    assert!(
        error_text(&reply).is_some_and(|e| e.contains("NOPERM")),
        "{reply:?}"
    );
    let reply = eval(&mut alice, "return redis.call('get', 'ot' .. 'her')", &[]);
    assert!(
        error_text(&reply).is_some_and(|e| e.contains("NOPERM")),
        "{reply:?}"
    );
    // pcall hands the refusal to the script; it still holds no data.
    let reply = eval(
        &mut alice,
        "local r = redis.pcall('GET', 'other'); return r.err or r",
        &[],
    );
    assert!(text(reply).contains("NOPERM"));
    // A command she may not run, on a key she may touch.
    let reply = eval(&mut alice, "return redis.call('DEL', KEYS[1])", &["app:x"]);
    assert!(
        error_text(&reply).is_some_and(|e| e.contains("NOPERM")),
        "{reply:?}"
    );
    let reply = eval(&mut alice, "return redis.call('FLUSHALL')", &[]);
    assert!(reply != Reply::Ok, "{reply:?}");
    // A channel she may not publish to.
    let reply = eval(
        &mut alice,
        "return redis.call('PUBLISH', 'secret', 'x')",
        &[],
    );
    assert!(
        error_text(&reply).is_some_and(|e| e.contains("NOPERM")),
        "{reply:?}"
    );
    assert_eq!(
        eval(
            &mut alice,
            "return redis.call('PUBLISH', 'news:1', 'x')",
            &[]
        ),
        Reply::Int(0)
    );
    // Inside a transaction the script is still hers.
    ok(alice.cmd(&["MULTI"]));
    alice.cmd(&["EVAL", "return redis.call('GET', 'other')", "0"]);
    let Reply::Array(results) = alice.cmd(&["EXEC"]) else {
        panic!("no EXEC reply");
    };
    assert!(
        error_text(&results[0]).is_some_and(|e| e.contains("NOPERM")),
        "{results:?}"
    );

    assert_eq!(admin.cmd(&["GET", "app:x"]), Reply::bulk("1"));
    assert_eq!(admin.cmd(&["GET", "other"]), Reply::bulk("secret"));
    // The limits were alice's, not the server's: the next script of an
    // unlimited user runs free.
    assert_eq!(
        eval(&mut admin, "return redis.call('GET', 'other')", &[]),
        Reply::bulk("secret")
    );
}

#[test]
fn channels_follow_the_users_patterns() {
    let (env, _admin, mut alice) = with_alice(&[
        "resetchannels",
        "&news:*",
        "+subscribe",
        "+psubscribe",
        "+publish",
    ]);
    refused(
        alice.cmd(&["PUBLISH", "secret", "x"]),
        "NOPERM No permissions to access the 'secret' channel",
    );
    assert_eq!(alice.cmd(&["PUBLISH", "news:1", "x"]), Reply::Int(0));

    let mut sub = env.client();
    ok(sub.cmd(&["AUTH", "alice", "alice-pw"]));
    refused(
        sub.cmd(&["SUBSCRIBE", "news:1", "secret"]),
        "NOPERM No permissions to access the 'secret' channel",
    );
    // A pattern wider than hers is refused; her own pattern is fine.
    refused(sub.cmd(&["PSUBSCRIBE", "*"]), "NOPERM");
    refused(sub.cmd(&["PSUBSCRIBE", "news:*", "s*"]), "NOPERM");
    assert!(matches!(
        sub.cmd(&["PSUBSCRIBE", "news:*"]),
        Reply::Array(_)
    ));
    assert_eq!(alice.cmd(&["PUBLISH", "news:2", "hello"]), Reply::Int(1));
    let Reply::Array(message) = sub.read() else {
        panic!("no message");
    };
    assert_eq!(message.last(), Some(&Reply::bulk("hello")));
}

#[test]
fn changes_to_a_user_reach_its_live_connections() {
    let (env, mut admin, mut alice) = with_alice(&["~*", "+get", "+set"]);
    ok(alice.cmd(&["SET", "k", "v"]));

    ok(admin.cmd(&["ACL", "SETUSER", "alice", "-set"]));
    refused(alice.cmd(&["SET", "k", "w"]), "NOPERM User alice");
    assert_eq!(alice.cmd(&["GET", "k"]), Reply::bulk("v"));

    ok(admin.cmd(&["ACL", "SETUSER", "alice", "resetkeys", "~mine:*"]));
    refused(
        alice.cmd(&["GET", "k"]),
        "NOPERM No permissions to access the 'k' key",
    );

    // Off: her connection can do nothing, and nobody logs in as her.
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "off", "~*", "+@all"]));
    refused(alice.cmd(&["GET", "k"]), "NOPERM");
    refused(
        env.client().cmd(&["AUTH", "alice", "alice-pw"]),
        "WRONGPASS",
    );
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "on"]));
    assert_eq!(alice.cmd(&["GET", "k"]), Reply::bulk("v"));

    // A new password: the old one is gone only when it is removed.
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "resetpass", ">new-pw"]));
    refused(
        env.client().cmd(&["AUTH", "alice", "alice-pw"]),
        "WRONGPASS",
    );
    ok(env.client().cmd(&["AUTH", "alice", "new-pw"]));

    // Deleted: the connection goes with the user.
    assert_eq!(admin.cmd(&["ACL", "DELUSER", "alice"]), Reply::Int(1));
    assert!(alice.read_until_closed().is_empty());
    refused(env.client().cmd(&["AUTH", "alice", "new-pw"]), "WRONGPASS");
    // A user of the same name made later is a new user for new logins.
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "on", ">third", "+ping"]));
    let mut again = env.client();
    ok(again.cmd(&["AUTH", "alice", "third"]));
    refused(again.cmd(&["GET", "k"]), "NOPERM");

    refused(admin.cmd(&["ACL", "DELUSER", "default"]), "ERR");
    assert_eq!(admin.cmd(&["GET", "k"]), Reply::bulk("v"));
}

#[test]
fn a_blocked_user_that_loses_its_rights_gets_nothing_more() {
    let (_env, mut admin, mut alice) = with_alice(&["~app:*", "+blpop", "+ping"]);
    alice.send(&["BLPOP", "app:q", "0"]);
    assert!(alice.silent_for(200));
    // Already waiting when the right goes: Redis serves such a client too.
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "-blpop"]));
    admin.cmd(&["RPUSH", "app:q", "one"]);
    assert_eq!(
        alice.read(),
        Reply::Array(vec![Reply::bulk("app:q"), Reply::bulk("one")])
    );
    // The next one is refused.
    admin.cmd(&["RPUSH", "app:q", "two"]);
    refused(alice.cmd(&["BLPOP", "app:q", "0"]), "NOPERM");
    assert_eq!(admin.cmd(&["LLEN", "app:q"]), Reply::Int(1));
}

#[test]
fn acl_setuser_refuses_what_it_does_not_understand() {
    let env = env();
    let mut c = env.client();
    refused(c.cmd(&["ACL", "SETUSER", "bob", "+nosuchcommand"]), "ERR");
    refused(c.cmd(&["ACL", "SETUSER", "bob", "+@nosuchcategory"]), "ERR");
    refused(c.cmd(&["ACL", "SETUSER", "bob", "garbage"]), "ERR");
    refused(c.cmd(&["ACL", "SETUSER", "bad name"]), "ERR");
    refused(c.cmd(&["ACL", "SETUSER", ""]), "ERR");
    // A refused SETUSER made no user, not even an empty one.
    assert_eq!(c.cmd(&["ACL", "GETUSER", "bob"]), Reply::Nil);
    refused(c.cmd(&["ACL"]), "ERR wrong number of arguments");
    refused(c.cmd(&["ACL", "NOSUCH"]), "ERR");

    // A new user can do nothing until told otherwise, password or not.
    ok(c.cmd(&["ACL", "SETUSER", "bob"]));
    refused(env.client().cmd(&["AUTH", "bob", ""]), "WRONGPASS");
    ok(c.cmd(&["ACL", "SETUSER", "bob", "on", "nopass"]));
    let mut bob = env.client();
    ok(bob.cmd(&["AUTH", "bob", "whatever"]));
    refused(bob.cmd(&["PING"]), "NOPERM");
    refused(bob.cmd(&["GET", "k"]), "NOPERM");

    let a = text(c.cmd(&["ACL", "GENPASS"]));
    let b = text(c.cmd(&["ACL", "GENPASS"]));
    assert_eq!(a.len(), 64);
    assert!(a.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_ne!(a, b);
    assert_eq!(text(c.cmd(&["ACL", "GENPASS", "5"])).len(), 2);
    refused(c.cmd(&["ACL", "GENPASS", "0"]), "ERR");
    refused(c.cmd(&["ACL", "GENPASS", "999999999"]), "ERR");

    // Many users, long names and many rules stay bounded.
    let long = "n".repeat(100_000);
    refused(c.cmd(&["ACL", "SETUSER", &long]), "ERR");
    let pattern = format!("~{}", "p".repeat(100_000));
    refused(c.cmd(&["ACL", "SETUSER", "bob", &pattern]), "ERR");
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
}

#[test]
fn command_describes_the_commands() {
    let env = env();
    let mut c = env.client();
    let Reply::Int(count) = c.cmd(&["COMMAND", "COUNT"]) else {
        panic!("no count");
    };
    assert!(count > 150, "{count}");
    let Reply::Array(all) = c.cmd(&["COMMAND"]) else {
        panic!("no table");
    };
    assert_eq!(all.len() as i64, count);

    let Reply::Array(infos) = c.cmd(&["COMMAND", "INFO", "get", "SET", "nosuch"]) else {
        panic!("no info");
    };
    assert_eq!(infos.len(), 3);
    let Reply::Array(get) = &infos[0] else {
        panic!("{infos:?}");
    };
    assert_eq!(get[0], Reply::bulk("get"));
    assert_eq!(get[1], Reply::Int(2));
    // First key, last key, step.
    assert_eq!(get[3..6], [Reply::Int(1), Reply::Int(1), Reply::Int(1)]);
    let Reply::Array(set) = &infos[1] else {
        panic!("{infos:?}");
    };
    assert_eq!(set[0], Reply::bulk("set"));
    assert_eq!(set[1], Reply::Int(-3));
    assert_eq!(infos[2], Reply::Nil);

    assert_eq!(
        c.cmd(&["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"]),
        Reply::Array(vec![Reply::bulk("a"), Reply::bulk("b")])
    );
    assert_eq!(
        c.cmd(&["COMMAND", "GETKEYS", "EVAL", "return 1", "2", "k1", "k2", "arg"]),
        Reply::Array(vec![Reply::bulk("k1"), Reply::bulk("k2")])
    );
    refused(c.cmd(&["COMMAND", "GETKEYS", "PING"]), "ERR");
    refused(c.cmd(&["COMMAND", "GETKEYS", "NOSUCH", "k"]), "ERR");
    refused(c.cmd(&["COMMAND", "GETKEYS"]), "ERR");
    refused(c.cmd(&["COMMAND", "NOSUCH"]), "ERR");
    assert!(matches!(c.cmd(&["COMMAND", "DOCS", "get"]), Reply::Array(docs) if !docs.is_empty()));
    assert!(
        matches!(c.cmd(&["COMMAND", "LIST"]), Reply::Array(names) if names.len() as i64 == count)
    );

    // Every command the table names is one the parser knows.
    let Reply::Array(names) = c.cmd(&["COMMAND", "LIST"]) else {
        panic!("no list");
    };
    for name in names {
        let name = text(name);
        // SYNC makes the connection a replica's link: a file and then the
        // stream come back, not a reply.
        if name == "sync" {
            continue;
        }
        let reply = c.cmd(&[name.as_str()]);
        assert!(
            !error_text(&reply).is_some_and(|e| e.starts_with("ERR unknown command")),
            "{name}: {reply:?}"
        );
        // Whatever it did, the connection is usable for the next one.
        if matches!(name.as_str(), "multi" | "subscribe" | "psubscribe" | "quit") {
            c = env.client();
        }
    }
}

#[test]
fn an_embedded_session_checks_credentials_but_needs_none() {
    let db = Db::open(None, engine_cfg(4)).unwrap();
    let s = db.session().unwrap();
    ok(s.execute(&["CONFIG", "SET", "requirepass", PASS]));
    // The application that embeds the database owns it.
    ok(s.execute(&["SET", "k", "v"]));
    refused(s.execute(&["AUTH", "wrong"]), "WRONGPASS");
    ok(s.execute(&["AUTH", PASS]));
    refused(
        s.execute(&["HELLO", "3", "AUTH", "default", "wrong"]),
        "WRONGPASS",
    );
    assert!(matches!(s.execute(&["COMMAND", "COUNT"]), Reply::Int(n) if n > 150));
}

#[test]
fn a_password_set_later_locks_out_who_was_already_there() {
    let env = env();
    let mut early = env.client();
    assert_eq!(early.cmd(&["PING"]), Reply::Simple("PONG".into()));
    let mut listener = env.client();
    assert!(matches!(
        listener.cmd(&["SUBSCRIBE", "ch"]),
        Reply::Array(_)
    ));
    let mut admin = env.client();
    ok(admin.cmd(&["CONFIG", "SET", "requirepass", PASS]));
    refused(early.cmd(&["FLUSHALL"]), "NOAUTH");
    refused(
        early.cmd(&["ACL", "SETUSER", "door", "on", "nopass", "+@all", "~*"]),
        "NOAUTH",
    );
    refused(early.cmd(&["CONFIG", "SET", "requirepass", ""]), "NOAUTH");
    refused(listener.cmd(&["SUBSCRIBE", "more"]), "NOAUTH");
    // A moment without a password lets nobody stay for good.
    ok(admin.cmd(&["AUTH", PASS]));
    ok(admin.cmd(&["CONFIG", "SET", "requirepass", ""]));
    assert_eq!(early.cmd(&["PING"]), Reply::Simple("PONG".into()));
    ok(admin.cmd(&["CONFIG", "SET", "requirepass", "next"]));
    refused(early.cmd(&["PING"]), "NOAUTH");
    ok(admin.cmd(&["SET", "k", "v"]));
}

#[test]
fn before_login_a_command_must_be_small() {
    let (env, _admin) = locked();
    // A bulk far larger than a password, announced and never sent.
    let mut c = env.client();
    c.send_raw(
        b"*2
$4
AUTH
$67108864
",
    );
    let replies = c.read_until_closed();
    assert!(
        matches!(replies.as_slice(), [Reply::Error(_)]),
        "{replies:?}"
    );
    // More arguments than any login has.
    let mut c = env.client();
    c.send_raw(
        b"*1000000
",
    );
    let replies = c.read_until_closed();
    assert!(
        matches!(replies.as_slice(), [Reply::Error(_)]),
        "{replies:?}"
    );
    // A long inline line.
    let mut c = env.client();
    let mut line = vec![b'A'; 100_000];
    line.extend_from_slice(
        b"
",
    );
    let _ = c.try_send_raw(&line);
    let replies = c.read_until_closed();
    assert!(
        replies.iter().all(|r| matches!(r, Reply::Error(_))),
        "{replies:?}"
    );

    // Logged in, the usual limits hold again.
    let mut c = env.client();
    ok(
        c.cmd(&["HELLO", "2", "AUTH", "default", PASS, "SETNAME", "big"])
            .pipe_array(),
    );
    let big = "x".repeat(1 << 20);
    ok(c.cmd(&["SET", "big", &big]));
    assert_eq!(c.cmd(&["STRLEN", "big"]), Reply::Int(1 << 20));
    // And after RESET they are the small ones once more.
    assert_eq!(c.cmd(&["RESET"]), Reply::Simple("RESET".into()));
    // The server closes after the header, so the rest of the write may fail
    // (on Linux with a reset).
    let _ = c.try_send_raw(
        format!(
            "*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${}\r\n{big}\r\n",
            big.len()
        )
        .as_bytes(),
    );
    let replies = c.read_until_closed();
    assert!(
        matches!(replies.as_slice(), [Reply::Error(_)]),
        "{replies:?}"
    );
}

trait PipeArray {
    fn pipe_array(self) -> Reply;
}

impl PipeArray for Reply {
    /// An array reply as the `OK` the helper `ok` expects.
    fn pipe_array(self) -> Reply {
        match self {
            Reply::Array(_) => Reply::Ok,
            other => other,
        }
    }
}

#[test]
fn a_user_made_again_is_not_the_user_that_was() {
    let (env, mut admin, mut alice) = with_alice(&["~app:*", "+get"]);
    // No command of hers falls between the two.
    assert_eq!(admin.cmd(&["ACL", "DELUSER", "alice"]), Reply::Int(1));
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "on", ">other-pw", "+@all", "~*"]));
    let _ = alice.try_send_raw(&wire_str(&["FLUSHALL"]));
    assert!(alice
        .read_until_closed()
        .iter()
        .all(|r| matches!(r, Reply::Error(_))));
    ok(admin.cmd(&["SET", "k", "v"]));
    assert_eq!(admin.cmd(&["GET", "k"]), Reply::bulk("v"));
    ok(env.client().cmd(&["AUTH", "alice", "other-pw"]));
}

#[test]
fn a_silent_listener_loses_what_its_user_loses() {
    let (env, mut admin, _alice) =
        with_alice(&["resetchannels", "&news:*", "+subscribe", "+psubscribe"]);
    let listen = |args: &[&str]| {
        let mut c = env.client();
        ok(c.cmd(&["AUTH", "alice", "alice-pw"]));
        assert!(matches!(c.cmd(args), Reply::Array(_)));
        c
    };
    let mut on_channel = listen(&["SUBSCRIBE", "news:1"]);
    let mut on_pattern = listen(&["PSUBSCRIBE", "news:*"]);
    assert_eq!(admin.cmd(&["PUBLISH", "news:1", "one"]), Reply::Int(2));
    assert!(matches!(on_channel.read(), Reply::Array(_)));
    assert!(matches!(on_pattern.read(), Reply::Array(_)));

    // The channels go; neither connection sends a thing.
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "resetchannels", "&sport:*"]));
    assert!(on_channel.read_until_closed().is_empty());
    assert!(on_pattern.read_until_closed().is_empty());
    assert!(wait_until(5, || admin.cmd(&["PUBLISH", "news:1", "two"])
        == Reply::Int(0)));

    // The user goes: so does a listener that was still allowed, and one
    // that waits in a blocking command.
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "+blpop", "~q"]));
    let mut still = listen(&["SUBSCRIBE", "sport:1"]);
    let mut blocked = env.client();
    ok(blocked.cmd(&["AUTH", "alice", "alice-pw"]));
    blocked.send(&["BLPOP", "q", "0"]);
    assert!(blocked.silent_for(200));
    assert_eq!(admin.cmd(&["ACL", "DELUSER", "alice"]), Reply::Int(1));
    assert!(still.read_until_closed().is_empty());
    assert!(blocked.read_until_closed().is_empty());
    assert_eq!(admin.cmd(&["RPUSH", "q", "kept"]), Reply::Int(1));
    assert_eq!(admin.cmd(&["LLEN", "q"]), Reply::Int(1));
}

#[test]
fn a_queue_filled_under_old_rights_does_not_run_under_new() {
    let (_env, mut admin, mut alice) = with_alice(&["~*", "+@all"]);
    ok(admin.cmd(&["SET", "k", "v"]));
    ok(alice.cmd(&["MULTI"]));
    assert_eq!(alice.cmd(&["FLUSHALL"]), Reply::Simple("QUEUED".into()));
    ok(admin.cmd(&["ACL", "SETUSER", "alice", "-flushall"]));
    refused(alice.cmd(&["EXEC"]), "EXECABORT");
    assert_eq!(admin.cmd(&["GET", "k"]), Reply::bulk("v"));
    // The next transaction is judged by the rules of now.
    ok(alice.cmd(&["MULTI"]));
    refused(alice.cmd(&["FLUSHALL"]), "NOPERM");
    refused(alice.cmd(&["EXEC"]), "EXECABORT");
    ok(alice.cmd(&["MULTI"]));
    assert_eq!(
        alice.cmd(&["SET", "k", "w"]),
        Reply::Simple("QUEUED".into())
    );
    // ACL has no place in a transaction.
    refused(
        alice.cmd(&["ACL", "WHOAMI"]),
        "ERR Command not allowed inside a transaction",
    );
    refused(alice.cmd(&["EXEC"]), "EXECABORT");
    // A change to someone else leaves an unlimited user's queue alone.
    ok(admin.cmd(&["MULTI"]));
    assert_eq!(
        admin.cmd(&["SET", "k", "x"]),
        Reply::Simple("QUEUED".into())
    );
    let mut second = _env.client();
    ok(second.cmd(&["ACL", "SETUSER", "carol", "on"]));
    assert_eq!(admin.cmd(&["EXEC"]), Reply::Array(vec![Reply::Ok]));
}
