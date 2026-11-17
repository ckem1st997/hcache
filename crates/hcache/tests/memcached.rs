//! The Memcached port: the text and meta commands over the keys of
//! database 0, the users of RESP, writes that replicate, a cap on its
//! connections and a flush_all that waits.

mod net;

use hcache::{Reply, ServerConfig};
use net::{engine_cfg, server_cfg, wait_until, Client, Env, IO_TIMEOUT};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

fn env_with(scfg: ServerConfig) -> (Env, SocketAddr) {
    let env = Env::new(
        engine_cfg(4),
        ServerConfig {
            memcached: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            ..scfg
        },
    );
    let addr = env.server.memcached_addr().expect("a Memcached port");
    (env, addr)
}

fn env() -> (Env, SocketAddr) {
    env_with(server_cfg(2, 64))
}

/// A Memcached text client.
struct Mc {
    rd: BufReader<TcpStream>,
    wr: TcpStream,
}

impl Mc {
    fn connect(addr: SocketAddr) -> Mc {
        let s = TcpStream::connect(addr).expect("connect");
        s.set_read_timeout(Some(IO_TIMEOUT)).expect("read timeout");
        let wr = s.try_clone().expect("clone socket");
        Mc {
            rd: BufReader::new(s),
            wr,
        }
    }

    fn send(&mut self, request: &str) {
        self.wr.write_all(request.as_bytes()).expect("write");
    }

    /// The next reply line, or `None` once the server has hung up.
    fn try_line(&mut self) -> Option<String> {
        let mut line = String::new();
        match self.rd.read_line(&mut line) {
            Ok(0) | Err(_) => None,
            Ok(_) => Some(line.trim_end_matches("\r\n").to_string()),
        }
    }

    fn line(&mut self) -> String {
        self.try_line().expect("a reply line")
    }

    fn ask(&mut self, request: &str) -> String {
        self.send(request);
        self.line()
    }

    /// Every line up to END, END left out.
    fn ask_until_end(&mut self, request: &str) -> Vec<String> {
        self.send(request);
        let mut lines = Vec::new();
        loop {
            match self.line() {
                end if end == "END" => return lines,
                line => lines.push(line),
            }
        }
    }

    fn stat(&mut self, name: &str) -> String {
        let prefix = format!("STAT {name} ");
        self.ask_until_end("stats\r\n")
            .iter()
            .find_map(|line| line.strip_prefix(prefix.as_str()).map(str::to_string))
            .unwrap_or_else(|| panic!("no stat {name}"))
    }
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

#[test]
fn what_memcached_stores_is_the_string_resp_reads() {
    let (env, addr) = env();
    let mut mc = Mc::connect(addr);
    let mut c = env.client();
    assert_eq!(mc.ask("set k 0 0 5\r\nhello\r\n"), "STORED");
    assert_eq!(c.cmd(&["GET", "k"]), Reply::bulk("hello"));
    ok(c.cmd(&["SET", "r", "from resp"]));
    assert_eq!(
        mc.ask_until_end("get r k missing\r\n"),
        ["VALUE r 0 9", "from resp", "VALUE k 0 5", "hello"]
    );
    // Client flags ride in front of the value.
    assert_eq!(mc.ask("set f 42 0 2\r\nhi\r\n"), "STORED");
    assert_eq!(mc.ask_until_end("get f\r\n"), ["VALUE f 42 2", "hi"]);
    assert_eq!(
        c.cmd(&["GET", "f"]),
        Reply::Bulk(b"\0MC\x01\0\0\0\x2ahi".to_vec())
    );
    // Database 0 only.
    ok(c.cmd(&["SELECT", "1"]));
    ok(c.cmd(&["SET", "elsewhere", "x"]));
    assert!(mc.ask_until_end("get elsewhere\r\n").is_empty());
    // A key of another type is no item.
    ok(c.cmd(&["SELECT", "0"]));
    c.cmd(&["RPUSH", "list", "a"]);
    assert!(mc.ask_until_end("get list\r\n").is_empty());
    assert_eq!(mc.ask("set list 0 0 1\r\nx\r\n"), "STORED");
    assert_eq!(c.cmd(&["TYPE", "list"]), Reply::Simple("string".into()));

    // An exptime is a TTL.
    assert_eq!(mc.ask("set t 0 100 1\r\nx\r\n"), "STORED");
    assert!(matches!(c.cmd(&["TTL", "t"]), Reply::Int(t) if (99..=100).contains(&t)));
    assert_eq!(mc.ask("touch t 0\r\n"), "TOUCHED");
    assert_eq!(c.cmd(&["TTL", "t"]), Reply::Int(-1));
    assert_eq!(mc.ask("set gone 0 -1 1\r\nx\r\n"), "STORED");
    assert_eq!(c.cmd(&["EXISTS", "gone"]), Reply::Int(0));

    assert_eq!(mc.ask("delete k\r\n"), "DELETED");
    assert_eq!(mc.ask("delete k\r\n"), "NOT_FOUND");
    assert_eq!(c.cmd(&["EXISTS", "k"]), Reply::Int(0));
    assert_eq!(mc.ask("version\r\n"), "VERSION 1.6.24");
    assert_eq!(mc.stat("curr_connections"), "1");
    assert_eq!(mc.stat("get_hits"), "3");
}

#[test]
fn cas_counters_and_concatenation() {
    let (_env, addr) = env();
    let mut mc = Mc::connect(addr);
    assert_eq!(mc.ask("set k 7 0 1\r\na\r\n"), "STORED");
    let head = mc.ask_until_end("gets k\r\n")[0].clone();
    let cas = head.rsplit(' ').next().expect("a CAS").to_string();
    assert_eq!(mc.ask("cas k 0 0 1 1\r\nb\r\n"), "EXISTS");
    assert_eq!(mc.ask(&format!("cas k 7 0 1 {cas}\r\nb\r\n")), "STORED");
    assert_eq!(mc.ask(&format!("cas k 7 0 1 {cas}\r\nc\r\n")), "EXISTS");
    assert_eq!(mc.ask("cas nope 0 0 1 1\r\nc\r\n"), "NOT_FOUND");
    assert_eq!(mc.ask("append k 0 0 2\r\nyz\r\n"), "STORED");
    assert_eq!(mc.ask("prepend k 0 0 1\r\nx\r\n"), "STORED");
    // The flags stay what they were.
    assert_eq!(mc.ask_until_end("get k\r\n"), ["VALUE k 7 4", "xbyz"]);
    assert_eq!(mc.ask("append nope 0 0 1\r\nx\r\n"), "NOT_STORED");
    // As memcached: no value grows past 1 MiB.
    let big = "x".repeat(1_000_000);
    assert_eq!(
        mc.ask(&format!("set big 0 0 {}\r\n{big}\r\n", big.len())),
        "STORED"
    );
    let more = "y".repeat(100_000);
    assert_eq!(
        mc.ask(&format!("append big 0 0 {}\r\n{more}\r\n", more.len())),
        "NOT_STORED"
    );
    assert_eq!(mc.ask("mg big s\r\n"), "HD s1000000");
    assert_eq!(mc.ask("add k 0 0 1\r\nx\r\n"), "NOT_STORED");
    assert_eq!(mc.ask("replace nope 0 0 1\r\nx\r\n"), "NOT_STORED");

    assert_eq!(mc.ask("set n 0 0 2\r\n10\r\n"), "STORED");
    assert_eq!(mc.ask("incr n 5\r\n"), "15");
    assert_eq!(mc.ask("decr n 100\r\n"), "0");
    assert_eq!(
        mc.ask("incr k 1\r\n"),
        "CLIENT_ERROR cannot increment or decrement non-numeric value"
    );
    assert_eq!(mc.ask("incr nope 1\r\n"), "NOT_FOUND");
    // Many clients at once lose no increment.
    let threads: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(move || {
                let mut mc = Mc::connect(addr);
                for _ in 0..200 {
                    mc.ask("incr n 1\r\n");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().expect("a client thread");
    }
    assert_eq!(mc.ask_until_end("get n\r\n"), ["VALUE n 0 3", "800"]);
}

#[test]
fn meta_commands() {
    let (_env, addr) = env();
    let mut mc = Mc::connect(addr);
    assert_eq!(
        mc.ask("ms m 2 F5 T100 c\r\nhi\r\n").split(' ').next(),
        Some("HD")
    );
    mc.send("mg m s v f t k O1\r\n");
    let head = mc.line();
    assert!(head.starts_with("VA 2 s2 f5 t"), "{head}");
    assert!(head.ends_with(" km O1"), "{head}");
    assert_eq!(mc.line(), "hi");
    assert_eq!(mc.ask("mg nope v\r\n"), "EN");
    // Quiet: only what went wrong, and mn to end the batch.
    mc.send("mg nope v q\r\nms m 1 q\r\nx\r\nmd nope q\r\nmn\r\n");
    assert_eq!(mc.line(), "NF");
    assert_eq!(mc.line(), "MN");
    let cas = mc.ask("mg m c\r\n");
    let cas = cas.strip_prefix("HD c").expect("a CAS");
    assert_eq!(mc.ask("md m C1\r\n"), "EX");
    assert_eq!(mc.ask(&format!("md m C{cas}\r\n")), "HD");
    assert_eq!(mc.ask("ma cnt N0 J10 v\r\n"), "VA 2");
    assert_eq!(mc.line(), "10");
    assert_eq!(mc.ask("ma cnt MD D3 v\r\n"), "VA 1");
    assert_eq!(mc.line(), "7");
    assert_eq!(
        mc.ask("ma cnt Mi\r\n"),
        "CLIENT_ERROR invalid mode for ma M token"
    );
    // A binary key, base64 on the wire.
    assert_eq!(mc.ask("ms AAEC 1 b\r\nx\r\n"), "HD");
    assert_eq!(mc.ask("mg AAEC b k s\r\n"), "HD kAAEC b s1");
    assert_eq!(mc.ask("mn\r\n"), "MN");
}

#[test]
fn noreply_answers_nothing_and_errors_keep_the_stream_in_step() {
    let (_env, addr) = env();
    let mut mc = Mc::connect(addr);
    mc.send("set a 0 0 1 noreply\r\na\r\nincr a 1 noreply\r\ndelete b noreply\r\n");
    assert_eq!(mc.ask_until_end("get a\r\n"), ["VALUE a 0 1", "a"]);
    // A value too large is dropped, and the next command is read whole.
    let big = "x".repeat(2 << 20);
    mc.send(&format!("set big 0 0 {}\r\n{big}\r\n", big.len()));
    assert_eq!(mc.line(), "SERVER_ERROR object too large for cache");
    // As memcached: what follows a bad chunk is read as the next command.
    assert_eq!(
        mc.ask("set b 0 0 3\r\nabcde\r\n"),
        "CLIENT_ERROR bad data chunk"
    );
    assert_eq!(mc.line(), "ERROR");
    assert_eq!(mc.ask("bogus\r\n"), "ERROR");
    assert_eq!(mc.ask("mn\r\n"), "MN");
    mc.send("quit\r\n");
    assert_eq!(mc.try_line(), None);
}

#[test]
fn a_login_is_needed_where_resp_needs_one() {
    let (env, addr) = env();
    let mut admin = env.client();
    ok(admin.cmd(&[
        "ACL", "SETUSER", "bob", "on", ">bob-pw", "~b*", "+get", "+set",
    ]));
    ok(admin.cmd(&["CONFIG", "SET", "requirepass", "door"]));
    ok(admin.cmd(&["AUTH", "door"]));

    let mut mc = Mc::connect(addr);
    assert_eq!(mc.ask("get k\r\n"), "CLIENT_ERROR unauthenticated");
    assert_eq!(
        mc.ask("set x 0 0 10\r\ndefault no\r\n"),
        "CLIENT_ERROR authentication failure"
    );
    assert_eq!(mc.ask("set x 0 0 12\r\ndefault door\r\n"), "STORED");
    assert_eq!(mc.ask("set k 0 0 1\r\nv\r\n"), "STORED");
    assert_eq!(mc.ask("flush_all\r\n"), "OK");

    // bob may only read and write his own keys.
    let mut bob = Mc::connect(addr);
    assert_eq!(bob.ask("set x 0 0 10\r\nbob bob-pw\r\n"), "STORED");
    assert_eq!(bob.ask("set b1 0 0 1\r\nx\r\n"), "STORED");
    assert_eq!(bob.ask_until_end("get b1\r\n"), ["VALUE b1 0 1", "x"]);
    let denied = bob.ask("set other 0 0 1\r\nx\r\n");
    assert!(denied.starts_with("SERVER_ERROR NOPERM"), "{denied}");
    let denied = bob.ask("delete b1\r\n");
    assert!(denied.starts_with("SERVER_ERROR NOPERM"), "{denied}");
    let denied = bob.ask("flush_all\r\n");
    assert!(denied.starts_with("SERVER_ERROR NOPERM"), "{denied}");
    // stats needs INFO's right, stats reset CONFIG RESETSTAT's.
    let denied = bob.ask("stats\r\n");
    assert!(
        denied.contains("NOPERM") && denied.contains("'info'"),
        "{denied}"
    );
    let denied = bob.ask("stats reset\r\n");
    assert!(denied.starts_with("SERVER_ERROR NOPERM"), "{denied}");
    assert_eq!(mc.ask("stats reset\r\n"), "RESET");
    assert_eq!(admin.cmd(&["GET", "b1"]), Reply::bulk("x"));
    // A deleted user's connections go.
    assert_eq!(admin.cmd(&["ACL", "DELUSER", "bob"]), Reply::Int(1));
    bob.send("get b1\r\n");
    assert_eq!(bob.try_line(), None);
}

#[test]
fn one_that_never_logs_in_does_not_keep_its_slot() {
    let (env, addr) = env();
    ok(env.client().cmd(&["CONFIG", "SET", "requirepass", "door"]));
    let quiet = TcpStream::connect(addr).expect("connect");
    quiet
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    let mut logged_in = Mc::connect(addr);
    assert_eq!(logged_in.ask("set x 0 0 12\r\ndefault door\r\n"), "STORED");
    let started = Instant::now();
    let read = (&quiet)
        .read(&mut [0; 1])
        .expect("a hang-up, not a timeout");
    assert_eq!(read, 0);
    assert!(
        started.elapsed() >= Duration::from_secs(9),
        "closed too soon"
    );
    // One that logged in stays.
    assert_eq!(logged_in.ask("mn\r\n"), "MN");
}

fn field(c: &mut Client, name: &str) -> Option<String> {
    let Reply::Bulk(info) = c.cmd(&["INFO", "replication"]) else {
        panic!("INFO is not a bulk string");
    };
    let prefix = format!("{name}:");
    String::from_utf8_lossy(&info)
        .lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()).map(str::to_string))
}

#[test]
fn writes_replicate_and_a_replica_refuses_its_own() {
    let ((primary, p_addr), (replica, r_addr)) = (env(), env());
    let mut r = replica.client();
    let port = primary.addr.port().to_string();
    ok(r.cmd(&["REPLICAOF", "127.0.0.1", &port]));
    let up = wait_until(15, || {
        field(&mut r, "master_link_status").as_deref() == Some("up")
    });
    assert!(up, "the link never came up");

    let mut on_primary = Mc::connect(p_addr);
    assert_eq!(on_primary.ask("set k 3 0 2\r\nhi\r\n"), "STORED");
    assert_eq!(on_primary.ask("set n 0 0 1\r\n1\r\n"), "STORED");
    assert_eq!(on_primary.ask("incr n 4\r\n"), "5");
    let mut on_replica = Mc::connect(r_addr);
    let arrived = wait_until(15, || on_replica.ask_until_end("get k n\r\n").len() == 4);
    assert!(arrived, "the writes did not reach the replica");
    assert_eq!(
        on_replica.ask_until_end("get k n\r\n"),
        ["VALUE k 3 2", "hi", "VALUE n 0 1", "5"]
    );
    let refused = on_replica.ask("set k 0 0 1\r\nx\r\n");
    assert!(refused.starts_with("SERVER_ERROR READONLY"), "{refused}");
    let refused = on_replica.ask("ms k 1\r\nx\r\n");
    assert!(refused.starts_with("SERVER_ERROR READONLY"), "{refused}");
    // Reads that change nothing still work there.
    assert_eq!(on_replica.ask("mg k s\r\n"), "HD s2");
}

#[test]
fn connections_beyond_the_cap_are_turned_away() {
    let (_env, addr) = env_with(ServerConfig {
        max_conns: 2,
        ..server_cfg(2, 64)
    });
    let mut first = Mc::connect(addr);
    let mut second = Mc::connect(addr);
    assert_eq!(first.ask("mn\r\n"), "MN");
    assert_eq!(second.ask("mn\r\n"), "MN");
    let mut third = Mc::connect(addr);
    assert_eq!(third.line(), "ERROR Too many open connections");
    assert_eq!(third.try_line(), None);
    drop(first);
    // One turned away may find the socket shut before it writes.
    let room = wait_until(10, || {
        let mut again = Mc::connect(addr);
        again.wr.write_all(b"mn\r\n").is_ok() && again.try_line().as_deref() == Some("MN")
    });
    assert!(room, "a closed connection gave back no room");
}

#[test]
fn a_delayed_flush_all_empties_database_0_later() {
    let (env, addr) = env();
    let mut mc = Mc::connect(addr);
    let mut c = env.client();
    ok(c.cmd(&["SELECT", "1"]));
    ok(c.cmd(&["SET", "kept", "x"]));
    assert_eq!(mc.ask("set k 0 0 1\r\nx\r\n"), "STORED");
    assert_eq!(mc.ask("flush_all 1\r\n"), "OK");
    assert_eq!(mc.ask_until_end("get k\r\n").len(), 2);
    let flushed = wait_until(10, || mc.ask_until_end("get k\r\n").is_empty());
    assert!(flushed, "the delayed flush_all never ran");
    assert_eq!(c.cmd(&["GET", "kept"]), Reply::bulk("x"));
    // A later flush_all calls off one still to come.
    assert_eq!(mc.ask("set k 0 0 1\r\nx\r\n"), "STORED");
    assert_eq!(mc.ask("flush_all 2\r\n"), "OK");
    assert_eq!(
        mc.ask("flush_all 0 noreply\r\nset k 0 0 1\r\ny\r\n"),
        "STORED"
    );
    std::thread::sleep(Duration::from_millis(2600));
    assert_eq!(mc.ask_until_end("get k\r\n"), ["VALUE k 0 1", "y"]);
    // One whose user may no longer flush by then empties nothing.
    assert_eq!(mc.ask("flush_all 1\r\n"), "OK");
    ok(c.cmd(&["ACL", "SETUSER", "default", "-flushdb"]));
    std::thread::sleep(Duration::from_millis(1800));
    assert_eq!(mc.ask_until_end("get k\r\n"), ["VALUE k 0 1", "y"]);
}
