//! The TLS listener: that it serves what the plain one serves, byte for
//! byte, and what it does with peers that are not TLS, are slow, show the
//! wrong certificate or none, or go away without a word.

mod net;

use hcache::{Db, Fsync, PersistOptions, Reply, ServerConfig};
use hcache_server::tls::{server_config, ClientCerts};
use hcache_server::TlsListener;
use net::*;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Test certificates, made once with openssl and good for a century: an
/// authority (`ca.crt`), a server and a client it signed, and a client
/// signed by an authority nobody trusts (`stranger`). The keys protect
/// nothing.
fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("tls")
        .join(name)
}

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn listener(clients: Option<ClientCerts>) -> TlsListener {
    let ca = fixture("ca.crt");
    let config = server_config(
        &fixture("server.crt"),
        &fixture("server.key"),
        clients.map(|mode| (ca.as_path(), mode)),
    )
    .expect("server TLS configuration");
    TlsListener {
        addr: loopback(),
        config,
    }
}

fn tls_cfg(clients: Option<ClientCerts>) -> ServerConfig {
    ServerConfig {
        tls: Some(listener(clients)),
        ..server_cfg(3, 64)
    }
}

struct TlsEnv {
    env: Env,
    tls: SocketAddr,
}

impl TlsEnv {
    fn new(scfg: ServerConfig) -> TlsEnv {
        let env = Env::new(engine_cfg(4), scfg);
        let tls = env.server.tls_addr().expect("a TLS listener");
        TlsEnv { env, tls }
    }

    fn client(&self) -> TlsClient {
        TlsClient::connect(self.tls, &client_config(None, &[])).expect("TLS handshake")
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// What a client trusts and shows: the test authority, and the named
/// certificate of its own if any. `versions` empty means TLS 1.2 and 1.3.
fn client_config(
    identity: Option<&str>,
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(fixture("ca.crt")).unwrap() {
        roots.add(cert.unwrap()).unwrap();
    }
    let builder = ClientConfig::builder_with_provider(provider());
    let builder = match versions {
        [] => builder.with_safe_default_protocol_versions(),
        some => builder.with_protocol_versions(some),
    }
    .unwrap()
    .with_root_certificates(roots);
    let config = match identity {
        None => builder.with_no_client_auth(),
        Some(name) => {
            let chain = CertificateDer::pem_file_iter(fixture(&format!("{name}.crt")))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let key = PrivateKeyDer::from_pem_file(fixture(&format!("{name}.key"))).unwrap();
            builder.with_client_auth_cert(chain, key).unwrap()
        }
    };
    Arc::new(config)
}

struct TlsClient {
    rd: BufReader<StreamOwned<ClientConnection, TcpStream>>,
}

impl TlsClient {
    /// Connect and finish the handshake, so that a refusal shows here.
    fn connect(addr: SocketAddr, config: &Arc<ClientConfig>) -> std::io::Result<TlsClient> {
        let sock = connect_stream(addr);
        sock.set_read_timeout(Some(IO_TIMEOUT))?;
        sock.set_write_timeout(Some(IO_TIMEOUT))?;
        let _ = sock.set_nodelay(true);
        let name = ServerName::try_from("localhost").unwrap();
        let conn = ClientConnection::new(config.clone(), name).map_err(std::io::Error::other)?;
        let mut stream = StreamOwned::new(conn, sock);
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock)?;
        }
        Ok(TlsClient {
            rd: BufReader::with_capacity(256 << 10, stream),
        })
    }

    fn stream(&mut self) -> &mut StreamOwned<ClientConnection, TcpStream> {
        self.rd.get_mut()
    }

    fn send_raw(&mut self, bytes: &[u8]) {
        let stream = self.stream();
        stream.write_all(bytes).expect("TLS write");
        stream.flush().expect("TLS flush");
    }

    fn send<A: AsRef<[u8]>>(&mut self, args: &[A]) {
        let mut out = Vec::new();
        hcache_core::resp::encode_command(args, &mut out);
        self.send_raw(&out);
    }

    fn try_read(&mut self) -> std::io::Result<Reply> {
        read_reply(&mut self.rd)
    }

    fn read(&mut self) -> Reply {
        self.try_read()
            .unwrap_or_else(|e| panic!("reading a reply over TLS: {e}"))
    }

    fn cmd<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Reply {
        self.send(args);
        self.read()
    }

    fn set_timeout(&mut self, t: Duration) {
        self.stream().sock.set_read_timeout(Some(t)).unwrap();
    }

    /// True when nothing arrives within `millis`.
    fn silent_for(&mut self, millis: u64) -> bool {
        self.set_timeout(Duration::from_millis(millis));
        let silent = match self.rd.fill_buf() {
            Ok(_) => false,
            Err(e) => is_timeout(&e),
        };
        self.set_timeout(IO_TIMEOUT);
        silent
    }

    /// How the connection ended: `Ok(true)` for a proper TLS goodbye,
    /// `Ok(false)` when the socket just closed.
    fn ending(&mut self) -> Result<bool, String> {
        self.set_timeout(Duration::from_secs(15));
        let mut byte = [0u8; 1];
        match self.rd.read(&mut byte) {
            Ok(0) => Ok(true),
            Ok(_) => Err(format!("data instead of an end: {byte:?}")),
            Err(e) if is_closed(&e) => Ok(false),
            Err(e) => Err(format!("{e}")),
        }
    }
}

fn pong() -> Reply {
    Reply::Simple("PONG".into())
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn int(reply: Reply) -> i64 {
    match reply {
        Reply::Int(n) => n,
        other => panic!("not an integer: {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------

#[test]
fn tls_serves_the_same_data_as_the_plain_port() {
    watchdog("tls-basic", 60, || {
        let t = TlsEnv::new(tls_cfg(None));
        assert_ne!(t.env.addr, t.tls);
        let mut secure = t.client();
        let mut plain = t.env.client();
        assert_eq!(secure.cmd(&["PING"]), pong());
        ok(secure.cmd(&["SET", "k", "over tls"]));
        assert_eq!(plain.cmd(&["GET", "k"]), b("over tls"));
        ok(plain.cmd(&["SET", "k2", "plain"]));
        assert_eq!(secure.cmd(&["GET", "k2"]), b("plain"));
        assert_eq!(
            secure.cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]),
            b("over tls")
        );
        ok(secure.cmd(&["MULTI"]));
        secure.cmd(&["INCR", "n"]);
        secure.cmd(&["INCR", "n"]);
        assert_eq!(
            secure.cmd(&["EXEC"]),
            Reply::Array(vec![Reply::Int(1), Reply::Int(2)])
        );
        // Both kinds are listed, and counted.
        let listed = match plain.cmd(&["CLIENT", "LIST"]) {
            Reply::Bulk(v) => String::from_utf8(v).unwrap(),
            other => panic!("{other:?}"),
        };
        assert_eq!(listed.lines().count(), 2, "{listed}");
        assert_eq!(t.env.server.connections(), 2);
        drop(secure);
        assert_eq!(t.env.wait_conns(1, 5), 1);
    });
}

#[test]
fn both_tls_versions_are_spoken() {
    let t = TlsEnv::new(tls_cfg(None));
    for (version, want) in [
        (&rustls::version::TLS12, rustls::ProtocolVersion::TLSv1_2),
        (&rustls::version::TLS13, rustls::ProtocolVersion::TLSv1_3),
    ] {
        let mut c = TlsClient::connect(t.tls, &client_config(None, &[version])).unwrap();
        assert_eq!(c.stream().conn.protocol_version(), Some(want));
        assert_eq!(c.cmd(&["PING"]), pong());
        ok(c.cmd(&["SET", "v", "1"]));
    }
    let mut c = t.client();
    assert_eq!(
        c.stream().conn.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
}

#[test]
fn large_values_and_long_pipelines_cross_tls_intact() {
    watchdog("tls-large", 180, || {
        let _heavy = heavy();
        let t = TlsEnv::new(tls_cfg(None));
        let mut c = t.client();
        // A value far larger than a TLS record, and than what TLS buffers.
        let mut rng = hcache_core::testutil::Rng::new(7);
        let big = rng.bytes(9 << 20);
        ok(c.cmd(&[&b"SET"[..], b"big", &big]));
        assert_eq!(c.cmd(&["STRLEN", "big"]), Reply::Int(9 << 20));
        match c.cmd(&["GET", "big"]) {
            Reply::Bulk(v) => assert!(v == big, "the value came back changed"),
            other => panic!("{}", short(&other)),
        }
        // Many replies in a row, each larger than the server sends at once,
        // read by a client that dawdles.
        let mut pipeline = Vec::new();
        for _ in 0..12 {
            pipeline.extend(wire_str(&["GET", "big"]));
            pipeline.extend(wire_str(&["STRLEN", "big"]));
        }
        c.send_raw(&pipeline);
        for i in 0..12 {
            if i % 4 == 0 {
                thread::sleep(Duration::from_millis(300));
            }
            match c.read() {
                Reply::Bulk(v) => assert!(v == big, "reply {i} came back changed"),
                other => panic!("{}", short(&other)),
            }
            assert_eq!(c.read(), Reply::Int(9 << 20));
        }
        // Long pipelines of small commands, checked against a model.
        let mut work = Workload::new(99, "tls:");
        for round in 0..150 {
            let (bytes, expects) = work.batch(120);
            c.send_raw(&bytes);
            check_replies(&mut c.rd, &expects, &format!("round {round}"));
        }
    });
}

#[test]
fn requests_that_arrive_in_crumbs_are_put_together() {
    watchdog("tls-crumbs", 60, || {
        let t = TlsEnv::new(tls_cfg(None));
        let mut c = t.client();
        let mut work = Workload::new(5, "crumbs:");
        let (bytes, expects) = work.batch(60);
        // Every few bytes its own TLS record and its own packet.
        let mut rng = hcache_core::testutil::Rng::new(11);
        let mut at = 0;
        while at < bytes.len() {
            let n = (1 + rng.below(9) as usize).min(bytes.len() - at);
            c.send_raw(&bytes[at..at + n]);
            at += n;
        }
        check_replies(&mut c.rd, &expects, "crumbs");
    });
}

#[test]
fn many_tls_clients_at_once() {
    watchdog("tls-many", 180, || {
        let _heavy = heavy();
        let t = TlsEnv::new(tls_cfg(None));
        let addr = t.tls;
        let config = client_config(None, &[]);
        let threads: Vec<_> = (0..16)
            .map(|n| {
                let config = config.clone();
                thread::spawn(move || {
                    let mut c = TlsClient::connect(addr, &config).unwrap();
                    let mut work = Workload::new(1000 + n, &format!("m{n}:"));
                    for round in 0..40 {
                        let (bytes, expects) = work.batch(50);
                        c.send_raw(&bytes);
                        check_replies(&mut c.rd, &expects, &format!("client {n} round {round}"));
                    }
                })
            })
            .collect();
        for h in threads {
            h.join().unwrap();
        }
        assert_eq!(t.env.wait_conns(0, 10), 0);
    });
}

#[test]
fn pubsub_blocking_and_kill_work_over_tls() {
    watchdog("tls-features", 60, || {
        let t = TlsEnv::new(tls_cfg(None));
        let mut c = t.client();
        let mut sub = t.client();
        sub.cmd(&["SUBSCRIBE", "news"]);
        assert_eq!(c.cmd(&["PUBLISH", "news", "hello"]), Reply::Int(1));
        assert_eq!(
            sub.read(),
            Reply::Array(vec![b("message"), b("news"), b("hello")])
        );
        // A message while the subscriber sleeps is pushed without a request.
        let mut waiter = t.client();
        waiter.send(&["BLPOP", "q", "0"]);
        assert!(waiter.silent_for(200));
        assert_eq!(c.cmd(&["RPUSH", "q", "job"]), Reply::Int(1));
        assert_eq!(waiter.read(), Reply::Array(vec![b("q"), b("job")]));
        // BLPOP with a timeout answers on time.
        let start = Instant::now();
        waiter.send(&["BLPOP", "q", "0.3"]);
        assert_eq!(waiter.read(), Reply::NilArray);
        assert!(start.elapsed() >= Duration::from_millis(250));
        // QUIT: the answer, then a proper TLS goodbye.
        waiter.send(&["QUIT"]);
        ok(waiter.read());
        assert_eq!(waiter.ending(), Ok(true));
        // CLIENT KILL closes a TLS connection like any other.
        let id = int(sub.cmd_id(&mut c));
        assert_eq!(
            c.cmd(&["CLIENT", "KILL", "ID", &id.to_string()]),
            Reply::Int(1)
        );
        assert!(sub.ending().is_ok());
        assert_eq!(c.cmd(&["PUBLISH", "news", "x"]), Reply::Int(0));
        assert_eq!(t.env.wait_conns(1, 5), 1);
    });
}

impl TlsClient {
    /// The id of this subscribed connection, found by `other` in the list.
    fn cmd_id(&mut self, other: &mut TlsClient) -> Reply {
        let listed = match other.cmd(&["CLIENT", "LIST"]) {
            Reply::Bulk(v) => String::from_utf8(v).unwrap(),
            other => panic!("{other:?}"),
        };
        let me = self.stream().sock.local_addr().unwrap().to_string();
        let line = listed
            .lines()
            .find(|line| line.contains(&format!("addr={me} ")))
            .unwrap_or_else(|| panic!("{me} is not in {listed}"));
        let id = line
            .split_whitespace()
            .find_map(|part| part.strip_prefix("id="))
            .unwrap();
        Reply::Int(id.parse().unwrap())
    }
}

#[test]
fn replies_wait_for_the_disk_over_tls_too() {
    watchdog("tls-always", 60, || {
        let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("tls");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("always-{}.hc", std::process::id()));
        for suffix in ["", ".lock", ".rewrite"] {
            let mut name = path.clone().into_os_string();
            name.push(suffix);
            let _ = std::fs::remove_file(name);
        }
        let options = PersistOptions {
            interval: Duration::from_secs(3600),
            fsync: Fsync::Always,
            ..PersistOptions::default()
        };
        let db = Db::open_with(Some(&path), engine_cfg(2), options).unwrap();
        let server = db.listen(tls_cfg(None)).unwrap();
        let mut c =
            TlsClient::connect(server.tls_addr().unwrap(), &client_config(None, &[])).unwrap();
        let mut pipeline = Vec::new();
        for i in 0..300 {
            pipeline.extend(wire_str(&["SET", &format!("k{i}"), "v"]));
        }
        c.send_raw(&pipeline);
        for _ in 0..300 {
            ok(c.read());
        }
        let copy = dir.join(format!("always-copy-{}.hc", std::process::id()));
        let _ = std::fs::remove_file(&copy);
        std::fs::copy(&path, &copy).unwrap();
        let seen = Db::open(Some(&copy), engine_cfg(2)).unwrap();
        assert_eq!(
            seen.session().unwrap().execute(&["DBSIZE"]),
            Reply::Int(300)
        );
    });
}

// ---------------------------------------------------------------------
// Peers that do not behave
// ---------------------------------------------------------------------

/// What comes back from the TLS port for bytes that are not TLS: never a
/// RESP reply, and the connection ends.
fn turned_away(addr: SocketAddr, bytes: &[u8]) {
    let mut s = connect_stream(addr);
    s.set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    // The server may close while this is still writing.
    let _ = s.write_all(bytes);
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(e) if is_closed(&e) => break,
            Err(e) => panic!("{} bytes in, still open: {e}", bytes.len()),
        }
    }
    for leak in [&b"PONG"[..], b"+OK", b"-ERR", b"secret-value"] {
        assert!(
            !got.windows(leak.len()).any(|w| w == leak),
            "the TLS port answered in the clear: {:?}",
            String::from_utf8_lossy(&got)
        );
    }
}

#[test]
fn what_is_not_tls_is_turned_away() {
    watchdog("tls-garbage", 120, || {
        let t = TlsEnv::new(tls_cfg(None));
        let mut c = t.client();
        ok(c.cmd(&["SET", "s", "secret-value"]));
        turned_away(t.tls, b"PING\r\n");
        turned_away(t.tls, b"*2\r\n$3\r\nGET\r\n$1\r\ns\r\n");
        turned_away(t.tls, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
        turned_away(t.tls, &[0u8; 4096]);
        turned_away(t.tls, &[0xffu8; 70_000]);
        // A record header that promises more than a record may hold.
        turned_away(t.tls, &[0x16, 0x03, 0x03, 0xff, 0xff, 1, 2, 3]);
        let mut rng = hcache_core::testutil::Rng::new(3);
        for _ in 0..60 {
            let len = 1 + rng.below(3000) as usize;
            let mut junk = rng.bytes(len);
            if rng.chance(50) {
                // It starts like a handshake record, and goes wrong after.
                junk.splice(0..0, [0x16, 0x03, 0x01, 0x02, 0x00, 0x01]);
            }
            turned_away(t.tls, &junk);
        }
        // A real hello cut off at every few bytes, then the connection
        // dropped: nothing stays behind.
        let hello = {
            let name = ServerName::try_from("localhost").unwrap();
            let mut conn = ClientConnection::new(client_config(None, &[]), name).unwrap();
            let mut out = Vec::new();
            conn.write_tls(&mut out).unwrap();
            out
        };
        for cut in (1..hello.len()).step_by(23) {
            let mut s = connect_stream(t.tls);
            s.write_all(&hello[..cut]).unwrap();
        }
        // A hello, then garbage where the rest of the handshake belongs.
        let mut both = hello.clone();
        both.extend(rng.bytes(500));
        turned_away(t.tls, &both);
        assert_eq!(t.env.wait_conns(1, 15), 1);
        assert_eq!(c.cmd(&["GET", "s"]), b("secret-value"));
        // TLS spoken to the plain port is a protocol error there, not a
        // way in.
        let mut s = connect_stream(t.env.addr);
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.write_all(&hello).unwrap();
        let mut got = Vec::new();
        let _ = s.read_to_end(&mut got);
        assert!(!got.windows(4).any(|w| w == b"PONG"));
        assert_eq!(c.cmd(&["PING"]), pong());
    });
}

#[test]
fn a_tls_only_server_has_no_plain_door() {
    watchdog("tls-only", 60, || {
        let t = TlsEnv::new(ServerConfig {
            plain: false,
            ..tls_cfg(None)
        });
        assert_eq!(t.env.addr, t.tls);
        let mut c = t.client();
        assert_eq!(c.cmd(&["PING"]), pong());
        turned_away(t.tls, b"PING\r\n");
        assert_eq!(t.env.wait_conns(1, 5), 1);
    });
}

#[test]
fn a_server_with_nothing_to_listen_on_does_not_start() {
    let db = Db::open(None, engine_cfg(1)).unwrap();
    let e = db
        .listen(ServerConfig {
            plain: false,
            ..server_cfg(1, 16)
        })
        .err()
        .expect("no listener at all");
    assert!(format!("{e}").contains("nothing to listen on"), "{e}");
}

#[test]
fn a_handshake_has_to_finish_in_time() {
    watchdog("tls-handshake-timeout", 60, || {
        let t = TlsEnv::new(tls_cfg(None));
        let start = Instant::now();
        // Says nothing at all.
        let mut silent = connect_stream(t.tls);
        // Starts and never goes on.
        let mut half = connect_stream(t.tls);
        half.write_all(&[0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00])
            .unwrap();
        // Meanwhile others are served.
        let mut c = t.client();
        assert_eq!(c.cmd(&["PING"]), pong());
        assert_eq!(t.env.server.connections(), 3);
        for s in [&mut silent, &mut half] {
            s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
            let mut buf = [0u8; 64];
            loop {
                match s.read(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e) if is_closed(&e) => break,
                    Err(e) => panic!("still open after {:?}: {e}", start.elapsed()),
                }
            }
        }
        let took = start.elapsed();
        assert!(took >= Duration::from_secs(8), "closed after only {took:?}");
        assert!(took < Duration::from_secs(16), "closed after {took:?}");
        // A plain connection that says nothing has no such deadline.
        let quiet = t.env.client();
        assert_eq!(c.cmd(&["PING"]), pong());
        assert_eq!(t.env.wait_conns(2, 5), 2);
        drop(quiet);
    });
}

#[test]
fn clients_that_vanish_leave_nothing_behind() {
    watchdog("tls-vanish", 120, || {
        let t = TlsEnv::new(tls_cfg(None));
        let config = client_config(None, &[]);
        for round in 0..40 {
            let mut c = TlsClient::connect(t.tls, &config).unwrap();
            match round % 4 {
                // Gone with a request half sent.
                0 => c.send_raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100\r\nabc"),
                // Gone while waiting.
                1 => {
                    c.send(&["BLPOP", "nothing", "0"]);
                    thread::sleep(Duration::from_millis(20));
                }
                // Gone with megabytes on their way.
                2 => {
                    ok(c.cmd(&["SET", "big", &"x".repeat(1 << 20)]));
                    let mut pipeline = Vec::new();
                    for _ in 0..30 {
                        pipeline.extend(wire_str(&["GET", "big"]));
                    }
                    c.send_raw(&pipeline);
                }
                // Gone inside a transaction, subscribed.
                _ => {
                    ok(c.cmd(&["MULTI"]));
                    c.cmd(&["SET", "half", "1"]);
                }
            }
            // No goodbye: the socket just closes.
            drop(c);
        }
        assert_eq!(t.env.wait_conns(0, 20), 0);
        let mut c = t.client();
        assert_eq!(c.cmd(&["EXISTS", "half", "k"]), Reply::Int(0));
        assert_eq!(c.cmd(&["RPUSH", "nothing", "x"]), Reply::Int(1));
        assert_eq!(c.cmd(&["LLEN", "nothing"]), Reply::Int(1));
    });
}

#[test]
fn a_goodbye_from_the_client_ends_the_connection_after_its_requests() {
    watchdog("tls-close-notify", 60, || {
        let t = TlsEnv::new(tls_cfg(None));
        let mut c = t.client();
        let mut pipeline = wire_str(&["SET", "last", "words"]);
        pipeline.extend(wire_str(&["GET", "last"]));
        c.send_raw(&pipeline);
        let stream = c.stream();
        stream.conn.send_close_notify();
        stream.conn.complete_io(&mut stream.sock).unwrap();
        ok(c.read());
        assert_eq!(c.read(), b("words"));
        assert!(c.ending().is_ok());
        assert_eq!(t.env.wait_conns(0, 5), 0);
    });
}

#[test]
fn timeouts_apply_to_tls_connections() {
    watchdog("tls-timeouts", 90, || {
        let t = TlsEnv::new(ServerConfig {
            idle_timeout: Some(Duration::from_secs(1)),
            stall_timeout: Duration::from_secs(2),
            ..tls_cfg(None)
        });
        let mut idle = t.client();
        assert_eq!(idle.cmd(&["PING"]), pong());
        let mut busy = t.client();
        ok(busy.cmd(&["SET", "big", &"x".repeat(256 << 10)]));
        // Asks for a hundred megabytes and reads none of it. Kept alive
        // so that only the server can end it.
        let mut hog = t.client();
        let mut pipeline = Vec::new();
        for _ in 0..400 {
            pipeline.extend(wire_str(&["GET", "big"]));
        }
        let hog_sock = hog.stream().sock.try_clone().unwrap();
        let writer = thread::spawn(move || {
            let stream = hog.stream();
            let _ = stream.write_all(&pipeline);
            let _ = stream.flush();
            hog
        });
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(3500) {
            assert_eq!(busy.cmd(&["PING"]), pong());
            thread::sleep(Duration::from_millis(200));
        }
        // The idle one got a goodbye, not just a closed socket.
        assert_eq!(idle.ending(), Ok(true));
        assert_eq!(t.env.wait_conns(1, 15), 1, "the hog is still connected");
        let _ = hog_sock.shutdown(std::net::Shutdown::Both);
        drop(writer.join().unwrap());
        assert_eq!(busy.cmd(&["PING"]), pong());
    });
}

// ---------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------

/// Whether a client with this identity gets to run a command.
fn admitted(addr: SocketAddr, identity: Option<&str>) -> bool {
    // TLS 1.3 tells a client that its certificate was refused only after
    // the client's side of the handshake is done: the first command shows.
    let Ok(mut c) = TlsClient::connect(addr, &client_config(identity, &[])) else {
        return false;
    };
    let stream = c.stream();
    let mut out = Vec::new();
    hcache_core::resp::encode_command(&["PING"], &mut out);
    if stream.write_all(&out).is_err() || stream.flush().is_err() {
        return false;
    }
    matches!(c.try_read(), Ok(reply) if reply == pong())
}

#[test]
fn client_certificates_are_checked_when_asked_for() {
    watchdog("tls-mtls", 60, || {
        let required = TlsEnv::new(tls_cfg(Some(ClientCerts::Required)));
        assert!(admitted(required.tls, Some("client")));
        assert!(!admitted(required.tls, None));
        assert!(!admitted(required.tls, Some("stranger")));
        // A server certificate is not a client certificate.
        assert!(!admitted(required.tls, Some("server")));
        assert_eq!(required.env.wait_conns(0, 10), 0);
        // The plain port of the same server is its own door.
        assert_eq!(required.env.client().cmd(&["PING"]), pong());

        let optional = TlsEnv::new(tls_cfg(Some(ClientCerts::Optional)));
        assert!(admitted(optional.tls, Some("client")));
        assert!(admitted(optional.tls, None));
        // Shown and wrong is refused, even where none would do.
        assert!(!admitted(optional.tls, Some("stranger")));

        let none = TlsEnv::new(tls_cfg(None));
        assert!(admitted(none.tls, None));
        // Not asked for, so not looked at.
        assert!(admitted(none.tls, Some("stranger")));
    });
}

#[test]
fn a_client_that_trusts_another_authority_refuses_the_server() {
    let t = TlsEnv::new(tls_cfg(None));
    let mut roots = RootCertStore::empty();
    // The stranger's own certificate as the only trusted root.
    for cert in CertificateDer::pem_file_iter(fixture("stranger.crt")).unwrap() {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = Arc::new(
        ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    assert!(TlsClient::connect(t.tls, &config).is_err());
    assert_eq!(t.env.wait_conns(0, 10), 0);
    assert_eq!(t.client().cmd(&["PING"]), pong());
}

#[test]
fn bad_certificate_files_are_refused_with_a_reason_and_no_key() {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("tls-files");
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key) = (fixture("server.crt"), fixture("server.key"));
    let key_text = std::fs::read_to_string(&key).unwrap();
    let secret: String = key_text.lines().nth(1).unwrap().to_string();
    let reason = |cert: &std::path::Path, key: &std::path::Path, ca: Option<&std::path::Path>| {
        let e = server_config(cert, key, ca.map(|ca| (ca, ClientCerts::Required)))
            .err()
            .map(|e| e.to_string())
            .unwrap_or_else(|| panic!("accepted {} + {}", cert.display(), key.display()));
        assert!(!e.contains(&secret), "the error quotes the key: {e}");
        assert!(!e.is_empty());
        e
    };
    let missing = dir.join("missing.pem");
    assert!(reason(&missing, &key, None).contains("missing.pem"));
    assert!(reason(&cert, &missing, None).contains("missing.pem"));
    assert!(reason(&cert, &key, Some(&missing)).contains("missing.pem"));
    let empty = dir.join("empty.pem");
    std::fs::write(&empty, "").unwrap();
    reason(&empty, &key, None);
    reason(&cert, &empty, None);
    reason(&cert, &key, Some(&empty));
    // Swapped, mismatched, damaged.
    reason(&key, &cert, None);
    reason(&cert, &fixture("client.key"), None);
    let damaged = dir.join("damaged.key");
    std::fs::write(&damaged, key_text.replacen(&secret[..8], "AAAAAAAA", 1)).unwrap();
    reason(&cert, &damaged, None);
    let junk = dir.join("junk.pem");
    std::fs::write(
        &junk,
        "-----BEGIN CERTIFICATE-----\nbm90IGEgY2VydA==\n-----END CERTIFICATE-----\n",
    )
    .unwrap();
    reason(&cert, &key, Some(&junk));
    // And the good ones are good.
    server_config(&cert, &key, None).unwrap();
    server_config(
        &cert,
        &key,
        Some((&fixture("ca.crt"), ClientCerts::Optional)),
    )
    .unwrap();
}
