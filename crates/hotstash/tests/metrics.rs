//! The metrics port: INFO in the Prometheus text format under the names
//! redis_exporter gives, the users of RESP as HTTP Basic, and the answers
//! to requests it does not serve.

mod net;

use hotstash::{Reply, ServerConfig};
use net::{engine_cfg, server_cfg, wait_until, Env, IO_TIMEOUT};
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

fn env_with(scfg: ServerConfig) -> (Env, SocketAddr) {
    let env = Env::new(
        engine_cfg(4),
        ServerConfig {
            metrics: Some(SocketAddr::from(([127, 0, 0, 1], 0))),
            ..scfg
        },
    );
    let addr = env.server.metrics_addr().expect("a metrics port");
    (env, addr)
}

fn env() -> (Env, SocketAddr) {
    env_with(server_cfg(2, 64))
}

/// The status line, the headers and the body of the answer to `request`.
fn http(addr: SocketAddr, request: &[u8]) -> (String, String, String) {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(IO_TIMEOUT)).expect("read timeout");
    s.write_all(request).expect("write");
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).expect("read to the end");
    let raw = String::from_utf8(raw).expect("UTF-8");
    let (head, body) = raw.split_once("\r\n\r\n").expect("a head");
    let (status, headers) = head.split_once("\r\n").unwrap_or((head, ""));
    (status.into(), headers.into(), body.into())
}

fn get(addr: SocketAddr, auth: Option<&str>) -> (String, String, String) {
    let auth = auth.map_or(String::new(), |a| {
        format!("Authorization: Basic {}\r\n", base64(a))
    });
    let request = format!("GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: text/plain\r\n{auth}\r\n");
    http(addr, request.as_bytes())
}

fn scrape(addr: SocketAddr) -> String {
    let (status, _, body) = get(addr, None);
    assert_eq!(status, "HTTP/1.1 200 OK");
    body
}

/// The value of the sample `series` (name and labels as written).
fn sample(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .map(|v| v.parse().expect("a number"))
}

fn base64(s: &str) -> String {
    const ABC: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in s.as_bytes().chunks(3) {
        let b = |i: usize| u32::from(chunk.get(i).copied().unwrap_or(0));
        let n = (b(0) << 16) | (b(1) << 8) | b(2);
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            match i <= chunk.len() {
                true => out.push(char::from(ABC[(n >> shift) as usize & 63])),
                false => out.push('='),
            }
        }
    }
    out
}

/// The text is well formed: each family's HELP and TYPE come once, before
/// its samples and all of them together; each sample is a name, labels
/// in braces maybe, and a number.
fn well_formed(text: &str) {
    let mut done = HashSet::new();
    let mut current = String::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let name = rest.split(' ').next().expect("a name");
            assert!(done.insert(name.to_string()), "{name} twice");
            current = name.to_string();
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').expect("a name and a type");
            assert_eq!(name, current, "TYPE out of place");
            assert!(kind == "gauge" || kind == "counter", "{line}");
            continue;
        }
        let (series, value) = line.rsplit_once(' ').expect("a value");
        let name = series.split('{').next().expect("a name");
        assert_eq!(name, current, "{line} away from its family");
        assert!(
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':'),
            "{line}"
        );
        if series.len() > name.len() {
            assert!(series.ends_with('}'), "{line}");
        }
        assert!(value.parse::<f64>().is_ok(), "{line}");
    }
}

#[test]
fn a_scrape_gives_info_under_redis_exporter_names() {
    let (env, addr) = env();
    let mut c = env.client();
    for i in 0..5 {
        assert_eq!(c.cmd(&["SET", &format!("k{i}"), "v"]), Reply::Ok);
    }
    c.cmd(&["GET", "k1"]);
    c.cmd(&["GET", "k2"]);
    c.cmd(&["GET", "nope"]);
    assert_eq!(c.cmd(&["SET", "brief", "v", "PX", "1"]), Reply::Ok);
    std::thread::sleep(Duration::from_millis(20));
    c.cmd(&["GET", "brief"]);
    // A read does not delete the key; the sweep that does counts it.
    let expired = |text: &str| sample(text, "redis_expired_keys_total");
    assert!(wait_until(5, || expired(&scrape(addr)) == Some(1.0)));

    let (status, headers, text) = get(addr, None);
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(
        headers.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"),
        "{headers}"
    );
    well_formed(&text);
    for (series, at_least) in [
        ("redis_up", 1.0),
        ("redis_db_keys{db=\"db0\"}", 5.0),
        ("redis_keyspace_hits_total", 2.0),
        ("redis_keyspace_misses_total", 2.0),
        ("redis_expired_keys_total", 1.0),
        ("redis_commands_processed_total", 10.0),
        ("redis_connections_received_total", 1.0),
        ("redis_connected_clients", 1.0),
        ("redis_max_clients", 1.0),
        ("redis_uptime_in_seconds", 0.0),
        ("redis_start_time_seconds", 1.7e9),
        // Counted by the allocator of hotstash-server, which a test has not.
        ("redis_memory_used_bytes", 0.0),
        ("redis_connected_slaves", 0.0),
        ("redis_rdb_last_bgsave_status", 1.0),
    ] {
        let got = sample(&text, series).unwrap_or_else(|| panic!("no {series} in\n{text}"));
        assert!(got >= at_least, "{series} {got} < {at_least}");
    }
    assert_eq!(sample(&text, "redis_db_keys{db=\"db0\"}"), Some(5.0));
    assert!(
        text.contains("# TYPE redis_keyspace_hits_total counter"),
        "{text}"
    );
    assert!(
        text.contains("# TYPE redis_connected_clients gauge"),
        "{text}"
    );
    let info = text
        .lines()
        .find(|l| l.starts_with("redis_instance_info{"))
        .expect("instance_info");
    assert!(info.contains("role=\"master\""), "{info}");
    assert!(info.contains("redis_mode=\"standalone\""), "{info}");
    assert!(info.ends_with("} 1"), "{info}");
}

#[test]
fn turned_away_connections_are_counted() {
    let (env, addr) = env_with(ServerConfig {
        max_conns: 1,
        ..server_cfg(1, 64)
    });
    let mut first = env.client();
    assert_eq!(first.cmd(&["PING"]), Reply::Simple("PONG".into()));
    let mut second = env.client();
    assert!(matches!(second.read(), Reply::Error(e) if e.contains("max number of clients")));
    let text = scrape(addr);
    assert_eq!(sample(&text, "redis_rejected_connections_total"), Some(1.0));
    assert_eq!(sample(&text, "redis_max_clients"), Some(1.0));
}

#[test]
fn only_get_and_head_of_metrics_are_served() {
    let (_env, addr) = env();
    let (status, headers, body) = http(addr, b"HEAD /metrics HTTP/1.1\r\n\r\n");
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(headers.contains("Content-Length: "), "{headers}");
    assert_eq!(body, "");
    let (status, _, _) = http(addr, b"GET /metrics?x=1 HTTP/1.0\n\n");
    assert_eq!(status, "HTTP/1.1 200 OK");
    let (status, _, _) = http(addr, b"GET / HTTP/1.1\r\n\r\n");
    assert_eq!(status, "HTTP/1.1 404 Not Found");
    let (status, headers, _) = http(addr, b"POST /metrics HTTP/1.1\r\n\r\n");
    assert_eq!(status, "HTTP/1.1 405 Method Not Allowed");
    assert!(headers.contains("Allow: GET, HEAD"), "{headers}");
    let (status, _, _) = http(addr, b"hello\r\n\r\n");
    assert_eq!(status, "HTTP/1.1 400 Bad Request");
    let (status, _, _) = http(addr, b"GET /metrics SPDY/3\r\n\r\n");
    assert_eq!(status, "HTTP/1.1 400 Bad Request");
    let mut long = b"GET /metrics HTTP/1.1\r\nX: ".to_vec();
    long.extend(std::iter::repeat_n(b'a', 9000));
    let (status, _, _) = http(addr, &long);
    assert_eq!(status, "HTTP/1.1 431 Request Header Fields Too Large");
}

#[test]
fn a_silent_client_is_dropped_and_others_are_served_meanwhile() {
    let (_env, addr) = env();
    let mut silent = TcpStream::connect(addr).expect("connect");
    silent
        .set_read_timeout(Some(IO_TIMEOUT))
        .expect("read timeout");
    scrape(addr);
    let mut rest = Vec::new();
    assert!(silent.read_to_end(&mut rest).is_ok(), "closed, not reset");
    assert!(rest.is_empty());
}

#[test]
fn with_a_password_the_port_needs_a_user_allowed_info() {
    let (env, addr) = env();
    let mut admin = env.client();
    let ok = |r: Reply| assert_eq!(r, Reply::Ok);
    ok(admin.cmd(&["ACL", "SETUSER", "prom", "on", ">scrape", "-@all", "+info"]));
    ok(admin.cmd(&["ACL", "SETUSER", "app", "on", ">work", "-@all", "+get"]));
    ok(admin.cmd(&["CONFIG", "SET", "requirepass", "door"]));

    let (status, headers, _) = get(addr, None);
    assert_eq!(status, "HTTP/1.1 401 Unauthorized");
    assert!(
        headers.contains("WWW-Authenticate: Basic realm=\"hotstash\""),
        "{headers}"
    );
    assert_eq!(
        get(addr, Some("default:wrong")).0,
        "HTTP/1.1 401 Unauthorized"
    );
    assert_eq!(
        get(addr, Some("nobody:door")).0,
        "HTTP/1.1 401 Unauthorized"
    );
    assert_eq!(get(addr, Some("default")).0, "HTTP/1.1 401 Unauthorized");
    assert_eq!(get(addr, Some("app:work")).0, "HTTP/1.1 403 Forbidden");
    let (status, _, text) = get(addr, Some("prom:scrape"));
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert_eq!(sample(&text, "redis_up"), Some(1.0));
    assert_eq!(get(addr, Some("default:door")).0, "HTTP/1.1 200 OK");

    // Turned off, the user is turned away at once.
    let mut admin = env.client();
    ok(admin.cmd(&["AUTH", "door"]));
    ok(admin.cmd(&["ACL", "SETUSER", "prom", "off"]));
    assert!(wait_until(5, || get(addr, Some("prom:scrape"))
        .0
        .contains("401")));
}
