//! Helpers shared by the TCP and mixed integration tests: a blocking RESP
//! client with timeouts, a watchdog, process memory, and a command generator
//! that carries its own model of what every reply must be.
#![allow(dead_code)]

use hcache::{Config, Db, Reply, ServerConfig};
use hcache_core::resp::{encode_command, Limits};
use hcache_core::testutil::Rng;
use hcache_server::Server;
use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::{mpsc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

pub const IO_TIMEOUT: Duration = Duration::from_secs(20);

static HEAVY: Mutex<()> = Mutex::new(());

/// Tests that measure latency or memory take this so they do not run beside
/// each other (light tests may still run alongside).
pub fn heavy() -> MutexGuard<'static, ()> {
    HEAVY.lock().unwrap_or_else(|e| e.into_inner())
}

/// Run `f` on its own thread; a hang becomes a failed test instead of a
/// stuck run. A panic inside `f` is re-raised here.
pub fn watchdog<T: Send + 'static>(
    name: &'static str,
    secs: u64,
    f: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name(format!("wd-{name}"))
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
            let _ = tx.send(result);
        })
        .expect("spawn watchdog body");
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(Ok(v)) => v,
        Ok(Err(payload)) => std::panic::resume_unwind(payload),
        Err(_) => panic!("{name}: HANG, no result after {secs} s"),
    }
}

pub fn wait_until(secs: u64, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Committed private memory of this process, where the OS tells us.
#[cfg(windows)]
pub fn private_bytes() -> Option<usize> {
    #[repr(C)]
    struct Counters {
        cb: u32,
        page_faults: u32,
        peak_working_set: usize,
        working_set: usize,
        peak_paged_pool: usize,
        paged_pool: usize,
        peak_nonpaged_pool: usize,
        nonpaged_pool: usize,
        pagefile: usize,
        peak_pagefile: usize,
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> isize;
        fn K32GetProcessMemoryInfo(process: isize, counters: *mut Counters, cb: u32) -> i32;
    }
    let mut c = Counters {
        cb: std::mem::size_of::<Counters>() as u32,
        page_faults: 0,
        peak_working_set: 0,
        working_set: 0,
        peak_paged_pool: 0,
        paged_pool: 0,
        peak_nonpaged_pool: 0,
        nonpaged_pool: 0,
        pagefile: 0,
        peak_pagefile: 0,
    };
    // SAFETY: `c` is a properly sized, writable PROCESS_MEMORY_COUNTERS and
    // the pseudo handle from GetCurrentProcess is always valid.
    let ok = unsafe { K32GetProcessMemoryInfo(GetCurrentProcess(), &mut c, c.cb) };
    (ok != 0).then_some(c.pagefile)
}

#[cfg(not(windows))]
pub fn private_bytes() -> Option<usize> {
    None
}

// ---------------------------------------------------------------------
// Server setup
// ---------------------------------------------------------------------

pub fn server_cfg(workers: usize, pipeline_cap: usize) -> ServerConfig {
    ServerConfig {
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        workers,
        max_conns: 10_000,
        pipeline_cap,
        limits: Limits::default(),
        ..ServerConfig::default()
    }
}

pub fn engine_cfg(shards: usize) -> Config {
    Config {
        shards,
        ..Config::default()
    }
}

pub struct Env {
    pub db: Db,
    pub server: Server,
    pub addr: SocketAddr,
}

impl Env {
    pub fn new(cfg: Config, scfg: ServerConfig) -> Env {
        let db = Db::open(None, cfg).expect("open db");
        let server = db.listen(scfg).expect("listen");
        let addr = server.local_addr();
        Env { db, server, addr }
    }

    pub fn client(&self) -> Client {
        Client::connect(self.addr)
    }

    pub fn client_with(&self, t: Duration) -> Client {
        Client::with_timeout(self.addr, t)
    }

    /// Wait for `connections()` to reach `target`; returns the last value.
    pub fn wait_conns(&self, target: usize, secs: u64) -> usize {
        wait_until(secs, || self.server.connections() == target);
        self.server.connections()
    }
}

// ---------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------

pub fn connect_stream(addr: SocketAddr) -> TcpStream {
    let mut last = None;
    for _ in 0..40 {
        match TcpStream::connect(addr) {
            Ok(s) => return s,
            Err(e) => {
                last = Some(e);
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    panic!("cannot connect to {addr}: {last:?}");
}

/// `shutdown(Write)`. Windows can answer WSAENOTCONN (10057) for a socket
/// whose connect has just completed, so that one error is retried.
pub fn shutdown_write(s: &TcpStream) -> io::Result<()> {
    let mut tries = 0;
    loop {
        match s.shutdown(Shutdown::Write) {
            Err(e) if e.raw_os_error() == Some(10057) && tries < 100 => {
                tries += 1;
                thread::sleep(Duration::from_millis(2));
            }
            other => return other,
        }
    }
}

pub fn is_closed(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

pub fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn read_line<R: BufRead>(r: &mut R, line: &mut Vec<u8>) -> io::Result<()> {
    line.clear();
    let n = r.read_until(b'\n', line)?;
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
    }
    if line.len() < 3 || !line.ends_with(b"\r\n") {
        return Err(invalid(format!("reply line not CRLF terminated: {line:?}")));
    }
    Ok(())
}

fn parse_int(body: &[u8]) -> io::Result<i64> {
    std::str::from_utf8(body)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| invalid(format!("bad integer {body:?}")))
}

/// Streaming reply reader: never rescans, so a 1M-element array costs 1M
/// elements, not 1M squared.
pub fn read_reply<R: BufRead>(r: &mut R) -> io::Result<Reply> {
    let mut line = Vec::with_capacity(32);
    read_line(r, &mut line)?;
    let body = &line[1..line.len() - 2];
    match line[0] {
        b'+' if body == b"OK" => Ok(Reply::Ok),
        b'+' => Ok(Reply::Simple(String::from_utf8_lossy(body).into_owned())),
        b'-' => Ok(Reply::Error(String::from_utf8_lossy(body).into_owned())),
        b':' => Ok(Reply::Int(parse_int(body)?)),
        b'$' => {
            let n = parse_int(body)?;
            if n < 0 {
                return Ok(Reply::Nil);
            }
            let n = n as usize;
            let mut buf = vec![0u8; n + 2];
            r.read_exact(&mut buf)?;
            if !buf.ends_with(b"\r\n") {
                return Err(invalid("bulk not CRLF terminated"));
            }
            buf.truncate(n);
            Ok(Reply::Bulk(buf))
        }
        b'*' => {
            let n = parse_int(body)?;
            if n < 0 {
                return Ok(Reply::NilArray);
            }
            let mut items = Vec::with_capacity((n as usize).min(4096));
            for _ in 0..n {
                items.push(read_reply(r)?);
            }
            Ok(Reply::Array(items))
        }
        other => Err(invalid(format!("unknown reply type byte {other:#04x}"))),
    }
}

pub fn wire(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_command(args, &mut out);
    out
}

pub fn wire_str(args: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_command(args, &mut out);
    out
}

pub struct Client {
    pub rd: BufReader<TcpStream>,
    pub wr: TcpStream,
    timeout: Duration,
}

impl Client {
    pub fn connect(addr: SocketAddr) -> Client {
        Client::with_timeout(addr, IO_TIMEOUT)
    }

    pub fn with_timeout(addr: SocketAddr, t: Duration) -> Client {
        Client::from_stream(connect_stream(addr), t)
    }

    pub fn from_stream(s: TcpStream, t: Duration) -> Client {
        s.set_read_timeout(Some(t)).expect("read timeout");
        s.set_write_timeout(Some(t)).expect("write timeout");
        let _ = s.set_nodelay(true);
        let wr = s.try_clone().expect("clone socket");
        Client {
            rd: BufReader::with_capacity(256 << 10, s),
            wr,
            timeout: t,
        }
    }

    pub fn set_timeout(&mut self, t: Duration) {
        self.timeout = t;
        self.rd
            .get_ref()
            .set_read_timeout(Some(t))
            .expect("read timeout");
    }

    pub fn send<A: AsRef<[u8]>>(&mut self, args: &[A]) {
        let mut b = Vec::new();
        encode_command(args, &mut b);
        self.send_raw(&b);
    }

    pub fn send_raw(&mut self, bytes: &[u8]) {
        self.wr.write_all(bytes).expect("client write");
    }

    pub fn try_send_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.wr.write_all(bytes)
    }

    pub fn try_read(&mut self) -> io::Result<Reply> {
        read_reply(&mut self.rd)
    }

    pub fn read(&mut self) -> Reply {
        self.try_read()
            .unwrap_or_else(|e| panic!("reading a reply: {e}"))
    }

    pub fn cmd_raw(&mut self, bytes: &[u8]) -> Reply {
        self.send_raw(bytes);
        self.read()
    }

    pub fn cmd<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Reply {
        self.send(args);
        self.read()
    }

    pub fn half_close(&mut self) {
        shutdown_write(&self.wr).expect("shutdown(Write)");
    }

    /// True when nothing (no data, no EOF) arrives within `d`.
    pub fn silent_for(&mut self, millis: u64) -> bool {
        self.set_timeout(Duration::from_millis(millis));
        let silent = match self.rd.fill_buf() {
            Ok(_) => false,
            Err(e) => is_timeout(&e),
        };
        let t = IO_TIMEOUT;
        self.set_timeout(t);
        silent
    }

    /// Read replies until the server closes the connection. Panics when it
    /// does not close in time or when the reply stream is not valid RESP.
    pub fn read_until_closed(&mut self) -> Vec<Reply> {
        let mut out = Vec::new();
        loop {
            match self.try_read() {
                Ok(r) => out.push(r),
                Err(e) if is_closed(&e) => return out,
                Err(e) => panic!(
                    "expected the server to close the connection; got `{e}` after {} replies",
                    out.len()
                ),
            }
        }
    }

    pub fn read_expecting(&mut self, expects: &[Expect], who: &str) {
        check_replies(&mut self.rd, expects, who);
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

pub fn check_replies<R: BufRead>(rd: &mut R, expects: &[Expect], who: &str) {
    for (i, want) in expects.iter().enumerate() {
        let got = match read_reply(rd) {
            Ok(r) => r,
            Err(e) => panic!("{who}: reply {i} of {} failed: {e}", expects.len()),
        };
        if !want.matches(&got) {
            panic!(
                "{who}: reply {i} of {} differs\n  want {}\n  got  {}",
                expects.len(),
                trunc(format!("{want:?}")),
                short(&got)
            );
        }
    }
}

pub fn trunc(mut s: String) -> String {
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

pub fn short(r: &Reply) -> String {
    match r {
        Reply::Bulk(b) => format!("Bulk(len={}, head={:?})", b.len(), &b[..b.len().min(24)]),
        Reply::Array(a) => format!("Array(len={})", a.len()),
        other => trunc(format!("{other:?}")),
    }
}

pub fn error_text(r: &Reply) -> Option<&str> {
    match r {
        Reply::Error(s) => Some(s),
        _ => None,
    }
}

// ---------------------------------------------------------------------
// Model-checked workload
// ---------------------------------------------------------------------

#[derive(Debug)]
pub enum Expect {
    Exact(Reply),
    /// An array of bulk strings, in any order (KEYS).
    Unordered(Vec<Vec<u8>>),
    /// A flat field/value array, pairs in any order (HGETALL).
    Pairs(Vec<(Vec<u8>, Vec<u8>)>),
}

impl Expect {
    pub fn matches(&self, got: &Reply) -> bool {
        match self {
            Expect::Exact(want) => want == got,
            Expect::Unordered(want) => {
                let Reply::Array(items) = got else {
                    return false;
                };
                let mut have = Vec::with_capacity(items.len());
                for item in items {
                    let Reply::Bulk(b) = item else { return false };
                    have.push(b.clone());
                }
                let mut want = want.clone();
                have.sort();
                want.sort();
                have == want
            }
            Expect::Pairs(want) => {
                let Reply::Array(items) = got else {
                    return false;
                };
                if items.len() % 2 != 0 {
                    return false;
                }
                let mut have = Vec::with_capacity(items.len() / 2);
                for pair in items.as_chunks::<2>().0 {
                    match pair {
                        [Reply::Bulk(f), Reply::Bulk(v)] => have.push((f.clone(), v.clone())),
                        _ => return false,
                    }
                }
                let mut want = want.clone();
                have.sort();
                want.sort();
                have == want
            }
        }
    }
}

pub const S_GET: &str = "return redis.call('GET', KEYS[1])";
pub const S_SET_INCR: &str =
    "redis.call('SET', KEYS[1], ARGV[1]) return redis.call('INCR', KEYS[2])";
pub const S_MGET: &str = "return redis.call('MGET', KEYS[1], KEYS[2])";

const KEY_UNIVERSE: u64 = 24;
const FIELD_UNIVERSE: u64 = 40;
const HASH_UNIVERSE: u64 = 3;
const CTR_UNIVERSE: u64 = 4;

/// Random commands over one connection's private key prefix, with the
/// expected reply of each worked out by a model. Keys of different
/// connections never overlap, so connections can run concurrently.
pub struct Workload {
    rng: Rng,
    prefix: String,
    kv: HashMap<Vec<u8>, Vec<u8>>,
    ctr: HashMap<Vec<u8>, i64>,
    hashes: HashMap<Vec<u8>, BTreeMap<Vec<u8>, Vec<u8>>>,
    pub allow_flush: bool,
    pub allow_eval: bool,
    pub max_value: u64,
}

type Parts = Vec<Vec<u8>>;

fn parts(items: &[&str]) -> Parts {
    items.iter().map(|s| s.as_bytes().to_vec()).collect()
}

impl Workload {
    pub fn new(seed: u64, prefix: &str) -> Workload {
        Workload {
            rng: Rng::new(seed),
            prefix: prefix.to_string(),
            kv: HashMap::new(),
            ctr: HashMap::new(),
            hashes: HashMap::new(),
            allow_flush: false,
            allow_eval: true,
            max_value: 600,
        }
    }

    fn key(&mut self) -> Vec<u8> {
        format!("{}k{}", self.prefix, self.rng.below(KEY_UNIVERSE)).into_bytes()
    }

    fn ctr_key(&mut self) -> Vec<u8> {
        format!("{}c{}", self.prefix, self.rng.below(CTR_UNIVERSE)).into_bytes()
    }

    fn hash_key(&mut self) -> Vec<u8> {
        format!("{}h{}", self.prefix, self.rng.below(HASH_UNIVERSE)).into_bytes()
    }

    fn field(&mut self) -> Vec<u8> {
        format!("f{}", self.rng.below(FIELD_UNIVERSE)).into_bytes()
    }

    fn value(&mut self) -> Vec<u8> {
        let len = if self.rng.chance(40) {
            self.rng.below(self.max_value)
        } else {
            self.rng.below(40)
        };
        self.rng.bytes(len as usize)
    }

    fn keys(&mut self, max: u64) -> Vec<Vec<u8>> {
        let n = 1 + self.rng.below(max);
        (0..n).map(|_| self.key()).collect()
    }

    fn lookup(&self, k: &[u8]) -> Reply {
        match self.kv.get(k) {
            Some(v) => Reply::Bulk(v.clone()),
            None => Reply::Nil,
        }
    }

    fn all_keys(&self) -> Vec<Vec<u8>> {
        self.kv
            .keys()
            .chain(self.ctr.keys())
            .chain(self.hashes.keys())
            .cloned()
            .collect()
    }

    /// The next command (as its arguments) and what the server must answer.
    pub fn next_op(&mut self) -> (Parts, Expect) {
        use Expect::{Exact, Pairs, Unordered};
        match self.rng.below(24) {
            0 => (parts(&["PING"]), Exact(Reply::Simple("PONG".into()))),
            1 => {
                let m = self.value();
                (vec![b"PING".to_vec(), m.clone()], Exact(Reply::Bulk(m)))
            }
            2 => {
                let m = self.value();
                (vec![b"ECHO".to_vec(), m.clone()], Exact(Reply::Bulk(m)))
            }
            3..=5 => {
                let (k, v) = (self.key(), self.value());
                self.kv.insert(k.clone(), v.clone());
                (vec![b"SET".to_vec(), k, v], Exact(Reply::Ok))
            }
            6..=8 => {
                let k = self.key();
                let want = self.lookup(&k);
                (vec![b"GET".to_vec(), k], Exact(want))
            }
            9 | 10 => {
                let k = self.ctr_key();
                let n = self.ctr.entry(k.clone()).or_insert(0);
                *n += 1;
                let want = Reply::Int(*n);
                (vec![b"INCR".to_vec(), k], Exact(want))
            }
            11 => {
                let keys = self.keys(3);
                let n = keys.iter().filter(|k| self.kv.remove(*k).is_some()).count();
                let mut p = vec![b"DEL".to_vec()];
                p.extend(keys);
                (p, Exact(Reply::Int(n as i64)))
            }
            12 => {
                let keys = self.keys(3);
                let n = keys.iter().filter(|k| self.kv.contains_key(*k)).count();
                let mut p = vec![b"EXISTS".to_vec()];
                p.extend(keys);
                (p, Exact(Reply::Int(n as i64)))
            }
            13 => {
                let n = 1 + self.rng.below(4);
                let mut p = vec![b"MSET".to_vec()];
                for _ in 0..n {
                    let (k, v) = (self.key(), self.value());
                    self.kv.insert(k.clone(), v.clone());
                    p.push(k);
                    p.push(v);
                }
                (p, Exact(Reply::Ok))
            }
            14 => {
                let keys = self.keys(5);
                let want = Reply::Array(keys.iter().map(|k| self.lookup(k)).collect());
                let mut p = vec![b"MGET".to_vec()];
                p.extend(keys);
                (p, Exact(want))
            }
            15 => (
                vec![b"KEYS".to_vec(), format!("{}*", self.prefix).into_bytes()],
                Unordered(self.all_keys()),
            ),
            16 | 17 => {
                let k = self.hash_key();
                let n = 1 + self.rng.below(3);
                let mut p = vec![b"HSET".to_vec(), k.clone()];
                let mut added = 0;
                for _ in 0..n {
                    let (f, v) = (self.field(), self.value());
                    let h = self.hashes.entry(k.clone()).or_default();
                    if h.insert(f.clone(), v.clone()).is_none() {
                        added += 1;
                    }
                    p.push(f);
                    p.push(v);
                }
                (p, Exact(Reply::Int(added)))
            }
            18 => {
                let k = self.hash_key();
                let pairs: Vec<(Vec<u8>, Vec<u8>)> = self
                    .hashes
                    .get(&k)
                    .map(|h| h.iter().map(|(f, v)| (f.clone(), v.clone())).collect())
                    .unwrap_or_default();
                (vec![b"HGETALL".to_vec(), k], Pairs(pairs))
            }
            19 => {
                let k = self.hash_key();
                let n = self.hashes.get(&k).map_or(0, BTreeMap::len);
                (vec![b"HLEN".to_vec(), k], Exact(Reply::Int(n as i64)))
            }
            20 => {
                let k = self.hash_key();
                let fields: Vec<Vec<u8>> =
                    (0..1 + self.rng.below(3)).map(|_| self.field()).collect();
                let mut removed = 0;
                if let Some(h) = self.hashes.get_mut(&k) {
                    for f in &fields {
                        if h.remove(f).is_some() {
                            removed += 1;
                        }
                    }
                    if h.is_empty() {
                        self.hashes.remove(&k);
                    }
                }
                let mut p = vec![b"HDEL".to_vec(), k];
                p.extend(fields);
                (p, Exact(Reply::Int(removed)))
            }
            21 if self.allow_eval => match self.rng.below(3) {
                0 => {
                    let k = self.key();
                    let want = self.lookup(&k);
                    (
                        vec![b"EVAL".to_vec(), S_GET.into(), b"1".to_vec(), k],
                        Exact(want),
                    )
                }
                1 => {
                    let (k, c, v) = (self.key(), self.ctr_key(), self.value());
                    self.kv.insert(k.clone(), v.clone());
                    let n = self.ctr.entry(c.clone()).or_insert(0);
                    *n += 1;
                    let want = Reply::Int(*n);
                    (
                        vec![b"EVAL".to_vec(), S_SET_INCR.into(), b"2".to_vec(), k, c, v],
                        Exact(want),
                    )
                }
                _ => {
                    let (a, b) = (self.key(), self.key());
                    let want = Reply::Array(vec![self.lookup(&a), self.lookup(&b)]);
                    (
                        vec![b"EVAL".to_vec(), S_MGET.into(), b"2".to_vec(), a, b],
                        Exact(want),
                    )
                }
            },
            22 => {
                if self.rng.chance(2) {
                    (
                        parts(&["NOSUCHCMD", "x"]),
                        Exact(Reply::err(
                            "ERR unknown command 'NOSUCHCMD', with args beginning with: 'x' ",
                        )),
                    )
                } else {
                    (
                        parts(&["GET"]),
                        Exact(Reply::err(
                            "ERR wrong number of arguments for 'get' command",
                        )),
                    )
                }
            }
            23 if self.allow_flush && self.rng.chance(40) => {
                self.kv.clear();
                self.ctr.clear();
                self.hashes.clear();
                (parts(&["FLUSHALL"]), Exact(Reply::Ok))
            }
            _ => {
                let k = self.key();
                let want = self.lookup(&k);
                (vec![b"GET".to_vec(), k], Exact(want))
            }
        }
    }

    /// `n` commands as one buffer, with their expected replies.
    pub fn batch(&mut self, n: usize) -> (Vec<u8>, Vec<Expect>) {
        let mut out = Vec::new();
        let mut expects = Vec::with_capacity(n);
        for _ in 0..n {
            let (p, e) = self.next_op();
            let refs: Vec<&[u8]> = p.iter().map(Vec::as_slice).collect();
            encode_command(&refs, &mut out);
            expects.push(e);
        }
        (out, expects)
    }
}

/// Write `bytes` from a helper thread (so a server that stops reading
/// because we are not reading cannot deadlock us), half-closing afterwards
/// when asked.
pub fn spawn_writer(
    mut wr: TcpStream,
    bytes: Vec<u8>,
    half_close: bool,
) -> thread::JoinHandle<io::Result<()>> {
    thread::spawn(move || {
        let r = wr.write_all(&bytes);
        if half_close {
            shutdown_write(&wr)?;
        }
        r
    })
}

pub fn join_writer(h: thread::JoinHandle<io::Result<()>>, who: &str) {
    match h.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("{who}: writer failed: {e}"),
        Err(_) => panic!("{who}: writer thread panicked"),
    }
}

/// Kills any script still running when dropped, so a failed assertion in a
/// script test cannot leave the server unable to shut down.
pub struct KillScriptsOnDrop(pub Db);

impl Drop for KillScriptsOnDrop {
    fn drop(&mut self) {
        let Ok(session) = self.0.session() else {
            return;
        };
        for _ in 0..80 {
            let r = session.execute(&["SCRIPT", "KILL"]);
            if matches!(&r, Reply::Error(e) if e.starts_with("NOTBUSY")) {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum End {
    Closed,
    TimedOut,
}

pub struct Flood {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: thread::JoinHandle<End>,
}

impl Flood {
    /// Stop sending, half-close, and wait for the server to finish and close.
    pub fn stop_and_join(self) -> End {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        self.handle.join().expect("flood thread")
    }

    pub fn into_handle(self) -> thread::JoinHandle<End> {
        self.handle
    }
}

/// A client that pipelines `batch` over and over and reads (and discards)
/// everything, until stopped or until the server closes the connection.
pub fn flood(addr: SocketAddr, batch: Vec<u8>, timeout: Duration) -> Flood {
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    use std::sync::Arc;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = stop.clone();
    let handle = thread::spawn(move || {
        let s = connect_stream(addr);
        s.set_read_timeout(Some(timeout)).expect("read timeout");
        s.set_write_timeout(Some(timeout)).expect("write timeout");
        let mut w = s.try_clone().expect("clone socket");
        let stop_w = stop_t.clone();
        let writer = thread::spawn(move || {
            while !stop_w.load(SeqCst) {
                if w.write_all(&batch).is_err() {
                    break;
                }
            }
            let _ = shutdown_write(&w);
        });
        let mut r = s;
        let mut buf = vec![0u8; 64 * 1024];
        let end = loop {
            match r.read(&mut buf) {
                Ok(0) => break End::Closed,
                Ok(_) => {}
                Err(e) if is_timeout(&e) => break End::TimedOut,
                Err(_) => break End::Closed,
            }
        };
        stop_t.store(true, SeqCst);
        let _ = r.shutdown(Shutdown::Both);
        let _ = writer.join();
        end
    });
    Flood { stop, handle }
}

/// One pipeline round of mixed work for `flood`.
pub fn flood_batch() -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..20 {
        let key = format!("fl{i}");
        encode_command(&["PING"], &mut out);
        encode_command(&["SET", key.as_str(), "value"], &mut out);
        encode_command(&["GET", key.as_str()], &mut out);
        encode_command(&["INCR", "flood-counter"], &mut out);
        encode_command(&["MGET", "fl1", "fl2", "fl3"], &mut out);
    }
    encode_command(&["KEYS", "fl*"], &mut out);
    encode_command(
        &["EVAL", "return redis.call('GET', KEYS[1])", "1", "fl1"],
        &mut out,
    );
    out
}
