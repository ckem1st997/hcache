//! The Memcached text protocol on a port of its own (`--memcached-port`):
//! the classic commands and the meta ones (mg, ms, md, ma, mn), over the
//! keys of database 0. The users are RESP's, and what it writes goes to
//! the replicas and the data file as RESP's writes do.
//!
//! What it stores is what RESP reads: the value as sent when its client
//! flags are 0, else an 8-byte header first (`FLAGS_MAGIC`, then the flags
//! big-endian). A CAS value is a hash of what is stored, so the same bytes
//! written again keep it. Each connection has a thread of its own.

use crate::Shared;
use hotstash_core::acl::User;
use hotstash_core::cmd::Cmd;
use hotstash_core::resp::Reply;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed, Ordering::SeqCst};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use xxhash_rust::xxh3::xxh3_64;

/// The memcached whose replies this port gives (`version`, `stats`).
const VERSION: &str = "1.6.24";
/// The longest key, as memcached.
const KEY_MAX: usize = 250;
/// The largest value, memcached's default (`-I 1m`).
const ITEM_MAX: usize = 1 << 20;
/// The longest login, `<user> <password>`, as memcached.
const AUTH_MAX: usize = 16384;
/// Before the login, memcached hangs up on a line shorter than this, its
/// `\r` counted, and on one longer than `LOGIN_LINE_MAX`.
const LOGIN_LINE_MIN: usize = 6;
const LOGIN_LINE_MAX: usize = 2048;
/// A connection that must log in and has not for this long is closed, so
/// ones that never do cannot take every slot. memcached has no such limit.
const LOGIN_WAIT: Duration = Duration::from_secs(10);
/// Exptimes up to this many seconds are relative; larger ones a Unix time.
const RELATIVE_MAX: i64 = 60 * 60 * 24 * 30;
/// The latest Unix time an exptime may name; later ones are cut to it.
const UNIX_MAX: i64 = 1 << 40;
/// The longest opaque token, `O` included, as memcached.
const OPAQUE_MAX: usize = 32;
/// The most tokens a meta command line may have, as memcached.
const META_TOKENS_MAX: usize = 19;
/// A value whose client flags are not 0 starts with this, then the flags.
const FLAGS_MAGIC: &[u8; 4] = b"\0MC\x01";
/// A read-modify-write (append, incr, cas) tries this often while others
/// change the key under it.
const RETRIES: usize = 1000;
/// How often the listener, and connections with nothing to read, look at
/// the stop flag, the idle timeout and a delayed flush_all.
const TICK: Duration = Duration::from_millis(200);
/// How often a write held back by a FAILOVER looks again.
const PAUSE_POLL: Duration = Duration::from_millis(1);
const READ_CHUNK: usize = 64 * 1024;
/// Output beyond this goes to the client before more commands run.
const OUT_FLUSH: usize = 256 * 1024;
/// How long a stopping server waits for connections inside a command.
const STOP_GRACE: Duration = Duration::from_secs(2);
const LISTENER: Token = Token(0);

const BAD_FORMAT: &str = "CLIENT_ERROR bad command line format";
const BAD_TOKEN: &str = "CLIENT_ERROR bad token in command line format";
const NON_NUMERIC: &str = "CLIENT_ERROR cannot increment or decrement non-numeric value";
const TOO_LARGE: &str = "SERVER_ERROR object too large for cache";
const LONG_OPAQUE: &str = "CLIENT_ERROR opaque token too long";

/// What the connections of the Memcached listener share.
struct Port {
    shared: Arc<Shared>,
    max_conns: usize,
    /// A command line longer than this closes its connection.
    line_max: usize,
    stall: Duration,
    started: Instant,
    /// When a `flush_all` with a delay empties database 0, and whose it is.
    flush_at: Mutex<Option<(Instant, Arc<User>)>>,
    curr: AtomicUsize,
    total: AtomicU64,
    cmd_get: AtomicU64,
    get_hits: AtomicU64,
    get_misses: AtomicU64,
    cmd_set: AtomicU64,
    cmd_touch: AtomicU64,
    cmd_flush: AtomicU64,
}

/// Listen on `addr`. The thread returned accepts until the server stops.
pub(crate) fn start(
    shared: Arc<Shared>,
    addr: SocketAddr,
    cfg: &crate::ServerConfig,
) -> io::Result<(SocketAddr, JoinHandle<()>)> {
    let mut listener = TcpListener::bind(addr)?;
    let bound = listener.local_addr()?;
    let poll = Poll::new()?;
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;
    // A delayed flush_all runs on the listener's thread.
    let slot = shared
        .engine
        .gate()
        .register()
        .ok_or_else(|| io::Error::other("no free execution gate slot for Memcached"))?;
    let engine = shared.engine.clone();
    let port = Arc::new(Port {
        shared,
        max_conns: cfg.max_conns,
        line_max: cfg.limits.max_inline,
        stall: cfg.stall_timeout,
        started: Instant::now(),
        flush_at: Mutex::new(None),
        curr: AtomicUsize::new(0),
        total: AtomicU64::new(0),
        cmd_get: AtomicU64::new(0),
        get_hits: AtomicU64::new(0),
        get_misses: AtomicU64::new(0),
        cmd_set: AtomicU64::new(0),
        cmd_touch: AtomicU64::new(0),
        cmd_flush: AtomicU64::new(0),
    });
    std::thread::Builder::new()
        .name("hotstash-memcached".into())
        .spawn(move || port.accept(listener, poll, slot))
        .map(|thread| (bound, thread))
        .inspect_err(|_| engine.gate().unregister(slot))
}

impl Port {
    fn accept(self: Arc<Self>, listener: TcpListener, mut poll: Poll, slot: usize) {
        let mut events = Events::with_capacity(64);
        let mut conns: Vec<JoinHandle<()>> = Vec::new();
        while !self.shared.stop.load(SeqCst) {
            if let Err(e) = poll.poll(&mut events, Some(TICK)) {
                if e.kind() != ErrorKind::Interrupted {
                    eprintln!("hotstash: the Memcached listener stopped: {e}");
                    break;
                }
            }
            self.flush_if_due(slot);
            conns.retain(|conn| !conn.is_finished());
            loop {
                match listener.accept() {
                    Ok((stream, _)) => conns.extend(self.open(stream)),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    // Out of descriptors and the like: again after a tick.
                    Err(e) => {
                        eprintln!("hotstash: Memcached accept: {e}");
                        break;
                    }
                }
            }
        }
        // Each connection sees the stop within a tick. One waiting for the
        // gate a script holds is left to finish on its own.
        let deadline = Instant::now() + STOP_GRACE;
        for conn in conns {
            while !conn.is_finished() && Instant::now() < deadline {
                std::thread::sleep(PAUSE_POLL * 10);
            }
            if conn.is_finished() && conn.join().is_err() {
                eprintln!("hotstash: a Memcached connection thread panicked");
            }
        }
        self.shared.engine.gate().unregister(slot);
    }

    /// A thread for a new connection, or the reason it gets none.
    fn open(self: &Arc<Self>, stream: mio::net::TcpStream) -> Option<JoinHandle<()>> {
        let mut stream = TcpStream::from(stream);
        let ready = stream.set_nonblocking(false).and_then(|()| {
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(TICK))?;
            stream.set_write_timeout((!self.stall.is_zero()).then_some(self.stall))
        });
        if let Err(e) = ready {
            eprintln!("hotstash: cannot set up a Memcached connection: {e}");
            return None;
        }
        let slot = match self.curr.load(SeqCst) < self.max_conns {
            true => self.shared.engine.gate().register(),
            false => None,
        };
        let Some(slot) = slot else {
            // As memcached says it, and then closes.
            let _ = stream.write_all(b"ERROR Too many open connections\r\n");
            return None;
        };
        self.curr.fetch_add(1, SeqCst);
        self.total.fetch_add(1, Relaxed);
        let acl = self.shared.engine.acl();
        // The generation first: a change in between is then seen later.
        let acl_gen = acl.generation();
        let conn = Conn {
            port: self.clone(),
            stream,
            slot,
            input: Vec::new(),
            out: Vec::new(),
            user: acl.default_user(),
            acl_gen,
            authed: false,
            swallow: 0,
            closing: false,
        };
        let spawned = std::thread::Builder::new()
            .name("hotstash-memcached-conn".into())
            .spawn(move || conn.serve());
        match spawned {
            Ok(thread) => Some(thread),
            // The connection went with the closure: its drop gave back the
            // slot.
            Err(e) => {
                eprintln!("hotstash: no thread for a Memcached connection: {e}");
                None
            }
        }
    }

    /// Run a command in database 0 from a thread that owns gate slot
    /// `slot`, as a RESP worker does once the ACL allows it.
    fn execute(&self, slot: usize, cmd: Cmd) -> Reply {
        let shared = &self.shared;
        let engine = &shared.engine;
        // A FAILOVER under way holds back what would add to the stream.
        while cmd.pausable() && shared.writes_paused() && !shared.stop.load(SeqCst) {
            std::thread::sleep(PAUSE_POLL);
        }
        if shared.stop.load(SeqCst) {
            return Reply::err("ERR server is shutting down");
        }
        let cmd = match engine.is_replica() {
            true => match cmd.on_replica() {
                Ok(cmd) => cmd,
                Err(refused) => return refused,
            },
            false => cmd,
        };
        let durable = cmd.may_change() && engine.sync_before_reply(0, &cmd);
        let reply = engine.with_db(0, || engine.execute_blocking(slot, cmd));
        // With `--appendfsync always` a change is on disk before its client
        // hears of it.
        match durable.then(|| engine.sync_now()).flatten() {
            Some(Err(e)) => Reply::Error(format!(
                "ERR the command ran, but writing the data file failed: {e}"
            )),
            _ => reply,
        }
    }

    fn flush_if_due(&self, slot: usize) {
        let due = {
            let mut at = self.flush_at.lock().unwrap_or_else(PoisonError::into_inner);
            match &*at {
                Some((when, _)) if *when <= Instant::now() => at.take(),
                _ => None,
            }
        };
        let Some((_, by)) = due else {
            return;
        };
        // Only while whoever asked for it still may: a user deleted, or
        // made again, or without the right by now, flushes nothing.
        let flush = Cmd::FlushDb(None);
        let allowed = match self.shared.engine.acl().user(by.name()) {
            Some(user) if user.id() == by.id() => user.check(b"flushdb", None, &flush).is_ok(),
            _ => false,
        };
        if !allowed {
            eprintln!(
                "hotstash: a delayed Memcached flush_all was dropped: its user may not flush"
            );
            return;
        }
        if let Reply::Error(e) = self.execute(slot, flush) {
            eprintln!("hotstash: a delayed Memcached flush_all failed: {e}");
        }
    }
}

/// When a stored item goes, as an exptime names it.
#[derive(Clone, Copy)]
enum Ttl {
    Never,
    /// Already: a negative exptime.
    Gone,
    In(i64),
    At(i64),
}

impl Ttl {
    fn from_exptime(exptime: i64) -> Ttl {
        match exptime {
            0 => Ttl::Never,
            e if e < 0 => Ttl::Gone,
            e if e <= RELATIVE_MAX => Ttl::In(e),
            e => Ttl::At(e.min(UNIX_MAX)),
        }
    }

    /// The options SET takes for it; GETEX takes the same, but PERSIST for
    /// never.
    fn args(self, getex: bool) -> Vec<Vec<u8>> {
        let pair = |name: &str, n: i64| vec![name.as_bytes().to_vec(), n.to_string().into_bytes()];
        match self {
            Ttl::Never if getex => vec![b"PERSIST".to_vec()],
            Ttl::Never => Vec::new(),
            Ttl::Gone => pair("PXAT", 1),
            Ttl::In(secs) => pair("EX", secs),
            Ttl::At(unix) => pair("EXAT", unix),
        }
    }

    /// When it is, for a delayed flush_all.
    fn deadline(self) -> Instant {
        let now = Instant::now();
        let secs = match self {
            Ttl::Never | Ttl::Gone => 0,
            Ttl::In(secs) => secs,
            Ttl::At(unix) => unix.saturating_sub(unix_now()),
        };
        now + Duration::from_secs(secs.max(0).unsigned_abs())
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// What a store writes: the data, behind a header when there are flags.
fn encode(flags: u32, data: &[u8]) -> Vec<u8> {
    if flags == 0 && !data.starts_with(FLAGS_MAGIC) {
        return data.to_vec();
    }
    let mut stored = Vec::with_capacity(8 + data.len());
    stored.extend_from_slice(FLAGS_MAGIC);
    stored.extend_from_slice(&flags.to_be_bytes());
    stored.extend_from_slice(data);
    stored
}

/// The client flags and the data of what is stored.
fn decode(stored: &[u8]) -> (u32, &[u8]) {
    match stored.strip_prefix(FLAGS_MAGIC) {
        Some([a, b, c, d, data @ ..]) => (u32::from_be_bytes([*a, *b, *c, *d]), data),
        _ => (0, stored),
    }
}

/// Never 0, which a meta command takes for "no CAS".
fn cas_of(stored: &[u8]) -> u64 {
    xxh3_64(stored).max(1)
}

/// C's isspace.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// The decimal number at the start of `s` as memcached's `safe_strtoull`
/// reads it: spaces and a sign before it, a space, NUL or the end after
/// it, and no minus sign before one that wraps.
fn strtoull(s: &[u8]) -> Option<u64> {
    let (neg, value) = c_number(s)?;
    let value = match neg {
        true => value.wrapping_neg(),
        false => value,
    };
    (!(neg && i64::try_from(value).is_err())).then_some(value)
}

/// As `safe_strtol`, without memcached's cut to 32 bits.
fn strtol(s: &[u8]) -> Option<i64> {
    let (neg, value) = c_number(s)?;
    match neg {
        true => 0i64.checked_sub_unsigned(value),
        false => i64::try_from(value).ok(),
    }
}

/// Whether a minus sign came first, and the digits' value.
fn c_number(s: &[u8]) -> Option<(bool, u64)> {
    let s = &s[s.iter().take_while(|b| is_space(**b)).count()..];
    let (neg, s) = match s {
        [b'-', rest @ ..] => (true, rest),
        [b'+', rest @ ..] => (false, rest),
        _ => (false, s),
    };
    let digits = s.iter().take_while(|b| b.is_ascii_digit()).count();
    if digits == 0 {
        return None;
    }
    let mut value = 0u64;
    for d in &s[..digits] {
        value = value.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
    }
    match s.get(digits) {
        Some(&b) if !is_space(b) && b != 0 => None,
        _ => Some((neg, value)),
    }
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(src: &[u8], out: &mut Vec<u8>) {
    for chunk in src.chunks(3) {
        let byte = |i: usize| u32::from(chunk.get(i).copied().unwrap_or(0));
        let n = (byte(0) << 16) | (byte(1) << 8) | byte(2);
        let quad = [18, 12, 6, 0].map(|shift| BASE64[(n >> shift) as usize & 63]);
        let keep = chunk.len() + 1;
        out.extend_from_slice(&quad[..keep]);
        out.extend_from_slice(&b"=="[..4 - keep]);
    }
}

/// As memcached decodes a `b` key: what is not base64 is skipped, and
/// nothing after the first padded block counts.
pub(crate) fn base64_decode(src: &[u8]) -> Option<Vec<u8>> {
    let value = |c: u8| match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        b'=' => Some(0),
        _ => None,
    };
    let count = src.iter().filter(|c| value(**c).is_some()).count();
    if count == 0 || count % 4 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(count / 4 * 3);
    let (mut block, mut filled, mut pad) = ([0u8; 4], 0, 0);
    for &c in src {
        let Some(v) = value(c) else { continue };
        if c == b'=' {
            pad += 1;
        }
        block[filled] = v;
        filled += 1;
        if filled < 4 {
            continue;
        }
        filled = 0;
        out.extend_from_slice(&[
            (block[0] << 2) | (block[1] >> 4),
            (block[1] << 4) | (block[2] >> 2),
            (block[2] << 6) | block[3],
        ]);
        if pad > 0 {
            if pad > 2 {
                return None;
            }
            out.truncate(out.len() - pad);
            break;
        }
    }
    (!out.is_empty()).then_some(out)
}

/// The length of a value, as memcached takes it.
fn data_len(token: &[u8]) -> Option<usize> {
    strtol(token)
        .filter(|n| (0..=i64::from(i32::MAX) - 2).contains(n))
        .and_then(|n| usize::try_from(n).ok())
}

/// The memcached line for an error RESP gave.
fn server_error(reply: &Reply) -> String {
    match reply {
        Reply::Error(e) if e.starts_with("OOM") => {
            "SERVER_ERROR out of memory storing object".into()
        }
        Reply::Error(e) => {
            let text: String = e
                .chars()
                .map(|c| if c.is_control() { ' ' } else { c })
                .collect();
            format!("SERVER_ERROR {text}")
        }
        _ => "SERVER_ERROR unexpected reply".into(),
    }
}

fn wrong_type(reply: &Reply) -> bool {
    matches!(reply, Reply::Error(e) if e.starts_with("WRONGTYPE"))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
}

/// How a store went.
enum Stored {
    /// With the new CAS and the size of the data now stored.
    Yes(u64, usize),
    Not,
    Exists,
    NotFound,
    Failed(Reply),
}

/// How an incr or decr went.
enum Delta {
    /// The number now, its CAS, and whether the command made the key.
    Done(u64, u64, bool),
    NotFound,
    NonNumeric,
    Exists,
    Failed(Reply),
}

/// What a meta command's flags ask for, read as memcached reads them.
struct Meta {
    /// Decoded, with `b`.
    key: Vec<u8>,
    binary: bool,
    quiet: bool,
    value: bool,
    /// `T`.
    ttl: Option<i64>,
    /// `N`.
    vivify: Option<i64>,
    client_flags: u32,
    cas: Option<u64>,
    mode: Option<u8>,
    initial: u64,
    delta: u64,
    /// `t` comes before `T`: it shows the TTL the key had.
    t_first: bool,
}

/// `cmd` is the meta command's second letter: g, s, d or a. A flag with a
/// bad number fails with `bad`, as for memcached that command's default.
fn parse_meta(
    cmd: u8,
    key: &[u8],
    flags: &[&[u8]],
    bad: &'static str,
) -> Result<Meta, &'static str> {
    let mut meta = Meta {
        key: key.to_vec(),
        binary: false,
        quiet: false,
        value: false,
        ttl: None,
        vivify: None,
        client_flags: 0,
        cas: None,
        mode: None,
        initial: 0,
        delta: 1,
        t_first: false,
    };
    let mut seen = [false; 127];
    let (mut err, mut long_opaque) = (None, false);
    for flag in flags {
        let o = flag[0];
        match seen.get_mut(usize::from(o)) {
            Some(seen) if !*seen => *seen = true,
            _ => return Err("CLIENT_ERROR duplicate flag"),
        }
        let arg = &flag[1..];
        match o {
            b'b' => {
                meta.binary = true;
                match base64_decode(key) {
                    Some(decoded) => meta.key = decoded,
                    None => err = Some("CLIENT_ERROR error decoding key"),
                }
            }
            b'N' if cmd == b'a' => match strtol(arg) {
                Some(n) => meta.vivify = Some(n),
                None => err = Some(BAD_TOKEN),
            },
            b'T' => match strtol(arg) {
                Some(n) => meta.ttl = Some(n),
                None => err = Some(BAD_TOKEN),
            },
            b't' => meta.t_first = !seen[usize::from(b'T')],
            b'O' => long_opaque |= flag.len() > OPAQUE_MAX,
            b'P' | b'L' | b'k' | b's' | b'c' | b'f' | b'u' => {}
            b'v' => meta.value = true,
            b'q' => meta.quiet = true,
            b'F' => match strtoull(arg) {
                // Cut to 32 bits, as memcached.
                Some(f) => meta.client_flags = f as u32,
                None => err = Some(bad),
            },
            b'C' => match strtoull(arg) {
                Some(cas) => meta.cas = Some(cas),
                None => err = Some(BAD_TOKEN),
            },
            b'M' if flag.len() == 2 => meta.mode = Some(flag[1]),
            b'M' => err = Some("CLIENT_ERROR incorrect length for M token"),
            b'J' => match strtoull(arg) {
                Some(n) => meta.initial = n,
                None => err = Some("CLIENT_ERROR invalid numeric initial value"),
            },
            b'D' => match strtoull(arg) {
                Some(n) => meta.delta = n,
                None => err = Some("CLIENT_ERROR invalid numeric delta value"),
            },
            // Marks nothing stale: there is no stale here.
            b'I' if cmd != b'd' => {}
            // h, l, x, E, R; N but in ma; I in md: not supported.
            _ => return Err("CLIENT_ERROR invalid flag"),
        }
    }
    match (err, long_opaque) {
        (Some(err), _) => Err(err),
        (None, true) => Err(LONG_OPAQUE),
        (None, false) => Ok(meta),
    }
}

/// What a meta reply may show about the item.
#[derive(Default)]
struct Shown {
    size: Option<usize>,
    ttl: Option<i64>,
    cas: Option<u64>,
    client_flags: Option<u32>,
}

/// The return flags of a meta reply, in the order asked: `O` and `k`
/// always, the others where `shown` has them.
fn return_flags(out: &mut Vec<u8>, flags: &[&[u8]], meta: &Meta, shown: &Shown) {
    for flag in flags {
        match flag[0] {
            b'O' => {
                out.push(b' ');
                out.extend_from_slice(flag);
            }
            b'k' => {
                out.extend_from_slice(b" k");
                match meta.binary {
                    true => {
                        base64_encode(&meta.key, out);
                        out.extend_from_slice(b" b");
                    }
                    false => out.extend_from_slice(&meta.key),
                }
            }
            b's' => push_number(out, b" s", shown.size),
            b't' => push_number(out, b" t", shown.ttl),
            b'c' => push_number(out, b" c", shown.cas),
            b'f' => push_number(out, b" f", shown.client_flags),
            _ => {}
        }
    }
}

fn push_number(out: &mut Vec<u8>, tag: &[u8], n: Option<impl std::fmt::Display>) {
    if let Some(n) = n {
        out.extend_from_slice(tag);
        out.extend_from_slice(n.to_string().as_bytes());
    }
}

/// One client connection, on a thread of its own.
struct Conn {
    port: Arc<Port>,
    stream: TcpStream,
    slot: usize,
    input: Vec<u8>,
    out: Vec<u8>,
    user: Arc<User>,
    acl_gen: u64,
    authed: bool,
    /// Bytes of a refused value still to drop.
    swallow: usize,
    closing: bool,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.port.shared.engine.gate().unregister(self.slot);
        self.port.curr.fetch_sub(1, SeqCst);
    }
}

impl Conn {
    fn serve(mut self) {
        let mut buf = vec![0; READ_CHUNK];
        let mut heard = Instant::now();
        // Since when it has had to log in.
        let mut waiting: Option<Instant> = None;
        loop {
            self.handle_input();
            if !self.flush() || self.closing || self.stopping() {
                return;
            }
            waiting = match self.must_log_in() {
                true => Some(waiting.unwrap_or_else(Instant::now)),
                false => None,
            };
            if waiting.is_some_and(|since| since.elapsed() >= LOGIN_WAIT) {
                return;
            }
            match self.stream.read(&mut buf) {
                Ok(0) => return,
                Ok(n) => {
                    self.input.extend_from_slice(&buf[..n]);
                    heard = Instant::now();
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    let idle = self.port.shared.engine.client_timeout();
                    if idle > 0 && heard.elapsed() >= Duration::from_secs(idle) {
                        return;
                    }
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => return,
            }
        }
    }

    fn stopping(&self) -> bool {
        self.port.shared.stop.load(SeqCst)
    }

    /// Answer every whole command in the input, in order.
    fn handle_input(&mut self) {
        let input = std::mem::take(&mut self.input);
        let mut pos = 0;
        while !self.closing && !self.stopping() {
            if self.swallow > 0 {
                let dropped = self.swallow.min(input.len() - pos);
                pos += dropped;
                self.swallow -= dropped;
                if self.swallow > 0 {
                    break;
                }
                continue;
            }
            let rest = &input[pos..];
            let Some(end) = rest.iter().position(|&b| b == b'\n') else {
                // memcached closes such a connection too.
                let line_max = match self.must_log_in() {
                    true => LOGIN_LINE_MAX,
                    false => self.port.line_max,
                };
                self.closing = rest.len() > line_max;
                break;
            };
            match self.command(&rest[..end], &rest[end + 1..]) {
                Some(data) => pos += end + 1 + data,
                // The rest of a value is still to come.
                None => break,
            }
            self.spill();
        }
        let mut input = input;
        input.drain(..pos);
        self.input = input;
    }

    fn flush(&mut self) -> bool {
        if self.out.is_empty() {
            return true;
        }
        let sent = self.stream.write_all(&self.out).is_ok();
        self.out.clear();
        sent
    }

    /// Send what is waiting once there is a lot of it.
    fn spill(&mut self) {
        if self.out.len() >= OUT_FLUSH && !self.flush() {
            self.closing = true;
        }
    }

    fn line(&mut self, text: &str) {
        self.out.extend_from_slice(text.as_bytes());
        self.out.extend_from_slice(b"\r\n");
    }

    fn reply(&mut self, noreply: bool, text: &str) {
        if !noreply {
            self.line(text);
        }
    }

    /// One command line, up to its `\n`; `after` is what follows it. The
    /// bytes of a value it took from `after`, or `None` when the value is
    /// not all there.
    fn command(&mut self, raw: &[u8], after: &[u8]) -> Option<usize> {
        let line = raw.strip_suffix(b"\r").unwrap_or(raw);
        let tokens: Vec<&[u8]> = line
            .split(|&b| b == b' ')
            .filter(|t| !t.is_empty())
            .collect();
        if !self.refresh_user() {
            self.closing = true;
            return Some(0);
        }
        if self.must_log_in() {
            if raw.len() < LOGIN_LINE_MIN {
                self.closing = true;
                return Some(0);
            }
            return self.login(&tokens, after);
        }
        let Some(&name) = tokens.first() else {
            self.line("ERROR");
            return Some(0);
        };
        match name {
            b"set" | b"add" | b"replace" | b"append" | b"prepend" | b"cas" => {
                return self.store(&tokens, after)
            }
            b"ms" => return self.meta_set(&tokens, after),
            b"get" | b"gets" | b"gat" | b"gats" => self.get(&tokens),
            b"delete" => self.delete(&tokens),
            b"incr" | b"decr" => self.incr(&tokens),
            b"touch" => self.touch(&tokens),
            b"mg" => self.meta_get(&tokens),
            b"md" => self.meta_delete(&tokens),
            b"ma" => self.meta_arithmetic(&tokens),
            b"mn" => self.line("MN"),
            b"flush_all" => self.flush_all(&tokens),
            b"version" => self.line(&format!("VERSION {VERSION}")),
            b"verbosity" => self.verbosity(&tokens),
            b"stats" => self.stats(&tokens),
            b"quit" => self.closing = true,
            _ => self.line("ERROR"),
        }
        Some(0)
    }

    /// Whether nothing but a login is taken yet.
    fn must_log_in(&self) -> bool {
        !self.authed && !self.user.open()
    }

    /// Follow a change of the users. False: this connection's user is
    /// gone, and so the connection goes.
    fn refresh_user(&mut self) -> bool {
        let acl = self.port.shared.engine.acl();
        let generation = acl.generation();
        if generation == self.acl_gen {
            return true;
        }
        self.acl_gen = generation;
        match acl.user(self.user.name()) {
            // Deleted and made again is another user.
            Some(user) if user.id() == self.user.id() => {
                self.user = user;
                true
            }
            _ => false,
        }
    }

    /// One RESP command as this connection's user, in database 0.
    fn run(&self, args: Vec<Vec<u8>>) -> Reply {
        let name = args[0].clone();
        let cmd = match Cmd::parse(args) {
            Ok(cmd) => cmd,
            Err(refused) => return refused,
        };
        if let Err(denied) = self.user.check(&name, None, &cmd) {
            return denied;
        }
        self.port.execute(self.slot, cmd)
    }

    /// What `key` holds, if it is a string.
    fn fetch(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Reply> {
        match self.run(vec![b"GET".to_vec(), key.to_vec()]) {
            Reply::Bulk(stored) => Ok(Some(stored)),
            Reply::Nil => Ok(None),
            reply if wrong_type(&reply) => Ok(None),
            reply => Err(reply),
        }
    }

    /// GET, or GETEX when the TTL changes too.
    fn fetch_touching(&self, key: &[u8], ttl: Option<Ttl>) -> Result<Option<Vec<u8>>, Reply> {
        let Some(ttl) = ttl else {
            return self.fetch(key);
        };
        let mut args = vec![b"GETEX".to_vec(), key.to_vec()];
        args.extend(ttl.args(true));
        match self.run(args) {
            Reply::Bulk(stored) => Ok(Some(stored)),
            Reply::Nil => Ok(None),
            reply if wrong_type(&reply) => Ok(None),
            reply => Err(reply),
        }
    }

    /// Seconds `key` has left, -1 for no end, as `t` shows them.
    fn ttl_left(&self, key: &[u8]) -> i64 {
        match self.run(vec![b"PTTL".to_vec(), key.to_vec()]) {
            Reply::Int(ms) if ms >= 0 => ms.saturating_add(999) / 1000,
            _ => -1,
        }
    }

    /// Store as `mode`; with `cas`, only over what has that CAS.
    fn write(
        &self,
        key: &[u8],
        mode: Mode,
        flags: u32,
        ttl: Ttl,
        data: &[u8],
        cas: Option<u64>,
    ) -> Stored {
        match (mode, cas) {
            (Mode::Append | Mode::Prepend, _) => return self.concat(key, mode, data, cas),
            (Mode::Set | Mode::Replace, Some(cas)) => {
                return self.compare_and_set(key, flags, ttl, data, cas)
            }
            _ => {}
        }
        let stored = encode(flags, data);
        let cas = cas_of(&stored);
        let mut args = vec![b"SET".to_vec(), key.to_vec(), stored];
        match mode {
            Mode::Add => args.push(b"NX".to_vec()),
            Mode::Replace => args.push(b"XX".to_vec()),
            _ => {}
        }
        args.extend(ttl.args(false));
        match self.run(args) {
            Reply::Ok => Stored::Yes(cas, data.len()),
            Reply::Nil => Stored::Not,
            reply => Stored::Failed(reply),
        }
    }

    fn compare_and_set(&self, key: &[u8], flags: u32, ttl: Ttl, data: &[u8], cas: u64) -> Stored {
        let stored = encode(flags, data);
        let new_cas = cas_of(&stored);
        for _ in 0..RETRIES {
            let old = match self.fetch(key) {
                Ok(Some(old)) => old,
                Ok(None) => return Stored::NotFound,
                Err(reply) => return Stored::Failed(reply),
            };
            if cas_of(&old) != cas {
                return Stored::Exists;
            }
            let mut args = vec![
                b"SET".to_vec(),
                key.to_vec(),
                stored.clone(),
                b"IFEQ".to_vec(),
                old,
            ];
            args.extend(ttl.args(false));
            match self.run(args) {
                Reply::Ok => return Stored::Yes(new_cas, data.len()),
                // Changed since: look again.
                Reply::Nil => {}
                reply => return Stored::Failed(reply),
            }
        }
        Stored::Failed(Reply::err("ERR the key kept changing"))
    }

    /// append and prepend keep the flags and the TTL the item has. A `cas`
    /// of 0 checks nothing, as memcached.
    fn concat(&self, key: &[u8], mode: Mode, data: &[u8], cas: Option<u64>) -> Stored {
        for _ in 0..RETRIES {
            let old = match self.fetch(key) {
                Ok(Some(old)) => old,
                Ok(None) => return Stored::Not,
                Err(reply) => return Stored::Failed(reply),
            };
            if cas.is_some_and(|cas| cas != 0 && cas != cas_of(&old)) {
                return Stored::Exists;
            }
            let (flags, body) = decode(&old);
            // memcached cannot make an item that large: NOT_STORED.
            if body.len() + data.len() > ITEM_MAX {
                return Stored::Not;
            }
            let joined = match mode {
                Mode::Prepend => [data, body].concat(),
                _ => [body, data].concat(),
            };
            let stored = encode(flags, &joined);
            let new_cas = cas_of(&stored);
            let args = vec![
                b"SET".to_vec(),
                key.to_vec(),
                stored,
                b"IFEQ".to_vec(),
                old,
                b"KEEPTTL".to_vec(),
            ];
            match self.run(args) {
                Reply::Ok => return Stored::Yes(new_cas, joined.len()),
                Reply::Nil => {}
                reply => return Stored::Failed(reply),
            }
        }
        Stored::Failed(Reply::err("ERR the key kept changing"))
    }

    /// incr and decr: a sum that wraps, a difference that stops at 0. A
    /// number no longer than the one before is padded with spaces to its
    /// length, as memcached rewrites it in place. With `vivify`, a missing
    /// key starts at that number with that TTL.
    fn add_delta(
        &self,
        key: &[u8],
        incr: bool,
        delta: u64,
        cas: Option<u64>,
        vivify: Option<(u64, Ttl)>,
    ) -> Delta {
        for _ in 0..RETRIES {
            let old = match self.fetch(key) {
                Ok(Some(old)) => old,
                Ok(None) => {
                    let Some((initial, ttl)) = vivify else {
                        return Delta::NotFound;
                    };
                    let stored = initial.to_string().into_bytes();
                    let new_cas = cas_of(&stored);
                    let mut args = vec![b"SET".to_vec(), key.to_vec(), stored, b"NX".to_vec()];
                    args.extend(ttl.args(false));
                    match self.run(args) {
                        Reply::Ok => return Delta::Done(initial, new_cas, true),
                        Reply::Nil => continue,
                        reply => return Delta::Failed(reply),
                    }
                }
                Err(reply) => return Delta::Failed(reply),
            };
            let (flags, body) = decode(&old);
            if body.is_empty() {
                return Delta::NonNumeric;
            }
            if cas.is_some_and(|cas| cas != 0 && cas != cas_of(&old)) {
                return Delta::Exists;
            }
            let Some(number) = strtoull(body) else {
                return Delta::NonNumeric;
            };
            let value = match incr {
                true => number.wrapping_add(delta),
                false => number.saturating_sub(delta),
            };
            let mut digits = value.to_string().into_bytes();
            if digits.len() <= body.len() {
                digits.resize(body.len(), b' ');
            }
            let stored = encode(flags, &digits);
            let new_cas = cas_of(&stored);
            let args = vec![
                b"SET".to_vec(),
                key.to_vec(),
                stored,
                b"IFEQ".to_vec(),
                old,
                b"KEEPTTL".to_vec(),
            ];
            match self.run(args) {
                Reply::Ok => return Delta::Done(value, new_cas, false),
                Reply::Nil => {}
                reply => return Delta::Failed(reply),
            }
        }
        Delta::Failed(Reply::err("ERR the key kept changing"))
    }

    /// memcached's `-Y`: before anything else, a set whose value is
    /// `<user> <password>`. Every other line is refused.
    fn login(&mut self, tokens: &[&[u8]], after: &[u8]) -> Option<usize> {
        let len = match tokens {
            [b"set", _, _, _, len, ..] => strtoull(len).and_then(|n| usize::try_from(n).ok()),
            _ => None,
        };
        let Some(len) = len.filter(|&n| u32::try_from(n).is_ok()) else {
            self.line("CLIENT_ERROR unauthenticated");
            return Some(0);
        };
        if len > AUTH_MAX {
            self.line("CLIENT_ERROR auth token too long");
            return Some(0);
        }
        let data = after.get(..len + 2)?;
        if !data.ends_with(b"\r\n") {
            self.line("CLIENT_ERROR bad command line termination");
            return Some(len + 2);
        }
        let Some(space) = data[..len].iter().position(|&b| b == b' ') else {
            self.line("CLIENT_ERROR bad authentication token format");
            return Some(len + 2);
        };
        let acl = self.port.shared.engine.acl();
        // The generation first: a change in between is then seen later.
        let generation = acl.generation();
        match acl.auth(&data[..space], &data[space + 1..len]) {
            Ok(user) => {
                self.user = user;
                self.authed = true;
                self.acl_gen = generation;
                self.line("STORED");
            }
            Err(_) => self.line("CLIENT_ERROR authentication failure"),
        }
        Some(len + 2)
    }

    /// `<set|add|replace|append|prepend> <key> <flags> <exptime> <bytes>
    /// [noreply]`, and `cas` with `<cas unique>` before noreply.
    fn store(&mut self, tokens: &[&[u8]], after: &[u8]) -> Option<usize> {
        let (mode, with_cas) = match tokens[0] {
            b"add" => (Mode::Add, false),
            b"replace" => (Mode::Replace, false),
            b"append" => (Mode::Append, false),
            b"prepend" => (Mode::Prepend, false),
            b"cas" => (Mode::Set, true),
            _ => (Mode::Set, false),
        };
        let need = 5 + usize::from(with_cas);
        if tokens.len() != need && tokens.len() != need + 1 {
            self.line("ERROR");
            return Some(0);
        }
        let noreply = tokens.get(need) == Some(&&b"noreply"[..]);
        let key = tokens[1];
        let flags = strtoull(tokens[2]).map(|f| f as u32);
        let exptime = strtol(tokens[3]);
        let len = data_len(tokens[4]);
        let cas = match with_cas {
            true => strtoull(tokens[5]).map(Some),
            false => Some(None),
        };
        let (Some(flags), Some(exptime), Some(len), Some(cas), true) =
            (flags, exptime, len, cas, key.len() <= KEY_MAX)
        else {
            self.reply(noreply, BAD_FORMAT);
            return Some(0);
        };
        if len > ITEM_MAX {
            // As memcached: a set it cannot store leaves no stale value.
            if mode == Mode::Set && !with_cas {
                let _ = self.run(vec![b"DEL".to_vec(), key.to_vec()]);
            }
            self.reply(noreply, TOO_LARGE);
            self.swallow = len + 2;
            return Some(0);
        }
        let data = after.get(..len + 2)?;
        if !data.ends_with(b"\r\n") {
            self.reply(noreply, "CLIENT_ERROR bad data chunk");
            return Some(len + 2);
        }
        self.port.cmd_set.fetch_add(1, Relaxed);
        let ttl = Ttl::from_exptime(exptime);
        let text = match self.write(key, mode, flags, ttl, &data[..len], cas) {
            Stored::Yes(..) => "STORED".to_string(),
            Stored::Not => "NOT_STORED".to_string(),
            Stored::Exists => "EXISTS".to_string(),
            Stored::NotFound => "NOT_FOUND".to_string(),
            Stored::Failed(reply) => server_error(&reply),
        };
        self.reply(noreply, &text);
        Some(len + 2)
    }

    /// `get|gets <key>*`, `gat|gats <exptime> <key>*`.
    fn get(&mut self, tokens: &[&[u8]]) {
        if tokens.len() < 2 {
            self.line("ERROR");
            return;
        }
        let with_cas = matches!(tokens[0], b"gets" | b"gats");
        let (ttl, keys) = match tokens[0] {
            b"gat" | b"gats" => match strtol(tokens[1]) {
                Some(exptime) => (Some(Ttl::from_exptime(exptime)), &tokens[2..]),
                None => {
                    self.line("CLIENT_ERROR invalid exptime argument");
                    return;
                }
            },
            _ => (None, &tokens[1..]),
        };
        // One key too long fails them all, before any is read.
        if keys.iter().any(|key| key.len() > KEY_MAX) {
            self.line(BAD_FORMAT);
            return;
        }
        for key in keys {
            self.port.cmd_get.fetch_add(1, Relaxed);
            if ttl.is_some() {
                self.port.cmd_touch.fetch_add(1, Relaxed);
            }
            let stored = match self.fetch_touching(key, ttl) {
                Ok(Some(stored)) => stored,
                Ok(None) => {
                    self.port.get_misses.fetch_add(1, Relaxed);
                    continue;
                }
                Err(reply) => {
                    self.line(&server_error(&reply));
                    return;
                }
            };
            self.port.get_hits.fetch_add(1, Relaxed);
            let (flags, data) = decode(&stored);
            self.out.extend_from_slice(b"VALUE ");
            self.out.extend_from_slice(key);
            let mut head = format!(" {flags} {}", data.len());
            if with_cas {
                head += &format!(" {}", cas_of(&stored));
            }
            self.line(&head);
            self.out.extend_from_slice(data);
            self.out.extend_from_slice(b"\r\n");
            self.spill();
        }
        self.line("END");
    }

    /// `delete <key> [0] [noreply]`.
    fn delete(&mut self, tokens: &[&[u8]]) {
        if !(2..=4).contains(&tokens.len()) {
            self.line("ERROR");
            return;
        }
        let noreply = tokens.len() > 2 && tokens[tokens.len() - 1] == b"noreply";
        if tokens.len() > 2 {
            let hold_is_zero = tokens[2] == b"0";
            let valid = (tokens.len() == 3 && (hold_is_zero || noreply))
                || (tokens.len() == 4 && hold_is_zero && noreply);
            if !valid {
                self.reply(
                    noreply,
                    "CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]",
                );
                return;
            }
        }
        if tokens[1].len() > KEY_MAX {
            self.reply(noreply, BAD_FORMAT);
            return;
        }
        let text = match self.run(vec![b"DEL".to_vec(), tokens[1].to_vec()]) {
            Reply::Int(1) => "DELETED".to_string(),
            Reply::Int(_) => "NOT_FOUND".to_string(),
            reply => server_error(&reply),
        };
        self.reply(noreply, &text);
    }

    /// `incr|decr <key> <delta> [noreply]`.
    fn incr(&mut self, tokens: &[&[u8]]) {
        if tokens.len() != 3 && tokens.len() != 4 {
            self.line("ERROR");
            return;
        }
        let noreply = tokens.get(3) == Some(&&b"noreply"[..]);
        if tokens[1].len() > KEY_MAX {
            self.reply(noreply, BAD_FORMAT);
            return;
        }
        let Some(delta) = strtoull(tokens[2]) else {
            self.reply(noreply, "CLIENT_ERROR invalid numeric delta argument");
            return;
        };
        let text = match self.add_delta(tokens[1], tokens[0] == b"incr", delta, None, None) {
            Delta::Done(value, ..) => value.to_string(),
            Delta::NotFound => "NOT_FOUND".to_string(),
            Delta::NonNumeric => NON_NUMERIC.to_string(),
            Delta::Exists => "EXISTS".to_string(),
            Delta::Failed(reply) => server_error(&reply),
        };
        self.reply(noreply, &text);
    }

    /// `touch <key> <exptime> [noreply]`.
    fn touch(&mut self, tokens: &[&[u8]]) {
        if tokens.len() != 3 && tokens.len() != 4 {
            self.line("ERROR");
            return;
        }
        let noreply = tokens.get(3) == Some(&&b"noreply"[..]);
        if tokens[1].len() > KEY_MAX {
            self.reply(noreply, BAD_FORMAT);
            return;
        }
        let Some(exptime) = strtol(tokens[2]) else {
            self.reply(noreply, "CLIENT_ERROR invalid exptime argument");
            return;
        };
        self.port.cmd_touch.fetch_add(1, Relaxed);
        let text = match self.fetch_touching(tokens[1], Some(Ttl::from_exptime(exptime))) {
            Ok(Some(_)) => "TOUCHED".to_string(),
            Ok(None) => "NOT_FOUND".to_string(),
            Err(reply) => server_error(&reply),
        };
        self.reply(noreply, &text);
    }

    /// `flush_all [delay] [noreply]`: database 0 empties now, or once the
    /// delay is up.
    fn flush_all(&mut self, tokens: &[&[u8]]) {
        if tokens.len() > 3 {
            self.line("ERROR");
            return;
        }
        self.port.cmd_flush.fetch_add(1, Relaxed);
        let noreply = tokens.len() > 1 && tokens[tokens.len() - 1] == b"noreply";
        let delay = match tokens.len() - usize::from(noreply) {
            1 => 0,
            _ => match strtol(tokens[1]) {
                Some(delay) => delay,
                None => {
                    self.reply(noreply, "CLIENT_ERROR invalid exptime argument");
                    return;
                }
            },
        };
        let flush = Cmd::FlushDb(None);
        if let Err(denied) = self.user.check(b"flushdb", None, &flush) {
            self.reply(noreply, &server_error(&denied));
            return;
        }
        // A later flush_all replaces one still to come.
        let mut at = self
            .port
            .flush_at
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *at = None;
        if delay > 0 {
            *at = Some((Ttl::from_exptime(delay).deadline(), self.user.clone()));
            drop(at);
            self.reply(noreply, "OK");
            return;
        }
        drop(at);
        let text = match self.port.execute(self.slot, flush) {
            Reply::Ok => "OK".to_string(),
            reply => server_error(&reply),
        };
        self.reply(noreply, &text);
    }

    /// `verbosity <level> [noreply]`: there is nothing to tell more of.
    fn verbosity(&mut self, tokens: &[&[u8]]) {
        if !(2..=3).contains(&tokens.len()) {
            self.line("ERROR");
            return;
        }
        let noreply = tokens[tokens.len() - 1] == b"noreply";
        let text = match strtoull(tokens[1]) {
            Some(_) => "OK",
            None => BAD_FORMAT,
        };
        self.reply(noreply, text);
    }

    /// Whether this connection's user may run `args`, as RESP checks it.
    /// If not, the refusal is written.
    fn may(&mut self, args: Vec<Vec<u8>>) -> bool {
        let name = args[0].clone();
        let sub = args.get(1).cloned();
        let checked = Cmd::parse(args).and_then(|cmd| self.user.check(&name, sub.as_deref(), &cmd));
        match checked {
            Ok(()) => true,
            Err(denied) => {
                self.line(&server_error(&denied));
                false
            }
        }
    }

    /// `stats`, with what this port counts; `stats reset` zeroes that.
    /// They need the rights of INFO and CONFIG RESETSTAT.
    fn stats(&mut self, tokens: &[&[u8]]) {
        let allowed = match tokens {
            [_] => self.may(vec![b"INFO".to_vec()]),
            [_, b"reset"] => self.may(vec![b"CONFIG".to_vec(), b"RESETSTAT".to_vec()]),
            _ => true,
        };
        if !allowed {
            return;
        }
        let port = self.port.clone();
        let counters = [
            ("cmd_get", &port.cmd_get),
            ("cmd_set", &port.cmd_set),
            ("cmd_flush", &port.cmd_flush),
            ("cmd_touch", &port.cmd_touch),
            ("get_hits", &port.get_hits),
            ("get_misses", &port.get_misses),
        ];
        match tokens {
            [_] => {}
            [_, b"reset"] => {
                for (_, counter) in counters {
                    counter.store(0, Relaxed);
                }
                self.line("RESET");
                return;
            }
            // Asked for, but with nothing here to list.
            [_, b"settings" | b"items" | b"slabs" | b"sizes" | b"conns"] => {
                self.line("END");
                return;
            }
            _ => {
                self.line("ERROR");
                return;
            }
        }
        let mut lines = vec![
            ("pid", std::process::id().to_string()),
            ("uptime", port.started.elapsed().as_secs().to_string()),
            ("time", unix_now().to_string()),
            ("version", VERSION.to_string()),
            ("pointer_size", usize::BITS.to_string()),
            ("max_connections", port.max_conns.to_string()),
            ("curr_connections", port.curr.load(SeqCst).to_string()),
            ("total_connections", port.total.load(Relaxed).to_string()),
        ];
        lines.extend(counters.map(|(name, counter)| (name, counter.load(Relaxed).to_string())));
        if let Reply::Int(items) = self.run(vec![b"DBSIZE".to_vec()]) {
            lines.push(("curr_items", items.to_string()));
        }
        for (name, value) in lines {
            self.line(&format!("STAT {name} {value}"));
        }
        self.line("END");
    }

    /// The checks every meta command makes first. False when the error is
    /// written. ms has a data length after the key.
    fn meta_checks(&mut self, tokens: &[&[u8]], with_data: bool) -> bool {
        let error = match tokens.len() {
            0 | 1 => "ERROR",
            _ if tokens[1].len() > KEY_MAX => BAD_FORMAT,
            2 if with_data => BAD_FORMAT,
            n if n > META_TOKENS_MAX => "CLIENT_ERROR options flags are too long",
            _ => return true,
        };
        self.line(error);
        false
    }

    /// The flags of mg, md or ma, or `None` when the error is written. md
    /// and ma do not say what is wrong with them, as memcached.
    fn meta_flags(&mut self, tokens: &[&[u8]]) -> Option<Meta> {
        if !self.meta_checks(tokens, false) {
            return None;
        }
        let cmd = tokens[0][1];
        parse_meta(cmd, tokens[1], &tokens[2..], BAD_FORMAT)
            .map_err(|err| match cmd {
                b'g' => err,
                _ if err == LONG_OPAQUE => err,
                _ => "CLIENT_ERROR invalid or duplicate flag",
            })
            .inspect_err(|err| self.line(err))
            .ok()
    }

    /// The reply line of a meta command, its return flags and its end.
    fn meta_line(&mut self, code: &str, flags: &[&[u8]], meta: &Meta, shown: &Shown) {
        self.out.extend_from_slice(code.as_bytes());
        return_flags(&mut self.out, flags, meta, shown);
        self.out.extend_from_slice(b"\r\n");
    }

    /// `mg <key> <flag>*`.
    fn meta_get(&mut self, tokens: &[&[u8]]) {
        let Some(meta) = self.meta_flags(tokens) else {
            return;
        };
        let flags = &tokens[2..];
        let wants_ttl = flags.iter().any(|f| f[0] == b't');
        let old_ttl =
            (wants_ttl && meta.t_first && meta.ttl.is_some()).then(|| self.ttl_left(&meta.key));
        self.port.cmd_get.fetch_add(1, Relaxed);
        let stored = match self.fetch_touching(&meta.key, meta.ttl.map(Ttl::from_exptime)) {
            Ok(stored) => stored,
            Err(reply) => {
                self.line(&server_error(&reply));
                return;
            }
        };
        let Some(stored) = stored else {
            self.port.get_misses.fetch_add(1, Relaxed);
            if !meta.quiet {
                self.meta_line("EN", flags, &meta, &Shown::default());
            }
            return;
        };
        self.port.get_hits.fetch_add(1, Relaxed);
        let (client_flags, data) = decode(&stored);
        let shown = Shown {
            size: Some(data.len()),
            ttl: wants_ttl.then(|| old_ttl.unwrap_or_else(|| self.ttl_left(&meta.key))),
            cas: Some(cas_of(&stored)),
            client_flags: Some(client_flags),
        };
        let code = match meta.value {
            true => format!("VA {}", data.len()),
            false => "HD".to_string(),
        };
        self.meta_line(&code, flags, &meta, &shown);
        if meta.value {
            self.out.extend_from_slice(data);
            self.out.extend_from_slice(b"\r\n");
        }
    }

    /// `ms <key> <datalen> <flag>*`, then the data.
    fn meta_set(&mut self, tokens: &[&[u8]], after: &[u8]) -> Option<usize> {
        if !self.meta_checks(tokens, true) {
            return Some(0);
        }
        let Some(len) = data_len(tokens[2]) else {
            // Without a length there is nothing to drop.
            self.line(BAD_FORMAT);
            return Some(0);
        };
        // From here on an error drops the value that follows.
        let meta = match parse_meta(b's', tokens[1], &tokens[3..], BAD_FORMAT) {
            Ok(meta) => meta,
            Err(err) => {
                self.line(err);
                self.swallow = len + 2;
                return Some(0);
            }
        };
        let mode = match meta.mode {
            None | Some(b'S') => Mode::Set,
            Some(b'E') => Mode::Add,
            Some(b'A') => Mode::Append,
            Some(b'P') => Mode::Prepend,
            Some(b'R') => Mode::Replace,
            Some(_) => {
                self.line("CLIENT_ERROR invalid mode for ms M token");
                self.swallow = len + 2;
                return Some(0);
            }
        };
        if len > ITEM_MAX {
            // As memcached: no stale value stays behind.
            let _ = self.run(vec![b"DEL".to_vec(), meta.key.clone()]);
            self.line(TOO_LARGE);
            self.swallow = len + 2;
            return Some(0);
        }
        let data = after.get(..len + 2)?;
        if !data.ends_with(b"\r\n") {
            self.line("CLIENT_ERROR bad data chunk");
            return Some(len + 2);
        }
        self.port.cmd_set.fetch_add(1, Relaxed);
        let ttl = Ttl::from_exptime(meta.ttl.unwrap_or(0));
        // add ignores a CAS, as memcached.
        let cas = meta.cas.filter(|_| mode != Mode::Add);
        let stored = self.write(&meta.key, mode, meta.client_flags, ttl, &data[..len], cas);
        let (code, cas, size) = match stored {
            Stored::Yes(..) if meta.quiet => return Some(len + 2),
            Stored::Yes(cas, size) => ("HD", cas, size),
            Stored::Not => ("NS", 0, len),
            Stored::Exists => ("EX", 0, len),
            Stored::NotFound => ("NF", 0, len),
            Stored::Failed(reply) => {
                self.line(&server_error(&reply));
                return Some(len + 2);
            }
        };
        // Whatever came of it: the CAS stored (0 for none) and the size of
        // the item, appended to or not. ms shows no t or f.
        let shown = Shown {
            size: Some(size),
            cas: Some(cas),
            ..Shown::default()
        };
        self.meta_line(code, &tokens[3..], &meta, &shown);
        Some(len + 2)
    }

    /// `md <key> <flag>*`.
    fn meta_delete(&mut self, tokens: &[&[u8]]) {
        let Some(meta) = self.meta_flags(tokens) else {
            return;
        };
        let code = match meta.cas {
            None => match self.run(vec![b"DEL".to_vec(), meta.key.clone()]) {
                Reply::Int(1) => "HD",
                Reply::Int(_) => "NF",
                reply => {
                    self.line(&server_error(&reply));
                    return;
                }
            },
            Some(cas) => match self.delete_if(&meta.key, cas) {
                Ok(code) => code,
                Err(reply) => {
                    self.line(&server_error(&reply));
                    return;
                }
            },
        };
        if code == "HD" && meta.quiet {
            return;
        }
        self.meta_line(code, &tokens[2..], &meta, &Shown::default());
    }

    /// md with C: HD, NF or EX.
    fn delete_if(&self, key: &[u8], cas: u64) -> Result<&'static str, Reply> {
        for _ in 0..RETRIES {
            let Some(old) = self.fetch(key)? else {
                return Ok("NF");
            };
            if cas_of(&old) != cas {
                return Ok("EX");
            }
            match self.run(vec![b"DELIFEQ".to_vec(), key.to_vec(), old]) {
                Reply::Int(1) => return Ok("HD"),
                Reply::Int(_) => {}
                reply => return Err(reply),
            }
        }
        Err(Reply::err("ERR the key kept changing"))
    }

    /// `ma <key> <flag>*`.
    fn meta_arithmetic(&mut self, tokens: &[&[u8]]) {
        let Some(meta) = self.meta_flags(tokens) else {
            return;
        };
        let incr = match meta.mode {
            None | Some(b'I' | b'+') => true,
            Some(b'D' | b'-') => false,
            Some(_) => {
                self.line("CLIENT_ERROR invalid mode for ma M token");
                return;
            }
        };
        let flags = &tokens[2..];
        let wants_ttl = flags.iter().any(|f| f[0] == b't');
        let key = meta.key.clone();
        let old_ttl =
            (wants_ttl && meta.t_first && meta.ttl.is_some()).then(|| self.ttl_left(&key));
        let vivify = meta.vivify.map(|n| (meta.initial, Ttl::from_exptime(n)));
        let (value, cas, created) = match self.add_delta(&key, incr, meta.delta, meta.cas, vivify) {
            Delta::Done(value, cas, created) => (value, cas, created),
            Delta::NotFound => return self.meta_line("NF", flags, &meta, &Shown::default()),
            Delta::Exists => return self.meta_line("EX", flags, &meta, &Shown::default()),
            Delta::NonNumeric => return self.line(NON_NUMERIC),
            Delta::Failed(reply) => return self.line(&server_error(&reply)),
        };
        if let Some(exptime) = meta.ttl {
            let mut args = vec![b"GETEX".to_vec(), key.clone()];
            args.extend(Ttl::from_exptime(exptime).args(true));
            if let reply @ Reply::Error(_) = self.run(args) {
                return self.line(&server_error(&reply));
            }
        }
        // Quiet hides a change, not a key it made.
        if meta.quiet && !created {
            return;
        }
        let number = value.to_string();
        let shown = Shown {
            ttl: wants_ttl.then(|| old_ttl.unwrap_or_else(|| self.ttl_left(&key))),
            cas: Some(cas),
            ..Shown::default()
        };
        let code = match meta.value {
            true => format!("VA {}", number.len()),
            false => "HD".to_string(),
        };
        self.meta_line(&code, flags, &meta, &shown);
        if meta.value {
            self.line(&number);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_read_as_memcached_reads_them() {
        assert_eq!(strtoull(b"12"), Some(12));
        assert_eq!(strtoull(b" 12"), Some(12));
        assert_eq!(strtoull(b"12 ab"), Some(12));
        assert_eq!(strtoull(b"+7"), Some(7));
        assert_eq!(strtoull(b"-0"), Some(0));
        assert_eq!(strtoull(b"12ab"), None);
        assert_eq!(strtoull(b"-1"), None);
        assert_eq!(strtoull(b""), None);
        assert_eq!(strtoull(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(strtoull(b"18446744073709551616"), None);
        assert_eq!(strtol(b"-1"), Some(-1));
        assert_eq!(strtol(b"9223372036854775808"), None);
        assert_eq!(strtol(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(strtol(b"abc"), None);
    }

    #[test]
    fn flags_ride_in_front_of_the_value() {
        assert_eq!(encode(0, b"hi"), b"hi");
        assert_eq!(decode(b"hi"), (0, &b"hi"[..]));
        let stored = encode(123, b"hi");
        assert_eq!(decode(&stored), (123, &b"hi"[..]));
        // Data that looks like a header gets one of its own.
        let tricky = encode(0, b"\0MC\x01\0\0\0\x05x");
        assert_eq!(decode(&tricky), (0, &b"\0MC\x01\0\0\0\x05x"[..]));
    }

    #[test]
    fn base64_keys_round_trip() {
        let mut out = Vec::new();
        base64_encode(b"mk", &mut out);
        assert_eq!(out, b"bWs=");
        assert_eq!(base64_decode(b"bWs="), Some(b"mk".to_vec()));
        assert_eq!(base64_decode(b"azE="), Some(b"k1".to_vec()));
        assert_eq!(base64_decode(b"mk"), None);
        assert_eq!(base64_decode(b"!!!!"), None);
        assert_eq!(base64_decode(b"===="), None);
    }

    #[test]
    fn meta_flags_refuse_repeats_and_strangers() {
        let flags: [&[u8]; 2] = [b"v", b"v"];
        assert_eq!(
            parse_meta(b'g', b"k", &flags, BAD_FORMAT).err(),
            Some("CLIENT_ERROR duplicate flag")
        );
        let flags: [&[u8]; 1] = [b"X"];
        assert_eq!(
            parse_meta(b'g', b"k", &flags, BAD_FORMAT).err(),
            Some("CLIENT_ERROR invalid flag")
        );
        let flags: [&[u8]; 1] = [b"Tabc"];
        assert_eq!(
            parse_meta(b'g', b"k", &flags, BAD_FORMAT).err(),
            Some(BAD_TOKEN)
        );
        let flags: [&[u8]; 3] = [b"t", b"T5", b"N1"];
        assert!(parse_meta(b'g', b"k", &flags, BAD_FORMAT).is_err());
        let meta = parse_meta(b'a', b"k", &flags, BAD_FORMAT).expect("ma takes N");
        assert!(meta.t_first);
        assert_eq!((meta.ttl, meta.vivify), (Some(5), Some(1)));
    }
}
