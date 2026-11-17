//! A replica's link to its primary: a thread of its own that logs in,
//! syncs, applies the primary's stream and says how far it got. It speaks
//! the protocol of Redis and Valkey, so a primary may be either, or
//! another HCache.

use hcache_core::engine::Engine;
use hcache_core::repl::Replay;
use hcache_core::resp::encode_command;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long one read waits, so that the thread notices it is to stop.
const TICK: Duration = Duration::from_millis(100);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Silence from the primary for this long ends the link (`repl-timeout`).
const LINK_TIMEOUT: Duration = Duration::from_secs(60);
/// How often the replica tells its primary where it is.
const ACK_EVERY: Duration = Duration::from_secs(1);
/// How long a failed link waits before it tries again.
const RETRY_AFTER: Duration = Duration::from_secs(1);
const READ_CHUNK: usize = 64 << 10;
/// The longest line the primary may answer the handshake with.
const MAX_LINE: usize = 64 << 10;
/// What is set aside for the RDB file before it arrives: its length is
/// only the primary's word.
const RDB_PREALLOC: usize = 64 << 20;

/// Where the link stands, as ROLE names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Connect,
    Connecting,
    Handshake,
    Sync,
    Connected,
}

impl Phase {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Phase::Connect => "connect",
            Phase::Connecting => "connecting",
            Phase::Handshake => "handshake",
            Phase::Sync => "sync",
            Phase::Connected => "connected",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Status {
    pub phase: Phase,
    /// When the primary last sent anything.
    pub last_io: Option<Instant>,
    /// Since when the link has been down, once it was up.
    pub down_since: Option<Instant>,
}

/// Told how a PSYNC FAILOVER went: whether the node it was sent to took
/// over this node's role.
pub(crate) type Handover = Box<dyn FnOnce(bool) + Send>;

struct State {
    stop: AtomicBool,
    /// CLIENT KILL TYPE master: drop the link and connect again.
    reset: AtomicBool,
    status: Mutex<Status>,
    /// While this node hands its role over (FAILOVER): its PSYNC says
    /// FAILOVER, and the answer goes here.
    handover: Mutex<Option<Handover>>,
}

impl State {
    fn status(&self) -> MutexGuard<'_, Status> {
        // Plain values: a panic elsewhere cannot leave them torn.
        self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn handover(&self) -> MutexGuard<'_, Option<Handover>> {
        // Taken whole or not at all.
        self.handover.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn handed_over(&self, took: bool) {
        let handover = self.handover().take();
        if let Some(handover) = handover {
            handover(took);
        }
    }

    fn set_phase(&self, phase: Phase) {
        self.status().phase = phase;
    }

    fn quitting(&self) -> bool {
        self.stop.load(SeqCst) || self.reset.load(SeqCst)
    }
}

/// The primary to follow, and this node's port to tell it.
struct Target {
    host: String,
    port: u16,
    my_port: u16,
}

/// The thread that follows a primary. Dropping it stops the thread and
/// waits for it.
pub(crate) struct Follower {
    pub host: String,
    pub port: u16,
    /// It was started by a FAILOVER of this node.
    pub handing: bool,
    state: Arc<State>,
    thread: Option<JoinHandle<()>>,
}

impl Follower {
    /// Follow `host:port` from now on. The engine must be a replica. With
    /// a `handover`, this node was that one's primary: it asks it to take
    /// over (PSYNC FAILOVER) and is told whether it did.
    pub(crate) fn start(
        engine: Arc<Engine>,
        host: String,
        port: u16,
        my_port: u16,
        handover: Option<Handover>,
    ) -> io::Result<Follower> {
        let handing = handover.is_some();
        let state = Arc::new(State {
            stop: AtomicBool::new(false),
            reset: AtomicBool::new(false),
            status: Mutex::new(Status {
                phase: Phase::Connect,
                last_io: None,
                down_since: None,
            }),
            handover: Mutex::new(handover),
        });
        let target = Target {
            host: host.clone(),
            port,
            my_port,
        };
        let shared = state.clone();
        let thread = std::thread::Builder::new()
            .name("hcache-replica".into())
            .spawn(move || follow(&engine, &target, &shared))?;
        Ok(Follower {
            host,
            port,
            handing,
            state,
            thread: Some(thread),
        })
    }

    pub(crate) fn status(&self) -> Status {
        *self.state.status()
    }

    /// Drop the link; the thread connects again. Whether there was one.
    pub(crate) fn reset(&self) -> bool {
        self.state.reset.store(true, SeqCst);
        let mut status = self.state.status();
        let up = status.phase == Phase::Connected;
        if up {
            status.phase = Phase::Connect;
            status.down_since = Some(Instant::now());
        }
        up
    }
}

impl Drop for Follower {
    fn drop(&mut self) {
        self.state.stop.store(true, SeqCst);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                eprintln!("hcache replica: the link thread panicked");
            }
        }
    }
}

fn follow(engine: &Engine, to: &Target, state: &State) {
    while !state.stop.load(SeqCst) {
        state.reset.store(false, SeqCst);
        let result = session(engine, to, state);
        {
            let mut status = state.status();
            if status.phase == Phase::Connected {
                status.down_since = Some(Instant::now());
            }
            status.phase = Phase::Connect;
        }
        if let Err(e) = result {
            if !state.quitting() {
                eprintln!(
                    "hcache replica: the link to {}:{} failed: {e}",
                    to.host, to.port
                );
            }
        }
        let retry = Instant::now() + RETRY_AFTER;
        while !state.quitting() && Instant::now() < retry {
            std::thread::sleep(TICK);
        }
    }
}

fn failed(what: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, what.into())
}

/// What the primary answered, for a log line: never more than a line,
/// and nothing that would start another.
fn shown(reply: &[u8]) -> String {
    String::from_utf8_lossy(&reply[..reply.len().min(200)])
        .escape_debug()
        .to_string()
}

fn is_replid(id: &str) -> bool {
    id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

/// One link from connecting to its end. `Ok` when told to stop.
fn session(engine: &Engine, to: &Target, state: &State) -> io::Result<()> {
    state.set_phase(Phase::Connecting);
    let addr = (to.host.as_str(), to.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "the host has no address"))?;
    let stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(TICK))?;
    stream.set_write_timeout(Some(LINK_TIMEOUT))?;
    let mut wire = Wire {
        stream,
        buf: Vec::new(),
        chunk: vec![0; READ_CHUNK].into_boxed_slice(),
        heard: Instant::now(),
        state,
    };
    state.set_phase(Phase::Handshake);
    // A primary that wants a login says so here; AUTH comes next.
    let pong = wire.ask(&[b"PING"])?;
    let login = [&b"-NOAUTH"[..], b"-NOPERM", b"-ERR operation not permitted"];
    if pong.starts_with(b"-") && !login.iter().any(|l| pong.starts_with(l)) {
        return Err(failed(format!("PING: {}", shown(&pong))));
    }
    if let (user, Some(pass)) = engine.master_auth() {
        let reply = match &user {
            Some(user) => wire.ask(&[b"AUTH", user, &pass])?,
            None => wire.ask(&[b"AUTH", &pass])?,
        };
        if reply.starts_with(b"-") {
            return Err(failed(format!("AUTH: {}", shown(&reply))));
        }
    }
    // For the primary's INFO only: an error is no reason to stop.
    wire.ask(&[
        b"REPLCONF",
        b"listening-port",
        to.my_port.to_string().as_bytes(),
    ])?;
    wire.ask(&[b"REPLCONF", b"capa", b"psync2"])?;
    // Ask to go on from where this node is: from its own history, if it
    // was the primary, or from the stream it had.
    let info = engine.repl_info();
    let next = info.offset.saturating_add(1).to_string();
    let psync = [
        &b"PSYNC"[..],
        info.replid.as_bytes(),
        next.as_bytes(),
        b"FAILOVER",
    ];
    let handing = state.handover().is_some();
    let reply = wire.ask(&psync[..if handing { 4 } else { 3 }])?;
    let text = String::from_utf8_lossy(&reply).into_owned();
    let synced = text.starts_with("+FULLRESYNC ") || text.starts_with("+CONTINUE");
    if handing {
        // Refused: this node takes its role back, and this link ends.
        if !synced {
            state.stop.store(true, SeqCst);
        }
        state.handed_over(synced);
    }
    if let Some(rest) = text.strip_prefix("+FULLRESYNC ") {
        let mut parts = rest.split(' ');
        let replid = parts.next().filter(|id| is_replid(id));
        // An offset is a signed 64-bit number, as in Redis.
        let offset = parts
            .next()
            .and_then(|n| n.parse::<i64>().ok())
            .and_then(|n| u64::try_from(n).ok());
        let (Some(replid), Some(offset)) = (replid, offset) else {
            return Err(failed(format!("PSYNC: {}", shown(&reply))));
        };
        state.set_phase(Phase::Sync);
        let rdb = wire.bulk()?;
        engine
            .repl_load(&rdb[..], replid, offset)
            .map_err(|e| failed(format!("the primary's RDB file: {e}")))?;
    } else if let Some(rest) = text.strip_prefix("+CONTINUE") {
        let replid = rest.trim();
        if is_replid(replid) {
            engine.repl_continue(replid);
        } else if !replid.is_empty() {
            return Err(failed(format!("PSYNC: {}", shown(&reply))));
        }
    } else {
        return Err(failed(format!("PSYNC: {}", shown(&reply))));
    }
    {
        let mut status = state.status();
        status.phase = Phase::Connected;
        status.down_since = None;
    }
    wire.stream_in(engine)
}

/// The socket to the primary and what has arrived on it.
struct Wire<'a> {
    stream: TcpStream,
    buf: Vec<u8>,
    chunk: Box<[u8]>,
    /// When the primary last sent anything.
    heard: Instant,
    state: &'a State,
}

impl Wire<'_> {
    fn send(&mut self, args: &[&[u8]]) -> io::Result<()> {
        let mut out = Vec::new();
        encode_command(args, &mut out);
        self.stream.write_all(&out)
    }

    fn ask(&mut self, args: &[&[u8]]) -> io::Result<Vec<u8>> {
        self.send(args)?;
        self.line()
    }

    /// Read what the socket has, or wait one tick. Whether anything came.
    fn read_more(&mut self) -> io::Result<bool> {
        if self.state.quitting() {
            return Err(io::Error::new(ErrorKind::Interrupted, "told to stop"));
        }
        match self.stream.read(&mut self.chunk) {
            Ok(0) => Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "the primary closed the link",
            )),
            Ok(n) => {
                self.buf.extend_from_slice(&self.chunk[..n]);
                self.heard = Instant::now();
                self.state.status().last_io = Some(self.heard);
                Ok(true)
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if self.heard.elapsed() >= LINK_TIMEOUT {
                    return Err(io::Error::new(
                        ErrorKind::TimedOut,
                        "nothing from the primary",
                    ));
                }
                Ok(false)
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// The next line, without its end. The empty lines a primary sends to
    /// keep the link alive while it prepares a sync are passed over.
    fn line(&mut self) -> io::Result<Vec<u8>> {
        loop {
            if let Some(end) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=end).collect();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.is_empty() {
                    continue;
                }
                return Ok(line);
            }
            if self.buf.len() > MAX_LINE {
                return Err(failed("an overlong line from the primary"));
            }
            self.read_more()?;
        }
    }

    /// The RDB file of a full sync: `$<length>` and that many bytes.
    fn bulk(&mut self) -> io::Result<Vec<u8>> {
        let header = self.line()?;
        let len = header
            .strip_prefix(b"$")
            .and_then(|n| std::str::from_utf8(n).ok())
            .and_then(|n| n.parse::<usize>().ok())
            .ok_or_else(|| failed(format!("the RDB file's length: {}", shown(&header))))?;
        let mut rdb = Vec::with_capacity(len.min(RDB_PREALLOC));
        loop {
            let take = self.buf.len().min(len - rdb.len());
            rdb.extend(self.buf.drain(..take));
            if rdb.len() == len {
                return Ok(rdb);
            }
            self.read_more()?;
        }
    }

    /// Apply the primary's stream as it comes, and say how far this node
    /// got: every second, and when the primary asks.
    fn stream_in(&mut self, engine: &Engine) -> io::Result<()> {
        let mut acked: Option<Instant> = None;
        let mut fresh = !self.buf.is_empty();
        let mut replay = Replay::new(engine);
        loop {
            if fresh {
                let applied = replay.apply(engine, &self.buf).map_err(failed)?;
                self.buf.drain(..applied.used);
                if applied.unknown > 0 {
                    eprintln!(
                        "hcache replica: passed over {} command(s) this node does not know",
                        applied.unknown
                    );
                }
                if applied.ack {
                    acked = None;
                }
            }
            if acked.is_none_or(|at| at.elapsed() >= ACK_EVERY) {
                let offset = engine.repl_info().offset.to_string();
                self.send(&[b"REPLCONF", b"ACK", offset.as_bytes()])?;
                acked = Some(Instant::now());
            }
            fresh = match self.read_more() {
                Ok(fresh) => fresh,
                Err(e) if e.kind() == ErrorKind::Interrupted => return Ok(()),
                Err(e) => return Err(e),
            };
        }
    }
}
