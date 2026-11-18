//! RESP over TCP on top of `hotstash_core::engine::Engine`.
//!
//! Threads: one acceptor, N event-loop workers, one executor for commands
//! that need the node to themselves (EVAL, FLUSHALL, escalated multi-key
//! commands). A worker never sleeps anywhere but in `poll`: a command that
//! cannot run at once parks its connection, and the reply comes back
//! through the worker's inbox.
//!
//! BLPOP and its relatives wait the same way. A connection that finds
//! nothing lists itself under its keys in `Shared::blockers`; whoever
//! changes one of those keys tells the first connection in line to try
//! again, and a connection that leaves the line passes the turn on.

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use follow::{Follower, Handover, Phase};
use hotstash_core::acl::{self, User};
use hotstash_core::cmd::{
    Class, ClientCmd, Cmd, Failover, KillFilter, LogKind, PubSubQuery, Sub as SubCmd, Tracking, Txn,
};
use hotstash_core::cmdtable;
use hotstash_core::engine::{
    Engine, Hooks, Protected, Submitted, Ticket, DEBUG_REFUSED, PREFETCH_KEYS,
};
use hotstash_core::ops::MultiOp;
use hotstash_core::repl::SyncPoint;
use hotstash_core::resp::{self, Limits, Parser, Reply, Shape};
use hotstash_core::txn::{TxnState, TxnStep};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};
use pubsub::{Message, Registry, Sub};
use rustls::ServerConnection;
use std::any::Any;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::io::{self, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracking::{Listener, Tracker};

pub mod container;
mod follow;
mod memcached;
mod metrics;
mod observe;
mod pubsub;
pub mod tls;
mod tracking;

pub use rustls;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub addr: SocketAddr,
    pub workers: usize,
    pub max_conns: usize,
    /// Commands one connection may run per turn before other connections
    /// of the same worker get theirs (rule 5).
    pub pipeline_cap: usize,
    pub limits: Limits,
    /// Whether to listen on `addr` without TLS.
    pub plain: bool,
    /// Listen for TLS connections as well, or only.
    pub tls: Option<TlsListener>,
    /// A connection that sends nothing for this long is closed (`timeout`;
    /// CONFIG SET changes it). `None` leaves the database's setting alone.
    pub idle_timeout: Option<Duration>,
    /// A connection that takes none of the output waiting for it for this
    /// long is closed; zero for never.
    pub stall_timeout: Duration,
    /// What SHUTDOWN calls to stop the process; without it, SHUTDOWN
    /// fails.
    pub on_shutdown: Option<OnShutdown>,
    /// Speak the Memcached text protocol here as well, over database 0.
    pub memcached: Option<SocketAddr>,
    /// Serve Prometheus metrics here, over HTTP (`GET /metrics`).
    pub metrics: Option<SocketAddr>,
}

/// Stops the process the way a signal would: the server, then the data
/// file. It returns at once; the stopping happens elsewhere.
#[derive(Clone)]
pub struct OnShutdown(pub Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for OnShutdown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OnShutdown")
    }
}

/// Where TLS connections are accepted, and with which certificate
/// (`tls::server_config` makes one from PEM files).
#[derive(Debug, Clone)]
pub struct TlsListener {
    pub addr: SocketAddr,
    pub config: Arc<rustls::ServerConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            addr: SocketAddr::from(([127, 0, 0, 1], 6379)),
            workers: std::thread::available_parallelism().map_or(4, |n| n.get()),
            max_conns: 10_000,
            pipeline_cap: 64,
            limits: Limits::default(),
            plain: true,
            tls: None,
            idle_timeout: None,
            stall_timeout: Duration::from_secs(60),
            on_shutdown: None,
            memcached: None,
            metrics: None,
        }
    }
}

const WAKER: Token = Token(0);
const MAX_WORKERS: usize = 256;
const MAX_CONNS_PER_WORKER: usize = 1 << 24;
const READ_CHUNK: usize = 64 * 1024;
/// Bytes one connection may read per turn.
const READ_BUDGET: usize = 4 * READ_CHUNK;
/// A connection with this much unsent output stops executing commands
/// until the client reads.
const OUT_SOFT_LIMIT: usize = 1 << 20;
/// What a connection may send before it has logged in: enough for HELLO
/// with all its options, and no more, so that nobody fills memory without
/// a password.
const PRE_AUTH_ARGS: usize = 16;
const PRE_AUTH_BYTES: usize = 16 * 1024;
/// Input a connection may send ahead while it waits in a blocking command.
/// Beyond it the connection is closed: its hang-up could not be seen
/// behind more.
const BLOCKED_BACKLOG: usize = 1 << 20;
/// An emptied connection buffer larger than this is freed, not reused.
const BUF_KEEP: usize = 1 << 20;
/// The executor releases the gate after this many jobs or this much time,
/// so the shared side is never locked out by a stream of exclusive work.
const EXCL_BATCH: usize = 32;
const EXCL_HOLD: Duration = Duration::from_micros(500);
/// How often the idle executor runs background expiry.
/// A subscriber this far behind is disconnected, as Redis does
/// (`client-output-buffer-limit pubsub`).
const PUBSUB_OUT_LIMIT: usize = 32 << 20;
const EXPIRE_TICK: Duration = Duration::from_millis(100);
/// Where a RESP2 connection hears which keys to drop from its cache.
const INVALIDATE_CHANNEL: &[u8] = b"__redis__:invalidate";
/// Invalidations kept back for a connection whose command is running.
/// Past this many they become one that says: every key.
const HELD_MAX: usize = 1024;
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(10);
/// How often a worker looks for connections whose time is up.
const SWEEP_EVERY: Duration = Duration::from_secs(1);
/// A TLS connection has this long to finish its handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// What a turn of reading is charged for TLS records that brought no
/// data, so that a peer cannot hold a worker with them.
const EMPTY_READ_COST: usize = READ_CHUNK / 16;
/// `Conn::sync_need` of a connection no sync has been asked for yet.
const NO_SYNC: u64 = u64::MAX;
const PLAIN: Token = Token(1);
const SECURE: Token = Token(2);
/// How long `shutdown` waits for the executor to finish what it is running.
const STOP_GRACE: Duration = Duration::from_secs(5);
const STOP_POLL: Duration = Duration::from_millis(1);
/// A replica that says nothing for this long is gone (`repl-timeout`).
const REPL_TIMEOUT: Duration = Duration::from_secs(60);
/// How often the stream gets a PING, so that replicas can tell a quiet
/// primary from a lost one (`repl-ping-replica-period`).
const PING_EVERY: Duration = Duration::from_secs(10);
const NOT_INT: &str = "ERR value is not an integer or out of range";

/// One per server started in this process. Part of every ticket, so work a
/// stopped server left in the engine cannot reach the connections of the
/// next server on the same engine.
static NEXT_EPOCH: AtomicU64 = AtomicU64::new(1);

enum Msg {
    /// A connection, whether it speaks TLS, and its id.
    NewConn(TcpStream, bool, u64),
    /// CLIENT KILL chose this connection.
    Kill(Sub),
    /// The sync with this number, asked for by `Worker::commit`, is done;
    /// `false` when it failed.
    Synced(u64, bool),
    Complete(Ticket, Reply),
    Push(Sub, Arc<Message>),
    /// A key this blocked connection waits for has changed.
    Retry(Sub),
    /// A key the listener may have cached changed; `None`: every key.
    Invalidate(Listener, Option<Vec<u8>>),
    /// A line of MONITOR output for a connection in MONITOR.
    Line(Sub, Arc<Vec<u8>>),
    /// The sync of the replica in this slot, made on a thread of its own,
    /// is ready to start.
    Start(Ticket, Box<Start>),
    /// The replication stream grew: this replica's link may send more.
    Feed(Sub),
    /// A replica took more of the stream: this connection in WAIT counts
    /// again.
    Acked(Sub),
}

/// How a replica's sync begins.
enum Start {
    /// The whole dataset as an RDB file, then the stream. The replicas that
    /// asked at the same time share the file.
    Full(Arc<Vec<u8>>, SyncPoint),
    /// The stream alone, from where the replica is.
    Continue(SyncPoint),
    Refused(Reply),
}

enum Job {
    /// A command, and the user it runs for when that user has limits its
    /// scripts must keep.
    Run(Cmd, Ticket, Option<Arc<User>>),
    Stop,
}

struct WorkerHandle {
    inbox: SegQueue<Msg>,
    waker: Waker,
    /// Set while a wake-up is on its way and the worker has not run yet.
    wake_pending: AtomicBool,
    /// Set while the worker waits for the gate to reopen.
    gate_waiting: AtomicBool,
    /// Commands this worker has read, for INFO.
    commands: AtomicU64,
}

/// What CLIENT LIST tells about one connection.
struct ClientEntry {
    addr: String,
    laddr: String,
    name: Vec<u8>,
    lib_name: Vec<u8>,
    lib_ver: Vec<u8>,
    since: Instant,
    resp3: bool,
    db: usize,
    user: Vec<u8>,
    /// Its channels, patterns and shard channels.
    subs: [usize; 3],
    /// Its worker and its slot there.
    at: (usize, usize),
    /// It is the link of a replica.
    replica: bool,
}

impl ClientEntry {
    fn line(&self, id: u64) -> String {
        format!(
            "id={id} addr={} laddr={} name={} age={} flags={} db={} sub={} psub={} ssub={} \
             user={} resp={} lib-name={} lib-ver={}\n",
            self.addr,
            self.laddr,
            String::from_utf8_lossy(&self.name),
            self.since.elapsed().as_secs(),
            match (self.replica, self.listening()) {
                (true, _) => "S",
                (false, true) => "P",
                (false, false) => "N",
            },
            self.db,
            self.subs[0],
            self.subs[1],
            self.subs[2],
            String::from_utf8_lossy(&self.user),
            if self.resp3 { 3 } else { 2 },
            String::from_utf8_lossy(&self.lib_name),
            String::from_utf8_lossy(&self.lib_ver),
        )
    }

    fn listening(&self) -> bool {
        self.subs != [0; 3]
    }
}

struct Shared {
    engine: Arc<Engine>,
    workers: Box<[CachePadded<WorkerHandle>]>,
    jobs: Sender<Job>,
    stop: AtomicBool,
    next_worker: AtomicUsize,
    conns: AtomicUsize,
    epoch: u64,
    /// Whether the executor holds the exclusive side of the gate, so
    /// `shutdown` kills a script only if it is this server's.
    executor_busy: Mutex<bool>,
    pubsub: Registry,
    next_conn: AtomicU64,
    /// Times a blocked connection was sent to look again, for INFO.
    retries: AtomicU64,
    /// Blocked connections by the hash of a key they wait for, the one
    /// that has waited longest first.
    blockers: Mutex<HashMap<u64, VecDeque<Sub>>>,
    /// Open connections by id.
    clients: Mutex<BTreeMap<u64, ClientEntry>>,
    /// Who cached which key (CLIENT TRACKING).
    tracker: Tracker,
    /// The connections in MONITOR, and how many they are.
    monitors: Mutex<Vec<Sub>>,
    monitoring: AtomicUsize,
    on_shutdown: Option<OnShutdown>,
    /// The replicas this node feeds, by connection id.
    replicas: Mutex<BTreeMap<u64, Replica>>,
    /// The connections in WAIT.
    waiting: Mutex<Vec<Sub>>,
    /// The link to this node's primary, while it is a replica.
    follower: Mutex<Option<Follower>>,
    /// REPLICAOF and PSYNC FAILOVER change the role one at a time.
    switching: Mutex<()>,
    /// When the stream gets its next PING.
    next_ping: Mutex<Instant>,
    /// The port this node tells its primary it listens on.
    port: u16,
    /// Syncs by kind, for INFO: whole, from where the replica was, and
    /// asked to go on but could not.
    sync_full: AtomicU64,
    sync_partial_ok: AtomicU64,
    sync_partial_err: AtomicU64,
    /// The replicas waiting for the snapshot a thread is making, by worker
    /// and ticket; `None` when no thread is at one.
    sync_asked: Mutex<Option<Vec<(usize, Ticket)>>>,
    /// The FAILOVER under way, if one is.
    failover: Mutex<Handing>,
    /// Commands that add to the stream wait (a FAILOVER under way).
    writes_paused: AtomicBool,
    /// CLIENT PAUSE: until when, and whether it holds every command.
    client_pause: Mutex<Option<(Instant, bool)>>,
    /// Set while `client_pause` may hold one, so that a command need not
    /// take the lock to know there is none.
    client_paused: AtomicBool,
    /// The most connections at once (`maxclients`), and those turned away
    /// for it (`rejected_connections`).
    max_conns: usize,
    rejected: AtomicU64,
}

/// Where a FAILOVER stands (`master_failover_state`).
#[derive(Default)]
enum Handing {
    #[default]
    No,
    /// Writes wait until a replica, or the one named, has the whole
    /// stream; or until the time is up.
    Waiting {
        to: Option<(String, i64)>,
        until: Option<Instant>,
        force: bool,
    },
    /// This node follows that replica and asks it to take over.
    Following,
}

impl Handing {
    /// As INFO shows it, on a node that is a `replica` or not. Following
    /// begins on its own thread: until this node is a replica it is still
    /// waiting, so whoever sees "failover-in-progress" sees the role of a
    /// replica too, as with Valkey.
    fn name(&self, replica: bool) -> &'static str {
        match self {
            Handing::No => "no-failover",
            Handing::Waiting { .. } => "waiting-for-sync",
            Handing::Following if replica => "failover-in-progress",
            Handing::Following => "waiting-for-sync",
        }
    }
}

/// A replica this node feeds.
struct Replica {
    at: Sub,
    ip: String,
    port: u16,
    /// How far it said it got (REPLCONF ACK), and when.
    ack: u64,
    acked: Instant,
    /// It has its file and takes the stream.
    online: bool,
}

thread_local! {
    /// (address of the `Shared` this thread works for, worker index).
    static CURRENT: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

fn pack(epoch: u64, worker: usize, idx: usize, gen: u32) -> Ticket {
    let conn = ((worker as u64) << 56) | ((idx as u64) << 32) | u64::from(gen);
    (Ticket::from(epoch) << 64) | Ticket::from(conn)
}

/// (epoch, worker, connection slot, generation)
fn unpack(ticket: Ticket) -> (u64, usize, usize, u32) {
    let conn = ticket as u64;
    (
        (ticket >> 64) as u64,
        (conn >> 56) as usize,
        ((conn >> 32) & 0xff_ffff) as usize,
        conn as u32,
    )
}

impl Shared {
    fn id(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    /// The worker index of the calling thread, if it is one of ours.
    fn on_worker(&self) -> Option<usize> {
        let (shared, worker) = CURRENT.get();
        (shared == self.id()).then_some(worker)
    }

    fn executor_busy(&self) -> MutexGuard<'_, bool> {
        // The flag is a plain bool; a panic elsewhere cannot leave it torn.
        self.executor_busy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn clients(&self) -> MutexGuard<'_, BTreeMap<u64, ClientEntry>> {
        self.clients.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn client_cmd(&self, id: u64, cmd: &ClientCmd) -> Reply {
        let mut clients = self.clients();
        if *cmd == ClientCmd::List {
            let lines: String = clients.iter().map(|(id, c)| c.line(*id)).collect();
            return Reply::Bulk(lines.into_bytes());
        }
        let Some(me) = clients.get_mut(&id) else {
            return Reply::err("ERR no such client");
        };
        match cmd {
            ClientCmd::SetName(name) => me.name.clone_from(name),
            ClientCmd::SetInfo {
                version: true,
                value,
            } => me.lib_ver.clone_from(value),
            ClientCmd::SetInfo {
                version: false,
                value,
            } => me.lib_name.clone_from(value),
            ClientCmd::GetName if me.name.is_empty() => return Reply::Nil,
            ClientCmd::GetName => return Reply::Bulk(me.name.clone()),
            ClientCmd::Id => return Reply::Int(id as i64),
            ClientCmd::Info | ClientCmd::List => return Reply::Bulk(me.line(id).into_bytes()),
            // Answered by the connection's worker.
            ClientCmd::Tracking(_)
            | ClientCmd::Caching(_)
            | ClientCmd::TrackingInfo
            | ClientCmd::GetRedir
            | ClientCmd::Kill(_)
            | ClientCmd::Pause { .. }
            | ClientCmd::Unpause => return Reply::err("ERR internal: not answered here"),
        }
        Reply::Ok
    }

    /// Send one invalidation on its way: `key` changed, or with `None`
    /// every key may have.
    fn invalidate(&self, listener: Listener, key: Option<&[u8]>) {
        if let Some(handle) = self.workers.get(listener.to.worker) {
            handle
                .inbox
                .push(Msg::Invalidate(listener, key.map(<[u8]>::to_vec)));
            self.wake(listener.to.worker);
        }
    }

    fn monitors(&self) -> MutexGuard<'_, Vec<Sub>> {
        self.monitors.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Show a command to the connections in MONITOR.
    fn feed_monitors(&self, id: u64, db: usize, args: &[Vec<u8>]) {
        let addr = self.clients().get(&id).map(|c| c.addr.clone());
        let line = Arc::new(observe::monitor_line(
            db,
            addr.as_deref().unwrap_or("?"),
            args,
        ));
        let watchers = self.monitors().clone();
        for sub in watchers {
            if let Some(handle) = self.workers.get(sub.worker) {
                handle.inbox.push(Msg::Line(sub, line.clone()));
                self.wake(sub.worker);
            }
        }
    }

    /// The command `conn` was timing is answered: log it if it was slow,
    /// large or had a large reply. Returns the time it read, if it did,
    /// for the next command to start from: one clock read per command
    /// instead of two.
    fn slow(&self, conn: &mut Conn) -> Option<Instant> {
        let started = conn.started.take()?;
        let limits = self.engine.log_limits();
        let now = Instant::now();
        let micros = u64::try_from(now.duration_since(started).as_micros()).unwrap_or(u64::MAX);
        let reply_bytes = conn.outbuf.len().saturating_sub(conn.reply_from);
        let values = [micros, conn.request_bytes as u64, reply_bytes as u64];
        let mut hits = LogKind::ALL
            .into_iter()
            .filter(|kind| {
                limits[*kind as usize].is_some_and(|limit| values[*kind as usize] >= limit)
            })
            .peekable();
        hits.peek()?;
        let (addr, name) = self
            .clients()
            .get(&conn.id)
            .map(|c| (c.addr.clone(), c.name.clone()))
            .unwrap_or_default();
        let args = observe::unstash(&conn.stash);
        for kind in hits {
            let value = values[kind as usize];
            self.engine
                .log_record(kind, value, args.clone(), addr.clone(), name.clone());
        }
        // Logging took time of its own, not the next command's.
        None
    }

    fn replicas(&self) -> MutexGuard<'_, BTreeMap<u64, Replica>> {
        // Every change leaves the table consistent.
        self.replicas.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn waiting(&self) -> MutexGuard<'_, Vec<Sub>> {
        self.waiting.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn follower(&self) -> MutexGuard<'_, Option<Follower>> {
        self.follower.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The replicas that have the stream up to `offset`.
    fn acked_at(&self, offset: u64) -> i64 {
        let replicas = self.replicas();
        let have = replicas.values().filter(|r| r.online && r.ack >= offset);
        i64::try_from(have.count()).unwrap_or(i64::MAX)
    }

    /// A replica says it has the stream up to `offset`: if that is further
    /// than before, the connections in WAIT count again and a FAILOVER may
    /// go on.
    fn acked(self: &Arc<Self>, id: u64, offset: u64) {
        let grew = match self.replicas().get_mut(&id) {
            Some(replica) => {
                replica.acked = Instant::now();
                let grew = offset > replica.ack;
                replica.ack = replica.ack.max(offset);
                grew
            }
            None => false,
        };
        if !grew {
            return;
        }
        let waiting = self.waiting().clone();
        for sub in waiting {
            if let Some(handle) = self.workers.get(sub.worker) {
                handle.inbox.push(Msg::Acked(sub));
                self.wake(sub.worker);
            }
        }
        self.failover_tick();
    }

    fn failover(&self) -> MutexGuard<'_, Handing> {
        // Every change leaves a whole state.
        self.failover.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether writes wait: for a FAILOVER, or a CLIENT PAUSE.
    fn writes_paused(&self) -> bool {
        self.writes_paused.load(SeqCst) || self.client_pause().is_some()
    }

    /// Whether `cmd` waits now. A FAILOVER, and CLIENT PAUSE WRITE, hold
    /// back what adds to the stream; CLIENT PAUSE ALL every command but
    /// the one that ends it and a replica's acknowledgements.
    fn holds(&self, cmd: &Cmd) -> bool {
        if self.writes_paused.load(SeqCst) && cmd.pausable() {
            return true;
        }
        let Some((_, all)) = self.client_pause() else {
            return false;
        };
        let exempt = matches!(cmd, Cmd::Replconf(_))
            || matches!(cmd, Cmd::Client(sub) if **sub == ClientCmd::Unpause);
        cmd.pausable() || all && !exempt
    }

    /// The CLIENT PAUSE in force: until when, and whether it holds every
    /// command.
    fn client_pause(&self) -> Option<(Instant, bool)> {
        if !self.client_paused.load(SeqCst) {
            return None;
        }
        let mut pause = self
            .client_pause
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match *pause {
            Some((until, all)) if until > Instant::now() => Some((until, all)),
            _ => {
                *pause = None;
                self.client_paused.store(false, SeqCst);
                None
            }
        }
    }

    /// CLIENT PAUSE: as in Redis, the pause lasts until the later of the
    /// two ends, and a pause of writes does not cut short one of every
    /// command.
    fn pause_clients(&self, ms: u64, all: bool) {
        let now = Instant::now();
        let until = now
            .checked_add(Duration::from_millis(ms))
            .or_else(|| now.checked_add(LONGEST_PAUSE))
            .unwrap_or(now);
        let mut pause = self
            .client_pause
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let current = match *pause {
            Some((at, all)) if at > now => Some((at, all)),
            _ => None,
        };
        *pause = Some(match current {
            Some((at, true)) if !all && until <= at => (at, true),
            Some((at, _)) => (at.max(until), all),
            None => (until, all),
        });
        self.client_paused.store(true, SeqCst);
    }

    /// CLIENT UNPAUSE: what waited runs.
    fn unpause_clients(&self) {
        *self
            .client_pause
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        self.client_paused.store(false, SeqCst);
        (0..self.workers.len()).for_each(|worker| self.wake(worker));
    }

    /// The commands that waited for a FAILOVER run.
    fn resume_writes(&self) {
        self.writes_paused.store(false, SeqCst);
        (0..self.workers.len()).for_each(|worker| self.wake(worker));
    }

    /// Move a FAILOVER on: once its replica has the whole stream, or its
    /// time is up.
    fn failover_tick(self: &Arc<Self>) {
        let mut failover = self.failover();
        let Handing::Waiting { to, until, force } = &*failover else {
            return;
        };
        let offset = self.engine.repl_info().offset;
        let synced = self
            .replicas()
            .values()
            .find(|r| {
                r.online
                    && r.ack == offset
                    && to.as_ref().is_none_or(|(host, port)| {
                        r.ip.eq_ignore_ascii_case(host) && i64::from(r.port) == *port
                    })
            })
            .map(|r| (r.ip.clone(), r.port));
        let target = match synced.clone() {
            Some(target) => {
                eprintln!(
                    "hotstash: FAILOVER target {}:{} is synced, failing over",
                    target.0, target.1
                );
                target
            }
            None if until.is_some_and(|at| Instant::now() >= at) => {
                let forced = to
                    .as_ref()
                    .filter(|_| *force)
                    .and_then(|(host, port)| Some((host.clone(), u16::try_from(*port).ok()?)));
                let Some(target) = forced else {
                    drop(failover);
                    self.abort_failover("Replica never caught up before timeout");
                    return;
                };
                eprintln!(
                    "hotstash: FAILOVER to {}:{} time out exceeded, failing over",
                    target.0, target.1
                );
                target
            }
            None => return,
        };
        // Not forced: if writes got into the stream after this, it goes
        // back to waiting.
        let back = synced.is_some().then(|| Handing::Waiting {
            to: to.clone(),
            until: *until,
            force: *force,
        });
        *failover = Handing::Following;
        drop(failover);
        let shared = self.clone();
        let spawned = std::thread::Builder::new()
            .name("hotstash-failover".into())
            .spawn(move || shared.hand_over(target.0, target.1, back));
        if let Err(e) = spawned {
            self.abort_failover(&format!("cannot start the handover: {e}"));
        }
    }

    /// The FAILOVER's last step, on a thread of its own: follow the replica
    /// and ask it to take over. Unless forced, only once it has the whole
    /// stream; else the FAILOVER goes `back` to waiting.
    fn hand_over(self: &Arc<Self>, host: String, port: u16, back: Option<Handing>) {
        let _turn = self
            .switching
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !matches!(*self.failover(), Handing::Following) {
            return;
        }
        self.unfollow();
        // On the exclusive side: a write that got past the pause before it
        // began is in the stream now, and none comes after.
        self.engine.become_replica();
        if let Some(back) = back {
            let offset = self.engine.repl_info().offset;
            let synced = self
                .replicas()
                .values()
                .any(|r| r.ip.eq_ignore_ascii_case(&host) && r.port == port && r.ack == offset);
            if !synced {
                self.engine.become_primary();
                let mut failover = self.failover();
                if matches!(*failover, Handing::Following) {
                    *failover = back;
                }
                return;
            }
        }
        self.release_blocked();
        let me = Arc::downgrade(self);
        let handover: Handover = Box::new(move |took| {
            if let Some(shared) = me.upgrade() {
                shared.handed_over(took);
            }
        });
        match Follower::start(self.engine.clone(), host, port, self.port, Some(handover)) {
            Ok(follower) => *self.follower() = Some(follower),
            Err(e) => {
                eprintln!("hotstash: FAILOVER aborted: cannot start following: {e}");
                self.engine.become_primary();
                *self.failover() = Handing::No;
                self.resume_writes();
            }
        }
    }

    /// The replica answered the PSYNC FAILOVER: it took over, or this
    /// node takes its role back.
    fn handed_over(self: &Arc<Self>, took: bool) {
        if !took {
            self.abort_failover("Failover target rejected psync request");
            return;
        }
        let mut failover = self.failover();
        if matches!(*failover, Handing::Following) {
            *failover = Handing::No;
            drop(failover);
            eprintln!("hotstash: FAILOVER done, this node is a replica");
            self.resume_writes();
        }
    }

    /// Stop the FAILOVER under way: whether there was one. A node that
    /// already follows its replica takes its role back, on a thread of its
    /// own since that takes the exclusive side.
    fn abort_failover(self: &Arc<Self>, why: &str) -> bool {
        let was = std::mem::take(&mut *self.failover());
        match was {
            Handing::No => return false,
            Handing::Waiting { .. } => self.resume_writes(),
            Handing::Following => {
                let shared = self.clone();
                let take_back = move || {
                    let _turn = shared
                        .switching
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    // Only from the replica this FAILOVER was handing to.
                    if shared.follower().as_ref().is_some_and(|f| f.handing) {
                        shared.unfollow();
                        shared.engine.become_primary();
                    }
                    // Not under a FAILOVER that began since.
                    if matches!(*shared.failover(), Handing::No) {
                        shared.resume_writes();
                    }
                };
                let spawned = std::thread::Builder::new()
                    .name("hotstash-failover".into())
                    .spawn(take_back);
                if let Err(e) = spawned {
                    eprintln!("hotstash: cannot take the primary's role back: {e}");
                    self.resume_writes();
                }
            }
        }
        eprintln!("hotstash: FAILOVER aborted: {why}");
        true
    }

    /// A replica needs the whole dataset: it gets the snapshot a thread is
    /// making, if one is and has not handed it out yet, or a new one.
    /// However many replicas ask at once, one snapshot is made at a time
    /// and they share it.
    fn full_sync(self: &Arc<Self>, worker: usize, ticket: Ticket) -> io::Result<()> {
        let mut asked = self
            .sync_asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(asked) = asked.as_mut() {
            asked.push((worker, ticket));
            return Ok(());
        }
        let maker = self.clone();
        let make = move || {
            let (rdb, at) = maker.engine.repl_snapshot();
            let rdb = Arc::new(rdb);
            let asked = maker
                .sync_asked
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            for (worker, ticket) in asked.unwrap_or_default() {
                maker.send_start(worker, ticket, Start::Full(rdb.clone(), at.clone()));
            }
        };
        std::thread::Builder::new()
            .name("hotstash-sync".into())
            .spawn(make)?;
        *asked = Some(vec![(worker, ticket)]);
        Ok(())
    }

    fn send_start(&self, worker: usize, ticket: Ticket, start: Start) {
        if let Some(handle) = self.workers.get(worker) {
            handle.inbox.push(Msg::Start(ticket, Box::new(start)));
            self.wake(worker);
        }
    }

    /// Put a PING in the stream when it is time for one. Not while a
    /// FAILOVER waits for a replica to have the whole stream.
    fn ping_replicas(&self, now: Instant) {
        if self.writes_paused() {
            return;
        }
        {
            let mut next = self
                .next_ping
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if now < *next {
                return;
            }
            *next = now + PING_EVERY;
        }
        self.engine.repl_ping();
    }

    /// The link to the primary is up.
    fn linked(&self) -> bool {
        self.follower()
            .as_ref()
            .is_some_and(|f| f.status().phase == Phase::Connected)
    }

    /// Stop following a primary; the link's thread is gone on return.
    fn unfollow(&self) {
        let follower = self.follower().take();
        drop(follower);
    }

    /// REPLICAOF: follow `target`, or with `None` take writes again.
    fn switch(&self, target: Option<(String, u16)>) -> Reply {
        let _turn = self
            .switching
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // A FAILOVER may have begun since the worker looked.
        if !matches!(*self.failover(), Handing::No) {
            return Reply::err("ERR REPLICAOF not allowed while failing over.");
        }
        self.unfollow();
        let Some((host, port)) = target else {
            self.engine.become_primary();
            return Reply::Ok;
        };
        self.engine.become_replica();
        self.release_blocked();
        match Follower::start(self.engine.clone(), host, port, self.port, None) {
            Ok(follower) => {
                *self.follower() = Some(follower);
                Reply::Ok
            }
            Err(e) => Reply::Error(format!("ERR cannot start following the primary: {e}")),
        }
    }

    /// What a sync that needs the exclusive side begins with. PSYNC
    /// FAILOVER, from this replica's own primary, first makes it the
    /// primary.
    fn prepare(&self, asked: Option<(Vec<u8>, i64)>, failover: bool) -> Start {
        if failover {
            let _turn = self
                .switching
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !self.engine.is_replica() {
                return Start::Refused(Reply::err("ERR PSYNC FAILOVER can't be sent to a master."));
            }
            let replid = self.engine.repl_info().replid;
            if !asked
                .as_ref()
                .is_some_and(|(id, _)| *id == replid.as_bytes())
            {
                return Start::Refused(Reply::err(
                    "ERR PSYNC FAILOVER replid must match my replid.",
                ));
            }
            self.unfollow();
            self.engine.become_primary();
            let resumed = asked.and_then(|(id, offset)| self.engine.repl_resume(&id, offset));
            if let Some(at) = resumed {
                return Start::Continue(at);
            }
        }
        let (rdb, at) = self.engine.repl_snapshot();
        Start::Full(Arc::new(rdb), at)
    }

    /// The head of INFO's replication section: the role, the link to the
    /// primary, and the replicas.
    fn replication_head(&self) -> String {
        use std::fmt::Write as _;
        let secs = |at: Option<Instant>| {
            at.map_or(-1, |at| {
                i64::try_from(at.elapsed().as_secs()).unwrap_or(i64::MAX)
            })
        };
        let mut out = String::new();
        // Read first: a FAILOVER looks at the replicas while it holds it.
        let replica = self.engine.is_replica();
        let failover = self.failover().name(replica);
        if replica {
            let offset = self.engine.repl_info().offset;
            let (host, port, status) = self
                .follower()
                .as_ref()
                .map(|f| (f.host.clone(), f.port, Some(f.status())))
                .unwrap_or_default();
            let phase = status.map_or(Phase::Connect, |s| s.phase);
            let up = phase == Phase::Connected;
            let _ = write!(
                out,
                "role:slave\r\nmaster_host:{host}\r\nmaster_port:{port}\r\n\
                 master_link_status:{}\r\nmaster_last_io_seconds_ago:{}\r\n\
                 master_sync_in_progress:{}\r\nslave_read_repl_offset:{offset}\r\n\
                 slave_repl_offset:{offset}\r\n",
                if up { "up" } else { "down" },
                if up {
                    secs(status.and_then(|s| s.last_io))
                } else {
                    -1
                },
                u8::from(phase == Phase::Sync),
            );
            if !up {
                let since = secs(status.and_then(|s| s.down_since));
                let _ = write!(out, "master_link_down_since_seconds:{since}\r\n");
            }
            let _ = write!(
                out,
                "slave_priority:{}\r\nslave_read_only:1\r\nreplica_announced:{}\r\n",
                self.engine.replica_priority(),
                u8::from(self.engine.replica_announced()),
            );
        } else {
            out.push_str("role:master\r\n");
        }
        let replicas = self.replicas();
        let _ = write!(out, "connected_slaves:{}\r\n", replicas.len());
        for (i, r) in replicas.values().enumerate() {
            let _ = write!(
                out,
                "slave{i}:ip={},port={},state={},offset={},lag={},type=replica\r\n",
                r.ip,
                r.port,
                if r.online { "online" } else { "wait_bgsave" },
                r.ack,
                r.acked.elapsed().as_secs(),
            );
        }
        let _ = write!(out, "master_failover_state:{failover}\r\n");
        out
    }

    fn role(&self) -> Reply {
        if self.engine.is_replica() {
            let (host, port, phase) = self
                .follower()
                .as_ref()
                .map_or((String::new(), 0, Phase::Connect), |f| {
                    (f.host.clone(), f.port, f.status().phase)
                });
            let offset = match phase {
                Phase::Connected => i64::try_from(self.engine.repl_info().offset).unwrap_or(-1),
                _ => -1,
            };
            return Reply::Array(vec![
                Reply::bulk("slave"),
                Reply::Bulk(host.into_bytes()),
                Reply::Int(port.into()),
                Reply::bulk(phase.name()),
                Reply::Int(offset),
            ]);
        }
        let replicas = self
            .replicas()
            .values()
            .filter(|r| r.online)
            .map(|r| {
                Reply::Array(vec![
                    Reply::Bulk(r.ip.clone().into_bytes()),
                    Reply::Bulk(r.port.to_string().into_bytes()),
                    Reply::Bulk(r.ack.to_string().into_bytes()),
                ])
            })
            .collect();
        let offset = i64::try_from(self.engine.repl_info().offset).unwrap_or(i64::MAX);
        Reply::Array(vec![
            Reply::bulk("master"),
            Reply::Int(offset),
            Reply::Array(replicas),
        ])
    }

    fn blockers(&self) -> MutexGuard<'_, HashMap<u64, VecDeque<Sub>>> {
        // Every change leaves the table consistent.
        self.blockers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn retry(&self, sub: Sub) {
        if let Some(handle) = self.workers.get(sub.worker) {
            self.retries.fetch_add(1, SeqCst);
            handle.inbox.push(Msg::Retry(sub));
            self.wake(sub.worker);
        }
    }

    /// This node became a replica: every blocked connection tries again,
    /// and one that would write is told it no longer can.
    fn release_blocked(&self) {
        let mut subs: Vec<Sub> = self.blockers().values().flatten().copied().collect();
        subs.sort_unstable_by_key(|s| (s.worker, s.idx, s.conn));
        subs.dedup();
        for sub in subs {
            self.retry(sub);
        }
    }

    fn block(&self, keys: &[u64], sub: Sub) {
        self.engine.key_waiters().fetch_add(1, SeqCst);
        let mut blockers = self.blockers();
        for key in keys {
            blockers.entry(*key).or_default().push_back(sub);
        }
    }

    /// Take `sub` out of the lines it stands in. Whoever is first now gets
    /// a turn: `sub` may have been told about data it did not take.
    fn unblock(&self, keys: &[u64], sub: Sub) {
        let mut next = Vec::new();
        {
            let mut blockers = self.blockers();
            for key in keys {
                let Some(line) = blockers.get_mut(key) else {
                    continue;
                };
                line.retain(|s| *s != sub);
                match line.front() {
                    Some(first) => next.push(*first),
                    None => {
                        blockers.remove(key);
                    }
                }
            }
        }
        self.engine.key_waiters().fetch_sub(1, SeqCst);
        for sub in next {
            self.retry(sub);
        }
    }

    /// `sub` looked and found nothing it can take: the one behind it in
    /// each line looks next.
    fn pass(&self, keys: &[u64], sub: Sub) {
        let mut next = Vec::new();
        {
            let blockers = self.blockers();
            for key in keys {
                let behind = blockers.get(key).and_then(|line| {
                    let at = line.iter().position(|s| *s == sub)?;
                    line.iter().skip(at + 1).find(|s| **s != sub).copied()
                });
                next.extend(behind);
            }
        }
        for sub in next {
            self.retry(sub);
        }
    }

    fn wake(&self, worker: usize) {
        // A worker looks at its inbox and at the service queue before it
        // sleeps, so it never needs to wake itself.
        if self.on_worker() == Some(worker) {
            return;
        }
        let handle = &self.workers[worker];
        if !handle.wake_pending.swap(true, SeqCst) && handle.waker.wake().is_err() {
            handle.wake_pending.store(false, SeqCst);
        }
    }
}

impl Hooks for Shared {
    fn complete(&self, ticket: Ticket, reply: Reply) {
        let (epoch, worker, _, _) = unpack(ticket);
        // Work of an earlier server on this engine: its client is gone.
        if epoch != self.epoch {
            return;
        }
        let Some(handle) = self.workers.get(worker) else {
            return;
        };
        handle.inbox.push(Msg::Complete(ticket, reply));
        self.wake(worker);
    }

    fn wake_any(&self) {
        if self.on_worker().is_some() {
            return;
        }
        let worker = self.next_worker.fetch_add(1, SeqCst) % self.workers.len();
        self.wake(worker);
    }

    fn gate_opened(&self) {
        for (worker, handle) in self.workers.iter().enumerate() {
            if handle.gate_waiting.swap(false, SeqCst) {
                self.wake(worker);
            }
        }
    }

    fn escalate(&self, cmd: Cmd, ticket: Ticket) {
        if unpack(ticket).0 != self.epoch {
            return;
        }
        if self.jobs.send(Job::Run(cmd, ticket, None)).is_err() {
            self.complete(ticket, Reply::err("ERR server is shutting down"));
        }
    }

    fn publish(&self, channel: &[u8], payload: &[u8]) -> usize {
        let targets = self.pubsub.targets(channel);
        let mut direct = None;
        for (sub, pattern) in &targets {
            let message = |pattern: Option<Vec<u8>>| {
                Arc::new(Message {
                    pattern,
                    shard: false,
                    channel: channel.to_vec(),
                    payload: payload.to_vec(),
                })
            };
            let msg = match pattern {
                Some(pattern) => message(Some(pattern.clone())),
                None => direct.get_or_insert_with(|| message(None)).clone(),
            };
            if let Some(handle) = self.workers.get(sub.worker) {
                handle.inbox.push(Msg::Push(*sub, msg));
                self.wake(sub.worker);
            }
        }
        targets.len()
    }

    fn spublish(&self, channel: &[u8], payload: &[u8]) -> usize {
        let targets = self.pubsub.shard_targets(channel);
        if targets.is_empty() {
            return 0;
        }
        let msg = Arc::new(Message {
            pattern: None,
            shard: true,
            channel: channel.to_vec(),
            payload: payload.to_vec(),
        });
        for sub in &targets {
            if let Some(handle) = self.workers.get(sub.worker) {
                handle.inbox.push(Msg::Push(*sub, msg.clone()));
                self.wake(sub.worker);
            }
        }
        targets.len()
    }

    fn pubsub(&self, query: &PubSubQuery) -> Option<Reply> {
        Some(self.pubsub.query(query))
    }

    fn info(&self) -> String {
        let commands: u64 = self.workers.iter().map(|w| w.commands.load(SeqCst)).sum();
        let tracking = self.tracker.active();
        let pubsub = self.pubsub.sizes();
        format!(
            "# Clients\r\nconnected_clients:{}\r\nmaxclients:{}\r\nblocked_clients:{}\r\n\
             tracking_clients:{tracking}\r\n\
             # Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{commands}\r\n\
             rejected_connections:{}\r\nblocked_retries:{}\r\n\
             pubsub_channels:{}\r\npubsub_patterns:{}\r\n\
             pubsubshard_channels:{}\r\nsync_full:{}\r\nsync_partial_ok:{}\r\n\
             sync_partial_err:{}\r\n{}{}",
            self.conns.load(SeqCst),
            self.max_conns,
            // A tracking connection waits for keys to change too.
            self.engine
                .key_waiters()
                .load(SeqCst)
                .saturating_sub(tracking),
            self.next_conn.load(SeqCst) - 1,
            self.rejected.load(SeqCst),
            self.retries.load(SeqCst),
            pubsub[0],
            pubsub[1],
            pubsub[2],
            self.sync_full.load(SeqCst),
            self.sync_partial_ok.load(SeqCst),
            self.sync_partial_err.load(SeqCst),
            self.engine.keyspace_stats(),
            cpu_section(),
        )
    }

    fn key_changed(&self, hash: u64, key: &[u8]) {
        if self.tracker.active() != 0 {
            // Who wrote, when the engine knows and it is one of ours.
            let (epoch, worker, idx, _) = unpack(hotstash_core::engine::running());
            let writer = (epoch == self.epoch).then_some((worker, idx));
            for listener in self.tracker.changed(key, writer) {
                self.invalidate(listener, Some(key));
            }
        }
        let first = self
            .blockers()
            .get(&hash)
            .and_then(|line| line.front().copied());
        if let Some(sub) = first {
            self.retry(sub);
        }
    }

    fn replicate(&self) {
        let links: Vec<Sub> = self.replicas().values().map(|r| r.at).collect();
        for sub in links {
            if let Some(handle) = self.workers.get(sub.worker) {
                handle.inbox.push(Msg::Feed(sub));
                self.wake(sub.worker);
            }
        }
    }

    fn replication(&self) -> Option<String> {
        Some(self.replication_head())
    }

    fn keys_flushed(&self) {
        if self.tracker.active() != 0 {
            for listener in self.tracker.flushed() {
                self.invalidate(listener, None);
            }
        }
        // A reader of a consumer group has to learn that its group is gone.
        let first: Vec<Sub> = self
            .blockers()
            .values()
            .filter_map(|line| line.front().copied())
            .collect();
        for sub in first {
            self.retry(sub);
        }
    }
}

/// The hashes of the keys a blocking command waits on, each once: a key
/// named twice is still one place in one line.
fn wait_hashes(engine: &Engine, op: &MultiOp) -> Vec<u64> {
    let mut keys: Vec<u64> = op.wait_keys().iter().map(|k| engine.route(k).0).collect();
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// What a connection in BLPOP or one of its relatives is waiting for.
struct Block {
    op: Box<MultiOp>,
    deadline: Option<Instant>,
    /// Hashes of the keys it waits on.
    keys: Vec<u64>,
    /// Listed in `Shared::blockers`.
    listed: bool,
    /// An attempt is running and its reply has not arrived.
    in_flight: bool,
    /// A key changed while the attempt was running.
    again: bool,
    /// The attempt that runs was started by a change to a key.
    woken: bool,
}

/// CLIENT TRACKING as one connection has it on.
struct Track {
    opts: Tracking,
    listener: Listener,
    /// What CLIENT CACHING said about the next command.
    caching: Option<bool>,
}

/// The keys `cmd` reads, those of a transaction's commands included.
fn read_keys(cmd: &Cmd, f: &mut dyn FnMut(&[u8])) {
    match cmd {
        Cmd::In(_, inner) => read_keys(inner, f),
        Cmd::ExecBlock { queue, .. } => queue.iter().for_each(|queued| read_keys(queued, f)),
        other => other.for_each_key(|key| f(key)),
    }
}

enum State {
    /// Ready to parse the next command.
    Idle,
    /// A command is running elsewhere; its reply arrives through the inbox.
    Parked,
    /// A parsed command waits for the gate to reopen.
    GateWait(Cmd),
    /// A command that adds to the stream waits for a FAILOVER to end.
    Paused(Cmd),
}

/// Read decrypted bytes into `buf`: what TLS already holds, or else what
/// one more read of the socket brings. `Interrupted` says that records
/// arrived and none of them carried data.
fn tls_read(
    tls: &mut ServerConnection,
    stream: &mut TcpStream,
    buf: &mut [u8],
) -> io::Result<usize> {
    match tls.reader().read(buf) {
        Err(e) if e.kind() == ErrorKind::WouldBlock => {}
        // Data, the peer's goodbye (0), or an end without one (an error).
        done => return done,
    }
    if tls.read_tls(stream)? == 0 {
        return Ok(0);
    }
    if let Err(e) = tls.process_new_packets() {
        // Best effort: the alert that tells the peer why.
        let _ = tls.write_tls(stream);
        return Err(io::Error::new(ErrorKind::InvalidData, e));
    }
    match tls.reader().read(buf) {
        Err(e) if e.kind() == ErrorKind::WouldBlock => Err(ErrorKind::Interrupted.into()),
        done => done,
    }
}

struct Conn {
    stream: TcpStream,
    /// The TLS session of a connection that came in through the TLS port.
    tls: Option<Box<ServerConnection>>,
    /// The TLS goodbye was said.
    goodbye: bool,
    /// When the connection was accepted.
    since: Instant,
    /// When it last sent anything.
    last_input: Instant,
    /// Since when output has been waiting without any of it being taken.
    stalled: Option<Instant>,
    /// The command in flight may change what the data file holds.
    wrote: bool,
    /// The output holds the reply to such a command, and the change is not
    /// known to be on disk yet (`appendfsync always`): nothing is sent.
    unsynced: bool,
    /// The sync that covers everything this connection changed so far;
    /// `NO_SYNC` while none has been asked for.
    sync_need: u64,
    /// Listed in `Worker::held`.
    sync_held: bool,
    parser: Parser,
    inbuf: Vec<u8>,
    in_pos: usize,
    outbuf: Vec<u8>,
    out_pos: usize,
    state: State,
    /// The socket may have bytes we have not read.
    readable: bool,
    /// The peer has shut down its side: read on until the end shows.
    read_closed: bool,
    /// The parser needs bytes that have not arrived.
    need_more: bool,
    eof: bool,
    /// Close once the output is flushed.
    closing: bool,
    /// Already on the worker's run list.
    queued: bool,
    /// The client switched to RESP3 with HELLO.
    resp3: bool,
    /// How the command being answered types its reply in RESP3.
    shape: Shape,
    /// MULTI queue and WATCH list.
    txn: TxnState,
    /// Unique among this server's connections; names it in the registry.
    id: u64,
    subs: HashSet<Vec<u8>>,
    psubs: HashSet<Vec<u8>>,
    ssubs: HashSet<Vec<u8>>,
    /// Fell too far behind on published messages; close without flushing.
    overflow: bool,
    block: Option<Block>,
    /// Who this connection is: "default" until AUTH says otherwise.
    user: Arc<User>,
    /// `Acl::generation` when `user` was last looked up.
    acl_gen: u64,
    /// AUTH succeeded. Without it the connection is "default", and that
    /// only while "default" asks for no password.
    authed: bool,
    /// It is in its worker's list of connections a FAILOVER holds back.
    paused: bool,
    /// `acl_gen` when MULTI was sent.
    txn_gen: u64,
    /// What a command may be once this connection has logged in.
    limits: Limits,
    tracking: Option<Track>,
    /// In MONITOR: sent every command the server gets.
    monitor: bool,
    /// When the command being run was read, while slow commands are
    /// logged; `stash` then has what the log would show of it.
    started: Option<Instant>,
    stash: Vec<u8>,
    /// It comes from this machine.
    local: bool,
    /// The size of the command being run, and where its reply starts in
    /// `outbuf`, for the logs of large requests and replies.
    request_bytes: usize,
    reply_from: usize,
    /// Invalidations that arrived while a command of this connection was
    /// running elsewhere. They go out after its reply: a client that got
    /// them first would cache the reply and never hear of the change.
    held: Vec<Option<Vec<u8>>>,
    /// What REPLCONF said, before a sync.
    replconf: Option<Box<ReplConf>>,
    /// The link of a replica this connection feeds.
    link: Option<Box<Link>>,
    /// What this connection waits for in WAIT.
    wait: Option<Wait>,
    /// What its EXEC leaves to the server, while the engine runs the rest.
    exec_tail: Option<Box<ExecTail>>,
}

/// The commands of an EXEC only the server can carry out (CLIENT,
/// REPLICAOF, FAILOVER), each with its place in the reply. The engine runs the
/// others first, with a stand-in for each of these.
struct ExecTail {
    replies: Vec<Reply>,
    rest: VecDeque<(usize, Cmd)>,
    /// The place of the REPLICAOF whose role change is under way.
    waiting: Option<usize>,
}

/// Take out of an EXEC the commands the engine cannot run.
fn split_exec(cmd: Cmd) -> (Cmd, Option<Box<ExecTail>>) {
    let Cmd::ExecBlock { mut queue, watched } = cmd else {
        return (cmd, None);
    };
    let mut rest = VecDeque::new();
    for (at, queued) in queue.iter_mut().enumerate() {
        let inner = match &*queued {
            Cmd::In(_, inner) => &**inner,
            other => other,
        };
        if !matches!(inner, Cmd::Client(_) | Cmd::ReplicaOf(_) | Cmd::Failover(_)) {
            continue;
        }
        let taken = std::mem::replace(queued, Cmd::Static(Reply::Nil));
        let cmd = match taken {
            Cmd::In(_, inner) => *inner,
            cmd => cmd,
        };
        rest.push_back((at, cmd));
    }
    let tail = (!rest.is_empty()).then(|| {
        Box::new(ExecTail {
            replies: Vec::new(),
            rest,
            waiting: None,
        })
    });
    (Cmd::ExecBlock { queue, watched }, tail)
}

/// What a replica said of itself with REPLCONF.
#[derive(Default)]
struct ReplConf {
    port: u16,
    ip: Option<String>,
    rdb_only: bool,
}

/// The link of a replica this connection feeds.
struct Link {
    /// Where its stream goes on; `None` while its sync is being made.
    at: Option<SyncPoint>,
    /// The file of a full sync while it is being sent, and how much of it
    /// went: the stream comes after.
    rdb: Option<(Arc<Vec<u8>>, usize)>,
    /// It asked with PSYNC, and so says how far it got.
    psync: bool,
    /// Close once the file is sent (REPLCONF rdb-only).
    rdb_only: bool,
}

/// WAIT: the stream offset the replicas have to reach, how many, and
/// until when.
struct Wait {
    offset: u64,
    replicas: i64,
    deadline: Option<Instant>,
}

impl Conn {
    fn new(
        stream: TcpStream,
        tls: Option<Box<ServerConnection>>,
        limits: Limits,
        id: u64,
        acl: &acl::Acl,
    ) -> Conn {
        // The generation first: a change in between is then seen later.
        let acl_gen = acl.generation();
        let user = acl.default_user();
        let now = Instant::now();
        let local = stream.peer_addr().is_ok_and(|a| a.ip().is_loopback());
        Conn {
            local,
            tls,
            goodbye: false,
            since: now,
            last_input: now,
            stalled: None,
            wrote: false,
            unsynced: false,
            sync_need: NO_SYNC,
            sync_held: false,
            authed: false,
            paused: false,
            user,
            acl_gen,
            txn_gen: acl_gen,
            limits,
            stream,
            parser: Parser::new(limits),
            inbuf: Vec::new(),
            in_pos: 0,
            outbuf: Vec::new(),
            out_pos: 0,
            state: State::Idle,
            readable: true,
            read_closed: false,
            need_more: true,
            eof: false,
            closing: false,
            queued: false,
            resp3: false,
            shape: Shape::Plain,
            txn: TxnState::default(),
            id,
            subs: HashSet::new(),
            psubs: HashSet::new(),
            ssubs: HashSet::new(),
            overflow: false,
            block: None,
            tracking: None,
            monitor: false,
            started: None,
            stash: Vec::new(),
            request_bytes: 0,
            reply_from: 0,
            held: Vec::new(),
            replconf: None,
            link: None,
            wait: None,
            exec_tail: None,
        }
    }

    /// Follow a change of the users. False: this connection's user is
    /// gone, and so the connection goes.
    fn refresh_user(&mut self, acl: &acl::Acl) -> bool {
        let generation = acl.generation();
        if generation == self.acl_gen {
            return true;
        }
        self.acl_gen = generation;
        match acl.user(self.user.name()) {
            // Deleted and made again is another user: no way to its rights
            // without its password.
            Some(user) if user.id() == self.user.id() => {
                self.user = user;
                true
            }
            _ => false,
        }
    }

    /// Whether this connection must log in before anything else.
    fn locked_out(&self) -> bool {
        !self.authed && !self.user.open()
    }

    /// Whether the user may still watch what this connection watches:
    /// every command (MONITOR), or the names of all keys (BCAST).
    fn may_keep_watching(&self) -> bool {
        if self.user.unrestricted() {
            return true;
        }
        let bcast = self.tracking.as_ref().is_some_and(|t| t.opts.bcast);
        !bcast && (!self.monitor || self.user.check(b"monitor", None, &Cmd::Monitor).is_ok())
    }

    /// Whether the user may still listen where this connection listens.
    fn may_keep_listening(&self) -> bool {
        if self.user.unrestricted() {
            return true;
        }
        if !self.may_keep_watching() {
            return false;
        }
        let allowed =
            |name: &[u8], sub: SubCmd| self.user.check(name, None, &Cmd::Sub(sub)).is_ok();
        (self.subs.is_empty()
            || allowed(
                b"subscribe",
                SubCmd::Subscribe(self.subs.iter().cloned().collect()),
            ))
            && (self.psubs.is_empty()
                || allowed(
                    b"psubscribe",
                    SubCmd::PSubscribe(self.psubs.iter().cloned().collect()),
                ))
            && (self.ssubs.is_empty()
                || allowed(
                    b"ssubscribe",
                    SubCmd::SSubscribe(self.ssubs.iter().cloned().collect()),
                ))
    }

    /// Subscribed to anything, with any of the three SUBSCRIBEs.
    fn listening(&self) -> bool {
        !(self.subs.is_empty() && self.psubs.is_empty() && self.ssubs.is_empty())
    }

    /// The next complete command of the input, if there is one. Before
    /// login a command may only be as large as a login needs.
    #[allow(clippy::type_complexity)]
    fn read_command(
        &mut self,
        acl: &acl::Acl,
    ) -> Result<(usize, Option<Vec<Vec<u8>>>), hotstash_core::resp::ProtoError> {
        if !self.refresh_user(acl) {
            return Err(hotstash_core::resp::ProtoError("this user is gone".into()));
        }
        let limits = if self.locked_out() {
            Limits {
                max_args: self.limits.max_args.min(PRE_AUTH_ARGS),
                max_bulk: self.limits.max_bulk.min(PRE_AUTH_BYTES),
                max_inline: self.limits.max_inline.min(PRE_AUTH_BYTES),
                max_request: self.limits.max_request.min(PRE_AUTH_BYTES),
            }
        } else {
            self.limits
        };
        self.parser.set_limits(limits);
        self.parser.feed(&self.inbuf[self.in_pos..])
    }

    /// A GET or a SET with no options at the head of the input, taken
    /// without the parser and the command table, and kept for the slow log
    /// if `timed`. Only between commands of a logged-in user every command
    /// is open to, with rights as last read, and outside RESP2
    /// subscriptions: there nothing else needs the arguments.
    fn read_fast(&mut self, acl: &acl::Acl, timed: bool) -> Option<(usize, Cmd)> {
        let plain = !self.parser.mid_command()
            && acl.generation() == self.acl_gen
            && !self.locked_out()
            && self.user.unrestricted()
            && (self.resp3 || !self.listening());
        if !plain {
            return None;
        }
        let fast = resp::read_fast(&self.inbuf[self.in_pos..], &self.limits)?;
        let cmd = match fast.args() {
            [_, key] => Cmd::Get(key.to_vec()),
            [_, key, val] => Cmd::Set(key.to_vec(), val.to_vec()),
            _ => return None,
        };
        if timed {
            observe::stash(&mut self.stash, fast.args());
        }
        Some((fast.used, cmd))
    }

    /// An out-of-band frame: an array in RESP2, a push in RESP3.
    fn frame(&mut self, len: usize) {
        self.outbuf.push(if self.resp3 { b'>' } else { b'*' });
        self.outbuf.extend_from_slice(len.to_string().as_bytes());
        self.outbuf.extend_from_slice(b"\r\n");
    }

    /// Confirm one (un)subscription with the number left.
    fn ack(&mut self, kind: &str, name: Option<&[u8]>) {
        self.frame(3);
        Reply::bulk(kind).encode(&mut self.outbuf);
        match name {
            Some(name) => Reply::bulk(name).encode(&mut self.outbuf),
            None if self.resp3 => self.outbuf.extend_from_slice(b"_\r\n"),
            None => Reply::Nil.encode(&mut self.outbuf),
        }
        // The shard channels are counted apart, as in Redis.
        let left = match kind {
            "ssubscribe" | "sunsubscribe" => self.ssubs.len(),
            _ => self.subs.len() + self.psubs.len(),
        };
        Reply::Int(left as i64).encode(&mut self.outbuf);
    }

    fn message(&mut self, msg: &Message) {
        match &msg.pattern {
            Some(pattern) => {
                self.frame(4);
                Reply::bulk("pmessage").encode(&mut self.outbuf);
                Reply::bulk(pattern.as_slice()).encode(&mut self.outbuf);
            }
            None => {
                self.frame(3);
                let kind = if msg.shard { "smessage" } else { "message" };
                Reply::bulk(kind).encode(&mut self.outbuf);
            }
        }
        Reply::bulk(msg.channel.as_slice()).encode(&mut self.outbuf);
        Reply::bulk(msg.payload.as_slice()).encode(&mut self.outbuf);
    }

    /// Tell the client that a key it may have cached changed (`None`:
    /// every key). A RESP2 connection hears of it only as a subscriber of
    /// the invalidation channel.
    fn invalidation(&mut self, key: Option<&[u8]>) {
        if self.resp3 {
            self.outbuf
                .extend_from_slice(b">2\r\n$10\r\ninvalidate\r\n");
        } else if self.subs.contains(INVALIDATE_CHANNEL) {
            self.outbuf.extend_from_slice(b"*3\r\n$7\r\nmessage\r\n");
            Reply::bulk(INVALIDATE_CHANNEL).encode(&mut self.outbuf);
        } else {
            return;
        }
        match key {
            Some(key) => Reply::Array(vec![Reply::bulk(key)]).encode(&mut self.outbuf),
            None if self.resp3 => self.outbuf.extend_from_slice(b"_\r\n"),
            None => Reply::Nil.encode(&mut self.outbuf),
        }
        self.overflow |= self.out_pending() > PUBSUB_OUT_LIMIT;
    }

    /// Whether a command of this connection is on its way to a reply
    /// that is not written yet. A connection that waits in BLPOP is not:
    /// it may wait for ever.
    fn answer_pending(&self) -> bool {
        self.block.is_none() && !matches!(self.state, State::Idle)
    }

    fn reply(&mut self, reply: &Reply) {
        if self.resp3 {
            reply.encode_as(&mut self.outbuf, &self.shape);
        } else {
            reply.encode(&mut self.outbuf);
        }
    }

    fn out_pending(&self) -> usize {
        self.outbuf.len() - self.out_pos
    }

    /// Encrypted bytes that the socket has not taken yet.
    fn tls_pending(&self) -> bool {
        self.tls.as_ref().is_some_and(|tls| tls.wants_write())
    }

    fn handshaking(&self) -> bool {
        self.tls.as_ref().is_some_and(|tls| tls.is_handshaking())
    }

    /// The command in flight is answered: if it may have changed
    /// something, its reply waits for the disk.
    fn answered(&mut self) {
        if std::mem::take(&mut self.wrote) {
            self.unsynced = true;
            // A sync asked for earlier does not cover this change.
            self.sync_need = NO_SYNC;
        }
    }

    /// Whether this connection is one that `timeout` closes when it says
    /// nothing: not one that waits or listens, which is silent by design.
    fn may_idle_out(&self) -> bool {
        self.block.is_none()
            && !self.unsynced
            && !self.listening()
            && !self.monitor
            && self.link.is_none()
            && matches!(self.state, State::Idle)
    }

    /// The server ends the connection: tell a TLS peer so, if the socket
    /// takes it. Best effort, without waiting.
    fn farewell(&mut self) {
        if self.handshaking() {
            return;
        }
        self.say_goodbye();
        if let Some(tls) = &mut self.tls {
            while tls.wants_write() && matches!(tls.write_tls(&mut self.stream), Ok(n) if n > 0) {}
        }
    }

    /// End the TLS session properly, once.
    fn say_goodbye(&mut self) {
        if let Some(tls) = self.tls.as_mut().filter(|_| !self.goodbye) {
            tls.send_close_notify();
            self.goodbye = true;
        }
    }

    /// Read what the socket has, within the per-turn budget. `false` means
    /// the connection is broken.
    fn fill(&mut self, scratch: &mut [u8]) -> bool {
        self.fill_to(scratch, READ_CHUNK)
    }

    /// `fill`, reading on until `backlog` bytes wait to be executed.
    fn fill_to(&mut self, scratch: &mut [u8], backlog: usize) -> bool {
        if self.eof || !self.readable {
            return true;
        }
        // Do not buffer far ahead of what gets executed.
        if !self.need_more && self.inbuf.len() - self.in_pos >= backlog {
            return true;
        }
        if self.in_pos == self.inbuf.len() {
            reset(&mut self.inbuf);
            self.in_pos = 0;
        } else if self.in_pos >= READ_CHUNK {
            self.inbuf.drain(..self.in_pos);
            self.in_pos = 0;
        }
        let mut got = 0;
        let mut data = false;
        // On until WouldBlock, even after a short read: a FIN that came in
        // with the last data is only seen by the next read, and edge-
        // triggered epoll does not report it again. On Linux the event
        // says so (EPOLLRDHUP), and anything that arrives after a read
        // brings a new edge: there a short read is the end of what there
        // is, and the read that would only say WouldBlock is saved. Not
        // through TLS, whose reads do not map onto the socket's.
        while got < READ_BUDGET {
            let read = match &mut self.tls {
                None => self.stream.read(scratch),
                Some(tls) => tls_read(tls, &mut self.stream, scratch),
            };
            match read {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(n) => {
                    self.inbuf.extend_from_slice(&scratch[..n]);
                    got += n;
                    data = true;
                    if cfg!(target_os = "linux")
                        && n < scratch.len()
                        && self.tls.is_none()
                        && !self.read_closed
                    {
                        self.readable = false;
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    self.readable = false;
                    break;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => got += EMPTY_READ_COST,
                Err(_) => return false,
            }
        }
        if data {
            self.last_input = Instant::now();
        }
        true
    }

    /// Write as much as the socket takes. `false` means the connection is
    /// broken.
    fn flush(&mut self) -> bool {
        let mut moved = false;
        match &mut self.tls {
            None => {
                while self.out_pos < self.outbuf.len() {
                    match self.stream.write(&self.outbuf[self.out_pos..]) {
                        Ok(0) => return false,
                        Ok(n) => {
                            self.out_pos += n;
                            moved = true;
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) if e.kind() == ErrorKind::Interrupted => {}
                        Err(_) => return false,
                    }
                }
            }
            Some(tls) => 'full: loop {
                // What is encrypted goes out first; then TLS takes more.
                while tls.wants_write() {
                    match tls.write_tls(&mut self.stream) {
                        Ok(0) => return false,
                        Ok(_) => moved = true,
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break 'full,
                        Err(e) if e.kind() == ErrorKind::Interrupted => {}
                        Err(_) => return false,
                    }
                }
                if self.out_pos == self.outbuf.len() {
                    break;
                }
                match tls.writer().write(&self.outbuf[self.out_pos..]) {
                    // It takes nothing and has nothing to send: not a
                    // state to spin in.
                    Ok(0) => break,
                    Ok(n) => self.out_pos += n,
                    Err(_) => return false,
                }
            },
        }
        if self.out_pending() == 0 && !self.tls_pending() {
            self.stalled = None;
        } else if moved || self.stalled.is_none() {
            self.stalled = Some(Instant::now());
        }
        if self.out_pos == self.outbuf.len() {
            reset(&mut self.outbuf);
            self.out_pos = 0;
        } else if self.out_pos >= OUT_SOFT_LIMIT && self.out_pos >= self.out_pending() {
            // Only once the sent part is at least half: a slow reader of a
            // huge reply would otherwise make every megabyte move the rest.
            self.outbuf.drain(..self.out_pos);
            self.out_pos = 0;
        }
        true
    }
}

/// Empty a connection buffer, giving back the memory of an unusually large
/// request or reply instead of keeping it for the life of the connection.
fn reset(buf: &mut Vec<u8>) {
    if buf.capacity() > BUF_KEEP {
        *buf = Vec::new();
    } else {
        buf.clear();
    }
}

/// HTTP sent to the port by a browser must not be read as commands.
fn is_cross_protocol(args: &[Vec<u8>]) -> bool {
    args.first().is_some_and(|name| {
        name.eq_ignore_ascii_case(b"POST") || name.eq_ignore_ascii_case(b"HOST:")
    })
}

struct Worker {
    id: usize,
    shared: Arc<Shared>,
    poll: Poll,
    /// This worker's slot in the execution gate.
    slot: usize,
    in_gate: bool,
    /// The gate was found closed during this loop iteration.
    gate_blocked: bool,
    /// Counted in the gate's starved participants.
    starved: bool,
    conns: Vec<Option<Conn>>,
    /// Per connection slot; a ticket is valid only for the generation it
    /// was issued in.
    gens: Vec<u32>,
    free: Vec<usize>,
    runnable: Vec<usize>,
    gate_wait: Vec<usize>,
    /// Connections whose command waits for a FAILOVER to end.
    paused: Vec<usize>,
    /// A WAIT asked the replicas where they are: they are asked once per
    /// turn of the loop, however many did.
    getack: bool,
    /// When each blocked connection with a timeout gives up.
    deadlines: BTreeSet<(Instant, usize)>,
    scratch: Box<[u8]>,
    pipeline_cap: usize,
    limits: Limits,
    /// `Acl::generation` when the connections were last looked over.
    acl_seen: u64,
    /// How TLS connections are set up, when there is a TLS listener.
    tls: Option<Arc<rustls::ServerConfig>>,
    stall_timeout: Duration,
    next_sweep: Instant,
    /// Connections whose replies wait for a sync that is yet to be asked
    /// for.
    unsynced: Vec<usize>,
    /// The number of the last sync asked for.
    sync_sent: u64,
    /// Connections that wait for a sync that was asked for: slot and id.
    held: Vec<(usize, u64)>,
}

impl Worker {
    fn handle(&self) -> &WorkerHandle {
        &self.shared.workers[self.id]
    }

    fn run(mut self) {
        CURRENT.set((self.shared.id(), self.id));
        let shared = self.shared.clone();
        let engine = &shared.engine;
        let mut events = Events::with_capacity(1024);
        loop {
            let busy = !self.runnable.is_empty()
                || !self.handle().inbox.is_empty()
                || (!self.gate_blocked && engine.has_service());
            let timeout = if busy {
                Some(Duration::ZERO)
            } else {
                let now = Instant::now();
                let next = self.deadlines.first().map(|(at, _)| *at);
                // With connections open there is a sweep to wake up for.
                let sweep = (self.conns.len() > self.free.len()).then_some(self.next_sweep);
                // And when a CLIENT PAUSE that holds commands here ends.
                let pause = match self.paused.is_empty() {
                    true => None,
                    false => shared.client_pause().map(|(until, _)| until),
                };
                let wake = [next, sweep, pause].into_iter().flatten().min();
                wake.map(|at| at.saturating_duration_since(now))
            };
            if let Err(e) = self.poll.poll(&mut events, timeout) {
                if e.kind() != ErrorKind::Interrupted {
                    eprintln!("hotstash worker {}: poll failed: {e}", self.id);
                    break;
                }
            }
            self.handle().wake_pending.store(false, SeqCst);
            if shared.stop.load(SeqCst) {
                break;
            }
            self.gate_blocked = false;

            for event in events.iter() {
                if event.token() == WAKER {
                    continue;
                }
                let idx = event.token().0 - 1;
                if let Some(Some(conn)) = self.conns.get_mut(idx) {
                    if event.is_readable() || event.is_read_closed() || event.is_error() {
                        conn.readable = true;
                    }
                    conn.read_closed |= event.is_read_closed() || event.is_error();
                    self.schedule(idx);
                }
            }
            while let Some(msg) = self.handle().inbox.pop() {
                match msg {
                    Msg::NewConn(stream, secure, id) => self.add_conn(stream, secure, id),
                    Msg::Kill(sub) => self.kill_conn(sub),
                    Msg::Synced(number, ok) => self.synced(number, ok),
                    Msg::Complete(ticket, reply) => self.complete(ticket, reply),
                    Msg::Push(sub, msg) => self.push(sub, &msg),
                    Msg::Retry(sub) => self.retry(sub),
                    Msg::Invalidate(listener, key) => self.invalidate(listener, key.as_deref()),
                    Msg::Line(sub, line) => self.line(sub, &line),
                    Msg::Start(ticket, start) => self.start(ticket, *start),
                    Msg::Feed(sub) => self.feed_link(sub),
                    Msg::Acked(sub) => self.recount(sub),
                }
            }
            self.expire_blocked();
            for idx in std::mem::take(&mut self.gate_wait) {
                self.schedule(idx);
            }
            if !self.paused.is_empty() && !shared.writes_paused() {
                for idx in std::mem::take(&mut self.paused) {
                    if let Some(Some(conn)) = self.conns.get_mut(idx) {
                        conn.paused = false;
                    }
                    self.schedule(idx);
                }
            }
            if engine.has_service() && self.enter() {
                engine.service();
                self.leave();
            }
            for idx in std::mem::take(&mut self.runnable) {
                self.turn(idx);
            }
            self.commit();
            self.review_users();
            self.sweep();
            // Not while a FAILOVER waits for the stream to stand still, as
            // in Valkey: the replicas say where they are every second.
            if self.getack && !shared.writes_paused() {
                self.getack = false;
                engine.repl_getack();
            }
            if self.starved && !self.gate_blocked {
                engine.gate().clear_starved();
                self.starved = false;
            }
        }
        self.leave();
        if self.starved {
            engine.gate().clear_starved();
        }
        engine.gate().unregister(self.slot);
    }

    /// After a change of the users: close the connections whose user is
    /// gone, and those that listen where their user no longer may. A
    /// connection that sends nothing would otherwise never learn of it.
    fn review_users(&mut self) {
        let shared = self.shared.clone();
        let acl = shared.engine.acl();
        let generation = acl.generation();
        if generation == self.acl_seen {
            return;
        }
        self.acl_seen = generation;
        for idx in 0..self.conns.len() {
            let Some(mut conn) = self.conns[idx].take() else {
                continue;
            };
            if conn.refresh_user(acl) && conn.may_keep_listening() {
                self.conns[idx] = Some(conn);
            } else {
                self.close(idx, conn);
            }
        }
    }

    /// `appendfsync always`: ask for the data file to be brought up to
    /// date. One sync serves every connection of this turn, and the other
    /// workers that ask at the same time. The worker does not wait: it
    /// hears of the result in `synced`, and serves the rest meanwhile,
    /// which is what lets SCRIPT KILL through while a script holds up the
    /// writer of the file.
    fn commit(&mut self) {
        if self.unsynced.is_empty() {
            return;
        }
        self.sync_sent += 1;
        let number = self.sync_sent;
        for idx in std::mem::take(&mut self.unsynced) {
            let Some(Some(conn)) = self.conns.get_mut(idx) else {
                continue;
            };
            if !conn.unsynced {
                continue;
            }
            conn.sync_need = number;
            if !std::mem::replace(&mut conn.sync_held, true) {
                self.held.push((idx, conn.id));
            }
        }
        let (shared, me) = (self.shared.clone(), self.id);
        self.shared.engine.sync_then(Box::new(move |result| {
            shared.workers[me]
                .inbox
                .push(Msg::Synced(number, result.is_ok()));
            shared.wake(me);
        }));
    }

    /// The sync numbered `number` is done: the replies it covers may go.
    fn synced(&mut self, number: u64, ok: bool) {
        for (idx, id) in std::mem::take(&mut self.held) {
            let conn = match self.conns.get_mut(idx) {
                // The slot may belong to a later connection by now.
                Some(Some(conn)) if conn.id == id => conn,
                _ => continue,
            };
            if conn.sync_need > number {
                // It changed more since; a later sync covers that.
                self.held.push((idx, id));
                continue;
            }
            conn.sync_held = false;
            conn.unsynced = false;
            // A change that is not on disk must not be answered as if it
            // were: the connection ends without the reply.
            conn.overflow |= !ok;
            self.schedule(idx);
        }
    }

    /// Close the connections whose time is up: silent for longer than
    /// `timeout`, not taking their output, or not done with the handshake.
    fn sweep(&mut self) {
        let now = Instant::now();
        if now < self.next_sweep {
            return;
        }
        self.next_sweep = now + SWEEP_EVERY;
        let idle = Duration::from_secs(self.shared.engine.client_timeout());
        let stall = self.stall_timeout;
        let late = |since: Instant, limit: Duration| {
            !limit.is_zero() && now.saturating_duration_since(since) >= limit
        };
        let mut links = false;
        for idx in 0..self.conns.len() {
            let Some(conn) = &self.conns[idx] else {
                continue;
            };
            // A replica says how far it got every second; one that has
            // not for this long is gone. Unless its output piles up: then
            // its input is not being read.
            let silent = conn.link.as_ref().is_some_and(|link| {
                link.psync
                    && link.at.is_some()
                    && link.rdb.is_none()
                    && conn.out_pending() < OUT_SOFT_LIMIT
            }) && late(conn.last_input, REPL_TIMEOUT);
            links |= conn.link.is_some();
            let over = conn.stalled.is_some_and(|since| late(since, stall))
                || (conn.handshaking() && late(conn.since, HANDSHAKE_TIMEOUT))
                || (conn.may_idle_out() && late(conn.last_input, idle))
                || silent;
            if over {
                if let Some(conn) = self.conns[idx].take() {
                    self.close(idx, conn);
                }
            }
        }
        if links {
            self.shared.ping_replicas(now);
        }
        self.shared.failover_tick();
    }

    /// CLIENT KILL chose a connection of this worker.
    fn kill_conn(&mut self, sub: Sub) {
        let Some(slot) = self.conns.get_mut(sub.idx) else {
            return;
        };
        // The slot may belong to a later connection by now.
        if slot.as_ref().is_some_and(|conn| conn.id == sub.conn) {
            if let Some(conn) = slot.take() {
                self.close(sub.idx, conn);
            }
        }
    }

    /// CLIENT KILL: close every connection the filter matches.
    fn kill(&self, conn: &mut Conn, filter: &KillFilter) -> Reply {
        let shared = &self.shared;
        let mut victims = Vec::new();
        let mut killed = 0;
        if filter.master {
            if shared.follower().as_ref().is_some_and(Follower::reset) {
                killed += 1;
            }
        } else {
            let is = |want: &Option<Vec<u8>>, have: &[u8]| {
                want.as_ref().is_none_or(|want| want.as_slice() == have)
            };
            for (id, client) in shared.clients().iter() {
                let hit = filter.id.is_none_or(|want| want == *id)
                    && is(&filter.addr, client.addr.as_bytes())
                    && is(&filter.laddr, client.laddr.as_bytes())
                    && is(&filter.user, &client.user)
                    && match (filter.replica, filter.pubsub) {
                        (true, _) => client.replica,
                        (false, Some(true)) => client.listening(),
                        (false, Some(false)) => !client.listening() && !client.replica,
                        (false, None) => true,
                    }
                    && filter
                        .maxage
                        .is_none_or(|age| client.since.elapsed().as_secs() >= age);
                if !hit || (*id == conn.id && filter.skipme) {
                    continue;
                }
                killed += 1;
                if *id == conn.id {
                    // After the reply.
                    conn.closing = true;
                } else {
                    victims.push(Sub {
                        worker: client.at.0,
                        idx: client.at.1,
                        conn: *id,
                    });
                }
            }
        }
        for sub in victims {
            if let Some(handle) = shared.workers.get(sub.worker) {
                handle.inbox.push(Msg::Kill(sub));
                shared.wake(sub.worker);
            }
        }
        match (filter.legacy, killed) {
            (true, 0) => Reply::err("ERR No such client"),
            (true, _) => Reply::Ok,
            (false, n) => Reply::Int(n),
        }
    }

    /// Enter the shared side of the gate, or arrange to be woken when it
    /// reopens.
    fn enter(&mut self) -> bool {
        if self.in_gate {
            return true;
        }
        let gate = self.shared.engine.gate();
        if !gate.try_enter(self.slot) {
            // Announce the wait, then look again: the executor reopens the
            // gate first and reads this flag second.
            self.handle().gate_waiting.store(true, SeqCst);
            if gate.is_closed() || !gate.try_enter(self.slot) {
                if !self.starved {
                    gate.mark_starved();
                    self.starved = true;
                }
                self.gate_blocked = true;
                return false;
            }
            self.handle().gate_waiting.store(false, SeqCst);
        }
        self.in_gate = true;
        true
    }

    fn leave(&mut self) {
        if self.in_gate {
            self.shared.engine.gate().leave(self.slot);
            self.in_gate = false;
        }
    }

    fn schedule(&mut self, idx: usize) {
        if let Some(Some(conn)) = self.conns.get_mut(idx) {
            if !conn.queued {
                conn.queued = true;
                self.runnable.push(idx);
            }
        }
    }

    fn add_conn(&mut self, mut stream: TcpStream, secure: bool, id: u64) {
        // A TLS connection without a way to set one up is not served.
        let tls = match (secure, &self.tls) {
            (false, _) => Ok(None),
            (true, Some(config)) => ServerConnection::new(config.clone())
                .map(|session| Some(Box::new(session)))
                .map_err(drop),
            (true, None) => Err(()),
        };
        let Ok(tls) = tls else {
            self.shared.conns.fetch_sub(1, SeqCst);
            return;
        };
        let idx = self.free.pop().unwrap_or(self.conns.len());
        let registered = idx < MAX_CONNS_PER_WORKER
            && self
                .poll
                .registry()
                .register(
                    &mut stream,
                    Token(idx + 1),
                    Interest::READABLE | Interest::WRITABLE,
                )
                .is_ok();
        if !registered {
            if idx < self.conns.len() {
                self.free.push(idx);
            }
            self.shared.conns.fetch_sub(1, SeqCst);
            return;
        }
        if idx == self.conns.len() {
            self.conns.push(None);
            self.gens.push(0);
        }
        let shown = |addr: io::Result<SocketAddr>| addr.map(|a| a.to_string()).unwrap_or_default();
        self.shared.clients().insert(
            id,
            ClientEntry {
                addr: shown(stream.peer_addr()),
                laddr: shown(stream.local_addr()),
                name: Vec::new(),
                lib_name: Vec::new(),
                lib_ver: Vec::new(),
                since: Instant::now(),
                resp3: false,
                db: 0,
                user: b"default".to_vec(),
                subs: [0; 3],
                at: (self.id, idx),
                replica: false,
            },
        );
        let acl = self.shared.engine.acl();
        self.conns[idx] = Some(Conn::new(stream, tls, self.limits, id, acl));
        // Bytes may have arrived before the socket was registered.
        self.schedule(idx);
    }

    /// Drop every subscription of `conn` without confirming any.
    fn unsubscribe_all(&self, idx: usize, conn: &mut Conn) {
        let me = Sub {
            worker: self.id,
            idx,
            conn: conn.id,
        };
        for channel in conn.subs.drain() {
            self.shared.pubsub.unsubscribe(&channel, me);
        }
        for pattern in conn.psubs.drain() {
            self.shared.pubsub.punsubscribe(&pattern, me);
        }
        for channel in conn.ssubs.drain() {
            self.shared.pubsub.sunsubscribe(&channel, me);
        }
    }

    fn close(&mut self, idx: usize, mut conn: Conn) {
        conn.farewell();
        self.unblock(idx, &mut conn);
        self.untrack(&mut conn);
        self.unmonitor(idx, &mut conn);
        self.unlink(&mut conn);
        self.unwait(idx, &mut conn);
        self.shared.clients().remove(&conn.id);
        // Deregistering can only fail for a socket the poller no longer
        // knows, and the socket is dropped right here either way.
        let _ = self.poll.registry().deregister(&mut conn.stream);
        let me = Sub {
            worker: self.id,
            idx,
            conn: conn.id,
        };
        for channel in &conn.subs {
            self.shared.pubsub.unsubscribe(channel, me);
        }
        for pattern in &conn.psubs {
            self.shared.pubsub.punsubscribe(pattern, me);
        }
        for channel in &conn.ssubs {
            self.shared.pubsub.sunsubscribe(channel, me);
        }
        // Whatever this connection still has in flight must not be
        // delivered to the next connection that gets the slot.
        self.gens[idx] = self.gens[idx].wrapping_add(1);
        self.free.push(idx);
        self.shared.conns.fetch_sub(1, SeqCst);
    }

    fn complete(&mut self, ticket: Ticket, reply: Reply) {
        let (_, _, idx, gen) = unpack(ticket);
        if self.gens.get(idx) != Some(&gen) {
            return;
        }
        let Some(mut conn) = self.conns.get_mut(idx).and_then(Option::take) else {
            return;
        };
        // A connection waiting in line is parked too, with nothing running.
        let expected = matches!(conn.state, State::Parked)
            && conn.block.as_ref().is_none_or(|block| block.in_flight);
        if expected {
            conn.state = State::Idle;
            match conn.exec_tail.take() {
                Some(tail) => self.finish_exec(idx, &mut conn, *tail, reply),
                None => self.deliver(idx, &mut conn, reply),
            }
            if conn.exec_tail.is_none() {
                self.shared.slow(&mut conn);
            }
        }
        self.conns[idx] = Some(conn);
        if expected {
            self.schedule(idx);
        }
    }

    /// Carry out what an EXEC left to the server, in the order it was
    /// queued, then answer with every reply. A role change runs on a thread
    /// of its own: the rest goes on once it is done.
    fn finish_exec(&mut self, idx: usize, conn: &mut Conn, mut tail: ExecTail, reply: Reply) {
        match tail.waiting.take() {
            Some(at) => {
                if let Some(slot) = tail.replies.get_mut(at) {
                    *slot = reply;
                }
            }
            None => match reply {
                Reply::Array(replies) => tail.replies = replies,
                // A watched key changed, or nothing ran: nothing else does.
                other => return self.deliver(idx, conn, other),
            },
        }
        while let Some((at, cmd)) = tail.rest.pop_front() {
            let reply = match cmd {
                Cmd::ReplicaOf(target) => match self.replicaof(idx, conn, target) {
                    Some(reply) => reply,
                    None => {
                        tail.waiting = Some(at);
                        conn.exec_tail = Some(Box::new(tail));
                        return;
                    }
                },
                Cmd::Client(sub) => self.tracking_cmd(idx, conn, &sub),
                Cmd::Failover(args) => self.failover(&args),
                _ => Reply::err("ERR internal: not a command of the server"),
            };
            if let Some(slot) = tail.replies.get_mut(at) {
                *slot = reply;
            }
        }
        self.deliver(idx, conn, Reply::Array(tail.replies));
    }

    /// Hand a command's reply to its connection. A blocking command that
    /// found nothing waits instead: the connection stays parked until one
    /// of its keys changes or its time is up.
    fn deliver(&mut self, idx: usize, conn: &mut Conn, reply: Reply) {
        let Some(block) = &mut conn.block else {
            conn.reply(&reply);
            conn.answered();
            return;
        };
        block.in_flight = false;
        let woken = std::mem::take(&mut block.woken);
        let stream = matches!(*block.op, MultiOp::XRead { .. });
        let group = stream && block.op.is_write();
        // A key that turns into another type while the connection waits is
        // no more use to it than a missing one, as in Redis. A reader of a
        // consumer group is told: its group is gone.
        let wrong_type = block.listed
            && !group
            && matches!(&reply, Reply::Error(e) if e.starts_with("WRONGTYPE"));
        let empty = wrong_type || matches!(reply, Reply::Nil | Reply::NilArray);
        let timed_out = block.deadline.is_some_and(|at| at <= Instant::now());
        if !empty || timed_out {
            self.unblock(idx, conn);
            conn.reply(if empty { &Reply::NilArray } else { &reply });
            conn.answered();
            return;
        }
        if !block.listed {
            block.listed = true;
            // Look once more after getting in line: data that arrived in
            // between told nobody.
            block.again = true;
            if let Some(at) = block.deadline {
                self.deadlines.insert((at, idx));
            }
            let me = Sub {
                worker: self.id,
                idx,
                conn: conn.id,
            };
            self.shared.block(&block.keys, me);
        }
        // Several kinds of waiter can stand in one line. What was no use
        // to this one may be what the one behind it waits for: an entry
        // for another reader of the stream, or a value of the other type.
        if woken && (stream || wrong_type) {
            let me = Sub {
                worker: self.id,
                idx,
                conn: conn.id,
            };
            self.shared.pass(&block.keys, me);
        }
        if std::mem::take(&mut block.again) {
            block.in_flight = true;
            block.woken = true;
            conn.state = State::GateWait(Cmd::Multi(block.op.clone()));
        } else {
            conn.state = State::Parked;
        }
    }

    /// The connection no longer waits, whatever the reason.
    fn unblock(&mut self, idx: usize, conn: &mut Conn) {
        let Some(block) = conn.block.take() else {
            return;
        };
        if let Some(at) = block.deadline {
            self.deadlines.remove(&(at, idx));
        }
        if block.listed {
            let me = Sub {
                worker: self.id,
                idx,
                conn: conn.id,
            };
            self.shared.unblock(&block.keys, me);
        }
    }

    /// A key a blocked connection waits for has changed: try again.
    fn retry(&mut self, sub: Sub) {
        let Some(Some(conn)) = self.conns.get_mut(sub.idx) else {
            return;
        };
        let Some(block) = conn.block.as_mut().filter(|_| conn.id == sub.conn) else {
            return;
        };
        if block.in_flight {
            block.again = true;
            return;
        }
        block.in_flight = true;
        block.woken = true;
        conn.state = State::GateWait(Cmd::Multi(block.op.clone()));
        self.schedule(sub.idx);
    }

    /// Answer the blocked connections whose time is up.
    fn expire_blocked(&mut self) {
        let now = Instant::now();
        while let Some(&(at, idx)) = self.deadlines.first().filter(|(at, _)| *at <= now) {
            self.deadlines.remove(&(at, idx));
            let Some(mut conn) = self.conns.get_mut(idx).and_then(Option::take) else {
                continue;
            };
            // With an attempt running, its reply settles it.
            let waiting = conn
                .block
                .as_ref()
                .is_some_and(|block| block.deadline == Some(at) && !block.in_flight);
            if waiting {
                self.unblock(idx, &mut conn);
                conn.reply(&Reply::NilArray);
                conn.answered();
                conn.state = State::Idle;
            }
            let replicas = conn
                .wait
                .as_ref()
                .filter(|wait| wait.deadline == Some(at))
                .map(|wait| self.shared.acked_at(wait.offset));
            if let Some(have) = replicas {
                self.unwait(idx, &mut conn);
                conn.reply(&Reply::Int(have));
                conn.answered();
                conn.state = State::Idle;
            }
            self.conns[idx] = Some(conn);
            if waiting || replicas.is_some() {
                self.schedule(idx);
            }
        }
    }

    /// Tell a connection on this worker that a key it may have cached
    /// changed (`None`: every key).
    fn invalidate(&mut self, listener: Listener, key: Option<&[u8]>) {
        let Some(Some(conn)) = self.conns.get_mut(listener.to.idx) else {
            return;
        };
        if conn.id != listener.to.conn || conn.closing {
            return;
        }
        if conn.answer_pending() {
            if conn.held.len() >= HELD_MAX {
                conn.held.clear();
                conn.held.push(None);
            }
            conn.held.push(key.map(<[u8]>::to_vec));
            return;
        }
        conn.invalidation(key);
        self.schedule(listener.to.idx);
    }

    /// Hand a line of MONITOR output to a watcher on this worker.
    fn line(&mut self, sub: Sub, line: &[u8]) {
        let Some(Some(conn)) = self.conns.get_mut(sub.idx) else {
            return;
        };
        if conn.id != sub.conn || conn.closing || !conn.monitor {
            return;
        }
        conn.outbuf.extend_from_slice(line);
        conn.overflow = conn.out_pending() > PUBSUB_OUT_LIMIT;
        self.schedule(sub.idx);
    }

    /// `conn` logged in as someone else: what the new user may not
    /// watch ends here.
    fn stop_watching(&self, idx: usize, conn: &mut Conn) {
        if !conn.may_keep_watching() {
            self.untrack(conn);
            self.unmonitor(idx, conn);
        }
    }

    /// Turn tracking off for `conn`, if it is on.
    fn untrack(&self, conn: &mut Conn) {
        if conn.tracking.take().is_some() {
            self.shared.tracker.stop(conn.id);
            self.shared.engine.key_waiters().fetch_sub(1, SeqCst);
        }
    }

    /// Take `conn` out of MONITOR, if it is in it.
    fn unmonitor(&self, idx: usize, conn: &mut Conn) {
        if std::mem::take(&mut conn.monitor) {
            let me = Sub {
                worker: self.id,
                idx,
                conn: conn.id,
            };
            self.shared.monitors().retain(|sub| *sub != me);
            self.shared.monitoring.fetch_sub(1, SeqCst);
        }
    }

    /// CLIENT TRACKING, CACHING, TRACKINGINFO and GETREDIR.
    fn tracking_cmd(&self, idx: usize, conn: &mut Conn, cmd: &ClientCmd) -> Reply {
        let shared = &self.shared;
        match cmd {
            ClientCmd::Tracking(None) => {
                self.untrack(conn);
                Reply::Ok
            }
            ClientCmd::Tracking(Some(want)) => {
                let mut opts = want.clone();
                // Every change under a prefix tells the names of keys: only
                // for a user who may see them all.
                if opts.bcast && !conn.user.unrestricted() {
                    return Reply::err(
                        "NOPERM this user has no permissions to use BCAST: it tells of keys the \
                         user may not be allowed to read",
                    );
                }
                if let Some(now) = &conn.tracking {
                    if now.opts.bcast != opts.bcast {
                        return Reply::err(
                            "ERR You can't switch BCAST mode on/off before disabling tracking \
                             for this client, and then re-enabling it with a different mode.",
                        );
                    }
                    let shown = |raw: &[u8]| {
                        String::from_utf8_lossy(&raw[..raw.len().min(64)]).into_owned()
                    };
                    for old in &now.opts.prefixes {
                        let clash = opts
                            .prefixes
                            .iter()
                            .find(|new| new.starts_with(old) || old.starts_with(new));
                        if let Some(new) = clash {
                            return Reply::Error(format!(
                                "ERR Prefix '{}' overlaps with an existing prefix '{}'. Prefixes \
                                 for a single client must not overlap.",
                                shown(new),
                                shown(old)
                            ));
                        }
                    }
                    // Prefixes add up from call to call.
                    opts.prefixes
                        .splice(0..0, now.opts.prefixes.iter().cloned());
                    if opts.prefixes.len() > hotstash_core::cmd::MAX_PREFIXES {
                        return hotstash_core::cmd::too_many_prefixes();
                    }
                }
                let to = match opts.redirect {
                    None => Sub {
                        worker: self.id,
                        idx,
                        conn: conn.id,
                    },
                    Some(id) if id == conn.id => {
                        return Reply::err("ERR A client can't redirect to itself")
                    }
                    Some(id) => match shared.clients().get(&id) {
                        Some(target) => Sub {
                            worker: target.at.0,
                            idx: target.at.1,
                            conn: id,
                        },
                        None => {
                            return Reply::err(
                                "ERR The client ID you want redirect to does not exist",
                            )
                        }
                    },
                };
                let listener = Listener {
                    owner: conn.id,
                    home: (self.id, idx),
                    noloop: opts.noloop,
                    to,
                };
                // BCAST without a prefix: every key.
                let every = [Vec::new()];
                let prefixes = opts.bcast.then(|| {
                    if opts.prefixes.is_empty() {
                        &every[..]
                    } else {
                        &opts.prefixes[..]
                    }
                });
                // First, so that no change goes unreported in between.
                if conn.tracking.is_none() {
                    shared.engine.key_waiters().fetch_add(1, SeqCst);
                }
                shared.tracker.start(listener, prefixes);
                conn.tracking = Some(Track {
                    opts,
                    listener,
                    caching: None,
                });
                Reply::Ok
            }
            ClientCmd::Caching(yes) => match &mut conn.tracking {
                Some(track) if *yes && track.opts.optin => {
                    track.caching = Some(true);
                    Reply::Ok
                }
                Some(track) if !*yes && track.opts.optout => {
                    track.caching = Some(false);
                    Reply::Ok
                }
                Some(track) if track.opts.optin => Reply::err(
                    "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
                ),
                Some(track) if track.opts.optout => Reply::err(
                    "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
                ),
                _ => Reply::err(
                    "ERR CLIENT CACHING can be called only when the client is in tracking mode \
                     with OPTIN or OPTOUT mode enabled",
                ),
            },
            ClientCmd::TrackingInfo => {
                let mut flags = Vec::new();
                let mut prefixes = Vec::new();
                let mut redirect = -1;
                match &conn.tracking {
                    None => flags.push("off"),
                    Some(track) => {
                        flags.push("on");
                        let o = &track.opts;
                        let named = [
                            (o.bcast, "bcast"),
                            (o.optin, "optin"),
                            (track.caching == Some(true), "caching-yes"),
                            (o.optout, "optout"),
                            (track.caching == Some(false), "caching-no"),
                            (o.noloop, "noloop"),
                        ];
                        flags.extend(named.iter().filter(|(set, _)| *set).map(|(_, name)| *name));
                        prefixes = o
                            .prefixes
                            .iter()
                            .map(|p| Reply::bulk(p.as_slice()))
                            .collect();
                        redirect = o.redirect.map_or(0, |id| id as i64);
                    }
                }
                Reply::Map(vec![
                    Reply::bulk("flags"),
                    Reply::Array(flags.into_iter().map(Reply::bulk).collect()),
                    Reply::bulk("redirect"),
                    Reply::Int(redirect),
                    Reply::bulk("prefixes"),
                    Reply::Array(prefixes),
                ])
            }
            ClientCmd::GetRedir => Reply::Int(match &conn.tracking {
                None => -1,
                Some(track) => track.opts.redirect.map_or(0, |id| id as i64),
            }),
            ClientCmd::Kill(filter) => self.kill(conn, filter),
            ClientCmd::Pause { ms, all } => {
                shared.pause_clients(*ms, *all);
                Reply::Ok
            }
            ClientCmd::Unpause => {
                shared.unpause_clients();
                Reply::Ok
            }
            _ => shared.client_cmd(conn.id, cmd),
        }
    }

    /// `conn` is about to run `cmd`: if it tracks what it reads, remember
    /// the keys before the read, so no change after it goes unreported.
    fn track_read(&self, conn: &mut Conn, cmd: &Cmd) {
        let Some(track) = &mut conn.tracking else {
            return;
        };
        // CLIENT CACHING speaks of the command right after it.
        let said = track.caching;
        if !matches!(cmd, Cmd::Client(sub) if matches!(**sub, ClientCmd::Caching(_))) {
            track.caching = None;
        }
        let wanted = if track.opts.optin {
            said == Some(true)
        } else {
            said != Some(false)
        };
        if track.opts.bcast || !wanted || cmd.class() == Class::Conn || cmd.is_write() {
            return;
        }
        let (shared, listener) = (&self.shared, track.listener);
        read_keys(cmd, &mut |key| {
            for (forgotten, readers) in shared.tracker.remember(key, listener) {
                for reader in readers {
                    shared.invalidate(reader, Some(&forgotten));
                }
            }
        });
    }

    /// Deliver a published message to a subscriber on this worker.
    fn push(&mut self, sub: Sub, msg: &Message) {
        let Some(Some(conn)) = self.conns.get_mut(sub.idx) else {
            return;
        };
        // The slot may belong to a later connection by now.
        if conn.id != sub.conn || conn.closing {
            return;
        }
        conn.message(msg);
        conn.overflow = conn.out_pending() > PUBSUB_OUT_LIMIT;
        self.schedule(sub.idx);
    }

    /// (P|S)SUBSCRIBE and (P|S)UNSUBSCRIBE: one confirmation per name.
    /// SAVE (`wait`) or BGSAVE when there is an RDB file. SAVE answers
    /// once the file and the data file are on disk, BGSAVE at once.
    fn save_rdb(&mut self, idx: usize, conn: &mut Conn, wait: bool) {
        let shared = self.shared.clone();
        if !shared.engine.rdb_save_begin() {
            conn.reply(&Reply::err("ERR Background save already in progress"));
            conn.answered();
            return;
        }
        let ticket = wait.then(|| self.ticket(idx));
        let saver = shared.clone();
        let job = move || match ticket {
            Some(ticket) => {
                let reply = saver.engine.rdb_save_reply();
                saver.complete(ticket, reply);
            }
            // How it went is kept for INFO, as Redis keeps it.
            None => {
                let _ = saver.engine.rdb_save_finish();
            }
        };
        let spawned = std::thread::Builder::new()
            .name("hotstash-rdb".into())
            .spawn(job);
        if let Err(e) = spawned {
            shared.engine.rdb_save_cancel();
            conn.reply(&Reply::Error(format!(
                "ERR cannot start writing the RDB file: {e}"
            )));
            conn.answered();
            return;
        }
        if wait {
            conn.state = State::Parked;
            return;
        }
        shared.engine.rewrite_data_file();
        conn.reply(&Reply::Simple("Background saving started".into()));
        conn.answered();
    }

    fn sub_cmd(&mut self, idx: usize, conn: &mut Conn, cmd: SubCmd) {
        let registry = &self.shared.pubsub;
        let me = Sub {
            worker: self.id,
            idx,
            conn: conn.id,
        };
        match cmd {
            SubCmd::Subscribe(channels) => {
                for channel in channels {
                    if conn.subs.insert(channel.clone()) {
                        registry.subscribe(&channel, me);
                    }
                    conn.ack("subscribe", Some(&channel));
                }
            }
            SubCmd::PSubscribe(patterns) => {
                for pattern in patterns {
                    if conn.psubs.insert(pattern.clone()) {
                        registry.psubscribe(&pattern, me);
                    }
                    conn.ack("psubscribe", Some(&pattern));
                }
            }
            SubCmd::Unsubscribe(channels) => {
                let channels = if channels.is_empty() {
                    conn.subs.iter().cloned().collect()
                } else {
                    channels
                };
                if channels.is_empty() {
                    conn.ack("unsubscribe", None);
                }
                for channel in channels {
                    if conn.subs.remove(&channel) {
                        registry.unsubscribe(&channel, me);
                    }
                    conn.ack("unsubscribe", Some(&channel));
                }
            }
            SubCmd::PUnsubscribe(patterns) => {
                let patterns = if patterns.is_empty() {
                    conn.psubs.iter().cloned().collect()
                } else {
                    patterns
                };
                if patterns.is_empty() {
                    conn.ack("punsubscribe", None);
                }
                for pattern in patterns {
                    if conn.psubs.remove(&pattern) {
                        registry.punsubscribe(&pattern, me);
                    }
                    conn.ack("punsubscribe", Some(&pattern));
                }
            }
            SubCmd::SSubscribe(channels) => {
                for channel in channels {
                    if conn.ssubs.insert(channel.clone()) {
                        registry.ssubscribe(&channel, me);
                    }
                    conn.ack("ssubscribe", Some(&channel));
                }
            }
            SubCmd::SUnsubscribe(channels) => {
                let channels = if channels.is_empty() {
                    conn.ssubs.iter().cloned().collect()
                } else {
                    channels
                };
                if channels.is_empty() {
                    conn.ack("sunsubscribe", None);
                }
                for channel in channels {
                    if conn.ssubs.remove(&channel) {
                        registry.sunsubscribe(&channel, me);
                    }
                    conn.ack("sunsubscribe", Some(&channel));
                }
            }
        }
        self.note_subs(conn);
    }

    /// Keep what CLIENT KILL TYPE goes by up to date.
    fn note_subs(&self, conn: &Conn) {
        if let Some(me) = self.shared.clients().get_mut(&conn.id) {
            me.subs = [conn.subs.len(), conn.psubs.len(), conn.ssubs.len()];
        }
    }

    fn turn(&mut self, idx: usize) {
        let Some(mut conn) = self.conns[idx].take() else {
            return;
        };
        conn.queued = false;
        if self.drive(idx, &mut conn) {
            self.conns[idx] = Some(conn);
        } else {
            self.close(idx, conn);
        }
    }

    /// One turn of one connection. `false` means close it.
    fn drive(&mut self, idx: usize, conn: &mut Conn) -> bool {
        // Output that waits for the disk stays where it is.
        if conn.overflow || !(conn.unsynced || conn.flush()) {
            return false;
        }
        let idle = matches!(conn.state, State::Idle);
        if idle
            && !conn.closing
            && conn.out_pending() < OUT_SOFT_LIMIT
            && !conn.fill(&mut self.scratch)
        {
            return false;
        }
        // A client that hangs up while it waits must leave the line, and
        // only reading tells.
        if self.hung_up(conn) {
            return false;
        }
        let mut more = self.pump(idx, conn);
        match self.feed(conn) {
            Some(filled) => more |= filled,
            // Too far behind: it has to sync again.
            None => return false,
        }
        if !conn.held.is_empty() && !conn.answer_pending() {
            for key in std::mem::take(&mut conn.held) {
                conn.invalidation(key.as_deref());
            }
        }
        // The same for a command that began to wait in this turn: the
        // hang-up may have been announced while its attempt was running.
        if self.hung_up(conn) {
            return false;
        }
        if conn.unsynced {
            // `commit` brings it back once the disk has the change.
            self.unsynced.push(idx);
            return true;
        }
        if !conn.flush() {
            return false;
        }
        if conn.closing {
            if conn.out_pending() == 0 {
                conn.say_goodbye();
                if !conn.flush() {
                    return false;
                }
            }
            return conn.out_pending() > 0 || conn.tls_pending();
        }
        // A waiting connection with more to read comes back for it: the
        // end of its input may lie behind what it sent ahead.
        if conn.block.is_some() && conn.readable && !conn.eof {
            conn.queued = true;
            self.runnable.push(idx);
            return true;
        }
        let idle = matches!(conn.state, State::Idle);
        if idle && conn.out_pending() < OUT_SOFT_LIMIT && (more || (conn.readable && !conn.eof)) {
            conn.queued = true;
            self.runnable.push(idx);
        }
        true
    }

    /// Whether a connection that waits in a blocking command is gone, or
    /// sent more ahead than a waiting connection may. Only reading tells
    /// of a hang-up, so a waiting connection is read past the usual mark.
    fn hung_up(&mut self, conn: &mut Conn) -> bool {
        let waiting =
            conn.block.as_ref().is_some_and(|block| !block.in_flight) || conn.wait.is_some();
        waiting
            && (!conn.fill_to(&mut self.scratch, BLOCKED_BACKLOG)
                || conn.eof
                || conn.inbuf.len() - conn.in_pos >= BLOCKED_BACKLOG)
    }

    /// Execute buffered commands. Returns whether the turn ended with
    /// commands possibly still buffered.
    fn pump(&mut self, idx: usize, conn: &mut Conn) -> bool {
        let shared = self.shared.clone();
        let engine = &shared.engine;
        let mut ran = 0;
        // Commands run when the keys of those read ahead are loading.
        let mut ahead = 0;
        // When the command before ended, if it was timed: this one starts
        // then.
        let mut mark = None;
        let more = loop {
            let ended = mark.take();
            if conn.closing || matches!(conn.state, State::Parked) {
                break false;
            }
            // The limits of a turn hold back new commands only. An attempt
            // of a blocked command that is due runs now: nothing else
            // would bring this connection back to it.
            let due = matches!(conn.state, State::GateWait(_));
            if !due && (ran >= self.pipeline_cap || conn.out_pending() >= OUT_SOFT_LIMIT) {
                break true;
            }
            if ran >= ahead && matches!(conn.state, State::Idle) && !conn.locked_out() {
                let mut keys = [&[][..]; PREFETCH_KEYS];
                let (seen, found) = resp::peek_keys(&conn.inbuf[conn.in_pos..], &mut keys);
                // One command alone gains nothing: its lookup is next.
                if seen > 1 {
                    engine.prefetch(conn.txn.db(), &keys[..found]);
                }
                ahead = ran + seen.max(1);
            }
            let cmd = match std::mem::replace(&mut conn.state, State::Idle) {
                State::GateWait(cmd) | State::Paused(cmd) => cmd,
                State::Idle | State::Parked => {
                    let db = conn.txn.db();
                    let watched = shared.monitoring.load(SeqCst) != 0;
                    let timed = engine.logging();
                    // Monitors are fed the arguments the usual way.
                    let quick = (!watched)
                        .then(|| conn.read_fast(engine.acl(), timed))
                        .flatten();
                    let (mut parsed, name, named) = match quick {
                        Some((used, cmd)) => {
                            conn.in_pos += used;
                            conn.need_more = false;
                            conn.started = timed.then(|| ended.unwrap_or_else(Instant::now));
                            conn.request_bytes = used;
                            conn.reply_from = conn.outbuf.len();
                            ran += 1;
                            (Ok(cmd), None, None)
                        }
                        None => match conn.read_command(engine.acl()) {
                            Ok((used, Some(args))) => {
                                conn.in_pos += used;
                                conn.need_more = false;
                                if is_cross_protocol(&args) {
                                    conn.closing = true;
                                    break false;
                                }
                                ran += 1;
                                if conn.locked_out()
                                    && !args.first().is_some_and(|name| no_auth(name))
                                {
                                    conn.shape = Shape::Plain;
                                    conn.reply(&Reply::err("NOAUTH Authentication required."));
                                    continue;
                                }
                                conn.started = None;
                                conn.request_bytes = used;
                                conn.reply_from = conn.outbuf.len();
                                if (watched || timed) && !observe::sensitive(&args) {
                                    if watched {
                                        shared.feed_monitors(conn.id, conn.txn.db(), &args);
                                    }
                                    if timed {
                                        observe::stash(&mut conn.stash, &args);
                                        // Feeding monitors is not the command's time.
                                        let since = ended.filter(|_| !watched);
                                        conn.started = Some(since.unwrap_or_else(Instant::now));
                                    }
                                }
                                // A RESP2 connection with subscriptions only
                                // speaks the subscription commands.
                                let subscribed = !conn.resp3 && conn.listening();
                                let name = subscribed.then(|| full_name(&args));
                                let named = match args.first() {
                                    Some(name) if !conn.user.unrestricted() => {
                                        Some((name.clone(), args.get(1).cloned()))
                                    }
                                    _ => None,
                                };
                                (Cmd::parse(args), name, named)
                            }
                            Ok((used, None)) => {
                                conn.in_pos += used;
                                conn.need_more = true;
                                // The client is gone and left no complete command.
                                conn.closing = conn.eof;
                                break false;
                            }
                            Err(e) => {
                                Reply::Error(format!("ERR {}", e.0)).encode(&mut conn.outbuf);
                                conn.closing = true;
                                break false;
                            }
                        },
                    };
                    // A replica's link sends REPLCONF and nothing else: a
                    // reply would land in the middle of its stream.
                    if conn.link.is_some() && !matches!(parsed, Ok(Cmd::Replconf(_))) {
                        continue;
                    }
                    if matches!(parsed, Ok(Cmd::Txn(Txn::Multi))) {
                        conn.txn_gen = conn.acl_gen;
                    }
                    if matches!(parsed, Ok(Cmd::Debug(_)))
                        && engine.debug_command() == Protected::Local
                        && !conn.local
                    {
                        parsed = Err(Reply::err(DEBUG_REFUSED));
                    }
                    if let (Some((name, sub)), Ok(cmd)) = (&named, &parsed) {
                        // Refused here, a command inside MULTI also
                        // spoils the transaction, as a bad one does.
                        if let Err(denied) = conn.user.check(name, sub.as_deref(), cmd) {
                            parsed = Err(denied);
                        }
                    }
                    if engine.is_replica() {
                        parsed = parsed.and_then(Cmd::on_replica);
                    }
                    let step = conn.txn.feed(engine, parsed);
                    if conn.txn.db() != db {
                        if let Some(me) = shared.clients().get_mut(&conn.id) {
                            me.db = conn.txn.db();
                        }
                    }
                    match step {
                        // Queued under rules that have changed since:
                        // what was allowed then may not be now.
                        TxnStep::Run(Cmd::ExecBlock { .. })
                            if conn.txn_gen != conn.acl_gen && !conn.user.unrestricted() =>
                        {
                            conn.shape = Shape::Plain;
                            conn.reply(&Reply::err(
                                "EXECABORT Transaction discarded because the permissions \
                                     of its user changed.",
                            ));
                            continue;
                        }
                        TxnStep::Run(cmd) => {
                            conn.shape = Shape::Plain;
                            match (name, &cmd) {
                                (None, _) | (Some(_), Cmd::Sub(_) | Cmd::Quit | Cmd::Reset) => {}
                                (Some(_), Cmd::Ping(arg)) => {
                                    conn.frame(2);
                                    Reply::bulk("pong").encode(&mut conn.outbuf);
                                    Reply::Bulk(arg.clone().unwrap_or_default())
                                        .encode(&mut conn.outbuf);
                                    continue;
                                }
                                (Some(name), _) => {
                                    let shown =
                                        String::from_utf8_lossy(&name[..name.len().min(64)]);
                                    conn.reply(&Reply::Error(format!(
                                        "ERR Can't execute '{shown}': only (P|S)SUBSCRIBE / \
                                             (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed \
                                             in this context"
                                    )));
                                    continue;
                                }
                            }
                            conn.shape = cmd.shape();
                            cmd
                        }
                        TxnStep::Reply(reply) => {
                            conn.shape = Shape::Plain;
                            conn.reply(&reply);
                            continue;
                        }
                    }
                }
            };
            // A FAILOVER under way holds back what would add to the
            // stream. An attempt of a blocked command goes on: it was let
            // in before.
            if conn.block.is_none() && shared.holds(&cmd) {
                conn.state = State::Paused(cmd);
                if !std::mem::replace(&mut conn.paused, true) {
                    self.paused.push(idx);
                }
                break false;
            }
            // Looked at again here, not only when it was read: a
            // transaction queued, or a command that waited, while this node
            // was still the primary.
            let cmd = if engine.is_replica() {
                match cmd.on_replica() {
                    Ok(cmd) => cmd,
                    Err(_) if conn.block.is_some() => {
                        self.unblock(idx, conn);
                        conn.reply(&Reply::err(
                            "UNBLOCKED force unblock from blocking operation, instance state \
                             changed (master -> replica?)",
                        ));
                        conn.answered();
                        continue;
                    }
                    Err(reply) => {
                        conn.shape = Shape::Plain;
                        self.deliver(idx, conn, reply);
                        continue;
                    }
                }
            } else {
                cmd
            };
            let cmd = match cmd {
                Cmd::Sub(sub) => {
                    self.sub_cmd(idx, conn, sub);
                    continue;
                }
                Cmd::Block(op, ms) => {
                    // The time it waits is not time it takes.
                    conn.started = None;
                    let wait = Duration::from_millis(ms);
                    conn.block = Some(Block {
                        deadline: (ms > 0).then(|| Instant::now().checked_add(wait)).flatten(),
                        keys: wait_hashes(engine, &op),
                        listed: false,
                        in_flight: true,
                        again: false,
                        woken: false,
                        op: op.clone(),
                    });
                    Cmd::Multi(op)
                }
                cmd => cmd,
            };
            self.track_read(conn, &cmd);
            // Asked per command: CONFIG SET may be earlier in the pipeline.
            conn.wrote = cmd.may_change() && engine.sync_before_reply(conn.txn.db(), &cmd);
            // Writing the RDB file takes the exclusive side of the gate and
            // then the disk: a thread of its own does it, never this loop.
            if matches!(cmd, Cmd::Save | Cmd::BgSave) && engine.rdb_file().is_some() {
                self.save_rdb(idx, conn, cmd == Cmd::Save);
                continue;
            }
            match cmd.class() {
                Class::Conn => {
                    let reply = match &cmd {
                        Cmd::Hello(proto, auth) => 'hello: {
                            if let Some((name, pass)) = auth {
                                match engine.acl().auth(name, pass) {
                                    Ok(user) => {
                                        login(&shared, conn, user);
                                        self.stop_watching(idx, conn);
                                    }
                                    Err(e) => break 'hello e,
                                }
                            } else if conn.locked_out() {
                                break 'hello Reply::err(
                                    "NOAUTH HELLO must be called with the client already \
                                     authenticated, otherwise the HELLO <proto> AUTH <user> \
                                     <pass> option can be used to authenticate the client and \
                                     select the RESP protocol version at the same time",
                                );
                            }
                            if let Some(proto) = proto {
                                conn.resp3 = *proto == 3;
                            }
                            if let Some(me) = shared.clients().get_mut(&conn.id) {
                                me.resp3 = conn.resp3;
                            }
                            engine.hello(if conn.resp3 { 3 } else { 2 })
                        }
                        Cmd::Auth(name, pass) => match engine.auth(name.as_deref(), pass) {
                            Ok(user) => {
                                login(&shared, conn, user);
                                self.stop_watching(idx, conn);
                                Reply::Ok
                            }
                            Err(e) => e,
                        },
                        Cmd::Acl(args) => engine.acl().command(args, &conn.user),
                        Cmd::Client(sub) => self.tracking_cmd(idx, conn, sub),
                        Cmd::Shutdown {
                            abort: false,
                            failover,
                        } if !failover || engine.node().is_some() => {
                            match &shared.on_shutdown {
                                // Gone without a reply, as from Valkey.
                                Some(OnShutdown(stop)) => {
                                    println!("SHUTDOWN asked by client {}: stopping", conn.id);
                                    stop();
                                    conn.closing = true;
                                    conn.answered();
                                    continue;
                                }
                                None => Reply::err("ERR Errors trying to SHUTDOWN. Check logs."),
                            }
                        }
                        Cmd::Monitor => {
                            if !std::mem::replace(&mut conn.monitor, true) {
                                shared.monitors().push(Sub {
                                    worker: self.id,
                                    idx,
                                    conn: conn.id,
                                });
                                shared.monitoring.fetch_add(1, SeqCst);
                            }
                            Reply::Ok
                        }
                        Cmd::Reset => {
                            self.unsubscribe_all(idx, conn);
                            self.note_subs(conn);
                            self.untrack(conn);
                            self.unmonitor(idx, conn);
                            conn.txn = TxnState::default();
                            conn.resp3 = false;
                            // Back to "default", logged in only if that
                            // needs no password.
                            conn.acl_gen = engine.acl().generation();
                            conn.user = engine.acl().default_user();
                            conn.authed = false;
                            if let Some(me) = shared.clients().get_mut(&conn.id) {
                                me.resp3 = false;
                                me.name.clear();
                                me.db = 0;
                                me.user = b"default".to_vec();
                            }
                            engine.exec_conn(&cmd)
                        }
                        // The reply waits for the disk, as every reply
                        // does under `appendfsync always`: the wait itself
                        // is not for a worker inside the gate.
                        Cmd::Save if engine.has_data_file() => {
                            conn.wrote = true;
                            Reply::Ok
                        }
                        // The same wait: the sync covers every change this
                        // connection made before, and there are no replicas.
                        // With numlocal 0 it answers at once: WAITAOF is for
                        // anyone, a sync on demand is SAVE's, for admins.
                        Cmd::WaitAof(true) if engine.has_data_file() => {
                            conn.wrote = true;
                            Reply::Array(vec![Reply::Int(1), Reply::Int(0)])
                        }
                        Cmd::Role => shared.role(),
                        Cmd::Replconf(args) => match self.replconf(conn, args) {
                            Some(reply) => reply,
                            None => continue,
                        },
                        Cmd::Psync(..) | Cmd::Sync => match self.sync(idx, conn, &cmd) {
                            Some(reply) => reply,
                            None => continue,
                        },
                        Cmd::ReplicaOf(target) => match self.replicaof(idx, conn, target.clone()) {
                            Some(reply) => reply,
                            None => continue,
                        },
                        Cmd::Wait(replicas, ms) => match self.wait(idx, conn, *replicas, *ms) {
                            Some(reply) => reply,
                            None => continue,
                        },
                        Cmd::Failover(args) => self.failover(args),
                        _ => engine.with_db(conn.txn.db(), || engine.exec_conn(&cmd)),
                    };
                    if engine.acl().generation() != self.acl_seen {
                        // Every worker looks its connections over.
                        (0..shared.workers.len()).for_each(|worker| shared.wake(worker));
                    }
                    conn.reply(&reply);
                    conn.answered();
                    shared.slow(conn);
                    conn.closing |= cmd == Cmd::Quit;
                }
                Class::Shared => {
                    if !self.enter() {
                        conn.state = State::GateWait(cmd);
                        self.gate_wait.push(idx);
                        break false;
                    }
                    let ticket = self.ticket(idx);
                    let out = &mut conn.outbuf;
                    let done =
                        engine.with_db(conn.txn.db(), || engine.submit_into(cmd, ticket, out));
                    match done {
                        Submitted::Reply(reply) => {
                            self.deliver(idx, conn, reply);
                            mark = shared.slow(conn);
                        }
                        Submitted::Written => {
                            // GET: no waiting command answers this way.
                            debug_assert!(conn.block.is_none());
                            conn.answered();
                            mark = shared.slow(conn);
                        }
                        Submitted::Parked => conn.state = State::Parked,
                    }
                }
                Class::Exclusive => {
                    conn.state = State::Parked;
                    let caller = (!conn.user.unrestricted()).then(|| conn.user.clone());
                    let (cmd, tail) = split_exec(cmd);
                    conn.exec_tail = tail;
                    let job = Job::Run(cmd.in_db(conn.txn.db()), self.ticket(idx), caller);
                    if shared.jobs.send(job).is_err() {
                        self.unblock(idx, conn);
                        Reply::err("ERR server is shutting down").encode(&mut conn.outbuf);
                        conn.state = State::Idle;
                        conn.closing = true;
                    }
                }
            }
        };
        self.leave();
        self.handle().commands.fetch_add(ran as u64, SeqCst);
        more
    }

    /// REPLCONF: what a replica says of itself before it syncs, and how
    /// far it got once it has. ACK and GETACK get no reply.
    fn replconf(&self, conn: &mut Conn, args: &[Vec<u8>]) -> Option<Reply> {
        if !args.len().is_multiple_of(2) {
            return Some(Reply::err("ERR syntax error"));
        }
        let conf = conn.replconf.get_or_insert_with(Box::default);
        for [option, value] in args.as_chunks::<2>().0 {
            match option.to_ascii_lowercase().as_slice() {
                b"listening-port" => match resp::parse_i64(value).map(u16::try_from) {
                    Some(Ok(port)) => conf.port = port,
                    _ => return Some(Reply::err(NOT_INT)),
                },
                b"ip-address" if value.len() >= 46 => {
                    return Some(Reply::Error(format!(
                        "ERR REPLCONF ip-address provided by replica instance is too long: {} \
                         bytes",
                        value.len()
                    )))
                }
                // Shown in INFO, which Sentinel reads: no room there for a
                // ',' or a line end.
                b"ip-address"
                    if !value
                        .iter()
                        .all(|&b| b.is_ascii_alphanumeric() || b".:-_%".contains(&b)) =>
                {
                    return Some(Reply::err(
                        "ERR REPLCONF ip-address provided by replica instance is not an address",
                    ))
                }
                b"ip-address" => conf.ip = Some(String::from_utf8_lossy(value).into_owned()),
                // What this node does not use is passed over, as Valkey
                // passes over what it does not know.
                b"capa" | b"version" => {}
                b"rdb-only" => match value.as_slice() {
                    b"0" => conf.rdb_only = false,
                    b"1" => conf.rdb_only = true,
                    _ => return Some(Reply::err("ERR value is out of range")),
                },
                b"rdb-channel" if value.as_slice() == b"0" => {}
                b"ack" => {
                    let offset = resp::parse_i64(value).and_then(|n| u64::try_from(n).ok());
                    if let (Some(offset), Some(_)) = (offset, &conn.link) {
                        self.shared.acked(conn.id, offset);
                    }
                    return None;
                }
                b"getack" => return None,
                _ => {
                    return Some(Reply::Error(format!(
                        "ERR Unrecognized REPLCONF option: {}",
                        String::from_utf8_lossy(&option[..option.len().min(64)])
                    )))
                }
            }
        }
        Some(Reply::Ok)
    }

    /// PSYNC and SYNC: this connection becomes the link of a replica. A
    /// sync from where the replica is starts at once; one that needs the
    /// whole dataset, or this node to become the primary first (PSYNC
    /// FAILOVER), is made on a thread of its own and starts in `start`.
    fn sync(&mut self, idx: usize, conn: &mut Conn, cmd: &Cmd) -> Option<Reply> {
        if conn.link.is_some() {
            return None;
        }
        let shared = self.shared.clone();
        let engine = &shared.engine;
        let (asked, failover) = match cmd {
            Cmd::Psync(replid, offset, failover) => (Some((replid.clone(), *offset)), *failover),
            _ => (None, false),
        };
        if !failover && engine.is_replica() && !shared.linked() {
            return Some(Reply::err(
                "NOMASTERLINK Can't SYNC while not connected with my master",
            ));
        }
        let conf = conn.replconf.take().unwrap_or_default();
        let psync = asked.is_some();
        let ip = conf.ip.clone().unwrap_or_else(|| {
            conn.stream
                .peer_addr()
                .map(|a| a.ip().to_string())
                .unwrap_or_default()
        });
        conn.link = Some(Box::new(Link {
            at: None,
            rdb: None,
            psync,
            rdb_only: conf.rdb_only,
        }));
        if !conf.rdb_only {
            let replica = Replica {
                at: Sub {
                    worker: self.id,
                    idx,
                    conn: conn.id,
                },
                ip,
                port: conf.port,
                ack: 0,
                acked: Instant::now(),
                online: false,
            };
            shared.replicas().insert(conn.id, replica);
            if let Some(me) = shared.clients().get_mut(&conn.id) {
                me.replica = true;
            }
        }
        // A replica that wants the file alone gets the whole of it.
        let resume = asked.as_ref().filter(|_| !failover && !conf.rdb_only);
        if let Some(at) = resume.and_then(|(replid, offset)| engine.repl_resume(replid, *offset)) {
            self.begin(conn, Start::Continue(at));
            return None;
        }
        // "?" asks for the whole dataset on purpose.
        if resume.is_some_and(|(replid, _)| replid.as_slice() != b"?") {
            shared.sync_partial_err.fetch_add(1, SeqCst);
        }
        let ticket = self.ticket(idx);
        let worker = self.id;
        let spawned = if failover {
            let maker = shared.clone();
            let job = move || {
                let start = maker.prepare(asked, failover);
                maker.send_start(worker, ticket, start);
            };
            std::thread::Builder::new()
                .name("hotstash-sync".into())
                .spawn(job)
                .map(drop)
        } else {
            shared.full_sync(worker, ticket)
        };
        match spawned {
            Ok(()) => {
                conn.state = State::Parked;
                None
            }
            Err(e) => {
                self.unlink(conn);
                Some(Reply::Error(format!("ERR cannot start the sync: {e}")))
            }
        }
    }

    /// A sync made on a thread of its own is ready.
    fn start(&mut self, ticket: Ticket, start: Start) {
        let (_, _, idx, gen) = unpack(ticket);
        if self.gens.get(idx) != Some(&gen) {
            return;
        }
        let Some(mut conn) = self.conns.get_mut(idx).and_then(Option::take) else {
            return;
        };
        let expected = matches!(conn.state, State::Parked) && conn.link.is_some();
        if expected {
            conn.state = State::Idle;
            self.begin(&mut conn, start);
        }
        self.conns[idx] = Some(conn);
        if expected {
            self.schedule(idx);
        }
    }

    /// Send what a replica's sync begins with; the stream follows.
    fn begin(&self, conn: &mut Conn, start: Start) {
        let Some((psync, rdb_only)) = conn.link.as_ref().map(|l| (l.psync, l.rdb_only)) else {
            return;
        };
        let at = match start {
            Start::Full(rdb, at) => {
                self.shared.sync_full.fetch_add(1, SeqCst);
                if psync {
                    let line = format!("+FULLRESYNC {} {}\r\n", at.replid, at.offset);
                    conn.outbuf.extend_from_slice(line.as_bytes());
                }
                let header = format!("${}\r\n", rdb.len());
                conn.outbuf.extend_from_slice(header.as_bytes());
                // Sent a part at a time, from the file the replicas share.
                if let Some(link) = conn.link.as_mut() {
                    link.rdb = Some((rdb, 0));
                }
                at
            }
            Start::Continue(at) => {
                self.shared.sync_partial_ok.fetch_add(1, SeqCst);
                let line = format!("+CONTINUE {}\r\n", at.replid);
                conn.outbuf.extend_from_slice(line.as_bytes());
                at
            }
            Start::Refused(reply) => {
                conn.reply(&reply);
                self.unlink(conn);
                return;
            }
        };
        // Closed once the file is sent: `feed`.
        if rdb_only {
            return;
        }
        let Some(link) = conn.link.as_mut() else {
            return;
        };
        link.at = Some(at);
        if link.rdb.is_none() {
            self.online(conn.id);
        }
    }

    /// A replica has what its sync begins with and takes the stream.
    fn online(&self, id: u64) {
        if let Some(replica) = self.shared.replicas().get_mut(&id) {
            replica.online = true;
            replica.acked = Instant::now();
        }
    }

    /// Send a replica the stream it has not had, as much as its output
    /// takes: whether there may be more. `None`: what it needs is no
    /// longer held.
    fn feed(&self, conn: &mut Conn) -> Option<bool> {
        let mut room = OUT_SOFT_LIMIT.saturating_sub(conn.out_pending());
        let Some(link) = conn.link.as_mut() else {
            return Some(false);
        };
        if room == 0 {
            return Some(false);
        }
        if let Some((rdb, sent)) = link.rdb.as_mut() {
            let take = room.min(rdb.len() - *sent);
            conn.outbuf.extend_from_slice(&rdb[*sent..*sent + take]);
            *sent += take;
            if *sent < rdb.len() {
                return Some(true);
            }
            link.rdb = None;
            room -= take;
            // The replica loads the file now, and says nothing until then.
            conn.last_input = Instant::now();
            if link.rdb_only {
                conn.closing = true;
                return Some(false);
            }
            self.online(conn.id);
        }
        let Some(at) = link.at.as_mut() else {
            return Some(false);
        };
        if room == 0 {
            return Some(true);
        }
        let engine = &self.shared.engine;
        // Asked first: whatever is added after the copy then wakes us.
        engine.repl_want_wake();
        let reached = engine.repl_copy(at, &mut conn.outbuf, room)?;
        let filled = reached - at.offset == room as u64;
        at.offset = reached;
        Some(filled)
    }

    /// The stream grew: a replica's link on this worker sends more.
    fn feed_link(&mut self, sub: Sub) {
        let linked = matches!(
            self.conns.get(sub.idx),
            Some(Some(conn)) if conn.id == sub.conn && conn.link.is_some()
        );
        if linked {
            self.schedule(sub.idx);
        }
    }

    /// The connection no longer feeds a replica.
    fn unlink(&self, conn: &mut Conn) {
        if conn.link.take().is_some() {
            self.shared.replicas().remove(&conn.id);
            if let Some(me) = self.shared.clients().get_mut(&conn.id) {
                me.replica = false;
            }
        }
    }

    /// REPLICAOF: answered once the change is made, on a thread of its
    /// own, since becoming a replica or a primary takes the exclusive
    /// side.
    fn replicaof(
        &mut self,
        idx: usize,
        conn: &mut Conn,
        target: Option<(String, u16)>,
    ) -> Option<Reply> {
        let shared = self.shared.clone();
        if shared.engine.node().is_some() {
            return Some(Reply::err("ERR REPLICAOF not allowed in cluster mode."));
        }
        if conn.link.is_some() {
            return Some(Reply::err(
                "ERR Command is not valid when client is a replica.",
            ));
        }
        if !matches!(*shared.failover(), Handing::No) {
            return Some(Reply::err("ERR REPLICAOF not allowed while failing over."));
        }
        match &target {
            None if !shared.engine.is_replica() => return Some(Reply::Ok),
            Some((host, port))
                if shared
                    .follower()
                    .as_ref()
                    .is_some_and(|f| f.host == *host && f.port == *port) =>
            {
                return Some(Reply::Simple(
                    "OK Already connected to specified master".into(),
                ));
            }
            _ => {}
        }
        let ticket = self.ticket(idx);
        let switcher = shared.clone();
        let job = move || {
            let reply = switcher.switch(target);
            switcher.complete(ticket, reply);
        };
        let spawned = std::thread::Builder::new()
            .name("hotstash-replicaof".into())
            .spawn(job);
        match spawned {
            Ok(_) => {
                conn.state = State::Parked;
                None
            }
            Err(e) => Some(Reply::Error(format!("ERR cannot change the role: {e}"))),
        }
    }

    /// WAIT: answered at once when enough replicas have every write so
    /// far; else once they say they do, or when the time is up.
    fn wait(&mut self, idx: usize, conn: &mut Conn, replicas: i64, ms: u64) -> Option<Reply> {
        let shared = self.shared.clone();
        let engine = &shared.engine;
        if engine.is_replica() {
            return Some(Reply::err(
                "ERR WAIT cannot be used with replica instances. Please also note that since \
                 Redis 4.0 if a replica is configured to be writable (which is not the default) \
                 writes to replicas are just local and are not propagated.",
            ));
        }
        let offset = engine.repl_info().offset;
        let have = shared.acked_at(offset);
        if have >= replicas {
            return Some(Reply::Int(have));
        }
        // Listed before counting again: an ACK that comes in between is
        // then seen by one or the other.
        let me = Sub {
            worker: self.id,
            idx,
            conn: conn.id,
        };
        shared.waiting().push(me);
        let have = shared.acked_at(offset);
        if have >= replicas {
            shared.waiting().retain(|sub| *sub != me);
            return Some(Reply::Int(have));
        }
        self.getack = true;
        let deadline = (ms > 0)
            .then(|| Instant::now().checked_add(Duration::from_millis(ms)))
            .flatten();
        if let Some(at) = deadline {
            self.deadlines.insert((at, idx));
        }
        conn.wait = Some(Wait {
            offset,
            replicas,
            deadline,
        });
        conn.state = State::Parked;
        None
    }

    /// FAILOVER, checked as Valkey checks it. Writes wait from here on;
    /// `Shared::failover_tick` hands over once a replica has the whole
    /// stream.
    fn failover(&self, cmd: &Failover) -> Reply {
        let shared = &self.shared;
        if shared.engine.node().is_some() {
            return Reply::err(
                "ERR FAILOVER not allowed in cluster mode. Use CLUSTER FAILOVER command instead.",
            );
        }
        let Failover::Start { to, timeout, force } = cmd else {
            return if shared.abort_failover("Failover manually aborted") {
                Reply::Ok
            } else {
                Reply::err("ERR No failover in progress.")
            };
        };
        let mut failover = shared.failover();
        if !matches!(*failover, Handing::No) {
            return Reply::err("ERR FAILOVER already in progress.");
        }
        if shared.engine.is_replica() {
            return Reply::err("ERR FAILOVER is not valid when server is a replica.");
        }
        {
            let replicas = shared.replicas();
            if replicas.is_empty() {
                return Reply::err("ERR FAILOVER requires connected replicas.");
            }
            if *force && (timeout.is_none() || to.is_none()) {
                return Reply::err(
                    "ERR FAILOVER with force option requires both a timeout and target HOST and \
                     IP.",
                );
            }
            if let Some((host, port)) = to {
                let target = replicas
                    .values()
                    .find(|r| r.ip.eq_ignore_ascii_case(host) && i64::from(r.port) == *port);
                match target {
                    None => {
                        return Reply::err("ERR FAILOVER target HOST and PORT is not a replica.")
                    }
                    Some(r) if !r.online => {
                        return Reply::err("ERR FAILOVER target replica is not online.")
                    }
                    Some(_) => {}
                }
            }
        }
        *failover = Handing::Waiting {
            to: to.clone(),
            until: timeout.and_then(|ms| Instant::now().checked_add(Duration::from_millis(ms))),
            force: *force,
        };
        shared.writes_paused.store(true, SeqCst);
        drop(failover);
        // The replicas say where they are now rather than within a second.
        shared.engine.repl_getack();
        Reply::Ok
    }

    /// A replica said how far it got: answer this connection's WAIT if
    /// enough have what it waits for.
    fn recount(&mut self, sub: Sub) {
        let wanted = match self.conns.get(sub.idx) {
            Some(Some(conn)) if conn.id == sub.conn => {
                conn.wait.as_ref().map(|wait| (wait.offset, wait.replicas))
            }
            _ => None,
        };
        let Some((offset, replicas)) = wanted else {
            return;
        };
        let have = self.shared.acked_at(offset);
        if have < replicas {
            return;
        }
        let Some(mut conn) = self.conns[sub.idx].take() else {
            return;
        };
        self.unwait(sub.idx, &mut conn);
        conn.reply(&Reply::Int(have));
        conn.answered();
        conn.state = State::Idle;
        self.conns[sub.idx] = Some(conn);
        self.schedule(sub.idx);
    }

    /// The connection no longer waits in WAIT.
    fn unwait(&mut self, idx: usize, conn: &mut Conn) {
        let Some(wait) = conn.wait.take() else {
            return;
        };
        if let Some(at) = wait.deadline {
            self.deadlines.remove(&(at, idx));
        }
        let me = Sub {
            worker: self.id,
            idx,
            conn: conn.id,
        };
        self.shared.waiting().retain(|sub| *sub != me);
    }

    /// A fresh ticket for the connection in slot `idx`.
    fn ticket(&mut self, idx: usize) -> Ticket {
        self.gens[idx] = self.gens[idx].wrapping_add(1);
        pack(self.shared.epoch, self.id, idx, self.gens[idx])
    }
}

/// The longest CLIENT PAUSE: one that asks for more ends then.
const LONGEST_PAUSE: Duration = Duration::from_secs(100 * 365 * 86_400);

/// The commands a connection may send before it has logged in.
/// A command's name as Redis gives it in errors: "pubsub|channels" for a
/// subcommand of a container.
fn full_name(args: &[Vec<u8>]) -> Vec<u8> {
    let mut name = args[0].to_ascii_lowercase();
    let known = |sub: &Vec<u8>| {
        cmdtable::lookup(&args[0]).is_some_and(|info| {
            info.subcommands
                .iter()
                .any(|s| s.as_bytes().eq_ignore_ascii_case(sub))
        })
    };
    if let Some(sub) = args.get(1).filter(|sub| known(sub)) {
        name.push(b'|');
        name.extend(sub.to_ascii_lowercase());
    }
    name
}

fn no_auth(name: &[u8]) -> bool {
    [&b"AUTH"[..], b"HELLO", b"QUIT", b"RESET"]
        .iter()
        .any(|allowed| name.eq_ignore_ascii_case(allowed))
}

/// AUTH succeeded: the connection is `user` from now on.
/// INFO's `# CPU`: the time this process spent in the kernel and outside
/// it. Where the system does not say, nothing.
#[cfg(unix)]
fn cpu_section() -> String {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: RUSAGE_SELF is always a valid target, and getrusage fills
    // the whole struct it is given when it returns 0.
    let usage = unsafe {
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) != 0 {
            return String::new();
        }
        usage.assume_init()
    };
    let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    format!(
        "# CPU\r\nused_cpu_sys:{:.6}\r\nused_cpu_user:{:.6}\r\n",
        secs(usage.ru_stime),
        secs(usage.ru_utime)
    )
}

#[cfg(not(unix))]
fn cpu_section() -> String {
    String::new()
}

fn login(shared: &Shared, conn: &mut Conn, user: Arc<User>) {
    if let Some(me) = shared.clients().get_mut(&conn.id) {
        me.user = user.name().to_vec();
    }
    conn.user = user;
    conn.authed = true;
}

/// Runs everything that needs the exclusive side of the gate, so no event
/// loop ever waits for it.
fn executor(shared: &Shared, jobs: &Receiver<Job>) {
    loop {
        let (cmd, ticket, caller) = match jobs.recv_timeout(EXPIRE_TICK) {
            Ok(Job::Run(cmd, ticket, caller)) => (cmd, ticket, caller),
            Ok(Job::Stop) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {
                // The idle moments of this thread drive background expiry;
                // not while a FAILOVER waits for the stream to stand still.
                if !shared.writes_paused() {
                    shared.engine.expire_tick();
                }
                continue;
            }
        };
        let mut done = Vec::new();
        let mut stop = false;
        {
            let mut exclusive = shared.engine.exclusive();
            *shared.executor_busy() = true;
            let mut run = |cmd, ticket, caller| {
                // Nobody is left to read the reply of a job that waited
                // until shutdown.
                if shared.stop.load(SeqCst) {
                    return Reply::err("ERR server is shutting down");
                }
                // Queued while this node was the primary: it may not be now.
                let cmd = match shared.engine.is_replica() {
                    true => match Cmd::on_replica(cmd) {
                        Ok(cmd) => cmd,
                        Err(refused) => return refused,
                    },
                    false => cmd,
                };
                hotstash_core::engine::run_as(ticket, || {
                    acl::with_caller(caller, || exclusive.run(cmd))
                })
            };
            let started = Instant::now();
            done.push((ticket, run(cmd, ticket, caller)));
            while done.len() < EXCL_BATCH && started.elapsed() < EXCL_HOLD {
                match jobs.try_recv() {
                    Ok(Job::Run(cmd, ticket, caller)) => {
                        done.push((ticket, run(cmd, ticket, caller)));
                    }
                    Ok(Job::Stop) => {
                        stop = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
            // Cleared before the gate reopens: while the flag is set, a
            // running script can only be one of ours.
            *shared.executor_busy() = false;
        }
        // Replies go out with the gate open again, so the connections that
        // get them can carry on at once.
        for (ticket, reply) in done {
            shared.complete(ticket, reply);
        }
        if stop {
            return;
        }
    }
}

/// The listeners: without TLS, with TLS, or both.
struct Listeners {
    plain: Option<TcpListener>,
    secure: Option<TcpListener>,
}

fn acceptor(shared: &Shared, mut listeners: Listeners, mut poll: Poll, max_conns: usize) {
    let mut events = Events::with_capacity(64);
    let both = [
        (&mut listeners.plain, PLAIN),
        (&mut listeners.secure, SECURE),
    ];
    for (listener, token) in both {
        let Some(listener) = listener else {
            continue;
        };
        if let Err(e) = poll
            .registry()
            .register(listener, token, Interest::READABLE)
        {
            eprintln!("hotstash acceptor: cannot register the listener: {e}");
            return;
        }
    }
    loop {
        if let Err(e) = poll.poll(&mut events, None) {
            if e.kind() != ErrorKind::Interrupted {
                eprintln!("hotstash acceptor: poll failed: {e}");
                return;
            }
        }
        for (listener, secure) in [(&listeners.plain, false), (&listeners.secure, true)] {
            let Some(listener) = listener else {
                continue;
            };
            loop {
                if shared.stop.load(SeqCst) {
                    return;
                }
                match listener.accept() {
                    Ok((stream, _)) => dispatch(shared, stream, secure, max_conns),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    // Out of handles, or the client gave up already. Keep
                    // accepting, without spinning.
                    Err(_) => std::thread::sleep(ACCEPT_ERROR_PAUSE),
                }
            }
        }
    }
}

fn dispatch(shared: &Shared, mut stream: TcpStream, secure: bool, max_conns: usize) {
    if shared.conns.fetch_add(1, SeqCst) >= max_conns {
        shared.conns.fetch_sub(1, SeqCst);
        shared.rejected.fetch_add(1, SeqCst);
        if !secure {
            // Best effort: the client is being turned away either way.
            let _ = stream.write(b"-ERR max number of clients reached\r\n");
        }
        return;
    }
    // Latency over batching; failing to set it is harmless.
    let _ = stream.set_nodelay(true);
    // Numbered here, in the order of accept: a larger id is a later
    // connection, as in Redis, whichever worker gets to it first.
    let id = shared.next_conn.fetch_add(1, SeqCst);
    let worker = shared.next_worker.fetch_add(1, SeqCst) % shared.workers.len();
    shared.workers[worker]
        .inbox
        .push(Msg::NewConn(stream, secure, id));
    shared.wake(worker);
}

pub struct Server {
    shared: Arc<Shared>,
    addr: SocketAddr,
    tls_addr: Option<SocketAddr>,
    memcached_addr: Option<SocketAddr>,
    metrics_addr: Option<SocketAddr>,
    accept_waker: Waker,
    threads: Vec<JoinHandle<()>>,
    executor: Option<JoinHandle<()>>,
    /// Whatever must outlive the server, released after it has stopped.
    keep: Option<Arc<dyn Any + Send + Sync>>,
}

impl Server {
    /// Keep `value` alive for as long as the server runs: the database's
    /// writer to disk, for one.
    pub fn keep_alive(&mut self, value: Arc<dyn Any + Send + Sync>) {
        self.keep = Some(value);
    }

    /// Bind and start serving `engine`. One engine serves one server at a
    /// time: a second `start` fails while the first server is running.
    pub fn start(engine: Arc<Engine>, cfg: ServerConfig) -> io::Result<Server> {
        let workers = cfg.workers.clamp(1, MAX_WORKERS);
        let plain = match cfg.plain {
            true => Some(TcpListener::bind(cfg.addr)?),
            false => None,
        };
        let secure = match &cfg.tls {
            Some(tls) => Some(TcpListener::bind(tls.addr)?),
            None => None,
        };
        let tls_addr = secure.as_ref().map(TcpListener::local_addr).transpose()?;
        let addr = match (&plain, tls_addr) {
            (Some(listener), _) => listener.local_addr()?,
            (None, Some(addr)) => addr,
            (None, None) => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "nothing to listen on: neither a plain nor a TLS listener is configured",
                ))
            }
        };
        if let Some(node) = engine.node() {
            node.set_port(addr.port());
        }
        let listeners = Listeners { plain, secure };
        let accept_poll = Poll::new()?;
        let accept_waker = Waker::new(accept_poll.registry(), WAKER)?;

        let mut polls = Vec::with_capacity(workers);
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let poll = Poll::new()?;
            handles.push(CachePadded::new(WorkerHandle {
                inbox: SegQueue::new(),
                waker: Waker::new(poll.registry(), WAKER)?,
                wake_pending: AtomicBool::new(false),
                gate_waiting: AtomicBool::new(false),
                commands: AtomicU64::new(0),
            }));
            polls.push(poll);
        }
        let (jobs_tx, jobs_rx) = crossbeam_channel::unbounded();
        let shared = Arc::new(Shared {
            engine,
            workers: handles.into_boxed_slice(),
            jobs: jobs_tx,
            stop: AtomicBool::new(false),
            next_worker: AtomicUsize::new(0),
            conns: AtomicUsize::new(0),
            epoch: NEXT_EPOCH.fetch_add(1, SeqCst),
            executor_busy: Mutex::new(false),
            pubsub: Registry::default(),
            next_conn: AtomicU64::new(1),
            retries: AtomicU64::new(0),
            blockers: Mutex::new(HashMap::new()),
            clients: Mutex::new(BTreeMap::new()),
            tracker: Tracker::new(tracking::MAX_KEYS, tracking::MAX_BYTES),
            monitors: Mutex::new(Vec::new()),
            monitoring: AtomicUsize::new(0),
            on_shutdown: cfg.on_shutdown.clone(),
            replicas: Mutex::new(BTreeMap::new()),
            waiting: Mutex::new(Vec::new()),
            follower: Mutex::new(None),
            switching: Mutex::new(()),
            next_ping: Mutex::new(Instant::now()),
            port: addr.port(),
            sync_full: AtomicU64::new(0),
            sync_partial_ok: AtomicU64::new(0),
            sync_partial_err: AtomicU64::new(0),
            sync_asked: Mutex::new(None),
            failover: Mutex::new(Handing::No),
            writes_paused: AtomicBool::new(false),
            client_pause: Mutex::new(None),
            client_paused: AtomicBool::new(false),
            max_conns: cfg.max_conns,
            rejected: AtomicU64::new(0),
        });
        if !shared.engine.attach_hooks(shared.clone()) {
            return Err(io::Error::other("this database already has a listener"));
        }
        let engine = &shared.engine;
        if let Some(idle) = cfg.idle_timeout {
            engine.set_client_timeout(idle.as_secs());
        }
        let mut slots = Vec::with_capacity(workers);
        for _ in 0..workers {
            match engine.gate().register() {
                Some(slot) => slots.push(slot),
                None => {
                    for slot in slots {
                        engine.gate().unregister(slot);
                    }
                    engine.set_hooks(None);
                    return Err(io::Error::other(
                        "no free execution gate slot for the workers",
                    ));
                }
            }
        }

        // From here on dropping `server` stops whatever has been started.
        let mut server = Server {
            shared: shared.clone(),
            addr,
            tls_addr,
            memcached_addr: None,
            metrics_addr: None,
            accept_waker,
            threads: Vec::with_capacity(workers + 1),
            executor: None,
            keep: None,
        };
        for (id, (poll, slot)) in polls.into_iter().zip(slots).enumerate() {
            let worker = Worker {
                id,
                shared: shared.clone(),
                poll,
                slot,
                in_gate: false,
                gate_blocked: false,
                starved: false,
                conns: Vec::new(),
                gens: Vec::new(),
                free: Vec::new(),
                runnable: Vec::new(),
                gate_wait: Vec::new(),
                paused: Vec::new(),
                getack: false,
                deadlines: BTreeSet::new(),
                scratch: vec![0; READ_CHUNK].into_boxed_slice(),
                pipeline_cap: cfg.pipeline_cap.max(1),
                limits: cfg.limits,
                acl_seen: 0,
                tls: cfg.tls.as_ref().map(|tls| tls.config.clone()),
                stall_timeout: cfg.stall_timeout,
                next_sweep: Instant::now() + SWEEP_EVERY,
                unsynced: Vec::new(),
                sync_sent: 0,
                held: Vec::new(),
            };
            server.threads.push(
                std::thread::Builder::new()
                    .name(format!("hotstash-worker-{id}"))
                    .spawn(move || worker.run())?,
            );
        }
        let for_executor = shared.clone();
        server.executor = Some(
            std::thread::Builder::new()
                .name("hotstash-exclusive".into())
                .spawn(move || executor(&for_executor, &jobs_rx))?,
        );
        let for_acceptor = shared.clone();
        let max_conns = cfg.max_conns;
        server.threads.push(
            std::thread::Builder::new()
                .name("hotstash-accept".into())
                .spawn(move || acceptor(&for_acceptor, listeners, accept_poll, max_conns))?,
        );
        if let Some(at) = cfg.memcached {
            let (bound, thread) = memcached::start(shared.clone(), at, &cfg)?;
            server.memcached_addr = Some(bound);
            server.threads.push(thread);
        }
        if let Some(at) = cfg.metrics {
            let (bound, thread) = metrics::start(shared.clone(), at)?;
            server.metrics_addr = Some(bound);
            server.threads.push(thread);
        }
        Ok(server)
    }

    /// Where the server listens: without TLS if it does that at all,
    /// else with.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Where the server listens for TLS connections.
    pub fn tls_addr(&self) -> Option<SocketAddr> {
        self.tls_addr
    }

    /// Where the server speaks the Memcached protocol.
    pub fn memcached_addr(&self) -> Option<SocketAddr> {
        self.memcached_addr
    }

    /// Where the server serves Prometheus metrics.
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics_addr
    }

    /// Open client connections.
    pub fn connections(&self) -> usize {
        self.shared.conns.load(SeqCst)
    }

    /// Stop all threads and close all connections. Commands still parked
    /// get no reply. A script this server is running is killed as SCRIPT
    /// KILL would; one that has already written cannot be, and after
    /// `STOP_GRACE` it is left to finish on its own thread.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        if self.shared.stop.swap(true, SeqCst) {
            return;
        }
        // A failed wake leaves a thread asleep and the join below hanging,
        // so say so instead of failing silently.
        for handle in self.shared.workers.iter() {
            if let Err(e) = handle.waker.wake() {
                eprintln!("hotstash: cannot wake a worker for shutdown: {e}");
            }
        }
        if let Err(e) = self.accept_waker.wake() {
            eprintln!("hotstash: cannot wake the acceptor for shutdown: {e}");
        }
        // The executor is gone only if it panicked; nothing left to stop.
        let _ = self.shared.jobs.send(Job::Stop);
        for thread in self.threads.drain(..) {
            if thread.join().is_err() {
                eprintln!("hotstash: a server thread panicked");
            }
        }
        if let Some(executor) = self.executor.take() {
            let deadline = Instant::now() + STOP_GRACE;
            while !executor.is_finished() && Instant::now() < deadline {
                {
                    let busy = self.shared.executor_busy();
                    if *busy {
                        // NOTBUSY or UNKILLABLE: nothing more to do here.
                        let _ = self.shared.engine.exec_conn(&Cmd::ScriptKill);
                    }
                }
                std::thread::sleep(STOP_POLL);
            }
            if !executor.is_finished() {
                eprintln!("hotstash: exclusive work that cannot be stopped outlives the server");
            } else if executor.join().is_err() {
                eprintln!("hotstash: the executor thread panicked");
            }
        }
        // With the gate free, the link to a primary can finish what it is
        // applying and stop.
        self.shared.unfollow();
        self.shared.engine.set_hooks(None);
        // Connections that were still waiting are gone with their workers.
        let waiting: usize = self
            .shared
            .blockers()
            .drain()
            .map(|(_, line)| line.len())
            .sum();
        if waiting > 0 {
            self.shared.engine.key_waiters().store(0, SeqCst);
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}
