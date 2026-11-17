//! The engine: shards, the per-shard lock protocol and the execution gate.
//!
//! Two kinds of callers exist.
//!
//! Event-loop workers go through `submit`, which never waits (rule 1): it
//! tries the shard lock, and when the lock is busy the command becomes a
//! task on that shard's queue and the connection is parked. Whoever holds a
//! shard lock drains a bounded batch of that queue before unlocking
//! (rule 2), and checks the queue once more after unlocking so no task is
//! stranded.
//!
//! Embedded sessions go through `execute_blocking`: they are application
//! threads and may sleep on a lock, so they never create tasks.

use crate::acl::{Acl, User};
use crate::clock::Clock;
use crate::cluster;
use crate::cmd::{Bytes, Class, ClientCmd, Cmd, Cond, LogKind, PubSubQuery, SlowLogCmd};
use crate::cmdtable;
use crate::gate::{ExclusiveGuard, Gate};
use crate::glob::glob_match;
use crate::lazyfree::{LazyFree, Reason};
use crate::mem;
use crate::notify;
use crate::ops::{Aggregate, BitOp, KeyOp, MultiOp, ObjectSub, SetOp, XIds};
use crate::persist::Fsync;
use crate::resp::Reply;
use crate::script::Scripts;
use crate::shard::{HashScan, Policy, Prefetch, Shard, Versions, NOT_HLL, WRONGTYPE};
use crate::sort;
use crate::stream::{IdSpec, XOp};
use crate::zset::format_score;
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone)]
pub struct Config {
    /// Shards per database.
    pub shards: usize,
    /// Databases SELECT can choose from, numbered from 0.
    pub databases: usize,
    /// Route `a{tag}b` by `tag`, as Redis Cluster does.
    pub hash_tags: bool,
    /// Queue tasks a lock holder runs before it must unlock (rule 2).
    pub drain_batch: usize,
    /// Elements KEYS and HGETALL visit per lock hold (rule 4).
    pub scan_chunk: usize,
    /// A multi-key command that found a shard busy this many times stops
    /// chasing locks and takes the exclusive side of the gate instead.
    pub multi_escalate_after: u32,
    /// Gate participants: workers plus embedded sessions.
    pub max_slots: usize,
    pub lua_memory_limit: usize,
    /// Heap bytes the process may hold before keys are evicted; 0 for no
    /// limit. Needs `mem::CountingAlloc`. Changed at run time by CONFIG SET.
    pub maxmemory: usize,
    pub maxmemory_policy: Policy,
    /// Databases that are never written to the data file: a cache beside
    /// the databases that are kept. Under `appendfsync always` their
    /// writes do not wait for the disk. Numbers past `databases` are
    /// ignored.
    pub memory_dbs: Vec<usize>,
    /// Answer as a one-node Redis Cluster that serves every slot: keys of
    /// one command must share a slot, and a slot's keys share a shard.
    pub cluster: bool,
    /// `enable-debug-command`: who may run DEBUG.
    pub debug_command: Protected,
    /// `dir` and `dbfilename`: the RDB file SAVE and BGSAVE write, as
    /// Redis does. `None` for none.
    pub rdb_file: Option<std::path::PathBuf>,
}

/// Who may run a command Valkey protects, such as DEBUG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protected {
    #[default]
    No,
    Yes,
    /// Connections from this machine; an embedded session is one.
    Local,
}

impl Protected {
    pub fn from_name(name: &str) -> Option<Protected> {
        match name {
            "no" => Some(Protected::No),
            "yes" => Some(Protected::Yes),
            "local" => Some(Protected::Local),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Protected::No => "no",
            Protected::Yes => "yes",
            Protected::Local => "local",
        }
    }
}

/// What DEBUG answers while `enable-debug-command` keeps it out.
pub const DEBUG_REFUSED: &str =
    "ERR DEBUG command not allowed. If the enable-debug-command option \
    is set to \"local\", you can run it from a local connection, otherwise you need to set this \
    option in the configuration file, and then restart the server.";

impl Default for Config {
    fn default() -> Self {
        Config {
            shards: 64,
            databases: 16,
            hash_tags: false,
            drain_batch: 32,
            scan_chunk: 256,
            multi_escalate_after: 8,
            max_slots: 1024,
            lua_memory_limit: 64 << 20,
            maxmemory: 0,
            maxmemory_policy: Policy::NoEviction,
            memory_dbs: Vec::new(),
            cluster: false,
            debug_command: Protected::No,
            rdb_file: None,
        }
    }
}

/// The Redis version whose commands and replies this follows. Clients and
/// frameworks read it to decide what they may send.
pub const COMPAT_VERSION: &str = "7.4.0";

/// Opaque to the engine; the server packs worker, connection and generation.
pub type Ticket = u128;

/// Asks the writer of the data file to bring it up to date and waits
/// until that is on disk.
/// What is called, on another thread, once a sync that was asked for
/// has been done or has failed.
pub type SyncDone = Box<dyn FnOnce(Result<(), String>) + Send>;
pub(crate) type Syncer = Arc<dyn Fn(SyncDone) + Send + Sync>;

/// How the engine reaches the event loops. All methods may be called from
/// any thread and must not block.
pub trait Hooks: Send + Sync {
    /// A parked command finished.
    fn complete(&self, ticket: Ticket, reply: Reply);
    /// Some shard queue needs a worker to call `service`.
    fn wake_any(&self);
    /// The exclusive side of the gate was released.
    fn gate_opened(&self);
    /// A parked multi-key command must run on the exclusive side.
    fn escalate(&self, cmd: Cmd, ticket: Ticket);
    /// Hand a message to the subscribers of `channel`; returns how many.
    fn publish(&self, _channel: &[u8], _payload: &[u8]) -> usize {
        0
    }
    /// The same for SPUBLISH and the subscribers of SSUBSCRIBE.
    fn spublish(&self, _channel: &[u8], _payload: &[u8]) -> usize {
        0
    }
    /// This key changed while `Engine::key_waiters` is not zero. Called
    /// with the key's shard locked.
    fn key_changed(&self, _hash: u64, _key: &[u8]) {}
    /// Every key may have changed (FLUSHALL, FLUSHDB, SWAPDB), with
    /// connections waiting.
    fn keys_flushed(&self) {}
    /// The server's sections of INFO.
    fn info(&self) -> String {
        String::new()
    }
    /// The replication stream grew, and a sender asked to hear of it
    /// (`Engine::repl_want_wake`). May be called with shards locked.
    fn replicate(&self) {}
    /// The lines of `# Replication` the server knows: the role and the
    /// links to other nodes. The engine adds those of the stream.
    fn replication(&self) -> Option<String> {
        None
    }
    /// Answer PUBSUB; `None` when there are no subscribers to ask about.
    fn pubsub(&self, _query: &PubSubQuery) -> Option<Reply> {
        None
    }
}

#[derive(Default)]
struct Stats {
    deferred: AtomicU64,
    multi_deferrals: AtomicU64,
    multi_escalations: AtomicU64,
    exclusive: AtomicU64,
    kicks: AtomicU64,
    tasks_run: AtomicU64,
    evicted: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Single-key commands that found their shard busy and were queued.
    pub deferred: u64,
    /// Times a multi-key command found one of its shards busy.
    pub multi_deferrals: u64,
    pub multi_escalations: u64,
    pub exclusive: u64,
    pub kicks: u64,
    /// Queue tasks executed by a lock holder on behalf of someone else.
    pub tasks_run: u64,
    /// Keys removed to stay under `maxmemory`.
    pub evicted: u64,
}

thread_local! {
    /// The database of the command this thread is running. Every database
    /// owns `Config::shards` consecutive shards, so the database decides
    /// where `Engine::route` sends a key.
    static DB: Cell<usize> = const { Cell::new(0) };
    /// The ticket of the connection whose command this thread is running
    /// (0: none that is known).
    static RUNNING: Cell<Ticket> = const { Cell::new(0) };
}

/// Restores the previous ticket when dropped.
struct TicketScope(Ticket);

impl Drop for TicketScope {
    fn drop(&mut self) {
        RUNNING.with(|cell| cell.set(self.0));
    }
}

fn enter_ticket(ticket: Ticket) -> TicketScope {
    TicketScope(RUNNING.with(|cell| cell.replace(ticket)))
}

/// The ticket of the command this thread is running, for a hook that
/// wants to know who changed a key; 0 when nobody is known.
pub fn running() -> Ticket {
    RUNNING.with(Cell::get)
}

/// Run `f` as the work of the connection behind `ticket`.
pub fn run_as<R>(ticket: Ticket, f: impl FnOnce() -> R) -> R {
    let _scope = enter_ticket(ticket);
    f()
}

/// Puts the previous database back when dropped.
struct DbScope(usize);

impl Drop for DbScope {
    fn drop(&mut self) {
        DB.with(|db| db.set(self.0));
    }
}

fn enter_db(db: usize) -> DbScope {
    DbScope(DB.with(|cell| cell.replace(db)))
}

fn current_db() -> usize {
    DB.with(Cell::get)
}

struct ShardSlot {
    data: Mutex<Shard>,
    queue: SegQueue<Task>,
    /// Set while this shard's index sits in `service_q`.
    kicked: AtomicBool,
    /// Key count as of the last unlock, for DBSIZE without locking.
    len: AtomicUsize,
    /// `Shard::stats` as of the last unlock, for INFO without locking.
    stats: [AtomicU64; 3],
}

impl ShardSlot {
    /// What INFO reads of the shard, as it is now.
    fn publish(&self, shard: &Shard) {
        self.len.store(shard.len(), Ordering::Relaxed);
        for (at, n) in self.stats.iter().zip(shard.stats()) {
            at.store(n, Ordering::Relaxed);
        }
    }
}

struct Task {
    ticket: Ticket,
    /// Times a multi-key command has been bounced off a busy shard.
    deferrals: u32,
    kind: Kind,
}

enum Kind {
    Cmd(Cmd),
    /// KEYS in progress on the shard whose queue holds the task.
    Keys {
        pattern: Vec<u8>,
        cursor: u64,
        out: Vec<Reply>,
    },
    Hgetall {
        key: Vec<u8>,
        cursor: u64,
        out: Vec<Reply>,
    },
    /// SCAN in progress on the shard whose queue holds the task.
    Scan(ScanState),
}

/// The low bits of a SCAN cursor name the shard, the rest is the cursor
/// inside that shard. `Engine::new` keeps the shard count within these bits.
const SCAN_SHARD_BITS: u32 = 16;

const MAX_DATABASES: usize = 256;

/// Keys `Engine::prefetch` loads ahead at once: enough for the misses of a
/// pipeline of 16 to overlap, as Valkey's default batch of 16.
pub const PREFETCH_KEYS: usize = 16;

/// What became of a command handed to `Engine::submit_into`.
#[derive(Debug, PartialEq)]
pub enum Submitted {
    /// Answered with this reply.
    Reply(Reply),
    /// Answered: the reply is at the end of the caller's buffer.
    Written,
    /// Parked: the reply arrives through `Hooks::complete` (or the command
    /// through `Hooks::escalate`).
    Parked,
}

struct ScanState {
    pattern: Option<Vec<u8>>,
    ty: Option<Vec<u8>>,
    /// Elements this call may still visit.
    left: usize,
    /// Cursor inside the current shard.
    cursor: u64,
    out: Vec<Reply>,
}

enum ScanStep {
    Done(Reply),
    /// More to visit on the same shard, under a later lock hold (rule 4).
    Same,
    Shard(usize),
}

fn scan_reply(cursor: u64, items: Vec<Reply>) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(cursor.to_string().into_bytes()),
        Reply::Array(items),
    ])
}

impl Task {
    fn new(ticket: Ticket, cmd: Cmd) -> Task {
        Task {
            ticket,
            deferrals: 0,
            kind: Kind::Cmd(cmd),
        }
    }
}

/// Outcome of running one task under a shard lock.
enum Step {
    Done(Ticket, Reply),
    /// Back on the same shard's queue, after the tasks already waiting.
    Requeue(Task),
    /// On to another shard's queue.
    Move(usize, Task),
    Escalate(Ticket, Cmd),
}

/// Outcomes collected under a lock, acted on after unlocking.
#[derive(Default)]
struct Batch {
    done: Vec<(Ticket, Reply)>,
    requeue: Vec<Task>,
    moves: Vec<(usize, Task)>,
    esc: Vec<(Ticket, Cmd)>,
}

impl Batch {
    fn absorb(&mut self, step: Step) {
        match step {
            Step::Done(ticket, reply) => self.done.push((ticket, reply)),
            Step::Requeue(task) => self.requeue.push(task),
            Step::Move(idx, task) => self.moves.push((idx, task)),
            Step::Escalate(ticket, cmd) => self.esc.push((ticket, cmd)),
        }
    }
}

/// How `apply` reaches a shard. The three implementations differ only in
/// how exclusive access was obtained (rule 6).
trait Access {
    fn with<R>(&mut self, idx: usize, f: impl FnOnce(&mut Shard) -> R) -> R;
}

struct One<'a> {
    idx: usize,
    shard: &'a mut Shard,
}

impl Access for One<'_> {
    fn with<R>(&mut self, idx: usize, f: impl FnOnce(&mut Shard) -> R) -> R {
        debug_assert_eq!(idx, self.idx);
        f(self.shard)
    }
}

struct Many<'a, 'g> {
    held: Option<(usize, &'a mut Shard)>,
    guards: &'a mut [(usize, MutexGuard<'g, Shard>)],
}

impl Access for Many<'_, '_> {
    fn with<R>(&mut self, idx: usize, f: impl FnOnce(&mut Shard) -> R) -> R {
        if let Some((held, shard)) = &mut self.held {
            if *held == idx {
                return f(shard);
            }
        }
        for (i, guard) in self.guards.iter_mut() {
            if *i == idx {
                return f(guard);
            }
        }
        unreachable!("shard {idx} is not locked by this command")
    }
}

/// Access from the exclusive side of the gate: nobody else is in the
/// keyspace, so each shard lock is free.
struct Excl<'a> {
    engine: &'a Engine,
}

impl Access for Excl<'_> {
    fn with<R>(&mut self, idx: usize, f: impl FnOnce(&mut Shard) -> R) -> R {
        let slot = &self.engine.shards[idx];
        let mut guard = slot.data.lock();
        let result = f(&mut guard);
        slot.publish(&guard);
        result
    }
}

pub struct Engine {
    cfg: Config,
    shards: Box<[CachePadded<ShardSlot>]>,
    /// Shards whose queue needs a visit from a worker.
    service_q: SegQueue<usize>,
    gate: Gate,
    pub(crate) scripts: Scripts,
    hooks: RwLock<Option<Arc<dyn Hooks>>>,
    stats: Stats,
    clock: Arc<Clock>,
    /// Per-key-stripe write counters, for WATCH.
    versions: Arc<Versions>,
    started: std::time::Instant,
    /// Where `expire_tick` continues.
    sweep_next: AtomicUsize,
    random_seq: AtomicUsize,
    /// One more than the highest database a key was ever routed to. The
    /// shards beyond it are empty, so background work passes them by.
    used_dbs: AtomicUsize,
    maxmemory: AtomicUsize,
    policy: AtomicU8,
    /// INFO's `run_id`: 40 hexadecimal digits, new on each start, which a
    /// Sentinel uses to tell a restarted node.
    run_id: String,
    /// `replica-priority`: what a Sentinel weighs when it chooses a new
    /// primary; 0 never.
    replica_priority: AtomicU64,
    /// `replica-announced`: whether a Sentinel lists this replica.
    replica_announced: AtomicBool,
    /// Users, passwords and what each user may do.
    acl: Acl,
    /// Counts FLUSHALLs, so the data file learns of each one.
    flush_gen: AtomicU64,
    persist_on: AtomicBool,
    bgsave: AtomicBool,
    /// Unix seconds of the last successful write to disk. Shared with the
    /// thread that writes the RDB file.
    last_save: Arc<AtomicU64>,
    /// `notify-keyspace-events` as it was set; `versions` has the flags
    /// while they amount to something.
    notify_cfg: AtomicU32,
    persist_error: Mutex<Option<String>>,
    /// The writes of the RDB file (`rdbfile.rs`).
    rdb_saves: Arc<crate::rdbfile::RdbSaves>,
    /// The replication stream and its backlog (`repl.rs`).
    repl: crate::repl::Repl,
    /// `masteruser` and `masterauth`: how a replica logs in at its primary.
    master_auth: Mutex<(Option<Bytes>, Option<Bytes>)>,
    /// Per `LogKind`: the least a command logs at (microseconds, or
    /// bytes), negative for never: `slowlog-log-slower-than`,
    /// `commandlog-request-larger-than`, `commandlog-reply-larger-than`.
    log_limit: [AtomicI64; 3],
    /// Per `LogKind`: the most entries kept, `slowlog-max-len` and the
    /// like.
    log_max: [AtomicUsize; 3],
    logs: [Mutex<SlowLog>; 3],
    /// `appendfsync`, as a `persist::Fsync`.
    fsync: AtomicU8,
    /// Set while a writer keeps the data file.
    syncer: RwLock<Option<Syncer>>,
    /// `timeout`: seconds a connection may stay silent; 0 for ever.
    client_timeout: AtomicU64,
    /// Frees large values off the thread that removed them.
    lazy: Arc<LazyFree>,
    /// Per database: kept in memory only (`Config::memory_dbs`).
    memory: Box<[bool]>,
    /// This node of a cluster, in cluster mode.
    node: Option<cluster::Node>,
    /// DEBUG SET-ACTIVE-EXPIRE 0 stops the sweep of keys whose time is up.
    active_expire: AtomicBool,
    /// The `bf.*` settings of BF.* commands.
    bloom: crate::bloom::Config,
    /// The `json.*` settings of JSON.* commands.
    json: crate::json::Config,
}

/// One command that took long (SLOWLOG).
struct SlowEntry {
    id: u64,
    /// Unix seconds when it ran.
    at: u64,
    micros: u64,
    args: Vec<Bytes>,
    addr: String,
    name: Bytes,
}

#[derive(Default)]
struct SlowLog {
    next_id: u64,
    /// The newest first.
    entries: VecDeque<SlowEntry>,
}

/// `slowlog-log-slower-than` and `slowlog-max-len` as Redis ships them.
const SLOW_DEFAULT_MICROS: i64 = 10_000;
const SLOW_DEFAULT_LEN: usize = 128;
/// `commandlog-request-larger-than` and `commandlog-reply-larger-than`.
const LARGE_DEFAULT_BYTES: i64 = 1 << 20;
/// The settings of each `LogKind`: its threshold, then its length.
const LOG_SETTINGS: [(&str, &str); 3] = [
    ("slowlog-log-slower-than", "slowlog-max-len"),
    (
        "commandlog-request-larger-than",
        "commandlog-large-request-max-len",
    ),
    (
        "commandlog-reply-larger-than",
        "commandlog-large-reply-max-len",
    ),
];
/// Valkey's names for the settings of the slow log.
const SLOW_ALIASES: (&str, &str) = (
    "commandlog-execution-slower-than",
    "commandlog-slow-execution-max-len",
);
/// The log is memory nobody asked for: `slowlog-max-len` stops here.
const SLOW_MAX_LEN: usize = 1 << 16;
/// An entry keeps this many arguments, each cut to this many bytes.
pub const SLOW_ARGS: usize = 32;
pub const SLOW_ARG_BYTES: usize = 128;

pub(crate) const DB_RANGE: &str = "ERR DB index is out of range";

fn wrongtype() -> Reply {
    Reply::err(WRONGTYPE)
}

/// Leaves the shared side of the gate, also when the holder panics.
struct Leave<'a>(&'a Gate, usize);

impl Drop for Leave<'_> {
    fn drop(&mut self) {
        self.0.leave(self.1);
    }
}

fn random_key(shard: &mut Shard) -> Reply {
    shard.random_key().map_or(Reply::Nil, Reply::Bulk)
}

/// A size as Redis configuration writes it: `1048576`, `100mb`, `1g`, `64k`.
pub fn parse_bytes(raw: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(raw).ok()?.to_ascii_lowercase();
    let digits = text.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit: usize = match &text[digits.len()..] {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1 << 10,
        "m" => 1_000_000,
        "mb" => 1 << 20,
        "g" => 1_000_000_000,
        "gb" => 1 << 30,
        _ => return None,
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<usize>().ok()?.checked_mul(unit)
}

/// Which log a setting of the command logs is for, and whether it is its
/// length (or else its threshold).
fn log_setting(name: &[u8]) -> Option<(usize, bool)> {
    let is = |setting: &str| setting.as_bytes() == name;
    if is(SLOW_ALIASES.0) {
        return Some((0, false));
    }
    if is(SLOW_ALIASES.1) {
        return Some((0, true));
    }
    LOG_SETTINGS
        .iter()
        .enumerate()
        .find_map(|(kind, (limit, len))| (is(limit) || is(len)).then_some((kind, is(len))))
}

/// The part of the key between the first `{` and the next `}`, if non-empty.
pub(crate) fn hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = memchr::memchr(b'{', key)?;
    let len = memchr::memchr(b'}', &key[open + 1..])?;
    (len > 0).then(|| &key[open + 1..open + 1 + len])
}

/// Closes the store-then-load race between "push a task, then try the
/// lock" and "unlock, then look at the queue". On x86-64 the lock and queue
/// operations are already full barriers.
#[inline]
fn dekker_fence() {
    #[cfg(not(target_arch = "x86_64"))]
    std::sync::atomic::fence(Ordering::SeqCst);
}

impl Engine {
    pub fn new(mut cfg: Config) -> Engine {
        cfg.shards = cfg.shards.clamp(1, 1 << 16);
        cfg.databases = cfg.databases.clamp(1, MAX_DATABASES);
        cfg.drain_batch = cfg.drain_batch.max(1);
        cfg.scan_chunk = cfg.scan_chunk.max(1);
        let clock = Arc::new(Clock::default());
        let versions = Arc::new(Versions::default());
        let lazy = Arc::new(LazyFree::default());
        let shards = (0..cfg.shards * cfg.databases)
            .map(|_| {
                CachePadded::new(ShardSlot {
                    data: Mutex::new(Shard::new(clock.clone(), versions.clone(), lazy.clone())),
                    queue: SegQueue::new(),
                    kicked: AtomicBool::new(false),
                    len: AtomicUsize::new(0),
                    stats: Default::default(),
                })
            })
            .collect();
        Engine {
            shards,
            service_q: SegQueue::new(),
            gate: Gate::new(cfg.max_slots),
            scripts: Scripts::new(cfg.lua_memory_limit),
            hooks: RwLock::new(None),
            stats: Stats::default(),
            clock,
            versions,
            started: std::time::Instant::now(),
            sweep_next: AtomicUsize::new(0),
            random_seq: AtomicUsize::new(0),
            used_dbs: AtomicUsize::new(1),
            maxmemory: AtomicUsize::new(cfg.maxmemory),
            policy: AtomicU8::new(cfg.maxmemory_policy as u8),
            run_id: crate::repl::new_id(),
            replica_priority: AtomicU64::new(DEFAULT_REPLICA_PRIORITY),
            replica_announced: AtomicBool::new(true),
            acl: Acl::new(),
            flush_gen: AtomicU64::new(0),
            persist_on: AtomicBool::new(false),
            bgsave: AtomicBool::new(false),
            last_save: Arc::new(AtomicU64::new(0)),
            notify_cfg: AtomicU32::new(0),
            persist_error: Mutex::new(None),
            rdb_saves: Arc::default(),
            repl: crate::repl::Repl::default(),
            master_auth: Mutex::new((None, None)),
            log_limit: [
                AtomicI64::new(SLOW_DEFAULT_MICROS),
                AtomicI64::new(LARGE_DEFAULT_BYTES),
                AtomicI64::new(LARGE_DEFAULT_BYTES),
            ],
            log_max: std::array::from_fn(|_| AtomicUsize::new(SLOW_DEFAULT_LEN)),
            logs: Default::default(),
            fsync: AtomicU8::new(Fsync::Interval as u8),
            syncer: RwLock::new(None),
            client_timeout: AtomicU64::new(0),
            lazy,
            memory: (0..cfg.databases)
                .map(|db| cfg.memory_dbs.contains(&db))
                .collect(),
            node: cfg.cluster.then(cluster::Node::new),
            active_expire: AtomicBool::new(true),
            bloom: crate::bloom::Config::default(),
            json: crate::json::Config::default(),
            cfg,
        }
    }

    // Persistence (`persist.rs`).

    /// Make every shard of a kept database remember which keys change.
    pub(crate) fn persist_enable(&self) {
        for (idx, slot) in self.shards.iter().enumerate() {
            if !self.memory[idx / self.cfg.shards] {
                slot.data.lock().track_changes();
            }
        }
        self.persist_on.store(true, Ordering::SeqCst);
        self.last_save
            .store(crate::clock::unix_ms() / 1000, Ordering::Relaxed);
    }

    /// The writer is gone: stop collecting changes nobody will write.
    pub(crate) fn persist_disable(&self) {
        self.persist_on.store(false, Ordering::SeqCst);
        for slot in self.shards.iter() {
            slot.data.lock().untrack_changes();
        }
    }

    pub(crate) fn flush_gen(&self) -> u64 {
        self.flush_gen.load(Ordering::SeqCst)
    }

    /// BGSAVE for the data file: the persistence thread rewrites it.
    /// `false` without one.
    pub fn rewrite_data_file(&self) -> bool {
        let on = self.persist_on.load(Ordering::SeqCst);
        if on {
            self.bgsave.store(true, Ordering::SeqCst);
        }
        on
    }

    pub(crate) fn take_bgsave(&self) -> bool {
        self.bgsave.swap(false, Ordering::SeqCst)
    }

    /// How the last write to disk went.
    pub(crate) fn persist_result(&self, error: Option<String>) {
        if error.is_none() {
            self.last_save
                .store(crate::clock::unix_ms() / 1000, Ordering::Relaxed);
        }
        *self.persist_error.lock() = error;
    }

    /// When what is written to the data file is forced to the disk.
    pub fn fsync(&self) -> Fsync {
        Fsync::from_u8(self.fsync.load(Ordering::Relaxed))
    }

    pub(crate) fn set_fsync(&self, mode: Fsync) {
        self.fsync.store(mode as u8, Ordering::Relaxed);
    }

    pub(crate) fn set_syncer(&self, syncer: Option<Syncer>) {
        *self.syncer.write() = syncer;
    }

    /// Whether a change `cmd` makes from database `db` has to be on disk
    /// before it is answered: `appendfsync always`, a data file, and a
    /// database that is kept. Only a key command confined to `db` skips
    /// the wait in a memory database: one that may reach into others
    /// (MOVE, SWAPDB, EXEC, a script) or changes what is not a key (ACL,
    /// CONFIG SET) waits whatever `db` is.
    pub fn sync_before_reply(&self, db: usize, cmd: &Cmd) -> bool {
        self.fsync() == Fsync::Always
            && self.persist_on.load(Ordering::SeqCst)
            && (!self.memory.get(db).copied().unwrap_or(false) || cmd.class() != Class::Shared)
    }

    /// Frees large values off the thread that removed them, and its
    /// `lazyfree-lazy-*` settings.
    pub fn lazy_free(&self) -> &LazyFree {
        &self.lazy
    }

    /// Whether changes go to a data file.
    pub fn has_data_file(&self) -> bool {
        self.persist_on.load(Ordering::SeqCst)
    }

    /// Write every change made so far to the data file and wait until it
    /// is on disk. `None` without a data file. Sleeps: not for a thread
    /// that is inside the gate.
    pub fn sync_now(&self) -> Option<Result<(), String>> {
        let syncer = self.syncer.read().clone()?;
        let (tx, rx) = std::sync::mpsc::channel();
        syncer(Box::new(move |result| {
            // The caller may be gone; nobody is left to tell then.
            let _ = tx.send(result);
        }));
        let gone = || Err("the persistence thread has stopped".to_string());
        Some(rx.recv().unwrap_or_else(|_| gone()))
    }

    /// As `sync_now`, without waiting: `done` hears how it went. Without
    /// a data file there is nothing to wait for and it hears so at once.
    pub fn sync_then(&self, done: SyncDone) {
        let syncer = self.syncer.read().clone();
        match syncer {
            Some(syncer) => syncer(done),
            None => done(Ok(())),
        }
    }

    /// Seconds a connection may send nothing before a server closes it; 0
    /// for no limit (`timeout`).
    pub fn client_timeout(&self) -> u64 {
        self.client_timeout.load(Ordering::Relaxed)
    }

    pub fn set_client_timeout(&self, secs: u64) {
        self.client_timeout.store(secs, Ordering::Relaxed);
    }

    /// What the libraries of functions and the users were last changed
    /// at: a pair that differs after any change to either.
    pub(crate) fn meta_gens(&self) -> (u64, u64) {
        (self.scripts.library_gen(), self.acl.generation())
    }

    pub(crate) fn library_sources(&self) -> HashMap<String, Arc<[u8]>> {
        self.scripts.library_sources()
    }

    /// Recovery: apply one record of the data file.
    pub(crate) fn load_record(
        &self,
        record: crate::persist::Record,
        db: &mut usize,
    ) -> Result<(), String> {
        use crate::persist::Record;
        let (key, entry) = match record {
            Record::Db(n) => {
                *db = n;
                return Ok(());
            }
            Record::Wipe => {
                for slot in self.shards.iter() {
                    slot.data.lock().clear(false);
                    slot.len.store(0, Ordering::Relaxed);
                }
                return Ok(());
            }
            Record::LibPut(code) => return self.scripts.restore_library(&code),
            Record::LibDel(name) => {
                self.scripts.forget_library(&name);
                return Ok(());
            }
            Record::UserPut(name, rules) => return self.acl.restore_user(&name, &rules),
            Record::UserDel(name) => {
                self.acl.forget_user(&name);
                return Ok(());
            }
            // Left from a start when the database was kept: it is not any
            // more, so it starts empty, and a rewrite drops the rest.
            Record::Del(_) | Record::Put(..) if self.memory[*db] => {
                self.bgsave.store(true, Ordering::SeqCst);
                return Ok(());
            }
            Record::Del(key) => (key, None),
            Record::Put(key, entry) => (key, Some(entry)),
        };
        let (h, idx) = self.route_in(*db, &key);
        let slot = &self.shards[idx];
        let mut shard = slot.data.lock();
        shard.load(h, &key, entry);
        slot.len.store(shard.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Append the state of every key changed since the last call to `out`.
    /// The caller owns gate slot `slot` and may sleep. `logged_gen` is the
    /// FLUSHALL generation the caller's file reflects.
    pub(crate) fn persist_collect(&self, slot: usize, logged_gen: &mut u64, out: &mut Vec<u8>) {
        self.gate.enter_blocking(slot);
        let _leave = Leave(&self.gate, slot);
        // FLUSHALL needs the other side of the gate, so within this round
        // it is either wholly before or wholly after.
        let gen = self.flush_gen();
        if gen != *logged_gen {
            crate::persist::encode_wipe(out);
            *logged_gen = gen;
        }
        // The buffer may hold records of an earlier round, so the first
        // database is named even when it is 0.
        let mut named = None;
        for idx in 0..self.used_shards() {
            let db = idx / self.cfg.shards;
            let before = out.len();
            if named != Some(db) {
                crate::persist::encode_db(out, db);
            }
            let tagged = out.len();
            self.with_locked(idx, |shard| shard.drain_changes(out));
            if out.len() == tagged {
                out.truncate(before);
            } else {
                named = Some(db);
            }
        }
    }

    /// How many shards a dump of the keyspace has to visit.
    pub(crate) fn shard_count(&self) -> usize {
        self.used_shards()
    }

    /// Append a chunk of shard `idx` to `out`, for a rewrite that started
    /// at FLUSHALL generation `gen`. `None` when a FLUSHALL has run since.
    pub(crate) fn persist_dump(
        &self,
        slot: usize,
        gen: u64,
        idx: usize,
        cursor: u64,
        budget: usize,
        out: &mut Vec<u8>,
    ) -> Option<u64> {
        self.gate.enter_blocking(slot);
        let _leave = Leave(&self.gate, slot);
        if self.flush_gen() != gen {
            return None;
        }
        if self.memory[idx / self.cfg.shards] {
            return Some(0);
        }
        // Frames are cut anywhere, so every chunk names its database.
        let before = out.len();
        crate::persist::encode_db(out, idx / self.cfg.shards);
        let tagged = out.len();
        let next = self.with_locked(idx, |shard| shard.dump_chunk(cursor, budget, out));
        if out.len() == tagged {
            out.truncate(before);
        }
        Some(next)
    }

    // RDB files (`rdbfile.rs`). The callers hold the exclusive side of the
    // gate, so a file is one moment of the whole node.

    /// Run `f` on every shard that can hold keys, with its database.
    pub(crate) fn rdb_each_shard(&self, mut f: impl FnMut(usize, &Shard)) {
        for idx in 0..self.used_shards() {
            self.with_locked(idx, |shard| f(idx / self.cfg.shards, shard));
        }
    }

    /// Whether no database holds a key, expired or not.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|slot| slot.data.lock().is_empty())
    }

    /// Store a key read from a file in database `db`. `false` when the key
    /// is there already.
    pub(crate) fn rdb_put(&self, db: usize, key: &[u8], entry: crate::shard::Entry) -> bool {
        let (h, idx) = self.route_in(db, key);
        self.with_locked(idx, |shard| shard.import(h, key, entry))
    }

    /// Load a library of an RDB file; its name. One of that name must not
    /// be there already.
    pub(crate) fn rdb_library(&self, code: &[u8]) -> Result<String, String> {
        self.scripts.add_library(code)
    }

    /// Take back a library `rdb_library` loaded.
    pub(crate) fn rdb_unload_library(&self, name: &str) {
        self.scripts.forget_library(name.as_bytes());
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    pub(crate) fn rdb_saves(&self) -> &Arc<crate::rdbfile::RdbSaves> {
        &self.rdb_saves
    }

    /// LASTSAVE's clock, for the thread that writes the RDB file.
    pub(crate) fn last_save_clock(&self) -> &Arc<AtomicU64> {
        &self.last_save
    }

    /// The write counter WATCH remembers for `key`. Lock-free. It changes
    /// whenever the key is written, deleted or found expired; keys sharing
    /// a stripe share a counter, so it can also change for no reason.
    pub fn watch_version(&self, key: &[u8]) -> u64 {
        self.versions.get(xxh3_64(key))
    }

    /// Move the engine's clock forward, so tests need not sleep.
    pub fn advance_clock(&self, ms: u64) {
        self.clock.advance(ms);
    }

    /// Remove every expired key now. Returns how many were removed.
    pub fn sweep_expired(&self) -> usize {
        (0..self.used_shards())
            .map(|idx| {
                let (removed, notes) =
                    self.with_locked(idx, |shard| (shard.sweep_all(), shard.take_notes()));
                self.publish_notes(idx / self.cfg.shards, notes);
                removed
            })
            .sum()
    }

    /// Background expiry: give a bounded number of shards the chance to
    /// drop some expired keys. Never waits for a lock. It runs outside the
    /// gate, which is sound because an expired key already reads as
    /// missing: removing it changes no command's result.
    pub fn expire_tick(&self) {
        if !self.active_expire.load(Ordering::Relaxed) {
            return;
        }
        const SHARDS_PER_TICK: usize = 256;
        // Most databases are empty most of the time: passing over their
        // shards is cheap, so they do not count towards the tick.
        const LOOKS_PER_TICK: usize = 8192;
        let mut swept = 0;
        let used = self.used_shards();
        for _ in 0..used.min(LOOKS_PER_TICK) {
            if swept == SHARDS_PER_TICK {
                break;
            }
            let idx = self.sweep_next.fetch_add(1, Ordering::Relaxed) % used;
            if self.shards[idx].len.load(Ordering::Relaxed) == 0 {
                continue;
            }
            swept += 1;
            if let Some(mut guard) = self.shards[idx].data.try_lock() {
                // Queued tasks stay queued: running them needs the gate.
                guard.sweep_due();
                let notes = guard.take_notes();
                self.release(idx, guard);
                self.publish_notes(idx / self.cfg.shards, notes);
            }
        }
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn gate(&self) -> &Gate {
        &self.gate
    }

    /// Install or remove the event-loop hooks. Without hooks, completions
    /// of parked commands are dropped, so remove them only after the event
    /// loops have stopped.
    pub fn set_hooks(&self, hooks: Option<Arc<dyn Hooks>>) {
        self.versions.set_hooks(hooks.clone());
        *self.hooks.write() = hooks;
    }

    /// How many clients wait for a key to change. While it is not zero,
    /// every change is reported to `Hooks::key_changed`.
    pub fn key_waiters(&self) -> &AtomicUsize {
        self.versions.waiting()
    }

    /// Install `hooks` unless some are installed already.
    pub fn attach_hooks(&self, hooks: Arc<dyn Hooks>) -> bool {
        let mut slot = self.hooks.write();
        if slot.is_some() {
            return false;
        }
        self.versions.set_hooks(Some(hooks.clone()));
        *slot = Some(hooks);
        true
    }

    fn hooks(&self) -> Option<Arc<dyn Hooks>> {
        self.hooks.read().clone()
    }

    pub fn stats(&self) -> StatsSnapshot {
        let s = &self.stats;
        StatsSnapshot {
            deferred: s.deferred.load(Ordering::Relaxed),
            multi_deferrals: s.multi_deferrals.load(Ordering::Relaxed),
            multi_escalations: s.multi_escalations.load(Ordering::Relaxed),
            exclusive: s.exclusive.load(Ordering::Relaxed),
            kicks: s.kicks.load(Ordering::Relaxed),
            tasks_run: s.tasks_run.load(Ordering::Relaxed),
            evicted: s.evicted.load(Ordering::Relaxed),
        }
    }

    /// Key hash and shard index. The shard comes from the high bits and the
    /// in-shard bucket from the low bits, so the two stay independent.
    pub fn route(&self, key: &[u8]) -> (u64, usize) {
        self.route_in(current_db(), key)
    }

    /// `route` for a key of database `db`.
    fn route_in(&self, db: usize, key: &[u8]) -> (u64, usize) {
        if db >= self.used_dbs.load(Ordering::Relaxed) {
            self.used_dbs.fetch_max(db + 1, Ordering::SeqCst);
        }
        self.locate(db, key)
    }

    /// `route_in` without noting that database `db` is in use.
    fn locate(&self, db: usize, key: &[u8]) -> (u64, usize) {
        let hash = xxh3_64(key);
        let local = if self.node.is_some() {
            self.slot_shard(cluster::key_slot(key))
        } else {
            let tag = self.cfg.hash_tags.then(|| hash_tag(key)).flatten();
            let routing = tag.map_or(hash, xxh3_64);
            (((routing >> 32) * self.cfg.shards as u64) >> 32) as usize
        };
        (hash, db * self.cfg.shards + local)
    }

    /// In cluster mode, the shard (within a database) of the keys of `slot`.
    fn slot_shard(&self, slot: u16) -> usize {
        usize::from(slot) * self.cfg.shards / usize::from(cluster::SLOTS)
    }

    /// This node, when the engine runs as a cluster.
    pub fn node(&self) -> Option<&cluster::Node> {
        self.node.as_ref()
    }

    /// The keys of the next commands of a pipeline in database `db`: start
    /// loading the lines their lookups will read, so that the cache misses
    /// of these lookups overlap instead of coming one after the other, as
    /// Valkey's `prefetchCommandQueueKeys` does. Only a hint: it changes
    /// nothing, and passes over a shard that is busy.
    pub fn prefetch(&self, db: usize, keys: &[&[u8]]) {
        let db = db.min(self.cfg.databases - 1);
        let mut spots = [(0, 0); PREFETCH_KEYS];
        let spots = &mut spots[..keys.len().min(PREFETCH_KEYS)];
        for (spot, key) in spots.iter_mut().zip(keys) {
            *spot = self.locate(db, key);
        }
        for step in [Prefetch::Bucket, Prefetch::Entry, Prefetch::Value] {
            for &(h, idx) in spots.iter() {
                if let Some(guard) = self.shards[idx].data.try_lock() {
                    guard.prefetch(step, h);
                    self.release(idx, guard);
                }
            }
        }
    }

    /// The shards that can hold keys.
    fn used_shards(&self) -> usize {
        self.used_dbs.load(Ordering::SeqCst) * self.cfg.shards
    }

    pub fn databases(&self) -> usize {
        self.cfg.databases
    }

    /// Run `f` with database `db` as the one commands on this thread use.
    /// Outside of it, a command belongs to database 0.
    pub fn with_db<R>(&self, db: usize, f: impl FnOnce() -> R) -> R {
        let _scope = enter_db(db.min(self.cfg.databases - 1));
        f()
    }

    /// The shards of the current database.
    fn db_shards(&self) -> Range<usize> {
        let first = current_db() * self.cfg.shards;
        first..first + self.cfg.shards
    }

    /// Keys in the current database, as of the last unlock of each shard.
    fn db_len(&self) -> usize {
        self.shards[self.db_shards()]
            .iter()
            .map(|s| s.len.load(Ordering::Relaxed))
            .sum()
    }

    /// Sorted, de-duplicated shards a keyed command touches.
    fn shard_set(&self, cmd: &Cmd) -> Vec<usize> {
        let mut set = Vec::new();
        cmd.for_each_key(|k| set.push(self.route(k).1));
        set.sort_unstable();
        set.dedup();
        set
    }

    /// Run a keyspace command. Every shard it touches is reachable
    /// through `a`.
    fn apply<A: Access>(&self, cmd: Cmd, a: &mut A) -> Reply {
        let cmd = match cmd {
            Cmd::In(db, inner) => {
                let _scope = enter_db(db.into());
                return self.apply(*inner, a);
            }
            cmd => cmd,
        };
        if self.versions.hash_ttls() != 0 {
            // Fields whose time has come go before the command looks.
            cmd.for_each_key(|k| {
                let (h, i) = self.route(k);
                a.with(i, |s| s.reap_fields(h, k));
            });
        }
        if self.repl.logging() {
            return self.run_logged(cmd, a);
        }
        if self.versions.notify() == 0 {
            return self.run(cmd, a);
        }
        self.run_noted(cmd, a)
    }

    /// `run`, and add what the command did to the replication stream while
    /// its shards are still held (`repl.rs`).
    fn run_logged<A: Access>(&self, cmd: Cmd, a: &mut A) -> Reply {
        use crate::repl::{Effect, Plan};
        let plan = Plan::of(&cmd);
        let shards = self.shard_set(&cmd);
        let reply = if self.versions.notify() == 0 {
            self.run(cmd, a)
        } else {
            self.run_noted(cmd, a)
        };
        let mut body = Vec::new();
        let mut touched = Vec::new();
        let mut own = Vec::new();
        for &i in &shards {
            if let Some(track) = a.with(i, Shard::take_repl) {
                // What went by itself goes before what the command did.
                crate::repl::encode_gone(&mut body, &track);
                touched.extend(track.touched.into_iter().map(|key| (i, key)));
                own.extend_from_slice(&track.own);
            }
        }
        if !touched.is_empty() {
            match plan {
                Plan::Same(command) => body.extend_from_slice(&command),
                Plan::Own => body.extend_from_slice(&own),
                Plan::SPop(key) => crate::repl::encode_spop(&mut body, &key, &reply),
                Plan::XAdd(before, fields) => {
                    crate::repl::encode_xadd(&mut body, &before, &fields, &reply);
                }
                Plan::State(effect) => self.encode_states(&mut body, touched, effect, a),
                // Changes of an earlier hold, found by a read: their effect.
                Plan::Quiet => self.encode_states(&mut body, touched, Effect::Value, a),
            }
        }
        self.repl_append(Some(current_db()), &body);
        reply
    }

    fn encode_states<A: Access>(
        &self,
        body: &mut Vec<u8>,
        mut touched: Vec<(usize, Bytes)>,
        effect: crate::repl::Effect,
        a: &mut A,
    ) {
        touched.sort_unstable();
        touched.dedup();
        for (i, key) in touched {
            a.with(i, |s| crate::repl::encode_state(body, s, &key, effect));
        }
    }

    /// What changed on shard `idx` outside of a command (keys whose time
    /// came, that made room) goes out, with the shard still held.
    fn log_stray(&self, idx: usize, shard: &Shard, track: crate::shard::ReplTrack) {
        let mut body = Vec::new();
        crate::repl::encode_gone(&mut body, &track);
        let mut touched = track.touched;
        touched.sort_unstable();
        touched.dedup();
        for key in touched {
            crate::repl::encode_state(&mut body, shard, &key, crate::repl::Effect::Value);
        }
        self.repl_append(Some(idx / self.cfg.shards), &body);
    }

    fn repl_append(&self, db: Option<usize>, body: &[u8]) {
        if !body.is_empty() && self.repl.append(db, body) {
            if let Some(hooks) = self.hooks() {
                hooks.replicate();
            }
        }
    }

    /// A command the exclusive side ran by hand goes out as `args`.
    fn repl_command(&self, db: Option<usize>, args: &[&[u8]]) {
        if self.repl.logging() {
            let mut body = Vec::new();
            crate::repl::command(&mut body, args);
            self.repl_append(db, &body);
        }
    }

    /// While the value lives, what the exclusive side writes goes out
    /// between MULTI and EXEC.
    fn repl_atomic(&self) -> Atomic<'_> {
        Atomic(self.repl.logging().then(|| {
            self.repl.open();
            self
        }))
    }

    /// The `# Replication` section of INFO: `head` holds what the server
    /// knows, the rest is the stream's.
    fn repl_section(&self, head: &str) -> String {
        let info = self.repl.info();
        format!(
            "# Replication\r\n{head}master_replid:{}\r\nmaster_replid2:{}\r\n\
             master_repl_offset:{}\r\nsecond_repl_offset:{}\r\nrepl_backlog_active:{}\r\n\
             repl_backlog_size:{}\r\nrepl_backlog_first_byte_offset:{}\r\n\
             repl_backlog_histlen:{}\r\n",
            info.replid,
            info.replid2,
            info.offset,
            info.second_offset,
            u8::from(info.active),
            info.size,
            info.first_byte,
            info.histlen,
        )
    }

    /// Keep the stream from now on, as the first replica needs. A replica
    /// keeps its primary's stream instead (`repl_feed`). Call with the
    /// exclusive side held.
    fn repl_start(&self) {
        self.repl.activate();
        if self.repl.logging() || self.versions.replica() {
            return;
        }
        for slot in self.shards.iter() {
            slot.data.lock().track_repl(true);
        }
        self.repl.set_logging(true);
    }

    /// Where the replication stream stands (INFO, ROLE).
    pub fn repl_info(&self) -> crate::repl::ReplInfo {
        self.repl.info()
    }

    /// A replica asks for the whole dataset: the RDB file of it, and where
    /// in the stream the replica goes on. From now on every change goes to
    /// the stream. Takes the exclusive side.
    pub fn repl_snapshot(&self) -> (Vec<u8>, crate::repl::SyncPoint) {
        let _excl = self.exclusive();
        self.repl_start();
        // Read before the file is made: only expiry runs outside the gate,
        // and the DELs it sends meanwhile are harmless to apply twice.
        let (point, db) = self.repl.sync_point(!self.versions.replica());
        let aux = [
            ("repl-stream-db", db.to_string()),
            ("repl-id", point.replid.clone()),
            ("repl-offset", point.offset.to_string()),
        ];
        (crate::rdbfile::rdb_bytes(self, &aux), point)
    }

    /// The database the replication stream is in.
    pub fn repl_stream_db(&self) -> u16 {
        self.repl.stream_db()
    }

    /// Ask the replicas where they are (WAIT): `REPLCONF GETACK *` goes
    /// into the stream.
    pub fn repl_getack(&self) {
        self.repl_command(None, &[b"REPLCONF", b"GETACK", b"*"]);
    }

    /// A PING in the stream, so that replicas can tell a quiet primary
    /// from a lost one.
    pub fn repl_ping(&self) {
        self.repl_command(None, &[b"PING"]);
    }

    /// How a replica logs in at its primary: `masteruser`, `masterauth`.
    pub fn master_auth(&self) -> (Option<Bytes>, Option<Bytes>) {
        self.master_auth.lock().clone()
    }

    /// A replica asks to go on from `offset` (counted from 1) of stream
    /// `replid`: where it goes on, or `None` when it needs the whole
    /// dataset.
    pub fn repl_resume(&self, replid: &[u8], offset: i64) -> Option<crate::repl::SyncPoint> {
        self.repl.resume(replid, offset)
    }

    /// Append the stream from `at` on, `max` bytes at most, to `out`: the
    /// offset reached, or `None` when what `at` needs is no longer held.
    pub fn repl_copy(
        &self,
        at: &crate::repl::SyncPoint,
        out: &mut Vec<u8>,
        max: usize,
    ) -> Option<u64> {
        let log = self.repl.log();
        if log.history() != at.history {
            return None;
        }
        log.copy(at.offset, out, max)
    }

    /// Call `Hooks::replicate` once the stream grows.
    pub fn repl_want_wake(&self) {
        self.repl.want_wake();
    }

    /// `repl-backlog-size`.
    pub fn set_repl_backlog_size(&self, size: usize) {
        self.repl.log().resize(size);
    }

    /// `replica-priority`, for INFO.
    pub fn replica_priority(&self) -> u64 {
        self.replica_priority.load(Ordering::Relaxed)
    }

    /// `replica-announced`, for INFO.
    pub fn replica_announced(&self) -> bool {
        self.replica_announced.load(Ordering::Relaxed)
    }

    /// Writes come only from the primary.
    pub fn is_replica(&self) -> bool {
        self.versions.replica()
    }

    /// Become a replica: keys stay until the primary deletes them, nothing
    /// is evicted, and the stream is the primary's. Takes the exclusive
    /// side.
    pub fn become_replica(&self) {
        let _excl = self.exclusive();
        self.versions.set_replica(true);
        self.repl.set_logging(false);
        for slot in self.shards.iter() {
            slot.data.lock().track_repl(false);
        }
    }

    /// Stop being a replica: writes are taken again, a new stream starts,
    /// and replicas of the old one may go on with it. Takes the exclusive
    /// side.
    pub fn become_primary(&self) {
        let _excl = self.exclusive();
        if !self.versions.replica() {
            return;
        }
        self.versions.set_replica(false);
        self.repl.shift_id();
        self.repl_start();
    }

    /// A full sync: the primary's file replaces every key and library
    /// here, and the stream goes on as `replid` from `offset`, in one hold
    /// of the exclusive side. On error the node is left empty.
    pub fn repl_load(
        &self,
        input: impl std::io::Read,
        replid: &str,
        offset: u64,
    ) -> Result<crate::rdbfile::RdbStats, String> {
        let mut excl = self.exclusive();
        excl.run(Cmd::FlushAll(Some(false)));
        excl.run(Cmd::Function(vec![b"FUNCTION".to_vec(), b"FLUSH".to_vec()]));
        let stats = self.load_rdb_held(&mut excl, input)?;
        self.repl
            .reset_to(replid, offset, stats.stream_db.unwrap_or(0));
        Ok(stats)
    }

    /// The primary went on as `replid` (PSYNC answered +CONTINUE with
    /// another ID).
    pub fn repl_continue(&self, replid: &str) {
        self.repl.continue_as(replid);
    }

    /// What the primary sent, now applied, goes to this node's stream for
    /// replicas of its own, and leaves the stream in database `db`. Call
    /// before the commands' hold of the gate ends, so that no sync sees
    /// their effect without them.
    pub fn repl_feed(&self, bytes: &[u8], db: u16) {
        if self.repl.feed(bytes, db) {
            if let Some(hooks) = self.hooks() {
                hooks.replicate();
            }
        }
    }

    /// `run`, and publish what the command did to its keys.
    fn run_noted<A: Access>(&self, cmd: Cmd, a: &mut A) -> Reply {
        let mut plan = notify::plan(&cmd);
        for planned in &mut plan {
            if let (notify::Rule::Existed, Some(key)) = (planned.rule, &planned.key) {
                let (h, i) = self.route(key);
                planned.existed = a.with(i, |s| s.exists(h, key));
            }
        }
        let shards = self.shard_set(&cmd);
        let reply = self.run(cmd, a);
        let db = current_db();
        // What the command found expired on its way comes first.
        for i in shards {
            let notes = a.with(i, Shard::take_notes);
            self.publish_notes(db, notes);
        }
        for planned in plan {
            if !planned.fires(&reply) {
                continue;
            }
            let key = match (&planned.key, &reply) {
                (Some(key), _) => key.as_slice(),
                (None, Reply::Array(items)) => match items.first() {
                    Some(Reply::Bulk(key)) => key.as_slice(),
                    _ => continue,
                },
                (None, _) => continue,
            };
            let gone = planned.may_empty && {
                let (h, i) = self.route(key);
                !a.with(i, |s| s.exists(h, key))
            };
            if !(gone && planned.del_instead) {
                self.emit(db, planned.class, planned.event, key);
            }
            if gone {
                self.emit(db, notify::GENERIC, "del", key);
            }
        }
        reply
    }

    /// Publish one keyspace event of database `db`, if its class is on.
    fn emit(&self, db: usize, class: u32, event: &str, key: &[u8]) {
        let flags = self.versions.notify();
        if flags & class == 0 {
            return;
        }
        let Some(hooks) = self.hooks() else {
            return;
        };
        if flags & notify::KEYSPACE != 0 {
            let mut channel = format!("__keyspace@{db}__:").into_bytes();
            channel.extend_from_slice(key);
            hooks.publish(&channel, event.as_bytes());
        }
        if flags & notify::KEYEVENT != 0 {
            hooks.publish(format!("__keyevent@{db}__:{event}").as_bytes(), key);
        }
    }

    fn publish_notes(&self, db: usize, notes: Vec<(crate::shard::Note, Bytes)>) {
        for (note, key) in notes {
            let (class, event) = notify::of_note(note);
            self.emit(db, class, event, &key);
        }
    }

    fn run<A: Access>(&self, cmd: Cmd, a: &mut A) -> Reply {
        if self.over_limit() && cmd.may_grow() {
            if let Some(out_of_memory) = self.make_room(&cmd, a) {
                return out_of_memory;
            }
        }
        match cmd {
            Cmd::Get(k) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.get(h, &k))
            }
            Cmd::Set(k, v) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.set(h, &k, v));
                Reply::Ok
            }
            Cmd::IncrBy(k, by) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.incr_by(h, &k, by))
            }
            Cmd::HSet(k, pairs) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.hset(h, &k, pairs))
            }
            Cmd::HMSet(k, pairs) => {
                let (h, i) = self.route(&k);
                match a.with(i, |s| s.hset(h, &k, pairs)) {
                    Reply::Int(_) => Reply::Ok,
                    wrong_type => wrong_type,
                }
            }
            Cmd::HGet(k, f) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.hget(h, &k, &f))
            }
            Cmd::HDel(k, fields) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.hdel(h, &k, &fields))
            }
            Cmd::HLen(k) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.hlen(h, &k))
            }
            Cmd::HGetAll(k) => {
                let (h, i) = self.route(&k);
                let mut out = Vec::new();
                match a.with(i, |s| s.hgetall_chunk(h, &k, 0, usize::MAX, &mut out)) {
                    HashScan::WrongType => wrongtype(),
                    HashScan::Missing | HashScan::Next(_) => Reply::Array(out),
                }
            }
            Cmd::Del(keys) => self.del(a, &keys, self.lazy.is_on(Reason::UserDel)),
            Cmd::Unlink(keys) => self.del(a, &keys, true),
            Cmd::Exists(keys) => {
                let mut n = 0;
                for k in &keys {
                    let (h, i) = self.route(k);
                    n += i64::from(a.with(i, |s| s.exists(h, k)));
                }
                Reply::Int(n)
            }
            Cmd::MSet(pairs) => {
                for (k, v) in pairs {
                    let (h, i) = self.route(&k);
                    a.with(i, |s| s.set(h, &k, v));
                }
                Reply::Ok
            }
            Cmd::MGet(keys) => {
                let values = keys.iter().map(|k| {
                    let (h, i) = self.route(k);
                    match a.with(i, |s| s.get(h, k)) {
                        // MGET reports a key of the wrong type as missing.
                        Reply::Error(_) => Reply::Nil,
                        other => other,
                    }
                });
                Reply::Array(values.collect())
            }
            Cmd::Keys(pattern) => {
                let mut out = Vec::new();
                for i in self.db_shards() {
                    a.with(i, |s| {
                        s.scan_chunk(0, usize::MAX, |k, _| {
                            if glob_match(&pattern, k) {
                                out.push(Reply::Bulk(k.to_vec()));
                            }
                        })
                    });
                }
                Reply::Array(out)
            }
            Cmd::SetOpts(k, v, opts) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.set_opts(h, &k, v, opts))
            }
            Cmd::SetNx(k, v) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.setnx(h, &k, v))
            }
            Cmd::GetDel(k) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.getdel(h, &k))
            }
            Cmd::GetEx(k, expire) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.getex(h, &k, expire))
            }
            Cmd::Append(k, v) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.append(h, &k, v))
            }
            Cmd::Strlen(k) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.strlen(h, &k))
            }
            Cmd::Expire { key, when, cond } => {
                let (h, i) = self.route(&key);
                a.with(i, |s| s.expire(h, &key, when, cond))
            }
            Cmd::Ttl(k, millis) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.ttl(h, &k, millis))
            }
            Cmd::ExpireTime(k, millis) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.expire_time(h, &k, millis))
            }
            Cmd::Persist(k) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.persist(h, &k))
            }
            Cmd::Type(k) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.type_of(h, &k))
            }
            Cmd::HExists(k, f) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.hexists(h, &k, &f))
            }
            Cmd::HMGet(k, fields) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.hmget(h, &k, &fields))
            }
            Cmd::HSetNx(k, f, v) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.hsetnx(h, &k, &f, v))
            }
            Cmd::HIncrBy(k, f, by) => {
                let (h, i) = self.route(&k);
                a.with(i, |s| s.hincr_by(h, &k, &f, by))
            }
            Cmd::HScan {
                key,
                cursor,
                pattern,
                count,
                novalues,
            } => {
                let (h, i) = self.route(&key);
                let budget = count.min(self.cfg.scan_chunk);
                let mut out = Vec::new();
                let next = a.with(i, |s| {
                    s.hash_chunk(h, &key, cursor, budget, |field, val| {
                        if pattern.as_ref().is_none_or(|p| glob_match(p, field)) {
                            out.push(Reply::Bulk(field.to_vec()));
                            if !novalues {
                                out.push(Reply::Bulk(val.clone()));
                            }
                        }
                    })
                });
                match next {
                    HashScan::WrongType => wrongtype(),
                    HashScan::Missing => scan_reply(0, out),
                    HashScan::Next(cursor) => scan_reply(cursor, out),
                }
            }
            Cmd::Scan {
                cursor,
                pattern,
                count,
                ty,
            } => {
                let (mut idx, mut st) = match self.scan_start(cursor, pattern, count, ty) {
                    Ok(start) => start,
                    Err(done) => return done,
                };
                loop {
                    match a.with(idx, |s| self.scan_step(idx, s, &mut st)) {
                        ScanStep::Done(reply) => return reply,
                        ScanStep::Same => {}
                        ScanStep::Shard(next) => idx = next,
                    }
                }
            }
            Cmd::Key(k, mut op) => {
                if let KeyOp::SScan { count, .. } | KeyOp::ZScan { count, .. } = &mut op {
                    *count = (*count).min(self.cfg.scan_chunk);
                }
                if matches!(op, KeyOp::Object(ObjectSub::Freq)) && !self.policy().is_lfu() {
                    return Reply::err(
                        "ERR An LFU maxmemory policy is not selected, access frequency not \
                         tracked. Please note that when switching between policies at runtime \
                         LRU and LFU data will take some time to adjust.",
                    );
                }
                // What a primary sent it already held to the limits.
                match &mut op {
                    KeyOp::Bloom(cmd) => cmd.settings = self.bloom.settings(!self.is_replica()),
                    KeyOp::Json(cmd) => cmd.settings = self.json.settings(!self.is_replica()),
                    _ => {}
                }
                let (h, i) = self.route(&k);
                a.with(i, |s| s.key_op(h, &k, op))
            }
            Cmd::Multi(op) | Cmd::Block(op, _) => self.apply_multi(*op, a),
            Cmd::RandomKey => self
                .random_shards(self.db_shards())
                .map(|i| a.with(i, random_key))
                .find(|reply| *reply != Reply::Nil)
                .unwrap_or(Reply::Nil),
            Cmd::Ping(_)
            | Cmd::Echo(_)
            | Cmd::Quit
            | Cmd::Static(_)
            | Cmd::Info(_)
            | Cmd::ScriptKill
            | Cmd::Dbsize
            | Cmd::Hello(..)
            | Cmd::Auth(..)
            | Cmd::Acl(_)
            | Cmd::Command(_)
            | Cmd::ConfigGet(_)
            | Cmd::ConfigSet(_)
            | Cmd::BgSave
            | Cmd::Save
            | Cmd::LastSave
            | Cmd::Time
            | Cmd::Client(_)
            | Cmd::Reset
            | Cmd::Sub(_)
            | Cmd::Publish(..)
            | Cmd::SPublish(..)
            | Cmd::PubSub(_)
            | Cmd::ScriptExists(_)
            | Cmd::ScriptFlush
            | Cmd::Txn(_)
            | Cmd::WaitAof(_)
            | Cmd::FlushAll(_)
            | Cmd::FlushDb(_)
            | Cmd::Select(_)
            | Cmd::Move { .. }
            | Cmd::In(..)
            | Cmd::ExecBlock { .. }
            | Cmd::Eval { .. }
            | Cmd::EvalSha { .. }
            | Cmd::Function(_)
            | Cmd::FCall { .. }
            | Cmd::SwapDb(..)
            | Cmd::Monitor
            | Cmd::SlowLog(_)
            | Cmd::Cluster(_)
            | Cmd::Asking
            | Cmd::Role
            | Cmd::Psync(..)
            | Cmd::Sync
            | Cmd::Replconf(_)
            | Cmd::ReplicaOf(_)
            | Cmd::Failover(_)
            | Cmd::Wait(..)
            | Cmd::Shutdown { .. }
            | Cmd::Debug(_)
            | Cmd::ScriptLoad(_) => Reply::err("ERR internal: not a keyspace command"),
        }
    }

    /// DEL, and UNLINK with `lazy`: how many of `keys` were there.
    fn del<A: Access>(&self, a: &mut A, keys: &[Bytes], lazy: bool) -> Reply {
        let mut n = 0;
        for k in keys {
            let (h, i) = self.route(k);
            n += i64::from(a.with(i, |s| s.del(h, k, lazy)));
        }
        Reply::Int(n)
    }

    /// The HyperLogLog strings at `keys`, without the keys that are missing.
    fn sketches<'k, A: Access>(
        &self,
        keys: impl Iterator<Item = &'k Bytes>,
        a: &mut A,
    ) -> Result<Vec<Bytes>, Reply> {
        let mut out = Vec::new();
        for k in keys {
            let (h, i) = self.route(k);
            let sketch = a.with(i, |s| s.with_string(h, k, <[u8]>::to_vec))?;
            if !sketch.is_empty() {
                out.push(sketch);
            }
        }
        Ok(out)
    }

    /// A command over several keys. Every shard is visited on its own, so
    /// values travel between shards by value.
    fn apply_multi<A: Access>(&self, op: MultiOp, a: &mut A) -> Reply {
        match op {
            MultiOp::Rename { src, dst, nx } => {
                let (sh, si) = self.route(&src);
                let (dh, di) = self.route(&dst);
                if !a.with(si, |s| s.exists(sh, &src)) {
                    return Reply::err("ERR no such key");
                }
                if nx && a.with(di, |s| s.exists(dh, &dst)) {
                    return Reply::Int(0);
                }
                if src != dst {
                    if let Some(entry) = a.with(si, |s| s.take(sh, &src)) {
                        a.with(di, |s| s.restore(dh, &dst, entry));
                    }
                }
                if nx {
                    Reply::Int(1)
                } else {
                    Reply::Ok
                }
            }
            MultiOp::Copy {
                src,
                dst,
                replace,
                db,
            } => {
                let (sh, si) = self.route(&src);
                let (dh, di) = match db {
                    None => self.route(&dst),
                    Some(db) => match usize::try_from(db) {
                        Ok(db) if db < self.cfg.databases => self.route_in(db, &dst),
                        _ => return Reply::err(DB_RANGE),
                    },
                };
                if src == dst && si == di {
                    return Reply::err("ERR source and destination objects are the same");
                }
                if !replace && a.with(di, |s| s.exists(dh, &dst)) {
                    return Reply::Int(0);
                }
                match a.with(si, |s| s.snapshot(sh, &src)) {
                    None => Reply::Int(0),
                    Some(entry) => {
                        a.with(di, |s| s.restore(dh, &dst, entry));
                        Reply::Int(1)
                    }
                }
            }
            MultiOp::Lcs { a: ka, b: kb, opts } => {
                let mut get = |k: &Bytes| {
                    let (h, i) = self.route(k);
                    a.with(i, |s| s.with_string(h, k, <[u8]>::to_vec))
                };
                match (get(&ka), get(&kb)) {
                    (Ok(x), Ok(y)) => crate::lcs::reply(&x, &y, opts),
                    _ => Reply::err("ERR The specified keys must contain string values"),
                }
            }
            MultiOp::PfCount(keys) => match self.sketches(keys.iter(), a) {
                Err(wrong_type) => wrong_type,
                Ok(sketches) => {
                    let parts: Vec<&[u8]> = sketches.iter().map(Vec::as_slice).collect();
                    match crate::hll::count(&parts) {
                        Ok(n) => Reply::Int(i64::try_from(n).unwrap_or(i64::MAX)),
                        Err(_) => Reply::err(NOT_HLL),
                    }
                }
            },
            MultiOp::PfMerge { dst, keys } => {
                let sketches = match self.sketches(std::iter::once(&dst).chain(&keys), a) {
                    Ok(sketches) => sketches,
                    Err(wrong_type) => return wrong_type,
                };
                let parts: Vec<&[u8]> = sketches.iter().map(Vec::as_slice).collect();
                let mut merged = crate::hll::new();
                if crate::hll::merge(&mut merged, &parts).is_err() {
                    return Reply::err(NOT_HLL);
                }
                let (h, i) = self.route(&dst);
                a.with(i, |s| s.replace_string(h, &dst, merged));
                Reply::Ok
            }
            MultiOp::JsonMGet { keys, path } => {
                let mut docs = Vec::with_capacity(keys.len());
                for k in &keys {
                    let (h, i) = self.route(k);
                    match a.with(i, |s| s.json_fetch(h, k, &path)) {
                        Ok(reply) => docs.push(reply),
                        Err(e) => return e,
                    }
                }
                Reply::Array(docs)
            }
            MultiOp::JsonMSet(triples) => {
                let settings = self.json.settings(!self.is_replica());
                // Every key is checked before any is written.
                for (k, path, value) in &triples {
                    let (h, i) = self.route(k);
                    if let Err(e) = a.with(i, |s| s.json_check(h, k, path, value, &settings)) {
                        return e;
                    }
                }
                for (k, path, value) in triples {
                    let (h, i) = self.route(&k);
                    a.with(i, |s| s.json_mset(h, &k, path, value, settings));
                    // Each key's event as it is set, so that they come in
                    // the order of the keys and not of the shards.
                    let notes = a.with(i, Shard::take_notes);
                    if !notes.is_empty() {
                        self.publish_notes(current_db(), notes);
                    }
                }
                Reply::Ok
            }
            MultiOp::MSetNx(pairs) => {
                let taken = pairs.iter().any(|(k, _)| {
                    let (h, i) = self.route(k);
                    a.with(i, |s| s.exists(h, k))
                });
                if taken {
                    return Reply::Int(0);
                }
                for (k, v) in pairs {
                    let (h, i) = self.route(&k);
                    a.with(i, |s| s.set(h, &k, v));
                }
                Reply::Int(1)
            }
            MultiOp::MSetEx {
                pairs,
                cond,
                expire,
            } => {
                // Every key is looked at before any is written.
                let mut exists = |k: &Bytes| {
                    let (h, i) = self.route(k);
                    a.with(i, |s| s.exists(h, k))
                };
                let blocked = match cond {
                    Some(Cond::Nx) => pairs.iter().any(|(k, _)| exists(k)),
                    Some(Cond::Xx) => pairs.iter().any(|(k, _)| !exists(k)),
                    _ => false,
                };
                if blocked {
                    return Reply::Int(0);
                }
                for (k, v) in pairs {
                    let (h, i) = self.route(&k);
                    a.with(i, |s| s.set_ex(h, &k, v, expire));
                }
                Reply::Int(1)
            }
            MultiOp::LMove {
                src,
                dst,
                from_left,
                to_left,
            } => {
                let (sh, si) = self.route(&src);
                let (dh, di) = self.route(&dst);
                // Redis order: a missing source is nil whatever the
                // destination holds, then the types, then the move.
                match a.with(si, |s| s.type_of(sh, &src)) {
                    Reply::Simple(t) if t == "none" => return Reply::Nil,
                    Reply::Simple(t) if t == "list" => {}
                    _ => return wrongtype(),
                }
                if src == dst {
                    return a.with(si, |s| s.rotate(sh, &src, from_left, to_left));
                }
                let dst_type = a.with(di, |s| s.type_of(dh, &dst));
                if !matches!(&dst_type, Reply::Simple(t) if t == "none" || t == "list") {
                    return wrongtype();
                }
                let pop = KeyOp::Pop {
                    left: from_left,
                    count: None,
                };
                let item = match a.with(si, |s| s.key_op(sh, &src, pop)) {
                    Reply::Bulk(item) => item,
                    other => return other,
                };
                let push = KeyOp::Push {
                    left: to_left,
                    existing: false,
                    items: vec![item.clone()],
                };
                a.with(di, |s| s.key_op(dh, &dst, push));
                Reply::Bulk(item)
            }
            MultiOp::SMove { src, dst, member } => {
                let (sh, si) = self.route(&src);
                let (dh, di) = self.route(&dst);
                match a.with(si, |s| s.type_of(sh, &src)) {
                    Reply::Simple(t) if t == "none" => return Reply::Int(0),
                    Reply::Simple(t) if t == "set" => {}
                    _ => return wrongtype(),
                }
                let dst_type = a.with(di, |s| s.type_of(dh, &dst));
                if !matches!(&dst_type, Reply::Simple(t) if t == "none" || t == "set") {
                    return wrongtype();
                }
                if src == dst {
                    let has = KeyOp::SIsMember(member);
                    return a.with(si, |s| s.key_op(sh, &src, has));
                }
                let rem = KeyOp::SRem(vec![member.clone()]);
                match a.with(si, |s| s.key_op(sh, &src, rem)) {
                    Reply::Int(1) => {
                        a.with(di, |s| s.key_op(dh, &dst, KeyOp::SAdd(vec![member])));
                        Reply::Int(1)
                    }
                    other => other,
                }
            }
            MultiOp::Sets {
                op,
                keys,
                store,
                card,
            } => {
                // A key named many times is read once: the result is the
                // same, and each reading is a copy of the whole set.
                let mut sets = Vec::new();
                let mut seen = HashSet::new();
                for (n, k) in keys.iter().enumerate() {
                    let again = n > 0 && (!seen.insert(k) || (*k == keys[0] && op != SetOp::Diff));
                    if again {
                        continue;
                    }
                    let (h, i) = self.route(k);
                    match a.with(i, |s| s.set_members(h, k)) {
                        Ok(members) => sets.push(members),
                        Err(wrong_type) => return wrong_type,
                    }
                }
                let mut sets = sets.into_iter();
                let first = sets.next().unwrap_or_default();
                let rest: Vec<HashSet<Vec<u8>>> =
                    sets.map(|members| members.into_iter().collect()).collect();
                let result: Vec<Vec<u8>> = match op {
                    SetOp::Inter => first
                        .into_iter()
                        .filter(|m| rest.iter().all(|s| s.contains(m)))
                        .collect(),
                    SetOp::Diff => first
                        .into_iter()
                        .filter(|m| !rest.iter().any(|s| s.contains(m)))
                        .collect(),
                    SetOp::Union => {
                        let mut all: HashSet<Vec<u8>> = first.into_iter().collect();
                        rest.into_iter().for_each(|s| all.extend(s));
                        all.into_iter().collect()
                    }
                };
                if let Some(limit) = card {
                    return Reply::Int(result.len().min(limit) as i64);
                }
                match store {
                    Some(dst) => {
                        let (h, i) = self.route(&dst);
                        a.with(i, |s| s.store_set(h, &dst, &result));
                        Reply::Int(result.len() as i64)
                    }
                    None => Reply::Array(result.into_iter().map(Reply::Bulk).collect()),
                }
            }
            MultiOp::ZRangeStore { dst, src, spec } => {
                let (sh, si) = self.route(&src);
                let entries = match a.with(si, |s| s.zrange_entries(sh, &src, &spec)) {
                    Ok(entries) => entries,
                    Err(wrong_type) => return wrong_type,
                };
                let (dh, di) = self.route(&dst);
                a.with(di, |s| s.store_zset(dh, &dst, &entries));
                Reply::Int(entries.len() as i64)
            }
            MultiOp::MPop {
                keys,
                zset,
                first,
                count,
                flat,
            } => {
                for k in keys {
                    let (h, i) = self.route(&k);
                    let count = Some(count);
                    let op = if zset {
                        KeyOp::ZPop { max: !first, count }
                    } else {
                        KeyOp::Pop { left: first, count }
                    };
                    let items = match a.with(i, |s| s.key_op(h, &k, op)) {
                        Reply::Array(items) if !items.is_empty() => items,
                        Reply::Array(_) | Reply::Nil | Reply::NilArray => continue,
                        other => return other,
                    };
                    if flat {
                        let mut out = vec![Reply::Bulk(k)];
                        out.extend(items);
                        return Reply::Array(out);
                    }
                    let items = if zset {
                        items.chunks(2).map(|p| Reply::Array(p.to_vec())).collect()
                    } else {
                        items
                    };
                    return Reply::Array(vec![Reply::Bulk(k), Reply::Array(items)]);
                }
                Reply::NilArray
            }
            MultiOp::XRead {
                keys,
                ids,
                count,
                noack,
                start,
            } => {
                // `$` is the end of the stream when the command first ran:
                // a blocked reader that tries again must not move it.
                let after = match (&ids, start.get()) {
                    (XIds::Group { .. }, _) => Vec::new(),
                    (XIds::After(_), Some(found)) => found.clone(),
                    (XIds::After(specs), None) => {
                        let mut found = Vec::with_capacity(keys.len());
                        for (k, spec) in keys.iter().zip(specs) {
                            found.push(match spec {
                                IdSpec::Id(id) => *id,
                                IdSpec::Last => {
                                    let (h, i) = self.route(k);
                                    match a.with(i, |s| s.stream_last(h, k)) {
                                        Ok(id) => id,
                                        Err(wrong_type) => return wrong_type,
                                    }
                                }
                            });
                        }
                        let _ = start.set(found.clone());
                        found
                    }
                };
                let mut out = Vec::new();
                for (at, k) in keys.into_iter().enumerate() {
                    let op = match &ids {
                        XIds::After(_) => XOp::ReadAfter {
                            after: IdSpec::Id(after[at]),
                            count,
                        },
                        XIds::Group {
                            group,
                            consumer,
                            ids,
                        } => XOp::ReadGroup {
                            group: group.clone(),
                            consumer: consumer.clone(),
                            id: ids[at],
                            count,
                            noack,
                        },
                    };
                    let (h, i) = self.route(&k);
                    match a.with(i, |s| s.stream_op(h, &k, op)) {
                        Reply::Nil => {}
                        rows @ Reply::Array(_) => {
                            out.push(Reply::Array(vec![Reply::Bulk(k), rows]));
                        }
                        other => return other,
                    }
                }
                if out.is_empty() {
                    Reply::NilArray
                } else {
                    Reply::Array(out)
                }
            }
            MultiOp::BitOp { op, dst, keys } => {
                // Each source is folded into the result where it lies, so
                // the command holds one string however many keys it names.
                let mut out: Vec<u8> = Vec::new();
                for (n, k) in keys.iter().enumerate() {
                    let (h, i) = self.route(k);
                    let fold = |src: &[u8]| {
                        if n == 0 {
                            out.extend_from_slice(src);
                            if op == BitOp::Not {
                                out.iter_mut().for_each(|byte| *byte = !*byte);
                            }
                            return;
                        }
                        if src.len() > out.len() {
                            out.resize(src.len(), 0);
                        }
                        // A shorter source counts as padded with zeros. One
                        // plain loop per operation, which the compiler
                        // turns into wide instructions.
                        let (head, tail) = out.split_at_mut(src.len());
                        let pairs = head.iter_mut().zip(src);
                        match op {
                            BitOp::Not => {}
                            BitOp::And => {
                                pairs.for_each(|(byte, y)| *byte &= y);
                                tail.fill(0);
                            }
                            BitOp::Or => pairs.for_each(|(byte, y)| *byte |= y),
                            BitOp::Xor => pairs.for_each(|(byte, y)| *byte ^= y),
                        }
                    };
                    if let Err(wrong_type) = a.with(i, |s| s.with_string(h, k, fold)) {
                        return wrong_type;
                    }
                }
                let len = out.len();
                let (h, i) = self.route(&dst);
                a.with(i, |s| s.store_string(h, &dst, out));
                Reply::Int(len as i64)
            }
            MultiOp::ZStore {
                dst,
                keys,
                weights,
                aggregate,
                op,
                withscores,
                card,
            } => {
                let mut acc: HashMap<Vec<u8>, (f64, usize)> = HashMap::new();
                for (n, (k, weight)) in keys.iter().zip(&weights).enumerate() {
                    let (h, i) = self.route(k);
                    let entries = match a.with(i, |s| s.zset_entries(h, k)) {
                        Ok(entries) => entries,
                        Err(wrong_type) => return wrong_type,
                    };
                    if op == SetOp::Diff && n > 0 {
                        for (member, _) in entries {
                            acc.remove(&member);
                        }
                        continue;
                    }
                    for (member, score) in entries {
                        // Redis' rule: infinity times zero counts as zero.
                        let score = match score * weight {
                            s if s.is_nan() => 0.0,
                            s => s,
                        };
                        acc.entry(member)
                            .and_modify(|(sum, seen)| {
                                *seen += 1;
                                *sum = match aggregate {
                                    Aggregate::Sum if (*sum + score).is_nan() => 0.0,
                                    Aggregate::Sum => *sum + score,
                                    Aggregate::Min => sum.min(score),
                                    Aggregate::Max => sum.max(score),
                                };
                            })
                            .or_insert((score, 1));
                    }
                }
                let mut entries: Vec<_> = acc
                    .into_iter()
                    .filter(|(_, (_, seen))| op != SetOp::Inter || *seen == keys.len())
                    .map(|(member, (score, _))| (member, score))
                    .collect();
                if let Some(limit) = card {
                    return Reply::Int(entries.len().min(limit) as i64);
                }
                let Some(dst) = dst else {
                    entries.sort_by(|x, y| {
                        x.1.partial_cmp(&y.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| x.0.cmp(&y.0))
                    });
                    let mut out = Vec::with_capacity(entries.len() * 2);
                    for (member, score) in entries {
                        out.push(Reply::Bulk(member));
                        if withscores {
                            out.push(Reply::Bulk(format_score(score)));
                        }
                    }
                    return Reply::Array(out);
                };
                let (h, i) = self.route(&dst);
                a.with(i, |s| s.store_zset(h, &dst, &entries));
                Reply::Int(entries.len() as i64)
            }
            MultiOp::GeoStore {
                dst,
                src,
                query,
                storedist,
            } => {
                let (h, i) = self.route(&src);
                let hits = match a.with(i, |s| s.geo_hits(h, &src, &query)) {
                    Ok(hits) => hits,
                    Err(e) => return e,
                };
                let entries: Vec<(Bytes, f64)> = hits
                    .into_iter()
                    .map(|hit| {
                        let score = if storedist {
                            hit.dist / query.unit.metres()
                        } else {
                            hit.bits as f64
                        };
                        (hit.member, score)
                    })
                    .collect();
                let (h, i) = self.route(&dst);
                a.with(i, |s| s.store_zset(h, &dst, &entries));
                Reply::Int(entries.len() as i64)
            }
            MultiOp::Sort { key, spec } => {
                let (h, i) = self.route(&key);
                let (elements, ranked) = match a.with(i, |s| s.sort_source(h, &key)) {
                    Ok(source) => source,
                    Err(wrong_type) => return wrong_type,
                };
                // Only a command that has the node to itself gets here
                // with a pattern that names other keys.
                let mut lookup = |pattern: &[u8], element: &[u8]| {
                    let (k, field) = sort::lookup_key(pattern, element)?;
                    let (h, i) = self.route(&k);
                    a.with(i, |s| s.sort_value(h, &k, field.as_deref()))
                };
                let arranged = match spec.arrange(elements, ranked, &mut lookup) {
                    Ok(arranged) => arranged,
                    Err(e) => return e,
                };
                let out = match spec.project(arranged, &mut lookup) {
                    Ok(out) => out,
                    Err(too_large) => return too_large,
                };
                match &spec.store {
                    Some(dst) => {
                        let len = out.len();
                        let items = out.into_iter().map(Option::unwrap_or_default).collect();
                        let (h, i) = self.route(dst);
                        a.with(i, |s| s.store_list(h, dst, items));
                        Reply::Int(len as i64)
                    }
                    None => Reply::Array(
                        out.into_iter()
                            .map(|v| v.map_or(Reply::Nil, Reply::Bulk))
                            .collect(),
                    ),
                }
            }
        }
    }

    fn over_limit(&self) -> bool {
        let max = self.maxmemory.load(Ordering::Relaxed);
        max != 0 && mem::used() > max
    }

    fn policy(&self) -> Policy {
        let raw = self.policy.load(Ordering::Relaxed);
        Policy::ALL
            .into_iter()
            .find(|p| *p as u8 == raw)
            .unwrap_or(Policy::NoEviction)
    }

    /// Memory is full and `cmd` wants more: evict, first from the shards
    /// the command holds anyway, then from others that happen to be free.
    /// Returns the error for the client when nothing could be evicted.
    fn make_room<A: Access>(&self, cmd: &Cmd, a: &mut A) -> Option<Reply> {
        const PER_SHARD: usize = 16;
        const OTHER_SHARDS: usize = 8;
        // Other threads free and take memory meanwhile, so one look that
        // finds the limit met and the next that finds it exceeded do not
        // mean nothing can go: only a few rounds in a row do.
        const ROUNDS: usize = 4;
        // Nothing left to evict, but the freeing thread may still hold
        // memory: how long to wait for it, as Valkey does, before refusing.
        const LAZY_WAIT: Duration = Duration::from_millis(5);
        // A replica takes all its primary sends (`replica-ignore-maxmemory`).
        if self.versions.replica() {
            return None;
        }
        let policy = self.policy();
        // A value freed lazily stays counted until the thread is done with
        // it. Once one went, the room is coming: evicting on would take
        // far more keys than needed.
        let handed = self.lazy.handed();
        let full = || self.over_limit() && self.lazy.handed() == handed;
        // What goes to make room is not a write of the connection that
        // needed the room.
        let _nobody = enter_ticket(0);
        for _ in 0..ROUNDS {
            if policy == Policy::NoEviction {
                break;
            }
            if !full() {
                return None;
            }
            let mut evicted = 0;
            for i in self.shard_set(cmd) {
                evicted += a.with(i, |s| s.evict(policy, PER_SHARD, full));
            }
            for _ in 0..OTHER_SHARDS {
                if !full() {
                    break;
                }
                let Some(i) = self.random_shards(0..self.used_shards()).next() else {
                    break;
                };
                // A shard this command holds is simply busy here.
                if let Some(mut guard) = self.shards[i].data.try_lock() {
                    evicted += guard.evict(policy, PER_SHARD, full);
                    let notes = guard.take_notes();
                    self.release(i, guard);
                    self.publish_notes(i / self.cfg.shards, notes);
                }
            }
            if evicted > 0 {
                self.stats
                    .evicted
                    .fetch_add(evicted as u64, Ordering::Relaxed);
                return None;
            }
        }
        let until = Instant::now() + LAZY_WAIT;
        while full() && self.lazy.pending() > 0 && Instant::now() < until {
            std::thread::sleep(Duration::from_micros(100));
        }
        full().then(|| Reply::err("OOM command not allowed when used memory > 'maxmemory'."))
    }

    /// Per `LogKind`, the least that a command is logged at: microseconds
    /// for the slow log, bytes for the others. `None`: nothing is logged.
    pub fn log_limits(&self) -> [Option<u64>; 3] {
        std::array::from_fn(|i| {
            let on = self.log_max[i].load(Ordering::Relaxed) > 0;
            u64::try_from(self.log_limit[i].load(Ordering::Relaxed))
                .ok()
                .filter(|_| on)
        })
    }

    /// Whether any command could be logged: the server then keeps what
    /// the log would show of each.
    pub fn logging(&self) -> bool {
        self.log_limits().iter().any(Option::is_some)
    }

    /// Add a command to a log. `value` is its time or size, `args` what
    /// the entry shows: the caller cuts it to `SLOW_ARGS` and
    /// `SLOW_ARG_BYTES`.
    pub fn log_record(
        &self,
        kind: LogKind,
        value: u64,
        args: Vec<Bytes>,
        addr: String,
        name: Bytes,
    ) {
        let micros = value;
        let max = self.log_max[kind as usize].load(Ordering::Relaxed);
        let mut log = self.logs[kind as usize].lock();
        let id = log.next_id;
        log.next_id += 1;
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        log.entries.push_front(SlowEntry {
            id,
            at,
            micros,
            args,
            addr,
            name,
        });
        log.entries.truncate(max);
    }

    fn slowlog_cmd(&self, cmd: SlowLogCmd) -> Reply {
        let log = |kind: LogKind| self.logs[kind as usize].lock();
        match cmd {
            SlowLogCmd::Len(kind) => Reply::Int(log(kind).entries.len() as i64),
            SlowLogCmd::Reset(kind) => {
                log(kind).entries.clear();
                Reply::Ok
            }
            SlowLogCmd::Get(count, kind) => Reply::Array(
                log(kind)
                    .entries
                    .iter()
                    .take(count.unwrap_or(usize::MAX))
                    .map(|e| {
                        Reply::Array(vec![
                            Reply::Int(e.id as i64),
                            Reply::Int(e.at as i64),
                            Reply::Int(i64::try_from(e.micros).unwrap_or(i64::MAX)),
                            Reply::Array(e.args.iter().cloned().map(Reply::Bulk).collect()),
                            Reply::bulk(e.addr.as_str()),
                            Reply::Bulk(e.name.clone()),
                        ])
                    })
                    .collect(),
            ),
            SlowLogCmd::Help => Reply::Array(
                [
                    "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "GET [<count>]",
                    "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
                    "    Entries are made of:",
                    "    id, timestamp, time in microseconds, arguments array, client IP and port,",
                    "    client name",
                    "LEN",
                    "    Return the length of the slowlog.",
                    "RESET",
                    "    Reset the slowlog.",
                    "HELP",
                    "    Print this help.",
                ]
                .iter()
                .map(|line| Reply::Simple((*line).into()))
                .collect(),
            ),
            SlowLogCmd::LogHelp => Reply::Array(
                [
                    "COMMANDLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "GET <count> <type>",
                    "    Return top <count> entries of the specified <type> from the commandlog \
                     (-1 mean all).",
                    "    Entries are made of:",
                    "    id, timestamp,",
                    "        time in microseconds for type of slow,",
                    "        or size in bytes for type of large-request,",
                    "        or size in bytes for type of large-reply",
                    "    arguments array, client IP and port,",
                    "    client name",
                    "LEN <type>",
                    "    Return the length of the specified type of commandlog.",
                    "RESET <type>",
                    "    Reset the specified type of commandlog.",
                    "HELP",
                    "    Print this help.",
                ]
                .iter()
                .map(|line| Reply::Simple((*line).into()))
                .collect(),
            ),
        }
    }

    /// Who may run DEBUG (`enable-debug-command`).
    pub fn debug_command(&self) -> Protected {
        self.cfg.debug_command
    }

    /// The few DEBUG subcommands there are; `enable-debug-command` has
    /// let the command through.
    fn debug(&self, args: &[Bytes]) -> Reply {
        let sub = args[1].to_ascii_uppercase();
        match (sub.as_slice(), args.len()) {
            (b"HELP", 2) => Reply::Array(
                [
                    "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "SET-ACTIVE-EXPIRE <0|1>",
                    "    Setting it to 0 disables expiry of volatile keys in the background.",
                    "SLEEP <seconds>",
                    "    Stop the server for <seconds>. Decimals allowed.",
                    "HELP",
                    "    Print this help.",
                ]
                .iter()
                .map(|line| Reply::Simple((*line).into()))
                .collect(),
            ),
            // Only the thread that runs it stops: the others go on.
            (b"SLEEP", 3) => {
                let secs = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|s| s.is_finite())
                    .unwrap_or(0.0);
                std::thread::sleep(Duration::from_secs_f64(secs.clamp(0.0, 3600.0)));
                Reply::Ok
            }
            (b"SET-ACTIVE-EXPIRE", 3) => {
                let on = crate::resp::parse_i64(&args[2]).is_some_and(|n| n != 0);
                self.active_expire.store(on, Ordering::Relaxed);
                Reply::Ok
            }
            _ => Reply::Error(format!(
                "ERR unknown subcommand or wrong number of arguments for '{}'. Try DEBUG HELP.",
                String::from_utf8_lossy(&args[1][..args[1].len().min(64)])
            )),
        }
    }

    /// `memory-dbs` as CONFIG GET shows it: `0,3`.
    fn memory_dbs(&self) -> String {
        let dbs: Vec<String> = (0..self.cfg.databases)
            .filter(|db| self.memory[*db])
            .map(|db| db.to_string())
            .collect();
        dbs.join(",")
    }

    fn lazy_setting(&self, reason: Reason) -> String {
        let on = self.lazy.is_on(reason);
        (if on { "yes" } else { "no" }).to_string()
    }

    /// One setting as `CONFIG SET name value` takes it, for a server that
    /// is starting; unlike CONFIG SET, an unknown name is an error.
    pub fn configure(&self, name: &str, value: &str) -> Result<(), String> {
        let name = name.to_ascii_lowercase();
        let Reply::Array(known) = self.config_get(name.as_bytes()) else {
            return Err(format!("unknown setting {name}"));
        };
        // A name may hold glob characters; only the exact one counts.
        if !known
            .iter()
            .step_by(2)
            .any(|n| *n == Reply::bulk(name.as_str()))
        {
            return Err(format!("unknown setting {name}"));
        }
        match self.config_set(&[(name.into_bytes(), value.as_bytes().to_vec())]) {
            Reply::Error(e) => Err(e),
            _ => Ok(()),
        }
    }

    /// CONFIG GET: every setting whose name matches `pattern`.
    fn config_get(&self, pattern: &[u8]) -> Reply {
        let settings = [
            (
                "maxmemory",
                self.maxmemory.load(Ordering::Relaxed).to_string(),
            ),
            ("maxmemory-policy", self.policy().name().to_string()),
            ("databases", self.cfg.databases.to_string()),
            (
                "notify-keyspace-events",
                notify::show(self.notify_cfg.load(Ordering::Relaxed)),
            ),
            (
                LOG_SETTINGS[0].0,
                self.log_limit[0].load(Ordering::Relaxed).to_string(),
            ),
            (
                LOG_SETTINGS[0].1,
                self.log_max[0].load(Ordering::Relaxed).to_string(),
            ),
            (
                SLOW_ALIASES.0,
                self.log_limit[0].load(Ordering::Relaxed).to_string(),
            ),
            (
                SLOW_ALIASES.1,
                self.log_max[0].load(Ordering::Relaxed).to_string(),
            ),
            (
                LOG_SETTINGS[1].0,
                self.log_limit[1].load(Ordering::Relaxed).to_string(),
            ),
            (
                LOG_SETTINGS[1].1,
                self.log_max[1].load(Ordering::Relaxed).to_string(),
            ),
            (
                LOG_SETTINGS[2].0,
                self.log_limit[2].load(Ordering::Relaxed).to_string(),
            ),
            (
                LOG_SETTINGS[2].1,
                self.log_max[2].load(Ordering::Relaxed).to_string(),
            ),
            // Only a hash of the password is kept, so it cannot be shown.
            ("requirepass", String::new()),
            // Shown by Redis, but a password all the same.
            ("masterauth", String::new()),
            (
                "masteruser",
                String::from_utf8_lossy(self.master_auth.lock().0.as_deref().unwrap_or_default())
                    .into_owned(),
            ),
            ("repl-backlog-size", self.repl.info().size.to_string()),
            ("replica-read-only", "yes".to_string()),
            ("replica-priority", self.replica_priority().to_string()),
            ("slave-priority", self.replica_priority().to_string()),
            ("replica-announced", yes_no(self.replica_announced())),
            ("appendfsync", self.fsync().name().to_string()),
            ("timeout", self.client_timeout().to_string()),
            ("memory-dbs", self.memory_dbs()),
            ("dir", self.rdb_dir()),
            ("dbfilename", self.rdb_name()),
            (
                "enable-debug-command",
                self.cfg.debug_command.name().to_string(),
            ),
            (
                Reason::Eviction.setting(),
                self.lazy_setting(Reason::Eviction),
            ),
            (Reason::Expire.setting(), self.lazy_setting(Reason::Expire)),
            (
                Reason::ServerDel.setting(),
                self.lazy_setting(Reason::ServerDel),
            ),
            (
                Reason::UserDel.setting(),
                self.lazy_setting(Reason::UserDel),
            ),
            (
                Reason::UserFlush.setting(),
                self.lazy_setting(Reason::UserFlush),
            ),
        ];
        let mut out = Vec::new();
        let modules = self.bloom.shown().into_iter().chain(self.json.shown());
        for (name, value) in settings.into_iter().chain(modules) {
            if glob_match(pattern, name.as_bytes()) {
                out.push(Reply::bulk(name));
                out.push(Reply::bulk(value));
            }
        }
        Reply::Array(out)
    }

    /// CONFIG SET: all the settings, or none when one is refused.
    fn config_set(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Reply {
        let refuse = |name: &[u8]| {
            Reply::Error(format!(
                "ERR CONFIG SET failed (possibly related to argument '{}')",
                String::from_utf8_lossy(&name[..name.len().min(64)])
            ))
        };
        let mut maxmemory = None;
        let mut policy = None;
        let mut requirepass = None;
        let mut master_auth = (None, None);
        let mut backlog = None;
        let mut events = None;
        let mut log_limit = [None; 3];
        let mut log_max = [None; 3];
        let mut fsync = None;
        let mut timeout = None;
        let mut priority = None;
        let mut announced = None;
        let mut lazy = Vec::new();
        let mut bloom = Vec::new();
        let mut json = Vec::new();
        for (name, value) in pairs {
            let refused = match crate::bloom::Config::check(name, value) {
                Some(Ok(change)) => {
                    bloom.push(change);
                    continue;
                }
                Some(Err(why)) => Some(why),
                None => match crate::json::Config::check(name, value) {
                    Some(Ok(change)) => {
                        json.push(change);
                        continue;
                    }
                    Some(Err(why)) => Some(why),
                    None => None,
                },
            };
            if let Some(why) = refused {
                return Reply::Error(format!(
                    "ERR CONFIG SET failed (possibly related to argument '{}') - {why}",
                    String::from_utf8_lossy(name)
                ));
            }
            if let Some(reason) = Reason::ALL
                .into_iter()
                .find(|r| r.setting().as_bytes() == name.as_slice())
            {
                match value.to_ascii_lowercase().as_slice() {
                    b"yes" => lazy.push((reason, true)),
                    b"no" => lazy.push((reason, false)),
                    _ => return refuse(name),
                }
                continue;
            }
            if let Some((kind, is_len)) = log_setting(name) {
                let n = crate::resp::parse_i64(value);
                if is_len {
                    match n.and_then(|n| usize::try_from(n).ok()) {
                        Some(len) if len <= SLOW_MAX_LEN => log_max[kind] = Some(len),
                        _ => return refuse(name),
                    }
                } else {
                    match n {
                        Some(limit) if limit >= -1 => log_limit[kind] = Some(limit),
                        _ => return refuse(name),
                    }
                }
                continue;
            }
            match name.as_slice() {
                b"maxmemory" => match parse_bytes(value) {
                    Some(bytes) if bytes == 0 || mem::counting() => maxmemory = Some(bytes),
                    Some(_) => {
                        return Reply::err(
                            "ERR maxmemory needs hcache::CountingAlloc as the global allocator",
                        )
                    }
                    None => return refuse(name),
                },
                b"maxmemory-policy" => match Policy::from_name(value) {
                    Some(p) => policy = Some(p),
                    None => return refuse(name),
                },
                b"requirepass" => requirepass = Some(value),
                b"masteruser" => master_auth.0 = Some(value),
                b"masterauth" => master_auth.1 = Some(value),
                b"repl-backlog-size" => match parse_bytes(value) {
                    Some(bytes) => backlog = Some(bytes),
                    None => return refuse(name),
                },
                // Writes on a replica would make it differ from its primary.
                b"replica-read-only" | b"slave-read-only" => {
                    if !value.eq_ignore_ascii_case(b"yes") {
                        return refuse(name);
                    }
                }
                b"notify-keyspace-events" => match notify::parse(value) {
                    Some(flags) => events = Some(flags),
                    None => return refuse(name),
                },
                b"appendfsync" => match Fsync::from_name(value) {
                    Some(mode) => fsync = Some(mode),
                    None => return refuse(name),
                },
                b"replica-priority" | b"slave-priority" => {
                    match crate::resp::parse_i64(value)
                        .filter(|n| (0..=i64::from(i32::MAX)).contains(n))
                    {
                        Some(n) => priority = Some(n.unsigned_abs()),
                        None => return refuse(name),
                    }
                }
                b"replica-announced" | b"slave-announced" => {
                    match value.to_ascii_lowercase().as_slice() {
                        b"yes" => announced = Some(true),
                        b"no" => announced = Some(false),
                        _ => return refuse(name),
                    }
                }
                b"timeout" => {
                    match crate::resp::parse_i64(value).and_then(|n| u64::try_from(n).ok()) {
                        Some(secs) => timeout = Some(secs),
                        None => return refuse(name),
                    }
                }
                b"databases" | b"memory-dbs" | b"enable-debug-command" => {
                    return Reply::Error(format!(
                        "ERR CONFIG SET failed (possibly related to argument '{}') - can't set \
                         immutable config",
                        String::from_utf8_lossy(name)
                    ))
                }
                // Where every key is written, chosen at start only: as in
                // Redis without `enable-protected-configs`.
                b"dir" | b"dbfilename" => {
                    return Reply::Error(format!(
                        "ERR CONFIG SET failed (possibly related to argument '{}') - can't set \
                         protected config",
                        String::from_utf8_lossy(name)
                    ))
                }
                // Settings of Redis that mean nothing here are accepted.
                _ => {}
            }
        }
        for (reason, on) in lazy {
            self.lazy.set(reason, on);
        }
        if let Some(n) = priority {
            self.replica_priority.store(n, Ordering::Relaxed);
        }
        if let Some(on) = announced {
            self.replica_announced.store(on, Ordering::Relaxed);
        }
        for change in bloom {
            self.bloom.apply(change);
        }
        for change in json {
            self.json.apply(change);
        }
        if let Some(bytes) = maxmemory {
            self.maxmemory.store(bytes, Ordering::Relaxed);
        }
        if let Some(p) = policy {
            self.policy.store(p as u8, Ordering::Relaxed);
        }
        if let Some(pass) = requirepass {
            self.acl.set_requirepass(Some(pass));
        }
        let (user, pass) = master_auth;
        if user.is_some() || pass.is_some() {
            let mut auth = self.master_auth.lock();
            // An empty value takes the setting away.
            let set = |value: &Bytes| (!value.is_empty()).then(|| value.clone());
            if let Some(user) = user {
                auth.0 = set(user);
            }
            if let Some(pass) = pass {
                auth.1 = set(pass);
            }
        }
        if let Some(bytes) = backlog {
            self.repl.log().resize(bytes);
        }
        if let Some(mode) = fsync {
            self.set_fsync(mode);
        }
        if let Some(secs) = timeout {
            self.set_client_timeout(secs);
        }
        for kind in 0..3 {
            if let Some(limit) = log_limit[kind] {
                self.log_limit[kind].store(limit, Ordering::Relaxed);
            }
            if let Some(len) = log_max[kind] {
                self.log_max[kind].store(len, Ordering::Relaxed);
                self.logs[kind].lock().entries.truncate(len);
            }
        }
        if let Some(flags) = events {
            self.notify_cfg.store(flags, Ordering::Relaxed);
            let on = if notify::active(flags) { flags } else { 0 };
            self.versions.set_notify(on);
        }
        Reply::Ok
    }

    /// The shards that hold keys, from a different starting point on each
    /// call.
    fn random_shards(&self, among: Range<usize>) -> impl Iterator<Item = usize> + '_ {
        let seq = self.random_seq.fetch_add(1, Ordering::Relaxed) as u64;
        let start =
            among.start + (seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize % among.len();
        (start..among.end)
            .chain(among.start..start)
            .filter(|&i| self.shards[i].len.load(Ordering::Relaxed) > 0)
    }

    /// Where a SCAN call starts: the shard and the state, or the reply when
    /// the cursor points past the last shard.
    fn scan_start(
        &self,
        cursor: u64,
        pattern: Option<Vec<u8>>,
        count: usize,
        ty: Option<Vec<u8>>,
    ) -> Result<(usize, ScanState), Reply> {
        let local = (cursor & ((1 << SCAN_SHARD_BITS) - 1)) as usize;
        if local >= self.cfg.shards {
            return Err(scan_reply(0, Vec::new()));
        }
        Ok((
            self.db_shards().start + local,
            ScanState {
                pattern,
                ty,
                left: count,
                cursor: cursor >> SCAN_SHARD_BITS,
                out: Vec::new(),
            },
        ))
    }

    /// One chunk of SCAN on shard `idx` (rule 4).
    fn scan_step(&self, idx: usize, shard: &Shard, st: &mut ScanState) -> ScanStep {
        let db = self.db_shards();
        let budget = st.left.min(self.cfg.scan_chunk);
        st.left -= budget;
        let ScanState {
            pattern, ty, out, ..
        } = st;
        let cursor = shard.scan_chunk(st.cursor, budget, |k, v| {
            let wanted = pattern.as_ref().is_none_or(|p| glob_match(p, k))
                && ty
                    .as_ref()
                    .is_none_or(|t| v.type_name().as_bytes() == &t[..]);
            if wanted {
                out.push(Reply::Bulk(k.to_vec()));
            }
        });
        st.cursor = cursor;
        // Shards that are empty hold nothing a scan has to return.
        let next = if cursor != 0 {
            Some(idx)
        } else {
            (idx + 1..db.end).find(|&j| self.shards[j].len.load(Ordering::Relaxed) > 0)
        };
        match next {
            None => ScanStep::Done(scan_reply(0, std::mem::take(&mut st.out))),
            Some(j) if st.left == 0 => ScanStep::Done(scan_reply(
                (cursor << SCAN_SHARD_BITS) | (j - db.start) as u64,
                std::mem::take(&mut st.out),
            )),
            Some(j) if j == idx => ScanStep::Same,
            Some(j) => ScanStep::Shard(j),
        }
    }

    fn scan_task(&self, idx: usize, shard: &Shard, ticket: Ticket, mut st: ScanState) -> Step {
        let next = self.scan_step(idx, shard, &mut st);
        let task = Task {
            ticket,
            deferrals: 0,
            kind: Kind::Scan(st),
        };
        match next {
            ScanStep::Done(reply) => Step::Done(ticket, reply),
            ScanStep::Same => Step::Requeue(task),
            ScanStep::Shard(next) => Step::Move(next, task),
        }
    }

    /// Run a keyspace command for a script. The caller holds the exclusive
    /// side of the gate.
    pub(crate) fn apply_excl(&self, cmd: Cmd) -> Reply {
        self.apply(cmd, &mut Excl { engine: self })
    }

    /// Run one task while holding shard `idx`.
    fn step(&self, idx: usize, shard: &mut Shard, task: Task) -> Step {
        // A task carries no database: the shard it waits on names it.
        let _scope = enter_db(idx / self.cfg.shards);
        let _who = enter_ticket(task.ticket);
        let Task {
            ticket,
            deferrals,
            kind,
        } = task;
        match kind {
            Kind::Cmd(Cmd::Keys(pattern)) => {
                self.scan_keys(idx, shard, ticket, pattern, 0, Vec::new())
            }
            Kind::Keys {
                pattern,
                cursor,
                out,
            } => self.scan_keys(idx, shard, ticket, pattern, cursor, out),
            Kind::Hgetall { key, cursor, out } => self.scan_hash(shard, ticket, key, cursor, out),
            Kind::Scan(st) => self.scan_task(idx, shard, ticket, st),
            Kind::Cmd(Cmd::HGetAll(key)) => {
                let (h, _) = self.route(&key);
                shard.reap_fields(h, &key);
                if shard
                    .hash_len(h, &key)
                    .is_some_and(|n| n > self.cfg.scan_chunk)
                {
                    self.scan_hash(shard, ticket, key, 0, Vec::new())
                } else {
                    Step::Done(
                        ticket,
                        self.apply(Cmd::HGetAll(key), &mut One { idx, shard }),
                    )
                }
            }
            Kind::Cmd(Cmd::RandomKey) => match random_key(shard) {
                // Only expired keys here: try the next shard, once around.
                Reply::Nil if (deferrals as usize) + 1 < self.cfg.shards => Step::Move(
                    idx - idx % self.cfg.shards + (idx + 1) % self.cfg.shards,
                    Task {
                        ticket,
                        deferrals: deferrals + 1,
                        kind: Kind::Cmd(Cmd::RandomKey),
                    },
                ),
                reply => Step::Done(ticket, reply),
            },
            Kind::Cmd(cmd) if cmd.single_key().is_some() => {
                Step::Done(ticket, self.apply(cmd, &mut One { idx, shard }))
            }
            Kind::Cmd(cmd) => self.multi(Some((idx, shard)), ticket, deferrals, cmd),
        }
    }

    /// One chunk of KEYS on shard `idx` (rule 4).
    fn scan_keys(
        &self,
        idx: usize,
        shard: &Shard,
        ticket: Ticket,
        pattern: Vec<u8>,
        cursor: u64,
        mut out: Vec<Reply>,
    ) -> Step {
        let cursor = shard.scan_chunk(cursor, self.cfg.scan_chunk, |k, _| {
            if glob_match(&pattern, k) {
                out.push(Reply::Bulk(k.to_vec()));
            }
        });
        if cursor != 0 {
            return Step::Requeue(Task {
                ticket,
                deferrals: 0,
                kind: Kind::Keys {
                    pattern,
                    cursor,
                    out,
                },
            });
        }
        if idx + 1 == self.db_shards().end {
            return Step::Done(ticket, Reply::Array(out));
        }
        Step::Move(
            idx + 1,
            Task {
                ticket,
                deferrals: 0,
                kind: Kind::Keys {
                    pattern,
                    cursor: 0,
                    out,
                },
            },
        )
    }

    /// One chunk of HGETALL (rule 4). If the hash is deleted or replaced
    /// between chunks the reply is what was collected so far.
    fn scan_hash(
        &self,
        shard: &Shard,
        ticket: Ticket,
        key: Vec<u8>,
        cursor: u64,
        mut out: Vec<Reply>,
    ) -> Step {
        let (h, _) = self.route(&key);
        match shard.hgetall_chunk(h, &key, cursor, self.cfg.scan_chunk, &mut out) {
            HashScan::WrongType if out.is_empty() => Step::Done(ticket, wrongtype()),
            HashScan::Missing | HashScan::WrongType | HashScan::Next(0) => {
                Step::Done(ticket, Reply::Array(out))
            }
            HashScan::Next(cursor) => Step::Requeue(Task {
                ticket,
                deferrals: 0,
                kind: Kind::Hgetall { key, cursor, out },
            }),
        }
    }

    /// Run a multi-key command: take every shard it needs without waiting,
    /// or give all of them back and follow the busy one.
    fn multi(
        &self,
        held: Option<(usize, &mut Shard)>,
        ticket: Ticket,
        deferrals: u32,
        cmd: Cmd,
    ) -> Step {
        let held_idx = held.as_ref().map(|(i, _)| *i);
        let set = self.shard_set(&cmd);
        let mut guards = Vec::with_capacity(set.len());
        for &i in &set {
            if Some(i) == held_idx {
                continue;
            }
            let Some(guard) = self.shards[i].data.try_lock() else {
                for (j, guard) in guards {
                    self.release(j, guard);
                }
                self.stats.multi_deferrals.fetch_add(1, Ordering::Relaxed);
                let deferrals = deferrals + 1;
                if deferrals >= self.cfg.multi_escalate_after {
                    self.stats.multi_escalations.fetch_add(1, Ordering::Relaxed);
                    return Step::Escalate(ticket, cmd.in_db(current_db()));
                }
                return Step::Move(
                    i,
                    Task {
                        ticket,
                        deferrals,
                        kind: Kind::Cmd(cmd),
                    },
                );
            };
            guards.push((i, guard));
        }
        let reply = self.apply(
            cmd,
            &mut Many {
                held,
                guards: &mut guards,
            },
        );
        for (j, guard) in guards {
            self.release(j, guard);
        }
        Step::Done(ticket, reply)
    }

    /// Unlock a shard, then make sure a task that arrived while it was
    /// locked gets a visit.
    fn release(&self, idx: usize, mut guard: MutexGuard<'_, Shard>) {
        if let Some(track) = guard.take_repl() {
            self.log_stray(idx, &guard, track);
        }
        let slot = &self.shards[idx];
        slot.publish(&guard);
        drop(guard);
        dekker_fence();
        if !slot.queue.is_empty() {
            self.kick(idx);
        }
    }

    fn kick(&self, idx: usize) {
        if self.shards[idx].kicked.swap(true, Ordering::SeqCst) {
            return;
        }
        self.stats.kicks.fetch_add(1, Ordering::Relaxed);
        self.service_q.push(idx);
        if let Some(hooks) = self.hooks() {
            hooks.wake_any();
        }
    }

    /// Run a bounded batch of queued tasks under the lock (rule 2).
    fn drain(&self, idx: usize, shard: &mut Shard, batch: &mut Batch) {
        let slot = &self.shards[idx];
        let mut ran = 0;
        while ran < self.cfg.drain_batch {
            let Some(task) = slot.queue.pop() else { break };
            batch.absorb(self.step(idx, shard, task));
            ran += 1;
        }
        if ran > 0 {
            self.stats
                .tasks_run
                .fetch_add(ran as u64, Ordering::Relaxed);
        }
        // Unfinished scans go behind whatever arrived in the meantime, and
        // run again under a later lock hold.
        for task in batch.requeue.drain(..) {
            slot.queue.push(task);
        }
        // Every lock hold also pays a little towards expiry, unless DEBUG
        // SET-ACTIVE-EXPIRE 0 turned the sweep off.
        if self.active_expire.load(Ordering::Relaxed) {
            shard.sweep_due();
        }
    }

    /// Deliver what a lock hold produced. Called with no shard locked.
    fn finish(&self, batch: Batch) {
        debug_assert!(batch.requeue.is_empty());
        for (idx, task) in batch.moves {
            self.shards[idx].queue.push(task);
            self.kick(idx);
        }
        if batch.done.is_empty() && batch.esc.is_empty() {
            return;
        }
        // Without hooks the event loops are gone and nobody is waiting.
        let Some(hooks) = self.hooks() else { return };
        for (ticket, reply) in batch.done {
            hooks.complete(ticket, reply);
        }
        for (ticket, cmd) in batch.esc {
            hooks.escalate(cmd, ticket);
        }
    }

    fn enqueue(&self, idx: usize, task: Task) {
        let slot = &self.shards[idx];
        slot.queue.push(task);
        dekker_fence();
        // If the holder unlocked just before the push it did not see the
        // task; in that case the lock is free now and we run it ourselves.
        if let Some(mut guard) = slot.data.try_lock() {
            let mut batch = Batch::default();
            self.drain(idx, &mut guard, &mut batch);
            self.release(idx, guard);
            self.finish(batch);
        }
    }

    /// Execute a `Class::Shared` command for an event loop. The caller is
    /// inside the shared side of the gate. `None` means the command was
    /// parked and its reply will arrive through `Hooks::complete` (or the
    /// command through `Hooks::escalate`).
    pub fn submit(&self, cmd: Cmd, ticket: Ticket) -> Option<Reply> {
        let _who = enter_ticket(ticket);
        debug_assert_eq!(cmd.class(), Class::Shared);
        if matches!(cmd, Cmd::Keys(_)) {
            self.enqueue(self.db_shards().start, Task::new(ticket, cmd));
            return None;
        }
        if matches!(cmd, Cmd::RandomKey) {
            let Some(idx) = self.random_shards(self.db_shards()).next() else {
                return Some(Reply::Nil);
            };
            self.enqueue(idx, Task::new(ticket, cmd));
            return None;
        }
        let cmd = match cmd {
            Cmd::Scan {
                cursor,
                pattern,
                count,
                ty,
            } => {
                let (idx, st) = match self.scan_start(cursor, pattern, count, ty) {
                    Ok(start) => start,
                    Err(done) => return Some(done),
                };
                let task = Task {
                    ticket,
                    deferrals: 0,
                    kind: Kind::Scan(st),
                };
                self.enqueue(idx, task);
                return None;
            }
            cmd => cmd,
        };
        let Some(key) = cmd.single_key() else {
            return match self.multi(None, ticket, 0, cmd) {
                Step::Done(_, reply) => Some(reply),
                other => {
                    let mut batch = Batch::default();
                    batch.absorb(other);
                    self.finish(batch);
                    None
                }
            };
        };
        let idx = self.route(key).1;
        let Some(mut guard) = self.shards[idx].data.try_lock() else {
            self.stats.deferred.fetch_add(1, Ordering::Relaxed);
            self.enqueue(idx, Task::new(ticket, cmd));
            return None;
        };
        let mut batch = Batch::default();
        let reply = match self.step(idx, &mut guard, Task::new(ticket, cmd)) {
            Step::Done(_, reply) => Some(reply),
            other => {
                batch.absorb(other);
                None
            }
        };
        self.drain(idx, &mut guard, &mut batch);
        self.release(idx, guard);
        self.finish(batch);
        reply
    }

    /// `submit`, for a caller that encodes RESP into `out`: a GET that finds
    /// a string writes its bulk reply there, from the value in place,
    /// without copying it out first. The commonest command, and its reply
    /// is the same in RESP2 and RESP3.
    pub fn submit_into(&self, cmd: Cmd, ticket: Ticket, out: &mut Vec<u8>) -> Submitted {
        // Keyspace events go the long way, which publishes what the
        // command found expired.
        let key = match &cmd {
            Cmd::Get(key) if self.versions.notify() == 0 => key,
            _ => {
                return match self.submit(cmd, ticket) {
                    Some(reply) => Submitted::Reply(reply),
                    None => Submitted::Parked,
                }
            }
        };
        let _who = enter_ticket(ticket);
        let (h, idx) = self.route(key);
        let Some(mut guard) = self.shards[idx].data.try_lock() else {
            self.stats.deferred.fetch_add(1, Ordering::Relaxed);
            self.enqueue(idx, Task::new(ticket, cmd));
            return Submitted::Parked;
        };
        if self.versions.hash_ttls() != 0 {
            // As `apply` does: a hash whose fields have all expired is gone.
            guard.reap_fields(h, key);
        }
        let done = match guard.get_into(h, key, out) {
            None => Submitted::Written,
            Some(reply) => Submitted::Reply(reply),
        };
        let mut batch = Batch::default();
        self.drain(idx, &mut guard, &mut batch);
        self.release(idx, guard);
        self.finish(batch);
        done
    }

    pub fn has_service(&self) -> bool {
        !self.service_q.is_empty()
    }

    /// Visit a few shards whose queues need attention. The caller is inside
    /// the shared side of the gate. Returns whether more remain.
    pub fn service(&self) -> bool {
        const BUDGET: usize = 16;
        for _ in 0..BUDGET {
            let Some(idx) = self.service_q.pop() else {
                return false;
            };
            let slot = &self.shards[idx];
            slot.kicked.store(false, Ordering::SeqCst);
            // If the lock is busy its holder looks at the queue after
            // unlocking and kicks again.
            if let Some(mut guard) = slot.data.try_lock() {
                let mut batch = Batch::default();
                self.drain(idx, &mut guard, &mut batch);
                self.release(idx, guard);
                self.finish(batch);
            }
        }
        self.has_service()
    }

    /// Lock one shard, waiting if needed, and behave like any other lock
    /// holder on the way out.
    fn with_locked<R>(&self, idx: usize, f: impl FnOnce(&mut Shard) -> R) -> R {
        let mut guard = self.shards[idx].data.lock();
        let result = f(&mut guard);
        let mut batch = Batch::default();
        self.drain(idx, &mut guard, &mut batch);
        self.release(idx, guard);
        self.finish(batch);
        result
    }

    /// Execute any command on behalf of an application thread that owns
    /// gate slot `slot` and is allowed to sleep.
    pub fn execute_blocking(&self, slot: usize, cmd: Cmd) -> Reply {
        match cmd.class() {
            Class::Conn => self.exec_conn(&cmd),
            Class::Exclusive => self.exclusive().run(cmd),
            Class::Shared => {
                self.gate.enter_blocking(slot);
                let _leave = Leave(&self.gate, slot);
                self.shared_blocking(cmd)
            }
        }
    }

    fn shared_blocking(&self, cmd: Cmd) -> Reply {
        match cmd {
            Cmd::Keys(pattern) => {
                let mut out = Vec::new();
                for idx in self.db_shards() {
                    let mut cursor = 0;
                    loop {
                        cursor = self.with_locked(idx, |shard| {
                            shard.scan_chunk(cursor, self.cfg.scan_chunk, |k, _| {
                                if glob_match(&pattern, k) {
                                    out.push(Reply::Bulk(k.to_vec()));
                                }
                            })
                        });
                        if cursor == 0 {
                            break;
                        }
                    }
                }
                Reply::Array(out)
            }
            Cmd::HGetAll(key) => {
                let (h, idx) = self.route(&key);
                if self.versions.hash_ttls() != 0 {
                    // Fields whose time has come go before the scan looks.
                    let notes = self.with_locked(idx, |shard| {
                        shard.reap_fields(h, &key);
                        shard.take_notes()
                    });
                    self.publish_notes(idx / self.cfg.shards, notes);
                }
                let mut out = Vec::new();
                let mut cursor = 0;
                loop {
                    let chunk = self.cfg.scan_chunk;
                    match self.with_locked(idx, |shard| {
                        shard.hgetall_chunk(h, &key, cursor, chunk, &mut out)
                    }) {
                        HashScan::WrongType if out.is_empty() => return wrongtype(),
                        HashScan::Missing | HashScan::WrongType | HashScan::Next(0) => {
                            return Reply::Array(out)
                        }
                        HashScan::Next(next) => cursor = next,
                    }
                }
            }
            Cmd::Scan {
                cursor,
                pattern,
                count,
                ty,
            } => {
                let (mut idx, mut st) = match self.scan_start(cursor, pattern, count, ty) {
                    Ok(start) => start,
                    Err(done) => return done,
                };
                loop {
                    match self.with_locked(idx, |shard| self.scan_step(idx, shard, &mut st)) {
                        ScanStep::Done(reply) => return reply,
                        ScanStep::Same => {}
                        ScanStep::Shard(next) => idx = next,
                    }
                }
            }
            Cmd::RandomKey => self
                .random_shards(self.db_shards())
                .map(|idx| self.with_locked(idx, random_key))
                .find(|reply| *reply != Reply::Nil)
                .unwrap_or(Reply::Nil),
            cmd => match cmd.single_key() {
                Some(key) => {
                    let idx = self.route(key).1;
                    self.with_locked(idx, |shard| self.apply(cmd, &mut One { idx, shard }))
                }
                None => {
                    // Sorted order, so two blocking callers cannot deadlock;
                    // event loops never wait at all.
                    let set = self.shard_set(&cmd);
                    let mut guards: Vec<_> = set
                        .iter()
                        .map(|&i| (i, self.shards[i].data.lock()))
                        .collect();
                    let reply = self.apply(
                        cmd,
                        &mut Many {
                            held: None,
                            guards: &mut guards,
                        },
                    );
                    for (i, guard) in guards {
                        self.release(i, guard);
                    }
                    reply
                }
            },
        }
    }

    /// Take the exclusive side of the gate. The caller must not be inside
    /// the shared side, and must be allowed to sleep.
    pub fn exclusive(&self) -> Exclusive<'_> {
        let guard = self.gate.exclusive();
        self.stats.exclusive.fetch_add(1, Ordering::Relaxed);
        Exclusive {
            engine: self,
            guard: Some(guard),
        }
    }

    /// Execute a `Class::Conn` command (rule 3): no lock, no gate, so it
    /// works while a script holds the whole node.
    /// Users and their permissions.
    pub fn acl(&self) -> &Acl {
        &self.acl
    }

    /// AUTH: the user these credentials belong to. Without a name they are
    /// the password of "default".
    pub fn auth(&self, user: Option<&[u8]>, pass: &[u8]) -> Result<Arc<User>, Reply> {
        match user {
            Some(name) => self.acl.auth(name, pass),
            None if self.acl.default_user().open() => Err(Reply::err(
                "ERR AUTH <password> called without any password configured for the default \
                 user. Are you sure your configuration is correct?",
            )),
            None => self.acl.auth(b"default", pass),
        }
    }

    /// The HELLO reply for a connection that speaks `proto` from now on.
    pub fn hello(&self, proto: u8) -> Reply {
        let bulk = |s: &str| Reply::Bulk(s.as_bytes().to_vec());
        Reply::Array(vec![
            bulk("server"),
            bulk("hcache"),
            bulk("version"),
            bulk(COMPAT_VERSION),
            bulk("proto"),
            Reply::Int(proto.into()),
            bulk("id"),
            Reply::Int(0),
            bulk("mode"),
            bulk(if self.node.is_some() {
                "cluster"
            } else {
                "standalone"
            }),
            bulk("role"),
            bulk(if self.versions.replica() {
                "replica"
            } else {
                "master"
            }),
            bulk("modules"),
            Reply::Array(Vec::new()),
        ])
    }

    pub fn exec_conn(&self, cmd: &Cmd) -> Reply {
        match cmd {
            Cmd::Ping(None) => Reply::Simple("PONG".into()),
            Cmd::Ping(Some(msg)) | Cmd::Echo(msg) => Reply::Bulk(msg.clone()),
            Cmd::Quit => Reply::Ok,
            Cmd::Static(reply) => reply.clone(),
            Cmd::Info(asked) => Reply::Bulk(info_sections(&self.info(), asked).into_bytes()),
            Cmd::Dbsize => Reply::Int(self.db_len() as i64),
            // The connection keeps its database; see `TxnState`.
            Cmd::Select(_) => Reply::err("ERR SELECT is not allowed in this context"),
            Cmd::ScriptKill => self.scripts.kill(),
            // Without a server there is no connection to log in: good
            // credentials are confirmed and change nothing.
            Cmd::Hello(proto, auth) => match auth {
                Some((user, pass)) => match self.acl.auth(user, pass) {
                    Ok(_) => self.hello(proto.unwrap_or(2)),
                    Err(e) => e,
                },
                None => self.hello(proto.unwrap_or(2)),
            },
            Cmd::Auth(user, pass) => match self.auth(user.as_deref(), pass) {
                Ok(_) => Reply::Ok,
                Err(e) => e,
            },
            Cmd::Acl(args) => self.acl.command(args, &self.acl.default_user()),
            Cmd::Command(args) => cmdtable::command(args),
            Cmd::ConfigGet(pattern) => self.config_get(pattern),
            Cmd::ConfigSet(pairs) => self.config_set(pairs),
            // Without a server there is no connection to name or list.
            Cmd::Client(sub) => match &**sub {
                ClientCmd::SetName(_) | ClientCmd::SetInfo { .. } => Reply::Ok,
                ClientCmd::GetName => Reply::Nil,
                ClientCmd::Id => Reply::Int(0),
                ClientCmd::Info | ClientCmd::List => Reply::Bulk(Vec::new()),
                // Invalidations are sent to network connections.
                ClientCmd::Tracking(None) => Reply::Ok,
                ClientCmd::Tracking(Some(_)) | ClientCmd::Caching(_) => {
                    Reply::err("ERR CLIENT TRACKING needs a network connection")
                }
                ClientCmd::TrackingInfo => Reply::Map(vec![
                    Reply::bulk("flags"),
                    Reply::Array(vec![Reply::bulk("off")]),
                    Reply::bulk("redirect"),
                    Reply::Int(-1),
                    Reply::bulk("prefixes"),
                    Reply::Array(Vec::new()),
                ]),
                ClientCmd::GetRedir => Reply::Int(-1),
                // Without a server there is no connection to close.
                ClientCmd::Kill(filter) if filter.legacy => Reply::err("ERR No such client"),
                ClientCmd::Kill(_) => Reply::Int(0),
                ClientCmd::Pause { .. } | ClientCmd::Unpause => Reply::Ok,
            },
            Cmd::Monitor => Reply::err("ERR MONITOR needs a network connection"),
            Cmd::Cluster(args) => cluster::command(self.node(), args),
            Cmd::Shutdown { abort: true, .. } => Reply::err("ERR No shutdown in progress."),
            Cmd::Shutdown { failover: true, .. } if self.node.is_none() => {
                Reply::err("ERR SHUTDOWN FAILOVER is only supported in cluster mode.")
            }
            Cmd::Shutdown { .. } => Reply::err("ERR SHUTDOWN needs a server"),
            Cmd::Debug(args) => self.debug(args),
            Cmd::Asking if self.node.is_some() => Reply::Ok,
            Cmd::Asking => Reply::err(cluster::DISABLED),
            // Without the server no replica can connect.
            Cmd::Role => Reply::Array(vec![
                Reply::bulk("master"),
                Reply::Int(i64::try_from(self.repl.info().offset).unwrap_or(i64::MAX)),
                Reply::Array(Vec::new()),
            ]),
            Cmd::Psync(..) | Cmd::Sync => Reply::err("ERR replication needs the server"),
            Cmd::Replconf(_) => Reply::Ok,
            Cmd::ReplicaOf(_) => Reply::err("ERR REPLICAOF needs the server"),
            Cmd::Failover(_) => Reply::err("ERR FAILOVER needs the server"),
            Cmd::Wait(..) => Reply::Int(0),
            Cmd::SlowLog(sub) => self.slowlog_cmd(*sub),
            Cmd::Reset => Reply::Simple("RESET".into()),
            Cmd::Time => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                Reply::Array(vec![
                    Reply::Bulk(now.as_secs().to_string().into_bytes()),
                    Reply::Bulk(now.subsec_micros().to_string().into_bytes()),
                ])
            }
            Cmd::Save if self.cfg.rdb_file.is_some() => match self.rdb_save_begin() {
                true => self.rdb_save_reply(),
                false => Reply::err(crate::rdbfile::SAVING),
            },
            // Here the caller waits: only a server has threads to spare.
            Cmd::BgSave if self.cfg.rdb_file.is_some() => match self.rdb_save_begin() {
                true => {
                    // How it went is kept for INFO, as Redis keeps it.
                    let _ = self.rdb_save_finish();
                    self.rewrite_data_file();
                    Reply::Simple("Background saving started".into())
                }
                false => Reply::err(crate::rdbfile::SAVING),
            },
            Cmd::BgSave if self.persist_on.load(Ordering::SeqCst) => {
                self.bgsave.store(true, Ordering::SeqCst);
                Reply::Simple("Background saving started".into())
            }
            Cmd::BgSave => Reply::err("ERR this database was opened without a data file"),
            Cmd::Save => match self.sync_now() {
                Some(Ok(())) => Reply::Ok,
                Some(Err(e)) => Reply::Error(format!("ERR writing the data file failed: {e}")),
                None => Reply::err("ERR this database was opened without a data file"),
            },
            Cmd::LastSave => Reply::Int(self.last_save.load(Ordering::Relaxed) as i64),
            // As things stand, without waiting: a server or a session that
            // can wait brings the disk up to date before it gets here, and
            // a transaction cannot wait. Under `appendfsync always` the
            // reply of the transaction itself waits for the disk.
            Cmd::WaitAof(local) => {
                if !self.has_data_file() {
                    if *local {
                        return Reply::err(
                            "ERR WAITAOF cannot be used when numlocal is set but appendonly is \
                             disabled.",
                        );
                    }
                    return Reply::Array(vec![Reply::Int(0), Reply::Int(0)]);
                }
                let synced = self.fsync() == Fsync::Always;
                Reply::Array(vec![Reply::Int(synced.into()), Reply::Int(0)])
            }
            // Subscribers are network connections; without a listener
            // there are none.
            Cmd::Publish(channel, payload) => Reply::Int(
                self.hooks()
                    .map_or(0, |hooks| hooks.publish(channel, payload)) as i64,
            ),
            Cmd::SPublish(channel, payload) => Reply::Int(
                self.hooks()
                    .map_or(0, |hooks| hooks.spublish(channel, payload)) as i64,
            ),
            Cmd::PubSub(query) => self
                .hooks()
                .and_then(|hooks| hooks.pubsub(query))
                .unwrap_or_else(|| match query {
                    PubSubQuery::Channels(_) | PubSubQuery::ShardChannels(_) => {
                        Reply::Array(Vec::new())
                    }
                    PubSubQuery::NumSub(names) | PubSubQuery::ShardNumSub(names) => Reply::Array(
                        names
                            .iter()
                            .flat_map(|name| [Reply::Bulk(name.clone()), Reply::Int(0)])
                            .collect(),
                    ),
                    PubSubQuery::NumPat => Reply::Int(0),
                }),
            Cmd::Sub(_) => Reply::err("ERR subscribing needs a network connection"),
            Cmd::ScriptExists(shas) => self.scripts.exists(shas),
            Cmd::ScriptFlush => self.scripts.flush(),
            _ => Reply::err("ERR internal: not a connection command"),
        }
    }

    fn info(&self) -> String {
        let s = self.stats();
        let uptime = self.started.elapsed().as_secs();
        let used = mem::used();
        format!(
            "# Server\r\nredis_version:{COMPAT_VERSION}\r\nhcache_version:{}\r\nredis_mode:{}\r\n\
             os:{}\r\narch_bits:{}\r\nprocess_id:{}\r\nrun_id:{}\r\nuptime_in_seconds:{uptime}\r\n\
             uptime_in_days:{}\r\n\
             {}# Cluster\r\ncluster_enabled:{}\r\n",
            env!("CARGO_PKG_VERSION"),
            if self.node.is_some() { "cluster" } else { "standalone" },
            std::env::consts::OS,
            usize::BITS,
            std::process::id(),
            self.run_id,
            uptime / 86_400,
            self.repl_section(
                &self
                    .hooks()
                    .and_then(|hooks| hooks.replication())
                    .unwrap_or_else(|| "role:master\r\nconnected_slaves:0\r\n".into())
            ),
            u8::from(self.node.is_some()),
        ) + &self
            .hooks()
            .map(|hooks| hooks.info())
            .unwrap_or_else(|| format!("# Stats\r\n{}", self.keyspace_stats()))
            + &format!(
            "# HCache spike\r\nshards:{}\r\ndatabases:{}\r\ndeferred:{}\r\ntasks_run:{}\r\nkicks:{}\r\nmulti_deferrals:{}\r\n\
             multi_escalations:{}\r\nexclusive:{}\r\n# Memory\r\nused_memory:{used}\r\n\
             used_memory_human:{:.2}M\r\nmaxmemory:{}\r\n\
             maxmemory_policy:{}\r\nevicted_keys:{}\r\nlazyfree_pending_objects:{}\r\n\
             lazyfreed_objects:{}\r\n",
            self.cfg.shards,
            self.cfg.databases,
            s.deferred,
            s.tasks_run,
            s.kicks,
            s.multi_deferrals,
            s.multi_escalations,
            s.exclusive,
            used as f64 / (1 << 20) as f64,
            self.maxmemory.load(Ordering::Relaxed),
            self.policy().name(),
            s.evicted,
            self.lazy.pending(),
            self.lazy.freed(),
        ) + &format!(
            "# Persistence\r\nloading:0\r\npersistence:{}\r\nlast_save_time:{}\r\n\
             last_write_error:{}\r\nrdb_last_save_time:{}\r\nrdb_bgsave_in_progress:{}\r\n\
             rdb_last_bgsave_status:{}\r\n",
            u8::from(self.persist_on.load(Ordering::SeqCst)),
            self.last_save.load(Ordering::Relaxed),
            self.persist_error.lock().as_deref().unwrap_or(""),
            self.last_save.load(Ordering::Relaxed),
            u8::from(self.rdb_saves.in_progress()),
            if self.rdb_saves.failed() { "err" } else { "ok" }
        ) + &self.keyspace()
    }

    /// The lines of INFO's `# Stats` the engine knows: reads that found
    /// their key and that did not, and keys gone because they expired.
    pub fn keyspace_stats(&self) -> String {
        let mut sums = [0; 3];
        for slot in self.shards.iter() {
            for (sum, n) in sums.iter_mut().zip(&slot.stats) {
                *sum += n.load(Ordering::Relaxed);
            }
        }
        let [hits, misses, expired] = sums;
        format!("expired_keys:{expired}\r\nkeyspace_hits:{hits}\r\nkeyspace_misses:{misses}\r\n")
    }

    /// The `# Keyspace` section of INFO: one line per database with keys.
    fn keyspace(&self) -> String {
        let mut out = String::from("# Keyspace\r\n");
        for db in 0..self.cfg.databases {
            let keys = self.with_db(db, || self.db_len());
            if keys > 0 {
                out += &format!("db{db}:keys={keys},expires=0,avg_ttl=0\r\n");
            }
        }
        out
    }
}

/// The node, held exclusively. Shared participants are locked out until
/// this is dropped.
pub struct Exclusive<'a> {
    engine: &'a Engine,
    guard: Option<ExclusiveGuard<'a>>,
}

impl Exclusive<'_> {
    pub fn run(&mut self, cmd: Cmd) -> Reply {
        let engine = self.engine;
        match cmd {
            Cmd::In(db, inner) => {
                let _scope = enter_db(db.into());
                self.run(*inner)
            }
            // COUNTKEYSINSLOT and GETKEYSINSLOT: a slot's keys share a shard.
            Cmd::Cluster(args) => {
                if engine.node.is_none() {
                    return Reply::err(cluster::DISABLED);
                }
                let (slot, most) = match cluster::keys_request(&args) {
                    Ok(request) => request,
                    Err(e) => return e,
                };
                let idx = current_db() * engine.cfg.shards + engine.slot_shard(slot);
                let (mut count, mut keys) = (0, Vec::new());
                Excl { engine }.with(idx, |s| {
                    s.scan_chunk(0, usize::MAX, |k, _| {
                        if cluster::key_slot(k) == slot {
                            count += 1;
                            if most.is_some_and(|most| keys.len() < most) {
                                keys.push(Reply::Bulk(k.to_vec()));
                            }
                        }
                    })
                });
                match most {
                    None => Reply::Int(count),
                    Some(_) => Reply::Array(keys),
                }
            }
            Cmd::FlushDb(lazy) => {
                let lazy = lazy.unwrap_or_else(|| engine.lazy.is_on(Reason::UserFlush));
                for idx in engine.db_shards() {
                    let slot = &engine.shards[idx];
                    slot.data.lock().clear_tracked(lazy);
                    slot.len.store(0, Ordering::Relaxed);
                }
                engine.versions.bump_all();
                engine.repl_command(Some(current_db()), &[b"FLUSHDB"]);
                Reply::Ok
            }
            Cmd::Move { key, db } => {
                let (h, si) = engine.route(&key);
                let di = match usize::try_from(db) {
                    Ok(db) if db < engine.cfg.databases => engine.route_in(db, &key).1,
                    _ => return Reply::err(DB_RANGE),
                };
                if si == di {
                    return Reply::err("ERR source and destination objects are the same");
                }
                let mut a = Excl { engine };
                if !a.with(si, |s| s.exists(h, &key)) || a.with(di, |s| s.exists(h, &key)) {
                    return Reply::Int(0);
                }
                a.with(si, |s| s.reap_fields(h, &key));
                if let Some(entry) = a.with(si, |s| s.take(h, &key)) {
                    a.with(di, |s| s.restore(h, &key, entry));
                    engine.emit(current_db(), notify::GENERIC, "move_from", &key);
                    engine.emit(di / engine.cfg.shards, notify::GENERIC, "move_to", &key);
                }
                // As the state of both sides: a replica may hold an expired
                // copy in the other database, which MOVE would refuse.
                for i in [si, di] {
                    let mut shard = engine.shards[i].data.lock();
                    if let Some(track) = shard.take_repl() {
                        engine.log_stray(i, &shard, track);
                    }
                }
                Reply::Int(1)
            }
            Cmd::SwapDb(first, second) => {
                let index = |db: i64| {
                    usize::try_from(db)
                        .ok()
                        .filter(|db| *db < engine.cfg.databases)
                };
                let (Some(first), Some(second)) = (index(first), index(second)) else {
                    return Reply::err(DB_RANGE);
                };
                if first == second {
                    return Reply::Ok;
                }
                engine
                    .used_dbs
                    .fetch_max(first.max(second) + 1, Ordering::SeqCst);
                let shards = engine.cfg.shards;
                for local in 0..shards {
                    let (i, j) = (first * shards + local, second * shards + local);
                    let (one, other) = (&engine.shards[i], &engine.shards[j]);
                    let (mut a, mut b) = (one.data.lock(), other.data.lock());
                    // What is still to send belongs to the database before.
                    for (idx, shard) in [(i, &mut a), (j, &mut b)] {
                        if let Some(track) = shard.take_repl() {
                            engine.log_stray(idx, shard, track);
                        }
                    }
                    a.swap_data(&mut b);
                    one.len.store(a.len(), Ordering::Relaxed);
                    other.len.store(b.len(), Ordering::Relaxed);
                }
                engine.versions.bump_all();
                let (first, second) = (first.to_string(), second.to_string());
                engine.repl_command(None, &[b"SWAPDB", first.as_bytes(), second.as_bytes()]);
                Reply::Ok
            }
            Cmd::FlushAll(lazy) => {
                let lazy = lazy.unwrap_or_else(|| engine.lazy.is_on(Reason::UserFlush));
                for slot in engine.shards.iter() {
                    slot.data.lock().clear(lazy);
                    slot.len.store(0, Ordering::Relaxed);
                }
                engine.versions.bump_all();
                engine.flush_gen.fetch_add(1, Ordering::SeqCst);
                engine.repl_command(None, &[b"FLUSHALL"]);
                Reply::Ok
            }
            Cmd::ExecBlock { queue, watched } => {
                for (db, key, seen) in &watched {
                    let (h, idx) = engine.route_in((*db).into(), key);
                    // A watched key that expired counts as modified.
                    Excl { engine }.with(idx, |shard| shard.purge(h, key));
                    if engine.versions.get(h) != *seen {
                        return Reply::NilArray;
                    }
                }
                let _atomic = engine.repl_atomic();
                let replies = queue.into_iter().map(|cmd| {
                    let as_map = cmd.replies_map();
                    match self.run(cmd) {
                        Reply::Array(items) if as_map => Reply::Map(items),
                        reply => reply,
                    }
                });
                Reply::Array(replies.collect())
            }
            Cmd::Eval {
                script,
                keys,
                args,
                read_only,
            } => {
                let _atomic = engine.repl_atomic();
                engine.scripts.set_read_only(read_only);
                engine.scripts.eval(engine, &script, keys, args)
            }
            Cmd::EvalSha {
                sha,
                keys,
                args,
                read_only,
            } => {
                let _atomic = engine.repl_atomic();
                engine.scripts.set_read_only(read_only);
                engine.scripts.evalsha(engine, &sha, keys, args)
            }
            Cmd::ScriptLoad(script) => engine.scripts.load(&script),
            Cmd::Function(args) => {
                let reply = engine.scripts.function(&args);
                if !matches!(reply, Reply::Error(_)) && crate::repl::function_writes(&args) {
                    let args: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
                    engine.repl_command(None, &args);
                }
                reply
            }
            Cmd::FCall {
                name,
                keys,
                args,
                read_only,
            } => {
                let _atomic = engine.repl_atomic();
                engine.scripts.fcall(engine, &name, keys, args, read_only)
            }
            // In a transaction: the gate is ours already.
            Cmd::BgSave if engine.cfg.rdb_file.is_some() => match engine.rdb_save_begin() {
                true => {
                    engine.rdb_save_held();
                    Reply::Simple("Background saving started".into())
                }
                false => Reply::err(crate::rdbfile::SAVING),
            },
            cmd if cmd.class() == Class::Conn => engine.exec_conn(&cmd),
            cmd => engine.apply_excl(cmd),
        }
    }
}

/// A transaction or script whose writes go between MULTI and EXEC
/// (`Engine::repl_atomic`).
struct Atomic<'a>(Option<&'a Engine>);

impl Drop for Atomic<'_> {
    fn drop(&mut self) {
        if let Some(engine) = self.0 {
            if engine.repl.close() {
                if let Some(hooks) = engine.hooks() {
                    hooks.replicate();
                }
            }
        }
    }
}

impl Drop for Exclusive<'_> {
    fn drop(&mut self) {
        // Reopen the gate first: a worker woken by the hook must find it
        // open, or it goes back to sleep with nobody left to wake it.
        self.guard.take();
        if let Some(hooks) = self.engine.hooks() {
            hooks.gate_opened();
        }
    }
}

/// A Sentinel's default weight for a replica it may promote.
const DEFAULT_REPLICA_PRIORITY: u64 = 100;

fn yes_no(on: bool) -> String {
    if on { "yes" } else { "no" }.to_string()
}

/// The sections of INFO's `text` that `asked` names, as Redis picks them:
/// every section for none, `default`, `all` or `everything`; an unknown
/// name adds nothing.
fn info_sections(text: &str, asked: &[Bytes]) -> String {
    let whole = |name: &Bytes| matches!(name.as_slice(), b"default" | b"all" | b"everything");
    if asked.is_empty() || asked.iter().any(whole) {
        return text.to_string();
    }
    let mut out = String::new();
    let mut keep = false;
    for line in text.split_inclusive("\r\n") {
        if let Some(name) = line.strip_prefix("# ") {
            let name = name.trim_end().to_ascii_lowercase();
            keep = asked.iter().any(|a| a.as_slice() == name.as_bytes());
        }
        if keep {
            out.push_str(line);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::hash_tag;

    #[test]
    fn hash_tag_rules() {
        assert_eq!(hash_tag(b"a{b}c"), Some(&b"b"[..]));
        assert_eq!(hash_tag(b"{}{b}"), None);
        assert_eq!(hash_tag(b"a{b"), None);
        assert_eq!(hash_tag(b"a}b{c}"), Some(&b"c"[..]));
        assert_eq!(hash_tag(b"{{x}}"), Some(&b"{x"[..]));
        assert_eq!(hash_tag(b""), None);
    }
}
