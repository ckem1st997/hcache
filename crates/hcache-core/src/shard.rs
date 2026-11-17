//! One shard of the keyspace. Everything here takes `&mut Shard` (rule 6):
//! the code does not know or care how exclusive access was obtained.
//!
//! Expiry is lazy first: a key whose deadline has passed is treated as
//! missing by every read and removed by the next write that touches it.
//! `sweep_due` removes the ones nobody touches.

use crate::bloom::Bloom;
use crate::clock::Clock;
use crate::cmd::{Cond, Expire, SetOpts, NOT_INT};
use crate::dict::{hash_bytes, Dict};
use crate::json;
use crate::lazyfree::{LazyFree, Reason};
use crate::resp::{parse_i64, Reply};
use crate::stream::Stream;
use crate::zset::ZSet;
use parking_lot::RwLock;
use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

mod coll;
pub(crate) mod dump;
mod fields;

pub use fields::{Entry, FieldTtl, Note};

#[derive(Clone)]
/// The collections are boxed so that an entry of the keyspace fits in one
/// cache line: most keys hold strings.
pub enum Value {
    Str(Vec<u8>),
    Hash(Box<Dict<Vec<u8>>>),
    List(VecDeque<Vec<u8>>),
    Set(Box<Dict<()>>),
    ZSet(Box<ZSet>),
    Stream(Box<Stream>),
    Bloom(Box<Bloom>),
    Json(Box<json::Node>),
}

const _: () = assert!(std::mem::size_of::<Value>() <= 32);
const _: () = assert!(crate::dict::entry_size::<Value>() <= 64);

/// The steps of a lookup, in the order their lines are needed: see
/// `Shard::prefetch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Prefetch {
    Bucket,
    Entry,
    Value,
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::Hash(_) => "hash",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
            Value::Stream(_) => "stream",
            Value::Bloom(_) => "bloomfltr",
            Value::Json(_) => "ReJSON-RL",
        }
    }
}

pub const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
pub const NOT_HLL: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";
/// A sparse HyperLogLog whose opcodes do not add up.
pub const HLL_CORRUPT: &str = "INVALIDOBJ Corrupted HLL object detected";
const OVERFLOW: &str = "ERR increment or decrement would overflow";
/// Longest string value, as in Redis (`proto-max-bulk-len`).
pub(crate) const MAX_STRING: usize = 512 << 20;
/// Pause between two sweeps of one shard, and entries looked at per round.
const SWEEP_INTERVAL_MS: u64 = 50;
const SWEEP_BUDGET: usize = 64;
/// A round that found this share of its sample expired is followed by
/// another one right away, up to `SWEEP_ROUNDS`.
const SWEEP_AGAIN_PERCENT: usize = 25;
const SWEEP_ROUNDS: usize = 4;

fn wrongtype() -> Reply {
    Reply::err(WRONGTYPE)
}

/// What to remove when memory is full (`maxmemory-policy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Policy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysRandom,
    VolatileRandom,
    VolatileTtl,
    AllKeysLfu,
    VolatileLfu,
}

impl Policy {
    pub const ALL: [Policy; 8] = [
        Policy::NoEviction,
        Policy::AllKeysLru,
        Policy::VolatileLru,
        Policy::AllKeysRandom,
        Policy::VolatileRandom,
        Policy::VolatileTtl,
        Policy::AllKeysLfu,
        Policy::VolatileLfu,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Policy::NoEviction => "noeviction",
            Policy::AllKeysLru => "allkeys-lru",
            Policy::VolatileLru => "volatile-lru",
            Policy::AllKeysRandom => "allkeys-random",
            Policy::VolatileRandom => "volatile-random",
            Policy::VolatileTtl => "volatile-ttl",
            Policy::AllKeysLfu => "allkeys-lfu",
            Policy::VolatileLfu => "volatile-lfu",
        }
    }

    /// Whether the policy goes by how often a key is used.
    pub fn is_lfu(self) -> bool {
        matches!(self, Policy::AllKeysLfu | Policy::VolatileLfu)
    }

    pub fn from_name(name: &[u8]) -> Option<Policy> {
        Policy::ALL
            .into_iter()
            .find(|p| name.eq_ignore_ascii_case(p.name().as_bytes()))
    }
}

/// Keys looked at to find one worth evicting.
const EVICT_SAMPLE: usize = 5;

/// Result of one chunk of a walk over a hash.
pub enum HashScan {
    Missing,
    WrongType,
    /// Next cursor; 0 means the hash has been fully visited.
    Next(u64),
}

/// Modification counters behind WATCH. A key maps to one of a fixed number
/// of counters, so a change is never missed, while two keys that share a
/// counter make each other's transactions abort (rare, and harmless: the
/// client retries).
pub struct Versions {
    stripes: Box<[AtomicU64]>,
    /// Clients blocked until a key changes. While there are none, a change
    /// costs nothing more than its counter.
    waiting: AtomicUsize,
    hooks: RwLock<Option<Arc<dyn crate::engine::Hooks>>>,
    /// Hashes that have fields with a deadline, over all shards. While
    /// there are none, commands skip the check for fields that are due.
    hash_ttls: AtomicUsize,
    /// The classes of keyspace notifications that are switched on.
    notify: AtomicU32,
    /// This node is a replica: keys and fields whose time has come stay
    /// until the primary deletes them, and nothing is evicted.
    replica: AtomicBool,
}

const VERSION_STRIPES: usize = 1 << 16;

impl Default for Versions {
    fn default() -> Self {
        Versions {
            stripes: (0..VERSION_STRIPES).map(|_| AtomicU64::new(0)).collect(),
            waiting: AtomicUsize::new(0),
            hooks: RwLock::new(None),
            hash_ttls: AtomicUsize::new(0),
            notify: AtomicU32::new(0),
            replica: AtomicBool::new(false),
        }
    }
}

impl Versions {
    fn stripe(&self, hash: u64) -> &AtomicU64 {
        // The middle bits: the high ones pick the shard and the low ones
        // the bucket, and neighbours in either should not share a counter.
        &self.stripes[(hash >> 16) as usize & (VERSION_STRIPES - 1)]
    }

    pub fn get(&self, hash: u64) -> u64 {
        self.stripe(hash).load(Ordering::SeqCst)
    }

    fn bump(&self, hash: u64, key: &[u8]) {
        self.stripe(hash).fetch_add(1, Ordering::SeqCst);
        if self.waiting.load(Ordering::SeqCst) != 0 {
            let hooks = self.hooks.read().clone();
            if let Some(hooks) = hooks {
                hooks.key_changed(hash, key);
            }
        }
    }

    pub(crate) fn set_hooks(&self, hooks: Option<Arc<dyn crate::engine::Hooks>>) {
        *self.hooks.write() = hooks;
    }

    pub(crate) fn waiting(&self) -> &AtomicUsize {
        &self.waiting
    }

    pub(crate) fn hash_ttls(&self) -> usize {
        self.hash_ttls.load(Ordering::SeqCst)
    }

    pub(crate) fn notify(&self) -> u32 {
        self.notify.load(Ordering::Relaxed)
    }

    pub(crate) fn set_notify(&self, flags: u32) {
        self.notify.store(flags, Ordering::Relaxed);
    }

    pub(crate) fn replica(&self) -> bool {
        self.replica.load(Ordering::SeqCst)
    }

    pub(crate) fn set_replica(&self, on: bool) {
        self.replica.store(on, Ordering::SeqCst);
    }

    /// Every key changed (FLUSHALL).
    pub fn bump_all(&self) {
        for v in self.stripes.iter() {
            v.fetch_add(1, Ordering::SeqCst);
        }
        if self.waiting.load(Ordering::SeqCst) != 0 {
            let hooks = self.hooks.read().clone();
            if let Some(hooks) = hooks {
                hooks.keys_flushed();
            }
        }
    }
}

#[derive(Default)]
pub struct Shard {
    map: Dict<Value>,
    /// Key to absolute deadline in milliseconds. Only keys that have one.
    expires: Dict<u64>,
    clock: Arc<Clock>,
    versions: Arc<Versions>,
    sweep_cursor: u64,
    next_sweep_ms: u64,
    /// State of the generator behind SPOP, SRANDMEMBER and the like.
    rng: u64,
    /// Keys changed since the last write to disk; `None` without
    /// persistence.
    dirty: Option<Dict<()>>,
    /// Hash key to the deadlines of its fields. Only hashes that have any.
    field_ttl: Dict<FieldTtl>,
    field_cursor: u64,
    /// For keyspace notifications; filled only while they are on.
    notes: Vec<(Note, Vec<u8>)>,
    /// Where large values that go are freed.
    lazy: Arc<LazyFree>,
    /// What the replicas have to hear of; `None` while nothing is sent.
    repl: Option<Box<ReplTrack>>,
    /// Reads that found their key and that did not, and keys that expired:
    /// INFO's keyspace_hits, keyspace_misses and expired_keys.
    hits: Cell<u64>,
    misses: Cell<u64>,
    expired: u64,
}

/// The changes of a shard that replication has not sent yet.
#[derive(Default)]
pub(crate) struct ReplTrack {
    /// What commands that word their own replication (BF.*) sent, in
    /// order.
    pub own: Vec<u8>,
    /// Keys whose time came, or that made room: they go as DEL.
    pub gone: Vec<Vec<u8>>,
    /// Fields of hashes whose time came: they go as HDEL.
    pub gone_fields: Vec<(Vec<u8>, Vec<u8>)>,
    /// Keys a command changed, as often as it changed them.
    pub touched: Vec<Vec<u8>>,
}

impl ReplTrack {
    pub(crate) fn is_empty(&self) -> bool {
        self.gone.is_empty()
            && self.gone_fields.is_empty()
            && self.touched.is_empty()
            && self.own.is_empty()
    }
}

impl Shard {
    pub fn new(clock: Arc<Clock>, versions: Arc<Versions>, lazy: Arc<LazyFree>) -> Shard {
        Shard {
            clock,
            versions,
            lazy,
            ..Shard::default()
        }
    }

    /// Keys stored, including expired ones not collected yet.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn deadline(&self, h: u64, key: &[u8]) -> Option<u64> {
        if self.expires.is_empty() {
            return None;
        }
        self.expires.peek(h, key).copied()
    }

    /// Whether a deadline read from an entry (0 for none) has passed.
    fn past(&self, at: u64) -> bool {
        at != 0 && at <= self.clock.now_ms()
    }

    fn expired(&self, h: u64, key: &[u8]) -> bool {
        !self.expires.is_empty()
            && self
                .map
                .peek_timed(h, key)
                .is_some_and(|(_, at)| self.past(at))
    }

    /// The value as readers see it: an expired key is missing. The
    /// deadline comes with the entry (as in Valkey 8.1), so a key with one
    /// costs no second lookup in `expires`.
    fn live(&self, h: u64, key: &[u8]) -> Option<&Value> {
        let found = self
            .map
            .get_timed(h, key)
            .and_then(|(value, at)| (!self.past(at)).then_some(value));
        let counter = if found.is_some() {
            &self.hits
        } else {
            &self.misses
        };
        counter.set(counter.get() + 1);
        found
    }

    /// INFO's keyspace_hits, keyspace_misses and expired_keys.
    pub fn stats(&self) -> [u64; 3] {
        [self.hits.get(), self.misses.get(), self.expired]
    }

    /// `live` without counting as a use of the key.
    fn live_quiet(&self, h: u64, key: &[u8]) -> Option<&Value> {
        let (value, at) = self.map.peek_timed(h, key)?;
        (!self.past(at)).then_some(value)
    }

    /// Remove the key if its deadline has passed. Every write starts here,
    /// so below it "in the map" means "exists". On a replica every write
    /// comes from the primary, which says itself when a key goes.
    pub fn purge(&mut self, h: u64, key: &[u8]) {
        if self.expired(h, key) && !self.versions.replica() {
            self.discard(h, key, Reason::Expire);
            self.gone(key);
            self.note(Note::Expired, key);
        }
    }

    /// Record that `key` changed. Every mutation ends up here.
    fn touch(&mut self, h: u64, key: &[u8]) {
        self.versions.bump(h, key);
        if let Some(dirty) = &mut self.dirty {
            dirty.insert(h, key, ());
        }
        if let Some(repl) = &mut self.repl {
            repl.touched.push(key.to_vec());
        }
    }

    /// The key `discard` just removed went by itself, not by a command:
    /// the replicas hear of it as a DEL.
    fn gone(&mut self, key: &[u8]) {
        if let Some(repl) = &mut self.repl {
            debug_assert_eq!(repl.touched.last().map(Vec::as_slice), Some(key));
            repl.touched.pop();
            repl.gone.push(key.to_vec());
        }
    }

    /// Start or stop keeping what the replicas have to hear of.
    pub(crate) fn track_repl(&mut self, on: bool) {
        self.repl = on.then(Box::default);
    }

    /// What the replicas have not heard of yet, if anything.
    pub(crate) fn take_repl(&mut self) -> Option<ReplTrack> {
        let repl = self.repl.as_mut().filter(|r| !r.is_empty())?;
        Some(std::mem::take(&mut **repl))
    }

    /// The state of `key` as replication sends it: the value and its
    /// deadline, or `None` when the key is gone.
    pub(crate) fn repl_state(&self, h: u64, key: &[u8]) -> Option<(&Value, Option<u64>)> {
        let value = self.map.peek(h, key)?;
        Some((value, self.deadline(h, key)))
    }

    /// The deadlines of the fields of the hash at `key`, if it has any.
    pub(crate) fn repl_field_times(&self, h: u64, key: &[u8]) -> Option<&FieldTtl> {
        self.field_times(h, key)
    }

    /// Start remembering which keys change.
    pub fn track_changes(&mut self) {
        self.dirty.get_or_insert_with(Dict::new);
    }

    pub fn untrack_changes(&mut self) {
        self.dirty = None;
    }

    /// Append the current state of every changed key to `out` and forget
    /// that they changed.
    pub fn drain_changes(&mut self, out: &mut Vec<u8>) {
        let Some(dirty) = self.dirty.as_mut().filter(|d| !d.is_empty()) else {
            return;
        };
        let dirty = std::mem::take(dirty);
        dirty.for_each(|key, _| {
            let h = hash_bytes(key);
            match self.map.peek(h, key) {
                Some(value) => crate::persist::encode_put(
                    out,
                    key,
                    value,
                    self.expires.peek(h, key).copied(),
                    self.field_times(h, key),
                ),
                None => crate::persist::encode_del(out, key),
            }
        });
    }

    /// Append about `budget` live keys to `out`, for a rewrite of the data
    /// file. Returns the next cursor, 0 at the end.
    pub fn dump_chunk(&self, cursor: u64, budget: usize, out: &mut Vec<u8>) -> u64 {
        self.scan_chunk(cursor, budget, |key, value| {
            let h = hash_bytes(key);
            let deadline = self.expires.peek(h, key).copied();
            crate::persist::encode_put(out, key, value, deadline, self.field_times(h, key));
        })
    }

    fn put(&mut self, h: u64, key: &[u8], val: Value) {
        // A write that did not look first replaces an expired key unseen;
        // listeners still hear that it expired.
        if self.versions.notify() != 0 && self.expired(h, key) {
            self.note(Note::Expired, key);
        }
        self.touch(h, key);
        self.drop_field_times(h, key);
        if let Some(old) = self.map.insert(h, key, val) {
            self.lazy.free_for(old, Reason::ServerDel);
        }
    }

    fn remove(&mut self, h: u64, key: &[u8]) -> Option<Value> {
        let old = self.map.remove(h, key)?;
        if !self.expires.is_empty() {
            self.expires.remove(h, key);
        }
        self.drop_field_times(h, key);
        self.touch(h, key);
        Some(old)
    }

    /// `remove`, for a key whose value nobody wants: freed the way the
    /// setting for `reason` says. Whether the key was there.
    fn discard(&mut self, h: u64, key: &[u8], reason: Reason) -> bool {
        let Some(old) = self.remove(h, key) else {
            return false;
        };
        self.lazy.free_for(old, reason);
        self.expired += u64::from(reason == Reason::Expire);
        true
    }

    fn clear_deadline(&mut self, h: u64, key: &[u8]) -> bool {
        if self.expires.is_empty() || self.expires.remove(h, key).is_none() {
            return false;
        }
        self.map.set_expire(h, key, 0);
        self.touch(h, key);
        true
    }

    /// Give an existing key a deadline. One that has already passed
    /// deletes the key.
    fn set_deadline(&mut self, h: u64, key: &[u8], at: i64, now: u64) {
        if at <= 0 || at as u64 <= now {
            self.discard(h, key, Reason::Expire);
        } else {
            self.store_deadline(h, key, at as u64);
            self.touch(h, key);
        }
    }

    /// Keep `at` as the deadline of `key`, which is in the map: exactly in
    /// `expires`, which the sweep and the volatile policies walk, and in the
    /// key's entry, which every read checks.
    fn store_deadline(&mut self, h: u64, key: &[u8], at: u64) {
        self.expires.insert(h, key, at);
        self.map.set_expire(h, key, at);
    }

    fn absolute(expire: Expire, now: u64) -> Option<i64> {
        match expire {
            Expire::Keep | Expire::Clear => None,
            Expire::At(at) => Some(at),
            Expire::In(ms) => Some(i64::try_from(now).unwrap_or(i64::MAX).saturating_add(ms)),
        }
    }

    fn apply_expire(&mut self, h: u64, key: &[u8], expire: Expire) {
        match expire {
            Expire::Keep => {}
            Expire::Clear => {
                self.clear_deadline(h, key);
            }
            Expire::In(_) | Expire::At(_) => {
                let now = self.clock.now_ms();
                if let Some(at) = Self::absolute(expire, now) {
                    self.set_deadline(h, key, at, now);
                }
            }
        }
    }

    /// Remove every key; with `lazy`, the memory is freed on the freeing
    /// thread (FLUSHALL ASYNC).
    pub fn clear(&mut self, lazy: bool) {
        self.drop_keys(lazy);
        self.drop_all_field_times();
        self.sweep_cursor = 0;
        if let Some(dirty) = &mut self.dirty {
            dirty.clear();
        }
        // The flush that comes with it says all there is to say.
        if let Some(repl) = &mut self.repl {
            **repl = ReplTrack::default();
        }
    }

    /// Trade every key with `other`, the same shard of another database
    /// (SWAPDB). The data file learns of every key on both sides.
    pub fn swap_data(&mut self, other: &mut Shard) {
        for dirty in [&mut self.dirty, &mut other.dirty] {
            let Some(dirty) = dirty else {
                continue;
            };
            for map in [&self.map, &other.map] {
                map.for_each(|key, _| {
                    dirty.insert(hash_bytes(key), key, ());
                });
            }
        }
        std::mem::swap(&mut self.map, &mut other.map);
        std::mem::swap(&mut self.expires, &mut other.expires);
        std::mem::swap(&mut self.field_ttl, &mut other.field_ttl);
        for shard in [self, other] {
            shard.sweep_cursor = 0;
            shard.field_cursor = 0;
        }
    }

    /// `clear` for one database among several: the data file learns of
    /// every key that goes.
    pub fn clear_tracked(&mut self, lazy: bool) {
        if self.dirty.is_some() {
            let mut keys = Vec::with_capacity(self.map.len());
            self.map.for_each(|key, _| keys.push(key.to_vec()));
            if let Some(dirty) = &mut self.dirty {
                for key in keys {
                    dirty.insert(hash_bytes(&key), &key, ());
                }
            }
        }
        self.drop_keys(lazy);
        self.drop_all_field_times();
        self.sweep_cursor = 0;
        if let Some(repl) = &mut self.repl {
            **repl = ReplTrack::default();
        }
    }

    fn drop_keys(&mut self, lazy: bool) {
        if lazy && !self.map.is_empty() {
            let keys = self.map.len();
            let garbage = (
                std::mem::take(&mut self.map),
                std::mem::take(&mut self.expires),
            );
            self.lazy.free_all(garbage, keys);
        } else {
            self.map.clear();
            self.expires.clear();
        }
    }

    // Strings.

    pub fn get(&self, h: u64, key: &[u8]) -> Reply {
        match self.live(h, key) {
            None => Reply::Nil,
            Some(Value::Str(v)) => Reply::Bulk(v.clone()),
            Some(_) => wrongtype(),
        }
    }

    /// `get` for a caller that encodes RESP: a string goes into `out` as a
    /// bulk reply, read in place instead of copied out first (`None`).
    /// Any other answer is returned.
    pub fn get_into(&self, h: u64, key: &[u8], out: &mut Vec<u8>) -> Option<Reply> {
        match self.live(h, key) {
            None => Some(Reply::Nil),
            Some(Value::Str(v)) => {
                crate::resp::encode_bulk(out, v);
                None
            }
            Some(_) => Some(wrongtype()),
        }
    }

    /// One step of loading ahead what a command on `key` will read: the
    /// bucket, then the entry, then a string's bytes. Each step needs the
    /// line of the step before in the cache, so a caller with several keys
    /// takes them through one step at a time, and their misses overlap.
    pub fn prefetch(&self, step: Prefetch, h: u64) {
        match step {
            Prefetch::Bucket => self.map.prefetch_bucket(h),
            Prefetch::Entry => self.map.prefetch_entry(h),
            Prefetch::Value => {
                if let Some(Value::Str(v)) = self.map.peek_hinted(h) {
                    crate::dict::prefetch(v.as_ptr());
                }
            }
        }
    }

    /// Plain SET: overwrite whatever is there and drop its deadline.
    pub fn set(&mut self, h: u64, key: &[u8], val: Vec<u8>) {
        self.put(h, key, Value::Str(val));
        if !self.expires.is_empty() && self.expires.remove(h, key).is_some() {
            self.map.set_expire(h, key, 0);
        }
    }

    pub fn set_opts(&mut self, h: u64, key: &[u8], val: Vec<u8>, opts: SetOpts) -> Reply {
        self.purge(h, key);
        let old = match self.map.get(h, key) {
            Some(Value::Str(v)) if opts.get => Reply::Bulk(v.clone()),
            Some(_) if opts.get => return wrongtype(),
            _ => Reply::Nil,
        };
        let exists = self.map.contains(h, key);
        let blocked = match opts.cond {
            Some(Cond::Nx) => exists,
            Some(Cond::Xx) => !exists,
            Some(Cond::Gt | Cond::Lt) | None => false,
        };
        let differs = match (&opts.ifeq, self.map.get(h, key)) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(want), Some(Value::Str(v))) => v != want,
            (Some(_), Some(_)) => return wrongtype(),
        };
        if blocked || differs {
            return old;
        }
        self.put(h, key, Value::Str(val));
        self.apply_expire(h, key, opts.expire);
        if opts.get {
            old
        } else {
            Reply::Ok
        }
    }

    pub fn setnx(&mut self, h: u64, key: &[u8], val: Vec<u8>) -> Reply {
        self.purge(h, key);
        if self.map.contains(h, key) {
            return Reply::Int(0);
        }
        self.put(h, key, Value::Str(val));
        Reply::Int(1)
    }

    pub fn getdel(&mut self, h: u64, key: &[u8]) -> Reply {
        self.purge(h, key);
        match self.map.get(h, key) {
            None => Reply::Nil,
            Some(Value::Str(_)) => match self.remove(h, key) {
                Some(Value::Str(v)) => Reply::Bulk(v),
                _ => Reply::Nil,
            },
            Some(_) => wrongtype(),
        }
    }

    /// DELIFEQ: 1 when the string at `key` held `value` and went.
    pub fn delifeq(&mut self, h: u64, key: &[u8], value: &[u8]) -> Reply {
        self.purge(h, key);
        match self.map.get(h, key) {
            None => Reply::Int(0),
            Some(Value::Str(v)) if v.as_slice() == value => {
                self.remove(h, key);
                Reply::Int(1)
            }
            Some(Value::Str(_)) => Reply::Int(0),
            Some(_) => wrongtype(),
        }
    }

    /// One key of MSETEX: SET with `expire`, but a deadline already past
    /// is kept, and the key expires at its next use, as in Valkey.
    pub fn set_ex(&mut self, h: u64, key: &[u8], val: Vec<u8>, expire: Expire) {
        self.purge(h, key);
        self.put(h, key, Value::Str(val));
        match Self::absolute(expire, self.clock.now_ms()) {
            Some(at) => self.store_deadline(h, key, u64::try_from(at).unwrap_or(1)),
            None => self.apply_expire(h, key, expire),
        }
    }

    pub fn getex(&mut self, h: u64, key: &[u8], expire: Expire) -> Reply {
        self.purge(h, key);
        let reply = self.get(h, key);
        if matches!(reply, Reply::Bulk(_)) {
            self.apply_expire(h, key, expire);
        }
        reply
    }

    pub fn incr_by(&mut self, h: u64, key: &[u8], by: i64) -> Reply {
        self.purge(h, key);
        let current = match self.map.get(h, key) {
            None => 0,
            Some(Value::Str(v)) => match parse_i64(v) {
                Some(n) => n,
                None => return Reply::err(NOT_INT),
            },
            Some(_) => return wrongtype(),
        };
        let Some(next) = current.checked_add(by) else {
            return Reply::err(OVERFLOW);
        };
        self.put(h, key, Value::Str(next.to_string().into_bytes()));
        Reply::Int(next)
    }

    pub fn append(&mut self, h: u64, key: &[u8], tail: Vec<u8>) -> Reply {
        self.purge(h, key);
        match self.map.get_mut(h, key) {
            None => {
                let len = tail.len();
                self.put(h, key, Value::Str(tail));
                Reply::Int(len as i64)
            }
            Some(Value::Str(v)) => {
                if v.len().saturating_add(tail.len()) > MAX_STRING {
                    return Reply::err(
                        "ERR string exceeds maximum allowed size (proto-max-bulk-len)",
                    );
                }
                v.extend_from_slice(&tail);
                let len = v.len();
                self.touch(h, key);
                Reply::Int(len as i64)
            }
            Some(_) => wrongtype(),
        }
    }

    pub fn strlen(&self, h: u64, key: &[u8]) -> Reply {
        match self.live(h, key) {
            None => Reply::Int(0),
            Some(Value::Str(v)) => Reply::Int(v.len() as i64),
            Some(_) => wrongtype(),
        }
    }

    // Hashes.

    /// The hash at `key` for writing, created when missing.
    fn hash_mut(&mut self, h: u64, key: &[u8]) -> Option<&mut Dict<Vec<u8>>> {
        self.purge(h, key);
        if !self.map.contains(h, key) {
            self.map.insert(h, key, Value::Hash(Box::default()));
        }
        match self.map.get_mut(h, key) {
            Some(Value::Hash(hash)) => Some(hash),
            _ => None,
        }
    }

    fn hash(&self, h: u64, key: &[u8]) -> Result<Option<&Dict<Vec<u8>>>, Reply> {
        match self.live(h, key) {
            None => Ok(None),
            Some(Value::Hash(hash)) => Ok(Some(hash)),
            Some(_) => Err(wrongtype()),
        }
    }

    pub fn hset(&mut self, h: u64, key: &[u8], pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Reply {
        // A field that is set anew loses its deadline.
        let timed = !self.field_ttl.is_empty() && self.field_ttl.contains(h, key);
        let mut set = Vec::new();
        let Some(hash) = self.hash_mut(h, key) else {
            return wrongtype();
        };
        let mut added = 0;
        for (field, val) in pairs {
            if hash.insert(hash_bytes(&field), &field, val).is_none() {
                added += 1;
            }
            if timed {
                set.push(field);
            }
        }
        self.forget_fields(h, key, &set);
        self.touch(h, key);
        Reply::Int(added)
    }

    pub fn hsetnx(&mut self, h: u64, key: &[u8], field: &[u8], val: Vec<u8>) -> Reply {
        let Some(hash) = self.hash_mut(h, key) else {
            return wrongtype();
        };
        let fh = hash_bytes(field);
        if hash.contains(fh, field) {
            return Reply::Int(0);
        }
        hash.insert(fh, field, val);
        self.touch(h, key);
        Reply::Int(1)
    }

    pub fn hincr_by(&mut self, h: u64, key: &[u8], field: &[u8], by: i64) -> Reply {
        // Checked before `hash_mut`, which would create the key.
        let fh = hash_bytes(field);
        let current = match self.hash(h, key) {
            Err(wrong_type) => return wrong_type,
            Ok(None) => 0,
            Ok(Some(hash)) => match hash.get(fh, field) {
                None => 0,
                Some(v) => match parse_i64(v) {
                    Some(n) => n,
                    None => return Reply::err("ERR hash value is not an integer"),
                },
            },
        };
        let Some(next) = current.checked_add(by) else {
            return Reply::err(OVERFLOW);
        };
        let Some(hash) = self.hash_mut(h, key) else {
            return wrongtype();
        };
        hash.insert(fh, field, next.to_string().into_bytes());
        self.touch(h, key);
        Reply::Int(next)
    }

    pub fn hget(&self, h: u64, key: &[u8], field: &[u8]) -> Reply {
        match self.hash(h, key) {
            Err(wrong_type) => wrong_type,
            Ok(hash) => match hash.and_then(|hash| hash.get(hash_bytes(field), field)) {
                Some(v) => Reply::Bulk(v.clone()),
                None => Reply::Nil,
            },
        }
    }

    pub fn hmget(&self, h: u64, key: &[u8], fields: &[Vec<u8>]) -> Reply {
        match self.hash(h, key) {
            Err(wrong_type) => wrong_type,
            Ok(hash) => Reply::Array(
                fields
                    .iter()
                    .map(|f| match hash.and_then(|hash| hash.get(hash_bytes(f), f)) {
                        Some(v) => Reply::Bulk(v.clone()),
                        None => Reply::Nil,
                    })
                    .collect(),
            ),
        }
    }

    pub fn hexists(&self, h: u64, key: &[u8], field: &[u8]) -> Reply {
        match self.hash(h, key) {
            Err(wrong_type) => wrong_type,
            Ok(hash) => Reply::Int(i64::from(
                hash.is_some_and(|hash| hash.contains(hash_bytes(field), field)),
            )),
        }
    }

    pub fn hdel(&mut self, h: u64, key: &[u8], fields: &[Vec<u8>]) -> Reply {
        self.purge(h, key);
        let hash = match self.map.get_mut(h, key) {
            None => return Reply::Int(0),
            Some(Value::Hash(hash)) => hash,
            Some(_) => return wrongtype(),
        };
        let removed = fields
            .iter()
            .filter(|f| hash.remove(hash_bytes(f), f).is_some())
            .count();
        let empty = hash.is_empty();
        if removed > 0 {
            self.forget_fields(h, key, fields);
        }
        if empty {
            // An empty hash does not exist, as in Redis.
            self.remove(h, key);
        } else if removed > 0 {
            self.touch(h, key);
        }
        Reply::Int(removed as i64)
    }

    pub fn hlen(&self, h: u64, key: &[u8]) -> Reply {
        match self.hash(h, key) {
            Err(wrong_type) => wrong_type,
            Ok(hash) => Reply::Int(hash.map_or(0, Dict::len) as i64),
        }
    }

    /// Number of fields, or `None` when the key is not a hash.
    pub fn hash_len(&self, h: u64, key: &[u8]) -> Option<usize> {
        self.hash(h, key).ok().flatten().map(Dict::len)
    }

    /// Visit about `budget` field/value pairs, starting at `cursor`.
    pub fn hash_chunk(
        &self,
        h: u64,
        key: &[u8],
        cursor: u64,
        budget: usize,
        f: impl FnMut(&[u8], &Vec<u8>),
    ) -> HashScan {
        match self.hash(h, key) {
            Err(_) => HashScan::WrongType,
            Ok(None) => HashScan::Missing,
            Ok(Some(hash)) => HashScan::Next(hash.scan_chunk(cursor, budget, f)),
        }
    }

    /// Append up to about `budget` field/value pairs to `out`.
    pub fn hgetall_chunk(
        &self,
        h: u64,
        key: &[u8],
        cursor: u64,
        budget: usize,
        out: &mut Vec<Reply>,
    ) -> HashScan {
        self.hash_chunk(h, key, cursor, budget, |f, v| {
            out.push(Reply::Bulk(f.to_vec()));
            out.push(Reply::Bulk(v.clone()));
        })
    }

    // Keys of any type.

    /// DEL, and UNLINK with `lazy`.
    pub fn del(&mut self, h: u64, key: &[u8], lazy: bool) -> bool {
        self.purge(h, key);
        match self.remove(h, key) {
            Some(old) => {
                self.lazy.free(old, lazy);
                true
            }
            None => false,
        }
    }

    pub fn exists(&self, h: u64, key: &[u8]) -> bool {
        self.live(h, key).is_some()
    }

    pub fn type_of(&self, h: u64, key: &[u8]) -> Reply {
        Reply::Simple(self.live(h, key).map_or("none", Value::type_name).into())
    }

    /// EXPIRE and its variants: 1 when the deadline was set, 0 when the key
    /// is missing or `cond` did not hold.
    pub fn expire(&mut self, h: u64, key: &[u8], when: Expire, cond: Option<Cond>) -> Reply {
        self.purge(h, key);
        if !self.map.contains(h, key) {
            return Reply::Int(0);
        }
        let now = self.clock.now_ms();
        let Some(at) = Self::absolute(when, now) else {
            return Reply::Int(0);
        };
        // A key without a deadline counts as one that never expires.
        let current = self.deadline(h, key);
        let far = |d: Option<u64>| d.map_or(i64::MAX, |d| i64::try_from(d).unwrap_or(i64::MAX));
        let holds = match cond {
            None => true,
            Some(Cond::Nx) => current.is_none(),
            Some(Cond::Xx) => current.is_some(),
            Some(Cond::Gt) => current.is_some() && at > far(current),
            Some(Cond::Lt) => at < far(current),
        };
        if !holds {
            return Reply::Int(0);
        }
        self.set_deadline(h, key, at, now);
        Reply::Int(1)
    }

    /// TTL and PTTL: -2 for a missing key, -1 for one without a deadline.
    pub fn ttl(&self, h: u64, key: &[u8], millis: bool) -> Reply {
        if self.live(h, key).is_none() {
            return Reply::Int(-2);
        }
        let Some(at) = self.deadline(h, key) else {
            return Reply::Int(-1);
        };
        let left = at.saturating_sub(self.clock.now_ms());
        Reply::Int(to_unit(left, millis))
    }

    /// EXPIRETIME and PEXPIRETIME, same special values as `ttl`.
    pub fn expire_time(&self, h: u64, key: &[u8], millis: bool) -> Reply {
        if self.live(h, key).is_none() {
            return Reply::Int(-2);
        }
        match self.deadline(h, key) {
            None => Reply::Int(-1),
            Some(at) => Reply::Int(to_unit(at, millis)),
        }
    }

    pub fn persist(&mut self, h: u64, key: &[u8]) -> Reply {
        self.purge(h, key);
        Reply::Int(i64::from(self.clear_deadline(h, key)))
    }

    /// Visit about `budget` live keys, starting at `cursor`. Returns the
    /// next cursor, 0 when the shard has been fully visited.
    pub fn scan_chunk(&self, cursor: u64, budget: usize, mut f: impl FnMut(&[u8], &Value)) -> u64 {
        if self.expires.is_empty() {
            return self.map.scan_chunk(cursor, budget, f);
        }
        let now = self.clock.now_ms();
        self.map.scan_chunk_timed(cursor, budget, |k, v, at| {
            if at == 0 || at > now {
                f(k, v);
            }
        })
    }

    /// Evict keys by `policy` while `full` says so, `max` at most. Returns
    /// how many went.
    pub fn evict(&mut self, policy: Policy, max: usize, full: impl Fn() -> bool) -> usize {
        // A replica keeps what its primary keeps (`replica-ignore-maxmemory`).
        if self.versions.replica() {
            return 0;
        }
        let mut evicted = 0;
        while evicted < max && full() && self.evict_one(policy) {
            evicted += 1;
        }
        evicted
    }

    fn evict_one(&mut self, policy: Policy) -> bool {
        let mut state = self.rng();
        let r = coll::next_rand(&mut state);
        // Independent picks: neighbours in the table are of similar age.
        let picks: [u64; EVICT_SAMPLE] = std::array::from_fn(|_| coll::next_rand(&mut state));
        self.rng = state;
        let (map, expires) = (&self.map, &self.expires);
        let any = picks.iter().filter_map(|&r| map.sample(r).next());
        let volatile = picks.iter().filter_map(|&r| expires.sample(r).next());
        let victim = match policy {
            Policy::NoEviction => None,
            Policy::AllKeysRandom => map.pick(r).map(|(k, _)| k),
            Policy::VolatileRandom => expires.pick(r).map(|(k, _)| k),
            Policy::AllKeysLru => any.map(|(k, _, age, _)| (age, k)).max().map(|(_, k)| k),
            // The least used; of two used as little, the one left alone
            // for longer.
            Policy::AllKeysLfu => any
                .map(|(k, _, age, freq)| (freq, Reverse(age), k))
                .min()
                .map(|(_, _, k)| k),
            Policy::VolatileLfu => volatile
                .map(|(k, _, _, _)| {
                    let h = hash_bytes(k);
                    (map.freq(h, k), Reverse(map.age(h, k)), k)
                })
                .min()
                .map(|(_, _, k)| k),
            Policy::VolatileLru => volatile
                .map(|(k, _, _, _)| (map.age(hash_bytes(k), k), k))
                .max()
                .map(|(_, k)| k),
            Policy::VolatileTtl => volatile.map(|(k, at, _, _)| (*at, k)).min().map(|(_, k)| k),
        };
        let Some(key) = victim.map(<[u8]>::to_vec) else {
            return false;
        };
        let gone = self.discard(hash_bytes(&key), &key, Reason::Eviction);
        if gone {
            self.gone(&key);
            self.note(Note::Evicted, &key);
        }
        gone
    }

    /// Remove expired keys nobody asked for, a bounded number per call and
    /// at most once per `SWEEP_INTERVAL_MS`. Returns how many were removed.
    pub fn sweep_due(&mut self) -> usize {
        if (self.expires.is_empty() && self.field_ttl.is_empty()) || self.versions.replica() {
            return 0;
        }
        let now = self.clock.now_ms();
        if now < self.next_sweep_ms {
            return 0;
        }
        self.next_sweep_ms = now + SWEEP_INTERVAL_MS;
        if !self.field_ttl.is_empty() {
            self.sweep_fields(now, fields::FIELD_SWEEP_BUDGET);
        }
        if self.expires.is_empty() {
            return 0;
        }
        let mut removed = 0;
        for _ in 0..SWEEP_ROUNDS {
            let (seen, dead) = self.sweep_round(now, SWEEP_BUDGET);
            removed += dead;
            if self.sweep_cursor == 0 || dead * 100 < seen * SWEEP_AGAIN_PERCENT {
                break;
            }
        }
        removed
    }

    fn sweep_round(&mut self, now: u64, budget: usize) -> (usize, usize) {
        let mut seen = 0;
        let mut dead = Vec::new();
        self.sweep_cursor = self
            .expires
            .scan_chunk(self.sweep_cursor, budget, |k, &at| {
                seen += 1;
                if at <= now {
                    dead.push(k.to_vec());
                }
            });
        for key in &dead {
            if self.discard(hash_bytes(key), key, Reason::Expire) {
                self.gone(key);
            }
            self.note(Note::Expired, key);
        }
        (seen, dead.len())
    }

    /// Remove every expired key now.
    pub fn sweep_all(&mut self) -> usize {
        let now = self.clock.now_ms();
        if !self.field_ttl.is_empty() {
            self.field_cursor = 0;
            self.sweep_fields(now, usize::MAX);
        }
        if self.expires.is_empty() {
            return 0;
        }
        self.sweep_cursor = 0;
        self.sweep_round(now, usize::MAX).1
    }
}

/// Milliseconds as they are, or seconds rounded to nearest.
fn to_unit(ms: u64, millis: bool) -> i64 {
    let v = if millis {
        ms
    } else {
        ms.saturating_add(500) / 1000
    };
    i64::try_from(v).unwrap_or(i64::MAX)
}
