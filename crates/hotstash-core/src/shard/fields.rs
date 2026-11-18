//! Deadlines of single hash fields (HEXPIRE and friends), the notes a shard
//! leaves for keyspace notifications, and the whole-key view that RENAME,
//! COPY, MOVE, RESTORE and the data file carry around.
//!
//! Field deadlines live beside the keyspace, in a table that only has the
//! hashes that use them. A field whose time has come is removed by
//! `reap_fields`, which the engine calls for every key of a command before
//! the command runs, so the commands themselves never see one.

use super::{to_unit, wrongtype, Shard, Value};
use crate::cmd::{Bytes, Cond, Expire};
use crate::dict::{hash_bytes, Dict};
use crate::lazyfree::Reason;
use crate::ops::FieldTtlArg;
use crate::resp::Reply;
use std::sync::atomic::Ordering;

/// Notes waiting to be published are dropped beyond this many: nobody is
/// collecting them.
const MAX_NOTES: usize = 1 << 16;
/// Fixed cost counted per key and per element by MEMORY USAGE.
const KEY_OVERHEAD: usize = 56;
const ELEMENT_OVERHEAD: usize = 32;
/// Hashes looked at by one round of the background sweep.
pub(super) const FIELD_SWEEP_BUDGET: usize = 16;

/// Something that happened to a key without a command asking for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Note {
    Expired,
    Evicted,
    /// Fields of a hash reached their deadline.
    HExpired,
    /// The hash went with its last field.
    Del,
    /// HSETEX set fields.
    HSet,
    /// HGETDEL removed fields.
    HDel,
    /// HGETEX or HSETEX gave fields a deadline.
    HExpire,
    /// HGETEX PERSIST took deadlines away.
    HPersist,
    /// Items went into a bloom filter.
    BloomAdd,
    /// A bloom filter was made.
    BloomReserve,
    /// A JSON.* command changed a document: the event's name.
    Json(&'static str),
}

/// The deadlines of one hash's fields, in milliseconds.
#[derive(Clone)]
pub struct FieldTtl {
    at: Dict<u64>,
    /// No deadline is earlier than this.
    next: u64,
}

impl Default for FieldTtl {
    fn default() -> Self {
        FieldTtl {
            at: Dict::new(),
            next: u64::MAX,
        }
    }
}

impl FieldTtl {
    pub fn is_empty(&self) -> bool {
        self.at.is_empty()
    }

    pub fn get(&self, field: &[u8]) -> Option<u64> {
        self.at.peek(hash_bytes(field), field).copied()
    }

    pub fn set(&mut self, field: &[u8], at: u64) {
        self.at.insert(hash_bytes(field), field, at);
        self.next = self.next.min(at);
    }

    pub fn for_each(&self, mut f: impl FnMut(&[u8], u64)) {
        self.at.for_each(|field, &at| f(field, at));
    }
}

/// A key as it travels: its value and everything that hangs on it.
pub struct Entry {
    pub value: Value,
    pub deadline: Option<u64>,
    pub fields: Option<FieldTtl>,
}

impl Entry {
    pub fn new(value: Value, deadline: Option<u64>) -> Entry {
        Entry {
            value,
            deadline,
            fields: None,
        }
    }
}

fn nils(n: usize) -> Reply {
    Reply::Array(vec![Reply::Nil; n])
}

fn holds(cond: Option<Cond>, current: Option<u64>, at: i64) -> bool {
    // A field without a deadline counts as one that never expires.
    let far = current.map_or(i64::MAX, |d| i64::try_from(d).unwrap_or(i64::MAX));
    match cond {
        None => true,
        Some(Cond::Nx) => current.is_none(),
        Some(Cond::Xx) => current.is_some(),
        Some(Cond::Gt) => current.is_some() && at > far,
        Some(Cond::Lt) => at < far,
    }
}

impl Shard {
    pub(super) fn note(&mut self, note: Note, key: &[u8]) {
        if self.versions.notify() != 0 && self.notes.len() < MAX_NOTES {
            self.notes.push((note, key.to_vec()));
        }
    }

    /// What happened here since the last call, oldest first.
    pub fn take_notes(&mut self) -> Vec<(Note, Bytes)> {
        std::mem::take(&mut self.notes)
    }

    pub(super) fn field_times(&self, h: u64, key: &[u8]) -> Option<&FieldTtl> {
        if self.field_ttl.is_empty() {
            return None;
        }
        self.field_ttl.peek(h, key)
    }

    fn set_field_times(&mut self, h: u64, key: &[u8], ttl: FieldTtl) {
        if self.field_ttl.insert(h, key, ttl).is_none() {
            self.versions.hash_ttls.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub(super) fn drop_field_times(&mut self, h: u64, key: &[u8]) -> Option<FieldTtl> {
        if self.field_ttl.is_empty() {
            return None;
        }
        let old = self.field_ttl.remove(h, key)?;
        self.versions.hash_ttls.fetch_sub(1, Ordering::SeqCst);
        Some(old)
    }

    pub(super) fn drop_all_field_times(&mut self) {
        self.versions
            .hash_ttls
            .fetch_sub(self.field_ttl.len(), Ordering::SeqCst);
        self.field_ttl.clear();
        self.field_cursor = 0;
    }

    /// The fields were overwritten or deleted: their deadlines go.
    pub(super) fn forget_fields(&mut self, h: u64, key: &[u8], fields: &[Bytes]) {
        if self.field_ttl.is_empty() {
            return;
        }
        let Some(ttl) = self.field_ttl.get_mut(h, key) else {
            return;
        };
        for field in fields {
            ttl.at.remove(hash_bytes(field), field);
        }
        if ttl.at.is_empty() {
            self.drop_field_times(h, key);
        }
    }

    /// Remove the fields of the hash at `key` whose time has come, and the
    /// hash with its last field.
    pub fn reap_fields(&mut self, h: u64, key: &[u8]) {
        // On a replica fields go when the primary says so.
        if self.field_ttl.is_empty() || self.versions.replica() {
            return;
        }
        let now = self.clock.now_ms();
        let Some(ttl) = self.field_ttl.get_mut(h, key) else {
            return;
        };
        if ttl.next > now {
            return;
        }
        let mut dead = Vec::new();
        let mut next = u64::MAX;
        ttl.at.for_each(|field, &at| {
            if at <= now {
                dead.push(field.to_vec());
            } else {
                next = next.min(at);
            }
        });
        ttl.next = next;
        for field in &dead {
            ttl.at.remove(hash_bytes(field), field);
        }
        let none_left = ttl.at.is_empty();
        if self.expired(h, key) {
            // The whole key is due: `purge` reports that.
            return;
        }
        let empty = match self.map.get_mut(h, key) {
            Some(Value::Hash(hash)) => {
                for field in &dead {
                    hash.remove(hash_bytes(field), field);
                }
                hash.is_empty()
            }
            _ => false,
        };
        if none_left {
            self.drop_field_times(h, key);
        }
        if dead.is_empty() {
            return;
        }
        self.note(Note::HExpired, key);
        if empty {
            self.remove(h, key);
            self.note(Note::Del, key);
        } else {
            self.touch(h, key);
        }
        // The replicas hear of the fields, not of the whole hash.
        if let Some(repl) = &mut self.repl {
            repl.touched.pop();
            let fields = dead.into_iter().map(|field| (key.to_vec(), field));
            repl.gone_fields.extend(fields);
        }
    }

    /// One round of the background sweep over hashes with field deadlines.
    pub(super) fn sweep_fields(&mut self, now: u64, budget: usize) {
        let mut due = Vec::new();
        self.field_cursor = self
            .field_ttl
            .scan_chunk(self.field_cursor, budget, |key, ttl| {
                if ttl.next <= now {
                    due.push(key.to_vec());
                }
            });
        for key in &due {
            self.reap_fields(hash_bytes(key), key);
        }
    }

    /// HEXPIRE and its variants. Per field: -2 when it does not exist, 0
    /// when `cond` did not hold, 1 when the deadline was set, 2 when the
    /// deadline had already passed and the field was deleted.
    pub fn hexpire(
        &mut self,
        h: u64,
        key: &[u8],
        when: Expire,
        cond: Option<Cond>,
        fields: &[Bytes],
    ) -> Reply {
        self.purge(h, key);
        let now = self.clock.now_ms();
        let Some(at) = Self::absolute(when, now) else {
            return Reply::err("ERR internal: no expire time");
        };
        let hash = match self.map.get_mut(h, key) {
            None => return Reply::Array(fields.iter().map(|_| Reply::Int(-2)).collect()),
            Some(Value::Hash(hash)) => hash,
            Some(_) => return wrongtype(),
        };
        let past = at <= 0 || at as u64 <= now;
        let had = self.field_ttl.contains(h, key);
        let mut ttl = self.field_ttl.remove(h, key).unwrap_or_default();
        let mut changed = false;
        let mut out = Vec::with_capacity(fields.len());
        for field in fields {
            let fh = hash_bytes(field);
            let code = if !hash.contains(fh, field) {
                -2
            } else if !holds(cond, ttl.at.peek(fh, field).copied(), at) {
                0
            } else if past {
                hash.remove(fh, field);
                ttl.at.remove(fh, field);
                changed = true;
                2
            } else {
                ttl.set(field, at as u64);
                changed = true;
                1
            };
            out.push(Reply::Int(code));
        }
        let empty = hash.is_empty();
        let keep = !ttl.at.is_empty() && !empty;
        if keep {
            self.field_ttl.insert(h, key, ttl);
        }
        match (had, keep) {
            (false, true) => self.versions.hash_ttls.fetch_add(1, Ordering::SeqCst),
            (true, false) => self.versions.hash_ttls.fetch_sub(1, Ordering::SeqCst),
            _ => 0,
        };
        if empty {
            self.remove(h, key);
        } else if changed {
            self.touch(h, key);
        }
        Reply::Array(out)
    }

    /// HTTL, HPTTL, HEXPIRETIME and HPEXPIRETIME. Per field: -2 when it
    /// does not exist, -1 when it has no deadline.
    pub fn httl(
        &self,
        h: u64,
        key: &[u8],
        fields: &[Bytes],
        millis: bool,
        absolute: bool,
    ) -> Reply {
        let hash = match self.hash(h, key) {
            Err(wrong_type) => return wrong_type,
            Ok(hash) => hash,
        };
        let ttl = self.field_times(h, key);
        let now = self.clock.now_ms();
        let answer = |field: &Bytes| {
            if !hash.is_some_and(|hash| hash.contains(hash_bytes(field), field)) {
                return -2;
            }
            match ttl.and_then(|ttl| ttl.get(field)) {
                None => -1,
                Some(at) if absolute => to_unit(at, millis),
                Some(at) => to_unit(at.saturating_sub(now), millis),
            }
        };
        Reply::Array(fields.iter().map(|f| Reply::Int(answer(f))).collect())
    }

    /// HPERSIST. Per field: -2 when it does not exist, -1 when it had no
    /// deadline, 1 when its deadline was removed.
    pub fn hpersist(&mut self, h: u64, key: &[u8], fields: &[Bytes]) -> Reply {
        self.purge(h, key);
        let hash = match self.map.get(h, key) {
            None => return Reply::Array(fields.iter().map(|_| Reply::Int(-2)).collect()),
            Some(Value::Hash(hash)) => hash,
            Some(_) => return wrongtype(),
        };
        let mut ttl = self.field_ttl.get_mut(h, key);
        let mut cleared = Vec::new();
        let out = fields
            .iter()
            .map(|field| {
                let fh = hash_bytes(field);
                if !hash.contains(fh, field) {
                    return Reply::Int(-2);
                }
                match ttl.as_mut().and_then(|ttl| ttl.at.remove(fh, field)) {
                    None => Reply::Int(-1),
                    Some(_) => {
                        cleared.push(field);
                        Reply::Int(1)
                    }
                }
            })
            .collect();
        if ttl.is_some_and(|ttl| ttl.at.is_empty()) {
            self.drop_field_times(h, key);
        }
        if !cleared.is_empty() {
            self.touch(h, key);
        }
        Reply::Array(out)
    }

    /// HGETDEL: the values of `fields`, which go; the hash goes with its
    /// last field.
    pub fn hgetdel(&mut self, h: u64, key: &[u8], fields: &[Bytes]) -> Reply {
        self.purge(h, key);
        let timed = !self.field_ttl.is_empty();
        let hash = match self.map.get_mut(h, key) {
            None => return nils(fields.len()),
            Some(Value::Hash(hash)) => hash,
            Some(_) => return wrongtype(),
        };
        let mut out = Vec::with_capacity(fields.len());
        let mut gone = Vec::new();
        for field in fields {
            match hash.remove(hash_bytes(field), field) {
                Some(value) => {
                    out.push(Reply::Bulk(value));
                    if timed {
                        gone.push(field.clone());
                    }
                }
                None => out.push(Reply::Nil),
            }
        }
        let empty = hash.is_empty();
        if out.iter().any(|r| matches!(r, Reply::Bulk(_))) {
            self.forget_fields(h, key, &gone);
            self.note(Note::HDel, key);
            if empty {
                self.remove(h, key);
                self.note(Note::Del, key);
            } else {
                self.touch(h, key);
            }
        }
        Reply::Array(out)
    }

    /// HGETEX: the values of `fields` as they were, after which `ttl`
    /// gives the fields there a deadline, takes it away, or removes them
    /// when the time is already past.
    pub fn hgetex(&mut self, h: u64, key: &[u8], ttl: &FieldTtlArg, fields: &[Bytes]) -> Reply {
        self.purge(h, key);
        let hash = match self.map.get(h, key) {
            None => None,
            Some(Value::Hash(hash)) => Some(hash),
            Some(_) => return wrongtype(),
        };
        // After the type, but with no key as well, as in Valkey.
        let when = match ttl.when("hgetex") {
            Ok(when) => when,
            Err(e) => return e,
        };
        let Some(hash) = hash else {
            return nils(fields.len());
        };
        let values = fields
            .iter()
            .map(|f| {
                hash.get(hash_bytes(f), f)
                    .map_or(Reply::Nil, |v| Reply::Bulk(v.clone()))
            })
            .collect();
        let (codes, set) = match (when, ttl) {
            (Some(when), _) => (self.hexpire(h, key, when, None, fields), Note::HExpire),
            (None, FieldTtlArg::Persist) => (self.hpersist(h, key, fields), Note::HPersist),
            (None, _) => return Reply::Array(values),
        };
        let has = |code| matches!(&codes, Reply::Array(c) if c.contains(&Reply::Int(code)));
        if has(1) {
            self.note(set, key);
        }
        if has(2) {
            self.note(Note::HExpired, key);
            if !self.map.contains(h, key) {
                self.note(Note::Del, key);
            }
        }
        Reply::Array(values)
    }

    /// HSETEX: 1 when the fields were set (or removed, at a time already
    /// past), 0 when NX, XX, FNX or FXX kept them from it.
    pub fn hsetex(
        &mut self,
        h: u64,
        key: &[u8],
        cond: Option<Cond>,
        fcond: Option<Cond>,
        ttl: &FieldTtlArg,
        pairs: Vec<(Bytes, Bytes)>,
    ) -> Reply {
        self.purge(h, key);
        let hash = match self.map.get(h, key) {
            None => None,
            Some(Value::Hash(hash)) => Some(hash),
            Some(_) => return wrongtype(),
        };
        // Valkey's order: type, the key's condition, the time, the fields'.
        if matches!(
            (cond, hash.is_some()),
            (Some(Cond::Nx), true) | (Some(Cond::Xx), false)
        ) {
            return Reply::Int(0);
        }
        let when = match ttl.when("hsetex") {
            Ok(when) => when,
            Err(e) => return e,
        };
        // Against the hash as it was: an earlier pair does not count.
        let has = |field: &Bytes| hash.is_some_and(|hash| hash.contains(hash_bytes(field), field));
        let blocked = match fcond {
            Some(Cond::Nx) => pairs.iter().any(|(f, _)| has(f)),
            Some(Cond::Xx) => pairs.iter().any(|(f, _)| !has(f)),
            _ => false,
        };
        if blocked {
            return Reply::Int(0);
        }
        let existed = hash.is_some();
        let now = self.clock.now_ms();
        let at = when.and_then(|when| Self::absolute(when, now));
        let fields: Vec<Bytes> = pairs.iter().map(|(f, _)| f.clone()).collect();
        self.note(Note::HSet, key);
        match at {
            // Already over: the fields go instead, and a missing hash is
            // as good as made and removed.
            Some(at) if at <= 0 || at as u64 <= now => {
                self.note(Note::HExpire, key);
                self.note(Note::HExpired, key);
                if existed {
                    self.hexpire(h, key, Expire::At(at), None, &fields);
                }
                if !self.map.contains(h, key) {
                    self.note(Note::Del, key);
                }
            }
            Some(at) => {
                self.hset_keep(h, key, pairs);
                self.hexpire(h, key, Expire::At(at), None, &fields);
                self.note(Note::HExpire, key);
            }
            None if *ttl == FieldTtlArg::Keep => self.hset_keep(h, key, pairs),
            None => {
                self.hset(h, key, pairs);
            }
        }
        Reply::Int(1)
    }

    /// Set the fields and leave their deadlines as they are.
    fn hset_keep(&mut self, h: u64, key: &[u8], pairs: Vec<(Bytes, Bytes)>) {
        if let Some(hash) = self.hash_mut(h, key) {
            for (field, value) in pairs {
                hash.insert(hash_bytes(&field), &field, value);
            }
        }
        self.touch(h, key);
    }

    /// Remove the key and hand back all of it.
    pub fn take(&mut self, h: u64, key: &[u8]) -> Option<Entry> {
        self.purge(h, key);
        let deadline = self.deadline(h, key);
        let fields = self.drop_field_times(h, key);
        Some(Entry {
            value: self.remove(h, key)?,
            deadline,
            fields,
        })
    }

    /// A copy of the key.
    pub fn snapshot(&self, h: u64, key: &[u8]) -> Option<Entry> {
        Some(Entry {
            value: self.live(h, key)?.clone(),
            deadline: self.deadline(h, key),
            fields: self.field_times(h, key).cloned(),
        })
    }

    /// Store `entry` under `key`, replacing whatever is there.
    pub fn restore(&mut self, h: u64, key: &[u8], entry: Entry) {
        self.discard(h, key, Reason::ServerDel);
        self.put(h, key, entry.value);
        if let Some(at) = entry.deadline {
            self.store_deadline(h, key, at);
        }
        if let Some(ttl) = entry.fields.filter(|ttl| !ttl.is_empty()) {
            self.set_field_times(h, key, ttl);
        }
    }

    /// Recovery: put a key back as it was, without marking it changed.
    pub fn load(&mut self, h: u64, key: &[u8], entry: Option<Entry>) {
        self.map.remove(h, key);
        self.expires.remove(h, key);
        self.drop_field_times(h, key);
        let Some(entry) = entry else {
            return;
        };
        self.map.insert(h, key, entry.value);
        if let Some(at) = entry.deadline {
            self.store_deadline(h, key, at);
        }
        if let Some(ttl) = entry.fields.filter(|ttl| !ttl.is_empty()) {
            self.set_field_times(h, key, ttl);
        }
    }

    /// MEMORY USAGE: about how many bytes the key takes, judged from
    /// `samples` of its elements (0: all of them).
    pub fn memory_usage(&self, h: u64, key: &[u8], samples: usize) -> Reply {
        let Some(value) = self.live(h, key) else {
            return Reply::Nil;
        };
        let budget = if samples == 0 { usize::MAX } else { samples };
        // Bytes of the elements looked at, how many those were, and how
        // many there are.
        let (mut bytes, mut seen) = (0usize, 0usize);
        let mut look = |len: usize| {
            bytes += len + ELEMENT_OVERHEAD;
            seen += 1;
        };
        let total = match value {
            Value::Str(v) => {
                return Reply::Int((KEY_OVERHEAD + key.len() + v.len()) as i64);
            }
            Value::Hash(hash) => {
                hash.scan_chunk(0, budget, |f, v| look(f.len() + v.len()));
                hash.len()
            }
            Value::List(list) => {
                list.iter().take(budget).for_each(|v| look(v.len()));
                list.len()
            }
            Value::Set(set) => {
                set.scan_chunk(0, budget, |m, _| look(m.len()));
                set.len()
            }
            Value::ZSet(zset) => {
                zset.range(0, budget.min(zset.len()))
                    .for_each(|(m, _)| look(m.len() + ELEMENT_OVERHEAD));
                zset.len()
            }
            Value::Stream(stream) => {
                let mut encoded = Vec::new();
                stream.encode(&mut encoded);
                return Reply::Int((KEY_OVERHEAD + key.len() + encoded.len()) as i64);
            }
            Value::Bloom(bloom) => {
                let size = (KEY_OVERHEAD + key.len()) as u64 + bloom.memory_usage();
                return Reply::Int(i64::try_from(size).unwrap_or(i64::MAX));
            }
            Value::Json(doc) => {
                let size = (KEY_OVERHEAD + key.len()) as u64 + doc.size();
                return Reply::Int(i64::try_from(size).unwrap_or(i64::MAX));
            }
        };
        let elements = if seen == 0 {
            0
        } else {
            (bytes as u128 * total as u128 / seen as u128) as usize
        };
        Reply::Int((KEY_OVERHEAD + key.len()).saturating_add(elements) as i64)
    }
}
