//! Values in Redis's RDB encoding: DUMP and RESTORE, and the keys of an
//! RDB file (`rdbfile.rs`). Every type travels in a form Redis or Valkey
//! reads: streams as RDB 11 does, hashes whose fields have deadlines as
//! Valkey 9 (RDB 80) does. Payloads of the private forms older Hotstash
//! builds wrote are still read.

use super::{Entry, FieldTtl, Shard, Value, MAX_STRING};
use crate::bloom::{self, Bloom};
use crate::dict::{hash_bytes, Dict};
use crate::json;
use crate::lazyfree::Reason;
use crate::rdb::{self, Input, Plain};
use crate::resp::Reply;
use crate::stream::Stream;
use crate::zset::ZSet;

const OLD_DUMP_STREAM: u8 = rdb::PRIVATE_TYPES;
const OLD_DUMP_HASH_TTL: u8 = rdb::PRIVATE_TYPES + 1;

// The RDB types written; see `rdb.rs` for the ones only read.
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_HASH: u8 = 4;
const TYPE_ZSET_2: u8 = 5;
/// A module's value, as a run of typed primitives (`bloom.rs`).
const TYPE_MODULE_2: u8 = 7;

const BAD_ENVELOPE: &str = "ERR DUMP payload version or checksum are wrong";
const BAD_BODY: &str = "ERR Bad data format";

fn take<'a>(input: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    let (head, tail) = input.split_at_checked(n)?;
    *input = tail;
    Some(head)
}

fn take_bytes<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let n = u32::from_le_bytes(take(input, 4)?.try_into().ok()?);
    take(input, n as usize)
}

/// The body of a hash with field deadlines in the private form: every
/// length is checked, and nothing is allocated on the word of a count.
fn old_timed_hash(mut input: &[u8]) -> Option<(Value, FieldTtl)> {
    let count = u32::from_le_bytes(take(&mut input, 4)?.try_into().ok()?);
    let mut hash = Dict::new();
    let mut ttl = FieldTtl::default();
    for _ in 0..count {
        let field = take_bytes(&mut input)?;
        let value = take_bytes(&mut input)?;
        let at = u64::from_le_bytes(take(&mut input, 8)?.try_into().ok()?);
        // Times are signed everywhere a client sees them.
        if i64::try_from(at).is_err() {
            return None;
        }
        if hash
            .insert(hash_bytes(field), field, value.to_vec())
            .is_some()
        {
            return None;
        }
        if at != 0 {
            ttl.set(field, at);
        }
    }
    (count > 0 && input.is_empty()).then_some((Value::Hash(Box::new(hash)), ttl))
}

/// The RDB type `value` is written as, and the oldest RDB version that
/// reads it. A hash goes as `TYPE_HASH_2` when `fields` has deadlines.
fn rdb_type(value: &Value, fields: Option<&FieldTtl>) -> (u8, u16) {
    match value {
        Value::Str(_) => (TYPE_STRING, rdb::VERSION),
        Value::List(_) => (TYPE_LIST, rdb::VERSION),
        Value::Set(_) => (TYPE_SET, rdb::VERSION),
        Value::ZSet(_) => (TYPE_ZSET_2, rdb::VERSION),
        Value::Hash(_) if fields.is_some_and(|ttl| !ttl.is_empty()) => {
            (rdb::TYPE_HASH_2, rdb::VERSION_80)
        }
        Value::Hash(_) => (TYPE_HASH, rdb::VERSION),
        Value::Stream(_) => (rdb::TYPE_STREAM_LISTPACKS_3, rdb::VERSION_11),
        Value::Bloom(_) | Value::Json(_) => (TYPE_MODULE_2, rdb::VERSION),
    }
}

/// The body of `value`, as `rdb_type` says to write it.
fn write_body(out: &mut Vec<u8>, value: &Value, fields: Option<&FieldTtl>) {
    match value {
        Value::Str(v) => rdb::put_string(out, v),
        Value::List(list) => {
            rdb::put_len(out, list.len() as u64);
            list.iter().for_each(|item| rdb::put_string(out, item));
        }
        Value::Set(set) => {
            rdb::put_len(out, set.len() as u64);
            set.for_each(|member, _| rdb::put_string(out, member));
        }
        Value::ZSet(zset) => {
            rdb::put_len(out, zset.len() as u64);
            for (member, score) in zset.range(0, usize::MAX) {
                rdb::put_string(out, member);
                out.extend_from_slice(&score.to_le_bytes());
            }
        }
        Value::Hash(hash) => {
            let ttl = fields.filter(|ttl| !ttl.is_empty());
            rdb::put_len(out, hash.len() as u64);
            hash.for_each(|field, value| {
                rdb::put_string(out, field);
                rdb::put_string(out, value);
                if let Some(ttl) = ttl {
                    let at = ttl.get(field).and_then(|at| i64::try_from(at).ok());
                    rdb::put_millis(out, at.unwrap_or(rdb::NO_DEADLINE));
                }
            });
        }
        Value::Stream(stream) => stream.write_rdb(out),
        Value::Bloom(bloom) => {
            rdb::put_len(out, bloom::MODULE_ID);
            bloom.write_rdb(out);
        }
        Value::Json(doc) => {
            rdb::put_len(out, json::MODULE_ID);
            json::write_rdb(doc, out);
        }
    }
}

/// Append the RDB type byte and body of `value` to `out`. Returns the
/// oldest RDB version that reads them.
pub(crate) fn write_value(out: &mut Vec<u8>, value: &Value, fields: Option<&FieldTtl>) -> u16 {
    let (t, version) = rdb_type(value, fields);
    out.push(t);
    write_body(out, value, fields);
    version
}

/// A `TYPE_HASH_2` body: fields with a deadline each.
fn timed_hash<I: Input + ?Sized>(r: &mut I) -> Option<Entry> {
    let n = r.count()?;
    let mut hash = Dict::new();
    let mut ttl = FieldTtl::default();
    for _ in 0..n {
        let field = r.string()?;
        let value = r.string()?;
        let at = r.millis()?;
        if at < rdb::NO_DEADLINE {
            return None;
        }
        if hash.insert(hash_bytes(&field), &field, value).is_some() {
            return None;
        }
        if at != rdb::NO_DEADLINE {
            // 0 is a time long past; here 0 means no deadline.
            ttl.set(&field, (at as u64).max(1));
        }
    }
    Some(Entry {
        value: Value::Hash(Box::new(hash)),
        deadline: None,
        fields: Some(ttl),
    })
}

fn plain_value(plain: Plain) -> Option<Value> {
    let value = match plain {
        Plain::Str(v) if v.len() > MAX_STRING => return None,
        Plain::Str(v) => Value::Str(v),
        Plain::List(items) => Value::List(items.into()),
        Plain::Set(members) => {
            let mut set = Dict::new();
            for member in &members {
                if set.insert(hash_bytes(member), member, ()).is_some() {
                    return None;
                }
            }
            Value::Set(Box::new(set))
        }
        Plain::ZSet(entries) => {
            // Valkey loads a sorted set whole: a listpack if it fits.
            let mut zset = ZSet::default();
            zset.expect(
                entries.len(),
                entries.iter().map(|(m, _)| m.len()).max().unwrap_or(0),
            );
            for (member, score) in &entries {
                if score.is_nan() || zset.insert(member, *score).is_some() {
                    return None;
                }
            }
            Value::ZSet(Box::new(zset))
        }
        Plain::Hash(pairs) => {
            let mut hash = Dict::new();
            for (field, value) in pairs {
                if hash.insert(hash_bytes(&field), &field, value).is_some() {
                    return None;
                }
            }
            Value::Hash(Box::new(hash))
        }
    };
    Some(value)
}

/// The value of RDB type `t` (its type byte already read) as RDB `version`
/// writes it. `None` when it is malformed, empty, or of a type we lack.
pub(crate) fn read_entry<I: Input + ?Sized>(r: &mut I, t: u8, version: u16) -> Option<Entry> {
    let entry = match t {
        rdb::TYPE_STREAM_LISTPACKS
        | rdb::TYPE_STREAM_LISTPACKS_2
        | rdb::TYPE_STREAM_LISTPACKS_3 => {
            Entry::new(Value::Stream(Box::new(Stream::read_rdb(r, t)?)), None)
        }
        // Only valkey-bloom's and valkey-json's values: no other module's
        // can be read.
        TYPE_MODULE_2 => match r.len()? {
            bloom::MODULE_ID => Entry::new(Value::Bloom(Box::new(Bloom::read_rdb(r)?)), None),
            json::MODULE_ID => Entry::new(Value::Json(Box::new(json::read_rdb(r)?)), None),
            _ => return None,
        },
        // Type 22 means something else to the Redis forks behind RDB 12.
        rdb::TYPE_HASH_2 if version >= rdb::VERSION_80 => timed_hash(r)?,
        _ => Entry::new(plain_value(rdb::read_plain(r, t)?)?, None),
    };
    let empty = match &entry.value {
        Value::Str(_) | Value::Stream(_) | Value::Bloom(_) | Value::Json(_) => false,
        Value::List(list) => list.is_empty(),
        Value::Set(set) => set.is_empty(),
        Value::ZSet(zset) => zset.is_empty(),
        Value::Hash(hash) => hash.is_empty(),
    };
    (!empty).then_some(entry)
}

/// The key a payload describes.
fn undump(payload: &[u8]) -> Result<Entry, Reply> {
    let bad = || Reply::err(BAD_BODY);
    let (body, version) = rdb::open_versioned(payload).map_err(|_| Reply::err(BAD_ENVELOPE))?;
    let mut r = rdb::Reader::new(body);
    let entry = match r.u8().ok_or_else(bad)? {
        OLD_DUMP_STREAM => {
            let mut rest = r.rest();
            let stream = Stream::decode(&mut rest).ok_or_else(bad)?;
            if !rest.is_empty() {
                return Err(bad());
            }
            return Ok(Entry::new(Value::Stream(Box::new(stream)), None));
        }
        OLD_DUMP_HASH_TTL => {
            let (value, ttl) = old_timed_hash(r.rest()).ok_or_else(bad)?;
            return Ok(Entry {
                value,
                deadline: None,
                fields: Some(ttl),
            });
        }
        t => read_entry(&mut r, t, version).ok_or_else(bad)?,
    };
    // A DUMP body is one value and nothing after it.
    if !r.rest().is_empty() {
        return Err(bad());
    }
    Ok(entry)
}

impl Shard {
    /// DUMP: the value in a form RESTORE takes back. The key's own
    /// deadline is not part of it, as in Redis.
    pub fn dump(&self, h: u64, key: &[u8]) -> Reply {
        let Some(value) = self.live(h, key) else {
            return Reply::Nil;
        };
        let mut body = Vec::new();
        let version = write_value(&mut body, value, self.field_times(h, key));
        Reply::Bulk(rdb::seal_as(body, version))
    }

    /// RESTORE. `ttl` is in milliseconds, 0 for none; with `absolute` it
    /// is a Unix time.
    pub fn restore_dump(
        &mut self,
        h: u64,
        key: &[u8],
        payload: &[u8],
        ttl: i64,
        absolute: bool,
        replace: bool,
    ) -> Reply {
        self.purge(h, key);
        if !replace && self.map.contains(h, key) {
            return Reply::err("BUSYKEY Target key name already exists.");
        }
        let mut entry = match undump(payload) {
            Ok(entry) => entry,
            Err(reply) => return reply,
        };
        let now = self.clock.now_ms();
        if ttl != 0 {
            let at = if absolute {
                u64::try_from(ttl).unwrap_or(0)
            } else {
                now.saturating_add(u64::try_from(ttl).unwrap_or(0))
            };
            if at <= now {
                // Already over: the key is not made, and what was there goes.
                self.discard(h, key, Reason::ServerDel);
                return Reply::Ok;
            }
            entry.deadline = Some(at);
        }
        self.restore(h, key, entry);
        Reply::Ok
    }

    /// Append every live key to `out` as an RDB file holds it: its
    /// deadline, type, name and value.
    pub(crate) fn write_rdb(&self, out: &mut Vec<u8>) -> RdbCounts {
        let mut counts = RdbCounts {
            keys: 0,
            expires: 0,
            version: rdb::VERSION,
        };
        self.map.for_each_timed(|key, value, at| {
            if self.past(at) {
                return;
            }
            if at != 0 {
                out.push(crate::rdbfile::OPCODE_EXPIRETIME_MS);
                rdb::put_millis(out, i64::try_from(at).unwrap_or(i64::MAX));
                counts.expires += 1;
            }
            let fields = self.field_times(hash_bytes(key), key);
            let (t, version) = rdb_type(value, fields);
            out.push(t);
            rdb::put_raw(out, key);
            write_body(out, value, fields);
            counts.version = counts.version.max(version);
            counts.keys += 1;
        });
        counts
    }

    /// RDB loading: store a key read from a file. `false` when the key is
    /// there already: a file holds each key once.
    pub(crate) fn import(&mut self, h: u64, key: &[u8], entry: Entry) -> bool {
        if self.map.contains(h, key) {
            return false;
        }
        self.restore(h, key, entry);
        true
    }
}

/// What `Shard::write_rdb` wrote: keys, how many have a deadline, and the
/// oldest RDB version that reads them all.
pub(crate) struct RdbCounts {
    pub keys: u64,
    pub expires: u64,
    pub version: u16,
}
