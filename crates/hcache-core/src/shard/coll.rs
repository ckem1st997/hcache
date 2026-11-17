//! Lists, sets and sorted sets, and the newer string, hash and key
//! commands: everything a `KeyOp` can ask of one key.

use super::{wrongtype, Note, Shard, Value, MAX_STRING};
use crate::bitfield;
use crate::bloom::{self, BfCmd, BfOp};
use crate::cmd::Bytes;
use crate::dict::{hash_bytes, Dict};
use crate::geo::{self, GeoOp};
use crate::glob::glob_match;
use crate::hll;
use crate::json::{self, Effect, JsonCmd, JsonOp, Settings as JsonSettings, WriteOp};
use crate::lazyfree::Reason;
use crate::ops::{KeyOp, LexBound, ObjectSub, ZAddFlags, ZBound, ZBy, ZRangeSpec, NOT_FLOAT};
use crate::resp::{parse_i64, Reply};
use crate::stream::{StreamId, XOp};
use crate::zset::{format_score, parse_float, ZSet};
use std::collections::{HashSet, VecDeque};

const NO_KEY: &str = "ERR no such key";
const BAD_INDEX: &str = "ERR index out of range";
/// Picks RANDOMKEY makes before it sweeps the expired keys away.
const RANDOM_KEY_TRIES: usize = 8;
const NAN_SCORE: &str = "ERR resulting score is not a number (NaN)";

fn bulk(v: &[u8]) -> Reply {
    Reply::Bulk(v.to_vec())
}

fn score(v: f64) -> Reply {
    Reply::Bulk(format_score(v))
}

fn scan_reply(cursor: u64, items: Vec<Reply>) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(cursor.to_string().into_bytes()),
        Reply::Array(items),
    ])
}

/// `start..end` for Redis' inclusive indexes, where negative ones count
/// from the end. `None` when the range is empty.
fn span(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let start = if start < 0 {
        len.saturating_add(start).max(0)
    } else {
        start
    };
    let stop = if stop < 0 {
        len.saturating_add(stop)
    } else {
        stop.min(len - 1)
    };
    (start <= stop).then(|| (start as usize, stop as usize + 1))
}

/// One position, negative ones counting from the end.
fn at(index: i64, len: usize) -> Option<usize> {
    let i = if index < 0 {
        (len as i64).saturating_add(index)
    } else {
        index
    };
    (0..len as i64).contains(&i).then_some(i as usize)
}

/// Up to `n` distinct keys of `dict`, picked at random.
fn sample<V>(dict: &Dict<V>, n: usize, mut rand: impl FnMut() -> u64) -> Vec<Bytes> {
    let mut all = |limit: usize| {
        let mut keys = Vec::with_capacity(dict.len());
        dict.for_each(|k, _| keys.push(k.to_vec()));
        for i in 0..limit.min(keys.len()) {
            let j = i + (rand() % (keys.len() - i) as u64) as usize;
            keys.swap(i, j);
        }
        keys.truncate(limit);
        keys
    };
    if n >= dict.len() / 4 {
        return all(n);
    }
    let mut seen = HashSet::with_capacity(n);
    let mut tries = n * 16;
    while seen.len() < n && tries > 0 {
        tries -= 1;
        if let Some((k, _)) = dict.pick(rand()) {
            seen.insert(k.to_vec());
        }
    }
    seen.into_iter().collect()
}

macro_rules! typed {
    ($read:ident, $edit:ident, $variant:ident, $ty:ty) => {
        fn $read(&self, h: u64, key: &[u8]) -> Result<Option<&$ty>, Reply> {
            match self.live(h, key) {
                None => Ok(None),
                Some(Value::$variant(v)) => Ok(Some(v)),
                Some(_) => Err(wrongtype()),
            }
        }

        /// Change the value in place. `f` returns the reply and whether it
        /// changed anything. A value left empty is deleted.
        fn $edit(
            &mut self,
            h: u64,
            key: &[u8],
            create: bool,
            missing: Reply,
            f: impl FnOnce(&mut $ty) -> (Reply, bool),
        ) -> Reply {
            self.purge(h, key);
            if !self.map.contains(h, key) {
                if !create {
                    return missing;
                }
                self.map.insert(h, key, Value::$variant(Default::default()));
            }
            let Some(Value::$variant(v)) = self.map.get_mut(h, key) else {
                return wrongtype();
            };
            let (reply, changed) = f(v);
            if v.is_empty() {
                self.remove(h, key);
            } else if changed {
                self.touch(h, key);
            }
            reply
        }
    };
}

impl Shard {
    typed!(list, list_edit, List, VecDeque<Bytes>);
    typed!(set_ref, set_edit, Set, Dict<()>);
    typed!(zset, zset_edit, ZSet, ZSet);
    typed!(hash_ref, hash_edit, Hash, Dict<Bytes>);

    pub fn key_op(&mut self, h: u64, key: &[u8], op: KeyOp) -> Reply {
        match op {
            KeyOp::IncrByFloat(by) => self.incr_by_float(h, key, by),
            KeyOp::GetRange(start, stop) => match self.live(h, key) {
                None => Reply::Bulk(Vec::new()),
                Some(Value::Str(v)) => match span(start, stop, v.len()) {
                    Some((a, b)) => bulk(&v[a..b]),
                    None => Reply::Bulk(Vec::new()),
                },
                Some(_) => wrongtype(),
            },
            KeyOp::SetRange(offset, patch) => self.set_range(h, key, offset, &patch),
            KeyOp::HKeys | KeyOp::HVals => match self.hash_ref(h, key) {
                Err(wrong_type) => wrong_type,
                Ok(hash) => {
                    let mut out = Vec::with_capacity(hash.map_or(0, Dict::len));
                    if let Some(hash) = hash {
                        let keys = op == KeyOp::HKeys;
                        hash.for_each(|f, v| out.push(bulk(if keys { f } else { v })));
                    }
                    Reply::Array(out)
                }
            },
            KeyOp::HStrlen(field) => match self.hash_ref(h, key) {
                Err(wrong_type) => wrong_type,
                Ok(hash) => Reply::Int(
                    hash.and_then(|hash| hash.get(hash_bytes(&field), &field))
                        .map_or(0, Vec::len) as i64,
                ),
            },
            KeyOp::HIncrByFloat(field, by) => self.hincr_by_float(h, key, &field, by),
            KeyOp::HRandField { count, values } => self.hrandfield(h, key, count, values),
            KeyOp::HExpire { when, cond, fields } => self.hexpire(h, key, when, cond, &fields),
            KeyOp::HTtl {
                fields,
                millis,
                absolute,
            } => self.httl(h, key, &fields, millis, absolute),
            KeyOp::HPersist(fields) => self.hpersist(h, key, &fields),
            KeyOp::HGetDel(fields) => self.hgetdel(h, key, &fields),
            KeyOp::HGetEx { ttl, fields } => self.hgetex(h, key, &ttl, &fields),
            KeyOp::HSetEx {
                cond,
                fcond,
                ttl,
                pairs,
            } => self.hsetex(h, key, cond, fcond, &ttl, pairs),
            KeyOp::DelIfEq(value) => self.delifeq(h, key, &value),
            KeyOp::MemoryUsage(samples) => self.memory_usage(h, key, samples),
            KeyOp::PfAdd(items) => self.pfadd(h, key, &items),
            KeyOp::Dump => self.dump(h, key),
            KeyOp::Restore {
                ttl,
                absolute,
                replace,
                payload,
            } => self.restore_dump(h, key, &payload, ttl, absolute, replace),
            // Asking about a key is not a use of it.
            KeyOp::Object(sub) => match (self.live_quiet(h, key), sub) {
                (None, _) => Reply::Nil,
                (Some(_), ObjectSub::RefCount) => Reply::Int(1),
                (Some(_), ObjectSub::Zero) => Reply::Int(0),
                (Some(_), ObjectSub::Freq) => Reply::Int(self.map.freq(h, key).unwrap_or(0).into()),
                (Some(value), ObjectSub::Encoding) => Reply::bulk(match value {
                    Value::Str(v) if parse_i64(v).is_some() => "int",
                    Value::Str(v) if v.len() <= 44 => "embstr",
                    Value::Str(_) => "raw",
                    Value::Hash(_) | Value::Set(_) => "hashtable",
                    Value::List(_) => "quicklist",
                    Value::ZSet(_) => "skiplist",
                    Value::Stream(_) => "stream",
                    Value::Bloom(_) | Value::Json(_) => "raw",
                }),
            },
            op @ (KeyOp::Push { .. }
            | KeyOp::Pop { .. }
            | KeyOp::LLen
            | KeyOp::LRange(..)
            | KeyOp::LIndex(_)
            | KeyOp::LSet(..)
            | KeyOp::LRem(..)
            | KeyOp::LTrim(..)
            | KeyOp::LInsert { .. }
            | KeyOp::LPos { .. }) => self.list_op(h, key, op),
            op @ (KeyOp::SAdd(_)
            | KeyOp::SRem(_)
            | KeyOp::SIsMember(_)
            | KeyOp::SMIsMember(_)
            | KeyOp::SMembers
            | KeyOp::SCard
            | KeyOp::SPop(_)
            | KeyOp::SRandMember(_)
            | KeyOp::SScan { .. }) => self.set_op(h, key, op),
            op @ (KeyOp::ZAdd { .. }
            | KeyOp::ZRem(_)
            | KeyOp::ZScore(_)
            | KeyOp::ZMScore(_)
            | KeyOp::ZIncrBy(..)
            | KeyOp::ZCard
            | KeyOp::ZCount(..)
            | KeyOp::ZRank { .. }
            | KeyOp::ZRange(_)
            | KeyOp::ZRemRange(_)
            | KeyOp::ZPop { .. }
            | KeyOp::ZScan { .. }
            | KeyOp::ZLexCount(..)
            | KeyOp::ZRandMember { .. }) => self.zset_op(h, key, op),
            KeyOp::X(op) => self.stream_op(h, key, *op),
            KeyOp::Bloom(cmd) => self.bloom_op(h, key, *cmd),
            KeyOp::Json(cmd) => self.json_op(h, key, *cmd),
            KeyOp::SetBit(offset, bit) => self.set_bit(h, key, offset, bit),
            KeyOp::BitField(ops) => self.bitfield(h, key, &ops),
            KeyOp::Geo(op) => self.geo_op(h, key, *op),
            KeyOp::GetBit(offset) => match self.live(h, key) {
                None => Reply::Int(0),
                Some(Value::Str(v)) => Reply::Int(bit_at(v, offset).into()),
                Some(_) => wrongtype(),
            },
            KeyOp::BitCount(range) => match self.live(h, key) {
                None => Reply::Int(0),
                Some(Value::Str(v)) => Reply::Int(bit_count(v, range) as i64),
                Some(_) => wrongtype(),
            },
            KeyOp::BitPos {
                bit,
                start,
                end,
                bits,
            } => match self.live(h, key) {
                // Nothing there is all zeros.
                None => Reply::Int(if bit { -1 } else { 0 }),
                Some(Value::Str(v)) => Reply::Int(bit_pos(v, bit, start, end, bits)),
                Some(_) => wrongtype(),
            },
        }
    }

    fn set_bit(&mut self, h: u64, key: &[u8], offset: usize, bit: bool) -> Reply {
        self.purge(h, key);
        if !self.map.contains(h, key) {
            self.map.insert(h, key, Value::Str(Vec::new()));
        }
        let Some(Value::Str(v)) = self.map.get_mut(h, key) else {
            return wrongtype();
        };
        let (byte, mask) = (offset / 8, 0x80u8 >> (offset % 8));
        if v.len() <= byte {
            v.resize(byte + 1, 0);
        }
        let old = v[byte] & mask != 0;
        if bit {
            v[byte] |= mask;
        } else {
            v[byte] &= !mask;
        }
        self.touch(h, key);
        Reply::Int(old.into())
    }

    fn bitfield(&mut self, h: u64, key: &[u8], ops: &[bitfield::Op]) -> Reply {
        if !ops.iter().any(bitfield::Op::writes) {
            // Reading creates nothing.
            return match self.live(h, key) {
                None => bitfield::read(&[], ops),
                Some(Value::Str(v)) => bitfield::read(v, ops),
                Some(_) => wrongtype(),
            };
        }
        self.purge(h, key);
        if !self.map.contains(h, key) {
            self.map.insert(h, key, Value::Str(Vec::new()));
        }
        let Some(Value::Str(v)) = self.map.get_mut(h, key) else {
            return wrongtype();
        };
        let reply = bitfield::run(v, ops);
        self.touch(h, key);
        reply
    }

    fn geo_op(&self, h: u64, key: &[u8], op: GeoOp) -> Reply {
        let zset = match self.zset(h, key) {
            Ok(zset) => zset,
            Err(wrong_type) => return wrong_type,
        };
        let bits = |member: &Bytes| zset.and_then(|z| z.score(member)).map(|score| score as u64);
        match op {
            GeoOp::Pos(members) => Reply::Array(
                members
                    .iter()
                    .map(|m| bits(m).map_or(Reply::NilArray, geo::position_reply))
                    .collect(),
            ),
            GeoOp::Hash(members) => Reply::Array(
                members
                    .iter()
                    .map(|m| bits(m).map_or(Reply::Nil, |b| bulk(&geo::hash_string(b))))
                    .collect(),
            ),
            GeoOp::Dist(from, to, unit) => match (bits(&from), bits(&to)) {
                (Some(a), Some(b)) => {
                    let ((lon1, lat1), (lon2, lat2)) = (geo::decode(a), geo::decode(b));
                    Reply::Bulk(geo::dist_text(geo::distance(lon1, lat1, lon2, lat2), unit))
                }
                _ => Reply::Nil,
            },
            GeoOp::Search(query) => match self.geo_hits(h, key, &query) {
                Ok(hits) => query.reply(hits),
                Err(e) => e,
            },
        }
    }

    /// The members of the sorted set at `key` that a search finds. The
    /// whole set is looked at: there is no index by area.
    pub fn geo_hits(&self, h: u64, key: &[u8], query: &geo::Query) -> Result<Vec<geo::Hit>, Reply> {
        let Some(zset) = self.zset(h, key)? else {
            return Ok(Vec::new());
        };
        let centre = match &query.origin {
            geo::Origin::LonLat(lon, lat) => (*lon, *lat),
            geo::Origin::Member(member) => match zset.score(member) {
                Some(score) => geo::decode(score as u64),
                None => return Err(Reply::err("ERR could not decode requested zset member")),
            },
        };
        Ok(query.select(centre, zset.range(0, usize::MAX)))
    }

    /// What SORT orders: the elements of a list, set or sorted set, and
    /// whether they come in an order of their own (a sorted set's). A set
    /// comes sorted by bytes, so that the reply does not depend on how the
    /// set happens to be stored.
    pub fn sort_source(&self, h: u64, key: &[u8]) -> Result<(Vec<Bytes>, bool), Reply> {
        match self.live(h, key) {
            None => Ok((Vec::new(), false)),
            Some(Value::List(list)) => Ok((list.iter().cloned().collect(), false)),
            Some(Value::Set(_)) => {
                let mut members = self.set_members(h, key)?;
                members.sort_unstable();
                Ok((members, false))
            }
            Some(Value::ZSet(z)) => Ok((
                z.range(0, usize::MAX).map(|(m, _)| m.to_vec()).collect(),
                true,
            )),
            Some(_) => Err(wrongtype()),
        }
    }

    /// What a BY or GET pattern of SORT reads: the string at `key`, or
    /// with `field` that field of the hash there.
    pub fn sort_value(&self, h: u64, key: &[u8], field: Option<&[u8]>) -> Option<Bytes> {
        match (self.live(h, key)?, field) {
            (Value::Str(v), None) => Some(v.clone()),
            (Value::Hash(hash), Some(field)) => hash.get(hash_bytes(field), field).cloned(),
            _ => None,
        }
    }

    /// Replace `key` with this list; an empty one deletes the key.
    pub fn store_list(&mut self, h: u64, key: &[u8], items: Vec<Bytes>) {
        self.discard(h, key, Reason::ServerDel);
        if !items.is_empty() {
            self.put(h, key, Value::List(items.into()));
        }
    }

    fn incr_by_float(&mut self, h: u64, key: &[u8], by: f64) -> Reply {
        self.purge(h, key);
        let current = match self.map.get(h, key) {
            None => 0.0,
            Some(Value::Str(v)) => match parse_float(v) {
                Some(n) => n,
                None => return Reply::err(NOT_FLOAT),
            },
            Some(_) => return wrongtype(),
        };
        let next = current + by;
        if !next.is_finite() {
            return Reply::err("ERR increment would produce NaN or Infinity");
        }
        let text = format_score(next);
        self.put(h, key, Value::Str(text.clone()));
        Reply::Bulk(text)
    }

    fn set_range(&mut self, h: u64, key: &[u8], offset: usize, patch: &[u8]) -> Reply {
        self.purge(h, key);
        let end = offset.saturating_add(patch.len());
        if end > MAX_STRING {
            return Reply::err("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
        }
        match self.map.get_mut(h, key) {
            Some(Value::Str(v)) => {
                if patch.is_empty() {
                    return Reply::Int(v.len() as i64);
                }
                if v.len() < end {
                    v.resize(end, 0);
                }
                v[offset..end].copy_from_slice(patch);
                let len = v.len();
                self.touch(h, key);
                Reply::Int(len as i64)
            }
            Some(_) => wrongtype(),
            None if patch.is_empty() => Reply::Int(0),
            None => {
                let mut v = vec![0; end];
                v[offset..].copy_from_slice(patch);
                self.put(h, key, Value::Str(v));
                Reply::Int(end as i64)
            }
        }
    }

    fn hincr_by_float(&mut self, h: u64, key: &[u8], field: &[u8], by: f64) -> Reply {
        let fh = hash_bytes(field);
        let current = match self.hash_ref(h, key) {
            Err(wrong_type) => return wrong_type,
            Ok(hash) => match hash.and_then(|hash| hash.get(fh, field)) {
                None => 0.0,
                Some(v) => match parse_float(v) {
                    Some(n) => n,
                    None => return Reply::err("ERR hash value is not a float"),
                },
            },
        };
        let next = current + by;
        if !next.is_finite() {
            return Reply::err("ERR increment would produce NaN or Infinity");
        }
        let text = format_score(next);
        self.hash_edit(h, key, true, Reply::Nil, |hash| {
            hash.insert(fh, field, text.clone());
            (Reply::Bulk(text), true)
        })
    }

    fn hrandfield(&mut self, h: u64, key: &[u8], count: Option<i64>, values: bool) -> Reply {
        let mut rng = self.rng();
        let mut rand = || next_rand(&mut rng);
        let hash = match self.hash_ref(h, key) {
            Err(wrong_type) => return wrong_type,
            Ok(None) if count.is_none() => return Reply::Nil,
            Ok(None) => return Reply::Array(Vec::new()),
            Ok(Some(hash)) => hash,
        };
        let fields = match count {
            None => {
                let reply = hash.pick(rand()).map_or(Reply::Nil, |(f, _)| bulk(f));
                self.rng = rng;
                return reply;
            }
            Some(n) if n >= 0 => sample(hash, n as usize, rand),
            Some(n) => (0..n.unsigned_abs())
                .filter_map(|_| hash.pick(rand()).map(|(f, _)| f.to_vec()))
                .collect(),
        };
        let mut out = Vec::with_capacity(fields.len() * 2);
        for field in fields {
            let value = hash.get(hash_bytes(&field), &field).cloned();
            out.push(Reply::Bulk(field));
            if values {
                out.push(Reply::Bulk(value.unwrap_or_default()));
            }
        }
        self.rng = rng;
        Reply::Array(out)
    }

    fn list_op(&mut self, h: u64, key: &[u8], op: KeyOp) -> Reply {
        // Reads first; the writes below go through `list_edit`.
        let list = match self.list(h, key) {
            Err(wrong_type) => return wrong_type,
            Ok(list) => list,
        };
        match op {
            KeyOp::LLen => Reply::Int(list.map_or(0, VecDeque::len) as i64),
            KeyOp::LRange(start, stop) => {
                let Some(list) = list else {
                    return Reply::Array(Vec::new());
                };
                match span(start, stop, list.len()) {
                    Some((a, b)) => Reply::Array(list.range(a..b).map(|v| bulk(v)).collect()),
                    None => Reply::Array(Vec::new()),
                }
            }
            KeyOp::LIndex(index) => list
                .and_then(|list| list.get(at(index, list.len())?))
                .map_or(Reply::Nil, |v| bulk(v)),
            KeyOp::LPos {
                item,
                rank,
                count,
                maxlen,
            } => {
                let empty = VecDeque::new();
                let list = list.unwrap_or(&empty);
                let scanned = if maxlen == 0 { list.len() } else { maxlen };
                let wanted = count.map_or(1, |c| if c == 0 { usize::MAX } else { c });
                let mut skip = rank.unsigned_abs() - 1;
                let mut hits = Vec::new();
                let mut visit = |i: usize, v: &Bytes| {
                    if *v == item && hits.len() < wanted {
                        if skip > 0 {
                            skip -= 1;
                        } else {
                            hits.push(Reply::Int(i as i64));
                        }
                    }
                };
                if rank > 0 {
                    list.iter()
                        .enumerate()
                        .take(scanned)
                        .for_each(|(i, v)| visit(i, v));
                } else {
                    let all = list.iter().enumerate().rev().take(scanned);
                    all.for_each(|(i, v)| visit(i, v));
                }
                match count {
                    Some(_) => Reply::Array(hits),
                    None => hits.pop().unwrap_or(Reply::Nil),
                }
            }
            KeyOp::Push {
                left,
                existing,
                items,
            } => self.list_edit(h, key, !existing, Reply::Int(0), |list| {
                for item in items {
                    if left {
                        list.push_front(item);
                    } else {
                        list.push_back(item);
                    }
                }
                (Reply::Int(list.len() as i64), true)
            }),
            KeyOp::Pop { left, count } => {
                let missing = if count.is_some() {
                    Reply::NilArray
                } else {
                    Reply::Nil
                };
                self.list_edit(h, key, false, missing, |list| {
                    let mut pop = || {
                        if left {
                            list.pop_front()
                        } else {
                            list.pop_back()
                        }
                    };
                    match count {
                        None => (pop().map_or(Reply::Nil, Reply::Bulk), true),
                        Some(n) => {
                            let out: Vec<_> = (0..n).map_while(|_| pop()).collect();
                            let changed = !out.is_empty();
                            (
                                Reply::Array(out.into_iter().map(Reply::Bulk).collect()),
                                changed,
                            )
                        }
                    }
                })
            }
            KeyOp::LSet(index, item) => self.list_edit(h, key, false, Reply::err(NO_KEY), |list| {
                match at(index, list.len()) {
                    Some(i) => {
                        list[i] = item;
                        (Reply::Ok, true)
                    }
                    None => (Reply::err(BAD_INDEX), false),
                }
            }),
            KeyOp::LRem(count, item) => self.list_edit(h, key, false, Reply::Int(0), |list| {
                let before = list.len();
                let mut left = if count == 0 {
                    usize::MAX
                } else {
                    count.unsigned_abs() as usize
                };
                let mut keep = |v: &Bytes| {
                    if left > 0 && *v == item {
                        left -= 1;
                        return false;
                    }
                    true
                };
                if count >= 0 {
                    list.retain(keep);
                } else {
                    // From the tail: decide back to front, then drop.
                    let mut flags: Vec<bool> = list.iter().rev().map(&mut keep).collect();
                    flags.reverse();
                    let mut flags = flags.into_iter();
                    list.retain(|_| flags.next().unwrap_or(true));
                }
                let removed = before - list.len();
                (Reply::Int(removed as i64), removed > 0)
            }),
            KeyOp::LTrim(start, stop) => self.list_edit(h, key, false, Reply::Ok, |list| {
                match span(start, stop, list.len()) {
                    Some((a, b)) => {
                        list.truncate(b);
                        list.drain(..a);
                    }
                    None => list.clear(),
                }
                (Reply::Ok, true)
            }),
            KeyOp::LInsert {
                before,
                pivot,
                item,
            } => self.list_edit(h, key, false, Reply::Int(0), |list| {
                match list.iter().position(|v| *v == pivot) {
                    Some(i) => {
                        list.insert(if before { i } else { i + 1 }, item);
                        (Reply::Int(list.len() as i64), true)
                    }
                    None => (Reply::Int(-1), false),
                }
            }),
            _ => Reply::err("ERR internal: not a list command"),
        }
    }

    fn set_op(&mut self, h: u64, key: &[u8], op: KeyOp) -> Reply {
        let mut rng = self.rng();
        let set = match self.set_ref(h, key) {
            Err(wrong_type) => return wrong_type,
            Ok(set) => set,
        };
        let has = |m: &Bytes| set.is_some_and(|set| set.contains(hash_bytes(m), m));
        match op {
            KeyOp::SCard => Reply::Int(set.map_or(0, Dict::len) as i64),
            KeyOp::SIsMember(member) => Reply::Int(has(&member).into()),
            KeyOp::SMIsMember(members) => {
                Reply::Array(members.iter().map(|m| Reply::Int(has(m).into())).collect())
            }
            KeyOp::SMembers => {
                let mut out = Vec::with_capacity(set.map_or(0, Dict::len));
                if let Some(set) = set {
                    set.for_each(|m, _| out.push(bulk(m)));
                }
                Reply::Array(out)
            }
            KeyOp::SScan {
                cursor,
                pattern,
                count,
            } => {
                let mut out = Vec::new();
                let next = set.map_or(0, |set| {
                    set.scan_chunk(cursor, count, |m, _| {
                        if pattern.as_ref().is_none_or(|p| glob_match(p, m)) {
                            out.push(bulk(m));
                        }
                    })
                });
                scan_reply(next, out)
            }
            KeyOp::SRandMember(count) => {
                let Some(set) = set else {
                    return if count.is_some() {
                        Reply::Array(Vec::new())
                    } else {
                        Reply::Nil
                    };
                };
                let mut rand = || next_rand(&mut rng);
                let reply = match count {
                    None => set.pick(rand()).map_or(Reply::Nil, |(m, _)| bulk(m)),
                    Some(n) if n >= 0 => Reply::Array(
                        sample(set, n as usize, rand)
                            .into_iter()
                            .map(Reply::Bulk)
                            .collect(),
                    ),
                    Some(n) => Reply::Array(
                        (0..n.unsigned_abs())
                            .filter_map(|_| set.pick(rand()).map(|(m, _)| bulk(m)))
                            .collect(),
                    ),
                };
                self.rng = rng;
                reply
            }
            KeyOp::SPop(count) => {
                let Some(set) = set else {
                    return if count.is_some() {
                        Reply::Array(Vec::new())
                    } else {
                        Reply::Nil
                    };
                };
                let picked = sample(set, count.unwrap_or(1), || next_rand(&mut rng));
                self.rng = rng;
                self.set_edit(h, key, false, Reply::Nil, |set| {
                    for m in &picked {
                        set.remove(hash_bytes(m), m);
                    }
                    let changed = !picked.is_empty();
                    let mut items = picked.into_iter().map(Reply::Bulk);
                    let reply = match count {
                        None => items.next().unwrap_or(Reply::Nil),
                        Some(_) => Reply::Array(items.collect()),
                    };
                    (reply, changed)
                })
            }
            KeyOp::SAdd(members) => self.set_edit(h, key, true, Reply::Nil, |set| {
                let added = members
                    .iter()
                    .filter(|m| set.insert(hash_bytes(m), m, ()).is_none())
                    .count();
                (Reply::Int(added as i64), added > 0)
            }),
            KeyOp::SRem(members) => self.set_edit(h, key, false, Reply::Int(0), |set| {
                let removed = members
                    .iter()
                    .filter(|m| set.remove(hash_bytes(m), m).is_some())
                    .count();
                (Reply::Int(removed as i64), removed > 0)
            }),
            _ => Reply::err("ERR internal: not a set command"),
        }
    }

    fn zset_op(&mut self, h: u64, key: &[u8], op: KeyOp) -> Reply {
        let mut rng = self.rng();
        let zset = match self.zset(h, key) {
            Err(wrong_type) => return wrong_type,
            Ok(zset) => zset,
        };
        match op {
            KeyOp::ZLexCount(min, max) => {
                let (a, b) = zset.map_or((0, 0), |z| lex_span(z, &min, &max));
                Reply::Int((b - a) as i64)
            }
            KeyOp::ZRandMember { count, withscores } => {
                let Some(z) = zset else {
                    return if count.is_some() {
                        Reply::Array(Vec::new())
                    } else {
                        Reply::Nil
                    };
                };
                let mut rand = || next_rand(&mut rng);
                let members = match count {
                    None => {
                        let reply = z.dict().pick(rand()).map_or(Reply::Nil, |(m, _)| bulk(m));
                        self.rng = rng;
                        return reply;
                    }
                    Some(n) if n >= 0 => sample(z.dict(), n as usize, rand),
                    Some(n) => (0..n.unsigned_abs())
                        .filter_map(|_| z.dict().pick(rand()).map(|(m, _)| m.to_vec()))
                        .collect(),
                };
                let mut out = Vec::with_capacity(members.len() * 2);
                for member in members {
                    let s = z.score(&member);
                    out.push(Reply::Bulk(member));
                    if withscores {
                        out.push(score(s.unwrap_or_default()));
                    }
                }
                self.rng = rng;
                Reply::Array(out)
            }
            KeyOp::ZCard => Reply::Int(zset.map_or(0, ZSet::len) as i64),
            KeyOp::ZScore(member) => zset
                .and_then(|z| z.score(&member))
                .map_or(Reply::Nil, score),
            KeyOp::ZMScore(members) => Reply::Array(
                members
                    .iter()
                    .map(|m| zset.and_then(|z| z.score(m)).map_or(Reply::Nil, score))
                    .collect(),
            ),
            KeyOp::ZCount(min, max) => {
                let (a, b) = zset.map_or((0, 0), |z| score_span(z, min, max));
                Reply::Int((b - a) as i64)
            }
            KeyOp::ZRank {
                member,
                rev,
                withscore,
            } => {
                let found = zset.and_then(|z| {
                    let rank = z.rank(&member)?;
                    let rank = if rev { z.len() - 1 - rank } else { rank };
                    Some((rank, z.score(&member)?))
                });
                match found {
                    None if withscore => Reply::NilArray,
                    None => Reply::Nil,
                    Some((rank, s)) if withscore => {
                        Reply::Array(vec![Reply::Int(rank as i64), score(s)])
                    }
                    Some((rank, _)) => Reply::Int(rank as i64),
                }
            }
            KeyOp::ZRange(spec) => {
                let mut out = Vec::new();
                for (member, s) in zset.map_or_else(Vec::new, |z| zrange(z, &spec)) {
                    out.push(bulk(member));
                    if spec.withscores {
                        out.push(score(s));
                    }
                }
                Reply::Array(out)
            }
            KeyOp::ZScan {
                cursor,
                pattern,
                count,
            } => {
                let mut out = Vec::new();
                let next = zset.map_or(0, |z| {
                    z.scan_chunk(cursor, count, |m, s| {
                        if pattern.as_ref().is_none_or(|p| glob_match(p, m)) {
                            out.push(bulk(m));
                            out.push(score(*s));
                        }
                    })
                });
                scan_reply(next, out)
            }
            KeyOp::ZAdd { flags, pairs } => {
                self.zset_edit(h, key, true, Reply::Nil, |z| zadd(z, flags, pairs))
            }
            KeyOp::ZIncrBy(by, member) => self.zset_edit(h, key, true, Reply::Nil, |z| {
                let next = z.score(&member).unwrap_or(0.0) + by;
                if next.is_nan() {
                    return (Reply::err(NAN_SCORE), false);
                }
                z.insert(&member, next);
                (score(next), true)
            }),
            KeyOp::ZRem(members) => self.zset_edit(h, key, false, Reply::Int(0), |z| {
                let removed = members.iter().filter(|m| z.remove(m).is_some()).count();
                (Reply::Int(removed as i64), removed > 0)
            }),
            KeyOp::ZRemRange(by) => self.zset_edit(h, key, false, Reply::Int(0), |z| {
                let (a, b) = zspan(z, &by);
                let removed = z.remove_range(a, b).len();
                (Reply::Int(removed as i64), removed > 0)
            }),
            KeyOp::ZPop { max, count } => {
                self.zset_edit(h, key, false, Reply::Array(Vec::new()), |z| {
                    let n = count.unwrap_or(1).min(z.len());
                    let mut gone = if max {
                        z.remove_range(z.len() - n, z.len())
                    } else {
                        z.remove_range(0, n)
                    };
                    if max {
                        gone.reverse();
                    }
                    let changed = !gone.is_empty();
                    let flat = gone
                        .into_iter()
                        .flat_map(|(m, s)| [Reply::Bulk(m), score(s)]);
                    (Reply::Array(flat.collect()), changed)
                })
            }
            _ => Reply::err("ERR internal: not a sorted set command"),
        }
    }

    // What the commands over several keys need from one shard.

    /// PFADD: 1 when the estimate may have changed or the key was created.
    fn pfadd(&mut self, h: u64, key: &[u8], items: &[Bytes]) -> Reply {
        self.purge(h, key);
        let invalid = |_| Reply::err(super::NOT_HLL);
        match self.map.get_mut(h, key) {
            None => {
                let mut sketch = hll::new();
                for item in items {
                    if let Err(e) = hll::add(&mut sketch, item) {
                        return invalid(e);
                    }
                }
                self.put(h, key, Value::Str(sketch));
                Reply::Int(1)
            }
            Some(Value::Str(sketch)) => {
                if let Err(e) = hll::validate(sketch) {
                    return invalid(e);
                }
                let mut changed = false;
                for item in items {
                    match hll::add(sketch, item) {
                        Ok(grew) => changed |= grew,
                        Err(e) => return invalid(e),
                    }
                }
                if changed {
                    self.touch(h, key);
                }
                Reply::Int(changed.into())
            }
            Some(_) => wrongtype(),
        }
    }

    /// Put a string at `key`, keeping the deadline of a string that is
    /// there already.
    pub fn replace_string(&mut self, h: u64, key: &[u8], value: Bytes) {
        self.purge(h, key);
        match self.map.get_mut(h, key) {
            Some(Value::Str(old)) => {
                *old = value;
                self.touch(h, key);
            }
            _ => {
                self.discard(h, key, Reason::ServerDel);
                self.put(h, key, Value::Str(value));
            }
        }
    }

    /// The entries a ZRANGE with `spec` selects, for ZRANGESTORE.
    pub fn zrange_entries(
        &self,
        h: u64,
        key: &[u8],
        spec: &ZRangeSpec,
    ) -> Result<Vec<(Bytes, f64)>, Reply> {
        Ok(self.zset(h, key)?.map_or_else(Vec::new, |z| {
            zrange(z, spec)
                .into_iter()
                .map(|(m, s)| (m.to_vec(), s))
                .collect()
        }))
    }

    /// Run one stream command.
    pub fn stream_op(&mut self, h: u64, key: &[u8], op: XOp) -> Reply {
        self.purge(h, key);
        let created = !self.map.contains(h, key);
        if created {
            if !op.creates() {
                return op.missing(key);
            }
            self.map.insert(h, key, Value::Stream(Box::default()));
        }
        let now = self.clock.now_ms();
        let Some(Value::Stream(stream)) = self.map.get_mut(h, key) else {
            return wrongtype();
        };
        let (reply, changed) = stream.apply(key, op, now);
        if changed {
            self.touch(h, key);
        } else if created {
            // The command that would have made the stream failed.
            self.map.remove(h, key);
        }
        reply
    }

    /// Run one BF.* command.
    pub fn bloom_op(&mut self, h: u64, key: &[u8], cmd: BfCmd) -> Reply {
        let BfCmd { op, settings } = cmd;
        // VALIDATESCALETO is checked before the key is looked at.
        if let BfOp::Insert(insert) = &op {
            if let Err(e) = insert.check(&settings) {
                return e;
            }
        }
        if !op.is_write() {
            return match self.live(h, key) {
                None => bloom::read(&op, None, &settings),
                Some(Value::Bloom(b)) => bloom::read(&op, Some(b), &settings),
                Some(_) => wrongtype(),
            };
        }
        self.purge(h, key);
        let done = match self.map.get_mut(h, key) {
            None => bloom::write(op, key, None, &settings),
            Some(Value::Bloom(b)) => bloom::write(op, key, Some(b), &settings),
            Some(_) => return wrongtype(),
        };
        if done.repl.is_empty() {
            return done.reply;
        }
        let created = done.new.is_some();
        if let Some(new) = done.new {
            self.map.insert(h, key, Value::Bloom(new));
        }
        self.touch(h, key);
        if let Some(repl) = &mut self.repl {
            repl.own.extend_from_slice(&done.repl);
        }
        if done.added {
            self.note(Note::BloomAdd, key);
        }
        if created {
            self.note(Note::BloomReserve, key);
        }
        done.reply
    }

    /// Run one JSON.* command.
    pub fn json_op(&mut self, h: u64, key: &[u8], cmd: JsonCmd) -> Reply {
        let JsonCmd { op, settings, repl } = cmd;
        let op = match op {
            JsonOp::Read(op) => {
                return match self.live(h, key) {
                    None => json::read(&op, None, &settings),
                    Some(Value::Json(doc)) => json::read(&op, Some(doc), &settings),
                    Some(_) => json::Error::NotADocument.reply(),
                };
            }
            JsonOp::Write(op) => op,
        };
        let event = op.event();
        self.purge(h, key);
        let done = match self.map.get_mut(h, key) {
            None => json::write(op, None, &settings),
            Some(Value::Json(doc)) => json::write(op, Some(doc), &settings),
            Some(_) => return json::Error::NotADocument.reply(),
        };
        match done.effect {
            Effect::Kept => return done.reply,
            Effect::Changed => self.touch(h, key),
            Effect::Replaced(doc) => {
                self.put(h, key, Value::Json(doc));
                if !self.expires.is_empty() && self.expires.remove(h, key).is_some() {
                    self.map.set_expire(h, key, 0);
                }
            }
            Effect::Deleted => {
                self.discard(h, key, Reason::ServerDel);
            }
        }
        if let Some(r) = &mut self.repl {
            r.own.extend_from_slice(&repl);
        }
        self.note(Note::Json(event), key);
        done.reply
    }

    /// JSON.MGET's answer for one key: `Nil` for a missing key.
    pub fn json_fetch(&self, h: u64, key: &[u8], path: &[u8]) -> Result<Reply, Reply> {
        match self.live(h, key) {
            None => Ok(Reply::Nil),
            Some(Value::Json(doc)) => json::mget_one(doc, path),
            Some(_) => Err(json::Error::NotADocument.reply()),
        }
    }

    /// Whether JSON.MSET may set `path` of `key` to `value`.
    pub fn json_check(
        &self,
        h: u64,
        key: &[u8],
        path: &[u8],
        value: &[u8],
        settings: &JsonSettings,
    ) -> Result<(), Reply> {
        match self.live_quiet(h, key) {
            None => json::mset_check(None, path, value, settings),
            Some(Value::Json(doc)) => json::mset_check(Some(doc), path, value, settings),
            Some(_) => Err(json::Error::NotADocument.reply()),
        }
    }

    /// One key of JSON.MSET, checked by `json_check`.
    pub fn json_mset(
        &mut self,
        h: u64,
        key: &[u8],
        path: Bytes,
        value: Bytes,
        settings: JsonSettings,
    ) {
        let op = WriteOp::Set {
            path,
            json: value,
            nx: false,
            xx: false,
            mset: true,
        };
        let cmd = JsonCmd {
            op: JsonOp::Write(op),
            settings,
            repl: Vec::new(),
        };
        // A key an earlier one of these changed may no longer fit: it is
        // left as it is, as valkey-json leaves it.
        self.json_op(h, key, cmd);
    }

    /// The last ID of the stream at `key`; a missing key has none yet.
    pub fn stream_last(&self, h: u64, key: &[u8]) -> Result<StreamId, Reply> {
        match self.live(h, key) {
            None => Ok(StreamId::MIN),
            Some(Value::Stream(stream)) => Ok(stream.last_id()),
            Some(_) => Err(wrongtype()),
        }
    }

    /// Show `f` the string at `key`; a missing key is the empty string.
    pub fn with_string<R>(
        &self,
        h: u64,
        key: &[u8],
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Reply> {
        match self.live(h, key) {
            None => Ok(f(&[])),
            Some(Value::Str(v)) => Ok(f(v)),
            Some(_) => Err(wrongtype()),
        }
    }

    /// Replace `key` with this string; an empty one deletes the key.
    pub fn store_string(&mut self, h: u64, key: &[u8], value: Bytes) {
        self.discard(h, key, Reason::ServerDel);
        if !value.is_empty() {
            self.put(h, key, Value::Str(value));
        }
    }

    /// The members of the set at `key`; a missing key is the empty set.
    pub fn set_members(&self, h: u64, key: &[u8]) -> Result<Vec<Bytes>, Reply> {
        let mut out = Vec::new();
        if let Some(set) = self.set_ref(h, key)? {
            out.reserve(set.len());
            set.for_each(|m, _| out.push(m.to_vec()));
        }
        Ok(out)
    }

    /// Members and scores of the sorted set at `key`. A plain set counts
    /// as one whose scores are all 1.
    pub fn zset_entries(&self, h: u64, key: &[u8]) -> Result<Vec<(Bytes, f64)>, Reply> {
        match self.live(h, key) {
            None => Ok(Vec::new()),
            Some(Value::ZSet(z)) => Ok(z
                .range(0, usize::MAX)
                .map(|(m, s)| (m.to_vec(), s))
                .collect()),
            Some(Value::Set(_)) => Ok(self
                .set_members(h, key)?
                .into_iter()
                .map(|m| (m, 1.0))
                .collect()),
            Some(_) => Err(wrongtype()),
        }
    }

    /// Replace `key` with a set of these members; none deletes the key.
    pub fn store_set(&mut self, h: u64, key: &[u8], members: &[Bytes]) {
        self.discard(h, key, Reason::ServerDel);
        if members.is_empty() {
            return;
        }
        let mut set = Dict::new();
        for m in members {
            set.insert(hash_bytes(m), m, ());
        }
        self.put(h, key, Value::Set(Box::new(set)));
    }

    pub fn store_zset(&mut self, h: u64, key: &[u8], entries: &[(Bytes, f64)]) {
        self.discard(h, key, Reason::ServerDel);
        if entries.is_empty() {
            return;
        }
        let mut zset = ZSet::default();
        for (m, s) in entries {
            zset.insert(m, *s);
        }
        self.put(h, key, Value::ZSet(Box::new(zset)));
    }

    /// LMOVE of a list onto itself: the element changes ends and the key
    /// keeps its deadline.
    pub fn rotate(&mut self, h: u64, key: &[u8], from_left: bool, to_left: bool) -> Reply {
        self.list_edit(h, key, false, Reply::Nil, |list| {
            let item = if from_left {
                list.pop_front()
            } else {
                list.pop_back()
            };
            let Some(item) = item else {
                return (Reply::Nil, false);
            };
            if to_left {
                list.push_front(item.clone());
            } else {
                list.push_back(item.clone());
            }
            (Reply::Bulk(item), true)
        })
    }

    /// A live key picked at random, for RANDOMKEY.
    pub fn random_key(&mut self) -> Option<Bytes> {
        let mut rng = self.rng();
        let mut found = None;
        for attempt in 0..=RANDOM_KEY_TRIES {
            if attempt == RANDOM_KEY_TRIES {
                // Mostly dead keys: clear them out, then any pick is live.
                self.sweep_all();
            }
            let Some((key, _)) = self.map.pick(next_rand(&mut rng)) else {
                break;
            };
            if !self.expired(hash_bytes(key), key) {
                found = Some(key.to_vec());
                break;
            }
        }
        self.rng = rng;
        found
    }

    pub(super) fn rng(&mut self) -> u64 {
        if self.rng == 0 {
            // Any odd, shard-specific value will do as a seed.
            self.rng = (self.clock.now_ms() ^ (self as *const Shard as u64)) | 1;
        }
        self.rng
    }
}

pub(super) fn next_rand(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    state.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

/// The ranks `a..b` of the members whose score lies between the bounds.
fn score_span(z: &ZSet, min: ZBound, max: ZBound) -> (usize, usize) {
    let a = z.count_below(min.value, min.exclusive);
    let b = z.count_below(max.value, !max.exclusive);
    (a, b.max(a))
}

fn lex_span(z: &ZSet, min: &LexBound, max: &LexBound) -> (usize, usize) {
    let a = match min {
        LexBound::Min => 0,
        LexBound::Max => z.len(),
        LexBound::Incl(m) => z.count_lex_below(m, false),
        LexBound::Excl(m) => z.count_lex_below(m, true),
    };
    let b = match max {
        LexBound::Min => 0,
        LexBound::Max => z.len(),
        LexBound::Incl(m) => z.count_lex_below(m, true),
        LexBound::Excl(m) => z.count_lex_below(m, false),
    };
    (a, b.max(a))
}

/// The ranks `by` selects, lowest first.
fn zspan(z: &ZSet, by: &ZBy) -> (usize, usize) {
    match by {
        ZBy::Rank(start, stop) => span(*start, *stop, z.len()).unwrap_or((0, 0)),
        ZBy::Score(min, max) => score_span(z, *min, *max),
        ZBy::Lex(min, max) => lex_span(z, min, max),
    }
}

fn bit_at(v: &[u8], offset: usize) -> bool {
    v.get(offset / 8)
        .is_some_and(|byte| byte & (0x80 >> (offset % 8)) != 0)
}

/// The bits `start..=end` of `len` bytes select, as a range of bit
/// positions; the two count bytes unless `bits`.
fn bit_span(len: usize, start: i64, end: i64, bits: bool) -> Option<(usize, usize)> {
    let unit = if bits { 1 } else { 8 };
    let total = (len * 8 / unit) as i64;
    if start < 0 && end < 0 && start > end {
        return None;
    }
    // Unlike list ranges, an end before the first position means the first.
    let clamp = |i: i64| {
        if i < 0 {
            total.saturating_add(i).max(0)
        } else {
            i
        }
    };
    let (start, end) = (clamp(start), clamp(end).min(total - 1));
    (start <= end).then(|| (start as usize * unit, (end as usize + 1) * unit))
}

fn bit_count(v: &[u8], range: Option<(i64, i64, bool)>) -> usize {
    let (a, b) = match range {
        None => (0, v.len() * 8),
        Some((start, end, bits)) => match bit_span(v.len(), start, end, bits) {
            Some(range) => range,
            None => return 0,
        },
    };
    // Whole bytes in the middle, single bits at the ragged ends.
    let (first, last) = (a.div_ceil(8), b / 8);
    if first > last {
        return (a..b).filter(|&i| bit_at(v, i)).count();
    }
    let whole: usize = v[first..last].iter().map(|x| x.count_ones() as usize).sum();
    let ends = (a..first * 8).chain(last * 8..b);
    whole + ends.filter(|&i| bit_at(v, i)).count()
}

/// BITPOS: the first bit equal to `bit`, or -1.
fn bit_pos(v: &[u8], bit: bool, start: i64, end: Option<i64>, bits: bool) -> i64 {
    let Some((a, b)) = bit_span(v.len(), start, end.unwrap_or(-1), bits) else {
        return -1;
    };
    let skip = if bit { 0x00 } else { 0xff };
    let mut i = a;
    while i < b {
        // Skip whole bytes that cannot hold the bit.
        if i % 8 == 0 && i + 8 <= b && v[i / 8] == skip {
            i += 8;
        } else if bit_at(v, i) == bit {
            return i as i64;
        } else {
            i += 1;
        }
    }
    // Looking for a clear bit without naming an end: the string counts as
    // followed by zeros.
    if !bit && end.is_none() {
        b as i64
    } else {
        -1
    }
}

fn zrange<'a>(z: &'a ZSet, spec: &ZRangeSpec) -> Vec<(&'a [u8], f64)> {
    let len = z.len();
    let (mut a, mut b) = match &spec.by {
        // In reverse the positions count from the highest score.
        ZBy::Rank(start, stop) if spec.rev => match span(*start, *stop, len) {
            Some((a, b)) => (len - b, len - a),
            None => (0, 0),
        },
        by => zspan(z, by),
    };
    if let Some((offset, count)) = spec.limit {
        let count = count.unwrap_or(usize::MAX);
        if spec.rev {
            b = b.saturating_sub(offset).max(a);
            a = a.max(b.saturating_sub(count));
        } else {
            a = a.saturating_add(offset).min(b);
            b = b.min(a.saturating_add(count));
        }
    }
    let mut rows: Vec<_> = z.range(a, b).collect();
    if spec.rev {
        rows.reverse();
    }
    rows
}

fn zadd(z: &mut ZSet, flags: ZAddFlags, pairs: Vec<(f64, Bytes)>) -> (Reply, bool) {
    let (mut added, mut updated) = (0, 0);
    let mut last = None;
    for (given, member) in pairs {
        let current = z.score(&member);
        let next = match current {
            None if flags.xx => continue,
            None => given,
            Some(_) if flags.nx => continue,
            Some(cur) => {
                let next = if flags.incr { cur + given } else { given };
                if next.is_nan() {
                    return (Reply::err(NAN_SCORE), added + updated > 0);
                }
                if (flags.gt && next <= cur) || (flags.lt && next >= cur) {
                    continue;
                }
                next
            }
        };
        last = Some(next);
        match current {
            None => added += 1,
            Some(cur) if cur != next => updated += 1,
            Some(_) => continue,
        }
        z.insert(&member, next);
    }
    let reply = if flags.incr {
        last.map_or(Reply::Nil, score)
    } else {
        Reply::Int(if flags.ch { added + updated } else { added })
    };
    (reply, added + updated > 0)
}
