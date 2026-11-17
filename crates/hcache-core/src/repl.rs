//! Replication: the stream of changes a primary sends to its replicas, and
//! the backlog a replica that lost the link picks up from.
//!
//! The stream is RESP commands, as Redis sends them, so a Valkey or Redis
//! replica takes it as well as an HCache one. What goes out is what each
//! command did (effects replication, the only mode since Redis 7.0):
//!
//! - a command whose outcome depends on nothing but the keys it finds goes
//!   as it was sent, when it changed something;
//! - one whose outcome also depends on the clock or on chance goes as the
//!   state it left: a deadline as PEXPIREAT, a string as SET ... PXAT, any
//!   other value as RESTORE ... REPLACE ABSTTL, a key that is gone as DEL.
//!   SPOP goes as SREM of what it took, XADD with the ID it chose;
//! - a key whose time came, or that made room, goes as DEL, and a field of
//!   a hash whose time came as HDEL.
//!
//! Order: a command's part of the stream is written while it still holds
//! the shards it ran on, so two commands that share a shard reach the
//! stream in the order they ran; commands on separate shards commute. A
//! transaction or a script, which has the node to itself, goes between
//! MULTI and EXEC, so a replica applies it at once too.
//!
//! Offsets count the bytes of the stream under one replication ID: `end`
//! is Redis' `master_repl_offset`.

use crate::cmd::{Bytes, Class, Cmd};
use crate::engine::Engine;
use crate::ops::{BitOp, KeyOp, MultiOp, SetOp};
use crate::resp::{Limits, Parser, Reply};
use crate::shard::{ReplTrack, Shard, Value};
use crate::stream::{TrimBy, XOp};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// `repl-backlog-size` until CONFIG SET says otherwise, as in Valkey.
pub const BACKLOG_SIZE: usize = 10 << 20;
/// The least `repl-backlog-size` takes, as in Redis.
pub const BACKLOG_MIN: usize = 16 << 10;

/// What a command sends, decided before it runs: it may consume what it
/// was given.
pub(crate) enum Plan {
    /// A read: only keys its lookups found gone go out.
    Quiet,
    /// The command as it was sent, in RESP, when it changed anything.
    Same(Vec<u8>),
    /// The state of every key it changed.
    State(Effect),
    /// SPOP: SREM of the members in the reply.
    SPop(Bytes),
    /// XADD: the arguments before the ID, and the fields. The ID is the
    /// one of the reply.
    XAdd(Vec<Bytes>, Vec<(Bytes, Bytes)>),
    /// BF.*: what the shard wrote down as the command ran.
    Own,
}

/// Which part of a key's state a command changed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Effect {
    /// Its value, and maybe its deadline.
    Value,
    /// Its deadline alone.
    Deadline,
}

/// A command as RESP: an array of bulk strings.
pub(crate) fn command(out: &mut Vec<u8>, args: &[&[u8]]) {
    out.push(b'*');
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for arg in args {
        out.push(b'$');
        out.extend_from_slice(arg.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

fn same(args: &[&[u8]]) -> Plan {
    let mut out = Vec::new();
    command(&mut out, args);
    Plan::Same(out)
}

/// A score as a replica reads back the same double: the shortest form
/// that round-trips, or `inf`.
fn score(value: f64) -> String {
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.into();
    }
    format!("{value:?}")
}

impl Plan {
    pub(crate) fn of(cmd: &Cmd) -> Plan {
        match cmd {
            Cmd::In(_, inner) => Plan::of(inner),
            Cmd::Set(k, v) => same(&[b"SET", k, v]),
            Cmd::SetNx(k, v) => same(&[b"SETNX", k, v]),
            Cmd::GetDel(k) => same(&[b"DEL", k]),
            Cmd::Append(k, v) => same(&[b"APPEND", k, v]),
            Cmd::Persist(k) => same(&[b"PERSIST", k]),
            Cmd::HSetNx(k, f, v) => same(&[b"HSETNX", k, f, v]),
            Cmd::HIncrBy(k, f, by) => same(&[b"HINCRBY", k, f, by.to_string().as_bytes()]),
            Cmd::IncrBy(k, by) => same(&[b"INCRBY", k, by.to_string().as_bytes()]),
            Cmd::HSet(k, pairs) | Cmd::HMSet(k, pairs) => {
                let mut args: Vec<&[u8]> = vec![b"HSET", k];
                for (f, v) in pairs {
                    args.extend([f.as_slice(), v.as_slice()]);
                }
                same(&args)
            }
            Cmd::HDel(k, fields) => same(&with(&[b"HDEL", k], fields)),
            Cmd::Del(keys) => same(&with(&[b"DEL"], keys)),
            Cmd::Unlink(keys) => same(&with(&[b"UNLINK"], keys)),
            Cmd::MSet(pairs) => {
                let mut args: Vec<&[u8]> = vec![b"MSET"];
                for (k, v) in pairs {
                    args.extend([k.as_slice(), v.as_slice()]);
                }
                same(&args)
            }
            // Relative times and NX, XX, GET: what SET left is the effect.
            Cmd::SetOpts(..) => Plan::State(Effect::Value),
            Cmd::GetEx(..) | Cmd::Expire { .. } => Plan::State(Effect::Deadline),
            Cmd::Key(k, op) => key_plan(k, op),
            Cmd::Multi(op) | Cmd::Block(op, _) => multi_plan(op),
            cmd if cmd.is_write() => Plan::State(Effect::Value),
            _ => Plan::Quiet,
        }
    }
}

/// `head` followed by every item of `tail`.
fn with<'a>(head: &[&'a [u8]], tail: &'a [Bytes]) -> Vec<&'a [u8]> {
    let mut args = head.to_vec();
    args.extend(tail.iter().map(Vec::as_slice));
    args
}

fn key_plan(k: &Bytes, op: &KeyOp) -> Plan {
    match op {
        KeyOp::Push {
            left,
            existing,
            items,
        } => {
            let name: &[u8] = match (left, existing) {
                (true, false) => b"LPUSH",
                (false, false) => b"RPUSH",
                (true, true) => b"LPUSHX",
                (false, true) => b"RPUSHX",
            };
            same(&with(&[name, k], items))
        }
        KeyOp::Pop { left, count } => {
            let name: &[u8] = if *left { b"LPOP" } else { b"RPOP" };
            match count {
                Some(n) => same(&[name, k, n.to_string().as_bytes()]),
                None => same(&[name, k]),
            }
        }
        KeyOp::LSet(at, v) => same(&[b"LSET", k, at.to_string().as_bytes(), v]),
        KeyOp::LRem(n, v) => same(&[b"LREM", k, n.to_string().as_bytes(), v]),
        KeyOp::LTrim(from, to) => same(&[
            b"LTRIM",
            k,
            from.to_string().as_bytes(),
            to.to_string().as_bytes(),
        ]),
        KeyOp::LInsert {
            before,
            pivot,
            item,
        } => {
            let at: &[u8] = if *before { b"BEFORE" } else { b"AFTER" };
            same(&[b"LINSERT", k, at, pivot, item])
        }
        KeyOp::SAdd(members) => same(&with(&[b"SADD", k], members)),
        KeyOp::SRem(members) => same(&with(&[b"SREM", k], members)),
        KeyOp::SPop(_) => Plan::SPop(k.clone()),
        KeyOp::ZAdd { flags, pairs } => {
            let mut args: Vec<Bytes> = vec![b"ZADD".to_vec(), k.clone()];
            for (on, name) in [
                (flags.nx, "NX"),
                (flags.xx, "XX"),
                (flags.gt, "GT"),
                (flags.lt, "LT"),
                (flags.ch, "CH"),
                (flags.incr, "INCR"),
            ] {
                if on {
                    args.push(name.into());
                }
            }
            for (s, member) in pairs {
                args.push(score(*s).into_bytes());
                args.push(member.clone());
            }
            same(&args.iter().map(Vec::as_slice).collect::<Vec<_>>())
        }
        KeyOp::ZRem(members) => same(&with(&[b"ZREM", k], members)),
        KeyOp::ZIncrBy(by, member) => same(&[b"ZINCRBY", k, score(*by).as_bytes(), member]),
        KeyOp::ZPop { max, count } => {
            let name: &[u8] = if *max { b"ZPOPMAX" } else { b"ZPOPMIN" };
            match count {
                Some(n) => same(&[name, k, n.to_string().as_bytes()]),
                None => same(&[name, k]),
            }
        }
        KeyOp::SetBit(at, bit) => {
            let bit: &[u8] = if *bit { b"1" } else { b"0" };
            same(&[b"SETBIT", k, at.to_string().as_bytes(), bit])
        }
        KeyOp::SetRange(at, v) => same(&[b"SETRANGE", k, at.to_string().as_bytes(), v]),
        KeyOp::PfAdd(items) => same(&with(&[b"PFADD", k], items)),
        KeyOp::X(op) => stream_plan(k, op),
        KeyOp::Bloom(cmd) if cmd.is_write() => Plan::Own,
        KeyOp::Json(cmd) if cmd.is_write() => Plan::Own,
        op if op.is_write() => Plan::State(Effect::Value),
        _ => Plan::Quiet,
    }
}

fn trim_args(by: TrimBy) -> [Bytes; 3] {
    let (name, at) = match by {
        TrimBy::MaxLen(n) => ("MAXLEN", n.to_string()),
        TrimBy::MinId(id) => ("MINID", id.to_string()),
    };
    [name.into(), b"=".to_vec(), at.into_bytes()]
}

fn stream_plan(k: &Bytes, op: &XOp) -> Plan {
    match op {
        // A trim to the nearest node depends on how the nodes fell: only an
        // exact one gives the same stream everywhere.
        XOp::Add {
            fields,
            nomkstream,
            trim,
            ..
        } if trim.is_none_or(|t| !t.approx) => {
            let mut before = vec![b"XADD".to_vec(), k.clone()];
            if *nomkstream {
                before.push(b"NOMKSTREAM".to_vec());
            }
            if let Some(trim) = trim {
                before.extend(trim_args(trim.by));
            }
            Plan::XAdd(before, fields.clone())
        }
        XOp::Trim(trim) if !trim.approx => {
            let [name, eq, at] = trim_args(trim.by);
            same(&[b"XTRIM", k, &name, &eq, &at])
        }
        XOp::Del(ids) => {
            let ids: Vec<Bytes> = ids.iter().map(|id| id.to_string().into_bytes()).collect();
            same(&with(&[b"XDEL", k], &ids))
        }
        XOp::Ack { group, ids } => {
            let ids: Vec<Bytes> = ids.iter().map(|id| id.to_string().into_bytes()).collect();
            same(&with(&[b"XACK", k, group], &ids))
        }
        XOp::GroupDestroy { group } => same(&[b"XGROUP", b"DESTROY", k, group]),
        XOp::GroupCreateConsumer { group, consumer } => {
            same(&[b"XGROUP", b"CREATECONSUMER", k, group, consumer])
        }
        XOp::GroupDelConsumer { group, consumer } => {
            same(&[b"XGROUP", b"DELCONSUMER", k, group, consumer])
        }
        op if op.is_write() => Plan::State(Effect::Value),
        _ => Plan::Quiet,
    }
}

fn multi_plan(op: &MultiOp) -> Plan {
    let side = |left: bool| -> &'static [u8] {
        if left {
            b"LEFT"
        } else {
            b"RIGHT"
        }
    };
    match op {
        MultiOp::Rename { src, dst, nx } => {
            let name: &[u8] = if *nx { b"RENAMENX" } else { b"RENAME" };
            same(&[name, src, dst])
        }
        MultiOp::Copy {
            src,
            dst,
            replace,
            db,
        } => {
            let mut args: Vec<Bytes> = vec![b"COPY".to_vec(), src.clone(), dst.clone()];
            if let Some(db) = db {
                args.extend([b"DB".to_vec(), db.to_string().into_bytes()]);
            }
            if *replace {
                args.push(b"REPLACE".to_vec());
            }
            same(&args.iter().map(Vec::as_slice).collect::<Vec<_>>())
        }
        MultiOp::MSetNx(pairs) => {
            let mut args: Vec<&[u8]> = vec![b"MSETNX"];
            for (k, v) in pairs {
                args.extend([k.as_slice(), v.as_slice()]);
            }
            same(&args)
        }
        MultiOp::PfMerge { dst, keys } => same(&with(&[b"PFMERGE", dst], keys)),
        // It wrote the cache in its key; the replica works out the same.
        MultiOp::PfCount(keys) => same(&with(&[b"PFCOUNT"], keys)),
        MultiOp::JsonMSet(triples) => {
            let mut args: Vec<&[u8]> = vec![b"JSON.MSET"];
            for (k, path, value) in triples {
                args.extend([k.as_slice(), path.as_slice(), value.as_slice()]);
            }
            same(&args)
        }
        MultiOp::LMove {
            src,
            dst,
            from_left,
            to_left,
        } => same(&[b"LMOVE", src, dst, side(*from_left), side(*to_left)]),
        MultiOp::SMove { src, dst, member } => same(&[b"SMOVE", src, dst, member]),
        MultiOp::Sets {
            op,
            keys,
            store: Some(dst),
            card: None,
        } => {
            let name: &[u8] = match op {
                SetOp::Inter => b"SINTERSTORE",
                SetOp::Union => b"SUNIONSTORE",
                SetOp::Diff => b"SDIFFSTORE",
            };
            same(&with(&[name, dst], keys))
        }
        MultiOp::MPop {
            keys,
            zset,
            first,
            count,
            ..
        } => {
            let (name, end): (&[u8], &[u8]) = match (zset, first) {
                (false, true) => (b"LMPOP", b"LEFT"),
                (false, false) => (b"LMPOP", b"RIGHT"),
                (true, true) => (b"ZMPOP", b"MIN"),
                (true, false) => (b"ZMPOP", b"MAX"),
            };
            let n = keys.len().to_string();
            let count = count.to_string();
            let mut args = with(&[name, n.as_bytes()], keys);
            args.extend([end, b"COUNT", count.as_bytes()]);
            same(&args)
        }
        MultiOp::BitOp { op, dst, keys } => {
            let name: &[u8] = match op {
                BitOp::And => b"AND",
                BitOp::Or => b"OR",
                BitOp::Xor => b"XOR",
                BitOp::Not => b"NOT",
            };
            same(&with(&[b"BITOP", name, dst], keys))
        }
        op if op.is_write() => Plan::State(Effect::Value),
        _ => Plan::Quiet,
    }
}

/// The DEL and HDEL of what went by itself.
pub(crate) fn encode_gone(out: &mut Vec<u8>, track: &ReplTrack) {
    for key in &track.gone {
        command(out, &[b"DEL", key]);
    }
    for (key, field) in &track.gone_fields {
        command(out, &[b"HDEL", key, field]);
    }
}

/// The state `key` is in, as commands that put a replica's copy in it.
pub(crate) fn encode_state(out: &mut Vec<u8>, shard: &Shard, key: &[u8], effect: Effect) {
    let h = crate::dict::hash_bytes(key);
    let Some((value, deadline)) = shard.repl_state(h, key) else {
        command(out, &[b"DEL", key]);
        return;
    };
    let at = deadline.map(|at| at.to_string());
    match (effect, value, &at) {
        (Effect::Deadline, _, Some(at)) => command(out, &[b"PEXPIREAT", key, at.as_bytes()]),
        (Effect::Deadline, _, None) => command(out, &[b"PERSIST", key]),
        (Effect::Value, Value::Str(s), Some(at)) => {
            command(out, &[b"SET", key, s, b"PXAT", at.as_bytes()]);
        }
        (Effect::Value, Value::Str(s), None) => command(out, &[b"SET", key, s]),
        (Effect::Value, value, _) => {
            let mut body = Vec::new();
            let fields = shard.repl_field_times(h, key);
            let version = crate::shard::dump::write_value(&mut body, value, fields);
            let payload = crate::rdb::seal_as(body, version);
            let at = at.as_deref().unwrap_or("0");
            command(
                out,
                &[
                    b"RESTORE",
                    key,
                    at.as_bytes(),
                    &payload,
                    b"REPLACE",
                    b"ABSTTL",
                ],
            );
        }
    }
}

/// SREM of the members SPOP replied with.
pub(crate) fn encode_spop(out: &mut Vec<u8>, key: &[u8], reply: &Reply) {
    let members: Vec<&[u8]> = match reply {
        Reply::Bulk(member) => vec![member],
        Reply::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                Reply::Bulk(member) => Some(member.as_slice()),
                _ => None,
            })
            .collect(),
        _ => return,
    };
    if !members.is_empty() {
        let mut args: Vec<&[u8]> = vec![b"SREM", key];
        args.extend(members);
        command(out, &args);
    }
}

/// XADD with the ID it replied with.
pub(crate) fn encode_xadd(
    out: &mut Vec<u8>,
    before: &[Bytes],
    fields: &[(Bytes, Bytes)],
    reply: &Reply,
) {
    let Reply::Bulk(id) = reply else {
        return;
    };
    let mut args: Vec<&[u8]> = before.iter().map(Vec::as_slice).collect();
    args.push(id);
    for (f, v) in fields {
        args.extend([f.as_slice(), v.as_slice()]);
    }
    command(out, &args);
}

/// The part of the stream a FUNCTION subcommand that succeeded adds.
pub(crate) fn function_writes(args: &[Bytes]) -> bool {
    args.get(1).is_some_and(|sub| {
        [&b"LOAD"[..], b"DELETE", b"FLUSH", b"RESTORE"]
            .iter()
            .any(|name| sub.eq_ignore_ascii_case(name))
    })
}

/// A fresh replication ID: 40 hexadecimal digits, as Redis makes them.
pub(crate) fn new_id() -> String {
    let mut bytes = [0u8; 20];
    // Unpredictable enough to tell two histories apart, without a crate.
    let seed = std::collections::hash_map::RandomState::new();
    for (i, chunk) in bytes.chunks_mut(8).enumerate() {
        use std::hash::{BuildHasher, Hasher};
        let mut h = seed.build_hasher();
        h.write_usize(i);
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        let word = h.finish().to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    let mut id = String::with_capacity(2 * bytes.len());
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(id, "{b:02x}");
    }
    id
}

/// The last bytes of the stream, and where the stream stands.
pub(crate) struct Backlog {
    /// A ring of `held` bytes that starts at `head`; empty until a replica
    /// first asks to sync.
    ring: Vec<u8>,
    head: usize,
    held: usize,
    /// How large the ring is once there is one (`repl-backlog-size`).
    size: usize,
    /// The offset just past the last byte (`master_repl_offset`).
    end: u64,
    /// The database the stream is in; `None` when the next command has to
    /// select one.
    db: Option<usize>,
    /// Transactions and scripts running now, and whether their MULTI went
    /// out.
    open: u32,
    multi: bool,
    replid: String,
    /// The ID of the stream this one continues, and the first offset that
    /// is not part of it: a replica of the old primary may go on from
    /// there (`second_repl_offset`).
    replid2: Option<(String, u64)>,
    /// Counts the full syncs from a primary (`SyncPoint::history`).
    history: u64,
}

impl Backlog {
    fn new() -> Backlog {
        Backlog {
            ring: Vec::new(),
            head: 0,
            held: 0,
            size: BACKLOG_SIZE,
            end: 0,
            db: None,
            open: 0,
            multi: false,
            replid: new_id(),
            replid2: None,
            history: 0,
        }
    }

    pub(crate) fn history(&self) -> u64 {
        self.history
    }

    pub(crate) fn active(&self) -> bool {
        !self.ring.is_empty()
    }

    /// The offset of the oldest byte held.
    pub(crate) fn start(&self) -> u64 {
        self.end - self.held as u64
    }

    fn activate(&mut self) {
        if self.ring.is_empty() {
            // A size the memory cannot hold falls back to the default
            // rather than end the process.
            self.ring = zeroed(self.size).unwrap_or_else(|| {
                eprintln!(
                    "hcache: no memory for a backlog of {} bytes, using {BACKLOG_SIZE}",
                    self.size
                );
                self.size = BACKLOG_SIZE;
                vec![0; BACKLOG_SIZE]
            });
            self.head = 0;
            self.held = 0;
        }
    }

    /// Keep the last bytes in a ring of `size` from now on. A ring the
    /// memory cannot hold leaves the one there is.
    pub(crate) fn resize(&mut self, size: usize) {
        let size = size.max(BACKLOG_MIN);
        if self.ring.is_empty() {
            self.size = size;
            return;
        }
        let Some(mut ring) = zeroed(size) else {
            eprintln!(
                "hcache: no memory for a backlog of {size} bytes, keeping {}",
                self.size
            );
            return;
        };
        self.size = size;
        let keep = self.held.min(self.size);
        let mut tail = Vec::with_capacity(keep);
        self.copy(self.end - keep as u64, &mut tail, keep);
        ring[..keep].copy_from_slice(&tail);
        self.ring = ring;
        self.head = 0;
        self.held = keep;
    }

    fn push(&mut self, bytes: &[u8]) {
        self.end += bytes.len() as u64;
        let cap = self.ring.len();
        if cap == 0 {
            return;
        }
        let bytes = &bytes[bytes.len().saturating_sub(cap)..];
        let tail = (self.head + self.held) % cap;
        let first = (cap - tail).min(bytes.len());
        self.ring[tail..tail + first].copy_from_slice(&bytes[..first]);
        self.ring[..bytes.len() - first].copy_from_slice(&bytes[first..]);
        let total = self.held + bytes.len();
        if total > cap {
            self.head = (self.head + total - cap) % cap;
            self.held = cap;
        } else {
            self.held = total;
        }
    }

    /// Append the bytes from offset `from` on, `max` at most, to `out`.
    /// The offset reached; `None` when `from` is no longer held.
    pub(crate) fn copy(&self, from: u64, out: &mut Vec<u8>, max: usize) -> Option<u64> {
        if from < self.start() || from > self.end {
            return None;
        }
        let n = usize::try_from(self.end - from).map_or(max, |n| n.min(max));
        if n == 0 {
            return Some(from);
        }
        let cap = self.ring.len();
        let at = (self.head + (from - self.start()) as usize) % cap;
        let first = (cap - at).min(n);
        out.extend_from_slice(&self.ring[at..at + first]);
        out.extend_from_slice(&self.ring[..n - first]);
        Some(from + n as u64)
    }

    /// One command's part of the stream, `body`, in database `db` (`None`
    /// when it names no database).
    fn append(&mut self, db: Option<usize>, body: &[u8]) {
        if self.open > 0 && !self.multi {
            self.multi = true;
            let mut multi = Vec::new();
            command(&mut multi, &[b"MULTI"]);
            self.push(&multi);
        }
        if let Some(db) = db.filter(|db| self.db != Some(*db)) {
            let mut select = Vec::new();
            command(&mut select, &[b"SELECT", db.to_string().as_bytes()]);
            self.push(&select);
            self.db = Some(db);
        }
        self.push(body);
    }

    fn close(&mut self) {
        self.open -= 1;
        if self.open == 0 && std::mem::take(&mut self.multi) {
            let mut exec = Vec::new();
            command(&mut exec, &[b"EXEC"]);
            self.push(&exec);
        }
    }
}

/// Where a replica goes on in a node's stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPoint {
    /// The ID of the stream, for +FULLRESYNC and +CONTINUE.
    pub replid: String,
    /// The offset to send from.
    pub offset: u64,
    /// Which of this node's histories the offset counts in: a full sync
    /// from its own primary starts another, and a replica of the old one
    /// must not read on in the new.
    pub history: u64,
}

/// What INFO and ROLE tell of the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplInfo {
    pub replid: String,
    pub replid2: String,
    /// `master_repl_offset`.
    pub offset: u64,
    /// `second_repl_offset`: -1 without a second ID.
    pub second_offset: i64,
    pub active: bool,
    pub size: usize,
    /// `repl_backlog_first_byte_offset`, which counts from 1.
    pub first_byte: u64,
    pub histlen: usize,
}

/// The replication state of a node.
pub(crate) struct Repl {
    /// Commands go to the backlog: this node is a primary that has one.
    logging: AtomicBool,
    /// A sender has sent everything and asks to hear of more.
    wake: AtomicBool,
    log: Mutex<Backlog>,
}

impl Default for Repl {
    fn default() -> Self {
        Repl {
            logging: AtomicBool::new(false),
            wake: AtomicBool::new(false),
            log: Mutex::new(Backlog::new()),
        }
    }
}

impl Repl {
    pub(crate) fn logging(&self) -> bool {
        self.logging.load(Ordering::SeqCst)
    }

    pub(crate) fn set_logging(&self, on: bool) {
        self.logging.store(on, Ordering::SeqCst);
    }

    pub(crate) fn log(&self) -> parking_lot::MutexGuard<'_, Backlog> {
        self.log.lock()
    }

    /// Append `body`; whether a sender asked to hear of it.
    pub(crate) fn append(&self, db: Option<usize>, body: &[u8]) -> bool {
        self.log.lock().append(db, body);
        self.woken()
    }

    /// Append bytes of another node's stream as they are (a replica keeps
    /// its primary's stream for replicas of its own and for later).
    pub(crate) fn feed(&self, bytes: &[u8], db: u16) -> bool {
        let mut log = self.log.lock();
        log.push(bytes);
        log.db = Some(db.into());
        drop(log);
        self.woken()
    }

    /// The database the stream is in: where a replica that starts from
    /// here applies what comes next.
    pub(crate) fn stream_db(&self) -> u16 {
        self.log
            .lock()
            .db
            .and_then(|db| u16::try_from(db).ok())
            .unwrap_or(0)
    }

    /// Where a full sync starts: the ID, the offset, and the database the
    /// stream is in. The stream of a primary selects its database again
    /// with the next command.
    pub(crate) fn sync_point(&self, primary: bool) -> (SyncPoint, usize) {
        let mut log = self.log.lock();
        let db = log.db.unwrap_or(0);
        if primary {
            log.db = None;
        }
        let point = SyncPoint {
            replid: log.replid.clone(),
            offset: log.end,
            history: log.history,
        };
        (point, db)
    }

    fn woken(&self) -> bool {
        self.wake.load(Ordering::Relaxed) && self.wake.swap(false, Ordering::SeqCst)
    }

    pub(crate) fn want_wake(&self) {
        self.wake.store(true, Ordering::SeqCst);
    }

    /// A transaction or script starts: what it writes goes between MULTI
    /// and EXEC.
    pub(crate) fn open(&self) {
        self.log.lock().open += 1;
    }

    pub(crate) fn close(&self) -> bool {
        self.log.lock().close();
        self.woken()
    }

    pub(crate) fn info(&self) -> ReplInfo {
        let log = self.log.lock();
        let (replid2, second_offset) = match &log.replid2 {
            Some((id, at)) => (id.clone(), i64::try_from(*at).unwrap_or(i64::MAX)),
            None => ("0".repeat(40), -1),
        };
        ReplInfo {
            replid: log.replid.clone(),
            replid2,
            offset: log.end,
            second_offset,
            active: log.active(),
            size: log.size,
            first_byte: if log.active() { log.start() + 1 } else { 0 },
            histlen: log.held,
        }
    }

    /// Start keeping the stream, if not yet: the first replica asks.
    pub(crate) fn activate(&self) {
        self.log.lock().activate();
    }

    /// Where a replica that asks to go on from `offset` (which counts from
    /// 1, as PSYNC sends it) of the stream `replid` would start, if it
    /// can.
    pub(crate) fn resume(&self, replid: &[u8], offset: i64) -> Option<SyncPoint> {
        let log = self.log.lock();
        if !log.active() {
            return None;
        }
        let from = u64::try_from(offset).ok()?.checked_sub(1)?;
        let ours = replid == log.replid.as_bytes();
        let before = log
            .replid2
            .as_ref()
            .is_some_and(|(id, until)| replid == id.as_bytes() && from <= *until);
        let held = from >= log.start() && from <= log.end;
        ((ours || before) && held).then(|| SyncPoint {
            replid: log.replid.clone(),
            offset: from,
            history: log.history,
        })
    }

    /// A full sync from a primary: its stream, from its offset, from now on.
    pub(crate) fn reset_to(&self, replid: &str, offset: u64, db: u16) {
        let mut log = self.log.lock();
        log.replid = replid.to_string();
        log.replid2 = None;
        log.end = offset;
        log.held = 0;
        log.head = 0;
        log.db = Some(db.into());
        log.history += 1;
        log.activate();
    }

    /// The primary goes on with another ID (it was promoted): ours so far
    /// is the old one.
    pub(crate) fn continue_as(&self, replid: &str) {
        let mut log = self.log.lock();
        if log.replid != replid {
            let old = std::mem::replace(&mut log.replid, replid.to_string());
            log.replid2 = Some((old, log.end));
        }
        log.activate();
    }

    /// This node becomes a primary: a new history starts here, and
    /// replicas of the old one may go on to it.
    pub(crate) fn shift_id(&self) {
        let mut log = self.log.lock();
        let old = std::mem::replace(&mut log.replid, new_id());
        log.replid2 = Some((old, log.end));
        log.db = None;
        log.open = 0;
        log.multi = false;
    }
}

/// What a primary may send in one command: as much as Redis takes
/// (`proto-max-bulk-len`), since a replica cannot refuse a write.
const STREAM_LIMITS: Limits = Limits {
    max_args: 1 << 30,
    max_bulk: 512 << 20,
    max_inline: 64 << 10,
    max_request: usize::MAX,
};

/// `len` zero bytes, or `None` when the memory cannot hold them.
fn zeroed(len: usize) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).ok()?;
    bytes.resize(len, 0);
    Some(bytes)
}

/// What one `Replay::apply` did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// The bytes at the start of the input it applied; the rest waits for
    /// more to arrive.
    pub used: usize,
    /// The primary asked where this replica is (`REPLCONF GETACK`).
    pub ack: bool,
    /// Commands this node does not know, passed over.
    pub unknown: u64,
}

/// A replica's reading of its primary's stream. What it has read of a
/// command or a transaction that is not whole yet stays read, so the bytes
/// that come next are read once: a large transaction costs its length, not
/// its length times the reads it took to arrive.
pub struct Replay {
    parser: Parser,
    /// Bytes at the start of the input read and not applied yet: the
    /// commands of an open transaction and part of the next command.
    read: usize,
    /// The database of the commands read so far.
    db: u16,
    /// The commands of a transaction whose EXEC has not come yet.
    queued: Option<Vec<Cmd>>,
    /// Seen since the last call that applied anything.
    ack: bool,
    unknown: u64,
}

impl Replay {
    /// Read the stream from where `engine`'s own stream is.
    pub fn new(engine: &Engine) -> Replay {
        Replay {
            parser: Parser::new(STREAM_LIMITS),
            read: 0,
            db: engine.repl_stream_db(),
            queued: None,
            ack: false,
            unknown: 0,
        }
    }

    /// Apply the whole commands at the start of `input`, the primary's
    /// stream from its first byte not applied yet: a transaction only once
    /// its EXEC is there, all in one hold of the exclusive side, which also
    /// keeps their bytes in this node's stream. The caller drops the bytes
    /// applied and hands the rest in again with what comes next. An error
    /// means the stream is broken: the link has to sync again.
    ///
    /// Only what changes data, and what is published, runs. The rest (SAVE,
    /// CONFIG, DEBUG, ACL...) is for the primary's own clients: from the
    /// stream it would run without the checks a client's command gets.
    pub fn apply(&mut self, engine: &Engine, input: &[u8]) -> Result<Applied, String> {
        let mut applied = Applied::default();
        let mut whole_db = self.db;
        // What runs, read before the gate is taken; a transaction joins it
        // at its EXEC.
        let mut run = Vec::new();
        loop {
            let (used, args) = self.parser.feed(&input[self.read..]).map_err(|e| e.0)?;
            self.read += used;
            let Some(args) = args else { break };
            let name = args[0].to_ascii_uppercase();
            match (name.as_slice(), &args[1..]) {
                (b"SELECT", [n]) => {
                    self.db = std::str::from_utf8(n)
                        .ok()
                        .and_then(|n| n.parse().ok())
                        .filter(|&n: &u16| usize::from(n) < engine.databases())
                        .ok_or("SELECT of a database this node does not have")?;
                }
                (b"PING", _) => {}
                (b"REPLCONF", [sub, ..]) => self.ack |= sub.eq_ignore_ascii_case(b"GETACK"),
                (b"MULTI", []) => self.queued = Some(Vec::new()),
                (b"EXEC", []) => run.extend(self.queued.take().unwrap_or_default()),
                _ => match Cmd::parse(args) {
                    Ok(cmd)
                        if cmd.class() != Class::Conn
                            || matches!(cmd, Cmd::Publish(..) | Cmd::SPublish(..)) =>
                    {
                        let cmd = Cmd::In(self.db, Box::new(cmd));
                        match &mut self.queued {
                            Some(queue) => queue.push(cmd),
                            None => run.push(cmd),
                        }
                    }
                    _ => self.unknown += 1,
                },
            }
            if self.queued.is_none() {
                applied = Applied {
                    used: self.read,
                    ack: std::mem::take(&mut self.ack) || applied.ack,
                    unknown: std::mem::take(&mut self.unknown) + applied.unknown,
                };
                whole_db = self.db;
            }
        }
        if applied.used > 0 {
            let mut excl = engine.exclusive();
            for cmd in run {
                excl.run(cmd);
            }
            engine.repl_feed(&input[..applied.used], whole_db);
            self.read -= applied.used;
        }
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Cmd {
        let args = line.split(' ').map(|a| a.as_bytes().to_vec()).collect();
        Cmd::parse(args).unwrap_or_else(|e| panic!("{line}: {e:?}"))
    }

    /// The arguments of one RESP command.
    fn args(resp: &[u8]) -> Vec<Vec<u8>> {
        let mut parser = crate::resp::Parser::new(crate::resp::Limits::default());
        let (used, args) = parser.feed(resp).unwrap();
        assert_eq!(used, resp.len());
        args.unwrap()
    }

    #[test]
    fn deterministic_commands_go_as_they_were_sent() {
        for line in [
            "SET k v",
            "SETNX k v",
            "APPEND k v",
            "PERSIST k",
            "INCRBY k -5",
            "HSET h a 1 b 2",
            "HSETNX h a 1",
            "HINCRBY h a 3",
            "HDEL h a b",
            "DEL a b",
            "UNLINK a",
            "MSET a 1 b 2",
            "LPUSH l a b",
            "RPUSHX l a",
            "LPOP l 2",
            "RPOP l",
            "LSET l -1 x",
            "LREM l -2 x",
            "LTRIM l 1 -1",
            "LINSERT l BEFORE p x",
            "SADD s a b",
            "SREM s a",
            "ZADD z NX CH 1.5 a -inf b 1e300 c",
            "ZADD z XX GT INCR 2 a",
            "ZREM z a",
            "ZINCRBY z 0.1 a",
            "ZPOPMIN z 3",
            "ZPOPMAX z",
            "SETBIT b 7 1",
            "SETRANGE s 3 abc",
            "PFADD p a b",
            "XDEL x 1-1 2-0",
            "XACK x g 1-1",
            "XGROUP DESTROY x g",
            "XGROUP CREATECONSUMER x g c",
            "XGROUP DELCONSUMER x g c",
            "XTRIM x MAXLEN = 10",
            "XTRIM x MINID = 5-0",
            "RENAME a b",
            "RENAMENX a b",
            "COPY a b DB 3 REPLACE",
            "COPY a b",
            "MSETNX a 1 b 2",
            "PFMERGE d a b",
            "LMOVE a b LEFT RIGHT",
            "SMOVE a b m",
            "SINTERSTORE d a b",
            "SUNIONSTORE d a",
            "SDIFFSTORE d a b",
            "LMPOP 2 a b RIGHT COUNT 3",
            "ZMPOP 1 z MIN COUNT 1",
            "BITOP XOR d a b",
            "BITOP NOT d a",
            "PFCOUNT h",
        ] {
            let cmd = parse(line);
            let Plan::Same(resp) = Plan::of(&cmd) else {
                panic!("{line}: not sent as it is");
            };
            assert_eq!(Cmd::parse(args(&resp)).unwrap(), cmd, "{line}");
        }
    }

    #[test]
    fn what_depends_on_the_clock_or_chance_goes_as_its_effect() {
        for (line, effect) in [
            ("SET k v EX 10", Effect::Value),
            ("SET k v NX", Effect::Value),
            ("SETEX k 10 v", Effect::Value),
            ("GETSET k v", Effect::Value),
            ("INCRBYFLOAT k 1.5", Effect::Value),
            ("RESTORE k 0 x", Effect::Value),
            ("ZREMRANGEBYSCORE z 0 1", Effect::Value),
            ("EXPIRE k 10", Effect::Deadline),
            ("PEXPIREAT k 10", Effect::Deadline),
            ("GETEX k EX 10", Effect::Deadline),
        ] {
            match Plan::of(&parse(line)) {
                Plan::State(got) => assert_eq!(got, effect, "{line}"),
                _ => panic!("{line}: not sent as its effect"),
            }
        }
        assert!(matches!(Plan::of(&parse("SPOP s 2")), Plan::SPop(k) if k == b"s"));
        assert!(matches!(Plan::of(&parse("XADD x * a 1")), Plan::XAdd(..)));
        assert!(matches!(
            Plan::of(&parse("XADD x MAXLEN ~ 10 * a 1")),
            Plan::State(Effect::Value)
        ));
        for read in [
            "GET k",
            "HGETALL h",
            "LRANGE l 0 -1",
            "SINTER a b",
            "XLEN x",
        ] {
            assert!(matches!(Plan::of(&parse(read)), Plan::Quiet), "{read}");
        }
    }

    #[test]
    fn xadd_goes_with_the_id_it_chose() {
        let Plan::XAdd(before, fields) = Plan::of(&parse("XADD x NOMKSTREAM MAXLEN = 5 * a 1"))
        else {
            panic!("not XADD");
        };
        let mut out = Vec::new();
        encode_xadd(&mut out, &before, &fields, &Reply::Bulk(b"7-3".to_vec()));
        assert_eq!(
            Cmd::parse(args(&out)).unwrap(),
            parse("XADD x NOMKSTREAM MAXLEN = 5 7-3 a 1")
        );
    }

    #[test]
    fn the_ring_keeps_the_last_bytes_and_says_what_it_lost() {
        let mut log = Backlog::new();
        log.size = 8;
        log.push(b"lost");
        assert!(!log.active());
        assert_eq!(log.end, 4);
        log.activate();
        log.push(b"abcde");
        log.push(b"fghij");
        assert_eq!((log.start(), log.end), (6, 14));
        let mut out = Vec::new();
        assert_eq!(log.copy(5, &mut out, 100), None);
        assert_eq!(log.copy(7, &mut out, 3), Some(10));
        assert_eq!(out, b"def");
        out.clear();
        assert_eq!(log.copy(10, &mut out, 100), Some(14));
        assert_eq!(out, b"ghij");
        assert_eq!(log.copy(14, &mut out, 100), Some(14));
        assert_eq!(log.copy(15, &mut out, 100), None);
        log.push(b"0123456789");
        out.clear();
        assert_eq!(log.copy(log.start(), &mut out, 100), Some(24));
        assert_eq!(out, b"23456789");
        log.resize(BACKLOG_MIN);
        out.clear();
        assert_eq!(log.copy(16, &mut out, 100), Some(24));
        assert_eq!(out, b"23456789");
    }

    #[test]
    fn a_transaction_goes_between_multi_and_exec_once_it_writes() {
        let mut log = Backlog::new();
        log.activate();
        log.open += 1;
        log.close();
        assert_eq!(log.end, 0);
        log.open += 1;
        log.open += 1;
        let mut set = Vec::new();
        command(&mut set, &[b"SET", b"k", b"v"]);
        log.append(Some(2), &set);
        log.close();
        log.append(Some(2), &set);
        log.close();
        let mut out = Vec::new();
        log.copy(0, &mut out, usize::MAX);
        let text = String::from_utf8(out).unwrap();
        assert_eq!(
            text,
            "*1\r\n$5\r\nMULTI\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n\
             *3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n\
             *1\r\n$4\r\nEXEC\r\n"
        );
    }

    #[test]
    fn a_replica_resumes_only_within_what_is_held_of_its_history() {
        let repl = Repl::default();
        let id = repl.info().replid;
        assert_eq!(id.len(), 40);
        assert_eq!(repl.resume(id.as_bytes(), 1), None);
        repl.activate();
        repl.append(None, b"0123456789");
        let at = |replid: &str, offset| {
            Some(SyncPoint {
                replid: replid.to_string(),
                offset,
                history: 0,
            })
        };
        assert_eq!(repl.resume(id.as_bytes(), 1), at(&id, 0));
        assert_eq!(repl.resume(id.as_bytes(), 11), at(&id, 10));
        assert_eq!(repl.resume(id.as_bytes(), 12), None);
        assert_eq!(repl.resume(b"other", 1), None);
        repl.shift_id();
        let info = repl.info();
        assert_ne!(info.replid, id);
        assert_eq!(
            (info.replid2.as_str(), info.second_offset),
            (id.as_str(), 10)
        );
        repl.append(None, b"ab");
        assert_eq!(repl.resume(id.as_bytes(), 11), at(&info.replid, 10));
        assert_eq!(repl.resume(id.as_bytes(), 12), None);
        assert_eq!(
            repl.resume(info.replid.as_bytes(), 13),
            at(&info.replid, 12)
        );
        // A full sync from a primary starts another history.
        repl.reset_to(&id, 5, 0);
        assert_eq!(repl.resume(id.as_bytes(), 6).map(|p| p.history), Some(1));
    }

    fn run(engine: &Engine, slot: usize, db: usize, args: &[&str]) -> Reply {
        let args = args.iter().map(|a| a.as_bytes().to_vec()).collect();
        let cmd = Cmd::parse(args).unwrap_or_else(|e| panic!("{e:?}"));
        engine.with_db(db, || engine.execute_blocking(slot, cmd))
    }

    /// Every key of database `db`, with its DUMP payload and deadline.
    fn contents(engine: &Engine, slot: usize, db: usize) -> Vec<(Vec<u8>, Reply, Reply)> {
        let Reply::Array(keys) = run(engine, slot, db, &["KEYS", "*"]) else {
            panic!("KEYS")
        };
        let mut all: Vec<_> = keys
            .into_iter()
            .map(|key| {
                let Reply::Bulk(key) = key else {
                    panic!("{key:?}")
                };
                let name = String::from_utf8(key.clone()).unwrap();
                let dump = run(engine, slot, db, &["DUMP", &name]);
                let at = run(engine, slot, db, &["PEXPIRETIME", &name]);
                (key, dump, at)
            })
            .collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all
    }

    #[test]
    fn a_replica_that_applies_the_stream_holds_what_the_primary_holds() {
        use crate::engine::Config;
        let cfg = Config {
            databases: 4,
            ..Config::default()
        };
        let primary = Engine::new(cfg.clone());
        let p = primary.gate().register().unwrap();
        // What is there before the first replica comes in the file.
        run(&primary, p, 0, &["SET", "before", "1"]);
        run(&primary, p, 0, &["HSET", "h", "a", "1", "b", "2", "c", "3"]);
        let (rdb, point) = primary.repl_snapshot();
        let (replid, offset) = (point.replid.clone(), point.offset);
        let replica = Engine::new(cfg);
        replica.become_replica();
        replica.repl_load(&rdb[..], &replid, offset).unwrap();

        let lib = "#!lua name=lib\nredis.register_function('set_pop', function(keys, args) \
                   redis.call('SET', keys[1], args[1]); return redis.call('SPOP', keys[2]) end)";
        let writes: &[&[&str]] = &[
            &["SET", "s1", "v", "EX", "100"],
            &["SETEX", "s2", "100", "v"],
            &["GETEX", "s1", "PX", "5000"],
            &["INCRBYFLOAT", "f", "1.5"],
            &["APPEND", "a", "xyz"],
            &["SETRANGE", "a", "1", "Q"],
            &["SETBIT", "b", "7", "1"],
            &["INCR", "c"],
            &["DECRBY", "c", "3"],
            &["RPUSH", "l", "a", "b", "c", "d"],
            &["LPOP", "l"],
            &["LINSERT", "l", "BEFORE", "c", "z"],
            &["LSET", "l", "0", "q"],
            &["LREM", "l", "1", "z"],
            &["SADD", "s", "a", "b", "c", "d", "e", "f", "g", "h"],
            &["SPOP", "s", "2"],
            &["SREM", "s", "a"],
            &["SADD", "t", "x", "b"],
            &["SMOVE", "s", "t", "c"],
            &["SINTERSTORE", "si", "s", "t"],
            &["ZADD", "z", "1", "a", "2", "b", "3", "c"],
            &["ZINCRBY", "z", "2.5", "a"],
            &["ZPOPMIN", "z"],
            &["ZADD", "z", "GT", "CH", "10", "b"],
            &["HSET", "h", "d", "4"],
            &["HINCRBY", "h", "a", "5"],
            &["HINCRBYFLOAT", "h", "b", "0.5"],
            &["HDEL", "h", "c"],
            &["HSETNX", "h", "z", "1"],
            &["HEXPIRE", "h", "100", "FIELDS", "1", "a"],
            &["XADD", "x", "*", "f", "v"],
            &["XADD", "x", "MAXLEN", "=", "5", "*", "f2", "v2"],
            &["XGROUP", "CREATE", "x", "g", "0"],
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "COUNT",
                "1",
                "STREAMS",
                "x",
                ">",
            ],
            &["PFADD", "hl", "a", "b", "c"],
            &["PFMERGE", "hl2", "hl"],
            &["RENAME", "a", "a2"],
            &["COPY", "a2", "a3"],
            &["GETDEL", "c"],
            &["EXPIRE", "l", "100"],
            &["PERSIST", "l"],
            &["PEXPIREAT", "z", "99999999999999"],
            &["LMOVE", "l", "l2", "LEFT", "RIGHT"],
            &["BITOP", "OR", "bo", "b", "b"],
            &["DEL", "before"],
            &["MSET", "m1", "1", "m2", "2"],
            &["MSETNX", "m3", "3"],
            &["UNLINK", "m2"],
            &["EXPIRE", "m1", "-1"],
            &["SET", "short", "v", "PX", "5"],
            &["SET", "mv", "1"],
            &[
                "EVAL",
                "redis.call('SET', KEYS[1], 'x'); return redis.call('SPOP', KEYS[2])",
                "2",
                "e",
                "s",
            ],
            &["FUNCTION", "LOAD", lib],
            &["FCALL", "set_pop", "2", "fk", "s", "v"],
        ];
        for args in writes {
            let reply = run(&primary, p, 0, args);
            assert!(!reply.is_error(), "{args:?}: {reply:?}");
        }
        run(&primary, p, 1, &["SET", "k1", "v"]);
        run(&primary, p, 1, &["LPUSH", "d1", "a"]);
        run(&primary, p, 2, &["SET", "k2", "v"]);
        run(&primary, p, 3, &["SET", "flushed", "v"]);
        assert_eq!(run(&primary, p, 3, &["FLUSHDB"]), Reply::Ok);
        assert_eq!(run(&primary, p, 0, &["SWAPDB", "1", "2"]), Reply::Ok);
        assert_eq!(run(&primary, p, 0, &["MOVE", "mv", "3"]), Reply::Int(1));
        let queue = ["INCR t2", "SPOP s", "LPUSH l3 x"]
            .into_iter()
            .map(parse)
            .collect();
        let reply = primary.execute_blocking(
            p,
            Cmd::ExecBlock {
                queue,
                watched: Vec::new(),
            },
        );
        assert!(matches!(reply, Reply::Array(_)), "{reply:?}");
        // A key whose time came goes as DEL.
        primary.advance_clock(10);
        assert_eq!(primary.sweep_expired(), 1);

        let mut stream = Vec::new();
        let end = primary.repl_info().offset;
        assert_eq!(
            primary.repl_copy(&point, &mut stream, usize::MAX),
            Some(end)
        );
        let applied = Replay::new(&replica).apply(&replica, &stream).unwrap();
        let all = Applied {
            used: stream.len(),
            ack: false,
            unknown: 0,
        };
        assert_eq!(applied, all);
        assert_eq!(replica.repl_info().offset, end);
        assert_eq!(replica.repl_info().replid, replid);
        let r = replica.gate().register().unwrap();
        for db in 0..4 {
            assert_eq!(
                contents(&replica, r, db),
                contents(&primary, p, db),
                "db {db}"
            );
        }
        assert_eq!(
            run(&replica, r, 0, &["FUNCTION", "LIST"]),
            run(&primary, p, 0, &["FUNCTION", "LIST"])
        );
        assert_eq!(run(&replica, r, 0, &["EXISTS", "short"]), Reply::Int(0));
    }

    #[test]
    fn a_replica_applies_a_transaction_only_once_it_is_whole() {
        let replica = Engine::new(crate::engine::Config::default());
        replica.become_replica();
        let r = replica.gate().register().unwrap();
        let mut stream = Vec::new();
        command(&mut stream, &[b"SET", b"a", b"1"]);
        let set = stream.len();
        command(&mut stream, &[b"MULTI"]);
        command(&mut stream, &[b"INCR", b"a"]);
        let open = stream.len();
        command(&mut stream, &[b"EXEC"]);
        command(&mut stream, &[b"REPLCONF", b"GETACK", b"*"]);
        command(&mut stream, &[b"NOSUCHCOMMAND"]);
        let whole = stream.len();
        command(&mut stream, &[b"SET", b"b", b"1"]);
        let mut replay = Replay::new(&replica);
        let applied = replay.apply(&replica, &stream[..open]).unwrap();
        assert_eq!((applied.used, applied.ack), (set, false));
        assert_eq!(run(&replica, r, 0, &["GET", "a"]), Reply::bulk("1"));
        let rest = &stream[set..stream.len() - 2];
        let applied = replay.apply(&replica, rest).unwrap();
        let expected = Applied {
            used: whole - set,
            ack: true,
            unknown: 1,
        };
        assert_eq!(applied, expected);
        assert_eq!(run(&replica, r, 0, &["GET", "a"]), Reply::bulk("2"));
        assert_eq!(run(&replica, r, 0, &["EXISTS", "b"]), Reply::Int(0));
        assert_eq!(replica.repl_info().offset, whole as u64);
        assert!(Replay::new(&replica)
            .apply(&replica, b"*1\r\n$x\r\n")
            .is_err());
    }

    #[test]
    fn a_replica_reads_a_transaction_that_comes_in_pieces_once() {
        let replica = Engine::new(crate::engine::Config::default());
        replica.become_replica();
        let r = replica.gate().register().unwrap();
        let mut stream = Vec::new();
        command(&mut stream, &[b"MULTI"]);
        for i in 0..100 {
            command(&mut stream, &[b"RPUSH", b"l", i.to_string().as_bytes()]);
        }
        command(&mut stream, &[b"EXEC"]);
        let mut replay = Replay::new(&replica);
        let mut buf = Vec::new();
        for byte in &stream {
            buf.push(*byte);
            let applied = replay.apply(&replica, &buf).unwrap();
            buf.drain(..applied.used);
        }
        assert!(buf.is_empty());
        assert_eq!(run(&replica, r, 0, &["LLEN", "l"]), Reply::Int(100));
        assert_eq!(replica.repl_info().offset, stream.len() as u64);
    }

    #[test]
    fn a_replica_runs_only_what_changes_data_from_its_primary() {
        let replica = Engine::new(crate::engine::Config::default());
        replica.become_replica();
        let r = replica.gate().register().unwrap();
        let mut stream = Vec::new();
        command(&mut stream, &[b"CONFIG", b"SET", b"maxmemory", b"1"]);
        command(&mut stream, &[b"SAVE"]);
        command(&mut stream, &[b"DEBUG", b"SLEEP", b"100"]);
        command(
            &mut stream,
            &[b"ACL", b"SETUSER", b"evil", b"on", b"nopass", b"+@all"],
        );
        command(&mut stream, &[b"SET", b"k", b"v"]);
        let applied = Replay::new(&replica).apply(&replica, &stream).unwrap();
        assert_eq!((applied.used, applied.unknown), (stream.len(), 4));
        assert_eq!(run(&replica, r, 0, &["GET", "k"]), Reply::bulk("v"));
        assert_eq!(
            run(&replica, r, 0, &["CONFIG", "GET", "maxmemory"]),
            Reply::Array(vec![Reply::bulk("maxmemory"), Reply::bulk("0")])
        );
    }

    #[test]
    fn a_backlog_the_memory_cannot_hold_leaves_the_one_there_is() {
        let mut log = Backlog::new();
        log.resize(BACKLOG_MIN);
        log.activate();
        log.push(b"abc");
        log.resize(usize::MAX);
        assert_eq!(log.size, BACKLOG_MIN);
        let mut out = Vec::new();
        log.copy(0, &mut out, 3);
        assert_eq!(out, b"abc");
    }

    #[test]
    fn a_replica_keeps_keys_whose_time_came_until_the_primary_deletes_them() {
        let replica = Engine::new(crate::engine::Config::default());
        replica.become_replica();
        let r = replica.gate().register().unwrap();
        let mut stream = Vec::new();
        command(&mut stream, &[b"SET", b"k", b"v", b"PX", b"5"]);
        Replay::new(&replica).apply(&replica, &stream).unwrap();
        replica.advance_clock(10);
        replica.expire_tick();
        // Hidden from reads, but still there for the primary's next write.
        assert_eq!(run(&replica, r, 0, &["GET", "k"]), Reply::Nil);
        assert_eq!(run(&replica, r, 0, &["DBSIZE"]), Reply::Int(1));
        replica.become_primary();
        assert_eq!(replica.sweep_expired(), 1);
        assert_eq!(run(&replica, r, 0, &["DBSIZE"]), Reply::Int(0));
    }
}
