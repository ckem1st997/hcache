//! Keyspace notifications (`notify-keyspace-events`): which events a
//! command gives rise to. The engine publishes them on
//! `__keyspace@<db>__:<key>` and `__keyevent@<db>__:<event>`.
//!
//! The events of a command are read off the command and its reply, not
//! recorded where the data changes, so that the keyspace code pays nothing
//! while notifications are off. The price is that a few borderline cases
//! differ from Redis; the README lists them.

use crate::cmd::{Bytes, Cmd, Expire};
use crate::ops::{KeyOp, MultiOp, SetOp, ZBy};
use crate::resp::Reply;
use crate::shard::Note;
use crate::stream::XOp;
use xxhash_rust::xxh3::xxh3_64;

pub(crate) const KEYSPACE: u32 = 1;
pub(crate) const KEYEVENT: u32 = 1 << 1;
pub(crate) const GENERIC: u32 = 1 << 2;
pub(crate) const STRING: u32 = 1 << 3;
pub(crate) const LIST: u32 = 1 << 4;
pub(crate) const SET: u32 = 1 << 5;
pub(crate) const HASH: u32 = 1 << 6;
pub(crate) const ZSET: u32 = 1 << 7;
pub(crate) const EXPIRED: u32 = 1 << 8;
pub(crate) const EVICTED: u32 = 1 << 9;
pub(crate) const STREAM: u32 = 1 << 10;
/// What `A` stands for.
const ALL: u32 = GENERIC | STRING | LIST | SET | HASH | ZSET | EXPIRED | EVICTED | STREAM;

const LETTERS: [(u8, u32); 9] = [
    (b'g', GENERIC),
    (b'$', STRING),
    (b'l', LIST),
    (b's', SET),
    (b'h', HASH),
    (b'z', ZSET),
    (b'x', EXPIRED),
    (b'e', EVICTED),
    (b't', STREAM),
];

/// The flags a `notify-keyspace-events` value stands for; `None` when it
/// has a letter that means nothing here.
pub(crate) fn parse(raw: &[u8]) -> Option<u32> {
    let mut flags = 0;
    for &c in raw {
        flags |= match c {
            b'K' => KEYSPACE,
            b'E' => KEYEVENT,
            b'A' => ALL,
            // Module events: there are no modules.
            b'd' => 0,
            c => LETTERS.iter().find(|(letter, _)| *letter == c)?.1,
        };
    }
    Some(flags)
}

/// The value CONFIG GET shows for `flags`.
pub(crate) fn show(flags: u32) -> String {
    let mut out = String::new();
    if flags & ALL == ALL {
        out.push('A');
    } else {
        for (letter, class) in LETTERS {
            if flags & class != 0 {
                out.push(letter as char);
            }
        }
    }
    if flags & KEYSPACE != 0 {
        out.push('K');
    }
    if flags & KEYEVENT != 0 {
        out.push('E');
    }
    out
}

/// Whether anything would be published with these flags.
pub(crate) fn active(flags: u32) -> bool {
    flags & (KEYSPACE | KEYEVENT) != 0 && flags & ALL != 0
}

/// The class and name of the event behind a shard's note.
pub(crate) fn of_note(note: Note) -> (u32, &'static str) {
    match note {
        Note::Expired => (EXPIRED, "expired"),
        Note::Evicted => (EVICTED, "evicted"),
        Note::HExpired => (HASH, "hexpired"),
        Note::Del => (GENERIC, "del"),
        Note::HSet => (HASH, "hset"),
        Note::HDel => (HASH, "hdel"),
        Note::HExpire => (HASH, "hexpire"),
        Note::HPersist => (HASH, "hpersist"),
        Note::BloomAdd => (GENERIC, "bloom.add"),
        Note::BloomReserve => (GENERIC, "bloom.reserve"),
        Note::Json(event) => (GENERIC, event),
    }
}

/// When a planned event really happened, judged by the command's reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rule {
    /// The command did not fail.
    Done,
    /// It did not fail and did not answer nil.
    NotNil,
    /// It answered something other than nil, zero, a negative number or
    /// an empty array.
    Truthy,
    /// The reply is an array that has this number in it.
    Has(i64),
    /// The key existed before the command ran.
    Existed,
    /// The reply is a string with this hash: SET IFEQ with GET set the
    /// key only when it held the value compared with.
    Is(u64),
}

#[derive(Debug)]
pub(crate) struct Planned {
    pub class: u32,
    pub event: &'static str,
    /// `None`: the key is the first item of the reply (LMPOP and the like).
    pub key: Option<Bytes>,
    pub rule: Rule,
    /// The command can take the key's last element: a `del` follows when
    /// the key is gone afterwards.
    pub may_empty: bool,
    /// The `del` then stands in for the event itself (EXPIRE in the past).
    pub del_instead: bool,
    /// For `Rule::Existed`; the engine fills it in.
    pub existed: bool,
}

impl Planned {
    pub(crate) fn fires(&self, reply: &Reply) -> bool {
        if matches!(reply, Reply::Error(_)) {
            return false;
        }
        match self.rule {
            Rule::Done => true,
            Rule::NotNil => !matches!(reply, Reply::Nil | Reply::NilArray),
            Rule::Truthy => match reply {
                Reply::Int(n) => *n > 0,
                Reply::Nil | Reply::NilArray => false,
                Reply::Array(items) | Reply::Map(items) => !items.is_empty(),
                _ => true,
            },
            Rule::Has(n) => {
                matches!(reply, Reply::Array(items) if items.contains(&Reply::Int(n)))
            }
            Rule::Existed => self.existed,
            Rule::Is(hash) => matches!(reply, Reply::Bulk(v) if xxh3_64(v) == hash),
        }
    }
}

struct Plan(Vec<Planned>);

impl Plan {
    fn add(&mut self, class: u32, event: &'static str, key: &[u8], rule: Rule) -> &mut Planned {
        self.0.push(Planned {
            class,
            event,
            key: Some(key.to_vec()),
            rule,
            may_empty: false,
            del_instead: false,
            existed: false,
        });
        let last = self.0.len() - 1;
        &mut self.0[last]
    }

    /// An event of a command that takes elements away.
    fn taking(&mut self, class: u32, event: &'static str, key: &[u8]) {
        self.add(class, event, key, Rule::Truthy).may_empty = true;
    }
}

fn side(left: bool) -> (&'static str, &'static str) {
    if left {
        ("lpop", "lpush")
    } else {
        ("rpop", "rpush")
    }
}

/// The events `cmd` gives rise to when it does what it says.
pub(crate) fn plan(cmd: &Cmd) -> Vec<Planned> {
    let mut events = Plan(Vec::new());
    let p = &mut events;
    match cmd {
        Cmd::Set(k, _) => {
            p.add(STRING, "set", k, Rule::Done);
        }
        Cmd::SetOpts(k, _, opts) => {
            // With GET the reply is the old value, nil included.
            let rule = match (opts.get, opts.cond, &opts.ifeq) {
                (true, _, Some(want)) => Rule::Is(xxh3_64(want)),
                (true, None, None) => Rule::Done,
                _ => Rule::NotNil,
            };
            p.add(STRING, "set", k, rule);
            if matches!(opts.expire, Expire::In(_) | Expire::At(_)) {
                p.add(GENERIC, "expire", k, rule);
            }
        }
        Cmd::SetNx(k, _) => {
            p.add(STRING, "set", k, Rule::Truthy);
        }
        Cmd::MSet(pairs) => {
            for (k, _) in pairs {
                p.add(STRING, "set", k, Rule::Done);
            }
        }
        Cmd::GetDel(k) => {
            p.add(GENERIC, "del", k, Rule::NotNil);
        }
        Cmd::GetEx(k, Expire::Clear) => {
            p.add(GENERIC, "persist", k, Rule::NotNil);
        }
        Cmd::GetEx(k, Expire::In(_) | Expire::At(_)) => {
            let planned = p.add(GENERIC, "expire", k, Rule::NotNil);
            planned.may_empty = true;
            planned.del_instead = true;
        }
        Cmd::Append(k, _) => {
            p.add(STRING, "append", k, Rule::Done);
        }
        Cmd::IncrBy(k, _) => {
            p.add(STRING, "incrby", k, Rule::Done);
        }
        Cmd::Expire { key, .. } => {
            let planned = p.add(GENERIC, "expire", key, Rule::Truthy);
            planned.may_empty = true;
            planned.del_instead = true;
        }
        Cmd::Persist(k) => {
            p.add(GENERIC, "persist", k, Rule::Truthy);
        }
        Cmd::Del(keys) | Cmd::Unlink(keys) => {
            for k in keys {
                p.add(GENERIC, "del", k, Rule::Existed);
            }
        }
        Cmd::HSet(k, _) | Cmd::HMSet(k, _) => {
            p.add(HASH, "hset", k, Rule::Done);
        }
        Cmd::HSetNx(k, ..) => {
            p.add(HASH, "hset", k, Rule::Truthy);
        }
        Cmd::HIncrBy(k, ..) => {
            p.add(HASH, "hincrby", k, Rule::Done);
        }
        Cmd::HDel(k, _) => p.taking(HASH, "hdel", k),
        Cmd::Key(k, op) => plan_key(p, k, op),
        Cmd::Multi(op) | Cmd::Block(op, _) => plan_multi(p, op),
        Cmd::In(_, inner) => return plan(inner),
        _ => {}
    }
    events.0
}

fn plan_key(p: &mut Plan, k: &[u8], op: &KeyOp) {
    match op {
        KeyOp::IncrByFloat(_) => {
            p.add(STRING, "incrbyfloat", k, Rule::Done);
        }
        KeyOp::SetRange(..) => {
            p.add(STRING, "setrange", k, Rule::Truthy);
        }
        KeyOp::SetBit(..) => {
            p.add(STRING, "setbit", k, Rule::Done);
        }
        KeyOp::BitField(ops) if ops.iter().any(crate::bitfield::Op::writes) => {
            p.add(STRING, "setbit", k, Rule::Done);
        }
        KeyOp::PfAdd(_) => {
            p.add(STRING, "pfadd", k, Rule::Truthy);
        }
        KeyOp::Restore { .. } => {
            p.add(GENERIC, "restore", k, Rule::Done);
        }
        KeyOp::HIncrByFloat(..) => {
            p.add(HASH, "hincrbyfloat", k, Rule::Done);
        }
        KeyOp::HExpire { .. } => {
            p.add(HASH, "hexpire", k, Rule::Has(1));
            p.add(HASH, "hdel", k, Rule::Has(2)).may_empty = true;
        }
        KeyOp::HPersist(_) => {
            p.add(HASH, "hpersist", k, Rule::Has(1));
        }
        // HGETDEL, HGETEX and HSETEX leave notes: what they did depends on
        // the time and the fields more than the reply tells.
        KeyOp::DelIfEq(_) => {
            p.add(GENERIC, "del", k, Rule::Truthy);
        }
        KeyOp::Push { left, .. } => {
            p.add(LIST, side(*left).1, k, Rule::Truthy);
        }
        KeyOp::Pop { left, .. } => p.taking(LIST, side(*left).0, k),
        KeyOp::LSet(..) => {
            p.add(LIST, "lset", k, Rule::Done);
        }
        KeyOp::LRem(..) => p.taking(LIST, "lrem", k),
        KeyOp::LTrim(..) => {
            p.add(LIST, "ltrim", k, Rule::Existed).may_empty = true;
        }
        KeyOp::LInsert { .. } => {
            p.add(LIST, "linsert", k, Rule::Truthy);
        }
        KeyOp::SAdd(_) => {
            p.add(SET, "sadd", k, Rule::Truthy);
        }
        KeyOp::SRem(_) => p.taking(SET, "srem", k),
        KeyOp::SPop(_) => p.taking(SET, "spop", k),
        KeyOp::ZAdd { flags, .. } => {
            let event = if flags.incr { "zincr" } else { "zadd" };
            p.add(ZSET, event, k, Rule::NotNil);
        }
        KeyOp::ZIncrBy(..) => {
            p.add(ZSET, "zincr", k, Rule::Done);
        }
        KeyOp::ZRem(_) => p.taking(ZSET, "zrem", k),
        KeyOp::ZRemRange(by) => {
            let event = match by {
                ZBy::Rank(..) => "zremrangebyrank",
                ZBy::Score(..) => "zremrangebyscore",
                ZBy::Lex(..) => "zremrangebylex",
            };
            p.taking(ZSET, event, k);
        }
        KeyOp::ZPop { max, .. } => {
            p.taking(ZSET, if *max { "zpopmax" } else { "zpopmin" }, k);
        }
        KeyOp::X(op) => {
            let (event, rule) = match **op {
                XOp::Add { .. } => ("xadd", Rule::NotNil),
                XOp::Del { .. } => ("xdel", Rule::Truthy),
                XOp::Trim { .. } => ("xtrim", Rule::Truthy),
                XOp::SetId { .. } => ("xsetid", Rule::Done),
                XOp::GroupCreate { .. } => ("xgroup-create", Rule::Done),
                XOp::GroupDestroy { .. } => ("xgroup-destroy", Rule::Truthy),
                XOp::GroupCreateConsumer { .. } => ("xgroup-createconsumer", Rule::Truthy),
                XOp::GroupDelConsumer { .. } => ("xgroup-delconsumer", Rule::Done),
                XOp::GroupSetId { .. } => ("xgroup-setid", Rule::Done),
                XOp::Claim { .. } => ("xclaim", Rule::Truthy),
                _ => return,
            };
            p.add(STREAM, event, k, rule);
        }
        _ => {}
    }
}

fn plan_multi(p: &mut Plan, op: &MultiOp) {
    match op {
        MultiOp::Rename { src, dst, .. } => {
            p.add(GENERIC, "rename_from", src, Rule::Truthy);
            p.add(GENERIC, "rename_to", dst, Rule::Truthy);
        }
        // With `DB` the copy lands in another database; that one is not
        // reported.
        MultiOp::Copy { dst, db: None, .. } => {
            p.add(GENERIC, "copy_to", dst, Rule::Truthy);
        }
        MultiOp::MSetNx(pairs) => {
            for (k, _) in pairs {
                p.add(STRING, "set", k, Rule::Truthy);
            }
        }
        MultiOp::MSetEx { pairs, expire, .. } => {
            for (k, _) in pairs {
                p.add(STRING, "set", k, Rule::Truthy);
                if matches!(expire, Expire::In(_) | Expire::At(_)) {
                    p.add(GENERIC, "expire", k, Rule::Truthy);
                }
            }
        }
        MultiOp::LMove {
            src,
            dst,
            from_left,
            to_left,
        } => {
            p.taking(LIST, side(*from_left).0, src);
            p.add(LIST, side(*to_left).1, dst, Rule::Truthy);
        }
        MultiOp::SMove { src, dst, .. } => {
            p.taking(SET, "srem", src);
            p.add(SET, "sadd", dst, Rule::Truthy);
        }
        MultiOp::Sets {
            op,
            store: Some(dst),
            ..
        } => {
            let event = match op {
                SetOp::Inter => "sinterstore",
                SetOp::Union => "sunionstore",
                SetOp::Diff => "sdiffstore",
            };
            p.add(SET, event, dst, Rule::Truthy);
        }
        MultiOp::ZStore {
            op, dst: Some(dst), ..
        } => {
            let event = match op {
                SetOp::Inter => "zinterstore",
                SetOp::Union => "zunionstore",
                SetOp::Diff => "zdiffstore",
            };
            p.add(ZSET, event, dst, Rule::Truthy);
        }
        MultiOp::ZRangeStore { dst, .. } => {
            p.add(ZSET, "zrangestore", dst, Rule::Truthy);
        }
        MultiOp::GeoStore { dst, .. } => {
            p.add(ZSET, "geosearchstore", dst, Rule::Truthy);
        }
        MultiOp::Sort { spec, .. } => {
            if let Some(dst) = &spec.store {
                p.add(LIST, "sortstore", dst, Rule::Truthy);
            }
        }
        MultiOp::MPop { zset, first, .. } => {
            let (class, event) = match (zset, first) {
                (true, true) => (ZSET, "zpopmin"),
                (true, false) => (ZSET, "zpopmax"),
                (false, true) => (LIST, "lpop"),
                (false, false) => (LIST, "rpop"),
            };
            p.0.push(Planned {
                class,
                event,
                key: None,
                rule: Rule::Truthy,
                may_empty: true,
                del_instead: false,
                existed: false,
            });
        }
        MultiOp::BitOp { dst, .. } => {
            p.add(STRING, "set", dst, Rule::Truthy);
        }
        MultiOp::PfMerge { dst, .. } => {
            p.add(STRING, "pfadd", dst, Rule::Done);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_round_trip() {
        assert_eq!(parse(b""), Some(0));
        assert_eq!(show(parse(b"KEA").unwrap()), "AKE");
        assert_eq!(show(parse(b"g$lshzxetKE").unwrap()), "AKE");
        assert_eq!(show(parse(b"Kx").unwrap()), "xK");
        assert_eq!(show(parse(b"Elh").unwrap()), "lhE");
        assert_eq!(parse(b"Kq"), None);
        assert!(!active(parse(b"A").unwrap()));
        assert!(!active(parse(b"KE").unwrap()));
        assert!(active(parse(b"Ex").unwrap()));
    }

    #[test]
    fn rules_read_the_reply() {
        let planned = |rule| Planned {
            class: GENERIC,
            event: "del",
            key: None,
            rule,
            may_empty: false,
            del_instead: false,
            existed: false,
        };
        assert!(!planned(Rule::Done).fires(&Reply::err("ERR no")));
        assert!(planned(Rule::Done).fires(&Reply::Nil));
        assert!(!planned(Rule::NotNil).fires(&Reply::Nil));
        assert!(!planned(Rule::Truthy).fires(&Reply::Int(0)));
        assert!(!planned(Rule::Truthy).fires(&Reply::Int(-1)));
        assert!(!planned(Rule::Truthy).fires(&Reply::Array(Vec::new())));
        assert!(planned(Rule::Truthy).fires(&Reply::Ok));
        let codes = Reply::Array(vec![Reply::Int(-2), Reply::Int(2)]);
        assert!(planned(Rule::Has(2)).fires(&codes));
        assert!(!planned(Rule::Has(1)).fires(&codes));
        // SET IFEQ v GET set the key only if it answered v.
        let ifeq = planned(Rule::Is(xxh3_64(b"v")));
        assert!(ifeq.fires(&Reply::bulk("v")));
        assert!(!ifeq.fires(&Reply::bulk("w")));
        assert!(!ifeq.fires(&Reply::Nil));
    }
}
