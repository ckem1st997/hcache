//! Streams: an append-only log of field/value entries ordered by ID, with
//! consumer groups, per-group pending entry lists (PEL) and the Redis 7
//! command semantics on top, as a self-contained value type.
//!
//! `parse` turns one single-key command into an `XOp`; `Stream::apply` runs
//! it. The multi-key readers (XREAD, XREADGROUP) are built by the caller from
//! `XOp::ReadAfter` and `XOp::ReadGroup`, one per stream.
//!
//! A stream keeps existing when its last entry is deleted or trimmed (its
//! `last_id` and groups live on), so `is_empty` is always false and the
//! number XLEN reports is `entries`.

use crate::cmd::{arity, syntax, Bytes, NOT_INT};
use crate::rdb;
use crate::resp::{parse_i64, Reply};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

type Fields = Vec<(Bytes, Bytes)>;

/// Entries per radix-tree node in Redis (`stream-node-max-entries`). Approximate
/// trimming removes whole nodes, which is simulated with nodes of this size
/// counted from the oldest entry.
const NODE_ENTRIES: usize = 100;
/// LIMIT of an approximate trim when none is given.
const DEFAULT_TRIM_LIMIT: usize = 100 * NODE_ENTRIES;
const NO_LIMIT: usize = usize::MAX;
/// IDs are at most this long as text.
const MAX_ID_LEN: usize = 127;
/// XAUTOCLAIM looks at this many PEL entries per entry it may return.
const AUTOCLAIM_ATTEMPTS: usize = 10;
const DEFAULT_AUTOCLAIM_COUNT: usize = 100;
/// Entries and PEL rows XINFO STREAM FULL lists unless COUNT says otherwise.
const DEFAULT_FULL_COUNT: usize = 10;

/// Entry ID: milliseconds then a sequence number, ordered the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    /// The next ID in order, `None` at the maximum.
    fn next(self) -> Option<StreamId> {
        match self.seq.checked_add(1) {
            Some(seq) => Some(StreamId { ms: self.ms, seq }),
            None => self.ms.checked_add(1).map(|ms| StreamId { ms, seq: 0 }),
        }
    }

    /// The previous ID in order, `None` at 0-0.
    fn prev(self) -> Option<StreamId> {
        match self.seq.checked_sub(1) {
            Some(seq) => Some(StreamId { ms: self.ms, seq }),
            None => self
                .ms
                .checked_sub(1)
                .map(|ms| StreamId { ms, seq: u64::MAX }),
        }
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// An ID argument that may also mean "the last ID of the stream" (`$`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdSpec {
    Id(StreamId),
    Last,
}

/// What XREADGROUP asks for: new entries (`>`) or the consumer's own history
/// after an ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupId {
    New,
    Id(StreamId),
}

/// The ID argument of XADD.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddId {
    /// `*`
    Auto,
    /// `ms-*`
    AutoSeq(u64),
    /// `ms-seq` or `ms`
    Explicit(StreamId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimBy {
    MaxLen(u64),
    MinId(StreamId),
}

/// A trimming strategy as XADD and XTRIM take it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trim {
    pub by: TrimBy,
    /// `~`: remove whole nodes only, so up to a node's worth of entries more
    /// than the threshold may stay.
    pub approx: bool,
    /// Most entries one trim may remove (`NO_LIMIT` for none).
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDetail {
    pub min_idle: u64,
    pub start: StreamId,
    pub end: StreamId,
    pub count: usize,
    pub consumer: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimArgs {
    pub group: Bytes,
    pub consumer: Bytes,
    pub min_idle: u64,
    pub ids: Vec<StreamId>,
    pub idle: Option<i64>,
    pub time: Option<i64>,
    pub retry: Option<u64>,
    pub force: bool,
    pub justid: bool,
    pub lastid: Option<StreamId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoClaimArgs {
    pub group: Bytes,
    pub consumer: Bytes,
    pub min_idle: u64,
    pub start: StreamId,
    pub count: usize,
    pub justid: bool,
}

/// One single-key stream command, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XOp {
    Add {
        id: AddId,
        fields: Fields,
        nomkstream: bool,
        trim: Option<Trim>,
    },
    Len,
    Range {
        start: StreamId,
        end: StreamId,
        /// `Some(0)` returns nothing, as in Redis.
        count: Option<usize>,
        rev: bool,
    },
    Del(Vec<StreamId>),
    Trim(Trim),
    SetId {
        last: StreamId,
        entries_added: Option<u64>,
        max_deleted: Option<StreamId>,
    },
    GroupCreate {
        group: Bytes,
        id: IdSpec,
        mkstream: bool,
        entries_read: Option<u64>,
    },
    GroupDestroy {
        group: Bytes,
    },
    GroupCreateConsumer {
        group: Bytes,
        consumer: Bytes,
    },
    GroupDelConsumer {
        group: Bytes,
        consumer: Bytes,
    },
    GroupSetId {
        group: Bytes,
        id: IdSpec,
        entries_read: Option<u64>,
    },
    Ack {
        group: Bytes,
        ids: Vec<StreamId>,
    },
    /// Summary form when `detail` is `None`.
    Pending {
        group: Bytes,
        detail: Option<PendingDetail>,
    },
    Claim(ClaimArgs),
    AutoClaim(AutoClaimArgs),
    /// `full` is the COUNT of XINFO STREAM FULL (0: no limit).
    InfoStream {
        full: Option<usize>,
    },
    InfoGroups,
    InfoConsumers {
        group: Bytes,
    },
    /// XREAD on one stream. `count` of `None` or `Some(0)`: no limit.
    ReadAfter {
        after: IdSpec,
        count: Option<usize>,
    },
    /// XREADGROUP on one stream. `count` of `None` or `Some(0)`: no limit.
    ReadGroup {
        group: Bytes,
        consumer: Bytes,
        id: GroupId,
        count: Option<usize>,
        noack: bool,
    },
}

// ---- parsing ----------------------------------------------------------

fn int(raw: &[u8]) -> Result<i64, Reply> {
    parse_i64(raw).ok_or_else(|| Reply::err(NOT_INT))
}

fn is(arg: &[u8], word: &str) -> bool {
    arg.eq_ignore_ascii_case(word.as_bytes())
}

fn bad_id() -> Reply {
    Reply::err("ERR Invalid stream ID specified as stream command argument")
}

/// Decimal digits only (no sign, no spaces), at most `u64::MAX`.
fn parse_num(raw: &[u8]) -> Option<u64> {
    if raw.is_empty() || raw.len() > 20 || !raw.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(raw).ok()?.parse().ok()
}

/// `ms-seq`, or `ms` alone with `missing_seq` as the sequence.
fn parse_strict(raw: &[u8], missing_seq: u64) -> Option<StreamId> {
    if raw.len() > MAX_ID_LEN {
        return None;
    }
    match raw.iter().position(|&b| b == b'-') {
        None => Some(StreamId {
            ms: parse_num(raw)?,
            seq: missing_seq,
        }),
        Some(dash) => Some(StreamId {
            ms: parse_num(&raw[..dash])?,
            seq: parse_num(&raw[dash + 1..])?,
        }),
    }
}

/// The ID argument of XREAD: `$` or an ID (a missing sequence is 0).
pub fn parse_read_id(raw: &[u8]) -> Result<IdSpec, Reply> {
    if raw == b"$" {
        return Ok(IdSpec::Last);
    }
    parse_strict(raw, 0).map(IdSpec::Id).ok_or_else(bad_id)
}

/// The ID argument of XREADGROUP: `>` or an ID (a missing sequence is 0).
pub fn parse_group_read_id(raw: &[u8]) -> Result<GroupId, Reply> {
    if raw == b">" {
        return Ok(GroupId::New);
    }
    parse_strict(raw, 0).map(GroupId::Id).ok_or_else(bad_id)
}

/// A range bound: `-`, `+`, an ID, optionally exclusive with a `(` prefix.
struct Bound {
    id: StreamId,
    exclusive: bool,
}

fn parse_interval(raw: &[u8], missing_seq: u64) -> Result<Bound, Reply> {
    let (exclusive, body) = match raw.strip_prefix(b"(") {
        Some(rest) => (true, rest),
        None => (false, raw),
    };
    let id = match body {
        b"-" => StreamId::MIN,
        b"+" => StreamId::MAX,
        _ => parse_strict(body, missing_seq).ok_or_else(bad_id)?,
    };
    Ok(Bound { id, exclusive })
}

fn start_of(bound: Bound) -> Result<StreamId, Reply> {
    if !bound.exclusive {
        return Ok(bound.id);
    }
    bound
        .id
        .next()
        .ok_or_else(|| Reply::err("ERR invalid start ID for the interval"))
}

fn end_of(bound: Bound) -> Result<StreamId, Reply> {
    if !bound.exclusive {
        return Ok(bound.id);
    }
    bound
        .id
        .prev()
        .ok_or_else(|| Reply::err("ERR invalid end ID for the interval"))
}

fn parse_ids(raw: &[Bytes]) -> Result<Vec<StreamId>, Reply> {
    raw.iter()
        .map(|id| parse_strict(id, 0).ok_or_else(bad_id))
        .collect()
}

/// What XADD and XTRIM parsed before the fields.
struct AddArgs {
    trim: Option<Trim>,
    nomkstream: bool,
    id: Option<AddId>,
    /// Index of the first argument after the ID (XADD) or the last one read.
    next: usize,
}

fn parse_add_id(raw: &[u8]) -> Result<AddId, Reply> {
    if raw == b"*" {
        return Ok(AddId::Auto);
    }
    if let Some(ms) = raw.strip_suffix(b"-*") {
        if raw.len() <= MAX_ID_LEN {
            if let Some(ms) = parse_num(ms) {
                return Ok(AddId::AutoSeq(ms));
            }
        }
        return Err(bad_id());
    }
    parse_strict(raw, 0).map(AddId::Explicit).ok_or_else(bad_id)
}

/// The options XADD and XTRIM share, then (XADD) the ID.
fn parse_add_args(args: &[Bytes], xadd: bool) -> Result<AddArgs, Reply> {
    let n = args.len();
    let mut i = 2;
    let mut by = None;
    let mut approx = false;
    let mut limit: Option<i64> = None;
    let mut nomkstream = false;
    let mut id = None;
    while i < n {
        let more = n - 1 - i;
        let opt = &args[i];
        if (is(opt, "MAXLEN") || is(opt, "MINID")) && more > 0 {
            if by.is_some() {
                return Err(Reply::err(
                    "ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
                ));
            }
            let maxlen = is(opt, "MAXLEN");
            i += 1;
            if more >= 2 && (args[i] == b"~" || args[i] == b"=") {
                approx = args[i] == b"~";
                i += 1;
            }
            by = Some(if maxlen {
                let threshold = int(&args[i])?;
                if threshold < 0 {
                    return Err(Reply::err("ERR The MAXLEN argument must be >= 0."));
                }
                TrimBy::MaxLen(threshold as u64)
            } else {
                TrimBy::MinId(parse_strict(&args[i], 0).ok_or_else(bad_id)?)
            });
            i += 1;
        } else if is(opt, "LIMIT") && more > 0 {
            let value = int(&args[i + 1])?;
            if value < 0 {
                return Err(Reply::err("ERR The LIMIT argument must be >= 0."));
            }
            limit = Some(value);
            i += 2;
        } else if xadd && is(opt, "NOMKSTREAM") {
            nomkstream = true;
            i += 1;
        } else if xadd {
            id = Some(parse_add_id(opt)?);
            i += 1;
            break;
        } else {
            return Err(syntax());
        }
    }
    if limit.is_some() && (by.is_none() || !approx) {
        return Err(Reply::err(
            "ERR syntax error, LIMIT cannot be used without the special ~ option",
        ));
    }
    let trim = by.map(|by| Trim {
        by,
        approx,
        limit: match limit {
            _ if !approx => NO_LIMIT,
            None => DEFAULT_TRIM_LIMIT,
            Some(0) => NO_LIMIT,
            Some(n) => n as usize,
        },
    });
    Ok(AddArgs {
        trim,
        nomkstream,
        id,
        next: i,
    })
}

fn parse_xadd(args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    if args.len() < 5 {
        return Err(arity("xadd"));
    }
    let parsed = parse_add_args(args, true)?;
    let fields = &args[parsed.next..];
    let Some(id) = parsed.id else {
        return Err(arity("xadd"));
    };
    if fields.len() < 2 || fields.len() % 2 == 1 {
        return Err(arity("xadd"));
    }
    let fields = fields
        .chunks(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect();
    Ok((
        args[1].clone(),
        XOp::Add {
            id,
            fields,
            nomkstream: parsed.nomkstream,
            trim: parsed.trim,
        },
    ))
}

fn parse_range(name: &str, args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    if args.len() < 4 {
        return Err(arity(name));
    }
    let rev = name == "XREVRANGE";
    let (start_raw, end_raw) = if rev {
        (&args[3], &args[2])
    } else {
        (&args[2], &args[3])
    };
    let start = start_of(parse_interval(start_raw, 0)?)?;
    let end = end_of(parse_interval(end_raw, u64::MAX)?)?;
    let mut count = None;
    let mut i = 4;
    while i < args.len() {
        if is(&args[i], "COUNT") && i + 1 < args.len() {
            count = Some(int(&args[i + 1])?.max(0) as usize);
            i += 2;
        } else {
            return Err(syntax());
        }
    }
    Ok((
        args[1].clone(),
        XOp::Range {
            start,
            end,
            count,
            rev,
        },
    ))
}

fn parse_setid(args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    if args.len() < 3 {
        return Err(arity("xsetid"));
    }
    let last = parse_strict(&args[2], 0).ok_or_else(bad_id)?;
    let mut entries_added = None;
    let mut max_deleted = None;
    let mut i = 3;
    while i < args.len() {
        let more = args.len() - 1 - i;
        if is(&args[i], "ENTRIESADDED") && more > 0 {
            let value = int(&args[i + 1])?;
            if value < 0 {
                return Err(Reply::err("ERR entries_added must be positive"));
            }
            entries_added = Some(value as u64);
        } else if is(&args[i], "MAXDELETEDID") && more > 0 {
            max_deleted = Some(parse_strict(&args[i + 1], 0).ok_or_else(bad_id)?);
        } else {
            return Err(syntax());
        }
        i += 2;
    }
    Ok((
        args[1].clone(),
        XOp::SetId {
            last,
            entries_added,
            max_deleted,
        },
    ))
}

fn parse_group_id_arg(raw: &[u8]) -> Result<IdSpec, Reply> {
    if raw == b"$" {
        return Ok(IdSpec::Last);
    }
    parse_strict(raw, 0).map(IdSpec::Id).ok_or_else(bad_id)
}

/// `-1` and absence both mean "unknown".
fn parse_entries_read(raw: &[u8]) -> Result<Option<u64>, Reply> {
    match int(raw)? {
        -1 => Ok(None),
        n if n < 0 => Err(Reply::err(
            "ERR value for ENTRIESREAD must be positive or -1",
        )),
        n => Ok(Some(n as u64)),
    }
}

fn parse_xgroup(args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    if args.len() < 2 {
        return Err(arity("xgroup"));
    }
    let n = args.len();
    let sub = args[1].to_ascii_uppercase();
    match sub.as_slice() {
        b"CREATE" | b"SETID" => {
            let create = sub == b"CREATE";
            if n < 5 {
                return Err(arity(if create {
                    "xgroup|create"
                } else {
                    "xgroup|setid"
                }));
            }
            let id = parse_group_id_arg(&args[4])?;
            let mut mkstream = false;
            let mut entries_read = None;
            let mut i = 5;
            while i < n {
                if create && is(&args[i], "MKSTREAM") {
                    mkstream = true;
                    i += 1;
                } else if is(&args[i], "ENTRIESREAD") && i + 1 < n {
                    entries_read = parse_entries_read(&args[i + 1])?;
                    i += 2;
                } else {
                    return Err(syntax());
                }
            }
            let group = args[3].clone();
            let op = if create {
                XOp::GroupCreate {
                    group,
                    id,
                    mkstream,
                    entries_read,
                }
            } else {
                XOp::GroupSetId {
                    group,
                    id,
                    entries_read,
                }
            };
            Ok((args[2].clone(), op))
        }
        b"DESTROY" => {
            if n != 4 {
                return Err(arity("xgroup|destroy"));
            }
            let group = args[3].clone();
            Ok((args[2].clone(), XOp::GroupDestroy { group }))
        }
        b"CREATECONSUMER" | b"DELCONSUMER" => {
            let create = sub == b"CREATECONSUMER";
            if n != 5 {
                return Err(arity(if create {
                    "xgroup|createconsumer"
                } else {
                    "xgroup|delconsumer"
                }));
            }
            let (group, consumer) = (args[3].clone(), args[4].clone());
            let op = if create {
                XOp::GroupCreateConsumer { group, consumer }
            } else {
                XOp::GroupDelConsumer { group, consumer }
            };
            Ok((args[2].clone(), op))
        }
        _ => Err(unknown_sub(&args[1], "XGROUP")),
    }
}

fn unknown_sub(sub: &[u8], command: &str) -> Reply {
    Reply::err(format!(
        "ERR unknown subcommand '{}'. Try {command} HELP.",
        String::from_utf8_lossy(sub)
    ))
}

fn parse_xinfo(args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    if args.len() < 2 {
        return Err(arity("xinfo"));
    }
    let n = args.len();
    match args[1].to_ascii_uppercase().as_slice() {
        b"STREAM" => {
            if n < 3 {
                return Err(arity("xinfo|stream"));
            }
            let mut full = None;
            if n > 3 {
                if (n != 4 && n != 6) || !is(&args[3], "FULL") {
                    return Err(syntax());
                }
                full = Some(DEFAULT_FULL_COUNT);
                if n == 6 {
                    if !is(&args[4], "COUNT") {
                        return Err(syntax());
                    }
                    full = Some(int(&args[5])?.max(0) as usize);
                }
            }
            Ok((args[2].clone(), XOp::InfoStream { full }))
        }
        b"GROUPS" => {
            if n != 3 {
                return Err(arity("xinfo|groups"));
            }
            Ok((args[2].clone(), XOp::InfoGroups))
        }
        b"CONSUMERS" => {
            if n != 4 {
                return Err(arity("xinfo|consumers"));
            }
            Ok((
                args[2].clone(),
                XOp::InfoConsumers {
                    group: args[3].clone(),
                },
            ))
        }
        _ => Err(unknown_sub(&args[1], "XINFO")),
    }
}

fn parse_pending(args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    let n = args.len();
    if n < 3 {
        return Err(arity("xpending"));
    }
    if n != 3 && !(6..=9).contains(&n) {
        return Err(syntax());
    }
    let (key, group) = (args[1].clone(), args[2].clone());
    if n == 3 {
        return Ok((
            key,
            XOp::Pending {
                group,
                detail: None,
            },
        ));
    }
    let mut j = 3;
    let mut min_idle = 0;
    if is(&args[3], "IDLE") {
        min_idle = int(&args[4])?.max(0) as u64;
        j = 5;
    }
    if n - j < 3 || n - j > 4 {
        return Err(syntax());
    }
    let start = start_of(parse_interval(&args[j], 0)?)?;
    let end = end_of(parse_interval(&args[j + 1], u64::MAX)?)?;
    let count = int(&args[j + 2])?.max(0) as usize;
    let consumer = args.get(j + 3).cloned();
    let detail = PendingDetail {
        min_idle,
        start,
        end,
        count,
        consumer,
    };
    Ok((
        key,
        XOp::Pending {
            group,
            detail: Some(detail),
        },
    ))
}

fn parse_claim(args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    if args.len() < 6 {
        return Err(arity("xclaim"));
    }
    let min_idle = parse_i64(&args[4])
        .ok_or_else(|| Reply::err("ERR Invalid min-idle-time argument for XCLAIM"))?
        .max(0) as u64;
    let mut ids = Vec::new();
    let mut i = 5;
    while let Some(id) = args.get(i).and_then(|raw| parse_strict(raw, 0)) {
        ids.push(id);
        i += 1;
    }
    let mut claim = ClaimArgs {
        group: args[2].clone(),
        consumer: args[3].clone(),
        min_idle,
        ids,
        idle: None,
        time: None,
        retry: None,
        force: false,
        justid: false,
        lastid: None,
    };
    let invalid =
        |option: &str| Reply::err(format!("ERR Invalid {option} option argument for XCLAIM"));
    while i < args.len() {
        let more = args.len() - 1 - i;
        let opt = &args[i];
        if is(opt, "FORCE") {
            claim.force = true;
        } else if is(opt, "JUSTID") {
            claim.justid = true;
        } else if is(opt, "IDLE") && more > 0 {
            i += 1;
            claim.idle = Some(parse_i64(&args[i]).ok_or_else(|| invalid("IDLE"))?);
        } else if is(opt, "TIME") && more > 0 {
            i += 1;
            claim.time = Some(parse_i64(&args[i]).ok_or_else(|| invalid("TIME"))?);
        } else if is(opt, "RETRYCOUNT") && more > 0 {
            i += 1;
            let value = parse_i64(&args[i]).ok_or_else(|| invalid("RETRYCOUNT"))?;
            claim.retry = Some(u64::try_from(value).map_err(|_| invalid("RETRYCOUNT"))?);
        } else if is(opt, "LASTID") && more > 0 {
            i += 1;
            claim.lastid = Some(parse_strict(&args[i], 0).ok_or_else(bad_id)?);
        } else {
            return Err(Reply::err(format!(
                "ERR Unrecognized XCLAIM option '{}'",
                String::from_utf8_lossy(opt)
            )));
        }
        i += 1;
    }
    Ok((args[1].clone(), XOp::Claim(claim)))
}

fn parse_autoclaim(args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    if args.len() < 6 {
        return Err(arity("xautoclaim"));
    }
    let min_idle = parse_i64(&args[4])
        .ok_or_else(|| Reply::err("ERR Invalid min-idle-time argument for XAUTOCLAIM"))?
        .max(0) as u64;
    let start = start_of(parse_interval(&args[5], 0)?)?;
    let mut count = DEFAULT_AUTOCLAIM_COUNT;
    let mut justid = false;
    let mut i = 6;
    while i < args.len() {
        let more = args.len() - 1 - i;
        if is(&args[i], "COUNT") && more > 0 {
            let value = int(&args[i + 1])?;
            if value < 1 || value > i64::MAX / AUTOCLAIM_ATTEMPTS as i64 {
                return Err(Reply::err("ERR COUNT must be > 0"));
            }
            count = value as usize;
            i += 2;
        } else if is(&args[i], "JUSTID") {
            justid = true;
            i += 1;
        } else {
            return Err(syntax());
        }
    }
    Ok((
        args[1].clone(),
        XOp::AutoClaim(AutoClaimArgs {
            group: args[2].clone(),
            consumer: args[3].clone(),
            min_idle,
            start,
            count,
            justid,
        }),
    ))
}

/// Parse one single-key stream command. `name` is the upper-cased command
/// name, `args[0]` the command name as sent. Returns the key and the op.
pub fn parse(name: &str, args: &[Bytes]) -> Result<(Bytes, XOp), Reply> {
    match name {
        "XADD" => parse_xadd(args),
        "XLEN" => {
            if args.len() != 2 {
                return Err(arity("xlen"));
            }
            Ok((args[1].clone(), XOp::Len))
        }
        "XRANGE" | "XREVRANGE" => parse_range(name, args),
        "XDEL" => {
            if args.len() < 3 {
                return Err(arity("xdel"));
            }
            Ok((args[1].clone(), XOp::Del(parse_ids(&args[2..])?)))
        }
        "XTRIM" => {
            if args.len() < 4 {
                return Err(arity("xtrim"));
            }
            let parsed = parse_add_args(args, false)?;
            match parsed.trim {
                Some(trim) => Ok((args[1].clone(), XOp::Trim(trim))),
                None => Err(syntax()),
            }
        }
        "XSETID" => parse_setid(args),
        "XGROUP" => parse_xgroup(args),
        "XACK" => {
            if args.len() < 4 {
                return Err(arity("xack"));
            }
            let ids = parse_ids(&args[3..])?;
            let group = args[2].clone();
            Ok((args[1].clone(), XOp::Ack { group, ids }))
        }
        "XPENDING" => parse_pending(args),
        "XCLAIM" => parse_claim(args),
        "XAUTOCLAIM" => parse_autoclaim(args),
        "XINFO" => parse_xinfo(args),
        _ => Err(Reply::err(format!(
            "ERR unknown command '{}'",
            name.to_ascii_lowercase()
        ))),
    }
}

// ---- replies ----------------------------------------------------------

fn lossy(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).into_owned()
}

fn id_reply(id: StreamId) -> Reply {
    Reply::bulk(id.to_string())
}

fn entry_reply(id: StreamId, fields: &Fields) -> Reply {
    let flat = fields
        .iter()
        .flat_map(|(f, v)| [Reply::bulk(f.as_slice()), Reply::bulk(v.as_slice())])
        .collect();
    Reply::Array(vec![id_reply(id), Reply::Array(flat)])
}

fn map(pairs: Vec<(&str, Reply)>) -> Reply {
    Reply::Map(
        pairs
            .into_iter()
            .flat_map(|(name, value)| [Reply::bulk(name), value])
            .collect(),
    )
}

fn int_reply(n: usize) -> Reply {
    Reply::Int(i64::try_from(n).unwrap_or(i64::MAX))
}

fn u64_reply(n: u64) -> Reply {
    Reply::Int(i64::try_from(n).unwrap_or(i64::MAX))
}

fn no_such_key() -> Reply {
    Reply::err("ERR no such key")
}

fn group_missing_key() -> Reply {
    Reply::err(
        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
    )
}

/// NOGROUP for the commands that name a group on an existing key.
fn nogroup_group(key: &[u8], group: &[u8]) -> Reply {
    Reply::err(format!(
        "NOGROUP No such consumer group '{}' for key name '{}'",
        lossy(group),
        lossy(key)
    ))
}

/// NOGROUP for the commands where a missing key and a missing group read the
/// same (XPENDING, XCLAIM, XAUTOCLAIM).
fn nogroup_either(key: &[u8], group: &[u8]) -> Reply {
    Reply::err(format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        lossy(key),
        lossy(group)
    ))
}

fn nogroup_read(key: &[u8], group: &[u8]) -> Reply {
    Reply::err(format!(
        "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
        lossy(key),
        lossy(group)
    ))
}

impl XOp {
    pub fn is_write(&self) -> bool {
        !matches!(
            self,
            XOp::Len
                | XOp::Range { .. }
                | XOp::Pending { .. }
                | XOp::InfoStream { .. }
                | XOp::InfoGroups
                | XOp::InfoConsumers { .. }
                | XOp::ReadAfter { .. }
        )
    }

    /// True when the op makes the key when it is missing.
    pub fn creates(&self) -> bool {
        match self {
            XOp::Add { nomkstream, .. } => !nomkstream,
            XOp::GroupCreate { mkstream, .. } => *mkstream,
            _ => false,
        }
    }

    /// The reply when the key does not exist and the op does not create it.
    pub fn missing(&self, key: &[u8]) -> Reply {
        match self {
            XOp::Add { .. } | XOp::ReadAfter { .. } => Reply::Nil,
            XOp::Len | XOp::Del(_) | XOp::Trim(_) | XOp::Ack { .. } => Reply::Int(0),
            XOp::Range { .. } => Reply::Array(Vec::new()),
            XOp::SetId { .. }
            | XOp::InfoStream { .. }
            | XOp::InfoGroups
            | XOp::InfoConsumers { .. } => no_such_key(),
            XOp::GroupCreate { .. }
            | XOp::GroupDestroy { .. }
            | XOp::GroupCreateConsumer { .. }
            | XOp::GroupDelConsumer { .. }
            | XOp::GroupSetId { .. } => group_missing_key(),
            XOp::Pending { group, .. } => nogroup_either(key, group),
            XOp::Claim(c) => nogroup_either(key, &c.group),
            XOp::AutoClaim(c) => nogroup_either(key, &c.group),
            XOp::ReadGroup { group, .. } => nogroup_read(key, group),
        }
    }
}

// ---- the stream -------------------------------------------------------

#[derive(Clone)]
struct Pending {
    consumer: Bytes,
    delivered_ms: u64,
    count: u64,
}

#[derive(Clone)]
struct Consumer {
    seen_ms: u64,
    /// Last time it got an entry; `None` when it never has.
    active_ms: Option<u64>,
    pending: BTreeSet<StreamId>,
}

#[derive(Clone)]
struct Group {
    last_id: StreamId,
    /// Entries ever read through the group; `None` when unknown.
    entries_read: Option<u64>,
    pel: BTreeMap<StreamId, Pending>,
    consumers: BTreeMap<Bytes, Consumer>,
}

impl Group {
    fn new(last_id: StreamId, entries_read: Option<u64>) -> Group {
        Group {
            last_id,
            entries_read,
            pel: BTreeMap::new(),
            consumers: BTreeMap::new(),
        }
    }

    /// Find or create the consumer and mark it seen. True when created.
    fn touch(&mut self, name: &[u8], now: u64) -> bool {
        if let Some(consumer) = self.consumers.get_mut(name) {
            consumer.seen_ms = now;
            return false;
        }
        let consumer = Consumer {
            seen_ms: now,
            active_ms: None,
            pending: BTreeSet::new(),
        };
        self.consumers.insert(name.to_vec(), consumer);
        true
    }

    fn remove_pending(&mut self, id: StreamId) -> bool {
        let Some(pending) = self.pel.remove(&id) else {
            return false;
        };
        if let Some(owner) = self.consumers.get_mut(&pending.consumer) {
            owner.pending.remove(&id);
        }
        true
    }

    /// Make `consumer` (which must exist) the owner of `id` in the PEL.
    fn assign(&mut self, id: StreamId, consumer: &[u8], delivered_ms: u64, count: u64) {
        let row = Pending {
            consumer: consumer.to_vec(),
            delivered_ms,
            count,
        };
        if let Some(old) = self.pel.insert(id, row) {
            if old.consumer != consumer {
                if let Some(owner) = self.consumers.get_mut(&old.consumer) {
                    owner.pending.remove(&id);
                }
            }
        }
        if let Some(owner) = self.consumers.get_mut(consumer) {
            owner.pending.insert(id);
        }
    }

    fn activate(&mut self, consumer: &[u8], now: u64) {
        if let Some(c) = self.consumers.get_mut(consumer) {
            c.active_ms = Some(now);
        }
    }
}

#[derive(Clone, Default)]
pub struct Stream {
    entries: BTreeMap<StreamId, Fields>,
    last_id: StreamId,
    max_deleted_id: StreamId,
    entries_added: u64,
    groups: BTreeMap<Bytes, Group>,
}

fn add_error(exhausted: bool) -> Reply {
    if exhausted {
        Reply::err("ERR The stream has exhausted the last possible ID, unable to add more items")
    } else {
        Reply::err(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        )
    }
}

impl Stream {
    /// Size for the keyspace, never zero: a stream with no entries is still a
    /// key. XLEN is `entries`.
    pub fn len(&self) -> usize {
        self.entries.len().max(1)
    }

    /// Always false: a stream exists until its key is deleted, even with no
    /// entries (its last ID and groups remain).
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Number of entries (XLEN).
    pub fn entries(&self) -> usize {
        self.entries.len()
    }

    /// The highest ID ever added; what `$` means to XREAD.
    pub fn last_id(&self) -> StreamId {
        self.last_id
    }

    /// Run `op` at `now_ms`. Returns the reply and whether the stream changed
    /// (group and PEL changes included), so the caller knows to persist it.
    /// A caller that just created the stream for an op with `creates()` should
    /// drop it again when the op failed with nothing changed.
    pub fn apply(&mut self, key: &[u8], op: XOp, now_ms: u64) -> (Reply, bool) {
        match op {
            XOp::Add {
                id, fields, trim, ..
            } => self.add(id, fields, trim, now_ms),
            XOp::Len => (int_reply(self.entries.len()), false),
            XOp::Range {
                start,
                end,
                count,
                rev,
            } => (self.range(start, end, count, rev), false),
            XOp::Del(ids) => self.del(&ids),
            XOp::Trim(trim) => {
                let removed = self.trim(&trim);
                (int_reply(removed), removed > 0)
            }
            XOp::SetId {
                last,
                entries_added,
                max_deleted,
            } => self.set_id(last, entries_added, max_deleted),
            XOp::GroupCreate {
                group,
                id,
                entries_read,
                ..
            } => self.group_create(group, id, entries_read, key),
            XOp::GroupDestroy { group } => {
                let removed = self.groups.remove(&group).is_some();
                (Reply::Int(i64::from(removed)), removed)
            }
            XOp::GroupCreateConsumer { group, consumer } => {
                let Some(g) = self.groups.get_mut(&group) else {
                    return (nogroup_group(key, &group), false);
                };
                let created = g.touch(&consumer, now_ms);
                (Reply::Int(i64::from(created)), created)
            }
            XOp::GroupDelConsumer { group, consumer } => {
                let Some(g) = self.groups.get_mut(&group) else {
                    return (nogroup_group(key, &group), false);
                };
                let Some(gone) = g.consumers.remove(&consumer) else {
                    return (Reply::Int(0), false);
                };
                for id in &gone.pending {
                    g.pel.remove(id);
                }
                (int_reply(gone.pending.len()), true)
            }
            XOp::GroupSetId {
                group,
                id,
                entries_read,
            } => {
                let last = match id {
                    IdSpec::Id(id) => id,
                    IdSpec::Last => self.last_id,
                };
                let Some(g) = self.groups.get_mut(&group) else {
                    return (nogroup_group(key, &group), false);
                };
                g.last_id = last;
                g.entries_read = entries_read;
                (Reply::Ok, true)
            }
            XOp::Ack { group, ids } => {
                let Some(g) = self.groups.get_mut(&group) else {
                    return (Reply::Int(0), false);
                };
                let mut acked = 0;
                for id in ids {
                    if g.remove_pending(id) {
                        acked += 1;
                    }
                }
                (int_reply(acked), acked > 0)
            }
            XOp::Pending { group, detail } => (self.pending(key, &group, detail, now_ms), false),
            XOp::Claim(args) => {
                let group = args.group.clone();
                match self.with_group(&group, |s, g| s.claim(g, &args, now_ms)) {
                    Some(result) => result,
                    None => (nogroup_either(key, &group), false),
                }
            }
            XOp::AutoClaim(args) => {
                let group = args.group.clone();
                match self.with_group(&group, |s, g| s.autoclaim(g, &args, now_ms)) {
                    Some(result) => result,
                    None => (nogroup_either(key, &group), false),
                }
            }
            XOp::InfoStream { full } => (self.info_stream(full, now_ms), false),
            XOp::InfoGroups => (
                Reply::Array(
                    self.groups
                        .iter()
                        .map(|(n, g)| self.group_info(n, g))
                        .collect(),
                ),
                false,
            ),
            XOp::InfoConsumers { group } => match self.groups.get(&group) {
                None => (nogroup_group(key, &group), false),
                Some(g) => {
                    let rows = g
                        .consumers
                        .iter()
                        .map(|(name, c)| {
                            map(vec![
                                ("name", Reply::bulk(name.as_slice())),
                                ("pending", int_reply(c.pending.len())),
                                ("idle", u64_reply(now_ms.saturating_sub(c.seen_ms))),
                                (
                                    "inactive",
                                    c.active_ms.map_or(Reply::Int(-1), |a| {
                                        u64_reply(now_ms.saturating_sub(a))
                                    }),
                                ),
                            ])
                        })
                        .collect();
                    (Reply::Array(rows), false)
                }
            },
            XOp::ReadAfter { after, count } => (self.read_after(after, count), false),
            XOp::ReadGroup {
                group,
                consumer,
                id,
                count,
                noack,
            } => {
                let result = self.with_group(&group, |s, g| {
                    s.read_group(g, &consumer, id, count, noack, now_ms)
                });
                result.unwrap_or_else(|| (nogroup_read(key, &group), false))
            }
        }
    }

    /// Run `f` with the group taken out of the map, so it can read the rest
    /// of the stream while changing the group. `None` when there is no such
    /// group.
    fn with_group<R>(
        &mut self,
        name: &[u8],
        f: impl FnOnce(&Stream, &mut Group) -> R,
    ) -> Option<R> {
        let (name, mut group) = self.groups.remove_entry(name)?;
        let result = f(self, &mut group);
        self.groups.insert(name, group);
        Some(result)
    }

    fn add(&mut self, id: AddId, fields: Fields, trim: Option<Trim>, now_ms: u64) -> (Reply, bool) {
        let last = self.last_id;
        let new_id = match id {
            AddId::Explicit(id) => {
                if id == StreamId::MIN {
                    return (
                        Reply::err("ERR The ID specified in XADD must be greater than 0-0"),
                        false,
                    );
                }
                if id <= last {
                    return (add_error(false), false);
                }
                id
            }
            AddId::AutoSeq(ms) => match ms.cmp(&last.ms) {
                std::cmp::Ordering::Less => return (add_error(false), false),
                std::cmp::Ordering::Greater => StreamId { ms, seq: 0 },
                std::cmp::Ordering::Equal => match last.seq.checked_add(1) {
                    Some(seq) => StreamId { ms, seq },
                    None => return (add_error(false), false),
                },
            },
            AddId::Auto => {
                if now_ms > last.ms {
                    StreamId { ms: now_ms, seq: 0 }
                } else {
                    match last.next() {
                        Some(next) => next,
                        None => return (add_error(true), false),
                    }
                }
            }
        };
        self.entries.insert(new_id, fields);
        self.last_id = new_id;
        self.entries_added = self.entries_added.saturating_add(1);
        if let Some(trim) = trim {
            self.trim(&trim);
        }
        (id_reply(new_id), true)
    }

    /// Remove entries as the strategy says; returns how many went.
    fn trim(&mut self, trim: &Trim) -> usize {
        let wanted = match trim.by {
            TrimBy::MaxLen(max) => {
                let max = usize::try_from(max).unwrap_or(usize::MAX);
                self.entries.len().saturating_sub(max)
            }
            TrimBy::MinId(min) => self.entries.range(..min).count(),
        };
        let mut remove = wanted;
        if trim.approx {
            remove = remove / NODE_ENTRIES * NODE_ENTRIES;
            if trim.limit != NO_LIMIT {
                remove = remove.min(trim.limit / NODE_ENTRIES * NODE_ENTRIES);
            }
        }
        for _ in 0..remove {
            self.entries.pop_first();
        }
        remove
    }

    fn range(&self, start: StreamId, end: StreamId, count: Option<usize>, rev: bool) -> Reply {
        let limit = count.unwrap_or(usize::MAX);
        if start > end || limit == 0 {
            return Reply::Array(Vec::new());
        }
        let found = self.entries.range(start..=end);
        let rows: Vec<Reply> = if rev {
            found
                .rev()
                .take(limit)
                .map(|(id, fields)| entry_reply(*id, fields))
                .collect()
        } else {
            found
                .take(limit)
                .map(|(id, fields)| entry_reply(*id, fields))
                .collect()
        };
        Reply::Array(rows)
    }

    fn del(&mut self, ids: &[StreamId]) -> (Reply, bool) {
        let mut deleted = 0;
        for id in ids {
            if self.entries.remove(id).is_some() {
                self.max_deleted_id = self.max_deleted_id.max(*id);
                deleted += 1;
            }
        }
        (int_reply(deleted), deleted > 0)
    }

    fn set_id(
        &mut self,
        last: StreamId,
        entries_added: Option<u64>,
        max_deleted: Option<StreamId>,
    ) -> (Reply, bool) {
        let top = self.entries.keys().next_back().copied().unwrap_or_default();
        if last < top {
            return (
                Reply::err(
                    "ERR The ID specified in XSETID is smaller than the target stream top item",
                ),
                false,
            );
        }
        if entries_added.is_some_and(|n| (self.entries.len() as u64) > n) {
            return (
                Reply::err(
                    "ERR The entries_added specified in XSETID is smaller than the target stream length",
                ),
                false,
            );
        }
        if max_deleted.is_some_and(|m| m != StreamId::MIN && last < m) {
            return (
                Reply::err(
                    "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                ),
                false,
            );
        }
        self.last_id = last;
        if let Some(n) = entries_added {
            self.entries_added = n;
        }
        if let Some(m) = max_deleted.filter(|m| *m != StreamId::MIN) {
            self.max_deleted_id = m;
        }
        (Reply::Ok, true)
    }

    fn group_create(
        &mut self,
        group: Bytes,
        id: IdSpec,
        entries_read: Option<u64>,
        key: &[u8],
    ) -> (Reply, bool) {
        let _ = key;
        if self.groups.contains_key(&group) {
            return (
                Reply::err("BUSYGROUP Consumer Group name already exists"),
                false,
            );
        }
        let last = match id {
            IdSpec::Id(id) => id,
            IdSpec::Last => self.last_id,
        };
        self.groups.insert(group, Group::new(last, entries_read));
        (Reply::Ok, true)
    }

    // ---- lag accounting ----

    /// True when an entry at or after `start` has been deleted.
    fn has_tombstones(&self, start: StreamId) -> bool {
        !self.entries.is_empty()
            && self.max_deleted_id != StreamId::MIN
            && start <= self.max_deleted_id
    }

    /// Entries ever added up to and including `id`, when that can be told
    /// exactly (Redis' `streamEstimateDistanceFromFirstEverEntry`).
    fn distance_to(&self, id: StreamId) -> Option<u64> {
        if self.entries_added == 0 {
            return Some(0);
        }
        if self.entries.is_empty() && id <= self.last_id {
            return Some(self.entries_added);
        }
        match id.cmp(&self.last_id) {
            std::cmp::Ordering::Equal => return Some(self.entries_added),
            std::cmp::Ordering::Greater => return None,
            std::cmp::Ordering::Less => {}
        }
        let first = *self.entries.keys().next()?;
        let len = self.entries.len() as u64;
        let intact = self.max_deleted_id == StreamId::MIN || self.max_deleted_id < first;
        if intact {
            match id.cmp(&first) {
                std::cmp::Ordering::Less => return Some(self.entries_added.saturating_sub(len)),
                std::cmp::Ordering::Equal => {
                    return Some(self.entries_added.saturating_sub(len).saturating_add(1))
                }
                std::cmp::Ordering::Greater => {}
            }
        }
        None
    }

    fn lag(&self, g: &Group) -> Reply {
        if self.entries_added == 0 {
            return Reply::Int(0);
        }
        let read = match g.entries_read {
            Some(read) if !self.has_tombstones(g.last_id) => Some(read),
            _ => self.distance_to(g.last_id),
        };
        read.map_or(Reply::Nil, |read| {
            u64_reply(self.entries_added.saturating_sub(read))
        })
    }

    // ---- readers ----

    fn read_after(&self, after: IdSpec, count: Option<usize>) -> Reply {
        let after = match after {
            IdSpec::Id(id) => id,
            IdSpec::Last => self.last_id,
        };
        let limit = match count {
            None | Some(0) => usize::MAX,
            Some(n) => n,
        };
        let Some(from) = after.next() else {
            return Reply::Nil;
        };
        let rows: Vec<Reply> = self
            .entries
            .range(from..)
            .take(limit)
            .map(|(id, fields)| entry_reply(*id, fields))
            .collect();
        if rows.is_empty() {
            Reply::Nil
        } else {
            Reply::Array(rows)
        }
    }

    fn read_group(
        &self,
        g: &mut Group,
        consumer: &[u8],
        id: GroupId,
        count: Option<usize>,
        noack: bool,
        now: u64,
    ) -> (Reply, bool) {
        let limit = match count {
            None | Some(0) => usize::MAX,
            Some(n) => n,
        };
        let mut changed = g.touch(consumer, now);
        match id {
            GroupId::New => {
                let ids: Vec<StreamId> = g
                    .last_id
                    .next()
                    .map(|from| {
                        self.entries
                            .range(from..)
                            .take(limit)
                            .map(|(id, _)| *id)
                            .collect()
                    })
                    .unwrap_or_default();
                if ids.is_empty() {
                    return (Reply::Nil, changed);
                }
                let mut rows = Vec::with_capacity(ids.len());
                for id in ids {
                    let Some(fields) = self.entries.get(&id) else {
                        continue;
                    };
                    rows.push(entry_reply(id, fields));
                    if !noack {
                        g.assign(id, consumer, now, 1);
                    }
                    if g.entries_read.is_some() && !self.has_tombstones(id) {
                        g.entries_read = g.entries_read.map(|n| n.saturating_add(1));
                    } else if self.entries_added != 0 {
                        g.entries_read = self.distance_to(id);
                    }
                    g.last_id = id;
                }
                g.activate(consumer, now);
                (Reply::Array(rows), true)
            }
            GroupId::Id(after) => {
                let ids: Vec<StreamId> = match (after.next(), g.consumers.get(consumer)) {
                    (Some(from), Some(c)) => c.pending.range(from..).take(limit).copied().collect(),
                    _ => Vec::new(),
                };
                let mut rows = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(row) = g.pel.get_mut(&id) {
                        row.delivered_ms = now;
                        row.count = row.count.saturating_add(1);
                        changed = true;
                    }
                    rows.push(match self.entries.get(&id) {
                        Some(fields) => entry_reply(id, fields),
                        None => Reply::Array(vec![id_reply(id), Reply::NilArray]),
                    });
                }
                (Reply::Array(rows), changed)
            }
        }
    }

    // ---- pending list ----

    fn pending(&self, key: &[u8], group: &[u8], detail: Option<PendingDetail>, now: u64) -> Reply {
        let Some(g) = self.groups.get(group) else {
            return nogroup_either(key, group);
        };
        let Some(detail) = detail else {
            let (Some((min, _)), Some((max, _))) =
                (g.pel.first_key_value(), g.pel.last_key_value())
            else {
                return Reply::Array(vec![Reply::Int(0), Reply::Nil, Reply::Nil, Reply::NilArray]);
            };
            let owners = g
                .consumers
                .iter()
                .filter(|(_, c)| !c.pending.is_empty())
                .map(|(name, c)| {
                    Reply::Array(vec![
                        Reply::bulk(name.as_slice()),
                        Reply::bulk(c.pending.len().to_string()),
                    ])
                })
                .collect();
            return Reply::Array(vec![
                int_reply(g.pel.len()),
                id_reply(*min),
                id_reply(*max),
                Reply::Array(owners),
            ]);
        };
        if detail.start > detail.end {
            return Reply::Array(Vec::new());
        }
        let row = |id: StreamId, p: &Pending| {
            Reply::Array(vec![
                id_reply(id),
                Reply::bulk(p.consumer.as_slice()),
                u64_reply(now.saturating_sub(p.delivered_ms)),
                u64_reply(p.count),
            ])
        };
        let idle_enough = |p: &Pending| now.saturating_sub(p.delivered_ms) >= detail.min_idle;
        let range = detail.start..=detail.end;
        let rows: Vec<Reply> = match &detail.consumer {
            None => g
                .pel
                .range(range)
                .filter(|(_, p)| idle_enough(p))
                .take(detail.count)
                .map(|(id, p)| row(*id, p))
                .collect(),
            Some(name) => match g.consumers.get(name) {
                None => Vec::new(),
                Some(c) => c
                    .pending
                    .range(range)
                    .filter_map(|id| g.pel.get(id).map(|p| (*id, p)))
                    .filter(|(_, p)| idle_enough(p))
                    .take(detail.count)
                    .map(|(id, p)| row(id, p))
                    .collect(),
            },
        };
        Reply::Array(rows)
    }

    fn claim(&self, g: &mut Group, a: &ClaimArgs, now: u64) -> (Reply, bool) {
        let mut changed = false;
        if let Some(last) = a.lastid {
            if last > g.last_id {
                g.last_id = last;
                changed = true;
            }
        }
        let wanted = match (a.time, a.idle) {
            (Some(t), _) => Some(i128::from(t)),
            (None, Some(idle)) => Some(i128::from(now) - i128::from(idle)),
            (None, None) => None,
        };
        let delivered = match wanted {
            Some(t) if (0..=i128::from(now)).contains(&t) => t as u64,
            _ => now,
        };
        let mut claimed = Vec::new();
        for &id in &a.ids {
            let exists = self.entries.contains_key(&id);
            let listed = g.pel.contains_key(&id);
            if !exists {
                // Redis 7 drops a PEL row whose entry is gone.
                changed |= g.remove_pending(id);
                continue;
            }
            let mut count = 1;
            if listed {
                let idle = now.saturating_sub(g.pel[&id].delivered_ms);
                if a.min_idle > 0 && idle < a.min_idle {
                    continue;
                }
                count = g.pel[&id].count;
            } else if !a.force {
                continue;
            }
            changed |= g.touch(&a.consumer, now);
            count = match a.retry {
                Some(retry) => retry,
                None if a.justid => count,
                None => count.saturating_add(1),
            };
            g.assign(id, &a.consumer, delivered, count);
            claimed.push(id);
        }
        if !claimed.is_empty() {
            g.activate(&a.consumer, now);
            changed = true;
        }
        let rows = claimed
            .iter()
            .filter_map(|id| {
                if a.justid {
                    Some(id_reply(*id))
                } else {
                    self.entries.get(id).map(|f| entry_reply(*id, f))
                }
            })
            .collect();
        (Reply::Array(rows), changed)
    }

    fn autoclaim(&self, g: &mut Group, a: &AutoClaimArgs, now: u64) -> (Reply, bool) {
        let mut changed = false;
        let mut attempts = a.count.saturating_mul(AUTOCLAIM_ATTEMPTS);
        let mut left = a.count;
        let mut cursor = Some(a.start);
        let mut rows = Vec::new();
        let mut deleted = Vec::new();
        while attempts > 0 && left > 0 {
            let Some(from) = cursor else { break };
            let Some((&id, _)) = g.pel.range(from..).next() else {
                cursor = None;
                break;
            };
            attempts -= 1;
            cursor = id.next();
            let Some(fields) = self.entries.get(&id) else {
                g.remove_pending(id);
                deleted.push(id_reply(id));
                changed = true;
                left -= 1;
                continue;
            };
            let (delivered, count) = {
                let row = &g.pel[&id];
                (row.delivered_ms, row.count)
            };
            if a.min_idle > 0 && now.saturating_sub(delivered) < a.min_idle {
                continue;
            }
            g.touch(&a.consumer, now);
            let count = if a.justid {
                count
            } else {
                count.saturating_add(1)
            };
            g.assign(id, &a.consumer, now, count);
            rows.push(if a.justid {
                id_reply(id)
            } else {
                entry_reply(id, fields)
            });
            changed = true;
            left -= 1;
        }
        if !rows.is_empty() {
            g.activate(&a.consumer, now);
        }
        let next = cursor
            .and_then(|from| g.pel.range(from..).next().map(|(id, _)| *id))
            .unwrap_or(StreamId::MIN);
        (
            Reply::Array(vec![
                id_reply(next),
                Reply::Array(rows),
                Reply::Array(deleted),
            ]),
            changed,
        )
    }

    // ---- XINFO ----

    fn group_info(&self, name: &[u8], g: &Group) -> Reply {
        map(vec![
            ("name", Reply::bulk(name)),
            ("consumers", int_reply(g.consumers.len())),
            ("pending", int_reply(g.pel.len())),
            ("last-delivered-id", id_reply(g.last_id)),
            ("entries-read", g.entries_read.map_or(Reply::Nil, u64_reply)),
            ("lag", self.lag(g)),
        ])
    }

    fn info_stream(&self, full: Option<usize>, now: u64) -> Reply {
        let keys = self.entries.len().div_ceil(NODE_ENTRIES);
        let first = self.entries.first_key_value();
        let mut pairs = vec![
            ("length", int_reply(self.entries.len())),
            ("radix-tree-keys", int_reply(keys)),
            ("radix-tree-nodes", int_reply(keys + 1)),
            ("last-generated-id", id_reply(self.last_id)),
            ("max-deleted-entry-id", id_reply(self.max_deleted_id)),
            ("entries-added", u64_reply(self.entries_added)),
            (
                "recorded-first-entry-id",
                id_reply(first.map(|(id, _)| *id).unwrap_or_default()),
            ),
        ];
        let Some(count) = full else {
            let entry = |e: Option<(&StreamId, &Fields)>| {
                e.map_or(Reply::Nil, |(id, fields)| entry_reply(*id, fields))
            };
            pairs.push(("groups", int_reply(self.groups.len())));
            pairs.push(("first-entry", entry(first)));
            pairs.push(("last-entry", entry(self.entries.last_key_value())));
            return map(pairs);
        };
        let limit = if count == 0 { usize::MAX } else { count };
        let entries = self
            .entries
            .iter()
            .take(limit)
            .map(|(id, fields)| entry_reply(*id, fields))
            .collect();
        pairs.push(("entries", Reply::Array(entries)));
        let groups = self
            .groups
            .iter()
            .map(|(name, g)| self.group_full(name, g, limit, now))
            .collect();
        pairs.push(("groups", Reply::Array(groups)));
        map(pairs)
    }

    fn group_full(&self, name: &[u8], g: &Group, limit: usize, now: u64) -> Reply {
        let pending = g
            .pel
            .iter()
            .take(limit)
            .map(|(id, p)| {
                Reply::Array(vec![
                    id_reply(*id),
                    Reply::bulk(p.consumer.as_slice()),
                    u64_reply(p.delivered_ms),
                    u64_reply(p.count),
                ])
            })
            .collect();
        let consumers = g
            .consumers
            .iter()
            .map(|(cname, c)| {
                let owned = c
                    .pending
                    .iter()
                    .take(limit)
                    .filter_map(|id| g.pel.get(id).map(|p| (id, p)))
                    .map(|(id, p)| {
                        Reply::Array(vec![
                            id_reply(*id),
                            u64_reply(p.delivered_ms),
                            u64_reply(p.count),
                        ])
                    })
                    .collect();
                map(vec![
                    ("name", Reply::bulk(cname.as_slice())),
                    ("seen-time", u64_reply(c.seen_ms)),
                    ("active-time", c.active_ms.map_or(Reply::Int(-1), u64_reply)),
                    ("pel-count", int_reply(c.pending.len())),
                    ("pending", Reply::Array(owned)),
                ])
            })
            .collect();
        let _ = now;
        map(vec![
            ("name", Reply::bulk(name)),
            ("last-delivered-id", id_reply(g.last_id)),
            ("entries-read", g.entries_read.map_or(Reply::Nil, u64_reply)),
            ("lag", self.lag(g)),
            ("pel-count", int_reply(g.pel.len())),
            ("pending", Reply::Array(pending)),
            ("consumers", Reply::Array(consumers)),
        ])
    }

    // ---- persistence ----

    /// Append a compact binary form of the whole stream to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_id(out, self.last_id);
        put_id(out, self.max_deleted_id);
        put_u64(out, self.entries_added);
        put_len(out, self.entries.len());
        for (id, fields) in &self.entries {
            put_id(out, *id);
            put_len(out, fields.len());
            for (field, value) in fields {
                put_bytes(out, field);
                put_bytes(out, value);
            }
        }
        put_len(out, self.groups.len());
        for (name, g) in &self.groups {
            put_bytes(out, name);
            put_id(out, g.last_id);
            out.push(u8::from(g.entries_read.is_some()));
            put_u64(out, g.entries_read.unwrap_or(0));
            put_len(out, g.pel.len());
            for (id, p) in &g.pel {
                put_id(out, *id);
                put_bytes(out, &p.consumer);
                put_u64(out, p.delivered_ms);
                put_u64(out, p.count);
            }
            put_len(out, g.consumers.len());
            for (cname, c) in &g.consumers {
                put_bytes(out, cname);
                put_u64(out, c.seen_ms);
                out.push(u8::from(c.active_ms.is_some()));
                put_u64(out, c.active_ms.unwrap_or(0));
            }
        }
    }

    /// Read a stream written by `encode`, consuming exactly its bytes.
    /// `None` (and `input` untouched) when the bytes are not a whole,
    /// consistent stream.
    pub fn decode(input: &mut &[u8]) -> Option<Stream> {
        let mut r = Reader(input);
        let stream = decode_stream(&mut r)?;
        *input = r.0;
        Some(stream)
    }
}

// ---- RDB ----
//
// A stream in RDB is a list of nodes, each a listpack of up to 100
// entries keyed by its first ID (big-endian, as in Redis's radix tree).
// A node starts with a header: live count, deleted count, the field names
// of the first entry, and 0. Each entry is `flags, ms and seq as the
// difference to the node's ID, [field count, fields and values | values
// alone when the names are the header's], number of pieces before this`.

/// Flags of an entry inside a node.
const RDB_DELETED: i64 = 1;
const RDB_SAME_FIELDS: i64 = 2;
/// A node is closed at this many bytes of fields and values, as Redis's
/// `stream-node-max-bytes`.
const RDB_NODE_BYTES: usize = 4096;
/// The `entries_read` of a group that does not know it.
const RDB_UNKNOWN_READ: u64 = u64::MAX;

fn rdb_id(id: StreamId) -> [u8; 16] {
    let mut raw = [0u8; 16];
    raw[..8].copy_from_slice(&id.ms.to_be_bytes());
    raw[8..].copy_from_slice(&id.seq.to_be_bytes());
    raw
}

fn read_rdb_id(raw: &[u8]) -> Option<StreamId> {
    let raw: &[u8; 16] = raw.try_into().ok()?;
    let (ms, seq) = raw.split_at(8);
    Some(StreamId {
        ms: u64::from_be_bytes(ms.try_into().ok()?),
        seq: u64::from_be_bytes(seq.try_into().ok()?),
    })
}

fn put_rdb_len_id(out: &mut Vec<u8>, id: StreamId) {
    rdb::put_len(out, id.ms);
    rdb::put_len(out, id.seq);
}

fn rdb_node(entries: &[(&StreamId, &Fields)]) -> Vec<u8> {
    let (master_id, master_fields) = entries[0];
    let mut lp = rdb::Listpack::new();
    lp.int(entries.len() as i64);
    lp.int(0);
    lp.int(master_fields.len() as i64);
    master_fields.iter().for_each(|(f, _)| lp.string(f));
    lp.int(0);
    for &(id, fields) in entries {
        let same = fields.len() == master_fields.len()
            && fields
                .iter()
                .zip(master_fields)
                .all(|((a, _), (b, _))| a == b);
        lp.int(if same { RDB_SAME_FIELDS } else { 0 });
        lp.int(id.ms.wrapping_sub(master_id.ms) as i64);
        lp.int(id.seq.wrapping_sub(master_id.seq) as i64);
        if !same {
            lp.int(fields.len() as i64);
        }
        for (f, v) in fields {
            if !same {
                lp.string(f);
            }
            lp.string(v);
        }
        let pieces = fields.len() + 3 + if same { 0 } else { fields.len() + 1 };
        lp.int(pieces as i64);
    }
    lp.finish()
}

/// The next count or flag of a listpack, where Redis writes integers.
fn lp_int(it: &mut std::vec::IntoIter<Vec<u8>>) -> Option<i64> {
    std::str::from_utf8(&it.next()?).ok()?.parse().ok()
}

/// The live entries of one node of an RDB stream.
fn read_rdb_node(
    master: StreamId,
    lp: &[u8],
    out: &mut BTreeMap<StreamId, Fields>,
    budget: &mut usize,
) -> Option<()> {
    let items = rdb::listpack(lp, budget)?;
    let mut it = items.into_iter();
    let live = lp_int(&mut it)?;
    let deleted = lp_int(&mut it)?;
    let n = usize::try_from(lp_int(&mut it)?).ok()?;
    if live < 0 || deleted < 0 || n > it.len() {
        return None;
    }
    let names: Vec<Bytes> = it.by_ref().take(n).collect();
    if names.len() != n || lp_int(&mut it)? != 0 {
        return None;
    }
    let (mut seen_live, mut seen_deleted) = (0, 0);
    while it.len() > 0 {
        let flags = lp_int(&mut it)?;
        // Differences are two's complement, as Redis's int64 arithmetic.
        let id = StreamId {
            ms: master.ms.wrapping_add(lp_int(&mut it)? as u64),
            seq: master.seq.wrapping_add(lp_int(&mut it)? as u64),
        };
        let mut fields = Vec::new();
        let count = if flags & RDB_SAME_FIELDS != 0 {
            for name in &names {
                // Every entry gets its own copy: charged, or a short
                // payload could have each of many entries copy a long name.
                rdb::charge(budget, name)?;
                fields.push((name.clone(), it.next()?));
            }
            n
        } else {
            let k = usize::try_from(lp_int(&mut it)?).ok()?;
            if k > it.len() {
                return None;
            }
            for _ in 0..k {
                fields.push((it.next()?, it.next()?));
            }
            k
        };
        let pieces = count
            + 3
            + if flags & RDB_SAME_FIELDS != 0 {
                0
            } else {
                count + 1
            };
        // An entry has a field, as XADD makes it.
        if lp_int(&mut it)? != pieces as i64 || fields.is_empty() {
            return None;
        }
        if flags & RDB_DELETED != 0 {
            seen_deleted += 1;
        } else {
            seen_live += 1;
            if out.insert(id, fields).is_some() {
                return None;
            }
        }
    }
    (seen_live == live && seen_deleted == deleted).then_some(())
}

impl Stream {
    /// The body of RDB type `TYPE_STREAM_LISTPACKS_3`, after its type byte.
    pub(crate) fn write_rdb(&self, out: &mut Vec<u8>) {
        let mut nodes = Vec::new();
        let mut node: Vec<(&StreamId, &Fields)> = Vec::new();
        let mut bytes = 0;
        for entry in &self.entries {
            node.push(entry);
            bytes += entry
                .1
                .iter()
                .map(|(f, v)| f.len() + v.len())
                .sum::<usize>();
            if node.len() == NODE_ENTRIES || bytes >= RDB_NODE_BYTES {
                nodes.push(std::mem::take(&mut node));
                bytes = 0;
            }
        }
        if !node.is_empty() {
            nodes.push(node);
        }
        rdb::put_len(out, nodes.len() as u64);
        for node in &nodes {
            rdb::put_raw(out, &rdb_id(*node[0].0));
            rdb::put_raw(out, &rdb_node(node));
        }
        rdb::put_len(out, self.entries.len() as u64);
        put_rdb_len_id(out, self.last_id);
        let first = self.entries.keys().next().copied().unwrap_or_default();
        put_rdb_len_id(out, first);
        put_rdb_len_id(out, self.max_deleted_id);
        rdb::put_len(out, self.entries_added);
        rdb::put_len(out, self.groups.len() as u64);
        for (name, g) in &self.groups {
            rdb::put_raw(out, name);
            put_rdb_len_id(out, g.last_id);
            rdb::put_len(out, g.entries_read.unwrap_or(RDB_UNKNOWN_READ));
            rdb::put_len(out, g.pel.len() as u64);
            for (id, p) in &g.pel {
                out.extend_from_slice(&rdb_id(*id));
                rdb::put_millis(out, i64::try_from(p.delivered_ms).unwrap_or(i64::MAX));
                rdb::put_len(out, p.count);
            }
            rdb::put_len(out, g.consumers.len() as u64);
            for (cname, c) in &g.consumers {
                rdb::put_raw(out, cname);
                rdb::put_millis(out, i64::try_from(c.seen_ms).unwrap_or(i64::MAX));
                let active = c.active_ms.and_then(|ms| i64::try_from(ms).ok());
                rdb::put_millis(out, active.unwrap_or(-1));
                rdb::put_len(out, c.pending.len() as u64);
                c.pending
                    .iter()
                    .for_each(|id| out.extend_from_slice(&rdb_id(*id)));
            }
        }
    }

    /// A stream of RDB type `t` (one of the three stream types), its type
    /// byte already read. `None` when it is malformed or inconsistent.
    pub(crate) fn read_rdb<I: rdb::Input + ?Sized>(r: &mut I, t: u8) -> Option<Stream> {
        let mut stream = Stream::default();
        for _ in 0..r.len()? {
            let master = read_rdb_id(&r.string()?)?;
            read_rdb_node(master, &r.string()?, &mut stream.entries, r.unpacked())?;
        }
        let length = r.len()?;
        let id = |r: &mut I| {
            Some(StreamId {
                ms: r.len()?,
                seq: r.len()?,
            })
        };
        stream.last_id = id(r)?;
        if t >= rdb::TYPE_STREAM_LISTPACKS_2 {
            id(r)?;
            stream.max_deleted_id = id(r)?;
            stream.entries_added = r.len()?;
        } else {
            stream.entries_added = length;
        }
        let entries = stream.entries.len() as u64;
        // The counters are trusted by arithmetic elsewhere.
        if length != entries
            || stream.entries_added < entries
            || stream
                .entries
                .keys()
                .next_back()
                .is_some_and(|&top| top > stream.last_id)
        {
            return None;
        }
        for _ in 0..r.len()? {
            let name = r.string()?;
            let last_id = id(r)?;
            let read = if t >= rdb::TYPE_STREAM_LISTPACKS_2 {
                Some(r.len()?).filter(|&n| n != RDB_UNKNOWN_READ)
            } else {
                None
            };
            let mut g = Group::new(last_id, read);
            let mut rows = BTreeMap::new();
            for _ in 0..r.len()? {
                let pid = read_rdb_id(r.take(16)?)?;
                let delivered_ms = u64::try_from(r.millis()?).ok()?;
                let count = r.len()?;
                if rows.insert(pid, (delivered_ms, count)).is_some() {
                    return None;
                }
            }
            for _ in 0..r.len()? {
                let cname = r.string()?;
                let seen_ms = u64::try_from(r.millis()?).ok()?;
                let active_ms = if t >= rdb::TYPE_STREAM_LISTPACKS_3 {
                    match r.millis()? {
                        -1 => None,
                        ms => Some(u64::try_from(ms).ok()?),
                    }
                } else {
                    Some(seen_ms)
                };
                let mut consumer = Consumer {
                    seen_ms,
                    active_ms,
                    pending: BTreeSet::new(),
                };
                for _ in 0..r.len()? {
                    let pid = read_rdb_id(r.take(16)?)?;
                    // Each row belongs to exactly one consumer.
                    let (delivered_ms, count) = rows.remove(&pid)?;
                    consumer.pending.insert(pid);
                    let row = Pending {
                        consumer: cname.clone(),
                        delivered_ms,
                        count,
                    };
                    g.pel.insert(pid, row);
                }
                if g.consumers.insert(cname, consumer).is_some() {
                    return None;
                }
            }
            if !rows.is_empty() || stream.groups.insert(name, g).is_some() {
                return None;
            }
        }
        Some(stream)
    }
}

fn put_len(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&(n as u32).to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, n: u64) {
    out.extend_from_slice(&n.to_le_bytes());
}

fn put_id(out: &mut Vec<u8>, id: StreamId) {
    put_u64(out, id.ms);
    put_u64(out, id.seq);
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_len(out, b.len());
    out.extend_from_slice(b);
}

/// A cursor over the input that never reads past its end.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if n > self.0.len() {
            return None;
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn id(&mut self) -> Option<StreamId> {
        Some(StreamId {
            ms: self.u64()?,
            seq: self.u64()?,
        })
    }

    fn bytes(&mut self) -> Option<Bytes> {
        let n = self.u32()? as usize;
        self.take(n).map(<[u8]>::to_vec)
    }

    /// An item count, refused when the items could not fit in what is left
    /// at `min_item` bytes each.
    fn count(&mut self, min_item: usize) -> Option<usize> {
        let n = self.u32()? as usize;
        (n <= self.0.len() / min_item).then_some(n)
    }

    /// A flag byte followed by a value that is only meaningful when set.
    fn optional(&mut self) -> Option<Option<u64>> {
        let flag = self.u8()?;
        let value = self.u64()?;
        match flag {
            0 => Some(None),
            1 => Some(Some(value)),
            _ => None,
        }
    }
}

fn decode_stream(r: &mut Reader<'_>) -> Option<Stream> {
    let mut stream = Stream {
        last_id: r.id()?,
        max_deleted_id: r.id()?,
        entries_added: r.u64()?,
        ..Stream::default()
    };
    for _ in 0..r.count(20)? {
        let id = r.id()?;
        let mut fields = Vec::new();
        for _ in 0..r.count(8)? {
            fields.push((r.bytes()?, r.bytes()?));
        }
        if id > stream.last_id || stream.entries.insert(id, fields).is_some() {
            return None;
        }
    }
    // The counters are trusted by arithmetic elsewhere.
    if stream.entries_added < stream.entries.len() as u64 {
        return None;
    }
    for _ in 0..r.count(37)? {
        let name = r.bytes()?;
        let mut g = Group::new(r.id()?, r.optional()?);
        let mut rows = Vec::new();
        for _ in 0..r.count(36)? {
            rows.push((r.id()?, r.bytes()?, r.u64()?, r.u64()?));
        }
        for _ in 0..r.count(21)? {
            let consumer = Consumer {
                seen_ms: 0,
                active_ms: None,
                pending: BTreeSet::new(),
            };
            let cname = r.bytes()?;
            let seen_ms = r.u64()?;
            let active_ms = r.optional()?;
            let consumer = Consumer {
                seen_ms,
                active_ms,
                ..consumer
            };
            if g.consumers.insert(cname, consumer).is_some() {
                return None;
            }
        }
        for (id, owner, delivered_ms, count) in rows {
            let consumer = g.consumers.get_mut(&owner)?;
            consumer.pending.insert(id);
            let row = Pending {
                consumer: owner,
                delivered_ms,
                count,
            };
            if g.pel.insert(id, row).is_some() {
                return None;
            }
        }
        if stream.groups.insert(name, g).is_some() {
            return None;
        }
    }
    Some(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_copy_of_a_shared_field_name_is_charged() {
        // Entries that share their node's field names hold none of their
        // own in the payload, but each gets its own copy when read.
        let name = vec![b'n'; 1000];
        let mut stream = Stream::default();
        for seq in 1..=50 {
            let fields = vec![(name.clone(), b"v".to_vec())];
            stream.entries.insert(StreamId { ms: 1, seq }, fields);
        }
        stream.last_id = StreamId { ms: 1, seq: 50 };
        stream.entries_added = 50;
        let mut out = Vec::new();
        stream.write_rdb(&mut out);
        let read = |budget| {
            let mut r = rdb::Reader::with_budget(&out, budget);
            Stream::read_rdb(&mut r, rdb::TYPE_STREAM_LISTPACKS_3)
        };
        assert!(read(usize::MAX).is_some());
        // The copies alone cost this much; everything else comes on top.
        assert!(read(50 * (48 + name.len())).is_none());
        assert!(
            Stream::read_rdb(&mut rdb::Reader::new(&out), rdb::TYPE_STREAM_LISTPACKS_3).is_some()
        );
    }

    /// A one-key database: runs a command line the way the engine would.
    #[derive(Default)]
    struct Db {
        stream: Option<Stream>,
        now: u64,
    }

    impl Db {
        fn at(now: u64) -> Db {
            Db { stream: None, now }
        }

        fn run(&mut self, line: &str) -> Reply {
            let args: Vec<Bytes> = line
                .split_whitespace()
                .map(|w| w.as_bytes().to_vec())
                .collect();
            let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
            let (key, op) = match parse(&name, &args) {
                Ok(parsed) => parsed,
                Err(e) => return e,
            };
            // Only the key "s" ever exists; any other name is a missing key.
            match self.stream.as_mut() {
                Some(s) if key == b"s" => s.apply(&key, op, self.now).0,
                None if op.creates() => {
                    let mut s = Stream::default();
                    let (reply, changed) = s.apply(&key, op, self.now);
                    if changed || s.last_id() != StreamId::MIN {
                        self.stream = Some(s);
                    }
                    reply
                }
                _ => op.missing(&key),
            }
        }

        fn s(&mut self, line: &str) -> String {
            show(&self.run(line))
        }

        fn stream(&self) -> &Stream {
            self.stream.as_ref().unwrap()
        }
    }

    fn show(r: &Reply) -> String {
        match r {
            Reply::Ok => "OK".into(),
            Reply::Simple(s) => s.clone(),
            Reply::Error(e) => e.clone(),
            Reply::Int(n) => n.to_string(),
            Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            Reply::Nil => "nil".into(),
            Reply::NilArray => "nilarray".into(),
            Reply::Array(items) | Reply::Map(items) => {
                let inner: Vec<String> = items.iter().map(show).collect();
                format!("[{}]", inner.join(" "))
            }
        }
    }

    const ERR_BAD_ID: &str = "ERR Invalid stream ID specified as stream command argument";
    const ERR_SMALL: &str =
        "ERR The ID specified in XADD is equal or smaller than the target stream top item";
    const MAX_ID: &str = "18446744073709551615-18446744073709551615";

    #[test]
    fn id_parsing_edge_cases() {
        let mut db = Db::at(1000);
        for bad in [
            "18446744073709551616-0",
            "0-18446744073709551616",
            "-",
            "+",
            "+5",
            "5-",
            "-5",
            "5-5-5",
            "abc",
            "5-x",
            "5-+5",
            "5.5",
        ] {
            assert_eq!(db.s(&format!("XADD s {bad} f v")), ERR_BAD_ID, "id {bad}");
        }
        assert_eq!(
            db.s("XADD s 0-0 f v"),
            "ERR The ID specified in XADD must be greater than 0-0"
        );
        assert_eq!(db.s("XADD s 5 f v"), "5-0");
        assert_eq!(db.s("XADD s 5-1 f v"), "5-1");
        assert_eq!(db.s("XADD s 5-1 f v"), ERR_SMALL);
        assert_eq!(db.s("XADD s 5-0 f v"), ERR_SMALL);
        // Leading zeros are plain digits.
        assert_eq!(db.s("XADD s 007-0 f v"), "7-0");
        let long = "1".repeat(128);
        assert_eq!(db.s(&format!("XADD s {long} f v")), ERR_BAD_ID);
        assert_eq!(
            db.s("XADD s 18446744073709551615-18446744073709551615 f v"),
            MAX_ID
        );
    }

    #[test]
    fn incomplete_ids_in_ranges() {
        let mut db = Db::at(1);
        for id in ["4-0", "5-0", "5-7", "5-18446744073709551615", "6-0"] {
            db.run(&format!("XADD s {id} f v"));
        }
        // "5" as a start is 5-0, as an end is 5-max.
        assert_eq!(
            db.s("XRANGE s 5 5"),
            "[[5-0 [f v]] [5-7 [f v]] [5-18446744073709551615 [f v]]]"
        );
        assert_eq!(
            db.s("XREVRANGE s 5 5"),
            "[[5-18446744073709551615 [f v]] [5-7 [f v]] [5-0 [f v]]]"
        );
        // "(5" is after 5-0, so the rest of the 5-x entries still count.
        assert_eq!(
            db.s("XRANGE s (5 6"),
            "[[5-7 [f v]] [5-18446744073709551615 [f v]] [6-0 [f v]]]"
        );
        assert_eq!(db.s("XRANGE s - (5-0"), "[[4-0 [f v]]]");
        // As an end "(5" is 5-max, exclusive: the 5-x entries below it stay.
        assert_eq!(
            db.s("XRANGE s - (5"),
            "[[4-0 [f v]] [5-0 [f v]] [5-7 [f v]]]"
        );
        assert_eq!(db.s("XRANGE s 5-7 5-7"), "[[5-7 [f v]]]");
        assert_eq!(
            db.s("XRANGE s (5-7 (6-0"),
            "[[5-18446744073709551615 [f v]]]"
        );
        assert_eq!(db.s("XRANGE s 6-1 +"), "[]");
        assert_eq!(db.s("XRANGE s + -"), "[]");
        assert_eq!(db.s("XRANGE s - + COUNT 2"), "[[4-0 [f v]] [5-0 [f v]]]");
        assert_eq!(db.s("XREVRANGE s + - COUNT 1"), "[[6-0 [f v]]]");
        assert_eq!(db.s("XRANGE s - + COUNT 0"), "[]");
        assert_eq!(db.s("XRANGE s - + COUNT -5"), "[]");
        assert_eq!(db.s("XRANGE s - + COUNT x"), NOT_INT);
        assert_eq!(db.s("XRANGE s - + COUNT"), "ERR syntax error");
        assert_eq!(db.s("XRANGE s - + FOO 1"), "ERR syntax error");
        assert_eq!(db.s("XRANGE s - abc"), ERR_BAD_ID);
        assert_eq!(
            db.s("XRANGE s - "),
            "ERR wrong number of arguments for 'xrange' command"
        );
    }

    #[test]
    fn exclusive_bounds_at_the_ends_of_the_id_space() {
        let mut db = Db::at(1);
        db.run("XADD s 1-1 f v");
        assert_eq!(
            db.s(&format!("XRANGE s ({MAX_ID} +")),
            "ERR invalid start ID for the interval"
        );
        assert_eq!(
            db.s("XRANGE s - (0-0"),
            "ERR invalid end ID for the interval"
        );
        // XREVRANGE takes the end first, so the errors swap sides.
        assert_eq!(
            db.s("XREVRANGE s (0-0 -"),
            "ERR invalid end ID for the interval"
        );
        assert_eq!(
            db.s(&format!("XREVRANGE s + ({MAX_ID}")),
            "ERR invalid start ID for the interval"
        );
        assert_eq!(db.s(&format!("XREVRANGE s ({MAX_ID} -")), "[[1-1 [f v]]]");
        assert_eq!(db.s("XREVRANGE s + (0-0"), "[[1-1 [f v]]]");
        assert_eq!(db.s("XRANGE s (0-0 +"), "[[1-1 [f v]]]");
        assert_eq!(db.s(&format!("XRANGE s - ({MAX_ID}")), "[[1-1 [f v]]]");
        // Carry across the sequence boundary.
        assert_eq!(db.s("XRANGE s (0-18446744073709551615 +"), "[[1-1 [f v]]]");
        assert_eq!(db.s("XRANGE s - (1-0"), "[]");
        assert_eq!(db.s("XRANGE s (1-1 +"), "[]");
        assert_eq!(db.s("XRANGE s - (1-1"), "[]");
        assert_eq!(db.s("XRANGE s - (1-2"), "[[1-1 [f v]]]");
        // A missing key reads as empty, even with bad bounds checked first.
        assert_eq!(Db::at(1).s("XRANGE nokey - +"), "[]");
        assert_eq!(
            Db::at(1).s(&format!("XRANGE nokey ({MAX_ID} +")),
            "ERR invalid start ID for the interval"
        );
    }

    #[test]
    fn auto_ids_stay_monotonic() {
        let mut db = Db::at(1000);
        assert_eq!(db.s("XADD s * f v"), "1000-0");
        assert_eq!(db.s("XADD s * f v"), "1000-1");
        db.now = 999;
        assert_eq!(db.s("XADD s * f v"), "1000-2");
        db.now = 1001;
        assert_eq!(db.s("XADD s * f v"), "1001-0");
        assert_eq!(db.s("XADD s 1001-* f v"), "1001-1");
        assert_eq!(db.s("XADD s 1002-* f v"), "1002-0");
        assert_eq!(db.s("XADD s 1001-* f v"), ERR_SMALL);
        // At the end of a millisecond's sequence numbers the next ms starts.
        assert_eq!(
            db.s("XADD s 1003-18446744073709551615 f v"),
            "1003-18446744073709551615"
        );
        db.now = 1;
        assert_eq!(db.s("XADD s * f v"), "1004-0");
        assert_eq!(db.s("XADD s 1004-* f v"), "1004-1");
    }

    #[test]
    fn zero_ms_with_auto_sequence_starts_at_one() {
        let mut db = Db::at(5);
        assert_eq!(db.s("XADD s 0-* f v"), "0-1");
        assert_eq!(db.s("XADD s 0-* f v"), "0-2");
    }

    #[test]
    fn id_space_exhaustion() {
        let mut db = Db::at(1000);
        assert_eq!(db.s(&format!("XSETID s {MAX_ID}")), "ERR no such key");
        db.run("XADD s 1-1 f v");
        db.run("XDEL s 1-1");
        assert_eq!(db.s(&format!("XSETID s {MAX_ID}")), "OK");
        let exhausted =
            "ERR The stream has exhausted the last possible ID, unable to add more items";
        assert_eq!(db.s("XADD s * f v"), exhausted);
        assert_eq!(db.s(&format!("XADD s {MAX_ID} f v")), ERR_SMALL);
        assert_eq!(db.s("XADD s 18446744073709551615-* f v"), ERR_SMALL);
        // Last id (5, max): the auto id rolls to the next ms.
        let mut db = Db::at(5);
        db.run("XADD s 5-18446744073709551615 f v");
        assert_eq!(db.s("XADD s * f v"), "6-0");
        // ms-* with a maxed sequence is refused rather than wrapped.
        let mut db = Db::at(1);
        db.run("XADD s 9-18446744073709551615 f v");
        assert_eq!(db.s("XADD s 9-* f v"), ERR_SMALL);
        assert_eq!(db.stream().entries(), 1);
    }

    #[test]
    fn xadd_arguments_and_errors() {
        let mut db = Db::at(1);
        assert_eq!(
            db.s("XADD s * f"),
            "ERR wrong number of arguments for 'xadd' command"
        );
        assert_eq!(
            db.s("XADD s * f v g"),
            "ERR wrong number of arguments for 'xadd' command"
        );
        assert_eq!(
            db.s("XADD s"),
            "ERR wrong number of arguments for 'xadd' command"
        );
        assert_eq!(db.s("XADD s MAXLEN 5 f v"), ERR_BAD_ID);
        assert_eq!(
            db.s("XADD s MAXLEN 1 MINID 0 * f v"),
            "ERR syntax error, MAXLEN and MINID options at the same time are not compatible"
        );
        assert_eq!(
            db.s("XADD s MAXLEN -1 * f v"),
            "ERR The MAXLEN argument must be >= 0."
        );
        assert_eq!(db.s("XADD s MAXLEN x * f v"), NOT_INT);
        assert_eq!(
            db.s("XADD s MAXLEN = 5 LIMIT 5 * f v"),
            "ERR syntax error, LIMIT cannot be used without the special ~ option"
        );
        assert_eq!(
            db.s("XADD s LIMIT 5 * f v"),
            "ERR syntax error, LIMIT cannot be used without the special ~ option"
        );
        assert_eq!(
            db.s("XADD s MAXLEN ~ 5 LIMIT -1 * f v"),
            "ERR The LIMIT argument must be >= 0."
        );
        assert_eq!(db.s("XADD s MINID abc * f v"), ERR_BAD_ID);
        assert!(db.stream.is_none());
        assert_eq!(db.s("XADD s NOMKSTREAM * f v"), "nil");
        assert!(db.stream.is_none());
        assert_eq!(db.s("XADD s 1-1 f v"), "1-1");
        assert_eq!(db.s("XADD s NOMKSTREAM 1-2 f v"), "1-2");
        assert_eq!(db.s("XADD s nomkstream maxlen ~ 10 2-1 a 1 b 2"), "2-1");
        assert_eq!(db.s("XRANGE s 2 2"), "[[2-1 [a 1 b 2]]]");
        assert_eq!(db.s("XLEN s"), "3");
        assert_eq!(
            db.s("XLEN s extra"),
            "ERR wrong number of arguments for 'xlen' command"
        );
        assert_eq!(db.s("XLEN nokey"), "0");
    }

    #[test]
    fn trimming_exact_and_approximate() {
        let mut db = Db::at(1);
        for i in 1..=250 {
            db.run(&format!("XADD s {i}-0 f v"));
        }
        assert_eq!(db.s("XTRIM s MAXLEN 250"), "0");
        // Approximate: whole nodes of 100 only.
        assert_eq!(db.s("XTRIM s MAXLEN ~ 240"), "0");
        assert_eq!(db.s("XTRIM s MAXLEN ~ 150"), "100");
        assert_eq!(db.stream().entries(), 150);
        assert_eq!(db.s("XRANGE s - + COUNT 1"), "[[101-0 [f v]]]");
        assert_eq!(db.s("XTRIM s MAXLEN = 120"), "30");
        assert_eq!(db.s("XRANGE s - + COUNT 1"), "[[131-0 [f v]]]");
        assert_eq!(db.s("XTRIM s MAXLEN 0"), "120");
        assert_eq!(db.stream().entries(), 0);
        assert!(!db.stream().is_empty());
        assert_eq!(db.stream().len(), 1);
        // The stream remembers its top id.
        assert_eq!(db.s("XADD s 250-0 f v"), ERR_SMALL);
        assert_eq!(db.s("XADD s 250-1 f v"), "250-1");
        assert_eq!(
            db.s("XTRIM s MAXLEN -1"),
            "ERR The MAXLEN argument must be >= 0."
        );
        assert_eq!(
            db.s("XTRIM s MAXLEN"),
            "ERR wrong number of arguments for 'xtrim' command"
        );
        assert_eq!(db.s("XTRIM s FOO 1"), "ERR syntax error");
        assert_eq!(db.s("XTRIM s MAXLEN 1 extra"), "ERR syntax error");
        assert_eq!(db.s("XTRIM s LIMIT 5 5"), "ERR syntax error");
        assert_eq!(
            db.s("XTRIM s MAXLEN 1 LIMIT 5"),
            "ERR syntax error, LIMIT cannot be used without the special ~ option"
        );
        assert_eq!(db.s("XTRIM nokey MAXLEN 1"), "0");
    }

    #[test]
    fn trim_minid_and_limit() {
        let mut db = Db::at(1);
        for i in 1..=1000 {
            db.run(&format!("XADD s {i}-0 f v"));
        }
        assert_eq!(db.s("XTRIM s MINID 11"), "10");
        assert_eq!(db.s("XTRIM s MINID 11-0"), "0");
        assert_eq!(db.s("XTRIM s MINID 0"), "0");
        // 990 left; entries below 600: 589. Nodes only: 500 go.
        assert_eq!(db.s("XTRIM s MINID ~ 600"), "500");
        assert_eq!(db.s("XRANGE s - + COUNT 1"), "[[511-0 [f v]]]");
        // LIMIT caps an approximate trim, in whole nodes.
        assert_eq!(db.s("XTRIM s MAXLEN ~ 0 LIMIT 250"), "200");
        assert_eq!(db.stream().entries(), 290);
        // LIMIT 0 means no limit.
        assert_eq!(db.s("XTRIM s MAXLEN ~ 10 LIMIT 0"), "200");
        assert_eq!(db.stream().entries(), 90);
        assert_eq!(db.s("XTRIM s MINID ~ 10000"), "0");
        assert_eq!(db.s("XTRIM s MINID 10000"), "90");
        assert_eq!(db.s("XTRIM s MINID abc"), ERR_BAD_ID);
        // XADD trims after adding.
        let mut db = Db::at(1);
        for i in 1..=5 {
            db.run(&format!("XADD s MAXLEN 3 {i}-0 f v"));
        }
        assert_eq!(
            db.s("XRANGE s - +"),
            "[[3-0 [f v]] [4-0 [f v]] [5-0 [f v]]]"
        );
        db.run("XADD s MINID 5 6-0 f v");
        assert_eq!(db.s("XRANGE s - +"), "[[5-0 [f v]] [6-0 [f v]]]");
    }

    #[test]
    fn xdel_then_xadd_keeps_last_id() {
        let mut db = Db::at(1);
        db.run("XADD s 1-1 f v");
        db.run("XADD s 2-2 f v");
        assert_eq!(db.s("XDEL s 2-2"), "1");
        assert_eq!(db.s("XDEL s 2-2"), "0");
        assert_eq!(db.s("XADD s 2-2 f v"), ERR_SMALL);
        assert_eq!(db.s("XADD s 2-1 f v"), ERR_SMALL);
        assert_eq!(db.s("XADD s 2-3 f v"), "2-3");
        assert_eq!(db.s("XDEL s 1-1 2-3 9-9"), "2");
        assert_eq!(db.stream().entries(), 0);
        assert_eq!(db.s("XADD s * f v"), "2-4");
        assert_eq!(db.s("XDEL s 1-1 bad"), ERR_BAD_ID);
        assert_eq!(
            db.s("XDEL s"),
            "ERR wrong number of arguments for 'xdel' command"
        );
        assert_eq!(db.s("XDEL nokey 1-1"), "0");
        // max-deleted-entry-id follows the largest deleted id.
        let info = db.s("XINFO STREAM s");
        assert!(info.contains("max-deleted-entry-id 2-3"), "{info}");
        assert!(info.contains("entries-added 4"), "{info}");
    }

    #[test]
    fn xsetid_rules() {
        let mut db = Db::at(1);
        db.run("XADD s 5-0 f v");
        db.run("XADD s 6-0 f v");
        assert_eq!(
            db.s("XSETID s 5-9"),
            "ERR The ID specified in XSETID is smaller than the target stream top item"
        );
        assert_eq!(db.s("XSETID s 6-0"), "OK");
        assert_eq!(db.s("XSETID s 7-0 ENTRIESADDED 2 MAXDELETEDID 3-0"), "OK");
        assert!(db.s("XINFO STREAM s").contains("entries-added 2"));
        assert_eq!(
            db.s("XSETID s 7-0 ENTRIESADDED 1"),
            "ERR The entries_added specified in XSETID is smaller than the target stream length"
        );
        assert_eq!(
            db.s("XSETID s 7-0 MAXDELETEDID 8-0"),
            "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id"
        );
        assert_eq!(
            db.s("XSETID s 7-0 ENTRIESADDED -1"),
            "ERR entries_added must be positive"
        );
        assert_eq!(db.s("XSETID s 7-0 FOO 1"), "ERR syntax error");
        assert_eq!(db.s("XSETID s bad"), ERR_BAD_ID);
        assert_eq!(
            db.s("XSETID s"),
            "ERR wrong number of arguments for 'xsetid' command"
        );
        assert_eq!(db.s("XADD s 7-0 f v"), ERR_SMALL);
        assert_eq!(db.s("XADD s 7-1 f v"), "7-1");
    }

    const NO_GROUP_KEY: &str = "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.";

    #[test]
    fn group_lifecycle() {
        let mut db = Db::at(1000);
        assert_eq!(db.s("XGROUP CREATE s g $"), NO_GROUP_KEY);
        assert_eq!(db.s("XGROUP DESTROY s g"), NO_GROUP_KEY);
        assert_eq!(db.s("XGROUP CREATECONSUMER s g c"), NO_GROUP_KEY);
        assert_eq!(db.s("XGROUP CREATE s g $ MKSTREAM"), "OK");
        assert_eq!(db.stream().entries(), 0);
        assert_eq!(
            db.s("XGROUP CREATE s g $"),
            "BUSYGROUP Consumer Group name already exists"
        );
        for i in 1..=5 {
            db.run(&format!("XADD s {i}-0 f {i}"));
        }
        // Created at $: sees nothing old.
        assert_eq!(db.s("XGROUP CREATE s late $"), "OK");
        assert_eq!(db.s("XGROUP CREATE s all 0"), "OK");
        let rg = |db: &mut Db, group: &str, consumer: &str, id: GroupId, count| {
            let op = XOp::ReadGroup {
                group: group.into(),
                consumer: consumer.into(),
                id,
                count,
                noack: false,
            };
            let now = db.now;
            let (reply, _) = db.stream.as_mut().unwrap().apply(b"s", op, now);
            show(&reply)
        };
        assert_eq!(rg(&mut db, "late", "c", GroupId::New, None), "nil");
        assert_eq!(
            rg(&mut db, "g", "c1", GroupId::New, Some(2)),
            "[[1-0 [f 1]] [2-0 [f 2]]]"
        );
        assert_eq!(
            rg(&mut db, "g", "c2", GroupId::New, Some(1)),
            "[[3-0 [f 3]]]"
        );
        assert_eq!(db.s("XPENDING s g"), "[3 1-0 3-0 [[c1 2] [c2 1]]]");
        // History of a consumer: after the given id, only its own entries.
        assert_eq!(
            rg(&mut db, "g", "c1", GroupId::Id(StreamId::MIN), None),
            "[[1-0 [f 1]] [2-0 [f 2]]]"
        );
        assert_eq!(
            rg(
                &mut db,
                "g",
                "c1",
                GroupId::Id(StreamId { ms: 1, seq: 0 }),
                None
            ),
            "[[2-0 [f 2]]]"
        );
        assert_eq!(
            rg(
                &mut db,
                "g",
                "c1",
                GroupId::Id(StreamId { ms: 2, seq: 0 }),
                None
            ),
            "[]"
        );
        assert_eq!(
            rg(&mut db, "g", "ghost", GroupId::Id(StreamId::MIN), None),
            "[]"
        );
        assert!(db.s("XINFO CONSUMERS s g").contains("name ghost"));
        // History bumped the delivery count of 1-0 and 2-0 (to 2 and 3 total).
        assert_eq!(
            db.s("XPENDING s g - + 10 c1"),
            "[[1-0 c1 0 2] [2-0 c1 0 3]]"
        );
        assert_eq!(db.s("XACK s g 1-0 9-9 bad"), ERR_BAD_ID);
        assert_eq!(db.s("XACK s g 1-0 9-9 1-0"), "1");
        assert_eq!(db.s("XACK s nogroup 1-0"), "0");
        assert_eq!(db.s("XACK nokey g 1-0"), "0");
        assert_eq!(db.s("XPENDING s g"), "[2 2-0 3-0 [[c1 1] [c2 1]]]");
        // Consumer management.
        assert_eq!(db.s("XGROUP CREATECONSUMER s g c1"), "0");
        assert_eq!(db.s("XGROUP CREATECONSUMER s g c3"), "1");
        assert_eq!(db.s("XGROUP DELCONSUMER s g c1"), "1");
        assert_eq!(db.s("XGROUP DELCONSUMER s g c1"), "0");
        assert_eq!(db.s("XPENDING s g"), "[1 3-0 3-0 [[c2 1]]]");
        assert_eq!(
            db.s("XGROUP DELCONSUMER s nog c1"),
            "NOGROUP No such consumer group 'nog' for key name 's'"
        );
        // SETID rewinds delivery.
        assert_eq!(db.s("XGROUP SETID s g 0"), "OK");
        assert_eq!(
            rg(&mut db, "g", "c2", GroupId::New, Some(1)),
            "[[1-0 [f 1]]]"
        );
        assert_eq!(db.s("XGROUP SETID s g $"), "OK");
        assert_eq!(rg(&mut db, "g", "c2", GroupId::New, None), "nil");
        assert_eq!(
            db.s("XGROUP SETID s nog $"),
            "NOGROUP No such consumer group 'nog' for key name 's'"
        );
        assert_eq!(db.s("XGROUP DESTROY s g"), "1");
        assert_eq!(db.s("XGROUP DESTROY s g"), "0");
        assert_eq!(
            rg(&mut db, "g", "c2", GroupId::New, None),
            "NOGROUP No such key 's' or consumer group 'g' in XREADGROUP with GROUP option"
        );
        assert_eq!(
            db.s("XPENDING s g"),
            "NOGROUP No such key 's' or consumer group 'g'"
        );
    }

    #[test]
    fn xgroup_argument_errors() {
        let mut db = Db::at(1);
        db.run("XADD s 1-0 f v");
        assert_eq!(
            db.s("XGROUP"),
            "ERR wrong number of arguments for 'xgroup' command"
        );
        assert_eq!(
            db.s("XGROUP CREATE s g"),
            "ERR wrong number of arguments for 'xgroup|create' command"
        );
        assert_eq!(
            db.s("XGROUP DESTROY s"),
            "ERR wrong number of arguments for 'xgroup|destroy' command"
        );
        assert_eq!(
            db.s("XGROUP DELCONSUMER s g"),
            "ERR wrong number of arguments for 'xgroup|delconsumer' command"
        );
        assert_eq!(
            db.s("XGROUP FOO s g"),
            "ERR unknown subcommand 'FOO'. Try XGROUP HELP."
        );
        assert_eq!(db.s("XGROUP CREATE s g abc"), ERR_BAD_ID);
        assert_eq!(db.s("XGROUP CREATE s g 0 FOO"), "ERR syntax error");
        assert_eq!(
            db.s("XGROUP CREATE s g 0 ENTRIESREAD -2"),
            "ERR value for ENTRIESREAD must be positive or -1"
        );
        assert_eq!(db.s("XGROUP CREATE s g 0 ENTRIESREAD x"), NOT_INT);
        assert_eq!(db.s("XGROUP CREATE s g 0 ENTRIESREAD -1"), "OK");
        assert_eq!(db.s("XGROUP CREATE s g2 0 ENTRIESREAD 1"), "OK");
        assert_eq!(db.s("XGROUP SETID s g 0 MKSTREAM"), "ERR syntax error");
        // A parse error wins over a missing key.
        assert_eq!(Db::at(1).s("XGROUP CREATE nokey g abc"), ERR_BAD_ID);
        assert_eq!(
            db.s("XINFO"),
            "ERR wrong number of arguments for 'xinfo' command"
        );
        assert_eq!(
            db.s("XINFO FOO s"),
            "ERR unknown subcommand 'FOO'. Try XINFO HELP."
        );
        assert_eq!(
            db.s("XINFO GROUPS"),
            "ERR wrong number of arguments for 'xinfo|groups' command"
        );
        assert_eq!(db.s("XINFO STREAM s FOO"), "ERR syntax error");
        assert_eq!(db.s("XINFO STREAM s FULL COUNT"), "ERR syntax error");
        assert_eq!(db.s("XINFO STREAM nokey"), "ERR no such key");
        assert_eq!(db.s("XINFO GROUPS nokey"), "ERR no such key");
        assert_eq!(db.s("XINFO CONSUMERS nokey g"), "ERR no such key");
        assert_eq!(
            db.s("XINFO CONSUMERS s nog"),
            "NOGROUP No such consumer group 'nog' for key name 's'"
        );
    }

    #[test]
    fn deleted_entries_stay_in_the_pel_until_claimed() {
        let mut db = Db::at(1000);
        for i in 1..=3 {
            db.run(&format!("XADD s {i}-0 f {i}"));
        }
        db.run("XGROUP CREATE s g 0");
        let read = |db: &mut Db| {
            let op = XOp::ReadGroup {
                group: b"g".to_vec(),
                consumer: b"c1".to_vec(),
                id: GroupId::New,
                count: None,
                noack: false,
            };
            let now = db.now;
            show(&db.stream.as_mut().unwrap().apply(b"s", op, now).0)
        };
        assert_eq!(read(&mut db), "[[1-0 [f 1]] [2-0 [f 2]] [3-0 [f 3]]]");
        db.run("XDEL s 2-0");
        // The PEL still lists it; history shows the hole.
        assert_eq!(db.s("XPENDING s g"), "[3 1-0 3-0 [[c1 3]]]");
        let op = XOp::ReadGroup {
            group: b"g".to_vec(),
            consumer: b"c1".to_vec(),
            id: GroupId::Id(StreamId::MIN),
            count: None,
            noack: false,
        };
        let history = show(&db.stream.as_mut().unwrap().apply(b"s", op, 1000).0);
        assert_eq!(history, "[[1-0 [f 1]] [2-0 nilarray] [3-0 [f 3]]]");
        // XCLAIM drops the dead row and does not return it.
        db.now = 2000;
        assert_eq!(
            db.s("XCLAIM s g c2 0 1-0 2-0 3-0"),
            "[[1-0 [f 1]] [3-0 [f 3]]]"
        );
        assert_eq!(db.s("XPENDING s g"), "[2 1-0 3-0 [[c2 2]]]");
        // XAUTOCLAIM reports deleted ids as the third element.
        let mut db = Db::at(1000);
        for i in 1..=3 {
            db.run(&format!("XADD s {i}-0 f {i}"));
        }
        db.run("XGROUP CREATE s g 0");
        read(&mut db);
        db.run("XDEL s 1-0 3-0");
        db.now = 5000;
        assert_eq!(
            db.s("XAUTOCLAIM s g c2 0 0"),
            "[0-0 [[2-0 [f 2]]] [1-0 3-0]]"
        );
        assert_eq!(db.s("XPENDING s g"), "[1 2-0 2-0 [[c2 1]]]");
        assert_eq!(db.s("XAUTOCLAIM s g c2 0 0 JUSTID"), "[0-0 [2-0] []]");
    }

    #[test]
    fn claim_with_min_idle_and_options() {
        let mut db = Db::at(1000);
        for i in 1..=3 {
            db.run(&format!("XADD s {i}-0 f {i}"));
        }
        db.run("XGROUP CREATE s g 0");
        let op = XOp::ReadGroup {
            group: b"g".to_vec(),
            consumer: b"c1".to_vec(),
            id: GroupId::New,
            count: None,
            noack: false,
        };
        db.stream.as_mut().unwrap().apply(b"s", op, 1000);
        db.now = 1500;
        // Not idle long enough.
        assert_eq!(db.s("XCLAIM s g c2 600 1-0 2-0"), "[]");
        assert_eq!(db.s("XPENDING s g"), "[3 1-0 3-0 [[c1 3]]]");
        assert_eq!(db.s("XCLAIM s g c2 500 1-0 2-0 JUSTID"), "[1-0 2-0]");
        // JUSTID does not bump the delivery count; a plain claim does.
        assert_eq!(
            db.s("XPENDING s g - + 10 c2"),
            "[[1-0 c2 0 1] [2-0 c2 0 1]]"
        );
        assert_eq!(db.s("XCLAIM s g c3 0 1-0"), "[[1-0 [f 1]]]");
        assert_eq!(db.s("XPENDING s g - + 10 c3"), "[[1-0 c3 0 2]]");
        // IDLE sets the idle time, RETRYCOUNT the count; TIME in the future is now.
        assert_eq!(
            db.s("XCLAIM s g c3 0 2-0 IDLE 300 RETRYCOUNT 7 JUSTID"),
            "[2-0]"
        );
        assert_eq!(
            db.s("XPENDING s g - + 10 c3"),
            "[[1-0 c3 0 2] [2-0 c3 300 7]]"
        );
        assert_eq!(db.s("XCLAIM s g c3 0 2-0 TIME 99999 JUSTID"), "[2-0]");
        assert_eq!(
            db.s("XPENDING s g - + 10 c3"),
            "[[1-0 c3 0 2] [2-0 c3 0 7]]"
        );
        // IDLE filter on XPENDING.
        db.now = 1700;
        assert_eq!(
            db.s("XPENDING s g IDLE 100 - + 10"),
            "[[1-0 c3 200 2] [2-0 c3 200 7] [3-0 c1 700 1]]"
        );
        assert_eq!(db.s("XPENDING s g IDLE 300 - + 10"), "[[3-0 c1 700 1]]");
        assert_eq!(db.s("XPENDING s g - + 1"), "[[1-0 c3 200 2]]");
        assert_eq!(db.s("XPENDING s g - + 0"), "[]");
        assert_eq!(db.s("XPENDING s g - + 10 nobody"), "[]");
        assert_eq!(
            db.s("XPENDING s g (1-0 + 10"),
            "[[2-0 c3 200 7] [3-0 c1 700 1]]"
        );
        // FORCE creates a row for an entry nobody holds; without it, nothing.
        assert_eq!(db.s("XACK s g 3-0"), "1");
        assert_eq!(db.s("XCLAIM s g c4 0 3-0"), "[]");
        assert_eq!(db.s("XCLAIM s g c4 0 3-0 FORCE JUSTID"), "[3-0]");
        assert_eq!(db.s("XPENDING s g - + 10 c4"), "[[3-0 c4 0 1]]");
        // FORCE of an id that is not in the stream does nothing.
        assert_eq!(db.s("XCLAIM s g c4 0 9-0 FORCE"), "[]");
        // LASTID moves the group's last delivered id forward only.
        assert_eq!(db.s("XCLAIM s g c4 0 1-0 LASTID 8-0 JUSTID"), "[1-0]");
        assert!(db.s("XINFO GROUPS s").contains("last-delivered-id 8-0"));
        db.run("XCLAIM s g c4 0 1-0 LASTID 2-0 JUSTID");
        assert!(db.s("XINFO GROUPS s").contains("last-delivered-id 8-0"));
        // Errors.
        assert_eq!(
            db.s("XCLAIM s g c4 x 1-0"),
            "ERR Invalid min-idle-time argument for XCLAIM"
        );
        assert_eq!(
            db.s("XCLAIM s g c4 0 1-0 BOGUS"),
            "ERR Unrecognized XCLAIM option 'BOGUS'"
        );
        assert_eq!(
            db.s("XCLAIM s g c4 0 1-0 IDLE x"),
            "ERR Invalid IDLE option argument for XCLAIM"
        );
        assert_eq!(
            db.s("XCLAIM s g c4 0 1-0 RETRYCOUNT -1"),
            "ERR Invalid RETRYCOUNT option argument for XCLAIM"
        );
        assert_eq!(db.s("XCLAIM s g c4 0 1-0 LASTID bad"), ERR_BAD_ID);
        assert_eq!(
            db.s("XCLAIM s g c4 0"),
            "ERR wrong number of arguments for 'xclaim' command"
        );
        assert_eq!(
            db.s("XCLAIM s nog c4 0 1-0"),
            "NOGROUP No such key 's' or consumer group 'nog'"
        );
        assert_eq!(
            db.s("XCLAIM nokey g c4 0 1-0"),
            "NOGROUP No such key 'nokey' or consumer group 'g'"
        );
        // Negative min-idle is zero.
        assert_eq!(db.s("XCLAIM s g c4 -5 1-0 JUSTID"), "[1-0]");
    }

    #[test]
    fn autoclaim_cursor_and_wrap() {
        let mut db = Db::at(1000);
        for i in 1..=5 {
            db.run(&format!("XADD s {i}-0 f {i}"));
        }
        db.run("XGROUP CREATE s g 0");
        let op = XOp::ReadGroup {
            group: b"g".to_vec(),
            consumer: b"c1".to_vec(),
            id: GroupId::New,
            count: None,
            noack: false,
        };
        db.stream.as_mut().unwrap().apply(b"s", op, 1000);
        db.now = 2000;
        assert_eq!(
            db.s("XAUTOCLAIM s g c2 0 0 COUNT 2 JUSTID"),
            "[3-0 [1-0 2-0] []]"
        );
        assert_eq!(
            db.s("XAUTOCLAIM s g c2 0 3-0 COUNT 2 JUSTID"),
            "[5-0 [3-0 4-0] []]"
        );
        // The last page wraps the cursor to 0-0.
        assert_eq!(
            db.s("XAUTOCLAIM s g c2 0 5-0 COUNT 2 JUSTID"),
            "[0-0 [5-0] []]"
        );
        assert_eq!(
            db.s("XAUTOCLAIM s g c2 0 (5-0 COUNT 2 JUSTID"),
            "[0-0 [] []]"
        );
        // Exactly COUNT left: the cursor still points past them, to nothing.
        assert_eq!(
            db.s("XAUTOCLAIM s g c2 0 4-0 COUNT 2 JUSTID"),
            "[0-0 [4-0 5-0] []]"
        );
        // Min idle skips rows but they cost attempts, not count.
        db.now = 2100;
        assert_eq!(db.s("XAUTOCLAIM s g c3 1000 0 JUSTID"), "[0-0 [] []]");
        assert_eq!(
            db.s("XAUTOCLAIM s g c3 50 0 COUNT 1 JUSTID"),
            "[2-0 [1-0] []]"
        );
        assert_eq!(
            db.s("XAUTOCLAIM s g c3 0 0 COUNT 0"),
            "ERR COUNT must be > 0"
        );
        assert_eq!(
            db.s("XAUTOCLAIM s g c3 0 0 COUNT -1"),
            "ERR COUNT must be > 0"
        );
        assert_eq!(db.s("XAUTOCLAIM s g c3 0 0 FOO"), "ERR syntax error");
        assert_eq!(
            db.s("XAUTOCLAIM s g c3 x 0"),
            "ERR Invalid min-idle-time argument for XAUTOCLAIM"
        );
        assert_eq!(
            db.s(&format!("XAUTOCLAIM s g c3 0 ({MAX_ID}")),
            "ERR invalid start ID for the interval"
        );
        assert_eq!(
            db.s("XAUTOCLAIM s nog c3 0 0"),
            "NOGROUP No such key 's' or consumer group 'nog'"
        );
        // Delivery counts: plain claims bump, JUSTID does not.
        assert_eq!(db.s("XPENDING s g - + 1 c3"), "[[1-0 c3 0 1]]");
        assert_eq!(
            db.s("XAUTOCLAIM s g c3 0 0 COUNT 1"),
            "[2-0 [[1-0 [f 1]]] []]"
        );
        assert_eq!(db.s("XPENDING s g - + 1 c3"), "[[1-0 c3 0 2]]");
    }

    #[test]
    fn autoclaim_attempt_budget_limits_the_scan() {
        let mut db = Db::at(1000);
        for i in 1..=30 {
            db.run(&format!("XADD s {i}-0 f v"));
        }
        db.run("XGROUP CREATE s g 0");
        let op = XOp::ReadGroup {
            group: b"g".to_vec(),
            consumer: b"c1".to_vec(),
            id: GroupId::New,
            count: None,
            noack: false,
        };
        db.stream.as_mut().unwrap().apply(b"s", op, 1000);
        db.now = 1001;
        // COUNT 1 allows 10 attempts; all are too fresh: the cursor stops at 11-0.
        assert_eq!(
            db.s("XAUTOCLAIM s g c2 10000 0 COUNT 1 JUSTID"),
            "[11-0 [] []]"
        );
    }

    #[test]
    fn xpending_forms_and_errors() {
        let mut db = Db::at(1000);
        db.run("XADD s 1-0 f v");
        assert_eq!(db.s("XGROUP CREATE s g 0"), "OK");
        assert_eq!(db.s("XPENDING s g"), "[0 nil nil nilarray]");
        assert_eq!(db.s("XPENDING s g - + 10"), "[]");
        assert_eq!(db.s("XPENDING s g - +"), "ERR syntax error");
        assert_eq!(db.s("XPENDING s g IDLE 5 - +"), "ERR syntax error");
        assert_eq!(db.s("XPENDING s g - + x"), NOT_INT);
        assert_eq!(db.s("XPENDING s g IDLE x - + 1"), NOT_INT);
        assert_eq!(
            db.s("XPENDING s"),
            "ERR wrong number of arguments for 'xpending' command"
        );
        assert_eq!(
            db.s("XPENDING s g - + 1 c extra extra extra"),
            "ERR syntax error"
        );
        assert_eq!(db.s("XPENDING s g bad + 1"), ERR_BAD_ID);
        assert_eq!(
            db.s(&format!("XPENDING s g ({MAX_ID} + 1")),
            "ERR invalid start ID for the interval"
        );
        assert_eq!(db.s("XPENDING s g + - 5"), "[]");
        assert_eq!(
            db.s("XPENDING nokey g"),
            "NOGROUP No such key 'nokey' or consumer group 'g'"
        );
    }

    #[test]
    fn read_after_and_noack() {
        let mut db = Db::at(1);
        for i in 1..=3 {
            db.run(&format!("XADD s {i}-0 f {i}"));
        }
        let run = |db: &mut Db, op: XOp| show(&db.stream.as_mut().unwrap().apply(b"s", op, 1).0);
        let after = |after, count| XOp::ReadAfter { after, count };
        let id = |ms| IdSpec::Id(StreamId { ms, seq: 0 });
        assert_eq!(
            run(&mut db, after(id(1), None)),
            "[[2-0 [f 2]] [3-0 [f 3]]]"
        );
        assert_eq!(run(&mut db, after(id(1), Some(1))), "[[2-0 [f 2]]]");
        assert_eq!(
            run(&mut db, after(id(1), Some(0))),
            "[[2-0 [f 2]] [3-0 [f 3]]]"
        );
        assert_eq!(run(&mut db, after(id(3), None)), "nil");
        assert_eq!(run(&mut db, after(IdSpec::Last, None)), "nil");
        assert_eq!(run(&mut db, after(IdSpec::Id(StreamId::MAX), None)), "nil");
        assert!(!XOp::ReadAfter {
            after: IdSpec::Last,
            count: None
        }
        .is_write());
        assert_eq!(parse_read_id(b"$").unwrap(), IdSpec::Last);
        assert_eq!(parse_read_id(b"5").unwrap(), id(5));
        assert!(parse_read_id(b"x").is_err());
        assert_eq!(parse_group_read_id(b">").unwrap(), GroupId::New);
        assert_eq!(
            parse_group_read_id(b"5-1").unwrap(),
            GroupId::Id(StreamId { ms: 5, seq: 1 })
        );
        assert!(parse_group_read_id(b"$").is_err());
        // NOACK: delivered, not tracked, and the group still advances.
        db.run("XGROUP CREATE s g 0");
        let op = XOp::ReadGroup {
            group: b"g".to_vec(),
            consumer: b"c".to_vec(),
            id: GroupId::New,
            count: None,
            noack: true,
        };
        assert_eq!(
            run(&mut db, op.clone()),
            "[[1-0 [f 1]] [2-0 [f 2]] [3-0 [f 3]]]"
        );
        assert_eq!(db.s("XPENDING s g"), "[0 nil nil nilarray]");
        assert_eq!(run(&mut db, op), "nil");
        assert!(XOp::ReadGroup {
            group: vec![],
            consumer: vec![],
            id: GroupId::New,
            count: None,
            noack: false
        }
        .is_write());
        assert!(XOp::ReadGroup {
            group: b"g".to_vec(),
            consumer: vec![],
            id: GroupId::New,
            count: None,
            noack: false
        }
        .missing(b"k")
        .is_error());
    }

    #[test]
    fn lag_and_entries_read() {
        let mut db = Db::at(1);
        for i in 1..=4 {
            db.run(&format!("XADD s {i}-0 f v"));
        }
        db.run("XGROUP CREATE s g 0");
        db.run("XGROUP CREATE s tail $");
        let info = db.s("XINFO GROUPS s");
        assert!(
            info.contains(
                "name g consumers 0 pending 0 last-delivered-id 0-0 entries-read nil lag 4"
            ),
            "{info}"
        );
        assert!(
            info.contains(
                "name tail consumers 0 pending 0 last-delivered-id 4-0 entries-read nil lag 0"
            ),
            "{info}"
        );
        let op = XOp::ReadGroup {
            group: b"g".to_vec(),
            consumer: b"c".to_vec(),
            id: GroupId::New,
            count: Some(3),
            noack: false,
        };
        db.stream.as_mut().unwrap().apply(b"s", op, 1);
        let info = db.s("XINFO GROUPS s");
        assert!(
            info.contains(
                "name g consumers 1 pending 3 last-delivered-id 3-0 entries-read 3 lag 1"
            ),
            "{info}"
        );
        // A tombstone after the group's position makes the lag unknowable from counters.
        db.run("XDEL s 4-0");
        let info = db.s("XINFO GROUPS s");
        assert!(
            info.contains("last-delivered-id 3-0 entries-read 3 lag nil"),
            "{info}"
        );
        db.run("XGROUP CREATE s counted 0 ENTRIESREAD 2");
        assert!(db.s("XINFO GROUPS s").contains(
            "name counted consumers 0 pending 0 last-delivered-id 0-0 entries-read 2 lag nil"
        ));
    }

    #[test]
    fn xinfo_stream_shapes() {
        let mut db = Db::at(1);
        db.run("XADD s 1-0 f v");
        db.run("XADD s 2-0 g w");
        db.run("XGROUP CREATE s grp 0");
        assert_eq!(
            db.s("XINFO STREAM s"),
            "[length 2 radix-tree-keys 1 radix-tree-nodes 2 last-generated-id 2-0 max-deleted-entry-id 0-0 entries-added 2 recorded-first-entry-id 1-0 groups 1 first-entry [1-0 [f v]] last-entry [2-0 [g w]]]"
        );
        let full = db.s("XINFO STREAM s FULL");
        assert!(full.starts_with("[length 2 "), "{full}");
        assert!(full.contains("entries [[1-0 [f v]] [2-0 [g w]]] groups [[name grp last-delivered-id 0-0 entries-read nil lag 2 pel-count 0 pending [] consumers []]]"), "{full}");
        let full1 = db.s("XINFO STREAM s FULL COUNT 1");
        assert!(full1.contains("entries [[1-0 [f v]]] groups"), "{full1}");
        // Empty stream: nil entries, zero keys.
        db.run("XTRIM s MAXLEN 0");
        assert_eq!(
            db.s("XINFO STREAM s"),
            "[length 0 radix-tree-keys 0 radix-tree-nodes 1 last-generated-id 2-0 max-deleted-entry-id 0-0 entries-added 2 recorded-first-entry-id 0-0 groups 1 first-entry nil last-entry nil]"
        );
        assert_eq!(db.s("XINFO CONSUMERS s grp"), "[]");
        db.run("XGROUP CREATECONSUMER s grp c");
        db.now = 51;
        assert_eq!(
            db.s("XINFO CONSUMERS s grp"),
            "[[name c pending 0 idle 50 inactive -1]]"
        );
    }

    #[test]
    fn op_classification() {
        let parse1 = |line: &str| {
            let args: Vec<Bytes> = line
                .split_whitespace()
                .map(|w| w.as_bytes().to_vec())
                .collect();
            let name = line.split_whitespace().next().unwrap().to_ascii_uppercase();
            parse(&name, &args).unwrap().1
        };
        assert!(parse1("XADD s * f v").creates());
        assert!(!parse1("XADD s NOMKSTREAM * f v").creates());
        assert!(parse1("XGROUP CREATE s g $ MKSTREAM").creates());
        assert!(!parse1("XGROUP CREATE s g $").creates());
        assert!(!parse1("XGROUP DESTROY s g").creates());
        for read in [
            "XLEN s",
            "XRANGE s - +",
            "XREVRANGE s + -",
            "XPENDING s g",
            "XINFO GROUPS s",
        ] {
            assert!(!parse1(read).is_write(), "{read}");
        }
        for write in [
            "XADD s * f v",
            "XDEL s 1",
            "XTRIM s MAXLEN 1",
            "XSETID s 1",
            "XACK s g 1",
            "XCLAIM s g c 0 1",
            "XAUTOCLAIM s g c 0 0",
            "XGROUP DESTROY s g",
        ] {
            assert!(parse1(write).is_write(), "{write}");
        }
        let (key, _) = parse(
            "XGROUP",
            &[
                b"xgroup".to_vec(),
                b"create".to_vec(),
                b"mykey".to_vec(),
                b"g".to_vec(),
                b"$".to_vec(),
            ],
        )
        .unwrap();
        assert_eq!(key, b"mykey");
        let (key, _) = parse(
            "XINFO",
            &[b"XINFO".to_vec(), b"stream".to_vec(), b"k2".to_vec()],
        )
        .unwrap();
        assert_eq!(key, b"k2");
        assert!(parse("XFOO", &[b"xfoo".to_vec()]).is_err());
    }

    // ---- differential test ----

    struct Rng(u64);

    impl Rng {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    fn model_entries(model: &[(u64, u64)], rev: bool) -> String {
        let mut rows: Vec<String> = model
            .iter()
            .map(|(ms, seq)| format!("[{ms}-{seq} [f v]]"))
            .collect();
        if rev {
            rows.reverse();
        }
        format!("[{}]", rows.join(" "))
    }

    #[test]
    fn differential_against_a_vec_model() {
        for seed in 0..20u64 {
            let mut rng = Rng(seed + 1);
            let mut db = Db::at(100);
            let mut model: Vec<(u64, u64)> = Vec::new();
            let mut last = (0u64, 0u64);
            for step in 0..400 {
                match rng.next(10) {
                    0..=4 => {
                        db.now = last.0.saturating_sub(2) + rng.next(6);
                        let (reply, expect) = if rng.next(2) == 0 {
                            let now = db.now;
                            let id = if now > last.0 {
                                (now, 0)
                            } else {
                                (last.0, last.1 + 1)
                            };
                            (db.s("XADD s * f v"), id)
                        } else {
                            let ms = last.0 + rng.next(3);
                            let seq = if ms == last.0 {
                                last.1 + 1 + rng.next(3)
                            } else {
                                rng.next(3)
                            };
                            let id = if (ms, seq) == (0, 0) {
                                (0, 1)
                            } else {
                                (ms, seq)
                            };
                            (db.s(&format!("XADD s {}-{} f v", id.0, id.1)), id)
                        };
                        assert_eq!(
                            reply,
                            format!("{}-{}", expect.0, expect.1),
                            "seed {seed} step {step}"
                        );
                        model.push(expect);
                        last = expect;
                    }
                    5 | 6 => {
                        let target = if !model.is_empty() && rng.next(10) < 7 {
                            model[rng.next(model.len() as u64) as usize]
                        } else {
                            (rng.next(last.0 + 2), rng.next(4))
                        };
                        let expected = model.iter().filter(|e| **e == target).count();
                        model.retain(|e| *e != target);
                        assert_eq!(
                            db.s(&format!("XDEL s {}-{}", target.0, target.1)),
                            expected.to_string()
                        );
                    }
                    7 => {
                        let keep = rng.next(model.len() as u64 + 2) as usize;
                        let drop = model.len().saturating_sub(keep);
                        model.drain(..drop);
                        assert_eq!(db.s(&format!("XTRIM s MAXLEN {keep}")), drop.to_string());
                    }
                    8 => {
                        let min = (rng.next(last.0 + 2), rng.next(3));
                        let before = model.len();
                        model.retain(|e| *e >= min);
                        let removed = before - model.len();
                        assert_eq!(
                            db.s(&format!("XTRIM s MINID {}-{}", min.0, min.1)),
                            removed.to_string()
                        );
                    }
                    _ => {}
                }
                if db.stream.is_none() {
                    continue;
                }
                assert_eq!(db.s("XLEN s"), model.len().to_string());
                assert_eq!(
                    db.s("XRANGE s - +"),
                    model_entries(&model, false),
                    "seed {seed} step {step}"
                );
                assert_eq!(db.s("XREVRANGE s + -"), model_entries(&model, true));
                let a = (rng.next(last.0 + 2), rng.next(4));
                let b = (rng.next(last.0 + 2), rng.next(4));
                let (ex_start, ex_end) = (rng.next(2) == 0, rng.next(2) == 0);
                let count = rng.next(4) as usize;
                let line = format!(
                    "XRANGE s {}{}-{} {}{}-{} COUNT {count}",
                    if ex_start { "(" } else { "" },
                    a.0,
                    a.1,
                    if ex_end { "(" } else { "" },
                    b.0,
                    b.1
                );
                let expect: Vec<(u64, u64)> = model
                    .iter()
                    .copied()
                    .filter(|e| if ex_start { *e > a } else { *e >= a })
                    .filter(|e| if ex_end { *e < b } else { *e <= b })
                    .take(count)
                    .collect();
                // (0-0 as an exclusive end is an error in Redis, as is max as a start.)
                if ex_end && b == (0, 0) {
                    assert_eq!(db.s(&line), "ERR invalid end ID for the interval");
                } else {
                    assert_eq!(
                        db.s(&line),
                        model_entries(&expect, false),
                        "{line} seed {seed} step {step}"
                    );
                }
            }
        }
    }

    // ---- persistence ----

    fn busy_stream() -> Stream {
        let mut db = Db::at(1000);
        for i in 1..=6 {
            db.run(&format!("XADD s {i}-0 f{i} v{i} g{i} w{i}"));
        }
        db.run("XADD s 7-0 e");
        db.run("XGROUP CREATE s g1 0");
        db.run("XGROUP CREATE s g2 $ ENTRIESREAD 3");
        db.run("XDEL s 2-0");
        let read = |db: &mut Db, group: &str, consumer: &str, count| {
            let op = XOp::ReadGroup {
                group: group.into(),
                consumer: consumer.into(),
                id: GroupId::New,
                count: Some(count),
                noack: false,
            };
            db.stream.as_mut().unwrap().apply(b"s", op, 1234);
        };
        read(&mut db, "g1", "alice", 3);
        read(&mut db, "g1", "bob", 2);
        db.run("XGROUP CREATECONSUMER s g1 idle");
        db.run("XGROUP CREATECONSUMER s g2 solo");
        db.now = 2000;
        db.run("XCLAIM s g1 bob 0 1-0 RETRYCOUNT 9");
        db.stream.unwrap()
    }

    #[test]
    fn encode_decode_round_trip() {
        let original = busy_stream();
        let mut bytes = Vec::new();
        original.encode(&mut bytes);
        let mut input = &bytes[..];
        let decoded = Stream::decode(&mut input).unwrap();
        assert!(
            input.is_empty(),
            "decode must consume exactly what encode wrote"
        );
        let mut again = Vec::new();
        decoded.encode(&mut again);
        assert_eq!(bytes, again);
        // And it behaves the same.
        let mut a = original.clone();
        let mut b = decoded;
        for op in [XOp::InfoStream { full: Some(0) }, XOp::InfoGroups] {
            assert_eq!(
                show(&a.apply(b"s", op.clone(), 3000).0),
                show(&b.apply(b"s", op, 3000).0)
            );
        }
        let (reply, _) = b.apply(
            b"s",
            XOp::Pending {
                group: b"g1".to_vec(),
                detail: None,
            },
            3000,
        );
        assert_eq!(show(&reply), "[5 1-0 6-0 [[alice 2] [bob 3]]]");
        // An empty stream and a stream with only a last id round-trip too.
        let mut empty = Vec::new();
        Stream::default().encode(&mut empty);
        let mut input = &empty[..];
        let decoded = Stream::decode(&mut input).unwrap();
        assert!(input.is_empty());
        assert_eq!(decoded.entries(), 0);
    }

    #[test]
    fn decode_leaves_trailing_input_alone() {
        let mut bytes = Vec::new();
        busy_stream().encode(&mut bytes);
        let len = bytes.len();
        bytes.extend_from_slice(b"tail");
        let mut input = &bytes[..];
        assert!(Stream::decode(&mut input).is_some());
        assert_eq!(input, b"tail");
        assert_eq!(bytes.len() - input.len(), len);
    }

    #[test]
    fn decode_rejects_every_truncation() {
        let mut bytes = Vec::new();
        busy_stream().encode(&mut bytes);
        for cut in 0..bytes.len() {
            let mut input = &bytes[..cut];
            assert!(
                Stream::decode(&mut input).is_none(),
                "prefix of {cut} bytes decoded"
            );
            assert_eq!(input.len(), cut, "failed decode must not consume");
        }
    }

    #[test]
    fn decode_survives_garbage() {
        let mut bytes = Vec::new();
        busy_stream().encode(&mut bytes);
        let mut rng = Rng(7);
        for _ in 0..3000 {
            let mut garbled = bytes.clone();
            for _ in 0..1 + rng.next(4) {
                let at = rng.next(garbled.len() as u64) as usize;
                garbled[at] = rng.next(256) as u8;
            }
            let mut input = &garbled[..];
            let _ = Stream::decode(&mut input);
        }
        // Huge claimed counts with nothing behind them.
        for fill in [0xffu8, 0x7f, 0x80, 0x00] {
            let hostile = [fill; 200];
            let mut input = &hostile[..];
            let _ = Stream::decode(&mut input);
        }
        let mut huge = vec![0u8; 40];
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut input = &huge[..];
        assert!(Stream::decode(&mut input).is_none());
        let mut input: &[u8] = &[];
        assert!(Stream::decode(&mut input).is_none());
    }

    #[test]
    fn decode_rejects_inconsistent_streams() {
        // A PEL row owned by a consumer that does not exist.
        let mut s = busy_stream();
        let g1 = s.groups.get_mut(b"g1".as_slice()).unwrap();
        g1.consumers.remove(b"alice".as_slice());
        let mut bytes = Vec::new();
        s.encode(&mut bytes);
        let mut input = &bytes[..];
        assert!(Stream::decode(&mut input).is_none());
        // An entry above last_id.
        let mut s = busy_stream();
        s.last_id = StreamId { ms: 1, seq: 0 };
        let mut bytes = Vec::new();
        s.encode(&mut bytes);
        let mut input = &bytes[..];
        assert!(Stream::decode(&mut input).is_none());
    }

    #[test]
    fn len_is_never_zero() {
        let s = Stream::default();
        assert_eq!(s.entries(), 0);
        assert_eq!(s.len(), 1);
        assert!(!s.is_empty());
    }
}
