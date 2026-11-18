//! The commands on lists, sets and sorted sets, and the newer ones on
//! strings, hashes and whole keys. `Cmd::Key` carries a `KeyOp` for one key,
//! `Cmd::Multi` a `MultiOp` over several.

use crate::bitfield;
use crate::bloom::{self, BfCmd, Settings};
use crate::cmd::{arity, parse_cond, parse_when, syntax, Bytes, Cmd, Cond, Expire, NOT_INT};
use crate::float::parse_bound;
use crate::geo::{self, GeoOp};
use crate::json::{self, JsonCmd};
use crate::resp::{parse_i64, Reply, Shape};
use crate::sort;
use crate::stream::{self, GroupId, IdSpec, StreamId, XOp};
use crate::zset::parse_float;
use std::sync::{Arc, OnceLock};

pub const NOT_FLOAT: &str = "ERR value is not a valid float";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZBound {
    pub value: f64,
    pub exclusive: bool,
}

/// One end of a range of members compared as bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum LexBound {
    /// `-`: before every member.
    Min,
    /// `+`: after every member.
    Max,
    Incl(Bytes),
    Excl(Bytes),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ZBy {
    /// Positions, negative ones counting from the end.
    Rank(i64, i64),
    /// Lowest and highest score.
    Score(ZBound, ZBound),
    /// First and last member, for a set whose scores are all the same.
    Lex(LexBound, LexBound),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZRangeSpec {
    pub by: ZBy,
    pub rev: bool,
    /// Offset and count; no count means everything after the offset.
    pub limit: Option<(usize, Option<usize>)>,
    pub withscores: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ZAddFlags {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
    pub incr: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectSub {
    Encoding,
    RefCount,
    /// IDLETIME: always 0.
    Zero,
    /// How often the key is used, on the scale of 0 to 255.
    Freq,
}

/// What HGETEX and HSETEX do to the deadlines of their fields.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldTtlArg {
    /// No option: HGETEX leaves the deadlines, HSETEX removes them.
    None,
    /// KEEPTTL (HSETEX).
    Keep,
    /// PERSIST (HGETEX).
    Persist,
    /// EX, PX, EXAT or PXAT with its value, which is checked only once
    /// the type of the key is, as Valkey does.
    When {
        secs: bool,
        absolute: bool,
        raw: Bytes,
    },
}

impl FieldTtlArg {
    /// The deadline it names, if any; the errors of an EX or the like that
    /// does not name one. 0 and times already past are fine: they remove
    /// the fields.
    pub fn when(&self, cmd: &str) -> Result<Option<Expire>, Reply> {
        let FieldTtlArg::When {
            secs,
            absolute,
            raw,
        } = self
        else {
            return Ok(None);
        };
        if parse_i64(raw).is_some_and(|v| v < 0) {
            return Err(Reply::Error(format!(
                "ERR invalid expire time in '{cmd}' command"
            )));
        }
        parse_when(raw, *secs, *absolute, false, cmd).map(Some)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum KeyOp {
    /// A stream command.
    X(Box<XOp>),
    /// A BF.* command.
    Bloom(Box<BfCmd>),
    /// A JSON.* command on one key.
    Json(Box<JsonCmd>),
    /// The increment as given, read only once the key's value has been.
    IncrByFloat(Bytes),
    GetRange(i64, i64),
    SetRange(usize, Bytes),
    HKeys,
    HVals,
    HStrlen(Bytes),
    /// The field and the increment as given: Valkey reads the increment
    /// when the command runs, so inside MULTI a bad one still queues.
    HIncrByFloat(Bytes, Bytes),
    HRandField {
        count: Option<i64>,
        values: bool,
    },
    Object(ObjectSub),
    /// HEXPIRE, HPEXPIRE, HEXPIREAT and HPEXPIREAT.
    HExpire {
        when: Expire,
        cond: Option<Cond>,
        fields: Vec<Bytes>,
    },
    /// HTTL and HPTTL, or with `absolute` HEXPIRETIME and HPEXPIRETIME.
    HTtl {
        fields: Vec<Bytes>,
        millis: bool,
        absolute: bool,
    },
    HPersist(Vec<Bytes>),
    HGetDel(Vec<Bytes>),
    HGetEx {
        ttl: FieldTtlArg,
        fields: Vec<Bytes>,
    },
    HSetEx {
        /// NX or XX, on the key.
        cond: Option<Cond>,
        /// FNX or FXX, on the fields.
        fcond: Option<Cond>,
        ttl: FieldTtlArg,
        pairs: Vec<(Bytes, Bytes)>,
    },
    /// DELIFEQ: delete the string if it holds this value.
    DelIfEq(Bytes),
    /// MEMORY USAGE with its SAMPLES (0: every element).
    MemoryUsage(usize),
    PfAdd(Vec<Bytes>),
    Dump,
    Restore {
        /// Milliseconds, 0 for none; a Unix time with `absolute`.
        ttl: i64,
        absolute: bool,
        replace: bool,
        payload: Bytes,
    },
    Push {
        left: bool,
        /// LPUSHX and RPUSHX: only when the list exists.
        existing: bool,
        items: Vec<Bytes>,
    },
    Pop {
        left: bool,
        count: Option<usize>,
    },
    LLen,
    LRange(i64, i64),
    LIndex(i64),
    LSet(i64, Bytes),
    LRem(i64, Bytes),
    LTrim(i64, i64),
    LInsert {
        before: bool,
        pivot: Bytes,
        item: Bytes,
    },
    LPos {
        item: Bytes,
        rank: i64,
        count: Option<usize>,
        maxlen: usize,
    },
    SAdd(Vec<Bytes>),
    SRem(Vec<Bytes>),
    SIsMember(Bytes),
    SMIsMember(Vec<Bytes>),
    SMembers,
    SCard,
    SPop(Option<usize>),
    SRandMember(Option<i64>),
    SScan {
        cursor: u64,
        pattern: Option<Bytes>,
        count: usize,
    },
    ZAdd {
        flags: ZAddFlags,
        pairs: Vec<(f64, Bytes)>,
    },
    ZRem(Vec<Bytes>),
    ZScore(Bytes),
    ZMScore(Vec<Bytes>),
    ZIncrBy(f64, Bytes),
    ZCard,
    ZCount(ZBound, ZBound),
    ZRank {
        member: Bytes,
        rev: bool,
        withscore: bool,
    },
    ZRange(Box<ZRangeSpec>),
    ZRemRange(ZBy),
    ZPop {
        max: bool,
        count: Option<usize>,
    },
    ZScan {
        cursor: u64,
        pattern: Option<Bytes>,
        count: usize,
    },
    ZLexCount(LexBound, LexBound),
    ZRandMember {
        count: Option<i64>,
        withscores: bool,
    },
    SetBit(usize, bool),
    GetBit(usize),
    /// BITFIELD and BITFIELD_RO: the steps, in order.
    BitField(Vec<bitfield::Op>),
    /// GEOPOS, GEOHASH, GEODIST and the searches that store nothing.
    Geo(Box<GeoOp>),
    /// Start, end and whether they count bits instead of bytes.
    BitCount(Option<(i64, i64, bool)>),
    BitPos {
        bit: bool,
        start: i64,
        end: Option<i64>,
        bits: bool,
    },
}

impl KeyOp {
    /// How the reply is typed in RESP3.
    pub fn shape(&self) -> Shape {
        match self {
            KeyOp::SMembers | KeyOp::SPop(Some(_)) => Shape::Set,
            KeyOp::ZScore(_) | KeyOp::ZMScore(_) | KeyOp::ZIncrBy(..) => Shape::Double,
            KeyOp::ZAdd { flags, .. } if flags.incr => Shape::Double,
            KeyOp::ZRank {
                withscore: true, ..
            }
            | KeyOp::ZPop { count: None, .. } => Shape::LastDouble,
            KeyOp::ZPop { count: Some(_), .. }
            | KeyOp::ZRandMember {
                withscores: true, ..
            } => Shape::ScorePairs,
            KeyOp::ZRange(spec) if spec.withscores => Shape::ScorePairs,
            KeyOp::HRandField {
                count: Some(_),
                values: true,
            } => Shape::Pairs,
            KeyOp::X(op) if matches!(**op, XOp::InfoStream { .. }) => Shape::Map,
            KeyOp::Bloom(cmd) if cmd.is_info() => Shape::Double,
            KeyOp::Geo(op) => match &**op {
                GeoOp::Pos(_) => Shape::Double,
                GeoOp::Search(query) if query.withcoord => Shape::Coords,
                GeoOp::Search(_) | GeoOp::Hash(_) | GeoOp::Dist(..) => Shape::Plain,
            },
            _ => Shape::Plain,
        }
    }

    pub fn is_write(&self) -> bool {
        matches!(
            self,
            KeyOp::IncrByFloat(_)
                | KeyOp::SetRange(..)
                | KeyOp::HIncrByFloat(..)
                | KeyOp::HExpire { .. }
                | KeyOp::HPersist(_)
                | KeyOp::HGetDel(_)
                | KeyOp::HGetEx { .. }
                | KeyOp::HSetEx { .. }
                | KeyOp::DelIfEq(_)
                | KeyOp::PfAdd(_)
                | KeyOp::Restore { .. }
                | KeyOp::Push { .. }
                | KeyOp::Pop { .. }
                | KeyOp::LSet(..)
                | KeyOp::LRem(..)
                | KeyOp::LTrim(..)
                | KeyOp::LInsert { .. }
                | KeyOp::SAdd(_)
                | KeyOp::SRem(_)
                | KeyOp::SPop(_)
                | KeyOp::ZAdd { .. }
                | KeyOp::ZRem(_)
                | KeyOp::ZIncrBy(..)
                | KeyOp::ZRemRange(_)
                | KeyOp::ZPop { .. }
                | KeyOp::SetBit(..)
        ) || matches!(self, KeyOp::X(op) if op.is_write())
            || matches!(self, KeyOp::Bloom(cmd) if cmd.is_write())
            || matches!(self, KeyOp::Json(cmd) if cmd.is_write())
            || matches!(self, KeyOp::BitField(ops) if ops.iter().any(bitfield::Op::writes))
    }

    /// Whether the command can make the key larger (refused when memory
    /// is full and nothing can be evicted).
    pub fn may_grow(&self) -> bool {
        matches!(
            self,
            KeyOp::IncrByFloat(_)
                | KeyOp::SetRange(..)
                | KeyOp::HIncrByFloat(..)
                | KeyOp::HSetEx { .. }
                | KeyOp::PfAdd(_)
                | KeyOp::Restore { .. }
                | KeyOp::Push { .. }
                | KeyOp::LSet(..)
                | KeyOp::LInsert { .. }
                | KeyOp::SAdd(_)
                | KeyOp::ZAdd { .. }
                | KeyOp::ZIncrBy(..)
                | KeyOp::SetBit(..)
        ) || matches!(
            self,
            KeyOp::X(op) if matches!(
                **op,
                XOp::Add { .. } | XOp::GroupCreate { .. } | XOp::GroupCreateConsumer { .. }
            )
        ) || matches!(self, KeyOp::BitField(ops) if ops.iter().any(bitfield::Op::writes))
            || matches!(self, KeyOp::Bloom(cmd) if cmd.is_write())
            || matches!(self, KeyOp::Json(cmd) if cmd.may_grow())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOp {
    And,
    Or,
    Xor,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Inter,
    Union,
    Diff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregate {
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MultiOp {
    Rename {
        src: Bytes,
        dst: Bytes,
        nx: bool,
    },
    Copy {
        src: Bytes,
        dst: Bytes,
        replace: bool,
        /// `DB n`: the database of `dst`, when the command names one.
        db: Option<i64>,
    },
    MSetNx(Vec<(Bytes, Bytes)>),
    MSetEx {
        pairs: Vec<(Bytes, Bytes)>,
        /// NX or XX: all keys must be missing, or all there.
        cond: Option<Cond>,
        expire: Expire,
    },
    Lcs {
        a: Bytes,
        b: Bytes,
        opts: LcsOpts,
    },
    PfCount(Vec<Bytes>),
    PfMerge {
        dst: Bytes,
        keys: Vec<Bytes>,
    },
    LMove {
        src: Bytes,
        dst: Bytes,
        from_left: bool,
        to_left: bool,
    },
    SMove {
        src: Bytes,
        dst: Bytes,
        member: Bytes,
    },
    /// SINTER, SUNION, SDIFF, their STORE forms and SINTERCARD.
    Sets {
        op: SetOp,
        keys: Vec<Bytes>,
        store: Option<Bytes>,
        /// SINTERCARD: reply with the size, counting no further than this.
        card: Option<usize>,
    },
    /// ZUNION, ZINTER, ZDIFF, their STORE forms and ZINTERCARD.
    ZStore {
        /// Where the result goes; without it, it is the reply.
        dst: Option<Bytes>,
        keys: Vec<Bytes>,
        weights: Vec<f64>,
        aggregate: Aggregate,
        op: SetOp,
        withscores: bool,
        /// ZINTERCARD: reply with the size, counting no further than this.
        card: Option<usize>,
        /// A bad option, which Valkey reports only once every key has
        /// passed its type check.
        bad_option: Option<Reply>,
    },
    ZRangeStore {
        dst: Bytes,
        src: Bytes,
        spec: ZRangeSpec,
    },
    /// LMPOP and ZMPOP: pop from the first key that has anything.
    MPop {
        keys: Vec<Bytes>,
        zset: bool,
        /// LEFT or MIN, as opposed to RIGHT or MAX.
        first: bool,
        count: usize,
        /// Reply as BLPOP and BZPOPMIN do: the key and what was popped in
        /// one array.
        flat: bool,
    },
    BitOp {
        op: BitOp,
        dst: Bytes,
        keys: Vec<Bytes>,
    },
    /// GEOSEARCHSTORE, and GEORADIUS with STORE or STOREDIST.
    GeoStore {
        dst: Bytes,
        src: Bytes,
        query: Box<geo::Query>,
        /// Store distances as scores instead of positions.
        storedist: bool,
    },
    /// SORT and SORT_RO.
    Sort {
        key: Bytes,
        spec: Box<sort::Spec>,
    },
    /// XREAD and XREADGROUP: one ID per key.
    XRead {
        keys: Vec<Bytes>,
        ids: XIds,
        count: Option<usize>,
        noack: bool,
        /// Where each stream is read from once `$` has been looked up.
        start: Arc<OnceLock<Vec<StreamId>>>,
    },
    /// JSON.MGET: one path of several documents.
    JsonMGet {
        keys: Vec<Bytes>,
        path: Bytes,
    },
    /// JSON.MSET: key, path and value, in order.
    JsonMSet(Vec<(Bytes, Bytes, Bytes)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LcsOpts {
    /// Reply with the length only.
    pub len: bool,
    /// Reply with where the matches are.
    pub idx: bool,
    pub min_match_len: usize,
    pub with_match_len: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum XIds {
    After(Vec<IdSpec>),
    Group {
        group: Bytes,
        consumer: Bytes,
        ids: Vec<GroupId>,
    },
}

impl MultiOp {
    pub fn is_write(&self) -> bool {
        if let MultiOp::Sort { spec, .. } = self {
            return spec.store.is_some();
        }
        !matches!(
            self,
            MultiOp::Sets { store: None, .. }
                | MultiOp::ZStore { dst: None, .. }
                | MultiOp::Lcs { .. }
                | MultiOp::PfCount(_)
                | MultiOp::XRead {
                    ids: XIds::After(_),
                    ..
                }
                | MultiOp::JsonMGet { .. }
        )
    }

    /// How the reply is typed in RESP3.
    pub fn shape(&self) -> Shape {
        match self {
            MultiOp::Sets {
                store: None,
                card: None,
                ..
            } => Shape::Set,
            MultiOp::ZStore {
                dst: None,
                withscores: true,
                card: None,
                ..
            } => Shape::ScorePairs,
            MultiOp::MPop {
                zset: true, flat, ..
            } => {
                if *flat {
                    Shape::LastDouble
                } else {
                    Shape::PopScores
                }
            }
            MultiOp::XRead { .. } => Shape::PairMap,
            MultiOp::Lcs { opts, .. } if opts.idx => Shape::Map,
            _ => Shape::Plain,
        }
    }

    /// The keys a blocking command waits on.
    pub fn wait_keys(&self) -> &[Bytes] {
        match self {
            MultiOp::MPop { keys, .. } | MultiOp::XRead { keys, .. } => keys,
            MultiOp::LMove { src, .. } => std::slice::from_ref(src),
            _ => &[],
        }
    }

    /// Whether the command can add data.
    pub fn may_grow(&self) -> bool {
        self.is_write()
            && !matches!(
                self,
                MultiOp::Rename { .. }
                    | MultiOp::SMove { .. }
                    | MultiOp::MPop { .. }
                    | MultiOp::XRead { .. }
            )
    }

    pub fn for_each_key(&self, mut f: impl FnMut(&[u8])) {
        match self {
            MultiOp::Rename { src, dst, .. }
            | MultiOp::Copy { src, dst, .. }
            | MultiOp::LMove { src, dst, .. }
            | MultiOp::SMove { src, dst, .. } => {
                f(src);
                f(dst);
            }
            MultiOp::MSetNx(pairs) | MultiOp::MSetEx { pairs, .. } => {
                pairs.iter().for_each(|(k, _)| f(k))
            }
            MultiOp::Lcs { a, b, .. } => {
                f(a);
                f(b);
            }
            MultiOp::PfCount(keys) => keys.iter().for_each(|k| f(k)),
            MultiOp::PfMerge { dst, keys } => {
                f(dst);
                keys.iter().for_each(|k| f(k));
            }
            MultiOp::Sets { keys, store, .. } => {
                keys.iter().for_each(|k| f(k));
                if let Some(dst) = store {
                    f(dst);
                }
            }
            MultiOp::ZStore { dst, keys, .. } => {
                keys.iter().for_each(|k| f(k));
                if let Some(dst) = dst {
                    f(dst);
                }
            }
            MultiOp::ZRangeStore { dst, src, .. } => {
                f(src);
                f(dst);
            }
            MultiOp::MPop { keys, .. }
            | MultiOp::XRead { keys, .. }
            | MultiOp::JsonMGet { keys, .. } => keys.iter().for_each(|k| f(k)),
            MultiOp::JsonMSet(triples) => triples.iter().for_each(|(k, _, _)| f(k)),
            MultiOp::BitOp { dst, keys, .. } => {
                keys.iter().for_each(|k| f(k));
                f(dst);
            }
            MultiOp::GeoStore { dst, src, .. } => {
                f(src);
                f(dst);
            }
            // The keys its patterns name are known only when it runs.
            MultiOp::Sort { key, spec } => {
                f(key);
                if let Some(dst) = &spec.store {
                    f(dst);
                }
            }
        }
    }
}

fn int(raw: &[u8]) -> Result<i64, Reply> {
    parse_i64(raw).ok_or_else(|| Reply::err(NOT_INT))
}

fn float(raw: &[u8]) -> Result<f64, Reply> {
    parse_float(raw).ok_or_else(|| Reply::err(NOT_FLOAT))
}

/// A count that must not be negative.
fn count(raw: &[u8]) -> Result<usize, Reply> {
    usize::try_from(int(raw)?)
        .map_err(|_| Reply::err("ERR value is out of range, must be positive"))
}

/// Picks with repeats one command may ask for. The whole reply is built
/// in memory, so the count alone must not decide how large it gets.
const MAX_REPEATS: i64 = 1 << 20;

/// The count of SRANDMEMBER and HRANDFIELD, where a negative one asks for
/// that many picks with repeats.
fn repeat_count(raw: &[u8]) -> Result<i64, Reply> {
    let n = int(raw)?;
    if n < -MAX_REPEATS {
        return Err(Reply::err("ERR value is out of range"));
    }
    Ok(n)
}

fn is(arg: &[u8], word: &str) -> bool {
    arg.eq_ignore_ascii_case(word.as_bytes())
}

fn zbound(raw: &[u8]) -> Result<ZBound, Reply> {
    let (exclusive, digits) = match raw.strip_prefix(b"(") {
        Some(rest) => (true, rest),
        None => (false, raw),
    };
    // Valkey 9.1 reads an empty bound, or a bare "(", as zero.
    let value = parse_bound(digits).ok_or_else(|| Reply::err("ERR min or max is not a float"))?;
    Ok(ZBound { value, exclusive })
}

fn lexbound(raw: &[u8]) -> Result<LexBound, Reply> {
    match raw.split_first() {
        Some((b'-', [])) => Ok(LexBound::Min),
        Some((b'+', [])) => Ok(LexBound::Max),
        Some((b'[', member)) => Ok(LexBound::Incl(member.to_vec())),
        Some((b'(', member)) => Ok(LexBound::Excl(member.to_vec())),
        _ => Err(Reply::err("ERR min or max not valid string range item")),
    }
}

/// `numkeys key [key ...]` starting at `args[at]`: how many keys follow.
fn numkeys(args: &[Bytes], at: usize) -> Result<usize, Reply> {
    let n = usize::try_from(int(&args[at])?)
        .ok()
        .filter(|&k| k > 0)
        .ok_or_else(|| Reply::err("ERR numkeys should be greater than 0"))?;
    if n > args.len() - at - 1 {
        return Err(syntax());
    }
    Ok(n)
}

/// The unit of a bit range: `BYTE` (the default) or `BIT`.
fn bit_unit(raw: Option<&Bytes>) -> Result<bool, Reply> {
    match raw {
        None => Ok(false),
        Some(unit) if is(unit, "BYTE") => Ok(false),
        Some(unit) if is(unit, "BIT") => Ok(true),
        Some(_) => Err(syntax()),
    }
}

fn cursor(raw: &[u8]) -> Result<u64, Reply> {
    std::str::from_utf8(raw)
        .ok()
        .filter(|s| s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Reply::err("ERR invalid cursor"))
}

/// `[MATCH pattern] [COUNT count]` of SSCAN and ZSCAN.
fn scan_opts(mut rest: &[Bytes]) -> Result<(Option<Bytes>, usize), Reply> {
    let (mut pattern, mut n) = (None, 10);
    while let Some(option) = rest.first() {
        let value = rest.get(1).ok_or_else(syntax)?;
        if is(option, "MATCH") {
            if value.len() > crate::cmd::MAX_PATTERN {
                return Err(Reply::err("ERR pattern too long"));
            }
            pattern = Some(value.clone());
        } else if is(option, "COUNT") {
            n = usize::try_from(int(value)?)
                .ok()
                .filter(|&c| c > 0)
                .ok_or_else(syntax)?;
        } else {
            return Err(syntax());
        }
        rest = &rest[2..];
    }
    Ok((pattern, n))
}

/// `LIMIT offset count` at `rest[0..3]`. A negative count means no limit.
fn limit(rest: &[Bytes]) -> Result<(usize, Option<usize>), Reply> {
    let (Some(offset), Some(n)) = (rest.get(1), rest.get(2)) else {
        return Err(syntax());
    };
    let offset = usize::try_from(int(offset)?).unwrap_or(usize::MAX);
    Ok((offset, usize::try_from(int(n)?).ok()))
}

/// `FIELDS numfields field [field ...]`, with FIELDS at `args[at]`.
fn field_list(args: &mut Vec<Bytes>, at: usize) -> Result<Vec<Bytes>, Reply> {
    if !args.get(at).is_some_and(|word| is(word, "FIELDS")) {
        return Err(Reply::err(
            "ERR Mandatory argument FIELDS is missing or not at the right position",
        ));
    }
    let given = args.len().saturating_sub(at + 2);
    let wanted = args.get(at + 1).map_or(Err(syntax()), |raw| int(raw))?;
    if wanted <= 0 {
        return Err(Reply::err(
            "ERR Parameter `numFields` should be greater than 0",
        ));
    }
    if usize::try_from(wanted) != Ok(given) {
        return Err(Reply::err(
            "ERR The `numFields` parameter must match the number of arguments",
        ));
    }
    Ok(args.split_off(at + 2))
}

const NUMFIELDS: &str =
    "ERR numfields should be greater than 0 and match the provided number of fields";

/// The fields of HGETDEL, HGETEX and HSETEX: `per` arguments each after
/// FIELDS at `at` and their number.
fn numbered_fields(args: &mut Vec<Bytes>, at: usize, per: usize) -> Result<Vec<Bytes>, Reply> {
    let wanted = args.get(at + 1).map_or(Err(syntax()), |raw| int(raw))?;
    let given = args.len().saturating_sub(at + 2);
    let fits = usize::try_from(wanted)
        .ok()
        .filter(|n| *n > 0)
        .and_then(|n| n.checked_mul(per))
        == Some(given);
    if !fits {
        return Err(Reply::err(NUMFIELDS));
    }
    Ok(args.split_off(at + 2))
}

/// The options of HGETEX, HSETEX and MSETEX, as Valkey's shared parser
/// takes them: any token not in `allowed`, or one that clashes with an
/// earlier one, is a syntax error. A token may come again; the last value
/// counts.
#[derive(Default)]
struct ExtOpts {
    cond: Option<Cond>,
    fcond: Option<Cond>,
    keep: bool,
    persist: bool,
    /// EX, PX, EXAT or PXAT, and its value.
    when: Option<(&'static str, Bytes)>,
}

fn ext_opts(tokens: &[Bytes], allowed: &[&'static str]) -> Result<ExtOpts, Reply> {
    let mut o = ExtOpts::default();
    let mut i = 0;
    while let Some(raw) = tokens.get(i) {
        let token = raw.to_ascii_uppercase();
        let Some(&name) = allowed.iter().find(|a| a.as_bytes() == token) else {
            return Err(syntax());
        };
        let cond = |c: &mut Option<Cond>, want: Cond| match c {
            Some(had) if *had != want => Err(syntax()),
            _ => {
                *c = Some(want);
                Ok(())
            }
        };
        match name {
            "NX" => cond(&mut o.cond, Cond::Nx)?,
            "XX" => cond(&mut o.cond, Cond::Xx)?,
            "FNX" => cond(&mut o.fcond, Cond::Nx)?,
            "FXX" => cond(&mut o.fcond, Cond::Xx)?,
            "KEEPTTL" if o.persist || o.when.is_some() => return Err(syntax()),
            "KEEPTTL" => o.keep = true,
            "PERSIST" if o.keep || o.when.is_some() => return Err(syntax()),
            "PERSIST" => o.persist = true,
            _ => {
                let other = o.when.as_ref().is_some_and(|(had, _)| *had != name);
                let value = tokens
                    .get(i + 1)
                    .filter(|_| !(o.keep || o.persist || other));
                let Some(value) = value else {
                    return Err(syntax());
                };
                o.when = Some((name, value.clone()));
                i += 1;
            }
        }
        i += 1;
    }
    Ok(o)
}

impl ExtOpts {
    fn ttl(self) -> FieldTtlArg {
        match self.when {
            Some((name, raw)) => FieldTtlArg::When {
                secs: !name.starts_with('P'),
                absolute: name.ends_with("AT"),
                raw,
            },
            None if self.keep => FieldTtlArg::Keep,
            None if self.persist => FieldTtlArg::Persist,
            None => FieldTtlArg::None,
        }
    }
}

fn key_cmd(args: &mut Vec<Bytes>, op: KeyOp) -> Cmd {
    args.truncate(2);
    Cmd::Key(args.pop().unwrap_or_default(), op)
}

/// Whether `n` arguments (the name included) can be right for `name`;
/// `None` when the command is not one of ours.
pub(crate) fn probe(name: &str, n: usize) -> Option<bool> {
    Some(match name {
        "LLEN" | "SMEMBERS" | "SCARD" | "ZCARD" | "HKEYS" | "HVALS" | "DUMP" => n == 2,
        "RESTORE" => n >= 4,
        "INCRBYFLOAT" | "SISMEMBER" | "ZSCORE" | "LINDEX" | "HSTRLEN" | "RENAME" | "RENAMENX"
        | "RPOPLPUSH" | "GETBIT" => n == 3,
        "GETRANGE" | "SUBSTR" | "SETRANGE" | "HINCRBYFLOAT" | "LRANGE" | "LSET" | "LREM"
        | "LTRIM" | "SMOVE" | "ZINCRBY" | "ZCOUNT" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE"
        | "ZLEXCOUNT" | "ZREMRANGEBYLEX" | "SETBIT" => n == 4,
        "ZRANDMEMBER" => (2..=4).contains(&n),
        "BITCOUNT" | "BITFIELD" | "BITFIELD_RO" | "GEOPOS" | "GEOHASH" | "SORT" | "SORT_RO" => {
            n >= 2
        }
        "GEOADD" | "GEORADIUSBYMEMBER" | "GEORADIUSBYMEMBER_RO" => n >= 5,
        "GEODIST" => n == 4 || n == 5,
        "GEORADIUS" | "GEORADIUS_RO" => n >= 6,
        "GEOSEARCH" => n >= 7,
        "GEOSEARCHSTORE" => n >= 8,
        "BITPOS" => (3..=6).contains(&n),
        "ZUNION" | "ZINTER" | "ZDIFF" | "ZINTERCARD" => n >= 3,
        "ZDIFFSTORE" | "LMPOP" | "ZMPOP" | "BITOP" | "ZRANGEBYLEX" | "ZREVRANGEBYLEX" => n >= 4,
        "ZRANGESTORE" | "BLMPOP" | "BZMPOP" => n >= 5,
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => n >= 3,
        "BRPOPLPUSH" => n == 4,
        "BLMOVE" => n == 6,
        "LINSERT" | "LMOVE" => n == 5,
        "LPOP" | "RPOP" | "SPOP" | "SRANDMEMBER" | "ZPOPMIN" | "ZPOPMAX" => n == 2 || n == 3,
        "ZRANK" | "ZREVRANK" => n == 3 || n == 4,
        "COPY" => n >= 3,
        "HRANDFIELD" => (2..=4).contains(&n),
        "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" => n >= 6,
        "HTTL" | "HPTTL" | "HEXPIRETIME" | "HPEXPIRETIME" | "HPERSIST" => n >= 5,
        "HGETDEL" | "HGETEX" => n >= 5,
        "HSETEX" => n >= 6,
        "DELIFEQ" => n == 3,
        "MSETEX" => n >= 4,
        "MEMORY" | "PFADD" | "PFCOUNT" | "PFMERGE" => n >= 2,
        "LCS" => n >= 3,
        "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "SADD" | "SREM" | "SMISMEMBER" | "ZREM"
        | "ZMSCORE" | "SSCAN" | "ZSCAN" | "LPOS" | "OBJECT" | "SDIFFSTORE" | "SINTERSTORE"
        | "SUNIONSTORE" | "SINTERCARD" => n >= 3,
        "SINTER" | "SUNION" | "SDIFF" => n >= 2,
        "MSETNX" => n >= 3 && n % 2 == 1,
        "ZADD" | "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZUNIONSTORE"
        | "ZINTERSTORE" => n >= 4,
        "XREAD" => n >= 4,
        "XREADGROUP" => n >= 7,
        // The stream parser counts the arguments of these itself.
        "XADD" | "XLEN" | "XRANGE" | "XREVRANGE" | "XDEL" | "XTRIM" | "XSETID" | "XGROUP"
        | "XACK" | "XPENDING" | "XCLAIM" | "XAUTOCLAIM" | "XINFO" => true,
        // So do these, as valkey-bloom does: the name upper case.
        "BF.ADD" | "BF.MADD" | "BF.EXISTS" | "BF.MEXISTS" | "BF.CARD" | "BF.RESERVE"
        | "BF.INSERT" | "BF.INFO" | "BF.LOAD" => true,
        // The JSON parser counts them, as valkey-json does.
        "JSON.SET" | "JSON.GET" | "JSON.MGET" | "JSON.MSET" | "JSON.MERGE" | "JSON.DEL"
        | "JSON.FORGET" | "JSON.NUMINCRBY" | "JSON.NUMMULTBY" | "JSON.TOGGLE" | "JSON.CLEAR"
        | "JSON.STRAPPEND" | "JSON.STRLEN" | "JSON.OBJLEN" | "JSON.OBJKEYS" | "JSON.ARRAPPEND"
        | "JSON.ARRINSERT" | "JSON.ARRPOP" | "JSON.ARRTRIM" | "JSON.ARRLEN" | "JSON.ARRINDEX"
        | "JSON.TYPE" | "JSON.RESP" | "JSON.DEBUG" => true,
        _ => return None,
    })
}

pub(crate) fn parse_known(name: &str, args: &mut Vec<Bytes>) -> Result<Cmd, Reply> {
    let n = args.len();
    let take = |args: &mut Vec<Bytes>, i: usize| std::mem::take(&mut args[i]);
    if name.starts_with('X') {
        return parse_stream(name, args);
    }
    if name.starts_with("BF.") {
        let (key, op) = bloom::parse(name, args)?;
        let cmd = BfCmd {
            op,
            // The engine puts in the settings when the command runs.
            settings: Settings::default(),
        };
        return Ok(Cmd::Key(key, KeyOp::Bloom(Box::new(cmd))));
    }
    if name.starts_with("JSON.") {
        return Ok(match json::parse(name, args)? {
            json::Parsed::Key(key, cmd) => Cmd::Key(key, KeyOp::Json(Box::new(cmd))),
            json::Parsed::MGet(keys, path) => {
                Cmd::Multi(Box::new(MultiOp::JsonMGet { keys, path }))
            }
            json::Parsed::MSet(triples) => Cmd::Multi(Box::new(MultiOp::JsonMSet(triples))),
            json::Parsed::Reply(reply) => Cmd::Static(reply),
        });
    }
    let op = match name {
        // Valkey checks the key's type and value before the increment.
        "INCRBYFLOAT" => KeyOp::IncrByFloat(take(args, 2)),
        "GETRANGE" | "SUBSTR" => KeyOp::GetRange(int(&args[2])?, int(&args[3])?),
        "SETRANGE" => {
            let offset = usize::try_from(int(&args[2])?)
                .map_err(|_| Reply::err("ERR offset is out of range"))?;
            KeyOp::SetRange(offset, take(args, 3))
        }
        "HKEYS" => KeyOp::HKeys,
        "HVALS" => KeyOp::HVals,
        "HSTRLEN" => KeyOp::HStrlen(take(args, 2)),
        "HINCRBYFLOAT" => KeyOp::HIncrByFloat(take(args, 2), take(args, 3)),
        "HEXPIRE" | "HPEXPIRE" | "HEXPIREAT" | "HPEXPIREAT" => {
            if int(&args[2])? < 0 {
                return Err(Reply::err("ERR invalid expire time, must be >= 0"));
            }
            let secs = !name.starts_with("HP");
            let when = parse_when(&args[2], secs, name.ends_with("AT"), false, name)?;
            let (cond, at) = if is(&args[3], "FIELDS") {
                (None, 3)
            } else {
                (Some(parse_cond(&args[3])?), 4)
            };
            KeyOp::HExpire {
                when,
                cond,
                fields: field_list(args, at)?,
            }
        }
        "HTTL" | "HPTTL" | "HEXPIRETIME" | "HPEXPIRETIME" => KeyOp::HTtl {
            fields: field_list(args, 2)?,
            millis: name.starts_with("HP"),
            absolute: name.ends_with("TIME"),
        },
        "HPERSIST" => KeyOp::HPersist(field_list(args, 2)?),
        "HGETDEL" => {
            if !is(&args[2], "FIELDS") {
                return Err(syntax());
            }
            KeyOp::HGetDel(numbered_fields(args, 2, 1)?)
        }
        "HGETEX" | "HSETEX" => {
            // The options end at the first FIELDS; the last argument is
            // never taken for it.
            let at = args[2..n - 1]
                .iter()
                .position(|a| is(a, "FIELDS"))
                .map(|i| i + 2)
                .ok_or_else(|| Reply::err(NUMFIELDS))?;
            if name == "HGETEX" {
                let opts = ext_opts(&args[2..at], &["EX", "PX", "EXAT", "PXAT", "PERSIST"])?;
                KeyOp::HGetEx {
                    ttl: opts.ttl(),
                    fields: numbered_fields(args, at, 1)?,
                }
            } else {
                let allowed = [
                    "NX", "XX", "FNX", "FXX", "KEEPTTL", "EX", "PX", "EXAT", "PXAT",
                ];
                let opts = ext_opts(&args[2..at], &allowed)?;
                let mut flat = numbered_fields(args, at, 2)?.into_iter();
                let mut pairs = Vec::with_capacity(flat.len() / 2);
                while let (Some(field), Some(value)) = (flat.next(), flat.next()) {
                    pairs.push((field, value));
                }
                KeyOp::HSetEx {
                    cond: opts.cond,
                    fcond: opts.fcond,
                    ttl: opts.ttl(),
                    pairs,
                }
            }
        }
        "DELIFEQ" => KeyOp::DelIfEq(take(args, 2)),
        "PFADD" => KeyOp::PfAdd(args.split_off(2)),
        "DUMP" => KeyOp::Dump,
        "RESTORE" => {
            let ttl = int(&args[2])?;
            if ttl < 0 {
                return Err(Reply::err("ERR Invalid TTL value, must be >= 0"));
            }
            let (mut absolute, mut replace) = (false, false);
            let mut rest = &args[4..];
            while let Some(option) = rest.first() {
                if is(option, "REPLACE") {
                    replace = true;
                } else if is(option, "ABSTTL") {
                    absolute = true;
                } else if (is(option, "IDLETIME") || is(option, "FREQ")) && rest.len() >= 2 {
                    // Accepted and ignored: there is no LRU clock or
                    // frequency counter to set.
                    let n = int(&rest[1])?;
                    if n < 0 || (is(option, "FREQ") && n > 255) {
                        return Err(Reply::err("ERR Invalid IDLETIME or FREQ value"));
                    }
                    rest = &rest[1..];
                } else {
                    return Err(syntax());
                }
                rest = &rest[1..];
            }
            KeyOp::Restore {
                ttl,
                absolute,
                replace,
                payload: take(args, 3),
            }
        }
        "MEMORY" => {
            return match args[1].to_ascii_uppercase().as_slice() {
                b"USAGE" if n == 3 || n == 5 => {
                    let samples = match args.get(3) {
                        None => 5,
                        Some(option) if is(option, "SAMPLES") => count(&args[4])?,
                        Some(_) => return Err(syntax()),
                    };
                    Ok(Cmd::Key(take(args, 2), KeyOp::MemoryUsage(samples)))
                }
                b"USAGE" => Err(arity("memory|usage")),
                b"PURGE" if n == 2 => Ok(Cmd::Static(Reply::Ok)),
                b"DOCTOR" if n == 2 => Ok(Cmd::Static(Reply::bulk(
                    "Sam, I have no memory problems to report.",
                ))),
                b"MALLOC-STATS" if n == 2 => Ok(Cmd::Static(Reply::bulk(
                    "Stats not supported for the current allocator",
                ))),
                b"HELP" if n == 2 => Ok(Cmd::Static(Reply::Array(vec![
                    Reply::bulk("MEMORY USAGE <key> [SAMPLES <count>]"),
                    Reply::bulk("MEMORY PURGE"),
                    Reply::bulk("MEMORY DOCTOR"),
                ]))),
                _ => Err(Reply::err("ERR unknown subcommand for 'memory'")),
            };
        }
        "HRANDFIELD" => {
            let count = match args.get(2) {
                Some(raw) => Some(repeat_count(raw)?),
                None => None,
            };
            let values = match args.get(3) {
                Some(option) if is(option, "WITHVALUES") => true,
                Some(_) => return Err(syntax()),
                None => false,
            };
            KeyOp::HRandField { count, values }
        }
        "OBJECT" => {
            let sub = match args[1].to_ascii_uppercase().as_slice() {
                b"ENCODING" => ObjectSub::Encoding,
                b"REFCOUNT" => ObjectSub::RefCount,
                b"IDLETIME" => ObjectSub::Zero,
                b"FREQ" => ObjectSub::Freq,
                b"HELP" => return Ok(Cmd::Static(Reply::Array(Vec::new()))),
                _ => return Err(Reply::err("ERR unknown subcommand for 'object'")),
            };
            if n != 3 {
                return Err(arity(&format!(
                    "object|{}",
                    String::from_utf8_lossy(&args[1]).to_ascii_lowercase()
                )));
            }
            return Ok(Cmd::Key(take(args, 2), KeyOp::Object(sub)));
        }
        "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" => KeyOp::Push {
            left: name.starts_with('L'),
            existing: name.ends_with('X'),
            items: args.split_off(2),
        },
        "LPOP" | "RPOP" => KeyOp::Pop {
            left: name == "LPOP",
            count: match args.get(2) {
                Some(raw) => Some(count(raw)?),
                None => None,
            },
        },
        "LLEN" => KeyOp::LLen,
        "LRANGE" => KeyOp::LRange(int(&args[2])?, int(&args[3])?),
        "LTRIM" => KeyOp::LTrim(int(&args[2])?, int(&args[3])?),
        "LINDEX" => KeyOp::LIndex(int(&args[2])?),
        "LSET" => KeyOp::LSet(int(&args[2])?, take(args, 3)),
        "LREM" => KeyOp::LRem(int(&args[2])?, take(args, 3)),
        "LINSERT" => {
            let before = if is(&args[2], "BEFORE") {
                true
            } else if is(&args[2], "AFTER") {
                false
            } else {
                return Err(syntax());
            };
            KeyOp::LInsert {
                before,
                pivot: take(args, 3),
                item: take(args, 4),
            }
        }
        "LPOS" => {
            let (mut rank, mut hits, mut maxlen) = (1, None, 0);
            let mut rest = &args[3..];
            while let Some(option) = rest.first() {
                let value = rest.get(1).ok_or_else(syntax)?;
                if is(option, "RANK") {
                    rank = int(value)?;
                    if rank == 0 || rank == i64::MIN {
                        return Err(Reply::err(
                            "ERR RANK can't be zero: use 1 to start from the first match, 2 from \
                             the second ... or use negative to start from the end of the list",
                        ));
                    }
                } else if is(option, "COUNT") {
                    hits = Some(
                        usize::try_from(int(value)?)
                            .map_err(|_| Reply::err("ERR COUNT can't be negative"))?,
                    );
                } else if is(option, "MAXLEN") {
                    maxlen = usize::try_from(int(value)?)
                        .map_err(|_| Reply::err("ERR MAXLEN can't be negative"))?;
                } else {
                    return Err(syntax());
                }
                rest = &rest[2..];
            }
            KeyOp::LPos {
                item: take(args, 2),
                rank,
                count: hits,
                maxlen,
            }
        }
        "SADD" => KeyOp::SAdd(args.split_off(2)),
        "SREM" => KeyOp::SRem(args.split_off(2)),
        "SISMEMBER" => KeyOp::SIsMember(take(args, 2)),
        "SMISMEMBER" => KeyOp::SMIsMember(args.split_off(2)),
        "SMEMBERS" => KeyOp::SMembers,
        "SCARD" => KeyOp::SCard,
        "SPOP" => KeyOp::SPop(match args.get(2) {
            Some(raw) => Some(count(raw)?),
            None => None,
        }),
        "SRANDMEMBER" => KeyOp::SRandMember(match args.get(2) {
            Some(raw) => Some(repeat_count(raw)?),
            None => None,
        }),
        "SSCAN" | "ZSCAN" => {
            let cursor = cursor(&args[2])?;
            let (pattern, count) = scan_opts(&args[3..])?;
            if name == "SSCAN" {
                KeyOp::SScan {
                    cursor,
                    pattern,
                    count,
                }
            } else {
                KeyOp::ZScan {
                    cursor,
                    pattern,
                    count,
                }
            }
        }
        "ZADD" => parse_zadd(args)?,
        "ZREM" => KeyOp::ZRem(args.split_off(2)),
        "ZSCORE" => KeyOp::ZScore(take(args, 2)),
        "ZMSCORE" => KeyOp::ZMScore(args.split_off(2)),
        "ZINCRBY" => {
            let by = float(&args[2])?;
            KeyOp::ZIncrBy(by, take(args, 3))
        }
        "ZCARD" => KeyOp::ZCard,
        "ZCOUNT" => KeyOp::ZCount(zbound(&args[2])?, zbound(&args[3])?),
        "ZRANK" | "ZREVRANK" => {
            let withscore = match args.get(3) {
                Some(option) if is(option, "WITHSCORE") => true,
                Some(_) => return Err(syntax()),
                None => false,
            };
            KeyOp::ZRank {
                member: take(args, 2),
                rev: name == "ZREVRANK",
                withscore,
            }
        }
        "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" => {
            KeyOp::ZRange(Box::new(parse_zrange(name, args, 4)?))
        }
        "ZLEXCOUNT" => KeyOp::ZLexCount(lexbound(&args[2])?, lexbound(&args[3])?),
        "ZREMRANGEBYLEX" => KeyOp::ZRemRange(ZBy::Lex(lexbound(&args[2])?, lexbound(&args[3])?)),
        "ZRANGEBYLEX" | "ZREVRANGEBYLEX" => KeyOp::ZRange(Box::new(parse_zrange(name, args, 4)?)),
        "ZRANDMEMBER" => {
            let count = match args.get(2) {
                Some(raw) => Some(repeat_count(raw)?),
                None => None,
            };
            let withscores = match args.get(3) {
                Some(option) if is(option, "WITHSCORES") => true,
                Some(_) => return Err(syntax()),
                None => false,
            };
            KeyOp::ZRandMember { count, withscores }
        }
        "SETBIT" | "GETBIT" => {
            // Redis' limit: a string of 512 MB.
            let offset = usize::try_from(int(&args[2]).unwrap_or(-1))
                .ok()
                .filter(|&bit| bit < 1 << 32)
                .ok_or_else(|| Reply::err("ERR bit offset is not an integer or out of range"))?;
            if name == "GETBIT" {
                KeyOp::GetBit(offset)
            } else {
                let bit = match args[3].as_slice() {
                    b"0" => false,
                    b"1" => true,
                    _ => return Err(Reply::err("ERR bit is not an integer or out of range")),
                };
                KeyOp::SetBit(offset, bit)
            }
        }
        "BITFIELD" | "BITFIELD_RO" => {
            KeyOp::BitField(bitfield::parse_ops(&args[2..], name == "BITFIELD_RO")?)
        }
        "GEOADD" => {
            let (nx, xx, ch, pairs) = geo::parse_add(&args[2..])?;
            let flags = ZAddFlags {
                nx,
                xx,
                ch,
                ..ZAddFlags::default()
            };
            KeyOp::ZAdd { flags, pairs }
        }
        "GEOPOS" => KeyOp::Geo(Box::new(GeoOp::Pos(args.split_off(2)))),
        "GEOHASH" => KeyOp::Geo(Box::new(GeoOp::Hash(args.split_off(2)))),
        "GEODIST" => {
            let unit = match args.get(4) {
                Some(raw) => geo::unit(raw)?,
                None => geo::Unit::M,
            };
            let to = take(args, 3);
            KeyOp::Geo(Box::new(GeoOp::Dist(take(args, 2), to, unit)))
        }
        "GEOSEARCH" => KeyOp::Geo(Box::new(GeoOp::Search(
            geo::parse_search(&args[2..], false)?.query,
        ))),
        "GEORADIUS" | "GEORADIUS_RO" | "GEORADIUSBYMEMBER" | "GEORADIUSBYMEMBER_RO" => {
            let by_member = name.starts_with("GEORADIUSBYMEMBER");
            let parsed = geo::parse_radius(&args[2..], by_member, name.ends_with("_RO"))?;
            match parsed.store {
                Some(dst) => {
                    return Ok(Cmd::Multi(Box::new(MultiOp::GeoStore {
                        dst,
                        src: take(args, 1),
                        query: Box::new(parsed.query),
                        storedist: parsed.storedist,
                    })))
                }
                None => KeyOp::Geo(Box::new(GeoOp::Search(parsed.query))),
            }
        }
        "BITCOUNT" if n > 5 => return Err(syntax()),
        "BITCOUNT" => KeyOp::BitCount(match args.get(2) {
            Some(start) => {
                let end = args.get(3).map_or(Ok(-1), |raw| int(raw))?;
                Some((int(start)?, end, bit_unit(args.get(4))?))
            }
            None => None,
        }),
        "BITPOS" => {
            let bit = match int(&args[2])? {
                0 => false,
                1 => true,
                _ => return Err(Reply::err("ERR The bit argument must be 1 or 0.")),
            };
            KeyOp::BitPos {
                bit,
                start: args.get(3).map_or(Ok(0), |raw| int(raw))?,
                end: match args.get(4) {
                    Some(raw) => Some(int(raw)?),
                    None => None,
                },
                bits: bit_unit(args.get(5))?,
            }
        }
        "ZREMRANGEBYRANK" => KeyOp::ZRemRange(ZBy::Rank(int(&args[2])?, int(&args[3])?)),
        "ZREMRANGEBYSCORE" => KeyOp::ZRemRange(ZBy::Score(zbound(&args[2])?, zbound(&args[3])?)),
        "ZPOPMIN" | "ZPOPMAX" => KeyOp::ZPop {
            max: name == "ZPOPMAX",
            count: match args.get(2) {
                Some(raw) => Some(count(raw)?),
                None => None,
            },
        },
        "BLPOP" | "BRPOP" | "BRPOPLPUSH" | "BLMOVE" | "BLMPOP" | "BZPOPMIN" | "BZPOPMAX"
        | "BZMPOP" => return parse_block(name, args),
        _ => return parse_multi(name, args).map(|op| Cmd::Multi(Box::new(op))),
    };
    Ok(key_cmd(args, op))
}

/// Longer than this a timeout cannot be: a hundred years.
const MAX_TIMEOUT_SECS: f64 = 3.2e9;

fn timeout_ms(raw: &[u8]) -> Result<u64, Reply> {
    let secs = parse_float(raw)
        .filter(|secs| *secs <= MAX_TIMEOUT_SECS)
        .ok_or_else(|| Reply::err("ERR timeout is not a float or out of range"))?;
    if secs < 0.0 {
        return Err(Reply::err("ERR timeout is negative"));
    }
    Ok((secs * 1000.0).ceil() as u64)
}

fn parse_block(name: &str, args: &mut Vec<Bytes>) -> Result<Cmd, Reply> {
    // BLMPOP and BZMPOP name the timeout first, the others last.
    let at = if name.ends_with("MPOP") {
        1
    } else {
        args.len() - 1
    };
    let timeout = timeout_ms(&args.remove(at))?;
    let op = match name {
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => MultiOp::MPop {
            keys: args.split_off(1),
            zset: name.starts_with("BZ"),
            first: name == "BLPOP" || name == "BZPOPMIN",
            count: 1,
            flat: true,
        },
        "BRPOPLPUSH" => parse_multi("RPOPLPUSH", args)?,
        // BLMOVE, BLMPOP and BZMPOP are their plain forms plus the timeout.
        other => parse_multi(&other[1..], args)?,
    };
    Ok(Cmd::Block(Box::new(op), timeout))
}

fn parse_stream(name: &str, args: &[Bytes]) -> Result<Cmd, Reply> {
    if name != "XREAD" && name != "XREADGROUP" {
        let (key, op) = stream::parse(name, args)?;
        return Ok(Cmd::Key(key, KeyOp::X(Box::new(op))));
    }
    let grouped = name == "XREADGROUP";
    let mut group = None;
    let mut count = None;
    let mut block = None;
    let mut noack = false;
    let mut i = 1;
    let streams = loop {
        let Some(option) = args.get(i) else {
            return Err(syntax());
        };
        if is(option, "STREAMS") {
            break &args[i + 1..];
        }
        match args.get(i + 1) {
            Some(raw) if is(option, "COUNT") => {
                // Zero and below: no limit.
                count = Some(usize::try_from(int(raw)?).unwrap_or(0));
                i += 2;
            }
            Some(raw) if is(option, "BLOCK") => {
                let ms = parse_i64(raw)
                    .ok_or_else(|| Reply::err("ERR timeout is not an integer or out of range"))?;
                let ms = u64::try_from(ms).map_err(|_| Reply::err("ERR timeout is negative"))?;
                block = Some(ms);
                i += 2;
            }
            Some(_) if grouped && is(option, "GROUP") && i + 2 < args.len() => {
                group = Some((args[i + 1].clone(), args[i + 2].clone()));
                i += 3;
            }
            _ if is(option, "NOACK") => {
                if !grouped {
                    return Err(Reply::err(
                        "ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.",
                    ));
                }
                noack = true;
                i += 1;
            }
            _ => return Err(syntax()),
        }
    };
    if streams.is_empty() {
        return Err(syntax());
    }
    if streams.len() % 2 != 0 {
        return Err(Reply::err(format!(
            "ERR Unbalanced '{}' list of streams: for each stream key an ID, '+', or '$' must be specified.",
            name.to_ascii_lowercase()
        )));
    }
    let (keys, raw_ids) = streams.split_at(streams.len() / 2);
    let ids = match group {
        Some((group, consumer)) => XIds::Group {
            group,
            consumer,
            ids: raw_ids
                .iter()
                .map(|raw| match raw.as_slice() {
                    b"$" => Err(Reply::err(
                        "ERR The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.",
                    )),
                    raw => stream::parse_group_read_id(raw),
                })
                .collect::<Result<_, _>>()?,
        },
        None if grouped => return Err(Reply::err("ERR Missing GROUP option for XREADGROUP")),
        None => XIds::After(
            raw_ids
                .iter()
                .map(|raw| match raw.as_slice() {
                    b">" => Err(Reply::err(
                        "ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
                    )),
                    raw => stream::parse_read_id(raw),
                })
                .collect::<Result<_, _>>()?,
        ),
    };
    let op = MultiOp::XRead {
        keys: keys.to_vec(),
        ids,
        count,
        noack,
        start: Arc::default(),
    };
    Ok(match block {
        Some(ms) => Cmd::Block(Box::new(op), ms),
        None => Cmd::Multi(Box::new(op)),
    })
}

fn parse_zadd(args: &[Bytes]) -> Result<KeyOp, Reply> {
    let mut flags = ZAddFlags::default();
    let mut i = 2;
    while let Some(option) = args.get(i) {
        match option.to_ascii_uppercase().as_slice() {
            b"NX" => flags.nx = true,
            b"XX" => flags.xx = true,
            b"GT" => flags.gt = true,
            b"LT" => flags.lt = true,
            b"CH" => flags.ch = true,
            b"INCR" => flags.incr = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &args[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return Err(syntax());
    }
    if flags.nx && flags.xx {
        return Err(Reply::err(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }
    if (flags.gt && flags.lt) || (flags.nx && (flags.gt || flags.lt)) {
        return Err(Reply::err(
            "ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if flags.incr && rest.len() != 2 {
        return Err(Reply::err(
            "ERR INCR option supports a single increment-element pair",
        ));
    }
    let pairs = rest
        .chunks(2)
        .map(|pair| Ok((float(&pair[0])?, pair[1].clone())))
        .collect::<Result<_, Reply>>()?;
    Ok(KeyOp::ZAdd { flags, pairs })
}

/// The range of a ZRANGE-like command whose options start at `args[opts]`
/// and whose two bounds come right before them.
fn parse_zrange(name: &str, args: &[Bytes], opts: usize) -> Result<ZRangeSpec, Reply> {
    let general = name == "ZRANGE" || name == "ZRANGESTORE";
    let mut by_score = name.ends_with("BYSCORE");
    let mut by_lex = name.ends_with("BYLEX");
    let mut rev = name.starts_with("ZREV");
    let (mut withscores, mut lim) = (false, None);
    let mut i = opts;
    while let Some(option) = args.get(i) {
        if is(option, "WITHSCORES") && name != "ZRANGESTORE" && !name.ends_with("BYLEX") {
            withscores = true;
        } else if is(option, "LIMIT") {
            lim = Some(limit(&args[i..])?);
            i += 2;
        } else if general && is(option, "BYSCORE") {
            by_score = true;
        } else if general && is(option, "BYLEX") {
            by_lex = true;
        } else if general && is(option, "REV") {
            rev = true;
        } else {
            return Err(syntax());
        }
        i += 1;
    }
    // In reverse the arguments come highest first.
    let (min, max) = if rev {
        (opts - 1, opts - 2)
    } else {
        (opts - 2, opts - 1)
    };
    let by = if by_score && by_lex {
        return Err(syntax());
    } else if by_lex {
        if withscores {
            return Err(Reply::err(
                "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
            ));
        }
        ZBy::Lex(lexbound(&args[min])?, lexbound(&args[max])?)
    } else if by_score {
        ZBy::Score(zbound(&args[min])?, zbound(&args[max])?)
    } else {
        if lim.is_some() {
            return Err(Reply::err(
                "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or \
                 BYLEX",
            ));
        }
        ZBy::Rank(int(&args[opts - 2])?, int(&args[opts - 1])?)
    };
    Ok(ZRangeSpec {
        by,
        rev,
        limit: lim,
        withscores,
    })
}

fn parse_multi(name: &str, args: &mut Vec<Bytes>) -> Result<MultiOp, Reply> {
    let n = args.len();
    let take = |args: &mut Vec<Bytes>, i: usize| std::mem::take(&mut args[i]);
    let side = |raw: &[u8]| {
        if is(raw, "LEFT") {
            Ok(true)
        } else if is(raw, "RIGHT") {
            Ok(false)
        } else {
            Err(syntax())
        }
    };
    Ok(match name {
        "RENAME" | "RENAMENX" => MultiOp::Rename {
            src: take(args, 1),
            dst: take(args, 2),
            nx: name == "RENAMENX",
        },
        "COPY" => {
            let mut replace = false;
            let mut db = None;
            let mut rest = &args[3..];
            while let Some(option) = rest.first() {
                if is(option, "REPLACE") {
                    replace = true;
                    rest = &rest[1..];
                } else if is(option, "DB") && rest.len() >= 2 {
                    db = Some(int(&rest[1])?);
                    rest = &rest[2..];
                } else {
                    return Err(syntax());
                }
            }
            MultiOp::Copy {
                src: take(args, 1),
                dst: take(args, 2),
                replace,
                db,
            }
        }
        "LCS" => {
            let mut opts = LcsOpts::default();
            let mut rest = &args[3..];
            while let Some(option) = rest.first() {
                if is(option, "LEN") {
                    opts.len = true;
                } else if is(option, "IDX") {
                    opts.idx = true;
                } else if is(option, "WITHMATCHLEN") {
                    opts.with_match_len = true;
                } else if is(option, "MINMATCHLEN") && rest.len() >= 2 {
                    opts.min_match_len = usize::try_from(int(&rest[1])?).unwrap_or(0);
                    rest = &rest[1..];
                } else {
                    return Err(syntax());
                }
                rest = &rest[1..];
            }
            if opts.len && opts.idx {
                return Err(Reply::err(
                    "ERR If you want both the length and indexes, please just use IDX.",
                ));
            }
            MultiOp::Lcs {
                a: take(args, 1),
                b: take(args, 2),
                opts,
            }
        }
        "PFCOUNT" => MultiOp::PfCount(args.split_off(1)),
        "PFMERGE" => {
            let keys = args.split_off(2);
            MultiOp::PfMerge {
                dst: take(args, 1),
                keys,
            }
        }
        "MSETEX" => {
            let keys = parse_i64(&args[1])
                .and_then(|k| usize::try_from(k).ok())
                .filter(|k| (1..=i32::MAX as usize).contains(k))
                .ok_or_else(|| Reply::err("ERR invalid numkeys value or out of range"))?;
            let end = keys
                .checked_mul(2)
                .and_then(|k| k.checked_add(2))
                .filter(|end| *end <= n)
                .ok_or_else(syntax)?;
            let opts = ext_opts(
                &args[end..],
                &["NX", "XX", "KEEPTTL", "EX", "PX", "EXAT", "PXAT"],
            )?;
            let cond = opts.cond;
            let expire = match opts.ttl() {
                FieldTtlArg::When {
                    secs,
                    absolute,
                    raw,
                } => parse_when(&raw, secs, absolute, true, name)?,
                FieldTtlArg::Keep => Expire::Keep,
                FieldTtlArg::None | FieldTtlArg::Persist => Expire::Clear,
            };
            let mut pairs = Vec::with_capacity(keys);
            let mut rest = args.drain(2..end);
            while let (Some(k), Some(v)) = (rest.next(), rest.next()) {
                pairs.push((k, v));
            }
            MultiOp::MSetEx {
                pairs,
                cond,
                expire,
            }
        }
        "MSETNX" => {
            let mut pairs = Vec::with_capacity((n - 1) / 2);
            let mut rest = args.drain(1..);
            while let (Some(k), Some(v)) = (rest.next(), rest.next()) {
                pairs.push((k, v));
            }
            MultiOp::MSetNx(pairs)
        }
        "RPOPLPUSH" => MultiOp::LMove {
            src: take(args, 1),
            dst: take(args, 2),
            from_left: false,
            to_left: true,
        },
        "LMOVE" => MultiOp::LMove {
            from_left: side(&args[3])?,
            to_left: side(&args[4])?,
            src: take(args, 1),
            dst: take(args, 2),
        },
        "SMOVE" => MultiOp::SMove {
            src: take(args, 1),
            dst: take(args, 2),
            member: take(args, 3),
        },
        "SINTER" | "SUNION" | "SDIFF" | "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE" => {
            let op = match &name[..4] {
                "SINT" => SetOp::Inter,
                "SUNI" => SetOp::Union,
                _ => SetOp::Diff,
            };
            let store = name.ends_with("STORE").then(|| args.remove(1));
            MultiOp::Sets {
                op,
                keys: args.split_off(1),
                store,
                card: None,
            }
        }
        "SINTERCARD" => {
            let numkeys = usize::try_from(int(&args[1])?)
                .ok()
                .filter(|&k| k > 0)
                .ok_or_else(|| Reply::err("ERR numkeys should be greater than 0"))?;
            if numkeys > n - 2 {
                return Err(Reply::err(
                    "ERR Number of keys can't be greater than number of args",
                ));
            }
            let card = match &args[2 + numkeys..] {
                [] => usize::MAX,
                [option, raw] if is(option, "LIMIT") => match int(raw)? {
                    0 => usize::MAX,
                    limit => usize::try_from(limit)
                        .map_err(|_| Reply::err("ERR LIMIT can't be negative"))?,
                },
                _ => return Err(syntax()),
            };
            args.truncate(2 + numkeys);
            MultiOp::Sets {
                op: SetOp::Inter,
                keys: args.split_off(2),
                store: None,
                card: Some(card),
            }
        }
        "GEOSEARCHSTORE" => {
            let parsed = geo::parse_search(&args[3..], true)?;
            MultiOp::GeoStore {
                dst: take(args, 1),
                src: take(args, 2),
                query: Box::new(parsed.query),
                storedist: parsed.storedist,
            }
        }
        "SORT" | "SORT_RO" => {
            let spec = sort::Spec::parse(&args[2..], name == "SORT_RO")?;
            MultiOp::Sort {
                key: take(args, 1),
                spec: Box::new(spec),
            }
        }
        "ZRANGESTORE" => {
            let spec = parse_zrange(name, args, 5)?;
            MultiOp::ZRangeStore {
                dst: take(args, 1),
                src: take(args, 2),
                spec,
            }
        }
        "LMPOP" | "ZMPOP" => {
            let zset = name == "ZMPOP";
            let numkeys = numkeys(args, 1)?;
            let rest = &args[2 + numkeys..];
            let (first, last) = if zset {
                ("MIN", "MAX")
            } else {
                ("LEFT", "RIGHT")
            };
            let first = match rest.first() {
                Some(end) if is(end, first) => true,
                Some(end) if is(end, last) => false,
                _ => return Err(syntax()),
            };
            let count = match &rest[1..] {
                [] => 1,
                [option, raw] if is(option, "COUNT") => usize::try_from(int(raw)?)
                    .ok()
                    .filter(|&c| c > 0)
                    .ok_or_else(|| Reply::err("ERR count should be greater than 0"))?,
                _ => return Err(syntax()),
            };
            args.truncate(2 + numkeys);
            MultiOp::MPop {
                keys: args.split_off(2),
                zset,
                first,
                count,
                flat: false,
            }
        }
        "BITOP" => {
            let op = match args[1].to_ascii_uppercase().as_slice() {
                b"AND" => BitOp::And,
                b"OR" => BitOp::Or,
                b"XOR" => BitOp::Xor,
                b"NOT" => BitOp::Not,
                _ => return Err(syntax()),
            };
            if op == BitOp::Not && n != 4 {
                return Err(Reply::err(
                    "ERR BITOP NOT must be called with a single source key.",
                ));
            }
            let keys = args.split_off(3);
            MultiOp::BitOp {
                op,
                dst: take(args, 2),
                keys,
            }
        }
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" | "ZUNION" | "ZINTER" | "ZDIFF"
        | "ZINTERCARD" => {
            let store = name.ends_with("STORE");
            let card_only = name == "ZINTERCARD";
            // Where `numkeys` is.
            let at = if store { 2 } else { 1 };
            let op = match &name[..4] {
                "ZUNI" => SetOp::Union,
                "ZINT" => SetOp::Inter,
                _ => SetOp::Diff,
            };
            let numkeys = usize::try_from(int(&args[at])?)
                .ok()
                .filter(|&k| k > 0)
                .ok_or_else(|| {
                    Reply::err(format!(
                        "ERR at least 1 input key is needed for '{}' command",
                        name.to_ascii_lowercase()
                    ))
                })?;
            if numkeys > n - at - 1 {
                return Err(syntax());
            }
            let mut weights = vec![1.0; numkeys];
            let mut aggregate = Aggregate::Sum;
            let mut withscores = false;
            let mut card = card_only.then_some(usize::MAX);
            let mut rest = &args[at + 1 + numkeys..];
            let mut options = || -> Result<(), Reply> {
                while let Some(option) = rest.first() {
                    let combines = op != SetOp::Diff && !card_only;
                    if combines && is(option, "WEIGHTS") && rest.len() > numkeys {
                        for (w, raw) in weights.iter_mut().zip(&rest[1..]) {
                            *w = parse_float(raw)
                                .ok_or_else(|| Reply::err("ERR weight value is not a float"))?;
                        }
                        rest = &rest[1 + numkeys..];
                    } else if combines && is(option, "AGGREGATE") && rest.len() >= 2 {
                        aggregate = match rest[1].to_ascii_uppercase().as_slice() {
                            b"SUM" => Aggregate::Sum,
                            b"MIN" => Aggregate::Min,
                            b"MAX" => Aggregate::Max,
                            _ => return Err(syntax()),
                        };
                        rest = &rest[2..];
                    } else if is(option, "WITHSCORES") && !store && !card_only {
                        withscores = true;
                        rest = &rest[1..];
                    } else if card_only && is(option, "LIMIT") && rest.len() >= 2 {
                        let limit = parse_i64(&rest[1])
                            .and_then(|limit| usize::try_from(limit).ok())
                            .ok_or_else(|| Reply::err("ERR LIMIT can't be negative"))?;
                        card = Some(if limit == 0 { usize::MAX } else { limit });
                        rest = &rest[2..];
                    } else {
                        return Err(syntax());
                    }
                }
                Ok(())
            };
            let bad_option = options().err();
            args.truncate(at + 1 + numkeys);
            let keys = args.split_off(at + 1);
            MultiOp::ZStore {
                dst: store.then(|| take(args, 1)),
                keys,
                weights,
                aggregate,
                op,
                withscores,
                card,
                bad_option,
            }
        }
        _ => return Err(Reply::err("ERR internal: unparsed command")),
    })
}
