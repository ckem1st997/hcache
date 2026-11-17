//! The payload of DUMP and RESTORE: one value in Redis's RDB encoding.
//!
//! We write the oldest plain form of each type (no LZF, no packed
//! encodings), so any Redis that accepts our footer version loads it. We
//! read more: everything a real Redis DUMP holds for the five types this
//! server shares with RDB (integer and LZF strings, zipmaps, ziplists,
//! intsets, listpacks, quicklists).
//!
//! Streams and hashes whose fields have deadlines are read and written by
//! their owners (`stream.rs`, `shard/dump.rs`) with the pieces here: the
//! `Input` readers, `put_len` and friends, and `Listpack`. Whole RDB files
//! are `rdbfile.rs`.
//!
//! RESTORE hands us bytes a client controls, so nothing here trusts them:
//! every length is checked against the bytes left before anything is
//! allocated, nothing recurses, and the work is linear in the input (an
//! LZF string can expand, but only as far as LZF itself can).
//!
//! Payload: `type, value, u16 version, u64 crc64(everything before)`, the
//! footer little-endian.

use std::collections::HashSet;

/// A value in the shapes RDB knows that this server also has.
#[derive(Debug, Clone, PartialEq)]
pub enum Plain {
    Str(Vec<u8>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    /// Member and score.
    ZSet(Vec<(Vec<u8>, f64)>),
    /// Field and value.
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Too short, wrong checksum, or an RDB version newer than we write.
    /// RESTORE answers "ERR DUMP payload version or checksum are wrong".
    Envelope,
    /// The envelope is right but the body is malformed or of a type this
    /// module does not read. RESTORE answers "ERR Bad data format".
    Body,
}

/// The RDB version written in the footer. RDB 8 (Redis 4.0) introduced `TYPE_ZSET_2`, the newest thing we
/// write; a higher number would make older servers refuse our payloads.
pub const VERSION: u16 = 8;

/// The newest RDB version `open` accepts: that of Redis 7.4. Payloads of
/// Redis 7 come in, and ours, written as `VERSION`, go to any Redis since 4.0.
pub const MAX_READ_VERSION: u16 = 12;

/// RDB 11 (Redis 7.2, Valkey 7.2 to 8.1) brought the stream type we write.
pub const VERSION_11: u16 = 11;
/// RDB 80 (Valkey 9) brought hashes whose fields have deadlines. Versions
/// 13 to 79 belong to Redis forks whose formats we do not read.
pub const VERSION_80: u16 = 80;

/// Type bytes at and above this are never used by Redis; the caller uses
/// them for its own private formats.
pub const PRIVATE_TYPES: u8 = 0xE0;

const VERSION_BYTES: usize = 2;
const CRC_BYTES: usize = 8;
const FOOTER: usize = VERSION_BYTES + CRC_BYTES;

const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
/// Scores as length-prefixed ASCII.
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
/// Scores as binary doubles.
const TYPE_ZSET_2: u8 = 5;
const TYPE_HASH_ZIPMAP: u8 = 9;
const TYPE_LIST_ZIPLIST: u8 = 10;
const TYPE_SET_INTSET: u8 = 11;
const TYPE_ZSET_ZIPLIST: u8 = 12;
const TYPE_HASH_ZIPLIST: u8 = 13;
const TYPE_LIST_QUICKLIST: u8 = 14;
const TYPE_HASH_LISTPACK: u8 = 16;
const TYPE_ZSET_LISTPACK: u8 = 17;
const TYPE_LIST_QUICKLIST_2: u8 = 18;
const TYPE_SET_LISTPACK: u8 = 20;
pub(crate) const TYPE_STREAM_LISTPACKS: u8 = 15;
/// Adds the first and the largest deleted ID, the count of entries ever
/// added, and how far each group has read.
pub(crate) const TYPE_STREAM_LISTPACKS_2: u8 = 19;
/// Adds when each consumer last got an entry.
pub(crate) const TYPE_STREAM_LISTPACKS_3: u8 = 21;
/// A hash whose fields carry a deadline each (RDB 80).
pub(crate) const TYPE_HASH_2: u8 = 22;
/// The deadline of a field that has none, in a `TYPE_HASH_2`.
pub(crate) const NO_DEADLINE: i64 = -1;

// Length encoding: the top two bits of the first byte say which form.
const LEN_6BIT: u8 = 0;
const LEN_14BIT: u8 = 1;
/// The rest of the byte says what kind of special string follows.
const LEN_ENCVAL: u8 = 3;
const LEN_32BIT: u8 = 0x80;
const LEN_64BIT: u8 = 0x81;
const ENCVAL_PREFIX: u8 = LEN_ENCVAL << 6;
const ENC_INT8: u8 = 0;
const ENC_INT16: u8 = 1;
const ENC_INT32: u8 = 2;
const ENC_LZF: u8 = 3;

/// Longest decimal form of an i64 ("-9223372036854775808").
const MAX_INT_TEXT: usize = 20;

// Length byte of an ASCII score (`TYPE_ZSET`) that stands for a value.
const SCORE_NAN: u8 = 253;
const SCORE_POS_INF: u8 = 254;
const SCORE_NEG_INF: u8 = 255;

/// The largest string an LZF blob may expand to, and what one payload may
/// decode to beyond what its own bytes back (`budget`).
pub(crate) const LZF_MAX_PLAIN: usize = 512 << 20;
/// What a decoded item is charged on top of its bytes: the `Vec` that holds
/// it and the allocation behind it.
const ITEM_COST: usize = 48;
/// What each byte of a payload adds to its budget: enough for any payload
/// without LZF, since every item takes at least one byte of it. Only LZF
/// can make a payload decode to more than this, and only up to
/// `LZF_MAX_PLAIN`.
const BYTE_CREDIT: usize = 64;
/// The most room reserved on the word of a count; beyond it a collection
/// grows as its items actually arrive.
const MAX_RESERVE: usize = 4096;
/// The most output one input byte can yield: a long back reference is
/// 3 bytes and copies at most 7 + 255 + 2 = 264.
const LZF_MAX_RATIO: usize = 88;
/// Control bytes below this start a literal run of `ctrl + 1` bytes.
const LZF_LITERAL_LIMIT: usize = 1 << 5;
/// A back reference whose 3-bit length is this reads one more length byte.
const LZF_LONG_MATCH: usize = 7;
const LZF_MIN_MATCH: usize = 2;

// Quicklist 2 node containers.
const QUICKLIST_PLAIN: u64 = 1;
const QUICKLIST_PACKED: u64 = 2;

// Ziplist: `u32 bytes, u32 tail offset, u16 count`, entries, 0xFF. An
// entry is `prevlen, encoding, data`.
const ZIPLIST_HEADER: usize = 10;
const ZIP_END: u8 = 0xFF;
/// A prevlen byte of this value is followed by the length as a u32.
const ZIP_BIG_PREVLEN: u8 = 254;
/// A count that means "too many to say; walk the list".
const ZIP_UNKNOWN_COUNT: u16 = u16::MAX;
// String encodings, by the top two bits.
const ZIP_STR_06B: u8 = 0;
const ZIP_STR_14B: u8 = 1;
const ZIP_STR_32B: u8 = 0x80;
const ZIP_INT_16B: u8 = 0xC0;
const ZIP_INT_32B: u8 = 0xD0;
const ZIP_INT_64B: u8 = 0xE0;
const ZIP_INT_24B: u8 = 0xF0;
const ZIP_INT_8B: u8 = 0xFE;
/// 0xF1..=0xFD carry the values 0..=12 in their low four bits, plus one.
const ZIP_INT_IMM_MIN: u8 = 0xF1;
const ZIP_INT_IMM_MAX: u8 = 0xFD;

// Zipmap: `u8 count`, then `len field len free value padding`, 0xFF.
const ZIPMAP_BIGLEN: u8 = 254;
const ZIPMAP_END: u8 = 0xFF;

// Intset: `u32 width, u32 count`, sorted little-endian integers.
const INTSET_ENC_INT16: usize = 2;
const INTSET_ENC_INT32: usize = 4;
const INTSET_ENC_INT64: usize = 8;

// Listpack: `u32 bytes, u16 count`, entries, 0xFF. An entry is
// `encoding, data, backlen`.
const LP_EOF: u8 = 0xFF;
const LP_UNKNOWN_COUNT: u16 = u16::MAX;
const LP_7BIT_UINT_MASK: u8 = 0x80;
const LP_7BIT_UINT: u8 = 0;
const LP_6BIT_STR_MASK: u8 = 0xC0;
const LP_6BIT_STR: u8 = 0x80;
const LP_13BIT_INT_MASK: u8 = 0xE0;
const LP_13BIT_INT: u8 = 0xC0;
const LP_12BIT_STR_MASK: u8 = 0xF0;
const LP_12BIT_STR: u8 = 0xE0;
const LP_32BIT_STR: u8 = 0xF0;
const LP_INT16: u8 = 0xF1;
const LP_INT24: u8 = 0xF2;
const LP_INT32: u8 = 0xF3;
const LP_INT64: u8 = 0xF4;

/// CRC-64/Jones, reflected, as Redis's `crc64` (initial value 0, no final
/// xor).
const CRC64_POLY: u64 = 0x95ac_9329_ac4b_c9b5;
const CRC64_TABLE: [u64; 256] = crc64_table();

const fn crc64_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ CRC64_POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

fn crc64(data: &[u8]) -> u64 {
    crc64_update(0, data)
}

/// The CRC of what came before, carried on over `data`.
pub(crate) fn crc64_update(crc: u64, data: &[u8]) -> u64 {
    data.iter().fold(crc, |crc, &b| {
        CRC64_TABLE[usize::from(crc as u8 ^ b)] ^ (crc >> 8)
    })
}

/// Whether we read values written as RDB `version`.
pub(crate) fn readable(version: u16) -> bool {
    version <= MAX_READ_VERSION || version == VERSION_80
}

/// body + 2-byte little-endian version + 8-byte little-endian CRC64 (Jones
/// polynomial, as Redis: crc64 of body+version, initial value 0).
pub fn seal(body: Vec<u8>) -> Vec<u8> {
    seal_as(body, VERSION)
}

/// `seal`, naming RDB `version` in the footer: the oldest that reads the body.
pub fn seal_as(mut body: Vec<u8>, version: u16) -> Vec<u8> {
    body.extend_from_slice(&version.to_le_bytes());
    let crc = crc64(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    body
}

/// Checks footer (length, a version we read, checksum) and returns the
/// body (type byte first).
pub fn open(payload: &[u8]) -> Result<&[u8], Error> {
    Ok(open_versioned(payload)?.0)
}

/// `open`, with the version the footer names.
pub fn open_versioned(payload: &[u8]) -> Result<(&[u8], u16), Error> {
    if payload.len() < FOOTER {
        return Err(Error::Envelope);
    }
    let (signed, crc) = payload
        .split_at_checked(payload.len() - CRC_BYTES)
        .ok_or(Error::Envelope)?;
    let (body, version) = signed
        .split_at_checked(signed.len() - VERSION_BYTES)
        .ok_or(Error::Envelope)?;
    let version = u16::from_le_bytes(version.try_into().map_err(|_| Error::Envelope)?);
    let crc = u64::from_le_bytes(crc.try_into().map_err(|_| Error::Envelope)?);
    if !readable(version) || crc64(signed) != crc {
        return Err(Error::Envelope);
    }
    Ok((body, version))
}

/// A full DUMP payload (sealed). An empty collection is written as such,
/// and `decode` refuses it: a key with an empty value does not exist.
pub fn encode(value: &Plain) -> Vec<u8> {
    let mut body = Vec::new();
    match value {
        Plain::Str(s) => {
            body.push(TYPE_STRING);
            put_string(&mut body, s);
        }
        Plain::List(items) => {
            body.push(TYPE_LIST);
            put_len(&mut body, items.len() as u64);
            items.iter().for_each(|item| put_string(&mut body, item));
        }
        Plain::Set(members) => {
            body.push(TYPE_SET);
            put_len(&mut body, members.len() as u64);
            members.iter().for_each(|m| put_string(&mut body, m));
        }
        Plain::ZSet(entries) => {
            body.push(TYPE_ZSET_2);
            put_len(&mut body, entries.len() as u64);
            for (member, score) in entries {
                put_string(&mut body, member);
                body.extend_from_slice(&score.to_le_bytes());
            }
        }
        Plain::Hash(pairs) => {
            body.push(TYPE_HASH);
            put_len(&mut body, pairs.len() as u64);
            for (field, value) in pairs {
                put_string(&mut body, field);
                put_string(&mut body, value);
            }
        }
    }
    seal(body)
}

/// The value in a body returned by `open`. A body whose type byte is
/// >= PRIVATE_TYPES is not ours: Err(Error::Body).
pub fn decode(body: &[u8]) -> Result<Plain, Error> {
    decode_within(body, budget(body.len()))
}

/// What a payload of `len` bytes may decode to (`Input::unpacked`). A file
/// is trusted further: its budget has no end.
fn budget(len: usize) -> usize {
    LZF_MAX_PLAIN.saturating_add(len.saturating_mul(BYTE_CREDIT))
}

/// Charge one decoded item to `budget`.
pub(crate) fn charge(budget: &mut usize, item: &[u8]) -> Option<()> {
    *budget = budget.checked_sub(ITEM_COST + item.len())?;
    Some(())
}

/// `decode`, with a budget of `unpacked` (`Input::unpacked`).
fn decode_within(body: &[u8], unpacked: usize) -> Result<Plain, Error> {
    let mut reader = Reader(body, unpacked);
    let value = reader
        .u8()
        .and_then(|t| read_plain(&mut reader, t))
        .ok_or(Error::Body)?;
    // A DUMP body is one value and nothing after it.
    if !reader.0.is_empty() {
        return Err(Error::Body);
    }
    Ok(value)
}

pub(crate) fn put_len(out: &mut Vec<u8>, n: u64) {
    if n < 1 << 6 {
        out.push(n as u8);
    } else if n < 1 << 14 {
        out.push(LEN_14BIT << 6 | (n >> 8) as u8);
        out.push(n as u8);
    } else if let Ok(n) = u32::try_from(n) {
        out.push(LEN_32BIT);
        out.extend_from_slice(&n.to_be_bytes());
    } else {
        out.push(LEN_64BIT);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

/// Bytes as they are, behind their length: never as an integer.
pub(crate) fn put_raw(out: &mut Vec<u8>, s: &[u8]) {
    put_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// A time in milliseconds, as RDB writes deadlines: 8 bytes little-endian.
pub(crate) fn put_millis(out: &mut Vec<u8>, ms: i64) {
    out.extend_from_slice(&ms.to_le_bytes());
}

/// A string that is the canonical decimal form of a 32-bit integer is
/// written as that integer, as Redis does; anything else ("007", "-0",
/// "+1") as its bytes, so it reads back exactly.
pub(crate) fn put_string(out: &mut Vec<u8>, s: &[u8]) {
    if let Some(n) = canonical_int(s) {
        if let Ok(v) = i8::try_from(n) {
            out.push(ENCVAL_PREFIX | ENC_INT8);
            out.extend_from_slice(&v.to_le_bytes());
            return;
        }
        if let Ok(v) = i16::try_from(n) {
            out.push(ENCVAL_PREFIX | ENC_INT16);
            out.extend_from_slice(&v.to_le_bytes());
            return;
        }
        if let Ok(v) = i32::try_from(n) {
            out.push(ENCVAL_PREFIX | ENC_INT32);
            out.extend_from_slice(&v.to_le_bytes());
            return;
        }
    }
    put_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

fn canonical_int(s: &[u8]) -> Option<i64> {
    if s.len() > MAX_INT_TEXT {
        return None;
    }
    let n: i64 = std::str::from_utf8(s).ok()?.parse().ok()?;
    (int_text(n) == s).then_some(n)
}

fn int_text(n: i64) -> Vec<u8> {
    n.to_string().into_bytes()
}

fn parse_score(text: &[u8]) -> Option<f64> {
    std::str::from_utf8(text).ok()?.parse().ok()
}

pub(crate) enum Len {
    Plain(u64),
    /// One of the `ENC_*` special strings.
    Encoded(u8),
}

/// Where values are read from: a payload in memory (`Reader`) or a file
/// on its way in (`rdbfile`). Every length is checked against what is left
/// before it is used, as far as the input can tell.
pub(crate) trait Input {
    /// The next `n` bytes; `None` when there are fewer.
    fn take(&mut self, n: usize) -> Option<&[u8]>;

    /// The most bytes that can be left.
    fn left(&self) -> usize;

    /// What decoded values may still take: LZF output, and every item at
    /// `ITEM_COST` plus its bytes.
    fn unpacked(&mut self) -> &mut usize;

    /// Charge `n` decoded bytes; `None` once the budget is spent.
    fn spend(&mut self, n: usize) -> Option<()> {
        let left = self.unpacked();
        *left = left.checked_sub(n)?;
        Some(())
    }

    fn take_len(&mut self, n: u64) -> Option<&[u8]> {
        self.take(usize::try_from(n).ok()?)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1)?.first().copied()
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    /// A time in milliseconds (`put_millis`).
    fn millis(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.array()?))
    }

    /// Room to reserve for `n` items that take at least one byte each:
    /// never more than the bytes left, and never much.
    fn room(&self, n: u64) -> usize {
        let left = self.left().min(MAX_RESERVE);
        usize::try_from(n).map_or(left, |n| n.min(left))
    }

    fn len_or_enc(&mut self) -> Option<Len> {
        let b = self.u8()?;
        let low = b & 0x3f;
        match b >> 6 {
            LEN_6BIT => Some(Len::Plain(u64::from(low))),
            LEN_14BIT => Some(Len::Plain(u64::from(low) << 8 | u64::from(self.u8()?))),
            LEN_ENCVAL => Some(Len::Encoded(low)),
            _ => match b {
                LEN_32BIT => Some(Len::Plain(u64::from(u32::from_be_bytes(self.array()?)))),
                LEN_64BIT => Some(Len::Plain(u64::from_be_bytes(self.array()?))),
                _ => None,
            },
        }
    }

    fn len(&mut self) -> Option<u64> {
        match self.len_or_enc()? {
            Len::Plain(n) => Some(n),
            Len::Encoded(_) => None,
        }
    }

    /// The size of a collection; an empty one is refused.
    fn count(&mut self) -> Option<u64> {
        self.len().filter(|&n| n > 0)
    }

    fn string(&mut self) -> Option<Vec<u8>> {
        let out = match self.len_or_enc()? {
            Len::Plain(n) => self.take_len(n)?.to_vec(),
            Len::Encoded(ENC_INT8) => int_text(i8::from_le_bytes(self.array()?).into()),
            Len::Encoded(ENC_INT16) => int_text(i16::from_le_bytes(self.array()?).into()),
            Len::Encoded(ENC_INT32) => int_text(i32::from_le_bytes(self.array()?).into()),
            Len::Encoded(ENC_LZF) => {
                let packed = self.len()?;
                let plain = self.len()?;
                let out = lzf_decompress(self.take_len(packed)?, plain)?;
                self.spend(out.len())?;
                out
            }
            Len::Encoded(_) => return None,
        };
        charge(self.unpacked(), &out)?;
        Some(out)
    }

    fn strings(&mut self, n: u64) -> Option<Vec<Vec<u8>>> {
        let mut items = Vec::with_capacity(self.room(n));
        for _ in 0..n {
            items.push(self.string()?);
        }
        Some(items)
    }

    fn ascii_score(&mut self) -> Option<f64> {
        match self.u8()? {
            SCORE_NAN => Some(f64::NAN),
            SCORE_POS_INF => Some(f64::INFINITY),
            SCORE_NEG_INF => Some(f64::NEG_INFINITY),
            n => parse_score(self.take(usize::from(n))?),
        }
    }
}

/// Reads values out of a payload or a blob in memory.
pub(crate) struct Reader<'a>(
    &'a [u8],
    /// Bytes LZF blobs may still expand to.
    usize,
);

impl Input for Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let (head, tail) = self.0.split_at_checked(n)?;
        self.0 = tail;
        Some(head)
    }

    fn left(&self) -> usize {
        self.0.len()
    }

    fn unpacked(&mut self) -> &mut usize {
        &mut self.1
    }
}

impl<'a> Reader<'a> {
    pub(crate) fn new(input: &'a [u8]) -> Self {
        Reader(input, budget(input.len()))
    }

    #[cfg(test)]
    pub(crate) fn with_budget(input: &'a [u8], budget: usize) -> Self {
        Reader(input, budget)
    }

    /// What has not been read yet.
    pub(crate) fn rest(&self) -> &'a [u8] {
        self.0
    }

    fn peek(&self) -> Option<u8> {
        self.0.first().copied()
    }

    fn u16_le(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.array()?))
    }

    fn u32_le(&mut self) -> Option<usize> {
        usize::try_from(u32::from_le_bytes(self.array()?)).ok()
    }
}

/// The value of type `t` (its type byte already read), for the five types
/// `Plain` has. `None` for anything else or anything malformed.
pub(crate) fn read_plain<I: Input + ?Sized>(r: &mut I, t: u8) -> Option<Plain> {
    match t {
        TYPE_STRING => Some(Plain::Str(r.string()?)),
        TYPE_LIST => {
            let n = r.count()?;
            list(r.strings(n)?)
        }
        TYPE_SET => {
            let n = r.count()?;
            set(r.strings(n)?)
        }
        TYPE_ZSET | TYPE_ZSET_2 => {
            let n = r.count()?;
            let mut entries = Vec::with_capacity(r.room(n));
            for _ in 0..n {
                let member = r.string()?;
                let score = if t == TYPE_ZSET_2 {
                    f64::from_le_bytes(r.array()?)
                } else {
                    r.ascii_score()?
                };
                entries.push((member, score));
            }
            zset(entries)
        }
        TYPE_HASH => {
            let n = r.count()?;
            let mut pairs = Vec::with_capacity(r.room(n));
            for _ in 0..n {
                pairs.push((r.string()?, r.string()?));
            }
            hash(pairs)
        }
        TYPE_HASH_ZIPMAP => hash(zipmap(&r.string()?, r.unpacked())?),
        TYPE_LIST_ZIPLIST => list(ziplist(&r.string()?, r.unpacked())?),
        TYPE_SET_INTSET => set(intset(&r.string()?, r.unpacked())?),
        TYPE_ZSET_ZIPLIST => zset(scored(ziplist(&r.string()?, r.unpacked())?)?),
        TYPE_HASH_ZIPLIST => hash(pairs(ziplist(&r.string()?, r.unpacked())?)?),
        TYPE_LIST_QUICKLIST => {
            let nodes = r.count()?;
            let mut items = Vec::new();
            for _ in 0..nodes {
                // An empty node is skipped, as Redis does.
                items.extend(ziplist(&r.string()?, r.unpacked())?);
            }
            list(items)
        }
        TYPE_HASH_LISTPACK => hash(pairs(listpack(&r.string()?, r.unpacked())?)?),
        TYPE_ZSET_LISTPACK => zset(scored(listpack(&r.string()?, r.unpacked())?)?),
        TYPE_SET_LISTPACK => set(listpack(&r.string()?, r.unpacked())?),
        TYPE_LIST_QUICKLIST_2 => {
            let nodes = r.count()?;
            let mut items = Vec::new();
            for _ in 0..nodes {
                match r.len()? {
                    QUICKLIST_PLAIN => items.push(r.string()?),
                    QUICKLIST_PACKED => items.extend(listpack(&r.string()?, r.unpacked())?),
                    _ => return None,
                }
            }
            list(items)
        }
        // Streams, modules, functions, hashes with field TTLs, and the
        // caller's private types.
        _ => None,
    }
}

/// Builds a listpack (`u32 bytes, u16 count`, entries, 0xFF) with the
/// smallest encoding for each entry, as Redis's `lpAppend` does.
pub(crate) struct Listpack {
    buf: Vec<u8>,
    count: usize,
}

impl Listpack {
    const HEADER: usize = 6;

    pub(crate) fn new() -> Listpack {
        Listpack {
            buf: vec![0; Self::HEADER],
            count: 0,
        }
    }

    fn entry(&mut self, from: usize) {
        let (backlen, width) = lp_backlen(self.buf.len() - from);
        self.buf.extend_from_slice(&backlen[..width]);
        self.count += 1;
    }

    pub(crate) fn int(&mut self, v: i64) {
        let from = self.buf.len();
        match v {
            0..=127 => self.buf.push(v as u8),
            -4096..=4095 => {
                // 13-bit two's complement.
                let u = (v as u16) & 0x1fff;
                self.buf.push(LP_13BIT_INT | (u >> 8) as u8);
                self.buf.push(u as u8);
            }
            _ => {
                if let Ok(v) = i16::try_from(v) {
                    self.buf.push(LP_INT16);
                    self.buf.extend_from_slice(&v.to_le_bytes());
                } else if (-(1 << 23)..1 << 23).contains(&v) {
                    self.buf.push(LP_INT24);
                    self.buf.extend_from_slice(&v.to_le_bytes()[..3]);
                } else if let Ok(v) = i32::try_from(v) {
                    self.buf.push(LP_INT32);
                    self.buf.extend_from_slice(&v.to_le_bytes());
                } else {
                    self.buf.push(LP_INT64);
                    self.buf.extend_from_slice(&v.to_le_bytes());
                }
            }
        }
        self.entry(from);
    }

    pub(crate) fn string(&mut self, s: &[u8]) {
        let from = self.buf.len();
        let n = s.len();
        if n < 1 << 6 {
            self.buf.push(LP_6BIT_STR | n as u8);
        } else if n < 1 << 12 {
            self.buf.push(LP_12BIT_STR | (n >> 8) as u8);
            self.buf.push(n as u8);
        } else {
            self.buf.push(LP_32BIT_STR);
            self.buf.extend_from_slice(&(n as u32).to_le_bytes());
        }
        self.buf.extend_from_slice(s);
        self.entry(from);
    }

    pub(crate) fn finish(mut self) -> Vec<u8> {
        self.buf.push(LP_EOF);
        let total = self.buf.len() as u32;
        let count = u16::try_from(self.count)
            .ok()
            .filter(|&c| c != LP_UNKNOWN_COUNT)
            .unwrap_or(LP_UNKNOWN_COUNT);
        self.buf[..4].copy_from_slice(&total.to_le_bytes());
        self.buf[4..Self::HEADER].copy_from_slice(&count.to_le_bytes());
        self.buf
    }
}

fn list(items: Vec<Vec<u8>>) -> Option<Plain> {
    (!items.is_empty()).then_some(Plain::List(items))
}

fn set(members: Vec<Vec<u8>>) -> Option<Plain> {
    let ok = !members.is_empty() && distinct(members.iter().map(Vec::as_slice));
    ok.then_some(Plain::Set(members))
}

fn hash(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Option<Plain> {
    let ok = !pairs.is_empty() && distinct(pairs.iter().map(|(f, _)| f.as_slice()));
    ok.then_some(Plain::Hash(pairs))
}

fn zset(entries: Vec<(Vec<u8>, f64)>) -> Option<Plain> {
    let ok = !entries.is_empty()
        && entries.iter().all(|(_, score)| !score.is_nan())
        && distinct(entries.iter().map(|(m, _)| m.as_slice()));
    ok.then_some(Plain::ZSet(entries))
}

/// Linear duplicate check.
fn distinct<'a>(mut keys: impl Iterator<Item = &'a [u8]>) -> bool {
    let mut seen = HashSet::new();
    keys.all(|k| seen.insert(k))
}

/// Alternating field and value.
fn pairs(flat: Vec<Vec<u8>>) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    if !flat.len().is_multiple_of(2) {
        return None;
    }
    let mut items = flat.into_iter();
    let mut out = Vec::with_capacity(items.len() / 2);
    while let (Some(a), Some(b)) = (items.next(), items.next()) {
        out.push((a, b));
    }
    Some(out)
}

/// Alternating member and score, the score as text or as an integer.
fn scored(flat: Vec<Vec<u8>>) -> Option<Vec<(Vec<u8>, f64)>> {
    pairs(flat)?
        .into_iter()
        .map(|(member, score)| Some((member, parse_score(&score)?)))
        .collect()
}

/// LZF as `lzf_d.c`, with the output held to the declared length. The
/// declared length must be one the input can actually produce, so the
/// work is bounded by the input size.
fn lzf_decompress(input: &[u8], plain_len: u64) -> Option<Vec<u8>> {
    let want = usize::try_from(plain_len).ok()?;
    if input.is_empty()
        || want == 0
        || want > LZF_MAX_PLAIN
        || want > input.len().saturating_mul(LZF_MAX_RATIO)
    {
        return None;
    }
    // Grows with what is actually produced, not with the claim.
    let mut out = Vec::with_capacity(want.min(input.len()));
    let mut r = Reader::new(input);
    while let Some(ctrl) = r.u8() {
        let ctrl = usize::from(ctrl);
        if ctrl < LZF_LITERAL_LIMIT {
            let literal = r.take(ctrl + 1)?;
            if out.len() + literal.len() > want {
                return None;
            }
            out.extend_from_slice(literal);
            continue;
        }
        let mut n = ctrl >> 5;
        if n == LZF_LONG_MATCH {
            n += usize::from(r.u8()?);
        }
        let back = ((ctrl & 0x1f) << 8 | usize::from(r.u8()?)) + 1;
        let from = out.len().checked_sub(back)?;
        n += LZF_MIN_MATCH;
        if out.len() + n > want {
            return None;
        }
        // Byte by byte: the source may overlap what is being written.
        for i in from..from + n {
            let b = *out.get(i)?;
            out.push(b);
        }
    }
    (out.len() == want).then_some(out)
}

fn ziplist(blob: &[u8], budget: &mut usize) -> Option<Vec<Vec<u8>>> {
    let mut r = Reader::new(blob);
    let total = r.u32_le()?;
    let tail = r.u32_le()?;
    let count = r.u16_le()?;
    if total != blob.len() {
        return None;
    }
    let mut items = Vec::new();
    let (mut prev_size, mut last_start) = (0, ZIPLIST_HEADER);
    while r.peek()? != ZIP_END {
        let start = blob.len() - r.0.len();
        // The 5-byte form may hold a small length too.
        let prevlen = match r.u8()? {
            ZIP_BIG_PREVLEN => r.u32_le()?,
            small => usize::from(small),
        };
        if prevlen != prev_size {
            return None;
        }
        let item = zip_entry(&mut r)?;
        charge(budget, &item)?;
        items.push(item);
        prev_size = blob.len() - r.0.len() - start;
        last_start = start;
    }
    r.u8();
    let ok = r.0.is_empty()
        && tail == last_start
        && (count == ZIP_UNKNOWN_COUNT || usize::from(count) == items.len());
    ok.then_some(items)
}

fn zip_entry(r: &mut Reader<'_>) -> Option<Vec<u8>> {
    let enc = r.u8()?;
    let n = match enc >> 6 {
        ZIP_STR_06B => usize::from(enc & 0x3f),
        ZIP_STR_14B => usize::from(enc & 0x3f) << 8 | usize::from(r.u8()?),
        _ if enc == ZIP_STR_32B => usize::try_from(u32::from_be_bytes(r.array()?)).ok()?,
        _ => return zip_int(r, enc).map(int_text),
    };
    Some(r.take(n)?.to_vec())
}

fn zip_int(r: &mut Reader<'_>, enc: u8) -> Option<i64> {
    Some(match enc {
        ZIP_INT_8B => i8::from_le_bytes(r.array()?).into(),
        ZIP_INT_16B => i16::from_le_bytes(r.array()?).into(),
        ZIP_INT_24B => int24(r.array()?),
        ZIP_INT_32B => i32::from_le_bytes(r.array()?).into(),
        ZIP_INT_64B => i64::from_le_bytes(r.array()?),
        ZIP_INT_IMM_MIN..=ZIP_INT_IMM_MAX => i64::from(enc & 0x0f) - 1,
        _ => return None,
    })
}

fn int24([a, b, c]: [u8; 3]) -> i64 {
    // Into the top of an i32, then an arithmetic shift extends the sign.
    (i32::from_le_bytes([0, a, b, c]) >> 8).into()
}

fn zipmap(blob: &[u8], budget: &mut usize) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut r = Reader::new(blob);
    let count = r.u8()?;
    let mut pairs = Vec::new();
    while r.peek()? != ZIPMAP_END {
        let n = zipmap_len(&mut r)?;
        let field = r.take(n)?.to_vec();
        let n = zipmap_len(&mut r)?;
        let free = r.u8()?;
        let value = r.take(n)?.to_vec();
        r.take(usize::from(free))?;
        charge(budget, &field)?;
        charge(budget, &value)?;
        pairs.push((field, value));
    }
    r.u8();
    let ok = r.0.is_empty() && (count == ZIPMAP_BIGLEN || usize::from(count) == pairs.len());
    ok.then_some(pairs)
}

fn zipmap_len(r: &mut Reader<'_>) -> Option<usize> {
    match r.u8()? {
        ZIPMAP_BIGLEN => r.u32_le(),
        ZIPMAP_END => None,
        n => Some(usize::from(n)),
    }
}

fn intset(blob: &[u8], budget: &mut usize) -> Option<Vec<Vec<u8>>> {
    let mut r = Reader::new(blob);
    let width = r.u32_le()?;
    let count = r.u32_le()?;
    let fits = matches!(
        width,
        INTSET_ENC_INT16 | INTSET_ENC_INT32 | INTSET_ENC_INT64
    ) && count.checked_mul(width) == Some(r.0.len());
    if !fits {
        return None;
    }
    let mut items = Vec::with_capacity(count);
    let mut prev = None;
    for chunk in r.0.chunks_exact(width) {
        let v = match width {
            INTSET_ENC_INT16 => i16::from_le_bytes(chunk.try_into().ok()?).into(),
            INTSET_ENC_INT32 => i32::from_le_bytes(chunk.try_into().ok()?).into(),
            _ => i64::from_le_bytes(chunk.try_into().ok()?),
        };
        // Sorted and without duplicates, as Redis keeps it.
        if prev.is_some_and(|p| p >= v) {
            return None;
        }
        prev = Some(v);
        let item = int_text(v);
        charge(budget, &item)?;
        items.push(item);
    }
    Some(items)
}

/// The entries of a listpack, integers as their decimal text.
pub(crate) fn listpack(blob: &[u8], budget: &mut usize) -> Option<Vec<Vec<u8>>> {
    let mut r = Reader::new(blob);
    let total = r.u32_le()?;
    let count = r.u16_le()?;
    if total != blob.len() {
        return None;
    }
    let mut items = Vec::new();
    while r.peek()? != LP_EOF {
        let before = r.0.len();
        let item = lp_entry(&mut r)?;
        charge(budget, &item)?;
        items.push(item);
        let (expected, width) = lp_backlen(before - r.0.len());
        if r.take(width)? != expected.get(..width)? {
            return None;
        }
    }
    r.u8();
    let ok = r.0.is_empty() && (count == LP_UNKNOWN_COUNT || usize::from(count) == items.len());
    ok.then_some(items)
}

fn lp_entry(r: &mut Reader<'_>) -> Option<Vec<u8>> {
    let first = r.u8()?;
    let n = if first & LP_7BIT_UINT_MASK == LP_7BIT_UINT {
        return Some(int_text(first.into()));
    } else if first & LP_6BIT_STR_MASK == LP_6BIT_STR {
        usize::from(first & 0x3f)
    } else if first & LP_13BIT_INT_MASK == LP_13BIT_INT {
        let v = i64::from(first & 0x1f) << 8 | i64::from(r.u8()?);
        // 13-bit two's complement.
        return Some(int_text(if v >= 1 << 12 { v - (1 << 13) } else { v }));
    } else if first & LP_12BIT_STR_MASK == LP_12BIT_STR {
        usize::from(first & 0x0f) << 8 | usize::from(r.u8()?)
    } else {
        let v = match first {
            LP_32BIT_STR => {
                let n = r.u32_le()?;
                return Some(r.take(n)?.to_vec());
            }
            LP_INT16 => i16::from_le_bytes(r.array()?).into(),
            LP_INT24 => int24(r.array()?),
            LP_INT32 => i32::from_le_bytes(r.array()?).into(),
            LP_INT64 => i64::from_le_bytes(r.array()?),
            _ => return None,
        };
        return Some(int_text(v));
    };
    Some(r.take(n)?.to_vec())
}

/// The bytes that follow a listpack entry of `size` bytes, as Redis's
/// `lpEncodeBacklen`: 7 bits per byte, the most significant first, every
/// byte after the first with its top bit set.
fn lp_backlen(size: usize) -> ([u8; 5], usize) {
    let l = size as u64;
    let width = match l {
        0..=127 => 1,
        128..=16382 => 2,
        16383..=2_097_150 => 3,
        2_097_151..=268_435_454 => 4,
        _ => 5,
    };
    let mut buf = [0u8; 5];
    for (i, b) in buf.iter_mut().take(width).enumerate() {
        let bits = (l >> (7 * (width - 1 - i))) as u8 & 0x7f;
        *b = if i == 0 { bits } else { bits | 0x80 };
    }
    (buf, width)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace()
            .map(|b| u8::from_str_radix(b, 16).unwrap())
            .collect()
    }

    fn strs(items: &[&str]) -> Vec<Vec<u8>> {
        items.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn round_trip(value: &Plain) -> Plain {
        let payload = encode(value);
        let back = decode(open(&payload).unwrap()).unwrap();
        assert_eq!(&back, value);
        back
    }

    /// A body of type `t` whose value is one raw string (a blob).
    fn blob_body(t: u8, blob: &[u8]) -> Vec<u8> {
        let mut out = vec![t];
        put_len(&mut out, blob.len() as u64);
        out.extend_from_slice(blob);
        out
    }

    // Ziplist, written from the format description.

    fn zl_str(s: &[u8]) -> Vec<u8> {
        let n = s.len();
        let mut out = if n < 64 {
            vec![n as u8]
        } else if n < 16384 {
            vec![0x40 | (n >> 8) as u8, n as u8]
        } else {
            let mut v = vec![0x80];
            v.extend_from_slice(&(n as u32).to_be_bytes());
            v
        };
        out.extend_from_slice(s);
        out
    }

    fn zl_raw(enc: u8, data: &[u8]) -> Vec<u8> {
        let mut out = vec![enc];
        out.extend_from_slice(data);
        out
    }

    /// `entries` are encoding+data; `big_prevlen` writes every prevlen in
    /// the 5-byte form.
    fn zl(entries: &[Vec<u8>], big_prevlen: bool) -> Vec<u8> {
        let mut body = Vec::new();
        let (mut prev, mut tail) = (0usize, 10usize);
        for e in entries {
            let start = body.len();
            tail = 10 + start;
            if prev < 254 && !big_prevlen {
                body.push(prev as u8);
            } else {
                body.push(254);
                body.extend_from_slice(&(prev as u32).to_le_bytes());
            }
            body.extend_from_slice(e);
            prev = body.len() - start;
        }
        let mut out = Vec::new();
        out.extend_from_slice(&((11 + body.len()) as u32).to_le_bytes());
        out.extend_from_slice(&(tail as u32).to_le_bytes());
        out.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        out.extend_from_slice(&body);
        out.push(0xFF);
        out
    }

    // Listpack, written from the format description.

    fn lp_str(s: &[u8]) -> Vec<u8> {
        let n = s.len();
        let mut out = if n < 64 {
            vec![0x80 | n as u8]
        } else if n < 4096 {
            vec![0xE0 | (n >> 8) as u8, n as u8]
        } else {
            let mut v = vec![0xF0];
            v.extend_from_slice(&(n as u32).to_le_bytes());
            v
        };
        out.extend_from_slice(s);
        out
    }

    fn lp_int(v: i64) -> Vec<u8> {
        if (0..=127).contains(&v) {
            vec![v as u8]
        } else if (-4096..=4095).contains(&v) {
            let u = (v as u16) & 0x1fff;
            vec![0xC0 | (u >> 8) as u8, u as u8]
        } else if let Ok(x) = i16::try_from(v) {
            zl_raw(0xF1, &x.to_le_bytes())
        } else if (-(1 << 23)..1 << 23).contains(&v) {
            zl_raw(0xF2, &(v as i32).to_le_bytes()[..3])
        } else if let Ok(x) = i32::try_from(v) {
            zl_raw(0xF3, &x.to_le_bytes())
        } else {
            zl_raw(0xF4, &v.to_le_bytes())
        }
    }

    /// `lpEncodeBacklen`, written separately from the decoder's copy.
    fn backlen(l: usize) -> Vec<u8> {
        if l <= 127 {
            vec![l as u8]
        } else if l < 16383 {
            vec![(l >> 7) as u8, (l & 127) as u8 | 128]
        } else {
            assert!(l < 2_097_151);
            vec![
                (l >> 14) as u8,
                ((l >> 7) & 127) as u8 | 128,
                (l & 127) as u8 | 128,
            ]
        }
    }

    fn lp(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Vec::new();
        for e in entries {
            body.extend_from_slice(e);
            body.extend_from_slice(&backlen(e.len()));
        }
        let mut out = Vec::new();
        out.extend_from_slice(&((7 + body.len()) as u32).to_le_bytes());
        out.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        out.extend_from_slice(&body);
        out.push(0xFF);
        out
    }

    fn intset_blob(width: u32, values: &[i64]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&width.to_le_bytes());
        out.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for &v in values {
            out.extend_from_slice(&v.to_le_bytes()[..width as usize]);
        }
        out
    }

    fn zipmap_blob(count: u8, pairs: &[(&[u8], &[u8], u8)]) -> Vec<u8> {
        let len = |out: &mut Vec<u8>, n: usize| {
            if n < 254 {
                out.push(n as u8);
            } else {
                out.push(254);
                out.extend_from_slice(&(n as u32).to_le_bytes());
            }
        };
        let mut out = vec![count];
        for &(field, value, free) in pairs {
            len(&mut out, field.len());
            out.extend_from_slice(field);
            len(&mut out, value.len());
            out.push(free);
            out.extend_from_slice(value);
            out.extend(std::iter::repeat_n(0xAA, free as usize));
        }
        out.push(0xFF);
        out
    }

    /// Valid LZF made of literal runs only.
    fn lzf_literals(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in data.chunks(32) {
            out.push((chunk.len() - 1) as u8);
            out.extend_from_slice(chunk);
        }
        out
    }

    fn lzf_body(t: u8, packed: &[u8], plain_len: u64) -> Vec<u8> {
        let mut out = vec![t, ENCVAL_PREFIX | ENC_LZF];
        put_len(&mut out, packed.len() as u64);
        put_len(&mut out, plain_len);
        out.extend_from_slice(packed);
        out
    }

    #[test]
    fn redis_doc_vectors() {
        let dump = hex("00 c0 0a 06 00 f8 72 3f c5 fb fb 5f 28");
        let body = open(&dump).unwrap();
        assert_eq!(decode(body), Ok(Plain::Str(b"10".to_vec())));
        // We write the same body for the same value.
        assert_eq!(open(&encode(&Plain::Str(b"10".to_vec()))).unwrap(), body);

        let restore = hex(
            "0a 17 17 00 00 00 12 00 00 00 03 00 00 c0 01 00 04 c0 02 00 04 c0 03 00 ff \
             04 00 75 23 3c c0 3b 2e e9 dd",
        );
        let body = open(&restore).unwrap();
        assert_eq!(decode(body), Ok(Plain::List(strs(&["1", "2", "3"]))));

        assert_eq!(crc64(b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn envelope_checks() {
        assert_eq!(open(&[]), Err(Error::Envelope));
        assert_eq!(open(&[0; 9]), Err(Error::Envelope));
        // A footer alone is a valid envelope around an empty body.
        assert_eq!(open(&seal(Vec::new())), Ok(&[][..]));
        assert_eq!(decode(&[]), Err(Error::Body));
        // A newer version is refused even with a right checksum.
        let mut newer = vec![TYPE_STRING, 1, b'x'];
        newer.extend_from_slice(&(MAX_READ_VERSION + 1).to_le_bytes());
        let crc = crc64(&newer);
        newer.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(open(&newer), Err(Error::Envelope));
        let mut current = encode(&Plain::Str(b"x".to_vec()));
        assert!(open(&current).is_ok());
        let last = current.len() - 1;
        current[last] ^= 1;
        assert_eq!(open(&current), Err(Error::Envelope));
    }

    #[test]
    fn strings_round_trip_across_length_forms() {
        for n in [
            0, 1, 2, 62, 63, 64, 65, 100, 16382, 16383, 16384, 16385, 65535, 65536, 70000,
        ] {
            let s: Vec<u8> = (0..n).map(|i| (i * 7 + 3) as u8).collect();
            let payload = encode(&Plain::Str(s.clone()));
            let header = &payload[1..];
            match n {
                0..=63 => assert_eq!(header[0], n as u8),
                64..=16383 => assert_eq!(header[..2], [0x40 | (n >> 8) as u8, n as u8]),
                _ => {
                    assert_eq!(header[0], LEN_32BIT);
                    assert_eq!(header[1..5], (n as u32).to_be_bytes());
                }
            }
            round_trip(&Plain::Str(s));
        }
        let mut out = Vec::new();
        put_len(&mut out, 1 << 32);
        assert_eq!(out, [0x81, 0, 0, 0, 1, 0, 0, 0, 0]);
    }

    #[test]
    fn integer_looking_strings_round_trip_exactly() {
        let ints = [
            ("0", ENC_INT8),
            ("-1", ENC_INT8),
            ("127", ENC_INT8),
            ("-128", ENC_INT8),
            ("128", ENC_INT16),
            ("-129", ENC_INT16),
            ("32767", ENC_INT16),
            ("32768", ENC_INT32),
            ("2147483647", ENC_INT32),
            ("-2147483648", ENC_INT32),
        ];
        for (s, enc) in ints {
            round_trip(&Plain::Str(s.into()));
            assert_eq!(encode(&Plain::Str(s.into()))[1], ENCVAL_PREFIX | enc, "{s}");
        }
        for s in [
            "007",
            "-0",
            "+1",
            " 1",
            "1 ",
            "",
            "12345678901234567890",
            "2147483648",
            "-2147483649",
            "9223372036854775807",
            "1.5",
            "0x10",
        ] {
            round_trip(&Plain::Str(s.into()));
            assert_eq!(encode(&Plain::Str(s.into()))[1], s.len() as u8, "{s:?} raw");
        }
    }

    #[test]
    fn collections_round_trip() {
        let binary: Vec<u8> = (0..=255).collect();
        round_trip(&Plain::List(vec![
            Vec::new(),
            b"a".to_vec(),
            b"007".to_vec(),
            b"-1".to_vec(),
            binary.clone(),
            vec![b'z'; 70000],
        ]));
        round_trip(&Plain::List(strs(&["same", "same"])));
        round_trip(&Plain::Set(vec![
            Vec::new(),
            b"1".to_vec(),
            b"01".to_vec(),
            binary.clone(),
        ]));
        round_trip(&Plain::Hash(vec![
            (Vec::new(), Vec::new()),
            (b"1".to_vec(), b"2".to_vec()),
            (binary.clone(), binary.clone()),
        ]));
        let scores = [
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE / 2.0,
            -5e-324,
            1.5,
            f64::MAX,
            -1e300,
        ];
        let zset: Vec<(Vec<u8>, f64)> = scores
            .iter()
            .enumerate()
            .map(|(i, &s)| (format!("m{i}").into_bytes(), s))
            .collect();
        let Plain::ZSet(back) = round_trip(&Plain::ZSet(zset)) else {
            panic!("not a zset")
        };
        for ((_, got), want) in back.iter().zip(scores) {
            assert_eq!(got.to_bits(), want.to_bits());
        }
        let many: Vec<Vec<u8>> = (0..20000).map(|i| format!("{i}").into_bytes()).collect();
        round_trip(&Plain::Set(many));
    }

    #[test]
    fn wide_lengths_are_read() {
        let mut body = vec![
            TYPE_LIST, 0x81, 0, 0, 0, 0, 0, 0, 0, 2, 0x80, 0, 0, 0, 1, b'x',
        ];
        body.extend_from_slice(&[0x40, 1, b'y']);
        assert_eq!(decode(&body), Ok(Plain::List(strs(&["x", "y"]))));
    }

    #[test]
    fn ascii_scores() {
        let mut body = vec![TYPE_ZSET, 4];
        for (m, score) in [("a", &b"\x031.5"[..]), ("b", b"\xfe"), ("c", b"\xff")] {
            put_string(&mut body, m.as_bytes());
            body.extend_from_slice(score);
        }
        body.extend_from_slice(b"\x01d\x05-2e-3");
        assert_eq!(
            decode(&body),
            Ok(Plain::ZSet(vec![
                (b"a".to_vec(), 1.5),
                (b"b".to_vec(), f64::INFINITY),
                (b"c".to_vec(), f64::NEG_INFINITY),
                (b"d".to_vec(), -0.002),
            ]))
        );
        assert_eq!(decode(b"\x03\x01\x01a\xfd"), Err(Error::Body));
        assert_eq!(decode(b"\x03\x01\x01a\x03abc"), Err(Error::Body));
    }

    fn ziplist_sample() -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let long14 = vec![b'x'; 300];
        let long32 = vec![b'y'; 16389];
        let entries = vec![
            zl_str(b"a"),
            zl_str(&long14),
            zl_str(&long32),
            zl_raw(ZIP_INT_8B, &(-5i8).to_le_bytes()),
            zl_raw(ZIP_INT_16B, &(-300i16).to_le_bytes()),
            zl_raw(ZIP_INT_24B, &(-70000i32).to_le_bytes()[..3]),
            zl_raw(ZIP_INT_24B, &(8_000_000i32).to_le_bytes()[..3]),
            zl_raw(ZIP_INT_32B, &(-2_000_000_000i32).to_le_bytes()),
            zl_raw(ZIP_INT_64B, &i64::MIN.to_le_bytes()),
            vec![0xF1],
            vec![0xF7],
            vec![0xFD],
            zl_str(b""),
        ];
        let mut want = vec![b"a".to_vec(), long14, long32];
        want.extend(strs(&[
            "-5",
            "-300",
            "-70000",
            "8000000",
            "-2000000000",
            "-9223372036854775808",
            "0",
            "6",
            "12",
            "",
        ]));
        (entries, want)
    }

    #[test]
    fn ziplists() {
        let (entries, want) = ziplist_sample();
        for big in [false, true] {
            let blob = zl(&entries, big);
            assert_eq!(
                decode(&blob_body(TYPE_LIST_ZIPLIST, &blob)),
                Ok(Plain::List(want.clone()))
            );
        }
        // Compressed blob.
        let blob = zl(&entries, false);
        let body = lzf_body(TYPE_LIST_ZIPLIST, &lzf_literals(&blob), blob.len() as u64);
        assert_eq!(decode(&body), Ok(Plain::List(want.clone())));

        // Quicklist 1: ziplist nodes, an empty one skipped.
        let mut body = vec![TYPE_LIST_QUICKLIST, 3];
        for node in [
            zl(&entries[..2], false),
            zl(&[], false),
            zl(&entries[2..], false),
        ] {
            put_len(&mut body, node.len() as u64);
            body.extend_from_slice(&node);
        }
        assert_eq!(decode(&body), Ok(Plain::List(want)));

        let pairs = [zl_str(b"f1"), zl_str(b"v1"), zl_str(b"f2"), vec![0xF8]];
        assert_eq!(
            decode(&blob_body(TYPE_HASH_ZIPLIST, &zl(&pairs, false))),
            Ok(Plain::Hash(vec![
                (b"f1".to_vec(), b"v1".to_vec()),
                (b"f2".to_vec(), b"7".to_vec()),
            ]))
        );
        let scored = [
            zl_str(b"m1"),
            zl_str(b"1.5"),
            zl_str(b"m2"),
            vec![0xF4],
            zl_str(b"m3"),
            zl_str(b"-inf"),
        ];
        assert_eq!(
            decode(&blob_body(TYPE_ZSET_ZIPLIST, &zl(&scored, false))),
            Ok(Plain::ZSet(vec![
                (b"m1".to_vec(), 1.5),
                (b"m2".to_vec(), 3.0),
                (b"m3".to_vec(), f64::NEG_INFINITY),
            ]))
        );
    }

    #[test]
    fn ziplist_damage_is_refused() {
        let blob = zl(&[zl_str(b"a"), zl_str(b"b")], false);
        let check = |blob: &[u8]| decode(&blob_body(TYPE_LIST_ZIPLIST, blob));
        assert!(check(&blob).is_ok());
        let mut bad = blob.clone();
        bad[0] += 1; // total bytes
        assert_eq!(check(&bad), Err(Error::Body));
        let mut bad = blob.clone();
        bad[4] += 1; // tail offset
        assert_eq!(check(&bad), Err(Error::Body));
        let mut bad = blob.clone();
        bad[8] = 3; // count
        assert_eq!(check(&bad), Err(Error::Body));
        let mut bad = blob.clone();
        bad[8..10].copy_from_slice(&u16::MAX.to_le_bytes()); // "count it yourself"
        assert!(check(&bad).is_ok());
        let mut bad = blob.clone();
        bad[13] = 2; // second entry's prevlen
        assert_eq!(check(&bad), Err(Error::Body));
        let mut bad = blob.clone();
        bad[11] = 0xC1; // no such encoding
        assert_eq!(check(&bad), Err(Error::Body));
        // Odd hash, empty list.
        let odd = zl(&[zl_str(b"f")], false);
        assert_eq!(
            decode(&blob_body(TYPE_HASH_ZIPLIST, &odd)),
            Err(Error::Body)
        );
        assert_eq!(check(&zl(&[], false)), Err(Error::Body));
    }

    fn listpack_sample() -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let s12 = vec![b'q'; 200];
        let s32 = vec![b'r'; 20000];
        let entries = vec![
            lp_int(5),
            lp_int(127),
            lp_str(b"hi"),
            lp_str(b""),
            lp_int(-1),
            lp_int(4095),
            lp_int(-4096),
            lp_str(&s12),
            lp_str(&s32),
            lp_int(-30000),
            lp_int(-8_000_000),
            lp_int(2_000_000_000),
            lp_int(i64::MAX),
        ];
        let mut want = strs(&["5", "127", "hi", "", "-1", "4095", "-4096"]);
        want.push(s12);
        want.push(s32);
        want.extend(strs(&[
            "-30000",
            "-8000000",
            "2000000000",
            "9223372036854775807",
        ]));
        (entries, want)
    }

    #[test]
    fn listpacks() {
        let (entries, want) = listpack_sample();
        assert_eq!(entries[1], [127]);
        assert_eq!(entries[4][0] & 0xE0, 0xC0);
        assert_eq!(entries[10][0], 0xF2);
        let blob = lp(&entries);
        assert_eq!(
            decode(&blob_body(TYPE_SET_LISTPACK, &blob)),
            Ok(Plain::Set(want.clone()))
        );

        // Quicklist 2: a packed node, an empty one, a plain one.
        let mut body = vec![TYPE_LIST_QUICKLIST_2, 3];
        for (container, blob) in [
            (QUICKLIST_PACKED, blob.clone()),
            (QUICKLIST_PACKED, lp(&[])),
            (QUICKLIST_PLAIN, b"plain".to_vec()),
        ] {
            put_len(&mut body, container);
            put_len(&mut body, blob.len() as u64);
            body.extend_from_slice(&blob);
        }
        let mut list = want.clone();
        list.push(b"plain".to_vec());
        assert_eq!(decode(&body), Ok(Plain::List(list)));
        body[2] = 3; // no such container
        assert_eq!(decode(&body), Err(Error::Body));

        let pairs = [lp_str(b"f"), lp_int(-7), lp_str(b"g"), lp_str(b"v")];
        assert_eq!(
            decode(&blob_body(TYPE_HASH_LISTPACK, &lp(&pairs))),
            Ok(Plain::Hash(vec![
                (b"f".to_vec(), b"-7".to_vec()),
                (b"g".to_vec(), b"v".to_vec()),
            ]))
        );
        let scored = [lp_str(b"a"), lp_int(-3), lp_str(b"b"), lp_str(b"2.5")];
        assert_eq!(
            decode(&blob_body(TYPE_ZSET_LISTPACK, &lp(&scored))),
            Ok(Plain::ZSet(vec![
                (b"a".to_vec(), -3.0),
                (b"b".to_vec(), 2.5)
            ]))
        );

        // A wrong backlen, a wrong count, an unused encoding.
        let mut bad = lp(&[lp_str(b"a"), lp_str(b"b")]);
        bad[8] = 3;
        assert_eq!(
            decode(&blob_body(TYPE_SET_LISTPACK, &bad)),
            Err(Error::Body)
        );
        let mut bad = lp(&[lp_str(b"a")]);
        bad[4] = 2;
        assert_eq!(
            decode(&blob_body(TYPE_SET_LISTPACK, &bad)),
            Err(Error::Body)
        );
        let bad = lp(&[vec![0xF5]]);
        assert_eq!(
            decode(&blob_body(TYPE_SET_LISTPACK, &bad)),
            Err(Error::Body)
        );
    }

    #[test]
    fn intsets() {
        let cases: [(u32, &[i64]); 3] = [
            (2, &[-32768, -1, 0, 7, 32767]),
            (4, &[-2_147_483_648, 0, 2_147_483_647]),
            (8, &[i64::MIN, -1, i64::MAX]),
        ];
        for (width, values) in cases {
            let want = values.iter().map(|v| v.to_string().into_bytes()).collect();
            let body = blob_body(TYPE_SET_INTSET, &intset_blob(width, values));
            assert_eq!(decode(&body), Ok(Plain::Set(want)));
        }
        let bad: [(u32, &[i64]); 4] = [(2, &[1, 1]), (2, &[2, 1]), (4, &[]), (3, &[1])];
        for (width, values) in bad {
            let body = blob_body(TYPE_SET_INTSET, &intset_blob(width, values));
            assert_eq!(decode(&body), Err(Error::Body), "{width} {values:?}");
        }
        let mut short = intset_blob(4, &[1, 2]);
        short.pop();
        assert_eq!(
            decode(&blob_body(TYPE_SET_INTSET, &short)),
            Err(Error::Body)
        );
    }

    #[test]
    fn zipmaps() {
        let big = vec![b'v'; 300];
        let blob = zipmap_blob(2, &[(b"name", b"x", 0), (b"big", &big, 3)]);
        assert_eq!(
            decode(&blob_body(TYPE_HASH_ZIPMAP, &blob)),
            Ok(Plain::Hash(vec![
                (b"name".to_vec(), b"x".to_vec()),
                (b"big".to_vec(), big.clone()),
            ]))
        );
        let unknown = zipmap_blob(254, &[(b"a", b"1", 0)]);
        assert!(decode(&blob_body(TYPE_HASH_ZIPMAP, &unknown)).is_ok());
        for bad in [
            zipmap_blob(3, &[(b"a", b"1", 0)]),
            zipmap_blob(2, &[(b"a", b"1", 0), (b"a", b"2", 1)]),
            zipmap_blob(0, &[]),
        ] {
            assert_eq!(decode(&blob_body(TYPE_HASH_ZIPMAP, &bad)), Err(Error::Body));
        }
    }

    #[test]
    fn counts_and_lzf_are_held_to_what_the_payload_can_back() {
        let junk = vec![0xffu8; 1 << 20];
        assert!(Reader::new(&junk).room(1 << 40) <= MAX_RESERVE);
        assert_eq!(Reader::new(&junk[..10]).room(1 << 40), 10);
        assert_eq!(Reader::new(&junk).room(3), 3);
        // Three blobs of 100 bytes, each charged twice: as LZF output and
        // as an item.
        let packed = lzf_literals(&[b'x'; 100]);
        let mut body = vec![TYPE_LIST, 3];
        for _ in 0..3 {
            body.extend_from_slice(&lzf_body(TYPE_STRING, &packed, 100)[1..]);
        }
        let cost = 3 * (100 + 100 + ITEM_COST);
        assert!(decode_within(&body, cost).is_ok());
        assert_eq!(decode_within(&body, cost - 1), Err(Error::Body));
    }

    #[test]
    fn packed_items_are_charged_and_a_payload_without_lzf_always_fits() {
        // 1000 one-byte items in a listpack: 2 bytes each in the payload,
        // ITEM_COST + 1 each decoded.
        let blob = lp(&(0..1000).map(|i| lp_int(i % 10)).collect::<Vec<_>>());
        let mut body = vec![TYPE_LIST_QUICKLIST_2];
        put_len(&mut body, 1);
        put_len(&mut body, QUICKLIST_PACKED);
        put_string(&mut body, &blob);
        let cost = (blob.len() + ITEM_COST) + 1000 * (ITEM_COST + 1);
        assert!(decode_within(&body, cost).is_ok());
        assert_eq!(decode_within(&body, cost - 1), Err(Error::Body));
        // The budget of a payload covers it with no LZF allowance at all.
        assert!(BYTE_CREDIT * body.len() >= cost);
        assert!(decode(&body).is_ok());
    }

    #[test]
    fn lzf_strings() {
        // "hello " then a long back reference (12 bytes) and a short one
        // (3 bytes, overlapping its own output).
        let packed = [
            0x05, b'h', b'e', b'l', b'l', b'o', b' ', 0xE0, 0x03, 0x05, 0x20, 0x00,
        ];
        let want = b"hello hello hello    ".to_vec();
        let body = lzf_body(TYPE_STRING, &packed, want.len() as u64);
        assert_eq!(decode(&body), Ok(Plain::Str(want.clone())));

        // Wrong declared length, either way.
        for n in [want.len() as u64 - 1, want.len() as u64 + 1, 0] {
            assert_eq!(decode(&lzf_body(TYPE_STRING, &packed, n)), Err(Error::Body));
        }
        // A reference before the start of the output.
        let before = [0x00, b'a', 0x20, 0x01];
        assert_eq!(decode(&lzf_body(TYPE_STRING, &before, 4)), Err(Error::Body));
        // Claims the input cannot back.
        let lit = lzf_literals(b"abc");
        assert_eq!(
            decode(&lzf_body(TYPE_STRING, &lit, 1 << 40)),
            Err(Error::Body)
        );
        assert_eq!(
            decode(&lzf_body(TYPE_STRING, &lit, 4 * 88 + 1)),
            Err(Error::Body)
        );
        // A truncated back reference.
        assert_eq!(
            decode(&lzf_body(TYPE_STRING, &[0x00, b'a', 0xE0], 9)),
            Err(Error::Body)
        );
    }

    #[test]
    fn refused_bodies() {
        let mut nan_zset = vec![TYPE_ZSET_2, 1, 1, b'a'];
        nan_zset.extend_from_slice(&f64::NAN.to_le_bytes());
        let mut dup_zset = vec![TYPE_ZSET_2, 2];
        for _ in 0..2 {
            dup_zset.extend_from_slice(&[1, b'a']);
            dup_zset.extend_from_slice(&1.0f64.to_le_bytes());
        }
        let nan_ziplist = zl(&[zl_str(b"a"), zl_str(b"nan")], false);
        let bad = [
            vec![TYPE_LIST, 0],
            vec![TYPE_SET, 0],
            vec![TYPE_ZSET, 0],
            vec![TYPE_HASH, 0],
            vec![TYPE_ZSET_2, 0],
            vec![TYPE_LIST_QUICKLIST, 0],
            vec![TYPE_LIST_QUICKLIST_2, 0],
            vec![TYPE_SET, 2, 1, b'a', 1, b'a'],
            vec![TYPE_HASH, 2, 1, b'f', 0, 1, b'f', 0],
            dup_zset,
            nan_zset,
            blob_body(TYPE_ZSET_ZIPLIST, &nan_ziplist),
            blob_body(TYPE_SET_LISTPACK, &lp(&[lp_str(b"1"), lp_int(1)])),
            blob_body(TYPE_SET_LISTPACK, &lp(&[])),
            // An encoded string where a count belongs.
            vec![TYPE_LIST, 0xC0, 1, 1, b'a'],
            // A length form that does not exist.
            vec![TYPE_STRING, 0x82, 0, 0, 0, 1, b'a'],
            vec![TYPE_STRING, 0xC4],
            // Trailing bytes.
            vec![TYPE_STRING, 1, b'a', 0],
            // Truncated.
            vec![TYPE_STRING, 2, b'a'],
            vec![TYPE_STRING, 0xC2, 1, 2, 3],
        ];
        for body in bad {
            assert_eq!(decode(&body), Err(Error::Body), "{body:?}");
        }
        // Streams, modules, function libraries, hashes with TTLs, unknown
        // and private types.
        for t in [
            6,
            7,
            8,
            15,
            19,
            21,
            22,
            23,
            24,
            25,
            30,
            PRIVATE_TYPES,
            0xF5,
            0xFF,
        ] {
            assert_eq!(decode(&[t, 1, b'a']), Err(Error::Body), "type {t}");
            assert_eq!(
                decode(&blob_body(t, &lp(&[lp_str(b"a")]))),
                Err(Error::Body)
            );
        }
    }

    #[test]
    fn huge_claims_fail_without_allocating() {
        let huge = [0x81, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        for t in [
            TYPE_STRING,
            TYPE_LIST,
            TYPE_SET,
            TYPE_HASH,
            TYPE_ZSET_2,
            TYPE_LIST_QUICKLIST,
        ] {
            let mut body = vec![t];
            body.extend_from_slice(&huge);
            body.extend_from_slice(&[1, b'a']);
            assert_eq!(decode(&body), Err(Error::Body));
        }
        let mut lzf = vec![TYPE_STRING, ENCVAL_PREFIX | ENC_LZF, 1];
        lzf.extend_from_slice(&huge);
        lzf.push(0);
        assert_eq!(decode(&lzf), Err(Error::Body));
        let mut blob = vec![0xff; 4];
        blob.extend_from_slice(&[0xff, 0xff, 0x80, 0x80]);
        for t in [
            TYPE_LIST_ZIPLIST,
            TYPE_SET_INTSET,
            TYPE_SET_LISTPACK,
            TYPE_HASH_ZIPMAP,
        ] {
            assert_eq!(decode(&blob_body(t, &blob)), Err(Error::Body));
        }
    }

    fn sample_payloads() -> Vec<Vec<u8>> {
        vec![
            hex("00 c0 0a 06 00 f8 72 3f c5 fb fb 5f 28"),
            encode(&Plain::Hash(vec![(b"field".to_vec(), b"value".to_vec())])),
            encode(&Plain::ZSet(vec![
                (b"m".to_vec(), 1.5),
                (b"n".to_vec(), -2.0),
            ])),
            encode(&Plain::Str(vec![7; 300])),
        ]
    }

    #[test]
    fn any_single_byte_change_is_caught() {
        for payload in sample_payloads() {
            for i in 0..payload.len() {
                for mask in 1..=255u8 {
                    let mut bad = payload.clone();
                    bad[i] ^= mask;
                    assert_eq!(open(&bad), Err(Error::Envelope), "byte {i} ^ {mask}");
                }
            }
        }
    }

    #[test]
    fn any_truncation_is_caught() {
        for payload in sample_payloads() {
            for n in 0..payload.len() {
                assert_eq!(open(&payload[..n]), Err(Error::Envelope), "{n} bytes");
            }
            let body = open(&payload).unwrap();
            assert!(decode(body).is_ok());
            for n in 0..body.len() {
                assert_eq!(decode(&body[..n]), Err(Error::Body), "body cut at {n}");
            }
        }
    }

    /// xorshift64*: deterministic, no dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }

        fn byte(&mut self) -> u8 {
            (self.next() >> 32) as u8
        }
    }

    const KNOWN_TYPES: [u8; 16] = [0, 1, 2, 3, 4, 5, 9, 10, 11, 12, 13, 14, 16, 17, 18, 20];

    /// Small valid bodies of every form we read.
    fn corpus() -> Vec<Vec<u8>> {
        let (zl_entries, _) = ziplist_sample();
        let small_zl = zl(&zl_entries[3..], false);
        let (lp_entries, _) = listpack_sample();
        let small_lp = lp(&[&lp_entries[..7], &lp_entries[9..]].concat());
        let mut quick2 = vec![TYPE_LIST_QUICKLIST_2, 2, 2];
        put_len(&mut quick2, small_lp.len() as u64);
        quick2.extend_from_slice(&small_lp);
        quick2.extend_from_slice(&[1, 2, b'p', b'q']);
        let mut quick1 = vec![TYPE_LIST_QUICKLIST, 1];
        put_len(&mut quick1, small_zl.len() as u64);
        quick1.extend_from_slice(&small_zl);
        let mut out = vec![
            blob_body(TYPE_LIST_ZIPLIST, &small_zl),
            blob_body(TYPE_HASH_ZIPLIST, &zl(&zl_entries[3..11], true)),
            blob_body(TYPE_SET_LISTPACK, &small_lp),
            blob_body(TYPE_HASH_LISTPACK, &lp(&lp_entries[..6])),
            blob_body(
                TYPE_ZSET_LISTPACK,
                &lp(&[lp_str(b"a"), lp_int(-3), lp_str(b"b"), lp_str(b"2.5")]),
            ),
            blob_body(TYPE_SET_INTSET, &intset_blob(2, &[1, 2, 3])),
            blob_body(TYPE_SET_INTSET, &intset_blob(8, &[-1, 9])),
            blob_body(TYPE_HASH_ZIPMAP, &zipmap_blob(1, &[(b"a", b"bc", 2)])),
            lzf_body(TYPE_STRING, &[0x00, b'a', 0xE0, 0x10, 0x00], 26),
            lzf_body(
                TYPE_LIST_ZIPLIST,
                &lzf_literals(&small_zl),
                small_zl.len() as u64,
            ),
            quick1,
            quick2,
            b"\x03\x02\x01a\x031.5\x01b\xfe".to_vec(),
        ];
        let plains = [
            Plain::Str(b"12345".to_vec()),
            Plain::List(strs(&["a", "-7", "1000", "100000"])),
            Plain::Set(strs(&["x", "y"])),
            Plain::ZSet(vec![(b"a".to_vec(), 1.0), (b"b".to_vec(), f64::INFINITY)]),
            Plain::Hash(vec![(b"f".to_vec(), b"v".to_vec())]),
        ];
        for p in &plains {
            out.push(open(&encode(p)).unwrap().to_vec());
        }
        for body in &out {
            assert!(decode(body).is_ok(), "corpus entry {body:?}");
        }
        out
    }

    #[test]
    fn random_bodies_never_panic() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut body = Vec::new();
        for _ in 0..50_000 {
            body.clear();
            let len = rng.below(64);
            for _ in 0..len {
                body.push(rng.byte());
            }
            if !body.is_empty() && rng.below(4) != 0 {
                body[0] = KNOWN_TYPES[rng.below(KNOWN_TYPES.len())];
            }
            let _ = decode(&body);
            let _ = open(&body);
        }
    }

    #[test]
    fn mutated_bodies_never_panic() {
        const SPECIAL: [u8; 10] = [0x00, 0x01, 0x3f, 0x40, 0x7f, 0x80, 0x81, 0xc3, 0xfe, 0xff];
        let huge: [&[u8]; 4] = [
            &[0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            &[0x80, 0xff, 0xff, 0xff, 0xff],
            &[0xff, 0xff, 0xff, 0x7f],
            &[0xfe, 0xff, 0xff, 0xff, 0xff],
        ];
        let mut rng = Rng(0x0123_4567_89ab_cdef);
        let corpus = corpus();
        let (mut tried, mut decoded) = (0, 0);
        for seed in &corpus {
            for _ in 0..3000 {
                let mut body = seed.clone();
                for _ in 0..1 + rng.below(3) {
                    if body.is_empty() {
                        break;
                    }
                    let i = rng.below(body.len());
                    match rng.below(6) {
                        0 => body[i] ^= rng.byte() | 1,
                        1 => body[i] = SPECIAL[rng.below(SPECIAL.len())],
                        2 => body.truncate(i),
                        3 => body.insert(i, rng.byte()),
                        4 => {
                            let h = huge[rng.below(huge.len())];
                            let end = (i + h.len()).min(body.len());
                            body.splice(i..end, h.iter().copied());
                        }
                        _ => {
                            body.remove(i);
                        }
                    }
                }
                tried += 1;
                if decode(&body).is_ok() {
                    decoded += 1;
                }
            }
        }
        // The point is that none panicked; most must have been refused.
        assert!(decoded < tried / 2, "{decoded} of {tried} decoded");
    }
}
