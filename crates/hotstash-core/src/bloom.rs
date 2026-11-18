//! Bloom filters (BF.*), as valkey-bloom 1.0.1 keeps them: the same sizing,
//! hashing and bit layout, so that with `bf.bloom-use-random-seed no` a key
//! answers exactly as there, false positives included. `docs/bloom-spec.md`
//! is the behaviour this follows, section by section.
//!
//! A key holds one or more sub-filters. Each new one has `expansion` times
//! the capacity of the last and its error rate times `tightening`; with an
//! expansion of 0 the first is the only one.

use crate::cmd::Bytes;
use crate::rdb::{self, Input};
use crate::resp::Reply;
use crate::zset::format_score;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};

/// The seed of every filter while `bf.bloom-use-random-seed` is off.
pub const FIXED_SEED: [u8; 32] = [
    89, 15, 245, 34, 234, 120, 17, 218, 167, 20, 216, 9, 59, 62, 123, 217, 29, 137, 138, 115, 62,
    152, 136, 135, 48, 127, 151, 205, 40, 7, 51, 131,
];

// Errors, as valkey-bloom words them.
const ERROR: &str = "ERROR";
const FULL: &str = "ERR non scaling filter is full";
const NOT_FOUND: &str = "ERR not found";
const ITEM_EXISTS: &str = "ERR item exists";
const INVALID_INFO: &str = "ERR invalid information value";
const INVALID_SEED: &str = "ERR invalid seed";
const BAD_EXPANSION: &str = "ERR bad expansion";
const BAD_CAPACITY: &str = "ERR bad capacity";
const BAD_ERROR_RATE: &str = "ERR bad error rate";
const ERROR_RATE_RANGE: &str = "ERR (0 < error rate range < 1)";
const BAD_TIGHTENING: &str = "ERR bad tightening ratio";
const TIGHTENING_RANGE: &str = "ERR (0 < tightening ratio range < 1)";
const CAPACITY_ZERO: &str = "ERR (capacity should be larger than 0)";
const FP_DEGRADES: &str = "ERR false positive degrades to 0 on scale out";
const UNKNOWN_ARGUMENT: &str = "ERR unknown argument received";
const OVER_LIMIT: &str = "ERR operation exceeds bloom object memory limit";
const SCALE_OVER_LIMIT: &str =
    "ERR provided VALIDATESCALETO causes bloom object to exceed memory limit";
const TOO_MANY_FILTERS: &str = "ERR bloom object reached max number of filters";
const SCALE_FP_DEGRADES: &str =
    "ERR provided VALIDATESCALETO causes false positive to degrade to 0";
const KEY_EXISTS: &str = "BUSYKEY Target key name already exists.";
const DECODE_FAILED: &str = "ERR bloom object decoding failed";
const UNSUPPORTED_VERSION: &str = "ERR bloom object decoding failed. Unsupported version";
const NONSCALING_SCALE_TO: &str = "ERR cannot use NONSCALING and VALIDATESCALETO options together";

/// What valkey-bloom counts for its structures in `BF.INFO SIZE`: the
/// object, one slot of its vector of filters, and a filter beside its bits.
const OBJECT_BYTES: u64 = 48;
const SLOT_BYTES: u64 = 8;
const FILTER_BYTES: u64 = 24 + 184;
/// The header of a filter's bits in valkey-bloom's encodings: version,
/// length in bytes, number of hashes and seed.
const HEADER: usize = 1 + 8 + 4 + 32;
const BITMAP_VERSION: u8 = 1;
/// The first byte of a BF.LOAD payload.
const OBJECT_VERSION: u8 = 1;
/// No filter valkey-bloom makes has more hashes (about 1030 at most); a
/// payload that claims more is refused rather than looped over.
const MAX_HASHES: u32 = 4096;
/// The largest prime below 2^64: the modulus of the derived hashes.
const PRIME: u64 = 0xFFFF_FFFF_FFFF_FFC5;
/// Sub-filters a key may have, as in valkey-bloom.
const MAX_FILTERS: usize = i32::MAX as usize;
/// The most sub-filters BF.LOAD takes from a client. Every check and add
/// looks at each one, and a payload costs nothing to make, unlike the adds
/// that grow a filter that far. valkey-bloom takes up to `MAX_FILTERS`.
const MAX_LOADED_FILTERS: usize = 4096;

/// `bf.*` defaults, as in valkey-bloom.
const DEFAULT_CAPACITY: i64 = 100;
const DEFAULT_EXPANSION: u32 = 2;
const DEFAULT_FP_RATE: f64 = 0.01;
const DEFAULT_TIGHTENING: f64 = 0.5;
const DEFAULT_LIMIT: u64 = 128 << 20;

/// The module type valkey-bloom registers, as an RDB module ID: the name
/// `bloomfltr` in 6-bit characters, then encoding version 1.
pub(crate) const MODULE_ID: u64 = module_id(b"bloomfltr", 1);
// Module values in RDB are a run of typed primitives, ended by EOF.
const OP_EOF: u64 = 0;
const OP_UINT: u64 = 2;
const OP_DOUBLE: u64 = 4;
const OP_STRING: u64 = 5;

pub(crate) const fn module_id(name: &[u8; 9], encver: u64) -> u64 {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut id = 0u64;
    let mut i = 0;
    while i < name.len() {
        let mut j = 0;
        while CHARS[j] != name[i] {
            j += 1;
        }
        id = id << 6 | j as u64;
        i += 1;
    }
    id << 10 | encver
}

/// The `bf.*` settings a command runs with, read when it arrives.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Settings {
    pub capacity: i64,
    /// 0: new filters do not scale.
    pub expansion: u32,
    pub fp_rate: f64,
    pub tightening: f64,
    /// `bf.bloom-memory-usage-limit`.
    pub limit: u64,
    /// Whether the limit holds. Not for what a primary sends: the primary
    /// checked it, and a replica that refused would differ from it.
    pub enforce: bool,
    pub random_seed: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            capacity: DEFAULT_CAPACITY,
            expansion: DEFAULT_EXPANSION,
            fp_rate: DEFAULT_FP_RATE,
            tightening: DEFAULT_TIGHTENING,
            limit: DEFAULT_LIMIT,
            enforce: true,
            random_seed: true,
        }
    }
}

/// The `bf.*` settings of a server, for CONFIG GET and CONFIG SET.
pub struct Config {
    capacity: AtomicI64,
    expansion: AtomicU32,
    fp_rate: AtomicU64,
    tightening: AtomicU64,
    limit: AtomicU64,
    random_seed: AtomicBool,
    defrag: AtomicBool,
    /// The two rates as they were set: CONFIG GET shows them so.
    shown: Mutex<(String, String)>,
}

impl Default for Config {
    fn default() -> Self {
        let s = Settings::default();
        Config {
            capacity: AtomicI64::new(s.capacity),
            expansion: AtomicU32::new(s.expansion),
            fp_rate: AtomicU64::new(s.fp_rate.to_bits()),
            tightening: AtomicU64::new(s.tightening.to_bits()),
            limit: AtomicU64::new(s.limit),
            random_seed: AtomicBool::new(s.random_seed),
            defrag: AtomicBool::new(true),
            shown: Mutex::new(("0.01".into(), "0.5".into())),
        }
    }
}

/// One `bf.*` setting CONFIG SET checked, to apply once all of them are.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Capacity(i64),
    Expansion(u32),
    FpRate(f64, String),
    Tightening(f64, String),
    Limit(u64),
    RandomSeed(bool),
    Defrag(bool),
}

impl Config {
    pub fn settings(&self, enforce: bool) -> Settings {
        Settings {
            capacity: self.capacity.load(Ordering::Relaxed),
            expansion: self.expansion.load(Ordering::Relaxed),
            fp_rate: f64::from_bits(self.fp_rate.load(Ordering::Relaxed)),
            tightening: f64::from_bits(self.tightening.load(Ordering::Relaxed)),
            limit: self.limit.load(Ordering::Relaxed),
            enforce,
            random_seed: self.random_seed.load(Ordering::Relaxed),
        }
    }

    /// Every setting with its value, as CONFIG GET shows it.
    pub fn shown(&self) -> [(&'static str, String); 7] {
        let yes = |on: &AtomicBool| {
            if on.load(Ordering::Relaxed) {
                "yes"
            } else {
                "no"
            }
        };
        let (fp_rate, tightening) = self.shown.lock().clone();
        [
            (
                "bf.bloom-capacity",
                self.capacity.load(Ordering::Relaxed).to_string(),
            ),
            (
                "bf.bloom-expansion",
                self.expansion.load(Ordering::Relaxed).to_string(),
            ),
            ("bf.bloom-fp-rate", fp_rate),
            ("bf.bloom-tightening-ratio", tightening),
            (
                "bf.bloom-memory-usage-limit",
                self.limit.load(Ordering::Relaxed).to_string(),
            ),
            ("bf.bloom-use-random-seed", yes(&self.random_seed).into()),
            ("bf.bloom-defrag-enabled", yes(&self.defrag).into()),
        ]
    }

    /// The change CONFIG SET of `name` to `value` makes; `None` when the
    /// name is not one of these, the reason when the value is refused.
    pub fn check(name: &[u8], value: &[u8]) -> Option<Result<Change, String>> {
        let text = String::from_utf8_lossy(value);
        let int = |min: i64, max: i64| match text.parse::<i64>() {
            Ok(n) if (min..=max).contains(&n) => Ok(n),
            Ok(_) => Err(format!(
                "argument must be between {min} and {max} inclusive"
            )),
            Err(_) => Err("argument couldn't be parsed into an integer".to_string()),
        };
        let rate = |range: &str| match text.parse::<f64>() {
            Ok(x) if x > 0.0 && x < 1.0 => Ok(x),
            Ok(_) => Err(range.to_string()),
            Err(_) => Err("Invalid floating-point value".to_string()),
        };
        let flag = || match value.to_ascii_lowercase().as_slice() {
            b"yes" => Ok(true),
            b"no" => Ok(false),
            _ => Err("argument must be 'yes' or 'no'".to_string()),
        };
        Some(match name {
            b"bf.bloom-capacity" => int(1, i64::MAX).map(Change::Capacity),
            b"bf.bloom-expansion" => int(0, u32::MAX.into()).map(|n| Change::Expansion(n as u32)),
            b"bf.bloom-fp-rate" => {
                rate(ERROR_RATE_RANGE).map(|x| Change::FpRate(x, text.to_string()))
            }
            b"bf.bloom-tightening-ratio" => {
                rate(TIGHTENING_RANGE).map(|x| Change::Tightening(x, text.to_string()))
            }
            b"bf.bloom-memory-usage-limit" => int(0, i64::MAX).map(|n| Change::Limit(n as u64)),
            b"bf.bloom-use-random-seed" => flag().map(Change::RandomSeed),
            b"bf.bloom-defrag-enabled" => flag().map(Change::Defrag),
            _ => return None,
        })
    }

    pub fn apply(&self, change: Change) {
        match change {
            Change::Capacity(n) => self.capacity.store(n, Ordering::Relaxed),
            Change::Expansion(n) => self.expansion.store(n, Ordering::Relaxed),
            Change::FpRate(x, text) => {
                self.fp_rate.store(x.to_bits(), Ordering::Relaxed);
                self.shown.lock().0 = text;
            }
            Change::Tightening(x, text) => {
                self.tightening.store(x.to_bits(), Ordering::Relaxed);
                self.shown.lock().1 = text;
            }
            Change::Limit(n) => self.limit.store(n, Ordering::Relaxed),
            Change::RandomSeed(on) => self.random_seed.store(on, Ordering::Relaxed),
            Change::Defrag(on) => self.defrag.store(on, Ordering::Relaxed),
        }
    }
}

/// SipHash-1-3 of `item` as Rust's `Hash` for `[u8]` feeds a hasher: the
/// length as 8 bytes little-endian, then the bytes.
fn sip13(key: &[u8], item: &[u8]) -> u64 {
    fn round(v: &mut [u64; 4]) {
        v[0] = v[0].wrapping_add(v[1]);
        v[1] = v[1].rotate_left(13) ^ v[0];
        v[0] = v[0].rotate_left(32);
        v[2] = v[2].wrapping_add(v[3]);
        v[3] = v[3].rotate_left(16) ^ v[2];
        v[0] = v[0].wrapping_add(v[3]);
        v[3] = v[3].rotate_left(21) ^ v[0];
        v[2] = v[2].wrapping_add(v[1]);
        v[1] = v[1].rotate_left(17) ^ v[2];
        v[2] = v[2].rotate_left(32);
    }
    fn compress(v: &mut [u64; 4], m: u64) {
        v[3] ^= m;
        round(v);
        v[0] ^= m;
    }
    let word = |bytes: &[u8]| {
        let mut w = [0u8; 8];
        w[..bytes.len()].copy_from_slice(bytes);
        u64::from_le_bytes(w)
    };
    let (k0, k1) = (word(&key[..8]), word(&key[8..16]));
    let mut v = [
        k0 ^ 0x736f_6d65_7073_6575,
        k1 ^ 0x646f_7261_6e64_6f6d,
        k0 ^ 0x6c79_6765_6e65_7261,
        k1 ^ 0x7465_6462_7974_6573,
    ];
    compress(&mut v, item.len() as u64);
    let (words, rest) = item.as_chunks::<8>();
    for w in words {
        compress(&mut v, u64::from_le_bytes(*w));
    }
    let total = (item.len() as u64).wrapping_add(8);
    compress(&mut v, (total & 0xff) << 56 | word(rest));
    v[2] ^= 0xff;
    round(&mut v);
    round(&mut v);
    round(&mut v);
    v[0] ^ v[1] ^ v[2] ^ v[3]
}

/// Bytes of the bits of a filter for `capacity` items at error rate `fp`,
/// computed as the `bloomfilter` crate does.
fn bitmap_bytes(capacity: i64, fp: f64) -> u64 {
    let log2 = std::f64::consts::LN_2;
    let log2_2 = log2 * log2;
    ((capacity as f64) * f64::ln(fp) / (-8.0 * log2_2)).ceil() as u64
}

fn hash_count(bits: u64, capacity: i64) -> u32 {
    let k = (bits as f64 / capacity as f64 * f64::ln(2.0)).round() as u32;
    k.max(1)
}

/// What a filter counts for in `BF.INFO SIZE`.
fn filter_bytes(capacity: i64, fp: f64) -> u64 {
    FILTER_BYTES.saturating_add(bitmap_bytes(capacity, fp))
}

/// What the vector of `n` filters takes: valkey-bloom's grows from one
/// slot to four, then doubles.
fn slots(n: u64) -> u64 {
    if n <= 1 {
        1
    } else {
        n.max(4).next_power_of_two()
    }
}

/// The error rate of sub-filter `n`; `None` once it is no longer above
/// the smallest normal double.
fn fp_at(fp: f64, n: usize, tightening: f64) -> Option<f64> {
    let n = i32::try_from(n).ok()?;
    Some(fp * tightening.powi(n)).filter(|x| *x > f64::MIN_POSITIVE)
}

#[derive(Clone, Debug, PartialEq)]
struct Filter {
    capacity: i64,
    items: i64,
    hashes: u32,
    bits: Box<[u8]>,
}

impl Filter {
    fn new(capacity: i64, fp: f64) -> Result<Filter, &'static str> {
        let bytes = usize::try_from(bitmap_bytes(capacity, fp)).map_err(|_| OVER_LIMIT)?;
        let mut bits = Vec::new();
        // A replica takes any size its primary did; one that cannot be
        // had is refused rather than allowed to end the process.
        bits.try_reserve_exact(bytes).map_err(|_| OVER_LIMIT)?;
        bits.resize(bytes, 0);
        Ok(Filter {
            capacity,
            items: 0,
            hashes: hash_count(bytes as u64 * 8, capacity),
            bits: bits.into_boxed_slice(),
        })
    }

    /// The bits of an item whose two hashes are `h`.
    fn offsets(&self, h: (u64, u64)) -> impl Iterator<Item = usize> {
        let bits = self.bits.len() as u64 * 8;
        (0..self.hashes).map(move |i| {
            let x = match i {
                0 => h.0,
                1 => h.1,
                i => h.0.wrapping_add(u64::from(i).wrapping_mul(h.1)) % PRIME,
            };
            (x % bits) as usize
        })
    }

    fn has(&self, h: (u64, u64)) -> bool {
        self.offsets(h)
            .all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0)
    }

    fn set(&mut self, h: (u64, u64)) {
        let offsets: Vec<usize> = self.offsets(h).collect();
        for bit in offsets {
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }
}

/// A BF.* value.
#[derive(Clone, Debug, PartialEq)]
pub struct Bloom {
    /// 0: never adds a filter.
    expansion: u32,
    fp_rate: f64,
    tightening: f64,
    random_seed: bool,
    seed: [u8; 32],
    /// Never empty.
    filters: Vec<Filter>,
}

/// What a new filter is made with.
struct Params {
    capacity: i64,
    fp_rate: f64,
    tightening: f64,
    expansion: u32,
    seed: Option<[u8; 32]>,
}

impl Params {
    fn of(s: &Settings) -> Params {
        Params {
            capacity: s.capacity,
            fp_rate: s.fp_rate,
            tightening: s.tightening,
            expansion: s.expansion,
            seed: None,
        }
    }
}

impl Bloom {
    fn create(p: &Params, s: &Settings) -> Result<Bloom, &'static str> {
        let size = (OBJECT_BYTES + SLOT_BYTES).saturating_add(filter_bytes(p.capacity, p.fp_rate));
        if s.enforce && size > s.limit {
            return Err(OVER_LIMIT);
        }
        let (seed, random_seed) = match p.seed {
            Some(seed) => (seed, seed != FIXED_SEED),
            None if s.random_seed => {
                let mut seed = [0u8; 32];
                getrandom::getrandom(&mut seed).map_err(|_| ERROR)?;
                (seed, true)
            }
            None => (FIXED_SEED, false),
        };
        Ok(Bloom {
            expansion: p.expansion,
            fp_rate: p.fp_rate,
            tightening: p.tightening,
            random_seed,
            seed,
            filters: vec![Filter::new(p.capacity, p.fp_rate)?],
        })
    }

    fn hash(&self, item: &[u8]) -> (u64, u64) {
        (sip13(&self.seed[..16], item), sip13(&self.seed[16..], item))
    }

    pub fn contains(&self, item: &[u8]) -> bool {
        let h = self.hash(item);
        self.filters.iter().any(|f| f.has(h))
    }

    /// Add `item`: `true` when it was not there yet.
    fn add(&mut self, item: &[u8], s: &Settings) -> Result<bool, &'static str> {
        let h = self.hash(item);
        if self.filters.iter().any(|f| f.has(h)) {
            return Ok(false);
        }
        let n = self.filters.len();
        let usage = self.memory_usage();
        let Some(last) = self.filters.last_mut() else {
            return Ok(false);
        };
        if last.items < last.capacity {
            last.set(h);
            last.items += 1;
            return Ok(true);
        }
        if self.expansion == 0 {
            return Err(FULL);
        }
        if n >= MAX_FILTERS {
            return Err(TOO_MANY_FILTERS);
        }
        let fp = fp_at(self.fp_rate, n, self.tightening).ok_or(FP_DEGRADES)?;
        let capacity = last
            .capacity
            .checked_mul(self.expansion.into())
            .ok_or(BAD_CAPACITY)?;
        if s.enforce && usage.saturating_add(filter_bytes(capacity, fp)) > s.limit {
            return Err(OVER_LIMIT);
        }
        let mut filter = Filter::new(capacity, fp)?;
        filter.set(h);
        filter.items = 1;
        self.filters.push(filter);
        Ok(true)
    }

    /// Items added, false positives aside: BF.CARD.
    pub fn card(&self) -> i64 {
        // A loaded filter may claim any counts.
        self.filters
            .iter()
            .fold(0, |n, f| n.saturating_add(f.items))
    }

    pub fn capacity(&self) -> i64 {
        self.filters
            .iter()
            .fold(0, |n, f| n.saturating_add(f.capacity))
    }

    pub fn filters(&self) -> usize {
        self.filters.len()
    }

    /// `BF.INFO SIZE`, counted as valkey-bloom counts it.
    pub fn memory_usage(&self) -> u64 {
        let bits: u64 = self
            .filters
            .iter()
            .map(|f| FILTER_BYTES + f.bits.len() as u64)
            .sum();
        OBJECT_BYTES + SLOT_BYTES * slots(self.filters.len() as u64) + bits
    }

    /// The BF.LOAD payload of this filter: what valkey-bloom's AOF rewrite
    /// writes, a version byte and the object in bincode.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(OBJECT_VERSION);
        out.extend_from_slice(&self.expansion.to_le_bytes());
        out.extend_from_slice(&self.fp_rate.to_le_bytes());
        out.extend_from_slice(&self.tightening.to_le_bytes());
        out.push(u8::from(self.random_seed));
        out.extend_from_slice(&(self.filters.len() as u64).to_le_bytes());
        for filter in &self.filters {
            out.extend_from_slice(&((HEADER + filter.bits.len()) as u64).to_le_bytes());
            self.put_bitmap(out, filter);
            out.extend_from_slice(&filter.items.to_le_bytes());
            out.extend_from_slice(&filter.capacity.to_le_bytes());
        }
    }

    /// A filter's bits behind their header, as the `bloomfilter` crate
    /// writes them.
    fn put_bitmap(&self, out: &mut Vec<u8>, filter: &Filter) {
        out.push(BITMAP_VERSION);
        out.extend_from_slice(&(filter.bits.len() as u64).to_le_bytes());
        out.extend_from_slice(&filter.hashes.to_le_bytes());
        out.extend_from_slice(&self.seed);
        out.extend_from_slice(&filter.bits);
    }

    /// The filter of a BF.LOAD payload. `limit` is the memory limit to
    /// hold it to, if any.
    pub fn decode(blob: &[u8], limit: Option<u64>) -> Result<Bloom, &'static str> {
        let (&version, mut rest) = blob.split_first().ok_or(DECODE_FAILED)?;
        if version != OBJECT_VERSION {
            return Err(UNSUPPORTED_VERSION);
        }
        let bloom = decode_body(&mut rest)
            .filter(|_| rest.is_empty())
            .ok_or(DECODE_FAILED)?;
        if !(bloom.fp_rate > 0.0 && bloom.fp_rate < 1.0) {
            return Err(ERROR_RATE_RANGE);
        }
        // valkey-bloom reports this one with the message of the error rate.
        if !(bloom.tightening > 0.0 && bloom.tightening < 1.0) {
            return Err(ERROR_RATE_RANGE);
        }
        if bloom.filters.len() >= MAX_FILTERS
            || (limit.is_some() && bloom.filters.len() > MAX_LOADED_FILTERS)
        {
            return Err(TOO_MANY_FILTERS);
        }
        // Each sub-filter has a smaller error rate than the one before; past
        // the smallest double there is none. Loading from RDB checks this too.
        let n = bloom.filters.len();
        if (0..n).any(|i| fp_at(bloom.fp_rate, i, bloom.tightening).is_none()) {
            return Err(DECODE_FAILED);
        }
        if limit.is_some_and(|limit| bloom.memory_usage() > limit) {
            return Err(OVER_LIMIT);
        }
        Ok(bloom)
    }

    /// Append the body of the RDB module value valkey-bloom saves: counts
    /// and rates, then each filter's capacity and bits, with the item count
    /// of the last one (the others are full).
    pub(crate) fn write_rdb(&self, out: &mut Vec<u8>) {
        let uint = |out: &mut Vec<u8>, n: u64| {
            rdb::put_len(out, OP_UINT);
            rdb::put_len(out, n);
        };
        let double = |out: &mut Vec<u8>, x: f64| {
            rdb::put_len(out, OP_DOUBLE);
            out.extend_from_slice(&x.to_le_bytes());
        };
        uint(out, self.filters.len() as u64);
        uint(out, self.expansion.into());
        double(out, self.fp_rate);
        double(out, self.tightening);
        uint(out, self.random_seed.into());
        for (i, filter) in self.filters.iter().enumerate() {
            uint(out, filter.capacity as u64);
            if i + 1 == self.filters.len() {
                uint(out, filter.items as u64);
            }
            let mut bitmap = Vec::with_capacity(HEADER + filter.bits.len());
            self.put_bitmap(&mut bitmap, filter);
            rdb::put_len(out, OP_STRING);
            rdb::put_raw(out, &bitmap);
        }
        rdb::put_len(out, OP_EOF);
    }

    /// The value `write_rdb` writes, its module ID already read. `None`
    /// when anything is off, as valkey-bloom refuses it.
    pub(crate) fn read_rdb<I: Input + ?Sized>(r: &mut I) -> Option<Bloom> {
        let uint = |r: &mut I| (r.len()? == OP_UINT).then(|| r.len()).flatten();
        let double = |r: &mut I| {
            (r.len()? == OP_DOUBLE).then_some(())?;
            Some(f64::from_le_bytes(r.array()?))
        };
        let n = uint(r)?;
        let expansion = u32::try_from(uint(r)?).ok()?;
        let fp_rate = double(r)?;
        let tightening = double(r)?;
        let random_seed = match uint(r)? {
            0 => false,
            1 => true,
            _ => return None,
        };
        if !(fp_rate > 0.0 && fp_rate < 1.0 && tightening > 0.0 && tightening < 1.0) || n == 0 {
            return None;
        }
        let mut filters = Vec::new();
        let mut seed = None;
        for i in 0..n {
            let capacity = i64::try_from(uint(r)?).ok().filter(|c| *c > 0)?;
            fp_at(fp_rate, usize::try_from(i).ok()?, tightening)?;
            let items = if i + 1 == n {
                i64::try_from(uint(r)?).ok()?
            } else {
                capacity
            };
            (r.len()? == OP_STRING).then_some(())?;
            let bitmap = r.string()?;
            let (bits_seed, hashes, bits) = read_bitmap(&bitmap)?;
            if *seed.get_or_insert(bits_seed) != bits_seed {
                return None;
            }
            filters.push(Filter {
                capacity,
                items,
                hashes,
                bits: bits.into(),
            });
        }
        let seed = seed?;
        if !random_seed && seed != FIXED_SEED {
            return None;
        }
        (r.len()? == OP_EOF).then_some(Bloom {
            expansion,
            fp_rate,
            tightening,
            random_seed,
            seed,
            filters,
        })
    }
}

/// The seed, number of hashes and bits of a filter, from the `bloomfilter`
/// crate's form. Bits that cannot be hashed into are refused, unlike there.
fn read_bitmap(bytes: &[u8]) -> Option<([u8; 32], u32, &[u8])> {
    if bytes.len() < HEADER || bytes[0] != BITMAP_VERSION {
        return None;
    }
    let len = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
    let hashes = u32::from_le_bytes(bytes[9..13].try_into().ok()?);
    let seed: [u8; 32] = bytes[13..HEADER].try_into().ok()?;
    let bits = &bytes[HEADER..];
    let fits = (1..=MAX_HASHES).contains(&hashes) && !bits.is_empty();
    (fits && len == bits.len() as u64).then_some((seed, hashes, bits))
}

/// The body of a BF.LOAD payload after its version byte, in bincode.
fn decode_body(input: &mut &[u8]) -> Option<Bloom> {
    fn take<'a>(input: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        let (head, tail) = input.split_at_checked(n)?;
        *input = tail;
        Some(head)
    }
    fn u64_of(input: &mut &[u8]) -> Option<u64> {
        Some(u64::from_le_bytes(take(input, 8)?.try_into().ok()?))
    }
    let expansion = u32::from_le_bytes(take(input, 4)?.try_into().ok()?);
    let fp_rate = f64::from_bits(u64_of(input)?);
    let tightening = f64::from_bits(u64_of(input)?);
    let random_seed = match take(input, 1)? {
        [0] => false,
        [1] => true,
        _ => return None,
    };
    let n = u64_of(input)?;
    if n == 0 {
        return None;
    }
    let mut filters = Vec::new();
    let mut seed = None;
    for _ in 0..n {
        let len = usize::try_from(u64_of(input)?).ok()?;
        let (bits_seed, hashes, bits) = read_bitmap(take(input, len)?)?;
        if *seed.get_or_insert(bits_seed) != bits_seed {
            return None;
        }
        let items = i64::from_le_bytes(take(input, 8)?.try_into().ok()?);
        let capacity = i64::from_le_bytes(take(input, 8)?.try_into().ok()?);
        if capacity <= 0 || items < 0 {
            return None;
        }
        filters.push(Filter {
            capacity,
            items,
            hashes,
            bits: bits.into(),
        });
    }
    Some(Bloom {
        expansion,
        fp_rate,
        tightening,
        random_seed,
        seed: seed?,
        filters,
    })
}

/// The total capacity a filter can reach before its error rate or the
/// memory limit stops it (BF.INFO MAXSCALEDCAPACITY); with `scale_to`,
/// whether it reaches that (BF.INSERT VALIDATESCALETO).
fn max_scaled(
    capacity: i64,
    fp_rate: f64,
    scale_to: Option<i64>,
    tightening: f64,
    expansion: u32,
    limit: u64,
) -> Result<i64, &'static str> {
    let (mut cap, mut total, mut n, mut used) = (capacity, 0i64, 0usize, 0u64);
    while scale_to.is_none_or(|to| total < to) {
        let Some(fp) = fp_at(fp_rate, n, tightening) else {
            return scale_to.map_or(Ok(total), |_| Err(SCALE_FP_DEGRADES));
        };
        let size = filter_bytes(cap, fp);
        let object = (OBJECT_BYTES + SLOT_BYTES * slots(n as u64 + 1))
            .saturating_add(used)
            .saturating_add(size);
        if object > limit {
            return scale_to.map_or(Ok(total), |_| Err(SCALE_OVER_LIMIT));
        }
        used = used.saturating_add(size);
        total = total.checked_add(cap).ok_or(BAD_CAPACITY)?;
        cap = cap.checked_mul(expansion.into()).ok_or(BAD_CAPACITY)?;
        n += 1;
    }
    Ok(total)
}

/// A field BF.INFO asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Capacity,
    Size,
    Filters,
    Items,
    Error,
    Tightening,
    Expansion,
    MaxScaled,
    /// Not a field; refused only once the key is found.
    Unknown,
}

/// BF.INSERT's options; those not given come from the settings.
#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    capacity: Option<i64>,
    fp_rate: Option<f64>,
    tightening: Option<f64>,
    /// `Some(0)`: NONSCALING.
    expansion: Option<u32>,
    seed: Option<[u8; 32]>,
    nocreate: bool,
    scale_to: Option<i64>,
    items: Vec<Bytes>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BfOp {
    /// BF.EXISTS, or with `multi` BF.MEXISTS.
    Exists {
        items: Vec<Bytes>,
        multi: bool,
    },
    Card,
    /// No field: all of them.
    Info(Option<Field>),
    /// BF.ADD, or with `multi` BF.MADD.
    Add {
        items: Vec<Bytes>,
        multi: bool,
    },
    Reserve {
        fp_rate: f64,
        capacity: i64,
        /// `None`: the setting; `Some(0)`: NONSCALING.
        expansion: Option<u32>,
    },
    Insert(Box<Insert>),
    Load(Bytes),
}

/// A BF.* command with the settings it runs under, which the engine fills
/// in when the command reaches it.
#[derive(Debug, Clone, PartialEq)]
pub struct BfCmd {
    pub op: BfOp,
    pub settings: Settings,
}

impl BfOp {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            BfOp::Add { .. } | BfOp::Reserve { .. } | BfOp::Insert(_) | BfOp::Load(_)
        )
    }
}

impl BfCmd {
    pub fn is_write(&self) -> bool {
        self.op.is_write()
    }

    pub fn is_info(&self) -> bool {
        matches!(self.op, BfOp::Info(_))
    }
}

/// An option word, compared as valkey-bloom does: upper-cased after any
/// bytes that are not UTF-8 became U+FFFD.
fn word(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).to_uppercase()
}

fn rate(raw: &[u8], bad: &'static str, range: &'static str) -> Result<f64, Reply> {
    match String::from_utf8_lossy(raw).parse::<f64>() {
        Ok(x) if x > 0.0 && x < 1.0 => Ok(x),
        Ok(_) => Err(Reply::err(range)),
        Err(_) => Err(Reply::err(bad)),
    }
}

fn capacity(raw: &[u8]) -> Result<i64, Reply> {
    match String::from_utf8_lossy(raw).parse::<i64>() {
        Ok(n) if n >= 1 => Ok(n),
        Ok(0) => Err(Reply::err(CAPACITY_ZERO)),
        _ => Err(Reply::err(BAD_CAPACITY)),
    }
}

fn expansion(raw: &[u8]) -> Result<u32, Reply> {
    match String::from_utf8_lossy(raw).parse::<u32>() {
        Ok(n) if n >= 1 => Ok(n),
        _ => Err(Reply::err(BAD_EXPANSION)),
    }
}

/// A BF.* command: its key and what to do. `name` is upper case. Each
/// command counts its own arguments, as valkey-bloom's do.
pub(crate) fn parse(name: &str, args: &mut Vec<Bytes>) -> Result<(Bytes, BfOp), Reply> {
    let n = args.len();
    let fits = match name {
        "BF.ADD" | "BF.EXISTS" | "BF.LOAD" => n == 3,
        "BF.MADD" | "BF.MEXISTS" => n >= 3,
        "BF.CARD" => n == 2,
        "BF.RESERVE" => (4..=6).contains(&n),
        "BF.INSERT" => n >= 2,
        "BF.INFO" => n == 2 || n == 3,
        _ => false,
    };
    let arity = || {
        Reply::Error(format!(
            "ERR wrong number of arguments for '{name}' command"
        ))
    };
    if !fits {
        return Err(arity());
    }
    let rest = args.split_off(2);
    let key = args.pop().unwrap_or_default();
    let op = match name {
        "BF.ADD" | "BF.MADD" => BfOp::Add {
            items: rest,
            multi: name == "BF.MADD",
        },
        "BF.EXISTS" | "BF.MEXISTS" => BfOp::Exists {
            items: rest,
            multi: name == "BF.MEXISTS",
        },
        "BF.CARD" => BfOp::Card,
        "BF.LOAD" => BfOp::Load(rest.into_iter().next().unwrap_or_default()),
        "BF.INFO" => BfOp::Info(rest.first().map(|raw| match word(raw).as_str() {
            "CAPACITY" => Field::Capacity,
            "SIZE" => Field::Size,
            "FILTERS" => Field::Filters,
            "ITEMS" => Field::Items,
            "ERROR" => Field::Error,
            "TIGHTENING" => Field::Tightening,
            "EXPANSION" => Field::Expansion,
            "MAXSCALEDCAPACITY" => Field::MaxScaled,
            _ => Field::Unknown,
        })),
        "BF.RESERVE" => {
            let fp_rate = rate(&rest[0], BAD_ERROR_RATE, ERROR_RATE_RANGE)?;
            let capacity = capacity(&rest[1])?;
            let expansion = match (rest.len(), rest.get(2).map(|raw| word(raw))) {
                (2, _) => None,
                (3, Some(w)) if w == "NONSCALING" => Some(0),
                (4, Some(w)) if w == "EXPANSION" => Some(expansion(&rest[3])?),
                _ => return Err(Reply::err(ERROR)),
            };
            BfOp::Reserve {
                fp_rate,
                capacity,
                expansion,
            }
        }
        _ => BfOp::Insert(Box::new(parse_insert(rest, arity)?)),
    };
    Ok((key, op))
}

fn parse_insert(mut rest: Vec<Bytes>, arity: impl Fn() -> Reply) -> Result<Insert, Reply> {
    let mut ins = Insert {
        capacity: None,
        fp_rate: None,
        tightening: None,
        expansion: None,
        seed: None,
        nocreate: false,
        scale_to: None,
        items: Vec::new(),
    };
    let mut i = 0;
    while i < rest.len() {
        let option = word(&rest[i]);
        let takes_value = matches!(
            option.as_str(),
            "ERROR" | "TIGHTENING" | "CAPACITY" | "SEED" | "EXPANSION" | "VALIDATESCALETO"
        );
        if takes_value {
            i += 1;
        }
        let Some(value) = rest.get(i) else {
            return Err(arity());
        };
        match option.as_str() {
            "ERROR" => ins.fp_rate = Some(rate(value, BAD_ERROR_RATE, ERROR_RATE_RANGE)?),
            "TIGHTENING" => {
                ins.tightening = Some(rate(value, BAD_TIGHTENING, TIGHTENING_RANGE)?);
            }
            "CAPACITY" => ins.capacity = Some(capacity(value)?),
            "SEED" => {
                let seed = value.as_slice().try_into();
                ins.seed = Some(seed.map_err(|_| Reply::err(INVALID_SEED))?);
            }
            "NOCREATE" => ins.nocreate = true,
            "NONSCALING" => ins.expansion = Some(0),
            "EXPANSION" => ins.expansion = Some(expansion(value)?),
            "VALIDATESCALETO" => ins.scale_to = Some(capacity(value)?),
            "ITEMS" => {
                ins.items = rest.split_off(i + 1);
                if ins.items.is_empty() {
                    return Err(arity());
                }
                break;
            }
            _ => return Err(Reply::err(UNKNOWN_ARGUMENT)),
        }
        i += 1;
    }
    Ok(ins)
}

impl Insert {
    fn params(&self, s: &Settings) -> Params {
        Params {
            capacity: self.capacity.unwrap_or(s.capacity),
            fp_rate: self.fp_rate.unwrap_or(s.fp_rate),
            tightening: self.tightening.unwrap_or(s.tightening),
            expansion: self.expansion.unwrap_or(s.expansion),
            seed: self.seed,
        }
    }

    /// VALIDATESCALETO, which BF.INSERT checks before it looks at the key.
    pub fn check(&self, s: &Settings) -> Result<(), Reply> {
        let Some(to) = self.scale_to else {
            return Ok(());
        };
        let p = self.params(s);
        if p.expansion == 0 {
            return Err(Reply::err(NONSCALING_SCALE_TO));
        }
        max_scaled(
            p.capacity,
            p.fp_rate,
            Some(to),
            p.tightening,
            p.expansion,
            s.limit,
        )
        .map(drop)
        .map_err(Reply::err)
    }
}

fn double(x: f64) -> Reply {
    Reply::Bulk(format_score(x))
}

/// A read: BF.EXISTS, BF.MEXISTS, BF.CARD and BF.INFO. `bloom` is the
/// key's filter, `None` when the key is missing.
pub fn read(op: &BfOp, bloom: Option<&Bloom>, s: &Settings) -> Reply {
    let has = |item: &Bytes| Reply::Int(bloom.is_some_and(|b| b.contains(item)).into());
    match op {
        BfOp::Exists {
            items,
            multi: false,
        } => items.first().map_or(Reply::Int(0), has),
        BfOp::Exists { items, multi: true } => Reply::Array(items.iter().map(has).collect()),
        BfOp::Card => Reply::Int(bloom.map_or(0, Bloom::card)),
        BfOp::Info(field) => match bloom {
            None => Reply::err(NOT_FOUND),
            Some(bloom) => info(bloom, *field, s.limit),
        },
        // Writes go through `write`.
        BfOp::Add { .. } | BfOp::Reserve { .. } | BfOp::Insert(_) | BfOp::Load(_) => {
            Reply::err(ERROR)
        }
    }
}

fn info(b: &Bloom, field: Option<Field>, limit: u64) -> Reply {
    let scales = b.expansion > 0;
    let max = || {
        let first = b.filters.first().map_or(0, |f| f.capacity);
        max_scaled(first, b.fp_rate, None, b.tightening, b.expansion, limit)
    };
    let int = |n: i64| Reply::Int(n);
    let size = i64::try_from(b.memory_usage()).unwrap_or(i64::MAX);
    let Some(field) = field else {
        let label = |s: &str| Reply::Simple(s.to_owned());
        let mut out = vec![
            label("Capacity"),
            int(b.capacity()),
            label("Size"),
            int(size),
            label("Number of filters"),
            int(b.filters.len() as i64),
            label("Number of items inserted"),
            int(b.card()),
            label("Error rate"),
            double(b.fp_rate),
            label("Expansion rate"),
        ];
        if !scales {
            out.push(Reply::Nil);
            return Reply::Array(out);
        }
        let max = match max() {
            Ok(max) => max,
            Err(e) => return Reply::err(e),
        };
        out.extend([
            int(b.expansion.into()),
            label("Tightening ratio"),
            double(b.tightening),
            label("Max scaled capacity"),
            int(max),
        ]);
        return Reply::Array(out);
    };
    match field {
        Field::Capacity => int(b.capacity()),
        Field::Size => int(size),
        Field::Filters => int(b.filters.len() as i64),
        Field::Items => int(b.card()),
        Field::Error => double(b.fp_rate),
        Field::Tightening if scales => double(b.tightening),
        Field::Expansion if scales => int(b.expansion.into()),
        Field::Expansion => Reply::Nil,
        Field::MaxScaled if scales => max().map_or_else(Reply::err, int),
        Field::Tightening | Field::MaxScaled | Field::Unknown => Reply::err(INVALID_INFO),
    }
}

/// What a write did, for the shard to keep.
pub struct Done {
    pub reply: Reply,
    /// The filter a missing key now holds.
    pub new: Option<Box<Bloom>>,
    /// Some item went in.
    pub added: bool,
    /// What a replica runs to reach the same state; empty when nothing
    /// changed.
    pub repl: Vec<u8>,
}

impl Done {
    fn refused(reply: Reply) -> Done {
        Done {
            reply,
            new: None,
            added: false,
            repl: Vec::new(),
        }
    }
}

/// Add `items` in order, stopping at the first error: the reply, whether
/// any went in, and how many were looked at without error.
fn add_items(
    bloom: &mut Bloom,
    items: &[Bytes],
    multi: bool,
    s: &Settings,
) -> (Reply, bool, usize) {
    let mut replies = Vec::with_capacity(items.len());
    let mut added = false;
    for item in items {
        match bloom.add(item, s) {
            Ok(new) => {
                added |= new;
                replies.push(Reply::Int(new.into()));
            }
            Err(e) => {
                let done = replies.len();
                replies.push(Reply::err(e));
                return (reply_of(replies, multi), added, done);
            }
        }
    }
    let done = replies.len();
    (reply_of(replies, multi), added, done)
}

fn reply_of(mut replies: Vec<Reply>, multi: bool) -> Reply {
    if multi {
        Reply::Array(replies)
    } else {
        replies.pop().unwrap_or(Reply::Int(0))
    }
}

/// How a replica makes the filter `bloom` became from nothing, as
/// valkey-bloom sends it: BF.INSERT with every property, the seed
/// included, and the items. Items an error stopped at are left out, so
/// that a replica, which does not hold its filters to the memory limit,
/// stops where the primary did.
fn created(key: &[u8], bloom: &Bloom, items: &[Bytes]) -> Vec<u8> {
    let first = bloom.filters.first().map_or(0, |f| f.capacity).to_string();
    let fp = bloom.fp_rate.to_string();
    let tightening = bloom.tightening.to_string();
    let expansion = bloom.expansion.to_string();
    let mut args: Vec<&[u8]> = vec![
        b"BF.INSERT",
        key,
        b"CAPACITY",
        first.as_bytes(),
        b"ERROR",
        fp.as_bytes(),
        b"TIGHTENING",
        tightening.as_bytes(),
        b"SEED",
        &bloom.seed,
    ];
    if bloom.expansion == 0 {
        args.push(b"NONSCALING");
    } else {
        args.extend([&b"EXPANSION"[..], expansion.as_bytes()]);
    }
    if !items.is_empty() {
        args.push(b"ITEMS");
        args.extend(items.iter().map(Vec::as_slice));
    }
    let mut out = Vec::new();
    crate::repl::command(&mut out, &args);
    out
}

/// Items added to a filter that was there: BF.ADD or BF.MADD of those
/// looked at, which a replica, whose filter is the same, adds the same.
fn added(key: &[u8], items: &[Bytes], multi: bool) -> Vec<u8> {
    let name: &[u8] = if multi { b"BF.MADD" } else { b"BF.ADD" };
    let mut args = vec![name, key];
    args.extend(items.iter().map(Vec::as_slice));
    let mut out = Vec::new();
    crate::repl::command(&mut out, &args);
    out
}

/// Add to `bloom`, which a write found at the key.
fn add_existing(key: &[u8], bloom: &mut Bloom, items: &[Bytes], multi: bool, s: &Settings) -> Done {
    let (reply, any, done) = add_items(bloom, items, multi, s);
    let repl = if any {
        added(key, &items[..done], multi)
    } else {
        Vec::new()
    };
    Done {
        reply,
        new: None,
        added: any,
        repl,
    }
}

/// Make a filter for the missing `key` with `p`, then add `items`.
fn add_new(
    key: &[u8],
    p: &Params,
    items: &[Bytes],
    multi: bool,
    reply: Option<Reply>,
    s: &Settings,
) -> Done {
    let mut bloom = match Bloom::create(p, s) {
        Ok(bloom) => bloom,
        Err(e) => return Done::refused(Reply::err(e)),
    };
    let (added, any, done) = add_items(&mut bloom, items, multi, s);
    Done {
        repl: created(key, &bloom, &items[..done]),
        reply: reply.unwrap_or(added),
        new: Some(Box::new(bloom)),
        added: any,
    }
}

/// A write: BF.ADD, BF.MADD, BF.RESERVE, BF.INSERT and BF.LOAD. `bloom`
/// is the key's filter, `None` when the key is missing.
pub fn write(op: BfOp, key: &[u8], bloom: Option<&mut Bloom>, s: &Settings) -> Done {
    match (op, bloom) {
        (BfOp::Add { items, multi }, Some(bloom)) => add_existing(key, bloom, &items, multi, s),
        (BfOp::Add { items, multi }, None) => add_new(key, &Params::of(s), &items, multi, None, s),
        (BfOp::Reserve { .. }, Some(_)) => Done::refused(Reply::err(ITEM_EXISTS)),
        (
            BfOp::Reserve {
                fp_rate,
                capacity,
                expansion,
            },
            None,
        ) => {
            let p = Params {
                capacity,
                fp_rate,
                expansion: expansion.unwrap_or(s.expansion),
                ..Params::of(s)
            };
            add_new(key, &p, &[], true, Some(Reply::Ok), s)
        }
        (BfOp::Insert(ins), Some(bloom)) => add_existing(key, bloom, &ins.items, true, s),
        (BfOp::Insert(ins), None) if ins.nocreate => Done::refused(Reply::err(NOT_FOUND)),
        (BfOp::Insert(ins), None) => add_new(key, &ins.params(s), &ins.items, true, None, s),
        (BfOp::Load(_), Some(_)) => Done::refused(Reply::err(KEY_EXISTS)),
        (BfOp::Load(blob), None) => {
            let limit = s.enforce.then_some(s.limit);
            match Bloom::decode(&blob, limit) {
                Ok(bloom) => {
                    let mut repl = Vec::new();
                    crate::repl::command(&mut repl, &[b"BF.LOAD", key, &blob]);
                    Done {
                        reply: Reply::Ok,
                        new: Some(Box::new(bloom)),
                        added: false,
                        repl,
                    }
                }
                Err(e) => Done::refused(Reply::err(e)),
            }
        }
        // Reads go through `read`.
        (BfOp::Exists { .. } | BfOp::Card | BfOp::Info(_), _) => Done::refused(Reply::err(ERROR)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed() -> Settings {
        Settings {
            random_seed: false,
            ..Settings::default()
        }
    }

    fn make(capacity: i64, fp_rate: f64, expansion: u32) -> Bloom {
        let p = Params {
            capacity,
            fp_rate,
            tightening: DEFAULT_TIGHTENING,
            expansion,
            seed: None,
        };
        Bloom::create(&p, &fixed()).unwrap()
    }

    #[test]
    fn module_id_is_valkey_blooms() {
        assert_eq!(MODULE_ID, 0x6e5a_2899_f96d_ac01);
    }

    #[test]
    fn siphash_matches_the_reference() {
        // SipHash-1-3 of le64(4) ++ "item" under the first half of the
        // fixed seed (bloom-spec.md 7.2).
        assert_eq!(sip13(&FIXED_SEED[..16], b"item"), 0x1589_599b_7117_5ebd);
    }

    #[test]
    fn sizes_follow_the_crate() {
        for (n, p, bytes, k) in [
            (100, 0.01, 120, 7),
            (1000, 0.01, 1199, 7),
            (1000, 0.001, 1798, 10),
            (1, 0.5, 1, 6),
            (2000, 0.5, 361, 1),
            (100, 0.5, 19, 1),
            (1000, 0.0001, 2397, 13),
        ] {
            let f = Filter::new(n, p).unwrap();
            assert_eq!((f.bits.len(), f.hashes), (bytes, k), "{n} {p}");
        }
    }

    #[test]
    fn bit_offsets_are_valkey_blooms() {
        let b = make(100, 0.01, 2);
        let h = b.hash(b"item");
        let offsets: Vec<usize> = b.filters[0].offsets(h).collect();
        assert_eq!(offsets, [701, 8, 461, 213, 925, 677, 429]);
        let offsets: Vec<usize> = b.filters[0].offsets(b.hash(b"foo")).collect();
        assert_eq!(offsets, [687, 90, 355, 189, 23, 817, 651]);
        let big = make(1000, 0.01, 2);
        let offsets: Vec<usize> = big.filters[0].offsets(big.hash(b"item")).collect();
        assert_eq!(offsets, [8173, 6208, 685, 6173, 2069, 7557, 3453]);
    }

    #[test]
    fn memory_and_max_scaled_capacity_match_upstream_constants() {
        assert_eq!(make(100, 0.01, 2).memory_usage(), 384);
        assert_eq!(make(1000, 0.01, 2).memory_usage(), 1463);
        let max = |cap, fp| max_scaled(cap, fp, None, 0.5, 2, DEFAULT_LIMIT).unwrap();
        assert_eq!(max(100, 0.01), 26_214_300);
        assert_eq!(max(2000, 0.5), 32_766_000);
        assert_eq!(max(10000, 0.001), 20_470_000);
        let to = |cap, fp, exp, n| max_scaled(cap, fp, Some(n), 0.5, exp, DEFAULT_LIMIT);
        assert!(to(100, 0.000_000_000_000_000_01, 25, 1_627_600).is_ok());
        assert_eq!(
            to(100, 0.000_000_000_000_000_01, 25, 1_627_601),
            Err(SCALE_OVER_LIMIT)
        );
        assert!(to(100, 0.01, 1, 101_600).is_ok());
        assert_eq!(to(100, 0.01, 1, 101_601), Err(SCALE_FP_DEGRADES));
    }

    #[test]
    fn scaling_adds_a_tighter_filter_only_when_full() {
        let s = fixed();
        let mut b = make(100, 0.01, 2);
        let mut added = 0;
        for i in 1..=300 {
            if b.add(format!("item{i}").as_bytes(), &s).unwrap() {
                added += 1;
            }
        }
        // bloom-spec.md 15.5, row 69 (simulated there, not confirmed).
        assert_eq!((b.filters.len(), b.card(), added), (2, 296, 296));
        assert_eq!(b.memory_usage(), 892);
        let mut ns = make(5, 0.01, 0);
        for i in 1..=5 {
            assert!(ns.add(format!("x{i}").as_bytes(), &s).unwrap());
        }
        assert_eq!(ns.add(b"x6", &s), Err(FULL));
        assert_eq!(ns.add(b"x1", &s), Ok(false));
    }

    #[test]
    fn encodings_round_trip() {
        let s = fixed();
        let mut b = make(5, 0.01, 2);
        for i in 0..12 {
            b.add(format!("x{i}").as_bytes(), &s).unwrap();
        }
        let mut blob = Vec::new();
        b.encode(&mut blob);
        assert_eq!(Bloom::decode(&blob, None), Ok(b.clone()));
        let mut body = Vec::new();
        b.write_rdb(&mut body);
        let mut r = rdb::Reader::new(&body);
        assert_eq!(Bloom::read_rdb(&mut r), Some(b));
        assert_eq!(Bloom::decode(b"", None), Err(DECODE_FAILED));
        assert_eq!(Bloom::decode(b"\x02abc", None), Err(UNSUPPORTED_VERSION));
        assert_eq!(
            Bloom::decode(&blob[..blob.len() - 1], None),
            Err(DECODE_FAILED)
        );
    }

    #[test]
    fn loading_from_a_client_holds_the_number_of_filters() {
        let mut b = make(1, 0.01, 1);
        b.tightening = 0.999_999;
        let one = b.filters[0].clone();
        b.filters = vec![one; MAX_LOADED_FILTERS + 1];
        let mut blob = Vec::new();
        b.encode(&mut blob);
        assert_eq!(Bloom::decode(&blob, Some(u64::MAX)), Err(TOO_MANY_FILTERS));
        // From a primary there is no limit to hold to.
        assert_eq!(
            Bloom::decode(&blob, None).map(|b| b.filters()),
            Ok(MAX_LOADED_FILTERS + 1)
        );
        b.filters.truncate(MAX_LOADED_FILTERS);
        let mut blob = Vec::new();
        b.encode(&mut blob);
        assert!(Bloom::decode(&blob, Some(u64::MAX)).is_ok());
        // An error rate that tightens below the smallest double.
        b.tightening = 1e-200;
        b.filters.truncate(3);
        let mut blob = Vec::new();
        b.encode(&mut blob);
        assert_eq!(Bloom::decode(&blob, None), Err(DECODE_FAILED));
    }

    #[test]
    fn decoding_refuses_filters_that_cannot_be_hashed_into() {
        let b = make(5, 0.01, 2);
        let mut blob = Vec::new();
        b.encode(&mut blob);
        // The number of hashes (offset 1 + 4 + 8 + 8 + 1 + 8 + 8 + 9).
        let at = 1 + 4 + 8 + 8 + 1 + 8 + 8 + 9;
        let mut zero = blob.clone();
        zero[at..at + 4].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(Bloom::decode(&zero, None), Err(DECODE_FAILED));
        let mut many = blob;
        many[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(Bloom::decode(&many, None), Err(DECODE_FAILED));
    }

    #[test]
    fn parse_counts_and_checks_arguments_in_order() {
        let args = |parts: &[&str]| parts.iter().map(|p| p.as_bytes().to_vec()).collect();
        let err = |parts: &[&str]| {
            let name = parts[0].to_ascii_uppercase();
            match parse(&name, &mut args(parts)) {
                Err(Reply::Error(e)) => e,
                other => panic!("{parts:?}: {other:?}"),
            }
        };
        assert_eq!(
            err(&["bf.add", "k"]),
            "ERR wrong number of arguments for 'BF.ADD' command"
        );
        assert_eq!(err(&["bf.reserve", "k", "abc", "0"]), BAD_ERROR_RATE);
        assert_eq!(err(&["bf.reserve", "k", "nan", "x"]), ERROR_RATE_RANGE);
        assert_eq!(err(&["bf.reserve", "k", "0.01", "0"]), CAPACITY_ZERO);
        assert_eq!(err(&["bf.reserve", "k", "0.01", "-1"]), BAD_CAPACITY);
        assert_eq!(err(&["bf.reserve", "k", "0.01", "10", "FOO"]), ERROR);
        assert_eq!(
            err(&["bf.reserve", "k", "0.01", "10", "EXPANSION", "0"]),
            BAD_EXPANSION
        );
        assert_eq!(err(&["bf.insert", "k", "HELLO", "WORLD"]), UNKNOWN_ARGUMENT);
        assert_eq!(
            err(&["bf.insert", "k", "ERROR", "0.5", "CAPACITY"]),
            "ERR wrong number of arguments for 'BF.INSERT' command"
        );
        assert_eq!(
            err(&["bf.insert", "k", "ITEMS"]),
            "ERR wrong number of arguments for 'BF.INSERT' command"
        );
        assert_eq!(err(&["bf.insert", "k", "SEED", "abc"]), INVALID_SEED);
        let (_, op) = parse(
            "BF.INSERT",
            &mut args(&["bf.insert", "k", "ITEMS", "1", "EXPANSION"]),
        )
        .unwrap();
        let BfOp::Insert(ins) = op else {
            panic!("{op:?}")
        };
        assert_eq!(ins.items.len(), 2);
    }
}
