//! Hash table with a resumable scan cursor.
//!
//! The standard library map cannot be iterated in pieces while it is being
//! modified, and rule 4 of the architecture needs exactly that: KEYS and
//! HGETALL walk a table a few buckets at a time and release the shard lock
//! in between. This table uses the reverse-binary cursor from Redis'
//! `dictScan`: an element that stays in the table for the whole scan is
//! reported exactly once, even when the table grows mid-scan. The table
//! never shrinks and grows in one step, which is what makes "exactly once"
//! hold (Redis only promises "at least once" because of incremental rehash
//! and shrinking).
//!
//! A bucket is one cache line with room for `SLOTS` entries, as in Valkey
//! 8.1's hashtable: next to the index of each entry it keeps a byte of the
//! entry's hash, so a lookup reads only the entry that matches, not every
//! entry that shares the bucket. A full bucket chains to an overflow one.
//! Entries live in fixed-size chunks, so growing the table never copies
//! them, and wastes at most one chunk.

use std::cell::Cell;
use xxhash_rust::xxh3::xxh3_64;

const NIL: u32 = u32::MAX;
const MIN_BUCKETS: usize = 1;
/// Entries one bucket line holds: 12 tag bytes and an overflow link make
/// 16 bytes, and 12 indices the other 48.
const SLOTS: usize = 12;
const SLOT_MASK: u32 = (1 << SLOTS) - 1;
/// The table doubles when it holds this many entries per bucket on
/// average. At 8 of 12, about one bucket in sixteen overflows just before
/// a growth, and almost none just after.
pub const MAX_LOAD: usize = 8;
/// Entries per chunk, after the first chunk has grown to this size.
const CHUNK_SHIFT: u32 = 10;
const CHUNK: usize = 1 << CHUNK_SHIFT;
/// Latest deadline an entry can hold, in milliseconds since the epoch
/// (year 10889); a later one is stored as this.
pub const EXPIRE_MAX: u64 = (1 << 48) - 1;
/// Where a new entry's use counter starts, so that it is not the first
/// to go under an LFU policy.
const FREQ_INIT: u8 = 5;
/// How much harder each step of the counter gets: the counter is
/// logarithmic, 255 stands for about a million uses.
const FREQ_LOG_FACTOR: u32 = 10;
/// The counter of an entry left alone loses one for every this many
/// lookups in the table.
const FREQ_DECAY_LOOKUPS: u32 = 1 << 16;

/// Longest key kept inside its entry. 16 bytes is what is left of a
/// 64-byte entry next to a 32-byte value and a deadline, and covers the
/// 16-byte keys of the usual benchmarks.
const INLINE_KEY: usize = 16;
/// First byte of a `Key` that lives on the heap; an inline key has its
/// length there.
const ON_HEAP: u8 = u8::MAX;

/// A key, inside the entry when it is short, so that comparing it reads
/// no memory beyond the entry, and costs no allocation of its own.
/// Longer keys live on the heap. Both forms start with a tag byte: the
/// length of an inline key, or `ON_HEAP`.
#[repr(C)]
union Key {
    inline: Inline,
    heap: Heap,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Inline {
    len: u8,
    bytes: [u8; INLINE_KEY],
}

/// A `Box<[u8]>` taken apart, packed to fit in an inline key's space.
#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Heap {
    tag: u8,
    ptr: *mut u8,
    len: usize,
}

// SAFETY: a `Key` owns its heap bytes alone, like the `Box<[u8]>` it was
// made from, and hands out only shared references to them.
unsafe impl Send for Key {}
// SAFETY: as above; nothing mutates a key through `&Key`.
unsafe impl Sync for Key {}

impl Key {
    const EMPTY: Key = Key {
        inline: Inline {
            len: 0,
            bytes: [0; INLINE_KEY],
        },
    };

    fn new(key: &[u8]) -> Key {
        if key.len() <= INLINE_KEY {
            let mut bytes = [0; INLINE_KEY];
            bytes[..key.len()].copy_from_slice(key);
            return Key {
                inline: Inline {
                    len: key.len() as u8,
                    bytes,
                },
            };
        }
        let boxed = Box::<[u8]>::from(key);
        let len = boxed.len();
        Key {
            heap: Heap {
                tag: ON_HEAP,
                ptr: Box::into_raw(boxed).cast::<u8>(),
                len,
            },
        }
    }

    fn tag(&self) -> u8 {
        // SAFETY: both forms are `repr(C)` with an initialised `u8` first.
        unsafe { self.inline.len }
    }

    /// Whether this is `key`. An inline key of 4 bytes or more is compared
    /// with two overlapping loads of 4 or 8 bytes from each side, not with
    /// a call to `memcmp`.
    fn is(&self, key: &[u8]) -> bool {
        let tag = self.tag();
        if tag == ON_HEAP {
            return self.as_bytes() == key;
        }
        let n = usize::from(tag);
        if n != key.len() {
            return false;
        }
        // SAFETY: the tag says this is the inline form, all initialised.
        let bytes = unsafe { &self.inline.bytes };
        match n {
            8.. => word(bytes, 0) == word(key, 0) && word(bytes, n - 8) == word(key, n - 8),
            4.. => half(bytes, 0) == half(key, 0) && half(bytes, n - 4) == half(key, n - 4),
            _ => bytes[..n] == *key,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        let tag = self.tag();
        if tag == ON_HEAP {
            // SAFETY: the tag says this is the heap form, which is all
            // initialised; it is copied out because it is packed.
            let heap = unsafe { self.heap };
            // SAFETY: `ptr` and `len` come from a live `Box<[u8]>` that this
            // key owns until it is dropped.
            unsafe { std::slice::from_raw_parts(heap.ptr, heap.len) }
        } else {
            // SAFETY: the tag says this is the inline form, whose first `tag`
            // bytes are initialised (`tag` is at most `INLINE_KEY`).
            unsafe { std::slice::from_raw_parts(self.inline.bytes.as_ptr(), usize::from(tag)) }
        }
    }
}

/// The 8 bytes of `s` from `at`.
fn word(s: &[u8], at: usize) -> u64 {
    let mut w = [0; 8];
    w.copy_from_slice(&s[at..at + 8]);
    u64::from_ne_bytes(w)
}

/// The 4 bytes of `s` from `at`.
fn half(s: &[u8], at: usize) -> u32 {
    let mut w = [0; 4];
    w.copy_from_slice(&s[at..at + 4]);
    u32::from_ne_bytes(w)
}

impl Drop for Key {
    fn drop(&mut self) {
        if self.tag() == ON_HEAP {
            // SAFETY: the heap form is all initialised.
            let heap = unsafe { self.heap };
            // SAFETY: `ptr` and `len` were taken from `Box::into_raw` in
            // `Key::new` and nothing else frees them.
            drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(heap.ptr, heap.len)) });
        }
    }
}

impl Clone for Key {
    fn clone(&self) -> Key {
        Key::new(self.as_bytes())
    }
}

#[derive(Clone)]
struct Entry<V> {
    /// The low bits of the key's hash: enough to pick a bucket, since the
    /// table never has more buckets than `u32` can count. The next free
    /// entry when vacant.
    hash: u32,
    /// The table's clock when the entry was last looked up.
    stamp: Cell<u32>,
    /// Deadline in milliseconds since the epoch, 0 for none: low 32 bits
    /// and high 16 bits, so that the entry stays one cache line.
    expire_lo: u32,
    expire_hi: u16,
    /// Logarithmic count of the lookups, as of `stamp`.
    freq: Cell<u8>,
    /// `Key::EMPTY` when vacant.
    key: Key,
    /// `None` when vacant.
    val: Option<V>,
}

impl<V> Entry<V> {
    fn expire(&self) -> u64 {
        u64::from(self.expire_lo) | u64::from(self.expire_hi) << 32
    }

    fn set_expire(&mut self, at: u64) {
        let at = at.min(EXPIRE_MAX);
        self.expire_lo = at as u32;
        self.expire_hi = (at >> 32) as u16;
    }
}

/// Bytes an entry of `Dict<V>` takes, so that the users of the table can
/// check that theirs fits in a cache line.
pub const fn entry_size<V>() -> usize {
    std::mem::size_of::<Entry<V>>()
}

/// One cache line of the table: for each slot, a tag byte (0 when the slot
/// is free) and the index of its entry.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Bucket {
    ctrl: [u8; SLOTS],
    /// Overflow bucket that continues this one, or `NIL`.
    next: u32,
    slots: [u32; SLOTS],
}

const _: () = assert!(std::mem::size_of::<Bucket>() == 64);

#[cfg(any(not(target_arch = "x86_64"), test))]
const LOW7: u64 = 0x7f7f_7f7f_7f7f_7f7f;

/// Bit i set when byte i of `x` is zero, for the eight bytes of `x`.
#[cfg(any(not(target_arch = "x86_64"), test))]
fn zero_bytes(x: u64) -> u32 {
    // Bit 7 of each byte that is zero; no carry crosses a byte.
    let high = !((x & LOW7).wrapping_add(LOW7) | x | LOW7);
    // Gathers bit 8i to bit 56 + i.
    ((high >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) as u32
}

/// `Bucket::matches` without SIMD, eight bytes at a time.
#[cfg(any(not(target_arch = "x86_64"), test))]
fn matches_swar(ctrl: [u8; SLOTS], tag: u8) -> u32 {
    let splat = u64::from(tag) * 0x0101_0101_0101_0101;
    let [a, b, c, d, e, f, g, h, i, j, k, l] = ctrl;
    let low = u64::from_le_bytes([a, b, c, d, e, f, g, h]);
    // The four bytes past the twelfth never match: none is zero.
    let high = u64::from(u32::from_le_bytes([i, j, k, l]) ^ splat as u32) | !0 << 32;
    zero_bytes(low ^ splat) | zero_bytes(high) << 8
}

impl Bucket {
    const EMPTY: Bucket = Bucket {
        ctrl: [0; SLOTS],
        next: NIL,
        slots: [0; SLOTS],
    };

    /// Bit i set when slot i holds `tag`; with 0, the free slots.
    #[cfg(target_arch = "x86_64")]
    fn matches(&self, tag: u8) -> u32 {
        use std::arch::x86_64::{
            _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
        };
        // SAFETY: SSE2 is part of x86_64, and the 16 bytes read are `ctrl`
        // and `next`, which `repr(C)` puts first in the bucket.
        let hits = unsafe {
            let head = _mm_loadu_si128(std::ptr::from_ref(self).cast());
            _mm_movemask_epi8(_mm_cmpeq_epi8(head, _mm_set1_epi8(tag as i8)))
        };
        hits as u32 & SLOT_MASK
    }

    /// Bit i set when slot i holds `tag`; with 0, the free slots.
    #[cfg(not(target_arch = "x86_64"))]
    fn matches(&self, tag: u8) -> u32 {
        matches_swar(self.ctrl, tag)
    }

    fn used(&self) -> u32 {
        !self.matches(0) & SLOT_MASK
    }
}

/// The byte of a hash that a bucket keeps for each entry: from all its 32
/// low bits, so that entries of one bucket differ in it however large the
/// table. Never 0, which marks a free slot.
fn tag_of(hash: u32) -> u8 {
    ((hash.wrapping_mul(0x9E37_79B1) >> 24) as u8).max(1)
}

/// Where an entry's slot is: in a bucket of the table (`None`) or in an
/// overflow bucket.
type Place = Option<usize>;

#[derive(Clone)]
pub struct Dict<V> {
    /// Length is a power of two.
    buckets: Vec<Bucket>,
    /// Buckets that full ones chain to; rebuilt on growth.
    overflow: Vec<Bucket>,
    /// Entry i is `chunks[i / CHUNK][i % CHUNK]`. Only the first chunk
    /// grows by reallocation, up to `CHUNK`.
    chunks: Vec<Vec<Entry<V>>>,
    /// Entries made so far, in use or vacant.
    made: u32,
    free: u32,
    len: usize,
    /// Counts lookups that found something; orders entries by last use.
    clock: Cell<u32>,
}

pub fn hash_bytes(key: &[u8]) -> u64 {
    xxh3_64(key)
}

/// Ask the CPU to start loading the cache line at `p`, and go on without
/// waiting for it. Only a hint: it reads nothing the program sees and
/// never faults, whatever `p` points at.
#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: PREFETCHT0 has no architectural effect and does not fault on
    // any address, mapped or not.
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(p.cast());
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: PRFM is a hint with no architectural effect; it does not
    // fault on any address, mapped or not.
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) p,
            options(nostack, readonly, preserves_flags)
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = p;
}

impl<V> Default for Dict<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Dict<V> {
    pub fn new() -> Self {
        Dict {
            buckets: vec![Bucket::EMPTY; MIN_BUCKETS],
            overflow: Vec::new(),
            chunks: Vec::new(),
            made: 0,
            free: NIL,
            len: 0,
            clock: Cell::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    fn bucket_of(&self, hash: u64) -> usize {
        hash as usize & (self.buckets.len() - 1)
    }

    fn entry(&self, i: u32) -> &Entry<V> {
        &self.chunks[(i >> CHUNK_SHIFT) as usize][i as usize & (CHUNK - 1)]
    }

    fn entry_mut(&mut self, i: u32) -> &mut Entry<V> {
        &mut self.chunks[(i >> CHUNK_SHIFT) as usize][i as usize & (CHUNK - 1)]
    }

    fn bucket(&self, b: usize, at: Place) -> &Bucket {
        match at {
            None => &self.buckets[b],
            Some(o) => &self.overflow[o],
        }
    }

    /// Start loading the bucket of `hash`: the first line a lookup reads.
    pub fn prefetch_bucket(&self, hash: u64) {
        prefetch(&self.buckets[self.bucket_of(hash)]);
    }

    /// Start loading the entries the bucket of `hash` points to under its
    /// tag: the second line a lookup reads, found once the bucket is in
    /// the cache. Usually one.
    pub fn prefetch_entry(&self, hash: u64) {
        let bucket = &self.buckets[self.bucket_of(hash)];
        let mut hits = bucket.matches(tag_of(hash as u32));
        while hits != 0 {
            let slot = hits.trailing_zeros() as usize;
            hits &= hits - 1;
            prefetch(self.entry(bucket.slots[slot]));
        }
    }

    /// The value of the entry that is most likely the key's, found from
    /// the tags and stored hashes of its bucket alone: no key is compared,
    /// no overflow bucket read, no use counted. For loading ahead, where a
    /// wrong guess only wastes a load.
    pub fn peek_hinted(&self, hash: u64) -> Option<&V> {
        let bucket = &self.buckets[self.bucket_of(hash)];
        let mut hits = bucket.matches(tag_of(hash as u32));
        while hits != 0 {
            let slot = hits.trailing_zeros() as usize;
            hits &= hits - 1;
            let e = self.entry(bucket.slots[slot]);
            if e.hash == hash as u32 {
                return e.val.as_ref();
            }
        }
        None
    }

    /// The bucket, slot and entry index of the key.
    fn locate(&self, hash: u64, key: &[u8]) -> Option<(Place, usize, u32)> {
        let b = self.bucket_of(hash);
        let tag = tag_of(hash as u32);
        let mut at = None;
        loop {
            let bucket = self.bucket(b, at);
            let mut hits = bucket.matches(tag);
            while hits != 0 {
                let slot = hits.trailing_zeros() as usize;
                hits &= hits - 1;
                let i = bucket.slots[slot];
                let e = self.entry(i);
                if e.hash == hash as u32 && e.key.is(key) {
                    return Some((at, slot, i));
                }
            }
            if bucket.next == NIL {
                return None;
            }
            at = Some(bucket.next as usize);
        }
    }

    fn find_quiet(&self, hash: u64, key: &[u8]) -> Option<u32> {
        self.locate(hash, key).map(|(_, _, i)| i)
    }

    /// Look the key up and mark it as the most recently used entry.
    fn find(&self, hash: u64, key: &[u8]) -> Option<u32> {
        let i = self.find_quiet(hash, key)?;
        let now = self.clock.get().wrapping_add(1);
        self.clock.set(now);
        let e = self.entry(i);
        let freq = self.freq_of(e);
        // The clock, scrambled, stands in for a random number below 2^16:
        // a step is taken with a chance of 1 in `odds`, which grows with
        // the counter. A product instead of a remainder: no division.
        let odds = u32::from(freq.saturating_sub(FREQ_INIT)) * FREQ_LOG_FACTOR + 1;
        let lucky = (now.wrapping_mul(0x9E37_79B1) >> 16) * odds < 1 << 16;
        e.freq.set(freq.saturating_add(u8::from(lucky)));
        e.stamp.set(now);
        Some(i)
    }

    /// How often the key is used, from 0 to 255 on a logarithmic scale.
    /// Does not count as a use.
    pub fn freq(&self, hash: u64, key: &[u8]) -> Option<u8> {
        let i = self.find_quiet(hash, key)?;
        Some(self.freq_of(self.entry(i)))
    }

    fn freq_of(&self, e: &Entry<V>) -> u8 {
        let lost = self.age_of(e) / FREQ_DECAY_LOOKUPS;
        e.freq
            .get()
            .saturating_sub(u8::try_from(lost).unwrap_or(u8::MAX))
    }

    /// How many lookups ago the key was last used. Does not count as a use.
    pub fn age(&self, hash: u64, key: &[u8]) -> Option<u32> {
        let i = self.find_quiet(hash, key)?;
        Some(self.age_of(self.entry(i)))
    }

    fn age_of(&self, e: &Entry<V>) -> u32 {
        self.clock.get().wrapping_sub(e.stamp.get())
    }

    pub fn get(&self, hash: u64, key: &[u8]) -> Option<&V> {
        let i = self.find(hash, key)?;
        self.entry(i).val.as_ref()
    }

    /// `get` with the key's deadline (0 for none), from the same lookup.
    pub fn get_timed(&self, hash: u64, key: &[u8]) -> Option<(&V, u64)> {
        let e = self.entry(self.find(hash, key)?);
        Some((e.val.as_ref()?, e.expire()))
    }

    /// `get` that does not count as a use of the entry.
    pub fn peek(&self, hash: u64, key: &[u8]) -> Option<&V> {
        let i = self.find_quiet(hash, key)?;
        self.entry(i).val.as_ref()
    }

    /// `peek` with the key's deadline (0 for none).
    pub fn peek_timed(&self, hash: u64, key: &[u8]) -> Option<(&V, u64)> {
        let e = self.entry(self.find_quiet(hash, key)?);
        Some((e.val.as_ref()?, e.expire()))
    }

    pub fn get_mut(&mut self, hash: u64, key: &[u8]) -> Option<&mut V> {
        let i = self.find(hash, key)?;
        self.entry_mut(i).val.as_mut()
    }

    pub fn contains(&self, hash: u64, key: &[u8]) -> bool {
        self.find(hash, key).is_some()
    }

    /// Give the key a deadline in milliseconds since the epoch, or none
    /// with 0. One past `EXPIRE_MAX` is kept as `EXPIRE_MAX`. Returns
    /// whether the key is there. Inserting over a key keeps its deadline;
    /// removing it drops the deadline.
    pub fn set_expire(&mut self, hash: u64, key: &[u8], at: u64) -> bool {
        let Some(i) = self.find_quiet(hash, key) else {
            return false;
        };
        self.entry_mut(i).set_expire(at);
        true
    }

    /// Insert or overwrite. Returns the previous value.
    pub fn insert(&mut self, hash: u64, key: &[u8], val: V) -> Option<V> {
        if let Some(i) = self.find(hash, key) {
            return self.entry_mut(i).val.replace(val);
        }
        if self.len >= self.buckets.len() * MAX_LOAD {
            self.grow();
        }
        let entry = Entry {
            hash: hash as u32,
            stamp: self.clock.clone(),
            expire_lo: 0,
            expire_hi: 0,
            freq: Cell::new(FREQ_INIT),
            key: Key::new(key),
            val: Some(val),
        };
        let i = if self.free != NIL {
            let i = self.free;
            let e = self.entry_mut(i);
            let next = e.hash;
            *e = entry;
            self.free = next;
            i
        } else {
            assert!(self.made < NIL, "dict is full");
            let i = self.made;
            let chunk = (i >> CHUNK_SHIFT) as usize;
            if chunk == self.chunks.len() {
                // The first chunk starts small and doubles like any vector;
                // the others are made full size at once.
                let size = if chunk == 0 { 0 } else { CHUNK };
                self.chunks.push(Vec::with_capacity(size));
            }
            self.chunks[chunk].push(entry);
            self.made += 1;
            i
        };
        let b = self.bucket_of(hash);
        place(
            &mut self.buckets,
            &mut self.overflow,
            b,
            tag_of(hash as u32),
            i,
        );
        self.len += 1;
        None
    }

    pub fn remove(&mut self, hash: u64, key: &[u8]) -> Option<V> {
        let (at, slot, i) = self.locate(hash, key)?;
        let b = self.bucket_of(hash);
        match at {
            None => self.buckets[b].ctrl[slot] = 0,
            Some(o) => self.overflow[o].ctrl[slot] = 0,
        }
        let free = self.free;
        let e = self.entry_mut(i);
        e.key = Key::EMPTY;
        e.hash = free;
        e.set_expire(0);
        let val = e.val.take();
        self.free = i;
        self.len -= 1;
        val
    }

    fn grow(&mut self) {
        let new_size = self.buckets.len() * 2;
        let mut buckets = vec![Bucket::EMPTY; new_size];
        let mut overflow = Vec::new();
        for (c, chunk) in self.chunks.iter().enumerate() {
            for (j, e) in chunk.iter().enumerate() {
                if e.val.is_some() {
                    let i = (c << CHUNK_SHIFT | j) as u32;
                    let b = e.hash as usize & (new_size - 1);
                    place(&mut buckets, &mut overflow, b, tag_of(e.hash), i);
                }
            }
        }
        self.buckets = buckets;
        self.overflow = overflow;
    }

    /// Drop everything and give the memory back.
    pub fn clear(&mut self) {
        *self = Dict::new();
    }

    /// Visit one bucket and return the next cursor. Start with 0; the scan
    /// is complete when 0 comes back.
    pub fn scan(&self, cursor: u64, mut f: impl FnMut(&[u8], &V)) -> u64 {
        self.scan_timed(cursor, |k, v, _| f(k, v))
    }

    /// `scan` that also gives each element's deadline (0 for none).
    pub fn scan_timed(&self, cursor: u64, mut f: impl FnMut(&[u8], &V, u64)) -> u64 {
        let mask = (self.buckets.len() - 1) as u64;
        let mut bucket = &self.buckets[(cursor & mask) as usize];
        loop {
            let mut used = bucket.used();
            while used != 0 {
                let slot = used.trailing_zeros() as usize;
                used &= used - 1;
                let e = self.entry(bucket.slots[slot]);
                if let Some(v) = &e.val {
                    f(e.key.as_bytes(), v, e.expire());
                }
            }
            if bucket.next == NIL {
                break;
            }
            bucket = &self.overflow[bucket.next as usize];
        }
        // Increment the cursor's masked bits in reverse-binary order.
        let mut v = cursor | !mask;
        v = v.reverse_bits();
        v = v.wrapping_add(1);
        v.reverse_bits()
    }

    /// Scan until about `budget` elements were visited or the table is
    /// done. Empty buckets count too, so a sparse table cannot make one
    /// chunk arbitrarily long.
    pub fn scan_chunk(&self, cursor: u64, budget: usize, mut f: impl FnMut(&[u8], &V)) -> u64 {
        self.scan_chunk_timed(cursor, budget, |k, v, _| f(k, v))
    }

    /// `scan_chunk` that also gives each element's deadline (0 for none).
    pub fn scan_chunk_timed(
        &self,
        mut cursor: u64,
        budget: usize,
        mut f: impl FnMut(&[u8], &V, u64),
    ) -> u64 {
        let budget = budget.max(1);
        let (mut seen, mut buckets) = (0usize, 0usize);
        loop {
            cursor = self.scan_timed(cursor, |k, v, at| {
                seen += 1;
                f(k, v, at)
            });
            buckets += 1;
            if cursor == 0 || seen >= budget || buckets >= budget.saturating_mul(4) {
                return cursor;
            }
        }
    }

    /// Some element, chosen by `r`: the first one at or after a slot
    /// picked by it. Close to uniform unless many slots are vacant.
    pub fn pick(&self, r: u64) -> Option<(&[u8], &V)> {
        self.sample(r).next().map(|(k, v, _, _)| (k, v))
    }

    /// The elements from a slot picked by `r` onwards, each with its age
    /// (see `age`) and how often it is used (see `freq`). Take as many as
    /// the sample needs.
    pub fn sample(&self, r: u64) -> impl Iterator<Item = (&[u8], &V, u32, u8)> {
        let made = self.made;
        let start = (r % u64::from(made.max(1))) as u32;
        (start..made).chain(0..start).filter_map(move |i| {
            let e = self.entry(i);
            let v = e.val.as_ref()?;
            Some((e.key.as_bytes(), v, self.age_of(e), self.freq_of(e)))
        })
    }

    pub fn for_each(&self, mut f: impl FnMut(&[u8], &V)) {
        self.for_each_timed(|k, v, _| f(k, v));
    }

    /// `for_each` that also gives each element's deadline (0 for none).
    pub fn for_each_timed(&self, mut f: impl FnMut(&[u8], &V, u64)) {
        for e in self.chunks.iter().flatten() {
            if let Some(v) = &e.val {
                f(e.key.as_bytes(), v, e.expire());
            }
        }
    }
}

/// Put entry `i` in a free slot of bucket `b` or of the overflow buckets
/// it chains to, adding one at the end of the chain if all are full.
fn place(buckets: &mut [Bucket], overflow: &mut Vec<Bucket>, b: usize, tag: u8, i: u32) {
    let mut at: Place = None;
    loop {
        let fresh = overflow.len();
        let bucket = match at {
            None => &mut buckets[b],
            Some(o) => &mut overflow[o],
        };
        let free = bucket.matches(0) & SLOT_MASK;
        if free != 0 {
            let slot = free.trailing_zeros() as usize;
            bucket.ctrl[slot] = tag;
            bucket.slots[slot] = i;
            return;
        }
        if bucket.next != NIL {
            at = Some(bucket.next as usize);
            continue;
        }
        assert!(fresh < NIL as usize, "dict has too many overflow buckets");
        bucket.next = fresh as u32;
        let mut last = Bucket::EMPTY;
        last.ctrl[0] = tag;
        last.slots[0] = i;
        overflow.push(last);
        return;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_bytes_marks_exactly_the_zero_bytes() {
        for pattern in 0u32..256 {
            let mut bytes = [0x5Au8; 8];
            for (i, byte) in bytes.iter_mut().enumerate() {
                if pattern & 1 << i != 0 {
                    *byte = 0;
                } else if i % 2 == 0 {
                    // Bytes that trip up the naive zero-byte test.
                    *byte = if i % 4 == 0 { 0x01 } else { 0x80 };
                }
            }
            assert_eq!(zero_bytes(u64::from_le_bytes(bytes)), pattern, "{bytes:?}");
        }
    }

    #[test]
    fn matches_finds_tags_and_free_slots_in_all_twelve_slots() {
        let mut bucket = Bucket::EMPTY;
        assert_eq!(bucket.matches(0), SLOT_MASK);
        assert_eq!(bucket.matches(0xFF), 0);
        for (slot, tag) in bucket.ctrl.iter_mut().enumerate() {
            *tag = if slot % 3 == 0 { 7 } else { 0xFF };
        }
        let sevens = (0..SLOTS).filter(|s| s % 3 == 0).fold(0, |m, s| m | 1 << s);
        assert_eq!(bucket.matches(7), sevens);
        assert_eq!(bucket.matches(0xFF), SLOT_MASK & !sevens);
        assert_eq!(bucket.matches(0), 0);
        assert_eq!(bucket.used(), SLOT_MASK);
    }

    #[test]
    fn key_is_tells_every_byte_and_length_apart() {
        for n in 0..=24u8 {
            let a: Vec<u8> = (1..=n).collect();
            let key = Key::new(&a);
            assert!(key.is(&a), "{n}");
            for i in 0..a.len() {
                let mut b = a.clone();
                b[i] ^= 0x80;
                assert!(!key.is(&b), "{n} {i}");
            }
            let mut longer = a.clone();
            longer.push(0);
            assert!(!key.is(&longer), "{n}");
            if let Some((_, shorter)) = a.split_last() {
                assert!(!key.is(shorter), "{n}");
            }
        }
    }

    #[test]
    fn peek_hinted_finds_what_peek_finds() {
        let mut d = Dict::new();
        let keys: Vec<Vec<u8>> = (0..5000).map(|i| format!("key:{i}").into_bytes()).collect();
        for (i, k) in keys.iter().enumerate() {
            d.insert(hash_bytes(k), k, i);
        }
        let mut found = 0;
        for k in &keys {
            let h = hash_bytes(k);
            if let Some(v) = d.peek_hinted(h) {
                assert_eq!(Some(v), d.peek(h, k));
                found += 1;
            }
        }
        // Only keys in overflow buckets are missed.
        assert!(found > keys.len() * 9 / 10, "{found}");
        assert_eq!(d.peek_hinted(hash_bytes(b"absent")), None);
    }

    #[test]
    fn matches_agrees_with_the_portable_version() {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..10_000 {
            let mut bucket = Bucket::EMPTY;
            for tag in &mut bucket.ctrl {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                // Few distinct tags, so that a bucket holds repeats.
                *tag = (x % 4) as u8 * 0x55;
            }
            bucket.next = x as u32;
            for tag in [0, 0x55, 0xAA, 0xFF, 7] {
                assert_eq!(bucket.matches(tag), matches_swar(bucket.ctrl, tag));
            }
        }
    }
}
