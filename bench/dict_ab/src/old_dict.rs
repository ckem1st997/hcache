// The table `hcache-core` used before the bucket one, kept only for this
// comparison. One change: its LFU step multiplies instead of dividing, as
// the current table does, so that only the layout differs.

//! Chained hash table with a resumable scan cursor.
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

use std::cell::Cell;
use xxhash_rust::xxh3::xxh3_64;

const NIL: u32 = u32::MAX;
const MIN_BUCKETS: usize = 4;
/// Where a new entry's use counter starts, so that it is not the first
/// to go under an LFU policy.
const FREQ_INIT: u8 = 5;
/// How much harder each step of the counter gets: the counter is
/// logarithmic, 255 stands for about a million uses.
const FREQ_LOG_FACTOR: u32 = 10;
/// The counter of an entry left alone loses one for every this many
/// lookups in the table.
const FREQ_DECAY_LOOKUPS: u32 = 1 << 16;

/// Longest key kept inside its entry. 18 bytes is what is left of a
/// 64-byte entry next to a 32-byte value, and covers the 16-byte keys
/// of the usual benchmarks.
const INLINE_KEY: usize = 18;
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
    /// table never has more buckets than `u32` can count.
    hash: u32,
    /// Next entry in the bucket chain, or next free slot when vacant.
    next: u32,
    /// The table's clock when the entry was last looked up.
    stamp: Cell<u32>,
    /// Logarithmic count of the lookups, as of `stamp`.
    freq: Cell<u8>,
    /// `Key::EMPTY` when vacant.
    key: Key,
    /// `None` when vacant.
    val: Option<V>,
}

/// Bytes an entry of `Dict<V>` takes, so that the users of the table can
/// check that theirs fits in a cache line.
pub const fn entry_size<V>() -> usize {
    std::mem::size_of::<Entry<V>>()
}

#[derive(Clone)]
pub struct Dict<V> {
    /// Head of each bucket chain. Length is a power of two.
    buckets: Vec<u32>,
    entries: Vec<Entry<V>>,
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
            buckets: vec![NIL; MIN_BUCKETS],
            entries: Vec::new(),
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

    /// Start loading the bucket of `hash`: the first line a lookup reads.
    pub fn prefetch_bucket(&self, hash: u64) {
        prefetch(&self.buckets[self.bucket_of(hash)]);
    }

    /// Start loading the first entry of the bucket of `hash`: the second
    /// line a lookup reads, found once the bucket is in the cache.
    pub fn prefetch_entry(&self, hash: u64) {
        if let Some(e) = self
            .entries
            .get(self.buckets[self.bucket_of(hash)] as usize)
        {
            prefetch(e);
        }
    }

    fn find_quiet(&self, hash: u64, key: &[u8]) -> Option<u32> {
        let mut i = self.buckets[self.bucket_of(hash)];
        while i != NIL {
            let e = &self.entries[i as usize];
            if e.hash == hash as u32 && e.val.is_some() && e.key.as_bytes() == key {
                return Some(i);
            }
            i = e.next;
        }
        None
    }

    /// Look the key up and mark it as the most recently used entry.
    fn find(&self, hash: u64, key: &[u8]) -> Option<u32> {
        let i = self.find_quiet(hash, key)?;
        let now = self.clock.get().wrapping_add(1);
        self.clock.set(now);
        let e = &self.entries[i as usize];
        let freq = self.freq_of(e);
        // The clock, scrambled, stands in for a random number: a step is
        // taken with a chance that falls as the counter rises.
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
        Some(self.freq_of(&self.entries[i as usize]))
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
        Some(self.age_of(&self.entries[i as usize]))
    }

    fn age_of(&self, e: &Entry<V>) -> u32 {
        self.clock.get().wrapping_sub(e.stamp.get())
    }

    pub fn get(&self, hash: u64, key: &[u8]) -> Option<&V> {
        let i = self.find(hash, key)?;
        self.entries[i as usize].val.as_ref()
    }

    /// `get` that does not count as a use of the entry.
    pub fn peek(&self, hash: u64, key: &[u8]) -> Option<&V> {
        let i = self.find_quiet(hash, key)?;
        self.entries[i as usize].val.as_ref()
    }

    pub fn get_mut(&mut self, hash: u64, key: &[u8]) -> Option<&mut V> {
        let i = self.find(hash, key)?;
        self.entries[i as usize].val.as_mut()
    }

    pub fn contains(&self, hash: u64, key: &[u8]) -> bool {
        self.find(hash, key).is_some()
    }

    /// Insert or overwrite. Returns the previous value.
    pub fn insert(&mut self, hash: u64, key: &[u8], val: V) -> Option<V> {
        if let Some(i) = self.find(hash, key) {
            return self.entries[i as usize].val.replace(val);
        }
        if self.len >= self.buckets.len() {
            self.grow();
        }
        let b = self.bucket_of(hash);
        let entry = Entry {
            hash: hash as u32,
            next: self.buckets[b],
            stamp: self.clock.clone(),
            freq: Cell::new(FREQ_INIT),
            key: Key::new(key),
            val: Some(val),
        };
        let i = if self.free != NIL {
            let i = self.free;
            self.free = self.entries[i as usize].next;
            self.entries[i as usize] = entry;
            i
        } else {
            assert!(self.entries.len() < NIL as usize, "dict is full");
            self.entries.push(entry);
            (self.entries.len() - 1) as u32
        };
        self.buckets[b] = i;
        self.len += 1;
        None
    }

    pub fn remove(&mut self, hash: u64, key: &[u8]) -> Option<V> {
        let b = self.bucket_of(hash);
        let mut prev = NIL;
        let mut i = self.buckets[b];
        while i != NIL {
            let e = &self.entries[i as usize];
            let next = e.next;
            if e.hash == hash as u32 && e.val.is_some() && e.key.as_bytes() == key {
                if prev == NIL {
                    self.buckets[b] = next;
                } else {
                    self.entries[prev as usize].next = next;
                }
                let e = &mut self.entries[i as usize];
                e.key = Key::EMPTY;
                e.next = self.free;
                self.free = i;
                self.len -= 1;
                return e.val.take();
            }
            prev = i;
            i = next;
        }
        None
    }

    fn grow(&mut self) {
        let new_size = self.buckets.len() * 2;
        let mask = new_size - 1;
        let mut buckets = vec![NIL; new_size];
        for (i, e) in self.entries.iter_mut().enumerate() {
            if e.val.is_some() {
                let b = e.hash as usize & mask;
                e.next = buckets[b];
                buckets[b] = i as u32;
            }
        }
        self.buckets = buckets;
    }

    /// Drop everything and give the memory back.
    pub fn clear(&mut self) {
        *self = Dict::new();
    }

    /// Visit one bucket and return the next cursor. Start with 0; the scan
    /// is complete when 0 comes back.
    pub fn scan(&self, cursor: u64, mut f: impl FnMut(&[u8], &V)) -> u64 {
        let mask = (self.buckets.len() - 1) as u64;
        let mut i = self.buckets[(cursor & mask) as usize];
        while i != NIL {
            let e = &self.entries[i as usize];
            if let Some(v) = &e.val {
                f(e.key.as_bytes(), v);
            }
            i = e.next;
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
    pub fn scan_chunk(&self, mut cursor: u64, budget: usize, mut f: impl FnMut(&[u8], &V)) -> u64 {
        let budget = budget.max(1);
        let (mut seen, mut buckets) = (0usize, 0usize);
        loop {
            cursor = self.scan(cursor, |k, v| {
                seen += 1;
                f(k, v)
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
        let start = (r % self.entries.len().max(1) as u64) as usize;
        let (head, tail) = self.entries.split_at(start.min(self.entries.len()));
        tail.iter().chain(head).filter_map(|e| {
            let v = e.val.as_ref()?;
            Some((e.key.as_bytes(), v, self.age_of(e), self.freq_of(e)))
        })
    }

    pub fn for_each(&self, mut f: impl FnMut(&[u8], &V)) {
        for e in &self.entries {
            if let Some(v) = &e.val {
                f(e.key.as_bytes(), v);
            }
        }
    }
}

