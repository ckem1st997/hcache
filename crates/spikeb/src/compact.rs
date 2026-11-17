//! Compact layout: 8-byte index slots plus size-class slabs.
//!
//! Index slot (u64): `[tag: u32 = low 32 bits of hash][ref: u32]`.
//! `0` is empty and `u64::MAX` is a tombstone. `ref` is the record address plus
//! one: `((chunk << 18) | (offset / 4)) + 1`, so a real ref is never 0 and
//! never 0xFFFF_FFFF (chunk index is capped at 16382).
//!
//! Record: `[klen: u8][flags: u8][vlen: u16 LE][key][value][expiry: u64 LE]`,
//! the expiry present only when `flags & 1`. Size = 4 + klen + vlen (+ 8).
//! Records are 4-byte aligned units; each size class is a multiple of 4.
//!
//! Slab: 1 MiB chunks, each carved only into records of one size class
//! (bump pointer, plus a per-class free list threaded through the first four
//! bytes of freed records). Chunks are never returned.
//!
//! Index: linear probing, position by multiply-shift (`fastrange`) on the full
//! 64-bit hash, so capacity need not be a power of two. Max load (live +
//! tombstones) is 80%. When full: grow by 1.5x if more than 70% of slots are
//! live, otherwise rebuild at the same size to purge tombstones. A rebuild
//! rehashes from the stored keys and frees the old array before returning.

use crate::{default_hash, validate, Table, TableError, MAX_KEY_LEN, MAX_VALUE_LEN};

pub const CHUNK_BYTES: usize = 1 << 20;
const UNIT: usize = 4;
const UNIT_BITS: u32 = 18; // CHUNK_BYTES / UNIT = 2^18
const UNIT_MASK: u32 = (1 << UNIT_BITS) - 1;
pub const MAX_CHUNKS: usize = (1 << 14) - 1;
const HEADER: usize = 4;
const EXPIRY_BYTES: usize = 8;
const FLAG_TTL: u8 = 1;
const EMPTY: u64 = 0;
const TOMB: u64 = u64::MAX;
const MIN_CAP: usize = 16;
const NO_CHUNK: usize = usize::MAX;

fn max_used(cap: usize) -> usize {
    cap / 5 * 4
}

fn fastrange(h: u64, cap: usize) -> usize {
    ((u128::from(h) * cap as u128) >> 64) as usize
}

/// Size classes: step 4 up to 128, step 8 up to 256, then four per doubling.
fn class_sizes() -> Vec<usize> {
    let max = HEADER + MAX_KEY_LEN + MAX_VALUE_LEN + EXPIRY_BYTES;
    let mut sizes: Vec<usize> = (1..=32).map(|k| k * 4).collect();
    sizes.extend((17..=32).map(|k| k * 8));
    let mut base = 256;
    while sizes.last().is_some_and(|&s| s < max) {
        sizes.extend((1..=4).map(|k| base + base / 4 * k));
        base *= 2;
    }
    sizes
}

fn locate(stored: u32) -> (usize, usize) {
    let r = stored - 1;
    ((r >> UNIT_BITS) as usize, (r & UNIT_MASK) as usize * UNIT)
}

fn make_ref(chunk: usize, offset: usize) -> u32 {
    (((chunk as u32) << UNIT_BITS) | (offset / UNIT) as u32) + 1
}

fn record_len(hdr: &[u8]) -> usize {
    let ttl = if hdr[1] & FLAG_TTL != 0 {
        EXPIRY_BYTES
    } else {
        0
    };
    HEADER + hdr[0] as usize + u16::from_le_bytes([hdr[2], hdr[3]]) as usize + ttl
}

struct ClassState {
    size: usize,
    free_head: u32,
    chunk: usize,
    bump: usize,
}

enum Probe {
    Found(usize),
    Vacant { slot: usize, tomb: bool },
}

pub struct CompactTable {
    slots: Vec<u64>,
    live: usize,
    tombs: usize,
    chunks: Vec<Box<[u8]>>,
    classes: Vec<ClassState>,
    hasher: fn(&[u8]) -> u64,
    rebuilds: usize,
}

impl Default for CompactTable {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactTable {
    pub fn new() -> Self {
        Self::with_hasher(default_hash)
    }

    /// Custom hash function, so tests can force tag and bucket collisions.
    pub fn with_hasher(hasher: fn(&[u8]) -> u64) -> Self {
        Self {
            slots: vec![EMPTY; MIN_CAP],
            live: 0,
            tombs: 0,
            chunks: Vec::new(),
            classes: class_sizes()
                .into_iter()
                .map(|size| ClassState {
                    size,
                    free_head: 0,
                    chunk: NO_CHUNK,
                    bump: 0,
                })
                .collect(),
            hasher,
            rebuilds: 0,
        }
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn slot_capacity(&self) -> usize {
        self.slots.len()
    }

    pub fn tombstones(&self) -> usize {
        self.tombs
    }

    /// Number of index rebuilds (growth or tombstone purge).
    pub fn rebuilds(&self) -> usize {
        self.rebuilds
    }

    pub fn load_factor(&self) -> f64 {
        self.live as f64 / self.slots.len() as f64
    }

    pub fn index_bytes(&self) -> usize {
        self.slots.len() * 8
    }

    pub fn slab_bytes(&self) -> usize {
        self.chunks.len() * CHUNK_BYTES
    }

    /// Slots inspected by a lookup of `key` (hit or miss).
    pub fn probe_len(&self, key: &[u8]) -> usize {
        let cap = self.slots.len();
        let h = (self.hasher)(key);
        let tag = h as u32;
        let mut i = fastrange(h, cap);
        let mut n = 1;
        loop {
            let s = self.slots[i];
            if s == EMPTY
                || (s != TOMB && (s >> 32) as u32 == tag && self.record_key(s as u32) == key)
            {
                return n;
            }
            i = if i + 1 == cap { 0 } else { i + 1 };
            n += 1;
        }
    }

    /// Longest distance from home slot over all live entries.
    pub fn max_probe_len(&self) -> usize {
        let cap = self.slots.len();
        let mut max = 0;
        for (i, &s) in self.slots.iter().enumerate() {
            if s == EMPTY || s == TOMB {
                continue;
            }
            let home = fastrange((self.hasher)(self.record_key(s as u32)), cap);
            max = max.max((i + cap - home) % cap + 1);
        }
        max
    }

    fn record(&self, stored: u32) -> &[u8] {
        let (c, o) = locate(stored);
        let chunk = &self.chunks[c];
        let len = record_len(&chunk[o..o + HEADER]);
        &chunk[o..o + len]
    }

    fn record_key(&self, stored: u32) -> &[u8] {
        let r = self.record(stored);
        &r[HEADER..HEADER + r[0] as usize]
    }

    fn class_of(&self, size: usize) -> usize {
        self.classes.partition_point(|c| c.size < size)
    }

    fn alloc(&mut self, class: usize) -> Result<u32, TableError> {
        let st = &mut self.classes[class];
        if st.free_head != 0 {
            let head = st.free_head;
            let (c, o) = locate(head);
            let b = &self.chunks[c][o..o + 4];
            st.free_head = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            return Ok(head);
        }
        if st.chunk == NO_CHUNK || st.bump + st.size > CHUNK_BYTES {
            if self.chunks.len() >= MAX_CHUNKS {
                return Err(TableError::Full);
            }
            self.chunks.push(vec![0u8; CHUNK_BYTES].into_boxed_slice());
            st.chunk = self.chunks.len() - 1;
            st.bump = 0;
        }
        let r = make_ref(st.chunk, st.bump);
        st.bump += st.size;
        Ok(r)
    }

    fn free_record(&mut self, stored: u32) {
        let (c, o) = locate(stored);
        let class = self.class_of(record_len(&self.chunks[c][o..o + HEADER]));
        let st = &mut self.classes[class];
        self.chunks[c][o..o + 4].copy_from_slice(&st.free_head.to_le_bytes());
        st.free_head = stored;
    }

    fn write_record(&mut self, stored: u32, key: &[u8], value: &[u8], expiry: Option<u64>) {
        let (c, o) = locate(stored);
        let ttl = if expiry.is_some() { EXPIRY_BYTES } else { 0 };
        let len = HEADER + key.len() + value.len() + ttl;
        let buf = &mut self.chunks[c][o..o + len];
        buf[0] = key.len() as u8;
        buf[1] = if expiry.is_some() { FLAG_TTL } else { 0 };
        buf[2..4].copy_from_slice(&(value.len() as u16).to_le_bytes());
        buf[HEADER..HEADER + key.len()].copy_from_slice(key);
        let vstart = HEADER + key.len();
        buf[vstart..vstart + value.len()].copy_from_slice(value);
        if let Some(e) = expiry {
            buf[len - EXPIRY_BYTES..].copy_from_slice(&e.to_le_bytes());
        }
    }

    fn probe(&self, h: u64, key: &[u8]) -> Probe {
        let cap = self.slots.len();
        let tag = h as u32;
        let mut i = fastrange(h, cap);
        let mut first_tomb = None;
        // Terminates: live + tombstones never exceeds 80% of the slots, so an
        // empty slot always exists.
        loop {
            let s = self.slots[i];
            if s == EMPTY {
                return Probe::Vacant {
                    slot: first_tomb.unwrap_or(i),
                    tomb: first_tomb.is_some(),
                };
            }
            if s == TOMB {
                first_tomb.get_or_insert(i);
            } else if (s >> 32) as u32 == tag && self.record_key(s as u32) == key {
                return Probe::Found(i);
            }
            i = if i + 1 == cap { 0 } else { i + 1 };
        }
    }

    /// Put `entry` in the first empty slot on the probe path (no tombstones
    /// exist right after a rebuild, and growth only happens before an
    /// insertion into an empty slot).
    fn place_empty(slots: &mut [u64], h: u64, entry: u64) {
        let cap = slots.len();
        let mut i = fastrange(h, cap);
        while slots[i] != EMPTY {
            i = if i + 1 == cap { 0 } else { i + 1 };
        }
        slots[i] = entry;
    }

    fn rebuild(&mut self, new_cap: usize) {
        let mut new_slots = vec![EMPTY; new_cap];
        for &s in &self.slots {
            if s == EMPTY || s == TOMB {
                continue;
            }
            let h = (self.hasher)(self.record_key(s as u32));
            // Keep the stored tag; it is the same low 32 bits of `h`.
            Self::place_empty(&mut new_slots, h, s);
        }
        // The old array is freed here, before the caller continues.
        self.slots = new_slots;
        self.tombs = 0;
        self.rebuilds += 1;
    }

    fn make_room(&mut self) {
        let cap = self.slots.len();
        let new_cap = if (self.live + 1) * 10 > cap * 7 {
            cap * 3 / 2
        } else {
            cap
        };
        self.rebuild(new_cap);
    }
}

impl Table for CompactTable {
    fn insert(&mut self, key: &[u8], value: &[u8], expiry: Option<u64>) -> Result<(), TableError> {
        validate(key, value)?;
        let h = (self.hasher)(key);
        let size =
            HEADER + key.len() + value.len() + if expiry.is_some() { EXPIRY_BYTES } else { 0 };
        let class = self.class_of(size);
        match self.probe(h, key) {
            Probe::Found(i) => {
                let old = self.slots[i];
                let (c, o) = locate(old as u32);
                let old_class = self.class_of(record_len(&self.chunks[c][o..o + HEADER]));
                if old_class == class {
                    self.write_record(old as u32, key, value, expiry);
                    return Ok(());
                }
                let r = self.alloc(class)?;
                self.write_record(r, key, value, expiry);
                self.free_record(old as u32);
                self.slots[i] = (old & !0xFFFF_FFFF) | u64::from(r);
            }
            Probe::Vacant { slot, tomb } => {
                let r = self.alloc(class)?;
                self.write_record(r, key, value, expiry);
                let entry = (u64::from(h as u32) << 32) | u64::from(r);
                if tomb {
                    self.slots[slot] = entry;
                    self.tombs -= 1;
                } else if self.live + self.tombs + 1 > max_used(self.slots.len()) {
                    self.make_room();
                    Self::place_empty(&mut self.slots, h, entry);
                } else {
                    self.slots[slot] = entry;
                }
                self.live += 1;
            }
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.probe((self.hasher)(key), key) {
            Probe::Found(i) => {
                let r = self.record(self.slots[i] as u32);
                let start = HEADER + r[0] as usize;
                Some(&r[start..start + u16::from_le_bytes([r[2], r[3]]) as usize])
            }
            Probe::Vacant { .. } => None,
        }
    }

    fn expiry(&self, key: &[u8]) -> Option<u64> {
        let Probe::Found(i) = self.probe((self.hasher)(key), key) else {
            return None;
        };
        let r = self.record(self.slots[i] as u32);
        if r[1] & FLAG_TTL == 0 {
            return None;
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&r[r.len() - EXPIRY_BYTES..]);
        Some(u64::from_le_bytes(b))
    }

    fn remove(&mut self, key: &[u8]) -> bool {
        let Probe::Found(i) = self.probe((self.hasher)(key), key) else {
            return false;
        };
        self.free_record(self.slots[i] as u32);
        self.live -= 1;
        let cap = self.slots.len();
        if self.slots[if i + 1 == cap { 0 } else { i + 1 }] == EMPTY {
            // Nothing probes past this slot, so it and any tombstones just
            // before it can become empty again.
            self.slots[i] = EMPTY;
            let mut j = i;
            loop {
                j = if j == 0 { cap - 1 } else { j - 1 };
                if self.slots[j] != TOMB {
                    break;
                }
                self.slots[j] = EMPTY;
                self.tombs -= 1;
            }
        } else {
            self.slots[i] = TOMB;
            self.tombs += 1;
        }
        true
    }

    fn len(&self) -> usize {
        self.live
    }

    fn stats(&self) -> String {
        format!(
            "slot_capacity={} slot_load={:.4} index_bytes={} slab_chunks={} slab_bytes={} rebuilds={}",
            self.slots.len(),
            self.load_factor(),
            self.index_bytes(),
            self.chunks.len(),
            self.slab_bytes(),
            self.rebuilds
        )
    }
}
