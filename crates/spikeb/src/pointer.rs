//! Pointer layout, modelled on the classic Redis dict.
//!
//! Main table: power-of-two bucket array of `*mut Entry`, chained. Each
//! `Entry` is its own 24-byte heap allocation `{next, key, val}`. Key and
//! value are separate heap blobs `[len: u32][bytes]` (like an sds string), so a
//! key costs three allocations. With a TTL, a second dict (Redis `expires`)
//! gets its own entry `{next, key -> the main key blob, expiry}`, with the
//! expiry stored in the `val` word. Both dicts double at load factor 1.0 and
//! rehash all at once (old bucket array freed before returning). All
//! allocations go through the system allocator.
//! Unlike Redis there is no robj header and no embedded-string optimisation.

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::ptr::null_mut;

use crate::{default_hash, validate, Table, TableError};

const _: () = assert!(std::mem::size_of::<usize>() == 8);

const BLOB_HEADER: usize = 4;
const INITIAL_BUCKETS: usize = 4;

#[repr(C)]
struct Entry {
    next: *mut Entry,
    key: *mut u8,
    /// Main dict: value blob pointer. Expires dict: the expiry itself.
    val: usize,
}

fn blob_layout(len: usize) -> Layout {
    // Align 1 and size at most 4 + 65535 always form a valid layout.
    Layout::from_size_align(BLOB_HEADER + len, 1).unwrap_or_else(|_| unreachable!())
}

fn blob_new(data: &[u8]) -> *mut u8 {
    let layout = blob_layout(data.len());
    // SAFETY: the layout has non-zero size (header is 4 bytes).
    let p = unsafe { alloc(layout) };
    if p.is_null() {
        handle_alloc_error(layout);
    }
    // SAFETY: `p` points to BLOB_HEADER + data.len() writable bytes.
    unsafe {
        p.copy_from_nonoverlapping((data.len() as u32).to_le_bytes().as_ptr(), BLOB_HEADER);
        p.add(BLOB_HEADER)
            .copy_from_nonoverlapping(data.as_ptr(), data.len());
    }
    p
}

/// # Safety
/// `p` must come from `blob_new` and not have been freed; the blob must outlive `'a`.
unsafe fn blob_bytes<'a>(p: *const u8) -> &'a [u8] {
    let mut len = [0u8; BLOB_HEADER];
    // SAFETY: per the contract, the header and `len` payload bytes are initialised.
    unsafe {
        len.as_mut_ptr().copy_from_nonoverlapping(p, BLOB_HEADER);
        std::slice::from_raw_parts(p.add(BLOB_HEADER), u32::from_le_bytes(len) as usize)
    }
}

/// # Safety
/// `p` must come from `blob_new` and not have been freed.
unsafe fn blob_free(p: *mut u8) {
    // SAFETY: the blob is live (contract), so the length header is readable and
    // gives the exact layout it was allocated with.
    unsafe {
        let len = blob_bytes(p).len();
        dealloc(p, blob_layout(len));
    }
}

fn entry_new(key: *mut u8, val: usize) -> *mut Entry {
    Box::into_raw(Box::new(Entry {
        next: null_mut(),
        key,
        val,
    }))
}

struct Dict {
    buckets: Vec<*mut Entry>,
    used: usize,
}

impl Dict {
    fn new() -> Self {
        Self {
            buckets: vec![null_mut(); INITIAL_BUCKETS],
            used: 0,
        }
    }

    fn index(&self, h: u64) -> usize {
        h as usize & (self.buckets.len() - 1)
    }

    fn find(&self, h: u64, key: &[u8]) -> *mut Entry {
        let mut e = self.buckets[self.index(h)];
        while !e.is_null() {
            // SAFETY: every entry reachable from a bucket is a live Box<Entry>
            // owned by this dict, and its key blob is live (the expires dict's
            // keys are owned by the main dict, which outlives them).
            let (k, next) = unsafe { (blob_bytes((*e).key), (*e).next) };
            if k == key {
                return e;
            }
            e = next;
        }
        null_mut()
    }

    fn push(&mut self, h: u64, e: *mut Entry, hasher: fn(&[u8]) -> u64) {
        if self.used >= self.buckets.len() {
            self.grow(hasher);
        }
        let i = self.index(h);
        // SAFETY: `e` is a live, unlinked Entry that we now own.
        unsafe { (*e).next = self.buckets[i] };
        self.buckets[i] = e;
        self.used += 1;
    }

    fn grow(&mut self, hasher: fn(&[u8]) -> u64) {
        let mut next = vec![null_mut::<Entry>(); self.buckets.len() * 2];
        let mask = next.len() - 1;
        for &head in &self.buckets {
            let mut e = head;
            while !e.is_null() {
                // SAFETY: live entry owned by this dict; relinking it into the
                // new array consumes its old `next` link exactly once.
                unsafe {
                    let following = (*e).next;
                    let i = hasher(blob_bytes((*e).key)) as usize & mask;
                    (*e).next = next[i];
                    next[i] = e;
                    e = following;
                }
            }
        }
        self.buckets = next;
    }

    /// Detach and return the entry for `key`, or null.
    fn unlink(&mut self, h: u64, key: &[u8]) -> *mut Entry {
        let i = self.index(h);
        let mut link: *mut *mut Entry = &mut self.buckets[i];
        // SAFETY: `link` always points either into the bucket vec (not resized
        // during this call) or at the `next` field of a live entry in the chain.
        unsafe {
            while !(*link).is_null() {
                let e = *link;
                if blob_bytes((*e).key) == key {
                    *link = (*e).next;
                    (*e).next = null_mut();
                    self.used -= 1;
                    return e;
                }
                link = &mut (*e).next;
            }
        }
        null_mut()
    }
}

pub struct PointerTable {
    main: Dict,
    expires: Dict,
    hasher: fn(&[u8]) -> u64,
}

impl Default for PointerTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PointerTable {
    pub fn new() -> Self {
        Self {
            main: Dict::new(),
            expires: Dict::new(),
            hasher: default_hash,
        }
    }
}

impl Drop for PointerTable {
    fn drop(&mut self) {
        for &head in &self.main.buckets {
            let mut e = head;
            while !e.is_null() {
                // SAFETY: each main entry is a live Box<Entry> that owns its key
                // and value blobs; we free them once and never touch them again.
                unsafe {
                    let next = (*e).next;
                    blob_free((*e).key);
                    blob_free((*e).val as *mut u8);
                    drop(Box::from_raw(e));
                    e = next;
                }
            }
        }
        for &head in &self.expires.buckets {
            let mut e = head;
            while !e.is_null() {
                // SAFETY: expires entries own only themselves; their key blobs
                // were freed above as part of the main dict and are not read.
                unsafe {
                    let next = (*e).next;
                    drop(Box::from_raw(e));
                    e = next;
                }
            }
        }
    }
}

impl Table for PointerTable {
    fn insert(&mut self, key: &[u8], value: &[u8], expiry: Option<u64>) -> Result<(), TableError> {
        validate(key, value)?;
        let h = (self.hasher)(key);
        let mut e = self.main.find(h, key);
        if e.is_null() {
            e = entry_new(blob_new(key), blob_new(value) as usize);
            self.main.push(h, e, self.hasher);
        } else {
            let new_val = blob_new(value);
            // SAFETY: `e` is a live main entry; its old value blob is replaced
            // and freed exactly once.
            unsafe {
                blob_free((*e).val as *mut u8);
                (*e).val = new_val as usize;
            }
        }
        let cur = self.expires.find(h, key);
        match (expiry, cur.is_null()) {
            (Some(x), false) => {
                // SAFETY: `cur` is a live expires entry.
                unsafe { (*cur).val = x as usize };
            }
            (Some(x), true) => {
                // SAFETY: `e` is live; its key blob is shared, not owned, by the
                // new expires entry.
                let shared_key = unsafe { (*e).key };
                let ee = entry_new(shared_key, x as usize);
                self.expires.push(h, ee, self.hasher);
            }
            (None, false) => {
                let gone = self.expires.unlink(h, key);
                // SAFETY: `gone` was just detached and is owned by us.
                drop(unsafe { Box::from_raw(gone) });
            }
            (None, true) => {}
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let e = self.main.find((self.hasher)(key), key);
        if e.is_null() {
            return None;
        }
        // SAFETY: `e` is live and its value blob lives as long as `&self`.
        Some(unsafe { blob_bytes((*e).val as *const u8) })
    }

    fn expiry(&self, key: &[u8]) -> Option<u64> {
        let e = self.expires.find((self.hasher)(key), key);
        // SAFETY: a non-null entry is live.
        (!e.is_null()).then(|| unsafe { (*e).val as u64 })
    }

    fn remove(&mut self, key: &[u8]) -> bool {
        let h = (self.hasher)(key);
        let e = self.main.unlink(h, key);
        if e.is_null() {
            return false;
        }
        let ee = self.expires.unlink(h, key);
        if !ee.is_null() {
            // SAFETY: detached expires entry owned by us; its key blob is not freed here.
            drop(unsafe { Box::from_raw(ee) });
        }
        // SAFETY: `e` is detached and owns its blobs; each is freed once.
        unsafe {
            blob_free((*e).key);
            blob_free((*e).val as *mut u8);
            drop(Box::from_raw(e));
        }
        true
    }

    fn len(&self) -> usize {
        self.main.used
    }

    fn stats(&self) -> String {
        format!(
            "buckets={} expires_buckets={}",
            self.main.buckets.len(),
            self.expires.buckets.len()
        )
    }
}
