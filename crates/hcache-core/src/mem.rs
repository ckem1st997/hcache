//! Heap usage, measured by the allocator itself.
//!
//! `maxmemory` needs to know how much memory the process holds. Estimating
//! it from the data structures would mean bookkeeping in every command, so
//! the count is taken where it is exact: a program that wants `maxmemory`
//! installs `CountingAlloc` as its global allocator.
//!
//! ```ignore
//! #[global_allocator]
//! static ALLOC: hcache_core::mem::CountingAlloc = hcache_core::mem::CountingAlloc;
//! ```
//!
//! Each thread counts on its own and adds to the shared total once it has
//! moved by `FLUSH_BYTES`, so threads do not fight over one counter. The
//! total can therefore be off by that much per thread.
//!
//! The allocation itself is mimalloc's: the keyspace is millions of small
//! blocks, which it serves faster than glibc's malloc and without a
//! header per block.

use mimalloc::MiMalloc;
use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

const FLUSH_BYTES: isize = 4096;

static USED: AtomicIsize = AtomicIsize::new(0);
static INSTALLED: AtomicBool = AtomicBool::new(false);

thread_local! {
    static DELTA: Cell<isize> = const { Cell::new(0) };
}

fn count(bytes: isize) {
    let flushed = DELTA.try_with(|delta| {
        let total = delta.get() + bytes;
        if total.abs() >= FLUSH_BYTES {
            USED.fetch_add(total, Ordering::Relaxed);
            INSTALLED.store(true, Ordering::Relaxed);
            delta.set(0);
        } else {
            delta.set(total);
        }
    });
    if flushed.is_err() {
        // The thread is shutting down and its counter is gone.
        USED.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// mimalloc, counting what is in use.
pub struct CountingAlloc;

// SAFETY: every method forwards to `MiMalloc` with the arguments it was
// given and only adds bookkeeping that does not allocate: an atomic and a
// const-initialised thread-local without a destructor.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
        let p = unsafe { MiMalloc.alloc(layout) };
        if !p.is_null() {
            count(layout.size() as isize);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        let p = unsafe { MiMalloc.alloc_zeroed(layout) };
        if !p.is_null() {
            count(layout.size() as isize);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from this allocator with this layout, which
        // means from `MiMalloc`.
        unsafe { MiMalloc.dealloc(ptr, layout) };
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::realloc`'s contract.
        let p = unsafe { MiMalloc.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            count(new_size as isize - layout.size() as isize);
        }
        p
    }
}

/// Whether `CountingAlloc` is this program's allocator (it has counted
/// something).
pub fn counting() -> bool {
    INSTALLED.load(Ordering::Relaxed)
}

/// Bytes of heap in use, or 0 when nothing counts them.
pub fn used() -> usize {
    usize::try_from(USED.load(Ordering::Relaxed)).unwrap_or(0)
}
