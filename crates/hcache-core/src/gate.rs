//! The execution gate: a node-wide reader/writer gate built so that the
//! shared side costs one store and one load on a cache line nobody else
//! writes.
//!
//! Every participant (an event-loop worker, an embedded session) owns a
//! slot. Entering the shared side sets the slot's flag and then checks the
//! `closed` flag; the exclusive side sets `closed` and then waits for every
//! slot flag to clear. Both sides write first and read second with SeqCst,
//! so at least one of them sees the other.

use crossbeam_utils::CachePadded;
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::time::{Duration, Instant};

/// How long an exclusive requester lets starved shared participants go
/// first before it closes the gate again.
const FAIRNESS_WAIT: Duration = Duration::from_millis(2);

pub struct Gate {
    flags: Box<[CachePadded<AtomicBool>]>,
    /// Slots at or above this index were never handed out.
    high_water: AtomicUsize,
    free: Mutex<Vec<usize>>,
    closed: CachePadded<AtomicBool>,
    /// Serializes exclusive requesters.
    excl: Mutex<()>,
    /// Participants that found the gate closed and have not entered since.
    starved: AtomicUsize,
    cv_lock: Mutex<()>,
    cv: Condvar,
}

impl Gate {
    pub fn new(max_slots: usize) -> Gate {
        Gate {
            flags: (0..max_slots)
                .map(|_| CachePadded::new(AtomicBool::new(false)))
                .collect(),
            high_water: AtomicUsize::new(0),
            free: Mutex::new(Vec::new()),
            closed: CachePadded::new(AtomicBool::new(false)),
            excl: Mutex::new(()),
            starved: AtomicUsize::new(0),
            cv_lock: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    /// Reserve a slot. `None` when all slots are taken.
    pub fn register(&self) -> Option<usize> {
        let mut free = self.free.lock();
        if let Some(slot) = free.pop() {
            return Some(slot);
        }
        let next = self.high_water.load(SeqCst);
        if next >= self.flags.len() {
            return None;
        }
        self.high_water.store(next + 1, SeqCst);
        Some(next)
    }

    /// Give a slot back. The slot must not be inside the shared side.
    pub fn unregister(&self, slot: usize) {
        debug_assert!(!self.flags[slot].load(SeqCst));
        self.free.lock().push(slot);
    }

    /// Enter the shared side without waiting. `false` means the gate is
    /// closed and the caller must not touch the keyspace.
    #[inline]
    pub fn try_enter(&self, slot: usize) -> bool {
        self.flags[slot].store(true, SeqCst);
        if self.closed.load(SeqCst) {
            self.flags[slot].store(false, SeqCst);
            return false;
        }
        true
    }

    #[inline]
    pub fn leave(&self, slot: usize) {
        self.flags[slot].store(false, SeqCst);
    }

    /// Enter the shared side, sleeping while the gate is closed. Only for
    /// threads that are allowed to block (embedded sessions).
    pub fn enter_blocking(&self, slot: usize) {
        if self.try_enter(slot) {
            return;
        }
        self.starved.fetch_add(1, SeqCst);
        loop {
            {
                let mut guard = self.cv_lock.lock();
                while self.closed.load(SeqCst) {
                    self.cv.wait(&mut guard);
                }
            }
            if self.try_enter(slot) {
                self.starved.fetch_sub(1, SeqCst);
                return;
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(SeqCst)
    }

    /// A non-blocking participant found the gate closed and will retry.
    pub fn mark_starved(&self) {
        self.starved.fetch_add(1, SeqCst);
    }

    /// The participant that called `mark_starved` got in (or gave up).
    pub fn clear_starved(&self) {
        self.starved.fetch_sub(1, SeqCst);
    }

    /// Close the gate and wait until no participant is inside. The caller
    /// must not be inside the shared side itself.
    pub fn exclusive(&self) -> ExclusiveGuard<'_> {
        let serial = self.excl.lock();
        // Back-to-back exclusive sections would otherwise lock the shared
        // side out for good.
        if self.starved.load(SeqCst) > 0 {
            let deadline = Instant::now() + FAIRNESS_WAIT;
            while self.starved.load(SeqCst) > 0 && Instant::now() < deadline {
                std::thread::yield_now();
            }
        }
        self.closed.store(true, SeqCst);
        let mut spins = 0u32;
        loop {
            let used = self.high_water.load(SeqCst);
            if self.flags[..used].iter().all(|f| !f.load(SeqCst)) {
                break;
            }
            spins += 1;
            if spins < 64 {
                std::hint::spin_loop();
            } else if spins < 1024 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        ExclusiveGuard {
            gate: self,
            _serial: serial,
        }
    }
}

pub struct ExclusiveGuard<'a> {
    gate: &'a Gate,
    _serial: MutexGuard<'a, ()>,
}

impl Drop for ExclusiveGuard<'_> {
    fn drop(&mut self) {
        self.gate.closed.store(false, SeqCst);
        let _guard = self.gate.cv_lock.lock();
        self.gate.cv.notify_all();
    }
}
