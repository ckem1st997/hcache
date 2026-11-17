//! Freeing large values away from the thread that removed them: UNLINK,
//! FLUSHALL ASYNC and the `lazyfree-lazy-*` settings, as in Redis. Freeing
//! a collection of a million elements takes tens of milliseconds, all of
//! it inside a shard lock and on a worker that has other clients to
//! serve; handed to the freeing thread, it costs the command one send.

use crate::shard::Value;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, OnceLock};

/// A value that takes more frees than this goes to the thread; a smaller
/// one is freed on the spot, which is cheaper than the send (Redis's
/// `LAZYFREE_THRESHOLD`).
const THRESHOLD: usize = 64;

/// Why a value goes. Each has its `lazyfree-lazy-*` setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Eviction,
    Expire,
    /// Replaced or deleted as a side effect of a command: SET over a hash,
    /// RENAME onto an existing key.
    ServerDel,
    /// DEL. UNLINK is lazy whatever the setting.
    UserDel,
    /// FLUSHALL and FLUSHDB without ASYNC or SYNC.
    UserFlush,
}

impl Reason {
    pub const ALL: [Reason; 5] = [
        Reason::Eviction,
        Reason::Expire,
        Reason::ServerDel,
        Reason::UserDel,
        Reason::UserFlush,
    ];

    /// The name of its setting, for CONFIG.
    pub fn setting(self) -> &'static str {
        match self {
            Reason::Eviction => "lazyfree-lazy-eviction",
            Reason::Expire => "lazyfree-lazy-expire",
            Reason::ServerDel => "lazyfree-lazy-server-del",
            Reason::UserDel => "lazyfree-lazy-user-del",
            Reason::UserFlush => "lazyfree-lazy-user-flush",
        }
    }

    fn bit(self) -> u8 {
        1 << self as u8
    }
}

/// What the thread is given: anything whose drop does the freeing, and
/// how many objects that counts as.
type Garbage = (Box<dyn Send>, usize);

#[derive(Default)]
struct Counters {
    pending: AtomicUsize,
    freed: AtomicU64,
    /// Values and keyspaces handed to the thread so far.
    handed: AtomicU64,
}

pub struct LazyFree {
    /// Started at the first value that needs it; `None` when the thread
    /// could not be started, and everything is freed on the spot.
    thread: OnceLock<Option<Sender<Garbage>>>,
    /// One bit per `Reason` whose setting is on.
    on: AtomicU8,
    counters: Arc<Counters>,
}

impl Default for LazyFree {
    /// Every setting on, as Valkey ships them since 8.0.
    fn default() -> Self {
        LazyFree {
            thread: OnceLock::new(),
            on: AtomicU8::new(Reason::ALL.iter().fold(0, |bits, r| bits | r.bit())),
            counters: Arc::default(),
        }
    }
}

/// How many frees dropping `value` takes, about.
fn effort(value: &Value) -> usize {
    match value {
        Value::Str(_) => 1,
        Value::Hash(hash) => hash.len(),
        Value::List(list) => list.len(),
        Value::Set(set) => set.len(),
        Value::ZSet(zset) => zset.len(),
        Value::Stream(stream) => stream.len(),
        Value::Bloom(bloom) => bloom.filters(),
        Value::Json(doc) => crate::json::count_up_to(doc, THRESHOLD + 1),
    }
}

impl LazyFree {
    pub fn is_on(&self, reason: Reason) -> bool {
        self.on.load(Ordering::Relaxed) & reason.bit() != 0
    }

    pub fn set(&self, reason: Reason, on: bool) {
        if on {
            self.on.fetch_or(reason.bit(), Ordering::Relaxed);
        } else {
            self.on.fetch_and(!reason.bit(), Ordering::Relaxed);
        }
    }

    /// Objects handed to the thread and not freed yet
    /// (`lazyfree_pending_objects`).
    pub fn pending(&self) -> usize {
        self.counters.pending.load(Ordering::Relaxed)
    }

    /// Objects the thread has freed (`lazyfreed_objects`).
    pub fn freed(&self) -> u64 {
        self.counters.freed.load(Ordering::Relaxed)
    }

    /// How many times something was handed to the thread: a change means
    /// memory that is still counted as used is on its way out.
    pub fn handed(&self) -> u64 {
        self.counters.handed.load(Ordering::Relaxed)
    }

    /// Drop `value`, on the thread when `lazy` and it is large.
    pub(crate) fn free(&self, value: Value, lazy: bool) {
        if lazy && effort(&value) > THRESHOLD {
            self.send(Box::new(value), 1);
        }
    }

    /// Drop `value` the way `reason`'s setting says.
    pub(crate) fn free_for(&self, value: Value, reason: Reason) {
        // Strings first: they are most of what goes, and never lazily.
        if !matches!(value, Value::Str(_)) {
            self.free(value, self.is_on(reason));
        }
    }

    /// Drop a whole keyspace of `objects` keys on the thread.
    pub(crate) fn free_all(&self, garbage: impl Send + 'static, objects: usize) {
        self.send(Box::new(garbage), objects);
    }

    fn send(&self, garbage: Box<dyn Send>, objects: usize) {
        let thread = self.thread.get_or_init(|| self.start());
        let Some(thread) = thread else {
            return;
        };
        self.counters.pending.fetch_add(objects, Ordering::Relaxed);
        self.counters.handed.fetch_add(1, Ordering::Relaxed);
        if let Err(mpsc::SendError((garbage, objects))) = thread.send((garbage, objects)) {
            // The thread is gone: free it here.
            drop(garbage);
            self.counters.pending.fetch_sub(objects, Ordering::Relaxed);
        }
    }

    fn start(&self) -> Option<Sender<Garbage>> {
        let (tx, rx) = mpsc::channel::<Garbage>();
        let counters = self.counters.clone();
        // It ends when the engine, which holds the only sender, is gone.
        std::thread::Builder::new()
            .name("hcache-lazyfree".into())
            .spawn(move || {
                for (garbage, objects) in rx {
                    drop(garbage);
                    counters.pending.fetch_sub(objects, Ordering::Relaxed);
                    counters.freed.fetch_add(objects as u64, Ordering::Relaxed);
                }
            })
            .ok()
            .map(|_| tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    fn list(n: usize) -> Value {
        Value::List(
            (0..n)
                .map(|i| i.to_string().into_bytes())
                .collect::<VecDeque<_>>(),
        )
    }

    fn wait_freed(lazy: &LazyFree, n: u64) {
        let until = Instant::now() + Duration::from_secs(10);
        while lazy.freed() < n || lazy.pending() != 0 {
            assert!(
                Instant::now() < until,
                "the freeing thread did not catch up"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn only_large_values_go_to_the_thread() {
        let lazy = LazyFree::default();
        lazy.free(list(THRESHOLD), true);
        lazy.free(Value::Str(vec![0; 1 << 20]), true);
        assert_eq!(lazy.freed(), 0);
        assert!(lazy.thread.get().is_none(), "nothing needed the thread yet");
        lazy.free(list(THRESHOLD + 1), true);
        wait_freed(&lazy, 1);
        lazy.free(list(1000), false);
        assert_eq!(lazy.freed(), 1);
    }

    #[test]
    fn settings_switch_one_reason_at_a_time() {
        let lazy = LazyFree::default();
        assert!(Reason::ALL.iter().all(|r| lazy.is_on(*r)));
        lazy.set(Reason::UserDel, false);
        assert!(!lazy.is_on(Reason::UserDel));
        assert!(lazy.is_on(Reason::UserFlush) && lazy.is_on(Reason::Eviction));
        lazy.free_for(list(500), Reason::UserDel);
        lazy.free_for(list(500), Reason::Eviction);
        wait_freed(&lazy, 1);
        assert_eq!(lazy.freed(), 1);
        lazy.set(Reason::UserDel, true);
        assert!(lazy.is_on(Reason::UserDel));
    }

    #[test]
    fn a_keyspace_counts_its_keys() {
        let lazy = LazyFree::default();
        lazy.free_all(vec![list(10); 7], 7);
        wait_freed(&lazy, 7);
        assert_eq!(lazy.freed(), 7);
    }
}
