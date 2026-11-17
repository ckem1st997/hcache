//! CLIENT TRACKING: which connection read which key, so that it can be
//! told when the key changes and drop its cached copy.
//!
//! The lock here is held for a lookup or an insert, never across I/O. It
//! is taken with a shard locked (a write reports the key it changed), so
//! nothing done under it may reach for a shard.

use crate::pubsub::Sub;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Keys remembered at most. One more makes the table forget another key,
/// whose readers are told that it changed.
pub(crate) const MAX_KEYS: usize = 1_000_000;
/// And this many bytes of their names: a name may be very long.
pub(crate) const MAX_BYTES: usize = 64 << 20;

/// Who to tell about a change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Listener {
    /// The connection that turned tracking on.
    pub owner: u64,
    /// The worker and slot `owner` lives in: how a writer is recognised.
    pub home: (usize, usize),
    /// Say nothing about changes `owner` made itself.
    pub noloop: bool,
    /// Where the invalidations go: `owner`, or the connection it
    /// redirects to.
    pub to: Sub,
}

#[derive(Default)]
struct Inner {
    /// Key to the connections that read it since it last changed.
    keys: HashMap<Vec<u8>, Vec<Listener>>,
    /// The bytes of the names in `keys`.
    bytes: usize,
    /// BCAST: a prefix and who hears of every change under it.
    prefixes: Vec<(Vec<u8>, Listener)>,
    /// Every tracking connection, by its id.
    all: HashMap<u64, Listener>,
}

pub(crate) struct Tracker {
    /// Connections with tracking on. Zero: nobody takes the lock.
    active: AtomicUsize,
    max_keys: usize,
    max_bytes: usize,
    inner: Mutex<Inner>,
}

impl Tracker {
    pub fn new(max_keys: usize, max_bytes: usize) -> Tracker {
        Tracker {
            active: AtomicUsize::new(0),
            max_keys: max_keys.max(1),
            max_bytes,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn active(&self) -> usize {
        self.active.load(SeqCst)
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        // Every change leaves the table consistent.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Turn tracking on for `listener.owner`, or change its options.
    /// `prefixes` makes it a BCAST listener of those prefixes. Returns
    /// whether it was off before.
    pub fn start(&self, listener: Listener, prefixes: Option<&[Vec<u8>]>) -> bool {
        let mut inner = self.inner();
        let new = inner.all.insert(listener.owner, listener).is_none();
        inner.prefixes.retain(|(_, l)| l.owner != listener.owner);
        for prefix in prefixes.unwrap_or_default() {
            inner.prefixes.push((prefix.clone(), listener));
        }
        if new {
            self.active.fetch_add(1, SeqCst);
        }
        new
    }

    /// Turn tracking off for `owner`. Returns whether it was on. The keys
    /// it read are forgotten as they change: looking for them here would
    /// hold the lock for as long as the table is large.
    pub fn stop(&self, owner: u64) -> bool {
        let mut inner = self.inner();
        let was = inner.all.remove(&owner).is_some();
        inner.prefixes.retain(|(_, l)| l.owner != owner);
        if was {
            self.active.fetch_sub(1, SeqCst);
        }
        was
    }

    /// `listener` is about to read `key`. Returns the keys the full table
    /// forgot to make room, each with the readers to tell.
    pub fn remember(&self, key: &[u8], listener: Listener) -> Vec<(Vec<u8>, Vec<Listener>)> {
        let mut guard = self.inner();
        let inner = &mut *guard;
        // Tracking was turned off in between, from another thread.
        if !inner.all.contains_key(&listener.owner) {
            return Vec::new();
        }
        if let Some(readers) = inner.keys.get_mut(key) {
            // Connections that are gone must not pile up on a key that
            // is read often and never changes.
            let all = &inner.all;
            readers.retain(|l| l.owner != listener.owner && all.contains_key(&l.owner));
            readers.push(listener);
            return Vec::new();
        }
        let mut victims = Vec::new();
        while inner.keys.len() >= self.max_keys
            || (!inner.keys.is_empty() && inner.bytes + key.len() > self.max_bytes)
        {
            let Some(any) = inner.keys.keys().next().cloned() else {
                break;
            };
            if let Some(readers) = inner.keys.remove(&any) {
                inner.bytes -= any.len();
                victims.push((any, readers));
            }
        }
        inner.bytes += key.len();
        inner.keys.insert(key.to_vec(), vec![listener]);
        victims
    }

    /// `key` changed, by the connection at `writer` when that is known.
    /// Returns who to tell; the key is forgotten, except for
    /// a NOLOOP writer that read it: it is not told now, and so must
    /// still be told of the next change by someone else.
    pub fn changed(&self, key: &[u8], writer: Option<(usize, usize)>) -> Vec<Listener> {
        let mut guard = self.inner();
        let inner = &mut *guard;
        let read = match inner.keys.remove(key) {
            Some(read) => {
                inner.bytes -= key.len();
                read
            }
            None => Vec::new(),
        };
        // Among the living only: the slot of a connection that is gone
        // may be the writer's by now.
        let mut live = read.iter().filter_map(|l| inner.all.get(&l.owner).copied());
        if let Some(own) = live.find(|l| l.noloop && Some(l.home) == writer) {
            inner.bytes += key.len();
            inner.keys.insert(key.to_vec(), vec![own]);
        }
        let wide = inner.prefixes.iter();
        let wide = wide.filter(|(p, _)| key.starts_with(p)).map(|(_, l)| l);
        // A reader is told the way it asks for now, and not at all once it
        // has turned tracking off.
        read.iter()
            .chain(wide)
            .filter_map(|l| inner.all.get(&l.owner).copied())
            .filter(|l| !(l.noloop && Some(l.home) == writer))
            .collect()
    }

    /// Every key may have changed. Returns every tracking connection;
    /// all keys are forgotten.
    pub fn flushed(&self) -> Vec<Listener> {
        let mut inner = self.inner();
        inner.keys = HashMap::new();
        inner.bytes = 0;
        inner.all.values().copied().collect()
    }

    #[cfg(test)]
    fn keys(&self) -> usize {
        self.inner().keys.len()
    }

    #[cfg(test)]
    fn readers(&self, key: &[u8]) -> usize {
        self.inner().keys.get(key).map_or(0, Vec::len)
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        self.inner().bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listener(owner: u64, noloop: bool) -> Listener {
        Listener {
            owner,
            home: (0, owner as usize),
            noloop,
            to: Sub {
                worker: 0,
                idx: owner as usize,
                conn: owner,
            },
        }
    }

    #[test]
    fn a_change_is_told_once() {
        let t = Tracker::new(10, 1000);
        assert!(t.start(listener(1, false), None));
        assert!(!t.start(listener(1, false), None));
        assert_eq!(t.remember(b"k", listener(1, false)), vec![]);
        assert_eq!(t.remember(b"k", listener(1, false)), vec![]);
        assert_eq!(t.changed(b"k", None), vec![listener(1, false)]);
        assert_eq!(t.changed(b"k", None), vec![]);
        assert_eq!(t.active(), 1);
    }

    #[test]
    fn noloop_skips_the_writer_only() {
        let t = Tracker::new(10, 1000);
        t.start(listener(1, true), None);
        t.start(listener(2, true), None);
        t.remember(b"k", listener(1, true));
        t.remember(b"k", listener(2, true));
        assert_eq!(t.changed(b"k", Some((0, 1))), vec![listener(2, true)]);
        // The writer was not told, so it still listens.
        assert_eq!(t.changed(b"k", None), vec![listener(1, true)]);
        assert_eq!(t.changed(b"k", None), vec![]);
    }

    #[test]
    fn prefixes_hear_of_keys_nobody_read() {
        let t = Tracker::new(10, 1000);
        t.start(listener(1, false), Some(&[b"user:".to_vec()]));
        assert_eq!(t.changed(b"user:7", None), vec![listener(1, false)]);
        assert_eq!(t.changed(b"order:7", None), vec![]);
        // Every key: the empty prefix.
        t.start(listener(1, false), Some(&[Vec::new()]));
        assert_eq!(t.changed(b"order:7", None), vec![listener(1, false)]);
    }

    #[test]
    fn a_full_table_forgets_another_key_and_says_so() {
        let t = Tracker::new(2, 1000);
        t.start(listener(1, false), None);
        assert_eq!(t.remember(b"a", listener(1, false)), vec![]);
        assert_eq!(t.remember(b"b", listener(1, false)), vec![]);
        let victims = t.remember(b"c", listener(1, false));
        assert_eq!(victims.len(), 1);
        assert!(victims[0].0 == b"a" || victims[0].0 == b"b");
        assert_eq!(victims[0].1, vec![listener(1, false)]);
        assert_eq!(t.keys(), 2);
    }

    #[test]
    fn long_names_are_bounded_by_their_bytes() {
        let t = Tracker::new(1000, 100);
        t.start(listener(1, false), None);
        for n in 0..10u8 {
            assert_eq!(t.remember(&[n; 10], listener(1, false)), vec![]);
        }
        assert_eq!((t.keys(), t.bytes()), (10, 100));
        // Room for 35 bytes means four names of ten go.
        assert_eq!(t.remember(&[b'x'; 35], listener(1, false)).len(), 4);
        assert_eq!((t.keys(), t.bytes()), (7, 95));
        // A name larger than the whole budget empties the table and is
        // remembered alone: its reader must still hear of a change.
        assert_eq!(t.remember(&[b'y'; 500], listener(1, false)).len(), 7);
        assert_eq!((t.keys(), t.bytes()), (1, 500));
        assert_eq!(t.changed(&[b'y'; 500], None), vec![listener(1, false)]);
        assert_eq!((t.keys(), t.bytes()), (0, 0));
        t.remember(b"k", listener(1, false));
        t.flushed();
        assert_eq!(t.bytes(), 0);
    }

    #[test]
    fn connections_that_are_gone_do_not_pile_up_on_a_key() {
        let t = Tracker::new(10, 1000);
        for owner in 1..=1000 {
            t.start(listener(owner, false), None);
            t.remember(b"hot", listener(owner, false));
            t.remember(b"hot", listener(owner, false));
            t.stop(owner);
        }
        t.start(listener(2000, false), None);
        t.remember(b"hot", listener(2000, false));
        assert_eq!(t.readers(b"hot"), 1);
    }

    #[test]
    fn noloop_is_not_fooled_by_a_slot_that_was_used_before() {
        let t = Tracker::new(10, 1000);
        // Connection 1 lived in slot 7 and is gone; 3 lives there now.
        let at = |owner, noloop| Listener {
            home: (0, 7),
            ..listener(owner, noloop)
        };
        t.start(at(1, false), None);
        t.start(listener(2, false), None);
        t.remember(b"k", at(1, false));
        t.remember(b"k", listener(2, false));
        t.stop(1);
        t.start(at(3, true), None);
        // Read through another key's path: appended, not replacing.
        t.inner()
            .keys
            .get_mut(&b"k"[..])
            .expect("k")
            .push(at(3, true));
        assert_eq!(t.changed(b"k", Some((0, 7))), vec![listener(2, false)]);
        // It was not told of its own write, so it hears of the next.
        assert_eq!(t.changed(b"k", None), vec![at(3, true)]);
    }

    #[test]
    fn stopping_silences_what_was_read() {
        let t = Tracker::new(10, 1000);
        t.start(listener(1, false), None);
        t.remember(b"k", listener(1, false));
        assert!(t.stop(1));
        assert!(!t.stop(1));
        assert_eq!(t.active(), 0);
        assert_eq!(t.changed(b"k", None), vec![]);
        // Too late to remember anything.
        assert_eq!(t.remember(b"k2", listener(1, false)), vec![]);
        assert_eq!(t.keys(), 0);
        // Other options later: what was read before is still reported.
        t.start(listener(1, false), None);
        t.remember(b"j", listener(1, false));
        t.start(listener(1, true), None);
        assert_eq!(t.changed(b"j", None), vec![listener(1, true)]);
    }

    #[test]
    fn a_flush_reaches_everyone() {
        let t = Tracker::new(10, 1000);
        t.start(listener(1, false), None);
        t.start(listener(2, false), Some(&[b"p".to_vec()]));
        t.remember(b"k", listener(1, false));
        let mut told = t.flushed();
        told.sort_by_key(|l| l.owner);
        assert_eq!(told, vec![listener(1, false), listener(2, false)]);
        assert_eq!(t.keys(), 0);
    }
}
