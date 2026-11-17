//! Adversarial tests: both layouts against a HashMap model, plus compact-only
//! checks of slab reuse, limits, collisions and tombstones.

use std::collections::HashMap;

use spikeb::compact::{CompactTable, CHUNK_BYTES};
use spikeb::pointer::PointerTable;
use spikeb::workload::{fill_value, make_key, mix};
use spikeb::{default_hash, Table, TableError, MAX_KEY_LEN, MAX_VALUE_LEN};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.0)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Lengths that cross many size classes, including empty and the maximum.
const LENS: [usize; 14] = [
    0,
    1,
    3,
    4,
    16,
    17,
    33,
    100,
    127,
    300,
    1000,
    5000,
    40_000,
    MAX_VALUE_LEN,
];

type Model = HashMap<Vec<u8>, (Vec<u8>, Option<u64>)>;

/// Mostly 16-byte keys; every eighth id gets a variable length of 8..=255.
fn key_for(id: u64) -> Vec<u8> {
    if !id.is_multiple_of(8) {
        let mut k = [0u8; 16];
        make_key(id, &mut k);
        return k.to_vec();
    }
    let len = 8 + (mix(id) % 248) as usize;
    let mut k = vec![0xA5u8; len];
    k[..8].copy_from_slice(&id.to_le_bytes());
    k
}

fn random_value(rng: &mut Rng, max_len: usize) -> Vec<u8> {
    let candidates: Vec<usize> = LENS.iter().copied().filter(|&l| l <= max_len).collect();
    let len = candidates[rng.below(candidates.len() as u64) as usize];
    let mut v = vec![0u8; len];
    fill_value(rng.next(), &mut v);
    v
}

fn assert_matches<T: Table>(t: &T, model: &Model) {
    assert_eq!(t.len(), model.len());
    for (k, (v, e)) in model {
        assert_eq!(t.get(k), Some(v.as_slice()));
        assert_eq!(t.expiry(k), *e);
    }
}

fn run_model<T: Table>(t: &mut T, seed: u64, ops: usize, keyspace: u64, max_len: usize) {
    let mut rng = Rng(seed);
    let mut model = Model::new();
    for step in 0..ops {
        let key = key_for(rng.below(keyspace));
        match rng.below(100) {
            0..=54 => {
                let v = random_value(&mut rng, max_len);
                let e = (rng.below(2) == 0).then(|| rng.next());
                t.insert(&key, &v, e).unwrap();
                model.insert(key, (v, e));
            }
            55..=79 => assert_eq!(t.remove(&key), model.remove(&key).is_some()),
            _ => {
                assert_eq!(t.get(&key), model.get(&key).map(|(v, _)| v.as_slice()));
                assert_eq!(t.expiry(&key), model.get(&key).and_then(|(_, e)| *e));
            }
        }
        if step % 5000 == 0 {
            assert_eq!(t.len(), model.len());
        }
    }
    assert_matches(t, &model);
    // Drain: everything removable exactly once, then nothing is left.
    let keys: Vec<_> = model.keys().cloned().collect();
    for k in &keys {
        assert!(t.remove(k));
        assert!(!t.remove(k));
        assert_eq!(t.get(k), None);
    }
    assert_eq!(t.len(), 0);
}

#[test]
fn compact_model_small_keyspace() {
    run_model(&mut CompactTable::new(), 1, 300_000, 5_000, MAX_VALUE_LEN);
}

#[test]
fn compact_model_large_keyspace() {
    run_model(&mut CompactTable::new(), 2, 400_000, 200_000, 1000);
}

#[test]
fn pointer_model_small_keyspace() {
    run_model(&mut PointerTable::new(), 3, 300_000, 5_000, MAX_VALUE_LEN);
}

#[test]
fn pointer_model_large_keyspace() {
    run_model(&mut PointerTable::new(), 4, 400_000, 200_000, 1000);
}

fn limits<T: Table>(mut t: T) {
    // Zero-length value and zero-length key.
    t.insert(b"k", b"", None).unwrap();
    assert_eq!(t.get(b"k"), Some(&b""[..]));
    t.insert(b"", b"v", Some(7)).unwrap();
    assert_eq!(t.get(b""), Some(&b"v"[..]));
    assert_eq!(t.expiry(b""), Some(7));

    // Maximum value and key lengths round-trip, with and without expiry.
    let big = vec![0xEEu8; MAX_VALUE_LEN];
    let long_key = vec![0x11u8; MAX_KEY_LEN];
    t.insert(&long_key, &big, Some(u64::MAX)).unwrap();
    assert_eq!(t.get(&long_key), Some(big.as_slice()));
    assert_eq!(t.expiry(&long_key), Some(u64::MAX));
    t.insert(&long_key, &big, None).unwrap();
    assert_eq!(t.expiry(&long_key), None);

    // Over the limit: clear error, nothing changes, existing data is intact.
    let too_big = vec![1u8; MAX_VALUE_LEN + 1];
    let before = t.len();
    assert_eq!(
        t.insert(b"new", &too_big, None),
        Err(TableError::ValueTooLong {
            len: MAX_VALUE_LEN + 1
        })
    );
    assert_eq!(t.get(b"new"), None);
    assert_eq!(
        t.insert(b"k", &too_big, None),
        Err(TableError::ValueTooLong {
            len: MAX_VALUE_LEN + 1
        })
    );
    assert_eq!(t.get(b"k"), Some(&b""[..]));
    let too_long_key = vec![2u8; MAX_KEY_LEN + 1];
    assert_eq!(
        t.insert(&too_long_key, b"v", None),
        Err(TableError::KeyTooLong {
            len: MAX_KEY_LEN + 1
        })
    );
    assert_eq!(t.len(), before);
    // Lookups with an oversized key are simply misses.
    assert_eq!(t.get(&too_long_key), None);
    assert!(!t.remove(&too_long_key));
}

#[test]
fn compact_limits() {
    limits(CompactTable::new());
}

#[test]
fn pointer_limits() {
    limits(PointerTable::new());
}

#[test]
fn compact_expiry_overwrite_semantics() {
    let mut t = CompactTable::new();
    t.insert(b"a", b"1", Some(5)).unwrap();
    t.insert(b"a", b"1", None).unwrap();
    assert_eq!(t.expiry(b"a"), None);
    t.insert(b"a", b"1", Some(9)).unwrap();
    assert_eq!(t.expiry(b"a"), Some(9));
    assert_eq!(t.len(), 1);
}

#[test]
fn compact_size_classes_cover_every_record_size() {
    // Every legal record size must be storable: the biggest key+value+expiry
    // fits a chunk and round-trips (checked via the extremes and class edges).
    let mut t = CompactTable::new();
    let mut rng = Rng(9);
    for len in (0..=MAX_VALUE_LEN)
        .step_by(97)
        .chain([MAX_VALUE_LEN, MAX_VALUE_LEN - 1])
    {
        let mut v = vec![0u8; len];
        fill_value(rng.next(), &mut v);
        let key = len.to_le_bytes();
        t.insert(&key, &v, Some(len as u64)).unwrap();
        assert_eq!(t.get(&key), Some(v.as_slice()), "len {len}");
    }
    const { assert!(MAX_VALUE_LEN + 300 < CHUNK_BYTES) };
}

#[test]
fn compact_refill_reuses_slab() {
    let n = 200_000u64;
    let mut t = CompactTable::new();
    let mut key = [0u8; 16];
    let mut val = [0u8; 40];
    let fill = |t: &mut CompactTable, key: &mut [u8; 16], val: &mut [u8; 40]| {
        for i in 0..n {
            make_key(i, key);
            fill_value(i, val);
            t.insert(key, val, Some(i)).unwrap();
        }
    };
    fill(&mut t, &mut key, &mut val);
    let chunks = t.chunk_count();
    let cap = t.slot_capacity();
    for i in 0..n {
        make_key(i, &mut key);
        assert!(t.remove(&key));
    }
    assert_eq!(t.len(), 0);
    fill(&mut t, &mut key, &mut val);
    assert_eq!(t.chunk_count(), chunks, "slab must reuse freed records");
    // The index may grow once at most while purging tombstones (1.5x step).
    assert!(t.slot_capacity() <= cap * 3 / 2);
    for i in 0..n {
        make_key(i, &mut key);
        fill_value(i, &mut val);
        assert_eq!(t.get(&key), Some(&val[..]));
    }
}

#[test]
fn compact_overwrite_churn_across_classes_is_bounded() {
    let keys = 10_000u64;
    let mut t = CompactTable::new();
    let mut rng = Rng(11);
    let mut chunks_mid = 0;
    for step in 0..600_000 {
        let mut k = [0u8; 16];
        make_key(rng.below(keys), &mut k);
        let v = random_value(&mut rng, 1000);
        t.insert(&k, &v, None).unwrap();
        if step == 100_000 {
            chunks_mid = t.chunk_count();
        }
    }
    // Live data is at most keys * ~1 KiB = ~10 MiB; chunks are per class, so
    // allow some per-class fragmentation but no unbounded growth.
    assert!(t.chunk_count() <= 40, "chunks {}", t.chunk_count());
    assert!(
        t.chunk_count() <= chunks_mid + 8,
        "{} vs {chunks_mid}",
        t.chunk_count()
    );
}

fn colliding_model(hasher: fn(&[u8]) -> u64, ops: usize, keyspace: u64) {
    let mut t = CompactTable::with_hasher(hasher);
    run_model(&mut t, 21, ops, keyspace, 300);
}

#[test]
fn compact_all_hashes_equal() {
    // Every key shares tag and bucket: one giant probe chain that wraps.
    colliding_model(|_| 0, 20_000, 1_500);
    colliding_model(|_| u64::MAX, 20_000, 1_500);
}

#[test]
fn compact_same_tag_different_buckets() {
    colliding_model(|k| (default_hash(k) & !0xFFFF_FFFF) | 0xABCD, 60_000, 5_000);
}

#[test]
fn compact_same_bucket_different_tags() {
    // High 32 bits zero: every key maps to bucket 0 but tags differ.
    colliding_model(|k| default_hash(k) & 0xFFFF_FFFF, 60_000, 5_000);
}

#[test]
fn compact_clustered_hashes_survive_growth_mid_chain() {
    // Only 256 distinct hash values with identical tags: long clusters that
    // are rebuilt (and moved) many times as the table grows.
    colliding_model(|k| u64::from(k[0]) << 56, 80_000, 6_000);
}

#[test]
fn compact_real_tag_collision() {
    // Find two distinct keys whose xxh3 low 32 bits (the stored tag) match.
    let mut seen: HashMap<u32, u64> = HashMap::new();
    let mut pair = None;
    let mut key = [0u8; 16];
    for i in 0..2_000_000u64 {
        make_key(i, &mut key);
        let tag = default_hash(&key) as u32;
        if let Some(&other) = seen.get(&tag) {
            pair = Some((other, i));
            break;
        }
        seen.insert(tag, i);
    }
    let (a, b) = pair.expect("birthday search over 2M keys finds a 32-bit collision");
    let (mut ka, mut kb) = ([0u8; 16], [0u8; 16]);
    make_key(a, &mut ka);
    make_key(b, &mut kb);
    assert_ne!(ka, kb);
    let mut t = CompactTable::new();
    t.insert(&ka, b"first", None).unwrap();
    t.insert(&kb, b"second", None).unwrap();
    assert_eq!(t.get(&ka), Some(&b"first"[..]));
    assert_eq!(t.get(&kb), Some(&b"second"[..]));
    t.insert(&ka, b"first-but-longer-value", None).unwrap();
    assert_eq!(t.get(&kb), Some(&b"second"[..]));
    assert!(t.remove(&ka));
    assert_eq!(t.get(&ka), None);
    assert_eq!(t.get(&kb), Some(&b"second"[..]));
}

#[test]
fn compact_tombstone_churn_stays_bounded() {
    let pop = 10_000u64;
    let mut t = CompactTable::new();
    let mut rng = Rng(5);
    let mut key = [0u8; 16];
    let mut live: Vec<u64> = (0..pop).collect();
    for &i in &live {
        make_key(i, &mut key);
        t.insert(&key, b"v", None).unwrap();
    }
    let cap0 = t.slot_capacity();
    let mut worst_miss = 0;
    for (step, next) in (0..1_000_000u64).zip(pop..) {
        let victim = rng.below(pop) as usize;
        make_key(live[victim], &mut key);
        assert!(t.remove(&key));
        live[victim] = next;
        make_key(next, &mut key);
        t.insert(&key, b"v", None).unwrap();
        assert_eq!(t.len() as u64, pop);
        assert!(t.tombstones() + t.len() <= t.slot_capacity() / 5 * 4);
        if step % 50_000 == 0 {
            for j in 0..200 {
                make_key(u64::MAX - j, &mut key);
                worst_miss = worst_miss.max(t.probe_len(&key));
            }
        }
    }
    assert!(t.rebuilds() > 0, "tombstones must trigger purging rebuilds");
    assert!(
        t.slot_capacity() <= cap0 * 3 / 2,
        "fixed population must not keep growing"
    );
    assert!(worst_miss < 200, "miss probe length {worst_miss}");
    assert!(t.max_probe_len() < 400, "max probe {}", t.max_probe_len());
    for &i in &live {
        make_key(i, &mut key);
        assert!(t.get(&key).is_some());
    }
}
