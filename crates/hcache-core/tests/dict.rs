//! Adversarial tests for the hash table and its scan cursor.

use hcache_core::dict::{hash_bytes, Dict, MAX_LOAD};
use hcache_core::testutil::Rng;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

fn key_of(id: u64) -> Vec<u8> {
    if id == 0 {
        return Vec::new();
    }
    match id % 5 {
        0 => format!("key:{id}").into_bytes(),
        1 => {
            // Keys that differ only in their last bytes.
            let mut k = b"a-rather-long-common-prefix-for-all-of-these-keys/".to_vec();
            k.extend_from_slice(&id.to_le_bytes());
            k
        }
        2 => {
            let mut k = vec![b'x'; 200 + (id % 100) as usize];
            k.extend_from_slice(&id.to_le_bytes());
            k
        }
        3 => id.to_le_bytes().to_vec(),
        _ => id.to_string().into_bytes(),
    }
}

fn assert_shape<V>(d: &Dict<V>) {
    let bc = d.bucket_count();
    assert!(bc.is_power_of_two(), "bucket_count {bc}");
    assert!(
        d.len() <= bc * MAX_LOAD,
        "len {} exceeds the load of {bc} buckets",
        d.len()
    );
    assert_eq!(d.is_empty(), matches!(d.len(), 0));
}

fn check_against_model(d: &Dict<u64>, m: &HashMap<Vec<u8>, u64>, hash: fn(&[u8]) -> u64) {
    assert_eq!(d.len(), m.len());
    let mut seen = HashSet::new();
    d.for_each(|k, v| {
        assert!(seen.insert(k.to_vec()), "for_each repeated {k:?}");
        assert_eq!(m.get(k), Some(v));
    });
    assert_eq!(seen.len(), m.len());
    for (k, v) in m {
        assert_eq!(d.get(hash(k), k), Some(v));
        assert!(d.contains(hash(k), k));
    }
}

fn model_run(seed: u64, keyspace: u64, ops: usize, hash: fn(&[u8]) -> u64) {
    let mut rng = Rng::new(seed);
    let mut d: Dict<u64> = Dict::new();
    let mut m: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut peak = 0usize;
    let mut last_bc = d.bucket_count();
    for step in 0..ops {
        let k = key_of(rng.below(keyspace));
        let h = hash(&k);
        match rng.below(10) {
            0..=3 => {
                let v = rng.next_u64();
                assert_eq!(d.insert(h, &k, v), m.insert(k.clone(), v), "step {step}");
            }
            4 | 5 => assert_eq!(d.remove(h, &k), m.remove(&k), "step {step}"),
            6 => assert_eq!(d.get(h, &k), m.get(&k), "step {step}"),
            7 => {
                if let Some(v) = d.get_mut(h, &k) {
                    *v = v.wrapping_add(1);
                    let mv = m.get_mut(&k).expect("model has key");
                    *mv = mv.wrapping_add(1);
                } else {
                    assert!(!m.contains_key(&k), "step {step}");
                }
            }
            8 => assert_eq!(d.contains(h, &k), m.contains_key(&k), "step {step}"),
            _ => {
                // Probe a key that was never inserted by this keyspace.
                let mut miss = k.clone();
                miss.extend_from_slice(b"\xFF\xFEmiss");
                assert_eq!(d.get(hash(&miss), &miss), None);
            }
        }
        assert_eq!(d.len(), m.len(), "step {step}");
        peak = peak.max(d.len());
        let bc = d.bucket_count();
        assert!(bc >= last_bc, "table shrank without clear");
        assert!(
            bc == 1 || bc / 2 * MAX_LOAD < peak,
            "bc {bc} too large for peak {peak}"
        );
        last_bc = bc;
        if step % 25_000 == 0 {
            assert_shape(&d);
        }
        if step % 100_000 == 99_999 {
            check_against_model(&d, &m, hash);
        }
    }
    check_against_model(&d, &m, hash);
    assert_shape(&d);
}

fn real_hash(k: &[u8]) -> u64 {
    hash_bytes(k)
}

#[test]
fn model_small_keyspace() {
    model_run(1, 40, 1_000_000, real_hash);
}

#[test]
fn model_large_keyspace_many_grow_cycles() {
    model_run(2, 150_000, 1_500_000, real_hash);
}

#[test]
fn model_keyspace_of_one_key() {
    model_run(3, 1, 20_000, real_hash);
}

#[test]
fn model_hash_with_only_four_distinct_values() {
    // Long chains; also exercises equal-hash different-key comparison.
    model_run(4, 3_000, 150_000, |k| hash_bytes(k) & 3);
}

#[test]
fn model_constant_hash() {
    model_run(5, 600, 60_000, |_| 42);
}

#[test]
fn model_extreme_hash_values() {
    model_run(6, 2_000, 100_000, |k| match hash_bytes(k) % 4 {
        0 => 0,
        1 => u64::MAX,
        2 => 1 << 63,
        _ => hash_bytes(k) | (1 << 63),
    });
}

#[test]
fn sequential_inserts_then_drain_and_reuse() {
    let mut d: Dict<u64> = Dict::new();
    let n = 100_000u64;
    for i in 0..n {
        let k = i.to_le_bytes();
        assert_eq!(d.insert(hash_bytes(&k), &k, i), None);
    }
    assert_eq!(d.len(), n as usize);
    let grown = d.bucket_count();
    assert!(grown * MAX_LOAD >= n as usize);
    for i in 0..n {
        let k = i.to_le_bytes();
        assert_eq!(d.get(hash_bytes(&k), &k), Some(&i));
    }
    for i in 0..n {
        let k = i.to_le_bytes();
        assert_eq!(d.remove(hash_bytes(&k), &k), Some(i));
        assert_eq!(d.remove(hash_bytes(&k), &k), None);
    }
    assert!(d.is_empty());
    // The doc says it never shrinks.
    assert_eq!(d.bucket_count(), grown);
    let mut count = 0;
    d.for_each(|_, _| count += 1);
    assert_eq!(count, 0);
    let mut scanned = 0;
    let mut cur = 0;
    let mut steps = 0;
    loop {
        cur = d.scan(cur, |_, _| scanned += 1);
        steps += 1;
        if cur == 0 {
            break;
        }
        assert!(steps <= grown);
    }
    assert_eq!((scanned, steps), (0, grown));
    // Reuse all free slots.
    for i in 0..n {
        let k = i.to_le_bytes();
        assert_eq!(d.insert(hash_bytes(&k), &k, i + 1), None);
    }
    assert_eq!(d.len(), n as usize);
    assert_eq!(
        d.get(hash_bytes(&5u64.to_le_bytes()), &5u64.to_le_bytes()),
        Some(&6)
    );
}

#[test]
fn empty_key_and_key_prefixes_are_distinct() {
    let mut d: Dict<u8> = Dict::new();
    let keys: [&[u8]; 5] = [b"", b"\0", b"\0\0", b"a", b"a\0"];
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(d.insert(hash_bytes(k), k, i as u8), None);
    }
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(d.get(hash_bytes(k), k), Some(&(i as u8)));
    }
    assert_eq!(d.len(), 5);
    assert_eq!(d.remove(hash_bytes(b""), b""), Some(0));
    assert!(!d.contains(hash_bytes(b""), b""));
    assert!(d.contains(hash_bytes(b"\0"), b"\0"));
}

#[test]
fn keys_on_both_sides_of_the_inline_limit_survive_clone_and_removal() {
    let keys: Vec<Vec<u8>> = (0..=40usize).map(|n| vec![b'k'; n]).collect();
    let mut d: Dict<usize> = Dict::new();
    for (i, k) in keys.iter().enumerate() {
        d.insert(hash_bytes(k), k, i);
    }
    let copy = d.clone();
    for k in keys.iter().step_by(2) {
        d.remove(hash_bytes(k), k);
    }
    drop(d);
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(
            copy.get(hash_bytes(k), k),
            Some(&i),
            "key of {} bytes",
            k.len()
        );
    }
    let mut seen = Vec::new();
    copy.for_each(|k, &v| seen.push((k.to_vec(), v)));
    seen.sort_by_key(|&(_, v)| v);
    assert_eq!(seen, keys.into_iter().zip(0..).collect::<Vec<_>>());
}

#[test]
fn values_are_dropped_exactly_once() {
    let token = Rc::new(());
    let mut rng = Rng::new(9);
    {
        let mut d: Dict<Rc<()>> = Dict::new();
        for _ in 0..50_000 {
            let k = key_of(rng.below(500));
            let h = hash_bytes(&k);
            match rng.below(4) {
                0 | 1 => {
                    d.insert(h, &k, Rc::clone(&token));
                }
                2 => {
                    d.remove(h, &k);
                }
                _ => {
                    if rng.chance(500) {
                        d.clear();
                    }
                }
            }
            assert_eq!(Rc::strong_count(&token), 1 + d.len());
        }
        d.clear();
        assert_eq!(Rc::strong_count(&token), 1);
        for i in 0..100u64 {
            d.insert(i, &i.to_le_bytes(), Rc::clone(&token));
        }
        assert_eq!(Rc::strong_count(&token), 101);
    }
    assert_eq!(Rc::strong_count(&token), 1);
}

// ---------------------------------------------------------------------------
// Colliding keys, found by brute force
// ---------------------------------------------------------------------------

fn colliding_keys(bits: u32, want: usize) -> Vec<Vec<u8>> {
    let mask = (1u64 << bits) - 1;
    let mut out = Vec::new();
    let mut i = 0u64;
    while out.len() < want {
        let k = format!("c{i}").into_bytes();
        if hash_bytes(&k) & mask == 0x5 {
            out.push(k);
        }
        i += 1;
    }
    out
}

#[test]
fn low_bit_colliding_real_keys() {
    let keys = colliding_keys(8, 1_500);
    let mut d: Dict<usize> = Dict::new();
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(d.insert(hash_bytes(k), k, i), None);
    }
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(d.get(hash_bytes(k), k), Some(&i));
    }
    // Remove every other key, scan, expect the rest exactly once.
    for k in keys.iter().step_by(2) {
        assert!(d.remove(hash_bytes(k), k).is_some());
    }
    let mut got = Vec::new();
    let mut cur = 0;
    loop {
        cur = d.scan(cur, |k, _| got.push(k.to_vec()));
        if cur == 0 {
            break;
        }
    }
    got.sort();
    let mut want: Vec<Vec<u8>> = keys.iter().skip(1).step_by(2).cloned().collect();
    want.sort();
    assert_eq!(got, want);
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

fn static_table(n: usize, seed: u64) -> (Dict<u64>, HashMap<Vec<u8>, u64>) {
    let mut rng = Rng::new(seed);
    let mut d = Dict::new();
    let mut m = HashMap::new();
    while m.len() < n {
        let k = key_of(rng.below(10 * n as u64 + 10));
        let v = rng.next_u64();
        d.insert(hash_bytes(&k), &k, v);
        m.insert(k, v);
    }
    (d, m)
}

#[test]
fn cursor_visits_every_bucket_exactly_once() {
    for n in [0usize, 1, 3, 4, 5, 8, 9, 100, 1000, 5000] {
        let (d, _) = static_table(n, n as u64);
        let b = d.bucket_count();
        let mut seen = HashSet::new();
        let mut cur = 0u64;
        for _ in 0..b {
            assert!(cur < b as u64, "cursor {cur} out of range for {b} buckets");
            assert!(seen.insert(cur), "cursor {cur} repeated");
            cur = d.scan(cur, |_, _| {});
        }
        assert_eq!(cur, 0, "scan must end after exactly {b} steps (n={n})");
    }
}

#[test]
fn static_scan_returns_each_key_exactly_once() {
    for n in [0usize, 1, 2, 3, 4, 5, 100, 1234, 20_000] {
        let (d, m) = static_table(n, 77 + n as u64);
        let mut got: HashMap<Vec<u8>, u64> = HashMap::new();
        let mut cur = 0;
        let mut steps = 0;
        loop {
            cur = d.scan(cur, |k, v| {
                assert_eq!(got.insert(k.to_vec(), *v), None, "duplicate {k:?}");
            });
            steps += 1;
            assert!(steps <= d.bucket_count());
            if cur == 0 {
                break;
            }
        }
        assert_eq!(got, m, "n={n}");
    }
}

#[test]
fn scan_chunk_returns_each_key_exactly_once_for_any_budget() {
    for budget in [0usize, 1, 2, 3, 7, 64, 100_000, usize::MAX] {
        for n in [0usize, 1, 5, 300, 5000] {
            let (d, m) = static_table(n, 1000 + n as u64);
            let mut got: HashMap<Vec<u8>, u64> = HashMap::new();
            let mut cur = 0;
            let mut calls = 0;
            loop {
                cur = d.scan_chunk(cur, budget, |k, v| {
                    assert_eq!(got.insert(k.to_vec(), *v), None, "duplicate {k:?}");
                });
                calls += 1;
                assert!(
                    calls <= d.bucket_count() + 1,
                    "scan_chunk did not terminate"
                );
                if cur == 0 {
                    break;
                }
            }
            assert_eq!(got, m, "budget={budget} n={n}");
        }
    }
}

/// Number of single-bucket steps from `from` to `to`.
fn steps_between(d: &Dict<u64>, from: u64, to: u64) -> usize {
    let mut c = from;
    for s in 1..=d.bucket_count() {
        c = d.scan(c, |_, _| {});
        if c == to {
            return s;
        }
    }
    panic!("cursor {to} is not reachable from {from}");
}

#[test]
fn scan_chunk_budget_semantics() {
    // Dense (long chains) and sparse (mostly empty buckets) tables.
    let mut dense: Dict<u64> = Dict::new();
    for i in 0..400u64 {
        let k = i.to_le_bytes();
        dense.insert(i % 3, &k, i);
    }
    let (sparse, _) = static_table(40, 3);
    let mut sparse2: Dict<u64> = Dict::new();
    for i in 0..300u64 {
        let k = i.to_le_bytes();
        sparse2.insert(hash_bytes(&k), &k, i);
    }
    for k in 0..290u64 {
        let key = k.to_le_bytes();
        sparse2.remove(hash_bytes(&key), &key);
    }
    for d in [&dense, &sparse, &sparse2] {
        for budget in [0usize, 1, 2, 5, 17, 1000, usize::MAX] {
            let b = budget.max(1);
            let mut cur = 0;
            loop {
                let mut chunk: Vec<Vec<u8>> = Vec::new();
                let ret = d.scan_chunk(cur, budget, |k, _| chunk.push(k.to_vec()));
                let s = steps_between(d, cur, ret);
                // Replay bucket by bucket and compute where it must stop.
                let (mut c, mut seen, mut expect_steps) = (cur, 0usize, 0usize);
                let mut replay: Vec<Vec<u8>> = Vec::new();
                loop {
                    c = d.scan(c, |k, _| {
                        seen += 1;
                        replay.push(k.to_vec());
                    });
                    expect_steps += 1;
                    if c == 0 || seen >= b || expect_steps >= b.saturating_mul(4) {
                        break;
                    }
                }
                assert_eq!(s, expect_steps, "budget {budget}: steps taken");
                assert_eq!(ret, c, "budget {budget}: returned cursor");
                assert_eq!(chunk, replay, "budget {budget}: items");
                if ret == 0 {
                    break;
                }
                cur = ret;
            }
        }
    }
}

#[test]
fn garbage_cursors_do_not_panic_and_terminate() {
    let mut rng = Rng::new(31);
    for n in [0usize, 1, 7, 500] {
        let (d, _) = static_table(n, n as u64 + 5);
        let b = d.bucket_count() as u64;
        let mut cursors = vec![
            0,
            1,
            b - 1,
            b,
            b + 1,
            u64::MAX,
            u64::MAX - 1,
            1 << 63,
            1 << 32,
        ];
        cursors.extend((0..200).map(|_| rng.next_u64()));
        for start in cursors {
            let mut cur = start;
            let mut steps = 0;
            loop {
                cur = d.scan(cur, |_, _| {});
                steps += 1;
                assert!(
                    cur < b,
                    "scan returned {cur} for {b} buckets (start {start})"
                );
                assert!(steps <= b as usize, "did not terminate from {start}");
                if cur == 0 {
                    break;
                }
            }
            // Chunked, with a garbage start and a tiny budget.
            let mut cur = start;
            let mut calls = 0;
            loop {
                cur = d.scan_chunk(cur, 1, |_, _| {});
                calls += 1;
                assert!(calls <= b as usize + 1);
                if cur == 0 {
                    break;
                }
            }
        }
    }
}

#[test]
fn scan_with_old_cursor_after_clear_is_safe() {
    let (mut d, _) = static_table(5000, 8);
    let mut cur = 0;
    for _ in 0..3000 {
        cur = d.scan(cur, |_, _| {});
    }
    assert_ne!(cur, 0);
    d.clear();
    assert_eq!(d.bucket_count(), Dict::<u8>::new().bucket_count());
    let k = b"after";
    d.insert(hash_bytes(k), k, 1);
    let mut steps = 0;
    while cur != 0 {
        cur = d.scan(cur, |_, _| {});
        steps += 1;
        assert!(steps <= 4);
    }
}

#[test]
fn clear_then_reuse() {
    let (mut d, m) = static_table(3000, 4);
    assert!(d.bucket_count() > 1000 / MAX_LOAD);
    d.clear();
    assert!(d.is_empty());
    assert_eq!(d.bucket_count(), Dict::<u8>::new().bucket_count());
    for k in m.keys().take(50) {
        assert_eq!(d.get(hash_bytes(k), k), None);
        assert!(!d.contains(hash_bytes(k), k));
        assert_eq!(d.remove(hash_bytes(k), k), None);
    }
    let mut n = 0;
    d.scan_chunk(0, usize::MAX, |_, _| n += 1);
    assert_eq!(n, 0);
    for (i, k) in m.keys().enumerate() {
        assert_eq!(d.insert(hash_bytes(k), k, i as u64), None);
    }
    assert_eq!(d.len(), m.len());
    let mut got = 0;
    d.for_each(|_, _| got += 1);
    assert_eq!(got, m.len());
}

/// What the module doc promises: a key present for the whole scan is
/// reported exactly once, whatever happens to other keys and however many
/// times the table grows in between. Nothing that never existed is reported.
fn scan_under_mutation(
    seed: u64,
    budget: Option<usize>,
    stable_n: usize,
    vol_inserts: usize,
) -> (usize, usize) {
    let mut rng = Rng::new(seed);
    let mut d: Dict<u64> = Dict::new();
    let mut present: HashMap<Vec<u8>, u64> = HashMap::new();
    for i in 0..stable_n {
        let k = format!("stable:{i}").into_bytes();
        d.insert(hash_bytes(&k), &k, i as u64);
        present.insert(k, i as u64);
    }
    for i in 0..stable_n / 2 {
        let k = format!("vol0:{i}").into_bytes();
        d.insert(hash_bytes(&k), &k, 7);
        present.insert(k, 7);
    }
    // `must` keys were present for the entire scan; `may` keys existed at
    // some point during it.
    let mut must: HashSet<Vec<u8>> = present.keys().cloned().collect();
    let mut may: HashSet<Vec<u8>> = must.clone();
    let start_buckets = d.bucket_count();
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut cur = 0u64;
    let mut inserted = 0usize;
    let mut calls = 0usize;
    loop {
        cur = match budget {
            None => d.scan(cur, |k, _| *counts.entry(k.to_vec()).or_default() += 1),
            Some(b) => d.scan_chunk(cur, b, |k, _| *counts.entry(k.to_vec()).or_default() += 1),
        };
        calls += 1;
        assert!(calls < 5_000_000, "scan does not terminate");
        if cur == 0 {
            break;
        }
        for _ in 0..rng.below(12) {
            if inserted < vol_inserts && rng.below(3) != 0 {
                let id = rng.below(1_000_000);
                let k = format!("vol:{id}").into_bytes();
                let v = rng.next_u64();
                d.insert(hash_bytes(&k), &k, v);
                present.insert(k.clone(), v);
                may.insert(k);
                inserted += 1;
            } else {
                // Remove a random volatile key (never a stable one).
                let id = rng.below(1_000_000);
                let k = if rng.chance(3) {
                    format!("vol0:{}", rng.below(stable_n as u64 / 2 + 1)).into_bytes()
                } else {
                    format!("vol:{id}").into_bytes()
                };
                if d.remove(hash_bytes(&k), &k).is_some() {
                    present.remove(&k);
                    must.remove(&k);
                }
            }
        }
    }
    for (k, n) in &counts {
        assert!(
            may.contains(k),
            "scan returned a key that never existed: {k:?}"
        );
        assert_eq!(*n, 1, "key {k:?} returned {n} times (budget {budget:?})");
    }
    for k in &must {
        assert_eq!(
            counts.get(k),
            Some(&1),
            "key {k:?} present for the whole scan was returned {:?} times (budget {budget:?}, \
             buckets {start_buckets} -> {})",
            counts.get(k),
            d.bucket_count()
        );
    }
    (start_buckets, d.bucket_count())
}

#[test]
fn scan_survives_growth_and_churn_single_bucket_steps() {
    // 330 stable and 165 volatile keys sit just under the load at which
    // 64 buckets double.
    for seed in 0..6 {
        let (a, b) = scan_under_mutation(seed, None, 330, 5_000);
        assert!(b > a, "no growth during scan");
    }
}

#[test]
fn scan_chunk_survives_growth_and_churn() {
    for (i, budget) in [0usize, 1, 2, 3, 10, 100, usize::MAX]
        .into_iter()
        .enumerate()
    {
        let _ = scan_under_mutation(100 + i as u64, Some(budget), 150, 4_000);
    }
}

#[test]
fn scan_survives_growth_from_tiny_table() {
    for seed in 0..20 {
        let _ = scan_under_mutation(500 + seed, None, 1, 3_000);
        let _ = scan_under_mutation(600 + seed, Some(1), 2, 3_000);
    }
}

#[test]
fn scan_with_fake_hash_collisions_under_growth() {
    // Scan the hash-collision-heavy table while it grows; all hashes share
    // low bits so most keys sit in a couple of long chains.
    let mut d: Dict<u64> = Dict::new();
    let mut keys = Vec::new();
    for i in 0..200u64 {
        let k = i.to_le_bytes().to_vec();
        d.insert((i % 4) << 5, &k, i);
        keys.push(k);
    }
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut cur = 0;
    let mut extra = 1000u64;
    let mut calls = 0;
    loop {
        cur = d.scan_chunk(cur, 3, |k, _| *counts.entry(k.to_vec()).or_default() += 1);
        calls += 1;
        assert!(calls < 100_000);
        if cur == 0 {
            break;
        }
        if extra < 1600 {
            for _ in 0..3 {
                let k = extra.to_le_bytes();
                d.insert((extra % 7) << 9, &k, extra);
                extra += 1;
            }
        }
    }
    for k in &keys {
        assert_eq!(counts.get(k), Some(&1), "{k:?}");
    }
    for (k, n) in &counts {
        assert_eq!(*n, 1, "{k:?}");
    }
}

#[test]
fn scan_only_sees_current_values() {
    let mut d: Dict<u64> = Dict::new();
    for i in 0..1000u64 {
        let k = i.to_le_bytes();
        d.insert(hash_bytes(&k), &k, i);
    }
    for i in 0..1000u64 {
        let k = i.to_le_bytes();
        assert_eq!(d.insert(hash_bytes(&k), &k, i + 10_000), Some(i));
    }
    let mut cur = 0;
    let mut n = 0;
    loop {
        cur = d.scan(cur, |k, v| {
            let id = u64::from_le_bytes(k.try_into().unwrap());
            assert_eq!(*v, id + 10_000);
            n += 1;
        });
        if cur == 0 {
            break;
        }
    }
    assert_eq!(n, 1000);
}
