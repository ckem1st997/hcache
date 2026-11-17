//! The chained dict used before the bucket one (`old_dict.rs`, kept here
//! only for this comparison) against the current `hcache-core` dict, alone:
//! build time, final and peak memory, lookups in random order (hits with
//! the server's two-step prefetch over batches of 16, hits without, misses,
//! hits on keys with a deadline). Run on Linux:
//! `cargo run --release -- 100000 1000000 4000000`.
#![allow(dead_code)]

#[path = "../../../crates/hcache-core/src/dict.rs"]
mod new_dict;
#[path = "old_dict.rs"]
mod old_dict;

use std::alloc::{GlobalAlloc, Layout};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

struct Counting;
static NOW: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static MI: mimalloc::MiMalloc = mimalloc::MiMalloc;

// SAFETY: forwards every call to mimalloc unchanged; only counts sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let n = NOW.fetch_add(l.size(), Relaxed) + l.size();
        PEAK.fetch_max(n, Relaxed);
        MI.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        NOW.fetch_sub(l.size(), Relaxed);
        MI.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        // A growing realloc may copy: old and new are both live for a while.
        let n = NOW.fetch_add(new, Relaxed) + new;
        PEAK.fetch_max(n, Relaxed);
        let r = MI.realloc(p, l, new);
        NOW.fetch_sub(l.size(), Relaxed);
        r
    }
}

#[global_allocator]
static A: Counting = Counting;

/// Same size as the server's `Option<Value>`: 32 bytes with a niche.
type V = (Vec<u8>, u64);

trait Table: Default {
    const NAME: &'static str;
    fn put(&mut self, h: u64, k: &[u8], v: V);
    fn read(&self, h: u64, k: &[u8]) -> Option<&V>;
    fn pf_bucket(&self, h: u64);
    fn pf_entry(&self, h: u64);
    fn size() -> usize;
}

macro_rules! table {
    ($m:ident, $name:expr) => {
        impl Table for $m::Dict<V> {
            const NAME: &'static str = $name;
            fn put(&mut self, h: u64, k: &[u8], v: V) {
                self.insert(h, k, v);
            }
            fn read(&self, h: u64, k: &[u8]) -> Option<&V> {
                self.get(h, k)
            }
            fn pf_bucket(&self, h: u64) {
                self.prefetch_bucket(h)
            }
            fn pf_entry(&self, h: u64) {
                self.prefetch_entry(h)
            }
            fn size() -> usize {
                $m::entry_size::<V>()
            }
        }
    };
}
table!(old_dict, "old");
table!(new_dict, "new");

struct Keys {
    keys: Vec<[u8; 16]>,
    hashes: Vec<u64>,
}

fn keys(from: u64, n: u64) -> Keys {
    let keys: Vec<[u8; 16]> = (from..from + n)
        .map(|i| {
            let mut k = [0; 16];
            k.copy_from_slice(format!("key:{i:012}").as_bytes());
            k
        })
        .collect();
    let hashes = keys.iter().map(|k| new_dict::hash_bytes(k)).collect();
    Keys { keys, hashes }
}

fn order(n: usize, len: usize, seed: u64) -> Vec<u32> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % n as u64) as u32
        })
        .collect()
}

/// Build time per insert, bytes per key at the end and at the peak.
fn build<T: Table>(k: &Keys) -> (T, f64, f64, f64) {
    let base = NOW.load(Relaxed);
    PEAK.store(base, Relaxed);
    let t = Instant::now();
    let mut d = T::default();
    for (i, (key, &h)) in k.keys.iter().zip(&k.hashes).enumerate() {
        d.put(h, key, (Vec::new(), i as u64));
    }
    let ns = t.elapsed().as_nanos() as f64 / k.keys.len() as f64;
    let n = k.keys.len() as f64;
    let fin = (NOW.load(Relaxed) - base) as f64 / n;
    let peak = (PEAK.load(Relaxed) - base) as f64 / n;
    (d, ns, fin, peak)
}

fn hits_prefetched<T: Table>(d: &T, k: &Keys, ord: &[u32]) -> f64 {
    let t = Instant::now();
    let mut sum = 0u64;
    for batch in ord.chunks(16) {
        for &i in batch {
            d.pf_bucket(k.hashes[i as usize]);
        }
        for &i in batch {
            d.pf_entry(k.hashes[i as usize]);
        }
        for &i in batch {
            let i = i as usize;
            sum += d.read(k.hashes[i], &k.keys[i]).map_or(0, |v| v.1);
        }
    }
    black_box(sum);
    t.elapsed().as_nanos() as f64 / ord.len() as f64
}

fn hits_plain<T: Table>(d: &T, k: &Keys, ord: &[u32]) -> f64 {
    let t = Instant::now();
    let mut sum = 0u64;
    for &i in ord {
        let i = i as usize;
        sum += d.read(k.hashes[i], &k.keys[i]).map_or(0, |v| v.1);
    }
    black_box(sum);
    t.elapsed().as_nanos() as f64 / ord.len() as f64
}

/// Every key has a deadline. Old: the value, then the deadline from the
/// separate map (as `rd` did). New: both from the one entry.
fn ttl_old(d: &old_dict::Dict<V>, exp: &old_dict::Dict<u64>, k: &Keys, ord: &[u32]) -> f64 {
    let t = Instant::now();
    let mut sum = 0u64;
    for batch in ord.chunks(16) {
        for &i in batch {
            d.prefetch_bucket(k.hashes[i as usize]);
        }
        for &i in batch {
            d.prefetch_entry(k.hashes[i as usize]);
        }
        for &i in batch {
            let (h, key) = (k.hashes[i as usize], &k.keys[i as usize]);
            let v = d.get(h, key).map_or(0, |v| v.1);
            sum += v + exp.peek(h, key).copied().unwrap_or(0);
        }
    }
    black_box(sum);
    t.elapsed().as_nanos() as f64 / ord.len() as f64
}

fn ttl_new(d: &new_dict::Dict<V>, k: &Keys, ord: &[u32]) -> f64 {
    let t = Instant::now();
    let mut sum = 0u64;
    for batch in ord.chunks(16) {
        for &i in batch {
            d.prefetch_bucket(k.hashes[i as usize]);
        }
        for &i in batch {
            d.prefetch_entry(k.hashes[i as usize]);
        }
        for &i in batch {
            let (h, key) = (k.hashes[i as usize], &k.keys[i as usize]);
            sum += d.get_timed(h, key).map_or(0, |(v, at)| v.1 + at);
        }
    }
    black_box(sum);
    t.elapsed().as_nanos() as f64 / ord.len() as f64
}

fn main() {
    let sizes: Vec<u64> = std::env::args()
        .skip(1)
        .map(|a| a.parse().expect("a key count"))
        .collect();
    println!(
        "entry bytes: old {} new {}",
        <old_dict::Dict<V> as Table>::size(),
        <new_dict::Dict<V> as Table>::size()
    );
    const LOOKUPS: usize = 4_000_000;
    const ROUNDS: usize = 7;
    for n in sizes {
        let hit = keys(0, n);
        let miss = keys(n, n.min(1 << 20));
        let ord = order(n as usize, LOOKUPS, 0x9E37_79B9_7F4A_7C15);
        let mord = order(miss.keys.len(), LOOKUPS, 0x2545_F491_4F6C_DD1D);
        // Build each twice, alternating, and keep the faster build.
        let mut best_build = [f64::MAX; 2];
        let mut mem = [(0.0, 0.0); 2];
        for _ in 0..2 {
            let (d, ns, fin, peak) = build::<old_dict::Dict<V>>(&hit);
            best_build[0] = best_build[0].min(ns);
            mem[0] = (fin, peak);
            drop(d);
            let (d, ns, fin, peak) = build::<new_dict::Dict<V>>(&hit);
            best_build[1] = best_build[1].min(ns);
            mem[1] = (fin, peak);
            drop(d);
        }
        let (old, ..) = build::<old_dict::Dict<V>>(&hit);
        let (mut new, ..) = build::<new_dict::Dict<V>>(&hit);
        let mut exp = old_dict::Dict::<u64>::new();
        for (key, &h) in hit.keys.iter().zip(&hit.hashes) {
            exp.insert(h, key, 1 << 47);
            new.set_expire(h, key, 1 << 47);
        }
        let mut res = [[f64::MAX; 4]; 2];
        for _ in 0..ROUNDS {
            res[0][0] = res[0][0].min(hits_prefetched(&old, &hit, &ord));
            res[1][0] = res[1][0].min(hits_prefetched(&new, &hit, &ord));
            res[0][1] = res[0][1].min(hits_plain(&old, &hit, &ord));
            res[1][1] = res[1][1].min(hits_plain(&new, &hit, &ord));
            res[0][2] = res[0][2].min(hits_prefetched(&old, &miss, &mord));
            res[1][2] = res[1][2].min(hits_prefetched(&new, &miss, &mord));
            res[0][3] = res[0][3].min(ttl_old(&old, &exp, &hit, &ord));
            res[1][3] = res[1][3].min(ttl_new(&new, &hit, &ord));
        }
        println!("== {n} keys (best of {ROUNDS}, ns per op)");
        for (j, name) in ["old", "new"].iter().enumerate() {
            println!(
                "{name}: build {:6.1}  hit+prefetch {:6.1}  hit plain {:6.1}  miss+prefetch {:6.1}  ttl hit {:6.1}  bytes/key {:5.1} peak {:5.1}",
                best_build[j], res[j][0], res[j][1], res[j][2], res[j][3], mem[j].0, mem[j].1
            );
        }
        drop((old, new, exp));
    }
}
