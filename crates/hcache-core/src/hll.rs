//! HyperLogLog in Redis's own string format (PFADD / PFCOUNT / PFMERGE).
//!
//! The value is an ordinary string, so a string made by real Redis can be
//! read here and the other way round. Only the dense encoding is ever
//! written: a sparse input is expanded to dense before it is changed, which
//! keeps this module free of the sparse-promotion logic. Both encodings are
//! read.
//!
//! Callers hand in strings a client fully controls (SET, then PFCOUNT), so
//! every read goes through [`registers`], which checks the shape first and
//! never allocates more than one fixed register table.

/// What is wrong with a string that should be a HyperLogLog.
#[derive(Debug, PartialEq, Eq)]
pub struct Invalid; // callers reply "WRONGTYPE Key is not a valid HyperLogLog string value."

/// log2 of the register count.
const P: u32 = 14;
/// Bits of the hash left for the run length once the index is taken out.
const Q: u32 = 64 - P;
const REGISTERS: usize = 1 << P;
const INDEX_MASK: u64 = (REGISTERS as u64) - 1;
/// Bits per register in the dense encoding.
const REG_BITS: usize = 6;
const REG_MAX: u8 = (1 << REG_BITS) - 1;

const MAGIC: &[u8; 4] = b"HYLL";
const HDR_LEN: usize = 16;
/// The cached cardinality: 8 bytes little-endian at the end of the header.
const CARD_START: usize = 8;
/// Most significant bit of the last cache byte: set means "stale".
const CARD_STALE_BIT: u8 = 0x80;
const ENC_DENSE: u8 = 0;
const ENC_SPARSE: u8 = 1;
const DENSE_LEN: usize = HDR_LEN + (REGISTERS * REG_BITS).div_ceil(8);

/// Sparse opcodes: ZERO `00xxxxxx`, XZERO `01xxxxxx yyyyyyyy`, VAL `1vvvvvxx`.
const SPARSE_XZERO_FLAG: u8 = 0b0100_0000;
const SPARSE_VAL_FLAG: u8 = 0b1000_0000;
const SPARSE_ZERO_LEN_MASK: u8 = 0b0011_1111;

const MURMUR_SEED: u64 = 0xadc8_3b19;
const MURMUR_M: u64 = 0xc6a4_a793_5bd1_e995;
const MURMUR_R: u32 = 47;

/// Ertl's limit constant, 1 / (2 ln 2).
const ALPHA_INF: f64 = 0.721_347_520_444_481_7;
/// Histogram slots: one per value a 6-bit register can hold.
const HISTO_LEN: usize = 1 << REG_BITS;

/// MurmurHash64A, as Redis uses it for HyperLogLog.
fn murmur64a(data: &[u8], seed: u64) -> u64 {
    let mut h = seed ^ (data.len() as u64).wrapping_mul(MURMUR_M);
    let (blocks, tail) = data.as_chunks::<8>();
    for block in blocks {
        let mut k = u64::from_le_bytes(*block);
        k = k.wrapping_mul(MURMUR_M);
        k ^= k >> MURMUR_R;
        k = k.wrapping_mul(MURMUR_M);
        h ^= k;
        h = h.wrapping_mul(MURMUR_M);
    }
    if !tail.is_empty() {
        for (i, &b) in tail.iter().enumerate() {
            h ^= u64::from(b) << (8 * i);
        }
        h = h.wrapping_mul(MURMUR_M);
    }
    h ^= h >> MURMUR_R;
    h = h.wrapping_mul(MURMUR_M);
    h ^ (h >> MURMUR_R)
}

/// Register index and run length (1..=Q+1) for an element, like `hllPatLen`.
fn pattern(element: &[u8]) -> (usize, u8) {
    let hash = murmur64a(element, MURMUR_SEED);
    let index = (hash & INDEX_MASK) as usize;
    // The guard bit makes the run stop at Q + 1 when the rest is all zero.
    let rest = (hash >> P) | (1u64 << Q);
    (index, (rest.trailing_zeros() + 1) as u8)
}

/// Splits a string into encoding and payload, checking the cheap invariants.
fn parse(hll: &[u8]) -> Result<(u8, &[u8]), Invalid> {
    if hll.len() < HDR_LEN || !hll.starts_with(MAGIC) {
        return Err(Invalid);
    }
    let payload = &hll[HDR_LEN..];
    match hll[4] {
        ENC_DENSE if hll.len() == DENSE_LEN => Ok((ENC_DENSE, payload)),
        ENC_SPARSE => Ok((ENC_SPARSE, payload)),
        _ => Err(Invalid),
    }
}

fn dense_get(payload: &[u8], reg: usize) -> u8 {
    let bit = reg * REG_BITS;
    let (byte, shift) = (bit / 8, bit % 8);
    let lo = u16::from(payload.get(byte).copied().unwrap_or(0));
    let hi = u16::from(payload.get(byte + 1).copied().unwrap_or(0));
    (((lo | (hi << 8)) >> shift) & u16::from(REG_MAX)) as u8
}

fn dense_set(payload: &mut [u8], reg: usize, value: u8) {
    let bit = reg * REG_BITS;
    let (byte, shift) = (bit / 8, bit % 8);
    let lo = u16::from(payload.get(byte).copied().unwrap_or(0));
    let hi = u16::from(payload.get(byte + 1).copied().unwrap_or(0));
    let mask = u16::from(REG_MAX) << shift;
    let word = ((lo | (hi << 8)) & !mask) | (u16::from(value & REG_MAX) << shift);
    // The last register ends exactly on a byte boundary, so the byte after
    // the payload would only ever receive zero bits.
    if let Some(b) = payload.get_mut(byte) {
        *b = word as u8;
    }
    if let Some(b) = payload.get_mut(byte + 1) {
        *b = (word >> 8) as u8;
    }
}

/// Expands sparse opcodes into registers; the opcodes must cover exactly
/// `REGISTERS` registers.
fn sparse_registers(payload: &[u8]) -> Result<Vec<u8>, Invalid> {
    let mut regs = vec![0u8; REGISTERS];
    let mut idx = 0usize;
    let mut ops = payload.iter();
    while let Some(&op) = ops.next() {
        let (len, value) = if op & SPARSE_VAL_FLAG != 0 {
            (usize::from(op & 0b11) + 1, ((op >> 2) & 0b1_1111) + 1)
        } else if op & SPARSE_XZERO_FLAG != 0 {
            let &low = ops.next().ok_or(Invalid)?;
            let len = (usize::from(op & SPARSE_ZERO_LEN_MASK) << 8) | usize::from(low);
            (len + 1, 0)
        } else {
            (usize::from(op & SPARSE_ZERO_LEN_MASK) + 1, 0)
        };
        let run = regs.get_mut(idx..idx + len).ok_or(Invalid)?;
        run.fill(value);
        idx += len;
    }
    if idx == REGISTERS {
        Ok(regs)
    } else {
        Err(Invalid)
    }
}

/// All register values of a valid HyperLogLog, whatever its encoding.
fn registers(hll: &[u8]) -> Result<Vec<u8>, Invalid> {
    let (encoding, payload) = parse(hll)?;
    if encoding == ENC_SPARSE {
        return sparse_registers(payload);
    }
    Ok((0..REGISTERS).map(|r| dense_get(payload, r)).collect())
}

fn invalidate_cache(hll: &mut [u8]) {
    if let Some(b) = hll.get_mut(HDR_LEN - 1) {
        *b |= CARD_STALE_BIT;
    }
}

/// Packs registers into a fresh dense string (cache left stale).
fn pack_dense(regs: &[u8]) -> Vec<u8> {
    let mut out = new();
    if let Some(payload) = out.get_mut(HDR_LEN..) {
        for (i, &v) in regs.iter().enumerate() {
            dense_set(payload, i, v);
        }
    }
    invalidate_cache(&mut out);
    out
}

/// A new, empty HyperLogLog (dense).
pub fn new() -> Vec<u8> {
    let mut hll = vec![0u8; DENSE_LEN];
    hll[..4].copy_from_slice(MAGIC);
    hll[4] = ENC_DENSE;
    hll
}

/// Checks header, encoding and length (dense: exact length; sparse: opcodes
/// must cover exactly 16384 registers, and no VAL beyond the format's limits).
pub fn validate(hll: &[u8]) -> Result<(), Invalid> {
    let (encoding, payload) = parse(hll)?;
    if encoding == ENC_SPARSE {
        sparse_registers(payload)?;
    }
    Ok(())
}

/// PFADD of one element. Returns whether a register changed. Converts sparse
/// to dense first. Invalidates the cached cardinality when it changes
/// something.
pub fn add(hll: &mut Vec<u8>, element: &[u8]) -> Result<bool, Invalid> {
    let (encoding, payload) = parse(hll)?;
    if encoding == ENC_SPARSE {
        let mut dense = pack_dense(&sparse_registers(payload)?);
        // Keep the old cache: it is still right until something changes.
        dense[CARD_START..HDR_LEN].copy_from_slice(&hll[CARD_START..HDR_LEN]);
        *hll = dense;
    }
    let (index, run) = pattern(element);
    let Some(payload) = hll.get_mut(HDR_LEN..) else {
        return Err(Invalid);
    };
    if run <= dense_get(payload, index) {
        return Ok(false);
    }
    dense_set(payload, index, run);
    invalidate_cache(hll);
    Ok(true)
}

/// Estimate for a register table (Ertl's improved estimator, as in Redis).
fn estimate(regs: &[u8]) -> u64 {
    let mut histo = [0u32; HISTO_LEN];
    for &r in regs {
        histo[usize::from(r & REG_MAX)] += 1;
    }
    let m = REGISTERS as f64;
    let top = Q as usize;
    let mut z = m * tau((m - f64::from(histo[top + 1])) / m);
    for &count in histo[1..=top].iter().rev() {
        z += f64::from(count);
        z *= 0.5;
    }
    z += m * sigma(f64::from(histo[0]) / m);
    // An empty set gives z = infinity, which rounds to 0 here.
    (ALPHA_INF * m * m / z).round() as u64
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let prev = z;
        z += x * y;
        y += y;
        if prev == z {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let prev = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if prev == z {
            return z / 3.0;
        }
    }
}

/// PFCOUNT of one or several HyperLogLogs (the union). Does not modify
/// anything, so it neither reads nor refreshes the cached cardinality.
pub fn count(hlls: &[&[u8]]) -> Result<u64, Invalid> {
    let mut union = vec![0u8; REGISTERS];
    for hll in hlls {
        for (u, r) in union.iter_mut().zip(registers(hll)?) {
            *u = (*u).max(r);
        }
    }
    Ok(estimate(&union))
}

/// PFMERGE: `dst` (possibly a freshly `new()` one) becomes the union of
/// itself and `sources`. Result is dense. On error `dst` is left untouched.
pub fn merge(dst: &mut Vec<u8>, sources: &[&[u8]]) -> Result<(), Invalid> {
    let mut union = registers(dst)?;
    for source in sources {
        for (u, r) in union.iter_mut().zip(registers(source)?) {
            *u = (*u).max(r);
        }
    }
    *dst = pack_dense(&union);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn element(i: usize) -> Vec<u8> {
        format!("element:{i}").into_bytes()
    }

    fn filled(range: std::ops::Range<usize>) -> Vec<u8> {
        let mut hll = new();
        for i in range {
            add(&mut hll, &element(i)).unwrap();
        }
        hll
    }

    fn count_one(hll: &[u8]) -> u64 {
        count(&[hll]).unwrap()
    }

    /// Straight transcription of Redis's MurmurHash64A, with the tail switch
    /// written as a fall-through cascade.
    fn murmur_reference(data: &[u8], seed: u64) -> u64 {
        let m: u64 = 0xc6a4a7935bd1e995;
        let mut h = seed ^ (data.len() as u64).wrapping_mul(m);
        let nblocks = data.len() / 8;
        for i in 0..nblocks {
            let mut k = 0u64;
            for b in 0..8 {
                k |= u64::from(data[i * 8 + b]) << (8 * b);
            }
            k = k.wrapping_mul(m);
            k ^= k >> 47;
            k = k.wrapping_mul(m);
            h ^= k;
            h = h.wrapping_mul(m);
        }
        let t = &data[nblocks * 8..];
        if t.len() >= 7 {
            h ^= u64::from(t[6]) << 48;
        }
        if t.len() >= 6 {
            h ^= u64::from(t[5]) << 40;
        }
        if t.len() >= 5 {
            h ^= u64::from(t[4]) << 32;
        }
        if t.len() >= 4 {
            h ^= u64::from(t[3]) << 24;
        }
        if t.len() >= 3 {
            h ^= u64::from(t[2]) << 16;
        }
        if t.len() >= 2 {
            h ^= u64::from(t[1]) << 8;
        }
        if !t.is_empty() {
            h ^= u64::from(t[0]);
            h = h.wrapping_mul(m);
        }
        h ^= h >> 47;
        h = h.wrapping_mul(m);
        h ^= h >> 47;
        h
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
        /// Random bytes, length in `0..max`.
        fn bytes_upto(&mut self, max: usize) -> Vec<u8> {
            let n = self.below(max);
            (0..n).map(|_| self.next() as u8).collect()
        }
    }

    fn sparse_with(ops: &[u8]) -> Vec<u8> {
        let mut s = new();
        s.truncate(HDR_LEN);
        s[4] = ENC_SPARSE;
        s.extend_from_slice(ops);
        s
    }

    #[test]
    fn murmur_matches_reference() {
        for data in [&b""[..], b"a", b"hello"] {
            assert_eq!(
                murmur64a(data, MURMUR_SEED),
                murmur_reference(data, MURMUR_SEED)
            );
        }
        let pattern: Vec<u8> = (0..70u8)
            .map(|i| i.wrapping_mul(37).wrapping_add(11))
            .collect();
        for len in 0..pattern.len() {
            assert_eq!(
                murmur64a(&pattern[..len], MURMUR_SEED),
                murmur_reference(&pattern[..len], MURMUR_SEED),
                "len {len}"
            );
        }
        // The empty input reduces to the finalizer applied to the seed.
        assert_ne!(murmur64a(b"", MURMUR_SEED), murmur64a(b"a", MURMUR_SEED));
    }

    #[test]
    fn pattern_stays_in_range() {
        let mut rng = Rng(7);
        for _ in 0..10_000 {
            let data = rng.bytes_upto(20);
            let (index, run) = pattern(&data);
            assert!(index < REGISTERS);
            assert!((1..=(Q as u8 + 1)).contains(&run));
        }
    }

    #[test]
    fn new_is_valid_dense_and_empty() {
        let hll = new();
        assert_eq!(hll.len(), 16 + 12288);
        assert_eq!(&hll[..4], b"HYLL");
        assert_eq!(validate(&hll), Ok(()));
        assert_eq!(count_one(&hll), 0);
        assert_eq!(count(&[]), Ok(0));
    }

    #[test]
    fn add_twice_changes_nothing() {
        let mut hll = new();
        assert_eq!(add(&mut hll, b"foo"), Ok(true));
        assert_eq!(hll[HDR_LEN - 1] & CARD_STALE_BIT, CARD_STALE_BIT);
        let snapshot = hll.clone();
        assert_eq!(add(&mut hll, b"foo"), Ok(false));
        assert_eq!(hll, snapshot);
    }

    #[test]
    fn small_counts_are_nearly_exact() {
        let mut hll = new();
        for n in 1..=100usize {
            add(&mut hll, &element(n)).unwrap();
            let got = count_one(&hll) as i64;
            assert!((got - n as i64).abs() <= 1, "n={n} got={got}");
        }
    }

    #[test]
    fn cardinality_error_within_two_percent() {
        for n in [1_000usize, 10_000, 100_000, 1_000_000] {
            let got = count_one(&filled(0..n)) as f64;
            let err = (got - n as f64).abs() / n as f64;
            println!("n={n} estimate={got} error={:.3}%", err * 100.0);
            assert!(err < 0.02, "n={n} got={got}");
        }
    }

    #[test]
    fn merge_equals_adding_everything() {
        let a = filled(0..6_000);
        let b = filled(4_000..10_000);
        let all = filled(0..10_000);
        let mut merged = new();
        merge(&mut merged, &[&a, &b]).unwrap();
        assert_eq!(count_one(&merged), count_one(&all));
        assert_eq!(registers(&merged), registers(&all));
        // Merging into a non-empty destination keeps what it had.
        let mut dst = a.clone();
        merge(&mut dst, &[&b]).unwrap();
        assert_eq!(registers(&dst), registers(&all));
    }

    #[test]
    fn count_of_several_equals_count_of_merge() {
        let a = filled(0..3_000);
        let b = filled(2_000..9_000);
        let c = filled(8_000..12_000);
        let mut merged = new();
        merge(&mut merged, &[&a, &b, &c]).unwrap();
        assert_eq!(count(&[&a, &b, &c]), Ok(count_one(&merged)));
    }

    #[test]
    fn merge_failure_leaves_dst_untouched() {
        let mut dst = filled(0..10);
        let before = dst.clone();
        assert_eq!(merge(&mut dst, &[b"nope"]), Err(Invalid));
        assert_eq!(dst, before);
    }

    #[test]
    fn dense_registers_round_trip() {
        let mut payload = vec![0u8; DENSE_LEN - HDR_LEN];
        let mut rng = Rng(99);
        let values: Vec<u8> = (0..REGISTERS)
            .map(|_| (rng.next() as u8) & REG_MAX)
            .collect();
        for (i, &v) in values.iter().enumerate() {
            dense_set(&mut payload, i, v);
        }
        for (i, &v) in values.iter().enumerate() {
            assert_eq!(dense_get(&payload, i), v, "register {i}");
        }
    }

    #[test]
    fn sparse_all_zero_validates_counts_zero_and_adds_like_dense() {
        // XZERO covering 16384 registers: length - 1 = 0x3FFF.
        let sparse = sparse_with(&[0x7F, 0xFF]);
        assert_eq!(validate(&sparse), Ok(()));
        assert_eq!(count_one(&sparse), 0);

        let mut from_sparse = sparse.clone();
        let mut from_dense = new();
        assert_eq!(add(&mut from_sparse, b"x"), Ok(true));
        assert_eq!(add(&mut from_dense, b"x"), Ok(true));
        assert_eq!(from_sparse, from_dense);
    }

    #[test]
    fn sparse_vals_expand_to_registers() {
        // ZERO x5, VAL(3) x2, XZERO x16377.
        let s = sparse_with(&[0x04, 0x89, 0x7F, 0xF8]);
        let regs = registers(&s).unwrap();
        assert_eq!(&regs[..8], &[0, 0, 0, 0, 0, 3, 3, 0]);
        assert!(regs[7..].iter().all(|&r| r == 0));

        // VAL(32) x4 is the largest value the format can hold.
        let s = sparse_with(&[0xFF, 0x7F, 0xFB]);
        let regs = registers(&s).unwrap();
        assert_eq!(&regs[..5], &[32, 32, 32, 32, 0]);

        assert_eq!(count_one(&s), count_one(&pack_dense(&regs)));
    }

    #[test]
    fn sparse_rejects_bad_coverage() {
        // Too short, too long, truncated XZERO, empty payload.
        assert_eq!(validate(&sparse_with(&[0x7F, 0xFE])), Err(Invalid));
        assert_eq!(validate(&sparse_with(&[0x7F, 0xFF, 0x00])), Err(Invalid));
        assert_eq!(validate(&sparse_with(&[0x7F])), Err(Invalid));
        assert_eq!(validate(&sparse_with(&[])), Err(Invalid));
    }

    #[test]
    fn rejects_bad_headers() {
        assert_eq!(validate(b""), Err(Invalid));
        assert_eq!(validate(b"HYLL"), Err(Invalid));
        let mut bad_magic = new();
        bad_magic[0] = b'X';
        assert_eq!(validate(&bad_magic), Err(Invalid));
        let mut bad_enc = new();
        bad_enc[4] = 2;
        assert_eq!(validate(&bad_enc), Err(Invalid));
        let mut short = new();
        short.pop();
        assert_eq!(validate(&short), Err(Invalid));
        let mut long = new();
        long.push(0);
        assert_eq!(validate(&long), Err(Invalid));
    }

    /// A random valid-looking sparse payload (opcodes cover all registers).
    fn random_sparse_ops(rng: &mut Rng) -> Vec<u8> {
        let mut ops = Vec::new();
        let mut left = REGISTERS;
        while left > 0 {
            match rng.below(3) {
                0 => {
                    let len = (1 + rng.below(64)).min(left);
                    ops.push((len - 1) as u8);
                    left -= len;
                }
                1 => {
                    let len = (1 + rng.below(REGISTERS)).min(left);
                    ops.push(SPARSE_XZERO_FLAG | ((len - 1) >> 8) as u8);
                    ops.push((len - 1) as u8);
                    left -= len;
                }
                _ => {
                    let len = (1 + rng.below(4)).min(left);
                    let val = rng.below(32) as u8;
                    ops.push(SPARSE_VAL_FLAG | (val << 2) | (len - 1) as u8);
                    left -= len;
                }
            }
        }
        ops
    }

    #[test]
    fn fuzz_never_panics() {
        let mut rng = Rng(0x1234_5678_9abc_def1);
        let good = filled(0..50);
        for round in 0..20_000 {
            let mut s = match rng.below(5) {
                0 => rng.bytes_upto(40),
                1 => {
                    let mut s = new();
                    s.truncate(HDR_LEN);
                    s[4] = rng.below(4) as u8;
                    s.extend(rng.bytes_upto(300));
                    s
                }
                2 => {
                    let mut s = new();
                    for b in &mut s[HDR_LEN..] {
                        *b = rng.next() as u8;
                    }
                    s.truncate(DENSE_LEN - rng.below(3) + rng.below(3));
                    s
                }
                3 => sparse_with(&random_sparse_ops(&mut rng)),
                _ => {
                    let mut ops = random_sparse_ops(&mut rng);
                    let cut = rng.below(ops.len() + 1);
                    ops.truncate(cut);
                    ops.extend(rng.bytes_upto(4));
                    sparse_with(&ops)
                }
            };
            let valid = validate(&s).is_ok();
            assert_eq!(count(&[&s]).is_ok(), valid, "round {round}");
            assert_eq!(count(&[&good, &s]).is_ok(), valid);
            let mut dst = good.clone();
            assert_eq!(merge(&mut dst, &[&s]).is_ok(), valid);
            let mut copy = s.clone();
            assert_eq!(add(&mut copy, b"e").is_ok(), valid);
            if valid {
                assert_eq!(validate(&copy), Ok(()));
                assert_eq!(validate(&dst), Ok(()));
            }
            // Also treat it as the destination of a merge.
            let _ = merge(&mut s, &[&good]);
        }
    }
}
