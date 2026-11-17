//! HyperLogLog in Valkey's own string format (PFADD / PFCOUNT / PFMERGE).
//!
//! The value is an ordinary string, so a string made by Valkey can be read
//! here and the other way round, byte for byte: a new sketch starts sparse
//! and turns dense where Valkey's does (a register past 32, or the string
//! past `hll-sparse-max-bytes`), adjacent sparse runs are joined as they
//! are written, and PFCOUNT keeps the cardinality in the header.
//!
//! Callers hand in strings a client fully controls (SET, then PFCOUNT), so
//! every access is bounds-checked: a damaged sketch gives [`Corrupt`],
//! never a panic.

/// The string is no HyperLogLog at all (header, encoding or dense length);
/// callers reply "WRONGTYPE Key is not a valid HyperLogLog string value.".
#[derive(Debug, PartialEq, Eq)]
pub struct NotHll;

/// A sparse sketch whose opcodes do not cover the registers; callers reply
/// "INVALIDOBJ Corrupted HLL object detected".
#[derive(Debug, PartialEq, Eq)]
pub struct Corrupt;

/// log2 of the register count.
const P: u32 = 14;
/// Bits of the hash left for the run length once the index is taken out.
const Q: u32 = 64 - P;
pub const REGISTERS: usize = 1 << P;
const INDEX_MASK: u64 = (REGISTERS as u64) - 1;
/// Bits per register in the dense encoding.
const REG_BITS: usize = 6;
const REG_MAX: u8 = (1 << REG_BITS) - 1;

const MAGIC: &[u8; 4] = b"HYLL";
const HDR_LEN: usize = 16;
const ENC_AT: usize = 4;
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
const SPARSE_KIND_MASK: u8 = 0b1100_0000;
const SPARSE_ZERO_LEN_MASK: u8 = 0b0011_1111;
const SPARSE_VAL_MAX_VALUE: u8 = 32;
const SPARSE_VAL_MAX_LEN: usize = 4;
const SPARSE_ZERO_MAX_LEN: usize = 64;
const SPARSE_XZERO_MAX_LEN: usize = 16384;
/// Valkey's `hll-sparse-max-bytes` default: past it a sketch turns dense.
const SPARSE_MAX_BYTES: usize = 3000;
/// Opcodes looked at, from the one before a change, to join equal runs.
const SPARSE_MERGE_SCAN: usize = 5;

const MURMUR_SEED: u64 = 0xadc8_3b19;
const MURMUR_M: u64 = 0xc6a4_a793_5bd1_e995;
const MURMUR_R: u32 = 47;

/// Ertl's limit constant, 1 / (2 ln 2).
const ALPHA_INF: f64 = 0.721_347_520_444_481_7;
/// Histogram slots: one per value a 6-bit register can hold.
const HISTO_LEN: usize = 1 << REG_BITS;

/// MurmurHash64A, as Valkey uses it for HyperLogLog.
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

/// `hllDenseSet`: raises the register, never lowers it.
fn dense_raise(payload: &mut [u8], reg: usize, value: u8) -> bool {
    if value <= dense_get(payload, reg) {
        return false;
    }
    dense_set(payload, reg, value);
    true
}

fn is_zero(op: u8) -> bool {
    op & SPARSE_KIND_MASK == 0
}

fn is_xzero(op: u8) -> bool {
    op & SPARSE_KIND_MASK == SPARSE_XZERO_FLAG
}

fn is_val(op: u8) -> bool {
    op & SPARSE_VAL_FLAG != 0
}

fn zero_len(op: u8) -> usize {
    usize::from(op & SPARSE_ZERO_LEN_MASK) + 1
}

fn xzero_len(op: u8, low: u8) -> usize {
    ((usize::from(op & SPARSE_ZERO_LEN_MASK) << 8) | usize::from(low)) + 1
}

fn val_value(op: u8) -> u8 {
    ((op >> 2) & 0b1_1111) + 1
}

fn val_len(op: u8) -> usize {
    usize::from(op & 0b11) + 1
}

fn val_op(value: u8, len: usize) -> u8 {
    SPARSE_VAL_FLAG | ((value - 1) << 2) | (len - 1) as u8
}

/// A run of `len` zero registers as one ZERO or XZERO opcode.
fn push_zeros(seq: &mut Vec<u8>, len: usize) {
    if len > SPARSE_ZERO_MAX_LEN {
        let l = len - 1;
        seq.extend([SPARSE_XZERO_FLAG | (l >> 8) as u8, l as u8]);
    } else {
        seq.push((len - 1) as u8);
    }
}

/// The byte at `i`. Valkey's strings end in a zero byte it may read, so an
/// XZERO cut short at the very end reads its second byte as zero.
fn byte_at(s: &[u8], i: usize) -> u8 {
    s.get(i).copied().unwrap_or(0)
}

/// Hands `run` each run of registers in a sparse payload (first register,
/// length, value), checking that together they cover every register once.
fn sparse_walk(payload: &[u8], mut run: impl FnMut(usize, usize, u8)) -> Result<(), Corrupt> {
    let (mut idx, mut p) = (0, 0);
    while let Some(&op) = payload.get(p) {
        let (len, value, oplen) = if is_zero(op) {
            (zero_len(op), 0, 1)
        } else if is_xzero(op) {
            (xzero_len(op, byte_at(payload, p + 1)), 0, 2)
        } else {
            (val_len(op), val_value(op), 1)
        };
        if idx + len > REGISTERS {
            return Err(Corrupt);
        }
        run(idx, len, value);
        idx += len;
        p += oplen;
    }
    if idx == REGISTERS {
        Ok(())
    } else {
        Err(Corrupt)
    }
}

fn encoding(hll: &[u8]) -> Option<u8> {
    hll.get(ENC_AT).copied()
}

/// `hllSparseToDense`: the header is kept (magic, cache and the unused
/// bytes), only the encoding changes. A dense sketch is left as it is.
fn sparse_to_dense(hll: &mut Vec<u8>) -> Result<(), Corrupt> {
    if encoding(hll) == Some(ENC_DENSE) {
        return Ok(());
    }
    let (header, payload) = hll.split_at_checked(HDR_LEN).ok_or(Corrupt)?;
    let mut dense = vec![0u8; DENSE_LEN];
    let (head, regs) = dense.split_at_mut(HDR_LEN);
    head.copy_from_slice(header);
    head[ENC_AT] = ENC_DENSE;
    sparse_walk(payload, |first, len, value| {
        if value != 0 {
            for reg in first..first + len {
                dense_set(regs, reg, value);
            }
        }
    })?;
    *hll = dense;
    Ok(())
}

/// `hllSparseSet`: raises register `index` of a sparse sketch to `count`,
/// rewriting the opcode that holds it, or turns the sketch dense when the
/// value or the new length does not fit. Whether the register changed.
fn sparse_set(hll: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, Corrupt> {
    if count > SPARSE_VAL_MAX_VALUE {
        return promote(hll, index, count);
    }
    // Step 1: find the opcode that covers `index`.
    let (mut p, mut first, mut span) = (HDR_LEN, 0, 0);
    let mut prev = None;
    while let Some(&op) = hll.get(p) {
        let oplen;
        (span, oplen) = if is_zero(op) {
            (zero_len(op), 1)
        } else if is_val(op) {
            (val_len(op), 1)
        } else {
            (xzero_len(op, byte_at(hll, p + 1)), 2)
        };
        if index < first + span {
            break;
        }
        prev = Some(p);
        p += oplen;
        first += span;
    }
    let Some(&op) = hll.get(p) else {
        return Err(Corrupt);
    };
    // Step 2: the cases that change one byte in place.
    if is_val(op) {
        if val_value(op) >= count {
            return Ok(false);
        }
        if span == 1 {
            hll[p] = val_op(count, 1);
            join_runs(hll, prev);
            return Ok(true);
        }
    }
    if is_zero(op) && span == 1 {
        hll[p] = val_op(count, 1);
        join_runs(hll, prev);
        return Ok(true);
    }
    // Otherwise the opcode splits in up to three: what comes before the
    // register, the register, what comes after.
    let last = first + span - 1;
    let mut seq = Vec::with_capacity(5);
    if is_val(op) {
        let current = val_value(op);
        if index != first {
            seq.push(val_op(current, index - first));
        }
        seq.push(val_op(count, 1));
        if index != last {
            seq.push(val_op(current, last - index));
        }
    } else {
        if index != first {
            push_zeros(&mut seq, index - first);
        }
        seq.push(val_op(count, 1));
        if index != last {
            push_zeros(&mut seq, last - index);
        }
    }
    // Step 3: put the new opcodes in place of the old one.
    let oldlen = if is_xzero(op) { 2 } else { 1 };
    let new_len = hll.len() + seq.len() - oldlen;
    if seq.len() > oldlen && new_len > SPARSE_MAX_BYTES {
        return promote(hll, index, count);
    }
    let cut = (p + oldlen).min(hll.len());
    hll.splice(p..cut, seq);
    // Only an XZERO cut short at the end loses a byte here, as in Valkey.
    hll.truncate(new_len);
    join_runs(hll, prev);
    Ok(true)
}

/// Step 4 of `hllSparseSet`: join equal VAL runs next to the change, then
/// mark the cached cardinality stale.
fn join_runs(hll: &mut Vec<u8>, prev: Option<usize>) {
    let mut p = prev.unwrap_or(HDR_LEN);
    let mut scan = SPARSE_MERGE_SCAN;
    while let Some(&op) = hll.get(p) {
        if scan == 0 {
            break;
        }
        scan -= 1;
        if is_xzero(op) {
            p += 2;
            continue;
        }
        if is_zero(op) {
            p += 1;
            continue;
        }
        if let Some(&next) = hll.get(p + 1).filter(|&&next| is_val(next)) {
            let len = val_len(op) + val_len(next);
            if val_value(op) == val_value(next) && len <= SPARSE_VAL_MAX_LEN {
                hll[p + 1] = val_op(val_value(op), len);
                hll.remove(p);
                // Try the joined run against the one after it too.
                continue;
            }
        }
        p += 1;
    }
    invalidate_cache(hll);
}

/// The sparse sketch turns dense, then takes the value.
fn promote(hll: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, Corrupt> {
    sparse_to_dense(hll)?;
    let regs = hll.get_mut(HDR_LEN..).ok_or(Corrupt)?;
    Ok(dense_raise(regs, index, count))
}

/// Raises one register, whatever the encoding.
fn set(hll: &mut Vec<u8>, index: usize, count: u8) -> Result<bool, Corrupt> {
    match encoding(hll) {
        Some(ENC_DENSE) => {
            let regs = hll.get_mut(HDR_LEN..).ok_or(Corrupt)?;
            Ok(dense_raise(regs, index, count))
        }
        Some(ENC_SPARSE) => sparse_set(hll, index, count),
        _ => Err(Corrupt),
    }
}

/// All register values of a sketch, whatever its encoding.
fn registers(hll: &[u8]) -> Result<Vec<u8>, Corrupt> {
    let mut regs = vec![0u8; REGISTERS];
    union_into(&mut regs, hll)?;
    Ok(regs)
}

/// Marks the cached cardinality stale.
pub fn invalidate_cache(hll: &mut [u8]) {
    if let Some(b) = hll.get_mut(HDR_LEN - 1) {
        *b |= CARD_STALE_BIT;
    }
}

/// A new, empty HyperLogLog: sparse, one XZERO over every register.
pub fn new() -> Vec<u8> {
    let mut hll = vec![0u8; HDR_LEN];
    hll[..MAGIC.len()].copy_from_slice(MAGIC);
    hll[ENC_AT] = ENC_SPARSE;
    let mut left = REGISTERS;
    while left > 0 {
        let run = left.min(SPARSE_XZERO_MAX_LEN);
        push_zeros(&mut hll, run);
        left -= run;
    }
    hll
}

/// `isHLLObjectOrReply`: the header, the encoding and, for a dense sketch,
/// the length. Sparse opcodes are only read when they are used.
pub fn check(hll: &[u8]) -> Result<(), NotHll> {
    if hll.len() < HDR_LEN || !hll.starts_with(MAGIC) {
        return Err(NotHll);
    }
    match encoding(hll) {
        Some(ENC_DENSE) if hll.len() == DENSE_LEN => Ok(()),
        Some(ENC_SPARSE) => Ok(()),
        _ => Err(NotHll),
    }
}

/// Whether a checked sketch is dense.
pub fn is_dense(hll: &[u8]) -> bool {
    encoding(hll) == Some(ENC_DENSE)
}

/// PFADD of one element to a checked sketch: whether a register changed.
/// A sparse sketch marks its cache stale itself; for a dense one that is
/// the caller's part, once per command, as in Valkey.
pub fn add(hll: &mut Vec<u8>, element: &[u8]) -> Result<bool, Corrupt> {
    let (index, count) = pattern(element);
    set(hll, index, count)
}

/// Estimate for a register table (Ertl's improved estimator, as in Valkey).
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

/// PFCOUNT of one checked sketch: the cached cardinality while it is
/// fresh, else a new estimate, which is written to the cache. The flag
/// says whether the string changed.
pub fn count_cached(hll: &mut [u8]) -> Result<(u64, bool), Corrupt> {
    let cache: [u8; 8] = hll
        .get(CARD_START..HDR_LEN)
        .and_then(|c| c.try_into().ok())
        .ok_or(Corrupt)?;
    if cache[7] & CARD_STALE_BIT == 0 {
        return Ok((u64::from_le_bytes(cache), false));
    }
    let card = estimate(&registers(hll)?);
    hll[CARD_START..HDR_LEN].copy_from_slice(&card.to_le_bytes());
    Ok((card, true))
}

/// `hllMerge`: raises `max` (one byte per register) to a checked sketch.
pub fn union_into(max: &mut [u8], hll: &[u8]) -> Result<(), Corrupt> {
    let payload = hll.get(HDR_LEN..).ok_or(Corrupt)?;
    if is_dense(hll) {
        for (reg, m) in max.iter_mut().enumerate().take(REGISTERS) {
            *m = (*m).max(dense_get(payload, reg));
        }
        return Ok(());
    }
    sparse_walk(payload, |first, len, value| {
        if let Some(run) = max.get_mut(first..first + len) {
            for m in run {
                *m = (*m).max(value);
            }
        }
    })
}

/// PFCOUNT of several keys: the estimate for registers `union_into` made.
pub fn count_union(max: &[u8]) -> u64 {
    estimate(max)
}

/// PFMERGE's write: `dst` (checked, and already part of `max`) takes the
/// registers in `max`. Dense when an input was, else register by register
/// in order, so the sparse opcodes come out as Valkey's do.
pub fn store_union(dst: &mut Vec<u8>, max: &[u8], dense: bool) -> Result<(), Corrupt> {
    if dense {
        sparse_to_dense(dst)?;
        let regs = dst.get_mut(HDR_LEN..).ok_or(Corrupt)?;
        for (reg, &value) in max.iter().enumerate().take(REGISTERS) {
            dense_set(regs, reg, value);
        }
    } else {
        for (reg, &value) in max.iter().enumerate().take(REGISTERS) {
            if value != 0 {
                set(dst, reg, value)?;
            }
        }
    }
    invalidate_cache(dst);
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
        estimate(&registers(hll).unwrap())
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// FNV-1a, to compare long strings with what Valkey wrote.
    fn fnv(data: &[u8]) -> u64 {
        data.iter().fold(0xcbf2_9ce4_8422_2325, |h, &b| {
            (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3)
        })
    }

    fn pfadd(hll: &mut Vec<u8>, items: &[&[u8]]) -> bool {
        let mut changed = false;
        for item in items {
            changed |= add(hll, item).unwrap();
        }
        if changed {
            invalidate_cache(hll);
        }
        changed
    }

    fn pfmerge(dst: &mut Vec<u8>, sources: &[&[u8]]) {
        let mut max = vec![0u8; REGISTERS];
        union_into(&mut max, dst).unwrap();
        let mut dense = is_dense(dst);
        for source in sources {
            union_into(&mut max, source).unwrap();
            dense |= is_dense(source);
        }
        store_union(dst, &max, dense).unwrap();
    }

    /// Straight transcription of Valkey's MurmurHash64A, with the tail
    /// switch written as a fall-through cascade.
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

    /// Strings Valkey 9.1.3 wrote for the same commands.
    #[test]
    fn writes_what_valkey_writes() {
        // PFADD e
        let mut e = new();
        assert!(!pfadd(&mut e, &[]));
        invalidate_cache(&mut e);
        assert_eq!(e, unhex("48594c4c0100000000000000000000807fff"));
        assert_eq!(check(&e), Ok(()));

        // PFADD s a b c
        let mut s = new();
        assert!(pfadd(&mut s, &[b"a", b"b", b"c"]));
        assert_eq!(
            s,
            unhex("48594c4c01000000000000000000008060f38050b1844bfb80425a")
        );

        // PFADD m e0 .. e49: joined runs and ZERO opcodes.
        let mut m = new();
        let items: Vec<Vec<u8>> = (0..50).map(|i| format!("e{i}").into_bytes()).collect();
        let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
        pfadd(&mut m, &refs);
        assert_eq!(
            m,
            unhex(concat!(
                "48594c4c01000000000000000000008040748041b99c048840e280420a8044528042da",
                "8441868440e18442a38442958440d38042638040a280404b8c406f8043868041478041",
                "638041618c01842188118041828440948844a7901184425f8c40e48440e180422a8441",
                "038442968c41d880411c88413c8803804078803f80409b8041518842f6803384407984",
                "419384408880406a840d80398013804152",
            ))
        );

        // PFADD big e0 .. e1999, one at a time: sparse, then dense.
        let mut big = new();
        for i in 0..2000 {
            pfadd(&mut big, &[format!("e{i}").as_bytes()]);
            let want = match i {
                300 => Some((715, 0x62c5_0f61_756e_1942)),
                600 => Some((1240, 0x1615_a9e4_5f8a_3b22)),
                900 => Some((1731, 0x1137_f921_65cc_1b7e)),
                _ => None,
            };
            if let Some((len, hash)) = want {
                assert_eq!((big.len(), fnv(&big)), (len, hash), "after e{i}");
            }
        }
        assert_eq!((big.len(), fnv(&big)), (DENSE_LEN, 0x1b55_76fa_1f6b_8092));
        // PFCOUNT writes the cache.
        assert_eq!(count_cached(&mut big), Ok((2000, true)));
        assert_eq!(fnv(&big), 0xdd4d_26f1_86b5_7d33);
        assert_eq!(count_cached(&mut big), Ok((2000, false)));

        let mut x = s.clone();
        assert_eq!(count_cached(&mut x), Ok((3, true)));
        assert_eq!(
            x,
            unhex("48594c4c01000000030000000000000060f38050b1844bfb80425a")
        );

        // PFMERGE dst s m, into a new key; and PFMERGE m s x in place.
        let want = unhex(concat!(
            "48594c4c01000000000000000000008040748041b99c048840e280420a8044528042da",
            "8441868440e18442a38442958440d38042638040a280404b8c406f8043868041478041",
            "638041618c01842188118040878040f98440948844a7901184425f8c40e48440e18042",
            "2a8441038442968c407184416580411c88413c8803804078803f80409b8041518842f6",
            "803384407984419384404b803b80406a840d80398013804152",
        ));
        let mut dst = new();
        pfmerge(&mut dst, &[&s, &m]);
        assert_eq!(dst, want);
        let mut m2 = m.clone();
        pfmerge(&mut m2, &[&s, &x]);
        assert_eq!(m2, want);

        // PFMERGE dst2 s big: a dense input makes the result dense.
        let mut dst2 = new();
        pfmerge(&mut dst2, &[&s, &big]);
        assert_eq!((dst2.len(), fnv(&dst2)), (DENSE_LEN, 0x5ca8_ff7d_a2bf_620a));
    }

    /// Damaged sparse strings behave as they do in Valkey 9.1.3.
    #[test]
    fn damaged_sketches_follow_valkey() {
        let header = unhex("48594c4c0100000000000000000000");
        let with = |ops: &[u8]| {
            let mut s = header.clone();
            s.push(0);
            s.extend_from_slice(ops);
            s
        };
        // One register short, with a fresh cache: PFCOUNT trusts the cache.
        let mut short = with(&[0x7f, 0xfe]);
        assert_eq!(count_cached(&mut short), Ok((0, false)));
        assert!(pfadd(&mut short, &[b"a"]));
        assert_eq!(short, unhex("48594c4c01000000000000000000008071a6844e56"));
        assert_eq!(count_cached(&mut short), Err(Corrupt));
        let mut max = vec![0u8; REGISTERS];
        assert_eq!(union_into(&mut max, &short), Err(Corrupt));

        // An XZERO cut short at the end reads its missing byte as zero.
        let mut cut = with(&[0x3f, 0x3f, 0x3f, 0x3e, 0x7f]);
        assert!(pfadd(&mut cut, &[b"a", b"b", b"c", b"d"]));
        assert_eq!(
            cut,
            unhex("48594c4c0100000000000000000000803f3f3f3e5b7c8044768050b1844bfb8042")
        );
        assert_eq!(count_cached(&mut cut), Err(Corrupt));

        // The cache is trusted on a dense sketch too.
        let mut dense = vec![0u8; DENSE_LEN];
        dense[..4].copy_from_slice(MAGIC);
        dense[CARD_START..HDR_LEN].copy_from_slice(&12345u64.to_le_bytes());
        assert_eq!(count_cached(&mut dense), Ok((12345, false)));
    }

    #[test]
    fn new_is_sparse_and_empty() {
        let hll = new();
        assert_eq!(hll.len(), HDR_LEN + 2);
        assert_eq!(check(&hll), Ok(()));
        assert!(!is_dense(&hll));
        assert_eq!(count_one(&hll), 0);
        assert_eq!(count_union(&[0; REGISTERS]), 0);
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
    fn sparse_and_dense_hold_the_same_registers() {
        // Sparse until it is not: every step agrees with a plain table.
        let mut hll = new();
        let mut table = vec![0u8; REGISTERS];
        let mut went_dense = false;
        for i in 0..5_000 {
            let e = element(i);
            let (index, count) = pattern(&e);
            let raised = count > table[index];
            table[index] = table[index].max(count);
            assert_eq!(add(&mut hll, &e), Ok(raised), "element {i}");
            went_dense |= is_dense(&hll);
            if i % 97 == 0 {
                assert_eq!(registers(&hll).unwrap(), table, "element {i}");
                assert_eq!(check(&hll), Ok(()));
                assert!(is_dense(&hll) || hll.len() <= SPARSE_MAX_BYTES);
            }
        }
        assert!(went_dense);
        assert_eq!(registers(&hll).unwrap(), table);
    }

    #[test]
    fn a_register_past_32_turns_the_sketch_dense() {
        let mut hll = new();
        assert_eq!(set(&mut hll, 5, 32), Ok(true));
        assert!(!is_dense(&hll));
        assert_eq!(set(&mut hll, 9, 33), Ok(true));
        assert!(is_dense(&hll));
        let regs = registers(&hll).unwrap();
        assert_eq!((regs[5], regs[9]), (32, 33));
    }

    #[test]
    fn merge_equals_adding_everything() {
        let a = filled(0..6_000);
        let b = filled(4_000..10_000);
        let all = filled(0..10_000);
        let mut merged = new();
        pfmerge(&mut merged, &[&a, &b]);
        assert_eq!(registers(&merged), registers(&all));
        // Merging into a non-empty destination keeps what it had.
        let mut dst = a.clone();
        pfmerge(&mut dst, &[&b]);
        assert_eq!(registers(&dst), registers(&all));
        // Small sparse inputs give a sparse result.
        let (c, d) = (filled(0..40), filled(30..80));
        let mut dst = new();
        pfmerge(&mut dst, &[&c, &d]);
        assert!(!is_dense(&dst));
        assert_eq!(registers(&dst), registers(&filled(0..80)));
    }

    #[test]
    fn count_of_several_equals_count_of_merge() {
        let a = filled(0..3_000);
        let b = filled(2_000..9_000);
        let c = filled(8_000..12_000);
        let mut merged = new();
        pfmerge(&mut merged, &[&a, &b, &c]);
        let mut max = vec![0u8; REGISTERS];
        for s in [&a, &b, &c] {
            union_into(&mut max, s).unwrap();
        }
        assert_eq!(count_union(&max), count_one(&merged));
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

        // Turning dense keeps the header but for the encoding.
        let mut s = s;
        s[5..HDR_LEN].copy_from_slice(b"abc12345678");
        let mut dense = s.clone();
        sparse_to_dense(&mut dense).unwrap();
        assert_eq!(registers(&dense).unwrap(), regs);
        assert_eq!(dense[ENC_AT], ENC_DENSE);
        assert_eq!(dense[5..HDR_LEN], s[5..HDR_LEN]);
    }

    #[test]
    fn sparse_rejects_bad_coverage() {
        // Too short, too long, empty payload.
        let mut max = vec![0u8; REGISTERS];
        for ops in [&[0x7F, 0xFE][..], &[0x7F, 0xFF, 0x00], &[]] {
            assert_eq!(union_into(&mut max, &sparse_with(ops)), Err(Corrupt));
        }
    }

    #[test]
    fn rejects_bad_headers() {
        assert_eq!(check(b""), Err(NotHll));
        assert_eq!(check(b"HYLL"), Err(NotHll));
        let mut bad_magic = new();
        bad_magic[0] = b'X';
        assert_eq!(check(&bad_magic), Err(NotHll));
        let mut bad_enc = new();
        bad_enc[ENC_AT] = 2;
        assert_eq!(check(&bad_enc), Err(NotHll));
        let mut dense = new();
        sparse_to_dense(&mut dense).unwrap();
        assert_eq!(check(&dense), Ok(()));
        let mut short = dense.clone();
        short.pop();
        assert_eq!(check(&short), Err(NotHll));
        let mut long = dense;
        long.push(0);
        assert_eq!(check(&long), Err(NotHll));
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
            let s = match rng.below(5) {
                0 => rng.bytes_upto(40),
                1 => {
                    let mut s = new();
                    s.truncate(HDR_LEN);
                    s[ENC_AT] = rng.below(4) as u8;
                    s.extend(rng.bytes_upto(300));
                    s
                }
                2 => {
                    let mut s = vec![0u8; DENSE_LEN];
                    s[..4].copy_from_slice(MAGIC);
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
            if check(&s).is_err() {
                continue;
            }
            let mut max = vec![0u8; REGISTERS];
            let whole = union_into(&mut max, &s).is_ok();
            let mut copy = s.clone();
            invalidate_cache(&mut copy);
            assert_eq!(count_cached(&mut copy).is_ok(), whole, "round {round}");
            let mut copy = s.clone();
            for i in 0..4 {
                if add(&mut copy, &element(i)).is_err() {
                    break;
                }
            }
            if whole {
                let mut dst = good.clone();
                let mut max = vec![0u8; REGISTERS];
                union_into(&mut max, &dst).unwrap();
                union_into(&mut max, &s).unwrap();
                let dense = is_dense(&s);
                store_union(&mut dst, &max, dense).unwrap();
                assert_eq!(check(&dst), Ok(()));
                assert_eq!(registers(&dst).unwrap(), max, "round {round}");
            }
        }
    }
}
