//! Latency histogram with logarithmic buckets: 16 per power of two, so a
//! reported quantile is at most about 6% above the true value.

const SUB: usize = 16;
const BUCKETS: usize = 61 * SUB;

#[derive(Clone)]
pub struct Hist {
    buckets: Vec<u64>,
    count: u64,
    max: u64,
}

impl Default for Hist {
    fn default() -> Self {
        Hist {
            buckets: vec![0; BUCKETS],
            count: 0,
            max: 0,
        }
    }
}

fn index(v: u64) -> usize {
    if v < SUB as u64 {
        return v as usize;
    }
    let msb = 63 - v.leading_zeros() as usize;
    (msb - 3) * SUB + ((v >> (msb - 4)) & 15) as usize
}

/// The largest value that falls into bucket `idx`.
fn upper(idx: usize) -> u64 {
    if idx < SUB {
        return idx as u64;
    }
    let msb = idx / SUB + 3;
    let low = ((SUB + idx % SUB) as u64) << (msb - 4);
    low + ((1u64 << (msb - 4)) - 1)
}

impl Hist {
    pub fn record_n(&mut self, value: u64, n: u64) {
        self.buckets[index(value)] += n;
        self.count += n;
        self.max = self.max.max(value);
    }

    pub fn merge(&mut self, other: &Hist) {
        for (a, b) in self.buckets.iter_mut().zip(&other.buckets) {
            *a += b;
        }
        self.count += other.count;
        self.max = self.max.max(other.max);
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    /// 0 when nothing was recorded.
    pub fn quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((self.count as f64 * q).ceil() as u64).clamp(1, self.count);
        let mut seen = 0;
        for (idx, n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= target {
                return upper(idx).min(self.max);
            }
        }
        self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_value_lands_in_a_bucket_that_contains_it() {
        let mut values: Vec<u64> = (0..2000).collect();
        for shift in 4..64 {
            for delta in [0u64, 1, 7] {
                values.push((1u64 << shift).wrapping_add(delta));
                values.push((1u64 << shift) - 1);
            }
        }
        values.push(u64::MAX);
        let mut last = 0;
        for &v in &values {
            let idx = index(v);
            assert!(idx < BUCKETS, "{v} -> {idx}");
            assert!(
                upper(idx) >= v,
                "{v} above its bucket's upper bound {}",
                upper(idx)
            );
            if idx > 0 {
                assert!(upper(idx - 1) < v, "{v} belongs in an earlier bucket");
            }
            // Relative error bound.
            assert!(upper(idx) - v <= v / 16 + 1, "{v} vs {}", upper(idx));
            last = last.max(idx);
        }
        assert_eq!(last, BUCKETS - 1);
    }

    #[test]
    fn quantiles_of_a_known_distribution() {
        let mut h = Hist::default();
        assert_eq!(h.quantile(0.99), 0);
        for v in 1..=1000u64 {
            h.record_n(v * 1000, 1);
        }
        let p50 = h.quantile(0.5);
        let p99 = h.quantile(0.99);
        assert!((500_000..=532_000).contains(&p50), "{p50}");
        assert!((990_000..=1_000_000).contains(&p99), "{p99}");
        assert_eq!(h.quantile(1.0), 1_000_000);
        assert_eq!(h.max(), 1_000_000);
    }
}
