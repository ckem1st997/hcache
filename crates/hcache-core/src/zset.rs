//! Sorted set: members ordered by score, then by member bytes.
//!
//! The order lives in a list of sorted chunks. Finding a position is a
//! binary search over the chunks and one inside a chunk; an insert or a
//! removal moves at most one chunk's worth of entries; a rank is the sum of
//! the lengths of the chunks in front, a few thousand additions even for
//! millions of members.

use crate::dict::{hash_bytes, Dict};
use std::cmp::Ordering;

type Entry = (f64, Box<[u8]>);

/// A chunk longer than this is split in two.
const MAX_CHUNK: usize = 512;

#[derive(Clone, Default)]
pub struct ZSet {
    scores: Dict<f64>,
    chunks: Vec<Vec<Entry>>,
}

fn order(a_score: f64, a: &[u8], b_score: f64, b: &[u8]) -> Ordering {
    // Scores are never NaN: `parse_score` and ZINCRBY refuse it.
    a_score
        .partial_cmp(&b_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.cmp(b))
}

impl ZSet {
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(hash_bytes(member), member).copied()
    }

    /// Chunk and offset of the first entry not ordered before the given one.
    fn locate(&self, score: f64, member: &[u8]) -> (usize, usize) {
        let before = |e: &Entry| order(e.0, &e.1, score, member) == Ordering::Less;
        let ci = self
            .chunks
            .partition_point(|c| c.last().is_some_and(before));
        let pos = self.chunks.get(ci).map_or(0, |c| c.partition_point(before));
        (ci, pos)
    }

    fn rank_at(&self, ci: usize, pos: usize) -> usize {
        self.chunks[..ci].iter().map(Vec::len).sum::<usize>() + pos
    }

    /// Set the member's score. Returns the score it had.
    pub fn insert(&mut self, member: &[u8], score: f64) -> Option<f64> {
        let old = self.scores.insert(hash_bytes(member), member, score);
        if let Some(old) = old {
            if old == score {
                return Some(old);
            }
            self.unlink(old, member);
        }
        let (ci, pos) = self.locate(score, member);
        if ci == self.chunks.len() {
            // Past the last entry: extend the last chunk.
            match self.chunks.last_mut() {
                Some(last) => last.push((score, member.into())),
                None => self.chunks.push(vec![(score, member.into())]),
            }
        } else {
            self.chunks[ci].insert(pos, (score, member.into()));
        }
        let ci = ci.min(self.chunks.len() - 1);
        if self.chunks[ci].len() > MAX_CHUNK {
            let tail = self.chunks[ci].split_off(MAX_CHUNK / 2);
            self.chunks.insert(ci + 1, tail);
        }
        old
    }

    fn unlink(&mut self, score: f64, member: &[u8]) {
        let (ci, pos) = self.locate(score, member);
        if let Some(chunk) = self.chunks.get_mut(ci) {
            if chunk.get(pos).is_some_and(|e| &*e.1 == member) {
                chunk.remove(pos);
                if chunk.is_empty() {
                    self.chunks.remove(ci);
                }
            }
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let score = self.scores.remove(hash_bytes(member), member)?;
        self.unlink(score, member);
        Some(score)
    }

    /// Position of the member, counting from the lowest score.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.score(member)?;
        let (ci, pos) = self.locate(score, member);
        Some(self.rank_at(ci, pos))
    }

    /// How many members have a score below `score`, or at most `score`
    /// when `inclusive`.
    pub fn count_below(&self, score: f64, inclusive: bool) -> usize {
        let before = |e: &Entry| e.0 < score || (inclusive && e.0 == score);
        let ci = self
            .chunks
            .partition_point(|c| c.last().is_some_and(before));
        let pos = self.chunks.get(ci).map_or(0, |c| c.partition_point(before));
        self.rank_at(ci, pos)
    }

    /// How many members sort before `member` as bytes, or up to and
    /// including it when `inclusive`. Meaningful when every score is the
    /// same, as the commands that compare members require.
    pub fn count_lex_below(&self, member: &[u8], inclusive: bool) -> usize {
        let before = |e: &Entry| &*e.1 < member || (inclusive && &*e.1 == member);
        let ci = self
            .chunks
            .partition_point(|c| c.last().is_some_and(before));
        let pos = self.chunks.get(ci).map_or(0, |c| c.partition_point(before));
        self.rank_at(ci, pos)
    }

    /// The members and their scores, in no order.
    pub fn dict(&self) -> &Dict<f64> {
        &self.scores
    }

    /// Entries with rank in `start..end`, lowest first.
    pub fn range(&self, start: usize, end: usize) -> impl Iterator<Item = (&[u8], f64)> {
        let mut skip = start;
        let mut first = 0;
        while first < self.chunks.len() && skip >= self.chunks[first].len() {
            skip -= self.chunks[first].len();
            first += 1;
        }
        self.chunks[first..]
            .iter()
            .flatten()
            .skip(skip)
            .take(end.saturating_sub(start))
            .map(|(score, member)| (&**member, *score))
    }

    /// Remove the entries with rank in `start..end`; returns them.
    pub fn remove_range(&mut self, start: usize, end: usize) -> Vec<(Vec<u8>, f64)> {
        let gone: Vec<_> = self
            .range(start, end)
            .map(|(member, score)| (member.to_vec(), score))
            .collect();
        for (member, _) in &gone {
            self.remove(member);
        }
        gone
    }

    /// Visit about `budget` members in hash order (ZSCAN).
    pub fn scan_chunk(&self, cursor: u64, budget: usize, f: impl FnMut(&[u8], &f64)) -> u64 {
        self.scores.scan_chunk(cursor, budget, f)
    }
}

/// A score as Redis prints it.
pub fn format_score(score: f64) -> Vec<u8> {
    if score == f64::INFINITY {
        b"inf".to_vec()
    } else if score == f64::NEG_INFINITY {
        b"-inf".to_vec()
    } else {
        format!("{score}").into_bytes()
    }
}

/// A float argument: any finite number, or an infinity. Never NaN.
pub fn parse_float(raw: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(raw).ok()?;
    let value = match text.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        _ => {
            let ok = text
                .bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+' | b'e' | b'E'));
            if !ok || text.is_empty() {
                return None;
            }
            // Out of range ("1e400") is an error, as with strtod.
            text.parse().ok().filter(|v: &f64| v.is_finite())?
        }
    };
    (!f64::is_nan(value)).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stays_sorted_through_inserts_updates_and_removals() {
        let mut z = ZSet::default();
        let mut model: Vec<(i64, Vec<u8>)> = Vec::new();
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for i in 0..20_000u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let member = format!("m{}", x % 3000).into_bytes();
            let score = (x >> 20) as i64 % 500;
            model.retain(|(_, m)| *m != member);
            if i % 5 == 4 {
                z.remove(&member);
            } else {
                z.insert(&member, score as f64);
                model.push((score, member));
            }
        }
        model.sort();
        assert_eq!(z.len(), model.len());
        let got: Vec<_> = z
            .range(0, usize::MAX)
            .map(|(m, s)| (s as i64, m.to_vec()))
            .collect();
        assert_eq!(got, model);
        for (i, (score, member)) in model.iter().enumerate().step_by(37) {
            assert_eq!(z.rank(member), Some(i));
            assert_eq!(z.score(member), Some(*score as f64));
        }
        for s in [-1i64, 0, 17, 250, 499, 500] {
            let below = model.iter().filter(|(v, _)| *v < s).count();
            let upto = model.iter().filter(|(v, _)| *v <= s).count();
            assert_eq!(z.count_below(s as f64, false), below);
            assert_eq!(z.count_below(s as f64, true), upto);
        }
        let mid: Vec<_> = z.range(100, 110).map(|(m, _)| m.to_vec()).collect();
        let want: Vec<_> = model[100..110].iter().map(|(_, m)| m.clone()).collect();
        assert_eq!(mid, want);
        let gone = z.remove_range(5, 900);
        assert_eq!(gone.len(), 895);
        assert_eq!(z.len(), model.len() - 895);
        assert_eq!(z.rank(&model[900].1), Some(5));
    }

    #[test]
    fn floats() {
        assert_eq!(parse_float(b"1.5"), Some(1.5));
        assert_eq!(parse_float(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_float(b"nan"), None);
        assert_eq!(parse_float(b" 1"), None);
        assert_eq!(parse_float(b""), None);
        assert_eq!(format_score(3.0), b"3");
        assert_eq!(format_score(f64::INFINITY), b"inf");
    }
}
