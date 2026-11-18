//! SORT and SORT_RO: what the command asks for and how its elements are
//! ordered. Reading the keys that `BY` and `GET` patterns name is left to
//! the caller, which knows how to reach them.

use crate::cmd::{syntax, Bytes, NOT_INT};
use crate::resp::{parse_i64, Reply};
use crate::zset::parse_float;
use std::cmp::Ordering;

const BAD_SCORE: &str = "ERR One or more scores can't be converted into double";

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Spec {
    /// `BY pattern`: order by the values of other keys. A pattern without
    /// `*` asks for no sorting at all.
    pub by: Option<Bytes>,
    /// Offset and count; no count means everything after the offset.
    pub limit: Option<(usize, Option<usize>)>,
    /// `GET pattern`, in order; `#` stands for the element itself.
    pub get: Vec<Bytes>,
    pub desc: bool,
    pub alpha: bool,
    pub store: Option<Bytes>,
}

fn is(arg: &[u8], word: &str) -> bool {
    arg.eq_ignore_ascii_case(word.as_bytes())
}

fn has_star(pattern: &[u8]) -> bool {
    pattern.contains(&b'*')
}

impl Spec {
    /// The options after the key. SORT_RO takes no STORE.
    pub fn parse(args: &[Bytes], read_only: bool) -> Result<Spec, Reply> {
        let mut spec = Spec::default();
        let mut i = 0;
        while let Some(word) = args.get(i) {
            let left = args.len() - i - 1;
            if is(word, "ASC") {
                spec.desc = false;
            } else if is(word, "DESC") {
                spec.desc = true;
            } else if is(word, "ALPHA") {
                spec.alpha = true;
            } else if is(word, "LIMIT") && left >= 2 {
                let int = |raw: &Bytes| parse_i64(raw).ok_or_else(|| Reply::err(NOT_INT));
                let offset = usize::try_from(int(&args[i + 1])?).unwrap_or(0);
                // A negative count means no limit.
                spec.limit = Some((offset, usize::try_from(int(&args[i + 2])?).ok()));
                i += 2;
            } else if is(word, "STORE") && left >= 1 && !read_only {
                spec.store = Some(args[i + 1].clone());
                i += 1;
            } else if is(word, "BY") && left >= 1 {
                spec.by = Some(args[i + 1].clone());
                i += 1;
            } else if is(word, "GET") && left >= 1 {
                spec.get.push(args[i + 1].clone());
                i += 1;
            } else {
                return Err(syntax());
            }
            i += 1;
        }
        Ok(spec)
    }

    /// `BY` with a pattern that names no key: keep the order of the source.
    fn nosort(&self) -> bool {
        self.by.as_ref().is_some_and(|by| !has_star(by))
    }

    /// Whether the command reads keys other than the one it sorts. Such a
    /// command needs the node to itself: the keys are known only once the
    /// elements are.
    pub fn external(&self) -> bool {
        self.by.as_ref().is_some_and(|by| has_star(by)) || self.get.iter().any(|g| g != b"#")
    }

    /// Order `elements` and cut them to the LIMIT. `ranked` says the source
    /// is a sorted set, whose own order `BY nosort` keeps (and DESC turns
    /// around). `lookup` reads what a pattern names for an element.
    pub fn arrange(
        &self,
        mut elements: Vec<Bytes>,
        ranked: bool,
        mut lookup: impl FnMut(&[u8], &[u8]) -> Option<Bytes>,
    ) -> Result<Vec<Bytes>, Reply> {
        if self.nosort() {
            if ranked && self.desc {
                elements.reverse();
            }
        } else {
            let mut items = Vec::with_capacity(elements.len());
            for element in elements {
                let weight = match (&self.by, self.alpha) {
                    (None, true) => Weight::Text(None),
                    (Some(by), true) => Weight::Text(lookup(by, &element)),
                    (None, false) => Weight::Num(number(&element)?),
                    // A missing weight counts as zero.
                    (Some(by), false) => match lookup(by, &element) {
                        Some(raw) => Weight::Num(number(&raw)?),
                        None => Weight::Num(0.0),
                    },
                };
                items.push((weight, element));
            }
            // Equal weights fall back on the elements, so the order never
            // depends on where the elements happened to be.
            items.sort_by(|a, b| {
                let order = a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1));
                if self.desc {
                    order.reverse()
                } else {
                    order
                }
            });
            elements = items.into_iter().map(|(_, element)| element).collect();
        }
        let (offset, count) = self.limit.unwrap_or((0, None));
        Ok(elements
            .into_iter()
            .skip(offset)
            .take(count.unwrap_or(usize::MAX))
            .collect())
    }

    /// What the command answers with or stores for the arranged elements:
    /// the elements, or what each GET pattern names for them.
    pub fn project(
        &self,
        elements: Vec<Bytes>,
        mut lookup: impl FnMut(&[u8], &[u8]) -> Option<Bytes>,
    ) -> Result<Vec<Option<Bytes>>, Reply> {
        if self.get.is_empty() {
            return Ok(elements.into_iter().map(Some).collect());
        }
        // Every GET repeats every element: a short command could ask
        // for more than the machine has.
        let mut room = MAX_PROJECTED_BYTES;
        let mut out = Vec::new();
        for element in &elements {
            for pattern in &self.get {
                let item = if pattern == b"#" {
                    Some(element.clone())
                } else {
                    lookup(pattern, element)
                };
                let cost = ITEM_OVERHEAD + item.as_ref().map_or(0, Vec::len);
                room = room.checked_sub(cost).ok_or_else(|| {
                    Reply::err("ERR the result of SORT with these GET patterns is too large")
                })?;
                out.push(item);
            }
        }
        Ok(out)
    }
}

/// SORT with GET builds at most this much, counted in bytes.
const MAX_PROJECTED_BYTES: usize = 256 << 20;
/// What one item of the result costs besides its bytes.
const ITEM_OVERHEAD: usize = std::mem::size_of::<Option<Bytes>>();

enum Weight {
    Num(f64),
    /// A missing value sorts before every other.
    Text(Option<Bytes>),
}

impl Weight {
    fn cmp(&self, other: &Weight) -> Ordering {
        match (self, other) {
            (Weight::Num(a), Weight::Num(b)) => a.total_cmp(b),
            (Weight::Text(a), Weight::Text(b)) => a.cmp(b),
            // One command gives every element the same kind of weight.
            _ => Ordering::Equal,
        }
    }
}

fn number(raw: &[u8]) -> Result<f64, Reply> {
    parse_float(raw).ok_or_else(|| Reply::err(BAD_SCORE))
}

/// The key a BY or GET pattern names for `element`: the pattern with its
/// first `*` replaced. With `->field` after the star, the value is that
/// field of the hash at the key. `None` for a pattern without a star.
pub fn lookup_key(pattern: &[u8], element: &[u8]) -> Option<(Bytes, Option<Bytes>)> {
    let star = pattern.iter().position(|&b| b == b'*')?;
    let (head, tail) = (&pattern[..star], &pattern[star + 1..]);
    let arrow = tail.windows(2).position(|pair| pair == b"->");
    let (tail, field) = match arrow {
        Some(at) if at + 2 < tail.len() => (&tail[..at], Some(tail[at + 2..].to_vec())),
        _ => (tail, None),
    };
    let mut key = Vec::with_capacity(head.len() + element.len() + tail.len());
    key.extend_from_slice(head);
    key.extend_from_slice(element);
    key.extend_from_slice(tail);
    Some((key, field))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(list: &[&str]) -> Vec<Bytes> {
        list.iter().map(|w| w.as_bytes().to_vec()).collect()
    }

    #[test]
    fn patterns_name_keys_and_fields() {
        assert_eq!(lookup_key(b"w_*", b"7"), Some((b"w_7".to_vec(), None)));
        assert_eq!(
            lookup_key(b"obj:*->name", b"7"),
            Some((b"obj:7".to_vec(), Some(b"name".to_vec())))
        );
        // Nothing after the arrow: the arrow is part of the key.
        assert_eq!(lookup_key(b"k*->", b"x"), Some((b"kx->".to_vec(), None)));
        assert_eq!(lookup_key(b"nosort", b"x"), None);
    }

    #[test]
    fn numbers_then_limit() {
        let spec = Spec::parse(&words(&["LIMIT", "1", "2", "DESC"]), false).unwrap();
        let out = spec
            .arrange(words(&["3", "10", "2", "1"]), false, |_, _| None)
            .unwrap();
        assert_eq!(out, words(&["3", "2"]));
    }

    #[test]
    fn text_needs_alpha() {
        let spec = Spec::default();
        assert!(spec
            .arrange(words(&["b", "a"]), false, |_, _| None)
            .is_err());
        let alpha = Spec::parse(&words(&["ALPHA"]), false).unwrap();
        let out = alpha
            .arrange(words(&["b", "a"]), false, |_, _| None)
            .unwrap();
        assert_eq!(out, words(&["a", "b"]));
    }

    #[test]
    fn store_is_refused_read_only() {
        assert!(Spec::parse(&words(&["STORE", "dst"]), true).is_err());
        assert!(Spec::parse(&words(&["LIMIT", "0"]), false).is_err());
    }
}
