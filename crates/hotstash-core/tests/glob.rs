//! Adversarial tests for the glob matcher: differential against a naive
//! recursive reference, plus pathological inputs.

use hotstash_core::glob::glob_match;
use hotstash_core::testutil::Rng;
use std::time::{Duration, Instant};

/// Naive recursive matcher following Redis `stringmatchlen` for the class
/// grammar. One deliberate difference from the C code, shared with the
/// implementation under test and its own unit test: `*` matches the empty
/// text (the C code returns 0 for any non-empty pattern against "").
fn reference(p: &[u8], t: &[u8]) -> bool {
    let Some(&pc) = p.first() else {
        return t.is_empty();
    };
    if pc == b'*' {
        return reference(&p[1..], t) || (!t.is_empty() && reference(p, &t[1..]));
    }
    let Some(&c) = t.first() else {
        return false;
    };
    match pc {
        b'?' => reference(&p[1..], &t[1..]),
        b'[' => {
            let (ok, used) = class(&p[1..], c);
            ok && reference(&p[1 + used..], &t[1..])
        }
        b'\\' if p.len() >= 2 => p[1] == c && reference(&p[2..], &t[1..]),
        _ => pc == c && reference(&p[1..], &t[1..]),
    }
}

/// `pat` starts right after '['. Returns (class matches c, bytes consumed).
fn class(pat: &[u8], c: u8) -> (bool, usize) {
    let mut i = 0;
    let negate = pat.first() == Some(&b'^');
    if negate {
        i = 1;
    }
    let mut matched = false;
    loop {
        let rem = pat.len() - i;
        if rem == 0 {
            break;
        }
        if pat[i] == b'\\' && rem >= 2 {
            matched |= pat[i + 1] == c;
            i += 2;
        } else if pat[i] == b']' {
            i += 1;
            break;
        } else if rem >= 3 && pat[i + 1] == b'-' {
            let (mut lo, mut hi) = (pat[i], pat[i + 2]);
            if lo > hi {
                std::mem::swap(&mut lo, &mut hi);
            }
            matched |= (lo..=hi).contains(&c);
            i += 3;
        } else {
            matched |= pat[i] == c;
            i += 1;
        }
    }
    (matched != negate, i)
}

fn strings_up_to(alphabet: &[u8], max_len: usize) -> Vec<Vec<u8>> {
    let mut all = vec![Vec::new()];
    let mut layer = vec![Vec::new()];
    for _ in 0..max_len {
        let mut next = Vec::new();
        for s in &layer {
            for &ch in alphabet {
                let mut n = s.clone();
                n.push(ch);
                next.push(n);
            }
        }
        all.extend(next.iter().cloned());
        layer = next;
    }
    all
}

fn show(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn check(p: &[u8], t: &[u8]) {
    assert_eq!(
        glob_match(p, t),
        reference(p, t),
        "pattern {:?} text {:?}",
        show(p),
        show(t)
    );
}

#[test]
fn exhaustive_short_patterns_against_reference() {
    let patterns = strings_up_to(b"ab*?[]^-\\", 4);
    let texts = strings_up_to(b"ab-]\\", 4);
    for p in &patterns {
        for t in &texts {
            check(p, t);
        }
    }
}

#[test]
fn exhaustive_length_five_patterns_against_reference() {
    let all = strings_up_to(b"ab*?[]^-\\", 5);
    let texts = strings_up_to(b"ab-]", 3);
    for p in all.iter().filter(|p| p.len() == 5) {
        for t in &texts {
            check(p, t);
        }
    }
}

#[test]
fn random_longer_patterns_against_reference() {
    const PAT: &[u8] = b"aabb*?[]^-\\abc";
    const TXT: &[u8] = b"aabbc-]\\^[";
    let mut rng = Rng::new(0x61);
    for _ in 0..300_000 {
        let plen = rng.below(11) as usize;
        let mut stars = 0;
        let p: Vec<u8> = (0..plen)
            .map(|_| {
                let ch = PAT[rng.below(PAT.len() as u64) as usize];
                if ch == b'*' {
                    stars += 1;
                    if stars > 3 {
                        return b'?';
                    }
                }
                ch
            })
            .collect();
        let tlen = rng.below(13) as usize;
        let t: Vec<u8> = (0..tlen)
            .map(|_| TXT[rng.below(TXT.len() as u64) as usize])
            .collect();
        check(&p, &t);
    }
}

#[test]
fn patterns_derived_from_the_text_match_often_and_agree() {
    // Mutate the text into a pattern so that true results are common.
    let mut rng = Rng::new(0x62);
    let mut trues = 0;
    for _ in 0..200_000 {
        let tlen = 1 + rng.below(14) as usize;
        let t: Vec<u8> = (0..tlen).map(|_| rng.next_u64() as u8).collect();
        let mut p = Vec::new();
        let mut stars = 0;
        let mut i = 0;
        while i < t.len() {
            match rng.below(8) {
                0 => p.push(b'?'),
                1 if stars < 3 => {
                    stars += 1;
                    p.push(b'*');
                    i += rng.below(3) as usize;
                }
                2 => {
                    p.push(b'[');
                    p.push(t[i]);
                    p.push(b']');
                }
                3 => {
                    p.extend_from_slice(b"[^");
                    p.push(t[i].wrapping_add(1));
                    p.push(b']');
                }
                4 => {
                    p.push(b'[');
                    p.push(t[i].saturating_sub(1));
                    p.push(b'-');
                    p.push(t[i].saturating_add(1));
                    p.push(b']');
                }
                5 => {
                    p.push(b'\\');
                    p.push(t[i]);
                }
                _ => {
                    // Literal; a metacharacter byte would change the meaning
                    // but the reference decides.
                    p.push(t[i]);
                }
            }
            i += 1;
        }
        if glob_match(&p, &t) {
            trues += 1;
        }
        check(&p, &t);
    }
    assert!(
        trues > 20_000,
        "only {trues} matching cases, generator is weak"
    );
}

#[test]
fn hand_checked_redis_semantics() {
    let m = |p: &str, t: &str| glob_match(p.as_bytes(), t.as_bytes());
    // Reversed range is the same range.
    assert!(m("[z-a]", "m"));
    assert!(m("[z-a]", "a") && m("[z-a]", "z"));
    assert!(!m("[z-a]", "A"));
    assert!(m("[^z-a]", "A"));
    // `]` first closes the class: "[]" is an empty class that matches nothing.
    assert!(!m("[]a]", "a"));
}

#[test]
fn empty_class_and_bracket_edge_cases_like_redis() {
    let m = |p: &str, t: &str| glob_match(p.as_bytes(), t.as_bytes());
    // In Redis the first `]` always terminates the class, so `[]a]` is the
    // empty class (matches nothing) followed by a literal "a]".
    assert!(!m("[]a]", "a"));
    assert!(!m("[]a]", "]"));
    assert!(!m("[]a]", "aa]"));
    // Negated empty class matches any single byte.
    assert!(m("[^]a]", "xa]"));
    assert!(!m("[^]a]", "a]"));
    // Unterminated class runs to the end of the pattern.
    assert!(m("[abc", "b"));
    assert!(!m("[abc", "d"));
    assert!(m("[^abc", "d"));
    assert!(!m("[", "a"));
    assert!(m("[^", "a"));
    assert!(!m("[abc", "bc"));
    // Trailing backslash is a literal backslash.
    assert!(m("a\\", "a\\"));
    assert!(!m("a\\", "a"));
    // Escapes inside classes, including an escaped `]` and `-`.
    assert!(m("[\\]]", "]"));
    assert!(m("[a\\-z]", "-"));
    assert!(!m("[a\\-z]", "b"));
    assert!(m("[\\^a]", "^"));
    assert!(m("[a\\", "\\"));
    // Range ending in `]` as in Redis (`a-]` is a range, class unterminated).
    assert!(m("[a-]", "_"));
    assert!(!m("[a-]", "b"));
    // `-` first or last is literal when it cannot form a range.
    assert!(m("[-a]", "-"));
    assert!(m("[a-", "-"));
    // Star combos.
    assert!(m("a*", "a"));
    assert!(m("*a", "ba"));
    assert!(!m("*a", "ab"));
    assert!(m("a*b*c", "aXbYc"));
    assert!(!m("a*b*c", "aXbY"));
    assert!(m("*?", "x"));
    assert!(!m("*?", ""));
    assert!(m("?*", "xyz"));
}

#[test]
fn star_matches_empty_text_as_the_unit_test_claims() {
    // Real Redis returns 0 for stringmatchlen("*", "") from the matcher
    // itself (callers special-case "*"); this implementation deliberately
    // does not. Pinned so a change is noticed.
    assert!(glob_match(b"*", b""));
    assert!(glob_match(b"***", b""));
    assert!(!glob_match(b"*a", b""));
    assert!(!glob_match(b"?", b""));
    assert!(glob_match(b"", b""));
    assert!(!glob_match(b"", b"a"));
}

#[test]
fn binary_data() {
    let mut rng = Rng::new(0x63);
    for _ in 0..50_000 {
        let t = rbytes(&mut rng, 20);
        // A text always matches itself when it has no metacharacters, and
        // always matches "*".
        assert!(glob_match(b"*", &t));
        let meta = t.iter().any(|b| b"*?[\\".contains(b));
        if !meta {
            assert!(glob_match(&t, &t));
            let mut longer = t.clone();
            longer.push(1);
            assert!(!glob_match(&t, &longer));
        }
        // Escaping every byte makes any text match itself.
        let escaped: Vec<u8> = t.iter().flat_map(|&b| [b'\\', b]).collect();
        assert!(glob_match(&escaped, &t), "{t:?}");
        // Question marks of the right length match.
        assert!(glob_match(&vec![b'?'; t.len()], &t));
        assert!(!glob_match(&vec![b'?'; t.len() + 1], &t));
        // Random binary against random binary pattern vs reference.
        let p = rbytes(&mut rng, 6);
        if p.iter().filter(|&&b| b == b'*').count() <= 2 {
            check(&p, &t);
        }
    }
    assert!(glob_match(b"a\0b", b"a\0b"));
    assert!(glob_match(b"a?b", b"a\0b"));
    assert!(glob_match(b"[\0-\x05]", b"\x03"));
    assert!(glob_match(b"[^\0]", b"\x01"));
    assert!(!glob_match(b"[^\0]", b"\0"));
    assert!(glob_match(b"[\xf0-\xff]", b"\xf5"));
}

fn assert_fast(label: &str, f: impl FnOnce() -> bool, expected: bool) {
    let start = Instant::now();
    let got = f();
    let el = start.elapsed();
    assert_eq!(got, expected, "{label}");
    assert!(el < Duration::from_secs(2), "{label} took {el:?}");
}

#[test]
fn pathological_star_patterns_finish_fast() {
    let text = vec![b'a'; 5_000];
    assert_fast(
        "a*a*...a*b vs a^5000",
        || glob_match(b"a*a*a*a*a*a*a*a*a*b", &text),
        false,
    );
    assert_fast(
        "a*a*...a* vs a^5000",
        || glob_match(b"a*a*a*a*a*a*a*a*a*a*", &text),
        true,
    );
    let mut text_b = text.clone();
    text_b.push(b'b');
    assert_fast(
        "a*a*...a*b vs a^5000 b",
        || glob_match(b"a*a*a*a*a*a*a*a*a*b", &text_b),
        true,
    );
    // Alternating stars and question marks.
    let p: Vec<u8> = b"*?".repeat(200);
    assert_fast("(*?)^200", || glob_match(&p, &text), true);
    let mut p2: Vec<u8> = b"*?".repeat(200);
    p2.push(b'b');
    assert_fast("(*?)^200 b", || glob_match(&p2, &text), false);
}

#[test]
fn many_consecutive_stars_and_long_patterns() {
    let stars = vec![b'*'; 100_000];
    assert_fast("stars", || glob_match(&stars, b"anything"), true);
    assert_fast("stars on empty", || glob_match(&stars, b""), true);
    let mut sb = stars.clone();
    sb.push(b'b');
    assert_fast(
        "stars then b",
        || glob_match(&sb, &vec![b'a'; 100_000]),
        false,
    );

    // 1000-byte patterns.
    let p1: Vec<u8> = b"a*".repeat(500);
    let t1 = vec![b'a'; 10_000];
    assert_fast("(a*)^500", || glob_match(&p1, &t1), true);
    let mut p2 = vec![b'a'; 999];
    p2.insert(0, b'*');
    p2.push(b'b');
    assert_fast(
        "*a^999 b vs a^5000",
        || glob_match(&p2, &vec![b'a'; 5_000]),
        false,
    );
    let p3: Vec<u8> = b"[a-c]?".repeat(500);
    assert_fast("classes", || glob_match(&p3, &vec![b'b'; 1000]), true);
    assert_fast("classes short", || glob_match(&p3, &vec![b'b'; 999]), false);
    // A very long unterminated class.
    let mut p4 = vec![b'['];
    p4.extend(std::iter::repeat_n(b'x', 1000));
    assert_fast("long class", || glob_match(&p4, b"x"), true);
    assert_fast("long class miss", || glob_match(&p4, b"y"), false);
}

#[test]
fn nested_stars_with_classes_do_not_blow_up() {
    // Exponential for a naive matcher: many stars each followed by a class.
    let mut p = Vec::new();
    for _ in 0..30 {
        p.extend_from_slice(b"*[ab]");
    }
    p.push(b'c');
    let t = vec![b'a'; 3_000];
    assert_fast("(*[ab])^30 c", || glob_match(&p, &t), false);
}

fn rbytes(rng: &mut Rng, below: u64) -> Vec<u8> {
    let n = rng.below(below) as usize;
    rng.bytes(n)
}
