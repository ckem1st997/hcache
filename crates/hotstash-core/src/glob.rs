//! Glob matching with Redis semantics (`*`, `?`, `[a-z]`, `[^x]`, `\` escape).
//!
//! The matcher is iterative and keeps a single backtrack point (the last
//! `*`), so the cost is O(pattern × text) at worst. A recursive matcher
//! goes exponential on patterns like `a*a*a*a*b`, which a client could use
//! to pin a core.

pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0, 0);
    // Position to resume from after the last `*`: (pattern index after the
    // star, text index the star currently extends to).
    let mut star: Option<(usize, usize)> = None;

    loop {
        if p < pattern.len() && pattern[p] == b'*' {
            p += 1;
            star = Some((p, t));
            continue;
        }
        if t == text.len() {
            // Only trailing stars may remain, and those were consumed above.
            return p == pattern.len();
        }
        let advanced = match_one(pattern, p, text[t]);
        match advanced {
            Some(next) => {
                p = next;
                t += 1;
            }
            None => match star {
                Some((sp, st)) => {
                    // Let the star swallow one more byte and retry.
                    p = sp;
                    t = st + 1;
                    star = Some((sp, st + 1));
                }
                None => return false,
            },
        }
    }
}

/// Match one text byte against the pattern element at `p` (which is not a
/// `*`). Returns the pattern index after that element on success.
fn match_one(pattern: &[u8], p: usize, c: u8) -> Option<usize> {
    let &pc = pattern.get(p)?;
    match pc {
        b'?' => Some(p + 1),
        b'[' => match_class(pattern, p, c),
        b'\\' if p + 1 < pattern.len() => (pattern[p + 1] == c).then_some(p + 2),
        _ => (pc == c).then_some(p + 1),
    }
}

/// `pattern[p]` is `[`. An unterminated class runs to the end of the pattern.
fn match_class(pattern: &[u8], p: usize, c: u8) -> Option<usize> {
    let mut i = p + 1;
    let negate = pattern.get(i) == Some(&b'^');
    if negate {
        i += 1;
    }
    let mut matched = false;
    loop {
        match pattern.get(i) {
            None => break,
            Some(b']') => {
                i += 1;
                break;
            }
            Some(b'\\') if i + 1 < pattern.len() => {
                matched |= pattern[i + 1] == c;
                i += 2;
            }
            Some(&lo) if i + 2 < pattern.len() && pattern[i + 1] == b'-' => {
                let hi = pattern[i + 2];
                let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
                matched |= lo <= c && c <= hi;
                i += 3;
            }
            Some(&x) => {
                matched |= x == c;
                i += 1;
            }
        }
    }
    (matched != negate).then_some(i)
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    fn m(p: &str, t: &str) -> bool {
        glob_match(p.as_bytes(), t.as_bytes())
    }

    #[test]
    fn basics() {
        assert!(m("*", ""));
        assert!(m("*", "abc"));
        assert!(m("", ""));
        assert!(!m("", "a"));
        assert!(m("a?c", "abc"));
        assert!(!m("a?c", "ac"));
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("h[a-c]llo", "hbllo"));
        assert!(m("h[c-a]llo", "hbllo"));
        assert!(m("a\\*b", "a*b"));
        assert!(!m("a\\*b", "axb"));
        assert!(m("user:*:name", "user:42:name"));
        assert!(!m("user:*:name", "user:42:nam"));
        assert!(m("***a***", "a"));
        assert!(m("[abc", "a"));
        assert!(m("a\\", "a\\"));
    }
}
