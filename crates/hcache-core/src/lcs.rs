//! LCS: the longest common subsequence of two strings.

use crate::ops::LcsOpts;
use crate::resp::Reply;

/// The table has (len a + 1) * (len b + 1) cells of four bytes; more than
/// this many cells are refused.
const MAX_CELLS: usize = 1 << 24;

struct Match {
    a: (usize, usize),
    b: (usize, usize),
}

pub(crate) fn reply(a: &[u8], b: &[u8], opts: LcsOpts) -> Reply {
    let (rows, cols) = (a.len() + 1, b.len() + 1);
    if rows.checked_mul(cols).is_none_or(|cells| cells > MAX_CELLS) || a.len() > u32::MAX as usize {
        return Reply::err("ERR String too long for LCS");
    }
    // table[i * cols + j]: the length for the first i bytes of `a` and
    // the first j bytes of `b`.
    let mut table = vec![0u32; rows * cols];
    for i in 1..rows {
        for j in 1..cols {
            table[i * cols + j] = if a[i - 1] == b[j - 1] {
                table[(i - 1) * cols + j - 1] + 1
            } else {
                table[(i - 1) * cols + j].max(table[i * cols + j - 1])
            };
        }
    }
    let len = table[rows * cols - 1] as usize;
    if opts.len {
        return Reply::Int(len as i64);
    }
    // Walk back from the end; matches come out last first, as in Redis.
    let mut text = vec![0u8; len];
    let mut at = len;
    let mut matches = Vec::new();
    let mut open: Option<Match> = None;
    let mut close = |open: &mut Option<Match>| {
        if let Some(m) = open.take() {
            if m.a.1 - m.a.0 + 1 >= opts.min_match_len {
                matches.push(m);
            }
        }
    };
    let (mut i, mut j) = (a.len(), b.len());
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            at -= 1;
            text[at] = a[i - 1];
            match &mut open {
                Some(m) if m.a.0 == i && m.b.0 == j => {
                    m.a.0 -= 1;
                    m.b.0 -= 1;
                }
                _ => {
                    close(&mut open);
                    open = Some(Match {
                        a: (i - 1, i - 1),
                        b: (j - 1, j - 1),
                    });
                }
            }
            i -= 1;
            j -= 1;
        } else {
            if table[(i - 1) * cols + j] > table[i * cols + j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
            close(&mut open);
        }
    }
    close(&mut open);
    if !opts.idx {
        return Reply::Bulk(text);
    }
    let pair = |(start, end): (usize, usize)| {
        Reply::Array(vec![Reply::Int(start as i64), Reply::Int(end as i64)])
    };
    let matches = matches
        .into_iter()
        .map(|m| {
            let mut item = vec![pair(m.a), pair(m.b)];
            if opts.with_match_len {
                item.push(Reply::Int((m.a.1 - m.a.0 + 1) as i64));
            }
            Reply::Array(item)
        })
        .collect();
    Reply::Array(vec![
        Reply::bulk("matches"),
        Reply::Array(matches),
        Reply::bulk("len"),
        Reply::Int(len as i64),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_subsequence_of_the_redis_example() {
        let opts = LcsOpts::default();
        assert_eq!(
            reply(b"ohmytext", b"mynewtext", opts),
            Reply::Bulk(b"mytext".to_vec())
        );
        let len = LcsOpts { len: true, ..opts };
        assert_eq!(reply(b"ohmytext", b"mynewtext", len), Reply::Int(6));
        assert_eq!(reply(b"", b"abc", opts), Reply::Bulk(Vec::new()));
    }

    #[test]
    fn reports_matches_last_first() {
        let opts = LcsOpts {
            idx: true,
            with_match_len: true,
            ..LcsOpts::default()
        };
        let int = Reply::Int;
        let pair = |a, b| Reply::Array(vec![int(a), int(b)]);
        assert_eq!(
            reply(b"ohmytext", b"mynewtext", opts),
            Reply::Array(vec![
                Reply::bulk("matches"),
                Reply::Array(vec![
                    Reply::Array(vec![pair(4, 7), pair(5, 8), int(4)]),
                    Reply::Array(vec![pair(2, 3), pair(0, 1), int(2)]),
                ]),
                Reply::bulk("len"),
                int(6),
            ])
        );
        let long = LcsOpts {
            min_match_len: 4,
            ..opts
        };
        let Reply::Array(items) = reply(b"ohmytext", b"mynewtext", long) else {
            panic!("not an array");
        };
        assert_eq!(
            items[1],
            Reply::Array(vec![Reply::Array(vec![pair(4, 7), pair(5, 8), int(4)])])
        );
    }

    #[test]
    fn refuses_a_table_that_would_be_too_large() {
        let a = vec![b'a'; 5000];
        assert!(matches!(reply(&a, &a, LcsOpts::default()), Reply::Error(_)));
    }
}
