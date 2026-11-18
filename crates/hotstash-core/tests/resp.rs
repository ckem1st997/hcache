//! Adversarial tests for the RESP parser, encoder and reply decoder.

use hotstash_core::resp::{
    decode_reply, encode_command, parse_i64, peek_keys, read_fast, Args, Limits, Parser,
    ProtoError, Reply,
};
use hotstash_core::testutil::Rng;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Allocation tracking (per thread, so parallel tests do not disturb each other)
// ---------------------------------------------------------------------------

struct TrackingAlloc;

thread_local! {
    static MAX_ALLOC: Cell<usize> = const { Cell::new(0) };
}

fn note(size: usize) {
    let _ = MAX_ALLOC.try_with(|m| {
        if size > m.get() {
            m.set(size);
        }
    });
}

// SAFETY: every method forwards to the system allocator unchanged; the only
// extra work is updating a const-initialised thread-local counter.
unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        // SAFETY: same contract as the caller's.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: same contract as the caller's.
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static ALLOC: TrackingAlloc = TrackingAlloc;

/// Run `f` and return its result with the largest single allocation it made.
fn max_alloc_of<R>(f: impl FnOnce() -> R) -> (R, usize) {
    MAX_ALLOC.with(|m| m.set(0));
    let r = f();
    (r, MAX_ALLOC.with(Cell::get))
}

// ---------------------------------------------------------------------------
// Drivers that emulate the documented caller contract
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    cmds: Vec<Args>,
    err: Option<ProtoError>,
    mid: bool,
}

/// Feed `input` in chunks, keeping `buf[n..]` between calls exactly as the
/// docs say. `on_state(total_consumed, mid_command)` runs after every delivery.
fn run_chunked(
    limits: Limits,
    input: &[u8],
    next_chunk: &mut dyn FnMut() -> usize,
    on_state: &mut dyn FnMut(usize, bool),
) -> Outcome {
    let mut p = Parser::new(limits);
    let mut pending: Vec<u8> = Vec::new();
    let mut delivered = 0;
    let mut base = 0;
    let mut cmds = Vec::new();
    while delivered < input.len() {
        let k = next_chunk().clamp(1, input.len() - delivered);
        pending.extend_from_slice(&input[delivered..delivered + k]);
        delivered += k;
        let mut off = 0;
        loop {
            match p.feed(&pending[off..]) {
                Err(e) => {
                    return Outcome {
                        cmds,
                        err: Some(e),
                        mid: p.mid_command(),
                    }
                }
                Ok((n, cmd)) => {
                    assert!(n <= pending.len() - off, "consumed more than given");
                    off += n;
                    match cmd {
                        Some(c) => {
                            assert!(n > 0, "a command with zero consumed bytes");
                            cmds.push(c);
                        }
                        None => break,
                    }
                }
            }
        }
        pending.drain(..off);
        base += off;
        on_state(base, p.mid_command());
    }
    Outcome {
        cmds,
        err: None,
        mid: p.mid_command(),
    }
}

fn run_whole(limits: Limits, input: &[u8]) -> Outcome {
    let len = input.len().max(1);
    run_chunked(limits, input, &mut || len, &mut |_, _| {})
}

fn c(args: &[&str]) -> Args {
    args.iter().map(|a| a.as_bytes().to_vec()).collect()
}

fn expect_cmds(input: &[u8], expected: Vec<Args>) {
    let o = run_whole(Limits::default(), input);
    assert_eq!(o.err, None, "input {input:?}");
    assert_eq!(o.cmds, expected, "input {input:?}");
    assert!(!o.mid, "input {input:?} left the parser mid-command");
}

fn expect_err_with(limits: Limits, input: &[u8], needle: &str) {
    let o = run_whole(limits, input);
    let e = o
        .err
        .unwrap_or_else(|| panic!("input {input:?} should fail, got {:?}", o.cmds));
    assert!(
        e.0.contains(needle),
        "input {input:?}: error {:?} lacks {needle:?}",
        e.0
    );
}

fn expect_err(input: &[u8], needle: &str) {
    expect_err_with(Limits::default(), input, needle);
}

fn expect_more(limits: Limits, input: &[u8], mid: bool) {
    let o = run_whole(limits, input);
    assert_eq!(o.err, None, "input {input:?}");
    assert!(o.cmds.is_empty(), "input {input:?} produced {:?}", o.cmds);
    assert_eq!(o.mid, mid, "mid_command for {input:?}");
}

fn lim(max_args: usize, max_bulk: usize, max_inline: usize, max_request: usize) -> Limits {
    Limits {
        max_args,
        max_bulk,
        max_inline,
        max_request,
    }
}

// ---------------------------------------------------------------------------
// Table tests: multibulk
// ---------------------------------------------------------------------------

#[test]
fn multibulk_well_formed() {
    expect_cmds(b"*1\r\n$4\r\nPING\r\n", vec![c(&["PING"])]);
    expect_cmds(
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
        vec![c(&["SET", "k", "v"])],
    );
    expect_cmds(
        b"*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n",
        vec![c(&["a"]), c(&["b"])],
    );
    expect_cmds(
        b"*3\r\n$3\r\nSET\r\n$0\r\n\r\n$0\r\n\r\n",
        vec![c(&["SET", "", ""])],
    );
}

#[test]
fn empty_multibulk_counts_are_skipped() {
    expect_cmds(b"*0\r\n", vec![]);
    expect_cmds(b"*-1\r\n", vec![]);
    expect_cmds(b"*-5\r\n", vec![]);
    expect_cmds(b"*-9223372036854775808\r\n", vec![]);
    expect_cmds(b"*0\r\n*-1\r\n*0\r\n*1\r\n$1\r\na\r\n", vec![c(&["a"])]);
    expect_cmds(b"*1\r\n$1\r\na\r\n*0\r\n", vec![c(&["a"])]);
}

#[test]
fn bad_multibulk_counts() {
    expect_err(b"*99999999999999999999\r\n", "invalid multibulk length");
    expect_err(b"*-9223372036854775809\r\n", "invalid multibulk length");
    expect_err(b"*9223372036854775808\r\n", "invalid multibulk length");
    expect_err(b"*9223372036854775807\r\n", "invalid multibulk length");
    expect_err(b"*\r\n", "invalid multibulk length");
    expect_err(b"*+1\r\n$1\r\na\r\n", "invalid multibulk length");
    expect_err(b"* 1\r\n$1\r\na\r\n", "invalid multibulk length");
    expect_err(b"*1 \r\n$1\r\na\r\n", "invalid multibulk length");
    expect_err(b"*x\r\n", "invalid multibulk length");
    expect_err(b"*--1\r\n", "invalid multibulk length");
    expect_err(b"*1\n$1\r\na\r\n", "invalid multibulk length");
    expect_err(b"*\n", "invalid multibulk length");
}

#[test]
fn bad_bulk_headers() {
    expect_err(b"*1\r\n$-1\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$-2\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$+1\r\na\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$ 1\r\na\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$1 \r\na\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$1x\r\na\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$99999999999999999999\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$9223372036854775807\r\n", "invalid bulk length");
    expect_err(b"*1\r\n$1\na\r\n", "invalid bulk length");
}

/// Redis `string2ll` rejects leading zeros, so `*03` and `$01` are protocol
/// errors there.
#[test]
fn leading_zero_lengths_are_rejected_like_redis() {
    expect_err(b"*1\r\n$01\r\na\r\n", "invalid bulk length");
    expect_err(b"*01\r\n$1\r\na\r\n", "invalid multibulk length");
    expect_err(b"*1\r\n$00\r\n\r\n", "invalid bulk length");
}

#[test]
fn bulk_must_be_followed_by_crlf() {
    expect_err(b"*1\r\n$1\r\nab\r\n", "not terminated by CRLF");
    expect_err(b"*1\r\n$1\r\na\n\n", "not terminated by CRLF");
    expect_err(b"*1\r\n$1\r\naXY", "not terminated by CRLF");
    expect_err(b"*1\r\n$0\r\nXY", "not terminated by CRLF");
    expect_err(b"*1\r\n$3\r\nabc\r\r", "not terminated by CRLF");
}

#[test]
fn non_bulk_element_inside_multibulk() {
    expect_err(b"*2\r\n$1\r\na\r\n:5\r\n", "expected '$'");
    expect_err(b"*1\r\n+OK\r\n", "expected '$'");
    expect_err(b"*1\r\n\r\n", "expected '$'");
    expect_err(b"*1\r\n*1\r\n$1\r\na\r\n", "expected '$'");
    expect_err(b"*1\r\nPING\r\n", "expected '$'");
}

#[test]
fn binary_args_survive_intact() {
    let a1: Vec<u8> = b"\r\n\0\n".to_vec();
    let a2: Vec<u8> = vec![0];
    let a3: Vec<u8> = b"*1\r\n$1\r\nx\r\n".to_vec();
    let mut wire = Vec::new();
    encode_command(&[&a1, &a2, &a3], &mut wire);
    let o = run_whole(Limits::default(), &wire);
    assert_eq!(o.err, None);
    assert_eq!(o.cmds, vec![vec![a1, a2, a3]]);
}

#[test]
fn incomplete_input_needs_more_and_tracks_mid_command() {
    let l = Limits::default();
    for (input, mid) in [
        (&b""[..], false),
        (b"*", false),
        (b"*1", false),
        (b"*1\r", false),
        (b"*1\r\n", true),
        (b"*1\r\n$", true),
        (b"*1\r\n$3", true),
        (b"*1\r\n$3\r", true),
        (b"*1\r\n$3\r\n", true),
        (b"*1\r\n$3\r\nab", true),
        (b"*1\r\n$3\r\nabc", true),
        (b"*1\r\n$3\r\nabc\r", true),
        (b"*2\r\n$1\r\na\r\n", true),
        (b"*2\r\n$1\r\na\r\n$1", true),
        (b"*0\r\n", false),
        (b"*0", false),
    ] {
        expect_more(l, input, mid);
    }
}

#[test]
fn header_line_length_cap() {
    // 30 digits then CRLF is a header of 33 bytes: refused as too big
    // (the number is invalid anyway), without needing the CRLF to arrive.
    let mut long = b"*".to_vec();
    long.extend(std::iter::repeat_n(b'1', 40));
    expect_err(&long, "too big");
    let mut p = Parser::new(Limits::default());
    assert!(p.feed(&long[..31]).is_ok());
    let mut q = Parser::new(Limits::default());
    assert!(q.feed(&long[..32]).is_err());
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

#[test]
fn max_args_exact_and_one_over() {
    let l = lim(3, 100, 100, 1000);
    let mut ok = Vec::new();
    encode_command(&["a", "b", "c"], &mut ok);
    assert_eq!(run_whole(l, &ok).cmds, vec![c(&["a", "b", "c"])]);
    // Rejected from the header alone, before any argument arrives.
    expect_err_with(l, b"*4\r\n", "invalid multibulk length");
    expect_err_with(l, b"*4\r\n$1\r\na\r\n", "invalid multibulk length");
    // Zero and negative counts are not subject to the limit.
    expect_cmds(b"*0\r\n", vec![]);
}

#[test]
fn max_bulk_exact_and_one_over() {
    let l = lim(10, 5, 100, 1000);
    expect_cmds(b"*1\r\n$0\r\n\r\n", vec![c(&[""])]);
    let o = run_whole(l, b"*1\r\n$5\r\nabcde\r\n");
    assert_eq!(o.cmds, vec![c(&["abcde"])]);
    // One over: refused from the header, no payload needed.
    expect_err_with(l, b"*1\r\n$6\r\n", "invalid bulk length");
    expect_err_with(l, b"*1\r\n$6\r\nabcdef\r\n", "invalid bulk length");
    // Exactly at the limit with the payload still missing: just wait.
    expect_more(l, b"*1\r\n$5\r\nab", true);
}

#[test]
fn max_inline_boundaries() {
    let l = lim(10, 100, 8, 1000);
    // Content of exactly max_inline bytes, bare LF.
    let o = run_whole(l, b"12345678\n");
    assert_eq!(o.err, None);
    assert_eq!(o.cmds, vec![c(&["12345678"])]);
    // One over, with newline.
    expect_err_with(l, b"123456789\n", "too big inline");
    // Without a newline: max_inline bytes wait, one more is an error.
    expect_more(l, b"12345678", false);
    expect_err_with(l, b"123456789", "too big inline");
    // A very long line with a newline far away.
    let mut long = vec![b'x'; 10_000];
    long.push(b'\n');
    expect_err_with(l, &long, "too big inline");
}

/// The line is `max_inline` bytes of content plus the CRLF terminator; the
/// CR is part of the terminator, not of the line.
#[test]
fn max_inline_counts_content_not_the_cr() {
    let l = lim(10, 100, 8, 1000);
    let o = run_whole(l, b"12345678\r\n");
    assert_eq!(
        o.err, None,
        "CRLF-terminated line of exactly max_inline bytes"
    );
    assert_eq!(o.cmds, vec![c(&["12345678"])]);
}

#[test]
fn max_request_header_time_rejection() {
    // 4 (array header) + 6 (bulk header) + 100 > 50: refuse without payload.
    let l = lim(10, 1000, 100, 50);
    expect_err_with(l, b"*1\r\n$100\r\n", "request too large");
    // A request spread over many small args is also capped.
    let mut wire = Vec::new();
    let args: Vec<&str> = std::iter::repeat_n("a", 20).collect();
    encode_command(&args, &mut wire);
    expect_err_with(lim(100, 1000, 100, 50), &wire, "request too large");
}

/// `max_request` is documented as the maximum total size of one command on
/// the wire: a command of exactly that size passes, one byte more fails.
#[test]
fn max_request_exact_wire_size_boundary() {
    for k in 1..=30usize {
        let args: Vec<&str> = std::iter::repeat_n("a", k).collect();
        let mut wire = Vec::new();
        encode_command(&args, &mut wire);
        let size = wire.len();
        let ok = run_whole(lim(100, 1000, 100, size), &wire);
        assert_eq!(ok.err, None, "k={k}: exact fit must pass");
        assert_eq!(ok.cmds.len(), 1);
        let over = run_whole(lim(100, 1000, 100, size - 1), &wire);
        assert!(
            over.err.is_some(),
            "k={k}: wire size {size} with max_request {} was accepted",
            size - 1
        );
    }
}

#[test]
fn max_request_single_bulk_boundary() {
    for len in [0usize, 1, 9, 10, 99, 100, 1000] {
        let payload = vec![b'z'; len];
        let mut wire = Vec::new();
        encode_command(&[&payload], &mut wire);
        let size = wire.len();
        let ok = run_whole(lim(10, 1 << 20, 100, size), &wire);
        assert_eq!(ok.err, None, "len={len}");
        let over = run_whole(lim(10, 1 << 20, 100, size - 1), &wire);
        assert!(over.err.is_some(), "len={len}: one byte over was accepted");
    }
}

#[test]
fn max_request_resets_between_commands() {
    // Two commands each exactly at the limit must both pass.
    let mut one = Vec::new();
    encode_command(&["abc"], &mut one);
    let mut wire = one.clone();
    wire.extend_from_slice(&one);
    wire.extend_from_slice(&one);
    let o = run_whole(lim(10, 100, 100, one.len()), &wire);
    assert_eq!(o.err, None);
    assert_eq!(o.cmds.len(), 3);
}

// ---------------------------------------------------------------------------
// Inline commands
// ---------------------------------------------------------------------------

#[test]
fn inline_basic() {
    expect_cmds(b"PING\r\n", vec![c(&["PING"])]);
    expect_cmds(b"PING\n", vec![c(&["PING"])]);
    expect_cmds(b"  SET\ta   b  \r\n", vec![c(&["SET", "a", "b"])]);
    expect_cmds(b"SET a\x0cb\n", vec![c(&["SET", "a", "b"])]);
    expect_cmds(b"\r\n", vec![]);
    expect_cmds(b"\n", vec![]);
    expect_cmds(b"   \r\n", vec![]);
    expect_cmds(b" \t \t\n", vec![]);
    expect_cmds(b"\n\n\nPING\n\n", vec![c(&["PING"])]);
    expect_cmds(b"A\nB\r\nC\n", vec![c(&["A"]), c(&["B"]), c(&["C"])]);
    // NUL is not a separator and a leading '$' is just a word.
    let o = run_whole(Limits::default(), b"SE\0T x\n");
    assert_eq!(o.cmds, vec![vec![b"SE\0T".to_vec(), b"x".to_vec()]]);
    expect_cmds(b"$3\r\nfoo\r\n", vec![c(&["$3"]), c(&["foo"])]);
    // A '*' that is not the first byte of the line is inline text.
    expect_cmds(b" *1\r\n", vec![c(&["*1"])]);
    expect_cmds(b"a*1\r\n", vec![c(&["a*1"])]);
}

#[test]
fn inline_incomplete() {
    let l = Limits::default();
    expect_more(l, b"PING", false);
    expect_more(l, b"PING\r", false);
    expect_more(l, b"", false);
    // Empty lines before an incomplete one are consumed.
    let mut p = Parser::new(l);
    let (n, cmd) = p.feed(b"\r\n\r\nPIN").unwrap();
    assert_eq!((n, cmd), (4, None));
}

#[test]
fn inline_quotes_are_split_like_redis() {
    // Redis `sdssplitargs`: double and single quotes group words.
    expect_cmds(
        b"SET \"hello world\" 'x y'\r\n",
        vec![c(&["SET", "hello world", "x y"])],
    );
    expect_cmds(b"ECHO \"a\\x41b\"\r\n", vec![c(&["ECHO", "aAb"])]);
    expect_cmds(b"ECHO \"a\\nb\"\r\n", vec![c(&["ECHO", "a\nb"])]);
    expect_cmds(b"ECHO 'it\\'s'\r\n", vec![c(&["ECHO", "it's"])]);
}

#[test]
fn inline_unbalanced_quotes_are_an_error_like_redis() {
    expect_err(b"SET \"abc\r\n", "unbalanced quotes");
    expect_err(b"SET 'abc\r\n", "unbalanced quotes");
    // Closing quote must be followed by a space.
    expect_err(b"SET \"abc\"def\r\n", "unbalanced quotes");
}

#[test]
fn inline_vertical_tab_is_a_separator_like_c_isspace() {
    expect_cmds(b"SET a\x0bb\n", vec![c(&["SET", "a", "b"])]);
}

// ---------------------------------------------------------------------------
// Delivery independence
// ---------------------------------------------------------------------------

struct Stream {
    bytes: Vec<u8>,
    cmds: Vec<Args>,
    /// Byte ranges of multibulk commands with at least one argument.
    spans: Vec<(usize, usize)>,
}

fn rand_arg(rng: &mut Rng) -> Vec<u8> {
    let len = match rng.below(10) {
        0 => 0,
        1..=6 => rng.below(20) as usize,
        7 | 8 => rng.below(120) as usize,
        _ => 150 + rng.below(200) as usize,
    };
    let mut v = rng.bytes(len);
    if len > 4 && rng.chance(3) {
        let at = rng.below(len as u64 - 2) as usize;
        let junk: &[u8] = match rng.below(4) {
            0 => b"\r\n",
            1 => b"\0",
            2 => b"*1\r\n$1\r\nx\r\n",
            _ => b"\n\r",
        };
        let end = (at + junk.len()).min(len);
        v[at..end].copy_from_slice(&junk[..end - at]);
    }
    v
}

fn gen_stream(rng: &mut Rng, n: usize) -> Stream {
    const WORD: &[u8] = b"abcXYZ019:_$.-+/";
    let mut s = Stream {
        bytes: Vec::new(),
        cmds: Vec::new(),
        spans: Vec::new(),
    };
    for _ in 0..n {
        match rng.below(20) {
            0..=13 => {
                let nargs = 1 + rng.below(6) as usize;
                let args: Vec<Vec<u8>> = (0..nargs).map(|_| rand_arg(rng)).collect();
                let start = s.bytes.len();
                encode_command(&args, &mut s.bytes);
                s.spans.push((start, s.bytes.len()));
                s.cmds.push(args);
            }
            14..=16 => {
                let nwords = 1 + rng.below(4) as usize;
                let mut line = Vec::new();
                let mut args = Vec::new();
                if rng.chance(3) {
                    line.extend_from_slice(b"  \t");
                }
                for i in 0..nwords {
                    if i > 0 {
                        line.extend_from_slice([&b" "[..], b"\t", b"   "][rng.below(3) as usize]);
                    }
                    let wl = 1 + rng.below(8) as usize;
                    let w: Vec<u8> = (0..wl)
                        .map(|_| WORD[rng.below(WORD.len() as u64) as usize])
                        .collect();
                    line.extend_from_slice(&w);
                    args.push(w);
                }
                if rng.chance(3) {
                    line.extend_from_slice(b" ");
                }
                line.extend_from_slice([&b"\r\n"[..], b"\n"][rng.below(2) as usize]);
                s.bytes.extend_from_slice(&line);
                s.cmds.push(args);
            }
            17 => s.bytes.extend_from_slice(b"*0\r\n"),
            18 => s.bytes.extend_from_slice(b"*-1\r\n"),
            _ => s
                .bytes
                .extend_from_slice([&b"\r\n"[..], b"\n", b"  \t \r\n"][rng.below(3) as usize]),
        }
    }
    s
}

fn expected_mid(spans: &[(usize, usize)], consumed: usize) -> bool {
    spans.iter().any(|&(a, b)| a < consumed && consumed < b)
}

fn check_stream(s: &Stream, next_chunk: &mut dyn FnMut() -> usize, label: &str) {
    let mut bad: Option<(usize, bool)> = None;
    let o = run_chunked(
        Limits::default(),
        &s.bytes,
        next_chunk,
        &mut |consumed, mid| {
            if bad.is_none() && mid != expected_mid(&s.spans, consumed) {
                bad = Some((consumed, mid));
            }
        },
    );
    assert_eq!(o.err, None, "{label}");
    assert_eq!(o.cmds, s.cmds, "{label}: command sequence differs");
    assert!(!o.mid, "{label}: mid_command at end of stream");
    assert_eq!(bad, None, "{label}: mid_command wrong at (consumed, got)");
}

#[test]
fn stream_whole_matches_generator() {
    for seed in 0..20 {
        let mut rng = Rng::new(seed);
        let s = gen_stream(&mut rng, 40);
        let len = s.bytes.len();
        check_stream(&s, &mut || len, &format!("seed {seed}"));
    }
}

#[test]
fn stream_every_two_chunk_split_point() {
    for seed in 100..106 {
        let mut rng = Rng::new(seed);
        let s = gen_stream(&mut rng, 25);
        for split in 1..s.bytes.len() {
            let mut first = true;
            let mut next = || {
                if first {
                    first = false;
                    split
                } else {
                    usize::MAX
                }
            };
            check_stream(&s, &mut next, &format!("seed {seed} split {split}"));
        }
    }
}

#[test]
fn stream_one_byte_at_a_time() {
    for seed in 200..208 {
        let mut rng = Rng::new(seed);
        let s = gen_stream(&mut rng, 30);
        check_stream(&s, &mut || 1, &format!("seed {seed}"));
    }
}

#[test]
fn stream_random_chunkings() {
    for seed in 300..500 {
        let mut rng = Rng::new(seed);
        let s = gen_stream(&mut rng, 20);
        let mut crng = Rng::new(seed ^ 0xABCD);
        let max = 1 + crng.below(60);
        let mut next = move || 1 + crng.below(max) as usize;
        check_stream(&s, &mut next, &format!("seed {seed}"));
    }
}

#[test]
fn large_bulk_in_dribbles_does_not_rescan_or_lose_bytes() {
    let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let mut wire = Vec::new();
    encode_command(&[&b"SET"[..], b"k", &payload], &mut wire);
    let mut rng = Rng::new(7);
    let start = Instant::now();
    let o = run_chunked(
        Limits::default(),
        &wire,
        &mut || 1 + rng.below(4000) as usize,
        &mut |_, _| {},
    );
    assert_eq!(o.err, None);
    assert_eq!(o.cmds, vec![vec![b"SET".to_vec(), b"k".to_vec(), payload]]);
    assert!(start.elapsed() < Duration::from_secs(5));
}

// ---------------------------------------------------------------------------
// Huge declared sizes must not be pre-allocated
// ---------------------------------------------------------------------------

#[test]
fn huge_declared_array_does_not_preallocate() {
    let start = Instant::now();
    let ((res, mid), peak) = max_alloc_of(|| {
        let mut p = Parser::new(Limits::default());
        let res = p.feed(b"*1000000\r\n").unwrap();
        (res, p.mid_command())
    });
    assert_eq!(res, (10, None));
    assert!(mid);
    assert!(
        peak < 1 << 20,
        "allocated {peak} bytes for a header with no payload"
    );
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[test]
fn huge_declared_bulk_does_not_preallocate() {
    let start = Instant::now();
    let (res, peak) = max_alloc_of(|| {
        let mut p = Parser::new(Limits::default());
        let mut buf = b"*1\r\n$67108864\r\n".to_vec();
        buf.extend_from_slice(b"0123456789");
        let first = p.feed(&buf).unwrap();
        // Caller keeps buf[n..] and retries with more bytes.
        buf.extend_from_slice(b"abc");
        let again = p.feed(&buf[first.0..]).unwrap();
        (first.0, again)
    });
    assert_eq!(res.0, 15);
    assert_eq!(res.1, (0, None));
    assert!(
        peak < 1 << 20,
        "allocated {peak} bytes for a bulk of which 13 bytes exist"
    );
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[test]
fn huge_declared_reply_does_not_preallocate() {
    let ((a, b, d), peak) = max_alloc_of(|| {
        (
            decode_reply(b"*1000000000\r\n:1\r\n").unwrap(),
            decode_reply(b"$9999999999\r\nabc").unwrap(),
            decode_reply(b"$9223372036854775807\r\nabc").unwrap(),
        )
    });
    assert_eq!((a, b, d), (None, None, None));
    assert!(peak < 1 << 20, "allocated {peak} bytes");
}

// ---------------------------------------------------------------------------
// Fuzz
// ---------------------------------------------------------------------------

fn mutate(rng: &mut Rng, base: &[u8]) -> Vec<u8> {
    let mut v = base.to_vec();
    for _ in 0..1 + rng.below(4) {
        if v.is_empty() {
            v.push(rng.next_u64() as u8);
            continue;
        }
        let at = rng.below(v.len() as u64) as usize;
        match rng.below(7) {
            0 => v[at] ^= 1 << rng.below(8),
            1 => v.truncate(at),
            2 => {
                let end = (at + 1 + rng.below(12) as usize).min(v.len());
                let seg = v[at..end].to_vec();
                v.splice(at..at, seg);
            }
            3 => v.insert(at, rng.next_u64() as u8),
            4 => {
                v.remove(at);
            }
            5 => v[at] = b"\r\n$*-0 9"[rng.below(8) as usize],
            _ => {
                let end = (at + 1 + rng.below(6) as usize).min(v.len());
                v.drain(at..end);
            }
        }
    }
    v
}

fn token_soup(rng: &mut Rng) -> Vec<u8> {
    const TOKENS: &[&[u8]] = &[
        b"*",
        b"$",
        b"\r\n",
        b"\n",
        b"\r",
        b"-",
        b"+",
        b"0",
        b"1",
        b"12",
        b"3",
        b"abc",
        b" ",
        b"\"",
        b"99999999999999999999",
        b"-1",
        b":",
        b"\0",
    ];
    let mut v = Vec::new();
    for _ in 0..1 + rng.below(14) {
        v.extend_from_slice(TOKENS[rng.below(TOKENS.len() as u64) as usize]);
    }
    v
}

fn fuzz_limits(rng: &mut Rng) -> Limits {
    if rng.chance(2) {
        Limits::default()
    } else {
        lim(
            1 + rng.below(5) as usize,
            rng.below(12) as usize,
            1 + rng.below(12) as usize,
            5 + rng.below(60) as usize,
        )
    }
}

#[test]
fn fuzz_parser_never_panics_and_is_chunking_independent() {
    let mut rng = Rng::new(0xF00D);
    let start = Instant::now();
    for i in 0..200_000u32 {
        let input = match rng.below(4) {
            0 => rbytes(&mut rng, 80),
            1 => token_soup(&mut rng),
            2 => {
                let s = gen_stream_n(&mut rng, 3);
                mutate(&mut rng, &s.bytes)
            }
            _ => {
                let s = gen_stream_n(&mut rng, 3);
                s.bytes
            }
        };
        let limits = fuzz_limits(&mut rng);
        let whole = run_whole(limits, &input);
        let max = 1 + rng.below(10);
        let mut crng = Rng::new(i as u64);
        let chunked = run_chunked(
            limits,
            &input,
            &mut || 1 + crng.below(max) as usize,
            &mut |_, _| {},
        );
        assert_eq!(
            whole.cmds, chunked.cmds,
            "iter {i} input {input:?} {limits:?}"
        );
        assert_eq!(
            whole.err, chunked.err,
            "iter {i} input {input:?} {limits:?}"
        );
        if whole.err.is_none() {
            assert_eq!(whole.mid, chunked.mid, "iter {i} input {input:?}");
        }
        for cmd in &whole.cmds {
            assert!(!cmd.is_empty() && cmd.len() <= limits.max_args.max(input.len()));
            for a in cmd {
                assert!(a.len() <= limits.max_bulk.max(limits.max_inline).max(input.len()));
            }
        }
    }
    assert!(start.elapsed() < Duration::from_secs(60));
}

#[test]
fn fuzz_feeding_after_an_error_does_not_panic_or_overconsume() {
    // The docs say nothing about parser state after an error; the minimum is
    // that continuing to call feed is memory safe and bounded.
    let mut rng = Rng::new(0xBEEF);
    for i in 0..60_000u32 {
        let input = if rng.chance(2) {
            token_soup(&mut rng)
        } else {
            let s = gen_stream(&mut rng, 2);
            mutate(&mut rng, &s.bytes)
        };
        let mut p = Parser::new(fuzz_limits(&mut rng));
        let mut off = 0;
        for _ in 0..8 {
            if let Ok((n, _)) = p.feed(&input[off..]) {
                assert!(n <= input.len() - off, "iter {i}");
                off += n;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_random_commands() {
    let mut rng = Rng::new(11);
    for i in 0..20_000 {
        let nargs = 1 + rng.below(8) as usize;
        let args: Vec<Vec<u8>> = (0..nargs).map(|_| rand_arg(&mut rng)).collect();
        let mut wire = Vec::new();
        encode_command(&args, &mut wire);
        let mut p = Parser::new(Limits::default());
        let (n, got) = p.feed(&wire).unwrap();
        assert_eq!(n, wire.len(), "iter {i}");
        assert_eq!(got, Some(args), "iter {i}");
        assert!(!p.mid_command());
    }
}

#[test]
fn encode_command_with_no_args_is_an_empty_command() {
    let mut wire = Vec::new();
    encode_command::<&[u8]>(&[], &mut wire);
    assert_eq!(wire, b"*0\r\n");
    let o = run_whole(Limits::default(), &wire);
    assert_eq!((o.err, o.cmds.len()), (None, 0));
}

fn gen_text(rng: &mut Rng) -> String {
    const CH: &[&str] = &[
        "a", "b", " ", "x", ":", "-", "_", "0", "\u{e9}", "\u{4e2d}", "+",
    ];
    (0..rng.below(12))
        .map(|_| CH[rng.below(CH.len() as u64) as usize])
        .collect()
}

fn gen_int(rng: &mut Rng) -> i64 {
    match rng.below(8) {
        0 => 0,
        1 => 1,
        2 => -1,
        3 => i64::MIN,
        4 => i64::MAX,
        5 => rng.below(1000) as i64 - 500,
        _ => rng.next_u64() as i64,
    }
}

fn gen_reply(rng: &mut Rng, depth: u32) -> Reply {
    let kinds = if depth >= 4 { 6 } else { 7 };
    match rng.below(kinds) {
        0 => Reply::Ok,
        1 => {
            let t = gen_text(rng);
            Reply::Simple(if t == "OK" { "OK!".into() } else { t })
        }
        2 => Reply::Error(gen_text(rng)),
        3 => Reply::Int(gen_int(rng)),
        4 => {
            let mut b = rbytes(rng, 30);
            if b.len() > 3 && rng.chance(2) {
                b[1..3].copy_from_slice(b"\r\n");
            }
            Reply::Bulk(b)
        }
        5 => Reply::Nil,
        _ => Reply::Array(
            (0..rng.below(5))
                .map(|_| gen_reply(rng, depth + 1))
                .collect(),
        ),
    }
}

#[test]
fn roundtrip_random_reply_trees_and_all_truncations() {
    let mut rng = Rng::new(21);
    for i in 0..3_000 {
        let r = gen_reply(&mut rng, 0);
        let mut wire = Vec::new();
        r.encode(&mut wire);
        let (got, used) = decode_reply(&wire).unwrap().unwrap();
        assert_eq!(got, r, "iter {i}");
        assert_eq!(used, wire.len(), "iter {i}");
        // Trailing bytes are left alone.
        let mut longer = wire.clone();
        longer.extend_from_slice(b"+junk\r\n");
        let (got2, used2) = decode_reply(&longer).unwrap().unwrap();
        assert_eq!((got2, used2), (r.clone(), wire.len()), "iter {i}");
        for cut in 0..wire.len() {
            assert_eq!(
                decode_reply(&wire[..cut]),
                Ok(None),
                "iter {i}: prefix of {cut}/{} bytes of {wire:?}",
                wire.len()
            );
        }
    }
}

#[test]
fn pipelined_replies_decode_back_to_back() {
    let mut rng = Rng::new(22);
    let replies: Vec<Reply> = (0..200).map(|_| gen_reply(&mut rng, 0)).collect();
    let mut wire = Vec::new();
    for r in &replies {
        r.encode(&mut wire);
    }
    let mut off = 0;
    for r in &replies {
        let (got, used) = decode_reply(&wire[off..]).unwrap().unwrap();
        assert_eq!(&got, r);
        off += used;
    }
    assert_eq!(off, wire.len());
}

#[test]
fn status_and_error_lines_cannot_be_split_into_forged_replies() {
    let mut out = Vec::new();
    Reply::Simple("a\r\n:1\r\nb\nc\r".into()).encode(&mut out);
    assert_eq!(out, b"+a  :1  b c \r\n");
    let (_, used) = decode_reply(&out).unwrap().unwrap();
    assert_eq!(used, out.len());
    let mut out = Vec::new();
    Reply::err("ERR x\r\n+OK").encode(&mut out);
    assert_eq!(out, b"-ERR x  +OK\r\n");
    let (_, used) = decode_reply(&out).unwrap().unwrap();
    assert_eq!(used, out.len());
}

#[test]
fn simple_ok_collapses_to_reply_ok() {
    // Documented-by-behaviour quirk: Simple("OK") is indistinguishable on
    // the wire from Ok.
    let mut out = Vec::new();
    Reply::Simple("OK".into()).encode(&mut out);
    assert_eq!(out, b"+OK\r\n");
    assert_eq!(decode_reply(&out), Ok(Some((Reply::Ok, 5))));
}

#[test]
fn encode_exact_bytes() {
    let mut out = Vec::new();
    Reply::Array(vec![
        Reply::Ok,
        Reply::Int(i64::MIN),
        Reply::Int(i64::MAX),
        Reply::Int(0),
        Reply::Int(-7),
        Reply::Nil,
        Reply::bulk(&b"hi"[..]),
        Reply::bulk(Vec::new()),
        Reply::Array(vec![]),
        Reply::err("E"),
    ])
    .encode(&mut out);
    assert_eq!(
        out,
        b"*10\r\n+OK\r\n:-9223372036854775808\r\n:9223372036854775807\r\n:0\r\n:-7\r\n$-1\r\n$2\r\nhi\r\n$0\r\n\r\n*0\r\n-E\r\n"
    );
}

#[test]
fn decode_garbage_and_malformed() {
    for bad in [
        &b"?\r\n"[..],
        b"+OK\n",
        b"\n",
        b":\r\n",
        b":+1\r\n",
        b":1 \r\n",
        b":99999999999999999999\r\n",
        b"$x\r\n",
        b"$3\r\nabcXY",
        b"*x\r\n",
        b"*1\r\n?\r\n",
    ] {
        assert!(decode_reply(bad).is_err(), "{bad:?} should be an error");
    }
    assert_eq!(decode_reply(b""), Ok(None));
    assert_eq!(decode_reply(b"*-1\r\n"), Ok(Some((Reply::NilArray, 5))));
    assert_eq!(decode_reply(b"$-1\r\n"), Ok(Some((Reply::Nil, 5))));
}

#[test]
fn decode_depth_is_bounded_without_stack_overflow() {
    let deep = "*1\r\n".repeat(100_000);
    assert!(decode_reply(deep.as_bytes()).is_err());
    let mut ok = "*1\r\n".repeat(10);
    ok.push_str(":1\r\n");
    let (r, used) = decode_reply(ok.as_bytes()).unwrap().unwrap();
    assert_eq!(used, ok.len());
    let mut depth = 0;
    let mut cur = &r;
    while let Reply::Array(items) = cur {
        depth += 1;
        cur = &items[0];
    }
    assert_eq!((depth, cur), (10, &Reply::Int(1)));
}

#[test]
fn fuzz_decode_never_panics() {
    let mut rng = Rng::new(0xDEC0DE);
    const TOKENS: &[&[u8]] = &[
        b"*",
        b"$",
        b"+",
        b"-",
        b":",
        b"\r\n",
        b"\n",
        b"\r",
        b"0",
        b"1",
        b"3",
        b"-1",
        b"abc",
        b"OK",
        b"99999999999999999999",
        b"\0",
    ];
    for i in 0..150_000 {
        let input = match rng.below(3) {
            0 => rbytes(&mut rng, 60),
            1 => {
                let mut v = Vec::new();
                for _ in 0..1 + rng.below(14) {
                    v.extend_from_slice(TOKENS[rng.below(TOKENS.len() as u64) as usize]);
                }
                v
            }
            _ => {
                let r = gen_reply(&mut rng, 0);
                let mut w = Vec::new();
                r.encode(&mut w);
                mutate(&mut rng, &w)
            }
        };
        if input.starts_with(b"\r\n") || input.windows(3).any(|w| w == b"\n\r\n") {
            continue; // known panic, covered by its own test
        }
        if let Ok(Some((_, used))) = decode_reply(&input) {
            assert!(used > 0 && used <= input.len(), "iter {i} {input:?}");
        }
    }
}

// ---------------------------------------------------------------------------
// parse_i64
// ---------------------------------------------------------------------------

#[test]
fn parse_i64_accepts_canonical_integers() {
    for (s, v) in [
        ("0", 0),
        ("1", 1),
        ("-1", -1),
        ("10", 10),
        ("123456789", 123_456_789),
        ("9223372036854775807", i64::MAX),
        ("-9223372036854775808", i64::MIN),
        ("-9223372036854775807", i64::MIN + 1),
    ] {
        assert_eq!(parse_i64(s.as_bytes()), Some(v), "{s}");
    }
}

#[test]
fn parse_i64_rejects_malformed() {
    for s in [
        "",
        "-",
        "+",
        "+5",
        "+0",
        " 5",
        "5 ",
        "5\n",
        "5\r",
        "\t5",
        "--1",
        "1-",
        "-+1",
        "0x10",
        "1e3",
        "1.0",
        "5a",
        "a5",
        "9223372036854775808",
        "-9223372036854775809",
        "99999999999999999999",
        "-99999999999999999999",
        "18446744073709551616",
        "18446744073709551615",
        "\u{663}",
    ] {
        assert_eq!(parse_i64(s.as_bytes()), None, "{s:?}");
    }
    assert_eq!(parse_i64(b"1\0"), None);
    assert_eq!(parse_i64(b"\0"), None);
    assert_eq!(parse_i64(&[0xFF, b'1']), None);
}

/// Redis `string2ll`: only "0" may start with a zero.
#[test]
fn parse_i64_rejects_leading_zeros_like_redis() {
    for s in [
        "00",
        "01",
        "007",
        "-007",
        "0000000000000000001",
        "09223372036854775807",
    ] {
        assert_eq!(parse_i64(s.as_bytes()), None, "{s}");
    }
}

/// The doc comment of `parse_i64` promises "-0" is accepted as 0 (Redis
/// `string2ll` rejects it; kept as documented here).
#[test]
fn parse_i64_rejects_negative_zero_like_redis() {
    assert_eq!(parse_i64(b"-0"), None);
}

#[test]
fn parse_i64_matches_to_string_roundtrip_and_wire_encoding() {
    let mut rng = Rng::new(5);
    for _ in 0..100_000 {
        let n = gen_int(&mut rng);
        assert_eq!(parse_i64(n.to_string().as_bytes()), Some(n));
        let mut out = Vec::new();
        Reply::Int(n).encode(&mut out);
        assert_eq!(out, format!(":{n}\r\n").as_bytes());
        // One digit more or less than i64 range edges must not wrap.
        if n == i64::MAX {
            assert_eq!(parse_i64(format!("{}", n as i128 + 1).as_bytes()), None);
        }
        if n == i64::MIN {
            assert_eq!(parse_i64(format!("{}", n as i128 - 1).as_bytes()), None);
        }
    }
    // Every 19/20-digit value around the u64 and i64 edges.
    for d in 0..2000i128 {
        let hi = i64::MAX as i128 + d;
        let lo = i64::MIN as i128 - d;
        assert_eq!(parse_i64(hi.to_string().as_bytes()).is_some(), d == 0);
        assert_eq!(parse_i64(lo.to_string().as_bytes()).is_some(), d == 0);
    }
}

fn rbytes(rng: &mut Rng, below: u64) -> Vec<u8> {
    let n = rng.below(below) as usize;
    rng.bytes(n)
}

fn gen_stream_n(rng: &mut Rng, max: u64) -> Stream {
    let n = 1 + rng.below(max) as usize;
    gen_stream(rng, n)
}

/// A reply line that starts with CR LF (kind byte `\r`, then `\n`) used to
/// slip past the CRLF check and slice `buf[1..0]`.
#[test]
fn decode_reply_starting_with_crlf_is_an_error_not_a_panic() {
    for input in [&b"\r\n"[..], b"\r\nabc", b"\r\n\r\n"] {
        let r = std::panic::catch_unwind(|| decode_reply(input));
        match r {
            Ok(res) => assert!(res.is_err(), "{input:?} should be a protocol error"),
            Err(_) => panic!("decode_reply panicked on {input:?}"),
        }
    }
}

#[test]
fn peek_keys_finds_the_second_argument_of_whole_commands() {
    let mut buf = Vec::new();
    encode_command(&["GET", "k1"], &mut buf);
    encode_command(&["PING"], &mut buf);
    encode_command(&["SET", "k2", "v"], &mut buf);
    let whole = buf.len();
    encode_command(&["GET", "k3"], &mut buf);
    let mut keys = [&[][..]; 8];
    assert_eq!(peek_keys(&buf, &mut keys), (4, 3));
    assert_eq!(keys[..3], [&b"k1"[..], b"k2", b"k3"]);
    // A command cut short ends the look.
    assert_eq!(peek_keys(&buf[..buf.len() - 1], &mut keys), (3, 2));
    assert_eq!(peek_keys(&buf[..whole + 3], &mut keys), (3, 2));
    // No more commands than there is room for keys.
    let mut two = [&[][..]; 2];
    assert_eq!(peek_keys(&buf, &mut two), (2, 1));
    // Inline commands and nonsense are not read ahead.
    assert_eq!(peek_keys(b"GET k1\r\n", &mut keys), (0, 0));
    assert_eq!(peek_keys(b"*2\r\n$3\r\nGET\r\n$-1\r\n", &mut keys), (0, 0));
    assert_eq!(peek_keys(b"*99999999999999999999\r\n", &mut keys), (0, 0));
    assert_eq!(
        peek_keys(b"*2\r\n$3\r\nGET\r\n$999999999999999999\r\nk", &mut keys),
        (0, 0)
    );
    assert_eq!(peek_keys(b"", &mut keys), (0, 0));
}

/// Whatever the input, what is read ahead is what the parser later reads,
/// and nothing panics.
#[test]
fn peek_keys_agrees_with_the_parser() {
    let mut rng = Rng::new(0x5eed_4ead);
    for _ in 0..3000 {
        let mut buf = Vec::new();
        for _ in 0..rng.below(20) {
            let argc = rng.below(4) as usize;
            let args: Vec<Vec<u8>> = (0..argc).map(|_| rbytes(&mut rng, 12)).collect();
            if !args.is_empty() {
                encode_command(&args, &mut buf);
            }
        }
        if rng.below(4) == 0 {
            let cut = rng.below(buf.len() as u64 + 1) as usize;
            buf.truncate(cut);
        }
        if rng.below(8) == 0 {
            buf.extend(rbytes(&mut rng, 16));
        }
        let mut keys = [&[][..]; 16];
        let (seen, found) = peek_keys(&buf, &mut keys);
        let mut parser = Parser::new(Limits::default());
        let mut at = 0;
        let mut expect = Vec::new();
        for _ in 0..seen {
            let (used, args) = parser.feed(&buf[at..]).expect("a well-formed command");
            let args = args.expect("a whole command");
            at += used;
            if let Some(key) = args.get(1) {
                expect.push(key.clone());
            }
        }
        let got: Vec<Vec<u8>> = keys[..found].iter().map(|k| k.to_vec()).collect();
        assert_eq!(got, expect);
    }
}

/// What the fast path takes, the parser reads the same way, limits
/// included, and it takes only GET and SET with no options.
#[test]
fn read_fast_agrees_with_the_parser() {
    let mut rng = Rng::new(0x5eed_fa57);
    let names: [&[u8]; 6] = [b"GET", b"get", b"SET", b"sEt", b"GETX", b"PING"];
    let mut taken = 0;
    for round in 0..5000 {
        let argc = 1 + rng.below(4) as usize;
        let mut args: Vec<Vec<u8>> = (0..argc).map(|_| rbytes(&mut rng, 12)).collect();
        args[0] = names[rng.below(names.len() as u64) as usize].to_vec();
        let mut buf = Vec::new();
        encode_command(&args, &mut buf);
        if rng.below(4) == 0 {
            let cut = rng.below(buf.len() as u64 + 1) as usize;
            buf.truncate(cut);
        }
        if rng.below(4) == 0 {
            buf.extend(rbytes(&mut rng, 16));
        }
        // Now and then limits tight enough for some commands to go over.
        let limits = match round % 3 {
            0 => lim(
                2 + rng.below(2) as usize,
                4,
                64,
                20 + rng.below(20) as usize,
            ),
            _ => Limits::default(),
        };
        let Some(fast) = read_fast(&buf, &limits) else {
            continue;
        };
        taken += 1;
        let Ok((want_used, Some(want))) = Parser::new(limits).feed(&buf) else {
            panic!("the fast path took what the parser does not: {buf:?}");
        };
        assert_eq!(fast.used, want_used, "{buf:?}");
        let got: Vec<Vec<u8>> = fast.args().iter().map(|a| a.to_vec()).collect();
        assert_eq!(got, want, "{buf:?}");
        let name = want[0].to_ascii_uppercase();
        match want.len() {
            2 => assert_eq!(name, b"GET"),
            3 => assert_eq!(name, b"SET"),
            n => panic!("the fast path took {n} arguments"),
        }
    }
    assert!(taken > 300, "only {taken} commands went the fast way");
}

#[test]
fn read_fast_leaves_the_rest_alone() {
    let limits = Limits::default();
    for input in [
        &b"*3\r\n$3\r\nGET\r\n$1\r\nk\r\n$1\r\nv\r\n"[..],
        b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n",
        b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n",
        b"*2\r\n$4\r\nPING\r\n$1\r\nk\r\n",
        b"GET k\r\n",
        b"*2\r\n$3\r\nGET\r\n$1\r\nk",
        b"*2\r\n$3\r\nGET\r\n$01\r\nk\r\n",
        b"*2\r\n$3\r\nGET\r\n$1\r\nkx\n",
        b"",
    ] {
        assert_eq!(read_fast(input, &limits), None, "{input:?}");
    }
    let two = b"*2\r\n$3\r\nget\r\n$1\r\nk\r\n*1\r\n$4\r\nPING\r\n";
    let fast = read_fast(two, &limits).expect("a GET");
    assert_eq!(fast.used, 20);
    assert_eq!(fast.args(), [&b"get"[..], b"k"]);
}
