//! What MONITOR and SLOWLOG show of a command: its arguments, cut to a
//! size that is safe to keep or to send to every watcher.

use hcache_core::engine::{SLOW_ARGS, SLOW_ARG_BYTES};
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

/// MONITOR shows this much of one argument.
const MONITOR_ARG_BYTES: usize = 4096;
/// MONITOR shows this many arguments of one command.
const MONITOR_ARGS: usize = 1024;
/// And about this much of one command in all.
const MONITOR_LINE_BYTES: usize = 64 << 10;

/// Whether the command carries a password: such a command is neither
/// logged nor shown.
pub(crate) fn sensitive(args: &[Vec<u8>]) -> bool {
    let is = |arg: &[u8], word: &str| arg.eq_ignore_ascii_case(word.as_bytes());
    let Some(name) = args.first() else {
        return false;
    };
    is(name, "AUTH")
        || is(name, "HELLO")
        || is(name, "ACL")
        || (is(name, "CONFIG") && args.iter().any(|arg| secret_setting(arg)))
}

/// Whether a CONFIG argument names a setting that holds a password:
/// requirepass, masterauth, tls-key-file-pass and their like.
fn secret_setting(arg: &[u8]) -> bool {
    let name = arg.to_ascii_lowercase();
    [&b"pass"[..], b"auth"]
        .iter()
        .any(|word| name.windows(word.len()).any(|part| part == *word))
}

/// Keep what SLOWLOG would show of `args` in `buf`, which is reused from
/// command to command: the number of arguments, then for each of the
/// first ones its full length and its first bytes.
pub(crate) fn stash<A: AsRef<[u8]>>(buf: &mut Vec<u8>, args: &[A]) {
    buf.clear();
    buf.extend_from_slice(&(args.len() as u64).to_le_bytes());
    for arg in args.iter().take(SLOW_ARGS) {
        let arg = arg.as_ref();
        let kept = arg.len().min(SLOW_ARG_BYTES);
        buf.extend_from_slice(&(arg.len() as u64).to_le_bytes());
        buf.extend_from_slice(&arg[..kept]);
    }
}

/// The arguments of a SLOWLOG entry from what `stash` kept, with a note
/// in place of what was cut, as Redis writes it.
pub(crate) fn unstash(buf: &[u8]) -> Vec<Vec<u8>> {
    fn number(rest: &mut &[u8]) -> Option<usize> {
        let (head, tail) = rest.split_first_chunk::<8>()?;
        *rest = tail;
        usize::try_from(u64::from_le_bytes(*head)).ok()
    }
    let mut rest = buf;
    let Some(count) = number(&mut rest) else {
        return Vec::new();
    };
    let shown = count.min(SLOW_ARGS);
    let mut out = Vec::with_capacity(shown);
    for n in 0..shown {
        let Some(len) = number(&mut rest) else {
            break;
        };
        let kept = len.min(SLOW_ARG_BYTES).min(rest.len());
        let (bytes, tail) = rest.split_at(kept);
        rest = tail;
        // The last place tells how many arguments are missing.
        if count > SLOW_ARGS && n == SLOW_ARGS - 1 {
            out.push(format!("... ({} more arguments)", count - SLOW_ARGS + 1).into_bytes());
            break;
        }
        let mut arg = bytes.to_vec();
        if len > kept {
            arg.extend_from_slice(format!("... ({} more bytes)", len - kept).as_bytes());
        }
        out.push(arg);
    }
    out
}

/// One line of MONITOR output, as Redis writes it:
/// `+1700000000.123456 [0 127.0.0.1:50000] "set" "k" "v"`.
pub(crate) fn monitor_line(db: usize, addr: &str, args: &[Vec<u8>]) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let mut line = String::new();
    // Writing to a String cannot fail.
    let _ = write!(
        line,
        "+{}.{:06} [{db} {addr}]",
        now.as_secs(),
        now.subsec_micros()
    );
    let mut shown = 0;
    for arg in args.iter().take(MONITOR_ARGS) {
        if line.len() > MONITOR_LINE_BYTES {
            break;
        }
        shown += 1;
        line.push_str(" \"");
        for &byte in arg.iter().take(MONITOR_ARG_BYTES) {
            match byte {
                b'"' => line.push_str("\\\""),
                b'\\' => line.push_str("\\\\"),
                b'\n' => line.push_str("\\n"),
                b'\r' => line.push_str("\\r"),
                b'\t' => line.push_str("\\t"),
                0x07 => line.push_str("\\a"),
                0x08 => line.push_str("\\b"),
                0x20..=0x7e => line.push(byte as char),
                _ => {
                    let _ = write!(line, "\\x{byte:02x}");
                }
            }
        }
        if arg.len() > MONITOR_ARG_BYTES {
            let _ = write!(line, "... ({} more bytes)", arg.len() - MONITOR_ARG_BYTES);
        }
        line.push('"');
    }
    if args.len() > shown {
        let _ = write!(line, " ... ({} more arguments)", args.len() - shown);
    }
    line.push_str("\r\n");
    line.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<Vec<u8>> {
        list.iter().map(|a| a.as_bytes().to_vec()).collect()
    }

    #[test]
    fn passwords_are_recognised() {
        assert!(sensitive(&args(&["auth", "secret"])));
        assert!(sensitive(&args(&["HELLO", "3", "AUTH", "u", "p"])));
        assert!(sensitive(&args(&["acl", "setuser", "u", ">p"])));
        assert!(sensitive(&args(&["config", "set", "RequirePass", "p"])));
        assert!(sensitive(&args(&["CONFIG", "SET", "MasterAuth", "p"])));
        assert!(sensitive(&args(&[
            "config",
            "set",
            "a",
            "1",
            "tls-key-file-pass",
            "p"
        ])));
        assert!(!sensitive(&args(&["config", "set", "maxmemory", "1"])));
        assert!(!sensitive(&args(&["get", "auth"])));
        assert!(!sensitive(&[]));
    }

    #[test]
    fn a_short_command_comes_back_whole() {
        let mut buf = Vec::new();
        let cmd = args(&["set", "k", ""]);
        stash(&mut buf, &cmd);
        assert_eq!(unstash(&buf), cmd);
        // The buffer is reused.
        stash(&mut buf, &args(&["ping"]));
        assert_eq!(unstash(&buf), args(&["ping"]));
    }

    #[test]
    fn long_arguments_and_long_commands_are_cut() {
        let mut buf = Vec::new();
        stash(&mut buf, &[b"set".to_vec(), vec![b'x'; SLOW_ARG_BYTES + 5]]);
        let out = unstash(&buf);
        assert_eq!(out[1].len(), SLOW_ARG_BYTES + "... (5 more bytes)".len());
        assert!(out[1].ends_with(b"... (5 more bytes)"));

        let many: Vec<Vec<u8>> = (0..SLOW_ARGS + 8)
            .map(|n| n.to_string().into_bytes())
            .collect();
        stash(&mut buf, &many);
        let out = unstash(&buf);
        assert_eq!(out.len(), SLOW_ARGS);
        assert_eq!(out[SLOW_ARGS - 2], many[SLOW_ARGS - 2]);
        assert_eq!(out[SLOW_ARGS - 1], b"... (9 more arguments)");
        assert!(buf.len() < 8 + SLOW_ARGS * (8 + SLOW_ARG_BYTES));
    }

    #[test]
    fn a_torn_buffer_is_no_panic() {
        let mut buf = Vec::new();
        stash(&mut buf, &args(&["set", "key", "value"]));
        for cut in 0..buf.len() {
            let _ = unstash(&buf[..cut]);
        }
        assert_eq!(unstash(&[]), Vec::<Vec<u8>>::new());
    }

    #[test]
    fn monitor_lines_escape_what_would_break_them() {
        let line = monitor_line(
            3,
            "127.0.0.1:1",
            &[b"set".to_vec(), b"a\"b\\c\r\n\x00\xff".to_vec()],
        );
        let text = String::from_utf8(line).expect("ascii");
        assert!(text.starts_with('+'));
        assert!(text.ends_with(" [3 127.0.0.1:1] \"set\" \"a\\\"b\\\\c\\r\\n\\x00\\xff\"\r\n"));
        // Nothing in between can end the line early.
        assert_eq!(text.matches('\r').count(), 1);
        assert_eq!(text.matches('\n').count(), 1);

        // However many arguments, however they escape: a line has an end.
        let wide = monitor_line(0, "a", &vec![vec![0xff; MONITOR_ARG_BYTES]; MONITOR_ARGS]);
        assert!(wide.len() < MONITOR_LINE_BYTES + 5 * MONITOR_ARG_BYTES);
        assert!(wide.ends_with(b" more arguments)\r\n"));
        let text = String::from_utf8(wide).expect("ascii");
        let told: usize = text
            .rsplit_once("... (")
            .and_then(|(_, tail)| tail.split(' ').next()?.parse().ok())
            .expect("a count");
        assert_eq!(text.matches(" \"").count() + told, MONITOR_ARGS);

        let long = monitor_line(0, "a", &[vec![b'x'; MONITOR_ARG_BYTES + 2]]);
        assert!(long.ends_with(b"... (2 more bytes)\"\r\n"));
    }
}
