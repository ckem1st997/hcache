//! Client side of RESP for the load generator: count replies without
//! building them, however the bytes are split across reads.

use hcache_core::resp::{decode_reply, Reply};
use std::io::{self, Read};
use std::net::TcpStream;

/// Counts complete top-level replies in a byte stream.
#[derive(Default)]
pub struct Scanner {
    /// Elements still missing from the reply in progress (0 = between replies).
    remaining: u64,
    /// Bytes of a bulk payload (and its CRLF) still to skip.
    skip: usize,
    /// A header line cut short by the end of a read.
    partial: Vec<u8>,
}

/// Longest header line worth buffering: an error message.
const MAX_LINE: usize = 64 * 1024;

impl Scanner {
    /// Returns (replies completed, of which top-level errors).
    pub fn feed(&mut self, mut buf: &[u8]) -> Result<(usize, usize), String> {
        let (mut done, mut errors) = (0, 0);
        while !buf.is_empty() {
            if self.skip > 0 {
                let n = self.skip.min(buf.len());
                self.skip -= n;
                buf = &buf[n..];
                if self.skip == 0 {
                    done += self.element_done();
                }
                continue;
            }
            let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
                self.partial.extend_from_slice(buf);
                if self.partial.len() > MAX_LINE {
                    return Err("reply line too long".into());
                }
                break;
            };
            let line = if self.partial.is_empty() {
                buf[..nl].to_vec()
            } else {
                let mut line = std::mem::take(&mut self.partial);
                line.extend_from_slice(&buf[..nl]);
                line
            };
            buf = &buf[nl + 1..];
            let line = line.strip_suffix(b"\r").ok_or("reply line without CR")?;
            let (&kind, rest) = line.split_first().ok_or("empty reply line")?;
            let top_level = self.remaining == 0;
            if top_level {
                self.remaining = 1;
            }
            match kind {
                b'+' | b':' => done += self.element_done(),
                b'-' => {
                    errors += usize::from(top_level);
                    done += self.element_done();
                }
                b'$' => {
                    let len = number(rest)?;
                    if len < 0 {
                        done += self.element_done();
                    } else {
                        self.skip = len as usize + 2;
                    }
                }
                b'*' => {
                    self.remaining += number(rest)?.max(0) as u64;
                    done += self.element_done();
                }
                other => return Err(format!("unexpected reply type byte {other:#04x}")),
            }
        }
        Ok((done, errors))
    }

    /// One element of the current reply is complete; 1 if that ends it.
    fn element_done(&mut self) -> usize {
        self.remaining -= 1;
        usize::from(self.remaining == 0)
    }
}

fn number(digits: &[u8]) -> Result<i64, String> {
    std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "bad length in reply".to_string())
}

/// Read exactly one reply, fully decoded.
pub fn read_reply(stream: &mut TcpStream) -> io::Result<Reply> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        match decode_reply(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? {
            Some((reply, _)) => return Ok(reply),
            None => {
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server closed the connection",
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hcache_core::testutil::Rng;

    fn sample() -> (Vec<u8>, usize, usize) {
        let replies = [
            Reply::Ok,
            Reply::Error("ERR top".into()),
            Reply::Int(-5),
            Reply::Nil,
            Reply::Bulk(Vec::new()),
            Reply::Bulk(b"with\r\n-inside\r\n*3\r\n".to_vec()),
            Reply::Array(Vec::new()),
            Reply::Array(vec![
                Reply::Error("ERR nested does not count".into()),
                Reply::Array(vec![
                    Reply::Bulk(b"$5\r\n".to_vec()),
                    Reply::Nil,
                    Reply::Array(Vec::new()),
                ]),
                Reply::Int(7),
            ]),
            Reply::Error("ERR last".into()),
        ];
        let mut bytes = Vec::new();
        replies.iter().for_each(|r| r.encode(&mut bytes));
        (bytes, replies.len(), 2)
    }

    #[test]
    fn counts_replies_at_every_split_point() {
        let (bytes, replies, errors) = sample();
        for cut in 0..=bytes.len() {
            let mut s = Scanner::default();
            let (d1, e1) = s.feed(&bytes[..cut]).unwrap();
            let (d2, e2) = s.feed(&bytes[cut..]).unwrap();
            assert_eq!((d1 + d2, e1 + e2), (replies, errors), "cut at {cut}");
        }
    }

    #[test]
    fn counts_replies_under_random_fragmentation() {
        let (bytes, replies, errors) = sample();
        let mut rng = Rng::new(7);
        for _ in 0..2000 {
            let mut s = Scanner::default();
            let (mut done, mut errs, mut pos) = (0, 0, 0);
            while pos < bytes.len() {
                let n = 1 + rng.below(9) as usize;
                let end = (pos + n).min(bytes.len());
                let (d, e) = s.feed(&bytes[pos..end]).unwrap();
                done += d;
                errs += e;
                pos = end;
            }
            assert_eq!((done, errs), (replies, errors));
        }
    }

    #[test]
    fn rejects_garbage_instead_of_miscounting() {
        assert!(Scanner::default().feed(b"hello\r\n").is_err());
        assert!(Scanner::default().feed(b"$abc\r\n").is_err());
        assert!(Scanner::default().feed(b"+OK\n").is_err());
        assert!(Scanner::default().feed(&vec![b'+'; MAX_LINE + 2]).is_err());
    }
}
