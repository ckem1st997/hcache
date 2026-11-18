//! The bit arithmetic behind BITFIELD / BITFIELD_RO.
//!
//! Fields are 1 to 64 bits wide (63 for unsigned) and can start at any bit.
//! Bit 0 is the most significant bit of byte 0, the same order as SETBIT.
//! Everything here is pure: callers parse the arguments, pick the offset,
//! and decide what to do with an overflow reply.

use crate::cmd::{syntax, NOT_INT};
use crate::resp::{parse_i64, Reply};

/// The largest string, in bytes (512 MiB), so offsets stay below 2^32 bits.
pub const MAX_BYTES: u64 = 512 << 20;

/// Widest signed field.
const MAX_SIGNED_BITS: u8 = 64;
/// Widest unsigned field (the reply is a signed 64-bit integer).
const MAX_UNSIGNED_BITS: u8 = 63;
const BITS_PER_BYTE: u64 = 8;
/// Shift that turns a bit offset into a byte index.
const BYTE_SHIFT: u32 = 3;
/// Mask that picks the bit inside its byte.
const BIT_IN_BYTE_MASK: u64 = 7;
/// Bit position of the most significant bit of a byte.
const TOP_BIT_IN_BYTE: u64 = 7;
/// Marks an offset counted in fields instead of bits ("#3").
const HASH: u8 = b'#';
/// Longest decimal text that can still be a valid width or offset step.
const MAX_WIDTH_DIGITS: usize = 2;

/// A field type: `signed` picks i or u, `bits` is the width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ty {
    pub signed: bool,
    pub bits: u8,
}

/// What to do when an INCRBY or SET does not fit the field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    #[default]
    Wrap,
    Sat,
    Fail,
}

/// Parse a whole decimal number the way Redis's `string2ll` does: digits
/// only, no sign, no spaces, no leading zero (except "0" itself).
fn parse_plain_u64(raw: &[u8]) -> Option<u64> {
    let (&first, _) = raw.split_first()?;
    if raw.len() > 1 && first == b'0' {
        return None;
    }
    raw.iter().try_fold(0u64, |acc, &b| {
        if !b.is_ascii_digit() {
            return None;
        }
        acc.checked_mul(10)?.checked_add(u64::from(b - b'0'))
    })
}

impl Ty {
    /// Parse "i8", "u16", ... The letter must be lower case, as in Redis.
    /// Returns `None` for anything else (u64, i0, i65, "i08", "i+8", "i-1").
    pub fn parse(raw: &[u8]) -> Option<Ty> {
        let (&letter, digits) = raw.split_first()?;
        let signed = match letter {
            b'i' => true,
            b'u' => false,
            _ => return None,
        };
        if digits.len() > MAX_WIDTH_DIGITS {
            return None;
        }
        let bits = u8::try_from(parse_plain_u64(digits)?).ok()?;
        let widest = if signed {
            MAX_SIGNED_BITS
        } else {
            MAX_UNSIGNED_BITS
        };
        (1..=widest).contains(&bits).then_some(Ty { signed, bits })
    }

    /// Width kept inside the range the arithmetic below can handle.
    fn width(self) -> u32 {
        u32::from(self.bits.clamp(1, MAX_SIGNED_BITS))
    }
}

impl Overflow {
    /// "WRAP" | "SAT" | "FAIL", case-insensitive.
    pub fn parse(raw: &[u8]) -> Option<Overflow> {
        if raw.eq_ignore_ascii_case(b"WRAP") {
            Some(Overflow::Wrap)
        } else if raw.eq_ignore_ascii_case(b"SAT") {
            Some(Overflow::Sat)
        } else if raw.eq_ignore_ascii_case(b"FAIL") {
            Some(Overflow::Fail)
        } else {
            None
        }
    }
}

/// A bit offset argument: a non-negative integer, or "#n" meaning n * ty.bits.
/// `None` when it is not a number, is negative, or when the field would reach
/// past the last bit of a [`MAX_BYTES`] string.
pub fn parse_offset(raw: &[u8], ty: Ty) -> Option<u64> {
    let width = u64::from(ty.width());
    let offset = match raw.split_first() {
        Some((&HASH, rest)) => parse_plain_u64(rest)?.checked_mul(width)?,
        _ => parse_plain_u64(raw)?,
    };
    let end = offset.checked_add(width)?;
    (end <= MAX_BYTES * BITS_PER_BYTE).then_some(offset)
}

/// The bit at position `pos`, or 0 past the end of `data`.
fn bit_at(data: &[u8], pos: u64) -> u64 {
    let byte = usize::try_from(pos >> BYTE_SHIFT)
        .ok()
        .and_then(|i| data.get(i));
    byte.map_or(0, |b| {
        u64::from(b >> (TOP_BIT_IN_BYTE - (pos & BIT_IN_BYTE_MASK)) & 1)
    })
}

/// The field at `offset`; bits past the end of `data` read as zero. An
/// unsigned field (at most 63 bits) is returned as a non-negative i64; a
/// signed one is sign-extended.
pub fn get(data: &[u8], offset: u64, ty: Ty) -> i64 {
    let bits = ty.width();
    let mut value = (0..u64::from(bits)).fold(0u64, |acc, i| {
        (acc << 1) | bit_at(data, offset.saturating_add(i))
    });
    let sign_set = (value >> (bits - 1)) & 1 == 1;
    if ty.signed && bits < u32::from(MAX_SIGNED_BITS) && sign_set {
        value |= u64::MAX << bits;
    }
    value as i64
}

/// Write the low `ty.bits` bits of `value` at `offset`, growing `data` with
/// zero bytes as needed. The caller guarantees the offset came from
/// [`parse_offset`].
pub fn set(data: &mut Vec<u8>, offset: u64, ty: Ty, value: i64) {
    let bits = ty.width();
    let last_bit = offset.saturating_add(u64::from(bits) - 1);
    let Ok(last_byte) = usize::try_from(last_bit >> BYTE_SHIFT) else {
        return;
    };
    if data.len() <= last_byte {
        data.resize(last_byte + 1, 0);
    }
    let raw = value as u64;
    for i in 0..bits {
        let pos = offset + u64::from(i);
        let bit = ((raw >> (bits - 1 - i)) & 1) as u8;
        let shift = TOP_BIT_IN_BYTE - (pos & BIT_IN_BYTE_MASK);
        // The resize above makes this index valid.
        if let Some(byte) = data.get_mut((pos >> BYTE_SHIFT) as usize) {
            *byte = (*byte & !(1 << shift)) | (bit << shift);
        }
    }
}

/// What `old + by` becomes in a field of type `ty` under `overflow`:
/// `Some(new value)` (wrapped for Wrap, clamped to the type's range for Sat),
/// or `None` for Fail when it would overflow.
///
/// With `by == 0` this is also how SET treats a value that does not fit. For
/// an unsigned type `old` is read as an unsigned 64-bit number, so a negative
/// SET value counts as a huge one and overflows, as in Redis.
pub fn apply(old: i64, by: i64, ty: Ty, overflow: Overflow) -> Option<i64> {
    let bits = ty.width();
    let (base, min, max) = if ty.signed {
        (
            i128::from(old),
            -(1i128 << (bits - 1)),
            (1i128 << (bits - 1)) - 1,
        )
    } else {
        (i128::from(old as u64), 0, (1i128 << bits) - 1)
    };
    let sum = base + i128::from(by);
    if (min..=max).contains(&sum) {
        return i64::try_from(sum).ok();
    }
    match overflow {
        Overflow::Fail => None,
        Overflow::Sat => i64::try_from(sum.clamp(min, max)).ok(),
        Overflow::Wrap => {
            let low = sum & ((1i128 << bits) - 1);
            let wrapped = if ty.signed && low > max {
                low - (1i128 << bits)
            } else {
                low
            };
            i64::try_from(wrapped).ok()
        }
    }
}

/// One step of a BITFIELD command: the field's type and bit offset, and
/// for a write the number and what to do when it does not fit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Get(Ty, u64),
    Set(Ty, u64, i64, Overflow),
    IncrBy(Ty, u64, i64, Overflow),
}

impl Op {
    pub fn writes(&self) -> bool {
        !matches!(self, Op::Get(..))
    }

    /// How long the string must be for this step to write its field.
    fn reach(&self) -> usize {
        match *self {
            Op::Get(..) => 0,
            Op::Set(ty, offset, ..) | Op::IncrBy(ty, offset, ..) => {
                let end = offset.saturating_add(u64::from(ty.width()));
                usize::try_from(end.div_ceil(BITS_PER_BYTE)).unwrap_or(usize::MAX)
            }
        }
    }
}

/// The steps of BITFIELD, from the arguments after the key. BITFIELD_RO
/// takes GET alone.
pub fn parse_ops(args: &[Vec<u8>], read_only: bool) -> Result<Vec<Op>, Reply> {
    let is = |arg: &[u8], word: &str| arg.eq_ignore_ascii_case(word.as_bytes());
    let mut ops = Vec::new();
    let mut overflow = Overflow::default();
    let mut rest = args;
    while let Some(word) = rest.first() {
        if read_only && !is(word, "GET") {
            return Err(Reply::err(
                "ERR BITFIELD_RO only supports the GET subcommand",
            ));
        }
        if is(word, "OVERFLOW") && rest.len() >= 2 {
            overflow = Overflow::parse(&rest[1])
                .ok_or_else(|| Reply::err("ERR Invalid OVERFLOW type specified"))?;
            rest = &rest[2..];
            continue;
        }
        let width = if is(word, "GET") {
            3
        } else if is(word, "SET") || is(word, "INCRBY") {
            4
        } else {
            return Err(syntax());
        };
        if rest.len() < width {
            return Err(syntax());
        }
        let ty = Ty::parse(&rest[1]).ok_or_else(|| {
            Reply::err(
                "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not \
                 supported but i64 is.",
            )
        })?;
        let offset = parse_offset(&rest[2], ty)
            .ok_or_else(|| Reply::err("ERR bit offset is not an integer or out of range"))?;
        ops.push(if width == 3 {
            Op::Get(ty, offset)
        } else {
            let number = parse_i64(&rest[3]).ok_or_else(|| Reply::err(NOT_INT))?;
            if is(word, "SET") {
                Op::Set(ty, offset, number, overflow)
            } else {
                Op::IncrBy(ty, offset, number, overflow)
            }
        });
        rest = &rest[width..];
    }
    Ok(ops)
}

/// The reply of steps that only read.
pub fn read(data: &[u8], ops: &[Op]) -> Reply {
    let fields = ops.iter().map(|op| match *op {
        Op::Get(ty, offset) => Reply::Int(get(data, offset, ty)),
        Op::Set(..) | Op::IncrBy(..) => Reply::Nil,
    });
    Reply::Array(fields.collect())
}

/// Run the steps on `data`. As in Redis, the string first grows as far as
/// the writes reach, also when an overflow then keeps one from happening.
pub fn run(data: &mut Vec<u8>, ops: &[Op]) -> Reply {
    let reach = ops.iter().map(Op::reach).max().unwrap_or(0);
    if data.len() < reach {
        data.resize(reach, 0);
    }
    let mut out = Vec::with_capacity(ops.len());
    for op in ops {
        out.push(match *op {
            Op::Get(ty, offset) => Reply::Int(get(data, offset, ty)),
            Op::Set(ty, offset, number, overflow) => {
                let old = get(data, offset, ty);
                match apply(number, 0, ty, overflow) {
                    Some(fitted) => {
                        set(data, offset, ty, fitted);
                        Reply::Int(old)
                    }
                    None => Reply::Nil,
                }
            }
            Op::IncrBy(ty, offset, by, overflow) => {
                match apply(get(data, offset, ty), by, ty, overflow) {
                    Some(sum) => {
                        set(data, offset, ty, sum);
                        Reply::Int(sum)
                    }
                    None => Reply::Nil,
                }
            }
        });
    }
    Reply::Array(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn ty(signed: bool, bits: u8) -> Ty {
        Ty { signed, bits }
    }

    fn all_types() -> Vec<Ty> {
        let signed = (1..=MAX_SIGNED_BITS).map(|b| ty(true, b));
        let unsigned = (1..=MAX_UNSIGNED_BITS).map(|b| ty(false, b));
        signed.chain(unsigned).collect()
    }

    struct XorShift(u64);

    impl XorShift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn incr(data: &mut Vec<u8>, offset: u64, t: Ty, by: i64, ov: Overflow) -> Option<i64> {
        let new = apply(get(data, offset, t), by, t, ov)?;
        set(data, offset, t, new);
        Some(new)
    }

    #[test]
    fn redis_docs_example() {
        let mut data = Vec::new();
        let i5 = ty(true, 5);
        assert_eq!(incr(&mut data, 100, i5, 1, Overflow::Wrap), Some(1));
        assert_eq!(get(&data, 0, ty(false, 4)), 0);
        assert_eq!(get(&data, 100, i5), 1);
    }

    #[test]
    fn u2_at_102_all_overflow_modes() {
        let u2 = ty(false, 2);
        let mut sat = Vec::new();
        let got: Vec<_> = (0..4)
            .map(|_| incr(&mut sat, 102, u2, 1, Overflow::Sat))
            .collect();
        assert_eq!(got, [Some(1), Some(2), Some(3), Some(3)]);

        let mut fail = Vec::new();
        let got: Vec<_> = (0..4)
            .map(|_| incr(&mut fail, 102, u2, 1, Overflow::Fail))
            .collect();
        assert_eq!(got, [Some(1), Some(2), Some(3), None]);

        let mut wrap = Vec::new();
        let got: Vec<_> = (0..4)
            .map(|_| incr(&mut wrap, 102, u2, 1, Overflow::Wrap))
            .collect();
        assert_eq!(got, [Some(1), Some(2), Some(3), Some(0)]);
    }

    #[test]
    fn signed_limits() {
        let i8t = ty(true, 8);
        assert_eq!(apply(127, 1, i8t, Overflow::Sat), Some(127));
        assert_eq!(apply(-128, -1, i8t, Overflow::Sat), Some(-128));
        assert_eq!(apply(127, 1, i8t, Overflow::Wrap), Some(-128));
        assert_eq!(apply(-128, -1, i8t, Overflow::Wrap), Some(127));
        assert_eq!(apply(127, 1, i8t, Overflow::Fail), None);
        assert_eq!(apply(-128, -1, i8t, Overflow::Fail), None);
        assert_eq!(apply(100, 27, i8t, Overflow::Fail), Some(127));

        let i64t = ty(true, 64);
        assert_eq!(apply(i64::MAX, 1, i64t, Overflow::Wrap), Some(i64::MIN));
        assert_eq!(apply(i64::MIN, -1, i64t, Overflow::Wrap), Some(i64::MAX));
        assert_eq!(apply(i64::MAX, 1, i64t, Overflow::Sat), Some(i64::MAX));
        assert_eq!(apply(i64::MIN, -1, i64t, Overflow::Sat), Some(i64::MIN));
        assert_eq!(apply(i64::MAX, 1, i64t, Overflow::Fail), None);
        assert_eq!(apply(i64::MIN, -1, i64t, Overflow::Fail), None);
        assert_eq!(
            apply(i64::MAX, i64::MAX, i64t, Overflow::Sat),
            Some(i64::MAX)
        );
        assert_eq!(apply(i64::MIN, i64::MIN, i64t, Overflow::Wrap), Some(0));
    }

    #[test]
    fn unsigned_limits() {
        let u63 = ty(false, 63);
        let max = i64::MAX;
        assert_eq!(apply(max, 1, u63, Overflow::Sat), Some(max));
        assert_eq!(apply(max, 1, u63, Overflow::Wrap), Some(0));
        assert_eq!(apply(max, 1, u63, Overflow::Fail), None);
        assert_eq!(apply(0, -1, u63, Overflow::Sat), Some(0));
        assert_eq!(apply(0, -1, u63, Overflow::Wrap), Some(max));
        assert_eq!(apply(0, -1, u63, Overflow::Fail), None);

        let u8t = ty(false, 8);
        assert_eq!(apply(5, -6, u8t, Overflow::Wrap), Some(255));
        assert_eq!(apply(250, 10, u8t, Overflow::Wrap), Some(4));
        assert_eq!(apply(250, 5, u8t, Overflow::Fail), Some(255));
    }

    #[test]
    fn set_values_that_do_not_fit() {
        let u8t = ty(false, 8);
        // SET u8 300: wraps to 44, saturates to 255, fails.
        assert_eq!(apply(300, 0, u8t, Overflow::Wrap), Some(44));
        assert_eq!(apply(300, 0, u8t, Overflow::Sat), Some(255));
        assert_eq!(apply(300, 0, u8t, Overflow::Fail), None);
        // A negative value is a huge unsigned number.
        assert_eq!(apply(-1, 0, u8t, Overflow::Wrap), Some(255));
        assert_eq!(apply(-1, 0, u8t, Overflow::Sat), Some(255));
        assert_eq!(apply(-1, 0, u8t, Overflow::Fail), None);
        let i8t = ty(true, 8);
        assert_eq!(apply(200, 0, i8t, Overflow::Wrap), Some(-56));
        assert_eq!(apply(200, 0, i8t, Overflow::Sat), Some(127));
        assert_eq!(apply(-200, 0, i8t, Overflow::Sat), Some(-128));
        assert_eq!(apply(-200, 0, i8t, Overflow::Fail), None);
        assert_eq!(apply(5, 0, i8t, Overflow::Fail), Some(5));
    }

    #[test]
    fn round_trip_every_type() {
        let offsets = [0u64, 1, 7, 8, 9, 63, 64, 65, 1000];
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
        for t in all_types() {
            let bits = u32::from(t.bits);
            for &offset in &offsets {
                for fill in [0xFFu8, 0x00] {
                    let len = ((offset + 64) / 8 + 2) as usize;
                    let mut data = vec![fill; len];
                    let raw = rng.next();
                    let expected = if t.signed {
                        ((raw << (64 - bits)) as i64) >> (64 - bits)
                    } else {
                        (raw & ((1u64 << bits) - 1)) as i64
                    };
                    set(&mut data, offset, t, raw as i64);
                    assert_eq!(data.len(), len, "{t:?} at {offset}");
                    assert_eq!(get(&data, offset, t), expected, "{t:?} at {offset}");
                    let fill_bit = u64::from(fill & 1);
                    for pos in 0..(len as u64 * 8) {
                        let inside = pos >= offset && pos < offset + u64::from(bits);
                        if !inside {
                            assert_eq!(bit_at(&data, pos), fill_bit, "{t:?} at {offset} bit {pos}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn set_grows_with_zero_bytes() {
        let mut data = Vec::new();
        set(&mut data, 1000, ty(false, 8), 0xFF);
        assert_eq!(data.len(), 126);
        assert!(data[..124].iter().all(|&b| b == 0));
        assert_eq!(get(&data, 1000, ty(false, 8)), 255);
    }

    #[test]
    fn get_beyond_the_end_is_zero() {
        let data = [0xFFu8];
        for t in all_types() {
            assert_eq!(get(&data, 100, t), 0, "{t:?}");
            assert_eq!(get(&[], 0, t), 0, "{t:?}");
        }
    }

    #[test]
    fn get_straddling_the_end_reads_zero_bits() {
        let data = [0xFFu8];
        assert_eq!(get(&data, 4, ty(false, 8)), 0xF0);
        assert_eq!(get(&data, 4, ty(true, 8)), -16);
        assert_eq!(get(&data, 7, ty(false, 3)), 0b100);
    }

    #[test]
    fn bit_order_is_big_endian() {
        let data = [0b1010_0000u8];
        assert_eq!(get(&data, 0, ty(false, 1)), 1);
        assert_eq!(get(&data, 1, ty(false, 1)), 0);
        assert_eq!(get(&data, 0, ty(false, 3)), 5);
        assert_eq!(get(&data, 0, ty(true, 3)), -3);
    }

    #[test]
    fn parse_type() {
        assert_eq!(Ty::parse(b"i8"), Some(ty(true, 8)));
        assert_eq!(Ty::parse(b"u16"), Some(ty(false, 16)));
        assert_eq!(Ty::parse(b"i64"), Some(ty(true, 64)));
        assert_eq!(Ty::parse(b"u63"), Some(ty(false, 63)));
        assert_eq!(Ty::parse(b"i1"), Some(ty(true, 1)));
        for bad in [
            &b"u64"[..],
            b"i65",
            b"i0",
            b"u0",
            b"x8",
            b"i",
            b"",
            b"i-1",
            b"i+8",
            b"i08",
            b"I8",
            b"U8",
            b"i 8",
            b"i8 ",
            b"i999999999999999999999",
        ] {
            assert_eq!(Ty::parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn parse_overflow_mode() {
        assert_eq!(Overflow::parse(b"wrap"), Some(Overflow::Wrap));
        assert_eq!(Overflow::parse(b"SAT"), Some(Overflow::Sat));
        assert_eq!(Overflow::parse(b"Fail"), Some(Overflow::Fail));
        assert_eq!(Overflow::parse(b"sat "), None);
        assert_eq!(Overflow::parse(b""), None);
        assert_eq!(Overflow::default(), Overflow::Wrap);
    }

    #[test]
    fn parse_offsets() {
        let i8t = ty(true, 8);
        assert_eq!(parse_offset(b"0", i8t), Some(0));
        assert_eq!(parse_offset(b"#0", i8t), Some(0));
        assert_eq!(parse_offset(b"#3", i8t), Some(24));
        assert_eq!(parse_offset(b"100", i8t), Some(100));
        assert_eq!(parse_offset(b"-1", i8t), None);
        assert_eq!(parse_offset(b"#-1", i8t), None);
        assert_eq!(parse_offset(b"abc", i8t), None);
        assert_eq!(parse_offset(b"", i8t), None);
        assert_eq!(parse_offset(b"#", i8t), None);
        assert_eq!(parse_offset(b"01", i8t), None);
        assert_eq!(parse_offset(b"+1", i8t), None);

        let u1 = ty(false, 1);
        let limit = MAX_BYTES * BITS_PER_BYTE;
        let last = (limit - 1).to_string();
        let past = limit.to_string();
        assert_eq!(parse_offset(last.as_bytes(), u1), Some(limit - 1));
        assert_eq!(parse_offset(past.as_bytes(), u1), None);
        assert_eq!(parse_offset(last.as_bytes(), i8t), None);

        let huge = u64::MAX.to_string();
        assert_eq!(parse_offset(format!("#{huge}").as_bytes(), i8t), None);
        assert_eq!(parse_offset(huge.as_bytes(), u1), None);
        assert_eq!(parse_offset(b"#99999999999999999999999", i8t), None);
        assert_eq!(parse_offset(b"#536870912", i8t), None);
        assert_eq!(parse_offset(b"#536870911", i8t), Some(536_870_911 * 8));
    }
}
