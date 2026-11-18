//! Numbers read and printed as Valkey reads and prints them. Doubles
//! follow C's strtod grammar, hex floats included. INCRBYFLOAT and
//! HINCRBYFLOAT add in the C `long double` of x86-64, an 80-bit float with
//! a 64-bit mantissa, and print the sum with "%.17Lf": done in software
//! here, so that 0.1 plus 0.2 answers "0.3" as Valkey does, where doubles
//! would give "0.30000000000000004".

use std::cmp::Ordering;

/// The longest number string2ld reads, its buffer's size less one.
const MAX_LONG_DOUBLE_CHARS: usize = 5 * 1024;

/// C's isspace in the "C" locale.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// A binary floating-point format: `prec` bits of mantissa, `2^emin` the
/// smallest subnormal, and every finite value below `2^emax`.
struct Format {
    prec: u32,
    emin: i64,
    emax: i64,
}

const BINARY64: Format = Format {
    prec: 53,
    emin: -1074,
    emax: 1024,
};

const X87: Format = Format {
    prec: 64,
    emin: -16445,
    emax: 16384,
};

/// Just enough of an unsigned big integer for exact conversions: 32-bit
/// limbs, least significant first, no zero limbs on top.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Big(Vec<u32>);

impl Big {
    fn from_u64(v: u64) -> Big {
        let mut big = Big(vec![v as u32, (v >> 32) as u32]);
        big.trim();
        big
    }

    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    fn bits(&self) -> u64 {
        self.0.last().map_or(0, |top| {
            (self.0.len() as u64 - 1) * 32 + u64::from(32 - top.leading_zeros())
        })
    }

    fn bit(&self, i: u64) -> bool {
        self.0
            .get((i / 32) as usize)
            .is_some_and(|limb| limb >> (i % 32) & 1 == 1)
    }

    /// Whether any bit below `i` is set.
    fn any_below(&self, i: u64) -> bool {
        let whole = ((i / 32) as usize).min(self.0.len());
        self.0[..whole].iter().any(|&limb| limb != 0)
            || (!i.is_multiple_of(32)
                && self
                    .0
                    .get(whole)
                    .is_some_and(|&limb| limb << (32 - i % 32) != 0))
    }

    /// The 64 bits from bit `i` up.
    fn bits_from(&self, i: u64) -> u64 {
        (0..64)
            .filter(|&b| self.bit(i + b))
            .fold(0, |v, b| v | 1 << b)
    }

    /// `self * mul + add`.
    fn mul_add(&mut self, mul: u32, add: u32) {
        let mut carry = u64::from(add);
        for limb in &mut self.0 {
            let t = u64::from(*limb) * u64::from(mul) + carry;
            *limb = t as u32;
            carry = t >> 32;
        }
        if carry != 0 {
            self.0.push(carry as u32);
        }
    }

    /// Divides in place and returns the remainder.
    fn div_rem(&mut self, div: u32) -> u32 {
        let mut rem = 0u64;
        for limb in self.0.iter_mut().rev() {
            let t = rem << 32 | u64::from(*limb);
            *limb = (t / u64::from(div)) as u32;
            rem = t % u64::from(div);
        }
        self.trim();
        rem as u32
    }

    fn shl(&mut self, n: u64) {
        if self.is_zero() {
            return;
        }
        let bits = (n % 32) as u32;
        if bits != 0 {
            let mut carry = 0;
            for limb in &mut self.0 {
                let t = u64::from(*limb) << bits | carry;
                *limb = t as u32;
                carry = t >> 32;
            }
            if carry != 0 {
                self.0.push(carry as u32);
            }
        }
        self.0
            .splice(0..0, std::iter::repeat_n(0, (n / 32) as usize));
    }

    fn mul_pow10(&mut self, mut n: u64) {
        while n >= 9 {
            self.mul_add(1_000_000_000, 0);
            n -= 9;
        }
        self.mul_add(10u32.pow(n as u32), 0);
    }

    /// Divides by 5^n, saying whether anything was left over.
    fn div_pow5(&mut self, mut n: u64) -> bool {
        const FIVE_13: u32 = 1_220_703_125;
        let mut inexact = false;
        while n >= 13 {
            inexact |= self.div_rem(FIVE_13) != 0;
            n -= 13;
        }
        inexact | (self.div_rem(5u32.pow(n as u32)) != 0)
    }

    fn cmp(&self, other: &Big) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.iter().rev().cmp(other.0.iter().rev()))
    }

    fn add(&self, other: &Big) -> Big {
        let mut out = Vec::with_capacity(self.0.len().max(other.0.len()) + 1);
        let mut carry = 0u64;
        for i in 0..self.0.len().max(other.0.len()) {
            let t = u64::from(self.0.get(i).copied().unwrap_or(0))
                + u64::from(other.0.get(i).copied().unwrap_or(0))
                + carry;
            out.push(t as u32);
            carry = t >> 32;
        }
        out.push(carry as u32);
        let mut big = Big(out);
        big.trim();
        big
    }

    /// `self - other`, where `other` is the smaller.
    fn sub(&self, other: &Big) -> Big {
        let mut out = Vec::with_capacity(self.0.len());
        let mut borrow = 0i64;
        for (i, &limb) in self.0.iter().enumerate() {
            let mut t = i64::from(limb) - i64::from(other.0.get(i).copied().unwrap_or(0)) - borrow;
            borrow = i64::from(t < 0);
            if t < 0 {
                t += 1 << 32;
            }
            out.push(t as u32);
        }
        let mut big = Big(out);
        big.trim();
        big
    }

    fn from_digits(digits: &[u8], radix: u32) -> Big {
        let mut big = Big(Vec::new());
        // Nine decimal digits, or seven hex ones, fit a limb at a time.
        let step = if radix == 10 { 9 } else { 7 };
        for chunk in digits.chunks(step) {
            let value = chunk.iter().fold(0u32, |v, &d| {
                v * radix + char::from(d).to_digit(radix).unwrap_or(0)
            });
            big.mul_add(radix.pow(chunk.len() as u32), value);
        }
        big.trim();
        big
    }

    fn into_decimal(mut self) -> String {
        let mut chunks = Vec::new();
        while !self.is_zero() {
            chunks.push(self.div_rem(1_000_000_000));
        }
        let mut out = chunks.pop().unwrap_or(0).to_string();
        for chunk in chunks.iter().rev() {
            out.push_str(&format!("{chunk:09}"));
        }
        out
    }
}

/// A value after rounding to a format: `mantissa × 2^exp`.
enum Rounded {
    Zero,
    Inf,
    Num(u64, i64),
}

/// Rounds `q × 2^lsb` to the nearest value of the format, ties to even as
/// IEEE arithmetic does. `sticky` says the exact value is a little above
/// `q × 2^lsb`, as a division with a remainder leaves it.
fn round(q: &Big, lsb: i64, sticky: bool, f: &Format) -> Rounded {
    let n = q.bits() as i64;
    if n == 0 {
        return Rounded::Zero;
    }
    let target = (lsb + n - i64::from(f.prec)).max(f.emin);
    let drop = target - lsb;
    let (mut m, mut e) = if drop <= 0 {
        debug_assert!(!sticky, "too few bits to round");
        (u128::from(q.bits_from(0)), lsb)
    } else {
        let drop = drop as u64;
        let m = u128::from(q.bits_from(drop));
        let half = q.bit(drop - 1);
        let rest = sticky || q.any_below(drop - 1);
        (m + u128::from(half && (rest || m & 1 == 1)), target)
    };
    if m >> f.prec != 0 {
        m >>= 1;
        e += 1;
    }
    if m == 0 {
        return Rounded::Zero;
    }
    if e + i64::from(128 - m.leading_zeros()) > f.emax {
        return Rounded::Inf;
    }
    Rounded::Num(m as u64, e)
}

/// `digits × 10^exp` rounded to the format, the digits without leading
/// zeros and not all of them zero.
fn decimal(digits: &[u8], exp: i64, f: &Format) -> Rounded {
    let nd = digits.len() as i64;
    // The value lies in [10^(nd - 1 + exp), 10^(nd + exp)), and the
    // format's range in 2^[emin, emax), where log10(2) is just over 0.30103.
    if nd - 1 + exp > f.emax * 30_103 / 100_000 + 1 {
        return Rounded::Inf;
    }
    if nd + exp < f.emin * 30_103 / 100_000 - 2 {
        return Rounded::Zero;
    }
    let mut q = Big::from_digits(digits, 10);
    if exp >= 0 {
        q.mul_pow10(exp as u64);
        return round(&q, 0, false, f);
    }
    // digits / 10^k = (digits × 2^shift / 5^k) × 2^-(shift + k), with the
    // shift leaving the quotient a few bits more than the format keeps;
    // log2(5) is just under 2.322.
    let k = (-exp) as u64;
    let shift = (k * 2322 / 1000 + 1 + u64::from(f.prec) + 3).saturating_sub(q.bits());
    q.shl(shift);
    let inexact = q.div_pow5(k);
    round(&q, -(shift as i64) - k as i64, inexact, f)
}

/// A number as its text spells it, before rounding.
enum Text {
    Inf,
    /// `digits × 10^exp`, the digits without leading or trailing zeros:
    /// none at all for zero.
    Dec {
        neg: bool,
        digits: Vec<u8>,
        exp: i64,
    },
    /// `mantissa × 2^exp`.
    Hex {
        neg: bool,
        mantissa: Big,
        exp: i64,
    },
}

/// strtod's grammar over the whole of `s`: None for what strtod would
/// stop short of the end on, and for NaN, which every caller refuses.
fn lex(s: &[u8]) -> Option<Text> {
    let (neg, s) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, s),
    };
    if s.eq_ignore_ascii_case(b"inf") || s.eq_ignore_ascii_case(b"infinity") {
        return Some(Text::Inf);
    }
    let hex = s.len() > 2 && s[0] == b'0' && s[1] | 0x20 == b'x';
    let body = if hex { &s[2..] } else { s };
    let digit = |b: &u8| {
        if hex {
            b.is_ascii_hexdigit()
        } else {
            b.is_ascii_digit()
        }
    };
    let int_len = body.iter().take_while(|b| digit(b)).count();
    let (int, rest) = body.split_at(int_len);
    let (frac, rest) = match rest.split_first() {
        Some((b'.', after)) => after.split_at(after.iter().take_while(|b| digit(b)).count()),
        _ => (&rest[..0], rest),
    };
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    let marker = if hex { b'p' } else { b'e' };
    let exp = match rest.split_first() {
        None => 0,
        Some((&m, after)) if m | 0x20 == marker => exponent(after)?,
        Some(_) => return None,
    };
    let all: Vec<u8> = int.iter().chain(frac).copied().collect();
    let first = all.iter().position(|&d| d != b'0').unwrap_or(all.len());
    if hex {
        let exp = exp - 4 * frac.len() as i64;
        let mantissa = Big::from_digits(&all[first..], 16);
        return Some(Text::Hex { neg, mantissa, exp });
    }
    let last = all
        .iter()
        .rposition(|&d| d != b'0')
        .map_or(first, |i| i + 1);
    let exp = exp - frac.len() as i64 + (all.len() - last) as i64;
    Some(Text::Dec {
        neg,
        digits: all[first..last].to_vec(),
        exp,
    })
}

/// An exponent after its e or p, sign included. Huge ones are clamped,
/// still far out of any format's range.
fn exponent(s: &[u8]) -> Option<i64> {
    const CLAMP: i64 = 1 << 40;
    let (neg, digits) = match s.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, s),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let v = digits
        .iter()
        .fold(0, |v: i64, &d| (v * 10 + i64::from(d - b'0')).min(CLAMP));
    Some(if neg { -v } else { v })
}

/// The double `mantissa × 2^exp`, which fits binary64.
fn to_f64(neg: bool, mantissa: u64, exp: i64) -> f64 {
    let n = 64 - i64::from(mantissa.leading_zeros());
    let top = exp + n - 1;
    let bits = if top >= -1022 {
        ((top + 1023) as u64) << 52 | (mantissa << (53 - n)) & (HIDDEN - 1)
    } else {
        mantissa << (exp + 1074)
    };
    f64::from_bits(bits | u64::from(neg) << 63)
}

const HIDDEN: u64 = 1 << 52;

/// A hex float rounded to a double; None when out of range.
fn hex_f64(neg: bool, mantissa: &Big, exp: i64) -> Option<f64> {
    if mantissa.is_zero() {
        return Some(if neg { -0.0 } else { 0.0 });
    }
    match round(mantissa, exp, false, &BINARY64) {
        Rounded::Num(m, e) => Some(to_f64(neg, m, e)),
        Rounded::Zero | Rounded::Inf => None,
    }
}

/// Whether the text starts, past its sign, with "0x", which Valkey hands
/// to the C library's strtod rather than its own fast parser.
fn is_hex(s: &[u8]) -> bool {
    let s = s
        .strip_prefix(b"-")
        .or_else(|| s.strip_prefix(b"+"))
        .unwrap_or(s);
    s.len() >= 2 && s[0] == b'0' && s[1] | 0x20 == b'x'
}

/// Valkey's string2d: the whole text one number, no blanks before it,
/// neither NaN nor out of range. What ZADD scores, ZINCRBY, WEIGHTS and
/// GEOADD coordinates must be.
pub fn parse_double(s: &[u8]) -> Option<f64> {
    if s.first().is_none_or(|&b| is_space(b)) {
        return None;
    }
    if is_hex(s) {
        if s.len() >= MAX_LONG_DOUBLE_CHARS {
            return None;
        }
        return match lex(s)? {
            Text::Hex { neg, mantissa, exp } => hex_f64(neg, &mantissa, exp),
            Text::Inf | Text::Dec { .. } => None,
        };
    }
    // Rust's parser reads the grammar of Valkey's fast_float and rounds
    // the same; only the range errors are left to find.
    let v: f64 = std::str::from_utf8(s).ok()?.parse().ok()?;
    if v.is_nan() {
        return None;
    }
    if v.is_infinite() && !matches!(lex(s), Some(Text::Inf)) {
        return None;
    }
    let mantissa = s.split(|&b| b | 0x20 == b'e').next().unwrap_or(s);
    if v == 0.0 && mantissa.iter().any(|b| matches!(b, b'1'..=b'9')) {
        return None;
    }
    Some(v)
}

/// A score bound as ZRANGEBYSCORE and its kin read it: strtod with only
/// NaN refused, so blanks may lead and a value out of range becomes an
/// infinity or a zero. As in C the text ends at a zero byte, and nothing
/// before it reads as zero.
pub fn parse_bound(s: &[u8]) -> Option<f64> {
    let s = &s[..s.iter().position(|&b| b == 0).unwrap_or(s.len())];
    if s.is_empty() {
        return Some(0.0);
    }
    let s = &s[s.iter().take_while(|&&b| is_space(b)).count()..];
    match lex(s)? {
        Text::Hex { neg, mantissa, exp } => Some(match round(&mantissa, exp, false, &BINARY64) {
            Rounded::Num(m, e) => to_f64(neg, m, e),
            Rounded::Inf if neg => f64::NEG_INFINITY,
            Rounded::Inf => f64::INFINITY,
            Rounded::Zero if neg => -0.0,
            Rounded::Zero => 0.0,
        }),
        Text::Inf | Text::Dec { .. } => std::str::from_utf8(s).ok()?.parse().ok(),
    }
}

/// A C `long double` as x86-64 has it: `mantissa × 2^exp`, the mantissa
/// no wider than 64 bits. Valkey on arm64 has a 128-bit one instead and
/// can print a digit differently in the far decimals; Hotstash follows x86.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LongDouble {
    mantissa: u64,
    exp: i32,
    neg: bool,
}

/// Why string2ld refused a number, or what it read instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LongDoubleError {
    /// Not a number, NaN, or out of range.
    NotFloat,
    /// "inf", which string2ld reads, though no sum may come to it.
    Infinite,
}

impl LongDouble {
    pub const ZERO: LongDouble = LongDouble {
        mantissa: 0,
        exp: 0,
        neg: false,
    };

    /// Valkey's string2ld.
    pub fn parse(s: &[u8]) -> Result<LongDouble, LongDoubleError> {
        use LongDoubleError::*;
        if s.len() >= MAX_LONG_DOUBLE_CHARS || s.first().is_none_or(|&b| is_space(b)) {
            return Err(NotFloat);
        }
        let (neg, zero, rounded) = match lex(s).ok_or(NotFloat)? {
            Text::Inf => return Err(Infinite),
            Text::Dec { neg, digits, exp } => {
                let zero = digits.is_empty();
                (
                    neg,
                    zero,
                    if zero {
                        Rounded::Zero
                    } else {
                        decimal(&digits, exp, &X87)
                    },
                )
            }
            Text::Hex { neg, mantissa, exp } => {
                (neg, mantissa.is_zero(), round(&mantissa, exp, false, &X87))
            }
        };
        match rounded {
            Rounded::Num(mantissa, exp) => Ok(LongDouble::new(neg, mantissa, exp)),
            Rounded::Zero if zero => Ok(LongDouble { neg, ..Self::ZERO }),
            // ERANGE: an overflow, or an underflow all the way to zero.
            Rounded::Zero | Rounded::Inf => Err(NotFloat),
        }
    }

    /// The double's exact value; it must be finite.
    pub fn from_f64(x: f64) -> LongDouble {
        let bits = x.to_bits();
        let field = ((bits >> 52) & 0x7ff) as i64;
        let frac = bits & (HIDDEN - 1);
        let (mantissa, exp) = if field == 0 {
            (frac, -1074)
        } else {
            (frac | HIDDEN, field - 1075)
        };
        LongDouble::new(bits >> 63 == 1, mantissa, exp)
    }

    /// One value, one form: the mantissa odd, so that equal values compare
    /// equal. The X87 format keeps every exponent within an i32.
    fn new(neg: bool, mantissa: u64, exp: i64) -> LongDouble {
        let zeros = if mantissa == 0 {
            0
        } else {
            mantissa.trailing_zeros()
        };
        LongDouble {
            mantissa: mantissa >> zeros,
            exp: (exp + i64::from(zeros)) as i32,
            neg,
        }
    }

    /// The sum, rounded as the x87 unit rounds it; None when it overflows.
    pub fn checked_add(self, other: LongDouble) -> Option<LongDouble> {
        if other.mantissa == 0 {
            // x + 0 is x, and only -0 + -0 stays negative.
            let neg = self.neg && (self.mantissa != 0 || other.neg);
            return Some(LongDouble { neg, ..self });
        }
        if self.mantissa == 0 {
            return Some(other);
        }
        let (hi, lo) = if self.exp >= other.exp {
            (self, other)
        } else {
            (other, self)
        };
        let gap = (i64::from(hi.exp) - i64::from(lo.exp)) as u64;
        // Then `lo` is under a quarter of `hi`'s last bit and cannot move it.
        if gap > 192 {
            return Some(hi);
        }
        let mut a = Big::from_u64(hi.mantissa);
        a.shl(gap);
        let b = Big::from_u64(lo.mantissa);
        let (neg, sum) = if hi.neg == lo.neg {
            (hi.neg, a.add(&b))
        } else {
            match a.cmp(&b) {
                Ordering::Greater => (hi.neg, a.sub(&b)),
                Ordering::Less => (lo.neg, b.sub(&a)),
                Ordering::Equal => return Some(Self::ZERO),
            }
        };
        match round(&sum, lo.exp.into(), false, &X87) {
            Rounded::Num(mantissa, exp) => Some(LongDouble::new(neg, mantissa, exp)),
            Rounded::Inf => None,
            Rounded::Zero => Some(Self::ZERO),
        }
    }

    /// "%.17Lf" less its trailing zeros, and "-0" as "0": Valkey's
    /// LD_STR_HUMAN.
    pub fn to_human(self) -> Vec<u8> {
        const SCALE: u128 = 100_000_000_000_000_000;
        if self.mantissa == 0 {
            return b"0".to_vec();
        }
        let sign = if self.neg { "-" } else { "" };
        if self.exp >= 0 {
            let mut n = Big::from_u64(self.mantissa);
            n.shl(self.exp as u64);
            return format!("{sign}{}", n.into_decimal()).into_bytes();
        }
        // Seventeen decimals of mantissa / 2^k, rounded half to even.
        let k = self.exp.unsigned_abs();
        let n = u128::from(self.mantissa) * SCALE;
        let q = if k >= 128 {
            0
        } else {
            let q = n >> k;
            let rem = n & ((1 << k) - 1);
            let half = 1 << (k - 1);
            q + u128::from(rem > half || (rem == half && q & 1 == 1))
        };
        let text = format!("{sign}{}.{:017}", q / SCALE, q % SCALE);
        let text = text.trim_end_matches('0').trim_end_matches('.');
        if text == "-0" {
            return b"0".to_vec();
        }
        text.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ld(s: &str) -> LongDouble {
        LongDouble::parse(s.as_bytes()).unwrap()
    }

    fn sum(a: &str, b: &str) -> String {
        String::from_utf8(ld(a).checked_add(ld(b)).unwrap().to_human()).unwrap()
    }

    #[test]
    fn long_double_sums_print_as_valkey() {
        assert_eq!(sum("0.1", "0.2"), "0.3");
        assert_eq!(sum("10.5", "0.1"), "10.6");
        assert_eq!(sum("0", "9223372036854775807"), "9223372036854775807");
        assert_eq!(sum("-9223372036854775808", "0"), "-9223372036854775808");
        assert_eq!(sum("1000", "0x10"), "1016");
        assert_eq!(sum("3.0e3", "1"), "3001");
        assert_eq!(sum("5", "-5"), "0");
        assert_eq!(sum("-0", "-0"), "0");
        assert_eq!(sum("1e-20", "0"), "0");
        assert_eq!(sum("-1e-20", "0"), "0");
        assert_eq!(sum("1.5", "1e300").len(), 301);
        // The nearest long double to 10^4000 is a little below it.
        let big = sum("1e4000", "0");
        assert!(
            big.starts_with("9999999999999999999") && big.len() == 4000,
            "{big}"
        );
        assert_eq!(ld(&big), ld("1e4000"));
        // 2^-18 has 18 decimals ending in 5: the 17th rounds to even.
        assert_eq!(sum("0.000003814697265625", "0"), "0.00000381469726562");
        assert_eq!(sum("0.000011444091796875", "0"), "0.00001144409179688");
        assert_eq!(ld("1e4932").checked_add(ld("1e4932")), None);
    }

    #[test]
    fn long_double_parse_follows_string2ld() {
        use LongDoubleError::*;
        for bad in [
            "", " 1", "1 ", "abc", "1e", "0x", "0x1p", "nan", "1\x002", ".", "1e5000", "1e-5000",
        ] {
            assert_eq!(LongDouble::parse(bad.as_bytes()), Err(NotFloat), "{bad:?}");
        }
        assert_eq!(LongDouble::parse(b"-inf"), Err(Infinite));
        assert_eq!(LongDouble::parse(b"Infinity"), Err(Infinite));
        let long = |len: usize| [&b"0."[..], &vec![b'1'; len - 2]].concat();
        assert_eq!(
            LongDouble::parse(&long(MAX_LONG_DOUBLE_CHARS)),
            Err(NotFloat)
        );
        let longest = String::from_utf8(long(MAX_LONG_DOUBLE_CHARS - 1)).unwrap();
        assert_eq!(ld(&longest), ld(&String::from_utf8(long(42)).unwrap()));
        for good in [
            "1.",
            ".5",
            "+3",
            "0x1.8p1",
            "0X10",
            "1E3",
            "0e999999999999",
            "1e-4940",
        ] {
            assert!(LongDouble::parse(good.as_bytes()).is_ok(), "{good:?}");
        }
        assert_eq!(ld("0x1.8p1"), ld("3"));
    }

    #[test]
    fn doubles_follow_string2d() {
        assert_eq!(parse_double(b"0x10"), Some(16.0));
        assert_eq!(parse_double(b"-0x1p-2"), Some(-0.25));
        assert_eq!(parse_double(b"1.5"), Some(1.5));
        assert_eq!(parse_double(b"+inf"), Some(f64::INFINITY));
        assert_eq!(parse_double(b"-Infinity"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_double(b"0e-999"), Some(0.0));
        assert_eq!(parse_double(b"4.9e-324"), Some(5e-324));
        for bad in [
            "",
            " 1",
            "1 ",
            "nan",
            "1e400",
            "1e-400",
            "0x1p-2000",
            "0x",
            "abc",
            "1e",
        ] {
            assert_eq!(parse_double(bad.as_bytes()), None, "{bad:?}");
        }
    }

    #[test]
    fn bounds_follow_strtod() {
        assert_eq!(parse_bound(b" 1.5"), Some(1.5));
        assert_eq!(parse_bound(b"1e400"), Some(f64::INFINITY));
        assert_eq!(parse_bound(b"1e-400"), Some(0.0));
        assert_eq!(parse_bound(b"0x10"), Some(16.0));
        assert_eq!(parse_bound(b"2\x00junk"), Some(2.0));
        assert_eq!(parse_bound(b"\x00"), Some(0.0));
        assert_eq!(parse_bound(b" "), None);
        assert_eq!(parse_bound(b"nan"), None);
        assert_eq!(parse_bound(b"x"), None);
    }

    #[test]
    fn decimal_parse_rounds_correctly() {
        // Against Rust's own correctly rounded parser, through binary64.
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..20_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let d = f64::from_bits(x);
            if !d.is_finite() || d == 0.0 {
                continue;
            }
            let text = format!("{d:e}");
            let Some(Text::Dec { neg, digits, exp }) = lex(text.as_bytes()) else {
                panic!("{text}");
            };
            let Rounded::Num(m, e) = decimal(&digits, exp, &BINARY64) else {
                panic!("{text}");
            };
            assert_eq!(to_f64(neg, m, e).to_bits(), d.to_bits(), "{text}");
        }
    }
}
