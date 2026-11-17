//! The maths behind the GEO commands (GEOADD, GEODIST, GEOHASH, GEOSEARCH).
//!
//! It follows Redis's `geohash.c`, `geohash_helper.c` and `geo.c` step by
//! step, floating-point operations included, so a position stored here
//! gives the same score, the same decoded point and the same distance as
//! real Redis. Nothing here touches the keyspace: it is pure functions,
//! and the parsers of the commands' arguments.

use crate::cmd::{syntax, NOT_INT};
use crate::float::LongDouble;
use crate::ops::NOT_FLOAT;
use crate::resp::{parse_i64, Reply};
use crate::zset::parse_float;

pub const LON_MIN: f64 = -180.0;
pub const LON_MAX: f64 = 180.0;
pub const LAT_MIN: f64 = -85.05112878;
pub const LAT_MAX: f64 = 85.05112878;

/// Latitude range of the standard geohash string that GEOHASH replies with.
const STD_LAT_MIN: f64 = -90.0;
const STD_LAT_MAX: f64 = 90.0;

/// Bits per axis in the stored hash.
const STEP: u32 = 26;
/// Bits in the stored hash: both axes interleaved.
const HASH_BITS: u32 = 2 * STEP;
#[cfg(test)]
const HASH_MASK: u64 = (1 << HASH_BITS) - 1;
/// Width of one half of an interleaved hash while it is being split or joined.
const AXIS_WORD_BITS: u32 = 32;

/// Earth radius in metres, the same constant Redis uses.
const EARTH_RADIUS_IN_METERS: f64 = 6_372_797.560_856;

/// Characters of a standard geohash string, by 5-bit value.
const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
const CHAR_BITS: u32 = 5;
const CHAR_MASK: u64 = (1 << CHAR_BITS) - 1;
/// Length of the geohash string.
const HASH_STRING_LEN: usize = 11;

const METRES_PER_KM: f64 = 1000.0;
const METRES_PER_FT: f64 = 0.3048;
const METRES_PER_MI: f64 = 1609.34;

/// Spreads the bits of `x` over the even bit positions of a `u64`.
fn interleave(x: u32) -> u64 {
    (0..AXIS_WORD_BITS).fold(0, |acc, i| acc | (u64::from((x >> i) & 1) << (2 * i)))
}

/// Gathers the even bit positions of `x` into a `u32` (the inverse of [`interleave`]).
fn deinterleave(x: u64) -> u32 {
    (0..AXIS_WORD_BITS).fold(0, |acc, i| acc | ((((x >> (2 * i)) & 1) as u32) << i))
}

/// Redis's `geohashEncode` with a caller-chosen latitude range. The inputs
/// are checked against the GEO limits, as Redis does.
fn encode_with(lon: f64, lat: f64, lat_min: f64, lat_max: f64) -> Option<u64> {
    if !(LON_MIN..=LON_MAX).contains(&lon) || !(LAT_MIN..=LAT_MAX).contains(&lat) {
        return None;
    }
    let scale = (1u64 << STEP) as f64;
    let lat_offset = (lat - lat_min) / (lat_max - lat_min);
    let lon_offset = (lon - LON_MIN) / (LON_MAX - LON_MIN);
    // Like the C cast: truncates. Exactly 180 degrees gives 2^26, one bit
    // past the axis width, which Redis keeps too.
    let lat_bits = (lat_offset * scale) as u32;
    let lon_bits = (lon_offset * scale) as u32;
    Some(interleave(lat_bits) | (interleave(lon_bits) << 1))
}

/// The 52-bit interleaved geohash Redis stores as the sorted-set score
/// (26 bits per axis, ranges above). None when out of range or not finite.
pub fn encode(lon: f64, lat: f64) -> Option<u64> {
    encode_with(lon, lat, LAT_MIN, LAT_MAX)
}

/// (lon, lat): the centre of the cell, as Redis's
/// `geohashDecodeToLongLatWGS84` gives it. Any u64 is accepted and none
/// panics: the bit past an axis, which `encode` sets at the very edge,
/// decodes to that edge.
pub fn decode(bits: u64) -> (f64, f64) {
    let lat_bits = deinterleave(bits);
    let lon_bits = deinterleave(bits >> 1);
    let scale = (1u64 << STEP) as f64;
    let lat_span = LAT_MAX - LAT_MIN;
    let lon_span = LON_MAX - LON_MIN;

    let lat_lo = LAT_MIN + (f64::from(lat_bits) / scale) * lat_span;
    let lat_hi = LAT_MIN + ((f64::from(lat_bits) + 1.0) / scale) * lat_span;
    let lon_lo = LON_MIN + (f64::from(lon_bits) / scale) * lon_span;
    let lon_hi = LON_MIN + ((f64::from(lon_bits) + 1.0) / scale) * lon_span;

    let lon = ((lon_lo + lon_hi) / 2.0).clamp(LON_MIN, LON_MAX);
    let lat = ((lat_lo + lat_hi) / 2.0).clamp(LAT_MIN, LAT_MAX);
    (lon, lat)
}

fn deg_rad(deg: f64) -> f64 {
    deg * (std::f64::consts::PI / 180.0)
}

/// Distance in metres along a meridian between two latitudes.
fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_IN_METERS * (deg_rad(lat2) - deg_rad(lat1)).abs()
}

/// Great-circle distance in metres, as `geohashGetDistance` (haversine,
/// `EARTH_RADIUS_IN_METERS`), including its shortcut when the two
/// longitudes are equal.
pub fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon_half = ((deg_rad(lon2) - deg_rad(lon1)) / 2.0).sin();
    if lon_half == 0.0 {
        return lat_distance(lat1, lat2);
    }
    let lat1_rad = deg_rad(lat1);
    let lat2_rad = deg_rad(lat2);
    let lat_half = ((lat2_rad - lat1_rad) / 2.0).sin();
    let a = lat_half * lat_half + lat1_rad.cos() * lat2_rad.cos() * lon_half * lon_half;
    2.0 * EARTH_RADIUS_IN_METERS * a.sqrt().asin()
}

/// The 11-character standard geohash string GEOHASH replies with: the
/// decoded position is encoded again with latitude range -90..90, then 5
/// bits per character are taken from the top. The last character is always
/// the first of the alphabet, because only 52 bits exist (Redis does the same).
pub fn hash_string(bits: u64) -> [u8; HASH_STRING_LEN] {
    let (lon, lat) = decode(bits);
    // The decoded point is inside the GEO limits, so this always succeeds;
    // zero is only a safe fallback.
    let std_bits = encode_with(lon, lat, STD_LAT_MIN, STD_LAT_MAX).unwrap_or(0);
    let mut out = [ALPHABET[0]; HASH_STRING_LEN];
    for (i, slot) in out.iter_mut().enumerate().take(HASH_STRING_LEN - 1) {
        let shift = HASH_BITS - CHAR_BITS * (i as u32 + 1);
        *slot = ALPHABET[((std_bits >> shift) & CHAR_MASK) as usize];
    }
    out
}

/// A distance unit accepted by the GEO commands.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Unit {
    M,
    Km,
    Ft,
    Mi,
}

impl Unit {
    /// "m", "km", "ft", "mi", case-insensitive.
    pub fn parse(raw: &[u8]) -> Option<Unit> {
        match raw.to_ascii_lowercase().as_slice() {
            b"m" => Some(Unit::M),
            b"km" => Some(Unit::Km),
            b"ft" => Some(Unit::Ft),
            b"mi" => Some(Unit::Mi),
            _ => None,
        }
    }

    /// Metres per unit: 1, 1000, 0.3048, 1609.34.
    pub fn metres(self) -> f64 {
        match self {
            Unit::M => 1.0,
            Unit::Km => METRES_PER_KM,
            Unit::Ft => METRES_PER_FT,
            Unit::Mi => METRES_PER_MI,
        }
    }
}

/// An area to search, measured in metres.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Shape {
    /// Radius in metres.
    Circle(f64),
    /// Width and height in metres.
    Box(f64, f64),
}

/// The distance in metres from the centre to the point when the point lies
/// inside the shape, else None. Circle: distance <= radius. Box: as
/// `geohashGetDistanceIfInRectangle`: the latitude distance along the
/// point's meridian must be <= height/2, the longitude distance along the
/// point's parallel <= width/2, then the real distance is returned.
pub fn within(shape: Shape, centre: (f64, f64), point: (f64, f64)) -> Option<f64> {
    let (lon1, lat1) = centre;
    let (lon2, lat2) = point;
    match shape {
        Shape::Circle(radius) => {
            let d = distance(lon1, lat1, lon2, lat2);
            (d <= radius).then_some(d)
        }
        Shape::Box(width, height) => {
            if lat_distance(lat2, lat1) > height / 2.0 {
                return None;
            }
            if distance(lon2, lat2, lon1, lat2) > width / 2.0 {
                return None;
            }
            Some(distance(lon1, lat1, lon2, lat2))
        }
    }
}

/// Where a search is centred.
#[derive(Debug, Clone, PartialEq)]
pub enum Origin {
    Member(Vec<u8>),
    LonLat(f64, f64),
}

/// GEOSEARCH and its older forms.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub origin: Origin,
    pub shape: Shape,
    /// The unit of the distances in the reply.
    pub unit: Unit,
    /// ASC or DESC by distance; unordered without.
    pub asc: Option<bool>,
    pub count: Option<usize>,
    /// With COUNT: the first matches found, not the nearest.
    pub any: bool,
    pub withcoord: bool,
    pub withdist: bool,
    pub withhash: bool,
}

/// The GEO commands that read one sorted set.
#[derive(Debug, Clone, PartialEq)]
pub enum GeoOp {
    Pos(Vec<Vec<u8>>),
    Hash(Vec<Vec<u8>>),
    Dist(Vec<u8>, Vec<u8>, Unit),
    Search(Query),
}

/// A member inside the searched area.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub member: Vec<u8>,
    /// Metres from the centre.
    pub dist: f64,
    pub bits: u64,
}

/// A parsed search, with where a storing form puts its result.
pub struct Parsed {
    pub query: Query,
    pub store: Option<Vec<u8>>,
    /// Store distances as scores instead of positions.
    pub storedist: bool,
}

const BAD_UNIT: &str = "ERR unsupported unit provided. please use M, KM, FT, MI";

fn is(arg: &[u8], word: &str) -> bool {
    arg.eq_ignore_ascii_case(word.as_bytes())
}

/// A longitude and latitude from two arguments.
fn position(lon: &[u8], lat: &[u8]) -> Result<(f64, f64), Reply> {
    let number = |raw: &[u8]| parse_float(raw).ok_or_else(|| Reply::err(NOT_FLOAT));
    let (lon, lat) = (number(lon)?, number(lat)?);
    if encode(lon, lat).is_none() {
        return Err(Reply::Error(format!(
            "ERR invalid longitude,latitude pair {lon:.6},{lat:.6}"
        )));
    }
    Ok((lon, lat))
}

pub fn unit(raw: &[u8]) -> Result<Unit, Reply> {
    Unit::parse(raw).ok_or_else(|| Reply::err(BAD_UNIT))
}

/// A length in metres from a number and its unit.
fn length(raw: &[u8], unit: Unit, what: &str) -> Result<f64, Reply> {
    let n = parse_float(raw).ok_or_else(|| Reply::Error(format!("ERR need numeric {what}")))?;
    if n < 0.0 {
        return Err(Reply::Error(format!("ERR {what} cannot be negative")));
    }
    Ok(n * unit.metres())
}

/// GEOADD after the key: NX, XX, CH and the members with their scores.
#[allow(clippy::type_complexity)]
pub fn parse_add(args: &[Vec<u8>]) -> Result<(bool, bool, bool, Vec<(f64, Vec<u8>)>), Reply> {
    let (mut nx, mut xx, mut ch) = (false, false, false);
    let mut rest = args;
    while let Some(word) = rest.first() {
        if is(word, "NX") {
            nx = true;
        } else if is(word, "XX") {
            xx = true;
        } else if is(word, "CH") {
            ch = true;
        } else {
            break;
        }
        rest = &rest[1..];
    }
    let (triples, loose) = rest.as_chunks::<3>();
    if triples.is_empty() || !loose.is_empty() {
        return Err(syntax());
    }
    if nx && xx {
        return Err(Reply::err(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }
    let mut pairs = Vec::with_capacity(triples.len());
    for [lon, lat, member] in triples {
        let (lon, lat) = position(lon, lat)?;
        // In range, so there is a hash; it fits a double exactly.
        let bits = encode(lon, lat).unwrap_or(0);
        pairs.push((bits as f64, member.clone()));
    }
    Ok((nx, xx, ch, pairs))
}

/// Which command the options of a search belong to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Form {
    Search,
    SearchStore,
    Radius,
    RadiusRo,
}

/// GEOSEARCH (the arguments after its key) or, with `storing`,
/// GEOSEARCHSTORE (those after its two keys).
pub fn parse_search(args: &[Vec<u8>], storing: bool) -> Result<Parsed, Reply> {
    let form = if storing {
        Form::SearchStore
    } else {
        Form::Search
    };
    options(args, form, None, None)
}

/// GEORADIUS and GEORADIUSBYMEMBER, from the arguments after the key. The
/// `_RO` forms take no STORE.
pub fn parse_radius(args: &[Vec<u8>], by_member: bool, read_only: bool) -> Result<Parsed, Reply> {
    let fixed = if by_member { 3 } else { 4 };
    if args.len() < fixed {
        return Err(syntax());
    }
    let origin = if by_member {
        Origin::Member(args[0].clone())
    } else {
        let (lon, lat) = position(&args[0], &args[1])?;
        Origin::LonLat(lon, lat)
    };
    let in_unit = unit(&args[fixed - 1])?;
    let radius = length(&args[fixed - 2], in_unit, "radius")?;
    let form = if read_only {
        Form::RadiusRo
    } else {
        Form::Radius
    };
    options(
        &args[fixed..],
        form,
        Some(origin),
        Some((Shape::Circle(radius), in_unit)),
    )
}

fn options(
    args: &[Vec<u8>],
    form: Form,
    mut origin: Option<Origin>,
    mut area: Option<(Shape, Unit)>,
) -> Result<Parsed, Reply> {
    let modern = matches!(form, Form::Search | Form::SearchStore);
    let (mut origins, mut areas) = (0, 0);
    let (mut asc, mut count, mut any) = (None, None, false);
    let (mut withcoord, mut withdist, mut withhash) = (false, false, false);
    let (mut store, mut storedist) = (None, false);
    let mut i = 0;
    while let Some(word) = args.get(i) {
        let left = args.len() - i - 1;
        if is(word, "WITHCOORD") {
            withcoord = true;
        } else if is(word, "WITHDIST") {
            withdist = true;
        } else if is(word, "WITHHASH") {
            withhash = true;
        } else if is(word, "ANY") {
            any = true;
        } else if is(word, "ASC") {
            asc = Some(true);
        } else if is(word, "DESC") {
            asc = Some(false);
        } else if is(word, "COUNT") && left >= 1 {
            let n = parse_i64(&args[i + 1]).ok_or_else(|| Reply::err(NOT_INT))?;
            count = Some(
                usize::try_from(n)
                    .ok()
                    .filter(|&n| n > 0)
                    .ok_or_else(|| Reply::err("ERR COUNT must be > 0"))?,
            );
            i += 1;
        } else if form == Form::Radius && (is(word, "STORE") || is(word, "STOREDIST")) && left >= 1
        {
            store = Some(args[i + 1].clone());
            storedist = is(word, "STOREDIST");
            i += 1;
        } else if form == Form::SearchStore && is(word, "STOREDIST") {
            storedist = true;
        } else if modern && is(word, "FROMMEMBER") && left >= 1 {
            origin = Some(Origin::Member(args[i + 1].clone()));
            origins += 1;
            i += 1;
        } else if modern && is(word, "FROMLONLAT") && left >= 2 {
            let (lon, lat) = position(&args[i + 1], &args[i + 2])?;
            origin = Some(Origin::LonLat(lon, lat));
            origins += 1;
            i += 2;
        } else if modern && is(word, "BYRADIUS") && left >= 2 {
            let in_unit = unit(&args[i + 2])?;
            let radius = length(&args[i + 1], in_unit, "radius")?;
            area = Some((Shape::Circle(radius), in_unit));
            areas += 1;
            i += 2;
        } else if modern && is(word, "BYBOX") && left >= 3 {
            let in_unit = unit(&args[i + 3])?;
            let width = length(&args[i + 1], in_unit, "width")?;
            let height = length(&args[i + 2], in_unit, "height")?;
            area = Some((Shape::Box(width, height), in_unit));
            areas += 1;
            i += 3;
        } else {
            return Err(syntax());
        }
        i += 1;
    }
    if modern && origins != 1 {
        return Err(Reply::err(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
        ));
    }
    if modern && areas != 1 {
        return Err(Reply::err(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
        ));
    }
    let (Some(origin), Some((shape, unit))) = (origin, area) else {
        return Err(syntax());
    };
    if any && count.is_none() {
        return Err(Reply::err("ERR the ANY argument requires COUNT argument"));
    }
    if (withcoord || withdist || withhash) && (store.is_some() || form == Form::SearchStore) {
        return Err(Reply::Error(format!(
            "ERR {} is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            if modern {
                "GEOSEARCHSTORE"
            } else {
                "STORE option in GEORADIUS"
            }
        )));
    }
    Ok(Parsed {
        query: Query {
            origin,
            shape,
            unit,
            asc,
            count,
            any,
            withcoord,
            withdist,
            withhash,
        },
        store,
        storedist,
    })
}

/// A distance as the GEO commands print it: four decimals, in `unit`.
pub fn dist_text(metres: f64, unit: Unit) -> Vec<u8> {
    format!("{:.4}", metres / unit.metres()).into_bytes()
}

/// A decoded position as a reply: longitude, then latitude, each with the
/// seventeen decimals of Valkey's addReplyHumanLongDouble.
pub fn position_reply(bits: u64) -> Reply {
    let (lon, lat) = decode(bits);
    let human = |x: f64| Reply::Bulk(LongDouble::from_f64(x).to_human());
    Reply::Array(vec![human(lon), human(lat)])
}

impl Query {
    /// The members of a sorted set (member and score, in any order) that
    /// lie inside the area around `centre`, in the order and number the
    /// query asks for.
    pub fn select<'a>(
        &self,
        centre: (f64, f64),
        entries: impl Iterator<Item = (&'a [u8], f64)>,
    ) -> Vec<Hit> {
        let mut hits = Vec::new();
        for (member, score) in entries {
            let bits = score as u64;
            let Some(dist) = within(self.shape, centre, decode(bits)) else {
                continue;
            };
            hits.push(Hit {
                member: member.to_vec(),
                dist,
                bits,
            });
            if self.any && self.count == Some(hits.len()) {
                break;
            }
        }
        // COUNT without ANY means the nearest ones.
        let nearest_first = self
            .asc
            .or((self.count.is_some() && !self.any).then_some(true));
        match nearest_first {
            Some(true) => hits.sort_by(|a, b| a.dist.total_cmp(&b.dist)),
            Some(false) => hits.sort_by(|a, b| b.dist.total_cmp(&a.dist)),
            None => {}
        }
        hits.truncate(self.count.unwrap_or(usize::MAX));
        hits
    }

    pub fn reply(&self, hits: Vec<Hit>) -> Reply {
        let plain = !(self.withcoord || self.withdist || self.withhash);
        let rows = hits.into_iter().map(|hit| {
            if plain {
                return Reply::Bulk(hit.member);
            }
            let mut row = vec![Reply::Bulk(hit.member)];
            if self.withdist {
                row.push(Reply::Bulk(dist_text(hit.dist, self.unit)));
            }
            if self.withhash {
                row.push(Reply::Int(hit.bits as i64));
            }
            if self.withcoord {
                row.push(position_reply(hit.bits));
            }
            Reply::Array(row)
        });
        Reply::Array(rows.collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PALERMO: (f64, f64) = (13.361389, 38.115556);
    const CATANIA: (f64, f64) = (15.087269, 37.502669);
    const PALERMO_BITS: u64 = 3_479_099_956_230_698;
    const CATANIA_BITS: u64 = 3_479_447_370_796_909;
    const KM: f64 = 1000.0;

    /// Small xorshift64 generator: fixed seed, no dependencies.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        /// A float in [lo, hi).
        fn range(&mut self, lo: f64, hi: f64) -> f64 {
            lo + (self.next() >> 11) as f64 / (1u64 << 53) as f64 * (hi - lo)
        }
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() <= 1e-15 * b.abs()
    }

    #[test]
    fn encodes_known_cities() {
        assert_eq!(encode(PALERMO.0, PALERMO.1), Some(PALERMO_BITS));
        assert_eq!(encode(CATANIA.0, CATANIA.1), Some(CATANIA_BITS));
    }

    #[test]
    fn decodes_palermo_like_redis() {
        let (lon, lat) = decode(PALERMO_BITS);
        // Redis prints "%.17g": 13.36138933897018433 and 38.11555639549629859.
        assert!(close(lon, 13.361_389_338_970_184), "{lon:.17}");
        assert!(close(lat, 38.115_556_395_496_3), "{lat:.17}");
    }

    #[test]
    fn distance_between_cities() {
        let (lon1, lat1) = decode(PALERMO_BITS);
        let (lon2, lat2) = decode(CATANIA_BITS);
        let d = distance(lon1, lat1, lon2, lat2);
        assert_eq!(format!("{d:.4}"), "166274.1516");
    }

    #[test]
    fn distance_same_longitude_uses_latitude_only() {
        let d = distance(10.0, 0.0, 10.0, 1.0);
        let expected = EARTH_RADIUS_IN_METERS * deg_rad(1.0);
        assert_eq!(d, expected);
        assert_eq!(distance(5.0, 5.0, 5.0, 5.0), 0.0);
    }

    #[test]
    fn hash_strings_match_redis() {
        assert_eq!(&hash_string(PALERMO_BITS), b"sqc8b49rny0");
        assert_eq!(&hash_string(CATANIA_BITS), b"sqdtr74hyu0");
    }

    #[test]
    fn encode_checks_limits() {
        assert!(encode(0.0, 85.06).is_none());
        assert!(encode(0.0, -85.06).is_none());
        assert!(encode(180.1, 0.0).is_none());
        assert!(encode(-180.1, 0.0).is_none());
        assert!(encode(f64::NAN, 0.0).is_none());
        assert!(encode(0.0, f64::NAN).is_none());
        assert!(encode(f64::INFINITY, 0.0).is_none());
        assert!(encode(0.0, f64::NEG_INFINITY).is_none());
        assert!(encode(LON_MIN, LAT_MIN).is_some());
        assert!(encode(LON_MAX, LAT_MAX).is_some());
    }

    #[test]
    fn decode_never_panics() {
        for bits in [0, 1, HASH_MASK, u64::MAX, 1 << 63] {
            let (lon, lat) = decode(bits);
            assert!((LON_MIN..=LON_MAX).contains(&lon));
            assert!((LAT_MIN..=LAT_MAX).contains(&lat));
            let _ = hash_string(bits);
        }
    }

    #[test]
    fn parses_units() {
        assert_eq!(Unit::parse(b"m"), Some(Unit::M));
        assert_eq!(Unit::parse(b"KM"), Some(Unit::Km));
        assert_eq!(Unit::parse(b"Ft"), Some(Unit::Ft));
        assert_eq!(Unit::parse(b"mI"), Some(Unit::Mi));
        assert_eq!(Unit::parse(b"yd"), None);
        assert_eq!(Unit::parse(b""), None);
        assert_eq!(Unit::M.metres(), 1.0);
        assert_eq!(Unit::Km.metres(), 1000.0);
        assert_eq!(Unit::Ft.metres(), 0.3048);
        assert_eq!(Unit::Mi.metres(), 1609.34);
    }

    #[test]
    fn circle_membership() {
        let centre = (15.0, 37.0);
        let both = within(Shape::Circle(200.0 * KM), centre, PALERMO);
        assert!(both.is_some());
        assert!(within(Shape::Circle(200.0 * KM), centre, CATANIA).is_some());

        assert!(within(Shape::Circle(100.0 * KM), centre, PALERMO).is_none());
        // Redis measures from the decoded (stored) position.
        let d = within(Shape::Circle(100.0 * KM), centre, decode(CATANIA_BITS));
        assert_eq!(format!("{:.4}", d.unwrap_or(f64::NAN)), "56441.2579");
    }

    #[test]
    fn box_membership() {
        let centre = (15.0, 37.0);
        let sq = Shape::Box(400.0 * KM, 400.0 * KM);
        assert!(within(sq, centre, PALERMO).is_some());
        assert!(within(sq, centre, CATANIA).is_some());

        // Catania is 55.9 km north of the centre line, Palermo 124 km.
        let wide = Shape::Box(400.0 * KM, 120.0 * KM);
        assert!(within(wide, centre, PALERMO).is_none());
        assert!(within(wide, centre, CATANIA).is_some());
        let too_flat = Shape::Box(400.0 * KM, 100.0 * KM);
        assert!(within(too_flat, centre, CATANIA).is_none());

        // Palermo is 146 km east of the centre line, Catania 7.7 km.
        let tall = Shape::Box(100.0 * KM, 400.0 * KM);
        assert!(within(tall, centre, PALERMO).is_none());
        assert!(within(tall, centre, CATANIA).is_some());
    }

    #[test]
    fn box_returns_real_distance() {
        let centre = (15.0, 37.0);
        let sq = Shape::Box(400.0 * KM, 400.0 * KM);
        let d = within(sq, centre, CATANIA);
        let expected = distance(centre.0, centre.1, CATANIA.0, CATANIA.1);
        assert_eq!(d, Some(expected));
    }

    #[test]
    fn round_trip_positions() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..5000 {
            let lon = rng.range(LON_MIN, LON_MAX);
            let lat = rng.range(LAT_MIN, LAT_MAX);
            let bits = encode(lon, lat);
            assert!(bits.is_some());
            let (lon2, lat2) = decode(bits.unwrap_or(0));
            assert!((lon - lon2).abs() < 1e-5, "{lon} vs {lon2}");
            assert!((lat - lat2).abs() < 1e-5, "{lat} vs {lat2}");
        }
    }

    #[test]
    fn round_trip_hashes() {
        let mut rng = Rng(0x1234_5678_9abc_def1);
        for _ in 0..5000 {
            let bits = rng.next() & HASH_MASK;
            let (lon, lat) = decode(bits);
            assert_eq!(encode(lon, lat), Some(bits));
        }
    }
}
