//! The P2 group: BITFIELD, GEO, SORT, SWAPDB, SLOWLOG and LATENCY, OBJECT
//! FREQ, MONITOR and CLIENT TRACKING. Written to find mistakes: odd
//! arguments, values at the edge of their range, secrets that must not be
//! shown, and messages that must not come early, twice or never.

mod net;

use hotstash::{Db, PersistOptions, Reply, Session};
use net::*;
use std::collections::VecDeque;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn fresh() -> (Db, Session) {
    let db = Db::open(None, engine_cfg(4)).unwrap();
    let s = db.session().unwrap();
    (db, s)
}

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn ok(reply: Reply) {
    assert_eq!(reply, Reply::Ok);
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn bulks(xs: &[&str]) -> Reply {
    Reply::Array(xs.iter().map(|x| b(x)).collect())
}

fn ints(xs: &[i64]) -> Reply {
    Reply::Array(xs.iter().map(|&x| Reply::Int(x)).collect())
}

fn int(reply: Reply) -> i64 {
    match reply {
        Reply::Int(n) => n,
        other => panic!("not an integer: {other:?}"),
    }
}

fn bytes(reply: Reply) -> Vec<u8> {
    match reply {
        Reply::Bulk(v) => v,
        other => panic!("not a bulk string: {other:?}"),
    }
}

fn text(reply: Reply) -> String {
    match reply {
        Reply::Bulk(v) => String::from_utf8(v).expect("utf-8"),
        Reply::Simple(s) => s,
        other => panic!("not a string: {other:?}"),
    }
}

fn float(reply: Reply) -> f64 {
    text(reply).parse().expect("a number")
}

fn error(reply: Reply) -> String {
    match reply {
        Reply::Error(text) => text,
        other => panic!("not an error: {other:?}"),
    }
}

fn items(reply: Reply) -> Vec<Reply> {
    match reply {
        Reply::Array(items) | Reply::Map(items) => items,
        other => panic!("not an array: {other:?}"),
    }
}

fn exists(s: &Session, key: &str) -> bool {
    int(s.execute(&["EXISTS", key])) == 1
}

/// A small generator, so that failures repeat.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ---- BITFIELD ----

#[test]
fn bitfield_reads_writes_and_adds() {
    let (_db, s) = fresh();
    assert_eq!(
        s.execute(&["BITFIELD", "k", "SET", "u8", "0", "255", "GET", "u8", "0", "GET", "i8", "0"]),
        ints(&[0, 255, -1])
    );
    // The default is to wrap.
    assert_eq!(
        s.execute(&["BITFIELD", "k", "INCRBY", "u8", "0", "1"]),
        ints(&[0])
    );
    assert_eq!(
        s.execute(&["BITFIELD", "k", "INCRBY", "i5", "100", "1", "GET", "u4", "0"]),
        ints(&[1, 0])
    );
    // A field may cross byte borders and sit at any bit.
    assert_eq!(
        s.execute(&["BITFIELD", "odd", "SET", "u13", "3", "5461", "GET", "u13", "3"]),
        ints(&[0, 5461])
    );
    assert_eq!(
        bytes(s.execute(&["GET", "odd"])),
        vec![0b0001_0101, 0b0101_0101]
    );
    // `#` counts in fields of that width.
    assert_eq!(
        s.execute(&["BITFIELD", "h", "SET", "u8", "#2", "7"]),
        ints(&[0])
    );
    assert_eq!(s.execute(&["BITFIELD", "h", "GET", "u8", "16"]), ints(&[7]));
    assert_eq!(int(s.execute(&["STRLEN", "h"])), 3);
    // The widest fields.
    assert_eq!(
        s.execute(&[
            "BITFIELD", "w", "SET", "i64", "0", "-1", "GET", "i64", "0", "GET", "u63", "1"
        ]),
        ints(&[0, -1, i64::MAX])
    );
    ok(s.execute(&["DEL", "w"])
        .eq(&Reply::Int(1))
        .then_some(Reply::Ok)
        .unwrap());
    assert_eq!(
        s.execute(&[
            "BITFIELD",
            "w",
            "SET",
            "i64",
            "0",
            "9223372036854775807",
            "INCRBY",
            "i64",
            "0",
            "1"
        ]),
        ints(&[0, i64::MIN])
    );
}

#[test]
fn bitfield_overflow_modes_apply_to_what_follows() {
    let (_db, s) = fresh();
    assert_eq!(
        s.execute(&["BITFIELD", "k", "OVERFLOW", "SAT", "INCRBY", "i8", "8", "200"]),
        ints(&[127])
    );
    assert_eq!(
        s.execute(&["BITFIELD", "k", "OVERFLOW", "SAT", "INCRBY", "i8", "8", "-300"]),
        ints(&[-128])
    );
    for want in [1, 2, 3, 3, 3] {
        assert_eq!(
            s.execute(&["BITFIELD", "sat", "OVERFLOW", "SAT", "INCRBY", "u2", "102", "1"]),
            ints(&[want])
        );
    }
    // FAIL answers nil and leaves the field alone.
    assert_eq!(
        s.execute(&[
            "BITFIELD", "f", "SET", "u2", "100", "2", "OVERFLOW", "FAIL", "INCRBY", "u2", "100",
            "5", "GET", "u2", "100"
        ]),
        Reply::Array(vec![Reply::Int(0), Reply::Nil, Reply::Int(2)])
    );
    // A value that does not fit SET under FAIL is refused the same way.
    assert_eq!(
        s.execute(&[
            "BITFIELD", "f", "OVERFLOW", "FAIL", "SET", "u2", "100", "9", "GET", "u2", "100"
        ]),
        Reply::Array(vec![Reply::Nil, Reply::Int(2)])
    );
    // The mode changes in the middle of a command.
    assert_eq!(
        s.execute(&[
            "BITFIELD", "m", "INCRBY", "u2", "0", "5", "OVERFLOW", "SAT", "INCRBY", "u2", "2", "5",
            "OVERFLOW", "WRAP", "INCRBY", "u2", "2", "1"
        ]),
        ints(&[1, 3, 0])
    );
    assert!(error(s.execute(&["BITFIELD", "m", "OVERFLOW", "BOGUS"])).starts_with("ERR"));
}

#[test]
fn bitfield_refuses_bad_types_and_offsets_without_side_effects() {
    let (_db, s) = fresh();
    for ty in [
        "u64", "i65", "u0", "i0", "x8", "u", "i", "u-1", "8", "u8x", "U 8",
    ] {
        let e = error(s.execute(&["BITFIELD", "k", "SET", ty, "0", "1"]));
        assert!(e.starts_with("ERR Invalid bitfield type"), "{ty}: {e}");
    }
    for off in [
        "-1",
        "#-1",
        "x",
        "",
        "#",
        "4294967296",
        "#536870912",
        "99999999999999999999",
    ] {
        let e = error(s.execute(&["BITFIELD", "k", "SET", "u8", off, "1"]));
        assert!(e.starts_with("ERR bit offset"), "{off}: {e}");
    }
    // The last bit of the field must fit too.
    assert!(s
        .execute(&["BITFIELD", "k", "SET", "u8", "4294967289", "1"])
        .is_error());
    assert!(s
        .execute(&["BITFIELD", "k", "SET", "u8", "0", "notanumber"])
        .is_error());
    assert!(s.execute(&["BITFIELD", "k", "SET", "u8", "0"]).is_error());
    assert!(s.execute(&["BITFIELD", "k", "GET", "u8"]).is_error());
    assert!(s.execute(&["BITFIELD", "k", "FROB", "u8", "0"]).is_error());
    // One bad operation refuses the whole command.
    assert!(s
        .execute(&["BITFIELD", "k", "SET", "u8", "0", "1", "GET", "u64", "0"])
        .is_error());
    assert!(!exists(&s, "k"));
    // Reading creates nothing and does not grow a value.
    assert_eq!(
        s.execute(&["BITFIELD", "k", "GET", "u8", "0", "GET", "i64", "1000"]),
        ints(&[0, 0])
    );
    assert_eq!(s.execute(&["BITFIELD", "k"]), Reply::Array(vec![]));
    assert!(!exists(&s, "k"));
    ok(s.execute(&["SET", "s", "a"]));
    assert_eq!(
        s.execute(&["BITFIELD", "s", "GET", "u8", "0", "GET", "u16", "0", "GET", "u8", "800"]),
        ints(&[97, 97 << 8, 0])
    );
    assert_eq!(int(s.execute(&["STRLEN", "s"])), 1);
    s.execute(&["RPUSH", "l", "x"]);
    assert!(error(s.execute(&["BITFIELD", "l", "GET", "u8", "0"])).starts_with("WRONGTYPE"));
    assert!(error(s.execute(&["BITFIELD", "l", "SET", "u8", "0", "1"])).starts_with("WRONGTYPE"));
}

#[test]
fn bitfield_ro_only_reads() {
    let (_db, s) = fresh();
    ok(s.execute(&["SET", "s", "ab"]));
    assert_eq!(
        s.execute(&["BITFIELD_RO", "s", "GET", "u8", "0", "GET", "u8", "#1"]),
        ints(&[97, 98])
    );
    for bad in [
        vec!["BITFIELD_RO", "s", "SET", "u8", "0", "1"],
        vec!["BITFIELD_RO", "s", "INCRBY", "u8", "0", "1"],
        vec!["BITFIELD_RO", "s", "GET", "u8", "0", "OVERFLOW", "SAT"],
    ] {
        let e = error(s.execute(&bad));
        assert!(
            e.contains("BITFIELD_RO only supports the GET subcommand"),
            "{e}"
        );
    }
    assert_eq!(s.execute(&["GET", "s"]), b("ab"));
}

/// The value as a row of bits, the first bit being the highest of byte 0.
struct BitModel(Vec<bool>);

impl BitModel {
    fn get(&self, signed: bool, width: u32, at: usize) -> i64 {
        let mut raw: u128 = 0;
        for i in 0..width as usize {
            raw = raw << 1 | u128::from(self.0.get(at + i).copied().unwrap_or(false));
        }
        if signed && raw >> (width - 1) & 1 == 1 {
            (raw as i128 - (1i128 << width)) as i64
        } else {
            raw as i64
        }
    }

    fn set(&mut self, width: u32, at: usize, value: i64) {
        let end = at + width as usize;
        if self.0.len() < end {
            self.0.resize(end.div_ceil(8) * 8, false);
        }
        for i in 0..width as usize {
            self.0[at + i] = (value as u64 >> (width as usize - 1 - i)) & 1 == 1;
        }
    }
}

#[test]
fn bitfield_agrees_with_a_model_of_bits() {
    let (_db, s) = fresh();
    let mut rng = Rng(0x00B1_7F1E_1D5E_ED01);
    let mut model = BitModel(Vec::new());
    for round in 0..4000 {
        let signed = rng.below(2) == 0;
        let width = 1 + rng.below(if signed { 64 } else { 63 }) as u32;
        let at = rng.below(200) as usize;
        let ty = format!("{}{width}", if signed { 'i' } else { 'u' });
        let at_s = at.to_string();
        let value = rng.next() as i64 >> rng.below(64);
        let value_s = value.to_string();
        let (got, want) = match rng.below(3) {
            0 => (
                s.execute(&["BITFIELD", "m", "GET", &ty, &at_s]),
                model.get(signed, width, at),
            ),
            1 => {
                let old = model.get(signed, width, at);
                model.set(width, at, value);
                (
                    s.execute(&["BITFIELD", "m", "SET", &ty, &at_s, &value_s]),
                    old,
                )
            }
            _ => {
                let sum = model.get(signed, width, at).wrapping_add(value);
                model.set(width, at, sum);
                (
                    s.execute(&["BITFIELD", "m", "INCRBY", &ty, &at_s, &value_s]),
                    model.get(signed, width, at),
                )
            }
        };
        assert_eq!(
            got,
            ints(&[want]),
            "round {round}: {ty} at {at} with {value}"
        );
    }
    let stored = bytes(s.execute(&["GET", "m"]));
    let want: Vec<u8> = model
        .0
        .chunks(8)
        .map(|byte| byte.iter().fold(0u8, |acc, &bit| acc << 1 | u8::from(bit)))
        .collect();
    assert_eq!(stored, want);
}

// ---- GEO ----

fn sicily(s: &Session) {
    assert_eq!(
        int(s.execute(&[
            "GEOADD",
            "Sicily",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania"
        ])),
        2
    );
}

#[test]
fn geo_known_answers() {
    let (_db, s) = fresh();
    sicily(&s);
    assert_eq!(
        s.execute(&["GEODIST", "Sicily", "Palermo", "Catania"]),
        b("166274.1516")
    );
    assert_eq!(
        s.execute(&["GEODIST", "Sicily", "Palermo", "Catania", "km"]),
        b("166.2742")
    );
    assert_eq!(
        s.execute(&["GEODIST", "Sicily", "Palermo", "Catania", "MI"]),
        b("103.3182")
    );
    assert_eq!(
        s.execute(&["GEODIST", "Sicily", "Palermo", "Palermo"]),
        b("0.0000")
    );
    assert_eq!(
        s.execute(&["GEODIST", "Sicily", "Palermo", "Nowhere"]),
        Reply::Nil
    );
    assert_eq!(s.execute(&["GEODIST", "none", "a", "b"]), Reply::Nil);
    assert!(s
        .execute(&["GEODIST", "Sicily", "Palermo", "Catania", "parsec"])
        .is_error());
    assert_eq!(
        s.execute(&["GEOHASH", "Sicily", "Palermo", "Catania", "Nowhere"]),
        Reply::Array(vec![b("sqc8b49rny0"), b("sqdtr74hyu0"), Reply::Nil])
    );
    let pos = items(s.execute(&["GEOPOS", "Sicily", "Palermo", "Nowhere", "Catania"]));
    assert_eq!(pos.len(), 3);
    let palermo = items(pos[0].clone());
    assert!((float(palermo[0].clone()) - 13.361389).abs() < 1e-5);
    assert!((float(palermo[1].clone()) - 38.115556).abs() < 1e-5);
    assert!(
        matches!(pos[1], Reply::Nil | Reply::NilArray),
        "{:?}",
        pos[1]
    );
    assert_eq!(
        s.execute(&["GEOPOS", "none", "a"]).clone(),
        Reply::Array(vec![pos[1].clone()])
    );
    // It is a sorted set underneath.
    assert_eq!(s.execute(&["TYPE", "Sicily"]), Reply::Simple("zset".into()));
    assert_eq!(
        s.execute(&["ZSCORE", "Sicily", "Palermo"]),
        b("3479099956230698")
    );
    assert_eq!(int(s.execute(&["ZREM", "Sicily", "Palermo"])), 1);
    assert_eq!(
        s.execute(&["GEOHASH", "Sicily", "Palermo"]),
        Reply::Array(vec![Reply::Nil])
    );
}

#[test]
fn geoadd_options_and_bad_input() {
    let (_db, s) = fresh();
    sicily(&s);
    // NX adds only, XX updates only, CH counts what changed.
    assert_eq!(
        int(s.execute(&["GEOADD", "Sicily", "NX", "1", "1", "Palermo", "2", "2", "New"])),
        1
    );
    assert_eq!(
        s.execute(&["GEOHASH", "Sicily", "Palermo"]),
        bulks(&["sqc8b49rny0"])
    );
    assert_eq!(
        int(s.execute(&["GEOADD", "Sicily", "XX", "1", "1", "Palermo", "3", "3", "Other"])),
        0
    );
    assert_eq!(
        s.execute(&["GEOPOS", "Sicily", "Other"]).clone(),
        s.execute(&["GEOPOS", "none", "x"])
    );
    assert_eq!(
        int(s.execute(&["GEOADD", "Sicily", "xx", "ch", "5", "5", "Palermo", "2", "2", "New"])),
        1
    );
    assert_eq!(
        int(s.execute(&["GEOADD", "Sicily", "CH", "5", "5", "Palermo", "9", "9", "Brand"])),
        1
    );
    let e = error(s.execute(&["GEOADD", "Sicily", "NX", "XX", "1", "1", "a"]));
    assert!(e.contains("XX and NX"), "{e}");
    for (lon, lat) in [
        ("180.1", "0"),
        ("-180.1", "0"),
        ("0", "85.06"),
        ("0", "-85.06"),
        ("nan", "0"),
        ("0", "inf"),
    ] {
        let e = error(s.execute(&["GEOADD", "bad", "0", "0", "fine", lon, lat, "m"]));
        assert!(e.starts_with("ERR"), "{lon},{lat}: {e}");
    }
    assert_eq!(
        error(s.execute(&["GEOADD", "bad", "200", "38", "m"])),
        "ERR invalid longitude,latitude pair 200.000000,38.000000"
    );
    // Nothing of a refused command is kept.
    assert!(!exists(&s, "bad"));
    assert!(s.execute(&["GEOADD", "bad", "1", "2"]).is_error());
    assert!(s.execute(&["GEOADD", "bad", "NX"]).is_error());
    assert!(s.execute(&["GEOADD", "bad", "1", "x", "m"]).is_error());
    // The edges are places too.
    assert_eq!(
        int(s.execute(&[
            "GEOADD",
            "edge",
            "180",
            "85.05112878",
            "ne",
            "-180",
            "-85.05112878",
            "sw"
        ])),
        2
    );
    let d = float(s.execute(&["GEODIST", "edge", "ne", "sw"]));
    assert!(d > 1.0e7 && d < 2.1e7, "{d}");
    s.execute(&["SET", "str", "x"]);
    for cmd in [
        vec!["GEOADD", "str", "1", "1", "m"],
        vec!["GEOPOS", "str", "m"],
        vec!["GEOHASH", "str", "m"],
        vec!["GEODIST", "str", "a", "b"],
        vec![
            "GEOSEARCH",
            "str",
            "FROMLONLAT",
            "1",
            "1",
            "BYRADIUS",
            "1",
            "m",
        ],
        vec!["GEORADIUS", "str", "1", "1", "1", "m"],
    ] {
        assert!(error(s.execute(&cmd)).starts_with("WRONGTYPE"), "{cmd:?}");
    }
}

#[test]
fn geosearch_shapes_order_count_and_extras() {
    let (_db, s) = fresh();
    sicily(&s);
    s.execute(&[
        "GEOADD",
        "Sicily",
        "12.758489",
        "38.788135",
        "edge1",
        "17.241510",
        "38.788135",
        "edge2",
    ]);
    assert_eq!(
        s.execute(&[
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "ASC"
        ]),
        bulks(&["Catania", "Palermo"])
    );
    assert_eq!(
        s.execute(&[
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "DESC"
        ]),
        bulks(&["Palermo", "Catania"])
    );
    assert_eq!(
        s.execute(&[
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYBOX",
            "400",
            "400",
            "km",
            "ASC"
        ]),
        bulks(&["Catania", "Palermo", "edge2", "edge1"])
    );
    assert_eq!(
        s.execute(&[
            "GEOSEARCH",
            "Sicily",
            "FROMMEMBER",
            "Palermo",
            "BYRADIUS",
            "100",
            "mi",
            "ASC",
            "COUNT",
            "2"
        ]),
        bulks(&["Palermo", "edge1"])
    );
    let full = items(s.execute(&[
        "GEOSEARCH",
        "Sicily",
        "FROMLONLAT",
        "15",
        "37",
        "BYRADIUS",
        "200",
        "km",
        "ASC",
        "WITHCOORD",
        "WITHDIST",
        "WITHHASH",
    ]));
    assert_eq!(full.len(), 2);
    let first = items(full[0].clone());
    assert_eq!(first[0], b("Catania"));
    assert_eq!(first[1], b("56.4413"));
    assert_eq!(first[2], Reply::Int(3479447370796909));
    let at = items(first[3].clone());
    assert!((float(at[0].clone()) - 15.087269).abs() < 1e-5);
    assert_eq!(items(full[1].clone())[1], b("190.4424"));
    // ANY stops early; with ASC it is still sorted among what it found.
    let any = items(s.execute(&[
        "GEOSEARCH",
        "Sicily",
        "FROMLONLAT",
        "15",
        "37",
        "BYRADIUS",
        "500",
        "km",
        "COUNT",
        "2",
        "ANY",
    ]));
    assert_eq!(any.len(), 2);
    assert_eq!(
        s.execute(&[
            "GEOSEARCH",
            "none",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "km"
        ]),
        Reply::Array(vec![])
    );
    assert_eq!(
        s.execute(&[
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "0",
            "km"
        ]),
        Reply::Array(vec![])
    );
    for bad in [
        vec!["GEOSEARCH", "Sicily", "BYRADIUS", "1", "km"],
        vec!["GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37"],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "FROMMEMBER",
            "Palermo",
            "BYRADIUS",
            "1",
            "km",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "km",
            "BYBOX",
            "1",
            "1",
            "km",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "km",
            "ANY",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "km",
            "COUNT",
            "0",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "km",
            "COUNT",
            "-1",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "-1",
            "km",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "furlong",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYBOX",
            "1",
            "km",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "500",
            "37",
            "BYRADIUS",
            "1",
            "km",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "km",
            "STORE",
            "x",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "km",
            "STOREDIST",
        ],
        vec![
            "GEOSEARCH",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "1",
            "km",
            "ASC",
            "DESC",
            "BOGUS",
        ],
    ] {
        assert!(s.execute(&bad).is_error(), "{bad:?}");
    }
    let e = error(s.execute(&[
        "GEOSEARCH",
        "Sicily",
        "FROMMEMBER",
        "Nowhere",
        "BYRADIUS",
        "1",
        "km",
    ]));
    assert!(e.contains("could not decode requested zset member"), "{e}");
}

#[test]
fn geosearchstore_and_the_old_radius_commands() {
    let (_db, s) = fresh();
    sicily(&s);
    s.execute(&[
        "GEOADD",
        "Sicily",
        "12.758489",
        "38.788135",
        "edge1",
        "17.241510",
        "38.788135",
        "edge2",
    ]);
    s.execute(&["SET", "dst", "old"]);
    assert_eq!(
        int(s.execute(&[
            "GEOSEARCHSTORE",
            "dst",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYBOX",
            "400",
            "400",
            "km",
            "ASC",
            "COUNT",
            "3"
        ])),
        3
    );
    assert_eq!(
        s.execute(&["ZRANGE", "dst", "0", "-1"]),
        bulks(&["Palermo", "Catania", "edge2"])
    );
    assert_eq!(
        s.execute(&["GEOHASH", "dst", "Catania"]),
        bulks(&["sqdtr74hyu0"])
    );
    assert_eq!(
        int(s.execute(&[
            "GEOSEARCHSTORE",
            "dst",
            "Sicily",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "STOREDIST"
        ])),
        2
    );
    let scored = items(s.execute(&["ZRANGE", "dst", "0", "-1", "WITHSCORES"]));
    assert_eq!(scored[0], b("Catania"));
    assert!((float(scored[1].clone()) - 56.4413).abs() < 1e-3);
    assert!((float(scored[3].clone()) - 190.4424).abs() < 1e-3);
    // Nothing found: the destination goes away.
    assert_eq!(
        int(s.execute(&[
            "GEOSEARCHSTORE",
            "dst",
            "Sicily",
            "FROMLONLAT",
            "100",
            "0",
            "BYRADIUS",
            "1",
            "km"
        ])),
        0
    );
    assert!(!exists(&s, "dst"));
    assert_eq!(
        int(s.execute(&[
            "GEOSEARCHSTORE",
            "dst",
            "none",
            "FROMLONLAT",
            "100",
            "0",
            "BYRADIUS",
            "1",
            "km"
        ])),
        0
    );
    for extra in ["WITHDIST", "WITHCOORD", "WITHHASH"] {
        assert!(s
            .execute(&[
                "GEOSEARCHSTORE",
                "dst",
                "Sicily",
                "FROMLONLAT",
                "15",
                "37",
                "BYRADIUS",
                "200",
                "km",
                extra
            ])
            .is_error());
    }
    // Storing onto the source works on what was there before.
    assert_eq!(
        int(s.execute(&[
            "GEOSEARCHSTORE",
            "Sicily",
            "Sicily",
            "FROMMEMBER",
            "Catania",
            "BYRADIUS",
            "1",
            "km"
        ])),
        1
    );
    assert_eq!(
        s.execute(&["ZRANGE", "Sicily", "0", "-1"]),
        bulks(&["Catania"])
    );

    s.execute(&["DEL", "Sicily"]);
    sicily(&s);
    assert_eq!(
        s.execute(&[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "WITHDIST",
            "ASC"
        ]),
        Reply::Array(vec![
            bulks(&["Catania", "56.4413"]),
            bulks(&["Palermo", "190.4424"])
        ])
    );
    assert_eq!(
        s.execute(&["GEORADIUS", "Sicily", "15", "37", "100", "km"]),
        bulks(&["Catania"])
    );
    assert_eq!(
        s.execute(&[
            "GEORADIUS_RO",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "COUNT",
            "1",
            "DESC"
        ]),
        bulks(&["Palermo"])
    );
    assert_eq!(
        s.execute(&["GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "ASC"]),
        bulks(&["Palermo", "Catania"])
    );
    assert_eq!(
        s.execute(&["GEORADIUSBYMEMBER_RO", "Sicily", "Palermo", "10", "km"]),
        bulks(&["Palermo"])
    );
    assert_eq!(
        int(s.execute(&[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "out"
        ])),
        2
    );
    assert_eq!(int(s.execute(&["ZCARD", "out"])), 2);
    assert_eq!(
        int(s.execute(&[
            "GEORADIUSBYMEMBER",
            "Sicily",
            "Palermo",
            "200",
            "km",
            "STOREDIST",
            "outd"
        ])),
        2
    );
    assert_eq!(float(s.execute(&["ZSCORE", "outd", "Palermo"])), 0.0);
    assert!(s
        .execute(&[
            "GEORADIUS_RO",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "out"
        ])
        .is_error());
    assert!(s
        .execute(&[
            "GEORADIUSBYMEMBER_RO",
            "Sicily",
            "Palermo",
            "200",
            "km",
            "STOREDIST",
            "out"
        ])
        .is_error());
    assert!(s
        .execute(&[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "out",
            "WITHDIST"
        ])
        .is_error());
    assert!(s
        .execute(&["GEORADIUS", "Sicily", "15", "37", "200"])
        .is_error());
    assert!(s
        .execute(&["GEORADIUSBYMEMBER", "Sicily", "Nowhere", "200", "km"])
        .is_error());
}

const EARTH_RADIUS: f64 = 6_372_797.560_856;

fn haversine(a: (f64, f64), b: (f64, f64)) -> f64 {
    let (lon1, lat1, lon2, lat2) = (
        a.0.to_radians(),
        a.1.to_radians(),
        b.0.to_radians(),
        b.1.to_radians(),
    );
    let u = ((lat2 - lat1) / 2.0).sin();
    let v = ((lon2 - lon1) / 2.0).sin();
    2.0 * EARTH_RADIUS * (u * u + lat1.cos() * lat2.cos() * v * v).sqrt().asin()
}

#[test]
fn geosearch_by_radius_agrees_with_measuring_every_point() {
    let (_db, s) = fresh();
    let mut rng = Rng(0x0006_E05E_A2C4_0001);
    let mut places = Vec::new();
    for i in 0..600 {
        // Some spread over the globe, some crowded at the date line and
        // near the top.
        let (lon, lat) = match i % 3 {
            0 => (rng.unit() * 360.0 - 180.0, rng.unit() * 170.0 - 85.0),
            1 => {
                let lon = 179.0 + rng.unit() * 2.0;
                (
                    if lon > 180.0 { lon - 360.0 } else { lon },
                    rng.unit() * 4.0 - 2.0,
                )
            }
            _ => (rng.unit() * 360.0 - 180.0, 80.0 + rng.unit() * 5.0),
        };
        let name = format!("p{i}");
        assert_eq!(
            int(s.execute(&["GEOADD", "g", &lon.to_string(), &lat.to_string(), &name])),
            1
        );
        let at = items(items(s.execute(&["GEOPOS", "g", &name]))[0].clone());
        places.push((name, (float(at[0].clone()), float(at[1].clone()))));
    }
    for round in 0..150 {
        let centre = match round % 3 {
            0 => (rng.unit() * 360.0 - 180.0, rng.unit() * 170.0 - 85.0),
            1 => (
                if round % 2 == 0 { 179.9 } else { -179.9 },
                rng.unit() * 2.0 - 1.0,
            ),
            _ => (rng.unit() * 360.0 - 180.0, 84.0),
        };
        let radius = 1000.0 + rng.unit() * rng.unit() * 6_000_000.0;
        let got: Vec<String> = items(s.execute(&[
            "GEOSEARCH",
            "g",
            "FROMLONLAT",
            &centre.0.to_string(),
            &centre.1.to_string(),
            "BYRADIUS",
            &radius.to_string(),
            "m",
            "ASC",
        ]))
        .into_iter()
        .map(text)
        .collect();
        // In order of distance.
        let dist = |name: &str| {
            let at = places
                .iter()
                .find(|(n, _)| n == name)
                .expect("a known place")
                .1;
            haversine(centre, at)
        };
        for pair in got.windows(2) {
            assert!(
                dist(&pair[0]) <= dist(&pair[1]) + 1e-6,
                "round {round}: order"
            );
        }
        for (name, at) in &places {
            let d = haversine(centre, *at);
            // A point on the edge may fall either way.
            if (d - radius).abs() < 0.5 {
                continue;
            }
            assert_eq!(
                got.contains(name),
                d < radius,
                "round {round}: {name} at {d} m, radius {radius} m, centre {centre:?}"
            );
        }
    }
}

// ---- SORT ----

#[test]
fn sort_orders_numbers_and_text() {
    let (_db, s) = fresh();
    s.execute(&["RPUSH", "l", "3", "1", "2", "10", "-5", "1.5"]);
    assert_eq!(
        s.execute(&["SORT", "l"]),
        bulks(&["-5", "1", "1.5", "2", "3", "10"])
    );
    assert_eq!(
        s.execute(&["SORT", "l", "DESC"]),
        bulks(&["10", "3", "2", "1.5", "1", "-5"])
    );
    assert_eq!(
        s.execute(&["SORT", "l", "ALPHA"]),
        bulks(&["-5", "1", "1.5", "10", "2", "3"])
    );
    assert_eq!(
        s.execute(&["SORT", "l", "LIMIT", "1", "2"]),
        bulks(&["1", "1.5"])
    );
    assert_eq!(
        s.execute(&["SORT", "l", "LIMIT", "4", "100"]),
        bulks(&["3", "10"])
    );
    assert_eq!(
        s.execute(&["SORT", "l", "LIMIT", "100", "5"]),
        Reply::Array(vec![])
    );
    assert_eq!(
        s.execute(&["SORT", "l", "LIMIT", "0", "0"]),
        Reply::Array(vec![])
    );
    assert_eq!(
        s.execute(&["SORT", "l", "limit", "0", "-1", "desc", "alpha"])
            .clone(),
        bulks(&["3", "2", "10", "1.5", "1", "-5"])
    );
    assert_eq!(s.execute(&["SORT", "none"]), Reply::Array(vec![]));
    // The list itself is as it was.
    assert_eq!(s.execute(&["LRANGE", "l", "0", "1"]), bulks(&["3", "1"]));
    s.execute(&["SADD", "set", "b", "c", "a"]);
    assert_eq!(
        s.execute(&["SORT", "set", "ALPHA"]),
        bulks(&["a", "b", "c"])
    );
    assert_eq!(
        error(s.execute(&["SORT", "set"])),
        "ERR One or more scores can't be converted into double"
    );
    s.execute(&["ZADD", "z", "3", "a", "2", "b", "1", "c"]);
    assert_eq!(s.execute(&["SORT", "z", "ALPHA"]), bulks(&["a", "b", "c"]));
    assert_eq!(
        s.execute(&["SORT", "z", "BY", "nosort"]),
        bulks(&["c", "b", "a"])
    );
    assert_eq!(
        s.execute(&["SORT", "z", "BY", "nosort", "DESC"]),
        bulks(&["a", "b", "c"])
    );
    assert_eq!(
        s.execute(&["SORT", "z", "BY", "nosort", "LIMIT", "1", "1"]),
        bulks(&["b"])
    );
    s.execute(&["SET", "str", "x"]);
    assert!(error(s.execute(&["SORT", "str"])).starts_with("WRONGTYPE"));
    s.execute(&["HSET", "hash", "f", "v"]);
    assert!(error(s.execute(&["SORT", "hash"])).starts_with("WRONGTYPE"));
    for bad in [
        vec!["SORT", "l", "LIMIT", "1"],
        vec!["SORT", "l", "LIMIT", "a", "b"],
        vec!["SORT", "l", "BY"],
        vec!["SORT", "l", "GET"],
        vec!["SORT", "l", "STORE"],
        vec!["SORT", "l", "BOGUS"],
    ] {
        assert!(s.execute(&bad).is_error(), "{bad:?}");
    }
    assert!(s.execute(&["SORT"]).is_error());
}

#[test]
fn sort_by_get_and_store_reach_other_keys() {
    let (_db, s) = fresh();
    s.execute(&["RPUSH", "ids", "1", "2", "3", "4"]);
    s.execute(&[
        "MSET", "w_1", "30", "w_2", "10", "w_3", "20", "name_1", "one", "name_2", "two", "name_3",
        "three",
    ]);
    s.execute(&["HSET", "obj_1", "age", "9", "nick", "uno"]);
    s.execute(&["HSET", "obj_2", "age", "7", "nick", "dos"]);
    s.execute(&["HSET", "obj_3", "age", "8"]);
    // A missing weight counts as nothing and sorts first.
    assert_eq!(
        s.execute(&["SORT", "ids", "BY", "w_*"]),
        bulks(&["4", "2", "3", "1"])
    );
    assert_eq!(
        s.execute(&["SORT", "ids", "BY", "w_*", "DESC"]),
        bulks(&["1", "3", "2", "4"])
    );
    assert_eq!(
        s.execute(&["SORT", "ids", "BY", "obj_*->age", "LIMIT", "1", "3"]),
        bulks(&["2", "3", "1"])
    );
    assert_eq!(
        s.execute(&[
            "SORT",
            "ids",
            "BY",
            "w_*",
            "GET",
            "#",
            "GET",
            "name_*",
            "GET",
            "obj_*->nick"
        ]),
        Reply::Array(vec![
            b("4"),
            Reply::Nil,
            Reply::Nil,
            b("2"),
            b("two"),
            b("dos"),
            b("3"),
            b("three"),
            Reply::Nil,
            b("1"),
            b("one"),
            b("uno"),
        ])
    );
    // A pattern without a star names nothing.
    assert_eq!(
        s.execute(&["SORT", "ids", "GET", "plain"]),
        Reply::Array(vec![Reply::Nil; 4])
    );
    // GET of a key of another type gives nil, not an error.
    assert_eq!(
        s.execute(&["SORT", "ids", "BY", "nosort", "GET", "obj_*", "LIMIT", "0", "1"]),
        Reply::Array(vec![Reply::Nil])
    );
    s.execute(&["SET", "w_4", "notanumber"]);
    assert_eq!(
        error(s.execute(&["SORT", "ids", "BY", "w_*"])),
        "ERR One or more scores can't be converted into double"
    );
    assert_eq!(
        s.execute(&["SORT", "ids", "BY", "w_*", "ALPHA"]),
        bulks(&["2", "3", "1", "4"])
    );
    s.execute(&["DEL", "w_4"]);

    s.execute(&["SET", "dst", "old"]);
    assert_eq!(
        int(s.execute(&["SORT", "ids", "BY", "w_*", "GET", "name_*", "STORE", "dst"])),
        4
    );
    assert_eq!(
        s.execute(&["LRANGE", "dst", "0", "-1"]),
        bulks(&["", "two", "three", "one"])
    );
    assert_eq!(s.execute(&["TYPE", "dst"]), Reply::Simple("list".into()));
    // Onto itself.
    assert_eq!(int(s.execute(&["SORT", "ids", "DESC", "STORE", "ids"])), 4);
    assert_eq!(
        s.execute(&["LRANGE", "ids", "0", "-1"]),
        bulks(&["4", "3", "2", "1"])
    );
    // Nothing to store: the destination goes away.
    assert_eq!(int(s.execute(&["SORT", "none", "STORE", "dst"])), 0);
    assert!(!exists(&s, "dst"));
    s.execute(&["SET", "ttl", "v", "EX", "100"]);
    assert_eq!(int(s.execute(&["SORT", "ids", "STORE", "ttl"])), 4);
    assert_eq!(int(s.execute(&["TTL", "ttl"])), -1);

    assert_eq!(
        s.execute(&["SORT_RO", "ids", "BY", "w_*", "GET", "#"]),
        bulks(&["4", "2", "3", "1"])
    );
    assert!(s.execute(&["SORT_RO", "ids", "STORE", "dst"]).is_error());
    assert!(!exists(&s, "dst"));
}

#[test]
fn sort_inside_transactions_scripts_and_over_tcp() {
    let env = env();
    let mut c = env.client();
    c.cmd(&["RPUSH", "ids", "2", "3", "1"]);
    c.cmd(&[
        "MSET", "w_1", "3", "w_2", "2", "w_3", "1", "n_1", "a", "n_2", "b", "n_3", "c",
    ]);
    assert_eq!(c.cmd(&["SORT", "ids"]), bulks(&["1", "2", "3"]));
    assert_eq!(
        c.cmd(&["SORT", "ids", "BY", "w_*", "GET", "n_*"]),
        bulks(&["c", "b", "a"])
    );
    assert_eq!(int(c.cmd(&["SORT", "ids", "BY", "w_*", "STORE", "out"])), 3);
    assert_eq!(
        c.cmd(&["LRANGE", "out", "0", "-1"]),
        bulks(&["3", "2", "1"])
    );
    ok(c.cmd(&["MULTI"]));
    c.cmd(&["SORT", "ids", "BY", "w_*", "GET", "#", "STORE", "out2"]);
    c.cmd(&["SORT", "ids", "DESC"]);
    assert_eq!(
        c.cmd(&["EXEC"]),
        Reply::Array(vec![Reply::Int(3), bulks(&["3", "2", "1"])])
    );
    assert_eq!(
        c.cmd(&["LRANGE", "out2", "0", "-1"]),
        bulks(&["3", "2", "1"])
    );
    assert_eq!(
        c.cmd(&[
            "EVAL",
            "return redis.call('SORT', KEYS[1], 'DESC')",
            "1",
            "ids"
        ]),
        bulks(&["3", "2", "1"])
    );
    assert_eq!(
        c.cmd(&[
            "EVAL",
            "return redis.call('SORT', KEYS[1], 'BY', 'w_*', 'GET', 'n_*')",
            "1",
            "ids"
        ]),
        bulks(&["c", "b", "a"])
    );
    assert_eq!(
        int(c.cmd(&[
            "EVAL",
            "return redis.call('SORT', KEYS[1], 'STORE', KEYS[2])",
            "2",
            "ids",
            "out3"
        ])),
        3
    );
    assert_eq!(
        c.cmd(&["LRANGE", "out3", "0", "-1"]),
        bulks(&["1", "2", "3"])
    );
    // A waiting reader is woken by what SORT stores.
    let mut waiter = env.client();
    waiter.send(&["BLPOP", "wake", "0"]);
    assert!(waiter.silent_for(100));
    assert_eq!(int(c.cmd(&["SORT", "ids", "STORE", "wake"])), 3);
    assert_eq!(waiter.read(), bulks(&["wake", "1"]));
}

#[test]
fn sort_does_not_let_a_restricted_user_read_other_keys() {
    let env = env();
    let mut admin = env.client();
    admin.cmd(&["RPUSH", "app:l", "2", "1"]);
    admin.cmd(&[
        "MSET", "secret_1", "x", "secret_2", "y", "app:w_1", "2", "app:w_2", "1",
    ]);
    ok(admin.cmd(&["ACL", "SETUSER", "u", "on", ">pw", "~app:*", "+@all"]));
    let mut c = env.client();
    ok(c.cmd(&["AUTH", "u", "pw"]));
    assert_eq!(c.cmd(&["SORT", "app:l"]), bulks(&["1", "2"]));
    assert_eq!(
        c.cmd(&["SORT", "app:l", "BY", "nosort", "GET", "#"]),
        bulks(&["2", "1"])
    );
    for cmd in [
        vec!["SORT", "app:l", "GET", "secret_*"],
        vec!["SORT", "app:l", "BY", "secret_*"],
        vec!["SORT_RO", "app:l", "GET", "secret_*"],
        vec![
            "SORT", "app:l", "BY", "nosort", "GET", "secret_*", "STORE", "app:out",
        ],
        vec![
            "EVAL",
            "return redis.call('SORT', KEYS[1], 'GET', 'secret_*')",
            "1",
            "app:l",
        ],
    ] {
        let reply = c.cmd(&cmd);
        assert!(reply.is_error(), "{cmd:?} gave {reply:?}");
    }
    ok(c.cmd(&["MULTI"]));
    let queued = c.cmd(&["SORT", "app:l", "GET", "secret_*"]);
    let done = c.cmd(&["EXEC"]);
    assert!(
        queued.is_error() || matches!(&done, Reply::Array(r) if r[0].is_error()) || done.is_error(),
        "{queued:?} {done:?}"
    );
    assert_eq!(admin.cmd(&["EXISTS", "app:out"]), Reply::Int(0));
    assert!(c.cmd(&["SORT", "app:l", "STORE", "elsewhere"]).is_error());
    assert_eq!(admin.cmd(&["EXISTS", "elsewhere"]), Reply::Int(0));
}

// ---- SWAPDB ----

#[test]
fn swapdb_exchanges_everything_two_databases_hold() {
    let (db, s) = fresh();
    s.execute(&["MSET", "a", "zero", "only0", "x"]);
    s.execute(&["SET", "ttl", "v", "PX", "60000"]);
    s.execute(&["HSET", "h", "f", "1", "g", "2"]);
    s.execute(&["HPEXPIRE", "h", "50", "FIELDS", "1", "f"]);
    ok(s.execute(&["SELECT", "1"]));
    s.execute(&["MSET", "a", "one", "only1", "y"]);
    s.execute(&["RPUSH", "list", "1", "2"]);
    s.execute(&["SET", "soon", "v", "PX", "50"]);
    ok(s.execute(&["SWAPDB", "0", "1"]));
    // Still in database 1, which now holds what 0 held.
    assert_eq!(s.execute(&["GET", "a"]), b("zero"));
    assert_eq!(int(s.execute(&["DBSIZE"])), 4);
    assert!(exists(&s, "only0") && !exists(&s, "only1"));
    let ttl = int(s.execute(&["PTTL", "ttl"]));
    assert!((50_000..=60_000).contains(&ttl), "{ttl}");
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["GET", "a"]), b("one"));
    assert_eq!(int(s.execute(&["DBSIZE"])), 4);
    assert_eq!(
        s.execute(&["LRANGE", "list", "0", "-1"]),
        bulks(&["1", "2"])
    );
    // Deadlines travel with their keys and fields.
    db.advance_clock(100);
    assert!(!exists(&s, "soon"));
    assert_eq!(int(s.execute(&["DBSIZE"])), 3);
    ok(s.execute(&["SELECT", "1"]));
    assert_eq!(s.execute(&["HGETALL", "h"]), bulks(&["g", "2"]));
    // With itself, and with a database never used.
    ok(s.execute(&["SWAPDB", "1", "1"]));
    assert_eq!(s.execute(&["GET", "a"]), b("zero"));
    ok(s.execute(&["SWAPDB", "1", "9"]));
    assert_eq!(int(s.execute(&["DBSIZE"])), 0);
    ok(s.execute(&["SELECT", "9"]));
    assert_eq!(s.execute(&["GET", "a"]), b("zero"));
    assert_eq!(int(s.execute(&["DBSIZE"])), 4);
    for (x, y) in [
        ("0", "16"),
        ("-1", "0"),
        ("a", "0"),
        ("0", "b"),
        ("0", "99999999999999999999"),
    ] {
        let e = error(s.execute(&["SWAPDB", x, y]));
        assert!(e.starts_with("ERR"), "{x} {y}: {e}");
    }
    assert!(s.execute(&["SWAPDB", "0"]).is_error());
    assert!(s.execute(&["SWAPDB", "0", "1", "2"]).is_error());
    // In a transaction.
    ok(s.execute(&["MULTI"]));
    s.execute(&["SET", "k", "in9"]);
    s.execute(&["SWAPDB", "9", "0"]);
    s.execute(&["GET", "k"]);
    assert_eq!(
        s.execute(&["EXEC"]),
        Reply::Array(vec![Reply::Ok, Reply::Ok, Reply::Nil])
    );
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(s.execute(&["GET", "k"]), b("in9"));
}

#[test]
fn swapdb_keeps_counts_scans_and_random_keys_right() {
    let (_db, s) = fresh();
    for i in 0..500 {
        s.execute(&["SET", &format!("zero{i}"), "v"]);
    }
    ok(s.execute(&["SELECT", "3"]));
    for i in 0..70 {
        s.execute(&["SET", &format!("three{i}"), "v"]);
    }
    ok(s.execute(&["SWAPDB", "3", "0"]));
    assert_eq!(int(s.execute(&["DBSIZE"])), 500);
    assert_eq!(items(s.execute(&["KEYS", "*"])).len(), 500);
    assert!(text(s.execute(&["RANDOMKEY"])).starts_with("zero"));
    let mut seen = 0;
    let mut cursor = "0".to_string();
    loop {
        let page = items(s.execute(&["SCAN", &cursor, "COUNT", "64"]));
        cursor = text(page[0].clone());
        seen += items(page[1].clone()).len();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen, 500);
    ok(s.execute(&["FLUSHDB"]));
    assert_eq!(int(s.execute(&["DBSIZE"])), 0);
    ok(s.execute(&["SELECT", "0"]));
    assert_eq!(int(s.execute(&["DBSIZE"])), 70);
    assert!(text(s.execute(&["RANDOMKEY"])).starts_with("three"));
    let info = text(s.execute(&["INFO", "keyspace"]));
    assert!(info.contains("db0:keys=70"), "{info}");
    assert!(!info.contains("db3:"), "{info}");
}

#[test]
fn swapdb_aborts_watchers_and_wakes_waiters() {
    let env = env();
    let mut a = env.client();
    let mut c = env.client();
    c.cmd(&["SET", "k", "zero"]);
    ok(a.cmd(&["WATCH", "k"]));
    ok(a.cmd(&["MULTI"]));
    a.cmd(&["GET", "k"]);
    ok(c.cmd(&["SELECT", "1"]));
    c.cmd(&["SET", "k", "one"]);
    c.cmd(&["RPUSH", "list", "x"]);
    let mut waiter = env.client();
    waiter.send(&["BLPOP", "list", "0"]);
    assert!(waiter.silent_for(100));
    ok(c.cmd(&["SWAPDB", "0", "1"]));
    assert!(matches!(a.cmd(&["EXEC"]), Reply::Nil | Reply::NilArray));
    assert_eq!(a.cmd(&["GET", "k"]), b("one"));
    // The list arrived in the waiter's database with the swap.
    assert_eq!(waiter.read(), bulks(&["list", "x"]));
    assert_eq!(c.cmd(&["GET", "k"]), b("zero"));
}

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("hotstash-p2-{}-{name}.hc", std::process::id()));
    remove(&path);
    path
}

fn remove(path: &Path) {
    for suffix in ["", ".lock", ".new", ".tmp"] {
        let mut name = path.to_path_buf().into_os_string();
        name.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(name));
    }
}

fn open_at(path: &Path) -> (Db, Session) {
    let options = PersistOptions {
        interval: Duration::from_millis(5),
        ..PersistOptions::default()
    };
    let db = Db::open_with(Some(path), engine_cfg(4), options).unwrap();
    let s = db.session().unwrap();
    (db, s)
}

#[test]
fn a_swap_and_the_new_kinds_of_value_survive_a_restart() {
    let path = scratch("swap");
    let (db, s) = open_at(&path);
    s.execute(&["MSET", "a", "zero", "only0", "x"]);
    db.sync().unwrap();
    ok(s.execute(&["SELECT", "2"]));
    s.execute(&["MSET", "a", "two", "only2", "y"]);
    s.execute(&["HSET", "h", "f", "v"]);
    ok(s.execute(&["SWAPDB", "0", "2"]));
    // Written after the swap, on both sides.
    s.execute(&["SET", "after", "in2"]);
    s.execute(&["DEL", "only0"]);
    s.execute(&["BITFIELD", "bits", "SET", "u16", "4", "4660"]);
    s.execute(&["GEOADD", "geo", "13.361389", "38.115556", "Palermo"]);
    s.execute(&["RPUSH", "l", "2", "1"]);
    s.execute(&["SORT", "l", "STORE", "sorted"]);
    db.sync().unwrap();
    drop(s);
    drop(db);
    let (db, s) = open_at(&path);
    assert_eq!(s.execute(&["GET", "a"]), b("two"));
    assert!(exists(&s, "only2") && exists(&s, "h") && !exists(&s, "only0"));
    assert_eq!(int(s.execute(&["DBSIZE"])), 3);
    ok(s.execute(&["SELECT", "2"]));
    assert_eq!(s.execute(&["GET", "a"]), b("zero"));
    assert_eq!(s.execute(&["GET", "after"]), b("in2"));
    assert!(!exists(&s, "only0"));
    assert_eq!(
        s.execute(&["BITFIELD", "bits", "GET", "u16", "4"]),
        ints(&[4660])
    );
    assert_eq!(
        s.execute(&["GEOHASH", "geo", "Palermo"]),
        bulks(&["sqc8b49rny0"])
    );
    assert_eq!(
        s.execute(&["LRANGE", "sorted", "0", "-1"]),
        bulks(&["1", "2"])
    );
    assert_eq!(int(s.execute(&["DBSIZE"])), 6);
    drop(s);
    drop(db);
    remove(&path);
}

// ---- SLOWLOG and LATENCY ----

/// The arguments of every entry of SLOWLOG GET, newest first.
fn slow_args(c: &mut Client, count: &str) -> Vec<Vec<String>> {
    items(c.cmd(&["SLOWLOG", "GET", count]))
        .into_iter()
        .map(|entry| {
            let entry = items(entry);
            assert_eq!(entry.len(), 6, "{entry:?}");
            assert!(int(entry[0].clone()) >= 0);
            assert!(int(entry[1].clone()) > 1_600_000_000);
            assert!(int(entry[2].clone()) >= 0);
            assert!(text(entry[4].clone()).starts_with("127.0.0.1:"));
            items(entry[3].clone()).into_iter().map(text).collect()
        })
        .collect()
}

#[test]
fn slowlog_records_what_is_slow_and_stays_bounded() {
    let env = env();
    let mut c = env.client();
    // Nothing is this slow by default.
    c.cmd(&["SET", "quick", "v"]);
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "slowlog-log-slower-than"]),
        bulks(&["slowlog-log-slower-than", "10000"])
    );
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "slowlog-max-len"]),
        bulks(&["slowlog-max-len", "128"])
    );
    ok(c.cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "0"]));
    ok(c.cmd(&["CLIENT", "SETNAME", "tester"]));
    c.cmd(&["SET", "k", "v"]);
    c.cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]);
    let log = slow_args(&mut c, "10");
    assert!(
        log.contains(&vec!["SET".to_string(), "k".to_string(), "v".to_string()]),
        "{log:?}"
    );
    assert!(log.iter().any(|args| args[0] == "EVAL"), "{log:?}");
    assert!(
        !log.iter().any(|args| args.contains(&"quick".to_string())),
        "{log:?}"
    );
    let entries = items(c.cmd(&["SLOWLOG", "GET"]));
    assert_eq!(items(entries[0].clone())[5], b("tester"));
    // Newest first, numbered without gaps.
    let ids: Vec<i64> = entries
        .iter()
        .map(|e| int(items(e.clone())[0].clone()))
        .collect();
    for pair in ids.windows(2) {
        assert_eq!(pair[0], pair[1] + 1, "{ids:?}");
    }
    assert_eq!(slow_args(&mut c, "1").len(), 1);
    let all = slow_args(&mut c, "-1").len();
    assert_eq!(int(c.cmd(&["SLOWLOG", "LEN"])) as usize, all + 1);

    // Long arguments and long commands are cut.
    let long = "x".repeat(1000);
    c.cmd(&["SET", "big", &long]);
    let mut many = vec!["MSET".to_string()];
    for i in 0..20 {
        many.push(format!("m{i}"));
        many.push("v".into());
    }
    c.cmd(&many);
    let log = slow_args(&mut c, "2");
    assert_eq!(log[0].len(), 32);
    assert_eq!(log[0][31], "... (10 more arguments)");
    assert_eq!(log[1][2].len(), 128 + "... (872 more bytes)".len());
    assert!(log[1][2].ends_with("... (872 more bytes)"));

    // The length is bounded, and shrinks when the bound does.
    ok(c.cmd(&["CONFIG", "SET", "slowlog-max-len", "3"]));
    for _ in 0..10 {
        c.cmd(&["PING"]);
    }
    assert_eq!(int(c.cmd(&["SLOWLOG", "LEN"])), 3);
    assert_eq!(slow_args(&mut c, "100").len(), 3);
    // No setting lets the log grow without end.
    assert!(c
        .cmd(&["CONFIG", "SET", "slowlog-max-len", "100000000"])
        .is_error());
    ok(c.cmd(&["CONFIG", "SET", "slowlog-max-len", "1000"]));
    for _ in 0..300 {
        c.cmd(&["PING"]);
    }
    assert!(int(c.cmd(&["SLOWLOG", "LEN"])) > 300);

    ok(c.cmd(&["SLOWLOG", "RESET"]));
    assert!(int(c.cmd(&["SLOWLOG", "LEN"])) <= 1);
    // A negative threshold logs nothing.
    ok(c.cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"]));
    ok(c.cmd(&["SLOWLOG", "RESET"]));
    for _ in 0..5 {
        c.cmd(&["PING"]);
    }
    assert_eq!(int(c.cmd(&["SLOWLOG", "LEN"])), 0);
    assert_eq!(c.cmd(&["SLOWLOG", "GET"]), Reply::Array(vec![]));
    assert!(!items(c.cmd(&["SLOWLOG", "HELP"])).is_empty());
    for bad in [
        vec!["SLOWLOG"],
        vec!["SLOWLOG", "BOGUS"],
        vec!["SLOWLOG", "GET", "x"],
        vec!["SLOWLOG", "GET", "-2"],
        vec!["SLOWLOG", "GET", "1", "2"],
        vec!["SLOWLOG", "LEN", "1"],
        vec!["CONFIG", "SET", "slowlog-max-len", "-1"],
        vec!["CONFIG", "SET", "slowlog-log-slower-than", "x"],
    ] {
        assert!(c.cmd(&bad).is_error(), "{bad:?}");
    }
}

#[test]
fn slowlog_never_holds_a_password() {
    let env = env();
    let mut c = env.client();
    ok(c.cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "0"]));
    ok(c.cmd(&["ACL", "SETUSER", "u", "on", ">hunter2secret", "~*", "+@all"]));
    ok(c.cmd(&["AUTH", "u", "hunter2secret"]));
    c.cmd(&["AUTH", "default", "wrong-hunter2secret"]);
    let mut d = env.client();
    d.send(&["HELLO", "2", "AUTH", "u", "hunter2secret"]);
    d.read();
    c.cmd(&["HELLO", "2", "AUTH", "default", "nopass-hunter2secret"]);
    ok(c.cmd(&["CONFIG", "SET", "requirepass", "hunter2secret"]));
    ok(c.cmd(&["AUTH", "hunter2secret"]));
    ok(c.cmd(&["CONFIG", "SET", "requirepass", ""]));
    // In a transaction as well.
    ok(c.cmd(&["MULTI"]));
    c.cmd(&["ACL", "SETUSER", "v", "on", ">hunter2secret"]);
    c.cmd(&["EXEC"]);
    c.cmd(&["SET", "marker", "v"]);
    let log = slow_args(&mut c, "-1");
    assert!(log.iter().any(|args| args.contains(&"marker".to_string())));
    for args in &log {
        assert!(
            !args.iter().any(|arg| arg.contains("hunter2secret")),
            "a password in the slow log: {args:?}"
        );
    }
}

#[test]
fn slowlog_times_commands_that_ran_on_another_thread() {
    let env = env();
    let mut c = env.client();
    ok(c.cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "20000"]));
    // A script that takes a while: slow, and the only slow thing.
    let spin = "local n = 0 for i = 1, 30000000 do n = n + 1 end return n";
    c.cmd(&["EVAL", spin, "0"]);
    c.cmd(&["PING"]);
    let log = slow_args(&mut c, "-1");
    assert_eq!(log.len(), 1, "{log:?}");
    assert_eq!(log[0][0], "EVAL");
    let entry = items(items(c.cmd(&["SLOWLOG", "GET"]))[0].clone());
    assert!(int(entry[2].clone()) >= 20_000);
    // A blocked client did not run for the time it waited.
    let mut waiter = env.client();
    waiter.send(&["BLPOP", "wait", "0"]);
    assert!(waiter.silent_for(150));
    c.cmd(&["RPUSH", "wait", "x"]);
    waiter.read();
    assert_eq!(slow_args(&mut c, "-1").len(), 1);
}

#[test]
fn latency_answers_as_a_server_that_measured_nothing() {
    let (_db, s) = fresh();
    assert_eq!(s.execute(&["LATENCY", "LATEST"]), Reply::Array(vec![]));
    assert_eq!(
        s.execute(&["LATENCY", "HISTORY", "command"]),
        Reply::Array(vec![])
    );
    assert_eq!(s.execute(&["LATENCY", "RESET"]), Reply::Int(0));
    assert_eq!(
        s.execute(&["LATENCY", "RESET", "command", "fork"]),
        Reply::Int(0)
    );
    assert!(!text(s.execute(&["LATENCY", "DOCTOR"])).is_empty());
    assert!(!items(s.execute(&["LATENCY", "HELP"])).is_empty());
    assert!(s.execute(&["LATENCY"]).is_error());
    assert!(s.execute(&["LATENCY", "BOGUS"]).is_error());
    assert!(s.execute(&["LATENCY", "HISTORY"]).is_error());
    // Without a connection there is nothing to watch.
    assert!(s.execute(&["MONITOR"]).is_error());
    assert!(s.execute(&["CLIENT", "TRACKING", "ON"]).is_error());
    ok(s.execute(&["CLIENT", "TRACKING", "OFF"]));
    assert_eq!(s.execute(&["SLOWLOG", "LEN"]), Reply::Int(0));
}

// ---- OBJECT FREQ ----

#[test]
fn object_freq_needs_an_lfu_policy_and_grows_with_use() {
    let (_db, s) = fresh();
    s.execute(&["SET", "k", "v"]);
    let e = error(s.execute(&["OBJECT", "FREQ", "k"]));
    assert!(e.contains("An LFU maxmemory policy is not selected"), "{e}");
    for policy in ["allkeys-lfu", "volatile-lfu"] {
        ok(s.execute(&["CONFIG", "SET", "maxmemory-policy", policy]));
        assert_eq!(
            s.execute(&["CONFIG", "GET", "maxmemory-policy"]),
            bulks(&["maxmemory-policy", policy])
        );
        assert!(int(s.execute(&["OBJECT", "FREQ", "k"])) >= 0);
    }
    assert_eq!(s.execute(&["OBJECT", "FREQ", "none"]), Reply::Nil);
    s.execute(&["SET", "cold", "v"]);
    s.execute(&["SET", "hot", "v"]);
    let start = int(s.execute(&["OBJECT", "FREQ", "hot"]));
    // Asking does not count as a use.
    for _ in 0..100 {
        s.execute(&["OBJECT", "FREQ", "cold"]);
    }
    assert_eq!(int(s.execute(&["OBJECT", "FREQ", "cold"])), start);
    for _ in 0..5000 {
        s.execute(&["GET", "hot"]);
    }
    let hot = int(s.execute(&["OBJECT", "FREQ", "hot"]));
    assert!(hot > start, "{hot} after 5000 reads, {start} before");
    assert!(hot <= 255);
    // Logarithmic: far from one step per read.
    assert!(hot < 100, "{hot}");
    ok(s.execute(&["CONFIG", "SET", "maxmemory-policy", "noeviction"]));
    assert!(s.execute(&["OBJECT", "FREQ", "hot"]).is_error());
}

// ---- MONITOR ----

fn line(c: &mut Client) -> String {
    match c.read() {
        Reply::Simple(s) => s,
        Reply::Ok => "OK".into(),
        other => panic!("not a monitor line: {other:?}"),
    }
}

#[test]
fn monitor_shows_commands_as_they_arrive() {
    let env = env();
    let mut m = env.client();
    let mut m2 = env.client();
    let mut c = env.client();
    ok(m.cmd(&["MONITOR"]));
    ok(m2.cmd(&["MONITOR"]));
    assert!(line(&mut m).ends_with("\"MONITOR\""));
    c.cmd(&["SET", "k", "a \"quoted\"\r\nvalue\\\x01"]);
    for who in [&mut m, &mut m2] {
        let got = line(who);
        let (stamp, rest) = got.split_once(' ').expect("a time, then the rest");
        let (secs, micros) = stamp.split_once('.').expect("seconds.micros");
        assert!(secs.parse::<u64>().expect("seconds") > 1_600_000_000);
        assert_eq!(micros.len(), 6);
        assert!(rest.starts_with("[0 127.0.0.1:"), "{rest}");
        assert!(
            rest.ends_with("] \"SET\" \"k\" \"a \\\"quoted\\\"\\r\\nvalue\\\\\\x01\""),
            "{rest}"
        );
    }
    ok(c.cmd(&["SELECT", "5"]));
    line(&mut m);
    c.cmd(&["get", "k"]);
    assert!(line(&mut m).contains("[5 127.0.0.1:"));
    // Commands that fail, are unknown or are queued show too.
    c.cmd(&["NOSUCH", "thing"]);
    assert!(line(&mut m).ends_with("\"NOSUCH\" \"thing\""));
    ok(c.cmd(&["MULTI"]));
    c.cmd(&["INCR", "n"]);
    c.cmd(&["EXEC"]);
    assert!(line(&mut m).ends_with("\"MULTI\""));
    assert!(line(&mut m).ends_with("\"INCR\" \"n\""));
    assert!(line(&mut m).ends_with("\"EXEC\""));
    // A very long argument is cut.
    c.cmd(&["SET", "big", &"y".repeat(100_000)]);
    let got = line(&mut m);
    assert!(
        got.len() < 5000 && got.ends_with("... (95904 more bytes)\""),
        "{}",
        got.len()
    );
    // One monitor leaving does not disturb the other.
    drop(m2);
    c.cmd(&["PING"]);
    assert!(line(&mut m).ends_with("\"PING\""));
    assert!(m.silent_for(100));
}

#[test]
fn monitor_never_shows_a_password() {
    let env = env();
    let mut admin = env.client();
    ok(admin.cmd(&["ACL", "SETUSER", "u", "on", ">hunter2secret", "~*", "+@all"]));
    let mut m = env.client();
    ok(m.cmd(&["MONITOR"]));
    let mut c = env.client();
    ok(c.cmd(&["AUTH", "u", "hunter2secret"]));
    c.cmd(&["AUTH", "wrong-hunter2secret"]);
    c.cmd(&["auth", "u", "wrong-hunter2secret"]);
    c.cmd(&["HELLO", "2", "AUTH", "u", "hunter2secret"]);
    let mut c = env.client();
    c.cmd(&["ACL", "SETUSER", "v", "on", ">hunter2secret"]);
    c.cmd(&["CONFIG", "SET", "requirepass", "hunter2secret"]);
    c.cmd(&["AUTH", "hunter2secret"]);
    c.cmd(&["CONFIG", "SET", "requirepass", ""]);
    c.cmd(&["MULTI"]);
    c.cmd(&["AUTH", "u", "hunter2secret"]);
    c.cmd(&["EXEC"]);
    c.cmd(&["SET", "marker", "v"]);
    loop {
        let got = line(&mut m);
        assert!(
            !got.contains("hunter2secret"),
            "a password on the wire: {got}"
        );
        if got.contains("marker") {
            break;
        }
    }
}

#[test]
fn monitor_is_for_administrators_and_ends_with_reset() {
    let env = env();
    let mut admin = env.client();
    ok(admin.cmd(&["ACL", "SETUSER", "u", "on", ">pw", "~*", "+@all", "-@admin"]));
    let mut c = env.client();
    ok(c.cmd(&["AUTH", "u", "pw"]));
    assert!(error(c.cmd(&["MONITOR"])).starts_with("NOPERM"));
    admin.cmd(&["SET", "k", "v"]);
    assert!(c.silent_for(100));
    // Not inside a transaction.
    ok(admin.cmd(&["MULTI"]));
    assert!(admin.cmd(&["MONITOR"]).is_error());
    admin.cmd(&["DISCARD"]);
    assert!(admin.cmd(&["MONITOR", "extra"]).is_error());
    admin.cmd(&["SET", "k", "v"]);
    assert!(admin.silent_for(100));

    let mut m = env.client();
    ok(m.cmd(&["MONITOR"]));
    // Twice is once.
    ok(m.cmd(&["MONITOR"]));
    admin.cmd(&["SET", "k", "1"]);
    let mut seen = 0;
    while !m.silent_for(150) {
        if line(&mut m).contains("\"SET\"") {
            seen += 1;
        }
    }
    assert_eq!(seen, 1);
    m.send(&["RESET"]);
    loop {
        let got = line(&mut m);
        if got == "RESET" {
            break;
        }
        assert!(got.contains("RESET"), "{got}");
    }
    admin.cmd(&["SET", "k", "2"]);
    assert!(m.silent_for(150));
    assert_eq!(m.cmd(&["GET", "k"]), b("2"));
}

#[test]
fn a_monitor_that_does_not_read_is_dropped() {
    let _heavy = heavy();
    let env = env();
    let mut m = env.client();
    ok(m.cmd(&["MONITOR"]));
    let mut c = env.client();
    let value = "z".repeat(4000);
    let mut batch = Vec::new();
    for _ in 0..200 {
        batch.extend_from_slice(&wire_str(&["SET", "k", &value]));
    }
    // 80 MB of lines for a reader that takes none of them.
    let mut dropped = false;
    for _ in 0..200 {
        c.send_raw(&batch);
        for _ in 0..200 {
            ok(c.read());
        }
        if env.server.connections() == 1 {
            dropped = true;
            break;
        }
    }
    assert!(
        dropped || env.wait_conns(1, 10) == 1,
        "the monitor is still connected"
    );
    // The writer was never held up, and the server is fine.
    assert_eq!(c.cmd(&["PING"]), Reply::Simple("PONG".into()));
    let mut m = env.client();
    ok(m.cmd(&["MONITOR"]));
    c.cmd(&["PING"]);
    assert!(line(&mut m).ends_with("\"PING\""));
}

// ---- CLIENT TRACKING ----

/// A RESP3 connection: replies, and the pushes that come between them.
struct Client3 {
    c: Client,
    pushes: VecDeque<Vec<Reply>>,
}

fn read_line3<R: BufRead>(r: &mut R) -> Vec<u8> {
    let mut line = Vec::new();
    let n = r.read_until(b'\n', &mut line).expect("reading a line");
    assert!(n >= 3 && line.ends_with(b"\r\n"), "torn line {line:?}");
    line.truncate(line.len() - 2);
    line
}

/// One RESP3 value; `true` when it is a push.
fn read3<R: BufRead>(r: &mut R) -> (bool, Reply) {
    let line = read_line3(r);
    let body = std::str::from_utf8(&line[1..]).expect("ascii");
    let count = |body: &str| body.parse::<usize>().expect("a count");
    let many = |r: &mut R, n: usize| (0..n).map(|_| read3(r).1).collect::<Vec<_>>();
    let value = match line[0] {
        b'+' if body == "OK" => Reply::Ok,
        b'+' => Reply::Simple(body.into()),
        b'-' => Reply::Error(body.into()),
        b':' => Reply::Int(body.parse().expect("an integer")),
        b'#' => Reply::Int(i64::from(body == "t")),
        b'_' => Reply::Nil,
        b',' | b'(' => Reply::bulk(body),
        b'$' | b'=' => {
            if body == "-1" {
                Reply::Nil
            } else {
                let mut buf = vec![0u8; count(body) + 2];
                r.read_exact(&mut buf).expect("a bulk string");
                buf.truncate(buf.len() - 2);
                Reply::Bulk(buf)
            }
        }
        b'*' if body == "-1" => Reply::NilArray,
        b'*' | b'~' => Reply::Array(many(r, count(body))),
        b'%' => Reply::Array(many(r, 2 * count(body))),
        b'>' => return (true, Reply::Array(many(r, count(body)))),
        other => panic!("unknown RESP3 type {:?}", other as char),
    };
    (false, value)
}

impl Client3 {
    fn new(env: &Env) -> Client3 {
        let mut me = Client3 {
            c: env.client(),
            pushes: VecDeque::new(),
        };
        let hello = items(me.cmd(&["HELLO", "3"]));
        assert!(hello.contains(&b("proto")));
        me
    }

    /// The reply to `args`; pushes that come first are kept for `push`.
    fn cmd(&mut self, args: &[&str]) -> Reply {
        self.c.send(args);
        loop {
            match read3(&mut self.c.rd) {
                (true, push) => self.pushes.push_back(items(push)),
                (false, reply) => return reply,
            }
        }
    }

    fn push(&mut self) -> Vec<Reply> {
        if let Some(push) = self.pushes.pop_front() {
            return push;
        }
        match read3(&mut self.c.rd) {
            (true, push) => items(push),
            (false, reply) => panic!("a reply where a push was due: {reply:?}"),
        }
    }

    /// The keys of the next invalidation; `None` when it is for every key.
    fn invalidated(&mut self) -> Option<Vec<String>> {
        let push = self.push();
        assert_eq!(push.len(), 2, "{push:?}");
        assert_eq!(push[0], b("invalidate"));
        match push[1].clone() {
            Reply::Nil => None,
            keys => Some(items(keys).into_iter().map(text).collect()),
        }
    }

    fn quiet(&mut self, millis: u64) -> bool {
        self.pushes.is_empty() && self.c.silent_for(millis)
    }
}

fn key(name: &str) -> Option<Vec<String>> {
    Some(vec![name.to_string()])
}

#[test]
fn tracking_tells_a_reader_once_when_its_key_changes() {
    let env = env();
    let mut a = Client3::new(&env);
    let mut w = env.client();
    w.cmd(&["MSET", "k", "1", "other", "1", "h", "1"]);
    // Read before tracking: not remembered.
    a.cmd(&["GET", "other"]);
    ok(a.cmd(&["CLIENT", "TRACKING", "ON"]));
    assert_eq!(a.cmd(&["GET", "k"]), b("1"));
    w.cmd(&["SET", "other", "2"]);
    assert!(a.quiet(100));
    w.cmd(&["SET", "k", "2"]);
    assert_eq!(a.invalidated(), key("k"));
    // Told once: the key must be read again to be remembered again.
    w.cmd(&["SET", "k", "3"]);
    assert!(a.quiet(100));
    assert_eq!(a.cmd(&["GET", "k"]), b("3"));
    assert_eq!(a.cmd(&["GET", "k"]), b("3"));
    w.cmd(&["APPEND", "k", "x"]);
    assert_eq!(a.invalidated(), key("k"));
    assert!(a.quiet(100));
    // A key that does not exist yet is remembered as well.
    assert_eq!(a.cmd(&["GET", "later"]), Reply::Nil);
    w.cmd(&["SET", "later", "v"]);
    assert_eq!(a.invalidated(), key("later"));
    // Every way a key changes: delete, rename, expire, a write that fails
    // to change it does not count.
    a.cmd(&["MGET", "k", "later"]);
    w.cmd(&["DEL", "k"]);
    assert_eq!(a.invalidated(), key("k"));
    w.cmd(&["RENAME", "later", "moved"]);
    assert_eq!(a.invalidated(), key("later"));
    a.cmd(&["EXISTS", "moved"]);
    a.cmd(&["STRLEN", "other"]);
    w.cmd(&["PEXPIRE", "moved", "30"]);
    assert_eq!(a.invalidated(), key("moved"));
    a.cmd(&["GET", "moved"]);
    thread::sleep(Duration::from_millis(80));
    w.cmd(&["GET", "moved"]);
    assert_eq!(a.invalidated(), key("moved"));
    w.cmd(&["SETNX", "other", "no"]);
    assert!(a.quiet(100));
    // Writes of the reader itself count like any other.
    a.cmd(&["HGET", "h", "f"]);
    assert!(a.cmd(&["HSET", "h", "f", "v"]).is_error());
    assert!(a.quiet(100));
    a.cmd(&["GET", "mine"]);
    ok(a.cmd(&["SET", "mine", "v"]));
    assert_eq!(a.invalidated(), key("mine"));
    // Scripts and transactions.
    a.cmd(&["GET", "s1"]);
    a.cmd(&["GET", "s2"]);
    w.cmd(&[
        "EVAL",
        "redis.call('SET', KEYS[1], 'v') return 1",
        "1",
        "s1",
    ]);
    assert_eq!(a.invalidated(), key("s1"));
    w.cmd(&["MULTI"]);
    w.cmd(&["INCR", "s2"]);
    w.cmd(&["EXEC"]);
    assert_eq!(a.invalidated(), key("s2"));
    assert!(a.quiet(100));
}

#[test]
fn positions_are_doubles_in_resp3() {
    let env = env();
    let mut c = env.client();
    c.cmd(&["GEOADD", "g", "13.361389", "38.115556", "Palermo"]);
    let raw = |c: &mut Client, args: &[&str]| -> String {
        c.send(args);
        assert!(!c.silent_for(2000));
        String::from_utf8_lossy(c.rd.buffer()).into_owned()
    };
    let mut c3 = Client3::new(&env);
    let pos = raw(&mut c3.c, &["GEOPOS", "g", "Palermo", "Nowhere"]);
    assert!(pos.starts_with("*2\r\n*2\r\n,13.36138"), "{pos:?}");
    assert!(pos.ends_with("\r\n_\r\n"), "{pos:?}");
    let mut c3 = Client3::new(&env);
    let hits = raw(
        &mut c3.c,
        &[
            "GEOSEARCH",
            "g",
            "FROMMEMBER",
            "Palermo",
            "BYRADIUS",
            "1",
            "km",
            "WITHDIST",
            "WITHCOORD",
        ],
    );
    assert!(
        hits.starts_with("*1\r\n*3\r\n$7\r\nPalermo\r\n$6\r\n0.0000\r\n*2\r\n,13.36138"),
        "{hits:?}"
    );
    // RESP2 has only strings.
    let pos = raw(&mut c, &["GEOPOS", "g", "Palermo"]);
    assert!(pos.starts_with("*1\r\n*2\r\n$"), "{pos:?}");
}

#[test]
fn tracking_sends_a_null_when_everything_goes() {
    let env = env();
    let mut a = Client3::new(&env);
    let mut idle = Client3::new(&env);
    ok(idle.cmd(&["CLIENT", "TRACKING", "ON"]));
    ok(a.cmd(&["CLIENT", "TRACKING", "ON"]));
    let mut w = env.client();
    w.cmd(&["SET", "k", "v"]);
    a.cmd(&["GET", "k"]);
    ok(w.cmd(&["FLUSHALL"]));
    assert_eq!(a.invalidated(), None);
    assert_eq!(idle.invalidated(), None);
    assert!(a.quiet(100));
    a.cmd(&["GET", "k"]);
    ok(w.cmd(&["FLUSHDB"]));
    assert_eq!(a.invalidated(), None);
    // What a swap brings in may differ from what was read.
    w.cmd(&["SET", "k", "zero"]);
    a.cmd(&["GET", "k"]);
    ok(w.cmd(&["SWAPDB", "0", "1"]));
    let mut told = a.invalidated();
    if told == key("k") {
        told = a.invalidated();
    }
    assert_eq!(told, None);
    assert_eq!(a.cmd(&["GET", "k"]), Reply::Nil);
}

#[test]
fn tracking_by_prefix_needs_no_read() {
    let env = env();
    let mut a = Client3::new(&env);
    let mut w = env.client();
    ok(a.cmd(&[
        "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "user:", "PREFIX", "order:",
    ]));
    w.cmd(&["SET", "user:1", "v"]);
    assert_eq!(a.invalidated(), key("user:1"));
    // Every change, not only the first.
    w.cmd(&["SET", "user:1", "w"]);
    assert_eq!(a.invalidated(), key("user:1"));
    w.cmd(&["SET", "other", "v"]);
    w.cmd(&["SET", "use", "v"]);
    assert!(a.quiet(100));
    w.cmd(&["DEL", "order:9"]);
    assert!(a.quiet(100));
    w.cmd(&["MSET", "order:9", "v", "nope", "v"]);
    assert_eq!(a.invalidated(), key("order:9"));
    // Its own writes come back too, unless it asked otherwise.
    ok(a.cmd(&["SET", "user:2", "v"]));
    assert_eq!(a.invalidated(), key("user:2"));
    // More prefixes may be added; they must not overlap.
    ok(a.cmd(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "cart:"]));
    w.cmd(&["SET", "cart:1", "v"]);
    assert_eq!(a.invalidated(), key("cart:1"));
    for prefix in ["user:1", "use", ""] {
        let e = error(a.cmd(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", prefix]));
        assert!(e.contains("overlaps"), "{prefix}: {e}");
    }
    let e = error(a.cmd(&[
        "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "x", "PREFIX", "xy",
    ]));
    assert!(e.contains("overlaps"), "{e}");
    w.cmd(&["SET", "xy1", "v"]);
    assert!(a.quiet(100));
    // Without a prefix: every key.
    let mut all = Client3::new(&env);
    ok(all.cmd(&["CLIENT", "TRACKING", "ON", "BCAST"]));
    w.cmd(&["SET", "anything", "v"]);
    assert_eq!(all.invalidated(), key("anything"));
    w.cmd(&["FLUSHALL"]);
    assert_eq!(all.invalidated(), None);
    assert_eq!(a.invalidated(), None);
    // The mode cannot change while tracking is on.
    assert!(error(a.cmd(&["CLIENT", "TRACKING", "ON"])).contains("BCAST"));
    let mut plain = Client3::new(&env);
    ok(plain.cmd(&["CLIENT", "TRACKING", "ON"]));
    assert!(error(plain.cmd(&["CLIENT", "TRACKING", "ON", "BCAST"])).contains("BCAST"));
    ok(plain.cmd(&["CLIENT", "TRACKING", "OFF"]));
    assert!(error(plain.cmd(&["CLIENT", "TRACKING", "ON", "PREFIX", "p"])).contains("BCAST"));
    ok(a.cmd(&["CLIENT", "TRACKING", "OFF"]));
    w.cmd(&["SET", "user:1", "x"]);
    assert!(a.quiet(100));
}

#[test]
fn tracking_can_send_its_messages_to_another_connection() {
    let env = env();
    let mut sink = env.client();
    let sink_id = int(sink.cmd(&["CLIENT", "ID"])).to_string();
    let subscribed = items(sink.cmd(&["SUBSCRIBE", "__redis__:invalidate"]));
    assert_eq!(subscribed[0], b("subscribe"));
    let mut a = env.client();
    let own = int(a.cmd(&["CLIENT", "ID"])).to_string();
    assert_eq!(a.cmd(&["CLIENT", "GETREDIR"]), Reply::Int(-1));
    assert!(
        error(a.cmd(&["CLIENT", "TRACKING", "ON", "REDIRECT", "999999"]))
            .contains("does not exist")
    );
    assert!(a
        .cmd(&["CLIENT", "TRACKING", "ON", "REDIRECT", &own])
        .is_error());
    assert!(a
        .cmd(&["CLIENT", "TRACKING", "ON", "REDIRECT", "x"])
        .is_error());
    assert!(a
        .cmd(&["CLIENT", "TRACKING", "ON", "REDIRECT", "-1"])
        .is_error());
    assert_eq!(a.cmd(&["CLIENT", "GETREDIR"]), Reply::Int(-1));
    ok(a.cmd(&["CLIENT", "TRACKING", "ON", "REDIRECT", &sink_id]));
    assert_eq!(
        a.cmd(&["CLIENT", "GETREDIR"]),
        Reply::Int(sink_id.parse().unwrap())
    );
    a.cmd(&["GET", "k"]);
    let mut w = env.client();
    w.cmd(&["SET", "k", "v"]);
    assert_eq!(
        sink.read(),
        Reply::Array(vec![b("message"), b("__redis__:invalidate"), bulks(&["k"])])
    );
    assert!(a.silent_for(100));
    a.cmd(&["GET", "k"]);
    w.cmd(&["FLUSHALL"]);
    assert_eq!(
        sink.read(),
        Reply::Array(vec![b("message"), b("__redis__:invalidate"), Reply::Nil])
    );
    // A RESP2 connection that tracks without a redirect hears nothing and
    // is not harmed.
    let mut deaf = env.client();
    ok(deaf.cmd(&["CLIENT", "TRACKING", "ON"]));
    assert_eq!(deaf.cmd(&["CLIENT", "GETREDIR"]), Reply::Int(0));
    deaf.cmd(&["GET", "k"]);
    w.cmd(&["SET", "k", "v"]);
    assert!(deaf.silent_for(100));
    assert_eq!(deaf.cmd(&["GET", "k"]), b("v"));
    // A PUBLISH on that channel is an ordinary message; it is not confused
    // with a real invalidation, which carries an array.
    a.cmd(&["GET", "k"]);
    w.cmd(&["SET", "k", "w"]);
    assert_eq!(
        sink.read(),
        Reply::Array(vec![b("message"), b("__redis__:invalidate"), bulks(&["k"])])
    );
    // The sink goes away: the reader keeps working, nothing is sent.
    drop(sink);
    env.wait_conns(3, 5);
    a.cmd(&["GET", "k"]);
    w.cmd(&["SET", "k", "x"]);
    assert!(a.silent_for(100));
    assert_eq!(a.cmd(&["GET", "k"]), b("x"));
}

#[test]
fn tracking_optin_optout_and_noloop() {
    let env = env();
    let mut w = env.client();
    let mut a = Client3::new(&env);
    assert!(a.cmd(&["CLIENT", "CACHING", "YES"]).is_error());
    assert!(a
        .cmd(&["CLIENT", "TRACKING", "ON", "OPTIN", "OPTOUT"])
        .is_error());
    ok(a.cmd(&["CLIENT", "TRACKING", "ON", "OPTIN"]));
    assert!(a.cmd(&["CLIENT", "CACHING", "NO"]).is_error());
    assert!(a.cmd(&["CLIENT", "CACHING", "MAYBE"]).is_error());
    a.cmd(&["GET", "a"]);
    ok(a.cmd(&["CLIENT", "CACHING", "YES"]));
    a.cmd(&["GET", "b"]);
    // Only the command right after CACHING YES.
    a.cmd(&["GET", "c"]);
    w.cmd(&["MSET", "a", "1", "c", "1"]);
    assert!(a.quiet(100));
    w.cmd(&["SET", "b", "1"]);
    assert_eq!(a.invalidated(), key("b"));
    // Inside a transaction CACHING YES covers the whole of it.
    ok(a.cmd(&["CLIENT", "CACHING", "YES"]));
    ok(a.cmd(&["MULTI"]));
    a.cmd(&["GET", "t1"]);
    a.cmd(&["GET", "t2"]);
    a.cmd(&["EXEC"]);
    a.cmd(&["GET", "t3"]);
    w.cmd(&["SET", "t3", "1"]);
    assert!(a.quiet(100));
    w.cmd(&["SET", "t2", "1"]);
    assert_eq!(a.invalidated(), key("t2"));
    ok(a.cmd(&["CLIENT", "TRACKING", "OFF"]));

    ok(a.cmd(&["CLIENT", "TRACKING", "ON", "OPTOUT"]));
    assert!(a.cmd(&["CLIENT", "CACHING", "YES"]).is_error());
    a.cmd(&["GET", "d"]);
    ok(a.cmd(&["CLIENT", "CACHING", "NO"]));
    a.cmd(&["GET", "e"]);
    a.cmd(&["GET", "f"]);
    w.cmd(&["SET", "e", "1"]);
    assert!(a.quiet(100));
    w.cmd(&["SET", "d", "1"]);
    assert_eq!(a.invalidated(), key("d"));
    w.cmd(&["SET", "f", "1"]);
    assert_eq!(a.invalidated(), key("f"));
    ok(a.cmd(&["CLIENT", "TRACKING", "OFF"]));

    // NOLOOP: not told of its own writes, however they run.
    ok(a.cmd(&["CLIENT", "TRACKING", "ON", "NOLOOP"]));
    a.cmd(&["GET", "n"]);
    ok(a.cmd(&["SET", "n", "mine"]));
    assert!(a.quiet(100));
    w.cmd(&["SET", "n", "theirs"]);
    assert_eq!(a.invalidated(), key("n"));
    a.cmd(&["MGET", "n", "n2", "n3", "n4"]);
    ok(a.cmd(&["MSET", "n", "1", "n2", "1"]));
    a.cmd(&[
        "EVAL",
        "redis.call('SET', KEYS[1], 'v') return 1",
        "1",
        "n3",
    ]);
    ok(a.cmd(&["MULTI"]));
    a.cmd(&["SET", "n4", "1"]);
    a.cmd(&["EXEC"]);
    assert!(a.quiet(150), "{:?}", a.pushes);
    let mut bcast = Client3::new(&env);
    ok(bcast.cmd(&[
        "CLIENT", "TRACKING", "ON", "BCAST", "NOLOOP", "PREFIX", "p:",
    ]));
    ok(bcast.cmd(&["SET", "p:1", "v"]));
    ok(bcast.cmd(&["MSET", "p:2", "v", "p:3", "v"]));
    assert!(bcast.quiet(150), "{:?}", bcast.pushes);
    w.cmd(&["SET", "p:1", "w"]);
    assert_eq!(bcast.invalidated(), key("p:1"));
}

#[test]
fn tracking_reports_its_state_and_ends_cleanly() {
    let env = env();
    let mut w = env.client();
    let tracking = |w: &mut Client| -> i64 {
        let info = text(w.cmd(&["INFO", "clients"]));
        let line = info
            .lines()
            .find_map(|l| l.strip_prefix("tracking_clients:"))
            .expect("tracking_clients in INFO");
        line.trim().parse().expect("a number")
    };
    assert_eq!(tracking(&mut w), 0);
    let mut a = Client3::new(&env);
    let flags = |c: &mut Client3| -> (Vec<String>, i64, Vec<String>) {
        let info = items(c.cmd(&["CLIENT", "TRACKINGINFO"]));
        assert_eq!(info[0], b("flags"));
        assert_eq!(info[2], b("redirect"));
        assert_eq!(info[4], b("prefixes"));
        (
            items(info[1].clone()).into_iter().map(text).collect(),
            int(info[3].clone()),
            items(info[5].clone()).into_iter().map(text).collect(),
        )
    };
    assert_eq!(flags(&mut a), (vec!["off".to_string()], -1, vec![]));
    ok(a.cmd(&["CLIENT", "TRACKING", "ON", "OPTIN", "NOLOOP"]));
    let (f, redirect, prefixes) = flags(&mut a);
    assert!(
        f.contains(&"on".to_string())
            && f.contains(&"optin".to_string())
            && f.contains(&"noloop".to_string()),
        "{f:?}"
    );
    assert_eq!((redirect, prefixes.len()), (0, 0));
    assert_eq!(tracking(&mut w), 1);
    // ON again changes nothing and counts once.
    ok(a.cmd(&["CLIENT", "TRACKING", "ON", "OPTIN", "NOLOOP"]));
    assert_eq!(tracking(&mut w), 1);
    ok(a.cmd(&["CLIENT", "TRACKING", "OFF"]));
    ok(a.cmd(&["CLIENT", "TRACKING", "OFF"]));
    assert_eq!(tracking(&mut w), 0);
    assert_eq!(flags(&mut a).0, vec!["off".to_string()]);
    ok(a.cmd(&[
        "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "a:", "PREFIX", "b:",
    ]));
    let (f, _, mut prefixes) = flags(&mut a);
    prefixes.sort();
    assert!(f.contains(&"bcast".to_string()), "{f:?}");
    assert_eq!(prefixes, ["a:", "b:"]);
    // RESET, and a closed connection, stop it.
    assert_eq!(a.cmd(&["RESET"]), Reply::Simple("RESET".into()));
    assert_eq!(tracking(&mut w), 0);
    w.cmd(&["SET", "a:1", "v"]);
    assert!(a.quiet(100));
    let mut b1 = Client3::new(&env);
    let mut b2 = Client3::new(&env);
    ok(b1.cmd(&["CLIENT", "TRACKING", "ON"]));
    ok(b2.cmd(&["CLIENT", "TRACKING", "ON", "BCAST"]));
    b1.cmd(&["GET", "k"]);
    assert_eq!(tracking(&mut w), 2);
    drop(b1);
    drop(b2);
    assert!(wait_until(5, || tracking(&mut w) == 0));
    // Writes after they are gone go nowhere and harm nothing.
    w.cmd(&["SET", "k", "v"]);
    w.cmd(&["FLUSHALL"]);
    // Blocked clients are still counted for what they are.
    let info = text(w.cmd(&["INFO", "clients"]));
    assert!(info.contains("blocked_clients:0"), "{info}");
    for bad in [
        vec!["CLIENT", "TRACKING"],
        vec!["CLIENT", "TRACKING", "MAYBE"],
        vec!["CLIENT", "TRACKING", "ON", "BOGUS"],
        vec!["CLIENT", "TRACKING", "ON", "PREFIX"],
        vec!["CLIENT", "TRACKING", "ON", "REDIRECT"],
        vec!["CLIENT", "TRACKINGINFO", "x"],
        vec!["CLIENT", "GETREDIR", "x"],
        vec!["CLIENT", "CACHING"],
    ] {
        assert!(a.cmd(&bad).is_error(), "{bad:?}");
    }
    // Not from inside a transaction.
    ok(a.cmd(&["MULTI"]));
    assert!(a.cmd(&["CLIENT", "TRACKING", "ON"]).is_error());
    a.cmd(&["DISCARD"]);
    assert_eq!(tracking(&mut w), 0);
}

#[test]
fn tracking_does_not_show_a_restricted_user_other_keys() {
    let env = env();
    let mut admin = env.client();
    ok(admin.cmd(&["ACL", "SETUSER", "u", "on", ">pw", "~app:*", "+@all"]));
    let mut a = Client3::new(&env);
    ok(a.cmd(&["AUTH", "u", "pw"]));
    // Every key, or a prefix outside what it may read: refused.
    assert!(error(a.cmd(&["CLIENT", "TRACKING", "ON", "BCAST"])).starts_with("NOPERM"));
    assert!(
        error(a.cmd(&["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "secret:"]))
            .starts_with("NOPERM")
    );
    admin.cmd(&["SET", "secret:1", "v"]);
    assert!(a.quiet(100));
    ok(a.cmd(&["CLIENT", "TRACKING", "ON"]));
    assert!(a.cmd(&["GET", "secret:1"]).is_error());
    a.cmd(&["GET", "app:1"]);
    admin.cmd(&["SET", "secret:1", "w"]);
    assert!(a.quiet(100));
    admin.cmd(&["SET", "app:1", "v"]);
    assert_eq!(a.invalidated(), key("app:1"));
}

#[test]
fn tracking_never_leaves_a_stale_value_behind() {
    let env = env();
    let done = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for t in 0..2 {
        let addr = env.addr;
        let done = done.clone();
        writers.push(thread::spawn(move || {
            let mut w = Client::connect(addr);
            let mut n = 0u64;
            while !done.load(Ordering::SeqCst) {
                n += 1;
                // By every road a write can take.
                match n % 4 {
                    0 => w.cmd(&["SET", "k", &format!("{t}-{n}")]),
                    1 => w.cmd(&["MSET", "k", &format!("{t}-{n}"), "other", "v"]),
                    2 => w.cmd(&[
                        "EVAL",
                        "return redis.call('SET', KEYS[1], ARGV[1])",
                        "1",
                        "k",
                        &format!("{t}-{n}"),
                    ]),
                    _ => {
                        w.cmd(&["MULTI"]);
                        w.cmd(&["SET", "k", &format!("{t}-{n}")]);
                        w.cmd(&["EXEC"])
                    }
                };
            }
        }));
    }
    let mut a = Client3::new(&env);
    ok(a.cmd(&["CLIENT", "TRACKING", "ON"]));
    let mut cached = Reply::Nil;
    let mut fresh = false;
    for round in 0..3000 {
        // What a caching client does: read when it has nothing cached.
        a.pushes.clear();
        cached = match round % 3 {
            0 => a.cmd(&["GET", "k"]),
            1 => items(a.cmd(&["MGET", "k", "other"]))[0].clone(),
            _ => a.cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "k"]),
        };
        // A push that came before the reply was for an earlier read.
        a.pushes.clear();
        fresh = true;
        if round % 3 == 2 {
            // A script's reads are not tracked; do not count on it.
            fresh = false;
        }
    }
    cached = if fresh { cached } else { a.cmd(&["GET", "k"]) };
    a.pushes.clear();
    done.store(true, Ordering::SeqCst);
    for w in writers {
        w.join().expect("a writer");
    }
    // Everything is quiet now. Either the value in hand is the true one,
    // or a message is on its way to say it is not.
    let mut told = false;
    while !a.quiet(200) {
        told |= a.invalidated() == key("k");
    }
    let mut check = env.client();
    let now = check.cmd(&["GET", "k"]);
    assert!(
        told || now == cached,
        "cached {cached:?}, in truth {now:?}, and no word of it"
    );
}

// ---- what a review of this group found ----

#[test]
fn sort_refuses_a_result_that_get_would_blow_up() {
    let (_db, s) = fresh();
    let big = "x".repeat(1 << 20);
    s.execute(&["RPUSH", "l", &big, "small"]);
    let mut cmd = vec!["SORT", "l", "BY", "nosort"];
    for _ in 0..300 {
        cmd.extend(["GET", "#"]);
    }
    // 300 copies of a megabyte, asked for in a few kilobytes.
    let e = error(s.execute(&cmd));
    assert!(e.contains("too large"), "{e}");
    cmd.extend(["STORE", "dst"]);
    assert!(s.execute(&cmd).is_error());
    assert!(!exists(&s, "dst"));
    // The same shape at a sane size is fine.
    assert_eq!(
        items(s.execute(&["SORT", "l", "BY", "nosort", "LIMIT", "1", "1", "GET", "#", "GET", "#"])),
        vec![b("small"), b("small")]
    );
    assert_eq!(
        items(s.execute(&["SORT", "l", "BY", "nosort", "GET", "#"])).len(),
        2
    );
}

#[test]
fn tracking_takes_only_so_many_prefixes() {
    let env = env();
    let mut a = Client3::new(&env);
    let names: Vec<String> = (0..300).map(|n| format!("p{n:04}:")).collect();
    let with = |range: std::ops::Range<usize>| {
        let mut cmd = vec!["CLIENT", "TRACKING", "ON", "BCAST"];
        for name in &names[range] {
            cmd.extend(["PREFIX", name.as_str()]);
        }
        cmd
    };
    assert!(error(a.cmd(&with(0..300))).contains("too many prefixes"));
    // Nor by adding them up over several calls.
    ok(a.cmd(&with(0..200)));
    assert!(error(a.cmd(&with(200..300))).contains("too many prefixes"));
    ok(a.cmd(&with(200..256)));
    let mut w = env.client();
    w.cmd(&["SET", "p0255:x", "v"]);
    assert_eq!(a.invalidated(), key("p0255:x"));
    w.cmd(&["SET", "p0256:x", "v"]);
    assert!(a.quiet(100));
}

#[test]
fn a_user_who_loses_the_right_stops_watching() {
    let env = env();
    let mut admin = env.client();
    ok(admin.cmd(&[
        "ACL",
        "SETUSER",
        "ops",
        "on",
        ">pw",
        "allkeys",
        "allchannels",
        "+@all",
    ]));
    ok(admin.cmd(&[
        "ACL", "SETUSER", "low", "on", ">pw", "~app:*", "+@all", "-@admin",
    ]));
    let mut m = env.client();
    ok(m.cmd(&["AUTH", "ops", "pw"]));
    ok(m.cmd(&["MONITOR"]));
    let mut b3 = Client3::new(&env);
    ok(b3.cmd(&["AUTH", "ops", "pw"]));
    ok(b3.cmd(&["CLIENT", "TRACKING", "ON", "BCAST"]));
    // Logging in as a lesser user ends both at once.
    let mut m2 = env.client();
    ok(m2.cmd(&["MONITOR"]));
    let mut b2 = Client3::new(&env);
    ok(b2.cmd(&["CLIENT", "TRACKING", "ON", "BCAST"]));
    admin.cmd(&["SET", "seen", "1"]);
    assert_eq!(b2.invalidated(), key("seen"));
    ok(b2.cmd(&["AUTH", "low", "pw"]));
    m2.send(&["AUTH", "low", "pw"]);
    loop {
        match m2.read() {
            Reply::Ok => break,
            Reply::Simple(line) => assert!(!line.contains("pw\""), "{line}"),
            other => panic!("{other:?}"),
        }
    }
    admin.cmd(&["SET", "secret", "1"]);
    assert!(m2.silent_for(150));
    assert!(b2.quiet(150));
    // A change of the rules closes the connections that watch too much.
    ok(admin.cmd(&["ACL", "SETUSER", "ops", "-@admin", "resetkeys", "~app:*"]));
    assert_eq!(env.wait_conns(3, 5), 3);
    admin.cmd(&["SET", "secret", "2"]);
    for reply in m.read_until_closed() {
        let line = text(reply);
        assert!(!line.contains("\"2\""), "{line}");
    }
}
