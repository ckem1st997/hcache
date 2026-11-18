//! Bloom filters (BF.*) as valkey-bloom 1.0 answers them: the test vectors
//! of `docs/bloom-spec.md` section 15, the filter through DUMP, RESTORE,
//! RDB files, the data file and BF.LOAD, replicas that build the same
//! filter as their primary, and the keyspace events.

mod net;

use hotstash::{Config, Db, PersistOptions, Reply, Session};
use hotstash_core::bloom::FIXED_SEED;
use net::{engine_cfg, server_cfg, wait_until, Client, Env};
use std::time::Duration;

fn fresh() -> (Db, Session) {
    let db = Db::open(
        None,
        Config {
            shards: 4,
            ..Config::default()
        },
    )
    .unwrap();
    let s = db.session().unwrap();
    (db, s)
}

/// A session whose new filters take the fixed seed, so that false
/// positives fall where valkey-bloom's do.
fn fixed() -> (Db, Session) {
    let (db, s) = fresh();
    let reply = s.execute(&["CONFIG", "SET", "bf.bloom-use-random-seed", "no"]);
    assert_eq!(reply, Reply::Ok);
    (db, s)
}

fn run(s: &Session, line: &str) -> Reply {
    let args: Vec<&str> = line.split(' ').collect();
    s.execute(&args)
}

fn int(n: i64) -> Reply {
    Reply::Int(n)
}

fn b(s: &str) -> Reply {
    Reply::bulk(s)
}

fn simple(s: &str) -> Reply {
    Reply::Simple(s.into())
}

fn err(s: &str) -> Reply {
    Reply::Error(s.into())
}

fn ints(ns: &[i64]) -> Reply {
    Reply::Array(ns.iter().map(|&n| Reply::Int(n)).collect())
}

fn bytes(reply: Reply) -> Vec<u8> {
    match reply {
        Reply::Bulk(b) => b,
        other => panic!("not a bulk string: {other:?}"),
    }
}

/// Run `(request, expected reply)` in order.
fn check(s: &Session, vectors: &[(&str, Reply)]) {
    for (line, want) in vectors {
        assert_eq!(&run(s, line), want, "{line}");
    }
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

fn arity(name: &str) -> Reply {
    err(&format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}

#[test]
fn reserve_add_exists_and_card_answer_as_valkey_bloom_does() {
    let (_db, s) = fixed();
    let range = "ERR (0 < error rate range < 1)";
    check(
        &s,
        &[
            // 15.1
            ("BF.RESERVE bf 0.01 1000", Reply::Ok),
            ("BF.RESERVE bf 0.01 1000", err("ERR item exists")),
            ("BF.RESERVE bf_non 0.01 1000 NONSCALING", Reply::Ok),
            ("BF.RESERVE bf_exp 0.01 1000 EXPANSION 2", Reply::Ok),
            ("bf.reserve lower 0.01 100 nonscaling", Reply::Ok),
            ("BF.RESERVE k 1e-3 100", Reply::Ok),
            ("BF.RESERVE k2 0 100", err(range)),
            ("BF.RESERVE k2 1 100", err(range)),
            ("BF.RESERVE k2 2 100", err(range)),
            ("BF.RESERVE k2 -0.5 100", err(range)),
            ("BF.RESERVE k2 0.99999999999999999 100", err(range)),
            ("BF.RESERVE k2 String 100", err("ERR bad error rate")),
            (
                "BF.RESERVE k2 0.01 0",
                err("ERR (capacity should be larger than 0)"),
            ),
            ("BF.RESERVE k2 0.01 -1", err("ERR bad capacity")),
            ("BF.RESERVE k2 0.01 0.01", err("ERR bad capacity")),
            (
                "BF.RESERVE k2 0.01 9223372036854775808",
                err("ERR bad capacity"),
            ),
            (
                "BF.RESERVE k2 0.01 9223372036854775807",
                err("ERR operation exceeds bloom object memory limit"),
            ),
            (
                "BF.RESERVE k2 0.01 100 EXPANSION 0",
                err("ERR bad expansion"),
            ),
            ("BF.RESERVE k2 0.01 100 FOO", err("ERROR")),
            (
                "BF.RESERVE TT 0.01 1 NONSCALING EXPANSION 1",
                arity("BF.RESERVE"),
            ),
            ("BF.RESERVE KEY SSS", arity("BF.RESERVE")),
            ("SET s v", Reply::Ok),
            ("BF.RESERVE s abc 100", err("ERR bad error rate")),
            ("BF.RESERVE s 0.01 100", err(WRONGTYPE)),
            // 15.2
            ("BF.ADD key item", int(1)),
            ("BF.ADD key item", int(0)),
            ("BF.EXISTS key item", int(1)),
            ("BF.EXISTS nokey item", int(0)),
            ("BF.MADD m a b a", ints(&[1, 1, 0])),
            ("BF.MEXISTS m a b c", ints(&[1, 1, 0])),
            ("BF.CARD m", int(2)),
            ("BF.CARD nokey", int(0)),
            ("BF.ADD key", arity("BF.ADD")),
            ("BF.ADD key a b", arity("BF.ADD")),
            ("BF.ADD", arity("BF.ADD")),
            ("BF.MADD key", arity("BF.MADD")),
            ("BF.MEXISTS key", arity("BF.MEXISTS")),
            ("bf.exists key item hello", arity("BF.EXISTS")),
            ("BF.CARD key item", arity("BF.CARD")),
            ("BF.ADD s x", err(WRONGTYPE)),
            ("BF.EXISTS s x", err(WRONGTYPE)),
            ("BF.MADD s x", err(WRONGTYPE)),
            ("BF.MEXISTS s x", err(WRONGTYPE)),
            ("BF.CARD s", err(WRONGTYPE)),
            ("BF.INFO s", err(WRONGTYPE)),
            ("BF.INSERT s ITEMS x", err(WRONGTYPE)),
            ("BF.LOAD s x", err(WRONGTYPE)),
        ],
    );
    // Nothing a refused command named was made.
    assert_eq!(run(&s, "EXISTS k2 TT KEY nokey"), int(0));
}

#[test]
fn insert_takes_its_options_and_refuses_bad_ones() {
    let (_db, s) = fixed();
    let scale_mem = "ERR provided VALIDATESCALETO causes bloom object to exceed memory limit";
    let scale_fp = "ERR provided VALIDATESCALETO causes false positive to degrade to 0";
    check(
        &s,
        &[
            ("BF.INSERT k", Reply::Array(vec![])),
            ("BF.INFO k CAPACITY", int(100)),
            (
                "BF.INSERT k2 CAPACITY 1000 ERROR 0.001 ITEMS a b",
                ints(&[1, 1]),
            ),
            ("BF.INFO k2 CAPACITY", int(1000)),
            ("BF.INFO k2 ERROR", b("0.001")),
            (
                // The words after ITEMS are items: the second "2" is in.
                "BF.INSERT k3 ITEMS 1 2 3 EXPANSION 2",
                ints(&[1, 1, 1, 1, 0]),
            ),
            ("BF.INSERT k4 NOCREATE ITEMS a", err("ERR not found")),
            (
                "BF.INSERT k5 HELLO WORLD",
                err("ERR unknown argument received"),
            ),
            (
                "BF.INSERT k5 ERROR 2 ITEMS a",
                err("ERR (0 < error rate range < 1)"),
            ),
            ("BF.INSERT k5 ERROR err ITEMS a", err("ERR bad error rate")),
            (
                "BF.INSERT k5 TIGHTENING tr ITEMS a",
                err("ERR bad tightening ratio"),
            ),
            (
                "BF.INSERT k5 TIGHTENING 2 ITEMS a",
                err("ERR (0 < tightening ratio range < 1)"),
            ),
            (
                "BF.INSERT k5 CAPACITY 0 ITEMS a",
                err("ERR (capacity should be larger than 0)"),
            ),
            (
                "BF.INSERT k5 EXPANSION 4294967299 ITEMS a",
                err("ERR bad expansion"),
            ),
            ("BF.INSERT k5 EXPANSION 2 ITEMS", arity("BF.INSERT")),
            ("BF.INSERT k5 ERROR 0.5 CAPACITY", arity("BF.INSERT")),
            (
                "BF.INSERT k5 VALIDATESCALETO 1000000000000 NONSCALING",
                err("ERR cannot use NONSCALING and VALIDATESCALETO options together"),
            ),
            ("BF.INSERT k5 VALIDATESCALETO 1000000000000", err(scale_mem)),
            (
                "BF.INSERT k5 CAPACITY 1 ERROR 0.0000000001 VALIDATESCALETO 10000000 EXPANSION 1",
                err(scale_fp),
            ),
            (
                "BF.INSERT k6 EXPANSION 1 VALIDATESCALETO 101600",
                Reply::Array(vec![]),
            ),
            (
                "BF.INSERT k7 EXPANSION 1 VALIDATESCALETO 101601",
                err(scale_fp),
            ),
            ("BF.INSERT k5 SEED abc ITEMS a", err("ERR invalid seed")),
            // VALIDATESCALETO is checked before the key is looked at.
            ("SET s v", Reply::Ok),
            (
                "BF.INSERT s EXPANSION 1 VALIDATESCALETO 101601",
                err(scale_fp),
            ),
            // An existing filter keeps its own options.
            ("BF.INSERT k2 CAPACITY 5 ITEMS a c", ints(&[0, 1])),
            ("BF.INFO k2 CAPACITY", int(1000)),
        ],
    );
    assert_eq!(run(&s, "EXISTS k4 k5 k7"), int(0));
    // A seed given is the seed used: the same items set the same bits.
    let seed = [7u8; 32];
    let insert = |key: &str| {
        let args: [&[u8]; 6] = [b"BF.INSERT", key.as_bytes(), b"SEED", &seed, b"ITEMS", b"x"];
        s.execute(&args)
    };
    assert_eq!(insert("seeded1"), ints(&[1]));
    assert_eq!(insert("seeded2"), ints(&[1]));
    assert_eq!(
        bytes(run(&s, "DUMP seeded1")),
        bytes(run(&s, "DUMP seeded2"))
    );
}

#[test]
fn info_gives_each_field_and_the_whole_description() {
    let (_db, s) = fixed();
    let invalid = err("ERR invalid information value");
    check(
        &s,
        &[
            ("BF.INFO nokey", err("ERR not found")),
            ("BF.INFO nokey CAPACITY", err("ERR not found")),
            ("BF.RESERVE d 0.01 100", Reply::Ok),
            (
                "BF.INFO d",
                Reply::Array(vec![
                    simple("Capacity"),
                    int(100),
                    simple("Size"),
                    int(384),
                    simple("Number of filters"),
                    int(1),
                    simple("Number of items inserted"),
                    int(0),
                    simple("Error rate"),
                    b("0.01"),
                    simple("Expansion rate"),
                    int(2),
                    simple("Tightening ratio"),
                    b("0.5"),
                    simple("Max scaled capacity"),
                    int(26_214_300),
                ]),
            ),
            ("BF.RESERVE n 0.5 2000 NONSCALING", Reply::Ok),
            (
                "BF.INFO n",
                Reply::Array(vec![
                    simple("Capacity"),
                    int(2000),
                    simple("Size"),
                    int(625),
                    simple("Number of filters"),
                    int(1),
                    simple("Number of items inserted"),
                    int(0),
                    simple("Error rate"),
                    b("0.5"),
                    simple("Expansion rate"),
                    Reply::Nil,
                ]),
            ),
            ("BF.INFO d CAPACITY", int(100)),
            ("BF.INFO d FILTERS", int(1)),
            ("BF.INFO d ITEMS", int(0)),
            ("BF.INFO d SIZE", int(384)),
            ("BF.INFO d ERROR", b("0.01")),
            ("BF.INFO d TIGHTENING", b("0.5")),
            ("BF.INFO d EXPANSION", int(2)),
            ("BF.INFO d MAXSCALEDCAPACITY", int(26_214_300)),
            ("bf.info d maxscaledcapacity", int(26_214_300)),
            ("bf.info n expansion", Reply::Nil),
            ("BF.INFO d FOO", invalid.clone()),
            ("bf.info d item", invalid.clone()),
            ("BF.INFO n TIGHTENING", invalid.clone()),
            ("BF.INFO n MAXSCALEDCAPACITY", invalid),
            ("BF.INFO d CAPACITY SIZE", arity("BF.INFO")),
            ("BF.RESERVE big 0.001 10000 EXPANSION 2", Reply::Ok),
            ("BF.INFO big MAXSCALEDCAPACITY", int(20_470_000)),
            ("BF.RESERVE two 0.5 2000", Reply::Ok),
            ("BF.INFO two MAXSCALEDCAPACITY", int(32_766_000)),
            (
                "BF.INSERT fp EXPANSION 1 VALIDATESCALETO 101600",
                Reply::Array(vec![]),
            ),
            ("BF.INFO fp MAXSCALEDCAPACITY", int(101_600)),
            // Sizes of other shapes (bloom-spec.md 8.1).
            ("BF.RESERVE s1 0.01 1000", Reply::Ok),
            ("BF.INFO s1 SIZE", int(1463)),
            ("BF.RESERVE s2 0.001 1000", Reply::Ok),
            ("BF.INFO s2 SIZE", int(2062)),
            ("BF.RESERVE s3 0.001 10000", Reply::Ok),
            ("BF.INFO s3 SIZE", int(18236)),
        ],
    );
}

#[test]
fn the_type_scaling_and_settings_behave_as_upstream() {
    let (_db, s) = fixed();
    check(
        &s,
        &[
            ("BF.ADD t x", int(1)),
            ("TYPE t", simple("bloomfltr")),
            ("OBJECT ENCODING t", b("raw")),
            ("SET plain v", Reply::Ok),
        ],
    );
    let mut cursor = "0".to_string();
    let mut found = Vec::new();
    loop {
        let reply = items(s.execute(&["SCAN", &cursor, "TYPE", "bloomfltr", "COUNT", "100"]));
        cursor = String::from_utf8(bytes(reply[0].clone())).unwrap();
        found.extend(items(reply[1].clone()));
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(found, [b("t")]);
    let full = err("ERR non scaling filter is full");
    check(
        &s,
        &[
            ("BF.RESERVE ns 0.01 5 NONSCALING", Reply::Ok),
            (
                "BF.MADD ns x1 x2 x3 x4 x5 x6 x7",
                Reply::Array(vec![int(1), int(1), int(1), int(1), int(1), full.clone()]),
            ),
            ("BF.ADD ns x8", full),
            // An item already in a full filter is no error.
            ("BF.ADD ns x1", int(0)),
            ("BF.CARD ns", int(5)),
            ("BF.RESERVE sc 0.01 5 EXPANSION 2", Reply::Ok),
            (
                "BF.MADD sc x1 x2 x3 x4 x5 x6 x7 x8 x9 x10 x11 x12",
                ints(&[1; 12]),
            ),
            ("BF.INFO sc CAPACITY", int(15)),
            ("BF.INFO sc SIZE", int(516)),
            ("BF.INFO sc FILTERS", int(2)),
            ("BF.INFO sc ITEMS", int(12)),
            ("COPY t t2", int(1)),
            ("BF.EXISTS t2 x", int(1)),
            (
                "BF.LOAD t blob",
                err("BUSYKEY Target key name already exists."),
            ),
            ("BF.LOAD z", arity("BF.LOAD")),
            (
                "BF.LOAD z \u{2}abc",
                err("ERR bloom object decoding failed. Unsupported version"),
            ),
            ("EXISTS z", int(0)),
        ],
    );
    assert_eq!(
        s.execute(&["BF.LOAD", "z", ""]),
        err("ERR bloom object decoding failed")
    );
    // bloom-spec.md 15.5 row 69: the false positives of a default filter.
    let mut misses = Vec::new();
    for i in 1..=300 {
        if run(&s, &format!("BF.ADD fpr item{i}")) == int(0) {
            misses.push(i);
        }
    }
    assert_eq!(misses, [76, 173, 256, 292]);
    assert_eq!(run(&s, "BF.INFO fpr FILTERS"), int(2));
    assert_eq!(run(&s, "BF.INFO fpr SIZE"), int(892));
    assert_eq!(run(&s, "BF.INFO fpr ITEMS"), int(296));
    let Reply::Int(usage) = run(&s, "MEMORY USAGE fpr") else {
        panic!("MEMORY USAGE")
    };
    assert!(usage > 892, "{usage}");
}

#[test]
fn settings_are_shown_checked_and_used() {
    let (_db, s) = fresh();
    let all = run(&s, "CONFIG GET bf.*");
    let want: Vec<Reply> = [
        ("bf.bloom-capacity", "100"),
        ("bf.bloom-expansion", "2"),
        ("bf.bloom-fp-rate", "0.01"),
        ("bf.bloom-tightening-ratio", "0.5"),
        ("bf.bloom-memory-usage-limit", "134217728"),
        ("bf.bloom-use-random-seed", "yes"),
        ("bf.bloom-defrag-enabled", "yes"),
    ]
    .iter()
    .flat_map(|(k, v)| [b(k), b(v)])
    .collect();
    assert_eq!(all, Reply::Array(want));
    // With random seeds, two filters of the same items differ.
    check(&s, &[("BF.ADD r1 x", int(1)), ("BF.ADD r2 x", int(1))]);
    assert_ne!(bytes(run(&s, "DUMP r1")), bytes(run(&s, "DUMP r2")));
    let refused = |name: &str, why: &str| {
        err(&format!(
            "ERR CONFIG SET failed (possibly related to argument '{name}') - {why}"
        ))
    };
    check(
        &s,
        &[
            ("CONFIG SET bf.bloom-capacity 10000", Reply::Ok),
            ("BF.ADD c x", int(1)),
            ("BF.INFO c CAPACITY", int(10000)),
            (
                "CONFIG SET bf.bloom-fp-rate 1.1",
                refused("bf.bloom-fp-rate", "ERR (0 < error rate range < 1)"),
            ),
            (
                "CONFIG SET bf.bloom-tightening-ratio 0",
                refused(
                    "bf.bloom-tightening-ratio",
                    "ERR (0 < tightening ratio range < 1)",
                ),
            ),
            (
                "CONFIG SET bf.bloom-fp-rate abc",
                refused("bf.bloom-fp-rate", "Invalid floating-point value"),
            ),
            (
                "CONFIG SET bf.bloom-use-random-seed maybe",
                refused("bf.bloom-use-random-seed", "argument must be 'yes' or 'no'"),
            ),
            // One refused setting leaves the others as they were.
            (
                "CONFIG SET bf.bloom-capacity 7 bf.bloom-expansion x",
                refused(
                    "bf.bloom-expansion",
                    "argument couldn't be parsed into an integer",
                ),
            ),
            ("CONFIG SET bf.bloom-fp-rate 0.001", Reply::Ok),
            ("CONFIG SET bf.bloom-expansion 0", Reply::Ok),
            ("BF.ADD d x", int(1)),
            ("BF.INFO d CAPACITY", int(10000)),
            ("BF.INFO d ERROR", b("0.001")),
            ("BF.INFO d EXPANSION", Reply::Nil),
            ("CONFIG SET bf.bloom-memory-usage-limit 100", Reply::Ok),
            (
                "BF.RESERVE f 0.001 100000",
                err("ERR operation exceeds bloom object memory limit"),
            ),
            (
                "BF.ADD f x",
                err("ERR operation exceeds bloom object memory limit"),
            ),
            ("EXISTS f", int(0)),
            ("CONFIG SET BF.BLOOM-USE-RANDOM-SEED NO", Reply::Ok),
        ],
    );
    assert_eq!(
        items(run(&s, "CONFIG GET bf.bloom-fp-rate")),
        [b("bf.bloom-fp-rate"), b("0.001")]
    );
    // With the fixed seed they are the same.
    check(
        &s,
        &[
            (
                "CONFIG SET bf.bloom-memory-usage-limit 134217728",
                Reply::Ok,
            ),
            ("BF.ADD f1 x", int(1)),
            ("BF.ADD f2 x", int(1)),
        ],
    );
    assert_eq!(bytes(run(&s, "DUMP f1")), bytes(run(&s, "DUMP f2")));
    // ACL knows the category.
    assert!(items(run(&s, "ACL CAT")).contains(&b("bloom")));
    let bloom = items(run(&s, "ACL CAT bloom"));
    for name in ["bf.add", "bf.madd", "bf.reserve", "bf.load", "bf.info"] {
        assert!(bloom.contains(&b(name)), "{name} in {bloom:?}");
    }
}

fn items(reply: Reply) -> Vec<Reply> {
    match reply {
        Reply::Array(items) => items,
        other => panic!("not an array: {other:?}"),
    }
}

/// A BF.LOAD payload, as valkey-bloom's AOF rewrite writes it: version 1,
/// then the object in bincode. `filters` are (bits, hashes, items,
/// capacity).
fn blob(expansion: u32, fp: f64, tightening: f64, filters: &[(Vec<u8>, u32, i64, i64)]) -> Vec<u8> {
    let mut out = vec![1u8];
    out.extend_from_slice(&expansion.to_le_bytes());
    out.extend_from_slice(&fp.to_le_bytes());
    out.extend_from_slice(&tightening.to_le_bytes());
    out.push(0); // not a random seed
    out.extend_from_slice(&(filters.len() as u64).to_le_bytes());
    for (bits, hashes, items, capacity) in filters {
        let mut bitmap = vec![1u8];
        bitmap.extend_from_slice(&(bits.len() as u64).to_le_bytes());
        bitmap.extend_from_slice(&hashes.to_le_bytes());
        bitmap.extend_from_slice(&FIXED_SEED);
        bitmap.extend_from_slice(bits);
        out.extend_from_slice(&(bitmap.len() as u64).to_le_bytes());
        out.extend_from_slice(&bitmap);
        out.extend_from_slice(&items.to_le_bytes());
        out.extend_from_slice(&capacity.to_le_bytes());
    }
    out
}

/// The bits of a default filter (100 items at 0.01: 120 bytes, 7 hashes)
/// that holds "item": the offsets valkey-bloom's hash gives with the fixed
/// seed (bloom-spec.md 7.2).
fn holding_item() -> Vec<u8> {
    let mut bits = vec![0u8; 120];
    for at in [701, 8, 461, 213, 925, 677, 429] {
        bits[at / 8] |= 1 << (at % 8);
    }
    bits
}

#[test]
fn load_takes_the_filter_valkey_bloom_wrote() {
    let (_db, s) = fresh();
    let payload = blob(2, 0.01, 0.5, &[(holding_item(), 7, 1, 100)]);
    let load = |key: &str, payload: &[u8]| {
        let args: [&[u8]; 3] = [b"BF.LOAD", key.as_bytes(), payload];
        s.execute(&args)
    };
    assert_eq!(load("l", &payload), Reply::Ok);
    check(
        &s,
        &[
            ("BF.EXISTS l item", int(1)),
            ("BF.EXISTS l foo", int(0)),
            ("BF.CARD l", int(1)),
            ("BF.INFO l SIZE", int(384)),
            ("BF.INFO l MAXSCALEDCAPACITY", int(26_214_300)),
            // The filter goes on as one made here would.
            ("BF.ADD l item", int(0)),
            ("BF.ADD l foo", int(1)),
        ],
    );
    // What a key made here holds is the same, bit for bit.
    check(
        &s,
        &[
            ("CONFIG SET bf.bloom-use-random-seed no", Reply::Ok),
            ("BF.ADD made item", int(1)),
            ("BF.ADD made foo", int(1)),
        ],
    );
    assert_eq!(bytes(run(&s, "DUMP made")), bytes(run(&s, "DUMP l")));
    assert_eq!(
        load("l", &payload),
        err("BUSYKEY Target key name already exists.")
    );
    let decoding = err("ERR bloom object decoding failed");
    assert_eq!(load("bad", &payload[..payload.len() - 1]), decoding);
    assert_eq!(load("bad", &[payload.as_slice(), b"x"].concat()), decoding);
    // No hashes, or more than any filter has, cannot be looked up.
    assert_eq!(
        load("bad", &blob(2, 0.01, 0.5, &[(holding_item(), 0, 1, 100)])),
        decoding
    );
    assert_eq!(
        load(
            "bad",
            &blob(2, 0.01, 0.5, &[(holding_item(), u32::MAX, 1, 100)])
        ),
        decoding
    );
    assert_eq!(load("bad", &blob(2, 0.01, 0.5, &[])), decoding);
    assert_eq!(
        load("bad", &blob(2, 1.5, 0.5, &[(holding_item(), 7, 1, 100)])),
        err("ERR (0 < error rate range < 1)")
    );
    assert_eq!(
        load("bad", &blob(2, 0.01, 0.0, &[(holding_item(), 7, 1, 100)])),
        err("ERR (0 < error rate range < 1)")
    );
    check(
        &s,
        &[("CONFIG SET bf.bloom-memory-usage-limit 300", Reply::Ok)],
    );
    assert_eq!(
        load("bad", &payload),
        err("ERR operation exceeds bloom object memory limit")
    );
    assert_eq!(run(&s, "EXISTS bad"), int(0));
}

/// Filters of every shape, made with the fixed seed.
fn filled(s: &Session) {
    check(
        s,
        &[
            ("CONFIG SET bf.bloom-use-random-seed no", Reply::Ok),
            ("BF.ADD one item", int(1)),
            ("BF.RESERVE empty 0.001 1000", Reply::Ok),
            ("BF.RESERVE nonscaling 0.5 20 NONSCALING", Reply::Ok),
            ("BF.MADD nonscaling a b c", ints(&[1, 1, 1])),
            ("BF.RESERVE scaled 0.01 5 EXPANSION 3", Reply::Ok),
            ("CONFIG SET bf.bloom-use-random-seed yes", Reply::Ok),
            ("BF.INSERT random CAPACITY 50 ITEMS x y z", ints(&[1, 1, 1])),
            ("PEXPIREAT random 4102444800000", int(1)),
        ],
    );
    for i in 0..400 {
        run(s, &format!("BF.ADD scaled n{i}"));
    }
}

/// Every filter's DUMP payload and what it answers.
fn views(s: &Session) -> Vec<Reply> {
    let mut out = Vec::new();
    for key in ["one", "empty", "nonscaling", "scaled", "random"] {
        out.push(run(s, &format!("DUMP {key}")));
        out.push(run(s, &format!("BF.INFO {key}")));
        out.push(run(
            s,
            &format!("BF.MEXISTS {key} item a b x y n7 n399 nothing"),
        ));
    }
    out.push(run(s, "PEXPIRETIME random"));
    out.push(run(s, "BF.INFO scaled FILTERS"));
    out
}

#[test]
fn dump_restore_and_rdb_files_keep_every_filter() {
    let (source, s) = fresh();
    filled(&s);
    let before = views(&s);
    assert_eq!(run(&s, "BF.INFO scaled FILTERS"), int(5));
    // A DUMP payload: module type 7, then valkey-bloom's module ID.
    let payload = bytes(run(&s, "DUMP one"));
    assert_eq!(payload[0], 7);
    assert_eq!(payload[1], 0x81);
    assert_eq!(payload[2..10], 0x6e5a_2899_f96d_ac01u64.to_be_bytes());
    let args: [&[u8]; 4] = [b"RESTORE", b"copy", b"0", &payload];
    assert_eq!(s.execute(&args), Reply::Ok);
    assert_eq!(bytes(run(&s, "DUMP copy")), payload);
    check(&s, &[("BF.EXISTS copy item", int(1)), ("DEL copy", int(1))]);
    // A payload cut short is refused.
    let args: [&[u8]; 4] = [b"RESTORE", b"cut", b"0", &payload[..payload.len() - 12]];
    assert!(matches!(s.execute(&args), Reply::Error(_)));

    let file = source.save_rdb();
    let (copy, t) = fresh();
    copy.load_rdb(&file[..]).unwrap();
    assert_eq!(views(&t), before);
    assert_eq!(
        run(&t, "BF.ADD scaled fresh-item"),
        run(&s, "BF.ADD scaled fresh-item")
    );
}

#[test]
fn the_data_file_keeps_every_filter() {
    let dir = std::env::temp_dir().join(format!("hotstash-bloom-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bloom.hc");
    let _ = std::fs::remove_file(&path);
    let open = || {
        let db = Db::open_with(
            Some(&path),
            Config {
                shards: 4,
                ..Config::default()
            },
            PersistOptions {
                interval: Duration::from_millis(5),
                ..PersistOptions::default()
            },
        )
        .unwrap();
        let s = db.session().unwrap();
        (db, s)
    };
    let before = {
        let (_db, s) = open();
        filled(&s);
        views(&s)
    };
    let (_db, s) = open();
    assert_eq!(views(&s), before);
    drop((_db, s));
    let _ = std::fs::remove_dir_all(&dir);
}

/// For `valkey-check-rdb`, which walks module values without the module
/// (`HOTSTASH_RDB_DIR`): `filled` as an RDB file.
#[test]
#[ignore]
fn write_a_file_for_valkey_check_rdb() {
    let dir =
        std::path::PathBuf::from(std::env::var("HOTSTASH_RDB_DIR").expect("HOTSTASH_RDB_DIR"));
    let (db, s) = fresh();
    filled(&s);
    std::fs::write(dir.join("bloom.rdb"), db.save_rdb()).unwrap();
}

// ---- over the network ----

fn env() -> Env {
    Env::new(engine_cfg(4), server_cfg(2, 64))
}

fn field(c: &mut Client, section: &str, name: &str) -> Option<String> {
    let info = String::from_utf8(bytes(c.cmd(&["INFO", section]))).unwrap();
    let prefix = format!("{name}:");
    info.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()).map(str::to_string))
}

fn offset(c: &mut Client) -> u64 {
    field(c, "replication", "master_repl_offset")
        .and_then(|n| n.parse().ok())
        .expect("master_repl_offset")
}

#[test]
fn a_replica_builds_the_same_filters() {
    let primary = env();
    let replica = env();
    let mut p = primary.client();
    let mut r = replica.client();
    // Limits hold on the primary only: the replica takes what it is sent.
    assert_eq!(
        r.cmd(&["CONFIG", "SET", "bf.bloom-memory-usage-limit", "1000"]),
        Reply::Ok
    );
    let port = primary.addr.port().to_string();
    assert_eq!(r.cmd(&["REPLICAOF", "127.0.0.1", &port]), Reply::Ok);
    let up = || field(&mut replica.client(), "replication", "master_link_status");
    assert!(wait_until(15, || up().as_deref() == Some("up")), "no link");
    let lines = [
        // Random seeds: the replica must be told the seed.
        "BF.ADD a x",
        "BF.MADD a y z x",
        "BF.RESERVE big 0.001 10000",
        "BF.INSERT ins CAPACITY 3 ERROR 0.1 TIGHTENING 0.25 EXPANSION 4 ITEMS 1 2 3 4 5 6 7 8",
        "BF.INSERT ns NONSCALING CAPACITY 2 ITEMS p q",
        // Full: refused on the primary, never sent.
        "BF.MADD ns p r s",
        "BF.INSERT ns ITEMS q",
        "BF.INSERT e",
    ];
    for line in lines {
        let args: Vec<&str> = line.split(' ').collect();
        let reply = p.cmd(&args);
        assert!(
            !matches!(&reply, Reply::Error(e) if !e.contains("full")),
            "{line}: {reply:?}"
        );
    }
    let payload = blob(2, 0.01, 0.5, &[(holding_item(), 7, 1, 100)]);
    let load: [&[u8]; 3] = [b"BF.LOAD", b"loaded", &payload];
    assert_eq!(p.cmd(&load), Reply::Ok);
    // In a transaction and in a script too.
    assert_eq!(p.cmd(&["MULTI"]), Reply::Ok);
    p.cmd(&["BF.ADD", "tx", "x"]);
    p.cmd(&["BF.MADD", "tx", "y", "z"]);
    p.cmd(&["BF.ADD", "a", "in-tx"]);
    assert!(matches!(p.cmd(&["EXEC"]), Reply::Array(r) if r.len() == 3));
    let script = "redis.call('BF.ADD', KEYS[1], 'x'); return redis.call('BF.ADD', KEYS[1], 'y')";
    assert_eq!(p.cmd(&["EVAL", script, "1", "lua"]), int(1));
    let synced = wait_until(15, || offset(&mut p) == offset(&mut r));
    assert!(synced, "the replica did not catch up");
    for key in ["a", "big", "ins", "ns", "e", "loaded", "tx", "lua"] {
        let dump = |c: &mut Client| bytes(c.cmd(&["DUMP", key]));
        assert_eq!(dump(&mut p), dump(&mut r), "{key}");
    }
    assert_eq!(r.cmd(&["BF.EXISTS", "loaded", "item"]), int(1));
    assert_eq!(r.cmd(&["BF.CARD", "ns"]), int(2));
    // A replica refuses writes of its own.
    assert!(matches!(r.cmd(&["BF.ADD", "a", "w"]), Reply::Error(e) if e.starts_with("READONLY")));
}

#[test]
fn writes_raise_bloom_add_and_bloom_reserve() {
    let env = env();
    let mut c = env.client();
    assert_eq!(
        c.cmd(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        Reply::Ok
    );
    let mut sub = env.client();
    sub.cmd(&["PSUBSCRIBE", "__keyevent@*"]);
    for line in [
        "BF.ADD n1 x",
        "BF.ADD n1 x",
        "BF.ADD n1 y",
        "BF.RESERVE n2 0.01 10",
        "BF.RESERVE n2 0.01 10",
        "BF.MADD n2 a b",
        "BF.INSERT n3 ITEMS a",
        "BF.INSERT n4",
        "BF.EXISTS n1 x",
        "SET end 1",
    ] {
        let args: Vec<&str> = line.split(' ').collect();
        c.cmd(&args);
    }
    let mut seen = Vec::new();
    loop {
        let message = items(sub.read());
        let channel = String::from_utf8(bytes(message[2].clone())).unwrap();
        let key = String::from_utf8(bytes(message[3].clone())).unwrap();
        let event = channel.rsplit_once("__:").unwrap().1.to_string();
        if key == "end" {
            break;
        }
        seen.push(format!("{event}:{key}"));
    }
    assert_eq!(
        seen,
        [
            "bloom.add:n1",
            "bloom.reserve:n1",
            "bloom.add:n1",
            "bloom.reserve:n2",
            "bloom.add:n2",
            "bloom.add:n3",
            "bloom.reserve:n3",
            "bloom.reserve:n4",
        ]
    );
}

#[test]
fn rates_are_doubles_in_resp3() {
    use std::io::Read;
    let env = env();
    let mut c = env.client();
    assert_eq!(c.cmd(&["BF.RESERVE", "d", "0.01", "100"]), Reply::Ok);
    c.send(&["HELLO", "3"]);
    // HELLO answers with a map of no fixed length: read up to the reply
    // of the command behind it.
    c.send(&["PING"]);
    let mut seen = Vec::new();
    let mut byte = [0u8; 1];
    while !seen.ends_with(b"+PONG\r\n") {
        c.rd.read_exact(&mut byte).unwrap();
        seen.push(byte[0]);
    }
    c.send(&["BF.INFO", "d", "ERROR"]);
    let want = ",0.01\r\n";
    let mut got = vec![0u8; want.len()];
    c.rd.read_exact(&mut got).unwrap();
    assert_eq!(String::from_utf8_lossy(&got), want);
    c.send(&["BF.INFO", "d", "EXPANSION"]);
    let want = ":2\r\n";
    let mut got = vec![0u8; want.len()];
    c.rd.read_exact(&mut got).unwrap();
    assert_eq!(String::from_utf8_lossy(&got), want);
}
