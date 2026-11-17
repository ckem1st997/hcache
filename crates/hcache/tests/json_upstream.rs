//! JSON.* commands against the vectors ported from valkey-json's own
//! integration suite, `tst/integration/test_json_basic.py`. Each `#[test]`
//! carries the name of the upstream test it mirrors, and runs every vector of
//! that test before reporting, so one wrong reply does not hide the rest.
//!
//! Skipped on purpose: exact `JSON.DEBUG MEMORY` / `MEMORY USAGE` byte counts,
//! the KEYTABLE debug commands, `max-depth-key` / `max-size-key`, the
//! `json_*` INFO statistics, digest, RDB and module-load tests, and the tests
//! that need the large data files.

use hcache::{Config, Db, Reply, Session};

const SYNTAX: &[&str] = &["SYNTAXERR", "unknown subcommand"];
const NONEXISTENT: &[&str] = &["NONEXISTENT"];
const WRONGTYPE: &[&str] = &["WRONGTYPE"];
const OVERFLOW: &[&str] = &["OVERFLOW"];
const OUT_OF_BOUNDS: &[&str] = &["OUTOFBOUNDARIES"];
const LIMIT: &[&str] = &["LIMIT"];
const WRITE_ERR: &[&str] = &["ERROR", "OUTOFBOUNDARIES", "WRONGTYPE", "NONEXISTENT"];
const ARGS: &[&str] = &["wrong number of arguments", "invalid number of arguments"];

/// The upstream `wikipedia.json` (pretty-printed, two-space indent).
const WIKI_PRETTY: &str = r#"{
  "firstName": "John",
  "lastName": "Smith",
  "age": 27,
  "weight": 135.17,
  "isAlive": true,
  "address": {
    "street": "21 2nd Street",
    "city": "New York",
    "state": "NY",
    "zipcode": "10021-3100"
  },
  "phoneNumbers": [
    {
      "type": "home",
      "number": "212 555-1234"
    },
    {
      "type": "office",
      "number": "646 555-4567"
    }
  ],
  "children": [],
  "spouse": null,
  "groups": {}
}"#;

/// The upstream `wikipedia_compact.json`.
const WIKI_COMPACT: &str = r#"{"firstName":"John","lastName":"Smith","age":27,"weight":135.17,"isAlive":true,"address":{"street":"21 2nd Street","city":"New York","state":"NY","zipcode":"10021-3100"},"phoneNumbers":[{"type":"home","number":"212 555-1234"},{"type":"office","number":"646 555-4567"}],"children":[],"spouse":null,"groups":{}}"#;

/// The upstream `store.json`, with its whitespace folded.
const STORE: &str = r#"{
  "store": {
    "books": [
      {"category": "reference", "author": "Nigel Rees", "title": "Sayings of the Century", "price": 8.95, "in-stock": true},
      {"category": "fiction", "author": "Evelyn Waugh", "title": "Sword of Honour", "price": 12.99, "in-stock": true,
        "movies": [{"title": "Sword of Honour - movie", "realisator": {"first_name": "Bill", "last_name": "Anderson"}}]},
      {"category": "fiction", "author": "Herman Melville", "title": "Moby Dick", "isbn": "0-553-21311-3", "price": 9, "in-stock": false},
      {"category": "fiction", "author": "J. R. R. Tolkien", "title": "The Lord of the Rings", "isbn": "0-115-03266-2", "price": 22.99, "in-stock": true},
      {"category": "reference", "author": "William Jr. Strunk", "title": "The Elements of Style", "price": 6.99, "in-stock": false},
      {"category": "fiction", "author": "Leo Tolstoy", "title": "Anna Karenina", "price": 22.99, "in-stock": true},
      {"category": "reference", "author": "Sarah Janssen", "title": "The World Almanac and Book of Facts 2021", "isbn": "0-925-23305-2", "price": 10.69, "in-stock": false},
      {"category": "reference", "author": "Kate L. Turabian", "title": "Manual for Writers of Research Papers", "isbn": "0-675-16695-1", "price": 8.59, "in-stock": true}
    ],
    "bicycle": {"color": "red", "price": 19.64, "in-stock": true}
  }
}"#;

/// One upstream test: a fresh database plus the mismatches found so far.
struct V {
    _db: Db,
    s: Session,
    fails: Vec<String>,
    vectors: usize,
}

fn text(r: &Reply) -> String {
    match r {
        Reply::Bulk(b) => format!("Bulk({:?})", String::from_utf8_lossy(b)),
        other => format!("{other:?}"),
    }
}

fn bulks(items: &[&str]) -> Reply {
    Reply::Array(items.iter().map(|s| Reply::bulk(*s)).collect())
}

fn ints(items: &[i64]) -> Reply {
    Reply::Array(items.iter().map(|&n| Reply::Int(n)).collect())
}

impl V {
    /// An empty database with the limits upstream's `setup_data` configures.
    fn bare() -> V {
        let db = Db::open(
            None,
            Config {
                shards: 4,
                ..Config::default()
            },
        )
        .unwrap();
        let s = db.session().unwrap();
        let mut v = V {
            _db: db,
            s,
            fails: vec![],
            vectors: 0,
        };
        v.ok(&["CONFIG", "SET", "json.max-path-limit", "128"]);
        v.ok(&["CONFIG", "SET", "json.max-document-size", "67108864"]);
        v
    }

    /// `setup_data`: the `wikipedia` document and the plain string `str_key`.
    fn new() -> V {
        let mut v = V::bare();
        v.ok(&["JSON.SET", "wikipedia", ".", WIKI_PRETTY]);
        v.ok(&[
            "SET",
            "str_key",
            r#"{"firstName":"John","lastName":"Smith"}"#,
        ]);
        v
    }

    /// Run a command whose reply the test does not check (upstream does not
    /// assert on it either).
    fn run(&self, args: &[&str]) -> Reply {
        self.s.execute(args)
    }

    fn fail(&mut self, args: &[&str], got: &Reply, want: &str) {
        self.fails.push(format!(
            "  {args:?}\n    got : {}\n    want: {want}",
            text(got)
        ));
    }

    fn want(&mut self, args: &[&str], want: Reply) {
        self.vectors += 1;
        let got = self.run(args);
        if got != want {
            self.fail(args, &got, &text(&want));
        }
    }

    fn ok(&mut self, args: &[&str]) {
        self.want(args, Reply::Ok);
    }

    fn bulk(&mut self, args: &[&str], want: &str) {
        self.want(args, Reply::bulk(want));
    }

    fn int(&mut self, args: &[&str], want: i64) {
        self.want(args, Reply::Int(want));
    }

    fn nil(&mut self, args: &[&str]) {
        self.want(args, Reply::Nil);
    }

    fn arr(&mut self, args: &[&str], want: &[&str]) {
        self.want(args, bulks(want));
    }

    fn ints(&mut self, args: &[&str], want: &[i64]) {
        self.want(args, ints(want));
    }

    /// Expect an error whose message starts with one of `prefixes`.
    fn err(&mut self, args: &[&str], prefixes: &[&str]) {
        self.vectors += 1;
        let got = self.run(args);
        match &got {
            // redis-py drops the `ERR ` of an error before the suite sees it.
            Reply::Error(m)
                if prefixes
                    .iter()
                    .any(|p| m.strip_prefix("ERR ").unwrap_or(m).starts_with(p)) => {}
            _ => self.fail(args, &got, &format!("error starting with {prefixes:?}")),
        }
    }

    /// Expect an error whose message contains one of `needles` (any case).
    fn err_has(&mut self, args: &[&str], needles: &[&str]) {
        self.vectors += 1;
        let got = self.run(args);
        match &got {
            Reply::Error(m)
                if needles
                    .iter()
                    .any(|n| m.to_lowercase().contains(&n.to_lowercase())) => {}
            _ => self.fail(args, &got, &format!("error containing {needles:?}")),
        }
    }

    fn errfull(&mut self, args: &[&str], msg: &str) {
        self.want(args, Reply::Error(msg.into()));
    }

    fn get(&mut self, args: &[&str], want: &str) {
        self.bulk(args, want);
    }

    fn done(self, name: &str) {
        eprintln!("json_upstream {name}: {} vectors", self.vectors);
        assert!(
            self.fails.is_empty(),
            "{name}: {} of {} vectors differ from valkey-json:\n{}",
            self.fails.len(),
            self.vectors,
            self.fails.join("\n")
        );
    }
}

#[test]
fn test_sanity() {
    let mut v = V::new();
    let d = r#"{"a":"1", "b":"2", "c":"3"}"#;
    v.ok(&["JSON.SET", "k1", ".", d]);
    v.ok(&["JSON.SET", "k2", ".", "[1,2,3,4,5]"]);
    v.ok(&["JSON.MSET", "k3", ".", "[1,2,3,4,5]"]);
    v.ok(&["JSON.MSET", "k2", ".", "[5,4,3,2,1]", "k1", ".", d]);
    v.arr(
        &["JSON.MGET", "k1", "k2", "k3", "."],
        &[r#"{"a":"1","b":"2","c":"3"}"#, "[5,4,3,2,1]", "[1,2,3,4,5]"],
    );
    v.ok(&["JSON.SET", "k2", ".", "[1,2,3,4,5]"]);
    v.get(&["JSON.GET", "k1"], r#"{"a":"1","b":"2","c":"3"}"#);
    for (key, path, exp) in [
        ("k1", ".", r#"{"a":"1","b":"2","c":"3"}"#),
        ("k1", ".a", r#""1""#),
        ("k2", ".", "[1,2,3,4,5]"),
        ("k2", "[-1]", "5"),
    ] {
        v.get(&["JSON.GET", key, path], exp);
    }
    for (key, fmt, path, exp) in [
        ("k1", "SPACE", ".", r#"{"a": "1","b": "2","c": "3"}"#),
        ("k1", "INDENT", ".", r#"{ "a":"1", "b":"2", "c":"3"}"#),
        ("k2", "INDENT", ".", "[ 1, 2, 3, 4, 5]"),
        ("k2", "INDENT", "[-2]", "4"),
    ] {
        v.get(&["JSON.GET", key, fmt, " ", path], exp);
    }
    v.arr(
        &["JSON.MGET", "k1", "k2", "$"],
        &[r#"[{"a":"1","b":"2","c":"3"}]"#, "[[1,2,3,4,5]]"],
    );
    for (key, path, exp) in [
        ("k1", "$", r#"[{"a":"1","b":"2","c":"3"}]"#),
        ("k1", "$.*", r#"["1","2","3"]"#),
        ("k2", "$", "[[1,2,3,4,5]]"),
        ("k2", "$.[*]", "[1,2,3,4,5]"),
        ("k2", "$.[-1]", "[5]"),
        ("k2", "$.[0:3]", "[1,2,3]"),
    ] {
        v.get(&["JSON.GET", key, path], exp);
    }
    for (key, fmt, path, exp) in [
        ("k1", "SPACE", "$", r#"[{"a": "1","b": "2","c": "3"}]"#),
        (
            "k1",
            "INDENT",
            "$",
            r#"[ {  "a":"1",  "b":"2",  "c":"3" }]"#,
        ),
        ("k1", "INDENT", "$.*", r#"[ "1", "2", "3"]"#),
        ("k2", "INDENT", "$", "[ [  1,  2,  3,  4,  5 ]]"),
        ("k2", "INDENT", "$.[0:3]", "[ 1, 2, 3]"),
    ] {
        v.get(&["JSON.GET", key, fmt, " ", path], exp);
    }
    v.int(&["JSON.DEL", "k1"], 1);
    v.int(&["JSON.DEL", "k2"], 1);
    v.ok(&["JSON.SET", "k1", "$", d]);
    v.ok(&["JSON.SET", "k2", "$", "[1,2,3,4,5]"]);
    for (key, before, del_path, del_ret, after) in [
        ("k1", r#"[{"a":"1","b":"2","c":"3"}]"#, "$.*", 3, "[{}]"),
        ("k2", "[[1,2,3,4,5]]", "$.[*]", 5, "[[]]"),
    ] {
        v.get(&["JSON.GET", key, "$"], before);
        v.int(&["JSON.DEL", key, del_path], del_ret);
        v.get(&["JSON.GET", key, "$"], after);
    }
    v.done("test_sanity");
}

#[test]
fn test_parse_invalid_json_string() {
    let mut v = V::new();
    for input in ["foo", r#"{"a"}"#, "[a]"] {
        v.err(&["JSON.SET", "wikipedia", ".firstName", input], SYNTAX);
    }
    v.done("test_parse_invalid_json_string");
}

#[test]
fn test_json_set_command_supports_all_datatypes() {
    let mut v = V::new();
    for (path, value) in [
        (".address.city", r#""Boston""#),
        (".age", "30"),
        (".foo", "[1,2,3]"),
        (".phoneNumbers[0].number", r#""1234567""#),
        (".foo", r#"{"a":"b"}"#),
        (".lastName", "null"),
        (".isAlive", "false"),
    ] {
        v.ok(&["JSON.SET", "wikipedia", path, value]);
        v.get(&["JSON.GET", "wikipedia", path], value);
    }
    v.done("test_json_set_command_supports_all_datatypes");
}

#[test]
fn test_json_set_command_root_document() {
    let mut v = V::new();
    for (key, value) in [
        ("k1", r#""Boston""#),
        ("k2", "123"),
        ("k3", r#"["Seattle","Boston"]"#),
        ("k3", "[1,2,3]"),
        ("k4", r#"{"a":"b"}"#),
        ("k4", "{}"),
        ("k5", "null"),
        ("k6", "false"),
    ] {
        v.ok(&["JSON.SET", key, ".", value]);
        v.get(&["JSON.GET", key], value);
    }
    v.done("test_json_set_command_root_document");
}

#[test]
fn test_json_set_command_nx_xx_options() {
    let mut v = V::new();
    type Case = (
        &'static str,
        &'static str,
        &'static str,
        Reply,
        &'static str,
    );
    let cases: Vec<Case> = vec![
        (
            ".address.city",
            r#""Boston""#,
            "NX",
            Reply::Nil,
            r#""New York""#,
        ),
        (
            ".address.city",
            r#""Boston""#,
            "XX",
            Reply::Ok,
            r#""Boston""#,
        ),
        (".foo", r#""bar""#, "NX", Reply::Ok, r#""bar""#),
        (".firstName", r#""bar""#, "NX", Reply::Nil, r#""John""#),
    ];
    for (path, value, cond, set_ret, get_ret) in cases {
        v.want(&["JSON.SET", "wikipedia", path, value, cond], set_ret);
        v.get(&["JSON.GET", "wikipedia", path], get_ret);
        // upstream's `'.' '"value"'` is one argument, `."value"`
        v.err(&["JSON.SET", "k1", r#"."value""#, "badword"], SYNTAX);
    }
    v.done("test_json_set_command_nx_xx_options");
}

#[test]
fn test_json_set_command_with_error_conditions() {
    let mut v = V::new();
    v.err(&["JSON.SET", "foo", ".bar", r#""bar""#], SYNTAX);
    v.nil(&["JSON.SET", "k1", ".", r#""some value""#, "XX"]);
    v.nil(&["JSON.SET", "wikipedia", ".foo", r#""bar""#, "XX"]);
    v.err(&["JSON.GET", "wikipedia", ".foo"], NONEXISTENT);
    v.nil(&["JSON.SET", "wikipedia", ".", r#""some new value""#, "NX"]);
    v.nil(&["JSON.SET", "wikipedia", ".firstName", r#""Tom""#, "NX"]);
    v.get(&["JSON.GET", "wikipedia", ".firstName"], r#""John""#);
    v.err(
        &["JSON.SET", "wikipedia", ".", r#""some new value""#, "NXXX"],
        SYNTAX,
    );
    v.err(
        &["JSON.SET", "wikipedia", ".", r#""some new value""#, "XXXX"],
        SYNTAX,
    );
    v.err_has(
        &["JSON.SET", "wikipedia", ".", r#""bar""#, "a", "b"],
        &["wrong number of arguments"],
    );
    v.done("test_json_set_command_with_error_conditions");
}

#[test]
fn test_json_set_ancestor_keys_should_not_be_overridden() {
    let mut v = V::new();
    for (path, val) in [
        (".firstName.a", r#""some new value""#),
        (".age.a", "1"),
        (".address.a.b", r#""some new value""#),
        (".address[0]", r#""some new value""#),
    ] {
        v.err(&["JSON.SET", "wikipedia", path, val], WRITE_ERR);
    }
    v.done("test_json_set_ancestor_keys_should_not_be_overridden");
}

#[test]
fn test_json_set_reject_out_of_boundary_array_index() {
    let mut v = V::new();
    v.err(
        &["JSON.SET", "wikipedia", "phoneNumbers[9]", r#""123""#],
        OUT_OF_BOUNDS,
    );
    v.done("test_json_set_reject_out_of_boundary_array_index");
}

#[test]
fn test_json_set_insert_value() {
    let mut v = V::new();
    for (path, new_val) in [
        (r#"["address"]["z"]"#, r#""z""#),
        (".address.z2", r#""z2""#),
    ] {
        v.run(&["JSON.SET", "wikipedia", path, new_val]);
        v.get(&["JSON.GET", "wikipedia", path], new_val);
    }
    for (path, new_val) in [
        (r#"["address"]["foo"]["z"]"#, r#""z""#),
        (".address.foo.z", r#""z""#),
    ] {
        v.err(&["JSON.SET", "wikipedia", path, new_val], NONEXISTENT);
    }
    v.done("test_json_set_insert_value");
}

#[test]
fn test_json_set_negative_array_index() {
    let mut v = V::new();
    v.run(&[
        "JSON.SET",
        "wikipedia",
        ".phoneNumbers[-1].number",
        r#""1-2-3""#,
    ]);
    v.get(
        &["JSON.GET", "wikipedia", ".phoneNumbers[-1].number"],
        r#""1-2-3""#,
    );
    v.get(
        &["JSON.GET", "wikipedia", "$.phoneNumbers[*].number"],
        r#"["212 555-1234","1-2-3"]"#,
    );
    v.done("test_json_set_negative_array_index");
}

#[test]
fn test_json_set_legacy_and_v2path_wildcard() {
    let mut v = V::new();
    let data = r#"{"firstName":"John","lastName":"Smith","age":27,"weight":135.17,"isAlive":true,"address":
        {"street":"21 2nd Street","city":"New York","state":"NY","zipcode":"10021-3100"},
        "phoneNumbers":[{"type":"home","number":"212 555-1234"},{"type":"office","number":"646 555-4567"}],
        "children":[],"spouse":null,"groups":{}}"#;
    for k in ["wikipedia2", "wikipedia3", "wikipedia4"] {
        v.run(&["JSON.SET", k, ".", data]);
    }
    let four = r#"["1","1","1","1"]"#;
    let two = r#"["1","1"]"#;
    for (key, path, exp, path2, exp2) in [
        ("wikipedia", "$.address.*", four, "$.address.*", four),
        ("wikipedia2", ".address.*", r#""1""#, "$.address.*", four),
        (
            "wikipedia3",
            "$.phoneNumbers[*].number",
            two,
            "$.phoneNumbers[*].number",
            two,
        ),
        (
            "wikipedia4",
            ".phoneNumbers[*].number",
            r#""1""#,
            "$.phoneNumbers[*].number",
            two,
        ),
    ] {
        v.run(&["JSON.SET", key, path, r#""1""#]);
        v.get(&["JSON.GET", key, path], exp);
        v.get(&["JSON.GET", key, path2], exp2);
    }
    v.run(&["JSON.SET", "k1", ".", r#"{"a":[], "b":[1], "c":[1,2,3]}"#]);
    v.run(&[
        "JSON.SET",
        "k2",
        ".",
        r#"{"a":{}, "b":{"a":1}, "c":{"a":1, "b":2, "c":3}}"#,
    ]);
    type Case = (
        &'static str,
        &'static str,
        &'static str,
        Option<(&'static str, &'static str)>,
    );
    let cases: Vec<Case> = vec![
        (
            "k1",
            "$.a[*]",
            "1",
            Some(("[]", r#"{"a":[],"b":[1],"c":[1,2,3]}"#)),
        ),
        (
            "k1",
            "$.b[*]",
            "2",
            Some(("[2]", r#"{"a":[],"b":[2],"c":[1,2,3]}"#)),
        ),
        (
            "k1",
            "$.c[*]",
            "4",
            Some(("[4,4,4]", r#"{"a":[],"b":[2],"c":[4,4,4]}"#)),
        ),
        ("k1", ".a[*]", "1", None),
        (
            "k1",
            ".b[*]",
            "3",
            Some(("3", r#"{"a":[],"b":[3],"c":[4,4,4]}"#)),
        ),
        (
            "k1",
            ".c[*]",
            "5",
            Some(("5", r#"{"a":[],"b":[3],"c":[5,5,5]}"#)),
        ),
        (
            "k2",
            "$.a.*",
            "1",
            Some(("[]", r#"{"a":{},"b":{"a":1},"c":{"a":1,"b":2,"c":3}}"#)),
        ),
        (
            "k2",
            "$.b.*",
            "2",
            Some(("[2]", r#"{"a":{},"b":{"a":2},"c":{"a":1,"b":2,"c":3}}"#)),
        ),
        (
            "k2",
            "$.c.*",
            "4",
            Some(("[4,4,4]", r#"{"a":{},"b":{"a":2},"c":{"a":4,"b":4,"c":4}}"#)),
        ),
        ("k2", ".a.*", "1", None),
        (
            "k2",
            ".b.*",
            "3",
            Some(("3", r#"{"a":{},"b":{"a":3},"c":{"a":4,"b":4,"c":4}}"#)),
        ),
        (
            "k2",
            ".c.*",
            "5",
            Some(("5", r#"{"a":{},"b":{"a":3},"c":{"a":5,"b":5,"c":5}}"#)),
        ),
    ];
    for (key, path, new_val, exp) in cases {
        match exp {
            // Upstream wraps SET and GET in one `pytest.raises`, so the
            // error may come from either of them.
            None => {
                v.vectors += 1;
                let set = v.run(&["JSON.SET", key, path, new_val]);
                let hit = |r: &Reply| matches!(r, Reply::Error(m) if m.starts_with(NONEXISTENT[0]));
                if !hit(&set) {
                    let got = v.run(&["JSON.GET", key, path]);
                    if !hit(&got) {
                        v.fail(
                            &["JSON.GET", key, path],
                            &got,
                            "NONEXISTENT from SET or GET",
                        );
                    }
                }
            }
            Some((exp, whole)) => {
                v.run(&["JSON.SET", key, path, new_val]);
                v.get(&["JSON.GET", key, path], exp);
                v.get(&["JSON.GET", key, "."], whole);
            }
        }
    }
    v.done("test_json_set_legacy_and_v2path_wildcard");
}

#[test]
fn test_json_get_command_supports_all_datatypes() {
    let mut v = V::new();
    for (path, value) in [
        (".firstName", r#""John""#),
        (".address.city", r#""New York""#),
        (".spouse", "null"),
        (".children", "[]"),
        (".groups", "{}"),
        (".isAlive", "true"),
        (".age", "27"),
        (r#"["weight"]"#, "135.17"),
    ] {
        v.get(&["JSON.GET", "wikipedia", path], value);
    }
    v.done("test_json_get_command_supports_all_datatypes");
}

#[test]
fn test_json_path_syntax_object_keys() {
    let mut v = V::new();
    for (path, value) in [
        (r#"["firstName"]"#, r#""John""#),
        ("address['city']", r#""New York""#),
        ("['address']['city']", r#""New York""#),
        (r#"["address"]["city"]"#, r#""New York""#),
        (r#"["address"]['city']"#, r#""New York""#),
        (r#"["isAlive"]"#, "true"),
        ("['age']", "27"),
        (r#"["weight"]"#, "135.17"),
    ] {
        v.get(&["JSON.GET", "wikipedia", path], value);
    }
    for path in [
        r#"[firstName"]"#,
        r#"address["city'"#,
        r#"["address'][[[["city"]"#,
        r#"[[["address"]]]["city"]"#,
        r#""["address"]['city']"#,
        r#""['address"]["city"]"#,
        r#"[""""address]["city"]"#,
        r#"[address""""]"#,
        "['address']]]['city']",
        r#"["address"]'['"city"]"#,
    ] {
        v.err(&["JSON.GET", "wikipedia", path], SYNTAX);
    }
    v.done("test_json_path_syntax_object_keys");
}

#[test]
fn test_json_get_command_floating_point() {
    let mut v = V::new();
    for value in [
        "0",
        "0.1",
        "0.3",
        "1.23456789",
        "-0.1",
        "-0.3",
        "-1.23456789",
    ] {
        v.ok(&["JSON.SET", "wikipedia", ".foo", value]);
        v.get(&["JSON.GET", "wikipedia", ".foo"], value);
    }
    v.ok(&["JSON.SET", "wikipedia", ".foo", "1.7976e+308"]);
    v.ok(&["JSON.SET", "wikipedia", ".bar", "-1.7976e+308"]);
    v.get(&["JSON.GET", "wikipedia", ".foo"], "1.7976e+308");
    v.get(&["JSON.GET", "wikipedia", ".bar"], "-1.7976e+308");
    v.ok(&["JSON.SET", "wikipedia", ".foo", "1.234567890123456789"]);
    v.get(&["JSON.GET", "wikipedia", ".foo"], "1.234567890123456789");
    v.ok(&["JSON.SET", "wikipedia", ".foo", "0.3000000"]);
    v.get(&["JSON.GET", "wikipedia", ".foo"], "0.3000000");
    v.done("test_json_get_command_floating_point");
}

#[test]
fn test_json_get_command_with_multiple_paths() {
    let mut v = V::new();
    v.get(
        &["JSON.GET", "wikipedia", ".firstName", ".lastName"],
        r#"{".firstName":"John",".lastName":"Smith"}"#,
    );
    v.err(
        &[
            "JSON.GET",
            "wikipedia",
            ".firstName",
            ".lastName",
            ".foo",
            ".bar",
        ],
        NONEXISTENT,
    );
    v.done("test_json_get_command_with_multiple_paths");
}

#[test]
fn test_json_get_command_returns_json_with_default_format() {
    let mut v = V::new();
    v.get(&["JSON.GET", "wikipedia"], WIKI_COMPACT);
    v.get(&["JSON.GET", "wikipedia", "NOESCAPE"], WIKI_COMPACT);
    v.done("test_json_get_command_returns_json_with_default_format");
}

#[test]
fn test_json_get_command_returns_json_with_custom_format() {
    let mut v = V::new();
    let exp = WIKI_PRETTY;
    let fmt = ["INDENT", "  ", "SPACE", " ", "NEWLINE", "\n"];
    let with = |pre: &[&'static str], post: &[&'static str]| -> Vec<&'static str> {
        let mut a = vec!["JSON.GET", "wikipedia"];
        a.extend_from_slice(pre);
        a.extend_from_slice(post);
        a
    };
    v.get(&with(&fmt, &[]), exp);
    v.get(&with(&fmt, &["NOESCAPE"]), exp);
    v.get(&with(&["NOESCAPE"], &fmt), exp);
    v.get(
        &with(
            &["INDENT", "  ", "NOESCAPE", "SPACE", " ", "NEWLINE", "\n"],
            &[],
        ),
        exp,
    );
    let addr = "{\n\t\"street\": \"21 2nd Street\",\n\t\"city\": \"New York\",\n\t\"state\": \"NY\",\n\t\"zipcode\": \"10021-3100\"\n}";
    v.get(
        &with(
            &["INDENT", "\t", "SPACE", " ", "NEWLINE", "\n", ".address"],
            &[],
        ),
        addr,
    );
    v.get(
        &with(
            &["INDENT", "=*=*", "SPACE", "--", "NEWLINE", "\r\n", ".address"],
            &[],
        ),
        "{\r\n=*=*\"street\":--\"21 2nd Street\",\r\n=*=*\"city\":--\"New York\",\r\n=*=*\"state\":--\"NY\",\r\n=*=*\"zipcode\":--\"10021-3100\"\r\n}",
    );
    v.get(&with(&["."], &fmt), exp);
    v.get(
        &with(
            &["INDENT", "  ", ".firstName", "SPACE", " ", "NEWLINE", "\n"],
            &[],
        ),
        "\"John\"",
    );
    let two = "{\n  \".firstName\": \"John\",\n  \".lastName\": \"Smith\"\n}";
    v.get(
        &with(
            &[
                "INDENT",
                "  ",
                "SPACE",
                " ",
                ".firstName",
                ".lastName",
                "NEWLINE",
                "\n",
            ],
            &[],
        ),
        two,
    );
    v.get(
        &with(
            &["INDENT", "\t", ".address", "SPACE", " ", "NEWLINE", "\n"],
            &[],
        ),
        addr,
    );
    v.get(
        &with(&["INDENT", "\t", "$.address", "SPACE", " ", "NEWLINE", "\n"], &[]),
        "[\n\t{\n\t\t\"street\": \"21 2nd Street\",\n\t\t\"city\": \"New York\",\n\t\t\"state\": \"NY\",\n\t\t\"zipcode\": \"10021-3100\"\n\t}\n]",
    );
    v.get(
        &with(
            &[
                "INDENT",
                "  ",
                ".firstName",
                "SPACE",
                " ",
                ".lastName",
                "NEWLINE",
                "\n",
            ],
            &[],
        ),
        two,
    );
    v.get(
        &with(
            &[
                ".firstName",
                "INDENT",
                "  ",
                "SPACE",
                " ",
                "NEWLINE",
                "\n",
                ".lastName",
            ],
            &[],
        ),
        two,
    );
    v.done("test_json_get_command_returns_json_with_custom_format");
}

#[test]
fn test_json_get_command_returns_json_with_custom_format_error_conditions() {
    let mut v = V::new();
    v.err(
        &[
            "JSON.GET",
            "wikipedia",
            "INDENT",
            "  ",
            "SPACE",
            " ",
            "NEWLINE",
        ],
        SYNTAX,
    );
    v.err(&["JSON.GET", "wikipedia", "INDENT", "  ", "SPACE"], SYNTAX);
    v.err(
        &[
            "JSON.GET",
            "wikipedia",
            "NEWLINE",
            "\n",
            "SPACE",
            " ",
            "INDENT",
        ],
        SYNTAX,
    );
    v.done("test_json_get_command_returns_json_with_custom_format_error_conditions");
}

#[test]
fn test_json_get_command_with_error_conditions() {
    let mut v = V::new();
    v.nil(&["JSON.GET", "foo", ".firstName"]);
    v.err(&["JSON.GET", "str_key"], WRONGTYPE);
    v.err(&["JSON.GET", "wikipedia", ".foo"], NONEXISTENT);
    v.err_has(&["JSON.GET"], &["wrong number of arguments"]);
    v.done("test_json_get_command_with_error_conditions");
}

#[test]
fn test_json_number_as_member_name() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", r#"{"1":1, "2":{"3":4}}"#]);
    for k in ["k2", "k3", "k4", "k5"] {
        v.run(&["COPY", "k1", k]);
    }
    for (path, exp) in [
        (r#"["2"]["3"]"#, "4"),
        ("['2']['3']", "4"),
        (".1", "1"),
        (".2.3", "4"),
        ("$.2.3", "[4]"),
    ] {
        v.get(&["JSON.GET", "k1", path], exp);
    }
    for (key, path, val, exp) in [
        ("k1", r#"["2"]["3"]"#, "5", r#"{"1":1,"2":{"3":5}}"#),
        ("k2", "['2']['3']", "5", r#"{"1":1,"2":{"3":5}}"#),
        ("k3", ".2.3", "5", r#"{"1":1,"2":{"3":5}}"#),
        ("k4", ".1", "2", r#"{"1":2,"2":{"3":4}}"#),
        ("k5", "$.2.*", "5", r#"{"1":1,"2":{"3":5}}"#),
    ] {
        v.ok(&["JSON.SET", key, path, val]);
        v.get(&["JSON.GET", key, "."], exp);
    }
    v.done("test_json_number_as_member_name");
}

#[test]
fn test_json_get_legacy_and_v2path_wildcard() {
    let mut v = V::new();
    let addr4 = r#"["21 2nd Street","New York","NY","10021-3100"]"#;
    for (path, exp) in [
        ("$.address.*", addr4),
        ("$.['address'].*", addr4),
        (".address.*", r#""21 2nd Street""#),
        (r#".["address"].*"#, r#""21 2nd Street""#),
        ("$.phoneNumbers.*.type", r#"["home","office"]"#),
        ("$.phoneNumbers[*].type", r#"["home","office"]"#),
        (r#"$.["phoneNumbers"][*].["type"]"#, r#"["home","office"]"#),
        (".phoneNumbers.*.type", r#""home""#),
        (".phoneNumbers[*].type", r#""home""#),
        (r#".["phoneNumbers"][*].["type"]"#, r#""home""#),
        ("$.[ 'address' ].*", addr4),
        (r#".[ "address" ].*"#, r#""21 2nd Street""#),
        (
            r#"$.[ "phoneNumbers" ][ * ].[ "type" ]"#,
            r#"["home","office"]"#,
        ),
    ] {
        v.get(&["JSON.GET", "wikipedia", path], exp);
    }
    v.run(&["JSON.SET", "k1", ".", r#"{"a":[], "b":[1], "c":[1,2]}"#]);
    v.run(&[
        "JSON.SET",
        "k2",
        ".",
        r#"{"a":{}, "b": {"a": 1}, "c": {"a": 1, "b": 2}}"#,
    ]);
    v.run(&[
        "JSON.SET",
        "k3",
        ".",
        r#"{"a":[[[1,2],[3,4],[5,6]],[[7,8],[9,10],[11,12]]]}"#,
    ]);
    v.run(&[
        "JSON.SET",
        "k4",
        ".",
        r#"{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h:":1}}}}}}}}"#,
    ]);
    for (key, path, exp) in [
        ("k1", "$.a[*]", "[]"),
        ("k1", "$.b[*]", "[1]"),
        ("k1", "$.c[*]", "[1,2]"),
        ("k2", "$.a.*", "[]"),
        ("k2", "$.b.*", "[1]"),
        ("k2", "$.c.*", "[1,2]"),
        (
            "k3",
            "$.a[*]",
            "[[[1,2],[3,4],[5,6]],[[7,8],[9,10],[11,12]]]",
        ),
        (
            "k3",
            "$.a[*][*]",
            "[[1,2],[3,4],[5,6],[7,8],[9,10],[11,12]]",
        ),
        ("k3", "$.a[*][*][1]", "[2,4,6,8,10,12]"),
        ("k4", "$.a.*.*", r#"[{"d":{"e":{"f":{"g":{"h:":1}}}}}]"#),
        ("k4", "$.a.*.*.*", r#"[{"e":{"f":{"g":{"h:":1}}}}]"#),
        ("k4", "$.a.*.c.*", r#"[{"e":{"f":{"g":{"h:":1}}}}]"#),
        ("k4", "$.a.*.c.*.e.*", r#"[{"g":{"h:":1}}]"#),
        ("k4", "$.a.*.c.*.e.*.g.*", "[1]"),
        ("k1", ".b[*]", "1"),
        ("k1", ".c[*]", "1"),
        ("k2", ".b.*", "1"),
        ("k2", ".c.*", "1"),
        ("k3", ".a[*]", "[[1,2],[3,4],[5,6]]"),
        ("k3", ".a[*][*]", "[1,2]"),
        ("k3", ".a[*][*][1]", "2"),
        ("k4", ".a.*.*", r#"{"d":{"e":{"f":{"g":{"h:":1}}}}}"#),
        ("k4", ".a.*.*.*", r#"{"e":{"f":{"g":{"h:":1}}}}"#),
        ("k4", ".a.*.c.*", r#"{"e":{"f":{"g":{"h:":1}}}}"#),
        ("k4", ".a.*.c.*.e.*", r#"{"g":{"h:":1}}"#),
        ("k4", ".a.*.c.*.e.*.g.*", "1"),
    ] {
        v.get(&["JSON.GET", key, path], exp);
    }
    v.err(&["JSON.GET", "k1", ".a[*]"], NONEXISTENT);
    v.err(&["JSON.GET", "k2", ".a.*"], NONEXISTENT);
    v.done("test_json_get_legacy_and_v2path_wildcard");
}

#[test]
fn test_json_get_negative_array_index_legacypath() {
    let mut v = V::new();
    for (path, exp) in [
        (".phoneNumbers[-2].type", r#""home""#),
        (".phoneNumbers[-1].type", r#""office""#),
        (".phoneNumbers[ -1 ].type", r#""office""#),
        (r#".["phoneNumbers"][-2]["type"]"#, r#""home""#),
        (r#".["phoneNumbers"][-1]["type"]"#, r#""office""#),
        (r#".["phoneNumbers"][ -1]["type" ]"#, r#""office""#),
        (r#".["phoneNumbers"][-1 ][ "type"]"#, r#""office""#),
        (r#".["phoneNumbers"][ -1 ][ "type" ]"#, r#""office""#),
    ] {
        v.get(&["JSON.GET", "wikipedia", path], exp);
    }
    for path in [
        ".phoneNumbers[2].type",
        ".phoneNumbers[10].type",
        ".phoneNumbers[-3].type",
    ] {
        v.err(&["JSON.GET", "wikipedia", path], OUT_OF_BOUNDS);
    }
    for path in [
        ".firstName[-1]",
        ".age[-1]",
        ".weight[-1]",
        ".address[-1]",
        ".phoneNumbers[0][-1]",
    ] {
        v.err(&["JSON.GET", "wikipedia", path], WRONGTYPE);
    }
    v.done("test_json_get_negative_array_index_legacypath");
}

#[test]
fn test_json_get_negative_array_index_v2path() {
    let mut v = V::new();
    for (path, exp) in [
        ("$.phoneNumbers[-2].type", r#"["home"]"#),
        ("$.phoneNumbers[ -2 ].type", r#"["home"]"#),
        ("$.phoneNumbers[-1].type", r#"["office"]"#),
        ("$.phoneNumbers[ -1].type", r#"["office"]"#),
        ("$.phoneNumbers[-1 ].type", r#"["office"]"#),
        (r#"$.["phoneNumbers"][-2]["type"]"#, r#"["home"]"#),
        (r#"$.["phoneNumbers"][-1]["type"]"#, r#"["office"]"#),
        ("$.phoneNumbers[2].type", "[]"),
        ("$.phoneNumbers[10].type", "[]"),
        ("$.firstName[-1]", "[]"),
        ("$.age[-1]", "[]"),
        ("$.weight[-1]", "[]"),
        ("$.weight[    -1 ]", "[]"),
        ("$.phoneNumbers[0][-1]", "[]"),
        ("$.phoneNumbers[ 0][ -1 ]", "[]"),
        ("$.phoneNumbers[ 0 ][ -1 ]", "[]"),
        (r#"$.[ "phoneNumbers" ][ -1 ][ "type" ]"#, r#"["office"]"#),
        ("$.phoneNumbers[-3].type", "[]"),
        ("$.phoneNumbers[ -3 ].type", "[]"),
    ] {
        v.get(&["JSON.GET", "wikipedia", path], exp);
    }
    v.done("test_json_get_negative_array_index_v2path");
}

#[test]
fn test_json_get_v2path_array_slice() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", "[0,1,2,3,4,5,6,7,8,9]"]);
    for (path, exp) in [
        ("$[0:3]", "[0,1,2]"),
        ("$[ 0 : 3 ]", "[0,1,2]"),
        ("$[0:+3]", "[0,1,2]"),
        ("$[ 0 : +3 ]", "[0,1,2]"),
        ("$[0:-1]", "[0,1,2,3,4,5,6,7,8]"),
        ("$[ 0 : -1 ]", "[0,1,2,3,4,5,6,7,8]"),
        ("$[2:-2]", "[2,3,4,5,6,7]"),
        ("$[ 2 : -2 ]", "[2,3,4,5,6,7]"),
        ("$[+2:-2]", "[2,3,4,5,6,7]"),
        ("$[1:1]", "[]"),
        ("$[1:2]", "[1]"),
        ("$[+1:+2]", "[1]"),
        ("$[1:3]", "[1,2]"),
        ("$[1:0]", "[]"),
        ("$[5:]", "[5,6,7,8,9]"),
        ("$[ 5 :  ]", "[5,6,7,8,9]"),
        ("$[:3]", "[0,1,2]"),
        ("$[  :  3]", "[0,1,2]"),
        ("$[:+3]", "[0,1,2]"),
        ("$[: +3]", "[0,1,2]"),
        ("$[:6:2]", "[0,2,4]"),
        ("$[ : 6 : 2]", "[0,2,4]"),
        ("$[:]", "[0,1,2,3,4,5,6,7,8,9]"),
        ("$[ : ]", "[0,1,2,3,4,5,6,7,8,9]"),
        ("$[::]", "[0,1,2,3,4,5,6,7,8,9]"),
        ("$[ : : ]", "[0,1,2,3,4,5,6,7,8,9]"),
        ("$[::2]", "[0,2,4,6,8]"),
        ("$[ : : 2 ]", "[0,2,4,6,8]"),
        ("$[3::2]", "[3,5,7,9]"),
        ("$[3 :: 2]", "[3,5,7,9]"),
        ("$[0::1]", "[0,1,2,3,4,5,6,7,8,9]"),
        ("$[0:8:2]", "[0,2,4,6]"),
        ("$[ 0 : 8 : 2 ]", "[0,2,4,6]"),
        ("$[0:+8:+2]", "[0,2,4,6]"),
        ("$[0 : +8 : +2]", "[0,2,4,6]"),
        ("$[6:0:-1]", "[6,5,4,3,2,1]"),
        ("$[6::-1]", "[]"),
        ("$[6:0:-2]", "[6,4,2]"),
        ("$[6::-2]", "[]"),
        ("$[8:0:-2]", "[8,6,4,2]"),
        ("$[0:2:4294967296]", "[0]"),
        ("$[2:0:-4294967296]", "[2]"),
        ("$[1:3:9223372036854775807]", "[1]"),
        ("$[2:0:-9223372036854775807]", "[2]"),
        ("$[2:0:-9223372036854775808]", "[2]"),
    ] {
        v.get(&["JSON.GET", "k1", path], exp);
    }
    for path in [
        "$[0:2:9223372036854775808]",
        "$[2:0:-9223372036854775809]",
        "$[0:2:18446744073709551616]",
    ] {
        v.err_has(&["JSON.GET", "k1", path], &["Invalid number"]);
    }
    v.done("test_json_get_v2path_array_slice");
}

#[test]
fn test_json_get_v2path_array_union() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", "[0,1,2,3,4,5,6,7,8,9]"]);
    for (path, exp) in [
        ("$[0,1,2]", "[0,1,2]"),
        ("$[0, 1, 2]", "[0,1,2]"),
        ("$[0, 1, 2  ]", "[0,1,2]"),
        ("$[ 0, 1, 2 ]", "[0,1,2]"),
        ("$[ 0,1, 2 ]", "[0,1,2]"),
        ("$[0,1]", "[0,1]"),
        ("$[-1,-2]", "[9,8]"),
        ("$[-10,-5,-6]", "[0,5,4]"),
        ("$[0,1,5,0,1,2]", "[0,1,5,0,1,2]"),
        ("$[0, 1,5,0, 1,2]", "[0,1,5,0,1,2]"),
        ("$[ 0, 1, 5, 0, 1, 2 ]", "[0,1,5,0,1,2]"),
        ("$[ -10 , -5 , -6 ]", "[0,5,4]"),
        ("$[-10,-9,-8,0,1,2,-1000,1000]", "[0,1,2,0,1,2]"),
    ] {
        v.get(&["JSON.GET", "k1", path], exp);
    }
    v.run(&[
        "JSON.SET",
        "k2",
        ".",
        r#"[{"name":"name0","id":0},{"name":"name1","id":1},{"name":"name2","id":2}]"#,
    ]);
    v.get(&["JSON.GET", "k2", "$[0,2].name"], r#"["name0","name2"]"#);
    for path in [
        "$[0,1,2:4]",
        "$[0:2,3,4]",
        "$[0,,4]",
        "$[0,,,4]",
        "$[,4]",
        "$[4,]",
        "$[,4,]",
        "$[,,4,,]",
        "$[,]",
        "$[,0,4]",
        "$[,0,4,]",
        "$[0,4,]",
    ] {
        v.err(&["JSON.GET", "k1", path], SYNTAX);
    }
    v.done("test_json_get_v2path_array_union");
}

#[test]
fn test_json_get_multipaths_legacy_and_v2path_wildcard() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", r#"{"a":[], "b":[1], "c":[1,2]}"#]);
    v.run(&[
        "JSON.SET",
        "k2",
        ".",
        r#"{"a":{}, "b": {"a": 1}, "c": {"a": 1, "b": 2}}"#,
    ]);
    v.get(
        &["JSON.GET", "k1", ".b[*]", ".c[*]"],
        r#"{".b[*]":1,".c[*]":1}"#,
    );
    v.get(
        &["JSON.GET", "k2", ".b.*", ".c.*"],
        r#"{".b.*":1,".c.*":1}"#,
    );
    v.err(&["JSON.GET", "k1", ".a[*]", ".b[*]"], NONEXISTENT);
    v.err(&["JSON.GET", "k2", ".a.*", ".b.*"], NONEXISTENT);
    v.err(&["JSON.GET", "k2", ".foo.*", ".c.*"], NONEXISTENT);
    for (key, p1, p2, p3, exp) in [
        (
            "k1",
            "$.a[*]",
            "$.b[*]",
            "$.c[*]",
            r#"{"$.a[*]":[],"$.b[*]":[1],"$.c[*]":[1,2]}"#,
        ),
        (
            "k1",
            "$.a[*]",
            ".b[*]",
            ".c[*]",
            r#"{"$.a[*]":[],".b[*]":[1],".c[*]":[1,2]}"#,
        ),
        (
            "k1",
            ".a[*]",
            "$.b[*]",
            ".c[*]",
            r#"{".a[*]":[],"$.b[*]":[1],".c[*]":[1,2]}"#,
        ),
        (
            "k2",
            ".a.*",
            ".b.*",
            "$.c.*",
            r#"{".a.*":[],".b.*":[1],"$.c.*":[1,2]}"#,
        ),
        (
            "k2",
            "$.a.*",
            "$.b.*",
            ".c.*",
            r#"{"$.a.*":[],"$.b.*":[1],".c.*":[1,2]}"#,
        ),
        (
            "k2",
            ".a.*",
            "$.b.*",
            "$.c.*",
            r#"{".a.*":[],"$.b.*":[1],"$.c.*":[1,2]}"#,
        ),
        (
            "k2",
            ".foo.*",
            "$.b.*",
            "$.c.*",
            r#"{".foo.*":[],"$.b.*":[1],"$.c.*":[1,2]}"#,
        ),
    ] {
        v.get(&["JSON.GET", key, p1, p2, p3], exp);
    }
    v.done("test_json_get_multipaths_legacy_and_v2path_wildcard");
}

#[test]
fn test_json_mget_command() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "k1", ".", r#"{"foo":"bar1"}"#]);
    v.ok(&["JSON.SET", "k2", ".", r#"{"foo":"bar2"}"#]);
    v.ok(&["JSON.SET", "k3", ".", r#"{"foo":"bar3"}"#]);
    v.arr(
        &["JSON.MGET", "k1", "k2", "k3", ".foo"],
        &[r#""bar1""#, r#""bar2""#, r#""bar3""#],
    );
    v.want(
        &["JSON.MGET", "k1", "k2", "k3", ".bar"],
        Reply::Array(vec![Reply::Nil, Reply::Nil, Reply::Nil]),
    );
    v.want(
        &["JSON.MGET", "baz", "foo", "."],
        Reply::Array(vec![Reply::Nil, Reply::Nil]),
    );
    v.want(
        &["JSON.MGET", "baz", "k2", ".foo"],
        Reply::Array(vec![Reply::Nil, Reply::bulk(r#""bar2""#)]),
    );
    v.err_has(&["JSON.MGET"], &["wrong number of arguments"]);
    v.ok(&[
        "json.set",
        "k4",
        ".",
        r#"[2,5,{"level0":[null,true,{"level0_1":[3,false]}],"level1":{"level1_0":33}}]"#,
    ]);
    v.ok(&[
        "json.set",
        "k5",
        ".",
        r#"[4,5,{"level0":[null,false,{"level0_1":[null,false]}],"level1":{"level1_0":[12,13]}}]"#,
    ]);
    v.ok(&[
        "json.set",
        "k6",
        ".",
        r#"[2,5,{"level0":[true,20,{"level0_1":[3,true]}],"level1":{"level1_0":33}}]"#,
    ]);
    for (path, exp) in [
        (
            "$..level0_1",
            ["[[3,false]]", "[[null,false]]", "[[3,true]]"],
        ),
        (
            "$.[2].level1",
            [
                r#"[{"level1_0":33}]"#,
                r#"[{"level1_0":[12,13]}]"#,
                r#"[{"level1_0":33}]"#,
            ],
        ),
        ("$.[2].level1.level1_0[2]", ["[]", "[]", "[]"]),
        ("$.[2].level1.level1_0[1]", ["[]", "[13]", "[]"]),
    ] {
        v.arr(&["JSON.MGET", "k4", "k5", "k6", path], &exp);
    }
    v.done("test_json_mget_command");
}

#[test]
fn test_json_key_declaration() {
    let mut v = V::new();
    let need_val = [
        "SET",
        "NUMMULTBY",
        "NUMINCRBY",
        "ARRAPPEND",
        "ARRINDEX",
        "STRAPPEND",
        "RESP",
    ];
    for cmd in [
        "DEL",
        "GET",
        "SET",
        "TYPE",
        "NUMINCRBY",
        "NUMMULTBY",
        "TOGGLE",
        "STRAPPEND",
        "STRLEN",
        "ARRAPPEND",
        "ARRINDEX",
        "ARRLEN",
        "ARRPOP",
        "CLEAR",
        "OBJKEYS",
        "OBJLEN",
        "FORGET",
        "RESP",
    ] {
        let name = format!("JSON.{cmd}");
        if need_val.contains(&cmd) {
            v.arr(&["COMMAND", "GETKEYS", &name, "k1", ".", "5"], &["k1"]);
        } else {
            v.arr(&["COMMAND", "GETKEYS", &name, "k1"], &["k1"]);
        }
    }
    v.arr(
        &["COMMAND", "GETKEYS", "JSON.ARRINSERT", "k1", ".", "0", "5"],
        &["k1"],
    );
    v.arr(
        &["COMMAND", "GETKEYS", "JSON.ARRTRIM", "k1", ".", "0", "5"],
        &["k1"],
    );
    for sub in ["MEMORY", "DEPTH"] {
        v.arr(&["COMMAND", "GETKEYS", "JSON.DEBUG", sub, "k1"], &["k1"]);
    }
    v.arr(
        &["COMMAND", "GETKEYS", "JSON.MGET", "k1", "k2", "k3", "."],
        &["k1", "k2", "k3"],
    );
    v.done("test_json_key_declaration");
}

fn del_or_forget(cmd: &str, name: &str) {
    let mut v = V::new();
    for path in [".spouse", ".phoneNumbers"] {
        v.int(&[cmd, "wikipedia", path], 1);
        v.err(&["JSON.GET", "wikipedia", path], NONEXISTENT);
    }
    v.int(&[cmd, "wikipedia"], 1);
    v.int(&["EXISTS", "wikipedia"], 0);
    v.run(&["json.set", "k1", ".", "1"]);
    v.int(&[cmd, "k1", "."], 1);
    v.int(&["EXISTS", "k1"], 0);
    v.int(&[cmd, "foo", ".firstName"], 0);
    // the document is gone by now, so this is the "path does not exist" case on a fresh one
    v.ok(&["JSON.SET", "wikipedia", ".", WIKI_PRETTY]);
    v.int(&[cmd, "wikipedia", ".foo"], 0);
    v.err_has(
        &[cmd, "wikipedia", ".children", "extra"],
        &["wrong number of arguments"],
    );
    v.done(name);
}

#[test]
fn test_json_del_command() {
    del_or_forget("JSON.DEL", "test_json_del_command");
}

#[test]
fn test_json_forget_command() {
    del_or_forget("JSON.FORGET", "test_json_forget_command");
}

#[test]
fn test_json_del_command_v2path_wildcard() {
    let mut v = V::new();
    v.run(&[
        "json.set",
        "k1",
        ".",
        r#"{"x": {}, "y": {"a":"a"}, "z": {"a":"", "b":"b"}}"#,
    ]);
    for k in ["k2", "k3", "k4", "k5"] {
        v.run(&["json.set", k, ".", "[0,1,2,3,4,5,6,7,8,9]"]);
    }
    for (key, path, ret, val) in [
        ("k1", "$.z.*", 2, r#"{"x":{},"y":{"a":"a"},"z":{}}"#),
        ("k1", "$.*", 3, "{}"),
        ("k2", "$.[3:6]", 3, "[0,1,2,6,7,8,9]"),
        ("k2", "$.*", 7, "[]"),
    ] {
        v.int(&["JSON.DEL", key, path], ret);
        v.get(&["JSON.GET", key], val);
    }
    for key in ["k1", "k2"] {
        v.int(&["JSON.DEL", key, "$"], 1);
        v.int(&["EXISTS", key], 0);
    }
    v.int(&["JSON.DEL", "k3", "$[*]"], 10);
    v.get(&["JSON.GET", "k3"], "[]");
    v.int(&["JSON.DEL", "k4", "$[3:7]"], 4);
    v.get(&["JSON.GET", "k4"], "[0,1,2,7,8,9]");
    v.int(&["JSON.DEL", "k5", "$[1,5,7,8]"], 4);
    v.get(&["JSON.GET", "k5"], "[0,2,3,4,6,9]");
    v.ok(&[
        "json.set",
        "k1",
        ".",
        r#"[2,5,{"level0":[null,true,{"level0_1":[3,false]}],"level1":{"level1_0":33}}]"#,
    ]);
    for (path, ret, val) in [
        (
            "$[2].level0..level0_1[1]",
            1,
            r#"[2,5,{"level0":[null,true,{"level0_1":[3]}],"level1":{"level1_0":33}}]"#,
        ),
        (
            "$[2].level0..level0_1",
            1,
            r#"[2,5,{"level0":[null,true,{}],"level1":{"level1_0":33}}]"#,
        ),
        (
            "$..level1.level1_0",
            1,
            r#"[2,5,{"level0":[null,true,{}],"level1":{}}]"#,
        ),
        ("$..level1", 1, r#"[2,5,{"level0":[null,true,{}]}]"#),
        (".*", 3, "[]"),
    ] {
        v.int(&["JSON.DEL", "k1", path], ret);
        v.get(&["JSON.GET", "k1"], val);
        if path == ".*" {
            v.err(&["JSON.GET", "k1", path], NONEXISTENT);
        } else {
            v.get(&["JSON.GET", "k1", path], "[]");
        }
    }
    v.done("test_json_del_command_v2path_wildcard");
}

#[test]
fn test_json_unicode_is_supported() {
    let mut v = V::new();
    for s in [r#""Eat, drink, 愛""#, r#""hyvää-élève""#] {
        v.ok(&["JSON.SET", "k1", ".", s]);
        v.get(&["JSON.GET", "k1", "NOESCAPE", "."], s);
        v.int(&["JSON.DEL", "k1"], 1);
        v.int(&["EXISTS", "k1"], 0);
    }
    v.done("test_json_unicode_is_supported");
}

#[test]
fn test_nonascii_in_jsonpath() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", r#"{"a": {"愛": "love", "b": "b"}}"#]);
    v.run(&["JSON.SET", "k2", ".", r#"{"愛": [1,2,3]}"#]);
    v.get(&["JSON.GET", "k1", ".a.愛"], r#""love""#);
    v.get(
        &["JSON.GET", "k1", ".a.愛", ".a.b"],
        r#"{".a.愛":"love",".a.b":"b"}"#,
    );
    v.get(&["JSON.GET", "k2", ".愛[1]"], "2");
    v.get(&["JSON.GET", "k2", "$.愛[*]"], "[1,2,3]");
    v.done("test_nonASCII_in_jsonpath");
}

#[test]
fn test_json_number_scanner() {
    let mut v = V::new();
    for (val, ty) in [
        ("9223372036854775807", "integer"),
        ("-9223372036854775807", "integer"),
        ("-9223372036854775808", "integer"),
        ("9223372036854775808", "number"),
        ("-9223372036854775809", "number"),
    ] {
        v.run(&["JSON.SET", "k1", ".", val]);
        v.simple(&["JSON.TYPE", "k1", "."], ty);
    }
    v.done("test_json_number_scanner");
}

#[test]
fn test_json_toggle() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "wikipedia", ".foobool", "false"]);
    v.get(&["JSON.GET", "wikipedia", ".foobool"], "false");
    v.get(&["JSON.TOGGLE", "wikipedia", ".foobool"], "true");
    v.get(&["JSON.GET", "wikipedia", ".foobool"], "true");
    v.get(&["JSON.TOGGLE", "wikipedia", ".foobool"], "false");
    v.get(&["JSON.GET", "wikipedia", ".foobool"], "false");
    v.err_has(&["JSON.TOGGLE", "wikipedia", ".foobool", "extra"], ARGS);
    v.ok(&["JSON.SET", "wikipedia", ".foonum", "55"]);
    v.err(&["JSON.TOGGLE", "wikipedia", ".foonum"], WRONGTYPE);
    v.ok(&["JSON.SET", "wikipedia", ".foostr", r#""ok""#]);
    v.err(&["JSON.TOGGLE", "wikipedia", ".foostr"], WRONGTYPE);
    v.done("test_json_toggle");
}

#[test]
fn test_json_toggle_jsonpath() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{"a":true, "b":false, "c":1, "d":null, "e":"foo", "f":[], "g":{}}"#,
    ]);
    v.ok(&[
        "JSON.SET",
        "k2",
        ".",
        r#"[true, false, 1, null, "foo", [], {}]"#,
    ]);
    let flags = |a: i64, b: i64| {
        let mut items = vec![Reply::Int(a), Reply::Int(b)];
        items.extend(std::iter::repeat_n(Reply::Nil, 5));
        Reply::Array(items)
    };
    v.want(&["JSON.TOGGLE", "k1", "$.*"], flags(0, 1));
    v.get(
        &["JSON.GET", "k1", "."],
        r#"{"a":false,"b":true,"c":1,"d":null,"e":"foo","f":[],"g":{}}"#,
    );
    v.want(&["JSON.TOGGLE", "k1", "$.*"], flags(1, 0));
    v.get(
        &["JSON.GET", "k1", "."],
        r#"{"a":true,"b":false,"c":1,"d":null,"e":"foo","f":[],"g":{}}"#,
    );
    v.want(&["JSON.TOGGLE", "k2", "$[*]"], flags(0, 1));
    v.get(
        &["JSON.GET", "k2", "."],
        r#"[false,true,1,null,"foo",[],{}]"#,
    );
    v.want(&["JSON.TOGGLE", "k2", "$[*]"], flags(1, 0));
    v.get(
        &["JSON.GET", "k2", "."],
        r#"[true,false,1,null,"foo",[],{}]"#,
    );
    v.done("test_json_toggle_jsonpath");
}

#[test]
fn test_json_numincrby() {
    let mut v = V::new();
    v.get(&["JSON.NUMINCRBY", "wikipedia", ".age", "1"], "28");
    v.get(&["JSON.NUMINCRBY", "wikipedia", ".age", "10"], "38");
    v.get(&["JSON.NUMINCRBY", "wikipedia", ".age", "-5"], "33");
    v.ok(&["JSON.SET", "wikipedia", ".foo", "1"]);
    v.get(&["JSON.NUMINCRBY", "wikipedia", ".foo", "0.5"], "1.5");
    v.get(&["JSON.NUMINCRBY", "wikipedia", ".foo", "0.5"], "2");
    v.err(&["JSON.NUMINCRBY", "foo", ".age", "2"], NONEXISTENT);
    v.err(&["JSON.NUMINCRBY", "wikipedia", ".bar", "2"], NONEXISTENT);
    v.err_has(
        &["JSON.NUMINCRBY", "wikipedia", ".age", "1", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_numincrby");
}

#[test]
fn test_json_nummultby() {
    let mut v = V::new();
    v.get(&["JSON.NUMMULTBY", "wikipedia", ".age", "10"], "270");
    v.get(&["JSON.NUMMULTBY", "wikipedia", ".age", "10"], "2700");
    v.get(&["JSON.NUMMULTBY", "wikipedia", ".age", "0.01"], "27");
    v.ok(&["JSON.SET", "wikipedia", ".foo", "1"]);
    v.get(&["JSON.NUMMULTBY", "wikipedia", ".foo", "0.5"], "0.5");
    v.get(&["JSON.NUMMULTBY", "wikipedia", ".foo", "0.5"], "0.25");
    v.get(&["JSON.NUMMULTBY", "wikipedia", ".foo", "4"], "1");
    v.err(&["JSON.NUMMULTBY", "foo", ".age", "2"], NONEXISTENT);
    v.err(&["JSON.NUMMULTBY", "wikipedia", ".bar", "2"], NONEXISTENT);
    v.err_has(
        &["JSON.NUMMULTBY", "wikipedia", ".age", "2", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_nummultby");
}

const STYLED: [(&str, &str); 12] = [
    ("2.50000", "2.5"),
    ("2e30", "2e+30"),
    ("2E+30", "2e+30"),
    ("2E30", "2e+30"),
    ("2E-30", "2.0000000000000002e-30"),
    ("2e5", "200000"),
    ("-2.50000", "-2.5"),
    ("-2e30", "-2e+30"),
    ("-2E+30", "-2e+30"),
    ("-2E30", "-2e+30"),
    ("-2E-30", "-2.0000000000000002e-30"),
    ("-2e5", "-200000"),
];

#[test]
fn test_json_simple_operations_remove_double_styling() {
    let mut v = V::new();
    for (cmd, arg) in [("JSON.NUMMULTBY", "1"), ("JSON.NUMINCRBY", "0")] {
        for (initial, unstyled) in STYLED {
            v.ok(&["JSON.SET", "wikipedia", ".foo", initial]);
            v.get(&["JSON.GET", "wikipedia", ".foo"], initial);
            v.run(&[cmd, "wikipedia", ".foo", arg]);
            v.get(&["JSON.GET", "wikipedia", ".foo"], unstyled);
        }
    }
    v.done("test_json_simple_operations_remove_double_styling");
}

#[test]
fn test_json_double_operations_wrongtype() {
    let mut v = V::new();
    for (cmd, field, arg) in [
        ("JSON.NUMMULTBY", ".age", r#""2.0""#),
        ("JSON.NUMMULTBY", ".age", "2."),
        ("JSON.NUMINCRBY", ".age", "2.0."),
        ("JSON.NUMMULTBY", ".age", "-2."),
        ("JSON.NUMINCRBY", ".age", "-2.0."),
        ("JSON.NUMMULTBY", ".age", "+2."),
        ("JSON.NUMINCRBY", ".age", "+2.0."),
        ("JSON.NUMINCRBY", ".age", ".2.0"),
        ("JSON.NUMINCRBY", ".age", "a2.0"),
        ("JSON.NUMINCRBY", ".age", "-a2.0"),
        ("JSON.NUMINCRBY", ".age", "+a2.0"),
        ("JSON.NUMINCRBY", ".age", "e2.0"),
        ("JSON.NUMINCRBY", ".age", "-e2.0"),
        ("JSON.NUMINCRBY", ".age", "+e2.0"),
        ("JSON.NUMINCRBY", ".age", "e+2.0"),
        ("JSON.NUMINCRBY", ".age", "-e-2.0"),
        ("JSON.NUMINCRBY", ".age", "+E+2.0"),
        ("JSON.NUMINCRBY", ".age", "2.0e"),
        ("JSON.NUMINCRBY", ".age", "2.0eq"),
        ("JSON.NUMINCRBY", ".age", "2.0e3q"),
        ("JSON.NUMINCRBY", ".age", "2.0e+"),
        ("JSON.NUMINCRBY", ".age", "2.0e+a"),
        ("JSON.NUMINCRBY", ".age", "2.0e+41a"),
        ("JSON.NUMINCRBY", ".firstName", "2"),
    ] {
        v.err(&[cmd, "wikipedia", field, arg], WRONGTYPE);
    }
    v.done("test_json_double_operations_wrongtype");
}

/// Upstream feeds 24000 random doubles from Python's `random`; the same
/// property is checked here with a xorshift stream (no inf, NaN, denormals).
#[test]
fn test_json_double_consistency() {
    let mut v = V::new();
    let mut x: u64 = 0x1234_5678_9abc_def1;
    let mut checked = 0;
    while checked < 3000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let d = f64::from_bits(x);
        if !d.is_normal() {
            continue;
        }
        checked += 1;
        let val = format!("{d:?}");
        v.ok(&["JSON.SET", "wikipedia", ".foo", &val]);
        let plain = v.run(&["JSON.GET", "wikipedia", ".foo"]);
        let pretty = v.run(&["JSON.GET", "wikipedia", "INDENT", "\t", ".foo"]);
        v.vectors += 1;
        if plain != pretty || plain != Reply::bulk(val.as_str()) {
            let args = ["JSON.GET", "wikipedia", ".foo"];
            v.fail(&args, &plain, &format!("{val} (pretty: {})", text(&pretty)));
        }
    }
    v.done("test_json_double_consistency");
}

impl V {
    /// A bulk reply equal to any of `alts` (upstream's `a and b or c` checks).
    fn any(&mut self, args: &[&str], alts: &[&str]) {
        self.vectors += 1;
        let got = self.run(args);
        let hit =
            matches!(&got, Reply::Bulk(b) if alts.iter().any(|a| a.as_bytes() == b.as_slice()));
        if !hit {
            self.fail(args, &got, &format!("one of {alts:?}"));
        }
    }
}

#[test]
fn test_json_numincrby_large_numbers() {
    let mut v = V::new();
    for val in [
        "9223372036854775807",
        "9223372036854775807",
        "-9223372036854775808",
        "-9223372036854775808",
    ] {
        v.ok(&["JSON.SET", "wikipedia", ".foo", val]);
        v.get(&["JSON.GET", "wikipedia", ".foo"], val);
        v.get(
            &[
                "JSON.GET",
                "wikipedia",
                "SPACE",
                " ",
                "NEWLINE",
                "\n",
                ".foo",
            ],
            val,
        );
    }
    // (value, alternative spelling, increment, expected, alternative)
    let data = [
        (
            "-9223372036854775808",
            "-9223372036854775808",
            "0",
            "-9.2233720368547758e+18",
            "-9223372036854775808",
        ),
        (
            "-9223372036854775808",
            "-9223372036854775808",
            "1",
            "-9.2233720368547758e+18",
            "-9223372036854775807",
        ),
        (
            "-9223372036854775808",
            "-9223372036854775808",
            "2",
            "-9.2233720368547758e+18",
            "-9223372036854775806",
        ),
        ("1.79e+308", "1.79e308", "0", "1.79e+308", "1.79e308"),
        ("1.79e+308", "1.79e308", "1", "1.79e+308", "1.79e308"),
        ("1.79e+308", "1.79e308", "-1", "1.79e+308", "1.79e308"),
        ("-1.79e+308", "-1.79e308", "0", "-1.79e+308", "-1.79e308"),
        ("-1.79e+308", "-1.79e308", "1", "-1.79e+308", "-1.79e308"),
        ("-1.79e+308", "-1.79e308", "-1", "-1.79e+308", "-1.79e308"),
        (
            "9223372036854775807",
            "9223372036854775807",
            "0",
            "9.2233720368547758e+18",
            "9223372036854775807",
        ),
        (
            "9223372036854775807",
            "9223372036854775807",
            "-1",
            "9.2233720368547758e+18",
            "9223372036854775806",
        ),
        (
            "9223372036854775807",
            "9223372036854775807",
            "-2",
            "9.2233720368547758e+18",
            "9223372036854775805",
        ),
    ];
    for (val, val_alt, incr, exp, exp_alt) in data {
        v.ok(&["JSON.SET", "wikipedia", ".foo", val]);
        v.any(&["JSON.GET", "wikipedia", ".foo"], &[val, val_alt]);
        v.any(
            &["JSON.GET", "wikipedia", "INDENT", "\t", ".foo"],
            &[val, val_alt],
        );
        v.any(
            &["JSON.NUMINCRBY", "wikipedia", ".foo", incr],
            &[exp, exp_alt],
        );
        v.any(&["JSON.GET", "wikipedia", ".foo"], &[exp, exp_alt]);
        v.any(
            &["JSON.GET", "wikipedia", "INDENT", "\t", ".foo"],
            &[exp, exp_alt],
        );
    }
    v.done("test_json_numincrby_large_numbers");
}

#[test]
fn test_json_nummultby_large_numbers() {
    let mut v = V::new();
    for (val, val_alt, mult) in [
        ("9223372036854775807", "9223372036854775807", "1.79e+308"),
        ("9223372036854775807", "9223372036854775807", "-1.79e+308"),
        ("1.79e+308", "1.79e308", "1.79e+308"),
        ("1.03e+300", "1.03e300", "1.03e+300"),
        ("1.79e+308", "1.79e308", "-1.79e+308"),
        ("-1.03e+300", "-1.03e300", "1.03e+300"),
    ] {
        v.ok(&["JSON.SET", "wikipedia", ".foo", val]);
        v.any(&["JSON.GET", "wikipedia", ".foo"], &[val, val_alt]);
        v.any(
            &[
                "JSON.GET",
                "wikipedia",
                "SPACE",
                " ",
                "NEWLINE",
                "\n",
                ".foo",
            ],
            &[val, val_alt],
        );
        v.err(&["JSON.NUMMULTBY", "wikipedia", ".foo", mult], OVERFLOW);
    }
    let data = [
        ("9223372036854775807", "9223372036854775807", "0", "0", "0"),
        (
            "9223372036854775807",
            "9223372036854775807",
            "2",
            "1.8446744073709552e+19",
            "18446744073709553000.0",
        ),
        (
            "9223372036854775807",
            "9223372036854775807",
            "9223372036854775807",
            "8.5070591730234616e+37",
            "8.5070591730234616e37",
        ),
        (
            "9223372036854775807",
            "9223372036854775807",
            "-9223372036854775807",
            "-8.5070591730234616e+37",
            "-8.5070591730234616e37",
        ),
        ("1.79e308", "1.79e+308", "0", "0", "0"),
        ("1.79e308", "1.79e+308", "1", "1.79e+308", "1.79e308"),
        ("-1.79e308", "-1.79e+308", "1", "-1.79e+308", "-1.79e308"),
        (
            "9223372036854775807",
            "9223372036854775807",
            "1",
            "9.2233720368547758e+18",
            "9223372036854775807",
        ),
    ];
    for (val, val_alt, mult, exp, exp_alt) in data {
        v.ok(&["JSON.SET", "wikipedia", ".foo", val]);
        v.any(&["JSON.GET", "wikipedia", ".foo"], &[val, val_alt]);
        v.any(
            &["JSON.GET", "wikipedia", "INDENT", "\t", ".foo"],
            &[val, val_alt],
        );
        v.any(
            &["JSON.NUMMULTBY", "wikipedia", ".foo", mult],
            &[exp, exp_alt],
        );
        v.any(&["JSON.GET", "wikipedia", ".foo"], &[exp, exp_alt]);
        v.any(
            &["JSON.GET", "wikipedia", "INDENT", "\t", ".foo"],
            &[exp, exp_alt],
        );
    }
    v.done("test_json_nummultby_large_numbers");
}

/// An array reply holding integers and nils (`None`).
fn opt_ints(items: &[Option<i64>]) -> Reply {
    Reply::Array(
        items
            .iter()
            .map(|n| n.map_or(Reply::Nil, Reply::Int))
            .collect(),
    )
}

fn any_err(v: &mut V, args: &[&str]) {
    v.err(args, &[""]);
}

#[test]
fn test_json_strlen_command() {
    let mut v = V::new();
    v.int(&["JSON.STRLEN", "wikipedia", ".address.state"], 2);
    v.int(&["JSON.STRLEN", "wikipedia", ".firstName"], 4);
    v.ok(&["JSON.SET", "wikipedia", ".foo", r#""""#]);
    v.int(&["JSON.STRLEN", "wikipedia", ".foo"], 0);
    v.nil(&["JSON.STRLEN", "foo", ".firstName"]);
    v.err(&["JSON.STRLEN", "wikipedia", ".bar"], NONEXISTENT);
    for path in [".address", ".groups", ".age", ".isAlive", ".spouse"] {
        any_err(&mut v, &["JSON.STRLEN", "wikipedia", path]);
    }
    v.err_has(
        &["JSON.STRLEN", "wikipedia", ".firstName", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_strlen_command");
}

#[test]
fn test_json_strappend_command() {
    let mut v = V::new();
    for (val, new_len, new_val) in [
        (r#""son""#, 7, r#""Johnson""#),
        (r#"" Junior""#, 14, r#""Johnson Junior""#),
        (r#"" is""#, 17, r#""Johnson Junior is""#),
        (r#"" my""#, 20, r#""Johnson Junior is my""#),
        (r#"" friend.""#, 28, r#""Johnson Junior is my friend.""#),
        (r#""""#, 28, r#""Johnson Junior is my friend.""#),
    ] {
        v.int(&["JSON.STRAPPEND", "wikipedia", ".firstName", val], new_len);
        v.get(&["JSON.GET", "wikipedia", ".firstName"], new_val);
    }
    v.ok(&["JSON.SET", "wikipedia", ".foo", r#""""#]);
    v.run(&["JSON.STRAPPEND", "wikipedia", ".foo", r#""abc""#]);
    v.get(&["JSON.GET", "wikipedia", ".foo"], r#""abc""#);
    v.err(
        &["JSON.STRAPPEND", "foo", ".firstName", r#""abc""#],
        NONEXISTENT,
    );
    v.err(
        &["JSON.STRAPPEND", "wikipedia", ".bar", r#""abc""#],
        NONEXISTENT,
    );
    for val in ["123", "true", "false", "null", "{}", "[]"] {
        v.err(
            &["JSON.STRAPPEND", "wikipedia", ".firstName", val],
            WRONGTYPE,
        );
    }
    for path in [".address", ".groups", ".age", ".isAlive", ".spouse"] {
        v.err(&["JSON.STRAPPEND", "wikipedia", path, r#""12""#], WRONGTYPE);
    }
    v.err_has(
        &[
            "JSON.STRAPPEND",
            "wikipedia",
            ".firstName",
            r#""abc""#,
            "extra",
        ],
        &["wrong number of arguments"],
    );
    v.done("test_json_strappend_command");
}

const STR_DOC: &str =
    r#"{"a":{"a":"a"}, "b":{"a":""}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":3}}"#;

#[test]
fn test_json_strlen_command_legacy_and_jsonpath_wildcard() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", STR_DOC]);
    for (path, exp) in [
        ("$.a.a", vec![Some(1)]),
        ("$.a.*", vec![Some(1)]),
        ("$.b.a", vec![Some(0)]),
        ("$.b.*", vec![Some(0)]),
        ("$.c.*", vec![Some(1), Some(2)]),
        ("$.c.b", vec![Some(2)]),
        ("$.d.*", vec![None, Some(1), None]),
    ] {
        v.want(&["JSON.STRLEN", "k1", path], opt_ints(&exp));
    }
    for (path, exp) in [
        (".a.a", 1),
        (".a.*", 1),
        (".b.a", 0),
        (".b.*", 0),
        (".c.*", 1),
        (".c.b", 2),
        (".d.*", 1),
    ] {
        v.int(&["JSON.STRLEN", "k1", path], exp);
    }
    v.done("test_json_strlen_command_legacy_and_jsonpath_wildcard");
}

#[test]
fn test_json_strappend_command_legacy_and_jsonpath_wildcard() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", STR_DOC]);
    enum Ret {
        Arr(Vec<Option<i64>>),
        One(i64),
    }
    use Ret::{Arr, One};
    let cases: Vec<(&str, &str, Ret, &str)> = vec![
        ("$.a.a", r#""x""#, Arr(vec![Some(2)]), r#"["ax"]"#),
        ("$.a.a", r#""""#, Arr(vec![Some(2)]), r#"["ax"]"#),
        ("$.a.*", r#""yz""#, Arr(vec![Some(4)]), r#"["axyz"]"#),
        (".a.a", r#""a""#, One(5), r#""axyza""#),
        (".a.*", r#""a""#, One(6), r#""axyzaa""#),
        ("$.b.a", r#""a""#, Arr(vec![Some(1)]), r#"["a"]"#),
        ("$.b.*", r#""""#, Arr(vec![Some(1)]), r#"["a"]"#),
        ("$.b.*", r#""a""#, Arr(vec![Some(2)]), r#"["aa"]"#),
        (".b.a", r#""a""#, One(3), r#""aaa""#),
        (".b.*", r#""a""#, One(4), r#""aaaa""#),
        (
            "$.c.*",
            r#""a""#,
            Arr(vec![Some(2), Some(3)]),
            r#"["aa","bba"]"#,
        ),
        ("$.c.b", r#""a""#, Arr(vec![Some(4)]), r#"["bbaa"]"#),
        (".c.*", r#""a""#, One(5), r#""aaa""#),
        (".c.b", r#""a""#, One(6), r#""bbaaaa""#),
        (
            "$.d.*",
            r#""a""#,
            Arr(vec![None, Some(2), None]),
            r#"[1,"ba",3]"#,
        ),
        (".d.*", r#""a""#, One(3), "1"),
    ];
    for (path, append, ret, new_str) in cases {
        let args = ["JSON.STRAPPEND", "k1", path, append];
        match ret {
            Arr(items) => v.want(&args, opt_ints(&items)),
            One(n) => v.int(&args, n),
        }
        v.get(&["JSON.GET", "k1", path], new_str);
    }
    v.get(
        &["JSON.GET", "k1", "."],
        r#"{"a":{"a":"axyzaa"},"b":{"a":"aaaa"},"c":{"a":"aaa","b":"bbaaaa"},"d":{"a":1,"b":"baa","c":3}}"#,
    );
    v.done("test_json_strappend_command_legacy_and_jsonpath_wildcard");
}

const OBJ_DOC: &str =
    r#"{"a":{}, "c":{"a":"a", "b":"bb"}, "d":{"a":1, "b":"b", "c":{"a":3,"b":4}}, "e":1}"#;

#[test]
fn test_json_object_len_command() {
    let mut v = V::new();
    v.int(&["JSON.OBJLEN", "wikipedia", ".address"], 4);
    v.int(&["JSON.OBJLEN", "wikipedia", ".groups"], 0);
    v.nil(&["JSON.OBJLEN", "foo", ".address"]);
    v.err(&["JSON.OBJLEN", "wikipedia", ".foo"], NONEXISTENT);
    for path in [
        ".children",
        ".phoneNumbers",
        ".age",
        ".weight",
        ".isAlive",
        ".spouse",
        ".firstName",
    ] {
        v.err(&["JSON.OBJLEN", "wikipedia", path], WRONGTYPE);
    }
    v.err_has(
        &["JSON.OBJLEN", "wikipedia", ".", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_object_len_command");
}

#[test]
fn test_json_objlen_command_jsonpath_wildcard() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", OBJ_DOC]);
    for (path, exp) in [
        ("$.a", vec![Some(0)]),
        ("$.a.*", vec![]),
        ("$.c", vec![Some(2)]),
        ("$.c.*", vec![None, None]),
        ("$.d", vec![Some(3)]),
        ("$.d.*", vec![None, None, Some(2)]),
        ("$.*", vec![Some(0), Some(2), Some(3), None]),
    ] {
        v.want(&["JSON.OBJLEN", "k1", path], opt_ints(&exp));
    }
    for (path, exp) in [(".a", 0), (".c", 2), (".d", 3), (".*", 0), (".d.*", 2)] {
        v.int(&["JSON.OBJLEN", "k1", path], exp);
    }
    v.err(&["JSON.OBJLEN", "k1", ".a.*"], NONEXISTENT);
    v.err(&["JSON.OBJLEN", "k1", ".c.*"], WRONGTYPE);
    v.err(&["JSON.OBJLEN", "k1", ".e"], WRONGTYPE);
    v.done("test_json_objlen_command_jsonpath_wildcard");
}

#[test]
fn test_json_object_keys_command() {
    let mut v = V::new();
    v.arr(
        &["JSON.OBJKEYS", "wikipedia", ".address"],
        &["street", "city", "state", "zipcode"],
    );
    v.want(
        &["JSON.OBJKEYS", "wikipedia", ".groups"],
        Reply::Array(vec![]),
    );
    v.nil(&["JSON.OBJKEYS", "foo", ".address"]);
    v.nil(&["JSON.OBJKEYS", "wikipedia", ".foo"]);
    for path in [
        ".children",
        ".phoneNumbers",
        ".age",
        ".weight",
        ".isAlive",
        ".spouse",
        ".firstName",
    ] {
        v.err(&["JSON.OBJKEYS", "wikipedia", path], WRONGTYPE);
    }
    v.err_has(
        &["JSON.OBJKEYS", "wikipedia", ".", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_object_keys_command");
}

/// Upstream's table of expected OBJKEYS replies for this test is never
/// asserted (its loop runs over an empty list); only the OBJLEN errors are.
/// The table is checked here too, since the replies are well defined.
#[test]
fn test_json_objkeys_command_jsonpath_wildcard() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", OBJ_DOC]);
    let nested = |rows: &[&[&str]]| Reply::Array(rows.iter().map(|r| bulks(r)).collect());
    v.want(&["JSON.OBJKEYS", "k1", "$.a"], nested(&[&[]]));
    v.want(&["JSON.OBJKEYS", "k1", "$.a.*"], nested(&[]));
    v.arr(&["JSON.OBJKEYS", "k1", ".a"], &[]);
    v.want(&["JSON.OBJKEYS", "k1", "$.c"], nested(&[&["a", "b"]]));
    v.arr(&["JSON.OBJKEYS", "k1", ".c"], &["a", "b"]);
    v.want(&["JSON.OBJKEYS", "k1", "$.d"], nested(&[&["a", "b", "c"]]));
    v.want(
        &["JSON.OBJKEYS", "k1", "$.d.*"],
        nested(&[&[], &[], &["a", "b"]]),
    );
    v.arr(&["JSON.OBJKEYS", "k1", ".d"], &["a", "b", "c"]);
    v.arr(&["JSON.OBJKEYS", "k1", ".d.*"], &["a", "b"]);
    v.want(
        &["JSON.OBJKEYS", "k1", "$.*"],
        nested(&[&[], &["a", "b"], &["a", "b", "c"], &[]]),
    );
    v.arr(&["JSON.OBJKEYS", "k1", ".*"], &["a", "b"]);
    v.want(&["JSON.OBJKEYS", "k1", "$.c.*"], nested(&[&[], &[]]));
    v.err(&["JSON.OBJLEN", "k1", ".c.*"], WRONGTYPE);
    v.err(&["JSON.OBJLEN", "k1", ".e"], WRONGTYPE);
    v.done("test_json_objkeys_command_jsonpath_wildcard");
}

#[test]
fn test_json_arrlen_command() {
    let mut v = V::new();
    v.int(&["JSON.ARRLEN", "wikipedia", ".phoneNumbers"], 2);
    v.int(&["JSON.ARRLEN", "wikipedia", ".children"], 0);
    v.nil(&["JSON.ARRLEN", "foo", ".phoneNumbers"]);
    v.err(&["JSON.ARRLEN", "wikipedia", ".foo"], NONEXISTENT);
    for path in [
        ".address",
        ".groups",
        ".age",
        ".weight",
        ".isAlive",
        ".spouse",
        ".firstName",
    ] {
        v.err(&["JSON.ARRLEN", "wikipedia", path], WRONGTYPE);
    }
    v.err_has(
        &["JSON.ARRLEN", "wikipedia", ".phoneNumbers", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_arrlen_command");
}

#[test]
fn test_json_arrlen_command_jsonpath() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"[[], ["a"], ["a", "b"], ["a", "b", "c"]]"#,
    ]);
    v.ok(&[
        "JSON.SET",
        "k2",
        ".",
        r#"[[], "a", ["a", "b"], ["a", "b", "c"], 4]"#,
    ]);
    v.ints(&["JSON.ARRLEN", "k1", "$.[*]"], &[0, 1, 2, 3]);
    v.want(
        &["JSON.ARRLEN", "k2", "$.[*]"],
        opt_ints(&[Some(0), None, Some(2), Some(3), None]),
    );
    v.done("test_json_arrlen_command_jsonpath");
}

const NON_ARRAY_PATHS: [&str; 7] = [
    ".address",
    ".groups",
    ".age",
    ".weight",
    ".isAlive",
    ".spouse",
    ".firstName",
];

#[test]
fn test_json_arrappend_command() {
    let mut v = V::new();
    v.int(
        &["JSON.ARRAPPEND", "wikipedia", ".children", r#""John""#],
        1,
    );
    v.get(&["JSON.GET", "wikipedia", ".children"], r#"["John"]"#);
    v.int(
        &[
            "JSON.ARRAPPEND",
            "wikipedia",
            ".children",
            r#""Mary""#,
            r#""Tom""#,
        ],
        3,
    );
    v.get(
        &["JSON.GET", "wikipedia", ".children"],
        r#"["John","Mary","Tom"]"#,
    );
    v.int(&["JSON.ARRLEN", "wikipedia", ".children"], 3);
    v.err(
        &["JSON.ARRAPPEND", "foo", ".children", r#""Mary""#],
        NONEXISTENT,
    );
    v.err(
        &["JSON.ARRAPPEND", "wikipedia", ".foo", r#""abc""#],
        NONEXISTENT,
    );
    for path in NON_ARRAY_PATHS {
        v.err(&["JSON.ARRAPPEND", "wikipedia", path, "123"], WRONGTYPE);
    }
    v.done("test_json_arrappend_command");
}

#[test]
fn test_json_arrappend_command_jsonpath() {
    let mut v = V::new();
    let d = r#"[[], ["a"], ["a", "b"], ["a", "b", "c"]]"#;
    v.ok(&["JSON.SET", "k1", ".", d]);
    v.ok(&["JSON.SET", "k2", ".", d]);
    v.ints(&["JSON.ARRAPPEND", "k1", "$.[*]", r#""a""#], &[1, 2, 3, 4]);
    v.get(
        &["JSON.GET", "k1", "$.[*]"],
        r#"[["a"],["a","a"],["a","b","a"],["a","b","c","a"]]"#,
    );
    v.ints(&["JSON.ARRAPPEND", "k2", "$.[*]", r#""""#], &[1, 2, 3, 4]);
    v.get(
        &["JSON.GET", "k2", "$.[*]"],
        r#"[[""],["a",""],["a","b",""],["a","b","c",""]]"#,
    );
    v.done("test_json_arrappend_command_jsonpath");
}

#[test]
fn test_json_arrpop_command() {
    let mut v = V::new();
    v.nil(&["JSON.ARRPOP", "wikipedia", ".children"]);
    v.int(
        &[
            "JSON.ARRAPPEND",
            "wikipedia",
            ".children",
            r#""John""#,
            r#""Mary""#,
            r#""Tom""#,
        ],
        3,
    );
    for (idx, popped, new_len, new_val) in [
        ("1", r#""Mary""#, 2, r#"["John","Tom"]"#),
        ("-1", r#""Tom""#, 1, r#"["John"]"#),
        ("0", r#""John""#, 0, "[]"),
    ] {
        v.get(&["JSON.ARRPOP", "wikipedia", ".children", idx], popped);
        v.int(&["JSON.ARRLEN", "wikipedia", ".children"], new_len);
        v.get(&["JSON.GET", "wikipedia", ".children"], new_val);
    }
    v.nil(&["JSON.ARRPOP", "wikipedia", ".children"]);
    v.err(&["JSON.ARRPOP", "foo", ".children"], NONEXISTENT);
    v.err(&["JSON.ARRPOP", "wikipedia", ".foo"], NONEXISTENT);
    for path in NON_ARRAY_PATHS {
        v.err(&["JSON.ARRPOP", "wikipedia", path], WRONGTYPE);
    }
    v.run(&["JSON.ARRPOP", "wikipedia", ".children", "3000000000"]);
    v.err_has(
        &["JSON.ARRPOP", "wikipedia", ".phoneNumbers", "0", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_arrpop_command");
}

#[test]
fn test_json_arrpop_command_jsonpath() {
    let mut v = V::new();
    let d = r#"[[], ["a"], ["a", "b"], ["a", "b", "c"]]"#;
    v.ok(&["JSON.SET", "k1", ".", d]);
    v.run(&["COPY", "k1", "k2"]);
    v.run(&["COPY", "k1", "k3"]);
    let popped = |items: [Option<&str>; 4]| {
        Reply::Array(
            items
                .iter()
                .map(|i| i.map_or(Reply::Nil, Reply::bulk))
                .collect(),
        )
    };
    for (key, idx, exp, new_val) in [
        (
            "k1",
            "0",
            [None, Some(r#""a""#), Some(r#""a""#), Some(r#""a""#)],
            r#"[[],[],["b"],["b","c"]]"#,
        ),
        (
            "k2",
            "1",
            [None, Some(r#""a""#), Some(r#""b""#), Some(r#""b""#)],
            r#"[[],[],["a"],["a","c"]]"#,
        ),
        (
            "k3",
            "-1",
            [None, Some(r#""a""#), Some(r#""b""#), Some(r#""c""#)],
            r#"[[],[],["a"],["a","b"]]"#,
        ),
    ] {
        v.want(&["JSON.ARRPOP", key, "$.[*]", idx], popped(exp));
        v.get(&["JSON.GET", key, "."], new_val);
    }
    v.done("test_json_arrpop_command_jsonpath");
}

#[test]
fn test_json_arrinsert_command() {
    let mut v = V::new();
    v.int(
        &["JSON.ARRINSERT", "wikipedia", ".children", "0", r#""foo""#],
        1,
    );
    v.get(&["JSON.GET", "wikipedia", ".children"], r#"["foo"]"#);
    v.get(&["JSON.ARRPOP", "wikipedia", ".children"], r#""foo""#);
    v.int(
        &[
            "JSON.ARRAPPEND",
            "wikipedia",
            ".children",
            r#""John""#,
            r#""Mary""#,
            r#""Tom""#,
        ],
        3,
    );
    v.get(
        &["JSON.GET", "wikipedia", ".children"],
        r#"["John","Mary","Tom"]"#,
    );
    for (idx, val, new_len, new_val) in [
        ("0", r#""Kathy""#, 4, r#"["Kathy","John","Mary","Tom"]"#),
        (
            "2",
            r#""Rose""#,
            5,
            r#"["Kathy","John","Rose","Mary","Tom"]"#,
        ),
        (
            "3",
            r#""Bob""#,
            6,
            r#"["Kathy","John","Rose","Bob","Mary","Tom"]"#,
        ),
        (
            "-1",
            r#""Peter""#,
            7,
            r#"["Kathy","John","Rose","Bob","Mary","Peter","Tom"]"#,
        ),
        (
            "-1",
            r#""Jane""#,
            8,
            r#"["Kathy","John","Rose","Bob","Mary","Peter","Jane","Tom"]"#,
        ),
        (
            "8",
            r#""Grace""#,
            9,
            r#"["Kathy","John","Rose","Bob","Mary","Peter","Jane","Tom","Grace"]"#,
        ),
    ] {
        v.int(
            &["JSON.ARRINSERT", "wikipedia", ".children", idx, val],
            new_len,
        );
        v.get(&["JSON.GET", "wikipedia", ".children"], new_val);
    }
    v.err(
        &["JSON.ARRINSERT", "foo", ".children", "0", r#""abc""#],
        NONEXISTENT,
    );
    v.err(
        &["JSON.ARRINSERT", "wikipedia", ".foo", "0", "123"],
        NONEXISTENT,
    );
    for path in NON_ARRAY_PATHS {
        v.err(&["JSON.ARRINSERT", "wikipedia", path, "0", "1"], WRONGTYPE);
    }
    v.err(
        &[
            "JSON.ARRINSERT",
            "wikipedia",
            ".children",
            "3000000000",
            r#""a""#,
        ],
        OUT_OF_BOUNDS,
    );
    v.err(
        &["JSON.ARRINSERT", "wikipedia", ".children", "31", r#""a""#],
        OUT_OF_BOUNDS,
    );
    v.done("test_json_arrinsert_command");
}

#[test]
fn test_json_arrinsert_command_jsonpath() {
    let mut v = V::new();
    let d = "[[], [0], [0, 1], [0, 1, 2]]";
    v.ok(&["JSON.SET", "k1", ".", d]);
    v.run(&["JSON.SET", "k2", ".", d]);
    v.run(&["JSON.SET", "k3", ".", d]);
    v.ints(&["JSON.ARRINSERT", "k1", "$.[*]", "0", "3"], &[1, 2, 3, 4]);
    v.get(&["JSON.GET", "k1", "."], "[[3],[3,0],[3,0,1],[3,0,1,2]]");
    v.ints(&["JSON.ARRINSERT", "k3", "$.[*]", "-1", "3"], &[1, 2, 3, 4]);
    v.get(&["JSON.GET", "k3", "."], "[[3],[3,0],[0,3,1],[0,1,3,2]]");
    v.err(&["JSON.ARRINSERT", "k2", "$.[*]", "1", "3"], OUT_OF_BOUNDS);
    v.done("test_json_arrinsert_command_jsonpath");
}

#[test]
fn test_json_clear_command() {
    let mut v = V::new();
    v.int(
        &[
            "JSON.ARRAPPEND",
            "wikipedia",
            ".children",
            r#""John""#,
            r#""Mary""#,
            r#""Tom""#,
        ],
        3,
    );
    v.get(
        &["JSON.GET", "wikipedia", ".children"],
        r#"["John","Mary","Tom"]"#,
    );
    v.int(&["JSON.CLEAR", "wikipedia", ".children"], 1);
    v.int(&["JSON.ARRLEN", "wikipedia", ".children"], 0);
    v.int(&["JSON.CLEAR", "wikipedia", ".children"], 0);
    v.int(&["JSON.ARRLEN", "wikipedia", ".children"], 0);
    v.int(&["JSON.CLEAR", "wikipedia", ".foo"], 0);
    v.ok(&["JSON.SET", "wikipedia", ".foobool", "false"]);
    v.int(&["JSON.CLEAR", "wikipedia", ".foobool"], 0);
    let before = v.run(&["JSON.OBJLEN", "wikipedia"]);
    v.vectors += 1;
    if before == Reply::Int(0) {
        v.fail(&["JSON.OBJLEN", "wikipedia"], &before, "non-zero");
    }
    v.int(&["JSON.CLEAR", "wikipedia"], 1);
    v.int(&["JSON.OBJLEN", "wikipedia"], 0);
    v.done("test_json_clear_command");
}

#[test]
fn test_json_clear_command_jsonpath() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{"a":{}, "b":{"a": 1, "b": null, "c": true}, "c":1, "d":true, "e":null, "f":"d"}"#,
    ]);
    v.ok(&[
        "JSON.SET",
        "k2",
        ".",
        r#"[[], [0], [0,1], [0,1,2], 1, true, null, "d"]"#,
    ]);
    v.int(&["JSON.CLEAR", "k1", "$.*"], 4);
    v.get(
        &["JSON.GET", "k1", "."],
        r#"{"a":{},"b":{},"c":0,"d":false,"e":null,"f":""}"#,
    );
    v.int(&["JSON.CLEAR", "k2", "$[*]"], 6);
    v.get(&["JSON.GET", "k2", "."], r#"[[],[],[],[],0,false,null,""]"#);
    v.done("test_json_clear_command_jsonpath");
}

#[test]
fn test_json_arrtrim_command() {
    let mut v = V::new();
    v.int(&["JSON.ARRTRIM", "wikipedia", ".children", "0", "1"], 0);
    v.int(
        &[
            "JSON.ARRAPPEND",
            "wikipedia",
            ".children",
            r#""John""#,
            r#""Mary""#,
            r#""Tom""#,
        ],
        3,
    );
    v.get(
        &["JSON.GET", "wikipedia", ".children"],
        r#"["John","Mary","Tom"]"#,
    );
    for (path, start, end, new_len, new_val) in [
        (".children", "1", "2", 2, r#"["Mary","Tom"]"#),
        (".children", "0", "0", 1, r#"["Mary"]"#),
        (".children", "-1", "5", 1, r#"["Mary"]"#),
        (".phoneNumbers", "2", "0", 0, "[]"),
    ] {
        v.int(&["JSON.ARRTRIM", "wikipedia", path, start, end], new_len);
        v.get(&["JSON.GET", "wikipedia", path], new_val);
    }
    v.err(&["JSON.ARRTRIM", "foo", ".children", "0", "1"], NONEXISTENT);
    v.err(
        &["JSON.ARRTRIM", "wikipedia", ".foo", "0", "3"],
        NONEXISTENT,
    );
    for path in NON_ARRAY_PATHS {
        v.err(&["JSON.ARRTRIM", "wikipedia", path, "0", "1"], WRONGTYPE);
    }
    v.run(&[
        "JSON.ARRTRIM",
        "wikipedia",
        ".phoneNumbers",
        "3000000000",
        "3000000001",
    ]);
    v.err_has(
        &[
            "JSON.ARRTRIM",
            "wikipedia",
            ".phoneNumbers",
            "0",
            "1",
            "extra",
        ],
        &["wrong number of arguments"],
    );
    v.done("test_json_arrtrim_command");
}

#[test]
fn test_json_arrtrim_command_jsonpath() {
    let mut v = V::new();
    let d = r#"[[], ["a"], ["a", "b"], ["a", "b", "c"]]"#;
    v.ok(&["JSON.SET", "k1", ".", d]);
    v.ok(&["JSON.SET", "k2", ".", d]);
    v.ok(&[
        "JSON.SET",
        "k3",
        ".",
        "[[], [0], [0,1], [0,1,2], [0,1,2,3]]",
    ]);
    v.ints(&["JSON.ARRTRIM", "k1", "$.[*]", "0", "1"], &[0, 1, 2, 2]);
    v.get(
        &["JSON.GET", "k1", "."],
        r#"[[],["a"],["a","b"],["a","b"]]"#,
    );
    v.ints(&["JSON.ARRTRIM", "k2", "$.[*]", "1", "1"], &[0, 0, 1, 1]);
    v.get(&["JSON.GET", "k2", "."], r#"[[],[],["b"],["b"]]"#);
    v.ints(&["JSON.ARRTRIM", "k3", "$.[*]", "1", "2"], &[0, 0, 1, 2, 2]);
    v.get(&["JSON.GET", "k3", "."], "[[],[],[1],[1,2],[1,2]]");
    v.done("test_json_arrtrim_command_jsonpath");
}

impl V {
    fn simple(&mut self, args: &[&str], want: &str) {
        self.want(args, Reply::Simple(want.into()));
    }
}

fn simples(items: &[&str]) -> Reply {
    Reply::Array(items.iter().map(|s| Reply::Simple((*s).into())).collect())
}

#[test]
fn test_json_arrindex_command() {
    let mut v = V::new();
    v.int(&["JSON.ARRINDEX", "wikipedia", ".children", r#""tom""#], -1);
    v.int(
        &[
            "JSON.ARRAPPEND",
            "wikipedia",
            ".children",
            r#""John""#,
            r#""Mary""#,
            r#""Tom""#,
            r#""Paul""#,
            r#""Peter""#,
        ],
        5,
    );
    for (val, idx) in [
        (r#""John""#, 0),
        (r#""Tom""#, 2),
        (r#""Peter""#, 4),
        (r#""Peter2""#, -1),
    ] {
        v.int(&["JSON.ARRINDEX", "wikipedia", ".children", val], idx);
    }
    for (val, start, stop, idx) in [
        (r#""Tom""#, "5", "0", -1),
        (r#""Paul""#, "0", "4", 3),
        (r#""Paul""#, "0", "0", 3),
    ] {
        v.int(
            &["JSON.ARRINDEX", "wikipedia", ".children", val, start, stop],
            idx,
        );
    }
    v.ok(&["JSON.SET", "arr", ".", "[0, 1, 2, 3, 4]"]);
    v.int(&["JSON.ARRINDEX", "arr", ".", "1"], 1);
    v.int(&["JSON.ARRINDEX", "arr", ".", "1", "2"], -1);
    v.err(
        &["JSON.ARRINDEX", "foo", ".children", "0", "5"],
        NONEXISTENT,
    );
    v.err(
        &["JSON.ARRINDEX", "wikipedia", ".foo", "0", "3"],
        NONEXISTENT,
    );
    for path in NON_ARRAY_PATHS {
        v.err(&["JSON.ARRINDEX", "wikipedia", path, "1"], WRONGTYPE);
    }
    v.run(&[
        "JSON.ARRINDEX",
        "wikipedia",
        ".phoneNumbers",
        "1",
        "3000000000",
        "0",
    ]);
    v.done("test_json_arrindex_command");
}

#[test]
fn test_json_arrindex_command_jsonpath() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"[[], ["a"], ["a", "b"], ["a", "b", "c"]]"#,
    ]);
    v.ok(&["JSON.SET", "k2", ".", "[[], [0], [0,1], [0,1,2]]"]);
    v.ok(&[
        "JSON.SET",
        "k3",
        ".",
        "[[], [0,true], [0,1,false], [0,1,2,null,true]]",
    ]);
    for (key, val, exp) in [
        ("k1", r#""a""#, [-1, 0, 0, 0]),
        ("k1", r#""b""#, [-1, -1, 1, 1]),
        ("k1", r#""c""#, [-1, -1, -1, 2]),
        ("k2", "1", [-1, -1, 1, 1]),
        ("k2", "2", [-1, -1, -1, 2]),
        ("k2", "true", [-1, -1, -1, -1]),
        ("k3", "true", [-1, 1, -1, 4]),
        ("k3", "null", [-1, -1, -1, 3]),
    ] {
        v.ints(&["JSON.ARRINDEX", key, "$.[*]", val], &exp);
    }
    v.done("test_json_arrindex_command_jsonpath");
}

#[test]
fn test_json_arrindex_should_not_limit_to_scalar_value() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", r#"[5, 6, {"a":"b"}, [99,100]]"#]);
    v.int(&["JSON.ARRINDEX", "k1", ".", r#"{"a":"b"}"#, "0", "0"], 2);
    v.int(&["JSON.ARRINDEX", "k1", ".", "[99,100]", "0", "0"], 3);
    v.int(&["JSON.ARRINDEX", "k1", ".", "5", "0", "0"], 0);
    v.int(&["JSON.ARRINDEX", "k1", ".", "6", "0", "0"], 1);
    v.err_has(
        &[
            "JSON.ARRINDEX",
            "wikipedia",
            ".phoneNumbers",
            "1",
            "0",
            "3",
            "extra",
        ],
        &["wrong number of arguments"],
    );
    v.done("test_json_arrindex_should_not_limit_to_scalar_value");
}

#[test]
fn test_json_arrindex_complex_v2_path() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{"level0":{"level1_0":{"level2":[1,2,3, [25, [4,5,{"c":"d"}]]]},"level1_1":{"level2":[[{"a":[2,5]},true,null]]}}}"#,
    ]);
    let n = |x: i64| Some(x);
    for (path, val, exp) in [
        (
            "$..level0.level1_0..",
            r#"[4,5,{"c":"d"}]"#,
            [None, n(-1), n(1), n(-1), None],
        ),
        (
            "$..level0.level1_0..",
            r#"[25,[4,5,{"c":"d"}]]"#,
            [None, n(3), n(-1), n(-1), None],
        ),
        (
            "$..level0.level1_0..",
            r#"{"c":"d"}"#,
            [None, n(-1), n(-1), n(2), None],
        ),
        (
            "$..level0.level1_1..",
            r#"[{"a":[2,5]},true,null]"#,
            [None, n(0), n(-1), None, n(-1)],
        ),
        (
            "$..level0.level1_1..",
            r#"[null,true,{"a":[2,5]}]"#,
            [None, n(-1), n(-1), None, n(-1)],
        ),
        (
            "$..level0.level1_1..",
            r#"[{"a":[2,5]},true]"#,
            [None, n(-1), n(-1), None, n(-1)],
        ),
        (
            "$..level0.level1_0..",
            r#"[4,{"c":"d"}]"#,
            [None, n(-1), n(-1), n(-1), None],
        ),
    ] {
        v.want(&["JSON.ARRINDEX", "k1", path, val], opt_ints(&exp));
    }
    v.done("test_json_arrindex_complex_v2_path");
}

#[test]
fn test_json_type_command() {
    let mut v = V::new();
    for (path, ty) in [
        (".", "object"),
        (".groups", "object"),
        (".phoneNumbers", "array"),
        (".children", "array"),
        (".isAlive", "boolean"),
        (".spouse", "null"),
        (".address.city", "string"),
        (".age", "integer"),
        (".weight", "number"),
    ] {
        v.simple(&["JSON.TYPE", "wikipedia", path], ty);
    }
    v.nil(&["JSON.TYPE", "foo"]);
    v.nil(&["JSON.TYPE", "wikipedia", ".foo"]);
    v.err_has(
        &["JSON.TYPE", "wikipedia", ".phoneNumbers", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_type_command");
}

#[test]
fn test_json_type_command_jsonpath() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{"a":1, "b":2.3, "c":"foo", "d":true, "e":null, "f":{}, "g":[]}"#,
    ]);
    v.ok(&[
        "JSON.SET",
        "k2",
        ".",
        r#"[1, 2.3, "foo", true, null, {}, []]"#,
    ]);
    let all = [
        "integer", "number", "string", "boolean", "null", "object", "array",
    ];
    v.want(&["JSON.TYPE", "k1", "$.*"], simples(&all));
    v.want(&["JSON.TYPE", "k2", "$[*]"], simples(&all));
    v.done("test_json_type_command_jsonpath");
}

fn pair(k: &str, val: Reply) -> Reply {
    Reply::Array(vec![Reply::bulk(k), val])
}

fn resp_obj(members: Vec<Reply>) -> Reply {
    let mut items = vec![Reply::Simple("{".into())];
    items.extend(members);
    Reply::Array(items)
}

#[test]
fn test_json_resp_command() {
    let mut v = V::new();
    v.get(&["JSON.RESP", "wikipedia", ".firstName"], "John");
    v.simple(&["JSON.RESP", "wikipedia", ".isAlive"], "true");
    v.int(&["JSON.RESP", "wikipedia", ".age"], 27);
    v.nil(&["JSON.RESP", "wikipedia", ".spouse"]);
    v.get(&["JSON.RESP", "wikipedia", ".weight"], "135.17");
    v.want(
        &["JSON.RESP", "wikipedia", ".children"],
        Reply::Array(vec![Reply::Simple("[".into())]),
    );
    v.want(
        &["JSON.RESP", "wikipedia", ".address"],
        resp_obj(vec![
            pair("street", Reply::bulk("21 2nd Street")),
            pair("city", Reply::bulk("New York")),
            pair("state", Reply::bulk("NY")),
            pair("zipcode", Reply::bulk("10021-3100")),
        ]),
    );
    v.want(
        &["JSON.RESP", "wikipedia", ".phoneNumbers"],
        Reply::Array(vec![
            Reply::Simple("[".into()),
            resp_obj(vec![
                pair("type", Reply::bulk("home")),
                pair("number", Reply::bulk("212 555-1234")),
            ]),
            resp_obj(vec![
                pair("type", Reply::bulk("office")),
                pair("number", Reply::bulk("646 555-4567")),
            ]),
        ]),
    );
    v.nil(&["JSON.RESP", "foo"]);
    v.err(&["JSON.RESP", "wikipedia", ".foo"], NONEXISTENT);
    v.err_has(
        &["JSON.RESP", "wikipedia", ".phoneNumbers", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_resp_command");
}

#[test]
fn test_json_resp_command_jsonpath() {
    let mut v = V::new();
    v.want(
        &["JSON.RESP", "wikipedia", "$.firstName"],
        Reply::Array(vec![Reply::bulk("John")]),
    );
    v.want(
        &["JSON.RESP", "wikipedia", "$.isAlive"],
        Reply::Array(vec![Reply::Simple("true".into())]),
    );
    v.want(
        &["JSON.RESP", "wikipedia", "$.age"],
        Reply::Array(vec![Reply::Int(27)]),
    );
    v.want(
        &["JSON.RESP", "wikipedia", "$.spouse"],
        Reply::Array(vec![Reply::Nil]),
    );
    v.want(&["JSON.RESP", "wikipedia", "$.foo"], Reply::Array(vec![]));
    v.want(
        &["JSON.RESP", "wikipedia", "$.weight"],
        Reply::Array(vec![Reply::bulk("135.17")]),
    );
    v.want(
        &["JSON.RESP", "wikipedia", "$.children"],
        Reply::Array(vec![Reply::Array(vec![Reply::Simple("[".into())])]),
    );
    v.arr(
        &["JSON.RESP", "wikipedia", "$.address.*"],
        &["21 2nd Street", "New York", "NY", "10021-3100"],
    );
    v.want(
        &["JSON.RESP", "wikipedia", "$.phoneNumbers.*"],
        Reply::Array(vec![
            resp_obj(vec![
                pair("type", Reply::bulk("home")),
                pair("number", Reply::bulk("212 555-1234")),
            ]),
            resp_obj(vec![
                pair("type", Reply::bulk("office")),
                pair("number", Reply::bulk("646 555-4567")),
            ]),
        ]),
    );
    v.done("test_json_resp_command_jsonpath");
}

/// The error and arity vectors of upstream `test_json_debug_memory`; byte
/// counts are not compared, only that the whole-document and `.` answers agree.
#[test]
fn test_json_debug_memory() {
    let mut v = V::new();
    v.nil(&["JSON.DEBUG", "MEMORY", "nonexistentkey"]);
    v.err(
        &["JSON.DEBUG", "MEMORY", "wikipedia", "nonexistentpath"],
        NONEXISTENT,
    );
    v.err_has(&["JSON.DEBUG", "MEMORY"], &["wrong number of arguments"]);
    v.err(&["JSON.DEBUG", "MEMORY123", "wikipedia"], SYNTAX);
    v.err(&["JSON.DEBUG", "M", "wikipedia"], SYNTAX);
    v.err_has(
        &["JSON.DEBUG", "MEMORY", "wikipedia", ".", "extra"],
        &["wrong number of arguments"],
    );
    let whole = v.run(&["JSON.DEBUG", "MEMORY", "wikipedia"]);
    let root = v.run(&["JSON.DEBUG", "MEMORY", "wikipedia", "."]);
    v.vectors += 1;
    if whole != root || !matches!(whole, Reply::Int(_)) {
        v.fail(
            &["JSON.DEBUG", "MEMORY", "wikipedia"],
            &whole,
            &format!("an integer equal to the `.` answer {}", text(&root)),
        );
    }
    v.done("test_json_debug_memory");
}

#[test]
fn test_json_duplicate_keys() {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", r#"{"a":0, "a":1}"#]);
    v.get(&["JSON.GET", "k1", "."], r#"{"a":1}"#);
    v.arr(&["JSON.OBJKEYS", "k1"], &["a"]);
    v.get(&["JSON.GET", "k1", "a"], "1");
    v.int(&["JSON.OBJLEN", "k1"], 1);
    v.run(&["JSON.SET", "k1", "a", "2"]);
    v.get(&["JSON.GET", "k1", "."], r#"{"a":2}"#);
    v.run(&["JSON.NUMINCRBY", "k1", "a", "2"]);
    v.get(&["JSON.GET", "k1", "."], r#"{"a":4}"#);
    v.run(&["JSON.NUMMULTBY", "k1", "a", "2"]);
    v.get(&["JSON.GET", "k1", "."], r#"{"a":8}"#);
    v.run(&["JSON.DEL", "k1", "a"]);
    v.get(&["JSON.GET", "k1", "."], "{}");
    v.done("test_json_duplicate_keys");
}

fn json_with_depth(depth: usize) -> String {
    format!("{}{{}}{}", r#"{"a":"#.repeat(depth), "}".repeat(depth))
}

#[test]
fn test_json_set_command_max_depth() {
    let mut v = V::new();
    v.run(&["CONFIG", "SET", "json.max-path-limit", "128"]);
    v.ok(&["JSON.SET", "k", ".", &json_with_depth(127)]);
    v.err(&["JSON.SET", "k", ".", &json_with_depth(200_000)], LIMIT);
    v.done("test_json_set_command_max_depth");
}

#[test]
fn test_json_set_command_max_size() {
    let mut v = V::new();
    let with_size = |size: usize| format!("\"{}\"", "a".repeat(size - 2));
    let mb = 1usize << 20;
    v.ok(&["JSON.SET", "k", ".", &with_size(33 * mb)]);
    v.err(&["JSON.SET", "k", ".", &with_size(64 * mb)], LIMIT);
    v.done("test_json_set_command_max_size");
}

#[test]
fn test_json_mset_command_supports_all_datatypes() {
    let mut v = V::new();
    for (path, value) in [
        (".address.city", r#""Boston""#),
        (".age", "30"),
        (".foo", "[1,2,3]"),
        (".phoneNumbers[0].number", r#""1234567""#),
        (".foo", r#"{"a":"b"}"#),
        (".lastName", "null"),
        (".isAlive", "false"),
    ] {
        v.ok(&["JSON.MSET", "wikipedia", path, value]);
        v.get(&["JSON.GET", "wikipedia", path], value);
    }
    v.done("test_json_mset_command_supports_all_datatypes");
}

#[test]
fn test_json_mset_command_root_document() {
    let mut v = V::new();
    v.ok(&[
        "JSON.MSET",
        "k1",
        ".",
        r#""Boston""#,
        "k2",
        ".",
        "123",
        "k3",
        ".",
        r#"["Seattle","Boston"]"#,
        "k4",
        ".",
        "[1,2,3]",
        "k5",
        ".",
        r#"{"a":"b"}"#,
        "k6",
        ".",
        "{}",
        "k7",
        ".",
        "null",
        "k8",
        ".",
        "false",
    ]);
    for (key, value) in [
        ("k1", r#""Boston""#),
        ("k2", "123"),
        ("k3", r#"["Seattle","Boston"]"#),
        ("k4", "[1,2,3]"),
        ("k5", r#"{"a":"b"}"#),
        ("k6", "{}"),
        ("k7", "null"),
        ("k8", "false"),
    ] {
        v.get(&["JSON.GET", key], value);
    }
    v.done("test_json_mset_command_root_document");
}

#[test]
fn test_json_mset_command_with_error_conditions() {
    let mut v = V::new();
    v.err(&["JSON.MSET", "foo", ".bar", r#""bar""#], SYNTAX);
    v.done("test_json_mset_command_with_error_conditions");
}

const MIXED_MSET: &[&str] = &[
    "JSON.MSET",
    "k1",
    ".",
    r#""Boston""#,
    "wikipedia",
    ".address.city",
    r#""Boston""#,
    "k2",
    ".",
    "123",
    "wikipedia",
    ".age",
    "30",
    "k3",
    ".",
    r#"["Seattle","Boston"]"#,
    "k4",
    ".",
    "[1,2,3]",
    "wikipedia",
    ".phoneNumbers[0].number",
    r#""1234567""#,
    "k5",
    ".",
    r#"{"a":"b"}"#,
    "wikipedia",
    ".foo",
    r#"{"a":"b"}"#,
    "k6",
    ".",
    "{}",
    "wikipedia",
    ".lastName",
    "null",
    "k7",
    ".",
    "null",
];

const MIXED_EXPECT: [(&str, &str, &str); 14] = [
    ("k1", ".", r#""Boston""#),
    ("k2", ".", "123"),
    ("k3", ".", r#"["Seattle","Boston"]"#),
    ("k4", ".", "[1,2,3]"),
    ("k5", ".", r#"{"a":"b"}"#),
    ("k6", ".", "{}"),
    ("k7", ".", "null"),
    ("k8", ".", "false"),
    ("wikipedia", ".address.city", r#""Boston""#),
    ("wikipedia", ".age", "30"),
    ("wikipedia", ".phoneNumbers[0].number", r#""1234567""#),
    ("wikipedia", ".foo", r#"{"a":"b"}"#),
    ("wikipedia", ".lastName", "null"),
    ("wikipedia", ".isAlive", "false"),
];

/// The full mixed MSET of upstream (`MIXED_MSET` plus its tail).
fn mixed_mset() -> Vec<&'static str> {
    let mut a = MIXED_MSET.to_vec();
    a.extend(["wikipedia", ".isAlive", "false", "k8", ".", "false"]);
    a
}

#[test]
fn test_json_mset_command_mixed_path_document() {
    let mut v = V::new();
    v.ok(&mixed_mset());
    for (key, path, value) in MIXED_EXPECT {
        v.get(&["JSON.GET", key, path], value);
    }
    v.done("test_json_mset_command_mixed_path_document");
}

#[test]
fn test_json_mset_command_atomicity() {
    let mut v = V::new();
    v.ok(&mixed_mset());
    v.err(
        &[
            "JSON.MSET",
            "k1",
            ".",
            r#""Seattle""#,
            "wikipedia",
            ".address.city",
            r#""Seattle""#,
            "k2",
            ".",
            "456",
            "wikipedia",
            ".age",
            "40",
            "k3",
            ".",
            r#"["Seattle","Chicago"]"#,
            "k4",
            ".",
            "[4,5,6]",
            "wikipedia",
            ".phoneNumbers[0].number",
            r#""56789""#,
            "k5",
            ".",
            r#"{"c":"d"}"#,
            "wikipedia",
            ".foo",
            r#"{"c":"d"}"#,
            "k6",
            ".",
            "{}",
            "wikipedia",
            ".lastName",
            "null",
            "k7",
            ".",
            "null",
            "foo",
            ".bar",
            r#""bar""#,
            "k8",
            ".",
            "true",
        ],
        SYNTAX,
    );
    for (key, path, value) in MIXED_EXPECT {
        v.get(&["JSON.GET", key, path], value);
    }
    v.done("test_json_mset_command_atomicity");
}

#[test]
fn test_json_mset_duplicate_key_root_then_path() {
    let mut v = V::new();
    v.run(&["JSON.SET", "dk", "$", r#"{"a":1}"#]);
    v.ok(&["JSON.MSET", "dk", "$", "[1,2,3]", "dk", ".a", "99"]);
    v.simple(&["PING"], "PONG");
    v.get(&["JSON.GET", "dk", "."], "[1,2,3]");
    v.done("test_json_mset_duplicate_key_root_then_path");
}

#[test]
fn test_json_mset_duplicate_key_nested_path() {
    let mut v = V::new();
    v.run(&["JSON.SET", "dk", "$", r#"{"a":{"x":1}}"#]);
    v.ok(&["JSON.MSET", "dk", ".a", "[9]", "dk", ".a.x", "5"]);
    v.simple(&["PING"], "PONG");
    v.get(&["JSON.GET", "dk", ".a"], "[9]");
    v.done("test_json_mset_duplicate_key_nested_path");
}

#[test]
fn test_json_mset_duplicate_key_3ops_best_effort() {
    let mut v = V::new();
    v.run(&["JSON.SET", "dk", "$", r#"{"a":1}"#]);
    v.run(&["JSON.SET", "mset3opk", "$", r#"{"v":0}"#]);
    v.ok(&[
        "JSON.MSET",
        "dk",
        "$",
        "[1,2,3]",
        "dk",
        ".a",
        "99",
        "mset3opk",
        ".v",
        "42",
    ]);
    v.simple(&["PING"], "PONG");
    v.get(&["JSON.GET", "dk", "."], "[1,2,3]");
    v.get(&["JSON.GET", "mset3opk", "."], r#"{"v":42}"#);
    v.done("test_json_mset_duplicate_key_3ops_best_effort");
}

#[test]
fn test_json_mset_duplicate_key_later_op_valid() {
    let mut v = V::new();
    v.run(&["JSON.SET", "dk", "$", r#"{"a":1}"#]);
    v.ok(&["JSON.MSET", "dk", "$", r#"{"b":1}"#, "dk", ".b", "2"]);
    v.simple(&["PING"], "PONG");
    v.get(&["JSON.GET", "dk", "."], r#"{"b":2}"#);
    v.done("test_json_mset_duplicate_key_later_op_valid");
}

#[test]
fn test_multi_exec() {
    let mut v = V::new();
    v.ok(&["MULTI"]);
    v.simple(&["JSON.DEL", "wikipedia", ".address.street"], "QUEUED");
    v.simple(&["JSON.DEL", "wikipedia", ".address.zipcode"], "QUEUED");
    v.simple(
        &["JSON.SET", "wikipedia", ".address.region", r#""US East""#],
        "QUEUED",
    );
    v.want(
        &["EXEC"],
        Reply::Array(vec![Reply::Int(1), Reply::Int(1), Reply::Ok]),
    );
    v.any(
        &["JSON.GET", "wikipedia", ".address"],
        &[
            r#"{"city":"New York","state":"NY","region":"US East"}"#,
            r#"{"state":"NY","city":"New York","region":"US East"}"#,
        ],
    );
    v.ok(&["MULTI"]);
    v.simple(&["JSON.ARRPOP", "wikipedia", ".phoneNumbers"], "QUEUED");
    v.simple(&["JSON.ARRPOP", "wikipedia", ".phoneNumbers"], "QUEUED");
    v.simple(
        &["JSON.ARRAPPEND", "wikipedia", ".phoneNumbers", "123"],
        "QUEUED",
    );
    v.simple(
        &["JSON.ARRAPPEND", "wikipedia", ".phoneNumbers", "456"],
        "QUEUED",
    );
    v.run(&["EXEC"]);
    v.get(&["JSON.GET", "wikipedia", ".phoneNumbers"], "[123,456]");
    v.done("test_multi_exec");
}

#[test]
fn test_escaped_member_names() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{"a\\a":1, "b\tb":2, "c\nc":3, "d\rd":4, "e\be":5, "f\"f":6, "":7, "'":8}"#,
    ]);
    v.ok(&[
        "JSON.SET",
        "k2",
        ".",
        r#"{"key\u0000":"value\u0000", "key\u001F":"value\u001F"}"#,
    ]);
    for (key, path, exp) in [
        ("k1", r#"$["a\\a"]"#, "[1]"),
        ("k1", r"$['a\a']", "[1]"),
        ("k1", r#"$["b\tb"]"#, "[2]"),
        ("k1", r"$['b\tb']", "[2]"),
        ("k1", r#"$["c\nc"]"#, "[3]"),
        ("k1", r"$['c\nc']", "[3]"),
        ("k1", r#"$["d\rd"]"#, "[4]"),
        ("k1", r"$['d\rd']", "[4]"),
        ("k1", r#"$["e\be"]"#, "[5]"),
        ("k1", r"$['e\be']", "[5]"),
        ("k1", r#"$["f\"f"]"#, "[6]"),
        ("k1", r#"$['f"f']"#, "[6]"),
        ("k1", r#"$[""]"#, "[7]"),
        ("k1", "$['']", "[7]"),
        ("k1", r#"$["'"]"#, "[8]"),
        ("k1", r"$['\'']", "[8]"),
        ("k2", r#"$["key\u0000"]"#, r#"["value\u0000"]"#),
        ("k2", r#"$["key\u001F"]"#, r#"["value\u001F"]"#),
    ] {
        v.get(&["JSON.GET", key, path], exp);
    }
    v.done("test_escaped_member_names");
}

#[test]
fn test_serializing_escaped_quotes_in_member_name() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "k1", ".", r#"{"\"a":1, "\"b":2}"#]);
    v.get(&["JSON.GET", "k1", "."], r#"{"\"a":1,"\"b":2}"#);
    v.get(
        &["JSON.GET", "k1", "space", " ", "."],
        r#"{"\"a": 1,"\"b": 2}"#,
    );
    v.done("test_serializing_escaped_quotes_in_member_name");
}

const NUM_K1: &str = r#"{"a":[], "b":[1], "c":[1,2], "d":[1,2,3]}"#;
const NUM_K2: &str = r#"{"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}, "d":{"a":1, "b":2, "c":3}}"#;
const NUM_K3: &str =
    r#"{"a":{"a":"a"}, "b":{"a":"a", "b":1}, "c":{"a":"a", "b":"b"}, "d":{"a":1, "b":"b", "c":3}}"#;

/// A step is `(command, key, path, argument, expected bulk reply)`.
type Step = (
    &'static str,
    &'static str,
    &'static str,
    Option<&'static str>,
    &'static str,
);

fn num_doc_steps(name: &str, steps: &[Step], legacy_errors: Option<&str>) {
    let mut v = V::new();
    v.run(&["JSON.SET", "k1", ".", NUM_K1]);
    v.run(&["JSON.SET", "k2", ".", NUM_K2]);
    v.run(&["JSON.SET", "k3", ".", NUM_K3]);
    if let Some(cmd) = legacy_errors {
        let arg = if cmd == "JSON.NUMINCRBY" { "1" } else { "2" };
        for key in ["k1", "k2"] {
            v.err(&[cmd, key, ".a.*", arg], NONEXISTENT);
            v.err(&["JSON.GET", key, ".a.*"], NONEXISTENT);
        }
        for path in [".a.*", ".c.*"] {
            v.err(&[cmd, "k3", path, arg], WRONGTYPE);
        }
    }
    for &(cmd, key, path, arg, exp) in steps {
        match arg {
            Some(a) => v.get(&[cmd, key, path, a], exp),
            None => v.get(&[cmd, key, path], exp),
        }
    }
    v.done(name);
}

#[test]
fn test_json_numincrby_jsonpath_and_wildcard() {
    const I: &str = "JSON.NUMINCRBY";
    const G: &str = "JSON.GET";
    num_doc_steps(
        "test_json_numincrby_jsonpath_and_wildcard",
        &[
            (I, "k1", "$.a.*", Some("1"), "[]"),
            (G, "k1", "$.a.*", None, "[]"),
            (I, "k1", "$.b.*", Some("1"), "[2]"),
            (G, "k1", "$.b.*", None, "[2]"),
            (I, "k1", "$.b[*]", Some("1"), "[3]"),
            (G, "k1", "$.b[*]", None, "[3]"),
            (I, "k1", "$.d.*", Some("1"), "[2,3,4]"),
            (G, "k1", "$.d.*", None, "[2,3,4]"),
            (I, "k1", "$.d[*]", Some("1"), "[3,4,5]"),
            (G, "k1", "$.d[*]", None, "[3,4,5]"),
            (I, "k2", "$.a.*", Some("1"), "[]"),
            (G, "k2", "$.a.*", None, "[]"),
            (I, "k2", "$.b.*", Some("1"), "[2]"),
            (G, "k2", "$.b.*", None, "[2]"),
            (I, "k2", "$.d.*", Some("1"), "[2,3,4]"),
            (G, "k2", "$.d.*", None, "[2,3,4]"),
            (I, "k3", "$.a.*", Some("1"), "[null]"),
            (G, "k3", "$.a.*", None, r#"["a"]"#),
            (I, "k3", "$.b.*", Some("1"), "[null,2]"),
            (G, "k3", "$.b.*", None, r#"["a",2]"#),
            (I, "k3", "$.c.*", Some("1"), "[null,null]"),
            (G, "k3", "$.c.*", None, r#"["a","b"]"#),
            (I, "k3", "$.d.*", Some("1"), "[2,null,4]"),
            (G, "k3", "$.d.*", None, r#"[2,"b",4]"#),
        ],
        None,
    );
}

#[test]
fn test_json_numincrby_legacy_path_and_wildcard() {
    const I: &str = "JSON.NUMINCRBY";
    const G: &str = "JSON.GET";
    num_doc_steps(
        "test_json_numincrby_legacy_path_and_wildcard",
        &[
            (I, "k1", ".b.*", Some("1"), "2"),
            (G, "k1", ".b.*", None, "2"),
            (I, "k1", ".b[*]", Some("1"), "3"),
            (G, "k1", ".b[*]", None, "3"),
            (I, "k1", ".d.*", Some("1"), "4"),
            (G, "k1", ".d.*", None, "2"),
            (I, "k1", ".d[*]", Some("1"), "5"),
            (G, "k1", ".d[*]", None, "3"),
            (I, "k2", ".b.*", Some("1"), "2"),
            (G, "k2", ".b.*", None, "2"),
            (I, "k2", ".d.*", Some("1"), "4"),
            (G, "k2", ".d.*", None, "2"),
            (I, "k3", ".b.*", Some("1"), "2"),
            (G, "k3", ".b.*", None, r#""a""#),
            (I, "k3", ".d.*", Some("1"), "4"),
            (G, "k3", ".d.*", None, "2"),
        ],
        Some(I),
    );
}

#[test]
fn test_json_nummultby_jsonpath_and_wildcard() {
    const M: &str = "JSON.NUMMULTBY";
    const G: &str = "JSON.GET";
    num_doc_steps(
        "test_json_nummultby_jsonpath_and_wildcard",
        &[
            (M, "k1", "$.a.*", Some("2"), "[]"),
            (G, "k1", "$.a.*", None, "[]"),
            (M, "k1", "$.b.*", Some("2"), "[2]"),
            (G, "k1", "$.b.*", None, "[2]"),
            (M, "k1", "$.b[*]", Some("2"), "[4]"),
            (G, "k1", "$.b[*]", None, "[4]"),
            (M, "k1", "$.d.*", Some("2"), "[2,4,6]"),
            (G, "k1", "$.d.*", None, "[2,4,6]"),
            (M, "k1", "$.d[*]", Some("2"), "[4,8,12]"),
            (G, "k1", "$.d[*]", None, "[4,8,12]"),
            (M, "k2", "$.a.*", Some("2"), "[]"),
            (G, "k2", "$.a.*", None, "[]"),
            (M, "k2", "$.b.*", Some("2"), "[2]"),
            (G, "k2", "$.b.*", None, "[2]"),
            (M, "k2", "$.d.*", Some("2"), "[2,4,6]"),
            (G, "k2", "$.d.*", None, "[2,4,6]"),
            (M, "k3", "$.a.*", Some("2"), "[null]"),
            (G, "k3", "$.a.*", None, r#"["a"]"#),
            (M, "k3", "$.b.*", Some("2"), "[null,2]"),
            (G, "k3", "$.b.*", None, r#"["a",2]"#),
            (M, "k3", "$.c.*", Some("2"), "[null,null]"),
            (G, "k3", "$.c.*", None, r#"["a","b"]"#),
            (M, "k3", "$.d.*", Some("2"), "[2,null,6]"),
            (G, "k3", "$.d.*", None, r#"[2,"b",6]"#),
        ],
        None,
    );
}

#[test]
fn test_json_nummultby_legacy_path_and_wildcard() {
    const M: &str = "JSON.NUMMULTBY";
    const G: &str = "JSON.GET";
    num_doc_steps(
        "test_json_nummultby_legacy_path_and_wildcard",
        &[
            (M, "k1", ".b.*", Some("2"), "2"),
            (G, "k1", ".b.*", None, "2"),
            (M, "k1", ".b[*]", Some("2"), "4"),
            (G, "k1", ".b[*]", None, "4"),
            (M, "k1", ".d.*", Some("2"), "6"),
            (G, "k1", ".d.*", None, "2"),
            (M, "k1", ".d[*]", Some("2"), "12"),
            (G, "k1", ".d[*]", None, "4"),
            (M, "k2", ".b.*", Some("2"), "2"),
            (G, "k2", ".b.*", None, "2"),
            (M, "k2", ".d.*", Some("2"), "6"),
            (G, "k2", ".d.*", None, "2"),
            (M, "k3", ".b.*", Some("2"), "2"),
            (G, "k3", ".b.*", None, r#""a""#),
            (M, "k3", ".d.*", Some("2"), "6"),
            (G, "k3", ".d.*", None, "2"),
        ],
        Some(M),
    );
}

const PRODUCTS: &str = r#"{"productsById":{"product1":{"price":10,"active":true,"title":"One","tags":[1,2,3],"meta":{"old":1}},"product2":{"price":20,"active":false,"title":"Two","tags":[4,5,6],"meta":{"old":2}},"product3":{"price":30,"active":true,"title":"Three","tags":[7,8,9],"meta":{"old":3}}}}"#;
const PRODUCT2: &str =
    r#"{"price":20,"active":false,"title":"Two","tags":[4,5,6],"meta":{"old":2}}"#;
const PRODUCT3: &str =
    r#"{"price":30,"active":true,"title":"Three","tags":[7,8,9],"meta":{"old":3}}"#;
const FILTER_DOC: &str = r#"{"a":1,"b":{"a":1,"x":2},"c":{"a":2}}"#;

/// A fresh database holding `doc` at `k1`.
fn with_doc(doc: &str) -> V {
    let mut v = V::new();
    v.ok(&["JSON.SET", "k1", "$", doc]);
    v
}

#[test]
fn test_jsonpath_filter_object_children() {
    let mut v = V::new();
    let cases: [(&str, &str, &str); 10] = [
        (FILTER_DOC, "$[?(@.a==1)]", r#"[{"a":1,"x":2}]"#),
        (
            PRODUCTS,
            "$.productsById[?(@.title)].title",
            r#"["One","Two","Three"]"#,
        ),
        (
            PRODUCTS,
            "$.productsById[?(@.price > 15 && @.price < 25)].title",
            r#"["Two"]"#,
        ),
        (
            PRODUCTS,
            r#"$.productsById[?(@.price > 25 || @.title == "One")].title"#,
            r#"["One","Three"]"#,
        ),
        (
            PRODUCTS,
            "$.productsById[?(@.tags[0] > 3)].title",
            r#"["Two","Three"]"#,
        ),
        (
            PRODUCTS,
            "$.productsById[?(@.tags[?(@==5)])].title",
            r#"["Two"]"#,
        ),
        (r#"{"low":1,"high":3}"#, "$[?(@ > 2)]", "[3]"),
        (
            r#"{"a value":{"weight":300},"weight":300}"#,
            "$[?(@.weight > 200)]",
            r#"[{"weight":300}]"#,
        ),
        (
            PRODUCTS,
            r#"$["productsById"][?(@["price"] > 20)]["title"]"#,
            r#"["Three"]"#,
        ),
        (PRODUCTS, "$.productsById[?(@.price > 100)].title", "[]"),
    ];
    for (doc, path, exp) in cases {
        v.ok(&["JSON.SET", "k1", "$", doc]);
        v.get(&["JSON.GET", "k1", path], exp);
    }
    v.done("test_jsonpath_filter_object_children");
}

#[test]
fn test_jsonpath_filter_object_children_read_commands() {
    let mut v = with_doc(PRODUCTS);
    let sel = "$.productsById[?(@.price > 15)]";
    v.get(
        &["JSON.GET", "k1", sel],
        &format!("[{PRODUCT2},{PRODUCT3}]"),
    );
    v.ints(&["JSON.OBJLEN", "k1", sel], &[5, 5]);
    let keys: &[&str] = &["price", "active", "title", "tags", "meta"];
    v.want(
        &["JSON.OBJKEYS", "k1", sel],
        Reply::Array(vec![bulks(keys), bulks(keys)]),
    );
    v.want(&["JSON.TYPE", "k1", sel], simples(&["object", "object"]));
    v.ints(&["JSON.STRLEN", "k1", &format!("{sel}.title")], &[3, 5]);
    v.ints(&["JSON.ARRLEN", "k1", &format!("{sel}.tags")], &[3, 3]);
    v.ints(
        &["JSON.ARRINDEX", "k1", &format!("{sel}.tags"), "5"],
        &[1, -1],
    );
    v.done("test_jsonpath_filter_object_children_read_commands");
}

#[test]
fn test_jsonpath_filter_object_children_command_impact() {
    let unchanged = FILTER_DOC;
    let cases: Vec<(Vec<&str>, Reply, &str)> = vec![
        (
            vec!["JSON.GET", "k1", "$[?(@.a==1)]"],
            Reply::bulk(r#"[{"a":1,"x":2}]"#),
            unchanged,
        ),
        (
            vec!["JSON.OBJLEN", "k1", "$[?(@.a==1)]"],
            ints(&[2]),
            unchanged,
        ),
        (
            vec!["JSON.SET", "k1", "$[?(@.a==1)].a", "9"],
            Reply::Ok,
            r#"{"a":1,"b":{"a":9,"x":2},"c":{"a":2}}"#,
        ),
        (
            vec!["JSON.NUMINCRBY", "k1", "$[?(@.a==1)].a", "10"],
            Reply::bulk("[11]"),
            r#"{"a":1,"b":{"a":11,"x":2},"c":{"a":2}}"#,
        ),
        (
            vec!["JSON.DEL", "k1", "$[?(@.a==1)]"],
            Reply::Int(1),
            r#"{"a":1,"c":{"a":2}}"#,
        ),
        (
            vec!["JSON.CLEAR", "k1", "$[?(@.a==1)]"],
            Reply::Int(1),
            r#"{"a":1,"b":{},"c":{"a":2}}"#,
        ),
    ];
    let mut v = V::new();
    for (args, reply, doc) in cases {
        v.ok(&["JSON.SET", "k1", "$", FILTER_DOC]);
        v.want(&args, reply);
        v.get(&["JSON.GET", "k1"], doc);
    }
    v.done("test_jsonpath_filter_object_children_command_impact");
}

#[test]
fn test_jsondel_filter_on_object_issue_79() {
    let mut v = with_doc(
        r#"{"root":{"1":{"value":1},"2":{"value":2},"3":{"value":3},"4":{"value":4},"5":{"value":5}}}"#,
    );
    v.int(&["JSON.DEL", "k1", "$.root[?(@.value > 2)]"], 3);
    v.get(
        &["JSON.GET", "k1"],
        r#"{"root":{"1":{"value":1},"2":{"value":2}}}"#,
    );
    v.done("test_jsondel_filter_on_object_issue_79");
}

#[test]
fn test_jsonpath_filter_object_children_preserves_member_name() {
    let mut v = V::new();
    for name in ["slash/key", "tilde~key", "0", "space key"] {
        let doc = |value: u8| {
            format!(
                r#"{{"items":{{"{name}":{{"selected":true,"value":{value}}},"other":{{"selected":false,"value":2}}}}}}"#
            )
        };
        v.ok(&["JSON.SET", "k1", "$", &doc(1)]);
        v.ok(&["JSON.SET", "k1", "$.items[?(@.selected==true)].value", "9"]);
        v.get(&["JSON.GET", "k1"], &doc(9));
    }
    v.done("test_jsonpath_filter_object_children_preserves_member_name");
}

#[test]
fn test_jsonpath_filter_object_children_mutation_commands() {
    let sel = "$.productsById[?(@.price > 15)]";
    let p = |tail: &str| format!("{sel}{tail}");
    let cases: Vec<(Vec<String>, &str, &str)> = vec![
        (
            vec!["JSON.SET".into(), p(".title"), r#""Updated""#.into()],
            "$.productsById.*.title",
            r#"["One","Updated","Updated"]"#,
        ),
        (
            vec![
                "JSON.MERGE".into(),
                p(".meta"),
                r#"{"updated":true}"#.into(),
            ],
            "$.productsById.*.meta",
            r#"[{"old":1},{"old":2,"updated":true},{"old":3,"updated":true}]"#,
        ),
        (
            vec!["JSON.NUMINCRBY".into(), p(".price"), "1".into()],
            "$.productsById.*.price",
            "[10,21,31]",
        ),
        (
            vec!["JSON.NUMMULTBY".into(), p(".price"), "2".into()],
            "$.productsById.*.price",
            "[10,40,60]",
        ),
        (
            vec!["JSON.TOGGLE".into(), p(".active")],
            "$.productsById.*.active",
            "[true,true,false]",
        ),
        (
            vec!["JSON.STRAPPEND".into(), p(".title"), r#""!""#.into()],
            "$.productsById.*.title",
            r#"["One","Two!","Three!"]"#,
        ),
        (
            vec!["JSON.ARRAPPEND".into(), p(".tags"), "10".into()],
            "$.productsById.*.tags",
            "[[1,2,3],[4,5,6,10],[7,8,9,10]]",
        ),
        (
            vec!["JSON.ARRINSERT".into(), p(".tags"), "0".into(), "0".into()],
            "$.productsById.*.tags",
            "[[1,2,3],[0,4,5,6],[0,7,8,9]]",
        ),
        (
            vec!["JSON.ARRPOP".into(), p(".tags"), "-1".into()],
            "$.productsById.*.tags",
            "[[1,2,3],[4,5],[7,8]]",
        ),
        (
            vec!["JSON.ARRTRIM".into(), p(".tags"), "0".into(), "1".into()],
            "$.productsById.*.tags",
            "[[1,2,3],[4,5],[7,8]]",
        ),
        (
            vec!["JSON.CLEAR".into(), p(".tags")],
            "$.productsById.*.tags",
            "[[1,2,3],[],[]]",
        ),
        (
            vec!["JSON.DEL".into(), sel.into()],
            "$.productsById.*.title",
            r#"["One"]"#,
        ),
    ];
    let mut v = V::new();
    for (args, verify, exp) in cases {
        v.ok(&["JSON.SET", "k1", "$", PRODUCTS]);
        let mut full = vec!["k1".to_string()];
        full.splice(0..0, [args[0].clone()]);
        full.extend(args[1..].iter().cloned());
        let refs: Vec<&str> = full.iter().map(String::as_str).collect();
        v.run(&refs);
        v.get(&["JSON.GET", "k1", verify], exp);
    }
    v.done("test_jsonpath_filter_object_children_mutation_commands");
}

#[test]
fn test_jsonpath_wildcard_filter_mutations() {
    let mut v = V::new();
    let inner = r#"{"a/b~c":{"a":1,"n":2},"keep":{"a":2,"n":3}}"#;
    let docs = [
        (
            format!(r#"[{{"group":{inner}}}]"#),
            r#"[{"group":{"keep":{"a":2,"n":3}}}]"#,
        ),
        (
            format!(r#"{{"outer":{{"group":{inner}}}}}"#),
            r#"{"outer":{"group":{"keep":{"a":2,"n":3}}}}"#,
        ),
    ];
    for wildcard in [".*", "[*]"] {
        for (doc, after) in &docs {
            let path = format!("${wildcard}{wildcard}[?(@.a==1)]");
            let n = format!("{path}.n");
            v.ok(&["JSON.SET", "k1", "$", doc]);
            v.get(&["JSON.GET", "k1", &path], r#"[{"a":1,"n":2}]"#);
            v.get(&["JSON.NUMINCRBY", "k1", &n, "5"], "[7]");
            v.get(&["JSON.GET", "k1", &n], "[7]");
            v.ok(&["JSON.SET", "k1", &n, "9"]);
            v.get(&["JSON.GET", "k1", &n], "[9]");
            v.int(&["JSON.DEL", "k1", &path], 1);
            v.get(&["JSON.GET", "k1"], after);
        }
    }
    v.done("test_jsonpath_wildcard_filter_mutations");
}

#[test]
fn test_jsonpath_wildcard_filter_no_match() {
    let mut v = V::new();
    for wildcard in [".*", "[*]"] {
        for doc in ["[]", "{}", "[[],{},1]", r#"[{"a":1}]"#, r#"{"b":{"a":1}}"#] {
            let path = format!("${wildcard}[?(@.a==1)]");
            v.run(&["JSON.SET", "k1", "$", doc]);
            v.get(&["JSON.GET", "k1", &path], "[]");
            v.run(&["JSON.SET", "k1", &path, r#"{"changed":true}"#]);
            v.int(&["JSON.DEL", "k1", &path], 0);
            v.get(&["JSON.NUMINCRBY", "k1", &format!("{path}.a"), "5"], "[]");
            v.get(&["JSON.GET", "k1"], doc);
        }
    }
    v.done("test_jsonpath_wildcard_filter_no_match");
}

#[test]
fn test_jsonpath_wildcard_malformed_filter_is_syntax_error() {
    let mut v = V::new();
    for doc in [
        "[]",
        "{}",
        "[[]]",
        r#"{"empty":{}}"#,
        "[null]",
        r#"{"child":null}"#,
    ] {
        v.run(&["JSON.SET", "k1", "$", doc]);
        for path in [
            "$.*[?(@.a==1]",
            "$[*][?(@.a==1]",
            "$.*[?(@.a==)]",
            "$[*][?(@.a==)]",
            "$.*[ ?(@.a==1]",
            "$[*][ ?(@.a==1]",
        ] {
            v.err(&["JSON.GET", "k1", path], &["SYNTAXERR"]);
        }
    }
    v.done("test_jsonpath_wildcard_malformed_filter_is_syntax_error");
}

#[test]
fn test_jsonpath_empty_fanout_checks_suffix_syntax() {
    let mut v = V::new();
    for prefix in [
        "$",
        "$[*]",
        "$[*][*]",
        "$[*].missing",
        "$[*][0]",
        "$[*][:]",
        "$[*][0:1]",
        "$[*][0,1]",
        "$[*]['a','b']",
        "$[*]..a",
        "$[?(@.a==1)]",
    ] {
        v.run(&["JSON.SET", "k1", "$", "[]"]);
        let path = format!("{prefix}[?(@.a==1)]");
        let bad = format!("{prefix}[?(@.a==1]");
        v.get(&["JSON.GET", "k1", &path], "[]");
        v.run(&["JSON.SET", "k1", &format!("{path}.added"), "1"]);
        v.int(&["JSON.DEL", "k1", &path], 0);
        v.get(&["JSON.GET", "k1"], "[]");
        v.err(&["JSON.GET", "k1", &bad], &["SYNTAXERR"]);
        v.err(&["JSON.SET", "k1", &bad, "1"], &["SYNTAXERR"]);
        v.err(&["JSON.NUMINCRBY", "k1", &bad, "1"], &["SYNTAXERR"]);
        v.get(&["JSON.GET", "k1"], "[]");
    }
    v.done("test_jsonpath_empty_fanout_checks_suffix_syntax");
}

#[test]
fn test_jsonpath_dot_sequence_is_syntax_error() {
    let mut v = V::new();
    for (doc, path) in [
        (r#"{"x":1}"#, "$...x"),
        (r#"[[{"a":1,"x":1}]]"#, "$[0][0]...x"),
        (r#"[[{"a":1,"x":1}]]"#, "$[*][?(@.a==1)]...x"),
        (r#"[[{"a":1,"x":1}]]"#, "$..a...x"),
        ("[]", "$[*]...a"),
    ] {
        v.run(&["JSON.SET", "k1", "$", doc]);
        let want = &["SYNTAXERR Invalid dot sequence"];
        v.err_has(&["JSON.GET", "k1", path], want);
        v.err_has(&["JSON.SET", "k1", path, "2"], want);
        v.err_has(&["JSON.DEL", "k1", path], want);
        v.get(&["JSON.GET", "k1"], doc);
    }
    v.done("test_jsonpath_dot_sequence_is_syntax_error");
}

#[test]
fn test_jsonpath_empty_fanout_rooted_comparison() {
    let mut v = V::new();
    for doc in [r#"{"limit":1,"groups":[]}"#, r#"{"limit":1,"groups":{}}"#] {
        v.run(&["JSON.SET", "k1", "$", doc]);
        v.get(&["JSON.GET", "k1", "$.groups[*][?(@.a==$.limit)]"], "[]");
        v.err(
            &["JSON.GET", "k1", "$.groups[*][?(@.a==$.limit]"],
            &["SYNTAXERR"],
        );
        v.err(
            &["JSON.GET", "k1", "$.groups[*][?(@.a==$.missing)]"],
            &["SYNTAXERR"],
        );
    }
    v.done("test_jsonpath_empty_fanout_rooted_comparison");
}

#[test]
fn test_jsonpath_wildcard_filter_scalar_and_mixed_attribute_candidates() {
    let mut v = V::new();
    let cases = [
        (
            r#"[1,"text",true,null]"#,
            "[]",
            r#"[1,"text",true,null]"#,
            0,
        ),
        (
            r#"[1,{"pick":{"a":1},"keep":2}]"#,
            r#"[{"a":1}]"#,
            r#"[1,{"keep":2}]"#,
            1,
        ),
        (
            r#"{"scalar":1,"group":{"pick":{"a":1},"keep":2}}"#,
            r#"[{"a":1}]"#,
            r#"{"scalar":1,"group":{"keep":2}}"#,
            1,
        ),
        (
            r#"[1,[{"a":1},{"b":2}]]"#,
            r#"[{"a":1}]"#,
            r#"[1,[{"b":2}]]"#,
            1,
        ),
    ];
    for wildcard in [".*", "[*]"] {
        for (doc, exp, after, count) in cases {
            let path = format!("${wildcard}[?(@.a)]");
            v.ok(&["JSON.SET", "k1", "$", doc]);
            v.get(&["JSON.GET", "k1", &path], exp);
            v.run(&["JSON.SET", "k1", &format!("{path}.a"), "9"]);
            v.get(
                &["JSON.GET", "k1", &path],
                if count > 0 { r#"[{"a":9}]"# } else { "[]" },
            );
            v.int(&["JSON.DEL", "k1", &path], count);
            v.get(&["JSON.GET", "k1"], after);
        }
    }
    v.done("test_jsonpath_wildcard_filter_scalar_and_mixed_attribute_candidates");
}

#[test]
fn test_jsonpath_wildcard_filter_rooted_comparison() {
    let mut v = V::new();
    for wildcard in [".*", "[*]"] {
        let path = format!("$.groups{wildcard}[ ?(@.a==$.limit)]");
        v.ok(&[
            "JSON.SET",
            "k1",
            "$",
            r#"{"limit":1,"groups":[[{"a":1}],[{"a":2}]]}"#,
        ]);
        v.get(&["JSON.GET", "k1", &path], r#"[{"a":1}]"#);
        v.int(&["JSON.DEL", "k1", &path], 1);
        v.get(
            &["JSON.GET", "k1"],
            r#"{"limit":1,"groups":[[],[{"a":2}]]}"#,
        );
    }
    v.done("test_jsonpath_wildcard_filter_rooted_comparison");
}

#[test]
fn test_jsonpath_wildcard_filter_recursive_suffix() {
    let mut v = V::new();
    let doc = format!(
        "[{}]",
        (0..20)
            .map(|i| format!(r#"[{{"a":1,"x":{i}}}]"#))
            .collect::<Vec<_>>()
            .join(",")
    );
    let want = format!(
        "[{}]",
        (0..20).map(|i| i.to_string()).collect::<Vec<_>>().join(",")
    );
    for wildcard in [".*", "[*]"] {
        v.ok(&["JSON.SET", "k1", "$", &doc]);
        v.get(
            &["JSON.GET", "k1", &format!("${wildcard}[?(@.a==1)]..x")],
            &want,
        );
    }
    v.done("test_jsonpath_wildcard_filter_recursive_suffix");
}

const FILTER_STORE: &str = r#"{"store":{"books":[
{"category":"reference","author":"Nigel Rees","title":"Sayings of the Century","price":8.95},
{"category":"fiction","author":"Evelyn Waugh","title":"Sword of Honour","price":12.99,
 "movies":[{"title":"Sword of Honour","realisator":{"first_name":"Bill","last_name":"Anderson"}}]},
{"category":"fiction","author":"Herman Melville","title":"Moby Dick","isbn":"0-553-21311-3","price":9},
{"category":"fiction","author":"J. R. R. Tolkien","title":"The Lord of the Rings","isbn":"0-395-19395-8","price":22.99}
],"bicycle":{"color":"red","price":19.95}}}"#;

#[test]
fn test_jsonpath_filter_expression() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "k1", ".", FILTER_STORE]);
    v.ok(&["JSON.SET", "k2", ".", "[1,2,3,4,5]"]);
    v.ok(&[
        "JSON.SET",
        "k3",
        ".",
        "[true,false,true,false,null,1,2,3,4]",
    ]);
    v.ok(&[
        "JSON.SET",
        "k4",
        ".",
        r#"{"books": [{"price":5,"sold":true,"in-stock":true,"title":"foo"}, {"price":15,"sold":false,"title":"abc"}]}"#,
    ]);
    v.ok(&["JSON.SET", "k5", ".", "[1,2,3,4,5,6,7,8,9]"]);
    let foo5 = r#"[{"price":5,"sold":true,"in-stock":true,"title":"foo"}]"#;
    let abc15 = r#"[{"price":15,"sold":false,"title":"abc"}]"#;
    for (key, path, exp) in [
        ("k1", "$.store.books[?(@.isbn)].price", "[9,22.99]"),
        ("k1", "$.store.books[?( @.isbn )].price", "[9,22.99]"),
        ("k1", r#"$.store.books[?(@["isbn"])]["price"]"#, "[9,22.99]"),
        (
            "k1",
            r#"$.store.books[?(@[ "isbn" ])][ "price" ]"#,
            "[9,22.99]",
        ),
        ("k1", "$.store.books[?(@['isbn'])]['price']", "[9,22.99]"),
        (
            "k1",
            r#"$.store.books[?(@.category == "reference")].price"#,
            "[8.95]",
        ),
        (
            "k1",
            r#"$.store.books[?(@.["category"] == "fiction")].price"#,
            "[12.99,9,22.99]",
        ),
        ("k1", "$.store.books[?(@.price<1.0E1)].price", "[8.95,9]"),
        (
            "k1",
            r#"$.store.books[?(@["price"]<1.0E1)]["price"]"#,
            "[8.95,9]",
        ),
        (
            "k1",
            r#"$.store.books[?(@.["price"]<1.0E1)]["price"]"#,
            "[8.95,9]",
        ),
        (
            "k1",
            "$.store.books[?(@['price']<1.0E1)]['price']",
            "[8.95,9]",
        ),
        (
            "k1",
            "$.store.books[?(@.price>-1.23e1&&@.price<1.0E1)].price",
            "[8.95,9]",
        ),
        (
            "k1",
            r#"$.store.books[?(@["price"]>-1.23e1&&@["price"]<1.0E1)]["price"]"#,
            "[8.95,9]",
        ),
        (
            "k1",
            r#"$.store.books[?(@.["price"]>-1.23e1&&@.["price"]<1.0E1)].["price"]"#,
            "[8.95,9]",
        ),
        (
            "k1",
            r#"$.store.books[?(@["price"] > -1.23e1 && @["price"] < 1.0E1)]["price"]"#,
            "[8.95,9]",
        ),
        (
            "k1",
            "$.store.books[?(@.price==22.99)].title",
            r#"["The Lord of the Rings"]"#,
        ),
        (
            "k1",
            r#"$.store.books[?(@["price"]==22.99)].title"#,
            r#"["The Lord of the Rings"]"#,
        ),
        ("k1", "$.store.books[?(@.price<10.0&&@.isbn)].price", "[9]"),
        (
            "k1",
            "$.store.books[?(@.price<9||@.price>20)].price",
            "[8.95,22.99]",
        ),
        (
            "k1",
            "$.store.books[?(@.price<9||@.price>10&&@.isbn)].price",
            "[8.95,22.99]",
        ),
        (
            "k1",
            "$.store.books[?((@.price<9||@.price>10)&&@.isbn)].price",
            "[22.99]",
        ),
        (
            "k1",
            "$.store.books[?((@.price < 9 || @.price>10) && @.isbn)].price",
            "[22.99]",
        ),
        (
            "k1",
            r#"$.store.books[?((@["price"]<9||@["price"]>10)&&@["isbn"])]["price"]"#,
            "[22.99]",
        ),
        (
            "k1",
            r#"$.store.books[?((@["price"] < 9 || @["price"] > 10) && @["isbn"])]["price"]"#,
            "[22.99]",
        ),
        ("k2", "$.*.[?(@>2)]", "[3,4,5]"),
        ("k2", "$.*.[?(@ > 2)]", "[3,4,5]"),
        ("k2", "$.*[?(@>2)]", "[3,4,5]"),
        ("k2", "$[*][?(@>2)]", "[3,4,5]"),
        ("k2", "$[ * ][?( @ > 2 )]", "[3,4,5]"),
        ("k2", "$.*[?(@ == 3)]", "[3]"),
        ("k2", "$.*[?(@ != 3)]", "[1,2,4,5]"),
        ("k3", "$.*.[?(@==true)]", "[true,true]"),
        ("k3", "$[*][?(@==true)]", "[true,true]"),
        ("k3", "$[*][?(@ == true)]", "[true,true]"),
        ("k3", "$.*.[?(@>1)]", "[2,3,4]"),
        ("k3", "$.*.[?( @ > 1 ) ]", "[2,3,4]"),
        ("k3", "$[*][?(@>1)]", "[2,3,4]"),
        ("k3", "$[ * ][?( @ > 1 ) ]", "[2,3,4]"),
        ("k4", "$.books[?(@.price>1&&@.price<20&&@.in-stock)]", foo5),
        (
            "k4",
            r#"$.books[?(@['price']>1&&@.price<20&&@["in-stock"])]"#,
            foo5,
        ),
        (
            "k4",
            "$.books[?((@.price>1&&@.price<20)&&(@.sold==false))]",
            abc15,
        ),
        (
            "k4",
            r#"$["books"][?((@["price"]>1&&@["price"]<20)&&(@["sold"]==false))]"#,
            abc15,
        ),
        ("k5", "$.*[?(@ > 7 || @ < 3)]", "[1,2,8,9]"),
        ("k5", "$.*[?(@ < 3 || @ > 7)]", "[1,2,8,9]"),
        ("k5", "$.*[?(@ > 3 && @ < 7)]", "[4,5,6]"),
    ] {
        v.get(&["JSON.GET", key, path], exp);
    }
    v.ok(&[
        "JSON.SET",
        "k1",
        "$.store.books[?(@.price<10.0)].price",
        "10.01",
    ]);
    for (key, path, exp) in [
        ("k1", "$.store.books[?(@.price<10.0)]", "[]"),
        (
            "k1",
            "$.store.books[?(@.price<=10.02)].price",
            "[10.01,10.01]",
        ),
        (
            "k1",
            "$.store.books[?(@.price <= 10.02)].price",
            "[10.01,10.01]",
        ),
        (
            "k1",
            "$.store.books[?(@.price==10.01)].title",
            r#"["Sayings of the Century","Moby Dick"]"#,
        ),
    ] {
        v.get(&["JSON.GET", key, path], exp);
    }
    for path in [
        "$.books[?((@.price>1&&@.price<20)&&(@.sold==false))].price",
        "$.books[?((@.price > 1 && @.price < 20) && (@.sold == false))].price",
        r#"$["books"][?((@["price"]>1&&@["price"]<20)&&(@["sold"]==false))]["price"]"#,
        r#"$["books"][?((@["price"] > 1 && @["price"] < 20) && (@["sold"] == false))]["price"]"#,
    ] {
        v.ok(&["JSON.SET", "k4", path, "13.13"]);
        v.get(&["JSON.GET", "k4", path], "[13.13]");
    }
    v.ok(&["JSON.SET", "k1", ".", FILTER_STORE]);
    v.ok(&["JSON.SET", "k2", ".", FILTER_STORE]);
    for (key, path, n) in [
        (
            "k1",
            r#"$.store.books[?(@.["category"] == "fiction")].price"#,
            3,
        ),
        (
            "k2",
            r#"$.store.books[?((@["price"] < 9 || @["price"] > 10) && @["isbn"])]["price"]"#,
            1,
        ),
    ] {
        v.int(&["JSON.DEL", key, path], n);
        v.get(&["JSON.GET", key, path], "[]");
    }
    v.done("test_jsonpath_filter_expression");
}

#[test]
fn test_jsonpath_recursive_descent() {
    let mut v = V::new();
    for (key, val) in [
        ("k1", r#"{"a":{"a":1}}"#),
        ("k2", r#"{"a":{"a":{"a":{"a":1}}}}"#),
        ("k3", r#"{"x": {}, "y": {"a":"a"}, "z": {"a":"", "b":"b"}}"#),
        (
            "k4",
            r#"{"a":{"b":{"z":{"y":1}}, "c":{"z":{"y":2}}, "z":{"y":3}}}"#,
        ),
        ("k5", r#"{"a":1, "b": {"e":[0,1,2]}, "c":{"e":[10,11,12]}}"#),
        ("k6", r#"{"a":[1], "b": {"a": [2,3]}, "c": {"a": [4,5,6]}}"#),
    ] {
        v.ok(&["JSON.SET", key, ".", val]);
    }
    for (key, path, exp) in [
        ("k1", "$..a", r#"[{"a":1},1]"#),
        (
            "k2",
            "$..a",
            r#"[{"a":{"a":{"a":1}}},{"a":{"a":1}},{"a":1},1]"#,
        ),
        ("k2", "$..a..a", r#"[{"a":{"a":1}},{"a":1},1]"#),
        ("k2", "$..a..a..a", r#"[{"a":1},1]"#),
        ("k2", "$..a..a..a..a", "[1]"),
        ("k2", "$..a..a..a..a..a", "[]"),
        ("k3", "$..a", r#"["a",""]"#),
        ("k4", "$.a..z.y", "[3,1,2]"),
        ("k4", "$.a..z.*", "[3,1,2]"),
        ("k4", "$.a.*..y", "[1,2,3]"),
        ("k5", "$..e.[*]", "[0,1,2,10,11,12]"),
        ("k5", "$..e[1]", "[1,11]"),
        ("k5", "$..e.[1]", "[1,11]"),
        ("k5", "$..e[0:2]", "[0,1,10,11]"),
        ("k5", r#"$..["e"][1]"#, "[1,11]"),
        ("k5", r#"$..["e"][0:2]"#, "[0,1,10,11]"),
        ("k5", r#"$..[ "e" ][ 0 : 2 ]"#, "[0,1,10,11]"),
    ] {
        v.get(&["JSON.GET", key, path], exp);
    }
    v.ints(&["JSON.ARRAPPEND", "k6", "$..a", "0"], &[2, 3, 4]);
    v.get(
        &["JSON.GET", "k6"],
        r#"{"a":[1,0],"b":{"a":[2,3,0]},"c":{"a":[4,5,6,0]}}"#,
    );
    v.int(&["JSON.DEL", "k3", "$..a"], 2);
    v.get(&["JSON.GET", "k3"], r#"{"x":{},"y":{},"z":{"b":"b"}}"#);
    v.int(&["JSON.DEL", "k2", "$..a"], 4);
    v.get(&["JSON.GET", "k2"], "{}");
    v.done("test_jsonpath_recursive_descent");
}

#[test]
fn test_jsonpath_recursive_insert_update_delete() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "store", ".", STORE]);
    v.ok(&[
        "JSON.SET",
        "store2",
        ".",
        r#"{"store":{"title":"foo","bicycle":{"title":"foo2","color":"red","price":19.64,"in-stock":true}}}"#,
    ]);
    v.int(&["JSON.DEL", "store2", "$..title"], 2);
    v.get(&["JSON.GET", "store2", "$..title"], "[]");
    v.get(
        &["JSON.GET", "store2"],
        r#"{"store":{"bicycle":{"color":"red","price":19.64,"in-stock":true}}}"#,
    );
    v.get(
        &["JSON.GET", "store", "$..title"],
        r#"["Sayings of the Century","Sword of Honour","Sword of Honour - movie","Moby Dick","The Lord of the Rings","The Elements of Style","Anna Karenina","The World Almanac and Book of Facts 2021","Manual for Writers of Research Papers"]"#,
    );
    v.ok(&["JSON.SET", "store", "$..title", r#""foo""#]);
    v.get(
        &["JSON.GET", "store", "$..title"],
        r#"["foo","foo","foo","foo","foo","foo","foo","foo","foo"]"#,
    );
    for (path, exp) in [
        ("$.title", "[]"),
        ("$.store.title", "[]"),
        ("$.store.bicycle.title", "[]"),
        ("$.store.books[1].title", r#"["foo"]"#),
        ("$.store.books[1].movies[0].title", r#"["foo"]"#),
        ("$.store.books[1].movies[0].realisator.title", "[]"),
    ] {
        v.get(&["JSON.GET", "store", path], exp);
    }
    v.int(&["JSON.DEL", "store", "$..title"], 9);
    v.get(&["JSON.GET", "store", "$..title"], "[]");
    v.done("test_jsonpath_recursive_insert_update_delete");
}

#[test]
fn test_jsonpath_recursive_insert_update_delete2() {
    let mut v = V::new();
    let doc = r#"{"input":{"a":1,"b":{"e":[0,1,2]},"c":{"e":[10,11,12]}}}"#;
    v.ok(&["JSON.SET", "input", ".", doc]);
    v.ok(&["JSON.SET", "input2", ".", doc]);
    v.int(&["JSON.DEL", "input2", "$..e"], 2);
    v.get(&["JSON.GET", "input2", "$..e"], "[]");
    v.get(
        &["JSON.GET", "input2"],
        r#"{"input":{"a":1,"b":{},"c":{}}}"#,
    );
    v.get(&["JSON.GET", "input", "$..e[*]"], "[0,1,2,10,11,12]");
    v.ok(&["JSON.SET", "input", "$..e[*]", "4"]);
    v.get(&["JSON.GET", "input", "$..e[*]"], "[4,4,4,4,4,4]");
    v.get(&["JSON.GET", "input", "$.input.b.e[*]"], "[4,4,4]");
    for (path, exp) in [
        ("$.e", "[]"),
        ("$.input.e", "[]"),
        ("$.input.a.e", "[]"),
        ("$.input.b.e[*]", "[4,4,4]"),
        ("$.input.c.e[*]", "[4,4,4]"),
    ] {
        v.get(&["JSON.GET", "input", path], exp);
    }
    v.int(&["JSON.DEL", "input", "$..e"], 2);
    v.int(&["JSON.DEL", "input", "$..e"], 0);
    v.get(&["JSON.GET", "input", "$..e"], "[]");
    v.done("test_jsonpath_recursive_insert_update_delete2");
}

#[test]
fn test_jsonpath_compatibility_invalid_array_index() {
    let mut v = V::new();
    for path in [
        ".phoneNumbers[]",
        ".phoneNumbers[x]",
        "$.phoneNumbers[]",
        "$.phoneNumbers[x]",
    ] {
        v.err(&["JSON.GET", "wikipedia", path], SYNTAX);
    }
    v.done("test_jsonpath_compatibility_invalidArrayIndex");
}

#[test]
fn test_jsonpath_compatibility_unquoted_member_name() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{
            "%x22key%x22":1, "+2":2, "-3":3, "/4":4,
            ")5":5, "6)":6, "(7":7, "8(":8,
            "]9":9, "10]":10, "[11":11, "12[":12,
            ">13":13, "14>":14, "<15":15, "16<":16,
            "=17":17, "18=":18, "!19":19, "20!":20,
            "21.21":21
        }"#,
    ]);
    for (path, exp) in [
        ("$.%x22key%x22", "[1]"),
        ("$.+2", "[2]"),
        ("$.-3", "[3]"),
        ("$./4", "[4]"),
        ("$.6)", "[]"),
        ("$.8(", "[]"),
        ("$.10]", "[]"),
        (r#"$["6)"]"#, "[6]"),
        (r#"$["8("]"#, "[8]"),
        (r#"$["10]"]"#, "[10]"),
        (r#"$["12["]"#, "[12]"),
        (r#"$["14>"]"#, "[14]"),
        (r#"$["16<"]"#, "[16]"),
        (r#"$["18="]"#, "[18]"),
        (r#"$["20!"]"#, "[20]"),
        (r#"$["21.21"]"#, "[21]"),
        ("$.12[", "[]"),
        ("$.14>", "[]"),
        ("$.16<", "[]"),
        ("$.18=", "[]"),
        ("$.20!", "[]"),
        ("$.21.21", "[]"),
    ] {
        v.get(&["JSON.GET", "k1", path], exp);
    }
    for path in [
        "$.)5", "$.]7", "$.]9", "$.[11", "$.>13", "$.<15", "$.=17", "$.!19",
    ] {
        v.err(&["JSON.GET", "k1", path], SYNTAX);
    }
    v.done("test_jsonpath_compatibility_unquotedMemberName");
}

#[test]
fn test_write_commands_duplicate_values() {
    let mut v = V::new();
    let ten = "[0,1,2,3,4,5,6,7,8,9]";
    let nested = "[[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]";
    for (key, val) in [
        ("k1", ten),
        ("k2", ten),
        ("k3", ten),
        ("k4", r#"["0","1","2","3","4","5","6","7","8","9"]"#),
        ("k5", nested),
        ("k6", "[true,true,true,true,true]"),
        ("k7", nested),
        ("k8", nested),
        ("k9", nested),
        ("k10", "[[0,1,2,3,4],[0,1,2,3,4],[0,1,2,3,4]]"),
        ("k11", ten),
        ("k12", ten),
    ] {
        v.ok(&["JSON.SET", key, ".", val]);
    }
    v.get(&["JSON.NUMINCRBY", "k1", "$[0,0,0,0,0]", "10"], "[10]");
    v.get(&["JSON.GET", "k1"], "[10,1,2,3,4,5,6,7,8,9]");
    v.get(&["JSON.NUMMULTBY", "k2", "$[9,9,9,9,9]", "2"], "[18]");
    v.get(&["JSON.GET", "k2"], "[0,1,2,3,4,5,6,7,8,18]");
    v.int(&["JSON.CLEAR", "k5", "$[0,0,0,0,0]"], 1);
    v.get(
        &["JSON.GET", "k5"],
        "[[],[1],[2],[3],[4],[5],[6],[7],[8],[9]]",
    );
    v.ints(&["JSON.TOGGLE", "k6", "$[0,0,0,0]"], &[0]);
    v.get(&["JSON.GET", "k6"], "[false,true,true,true,true]");
    v.ints(&["JSON.STRAPPEND", "k4", "$[0,0,0,0,0]", r#""foo""#], &[4]);
    v.get(
        &["JSON.GET", "k4"],
        r#"["0foo","1","2","3","4","5","6","7","8","9"]"#,
    );
    v.ints(&["JSON.ARRAPPEND", "k7", "$[0,0,0,0,0]", "8", "9"], &[3]);
    v.get(
        &["JSON.GET", "k7"],
        "[[0,8,9],[1],[2],[3],[4],[5],[6],[7],[8],[9]]",
    );
    v.ints(&["JSON.ARRINSERT", "k8", "$[0,0,0,0,0]", "0", "9"], &[2]);
    v.get(
        &["JSON.GET", "k8"],
        "[[9,0],[1],[2],[3],[4],[5],[6],[7],[8],[9]]",
    );
    v.arr(&["JSON.ARRPOP", "k9", "$[0,0,0,0,0]"], &["0"]);
    v.get(
        &["JSON.GET", "k9"],
        "[[],[1],[2],[3],[4],[5],[6],[7],[8],[9]]",
    );
    v.ints(&["JSON.ARRTRIM", "k10", "$[0,0,0,0,0]", "0", "1"], &[2]);
    v.get(&["JSON.GET", "k10"], "[[0,1],[0,1,2,3,4],[0,1,2,3,4]]");
    v.int(&["JSON.DEL", "k3", "$[0,0,0,0,0]"], 1);
    v.get(&["JSON.GET", "k3"], "[1,2,3,4,5,6,7,8,9]");
    v.int(&["JSON.DEL", "k12", "$[1,4,7,0,0,3,3]"], 5);
    v.get(&["JSON.GET", "k12"], "[2,5,6,8,9]");
    v.ok(&["JSON.SET", "k11", "$[0,0,0,0,0]", "9"]);
    v.get(&["JSON.GET", "k11"], "[9,1,2,3,4,5,6,7,8,9]");
    v.done("test_write_commands_duplicate_values");
}

/// Upstream's `DATA_ORGANISM`, without its indentation.
const ORGANISM: &str = r#"{"heavy_animal":200,
"plants":[{"name":"Cactus","height":120,"weight":90},
 {"name":"Ghost Plant","height":"Unknown","weight":"Unknown"},
 {"name":"Redwood","height":4200,"weight":50000}],
"animals":[{"name":"Platypus","length":24,"weight":5},
 {"fish":[{"name":"Bass","length":34,"weight":5},{"name":"Swordfish","length":177,"weight":200}]},
 {"mammals":[{"name":"Platypus","length":24,"weight":5},
   {"name":"Horse","height":68,"weight":660},
   {"primates":[{"name":"Monkey","height":18,"weight":30},
     {"name":"Baboon","height":26,"weight":50},
     {"apes":[{"name":"Chimpanzee","height":66,"weight":130},
       {"name":"Gorilla","height":66,"weight":400},
       {"name":"Orangutan","height":70,"weight":300}]}]}]}]}"#;

#[test]
fn test_jsonpath_compatibility_filter_on_object_and_string_comparison() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{"key for key":"key inside here",
"an object":{"weight":300,"a value":300,"my key":"key inside here"},
"another object":{"weight":400,"a value":400,"my key":"key inside there"},
"objects":[
 {"weight":100,"a value":100,"my key":"key inside here"},
 {"weight":200,"a value":200,"my key":"key inside there"},
 {"weight":300,"a value":300,"my key":"key inside here"},
 {"weight":400,"a value":400,"my key":"key inside there"}]}"#,
    ]);
    for (path, exp) in [
        (r#"$["an object"].[?(@.weight > 200)].["a value"]"#, "[]"),
        (r#"$["an object"].[?(@.weight == 300)].["a value"]"#, "[]"),
        (r#"$["an object"].[?(@.weight > 300)].["a value"]"#, "[]"),
        (
            r#"$["another object"].[?(@["a value"] > 200)].weight"#,
            "[]",
        ),
        (
            r#"$["another object"].[?(@.["a value"] > 200)].weight"#,
            "[]",
        ),
        (
            r#"$["another object"].[?(@.["my key"] == "key inside there")].weight"#,
            "[]",
        ),
        (
            r#"$["objects"].[?(@.weight > 200)].["a value"]"#,
            "[300,400]",
        ),
        (
            r#"$["objects"].[?(@.["my key"] == "key inside there")].weight"#,
            "[200,400]",
        ),
        (
            r#"$["objects"].[?(@.["my key"] != "key inside there")].weight"#,
            "[100,300]",
        ),
        (
            r#"$["objects"].[?(@.["my key"] == "key inside here")].weight"#,
            "[100,300]",
        ),
        (
            r#"$["objects"].[?(@.["my key"] != "key inside here")].weight"#,
            "[200,400]",
        ),
        (
            r#"$["objects"].[?(@.["my key"] <= "key inside here")].weight"#,
            "[100,300]",
        ),
        (
            r#"$["objects"].[?(@.["my key"] >= "key inside here")].weight"#,
            "[100,200,300,400]",
        ),
        (
            r#"$["objects"].[?(@.["my key"] < "key inside herf")].weight"#,
            "[100,300]",
        ),
        (
            r#"$["objects"].[?(@.["my key"] > "key insidd here")].weight"#,
            "[100,200,300,400]",
        ),
        (r#"$["key for key"].[?(@.["a value"] > 200)]"#, "[]"),
    ] {
        v.get(&["JSON.GET", "k1", path], exp);
    }
    v.done("test_jsonpath_compatibility_filterOnObject_and_stringComparison");
}

#[test]
fn test_jsonpath_compatibility_union_of_object_members() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "organism", ".", ORGANISM]);
    for (key, path, exp) in [
        (
            "wikipedia",
            r#"$.["firstName","lastName"]"#,
            r#"["John","Smith"]"#,
        ),
        ("organism", r#"$.animals["2","2"].mammals..weight"#, "[]"),
        ("organism", r#"$.animals["2","Junk"]"#, "[]"),
        ("organism", r#"$.animals["Junk","Junk"]"#, "[]"),
        (
            "organism",
            "$..[?(@.weight>400)].name",
            r#"["Redwood","Horse"]"#,
        ),
        (
            "organism",
            r#"$..[?(@.weight>=60&&@.name=="Chimpanzee")].name"#,
            r#"["Chimpanzee"]"#,
        ),
        (
            "wikipedia",
            r#"$.[ "firstName", "lastName" ]"#,
            r#"["John","Smith"]"#,
        ),
        (
            "wikipedia",
            r#"$.address.[ "street", "city", "state", "zipcode" ]"#,
            r#"["21 2nd Street","New York","NY","10021-3100"]"#,
        ),
    ] {
        v.get(&["JSON.GET", key, path], exp);
    }
    v.done("test_jsonpath_compatibility_union_of_object_members");
}

#[test]
fn test_jsonpath_malformed_path() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{"store":{"book":[{"price":5,"sold":true,"in-stock":true,"title":"foo","author":"me","isbn":"978-3-16-148410-0"}]}}"#,
    ]);
    v.ok(&["JSON.SET", "k2", ".", "[1,2,3,4,5,6,7,8,9,10]"]);
    for (key, path) in [
        ("k1", "$[0:2]$[0:1]$[0:2]$[0:2]$[0<2065>:2]$[0:2]"),
        ("k1", "$[0,1]"),
        ("k2", r#"$.["a"].["b"].["c"]"#),
        ("k1", "$a.b.c.d"),
        ("k2", "$a$b$c$d"),
    ] {
        v.get(&["JSON.GET", key, path], "[]");
    }
    for path in [
        ".[0:2].[0:1].[0:2].[0:2].[0<2065>:2].[0:2]",
        ".[0:2]$[0:1]$[0:2]$[0:2]$[0<2065>:2]$[0:2]",
    ] {
        v.err(&["JSON.GET", "k1", path], &["WRONGTYPE", "NONEXISTENT"]);
    }
    for path in [
        "&&$.store..price",
        "!$.store..price",
        "=$.store..price",
        "=.store..price",
        "||.store..price",
    ] {
        v.err(&["JSON.GET", "k1", path], SYNTAX);
        v.err(&["JSON.SET", "k1", path, "0"], SYNTAX);
    }
    v.done("test_jsonpath_malformed_path");
}

#[test]
fn test_v2_path_limit_recursive_descent() {
    let mut v = V::new();
    let depth_limit = 10;
    v.ok(&["CONFIG", "SET", "json.max-path-limit", "10"]);
    v.ok(&["JSON.SET", "k1", "$", r#"{"a":0}"#]);
    for _ in 0..depth_limit - 1 {
        v.run(&["JSON.SET", "k1", "$..a", r#"{"a":0}"#]);
    }
    v.int(&["JSON.DEBUG", "DEPTH", "k1"], depth_limit);
    v.err(&["JSON.SET", "k1", "$..a", r#"{"a":0}"#], LIMIT);
    v.done("test_v2_path_limit_recursive_descent");
}

#[test]
fn test_v2_path_limit_insert_member() {
    let mut v = V::new();
    v.ok(&["CONFIG", "SET", "json.max-path-limit", "3"]);
    v.ok(&["JSON.SET", "k1", "$", r#"{"a":{"a":{"a":0}}}"#]);
    v.int(&["JSON.DEBUG", "DEPTH", "k1"], 3);
    v.err(&["JSON.SET", "k1", "$.a.a.b", r#"{"b":0}"#], LIMIT);
    v.ok(&["CONFIG", "SET", "json.max-path-limit", "4"]);
    v.ok(&["JSON.SET", "k1", "$.a.a.b", r#"{"b":0}"#]);
    v.done("test_v2_path_limit_insert_member");
}

#[test]
fn test_debug_command_getkeys_api() {
    let mut v = V::new();
    for sub in ["memory", "fields", "depth", "memory"] {
        v.arr(&["COMMAND", "GETKEYS", "json.debug", sub, "k1"], &["k1"]);
    }
    for sub in [
        "",
        "memory",
        "mem",
        "memofieldsdepthhelp",
        "max-depth-key",
        "max-size-key",
    ] {
        let mut args = vec!["COMMAND", "GETKEYS", "json.debug"];
        if !sub.is_empty() {
            args.push(sub);
        }
        v.err_has(
            &args,
            &[
                "wrong number of arguments",
                "invalid number of arguments",
                "invalid command",
                "the command has no key arguments",
            ],
        );
    }
    v.done("test_debug_command_getkeys_api");
}

#[test]
fn test_debug_command_depth() {
    let mut v = V::new();
    for (key, val, depth) in [
        ("k1", "1", 0),
        ("k2", r#""a""#, 0),
        ("k3", "null", 0),
        ("k4", "true", 0),
        ("k5", "{}", 0),
        ("k6", r#"{"a":0}"#, 1),
        ("k7", r#"{"a":{"a":0}}"#, 2),
        ("k7", r#"{"a":{"a":{"a":0}}}"#, 3),
        ("k8", "[]", 0),
        ("k9", "[0]", 1),
        ("k10", "[[0]]", 2),
        ("k11", "[[0],[[0]]]", 3),
    ] {
        v.ok(&["JSON.SET", key, "$", val]);
        v.int(&["JSON.DEBUG", "DEPTH", key], depth);
    }
    v.nil(&["JSON.DEBUG", "DEPTH", "foobar"]);
    v.done("test_debug_command_depth");
}

#[test]
fn test_insert_update_delete_mode() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "organism", ".", ORGANISM]);
    v.ok(&["JSON.SET", "organism2", ".", ORGANISM]);
    let path = "$.animals[*].mammals[*].primates[*].apes[?(@.weight<400)].weight";
    v.get(&["JSON.GET", "organism", path], "[130,300]");
    v.int(&["JSON.DEL", "organism", path], 2);
    v.get(&["JSON.GET", "organism", path], "[]");
    v.ok(&["JSON.SET", "organism", path, "25"]);
    v.get(&["JSON.GET", "organism", path], "[]");
    v.get(&["JSON.GET", "organism2", path], "[130,300]");
    v.ok(&["JSON.SET", "organism2", path, "25"]);
    v.get(&["JSON.GET", "organism2", path], "[25,25]");
    v.int(&["JSON.DEL", "organism2", path], 2);
    v.get(&["JSON.GET", "organism2", path], "[]");
    v.done("test_insert_update_delete_mode");
}

impl V {
    /// A bulk reply that contains, for every group, at least one of its
    /// alternatives as a substring (upstream's `b'x' in result` checks).
    fn has(&mut self, args: &[&str], groups: &[&[&str]]) {
        self.vectors += 1;
        let got = self.run(args);
        let ok = match &got {
            Reply::Bulk(b) => {
                let s = String::from_utf8_lossy(b);
                groups.iter().all(|g| g.iter().any(|alt| s.contains(alt)))
            }
            _ => false,
        };
        if !ok {
            self.fail(args, &got, &format!("a bulk containing {groups:?}"));
        }
    }

    /// A bulk reply that contains none of `needles`.
    fn lacks(&mut self, args: &[&str], needles: &[&str]) {
        self.vectors += 1;
        let got = self.run(args);
        let ok = match &got {
            Reply::Bulk(b) => {
                let s = String::from_utf8_lossy(b);
                needles.iter().all(|n| !s.contains(n))
            }
            _ => false,
        };
        if !ok {
            self.fail(args, &got, &format!("a bulk without {needles:?}"));
        }
    }
}

#[test]
fn test_json_merge_command_basic() {
    let mut v = V::new();
    v.ok(&["JSON.MERGE", "merge_test", ".", r#"{"a":1,"b":2}"#]);
    v.get(&["JSON.GET", "merge_test"], r#"{"a":1,"b":2}"#);
    v.ok(&["JSON.MERGE", "merge_test", ".", r#"{"b":3,"c":4}"#]);
    v.get(&["JSON.GET", "merge_test"], r#"{"a":1,"b":3,"c":4}"#);
    v.done("test_json_merge_command_basic");
}

#[test]
fn test_json_merge_command_nested_objects() {
    let mut v = V::new();
    let k = "merge_nested";
    v.ok(&[
        "JSON.MERGE",
        k,
        ".",
        r#"{"user":{"name":"John","age":30},"city":"NYC"}"#,
    ]);
    v.ok(&[
        "JSON.MERGE",
        k,
        ".",
        r#"{"user":{"age":31,"email":"john@example.com"},"country":"USA"}"#,
    ]);
    v.get(&["JSON.GET", k, ".user.name"], r#""John""#);
    v.get(&["JSON.GET", k, ".user.age"], "31");
    v.get(&["JSON.GET", k, ".user.email"], r#""john@example.com""#);
    v.get(&["JSON.GET", k, ".city"], r#""NYC""#);
    v.get(&["JSON.GET", k, ".country"], r#""USA""#);
    v.done("test_json_merge_command_nested_objects");
}

#[test]
fn test_json_merge_command_path_merge() {
    let mut v = V::new();
    let k = "merge_path";
    v.ok(&["JSON.SET", k, ".", r#"{"a":{"x":1,"y":2},"b":10}"#]);
    v.ok(&["JSON.MERGE", k, ".a", r#"{"y":3,"z":4}"#]);
    v.get(&["JSON.GET", k, ".a"], r#"{"x":1,"y":3,"z":4}"#);
    v.get(&["JSON.GET", k, ".b"], "10");
    v.done("test_json_merge_command_path_merge");
}

#[test]
fn test_json_merge_command_create_new_key() {
    let mut v = V::new();
    v.ok(&["JSON.MERGE", "merge_new_key", ".", r#"{"a":1,"b":2}"#]);
    v.get(&["JSON.GET", "merge_new_key"], r#"{"a":1,"b":2}"#);
    v.done("test_json_merge_command_create_new_key");
}

#[test]
fn test_json_merge_command_replace_non_object() {
    let mut v = V::new();
    let k = "merge_replace";
    v.ok(&["JSON.SET", k, ".", r#"{"a":1,"b":"string","c":true}"#]);
    v.ok(&["JSON.MERGE", k, ".b", r#"{"nested":"object"}"#]);
    v.get(&["JSON.GET", k, ".b"], r#"{"nested":"object"}"#);
    v.ok(&["JSON.MERGE", k, ".c", r#"{"another":"object"}"#]);
    v.get(&["JSON.GET", k, ".c"], r#"{"another":"object"}"#);
    v.done("test_json_merge_command_replace_non_object");
}

#[test]
fn test_json_merge_command_deep_nesting() {
    let mut v = V::new();
    let k = "merge_deep";
    v.ok(&[
        "JSON.MERGE",
        k,
        ".",
        r#"{"level1":{"level2":{"level3":{"a":1}}}}"#,
    ]);
    v.ok(&[
        "JSON.MERGE",
        k,
        ".",
        r#"{"level1":{"level2":{"level3":{"b":2}}}}"#,
    ]);
    v.get(
        &["JSON.GET", k, ".level1.level2.level3"],
        r#"{"a":1,"b":2}"#,
    );
    v.done("test_json_merge_command_deep_nesting");
}

#[test]
fn test_json_merge_command_root_path_merge() {
    let mut v = V::new();
    let k = "merge_root";
    v.ok(&["JSON.SET", k, ".", r#"{"a":1,"b":2}"#]);
    v.ok(&["JSON.MERGE", k, ".", r#"{"b":3,"c":4}"#]);
    v.get(&["JSON.GET", k], r#"{"a":1,"b":3,"c":4}"#);
    v.done("test_json_merge_command_root_path_merge");
}

#[test]
fn test_json_merge_command_error_conditions() {
    let mut v = V::new();
    v.err(
        &["JSON.MERGE", "merge_error", ".invalid.path", r#"{"a":1}"#],
        &[""],
    );
    v.err(&["JSON.MERGE", "merge_error", ".", "invalid json"], &[""]);
    v.run(&["SET", "string_key", "not_json"]);
    v.err(&["JSON.MERGE", "string_key", ".", r#"{"a":1}"#], &[""]);
    v.done("test_json_merge_command_error_conditions");
}

#[test]
fn test_json_merge_command_with_arrays() {
    let mut v = V::new();
    let k = "merge_array";
    v.ok(&["JSON.SET", k, ".", r#"{"arr":[1,2,3]}"#]);
    v.ok(&["JSON.MERGE", k, ".arr", r#"{"merged":"object"}"#]);
    v.get(&["JSON.GET", k, ".arr"], r#"{"merged":"object"}"#);
    v.done("test_json_merge_command_with_arrays");
}

#[test]
fn test_json_merge_command_add_new_child() {
    let mut v = V::new();
    let k = "merge_new_child";
    v.ok(&["JSON.SET", k, ".", r#"{"a":{"b":{"c":1}}}"#]);
    v.ok(&["JSON.MERGE", k, ".a.b.d", r#"{"new":"value"}"#]);
    v.get(&["JSON.GET", k, ".a.b.d"], r#"{"new":"value"}"#);
    v.get(&["JSON.GET", k, ".a.b.c"], "1");
    v.ok(&["JSON.MERGE", k, ".a.b.d", r#"{"another":"field"}"#]);
    v.has(
        &["JSON.GET", k, ".a.b.d"],
        &[&["new"], &["value"], &["another"], &["field"]],
    );
    v.done("test_json_merge_command_add_new_child");
}

#[test]
fn test_json_merge_command_null_deletes_key() {
    let mut v = V::new();
    let k = "merge_null";
    v.ok(&["JSON.SET", k, ".", r#"{"a":1,"b":2,"c":3}"#]);
    v.ok(&["JSON.MERGE", k, ".", r#"{"b":null}"#]);
    v.lacks(&["JSON.GET", k], &[r#""b""#]);
    v.has(
        &["JSON.GET", k],
        &[&[r#""a":1"#, r#""a": 1"#], &[r#""c":3"#, r#""c": 3"#]],
    );
    v.ok(&[
        "JSON.SET",
        k,
        ".",
        r#"{"user":{"name":"John","age":30,"email":"test@example.com"}}"#,
    ]);
    v.ok(&["JSON.MERGE", k, ".", r#"{"user":{"age":null}}"#]);
    v.lacks(&["JSON.GET", k, ".user"], &[r#""age""#]);
    v.has(
        &["JSON.GET", k, ".user"],
        &[&[r#""name""#], &[r#""email""#]],
    );
    v.ok(&["JSON.SET", k, ".", r#"{"a":1}"#]);
    v.ok(&["JSON.MERGE", k, ".", r#"{"b":null,"c":3}"#]);
    v.lacks(&["JSON.GET", k], &[r#""b""#]);
    v.has(&["JSON.GET", k], &[&[r#""c":3"#, r#""c": 3"#]]);
    v.done("test_json_merge_command_null_deletes_key");
}

#[test]
fn test_json_merge_command_value_rules() {
    let mut v = V::new();
    let k = "merge_values";
    v.ok(&[
        "JSON.SET",
        k,
        ".",
        r#"{"str":"hello","num":42,"arr":[1,2,3],"obj":{"x":1}}"#,
    ]);
    v.ok(&[
        "JSON.MERGE",
        k,
        ".",
        r#"{"str":"world","num":99,"arr":[4,5],"obj":{"y":2},"new":"added"}"#,
    ]);
    v.get(&["JSON.GET", k, ".str"], r#""world""#);
    v.get(&["JSON.GET", k, ".num"], "99");
    v.get(&["JSON.GET", k, ".arr"], "[4,5]");
    v.has(
        &["JSON.GET", k, ".obj"],
        &[&[r#""x":1"#, r#""x": 1"#], &[r#""y":2"#, r#""y": 2"#]],
    );
    v.get(&["JSON.GET", k, ".new"], r#""added""#);
    v.done("test_json_merge_command_value_rules");
}

#[test]
fn test_json_merge_command_wildcards_multiple_targets() {
    let mut v = V::new();
    let k = "merge_wildcards";
    v.ok(&[
        "JSON.SET",
        k,
        ".",
        r#"{"users":[{"name":"Alice","age":25,"meta":{"active":true}},{"name":"Bob","age":30,"meta":{"active":false}}],"items":[{"id":1,"price":10},{"id":2,"price":20}]}"#,
    ]);
    v.ok(&[
        "JSON.MERGE",
        k,
        "$..meta",
        r#"{"active":true,"updated":true}"#,
    ]);
    for path in [".users[0].meta", ".users[1].meta"] {
        v.has(
            &["JSON.GET", k, path],
            &[
                &[r#""active":true"#, r#""active": true"#],
                &[r#""updated":true"#, r#""updated": true"#],
            ],
        );
    }
    v.ok(&["JSON.MERGE", k, "$..age", "31"]);
    v.get(&["JSON.GET", k, ".users[0].age"], "31");
    v.get(&["JSON.GET", k, ".users[1].age"], "31");
    v.ok(&["JSON.MERGE", k, "$..price", "15"]);
    v.get(&["JSON.GET", k, ".items[0].price"], "15");
    v.get(&["JSON.GET", k, ".items[1].price"], "15");
    v.done("test_json_merge_command_wildcards_multiple_targets");
}

#[test]
fn test_json_merge_command_mixed_updates_inserts() {
    let mut v = V::new();
    let k = "merge_mixed";
    v.ok(&[
        "JSON.SET",
        k,
        ".",
        r#"{"users":[{"id":1,"name":"Alice","profile":{"email":"alice@example.com"}},{"id":2,"name":"Bob","profile":{"email":"bob@example.com"}}]}"#,
    ]);
    v.ok(&[
        "JSON.MERGE",
        k,
        ".users[*].profile",
        r#"{"email":"updated@example.com","phone":"123-456-7890"}"#,
    ]);
    for path in [".users[0].profile", ".users[1].profile"] {
        v.has(
            &["JSON.GET", k, path],
            &[
                &[
                    r#""email":"updated@example.com""#,
                    r#""email": "updated@example.com""#,
                ],
                &[r#""phone":"123-456-7890""#, r#""phone": "123-456-7890""#],
            ],
        );
    }
    v.get(&["JSON.GET", k, ".users[1].name"], r#""Bob""#);
    v.get(&["JSON.GET", k, ".users[1].id"], "2");
    v.ok(&[
        "JSON.SET",
        k,
        ".",
        r#"{"user1":{"name":"Alice","profile":{"email":"alice@example.com"}},"user2":{"name":"Bob"}}"#,
    ]);
    v.ok(&[
        "JSON.MERGE",
        k,
        ".user1.profile",
        r#"{"email":"updated1@example.com","phone":"111-111-1111"}"#,
    ]);
    v.ok(&[
        "JSON.MERGE",
        k,
        ".user2.profile",
        r#"{"email":"new@example.com","phone":"222-222-2222"}"#,
    ]);
    v.has(
        &["JSON.GET", k, ".user1.profile"],
        &[&[
            r#""email":"updated1@example.com""#,
            r#""email": "updated1@example.com""#,
        ]],
    );
    v.has(
        &["JSON.GET", k, ".user2.profile"],
        &[&[
            r#""email":"new@example.com""#,
            r#""email": "new@example.com""#,
        ]],
    );
    v.done("test_json_merge_command_mixed_updates_inserts");
}

#[test]
fn test_json_merge_command_updates_and_inserts_together() {
    let mut v = V::new();
    let k = "merge_updates_inserts";
    v.ok(&[
        "JSON.SET",
        k,
        ".",
        r#"{"users":[{"id":1,"profile":{"email":"a@x.com"}},{"id":2}]}"#,
    ]);
    v.ok(&["JSON.MERGE", k, ".users[*].profile", r#"{"phone":"123"}"#]);
    v.has(
        &["JSON.GET", k, ".users[0].profile"],
        &[
            &[r#""email":"a@x.com""#, r#""email": "a@x.com""#],
            &[r#""phone":"123""#, r#""phone": "123""#],
        ],
    );
    v.has(
        &["JSON.GET", k, ".users[1].profile"],
        &[&[r#""phone":"123""#, r#""phone": "123""#]],
    );
    v.get(&["JSON.GET", k, ".users[1].id"], "2");
    v.done("test_json_merge_command_updates_and_inserts_together");
}

#[test]
fn test_json_merge_command_non_root_path_new_key_error() {
    let mut v = V::new();
    let msg = "SYNTAXERR A new Valkey key's path must be root";
    v.errfull(
        &[
            "JSON.MERGE",
            "merge_new_key_error",
            ".user",
            r#"{"name":"John"}"#,
        ],
        msg,
    );
    v.errfull(
        &["JSON.MERGE", "merge_new_key_error2", ".a.b", r#"{"x":1}"#],
        msg,
    );
    v.errfull(
        &[
            "JSON.MERGE",
            "merge_new_key_error3",
            "$..field",
            r#"{"value":1}"#,
        ],
        msg,
    );
    v.ok(&["JSON.MERGE", "merge_new_key_error", ".", r#"{"a":1}"#]);
    v.ok(&[
        "JSON.MERGE",
        "merge_new_key_error",
        ".user",
        r#"{"name":"John"}"#,
    ]);
    v.done("test_json_merge_command_non_root_path_new_key_error");
}

#[test]
fn test_json_merge_command_empty_object_noop() {
    let mut v = V::new();
    let k = "merge_empty_noop";
    let original = r#"{"a":1,"b":{"x":2,"y":3},"c":[4,5,6],"d":"string"}"#;
    v.ok(&["JSON.SET", k, ".", original]);
    v.ok(&["JSON.MERGE", k, ".", "{}"]);
    v.get(&["JSON.GET", k], original);
    v.get(&["JSON.GET", k, ".a"], "1");
    v.get(&["JSON.GET", k, ".b.x"], "2");
    v.get(&["JSON.GET", k, ".b.y"], "3");
    v.get(&["JSON.GET", k, ".c"], "[4,5,6]");
    v.get(&["JSON.GET", k, ".d"], r#""string""#);
    v.ok(&["JSON.MERGE", k, ".b", "{}"]);
    v.get(&["JSON.GET", k, ".b.x"], "2");
    v.get(&["JSON.GET", k, ".b.y"], "3");
    v.done("test_json_merge_command_empty_object_noop");
}

#[test]
fn test_json_merge_command_recursive_descent_ancestor_wins() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "merge_recursive_overlap",
        ".",
        r#"{"a":{"a":1}}"#,
    ]);
    v.ok(&["JSON.MERGE", "merge_recursive_overlap", "$..a", "5"]);
    v.get(&["JSON.GET", "merge_recursive_overlap"], r#"{"a":5}"#);
    v.ok(&[
        "JSON.SET",
        "merge_recursive_overlap_set",
        ".",
        r#"{"a":{"a":1}}"#,
    ]);
    v.ok(&["JSON.SET", "merge_recursive_overlap_set", "$..a", "5"]);
    v.get(&["JSON.GET", "merge_recursive_overlap_set"], r#"{"a":5}"#);
    v.done("test_json_merge_command_recursive_descent_ancestor_wins");
}

/// Byte counts are only compared with each other: the limit is derived from
/// what `JSON.DEBUG MEMORY` reports for this very server.
#[test]
fn test_json_merge_command_size_limit_multi_target() {
    let mut v = V::new();
    let key = "merge_size_limit";
    v.ok(&["JSON.SET", key, ".", r#"{"a":{},"b":{}}"#]);
    let mem = |v: &V, k: &str| match v.run(&["JSON.DEBUG", "MEMORY", k]) {
        Reply::Int(n) => n,
        other => panic!("JSON.DEBUG MEMORY {k}: {other:?}"),
    };
    let base_size = mem(&v, key);
    let patch = format!(r#"{{"data":"{}"}}"#, "x".repeat(1000));
    v.ok(&["JSON.SET", "merge_size_limit_tmp", ".", r#"{"a":{}}"#]);
    let before = mem(&v, "merge_size_limit_tmp");
    v.ok(&["JSON.MERGE", "merge_size_limit_tmp", "$.a", &patch]);
    let single_growth = mem(&v, "merge_size_limit_tmp") - before;
    v.vectors += 1;
    if single_growth <= 0 {
        v.fails.push(format!(
            "  merging into one target grew the document by {single_growth}"
        ));
    }
    let limit = base_size + single_growth + single_growth / 2;
    v.ok(&[
        "CONFIG",
        "SET",
        "json.max-document-size",
        &limit.to_string(),
    ]);
    v.err_has(&["JSON.MERGE", key, "$.*", &patch], &["LIMIT"]);
    v.get(&["JSON.GET", key, ".a"], "{}");
    v.get(&["JSON.GET", key, ".b"], "{}");
    v.done("test_json_merge_command_size_limit_multi_target");
}

#[test]
fn test_json_merge_command_new_key_strips_nulls() {
    let mut v = V::new();
    v.ok(&[
        "JSON.MERGE",
        "merge_new_key_null",
        ".",
        r#"{"a":null,"b":1}"#,
    ]);
    v.get(&["JSON.GET", "merge_new_key_null"], r#"{"b":1}"#);
    v.ok(&[
        "JSON.MERGE",
        "merge_new_key_nested_null",
        ".",
        r#"{"a":{"bb":{"ccc":null,"d":1}}}"#,
    ]);
    v.get(
        &["JSON.GET", "merge_new_key_nested_null"],
        r#"{"a":{"bb":{"d":1}}}"#,
    );
    v.ok(&["JSON.MERGE", "merge_new_key_scalar", ".", "[1,2,3]"]);
    v.get(&["JSON.GET", "merge_new_key_scalar"], "[1,2,3]");
    v.done("test_json_merge_command_new_key_strips_nulls");
}

#[test]
fn test_json_merge_command_insert_path_strips_nulls() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "merge_insert_null", ".", r#"{"u":{}}"#]);
    v.ok(&[
        "JSON.MERGE",
        "merge_insert_null",
        ".u.bob",
        r#"{"email":"x","tmp":null}"#,
    ]);
    v.get(
        &["JSON.GET", "merge_insert_null", ".u.bob"],
        r#"{"email":"x"}"#,
    );
    v.done("test_json_merge_command_insert_path_strips_nulls");
}

#[test]
fn test_json_merge_command_nested_non_object_target_null() {
    let mut v = V::new();
    v.ok(&["JSON.SET", "merge_nested_non_object", ".", r#"{"a":"b"}"#]);
    v.ok(&[
        "JSON.MERGE",
        "merge_nested_non_object",
        ".",
        r#"{"a":{"x":null,"y":1}}"#,
    ]);
    v.get(&["JSON.GET", "merge_nested_non_object"], r#"{"a":{"y":1}}"#);
    v.done("test_json_merge_command_nested_non_object_target_null");
}

const RFC7396: [(&str, &str, &str); 15] = [
    (r#"{"a":"b"}"#, r#"{"a":"c"}"#, r#"{"a":"c"}"#),
    (r#"{"a":"b"}"#, r#"{"b":"c"}"#, r#"{"a":"b","b":"c"}"#),
    (r#"{"a":"b"}"#, r#"{"a":null}"#, "{}"),
    (r#"{"a":"b","b":"c"}"#, r#"{"a":null}"#, r#"{"b":"c"}"#),
    (r#"{"a":["b"]}"#, r#"{"a":"c"}"#, r#"{"a":"c"}"#),
    (r#"{"a":"c"}"#, r#"{"a":["b"]}"#, r#"{"a":["b"]}"#),
    (
        r#"{"a":{"b":"c"}}"#,
        r#"{"a":{"b":"d","c":null}}"#,
        r#"{"a":{"b":"d"}}"#,
    ),
    (r#"{"a":[{"b":"c"}]}"#, r#"{"a":[1]}"#, r#"{"a":[1]}"#),
    (r#"["a","b"]"#, r#"["c","d"]"#, r#"["c","d"]"#),
    (r#"{"a":"b"}"#, r#"["c"]"#, r#"["c"]"#),
    (r#"{"a":"foo"}"#, "null", "null"),
    (r#"{"a":"foo"}"#, r#""bar""#, r#""bar""#),
    (r#"{"e":null}"#, r#"{"a":1}"#, r#"{"e":null,"a":1}"#),
    ("[1,2]", r#"{"a":"b","c":null}"#, r#"{"a":"b"}"#),
    ("{}", r#"{"a":{"bb":{"ccc":null}}}"#, r#"{"a":{"bb":{}}}"#),
];

#[test]
fn test_json_merge_command_rfc7396_appendix_a() {
    let mut v = V::new();
    for (original, patch, expected) in RFC7396 {
        v.run(&["DEL", "merge_rfc_appendix"]);
        v.ok(&["JSON.SET", "merge_rfc_appendix", ".", original]);
        v.ok(&["JSON.MERGE", "merge_rfc_appendix", ".", patch]);
        v.get(&["JSON.GET", "merge_rfc_appendix"], expected);
    }
    v.done("test_json_merge_command_rfc7396_appendix_a");
}

#[test]
fn test_json_merge_command_rfc7396_appendix_a_subpath() {
    let mut v = V::new();
    for (original, patch, expected) in RFC7396 {
        v.run(&["DEL", "merge_rfc_appendix_subpath"]);
        v.ok(&[
            "JSON.SET",
            "merge_rfc_appendix_subpath",
            ".",
            &format!(r#"{{"wrap":{original}}}"#),
        ]);
        v.ok(&["JSON.MERGE", "merge_rfc_appendix_subpath", ".wrap", patch]);
        v.get(
            &["JSON.GET", "merge_rfc_appendix_subpath", ".wrap"],
            expected,
        );
    }
    v.done("test_json_merge_command_rfc7396_appendix_a_subpath");
}

#[test]
fn test_json_merge_command_multi_target_insert_strips_nulls() {
    let mut v = V::new();
    let k = "merge_multi_insert_null";
    v.ok(&["JSON.SET", k, ".", r#"{"users":[{"id":1},{"id":2}]}"#]);
    v.ok(&[
        "JSON.MERGE",
        k,
        "$.users[*].profile",
        r#"{"email":"x","tmp":null}"#,
    ]);
    v.get(&["JSON.GET", k, ".users[0].profile"], r#"{"email":"x"}"#);
    v.get(&["JSON.GET", k, ".users[1].profile"], r#"{"email":"x"}"#);
    v.done("test_json_merge_command_multi_target_insert_strips_nulls");
}

/// The per-command arity half of upstream's test; the `JSON.DEBUG` subcommand
/// table (KEYTABLE-*, TEST-SHARED-API) is skipped.
#[test]
fn test_json_arity_per_command() {
    let mut v = V::new();
    for (cmd, arity) in [
        ("SET", -4),
        ("MERGE", 4),
        ("GET", -2),
        ("MGET", -3),
        ("DEL", -2),
        ("FORGET", -2),
        ("NUMINCRBY", 4),
        ("NUMMULTBY", 4),
        ("STRLEN", -2),
        ("STRAPPEND", -3),
        ("TOGGLE", -2),
        ("OBJLEN", -2),
        ("OBJKEYS", -2),
        ("ARRLEN", -2),
        ("ARRAPPEND", -4),
        ("ARRPOP", -2),
        ("ARRINSERT", -5),
        ("ARRTRIM", 5),
        ("CLEAR", -2),
        ("ARRINDEX", -4),
        ("TYPE", -2),
        ("RESP", -2),
        ("DEBUG", -2),
    ] {
        let name = format!("JSON.{cmd}");
        let args = ["COMMAND", "INFO", name.as_str()];
        v.vectors += 1;
        let got = v.run(&args);
        let seen = match &got {
            Reply::Array(rows) => match rows.first() {
                Some(Reply::Array(info)) => match info.get(1) {
                    Some(Reply::Int(n)) => Some(*n),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        };
        if seen != Some(arity) {
            v.fail(&args, &got, &format!("arity {arity} in the first row"));
        }
    }
    v.done("test_json_arity_per_command");
}

/// Upstream runs 10000 rounds; 300 are enough to see the property fail.
#[test]
fn test_doc_mem_size_with_numincrby() {
    let mut v = V::new();
    v.ok(&[
        "JSON.SET",
        "k1",
        ".",
        r#"{"a":1955439684,"b":5398,"c":[],"d":{"e":0,"f":2.7233281135559082,"g":1732953600,"h":false,"i":true}}"#,
    ]);
    for _ in 0..300 {
        v.run(&[
            "json.numincrby",
            "k1",
            ".d.e",
            "7.055081799084578998899889890890",
        ]);
        v.ok(&["json.set", "k1", ".d.e", "0"]);
        let meta = v.run(&["json.debug", "memory", "k1"]);
        let walked = v.run(&["json.debug", "memory", "k1", "."]);
        v.vectors += 1;
        let small = matches!(meta, Reply::Int(n) if n < 1000);
        if !small || meta != walked {
            v.fail(
                &["json.debug", "memory", "k1"],
                &meta,
                &format!(
                    "an integer below 1000, equal to the `.` answer {}",
                    text(&walked)
                ),
            );
            break;
        }
    }
    v.done("test_doc_mem_size_with_numincrby");
}

#[test]
fn test_jsonpath_member_name_containing_slash() {
    let mut v = V::new();
    v.run(&["json.set", "k1", ".", r#"{"a": 4.5, "b/c": 85}"#]);
    v.get(&["json.get", "k1", "$.b/c"], "[85]");
    v.run(&["json.set", "k1", "$.b/c", "0"]);
    v.get(&["json.get", "k1"], r#"{"a":4.5,"b/c":0}"#);
    v.run(&["json.set", "k1", "$.b~1c", "2"]);
    v.get(&["json.get", "k1"], r#"{"a":4.5,"b/c":2}"#);
    v.run(&["json.numincrby", "k1", "$.b/c", "1"]);
    v.get(&["json.get", "k1"], r#"{"a":4.5,"b/c":3}"#);
    v.run(&["json.nummultby", "k1", "$.b/c", "2"]);
    v.get(&["json.get", "k1"], r#"{"a":4.5,"b/c":6}"#);
    v.get(&["json.get", "k1", "$.b/c"], "[6]");
    v.run(&["json.set", "k1", "$..b/c", "7"]);
    v.get(&["json.get", "k1"], r#"{"a":4.5,"b/c":7}"#);
    v.run(&["json.numincrby", "k1", "$..b/c", "2"]);
    v.get(&["json.get", "k1"], r#"{"a":4.5,"b/c":9}"#);
    v.done("test_jsonpath_member_name_containing_slash");
}

#[test]
fn test_recursive_path_mutation_use_after_free() {
    let mut v = V::new();
    let cases: Vec<(&str, Vec<&str>, Reply, &str)> = vec![
        (
            "uaf_append",
            vec!["JSON.ARRAPPEND", "$..", r#""x""#],
            ints(&[3, 2]),
            r#"[[[0,"x"],0,"x"]]"#,
        ),
        (
            "uaf_insert",
            vec!["JSON.ARRINSERT", "$..", "0", r#""x""#],
            ints(&[3, 2]),
            r#"[["x",["x",0],0]]"#,
        ),
        (
            "uaf_pop",
            vec!["JSON.ARRPOP", "$.."],
            bulks(&["0", "0"]),
            "[[[]]]",
        ),
        (
            "uaf_trim",
            vec!["JSON.ARRTRIM", "$..", "0", "0"],
            ints(&[1, 1]),
            "[[[0]]]",
        ),
        (
            "uaf_clear",
            vec!["JSON.CLEAR", "$.."],
            Reply::Int(2),
            "[[]]",
        ),
    ];
    for (key, cmd, reply, doc) in cases {
        v.ok(&["JSON.SET", key, "$", "[[0],0]"]);
        let mut args = vec![cmd[0], key];
        args.extend_from_slice(&cmd[1..]);
        v.want(&args, reply);
        v.get(&["JSON.GET", key, "$"], doc);
    }
    v.done("test_recursive_path_mutation_use_after_free");
}

/// `JSON.DEBUG FIELDS` and `HELP` are not exercised by the upstream suite
/// (only `test_json_shared_api` prints HELP); the expectations here are read
/// from `json.cc` (`processFieldsSubCmd`, `dom_num_fields`, the HELP branch).
#[test]
fn test_json_debug_fields_and_help() {
    let mut v = V::new();
    v.int(&["JSON.DEBUG", "FIELDS", "wikipedia"], 20);
    v.int(&["JSON.DEBUG", "FIELDS", "wikipedia", "."], 20);
    v.int(&["JSON.DEBUG", "FIELDS", "wikipedia", ".address"], 4);
    v.int(&["JSON.DEBUG", "FIELDS", "wikipedia", ".phoneNumbers"], 6);
    v.int(&["JSON.DEBUG", "FIELDS", "wikipedia", ".firstName"], 1);
    v.int(&["JSON.DEBUG", "FIELDS", "wikipedia", ".children"], 0);
    v.ints(&["JSON.DEBUG", "FIELDS", "wikipedia", "$.address"], &[4]);
    v.ints(
        &["JSON.DEBUG", "FIELDS", "wikipedia", "$.phoneNumbers[*]"],
        &[2, 2],
    );
    v.ints(&["JSON.DEBUG", "FIELDS", "wikipedia", "$.foo"], &[]);
    v.err(&["JSON.DEBUG", "FIELDS", "wikipedia", ".foo"], NONEXISTENT);
    v.nil(&["JSON.DEBUG", "FIELDS", "nonexistentkey"]);
    v.err_has(&["JSON.DEBUG", "FIELDS"], &["wrong number of arguments"]);
    let help = v.run(&["JSON.DEBUG", "HELP"]);
    v.vectors += 1;
    let fine = match &help {
        Reply::Array(rows) => {
            !rows.is_empty()
                && rows
                    .iter()
                    .all(|r| matches!(r, Reply::Simple(_) | Reply::Bulk(_)))
                && matches!(&rows[0], Reply::Simple(s) if s.starts_with("JSON.DEBUG MEMORY"))
        }
        _ => false,
    };
    if !fine {
        v.fail(
            &["JSON.DEBUG", "HELP"],
            &help,
            "an array of simple strings starting with `JSON.DEBUG MEMORY ...`",
        );
    }
    v.err_has(
        &["JSON.DEBUG", "HELP", "extra"],
        &["wrong number of arguments"],
    );
    v.done("test_json_debug_fields_and_help");
}
