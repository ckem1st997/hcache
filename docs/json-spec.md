# valkey-json 1.1.0 behaviour spec (for the HCache JSON.* reimplementation)

Scope: what `JSON.*` does in valkey-json 1.1.0, the latest release (GitHub tag 1.1.0, commit b6644b78a8ae7fa62f70d4c8f750a388c0c2ca83), where RedisJSON differs, and test vectors.

Evidence labels:

- "(from file:line)" = read in the 1.1.0 source tarball. Paths are relative to `src/json/` unless they start with `tst/` or `src/rapidjson/`. The tarball was only read, never built or run.
- "(from URL)" = read on a docs page. Docs pages are fetched text, not a live server.
- Where I summarise a whole function and give no line, the label names the file and function only.
- Line numbers I did not re-verify after reading are given as file only. Treat them as "look in this file".

Conventions: `legacy path` = any path not starting with `$`. `JSONPath` / `v2 path` = path starting with `$`. "Test file" = `tst/integration/test_json_basic.py` (cited as `T:<line>`).

---

## 1. Module identity and data type

| Fact | Value | Source |
|---|---|---|
| Module name | `json` | json.cc:40-49 |
| Module version | 10100 (1.1.0) | json.cc:40-49 |
| Data type name (9 chars, what `TYPE key` returns) | `ReJSON-RL` | json.cc:40-49 |
| RDB encoding version | 3. Load accepts 1 and 3, `encver > 3` is rejected. A legacy node-by-node encoding (encver 1) is handled in `dom_load` case 0 | json.cc:2440, dom.cc `dom_save`/`dom_load` |
| RDB payload (encver 3) | the compact JSON text of the document | dom.cc `dom_save` case 3 |
| AOF rewrite | one `JSON.SET <key> . <compact-json>` per key | json.cc:2522 |
| COPY | deep copy of the document (`DocumentType_Copy`) | json.cc |
| MEMORY USAGE | `dom_get_doc_size` callback. This is the module's tracked allocator size, not the JSON text length | json.cc, dom.cc |
| DEBUG/MEMORY value | allocator dependent. A test only checks `JSON.DEBUG MEMORY k` == `JSON.DEBUG MEMORY k .` | T:2784-2788 |
| Replication | every write command calls `ReplicateVerbatim`. Replicas and AOF replay get the original command | json.cc |
| Keyspace event class | `GENERIC` | json.cc |
| Keyspace event names | `json.set`, `json.merge`, `json.mset` (one per applied triple), `json.del` (DEL and FORGET, only when something was deleted), `json.numincrby`, `json.nummultby`, `json.strappend`, `json.toggle`, `json.arrappend`, `json.arrpop`, `json.arrinsert`, `json.arrtrim`, `json.clear` | json.cc |
| ACL category | `@json` (all JSON commands, no others). The module also puts them in `@read`/`@write`/`@fast`/`@slow` | json.cc:2846-3250; (from https://valkey.io/topics/valkey-json/) |
| INFO section | `INFO json_core_metrics`: `json_total_memory_bytes`, `json_num_documents` | (from https://valkey.io/topics/valkey-json/) |

Error-reply prefixes used by the module: `ERR`, `LIMIT`, `NONEXISTENT`, `OUTOFBOUNDARIES`, `SYNTAXERR`, `WRONGTYPE` (from https://valkey.io/topics/valkey-json/). Source also uses `OPFAIL`, `OVERFLOW`, `EMPTYVAL`, `ERROR` (util.cc:15-66).

### 1.1 Registration table (flags, ACL categories, arity)

Arity values below are the ones registered by `set_command_info` in json.cc. They override the arities in `src/commands/*.json`. A test confirms them via `COMMAND INFO` (T:5343-5369). (from json.cc:2846-3250)

| Command | Flags | ACL cats | Arity | Key spec |
|---|---|---|---|---|
| JSON.SET | write deny-oom | json write slow | -4 | key 1, RW UPDATE |
| JSON.MERGE | write deny-oom | json write slow | 4 | key 1, RW UPDATE |
| JSON.MSET | write deny-oom | json write slow | -4 | keys 1,4,7,... (last -3, step 3), RW UPDATE |
| JSON.GET | fast readonly | json read fast | -2 | RO ACCESS |
| JSON.MGET | fast readonly | json read fast | -3 | keys 1..argc-2, RO ACCESS |
| JSON.DEL, JSON.FORGET (same handler) | fast write | json write fast | -2 | RW DELETE |
| JSON.NUMINCRBY, JSON.NUMMULTBY | fast write | json write fast | 4 | RW ACCESS UPDATE |
| JSON.STRLEN, JSON.OBJLEN, JSON.ARRLEN | fast readonly | json read fast | -2 | RO |
| JSON.STRAPPEND | fast write deny-oom | json write fast | -3 | RW INSERT |
| JSON.TOGGLE | fast write deny-oom | json write fast | -2 | RW ACCESS UPDATE |
| JSON.OBJKEYS, JSON.TYPE, JSON.RESP | fast readonly | json read fast | -2 | RO ACCESS |
| JSON.ARRAPPEND | fast write deny-oom | json write fast | -4 | RW INSERT |
| JSON.ARRPOP | fast write | json write fast | -2 | RW ACCESS DELETE |
| JSON.ARRINSERT | fast write deny-oom | json write fast | -5 | RW INSERT |
| JSON.ARRTRIM | fast write | json write fast | 5 | RW DELETE |
| JSON.CLEAR | fast write | json write fast | -2 | RW DELETE |
| JSON.ARRINDEX | fast readonly | json read fast | -4 | RO ACCESS |
| JSON.DEBUG (container) | readonly getkeys-api | json read slow | -2 | keys via getkeys callback (key is argv[2]) |

JSON.DEBUG subcommands (arity from T:5358-5369; flags/availability from json.cc):

| Subcommand | Arity | Notes |
|---|---|---|
| MEMORY | -3 | `JSON.DEBUG MEMORY key [path]` |
| FIELDS | -3 | `JSON.DEBUG FIELDS key [path]` |
| DEPTH | 3 | `JSON.DEBUG DEPTH key` |
| HELP | 2 | array of simple strings (11 lines, json.cc:2410-2424) |
| MAX-DEPTH-KEY, MAX-SIZE-KEY, KEYTABLE-CHECK | 2 | undocumented developer scans, always present |
| KEYTABLE-CORRUPT | 3 | only when `json.debug-mode yes` |
| KEYTABLE-CLEAR | 2 | only when `json.debug-mode yes` (T:2803-2814 returns the count of cleared handles) |
| KEYTABLE-DISTRIBUTION | 3 | only when `json.debug-mode yes` |
| TEST-SHARED-API | -2 in the test, 4 in my first reading of json.cc | CONFLICT, not resolved. See section 14 |

The `COMMAND GETKEYS` test shows `JSON.DEBUG MEMORY k` / `DEPTH k` return `[k]` (T:1196-1199).

---

## 2. Configs (names and defaults)

(from json.cc:2705-2721; names are prefixed `json.`)

| Config | Default | Range / type | Meaning |
|---|---|---|---|
| `json.max-document-size` | 0 (unlimited) | 0..LLONG_MAX, memory type, runtime-settable | Limit on document size. The size is the module's approximate allocation size, not the JSON text length. Not enforced when the command comes from the master/AOF (`CTX_FLAGS_REPLICATED`). Error: `LIMIT Document size limit is exceeded` |
| `json.max-path-limit` | 128 | 0..INT_MAX, runtime-settable | Maximum nesting depth. Used for parser depth, JSON.SET path+value depth, merge recursion. Error: `LIMIT Document path nesting limit is exceeded` |
| `json.debug-mode` | no | bool, hidden, immutable | Enables KEYTABLE-CORRUPT/CLEAR/DISTRIBUTION |

Docs agree on the first two defaults (from https://valkey.io/topics/valkey-json/).

Compile-time constants, not configurable:

| Constant | Value | Effect |
|---|---|---|
| max parser recursion depth | 200 | `LIMIT Parser recursion depth is exceeded` (JSONPath parser) |
| max recursive-descent tokens | 20 | more than 20 `..` in one query gives `LIMIT Total number of recursive descent tokens in the query string exceeds the limit` |
| max query string size | 128 KB | `LIMIT Query string size limit is exceeded` |
| defrag threshold | 64 MB | defrag only |
| key table shards | 32768 | `json.key-table-*` is not a config |

The integration tests run with the document limit set to 64 MB by the test harness (`SIZE_64MB`, T:5226), so "33 MB string OK, 64 MB string rejected" (T:2863-2876) is not the module default (default is unlimited). The harness line that sets it was not read.

---

## 3. Path model

### 3.1 Legacy vs JSONPath recognition

- A path is JSONPath iff its first character is `$` (from selector.cc `parseSupportedPath`, `isV2Path`; json.cc `has_at_least_one_v2path` checks the first char).
- Anything else is legacy. Legacy paths use the same parser. They may start with `.` or not, and may use `[..]`, quoted names, `*`. The semantics differ, not the grammar: legacy operates on the first selected value and reports errors, JSONPath operates on all and reports nulls/empty arrays (from T:779-870, T:1076-1129).
- Root path for SET/DEL/etc. is exactly `.` or `$` (`jsonutil_is_root_path`, util.cc).
- A `$` anywhere other than position 0 gives `SYNTAXERR Dollar sign cannot apply to non-root element` (util.cc:15-66).
- The default path when omitted is `.` (root, legacy). Hence `JSON.GET k` returns the bare document text, not a one-element array (from json.cc handler for JSON.GET; T:1269 `JSON.GET key` returns `{}` style output, T:1280 `JSON.GET k3` returns `[]`).

### 3.2 JSONPath grammar actually implemented

EBNF is at selector.cc:65-115. Supported:

| Syntax | Notes | Evidence |
|---|---|---|
| `$` | root | selector.cc |
| `.name`, `['name']`, `["name"]` | spaces inside brackets allowed (`$.[ "a" ]`) | T:779-806 |
| `..name`, `..[..]`, `..*` | recursive descent, max 20 per query | selector.cc, T:4055-4111 |
| `*`, `[*]` | wildcard over object members or array elements | T:823-840 |
| `[n]`, `[-n]` | index, negative from end. Out-of-range selects nothing (JSONPath) or `OUTOFBOUNDARIES` error (legacy) | T:871-952 |
| `[s:e:step]` | slice. All parts optional, `+` sign allowed, spaces allowed, negative start/end add the size, clamped to the array. `step` 0 gives `SYNTAXERR Step in the slice cannot be zero`. Steps beyond int64 give `Invalid number` | T:954-1020 |
| `[a,b,c]` | union of indexes in written order, duplicates kept (`$[0,1,5,0,1,2]` returns `[0,1,5,0,1,2]`), out-of-range skipped. Also union of names `['a','b']`. Cannot mix slices and indexes, no empty elements (`$[0,,4]`, `$[,]`, `$[4,]` give syntax errors) | T:1022-1074 |
| `[?(expr)]` | filter, see 3.3 | T:3856-4053 |

Name tokens:

- Unquoted member names end at any of `. [ ] ( ) < > = ! ' " space | &`. If the first character is in that set the error is `SYNTAXERR Invalid object member name`. (from selector.cc lexer)
- Three or more dots: `SYNTAXERR Invalid dot sequence`.
- Single-quoted names accept `\'` and the control escapes. Double-quoted names are parsed with the JSON string parser (full escapes including `\uXXXX`) (T:3108-3139).
- Non-ASCII member names work unquoted (`.a.愛`) (T:1330-1348).
- Not supported: `~` (member names), `=~` regex, `!`, `in`, `nin`, `size`, `empty`, `subsetof`, `anyof`, `noneof`, arithmetic, script expressions `()`, and all functions (`length()`, `min()` etc.). I found no tokens for these in selector.cc and the valkey docs list only the operators in 3.3 (from https://valkey.io/topics/valkey-json/). RedisJSON supports them (from https://redis.io/docs/latest/develop/data-types/json/path/).

### 3.3 Filters

Operators (from https://valkey.io/topics/valkey-json/ and selector.cc `evalOp`): `==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`, parentheses.

- `&&` binds tighter than `||` (T:3954-3959: `@.price<9||@.price>10&&@.isbn` returns `[8.95,22.99]`, and with parentheses `[22.99]`).
- Operands: `@` (the element itself), `@.member`, `@['member']`, `@["member"]`, `@.member[idx]`, a literal (null, true, false, number, quoted string), or a `$`-rooted partial path that resolves to exactly one scalar. A bare `@.member` with no operator is an existence test (T:3918: `[?(@.isbn)]` selects the books that have `isbn`). `value op @...` (literal on the left) is accepted.
- Type rules: operands of different type compare false for every operator including `!=`. `true` and `false` count as one type (`@==true` on `[true,false,null,1,2]` selects `[true,true]` in T:3981). `null == null` is true. Ordering operators on null, objects and arrays are false. Strings compare bytewise. Numbers compare as double if either side is double, else as integers. (from selector.cc `evalOp`; this summary was written from reading the code, not from a test of every combination)
- Filters apply to array elements and to object member values. Results come back in document order, whichever `||` branch matched (T:4003-4006: `[?(@ > 7 || @ < 3)]` on 1..9 returns `[1,2,8,9]`).
- Numeric literals accept exponent forms (`1.0E1`, `-1.23e1`) (T:3932-3941).
- Spaces around operators and inside `?( )` are allowed.
- Filters work in writes: `JSON.SET`/`JSON.DEL` with a filter path update/delete all matches (T:4013-4053).

### 3.4 Errors in paths: which fail the command in JSONPath mode

In JSONPath mode only these "syntax class" errors fail the command; everything else becomes an empty result or `null` entries (from selector.cc:971-984):

`INVALID_JSON_PATH`, `INVALID_MEMBER_NAME`, `INVALID_NUMBER`, `INVALID_IDENTIFIER`, `INVALID_DOT_SEQUENCE`, `EMPTY_EXPR_TOKEN`, `ARRAY_INDEX_NOT_NUMBER`, `STEP_ZERO`, `PARENT_ELEMENT_NOT_EXIST`, `PARSER_RECURSION_DEPTH_LIMIT_EXCEEDED`, `RECURSIVE_DESCENT_TOKEN_LIMIT_EXCEEDED`, `QUERY_STRING_SIZE_LIMIT_EXCEEDED`.

Important exception: the write commands NUMINCRBY, NUMMULTBY, STRAPPEND, ARRAPPEND, ARRPOP, ARRINSERT, ARRTRIM and CLEAR return the `dom_*` return code unchanged when path resolution fails, even for a `$` path. So `OUTOFBOUNDARIES` from an array index can surface in JSONPath mode for those commands (from dom.cc `dom_increment_by` ... `dom_clear`, summary of my first read). A test shows ARRINSERT with a bad index gives `OUTOFBOUNDARIES` under `$.[*]` (T:2350-2354). The read-only commands (STRLEN, OBJLEN, OBJKEYS, ARRLEN, ARRINDEX, TYPE, RESP, DEBUG MEMORY/FIELDS) and TOGGLE swallow non-syntax errors in JSONPath mode.

### 3.5 Legacy path error rule (read and write)

- No value selected: `NONEXISTENT JSON path does not exist`.
- Values selected but none of the right type: the matching WRONGTYPE error, for example `WRONGTYPE JSON element is not a number`.
- Legacy wildcard on a scalar: `SYNTAXERR Invalid JSON path`. In JSONPath mode the same case is a swallowed `ERR Invalid use of wildcard` (no match) (from selector.cc).
- Legacy index out of range: `OUTOFBOUNDARIES Array index is out of bounds` (T:871-905, `.phoneNumbers[2].type`, `[10]`, `[-3]`).
- Legacy negative index on a non-array (`.firstName[-1]`, `.address[-1]`): a `WRONGTYPE` error (T:907-918).

### 3.6 "Definite" paths and creation by JSON.SET

(from selector.cc insert/update logic; docs from https://valkey.io/commands/json.set/)

- If the path resolves to one or more existing values, each one is replaced.
- If the LAST element is a plain object member name (`.a`, `['a']`) that does not exist and the parent exists and is an object, the member is appended (insert). Order: appended at the end.
- If a missing member is not the last element: `NONEXISTENT JSON path does not exist` (T:`JSON.SET w .address.foo.z` in the set tests).
- If the parent exists but is not an object: `ERROR Cannot insert a member into a non-object value`. Observed class for `.firstName.a` where firstName is a string: a write error (the test checks only the class).
- Array index out of range for set: `OUTOFBOUNDARIES Array index is out of bounds` (`JSON.SET w phoneNumbers[9] "123"`). You cannot create array elements by index; use ARRINSERT/ARRAPPEND.
- Wildcards, filters, unions, slices and recursive descent only update existing matches; they never insert.
- NX: fail (null reply) if the path matches any existing value. XX: fail (null reply) if the path would insert, i.e. nothing matched (`hasInserts` / `hasUpdates` in selector.cc).
- A set that matches several values updates all of them (`JSON.SET k $..a 3` style, T:4189 area and RedisJSON docs example).
- Recursive descent with nested matches: JSON.SET applies ancestor first and then does not patch the removed child (T:5176-5190: `JSON.SET k $..a 5` on `{"a":{"a":1}}` gives `{"a":5}`).

---

## 4. Value model, parsing and output

### 4.1 Parser (from `src/rapidjson/reader.h`, `document.h`, README in that directory)

- RapidJSON with default flags: no comments, no NaN/Infinity, no trailing commas. Modified to hold integers in int64 range only.
- Integers: parsed as int64. An integer literal outside int64 (`9223372036854775808`, `-9223372036854775809`) becomes a double and reports as type `number` (T:1350-1372).
- Anything with a fraction or an exponent is a double, type `number`.
- A double is stored as its ORIGINAL TEXT (raw number) and re-emitted unchanged. Examples that round-trip exactly: `0.3000000`, `1.234567890123456789`, `1.7976e+308`, `1e3`, `2.50000`, `2E+30` (T:1507-1530, T:1580-1608 round-trips 24000 random doubles).
- Duplicate keys in input: the LAST value wins and the member keeps the position of the FIRST occurrence (T:2817-2840: `{"a":0,"a":1}` stored as `{"a":1}`, OBJLEN 1, OBJKEYS `["a"]`).
- Strings: `\uXXXX` and surrogate pairs decode to UTF-8. Invalid UTF-8 is not rejected (no validation flag set).
- A parse error maps to `SYNTAXERR Failed to parse JSON string due to syntax error`, except nesting beyond `json.max-path-limit` (parse termination), which maps to `LIMIT Document path nesting limit is exceeded` (dom.h parse-error mapping).
- Max nesting default 128: depth 127 `{"a":{...}}` is accepted, a 200000-deep document is rejected with a LIMIT error (T:2842-2861). Note: the test checks only the LIMIT class, and I did not verify the exact boundary between 127 and 128.

### 4.2 Writer (from `src/rapidjson/writer.h`, `prettywriter.h`)

- Compact output has no spaces and no newlines.
- Escapes: `"` `\\` `\b` `\t` `\n` `\f` `\r`; other control characters below 0x20 as `\u00XX` with UPPERCASE hex (T:3135-3136 reads back `\u001F`); `/` is NOT escaped; non-ASCII is emitted as raw UTF-8 (T:1316-1328); 0x7F is not escaped.
- Strings that contain nothing needing escapes carry a flag and are written raw.
- `NOESCAPE` on JSON.GET is accepted and has no effect (json.cc).
- Object member order is insertion order (small objects stored as a vector, objects over 64 members as a hash table with an insertion-ordered list) (document.h). A member added by JSON.SET goes last.
- Deleting a member keeps the order of the others only if the module uses the order-preserving erase. `DoRemoveMemberVec` in document.h:2466 is marked `RAPIDJSON_ASSERT(false) // broken` and swaps the last member into the hole. The module's deletion goes through `ptr.Erase` (selector.cc:755), and `rapidjson/pointer.h` is not in the tarball, so I could not confirm that it calls `EraseMember` (order-preserving). Assume order preserved, flagged in section 14.

### 4.3 Pretty printing (JSON.GET INDENT/NEWLINE/SPACE)

(from prettywriter.h; examples from tests)

- Option keywords are case-insensitive (`space` lowercase works, T:3154). Each takes the next argument as its value; if it is the last argument the error is `SYNTAXERR Command syntax error`.
- With none of the three options the compact writer is used.
- With any of them, the pretty writer is used with these rules: after `{` or `[` emit NEWLINE then INDENT times depth before each element; between members/elements emit `,` NEWLINE; after a key emit `:` then SPACE; before the closing bracket emit NEWLINE + indent, but only if the container is non-empty; empty containers print as `{}` and `[]`. Unset options are empty strings.
- Consequence: with only INDENT set, there is no newline, so `JSON.GET k1 INDENT " " .` on `{"a":"1","b":"2","c":"3"}` returns `{ "a":"1", "b":"2", "c":"3"}`... see vector V11 for the exact expected strings from the test.
- JSONPath results are wrapped in an array that is formatted by the same writer (`[\n\t{...}\n]`).
- Numbers are written identically by both writers (T:1580-1608).

### 4.4 Numeric results of NUMINCRBY / NUMMULTBY

(from dom.cc `dom_increment_by`, `dom_multiply_by`; util.cc; T:1437-1770)

- The argument is parsed as JSON. A non-number (string, `2.`, `2.0.`, `.2.0`, `+2.` etc.) gives `WRONGTYPE Value is not a number` (T:1546-1578 checks the class WRONGTYPE for all of those, including an arg `"2.0"`).
- NUMINCRBY: both operands int64 gives an int64 add with overflow check. If int64 overflows it falls back to a double add; `OVERFLOW Addition would overflow` only when the double result is infinite.
- NUMMULTBY: always computed in double. If the result is a whole number inside int64 it is stored as an integer, otherwise as text from `%.17g`. An infinite result gives `OVERFLOW Multiplication would overflow` (T:1704-1731).
- A computed double is formatted with `snprintf("%.17g")` (util.cc `jsonutil_double_to_string`), so `0.1+0.2` gives `0.30000000000000004`, `1.5+0.5` gives `2`, `2E-30*1` gives `2.0000000000000002e-30`, `2e30*1` gives `2e+30`, and `2e5*1` gives `200000` (T:1507-1544). Multiplying by 1 or adding 0 therefore removes the original styling.
- Reply: bulk string with the new value (legacy: the LAST updated value; JSONPath: a JSON array text with `null` for non-numbers, `[2,null,4]`). Example reply texts: `9.2233720368547758e+18` for 9223372036854775807 + 0 as double (T:1644-1645 accepts either that or the integer text; the test uses `or`, so the exact text is not pinned).
- Missing key and missing path both reply `NONEXISTENT ...` (T:1453-1463).

### 4.5 Reply value rules, by type

| Value | JSON.TYPE name | JSON.RESP element |
|---|---|---|
| null | `null` | null |
| true/false | `boolean` | simple string `true` / `false` |
| int64 | `integer` | integer |
| double | `number` | bulk string (RapidJSON dtoa text, `135.17`) |
| string | `string` | bulk string |
| array | `array` | array whose first element is bulk string `[` followed by the elements |
| object | `object` | array whose first element is bulk string `{` followed by `[key, value]` pairs |

(from dom.cc `TYPE_NAMES = {"null","boolean","string","number","integer","object","array"}`, `dom_reply_with_resp`; T:2613-2749)

---

## 5. Command reference

Conventions for replies: "legacy" and "v2" refer to the path kind. RESP3 note at the end of the section.

### JSON.SET key path value [NX|XX]

- Arity: exactly 4 or 5 arguments (json.cc). The fifth is `NX` or `XX`, case-insensitive; anything else gives `SYNTAXERR Command syntax error` (T: `JSON.SET k . x NXXX`).
- Reply: `OK` or null (NX/XX failed).
- New key (key missing): path must be root, else `SYNTAXERR A new Valkey key's path must be root` (json.cc:51-52). With `XX` on a missing key the reply is null (not an error).
- Existing key, root path: parse and replace whole document. `NX` on an existing key gives null.
- Wrong key type: `WRONGTYPE Operation against a key holding the wrong kind of value` (core error, standard server reply). The module's own `WRONGTYPE Not a JSON document key` is `NOT_A_DOCUMENT_KEY` (util.cc).
- Size check: current doc size + new value size against `json.max-document-size`; depth check: path depth + value depth against `json.max-path-limit` (from dom.cc `dom_set_value`, `CHECK_DOCUMENT_PATH_LIMIT`).
- Event `json.set`.

### JSON.GET key [INDENT s] [NEWLINE s] [SPACE s] [NOESCAPE] [path ...]

- Arity -2. Format options and paths may be interleaved; any other token is a path. No path means `.`.
- Missing key: null.
- One path: legacy returns the first value's JSON text; v2 returns a JSON array of all matches (`[]` if none).
- Legacy and nothing matches: `NONEXISTENT JSON path does not exist`.
- Several paths: a JSON object whose keys are the path strings exactly as given. If any path starts with `$` every value is an array (empty array if no match), even the legacy-style keys (T:1110-1129). If all are legacy, a missing path fails the whole command with the NONEXISTENT error (T:1098-1108).
- Multi-path key order is the order of the path arguments in the examples in tests, but T:1346 notes "Valkey behavior is weird and returns dictionaries in a non-deterministic order". Do not rely on the order of the multi-path object; the docs example shows `$..b` before `..a` for the command `JSON.GET doc ..a $..b` (from https://redis.io/docs/latest/commands/json.get/, RedisJSON). Whether valkey-json emits path order or hash order is UNDETERMINED (section 14).
- Doc difference: RedisJSON's docs state the default path is `$`; valkey-json defaults to `.` (see section 12).

### JSON.MGET key [key ...] path

- Arity -3. The LAST argument is the path (required). Reply is an array with one bulk string (or null) per key.
- Missing key, missing path, invalid path for that key: null. A key of the wrong type fails the whole command (from json.cc `JSON.MGET`).
- v2 path: each element is a JSON array text (`[...]`, `[]` when no match); legacy path: each element is the first value text. Values are not wrapped in the multi-path object (T:1131-1170).

### JSON.MSET key path value [key path value ...]

- `(argc-1) % 3 == 0` or the standard arity error.
- All triples are validated first: key type, new key with non-root path (`SYNTAXERR Command syntax error`, T:2918-2924), JSON parse, depth and size. Any failure rejects the whole command and nothing is applied.
- After validation, triples are applied in order, best effort. A triple that can no longer apply (for example NX-like path loss) is skipped and the reply is still `OK`. One `json.mset` event per applied triple. The command is replicated verbatim.

### JSON.MERGE key path value

- Arity exactly 4. RFC 7396 semantics (T:5272-5326 runs all RFC 7396 Appendix A cases both at the root and at a sub-path).
- Object patch into an object: members merged recursively; a member with `null` in the patch is deleted; arrays and scalars replace; a non-object target is treated as `{}` when the patch is an object (T:5262-5270); an empty-object patch changes nothing (T:5144-5173). A non-object patch replaces the target, `null` included (`{"a":"foo"}` + `null` gives `null`, T:5284).
- Existing `null` members in the target are kept (`{"e":null}` + `{"a":1}` gives `{"e":null,"a":1}`).
- New key: path must be root (else `SYNTAXERR A new Valkey key's path must be root`); the patch is merged into `{}` so nulls in it are stripped (T:5229-5247). Non-root path on a new key: error (T:5111-5120).
- Non-existing last member of an existing parent: inserted, with nulls stripped (T:5250-5259 `.u.bob`). Same for several targets (T:5329-5341).
- Multi-target path (`$..meta`, `.users[*].profile`): applied to each; with `$..a` overlapping parents and children only the top-most match is applied (T:5176-5190).
- Atomic: size and depth are checked for the whole operation first, so a multi-target merge that would exceed `json.max-document-size` fails with a `LIMIT` error and changes nothing (T:5193-5226).
- Reply `OK`, event `json.merge`. Not in RedisJSON 2.x docs I fetched; present in RedisJSON 2.6+ (not fetched, so treated as not determined).

### JSON.DEL / JSON.FORGET key [path]

- Arity -2 (2 or 3 args; extra args give `wrong number of arguments`, T:1233-1238). Default path `.`.
- Missing key: 0. Root path: deletes the key, reply 1 (T:1216-1223). Non-root legacy: deletes the first match, reply 1, or 0 if the path does not exist (T:1229-1231). v2: number of deleted values. Deleting several values goes deepest-first so indexes stay valid (from dom.cc `pathCompare`).
- Deleting inside arrays shifts later elements (T:1263: `JSON.DEL k2 $.[3:6]` on 0..9 leaves `[0,1,2,6,7,8,9]`).
- Event `json.del` only when something was deleted.
- Known divergence from RedisJSON: `JSON.DEL k $..a` on `{"a":{"a":{"a":{"a":1}}}}` deletes 4 in valkey-json (RedisJSON 1) (T:4106-4111).

### JSON.TYPE key [path]

- Missing key, missing path, invalid path: null. Legacy: bulk string name of the first match. v2: array of names (`[]` when none). Names: section 4.5. Reply type is a simple string/bulk (the test decodes bytes either way).
- TYPE of a number: `integer` for int64 values, `number` for doubles and out-of-int64 integers.

### JSON.CLEAR key [path]

- Arrays and objects are emptied; numbers become `0` (a double becomes `0.0`... see note); strings become `""`; `true` becomes `false`; `false`, `null`, empty containers and `0` are not changed. The reply is the number of values actually changed (T:2356-2413: `{"a":{},"b":{...},"c":1,"d":true,"e":null,"f":"d"}` with `$.*` returns 4 and result `{"a":{},"b":{},"c":0,"d":false,"e":null,"f":""}`).
- Missing path: legacy and v2 both return 0 in the tests (T:2377-2379 `JSON.CLEAR w .foo` gives 0). Missing key: `NONEXISTENT Document key does not exist`.
- Note: I did not re-verify the "double becomes 0.0" claim from my first read; treat as probable.

### JSON.TOGGLE key [path]

- Only booleans. Legacy: reply is the bulk string `true` or `false` (new value) (T:1374-1390). Non-boolean at the path: `WRONGTYPE JSON element is not a bool` (T:1399-1412 checks WRONGTYPE).
- v2: array with integer 0/1 per match (0 means the new value is false) and null for non-booleans (T:1414-1435: `[0,1,null,null,null,null,null]`).

### JSON.NUMINCRBY / JSON.NUMMULTBY key path number

- See 4.4. Legacy reply: bulk string (the last updated value). v2: bulk string holding a JSON array (`[null,2]`).

### JSON.STRLEN key [path]

- Byte length of the UTF-8 string. Missing key: null. Legacy missing path: `NONEXISTENT`. Wrong type at the path: WRONGTYPE.
- v2: array of integers, `null` for non-strings (T:1864-1889: `$.d.*` returns `[null,1,null]`).
- Legacy on several matches returns the first (T:1878-1887); `.d.*` legacy where the first match is not a string is an error (T:1888-1889 expects error or value; my read of the test is that it returns an error but the lines after 1889 were not read).

### JSON.STRAPPEND key [path] json-string

- Arity -3, so 3 or 4 args; with 3 args the path is `.`. The value must be a JSON string text (`"abc"` with quotes), else `WRONGTYPE Value is not a string` (T:1844-1849 for `123 true false null {} []`). Invalid JSON text gives the parse error.
- Reply: new length (legacy last updated value; v2 array of lengths, null for non-strings) (T:1809-1822).
- Target not a string: `WRONGTYPE JSON element is not a string`.

### JSON.OBJLEN key [path] and JSON.OBJKEYS key [path]

- OBJLEN: member count; missing key null; non-object `WRONGTYPE JSON element is not an object`. Clearing then OBJLEN gives 0 (T:2393-2394).
- OBJKEYS: legacy returns an array of key names of the first object match; v2 returns an array of arrays (empty array for an empty object). Missing or invalid path gives null per the first reading.

### JSON.ARRLEN key [path]

- Missing key null; legacy missing path `NONEXISTENT`; non-array `WRONGTYPE JSON element is not an array` (T:2083-2113). v2: `[0,null,2,3,null]` style (T:2115-2128).

### JSON.ARRAPPEND key path json [json ...]

- Arity -4. Returns the new length (v2: array of lengths). A missing key gives `NONEXISTENT Document key does not exist`; missing path `NONEXISTENT`; non-array WRONGTYPE (T:2130-2163). Each value must be valid JSON.
- v2 over `$..a` returns `[2,3,4]` and appends to every match (T:4094-4098).

### JSON.ARRINSERT key path index json [json ...]

- Arity -5. Index must be an integer. Negative index counts from the end; index outside `[0, size]` after normalisation gives `OUTOFBOUNDARIES Array index is out of bounds` (T:2313-2323: index 31 and 3000000000 on an 8-element array). Insert at index == size appends (T:2286: index 8 on 8 elements works). Reply: new length(s).
- v2 over several arrays: negative index -1 on arrays of length 0..3 gives `[[3],[3,0],[0,3,1],[0,1,3,2]]` (T:2339-2340: inserting before the last element; an empty array gets the element at 0).
- v2 where one match is out of bounds fails the whole command (T:2350-2354, `OUTOFBOUNDARIES`) and (I believe, not verified) applies nothing.

### JSON.ARRPOP key [path [index]]

- Arity -2, so up to 4 args. Path default `.`, index default -1. Non-integer index: `WRONGTYPE Value is not an integer`.
- Returns the popped element as bulk JSON text. Empty array: null (T:2184-2208). Out-of-range index is clamped (T:2229-2231 index 3000000000 does not error); negative counts from the end.
- v2: array with one bulk (or null) per match: `[null,"a","a","a"]` (T:2249-2255).

### JSON.ARRTRIM key path start stop

- Arity exactly 5. start and stop must be integers.
- start < 0 is clamped to 0 (valkey-json: `start=-1` acts as 0, T:2430 `-1 5` on `["Mary"]` gives len 1). stop >= size is clamped to size-1. If start >= size, or start > stop, the array is emptied and the reply is 0 (T:2431: `2 0` on a 2-element array gives 0 and `[]`). A negative stop is NOT adjusted from the end. This is a difference from RedisJSON, whose docs say `start < 0` counts from the end and negative `stop` counts from the end (from https://redis.io/docs/latest/commands/json.arrtrim/).
- Reply: new length (v2 array of lengths) (T:2427-2436, T:2467-2486).

### JSON.ARRINDEX key path value [start [stop]]

- Arity -4 (4 to 6 args). Returns the index of the first equal element or -1 (T:2488-2521).
- start default 0, negative start treated as 0. `stop` 0 or -1 means "to the end"; stop beyond size is clamped; start > stop returns -1 (T:2507-2513: `"Tom" 5 0` gives -1, `"Paul" 0 0` gives 3).
- Compares full JSON values (objects and arrays included, int vs double numerically) (T:2569-2580: `{"a":"b"}` found at 2, `[99,100]` at 3).
- v2: array of indexes per match, `null` for non-arrays (`-1` where not found) (T:2546-2567, T:2588-2611).
- Missing key: `NONEXISTENT Document key does not exist`; missing path `NONEXISTENT`; non-array WRONGTYPE.

### JSON.RESP key [path]

- Missing key: null. Legacy missing path: `NONEXISTENT JSON path does not exist` (T:2703-2707). v2: array of per-match replies; missing path gives `[]` (T:2716-2749).
- Value mapping: section 4.5.

### JSON.DEBUG MEMORY key [path] / FIELDS key [path] / DEPTH key / HELP

- MEMORY with the default path reports the stored document size; with another path walks the value. Missing key: null. Missing path: `NONEXISTENT` (T:2751-2762). Unknown subcommand (`MEMORY123`, `M`): `SYNTAXERR Unknown subcommand` (T:2769-2777).
- `JSON.DEBUG MEMORY` with no key: `wrong number of arguments...` (T:2764-2767).
- FIELDS counts the values under the path, not including the queried container itself (from dom.cc `dom_num_fields`). DEPTH: maximum depth, a root scalar is 0.

### RESP2 and RESP3

The module only uses the classic reply calls (array, integer, bulk string, simple string, null), so the logical shapes are identical on both protocols; RESP3 only changes the wire encoding of null (`_`) and arrays (`*`). No RESP3 map or double types are used (from json.cc reply helpers; summary of the first read). RedisJSON's docs describe RESP3 returns with the same shapes (from https://redis.io/docs/latest/commands/json.arrtrim/, https://redis.io/docs/latest/commands/json.get/).

---

## 6. Error strings

All verbatim from util.cc:15-66 unless a line is given. The reply is `-<FIRSTWORD> <rest>`; the first word is the error code.

| Code in source | Reply text | When |
|---|---|---|
| PARSE_ERROR | `SYNTAXERR Failed to parse JSON string due to syntax error` | invalid JSON text |
| NX_XX | `SYNTAXERR Option NX and XX should be mutually exclusive` | (declared; JSON.SET accepts one option only) |
| INVALID_JSON_PATH | `SYNTAXERR Invalid JSON path` | malformed path |
| INVALID_MEMBER_NAME | `SYNTAXERR Invalid object member name` | bad unquoted name |
| INVALID_NUMBER | `SYNTAXERR Invalid number` | bad index, step out of int64 |
| INVALID_IDENTIFIER | `SYNTAXERR Invalid identifier` | |
| INVALID_DOT_SEQUENCE | `SYNTAXERR Invalid dot sequence` | `...` |
| EMPTY_EXPR_TOKEN | `SYNTAXERR Expression token cannot be empty` | |
| ARRAY_INDEX_NOT_NUMBER | `SYNTAXERR Array index is not a number` | |
| STEP zero | `SYNTAXERR Step in the slice cannot be zero` | `[::0]` |
| INVALID_USE_OF_WILDCARD | `ERR Invalid use of wildcard` | |
| JSON_PATH_NOT_EXIST | `NONEXISTENT JSON path does not exist` | legacy path with no match |
| PARENT_ELEMENT_NOT_EXIST | `NONEXISTENT Parent element does not exist` | |
| DOCUMENT_KEY_NOT_FOUND | `NONEXISTENT Document key does not exist` | write command on a missing key |
| NOT_A_DOCUMENT_KEY | `WRONGTYPE Not a JSON document key` | |
| FAILED_TO_DELETE_VALUE | `OPFAIL Failed to delete JSON value` | |
| ELEMENT_NOT_NUMBER / BOOL / STRING / OBJECT / ARRAY | `WRONGTYPE JSON element is not a number` / `... is not a bool` / `... is not a string` / `... is not an object` / `... is not an array` | wrong type at path |
| VALUE_NOT_NUMBER / STRING / INTEGER | `WRONGTYPE Value is not a number` / `... is not a string` / `... is not an integer` | bad argument |
| PATH_SHOULD_BE_AT_THE_END | `SYNTAXERR Path arguments should be positioned at the end` | |
| COMMAND_SYNTAX_ERROR | `SYNTAXERR Command syntax error` | bad option, MSET new key non-root |
| MULTIPLICATION_OVERFLOW | `OVERFLOW Multiplication would overflow` | |
| ADDITION_OVERFLOW | `OVERFLOW Addition would overflow` | |
| EMPTY_JSON_OBJECT / ARRAY | `EMPTYVAL Empty JSON object` / `EMPTYVAL Empty JSON array` | internal; ARRPOP turns the array one into null |
| INDEX_OUT_OF_ARRAY_BOUNDARIES | `OUTOFBOUNDARIES Array index is out of bounds` | |
| UNKNOWN_SUBCOMMAND | `SYNTAXERR Unknown subcommand` | JSON.DEBUG |
| DOCUMENT_SIZE_LIMIT_EXCEEDED | `LIMIT Document size limit is exceeded` | |
| DOCUMENT_PATH_LIMIT_EXCEEDED | `LIMIT Document path nesting limit is exceeded` | |
| PARSER_RECURSION_DEPTH | `LIMIT Parser recursion depth is exceeded` | |
| RECURSIVE_DESCENT_TOKEN_LIMIT | `LIMIT Total number of recursive descent tokens in the query string exceeds the limit` | |
| QUERY_STRING_SIZE_LIMIT | `LIMIT Query string size limit is exceeded` | |
| CANNOT_INSERT_MEMBER_INTO_NON_OBJECT | `ERROR Cannot insert a member into a non-object value` | |
| INVALID_RDB_FORMAT | `ERROR Invalid value in RDB format` | |
| DOLLAR_CANNOT_APPLY_TO_NON_ROOT | `SYNTAXERR Dollar sign cannot apply to non-root element` | |
| ALLOCATION_FAILURE | `ERROR Memory Allocation failed ` (trailing space) | |
| KEY_OPEN_ERROR | `ERROR Unable to open valkeymodule key` | |
| (json.cc:51-52) | `SYNTAXERR A new Valkey key's path must be root` | new key with non-root path (SET, MERGE) |
| (json.cc:51-52) | `NONEXISTENT JSON document is not found` | only if the key opens but has no value (practically unreachable) |
| JSON.DEBUG | `ERR unknown subcommand 'KEYTABLE-CORRUPT'. Try JSON.DEBUG HELP.` (and CLEAR, DISTRIBUTION) | when `json.debug-mode` is off |

Standard server errors that also apply: arity (`wrong number of arguments for 'json.xxx' command`), and `WRONGTYPE Operation against a key holding the wrong kind of value` for a non-JSON key (T:4900-4902 shows an error on a string key).

Missing-key behaviour summary:

| Reply on a missing key | Commands |
|---|---|
| null | GET, TYPE, STRLEN, OBJLEN, OBJKEYS, ARRLEN, RESP, DEBUG MEMORY/FIELDS/DEPTH |
| array of nulls | MGET |
| 0 | DEL, FORGET |
| `NONEXISTENT Document key does not exist` | ARRAPPEND, ARRINSERT, ARRPOP, ARRTRIM, ARRINDEX, CLEAR, TOGGLE, NUMINCRBY, NUMMULTBY, STRAPPEND |
| null (XX) or `SYNTAXERR A new Valkey key's path must be root` | SET non-root |

---

## 7. Differences: valkey-json 1.1.0 vs RedisJSON

| Area | valkey-json | RedisJSON | Source |
|---|---|---|---|
| Default path | `.` (legacy; bare value reply) | `$` per the docs (JSONPath; array reply) | json.cc; (from https://redis.io/docs/latest/commands/json.get/) |
| Filter operators | `== != < <= > >= && \|\|` and parentheses | adds `=~`, `!`, `in`, `nin`, `size`, `empty`, `subsetof`, `anyof`, `noneof`, arithmetic, many functions, `~` | (from https://valkey.io/topics/valkey-json/ and https://redis.io/docs/latest/develop/data-types/json/path/) |
| Error for new key at non-root path | `SYNTAXERR A new Valkey key's path must be root` | `ERR new objects must be created at the root` | json.cc:51-52; (from https://redis.io/docs/latest/commands/json.set/) |
| JSON.SET missing intermediate member | `NONEXISTENT JSON path does not exist` | nil | selector.cc; (from https://redis.io/docs/latest/commands/json.set/) |
| JSON.SET path outside array | `OUTOFBOUNDARIES Array index is out of bounds` | `ERR index out of bounds` | same |
| JSON.SET options | NX / XX only | also `FPHA <FP16\|BF16\|FP32\|FP64>` (Redis 8.8) | (from https://redis.io/docs/latest/commands/json.set/) |
| ARRTRIM negative start/stop | start < 0 treated as 0; negative stop not adjusted | negative start/stop count from the end | dom.cc `dom_array_trim`; (from https://redis.io/docs/latest/commands/json.arrtrim/) |
| ARRTRIM defaults | all four args required (arity 5) | docs list path/start/stop as optional with defaults | json.cc; (from https://redis.io/docs/latest/commands/json.arrtrim/) |
| Doubles | stored and printed as the original text; computed values use `%.17g` | normalised doubles (not fetched, not determined) | document.h, util.cc |
| JSON.DEL `$..a` nested | deletes 4 where RedisJSON deletes 1 | 1 | T:4106-4111 |
| COPY | works | not supported by RedisJSON per a test comment | T:2330 |
| MEMORY/RDB | type name `ReJSON-RL`, RDB encoding 3 (compatible loads of 1 and 3) | same type name | json.cc:40-49 |
| Unsupported syntax in valkey-json | the functions and `~` above | supported | as above |
| Config names | `json.max-document-size`, `json.max-path-limit`, `json.debug-mode` | RedisJSON uses `JSON.MAX_PATH_LIMIT` style module args (not fetched, not determined) | json.cc:2705-2721 |
| DEBUG | MEMORY, FIELDS, DEPTH, HELP and dev subcommands | MEMORY, DOCS, HELP (not fetched, not determined) | json.cc |

---

## 8. Test vectors

Format: `>` is the request (valkey-cli style), `<` the RESP2 reply in `--no-raw` style. "S" = source-traced from a test in the tarball, with a line. "D" = doc-only. Unless stated, vectors in a group continue on the same key. Where a test checks the reply only by class (for example "is a WRONGTYPE error") the vector says `(error) WRONGTYPE ...` and is flagged "class only".

### Basic set/get

```
V01 S T:~190-770 (sanity)
> JSON.SET k . '{"a":"1","b":"2","c":"3"}'
< OK
V02 S
> JSON.GET k .a
< "\"1\""
V03 S
> JSON.GET k $.*
< "[\"1\",\"2\",\"3\"]"
V04 S T:~190 (sanity)
> JSON.SET arr . '[1,2,3,4,5]'
< OK
> JSON.GET arr [-1]
< "5"
V05 S
> JSON.GET arr $.[0:3]
< "[1,2,3]"
V06 S
> JSON.MGET k arr $
< 1) "[{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}]"
< 2) "[[1,2,3,4,5]]"
V07 S T:1139-1143
> JSON.SET a . '{"foo":"bar1"}'
< OK
> JSON.MGET a missing .foo
< 1) "\"bar1\""
< 2) (nil)
V08 S T:1142
> JSON.MGET a a .bar
< 1) (nil)
< 2) (nil)
V09 S
> JSON.SET newkey .bar '"bar"'
< (error) SYNTAXERR A new Valkey key's path must be root   (class only in test)
V10 S
> JSON.SET k . x NXXX
< (error) SYNTAXERR Command syntax error   (class only)
V11 S T:~600-700 (custom format)
> JSON.SET k1 . '{"a":"1","b":"2","c":"3"}'
> JSON.GET k1 INDENT " " .
< "{ \"a\":\"1\", \"b\":\"2\", \"c\":\"3\"}"   (test expectation from the summary of test_sanity; newline-free)
V12 S
> JSON.GET k1 SPACE " " .
< "{\"a\": \"1\",\"b\": \"2\",\"c\": \"3\"}"
V13 S T:3141-3154
> JSON.SET q . '{"\"a":1,"\"b":2}'
> JSON.GET q space " " .
< "{\"\\\"a\": 1,\"\\\"b\": 2}"
V14 S
> JSON.GET missingkey .a
< (nil)
V15 S
> JSON.GET k .foo
< (error) NONEXISTENT JSON path does not exist   (class only)
```

Note on V11: the exact whitespace of the INDENT-only output was transcribed from my notes of the test; the rule is in 4.3. Re-check against the test before using it as an exact oracle.

### Legacy vs JSONPath replies

```
V16 S T:790-806
> JSON.SET w . '{"address":{"street":"21 2nd Street","city":"New York","state":"NY","zip":"10021-3100"}}'
> JSON.GET w $.address.*
< "[\"21 2nd Street\",\"New York\",\"NY\",\"10021-3100\"]"
V17 S
> JSON.GET w .address.*
< "\"21 2nd Street\""
V18 S T:823-837
> JSON.SET c . '{"a":[], "b":[1], "c":[1,2]}'
> JSON.GET c $.a[*]
< "[]"
V19 S T:861-869
> JSON.GET c .a[*]
< (error) NONEXISTENT JSON path does not exist
V20 S T:1076-1129
> JSON.GET c .b[*] .c[*]
< "{\".b[*]\":1,\".c[*]\":1}"
V21 S
> JSON.GET c .a[*] .b[*]
< (error) NONEXISTENT JSON path does not exist
V22 S
> JSON.GET c $.a[*] $.b[*] $.c[*]
< "{\"$.a[*]\":[],\"$.b[*]\":[1],\"$.c[*]\":[1,2]}"
V23 S
> JSON.GET c .a[*] $.b[*] .c[*]
< "{\".a[*]\":[],\"$.b[*]\":[1],\".c[*]\":[1,2]}"
V24 D (https://redis.io/docs/latest/commands/json.get/)
> JSON.SET doc $ '{"a":2, "b": 3, "nested": {"a": 4, "b": null}}'
> JSON.GET doc $..b
< "[3,null]"
```

### Paths: indexes, slices, unions

```
V25 S T:954-1011 (k = [0,1,2,3,4,5,6,7,8,9])
> JSON.GET k $[0:-1]
< "[0,1,2,3,4,5,6,7,8]"
V26 S
> JSON.GET k $[6:0:-1]
< "[6,5,4,3,2,1]"
V27 S
> JSON.GET k $[6::-1]
< "[]"
V28 S
> JSON.GET k $[::2]
< "[0,2,4,6,8]"
V29 S
> JSON.GET k $[1:3:9223372036854775807]
< "[1]"
V30 S
> JSON.GET k $[0:2:9223372036854775808]
< (error) SYNTAXERR Invalid number   (message matched in test)
V31 S T:1028-1041
> JSON.GET k $[0,1,5,0,1,2]
< "[0,1,5,0,1,2]"
V32 S
> JSON.GET k $[-10,-9,-8,0,1,2,-1000,1000]
< "[0,1,2,0,1,2]"
V33 S T:1056-1074
> JSON.GET k $[0,,4]
< (error) SYNTAXERR ...   (class only)
V34 S T:871-905 (wikipedia doc, phoneNumbers has 2 elements)
> JSON.GET w .phoneNumbers[2].type
< (error) OUTOFBOUNDARIES Array index is out of bounds   (class only)
V35 S T:925-947
> JSON.GET w $.phoneNumbers[2].type
< "[]"
V36 S T:1350-1372
> JSON.SET n . 9223372036854775808
> JSON.TYPE n .
< "number"
V37 S
> JSON.SET n . 9223372036854775807
> JSON.TYPE n .
< "integer"
```

### Filters and recursive descent

```
V38 S T:3918 (bookstore doc from the test)
> JSON.GET s '$.store.books[?(@.isbn)].price'
< "[9,22.99]"
V39 S T:3932
> JSON.GET s '$.store.books[?(@.price<1.0E1)].price'
< "[8.95,9]"
V40 S T:3955-3959
> JSON.GET s '$.store.books[?(@.price<9||@.price>10&&@.isbn)].price'
< "[8.95,22.99]"
> JSON.GET s '$.store.books[?((@.price<9||@.price>10)&&@.isbn)].price'
< "[22.99]"
V41 S T:3977-3980 (k2 = [1,2,3,4,5])
> JSON.GET k2 '$.*[?(@ != 3)]'
< "[1,2,4,5]"
V42 S T:3981 (k3 = [true,false,true,false,null,1,2,3,4])
> JSON.GET k3 '$[*][?(@==true)]'
< "[true,true]"
V43 S T:4070-4076
> JSON.SET r . '{"a":{"a":{"a":{"a":1}}}}'
> JSON.GET r $..a
< "[{\"a\":{\"a\":{\"a\":1}}},{\"a\":{\"a\":1}},{\"a\":1},1]"
V44 S T:4078
> JSON.SET r4 . '{"a":{"b":{"z":{"y":1}}, "c":{"z":{"y":2}}, "z":{"y":3}}}'
> JSON.GET r4 $.a..z.y
< "[3,1,2]"
V45 S T:4081
> JSON.SET r5 . '{"a":1,"b":{"e":[0,1,2]},"c":{"e":[10,11,12]}}'
> JSON.GET r5 $..e[0:2]
< "[0,1,10,11]"
V46 S T:4013-4025
> JSON.SET s '$.store.books[?(@.price<10.0)].price' 10.01
< OK
> JSON.GET s '$.store.books[?(@.price<=10.02)].price'
< "[10.01,10.01]"
```

### Numbers

```
V47 S T:1437-1451 (wikipedia doc with .age 27... note the test starts at 27)
> JSON.NUMINCRBY w .age 1
< "28"
V48 S
> JSON.SET w .foo 1
> JSON.NUMINCRBY w .foo 0.5
< "1.5"
> JSON.NUMINCRBY w .foo 0.5
< "2"
V49 S T:1471-1487
> JSON.NUMMULTBY w .age 10
< "270"
> JSON.NUMMULTBY w .age 0.01
< "27"
V50 S T:1480-1487
> JSON.SET w .foo 1
> JSON.NUMMULTBY w .foo 0.5
< "0.5"
> JSON.NUMMULTBY w .foo 0.5
< "0.25"
> JSON.NUMMULTBY w .foo 4
< "1"
V51 S T:1507-1544
> JSON.SET w .foo 2e5
> JSON.NUMMULTBY w .foo 1
> JSON.GET w .foo
< "200000"
V52 S
> JSON.SET w .foo 2E-30
> JSON.NUMINCRBY w .foo 0
> JSON.GET w .foo
< "2.0000000000000002e-30"
V53 S T:1507
> JSON.SET w .foo 2.50000
> JSON.GET w .foo
< "2.50000"
V54 S T:1704-1731
> JSON.SET w .foo 1.79e+308
> JSON.NUMMULTBY w .foo 1.79e308
< (error) OVERFLOW Multiplication would overflow   (class only)
V55 S T:1546-1578
> JSON.NUMINCRBY w .age 2.0.
< (error) WRONGTYPE Value is not a number   (class only)
V56 S T:3186-3193
> JSON.SET d . '{"a":{"a":"a"},"b":{"a":"a","b":1}}'
> JSON.NUMINCRBY d $.b.* 1
< "[null,2]"
V57 S T:3236-3241 (k1 = {"d":[1,2,3]...} after earlier steps)
> JSON.NUMINCRBY k1 .d.* 1
< "4"
> JSON.GET k1 .d.*
< "2"
V58 S T:3210-3217
> JSON.NUMINCRBY k1 .a.* 1
< (error) NONEXISTENT JSON path does not exist   (class only)
V59 S T:3225-3231
> JSON.NUMINCRBY k3 .a.* 1
< (error) WRONGTYPE JSON element is not a number   (class only)
```

### Strings and objects

```
V60 S T:1809-1822
> JSON.SET w .firstName '"John"'
> JSON.STRAPPEND w .firstName '"son"'
< (integer) 7
V61 S T:1844-1849
> JSON.STRAPPEND w .firstName 123
< (error) WRONGTYPE Value is not a string   (class only)
V62 S T:1864-1889
> JSON.SET s . '{"d":{"a":1,"b":"b","c":3}}'
> JSON.STRLEN s $.d.*
< 1) (nil)
< 2) (integer) 1
< 3) (nil)
V63 S T:2817-2840
> JSON.SET dup . '{"a":0,"a":1}'
> JSON.GET dup .
< "{\"a\":1}"
> JSON.OBJLEN dup
< (integer) 1
V64 S T:3114-3139
> JSON.SET e . '{"b\tb":2,"":7}'
> JSON.GET e '$["b\tb"]'
< "[2]"
V65 S T:3114-3139
> JSON.GET e '$[""]'
< "[7]"
```

### Arrays

```
V66 S T:2130-2143
> JSON.SET a . '{"children":[]}'
> JSON.ARRAPPEND a .children '"John"'
< (integer) 1
> JSON.ARRAPPEND a .children '"Mary"' '"Tom"'
< (integer) 3
V67 S T:2184-2208
> JSON.ARRPOP a .children 1
< "\"Mary\""
> JSON.ARRPOP a .children -1
< "\"Tom\""
V68 S T:2184-2188
> JSON.SET e . '{"children":[]}'
> JSON.ARRPOP e .children
< (nil)
V69 S T:2243-2255
> JSON.SET p . '[[], ["a"], ["a","b"], ["a","b","c"]]'
> JSON.ARRPOP p $.[*] 0
< 1) (nil)
< 2) "\"a\""
< 3) "\"a\""
< 4) "\"a\""
V70 S T:2262-2292 (children = ["John","Mary","Tom"])
> JSON.ARRINSERT a .children -1 '"Peter"'
< (integer) 4
> JSON.GET a .children
< "[\"John\",\"Mary\",\"Peter\",\"Tom\"]"
V71 S T:2313-2323
> JSON.ARRINSERT a .children 31 '"a"'
< (error) OUTOFBOUNDARIES Array index is out of bounds   (class only)
V72 S T:2325-2348
> JSON.SET q . '[[], [0], [0,1], [0,1,2]]'
> JSON.ARRINSERT q $.[*] -1 3
< 1) (integer) 1
< 2) (integer) 2
< 3) (integer) 3
< 4) (integer) 4
> JSON.GET q .
< "[[3],[3,0],[0,3,1],[0,1,3,2]]"
V73 S T:2415-2436
> JSON.SET t . '{"children":["John","Mary","Tom"]}'
> JSON.ARRTRIM t .children 1 2
< (integer) 2
> JSON.ARRTRIM t .children 0 0
< (integer) 1
> JSON.ARRTRIM t .children -1 5
< (integer) 1
V74 S T:2467-2486
> JSON.SET tt . '[[], ["a"], ["a","b"], ["a","b","c"]]'
> JSON.ARRTRIM tt $.[*] 0 1
< 1) (integer) 0
< 2) (integer) 1
< 3) (integer) 2
< 4) (integer) 2
V75 S T:2488-2513
> JSON.SET i . '{"children":["John","Mary","Tom","Paul","Peter"]}'
> JSON.ARRINDEX i .children '"Tom"'
< (integer) 2
> JSON.ARRINDEX i .children '"Tom"' 5 0
< (integer) -1
> JSON.ARRINDEX i .children '"Paul"' 0 0
< (integer) 3
V76 S T:2569-2580
> JSON.SET o . '[5, 6, {"a":"b"}, [99,100]]'
> JSON.ARRINDEX o . '{"a":"b"}' 0 0
< (integer) 2
V77 S T:2115-2128
> JSON.SET al . '[[], "a", ["a","b"], ["a","b","c"], 4]'
> JSON.ARRLEN al $.[*]
< 1) (integer) 0
< 2) (nil)
< 3) (integer) 2
< 4) (integer) 3
< 5) (nil)
V78 S T:2083-2107
> JSON.ARRLEN w .firstName
< (error) WRONGTYPE JSON element is not an array   (class only)
V79 S T:4094-4098
> JSON.SET ap . '{"a":[1], "b": {"a": [2,3]}, "c": {"a": [4,5,6]}}'
> JSON.ARRAPPEND ap $..a 0
< 1) (integer) 2
< 2) (integer) 3
< 3) (integer) 4
V80 S T:2396-2413
> JSON.SET cl . '[[], [0], [0,1], [0,1,2], 1, true, null, "d"]'
> JSON.CLEAR cl $[*]
< (integer) 6
> JSON.GET cl .
< "[[],[],[],[],0,false,null,\"\"]"
```

### Toggle, type, resp, delete

```
V81 S T:1374-1390
> JSON.SET w .foobool false
> JSON.TOGGLE w .foobool
< "true"
V82 S T:1414-1435
> JSON.SET tg . '[true, false, 1, null, "foo", [], {}]'
> JSON.TOGGLE tg $[*]
< 1) (integer) 0
< 2) (integer) 1
< 3) (nil)
< 4) (nil)
< 5) (nil)
< 6) (nil)
< 7) (nil)
V83 S T:2643-2658
> JSON.SET ty . '{"a":1, "b":2.3, "c":"foo", "d":true, "e":null, "f":{}, "g":[]}'
> JSON.TYPE ty $.*
< 1) "integer"
< 2) "number"
< 3) "string"
< 4) "boolean"
< 5) "null"
< 6) "object"
< 7) "array"
V84 S T:2613-2634
> JSON.TYPE w .foo
< (nil)
> JSON.TYPE nosuchkey
< (nil)
V85 S T:2660-2672
> JSON.RESP w .isAlive
< "true"
> JSON.RESP w .age
< (integer) 27
> JSON.RESP w .spouse
< (nil)
V86 S T:2675-2698
> JSON.RESP w .phoneNumbers
< 1) "["
< 2) 1) "{"
<    2) 1) "type"
<       2) "home"
<    3) 1) "number"
<       2) "212 555-1234"
< 3) ... (second object, same layout)
V87 S T:2716-2724
> JSON.RESP w $.foo
< (empty array)
V88 S T:2703-2707
> JSON.RESP w .foo
< (error) NONEXISTENT JSON path does not exist   (class only)
V89 S T:1246-1269
> JSON.SET dl . '{"x": {}, "y": {"a":"a"}, "z": {"a":"", "b":"b"}}'
> JSON.DEL dl $.z.*
< (integer) 2
> JSON.GET dl
< "{\"x\":{},\"y\":{\"a\":\"a\"},\"z\":{}}"
V90 S T:1281-1284
> JSON.SET d4 . '[0,1,2,3,4,5,6,7,8,9]'
> JSON.DEL d4 $[3:7]
< (integer) 4
> JSON.GET d4
< "[0,1,2,7,8,9]"
V91 S T:1285-1288
> JSON.SET d5 . '[0,1,2,3,4,5,6,7,8,9]'
> JSON.DEL d5 $[1,5,7,8]
< (integer) 4
> JSON.GET d5
< "[0,2,3,4,6,9]"
V92 S T:1216-1231
> JSON.DEL w
< (integer) 1
> JSON.DEL nosuchkey .a
< (integer) 0
V93 S T:4100-4111
> JSON.SET k3r . '{"x": {}, "y": {"a":"a"}, "z": {"a":"", "b":"b"}}'
> JSON.DEL k3r $..a
< (integer) 2
V94 S T:4106-4111
> JSON.SET k2r . '{"a":{"a":{"a":{"a":1}}}}'
> JSON.DEL k2r $..a
< (integer) 4
```

### Merge, limits, debug

```
V95 S T:4790-4801
> JSON.MERGE m . '{"a":1,"b":2}'
< OK
> JSON.MERGE m . '{"b":3,"c":4}'
< OK
> JSON.GET m
< "{\"a\":1,\"b\":3,\"c\":4}"
V96 S T:5229-5236
> JSON.MERGE mn . '{"a":null,"b":1}'
< OK
> JSON.GET mn
< "{\"b\":1}"
V97 S T:5262-5270
> JSON.SET ms . '{"a":"b"}'
> JSON.MERGE ms . '{"a":{"x":null,"y":1}}'
< OK
> JSON.GET ms
< "{\"a\":{\"y\":1}}"
V98 S T:5284
> JSON.SET mr . '{"a":"foo"}'
> JSON.MERGE mr . null
< OK
> JSON.GET mr
< "null"
V99 S T:5176-5184
> JSON.SET mo . '{"a":{"a":1}}'
> JSON.MERGE mo $..a 5
< OK
> JSON.GET mo
< "{\"a\":5}"
V100 S T:2878-2891
> JSON.MSET w .address.city '"Boston"' w .age 30
< OK
V101 S T:2918-2924
> JSON.MSET brandnew .bar '"bar"'
< (error) SYNTAXERR Command syntax error   (class only)
V102 S T:2751-2788
> JSON.DEBUG MEMORY nosuchkey
< (nil)
> JSON.DEBUG MEMORY123 w
< (error) SYNTAXERR Unknown subcommand   (class only)
V103 D (https://redis.io/docs/latest/commands/json.set/, RedisJSON; valkey differs)
> JSON.SET doc $ 1
> JSON.SET doc $.x.y 2
< (nil)        (RedisJSON; valkey-json replies NONEXISTENT JSON path does not exist)
V104 D (https://redis.io/docs/latest/commands/json.arrtrim/, RedisJSON; valkey differs for negatives)
> JSON.ARRTRIM key $.[1].max_level 4 8
< 1) (integer) 5
```

V103 and V104 are RedisJSON doc examples and are marked doc-only. Do not use them as valkey-json oracles. V104's call shape is valid in valkey-json (ARRTRIM with non-negative ranges behaves the same), but I did not run it.

Vector counts: 104 pairs, of which 102 are source-traced from tests (several run several commands) and 2 are doc-only. A strict count of single request/reply pairs is higher than 104 because some vectors hold two or three requests.

---

## 9. Implementation notes for HCache (derived from the above)

1. Store doubles as their original text and keep a "double" flag; ints as i64. Computed doubles use `%.17g` (C `printf`). Rust's `{}` for f64 is not the same format. Needs a `%.17g` clone (Valkey-json even prints a whole-valued double product as an integer text).
2. Objects: keep insertion order; duplicate keys on parse keep the first position and the last value.
3. Path engine: one parser for both kinds; the `isV2` flag selects reply and error behaviour (3.1, 3.4, 3.5).
4. Writes resolve matches first, then apply deepest-first (arrays) so that indexes stay valid; dedupe matches by node identity.
5. NX/XX: NX fails if any match exists; XX fails if nothing matched or the path would insert.
6. JSON.MSET validates everything before applying; JSON.MERGE is atomic across all targets.
7. Replicate and AOF as verbatim commands; AOF rewrite emits `JSON.SET key . <compact>`.
8. RDB: the type name is `ReJSON-RL`, encver 3, payload = compact JSON string.

---

## 10. Items NOT determined

1. Order of the multi-path JSON.GET object: path order or hash order (T:1346 says non-deterministic; the code that builds it, dom.cc `buildJsonForMultiPaths`, was read but I did not record the container type).
2. Whether member deletion preserves order: depends on `rapidjson::Pointer::Erase`, in `pointer.h`, which is not in the tarball (it comes from an upstream copy at build time). `DoRemoveMemberVec` in the vendored document.h is marked broken and reorders.
3. TEST-SHARED-API arity: -2 in the test (T:5361) versus 4 in my first reading of json.cc.
4. Exact INDENT-only output whitespace (V11). Rule in 4.3 but the literal expected string was copied from notes.
5. Exact depth boundary: 127 accepted, 128 behaviour not pinned by a test (the parser checks depth against `json.max-path-limit` in `IncrDepth`).
6. `JSON.CLEAR` on a double (`0.0` vs `0`); and the exact `JSON.OBJKEYS` reply for a missing path in legacy mode.
7. The remaining 5-20% of `test_json_basic.py` I only skimmed or did not read (lines 2975-3107 mset atomicity, 3320-3855, 4186-4789, 5373 onward) and `tst/unit/*.cc`; edge cases there may add behaviour.
8. RESP3 wire differences: assumed none beyond null/array encoding since the module only calls classic reply functions.
9. RedisJSON: the pages for JSON.MERGE, JSON.MSET, JSON.DEBUG, JSON.NUMINCRBY and the full legacy-path rules were not fetched (the path page returned only its first 100000 of 352976 characters); differences for those are unconfirmed. The valkey.io per-command pages other than JSON.SET were not fetched.
10. `JSON.DEBUG MEMORY` numbers (allocator dependent), `MEMORY USAGE` numbers, defrag behaviour and key table internals.
11. No live module was run, so every reply above is from reading code, tests, or docs, not from observing a server. Doc pages came through a summarising fetcher, so quoted doc text is a paraphrase.
