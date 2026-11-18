# Bloom filter commands: behaviour spec for Hotstash (target: valkey-bloom 1.0.1)

Written 2026-11-17. Target: **valkey-bloom 1.0.1** (latest release, tag `1.0.1`, commit `43b4e70`, released 2026-02-23 per `00-RELEASENOTES`), the official Valkey module. RedisBloom differences are in section 13.

## 0. How to read this spec

Sources, all read only, never built or run:

| Tag | Source |
|---|---|
| `VB:` | valkey-bloom source at commit `43b4e70` (<https://github.com/valkey-io/valkey-bloom>). Paths are relative to it, e.g. `src/bloom/command_handler.rs:337`. |
| `BFCRATE:` | Rust crate `bloomfilter` 3.0.1 from crates.io. valkey-bloom's `Cargo.toml` pins `bloomfilter = 3.0.1` with the serde feature. It does the sizing, hashing and bit layout. |
| `SIPCRATE:` | Rust crate `siphasher` 1.0.1 from crates.io. It is `bloomfilter`'s dependency. |
| `VMOD:` | Rust crate `valkey-module` 0.1.5 from crates.io. It defines how Rust values become RESP replies. |
| `RB:` | RedisBloom master, `rebloom.c`, `sb.c`, `bloom.c`, `config.c` (<https://github.com/RedisBloom/RedisBloom>), read for comparison only. |
| `docs:` | valkey.io and redis.io pages (WebFetch, so summarised by a small model; treat wording as paraphrase). |
| `TEST:` | valkey-bloom integration tests under `tests/*.py`. They show behaviour the maintainers assert against a real server. |
| `DERIVED:` | My own Python reimplementation of the algorithm (not in this repository). It reproduces the upstream constants 384, 26214300, 32766000, 20470000, 1627600/1627601 and 101600/101601 exactly. Anything outside those constants is **not** confirmed against a running module. |

Server-side facts that are not in the module (how `ReplyWithError` frames a message, what MEMORY USAGE adds) are marked "(server knowledge)". They are less certain than the rest.

Confidence tags used in the test vectors in section 15:
- **S-test**: the upstream integration test asserts it, and it also follows from the code.
- **S-code**: follows from the code, with no upstream test.
- **D**: documentation only.
- **X**: derived by simulation or a formula I reimplemented. It is not confirmed against a real module.

Module facts (`VB: src/lib.rs:16-22`): module name `bf`, version 10001, stage "ga". It requires Valkey >= 8.0 (`src/configs.rs:73`, `src/lib.rs:24-41`). It is a Rust cdylib that registers one data type (`bloomfltr`) and 9 commands.

---

## 1. Command inventory

valkey-bloom 1.0.1 registers exactly these nine commands (`VB: src/lib.rs:117-125`, README "Supported commands"):

`BF.ADD`, `BF.MADD`, `BF.EXISTS`, `BF.MEXISTS`, `BF.CARD`, `BF.RESERVE`, `BF.INSERT`, `BF.INFO`, `BF.LOAD`.

There are **no** `BF.SCANDUMP`, `BF.LOADCHUNK` or `BF.DEBUG` (RedisBloom has them). `BF.LOAD` exists only so AOF rewrite can emit one self-contained command (`VB: src/wrapper/bloom_callback.rs:67-90`).

Registration (`VB: src/lib.rs:117-125`), format `[name, handler, flags, first_key, last_key, step, acl_categories]`:

| Command | Module flags | COMMAND INFO flags (TEST) | Key spec (first,last,step) | ACL categories |
|---|---|---|---|---|
| BF.ADD | `write fast deny-oom` | write, denyoom, module, fast | 1,1,1 | @fast @write @bloom |
| BF.MADD | `write fast deny-oom` | same | 1,1,1 | @fast @write @bloom |
| BF.EXISTS | `readonly fast` | readonly, module, fast | 1,1,1 | @fast @read @bloom |
| BF.MEXISTS | `readonly fast` | same | 1,1,1 | @fast @read @bloom |
| BF.CARD | `readonly fast` | same | 1,1,1 | @fast @read @bloom |
| BF.RESERVE | `write fast deny-oom` | write, denyoom, module, fast | 1,1,1 | @fast @write @bloom |
| BF.INFO | `readonly fast` | readonly, module, fast | 1,1,1 | @fast @read @bloom |
| BF.INSERT | `write fast deny-oom` | write, denyoom, module, fast | 1,1,1 | @fast @write @bloom |
| BF.LOAD | `write deny-oom` | write, denyoom, module (no fast) | 1,1,1 | @write @bloom |

- ACL category `bloom` is created with `acl_categories: ["bloom"]` (`VB: src/lib.rs:113-115`). `COMMAND LIST FILTERBY ACLCAT bloom` lists all nine. A user with `-@bloom` gets "has no permissions to run the 'BF.ADD' command" (`TEST: tests/test_bloom_acl_category.py:60-83`).
- **Arity as the server sees it is -1 for every command** (`TEST: tests/test_bloom_command.py:6-15`, which checks 8 of the 9; BF.LOAD is not checked). Each handler does its own argument count check. The `src/commands/*.json` files declare arities 3, 3, 3, 3, 2, -4, -2, -2, 3 for ADD, MADD, EXISTS, MEXISTS, CARD, RESERVE, INFO, INSERT, LOAD (`VB: src/commands/bf.*.json`). The tests show the runtime value is -1, so the JSON is documentation only.
- Consequence for Hotstash: the server-level arity check never fires. Wrong-arg-count errors come from the handlers, in the order "OOM and ACL checks first, then arity". The text is the standard one with the registered upper-case name: `ERR wrong number of arguments for 'BF.ADD' command` (`TEST: tests/test_bloom_command.py:75-111`; a lowercase `bf.card` still reports `'BF.CARD'`).
- `deny-oom` means that under `maxmemory` pressure the write commands are rejected by the server with `OOM command not allowed when used memory > 'maxmemory'.` (`TEST: tests/test_bloom_basic.py:131-142`). BF.EXISTS, BF.INFO and BF.CARD are unaffected.

---

## 2. Argument parsing rules (shared by all commands)

### 2.1 Case-insensitivity and tokens
- Option tokens (`ERROR`, `CAPACITY`, `EXPANSION`, `NONSCALING`, `NOCREATE`, `ITEMS`, `SEED`, `TIGHTENING`, `VALIDATESCALETO`) and BF.INFO field names are compared as `to_string_lossy().to_uppercase()`. They are fully case-insensitive and need the whole token (`VB: src/bloom/command_handler.rs:371,471,712-716`). RedisBloom accepts abbreviations; valkey-bloom does not.
- Non-UTF-8 option bytes become U+FFFD and match nothing, so BF.INSERT replies `unknown argument received`.
- **Items are binary-safe raw bytes** and are never case-folded. The command-name argument and the key are raw bytes.

### 2.2 Numeric parsing (Rust `str::parse`, applied to the lossy-UTF-8 string)

| Argument | Parser | Accepted | Rejected / special |
|---|---|---|---|
| error_rate (`ERROR`, RESERVE arg 2) | `f64::from_str` | `0.01`, `1e-3`, `1E-3`, `.5`, `5.`, `+0.5`, `0.99999999999999999` (rounds to 1.0, so it is then out of range), denormals such as `1e-310` | `inf`, `infinity`, `nan` (any case) parse OK but fail the range test (see below). `0x1p-3`, `1_0`, ` 0.5` (space), `""` and `0,5` fail to parse. |
| tightening (`TIGHTENING`) | `f64::from_str` | same as above | same |
| capacity (`CAPACITY`, RESERVE arg 3, `VALIDATESCALETO`) | `i64::from_str` | `100`, `+100`, `9223372036854775807` | `-1`, `1.5`, `1e3`, `9223372036854775808` (overflow) all give `bad capacity`. `0` gives its own message. |
| expansion (`EXPANSION`) | `u32::from_str` | `1` ... `4294967295`, `+2` | `0`, `-1`, `4294967296`, `0.99`, `x` all give `bad expansion`. |
| seed (`SEED`) | none | exactly 32 raw bytes | any other length gives `invalid seed` |

Range tests (`VB: src/configs.rs:11-27`):
- error_rate must satisfy `0.0 < x < 1.0` strictly. Parse OK but out of range, including NaN, inf, 0, 1 and negatives, gives `ERR (0 < error rate range < 1)`. A parse failure gives `ERR bad error rate`.
- tightening must satisfy `0.0 < x < 1.0` strictly. Out of range gives `ERR (0 < tightening ratio range < 1)`. A parse failure gives `ERR bad tightening ratio`.
- capacity is accepted in `1..=i64::MAX`. `0` gives `ERR (capacity should be larger than 0)`. Negative values and parse failures give `ERR bad capacity`. There is no explicit upper cap other than the memory limit (section 7.4), so `i64::MAX` gets `ERR operation exceeds bloom object memory limit`.
- expansion is accepted in `1..=u32::MAX` (`BLOOM_EXPANSION_MIN = 1`). **`NONSCALING` is represented internally as expansion = 0.**

The match structure is "parse OK and in range, else parse OK and out of range, else any error". The parse-error arm and the range arm are distinct: `abc` gives "bad ...", `2` gives the range message (`VB: src/bloom/command_handler.rs:347-366`).

### 2.3 Duplicate options and ordering
- BF.INSERT: options are parsed left to right and **the last occurrence wins** (for example `CAPACITY 10 CAPACITY 20` gives 20). Every occurrence is validated even if later overridden. `NONSCALING` sets expansion to 0 and a later `EXPANSION n` sets it back to n, so the last one wins. `NOCREATE` is a sticky flag.
- `ITEMS` ends option parsing. Everything after it is an item, even tokens that look like options. `BF.INSERT k ITEMS 1 2 3 EXPANSION 2` adds 5 items (`TEST: tests/test_bloom_command.py:105`).
- A value-taking option at the end of the arguments (`ERROR`, `CAPACITY`, `EXPANSION`, `TIGHTENING`, `SEED`, `VALIDATESCALETO` with nothing after it) gives the wrong-arity error.
- `ITEMS` with nothing after it gives the wrong-arity error. No `ITEMS` at all is allowed and means "zero items".
- BF.RESERVE accepts at most **one** trailing option (section 3.6).

### 2.4 Check order (matters for which error wins)
1. Handler arity check.
2. Argument parsing and validation, in argument order. Errors here win over everything after.
3. BF.INSERT only: the `VALIDATESCALETO` simulation. It runs *before* the key is opened, so it can fail even when the key already exists.
4. Open the key. A wrong type gives WRONGTYPE.
5. Existence logic (create, or `item exists`, or `not found`).
6. Memory-limit check and the actual add.

---

## 3. Command behaviour

Replies below are RESP2, with RESP3 differences stated in section 5. "Err" is an error reply. Error strings are verbatim and start with the `-` code word that goes on the wire.

### 3.1 BF.ADD key item
- Arity: exactly 3 arguments including the command name (`VB: src/bloom/command_handler.rs:165-168`). Otherwise `ERR wrong number of arguments for 'BF.ADD' command`.
- Key exists with another type: `WRONGTYPE Operation against a key holding the wrong kind of value`.
- Key exists as a bloom: reply `:1` if the item was newly added, `:0` if it was already present (or is a false positive), or `ERR non scaling filter is full` and the other add errors in section 4.
- Key missing: **auto-create** with the current global defaults (`capacity`, `fp_rate`, `tightening`, `expansion`, `use_random_seed` configs, section 7). Then add. A creation failure (memory limit) returns the error and creates nothing (`VB: src/bloom/command_handler.rs:206-231`).

### 3.2 BF.MADD key item [item ...]
- Arity: at least 3 arguments. Same create-if-missing rule as BF.ADD.
- Reply: an array with one element per item processed, each `:1` or `:0`. **If an item fails, the loop stops.** The error becomes the last element of the array (an error element inside the array) and the remaining items get no entry, so the array is *shorter* than the item count (`VB: src/bloom/command_handler.rs:26-45`; `TEST: tests/valkey_bloom_test_case.py:validate_nonscaling_failure` asserts `len(response) == 2` for `MADD f existing new new`, with `response[0] == 0`).
- The same item twice in one call: the second occurrence gives `:0`. `add_item` checks existence before inserting, so a second add of an identical item sees the first one's bits.
- Whole-command errors (wrong type, creation error) are plain error replies, not arrays.

### 3.3 BF.EXISTS key item / BF.MEXISTS key item [item ...]
- BF.EXISTS arity is exactly 3, BF.MEXISTS at least 3.
- Key missing: `:0` (or an array of `:0`). The key is not created.
- Wrong type: WRONGTYPE.
- An item "exists" if **any** sub-filter reports it present (`VB: src/bloom/utils.rs:232-234`).
- Read-only: never replicated, no events.

### 3.4 BF.CARD key
- Arity: exactly 2. Missing key gives `:0`. Otherwise the **sum of `num_items` across sub-filters** (`VB: src/bloom/utils.rs:237-243`). `num_items` counts only successful adds (replies of `:1`), so false positives and duplicates are not counted.
- Equals `BF.INFO key ITEMS`.

### 3.5 BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | ERROR | TIGHTENING | EXPANSION | MAXSCALEDCAPACITY]
- Arity: 2 or 3 arguments (otherwise wrong arity). Wrong type gives WRONGTYPE. **A missing key gives `ERR not found` for both forms, and this wins over any field validation** (`VB: src/bloom/command_handler.rs:788`).
- One field (case-insensitive) returns a **bare scalar**, not an array:

| Field | Reply (RESP2) | Notes |
|---|---|---|
| CAPACITY | integer | sum of sub-filter capacities |
| SIZE | integer | memory usage in bytes (section 8.2) |
| FILTERS | integer | number of sub-filters |
| ITEMS | integer | = BF.CARD |
| ERROR | bulk string such as `"0.01"` | the object's base error rate. It is the creation value, **not** the current sub-filter rate. |
| TIGHTENING | bulk string such as `"0.5"` | only when scaling; non-scaling gives `ERR invalid information value` |
| EXPANSION | integer, or **nil** when non-scaling | nil is `$-1` in RESP2 and `_` in RESP3 |
| MAXSCALEDCAPACITY | integer | only when scaling; non-scaling gives `ERR invalid information value` |

- Any other field name gives `ERR invalid information value`.
- No field: a flat array of alternating label and value. Labels are **simple strings** (`+Capacity`), with these values:

  Scaling filter (16 elements):
  ```
  "Capacity" <int>  "Size" <int>  "Number of filters" <int>  "Number of items inserted" <int>
  "Error rate" <double>  "Expansion rate" <int>  "Tightening ratio" <double>  "Max scaled capacity" <int>
  ```
  Non-scaling filter (12 elements): same first five pairs, then `"Expansion rate"` followed by **nil**, and **no** Tightening ratio and no Max scaled capacity (`VB: src/bloom/command_handler.rs:750-787`; `TEST: tests/test_bloom_command.py:150-181`). Doubles are bulk strings in RESP2 and doubles in RESP3. Docs example shows `Size 384` and `Max scaled capacity 26214300` for a default filter.
- The field list on valkey.io omits `ITEMS`, but the code accepts it (`VB: command_handler.rs:720`).
- `Max scaled capacity` is computed on the fly by the algorithm in section 6.3 (with `validate_scale_to = -1`).

### 3.6 BF.RESERVE key error_rate capacity [EXPANSION expansion | NONSCALING]
- Arity: 4 to 6 arguments including the command name (`VB: command_handler.rs:339`). Outside that gives wrong arity.
- Parse order: error_rate, capacity, then the trailing option, then key checks. See 2.2 for messages.
- Trailing option (`VB: command_handler.rs:369-391`):
  - 5 args, token `NONSCALING`: expansion = 0.
  - 6 args, tokens `EXPANSION <n>`: expansion = n (1..=u32::MAX), otherwise `ERR bad expansion`.
  - **Anything else** (5 args with `EXPANSION` and no value, 6 args with `NONSCALING extra`, an unknown word) gives the bare error `ERROR` (see 4).
  - Both options together make 7 args, so the arity error (`TEST: BF.RESERVE TT 0.01 1 NONSCALING EXPANSION 1`).
  - No option: expansion = `bf.bloom-expansion` config (default 2).
- Then open the key writable. Wrong type gives WRONGTYPE. Key already a bloom gives `ERR item exists`. Otherwise create with the global `tightening-ratio` and seed-mode configs and reply `+OK`.
- Note the ordering consequence: `BF.RESERVE <existing> abc 100` gives `bad error rate`, not `item exists`.
- A creation exceeding the memory limit gives `ERR operation exceeds bloom object memory limit`.

### 3.7 BF.INSERT key [CAPACITY c] [ERROR e] [EXPANSION x] [NONSCALING] [NOCREATE] [TIGHTENING t] [SEED s] [VALIDATESCALETO n] [ITEMS item ...]
- Arity: at least 2 arguments (just `BF.INSERT key` is valid).
- Defaults come from the configs (section 7). Option semantics:

| Option | Argument | Meaning |
|---|---|---|
| ERROR | float | base error rate for a newly created filter (ignored if key exists) |
| CAPACITY | int | initial capacity (ignored if key exists) |
| EXPANSION | uint | sets scaling and expansion (ignored if key exists) |
| NONSCALING | none | expansion = 0 |
| NOCREATE | none | if key is missing, reply `ERR not found` instead of creating |
| TIGHTENING | float | tightening ratio for a new filter. The docs say it is meant for replicated commands, but any client can use it. |
| SEED | exactly 32 raw bytes | hash seed for a new filter. The object is marked "random-seed" if the seed differs from the fixed seed (section 9). |
| VALIDATESCALETO | int >= 1 | dry-run: check the filter could scale to that total capacity (section 6.4). Does not change the filter. |
| ITEMS | items... | end of options |

- Errors from options: `ERR bad error rate`, `ERR (0 < error rate range < 1)`, `ERR bad tightening ratio`, `ERR (0 < tightening ratio range < 1)`, `ERR bad capacity`, `ERR (capacity should be larger than 0)`, `ERR bad expansion`, `ERR invalid seed`, `ERR unknown argument received`, wrong arity.
- `VALIDATESCALETO` together with `NONSCALING` (expansion 0 at that point) gives `ERR cannot use NONSCALING and VALIDATESCALETO options together`. The check uses the expansion value *after* all options are parsed, so `NONSCALING EXPANSION 2 VALIDATESCALETO 10` is accepted (last wins).
- Reply: array of `:1` / `:0` (or a trailing error element, as for MADD). **With no items the reply is an empty array.** If the key was missing and `NOCREATE` was not given, the (empty or default) filter **is still created** (`TEST: tests/test_bloom_command.py:119,121` assert `BF.INSERT KEY` replies with length 0; `command_handler.rs:644-689` creates the key).
- If the key exists, the creation options are silently ignored and only the items are added (docs: "the command only adds the provided items").

### 3.8 BF.LOAD key dump
- Arity: exactly 3. Wrong type gives WRONGTYPE. Key exists gives **`BUSYKEY Target key name already exists.`** (a different error from BF.RESERVE's `item exists`).
- Decodes the blob produced by AOF rewrite (section 10.3), creates the key, and replies `+OK`. Errors: `ERR bloom object decoding failed` (empty blob, bad bincode, bad sub-filter bytes), `ERR bloom object decoding failed. Unsupported version` (first byte != 1), and the range or limit errors listed in 10.3.
- Not intended for clients. `must_obey_client` skips the size-limit validation when it comes from AOF or a replica.
- **Hotstash only:** from a client (not from a primary), a blob with more than 4096 sub-filters gets `ERR bloom object reached max number of filters`, and a blob whose tightened error rate falls below the smallest double at some sub-filter gets `ERR bloom object decoding failed` (the check RDB loading already makes). Every check and add looks at each sub-filter, and a blob costs nothing to make, unlike the hundreds of thousands of adds that grow a real filter that far. valkey-bloom takes up to `i32::MAX`.

---

## 4. Error message catalogue (verbatim, as sent on the wire)

All are produced by `ReplyWithError(msg)` (`VMOD: src/context/mod.rs:488,639`). The module strings begin with their own code word and the server does not prepend `ERR` when the message already starts with a code word (server knowledge). Source: `VB: src/bloom/utils.rs:19-48`.

| Message (wire) | When it fires |
|---|---|
| `ERR wrong number of arguments for 'BF.<NAME>' command` | handler arity check, or a missing value or empty `ITEMS` in BF.INSERT. `<NAME>` is the registered upper-case name. Produced by the server's WrongArity helper (`VMOD: context/mod.rs:624-631`). |
| `WRONGTYPE Operation against a key holding the wrong kind of value` | key holds a non-bloom value, for every command including BF.EXISTS and BF.CARD |
| `ERR bad error rate` | error_rate is not parseable as f64 |
| `ERR (0 < error rate range < 1)` | error_rate parsed but is not in the open interval (0,1), including NaN, inf, 0, 1, negatives |
| `ERR bad tightening ratio` / `ERR (0 < tightening ratio range < 1)` | same, for `TIGHTENING` |
| `ERR bad capacity` | capacity (or `VALIDATESCALETO`) not an i64, or negative, or the `checked_mul` overflow while scaling |
| `ERR (capacity should be larger than 0)` | capacity (or `VALIDATESCALETO`) is exactly 0 |
| `ERR bad expansion` | expansion not a u32, or 0, or out of range; also a decoded object with out-of-range expansion |
| `ERR item exists` | BF.RESERVE on a key that is already a bloom |
| `ERR not found` | BF.INFO on a missing key; BF.INSERT `NOCREATE` on a missing key |
| `ERR invalid information value` | BF.INFO with an unknown field, or `TIGHTENING` or `MAXSCALEDCAPACITY` on a non-scaling filter |
| `ERR invalid seed` | `SEED` argument is not exactly 32 bytes |
| `ERR unknown argument received` | unknown BF.INSERT option (including a bare `HELLO`) |
| `ERROR` (bare, no `ERR ` prefix) | BF.RESERVE with a malformed or extra 4th/5th argument (`VB: command_handler.rs:387-389`), and any `set_value` failure (never expected) |
| `ERR non scaling filter is full` | adding a new unique item to a non-scaling filter whose last sub-filter has `num_items == capacity`. In MADD/INSERT it is an array element and stops the loop. |
| `ERR false positive degrades to 0 on scale out` | scaling would need a sub-filter with error rate <= `f64::MIN_POSITIVE` (2.2250738585072014e-308) (`VB: utils.rs:385-394`) |
| `ERR operation exceeds bloom object memory limit` | creation or scale-out that would push the object past `bf.bloom-memory-usage-limit`; also BF.LOAD of an over-limit blob |
| `ERR bloom object reached max number of filters` | sub-filter count reached `i32::MAX` (unreachable in practice) |
| `ERR cannot use NONSCALING and VALIDATESCALETO options together` | BF.INSERT |
| `ERR provided VALIDATESCALETO causes bloom object to exceed memory limit` | the dry run hit the memory limit before reaching the target |
| `ERR provided VALIDATESCALETO causes false positive to degrade to 0` | the dry run hit the fp floor before reaching the target |
| `BUSYKEY Target key name already exists.` | BF.LOAD on an existing key |
| `ERR bloom object decoding failed` | BF.LOAD blob is empty, truncated, or not valid bincode |
| `ERR bloom object decoding failed. Unsupported version` | BF.LOAD blob's first byte is not 1 |
| `OOM command not allowed when used memory > 'maxmemory'.` | server-level, for the three `deny-oom` writes when over maxmemory (`TEST: test_bloom_basic.py:131-142`) |

Quirks worth copying:
- BF.LOAD range-checks the tightening ratio but reports it with the *error-rate* message `ERR (0 < error rate range < 1)` (`VB: utils.rs:446-450`).
- The decode failure for one invalid sub-filter bitmap is also `ERR bloom object decoding failed` (serde error path).
- In RESP2 the wire form of a module error element inside an array is the normal `-ERR ...` line.

---

## 5. Reply types in RESP2 and RESP3

valkey-bloom never branches on the protocol. It returns Rust values, and the server converts them (`VMOD: src/context/mod.rs:529-640`, `VB: src/bloom/command_handler.rs`).

| Rust value | RESP2 wire | RESP3 wire | Used for |
|---|---|---|---|
| `Integer` | `:n` | `:n` | ADD/EXISTS/CARD scalars and every array element of MADD/MEXISTS/INSERT; INFO integer fields |
| `Float` (`ReplyWithDouble`) | bulk string, shortest repr such as `"0.01"` | double `,0.01` | BF.INFO `ERROR`, `TIGHTENING`, and the "Error rate" / "Tightening ratio" values |
| `SimpleStringStatic` | `+Capacity` | simple string | the labels in BF.INFO |
| `Null` | `$-1` | `_` | BF.INFO `EXPANSION` and "Expansion rate" on non-scaling filters |
| `Array` | `*n` | `*n` array | **BF.INFO is a flat array in both protocols (never a map)**; MADD/MEXISTS/INSERT results are arrays |
| `StaticError` in an array | `-ERR ...` element | same | MADD / INSERT stop-at-error element |
| OK | `+OK` | `+OK` | BF.RESERVE, BF.LOAD |

Consequences:
- **No booleans in RESP3.** BF.ADD, BF.EXISTS and the array elements stay `:1` / `:0` (RedisBloom sends `#t` / `#f`).
- The valkey-py compatibility test accepts both shapes because the client library parses the flat array (`TEST: tests/test_bloom_valkeypy_compatibility.py:19-26`).
- Empty BF.INSERT result: `*0`.

---

## 6. Scaling

### 6.1 Object model (`VB: src/bloom/utils.rs:90-98,566-574`)
A bloom object holds `expansion: u32` (0 means non-scaling), `fp_rate: f64`, `tightening_ratio: f64`, `is_seed_random: bool` and `filters: Vec<BloomFilter>`. Each sub-filter has its own `capacity: i64`, `num_items: i64` and a bit array. The tightening ratio is stored per object and fixed at creation. Changing the config later does not affect existing objects.

### 6.2 The add algorithm (`VB: utils.rs:308-369`)
For each item:
1. If **any** sub-filter reports the item present, return 0 and change nothing.
2. Take the **last** sub-filter. If `num_items < capacity`, set the bits, `num_items += 1`, return 1.
3. Otherwise the last filter is full:
   - `expansion == 0`: return `ERR non scaling filter is full`.
   - Compute `new_fp = fp_rate * tightening_ratio^n` where n is the current number of sub-filters (see below). If `new_fp <= f64::MIN_POSITIVE`, return `ERR false positive degrades to 0 on scale out`.
   - Compute `new_capacity = last.capacity * expansion` with `checked_mul` on i64. Overflow gives `ERR bad capacity`.
   - If the size limit is being validated and `current_memory_usage + size_of_new_filter(new_capacity, new_fp) > limit`, return `ERR operation exceeds bloom object memory limit`.
   - Create a sub-filter with `new_capacity`, `new_fp` and the **same seed** as the first filter. Insert the item with `num_items = 1`. Return 1.

Rules to note:
- The **first** sub-filter uses `fp_rate` as given. Sub-filter number i (0-based) has error rate `fp_rate * tightening_ratio^i`, with `powi` (repeated multiplication). With the defaults that is 0.01, 0.005, 0.0025 and so on (`VB: utils.rs:385-394`). Sub-filter i has capacity `initial * expansion^i`.
- A new sub-filter is added only when a **new unique** item arrives while the last is full. `BF.INFO FILTERS` stays 1 at exactly full.
- The max sub-filter count is `i32::MAX` (`VB: src/configs.rs:65`), so the practical bounds are the memory limit and the fp floor.
- NONSCALING: the filter accepts exactly `capacity` unique items. After that, new unique items fail with `ERR non scaling filter is full`. Items already present still reply `:0`.

### 6.3 Max scaled capacity (BF.INFO) (`VB: utils.rs:500-556`)
Loop with `cur_cap = initial capacity`, `total = 0`, `n = 0`, `filters_mem = 0`:
1. `fp = fp_rate * tightening^n`. If `fp <= MIN_POSITIVE`, stop and return `total`.
2. `fsize = sizeof_filter(cur_cap, fp)` (section 8.1). `vec_cap = 1` if n == 0, else `next_power_of_two(max(4, n + 1))`. `object = 48 + 8 * vec_cap + filters_mem + fsize`. If `object > memory_limit`, stop and return `total`.
3. `filters_mem += fsize`, `total += cur_cap`, `cur_cap = cur_cap * expansion` (overflow gives `bad capacity`), `n += 1`.

Verified in my reimplementation against upstream constants: default `(100, 0.01, exp 2, tight 0.5)` gives **26214300**; `(2000, 0.5)` gives **32766000**; `(10000, 0.001)` gives **20470000** (`TEST: test_bloom_command.py:125,175`; `test_bloom_correctness.py:73`).

### 6.4 VALIDATESCALETO (BF.INSERT)
Runs the same loop with `scale_to = n` and stops when `total >= scale_to`. A stop caused by the fp floor gives `provided VALIDATESCALETO causes false positive to degrade to 0`. A stop caused by memory gives `provided VALIDATESCALETO causes bloom object to exceed memory limit`. Upstream boundary cases, reproduced by my reimplementation:
- `BF.INSERT k EXPANSION 25 ERROR 0.00000000000000001 VALIDATESCALETO 1627600` passes and `...1627601` fails with the memory error.
- `BF.INSERT k EXPANSION 1 VALIDATESCALETO 101600` passes and `...101601` fails with the fp error (`TEST: test_bloom_correctness.py:143-158`).

---

## 7. Sizing, hash function and seeds

### 7.1 Bits and hash count per filter (`BFCRATE: src/lib.rs:112-130,293-298`)
For a sub-filter of capacity `n` and error rate `p`:

```
log2_2    = LN_2 * LN_2
nbytes    = ceil( (n as f64) * ln(p) / (-8.0 * log2_2) )   // as usize, evaluated left to right
bits      = nbytes * 8
k (hashes)= max( round( (bits as f64) / (n as f64) * LN_2 ), 1 )   // Rust f64::round: half away from zero
```

- Bits are `nbytes * 8` exactly: **no power-of-two rounding** and no 64-bit rounding.
- nbytes is always >= 1. The `bitmap_bits` used for modulo is `nbytes * 8`.
- Examples (`DERIVED`): n=100, p=0.01 gives 120 bytes (960 bits), k=7. n=1000, p=0.01 gives 1199 bytes, k=7. n=1000, p=0.001 gives 1798 bytes, k=10. n=1, p=0.5 gives 1 byte, k=6. n=2000, p=0.5 gives 361 bytes, k=1. n=100, p=0.5 gives 19 bytes, k=1. n=1000, p=0.0001 gives 2397 bytes, k=13.
- Python's `round` is banker's rounding; use half-away-from-zero in Rust.

### 7.2 Hash function and bit selection (`BFCRATE: src/lib.rs:136-163,300-314`, `src/bitmap.rs:143-153`)
- Two keyed **SipHash-1-3** hashers (`siphasher::sip::SipHasher13`). The 32-byte seed is split: `key0 = seed[0..16]`, `key1 = seed[16..32]`. Each 16-byte key is two little-endian u64 (`k0 = LE(key[0..8])`, `k1 = LE(key[8..16])`) (`SIPCRATE: src/sip.rs:208-218`).
- The hashed input is Rust's `Hash for [u8]`: the item length as a **usize, 8 bytes little-endian**, followed by the item bytes, as one SipHash stream. The length prefix comes from the std library and is not in the tarball (Rust std knowledge); `SIPCRATE` confirms `write_usize` is `to_le`.
- Per item compute `h0 = SipHash13(key0, msg)`, `h1 = SipHash13(key1, msg)` (u64 finish).
- For `k_i` in `0..k`:
  - `k_i == 0`: `x = h0`.
  - `k_i == 1`: `x = h1`.
  - `k_i >= 2`: `x = (h0 + k_i * h1) mod 2^64 (wrapping) % 0xFFFF_FFFF_FFFF_FFC5` (the largest u64 prime). The multiplication and add are wrapping u64.
  - `bit = x % bitmap_bits`. Set or test bit `bit` as `bytes[bit / 8] |= 1 << (bit % 8)` (LSB-first).
- `add` sets all k bits; `check` requires all k bits. All sub-filters of one object share the seed but have different `bits` and `k`.
- Worked example (`DERIVED`, unconfirmed against the module): fixed seed, a 100/0.01 filter (960 bits, k=7), item `item`, gives bit offsets `[701, 8, 461, 213, 925, 677, 429]`; `foo` gives `[687, 90, 355, 189, 23, 817, 651]`. For a 1000/0.01 filter (9592 bits, k=7), `item` gives `[8173, 6208, 685, 6173, 2069, 7557, 3453]`. `SipHash13(FIXED[0..16], le64(4) ++ "item")` equals `0x1589599b71175ebd` (my SipHash-1-3 was validated against the SipHash-2-4 reference vectors, with the same round code).
- A reimplementation that uses the same seed produces the same bit pattern, the same false positives, and therefore the same `:0` replies. This only holds in fixed-seed mode.

### 7.3 Seeds (`VB: src/configs.rs:68-71`, `src/bloom/command_handler.rs:208-231,403-407`)
- `bf.bloom-use-random-seed` (default **yes**): each new object gets a random 32-byte seed from the crate (`getrandom`). False positives then differ from object to object and from run to run.
- `bf.bloom-use-random-seed no`: the fixed seed is used for every object:

```
FIXED_SEED = [89,15,245,34,234,120,17,218,167,20,216,9,59,62,123,217,29,137,138,115,62,152,136,135,48,127,151,205,40,7,51,131]
```
  (hex `590ff522ea7811daa714d8093b3e7bd91d898a733e988887307f97cd28073383`; `VB: configs.rs:68-71`).
- `BF.INSERT ... SEED <32 raw bytes>` sets an explicit seed. The object is marked `is_seed_random = (seed != FIXED_SEED)`.
- Sub-filters created by scale-out reuse the first filter's seed (`VB: utils.rs:346-351`).
- The seed travels in the replication stream (section 11) and in the RDB as part of each sub-filter's bitmap header.
- For Hotstash, byte-exact answers need `bf.bloom-use-random-seed no`. In random mode only the statistical error rate is comparable (the tests accept an observed fp rate under `p + 0.002`).

### 7.4 Memory limit (`VB: src/configs.rs:33-38`, `src/bloom/utils.rs:200-221`)
- `bf.bloom-memory-usage-limit`, default **134217728 (128 MiB)**, range 0..i64::MAX, runtime-settable. Docs example text shows 268435456. The source default is 128 MiB.
- Creation check: `48 + 8 + sizeof_filter(cap, p) > limit` gives `ERR operation exceeds bloom object memory limit` (constants in 8.1).
- Scale-out check: `current_memory_usage + sizeof_filter(new_cap, new_fp) > limit` gives the same error.
- Skipped for commands that arrive from the primary (`must_obey_client`: replication stream and AOF load), so a replica never rejects what the primary accepted. With the default limit and p=0.01 the largest creatable capacity is about 112,022,240 (`DERIVED`).
- At the default limit, `BF.RESERVE k 0.01 9223372036854775807` fails with this error (parse succeeds first).

---

## 8. Memory accounting (needed for `BF.INFO SIZE` and the metrics)

### 8.1 Constants (reproduced from the upstream default of 384 bytes)
`BF.INFO SIZE` is `BloomObject::memory_usage()`:

```
sizeof_filter(n, p) = 24 (BloomFilter) + 184 (Bloom<[u8]> struct) + nbytes(n, p)
memory_usage        = 48 (BloomObject) + 8 * vec_capacity + sum over filters of sizeof_filter
```

- `BloomObject` = 48, `Box<BloomFilter>` = 8, `BloomFilter` = 24, `bloomfilter::Bloom` = 184. These are Rust struct layouts for the 64-bit build: they are not stated in the source. They are inferred from the upstream test constant `DEFAULT_BLOOM_FILTER_SIZE = 384` (`TEST: tests/test_bloom_metrics.py:6`), which equals `48 + 8 + 24 + 184 + 120`, and from the docs example `Size 384`.
- The `nbytes` term counts only the bit array, `bloom.len()/8`. The 45-byte crate header (section 10.2) is *not* counted (`VB: utils.rs:668-672`).
- `vec_capacity` is the allocated capacity of the `Vec` of filters: 1 filter gives 1, 2..4 filters give 4, 5..8 give 8, then 16, 32, ... (Rust `Vec` growth from a vec created with capacity 1). A BF.LOAD or RDB-loaded object starts with capacity 1 too, so it grows the same way.
- Worked: default filter 384; `BF.RESERVE k 0.01 1000` 1463; `0.001 1000` 2062; `0.5 2000` 625; `0.001 10000` 18236 (`DERIVED`, X).
- After scaling the default filter to 2 sub-filters, Size is 892 (`DERIVED`: `48 + 8*4 + (208+120) + (208+276)`). A Hotstash implementation need not match the byte count of its own memory use, but should return these numbers for `SIZE` if clients compare.

### 8.2 MEMORY USAGE
`MEMORY USAGE key` calls the type's `mem_usage` callback (the same `memory_usage()`, `VB: src/wrapper/bloom_callback.rs:110-113`) and the server adds its own key overhead (server knowledge). So `MEMORY USAGE >= BF.INFO SIZE` (`TEST: tests/test_bloom_basic.py:51-56`).

### 8.3 INFO module section
`INFO bf` (also in `INFO modules`) prints two sections: core metrics `bloom_total_memory_bytes`, `bloom_num_objects`, `bloom_num_filters_across_objects`, `bloom_num_items_across_objects`, `bloom_capacity_across_objects` and defrag metrics `bloom_defrag_hits`, `bloom_defrag_misses`. The server prefixes them with the module name, so the lines read `bf_bloom_total_memory_bytes:...` (`VB: src/metrics.rs:15-60`, `TEST: tests/valkey_bloom_test_case.py:verify_bloom_metrics`). Hotstash can add them cheaply; they are optional for client compatibility.

---

## 9. Module configuration

Names are `bf.<name>` (module name `bf`). All are registered with default flags: runtime-settable with CONFIG SET, settable in the config file, and settable as `loadmodule` args (`module_args_as_configuration: true`) (`VB: src/lib.rs:127-144`).

| Config | Type | Default | Range / validation | Used by |
|---|---|---|---|---|
| `bf.bloom-capacity` | int | 100 | 1..i64::MAX | default capacity for auto-created filters (BF.ADD, BF.MADD, BF.INSERT without `CAPACITY`) |
| `bf.bloom-expansion` | int | 2 | **0**..u32::MAX | default expansion. **0 means non-scaling by default.** The command-level `EXPANSION` still needs 1+. |
| `bf.bloom-fp-rate` | string | `"0.01"` | parses as f64 and 0 < x < 1, else the CONFIG SET error below | default base error rate |
| `bf.bloom-tightening-ratio` | string | `"0.5"` | parses as f64 and 0 < x < 1 | tightening ratio for new objects (also BF.RESERVE) |
| `bf.bloom-memory-usage-limit` | int | 134217728 | 0..i64::MAX | per-object memory limit (7.4) |
| `bf.bloom-use-random-seed` | bool | yes | yes/no | random vs fixed seed (7.3) |
| `bf.bloom-defrag-enabled` | bool | yes | yes/no | active defrag callback (not relevant to Hotstash) |

- CONFIG SET errors: `ERR Invalid floating-point value` for a non-float, and `ERR (0 < error rate range < 1)` or `ERR (0 < tightening ratio range < 1)` for out of range, as the server reports `CONFIG SET failed (possibly related to argument 'bf.bloom-fp-rate') - ERR (0 < error rate range < 1)` (`TEST: test_bloom_basic.py:185-196`). `CONFIG GET bf.bloom-fp-rate` returns the string as set (`0.1`).
- Config changes apply only to objects created afterwards (docs, and `TEST: test_bloom_basic.py:198-212` and `test_deterministic_replication`).
- `bf.bloom-fp-rate` and `bf.bloom-tightening-ratio` are strings in the registration so that doubles can be configured. They are parsed into the `f64` statics on set.
- The module only loads on Valkey >= 8.0 (`BLOOM_MIN_SUPPORTED_VERSION = [8,0,0]`).

---

## 10. Data type facts and persistence

### 10.1 Type name and visibility
- `TYPE key` returns **`bloomfltr`** (`VB: src/bloom/data_type.rs:18`; `TEST: test_bloom_basic.py:test_module_data_type`).
- `OBJECT ENCODING key` returns **`raw`** (the server default for module values; `TEST: same`).
- `DEL`, `UNLINK`, `EXPIRE`/`TTL`/`PERSIST`, `RENAME`, `TOUCH`, `RANDOMKEY`, `FLUSHALL`, MULTI/EXEC and Lua `redis.call('BF.ADD', ...)` all work as for any key (`TEST: test_bloom_basic.py`).
- `COPY src dst` calls the type's `copy` callback: a deep copy of every sub-filter, bitmaps and counters, with the same seed. `DEBUG DIGEST-VALUE` is equal on both (`VB: bloom_callback.rs:117-126`, `utils.rs:158-174`).
- `DEBUG DIGEST-VALUE` hashes `expansion, fp_rate (LE 8 bytes), tightening_ratio (LE), is_seed_random`, then per filter the full bitmap bytes (including the crate header), `num_items`, `capacity` (`VB: data_type.rs:140-152`). Hotstash can define any digest, but the same state must give the same digest after COPY, DUMP/RESTORE and replication.

### 10.2 RDB save and DUMP / RESTORE
- Module type name `bloomfltr`, **encver 1** (`BLOOM_TYPE_ENCODING_VERSION = 1`, `VB: data_type.rs:16`). In the DUMP/RDB module id this encodes as `0x6e5a2899f96dac01` (name `bloomfltr` in the 6-bit charset `A-Za-z0-9-_`, then 10-bit encver 1). `DERIVED`, and it matches the 8 bytes `6e 5a 28 99 f9 6d ac 01` in the upstream test's sample dump (`TEST: test_bloom_basic.py:test_bloom_dump_and_restore`).
- Saved by the module as a sequence of module-RDB primitives (`VB: src/wrapper/bloom_callback.rs:27-50`):
  1. unsigned `num_filters`
  2. unsigned `expansion`
  3. double `fp_rate`
  4. double `tightening_ratio`
  5. unsigned `is_seed_random` (0 or 1)
  6. for each sub-filter in order: unsigned `capacity`; **only for the last filter**, unsigned `num_items`; then a string buffer with the crate bitmap.
- The per-filter bitmap string is the `bloomfilter` crate's byte format (`BFCRATE: src/bitmap.rs:4-80`):

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | version = 1 |
| 1 | 8 | bit-array length in bytes, u64 little-endian |
| 9 | 4 | k (number of hash functions), u32 LE (must be non-zero) |
| 13 | 32 | the seed |
| 45 | nbytes | the bit array |

  Total length is `45 + nbytes`, and the loader rejects anything else.
- On load (`VB: data_type.rs:56-137`):
  - `encver > 1` is rejected with a log line.
  - For filter i, the fp rate is recomputed as `fp * tightening^i` to size-check it. Failure to compute gives `False positive degrades to 0`.
  - The cumulative size is checked against the memory limit. A failure logs "Failed to restore bloom object: Object larger than the allowed memory limit." and the server **aborts the load** ("Internal error in RDB", `TEST: test_restore_failed_large_bloom_filter`).
  - Every non-last filter gets `num_items = capacity` (they are assumed full). The last filter gets the stored `num_items`.
  - If `is_seed_random == 0`, every filter's seed must equal `FIXED_SEED`, otherwise the load fails.
- `RESTORE` with a malformed payload replies `Bad data format` and creates no key (`TEST: test_bloom_dump_and_restore`). Version 1.0.1 fixes CVE-2026-21864 (remote DoS via a malformed RESTORE) by enabling `ModuleOptions::HANDLE_IO_ERRORS` (`00-RELEASENOTES`, `VB: src/lib.rs:25`).
- The RDB may be loaded by a server without the module only if the keys of this type are absent. A server without the module cannot load `bloomfltr` keys.
- AUX data: none is saved (`aux_save: None`), and aux load ignores any AUX fields.

### 10.3 AOF rewrite and BF.LOAD blob
AOF rewrite emits one `BF.LOAD <key> <blob>` per key (`VB: bloom_callback.rs:67-90`). The blob is `BloomObject::encode_object()`:

```
byte 0        : BLOOM_OBJECT_VERSION = 1
bytes 1..     : bincode 1.x default config (fixed-int, little endian) of the tuple
                (expansion: u32, fp_rate: f64, tightening_ratio: f64, is_seed_random: bool (1 byte),
                 filters: Vec<BloomFilter>)
BloomFilter   : bloom bytes  (u64 length prefix + the crate bitmap of 10.2),
                num_items: i64, capacity: i64
Vec           : u64 length prefix, then the elements
```
(`VB: utils.rs:90-98,372-382,565-574`; bincode 1.3 layout is standard knowledge, not in the tarball.) Decode validates expansion in range, fp in (0,1), tightening in (0,1) (reporting the error-rate message for tightening, see section 4), `filters.len() < i32::MAX`, and the memory limit (skipped for AOF or replication clients).

An AOF rewrite or RDB persist is only required for Hotstash if it wants to interoperate. For Hotstash's own persistence any stable encoding will do, as long as the whole state (seed, per-filter capacity, num_items, expansion, fp, tightening, bitmaps) survives.

---

## 11. Replication and keyspace events

### 11.1 Replication (`VB: src/bloom/command_handler.rs:60-157`)
Writes are rewritten for determinism. The replica gets exactly the same state, including the seed.

| Primary event | Replicated as |
|---|---|
| New object created (by BF.ADD, BF.MADD, BF.INSERT, BF.RESERVE or BF.LOAD) | `BF.INSERT <key> CAPACITY <cap> ERROR <fp> TIGHTENING <t> SEED <32 raw bytes> (NONSCALING \| EXPANSION <e>) [ITEMS <items...>]` |
| Items added to an existing object | the original command, **verbatim** (BF.ADD, BF.MADD or BF.INSERT as typed), and only if **at least one** item returned 1 |
| No item was added (all `:0`), or error | not replicated |
| BF.EXISTS, BF.MEXISTS, BF.INFO, BF.CARD | not replicated |

Details:
- `CAPACITY` is the **initial** capacity (the object's total at creation time, before the adds). `ERROR` and `TIGHTENING` are `f64::to_string()`, the shortest round-trip form, which never uses an exponent in Rust (`1e-17` becomes `0.00000000000000001`). Expansion 0 gives `NONSCALING`, else `EXPANSION <n>`.
- `ITEMS` carries every item argument of the original command, **including items that returned 0 and items after an error**; it is omitted only when the list is empty (BF.RESERVE).
- Replica-side commands are applied with `must_obey_client`, so the memory-limit checks are skipped.
- BF.RESERVE on the primary becomes a `BF.INSERT` without `ITEMS` on the replica. Test counts: primary 1 reserve, replica 1 `BF.INSERT` (`TEST: test_bloom_replication.py`).
- A failed command or an `item exists` reply is never replicated.
- **Probable upstream bug:** BF.LOAD sets the replication items to `&input_args[idx..]`, which is the dump blob itself (`VB: command_handler.rs:827-834`). So BF.LOAD on a primary would be replicated as a `BF.INSERT` that adds the blob as an *item* to a fresh default-capacity filter, not as a restore. It only matters if clients call BF.LOAD manually (AOF load is not replicated). A Hotstash reimplementation should replicate BF.LOAD verbatim, or as BF.LOAD, and treat this as a deliberate deviation.

### 11.2 Keyspace notifications (`VB: src/bloom/utils.rs:15-17`, `command_handler.rs:151-156`)
Class `GENERIC` (so the `g` flag, or `A` / `KEA`, must be enabled in `notify-keyspace-events`).

| Event name | Fired when |
|---|---|
| `bloom.add` | at least one item was newly added (reply `:1`) by BF.ADD, BF.MADD or BF.INSERT |
| `bloom.reserve` | a new object was created by BF.ADD, BF.MADD, BF.INSERT, BF.RESERVE or BF.LOAD |

- When both apply (a creation that also added items) both fire, in the order **`bloom.add` first, then `bloom.reserve`**. The tests treat message order as unordered (`TEST: test_bloom_keyspace.py`: BF.ADD on a new key gives 2 events, BF.INSERT with items gives both, BF.RESERVE gives only `bloom.reserve`, BF.EXISTS none).
- A BF.ADD of an existing item (`:0`) fires nothing. BF.INSERT with no items on a new key fires only `bloom.reserve`.
- The wire messages use the usual `__keyspace@0__:<key>` (payload `bloom.add`) and `__keyevent@0__:bloom.add` (payload `<key>`).

---

## 12. Implementation order for Hotstash

1. `bloomfltr` type with: base fp, tightening, expansion, seed (32 bytes), `Vec<SubFilter {capacity, num_items, k, bits, bitmap}>`.
2. Per-filter sizing (7.1) and the hash (7.2). Write unit tests against the worked bit offsets, but remember those are simulated (X).
3. Add/exists algorithm (6.2), including the existence check across all sub-filters first.
4. Config store: the seven `bf.*` configs (section 9) and the per-object snapshot of tightening/seed mode.
5. Commands in this order of difficulty: BF.EXISTS, BF.MEXISTS, BF.CARD, BF.RESERVE, BF.ADD, BF.MADD, BF.INSERT (all options, VALIDATESCALETO last), BF.INFO (scalar and full), BF.LOAD.
6. Errors (section 4) and the check order (2.4).
7. Memory accounting for `SIZE` and the limit (section 8, 7.4).
8. Replication rewrite and the two keyspace events (section 11).
9. Persistence (DUMP/RESTORE/RDB/AOF) with the layouts in section 10, or Hotstash's own encoding.
10. ACL category `@bloom`, command flags and key specs (section 1), `TYPE` and `OBJECT ENCODING` strings.

---

## 13. valkey-bloom versus RedisBloom: differences a client notices

RedisBloom facts come from `RB:` source (master) and redis.io docs. valkey-bloom facts are as above. Older RedisBloom releases may differ in message wording (not checked).

| Area | valkey-bloom 1.0.1 | RedisBloom |
|---|---|---|
| Commands | BF.ADD/MADD/EXISTS/MEXISTS/CARD/RESERVE/INSERT/INFO/LOAD | the same minus `BF.LOAD`, **plus `BF.SCANDUMP`, `BF.LOADCHUNK`, `BF.DEBUG`** (`RB: rebloom.c:1658-1672`). Clients that back up with SCANDUMP/LOADCHUNK will not work. |
| RESP3 booleans | `:1`/`:0` always | `#t`/`#f` booleans for BF.ADD, BF.EXISTS and array elements in RESP3 (`RB: rebloom.c:259,306`, docs) |
| BF.INFO shape | flat array in both protocols; one-field form returns a **bare scalar** | RESP2 array of pairs, and the one-field form returns a **one-element array**; RESP3 returns a **map** (`RB: rebloom.c:1065-1100`, docs) |
| BF.INFO fields | 8 (adds Error rate, Tightening ratio, Max scaled capacity for scaling filters), options `ERROR`, `TIGHTENING`, `MAXSCALEDCAPACITY`, `ITEMS` | 5 fields (Capacity, Size, Number of filters, Number of items inserted, Expansion rate); options only `CAPACITY`, `SIZE`, `FILTERS`, `ITEMS`, `EXPANSION` |
| Expansion rate for non-scaling | nil | nil |
| Missing key, BF.INFO | `ERR not found` | `ERR not found` |
| BF.EXISTS on a missing key | `:0` | `:0` (docs) |
| Hash | keyed **SipHash-1-3** (seed 32 bytes, fixed or random) | **MurmurHash2** (seed `0x9747b28c`, second hash seeded with the first) (`RB: bloom.c:65-68`) |
| Sizing | bits = `ceil(n*ln p / (-8 ln2^2))*8` exactly, k = `round(bits/n * ln2)` | `bpe = -ln(p)/ln2^2`, `hashes = ceil(ln2 * bpe)`; bits rounded up to a power of two by default, then to a 64-bit multiple (`RB: bloom.c:119-194`). Bytes and hash counts differ. |
| Scaling error rates | sub-filter i has `p * t^i` with t configurable (default 0.5); first filter uses `p` | scaling filters start at `p * 0.5` and each new filter is the previous times a fixed 0.5 (`RB: sb.c:29,38-39,100-103`) |
| Error-rate cap | none | error rates above **0.25** are silently capped (`BF_ERROR_RATE_CAP`, `RB: config.h:20`) |
| Limits | memory limit 128 MiB per object (`bf.bloom-memory-usage-limit`), no explicit capacity cap | `bf-initial-size` max 2^30, `bf-expansion-factor` max 32768 (`RB: config.c:22-32`); no memory-limit config |
| Config names | `bf.bloom-capacity`, `bf.bloom-expansion`, `bf.bloom-fp-rate`, `bf.bloom-tightening-ratio`, `bf.bloom-memory-usage-limit`, `bf.bloom-use-random-seed`, `bf.bloom-defrag-enabled` | `bf-error-rate`, `bf-initial-size`, `bf-expansion-factor` (legacy `ERROR_RATE`, `INITIAL_SIZE`) (`RB: config.h:16-39`) |
| Error wording (BF.RESERVE) | `ERR (0 < error rate range < 1)`, `ERR (capacity should be larger than 0)` | `ERR error rate must be in the range (0.000000, 1.000000)`, `ERR capacity must be in the range [1, 1073741824]`, `ERR expansion must be in the range [0, 32768]` (`RB: rebloom.c:148-190`) |
| Error wording (BF.INSERT) | `ERR bad error rate`, `ERR bad capacity`, `ERR bad expansion`, `ERR unknown argument received` | capitalised, without `ERR `: `Bad error rate`, `Bad capacity`, `Bad expansion`, `Unknown argument received` (`RB: rebloom.c:386-422`) |
| BF.INFO bad field | `ERR invalid information value` | `Invalid information value` (`RB: rebloom.c:1092`) |
| BF.INSERT option matching | exact whole tokens, case-insensitive | first-letter switch: any token starting with `i` is `ITEMS`, `e` is ERROR or EXPANSION by second letter, and so on (`RB: rebloom.c:366-424`) |
| BF.INSERT without ITEMS | valid: creates the key, replies `*0` | wrong arity (`argc < 4` at `RB: rebloom.c:364`, `items_index < 0` at `:430`) |
| BF.INSERT extra options | `SEED`, `TIGHTENING`, `VALIDATESCALETO` | none of these |
| BF.RESERVE arity / options | exactly one optional trailing option | up to 7 args, `NONSCALING` and `EXPANSION` anywhere; `EXPANSION 0` means non-scaling; `NONSCALING EXPANSION n` gives `Nonscaling filters cannot expand`; `ERR no expansion` when `EXPANSION` is last (`RB: rebloom.c:140-190`) |
| Malformed RESERVE tail | bare `ERROR` | specific messages as above |
| MADD/INSERT full filter | error element then stop (array shorter than the item count) | error element then stop too (`RB: rebloom.c:296-318`) |
| Replication | creation rewritten to `BF.INSERT ... SEED ...`; adds verbatim | `ReplicateVerbatim` for everything (`RB: rebloom.c:212,318`) |
| Keyspace events | `bloom.add`, `bloom.reserve` | none (no `NotifyKeyspaceEvent` call in `rebloom.c`) |
| ACL / flags | `@bloom` plus fast/write/read; BF.ADD/MADD/INSERT are `@fast` | `@bloom` plus write/read; docs list BF.ADD, MADD, INSERT as `@slow` |
| Type name / encver | `bloomfltr` / 1 | `MBbloom--` / 4 (`RB: rebloom.c:1722`). **RDB payloads are not interchangeable.** |
| Persistence of big filters | AOF rewrite emits one `BF.LOAD`, no chunking | `BF.SCANDUMP`/`BF.LOADCHUNK` chunks |
| Memory limit and OOM | per-object limit, plus `deny-oom` | `deny-oom` only |

Compatibility note: valkey-bloom documents API compatibility with the valkey-py, valkey-java, valkey-go client command syntax (README), and the upstream suite runs the valkey-py bloom tests (`TEST: tests/test_bloom_valkeypy_compatibility.py`).

---

## 14. Gaps between source and docs (do not "fix" these in Hotstash)

- valkey.io `BF.INFO` syntax omits `ITEMS`, but the code accepts it.
- valkey.io's memory-limit example shows 268435456, while the source default is 134217728.
- The docs describe `BF.INFO ... ERROR` as "a double". In RESP2 that is a bulk string (`"0.01"`).
- Docs state `BF.INSERT` "TIGHTENING" and "SEED" are for replication, yet they are accepted from any client.

---

## 15. Test vectors (RESP2)

Each vector runs on an empty database with default configs unless stated. Notation: `:n` integer, `+x` simple string, `$x` bulk string, `$nil` null bulk, `*n[...]` array of n elements, `-...` error line. `[S-test]` etc. are the confidence tags from section 0. Anything whose answer depends on a false positive is marked *FP-dependent*: it only holds in fixed-seed mode (`bf.bloom-use-random-seed no`) with the exact hash of section 7, and is X (simulated).

### 15.1 BF.RESERVE

| # | Request | Expected reply | Confidence |
|---|---|---|---|
| 1 | `BF.RESERVE bf 0.01 1000` | `+OK` | S-test |
| 2 | `BF.RESERVE bf 0.01 1000` (again) | `-ERR item exists` | S-test |
| 3 | `BF.RESERVE bf_non 0.01 1000 NONSCALING` | `+OK` | S-test |
| 4 | `BF.RESERVE bf_exp 0.01 1000 EXPANSION 2` | `+OK` | S-test |
| 5 | `bf.reserve lower 0.01 100 nonscaling` | `+OK` (case-insensitive) | S-code |
| 6 | `BF.RESERVE k 1e-3 100` | `+OK` (exponent form accepted) | S-code |
| 7 | `BF.RESERVE k2 0 100` | `-ERR (0 < error rate range < 1)` | S-code |
| 8 | `BF.RESERVE k2 1 100` and `BF.RESERVE k2 2 100` and `BF.RESERVE k2 -0.5 100` | each `-ERR (0 < error rate range < 1)` | S-test (value 2 and `0.99999999999999999`), S-code (others) |
| 9 | `BF.RESERVE k2 String 100` | `-ERR bad error rate` | S-test |
| 10 | `BF.RESERVE k2 0.01 0` | `-ERR (capacity should be larger than 0)` | S-test |
| 11 | `BF.RESERVE k2 0.01 -1` | `-ERR bad capacity` | S-test |
| 12 | `BF.RESERVE k2 0.01 0.01` and `BF.RESERVE k2 0.01 9223372036854775808` | each `-ERR bad capacity` | S-test |
| 13 | `BF.RESERVE k2 0.01 9223372036854775807` | `-ERR operation exceeds bloom object memory limit` | S-code |
| 14 | `BF.RESERVE k2 0.01 100 EXPANSION 0` | `-ERR bad expansion` | S-code |
| 15 | `BF.RESERVE k2 0.01 100 FOO` | `-ERROR` | S-code |
| 16 | `BF.RESERVE TT 0.01 1 NONSCALING EXPANSION 1` | `-ERR wrong number of arguments for 'BF.RESERVE' command` | S-test |
| 17 | `BF.RESERVE KEY SSS` | `-ERR wrong number of arguments for 'BF.RESERVE' command` | S-test |
| 18 | `SET s v` then `BF.RESERVE s abc 100` | `-ERR bad error rate` (parse before key) | S-code |
| 19 | `SET s v` then `BF.RESERVE s 0.01 100` | `-WRONGTYPE Operation against a key holding the wrong kind of value` | S-code |

### 15.2 BF.ADD / BF.MADD / BF.EXISTS / BF.MEXISTS / BF.CARD

| # | Request | Expected reply | Confidence |
|---|---|---|---|
| 20 | `BF.ADD key item` (key absent) | `:1` (and the key is created) | S-test |
| 21 | `BF.ADD key item` (again) | `:0` | S-test |
| 22 | `BF.EXISTS key item` | `:1` | S-test |
| 23 | `BF.EXISTS nokey item` | `:0` | S-test |
| 24 | `BF.MADD m a b a` (new key) | `*3[:1,:1,:0]` (the repeat is 0) | S-code |
| 25 | `BF.MEXISTS m a b c` | `*3[:1,:1,:0]` (c absent; a false positive could give `:1`) | S-code |
| 26 | `BF.CARD m` (after 24) | `:2` | S-code |
| 27 | `BF.CARD nokey` | `:0` | S-test |
| 28 | `BF.ADD key` and `BF.ADD key a b` and `BF.ADD` | `-ERR wrong number of arguments for 'BF.ADD' command` | S-test |
| 29 | `BF.MADD key` and `BF.MEXISTS key` | `-ERR wrong number of arguments for 'BF.MADD' command` (resp. `'BF.MEXISTS'`) | S-test |
| 30 | `bf.exists key item hello` | `-ERR wrong number of arguments for 'BF.EXISTS' command` | S-test |
| 31 | `BF.CARD key item` | `-ERR wrong number of arguments for 'BF.CARD' command` | S-test |
| 32 | `SET s v` then `BF.ADD s x` (also EXISTS, MADD, MEXISTS, CARD, INFO, INSERT) | `-WRONGTYPE Operation against a key holding the wrong kind of value` | S-test |

### 15.3 BF.INSERT

| # | Request | Expected reply | Confidence |
|---|---|---|---|
| 33 | `BF.INSERT k` (key absent) | `*0` and an empty default filter now exists | S-test (length 0), S-code (creation) |
| 34 | `BF.INSERT k2 CAPACITY 1000 ERROR 0.001 ITEMS a b` | `*2[:1,:1]`; then `BF.INFO k2 CAPACITY` is `:1000` | S-test |
| 35 | `BF.INSERT k3 ITEMS 1 2 3 EXPANSION 2` | `*5[...]` (the words after ITEMS are items) | S-test |
| 36 | `BF.INSERT k4 NOCREATE ITEMS a` | `-ERR not found` | S-test |
| 37 | `BF.INSERT k5 HELLO WORLD` | `-ERR unknown argument received` | S-test |
| 38 | `BF.INSERT k5 ERROR 2 ITEMS a` | `-ERR (0 < error rate range < 1)` | S-test |
| 39 | `BF.INSERT k5 ERROR err ITEMS a` | `-ERR bad error rate` | S-test |
| 40 | `BF.INSERT k5 TIGHTENING tr ITEMS a` and `TIGHTENING 2` | `-ERR bad tightening ratio` / `-ERR (0 < tightening ratio range < 1)` | S-test |
| 41 | `BF.INSERT k5 CAPACITY 0 ITEMS a` | `-ERR (capacity should be larger than 0)` | S-test |
| 42 | `BF.INSERT k5 EXPANSION 4294967299 ITEMS a` | `-ERR bad expansion` | S-test |
| 43 | `BF.INSERT k5 EXPANSION 2 ITEMS` | `-ERR wrong number of arguments for 'BF.INSERT' command` | S-test |
| 44 | `BF.INSERT k5 ERROR 0.5 CAPACITY` | `-ERR wrong number of arguments for 'BF.INSERT' command` | S-test |
| 45 | `BF.INSERT k5 VALIDATESCALETO 1000000000000 NONSCALING` | `-ERR cannot use NONSCALING and VALIDATESCALETO options together` | S-test |
| 46 | `BF.INSERT k5 VALIDATESCALETO 1000000000000` | `-ERR provided VALIDATESCALETO causes bloom object to exceed memory limit` | S-test |
| 47 | `BF.INSERT k5 CAPACITY 1 ERROR 0.0000000001 VALIDATESCALETO 10000000 EXPANSION 1` | `-ERR provided VALIDATESCALETO causes false positive to degrade to 0` | S-test |
| 48 | `BF.INSERT k6 EXPANSION 1 VALIDATESCALETO 101600` | `*0` (valid; creates an empty filter) | S-test |
| 49 | `BF.INSERT k7 EXPANSION 1 VALIDATESCALETO 101601` | `-ERR provided VALIDATESCALETO causes false positive to degrade to 0` | S-test |
| 50 | `BF.INSERT k5 SEED abc ITEMS a` | `-ERR invalid seed` | S-code |

### 15.4 BF.INFO

| # | Request | Expected reply | Confidence |
|---|---|---|---|
| 51 | `BF.INFO nokey` | `-ERR not found` | S-test |
| 52 | `BF.RESERVE d 0.01 100` then `BF.INFO d` | `*16[+Capacity,:100,+Size,:384,+Number of filters,:1,+Number of items inserted,:0,+Error rate,$0.01,+Expansion rate,:2,+Tightening ratio,$0.5,+Max scaled capacity,:26214300]` | Size 384 and Max scaled 26214300: S-test (constants) and D (docs example). The full array shape: S-code. |
| 53 | `BF.RESERVE n 0.5 2000 NONSCALING` then `BF.INFO n` | `*12[+Capacity,:2000,+Size,:625,+Number of filters,:1,+Number of items inserted,:0,+Error rate,$0.5,+Expansion rate,$nil]` | shape S-test; `625` is X |
| 54 | `BF.INFO d CAPACITY` / `FILTERS` / `ITEMS` / `SIZE` | `:100` / `:1` / `:0` / `:384` | S-test (384: S-test constant) |
| 55 | `BF.INFO d ERROR` / `BF.INFO d TIGHTENING` | `$0.01` / `$0.5` | S-test (`0.0001` and `0.5` style values are asserted) |
| 56 | `BF.INFO d EXPANSION` / `BF.INFO d MAXSCALEDCAPACITY` | `:2` / `:26214300` | S-test |
| 57 | `bf.info n expansion` | `$nil` | S-test |
| 58 | `BF.INFO d FOO` and `bf.info key item` | `-ERR invalid information value` | S-test |
| 59 | `BF.INFO n TIGHTENING` and `BF.INFO n MAXSCALEDCAPACITY` | `-ERR invalid information value` | S-test |
| 60 | `BF.INFO d CAPACITY SIZE` | `-ERR wrong number of arguments for 'BF.INFO' command` | S-test |
| 61 | `BF.RESERVE big 0.001 10000 EXPANSION 2` then `BF.INFO big MAXSCALEDCAPACITY` | `:20470000` | S-test |
| 62 | `BF.RESERVE two 0.5 2000` then `BF.INFO two MAXSCALEDCAPACITY` | `:32766000` | S-test |
| 63 | `BF.INSERT fp EXPANSION 1 VALIDATESCALETO 101600` then `BF.INFO fp MAXSCALEDCAPACITY` | `:101600` | S-test |

### 15.5 Data type, scaling and config

| # | Request | Expected reply | Confidence |
|---|---|---|---|
| 64 | `BF.ADD t x` then `TYPE t` | `+bloomfltr` | S-test |
| 65 | `OBJECT ENCODING t` | `$raw` | S-test |
| 66 | `BF.RESERVE ns 0.01 5 NONSCALING`, then `BF.MADD ns x1 x2 x3 x4 x5 x6 x7` (fixed seed) | `*6[:1,:1,:1,:1,:1,-ERR non scaling filter is full]` | X (the array is shorter than the 7 items: S-test) |
| 67 | `BF.ADD ns x8` (after 66) | `-ERR non scaling filter is full` | S-test |
| 68 | `BF.RESERVE sc 0.01 5 EXPANSION 2`, `BF.MADD sc x1 ... x12` (fixed seed), then `BF.INFO sc` | twelve `:1`; Capacity `:15`, Size `:516`, Number of filters `:2`, Items `:12`, Max scaled capacity per 6.3 | X |
| 69 | `BF.MADD` on a fresh default filter (cap 100, p 0.01, fixed seed) with `item1 ... item76` one at a time | `item1`..`item75` give `:1`; `item76` gives `:0` (a false positive). Next false positives at 173, 256 and 292 up to 300. After 300 adds `BF.INFO` shows Number of filters `:2`, Size `:892`, items `:296`. | X (simulation only; verify before relying) |
| 70 | `CONFIG SET bf.bloom-capacity 10000`, `BF.ADD c x`, `BF.INFO c CAPACITY` | `:10000` | S-test |
| 71 | `CONFIG SET bf.bloom-fp-rate 1.1` | `-ERR CONFIG SET failed (possibly related to argument 'bf.bloom-fp-rate') - ERR (0 < error rate range < 1)` | S-test |
| 72 | `CONFIG SET bf.bloom-memory-usage-limit 100` then `BF.RESERVE f 0.001 100000` | `-ERR operation exceeds bloom object memory limit` | S-test |
| 73 | `BF.LOAD t blob` where `t` already holds a bloom | `-BUSYKEY Target key name already exists.` | S-code |
| 74 | `BF.LOAD z ""` | `-ERR bloom object decoding failed` | S-code |
| 75 | `BF.LOAD z "\x02abc"` | `-ERR bloom object decoding failed. Unsupported version` | S-code |
| 76 | `COPY t t2` then `BF.EXISTS t2 x` | `:1` (the copy shares all content) | S-test |

(Count: 76 rows, some with several requests. The 30-60 target is exceeded to keep distinct error paths covered; trim the X-tagged rows if you want a minimal set.)

---

## 16. What I could not determine

1. **Wire formatting of tiny doubles in RESP2** (for example `BF.INFO k ERROR` with 1e-17). The module sends a double and the server formats it. Tests only show values such as `0.5`, `0.0001`, `0.75`, `0.01`. Hotstash should reuse whatever formatter it already uses for ZSCORE, which should be the Valkey one.
2. **Exact server framing of `-ERROR`** (bare). It follows from `ReplyWithError("ERROR")` and the server's rule about messages that already start with a code word (server knowledge, not in the tarball).
3. **Whether the arity -1 holds for BF.LOAD.** The upstream test checks 8 commands; BF.LOAD is inferred.
4. **Rust struct sizes (48/8/24/184)** are inferred from the 384 test constant, not read from a compiler. If the module is rebuilt with different layouts, `BF.INFO SIZE` changes, and so does the memory-limit boundary (these constants shift `MAXSCALEDCAPACITY` by a small amount only; the reproduced 26214300, 32766000, 20470000, 1627600 and 101600 values all match).
5. **The length prefix of `Hash for [u8]`** (usize LE) is from Rust std knowledge. I could not read the std source here. The 384-style constants cannot prove it. A real-module bit-offset check, for example from a known bitmap in a RESTORE payload, is the only way to confirm the worked offsets in 7.2.
6. **Real-module confirmation of every X vector** (66, 68, 69, 53). No module could be built here (no clang), so false-positive positions and Sizes after scaling come from my reimplementation.
7. **`MEMORY USAGE` key overhead** and the exact value for a given filter.
8. **The bincode layout used by BF.LOAD/AOF** is standard bincode 1.3, but I did not decode a real blob. Not needed for Hotstash unless it wants AOF compatibility with valkey-bloom.
9. **The exact RDB/DUMP bytes** around the module opcodes (the primitive sequence is known, the surrounding Valkey RDB framing was not read). The type id `0x6e5a2899f96dac01` was derived and matches the one sample dump.
10. **Unreleased changes on valkey-bloom's main branch.** I read tag `1.0.1` only.
11. **`src/bloom/utils.rs` unit tests beyond line 760** were not read (they are internal tests, not behaviour documentation).
12. **RedisBloom error wording for older releases.** I read master. Older releases may use the `(0 < error rate range < 1)` wording that valkey-bloom copied.
13. **Behaviour of `BF.INSERT` when `NOCREATE` is combined with `CAPACITY` or `ERROR`.** RedisBloom docs call it an error; valkey-bloom ignores the combination (no check found).
14. **Replica-side `BF.INSERT` on an existing key.** The module replicates verbatim and applies it as a plain add, which I did not execute.
