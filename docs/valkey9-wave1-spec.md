# Valkey 9.1.3 wave-1 behaviour spec

Target: reimplement `HGETDEL`, `HGETEX`, `HSETEX`, `DELIFEQ`, `MSETEX`, `SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE` byte-for-byte, and write differential tests against Valkey 9.1.3.

Evidence labels: everything not marked is **observed** on a live `valkey-server` 9.1.3 (`valkey_version:9.1.3`, standalone, port 7391). Statements marked "(from source)" come from reading `src/t_hash.c`, `src/t_string.c`, `src/pubsub.c`, `src/server.c`, `src/expire.c`, `src/commands/*.json` and were not (or not fully) exercised live.

## 0. Method and conventions

- Vectors in sections 2-6 were captured with `valkey-cli -p 7391 --no-raw` (the interactive style: `(integer) 1`, `1) "a"`, `(nil)`, `(error) ...`). Each vector starts from `FLUSHALL`. Empty-string arguments are printed by the capture harness as nothing (so `HSET h  empty` means field `""`).
- Vectors that need a field or key that is expired but not yet reaped were run with `DEBUG SET-ACTIVE-EXPIRE 0` (server started with `--enable-debug-command yes`) and `DEBUG SLEEP 0.05` after a `PX 1` TTL. This is the only deviation from the requested server command line. Active expiry is on in production, so such states are transient there; the behaviour is still what lazy paths do.
- Relative TTL values (`HTTL`, `TTL`, `HPTTL`) can read 1 lower (or 1 s lower) than the number set under load. Differential tests should use `EXAT`/`PXAT` with a far-future constant and `HPEXPIRETIME`/`PEXPIRETIME`, or tolerate +-1.
- Wire bytes (RESP2 and RESP3) and keyspace-notification traces were captured with a raw-socket harness; `\r\n` are shown escaped. Notifications used `notify-keyspace-events EA` and a `PSUBSCRIBE __keyevent@0__:*` client; "events" lines list `<event> <key>` in arrival order.
- Command names in error text are lowercase full names (`'hsetex'`, `'pubsub|shardnumsub'`).

### 0.1 Command table (from `src/commands/*.json`)

| Command | Arity | Flags | ACL categories | Since | Keys |
|---|---|---|---|---|---|
| HGETDEL | -5 | WRITE FAST | @fast @hash @write | 9.1.0 | argv[1] |
| HGETEX | -5 | WRITE FAST | @fast @hash @write | 9.0.0 | argv[1] (RW ACCESS UPDATE) |
| HSETEX | -6 | WRITE DENYOOM FAST | @fast @hash @write | 9.0.0 | argv[1] |
| DELIFEQ | 3 | WRITE FAST | @fast @string @write | 9.0.0 | argv[1] |
| MSETEX | -4 | WRITE DENYOOM | @slow @string @write | 9.1.0 | keynum form: `numkeys` at argv[1], first key argv[2], step 2 (OW UPDATE) |
| SPUBLISH | 3 | PUBSUB LOADING STALE FAST MAY_REPLICATE | @fast @pubsub | 7.0.0 | argv[1] flagged NOT_KEY |
| SSUBSCRIBE | -2 | PUBSUB NOSCRIPT LOADING STALE | @slow @pubsub | 7.0.0 | argv[1..] NOT_KEY |
| SUNSUBSCRIBE | -1 | PUBSUB NOSCRIPT LOADING STALE | @slow @pubsub | 7.0.0 | argv[1..] NOT_KEY |

### 0.2 Error strings used (exact, without the leading `-`)

| Id | Text |
|---|---|
| E_ARITY | `ERR wrong number of arguments for '<cmd>' command` |
| E_SYNTAX | `ERR syntax error` |
| E_INT | `ERR value is not an integer or out of range` |
| E_NUMF | `ERR numfields should be greater than 0 and match the provided number of fields` |
| E_EXP | `ERR invalid expire time in '<cmd>' command` |
| E_WT | `WRONGTYPE Operation against a key holding the wrong kind of value` |
| E_NUMKEYS | `ERR invalid numkeys value or out of range` |
| E_SUBCTX | ERR Can't execute '&lt;cmd&gt;': only (P&#124;S)SUBSCRIBE / (P&#124;S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context (the real text has a plain pipe: `(P` `|` `S)SUBSCRIBE`; see SP-09 for the exact bytes) |
| E_DENY | `ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client` |

Integer parsing (`getLongLongFromObjectOrReply`) is strict: decimal `long long` only; `abc`, `1.5`, `+1`, ` 1` (leading space), `99999999999999999999` and `9223372036854775808` all give E_INT (observed). A leading `-` is fine.

### 0.3 The shared extended-option parser (from source: `parseExtendedCommandArgumentsOrReply`, `server.c` ~7824)

Used by HGETEX, HSETEX (range = tokens between the key and the `FIELDS` token) and MSETEX (range = tokens after the key/value pairs). All option names are case-insensitive. Any token that is not accepted at that point yields **E_SYNTAX**. Accepted tokens per command:

| Token | HGETEX | HSETEX | MSETEX | Rejected (syntax error) when |
|---|---|---|---|---|
| `NX` | no | yes | yes | `XX` already seen |
| `XX` | no | yes | yes | `NX` already seen |
| `FNX` | no | yes | no | `FXX` already seen |
| `FXX` | no | yes | no | `FNX` already seen |
| `KEEPTTL` | no | yes | yes | any of EX/PX/EXAT/PXAT/PERSIST already seen |
| `PERSIST` | yes | no | no | any of EX/PX/EXAT/PXAT/KEEPTTL already seen |
| `EX n` / `PX n` / `EXAT n` / `PXAT n` | yes | yes | yes | KEEPTTL/PERSIST or a *different* expire token already seen; or the token is the last one in the range (no value) |

Consequences, all observed: the same token may be repeated (`NX NX`, `PERSIST PERSIST`, `EX 10 EX 20` -> last value wins, TTL 20); `NX` + `FNX` or `XX` + `FXX` combine; `GET`, `IFEQ` (SET-only options) are syntax errors here; the expire *value* is not looked at by the parser (it is validated later, see 0.4). Because the HGETEX/HSETEX range ends right before `FIELDS`, `EX FIELDS ...` is a syntax error (EX has no value inside the range).

### 0.4 Expire value conversion

Two different converters exist; they disagree on `0`.

**Hash commands (HGETEX, HSETEX)** - `convertExpireArgumentToUnixTime` (from source, confirmed by vectors):
1. parse as `long long` else E_INT;
2. `< 0` -> E_EXP (so `0` is accepted);
3. EX/EXAT: `when > LLONG_MAX/1000` -> E_EXP, then `when *= 1000`;
4. EX/PX: base = command time snapshot (ms); EXAT/PXAT: base = 0; `when > LLONG_MAX - base` -> E_EXP; `when += base`;
5. `when <= now` (command time snapshot) -> "already expired": the listed fields are **deleted** (see each command); otherwise the TTL is stored as an absolute ms timestamp.
There is **no** upper bound at the hash-field-expiry maximum: `PXAT 9223372036854775807` and `PXAT 281474976710656` (2^48) are accepted and stored (`HPEXPIRETIME` returns them), while `HEXPIRE h 9223372036854775807 ...` is rejected. A reimplementation must not reject these. `EX 9223372036854775807` -> E_EXP (overflow), `EXAT 9223372036854775807` -> E_EXP (x1000 overflow), `PX 9223372036854775807` -> E_EXP (+now overflow), `PXAT 9223372036854775807` -> OK.

**MSETEX** - `getExpireMillisecondsOrReply` (from source, confirmed):
1. parse `long long` else E_INT;
2. `<= 0` -> E_EXP (so `EX 0`, `PX 0`, `EXAT 0` and negatives are errors);
3. EX/EXAT: `> LLONG_MAX/1000` -> E_EXP, then `*1000`;
4. EX/PX only: `ms > LLONG_MAX - now` -> E_EXP, else `ms += now`. EXAT/PXAT: no overflow check, so `PXAT 9223372036854775807` is accepted.
No "already expired deletes" logic: a past absolute time stores the key with a past TTL.

---

## 1. Replication/AOF propagation (from source, informational)

- HGETDEL: propagated verbatim unless the hash had volatile (TTL) fields and some requested fields were absent, in which case it is rewritten to `HDEL key <fields actually deleted>`.
- HGETEX: never verbatim. `HDEL key f...` (past time), `HPEXPIREAT key <abs ms> FIELDS n f...` (only fields whose TTL changed), or `HPERSIST key FIELDS n f...` (only fields that had a TTL).
- HSETEX: rewritten when NX/XX/FNX/FXX or EX/PX/EXAT present: flags NX/XX/FNX/FXX dropped, `EX/PX/EXAT n` replaced by `PXAT <abs ms>`; past time -> `HDEL key <fields that existed>`. Overwritten expired fields under KEEPTTL are additionally propagated as HDEL.
- DELIFEQ: `DEL key`. MSETEX: `EX/PX/EXAT n` replaced by `PXAT <abs ms>`.
- SPUBLISH: `forceCommandPropagation(PROPAGATE_REPL)` in non-cluster mode (replicas get it; not an AOF write command).

---

## 2. HGETDEL key FIELDS numfields field [field ...]

### 2.1 Parsing order
1. Arity < 5 -> E_ARITY (so `HGETDEL h FIELDS 1` is an arity error).
2. `argv[2]` is not `FIELDS` (case-insensitive) -> E_SYNTAX. This is checked **before** the key is looked up, so it wins over WRONGTYPE.
3. `argv[3]` not a `long long` -> E_INT (`abc`, `1.5`, `99999999999999999999`).
4. `numfields == 0`, negative, or `!= argc - 4` -> E_NUMF. Checked before the key lookup, so also wins over WRONGTYPE.
5. Key exists and is not a hash -> E_WT. A missing key is **not** an error (treated as an empty hash).

No other options exist (any extra token just breaks the count in step 4).

### 2.2 Reply
Array of exactly `numfields` elements, in argument order: the bulk string value of the field, or nil (RESP2 `$-1`, RESP3 `_`) if the field is absent. Missing key -> array of nils, key not created. Duplicate field names: first occurrence returns the value (and deletes), later occurrences return nil. Values come back exactly as stored (`007`, `-0`).

### 2.3 Side effects
- Each present field is removed. **When the last field is removed the key is deleted** (`EXISTS` -> 0, `TYPE` -> none).
- Field TTLs of the remaining fields are untouched.
- A field that is expired but not yet reaped is returned as nil and is **not removed** (observed: the hash kept length 1 and still existed after the call).
- Key-level lookup is a write lookup (lazily expires an expired key first) (from source).
- Events (observed): if at least one field was removed: `hdel <key>` once, then `del <key>` if the key was emptied. Nothing is emitted when no field was removed (missing key, or only absent fields).

### 2.4 Test vectors
**HD-01** basic: existing + missing field, hash survives
```text
> HSET h a 1 b 2 c 3
(integer) 3
> HGETDEL h FIELDS 2 a x
1) "1"
2) (nil)
> HGETALL h
1) "b"
2) "2"
3) "c"
4) "3"
```

**HD-02** deleting the last field removes the key
```text
> HGETDEL h FIELDS 2 b c
1) (nil)
2) (nil)
> EXISTS h
(integer) 0
> TYPE h
none
```

**HD-03** missing key: one nil per field, no key created
```text
> HGETDEL nokey FIELDS 2 a b
1) (nil)
2) (nil)
> EXISTS nokey
(integer) 0
```

**HD-04** duplicate field in one call: second occurrence is nil
```text
> HSET h a 1 b 2
(integer) 2
> HGETDEL h FIELDS 3 a a b
1) "1"
2) (nil)
3) "2"
> EXISTS h
(integer) 0
```

**HD-05** arity errors
```text
> HGETDEL
(error) ERR wrong number of arguments for 'hgetdel' command
> HGETDEL h
(error) ERR wrong number of arguments for 'hgetdel' command
> HGETDEL h FIELDS
(error) ERR wrong number of arguments for 'hgetdel' command
> HGETDEL h FIELDS 1
(error) ERR wrong number of arguments for 'hgetdel' command
```

**HD-06** FIELDS token wrong or misplaced (checked before numfields)
```text
> HSET h a 1
(integer) 1
> HGETDEL h FIELDZ 1 a
(error) ERR syntax error
> HGETDEL h a FIELDS 1
(error) ERR syntax error
> HGETDEL h 1 FIELDS a
(error) ERR syntax error
> HGETDEL h fields 1 a
1) "1"
> HGETALL h
(empty array)
> EXISTS h
(integer) 0
```

**HD-07** numfields not an integer / out of range / zero / negative
```text
> HSET h a 1 b 2
(integer) 2
> HGETDEL h FIELDS abc a
(error) ERR value is not an integer or out of range
> HGETDEL h FIELDS 1.5 a
(error) ERR value is not an integer or out of range
> HGETDEL h FIELDS 99999999999999999999 a
(error) ERR value is not an integer or out of range
> HGETDEL h FIELDS 0 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETDEL h FIELDS -1 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETDEL h FIELDS 0 a b
(error) ERR numfields should be greater than 0 and match the provided number of fields
```

**HD-08** numfields mismatch (too many / too few); nothing deleted
```text
> HSET h a 1 b 2
(integer) 2
> HGETDEL h FIELDS 3 a b
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETDEL h FIELDS 1 a b
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETALL h
1) "a"
2) "1"
3) "b"
4) "2"
```

**HD-09** WRONGTYPE, and error ordering (syntax/numfields before type check)
```text
> SET s v
OK
> HGETDEL s FIELDS 1 a
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> HGETDEL s FIELDZ 1 a
(error) ERR syntax error
> HGETDEL s FIELDS 0 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETDEL s FIELDS abc a
(error) ERR value is not an integer or out of range
> HGETDEL nokey FIELDS 0 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
```

**HD-11** field with a TTL is returned and deleted; other TTLs untouched
```text
> DEL h
(integer) 0
> HSETEX h PXAT 4102444800000 FIELDS 2 a 1 b 2
(integer) 1
> HSET h c 3
(integer) 1
> HGETDEL h FIELDS 1 a
1) "1"
> HPEXPIRETIME h FIELDS 3 a b c
1) (integer) -2
2) (integer) 4102444800000
3) (integer) -1
```

**HD-12** expired-but-not-reaped field (active expire disabled): nil; it is not removed by HGETDEL
```text
> DEL h
(integer) 0
> HSETEX h PX 1 FIELDS 1 e v
(integer) 1
> HSET h k 1
(integer) 1
> HGETDEL h FIELDS 2 e k
1) (nil)
2) "1"
> HGETALL h
(empty array)
> HLEN h
(integer) 1
> EXISTS h
(integer) 1
```

**HD-13** case-insensitive FIELDS; binary-safe/empty field names
```text
> HSET h  empty
(integer) 1
> HGETDEL h FiElDs 1 
1) "empty"
> EXISTS h
(integer) 0
```

**HD-14** values come back as stored (ints, leading zeros)
```text
> HSET h n 007 m -0
(integer) 2
> HGETDEL h FIELDS 2 n m
1) "007"
2) "-0"
```

### 2.5 Keyspace notifications (captured)
**EV-HGETDEL**
```text
(setup) > HSET h a 1 b 2
> HGETDEL h FIELDS 1 zz
    reply: '*1\r\n$-1\r\n'
    events: (none)
> HGETDEL h FIELDS 2 a zz
    reply: '*2\r\n$1\r\n1\r\n$-1\r\n'
    events: hdel h
> HGETDEL h FIELDS 1 b
    reply: '*1\r\n$1\r\n2\r\n'
    events: hdel h, del h
> HGETDEL nokey FIELDS 1 a
    reply: '*1\r\n$-1\r\n'
    events: (none)
```

---

## 3. HGETEX key [EX s | PX ms | EXAT ts | PXAT tms | PERSIST] FIELDS numfields field [field ...]

### 3.1 Parsing order
1. Arity < 5 -> E_ARITY.
2. Scan `argv[2 .. argc-2]` for the first token equal to `FIELDS` (case-insensitive). The final argument is never considered, so a trailing `FIELDS` does not count.
   - Found at index `f`: parse the options in `argv[2 .. f-1]` with the shared parser (0.3); any failure -> E_SYNTAX. Then `argv[f+1]` must be a `long long` -> else E_INT. Fields start at `f+2`.
   - Not found: no options are parsed at all, and the result is E_NUMF (e.g. `HGETEX h EX 10 a b`, `HGETEX h EX 10 FIELDS`).
3. `numfields == 0`, negative or `!= argc - (f+2)` -> E_NUMF. `HGETEX h FIELDS 1 a EX 10` is E_NUMF (FIELDS is found at index 2 and the trailing options break the count).
4. Key exists and is not a hash -> E_WT. This precedes validation of the expire value: `HGETEX s EX abc FIELDS 1 a` on a string key gives E_WT.
5. Expire value (if `EX/PX/EXAT/PXAT`): conversion per 0.4 -> E_INT or E_EXP. This runs even when the key does not exist (`HGETEX nokey EX abc FIELDS 1 a` -> E_INT). `PERSIST` has no value.

### 3.2 Reply
Same as HGETDEL: array of `numfields` bulk-or-nil, in argument order, **always** returned (also for a missing key: nils), and the values returned are those from before the change. With a past time the old values are still returned.

### 3.3 Side effects
- No option: pure multi-get, nothing changes, no events.
- `EX/PX/EXAT/PXAT` with a future time: every **existing** field gets that absolute TTL, replacing any previous TTL unconditionally (no NX/GT/LT logic). Absent fields are skipped.
- `EX 0`/`PX 0`/`EXAT <= now`/`PXAT <= now` ("already expired", `when <= now`): the existing fields are **deleted** (values still returned). `EX 0` is not an error here. If the hash becomes empty the key is deleted.
- `PERSIST`: removes the TTL from fields that have one. On a field with no TTL (or an absent field) it is a no-op.
- Expired-but-not-reaped fields reply nil and are untouched: neither `PERSIST` nor `EX` revives or removes them (observed).
- Events (observed), emitted once per command and only if at least one field actually changed: `hexpire` (TTL set), `hexpired` (past time, fields deleted), `hpersist` (TTL removed); then `del` if the key was emptied. Absent-only fields or PERSIST on TTL-less fields emit nothing. The `EXAT 1` case emitted `hexpired` and `del` only when the hash became empty.
- Key-level TTL of the hash is not touched.

### 3.4 Test vectors
**HG-01** no option: plain multi-get, no side effects
```text
> HSET h a 1 b 2
(integer) 2
> HGETEX h FIELDS 3 a b zz
1) "1"
2) "2"
3) (nil)
> HGETALL h
1) "a"
2) "1"
3) "b"
4) "2"
```

**HG-02** EX sets per-field TTL on existing fields only
```text
> HSET h a 1 b 2
(integer) 2
> HGETEX h EX 100 FIELDS 3 a b zz
1) "1"
2) "2"
3) (nil)
> HTTL h FIELDS 3 a b zz
1) (integer) 100
2) (integer) 100
3) (integer) -2
```

**HG-03** PXAT / EXAT / PX set absolute or relative ms TTL
```text
> HSET h a 1 b 2
(integer) 2
> HGETEX h PXAT 4102444800000 FIELDS 1 a
1) "1"
> HPEXPIRETIME h FIELDS 1 a
1) (integer) 4102444800000
> HGETEX h EXAT 4102444800 FIELDS 1 b
1) "2"
> HPEXPIRETIME h FIELDS 1 b
1) (integer) 4102444800000
> HGETEX h PX 100000 FIELDS 1 b
1) "2"
> HPTTL h FIELDS 1 b
1) (integer) 99999
```

**HG-04** PERSIST removes TTL; on a field without TTL it is a no-op
```text
> HSET h a 1 b 2 c 3
(integer) 3
> HEXPIRE h 100 FIELDS 2 a b
1) (integer) 1
2) (integer) 1
> HGETEX h PERSIST FIELDS 3 a b c
1) "1"
2) "2"
3) "3"
> HTTL h FIELDS 3 a b c
1) (integer) -1
2) (integer) -1
3) (integer) -1
```

**HG-05** past EXAT deletes the fields (value still returned); key removed when empty
```text
> HSET h x 1 y 2 z 3
(integer) 3
> HGETEX h EXAT 1 FIELDS 2 x y
1) "1"
2) "2"
> HGETALL h
1) "z"
2) "3"
> HGETEX h PXAT 1 FIELDS 2 z nope
1) "3"
2) (nil)
> EXISTS h
(integer) 0
```

**HG-06** EX 0 and PX 0 are NOT errors: they expire the fields immediately
```text
> HSET h a 1 b 2 c 3
(integer) 3
> HGETEX h EX 0 FIELDS 1 a
1) "1"
> HGETALL h
1) "b"
2) "2"
3) "c"
4) "3"
> HGETEX h PX 0 FIELDS 2 b c
1) "2"
2) "3"
> EXISTS h
(integer) 0
```

**HG-07** negative / non-integer / overflowing expire values
```text
> HSET h a 1
(integer) 1
> HGETEX h EX -1 FIELDS 1 a
(error) ERR invalid expire time in 'hgetex' command
> HGETEX h PX -1 FIELDS 1 a
(error) ERR invalid expire time in 'hgetex' command
> HGETEX h EXAT -1 FIELDS 1 a
(error) ERR invalid expire time in 'hgetex' command
> HGETEX h EX abc FIELDS 1 a
(error) ERR value is not an integer or out of range
> HGETEX h EX 1.5 FIELDS 1 a
(error) ERR value is not an integer or out of range
> HGETEX h EX 9223372036854775807 FIELDS 1 a
(error) ERR invalid expire time in 'hgetex' command
> HGETEX h PX 9223372036854775807 FIELDS 1 a
(error) ERR invalid expire time in 'hgetex' command
> HGETEX h EX 9223372036854775808 FIELDS 1 a
(error) ERR value is not an integer or out of range
> HGETEX h EXAT 9223372036854775807 FIELDS 1 a
(error) ERR invalid expire time in 'hgetex' command
> HGETEX h PXAT 9223372036854775807 FIELDS 1 a
1) "1"
> HGETALL h
1) "a"
2) "1"
```

**HG-08** conflicting / invalid options -> syntax error
```text
> HSET h a 1
(integer) 1
> HGETEX h EX 10 PERSIST FIELDS 1 a
(error) ERR syntax error
> HGETEX h PERSIST EX 10 FIELDS 1 a
(error) ERR syntax error
> HGETEX h EX 10 PX 10 FIELDS 1 a
(error) ERR syntax error
> HGETEX h EX 10 EXAT 10 FIELDS 1 a
(error) ERR syntax error
> HGETEX h NX FIELDS 1 a
(error) ERR syntax error
> HGETEX h KEEPTTL FIELDS 1 a
(error) ERR syntax error
> HGETEX h FOO FIELDS 1 a
(error) ERR syntax error
> HGETEX h EX FIELDS 1 a
(error) ERR syntax error
```

**HG-09** repeated same option is accepted (last wins); repeated PERSIST accepted
```text
> HSET h a 1
(integer) 1
> HGETEX h EX 10 EX 20 FIELDS 1 a
1) "1"
> HTTL h FIELDS 1 a
1) (integer) 20
> HGETEX h PERSIST PERSIST FIELDS 1 a
1) "1"
> HTTL h FIELDS 1 a
1) (integer) -1
```

**HG-10** FIELDS missing / misplaced / numfields errors
```text
> HGETEX h EX 10 a b
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETEX h FIELDS 1 a EX 10
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETEX h FIELDS
(error) ERR wrong number of arguments for 'hgetex' command
> HGETEX h FIELDS 1
(error) ERR wrong number of arguments for 'hgetex' command
> HGETEX h FIELDS abc a
(error) ERR value is not an integer or out of range
> HGETEX h FIELDS 0 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETEX h FIELDS -1 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETEX h FIELDS 2 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETEX h FIELDS 1 a b
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETEX h FIELDS 99999999999999999999 a
(error) ERR value is not an integer or out of range
> HGETEX h EX 10 FIELDS
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HGETEX
(error) ERR wrong number of arguments for 'hgetex' command
> HGETEX h
(error) ERR wrong number of arguments for 'hgetex' command
```

**HG-11** missing key: nils, no key created, expire options ignored
```text
> HGETEX nokey EX 100 FIELDS 2 a b
1) (nil)
2) (nil)
> HGETEX nokey PERSIST FIELDS 1 a
1) (nil)
> HGETEX nokey EXAT 1 FIELDS 1 a
1) (nil)
> EXISTS nokey
(integer) 0
```

**HG-12** WRONGTYPE and ordering (parse errors first; type before expire-value validation)
```text
> SET s v
OK
> HGETEX s FIELDS 1 a
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> HGETEX s EX 10 FIELDS 1 a
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> HGETEX s EX abc FIELDS 1 a
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> HGETEX s EX 10 PERSIST FIELDS 1 a
(error) ERR syntax error
> HGETEX s FIELDS 0 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
```

**HG-13** expire value error leaves hash untouched, even for missing key
```text
> HGETEX nokey EX abc FIELDS 1 a
(error) ERR value is not an integer or out of range
> HGETEX nokey EX -5 FIELDS 1 a
(error) ERR invalid expire time in 'hgetex' command
```

**HG-15** re-setting an existing TTL: EX replaces it, no GT/LT logic
```text
> DEL h
(integer) 0
> HSETEX h EXAT 4102444800 FIELDS 1 a 1
(integer) 1
> HGETEX h EX 50 FIELDS 1 a
1) "1"
> HTTL h FIELDS 1 a
1) (integer) 50
> HGETEX h PXAT 4102444800000 FIELDS 1 a
1) "1"
> HPEXPIRETIME h FIELDS 1 a
1) (integer) 4102444800000
```

**HG-16** expired-but-not-reaped field (active expire off): nil, and PERSIST/EX don't revive it
```text
> DEL h
(integer) 0
> HSETEX h PX 1 FIELDS 1 e v
(integer) 1
> HSET h k 1
(integer) 1
> HGETEX h PERSIST FIELDS 2 e k
1) (nil)
2) "1"
> HGETEX h EX 100 FIELDS 2 e k
1) (nil)
2) "1"
> HLEN h
(integer) 2
> HGETALL h
1) "k"
2) "1"
```

### 3.5 Keyspace notifications (captured)
**EV-HGETEX**
```text
(setup) > HSET h a 1 b 2 c 3
> HGETEX h FIELDS 1 a
    reply: '*1\r\n$1\r\n1\r\n'
    events: (none)
> HGETEX h EX 100 FIELDS 2 a zz
    reply: '*2\r\n$1\r\n1\r\n$-1\r\n'
    events: hexpire h
> HGETEX h EX 100 FIELDS 1 zz
    reply: '*1\r\n$-1\r\n'
    events: (none)
> HGETEX h PERSIST FIELDS 1 b
    reply: '*1\r\n$1\r\n2\r\n'
    events: (none)
> HGETEX h PERSIST FIELDS 2 a zz
    reply: '*2\r\n$1\r\n1\r\n$-1\r\n'
    events: hpersist h
> HGETEX h EXAT 1 FIELDS 2 a b
    reply: '*2\r\n$1\r\n1\r\n$1\r\n2\r\n'
    events: hexpired h
> HGETEX h EX 0 FIELDS 1 c
    reply: '*1\r\n$1\r\n3\r\n'
    events: hexpired h, del h
> EXISTS h
    reply: ':0\r\n'
    events: (none)
```

---

## 4. HSETEX key [NX | XX] [FNX | FXX] [EX s | PX ms | EXAT ts | PXAT tms | KEEPTTL] FIELDS numfields field value [field value ...]

### 4.1 Parsing order
1. Arity < 6 -> E_ARITY (`HSETEX h FIELDS 1 a` is an arity error).
2. FIELDS scan and option parse exactly as HGETEX 3.1 step 2 (options allowed here: NX XX FNX FXX KEEPTTL EX PX EXAT PXAT; `PERSIST`, `GET` -> E_SYNTAX). Then `argv[f+1]` -> `long long` else E_INT.
3. `numfields == 0`, negative, `> LLONG_MAX/2`, or `numfields*2 != argc - (f+2)` -> E_NUMF. FIELDS not found -> E_NUMF. (`FIELDS 9223372036854775807 a 1` and `FIELDS 4611686018427387904 a 1` are E_NUMF; `99999999999999999999` is E_INT.)
4. Key exists and is not a hash -> E_WT (before any NX/XX/expire evaluation).
5. Key-level condition: `NX` and the key exists, or `XX` and the key is missing -> reply `0` immediately. This precedes expire-value validation: `HSETEX existing NX EX abc FIELDS 1 a 1` -> `0`; `HSETEX nokey XX EX abc ...` -> `0`.
6. If not KEEPTTL and an expire option was given: conversion per 0.4 -> E_INT / E_EXP. A bad expire value with `FNX`/`FXX` also errors here, before the field check.
7. Field-level condition (only if FNX or FXX): all fields are checked **before any write**, against the hash state as it was at command start (earlier pairs in the same call are not visible). FNX: any listed field already exists -> reply `0`, nothing changed. FXX: any listed field missing (or the key is missing) -> reply `0`, no key created. FNX on a missing key passes. Expired-not-reaped fields count as absent for FNX (observed: `FNX` over such a field succeeded).

### 4.2 Reply
Integer `1` if all checks passed (including the "past time" case and including `numfields` pairs that changed nothing), integer `0` if a NX/XX/FNX/FXX condition failed. There is no other success value. Same bytes in RESP2 and RESP3 (`:1`, `:0`).

### 4.3 Side effects
- The key is created if missing (and `NX`/no-`XX`/no-`FXX` allow it). `XX` and `FXX` never create it.
- Each listed field is set to its value. Duplicate field names in one call: last pair wins (`FIELDS 2 a 1 a 2` -> `a=2`).
- TTL handling per field: no expire option -> any existing field TTL is **cleared** (field becomes persistent); `EX/PX/EXAT/PXAT` -> set to the same absolute ms for every pair, replacing old TTLs; `KEEPTTL` -> existing field TTL kept, new fields get none.
- Past time (`when <= now`, includes `EX 0`/`PX 0`): the listed fields are **deleted instead of set**; reply is still `1`; if the hash ends up empty the key is deleted. For a key that did not exist it is created and removed in the same command (never visible). Naming a field that does not exist is fine (no-op, still `1`).
- The hash key's own TTL (`EXPIRE`) is never changed by HSETEX (observed: `TTL` stayed 1000).
- Overwriting a field that was expired but not reaped counts as success and emits `hexpired` before `hset`.
- No upper bound on absolute time beyond `long long` (see 0.4).
- Events (observed, in this order; `hset` is emitted whenever the command passes all checks, even for the no-op past-time case): `hset`; then `hexpire` if any of EX/PX/EXAT/PXAT was given (also when the time is in the past); then `hexpired` if the time was in the past; then `del` if the hash is empty afterwards. KEEPTTL emits only `hset`. Failed NX/XX/FNX/FXX and all errors emit nothing. If overwritten fields were expired-not-reaped, `hexpired` is emitted first, then `hset`.
- "(from source)": for an existing hash and a past time naming only absent fields the events `hset`, `hexpire`, `hexpired` are still emitted, with no `del`.

### 4.4 Test vectors
**HS-01** create key, no options: reply 1; no TTL
```text
> HSETEX h FIELDS 2 a 1 b 2
(integer) 1
> HGETALL h
1) "a"
2) "1"
3) "b"
4) "2"
> HTTL h FIELDS 2 a b
1) (integer) -1
2) (integer) -1
```

**HS-02** EX sets TTL on every listed field
```text
> HSETEX h EX 100 FIELDS 2 a 1 b 2
(integer) 1
> HTTL h FIELDS 2 a b
1) (integer) 100
2) (integer) 100
```

**HS-03** EXAT / PXAT / PX
```text
> HSETEX h EXAT 4102444800 FIELDS 1 a 1
(integer) 1
> HPEXPIRETIME h FIELDS 1 a
1) (integer) 4102444800000
> HSETEX h PXAT 4102444800000 FIELDS 1 a 1
(integer) 1
> HPEXPIRETIME h FIELDS 1 a
1) (integer) 4102444800000
> HSETEX h PX 100000 FIELDS 1 a 1
(integer) 1
> HPTTL h FIELDS 1 a
1) (integer) 99999
```

**HS-04** no option on a field that has a TTL clears that TTL
```text
> HSETEX h PXAT 4102444800000 FIELDS 1 a 1
(integer) 1
> HSETEX h FIELDS 1 a 2
(integer) 1
> HPEXPIRETIME h FIELDS 1 a
1) (integer) -1
```

**HS-05** KEEPTTL keeps field TTL; new field gets none
```text
> HSETEX h PXAT 4102444800000 FIELDS 1 a 1
(integer) 1
> HSETEX h KEEPTTL FIELDS 2 a 2 n 9
(integer) 1
> HPEXPIRETIME h FIELDS 2 a n
1) (integer) 4102444800000
2) (integer) -1
> HGET h a
"2"
```

**HS-06** key-level NX / XX (whole key), XX does not create
```text
> DEL h
(integer) 0
> HSETEX h XX FIELDS 1 a 1
(integer) 0
> EXISTS h
(integer) 0
> HSETEX h NX FIELDS 1 a 1
(integer) 1
> HSETEX h NX FIELDS 1 b 2
(integer) 0
> HGETALL h
1) "a"
2) "1"
> HSETEX h XX FIELDS 1 b 2
(integer) 1
> HGETALL h
1) "a"
2) "1"
3) "b"
4) "2"
```

**HS-07** FNX is all-or-nothing: one existing field blocks all
```text
> DEL h
(integer) 0
> HSET h a 1
(integer) 1
> HSETEX h FNX FIELDS 2 b 2 a 9
(integer) 0
> HGETALL h
1) "a"
2) "1"
> HSETEX h FNX FIELDS 2 b 2 c 3
(integer) 1
> HGETALL h
1) "a"
2) "1"
3) "b"
4) "2"
5) "c"
6) "3"
```

**HS-08** FXX is all-or-nothing; FXX on missing key is 0, no key created
```text
> DEL h
(integer) 0
> HSETEX h FXX FIELDS 1 a 1
(integer) 0
> EXISTS h
(integer) 0
> HSET h a 1 b 2
(integer) 2
> HSETEX h FXX FIELDS 2 a 10 zz 0
(integer) 0
> HGETALL h
1) "a"
2) "1"
3) "b"
4) "2"
> HSETEX h FXX FIELDS 2 a 10 b 20
(integer) 1
> HGETALL h
1) "a"
2) "10"
3) "b"
4) "20"
```

**HS-09** FNX/FXX duplicate fields in one call; FNX does not see earlier pairs of same call
```text
> DEL h
(integer) 0
> HSETEX h FNX FIELDS 2 a 1 a 2
(integer) 1
> HGETALL h
1) "a"
2) "2"
```

**HS-10** past expire: fields are deleted (not set); reply still 1; empty key removed
```text
> DEL h
(integer) 0
> HSET h a 1 b 2 c 3
(integer) 3
> HSETEX h EXAT 1 FIELDS 2 a x b y
(integer) 1
> HGETALL h
1) "c"
2) "3"
> HSETEX h PXAT 1 FIELDS 1 c z
(integer) 1
> EXISTS h
(integer) 0
> HSETEX newkey PXAT 1 FIELDS 1 q z
(integer) 1
> EXISTS newkey
(integer) 0
```

**HS-11** EX 0 / PX 0 are not errors: they delete the listed fields
```text
> HSET h a 1 b 2
(integer) 2
> HSETEX h EX 0 FIELDS 1 a v
(integer) 1
> HGETALL h
1) "b"
2) "2"
> HSETEX h PX 0 FIELDS 1 b v
(integer) 1
> EXISTS h
(integer) 0
```

**HS-12** invalid expire values
```text
> HSETEX h EX -1 FIELDS 1 a 1
(error) ERR invalid expire time in 'hsetex' command
> HSETEX h PX -1 FIELDS 1 a 1
(error) ERR invalid expire time in 'hsetex' command
> HSETEX h EXAT -1 FIELDS 1 a 1
(error) ERR invalid expire time in 'hsetex' command
> HSETEX h EX abc FIELDS 1 a 1
(error) ERR value is not an integer or out of range
> HSETEX h EX 1.5 FIELDS 1 a 1
(error) ERR value is not an integer or out of range
> HSETEX h EX 9223372036854775807 FIELDS 1 a 1
(error) ERR invalid expire time in 'hsetex' command
> HSETEX h PX 9223372036854775807 FIELDS 1 a 1
(error) ERR invalid expire time in 'hsetex' command
> HSETEX h EX 9223372036854775808 FIELDS 1 a 1
(error) ERR value is not an integer or out of range
> HSETEX h EXAT 9223372036854775807 FIELDS 1 a 1
(error) ERR invalid expire time in 'hsetex' command
> HSETEX h PXAT 9223372036854775807 FIELDS 1 a 1
(integer) 1
> EXISTS h
(integer) 1
```

**HS-13** conflicting options -> syntax error
```text
> HSETEX h NX XX FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h XX NX FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h FNX FXX FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h EX 10 PX 10 FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h KEEPTTL EX 10 FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h EX 10 KEEPTTL FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h PERSIST FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h GET FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h EX FIELDS 1 a 1
(error) ERR syntax error
> HSETEX h FOO FIELDS 1 a 1
(error) ERR syntax error
```

**HS-14** allowed combos: NX+FNX, NX+EX, repeated flags
```text
> DEL h
(integer) 0
> HSETEX h NX FNX EX 50 FIELDS 1 a 1
(integer) 1
> HTTL h FIELDS 1 a
1) (integer) 50
> HSETEX h XX XX FXX FXX EX 10 EX 60 FIELDS 1 a 2
(integer) 1
> HTTL h FIELDS 1 a
1) (integer) 60
```

**HS-15** FIELDS / numfields errors
```text
> HSETEX h EX 10 a 1
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSETEX h FIELDS 1 a 1 EX 10
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSETEX h FIELDS 1 a
(error) ERR wrong number of arguments for 'hsetex' command
> HSETEX h FIELDS abc a 1
(error) ERR value is not an integer or out of range
> HSETEX h FIELDS 0 a 1
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSETEX h FIELDS -1 a 1
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSETEX h FIELDS 2 a 1 b
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSETEX h FIELDS 1 a 1 b 2
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSETEX h FIELDS 9223372036854775807 a 1
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSETEX h FIELDS 4611686018427387904 a 1
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSETEX h FIELDS 99999999999999999999 a 1
(error) ERR value is not an integer or out of range
> HSETEX h
(error) ERR wrong number of arguments for 'hsetex' command
> HSETEX h FIELDS
(error) ERR wrong number of arguments for 'hsetex' command
> HSETEX h EX 10 FIELDS 1 a
(error) ERR numfields should be greater than 0 and match the provided number of fields
```

**HS-16** WRONGTYPE and ordering: type check precedes NX/XX/expire-value checks; NX/XX 0-reply precedes expire-value validation
```text
> SET s v
OK
> HSETEX s FIELDS 1 a 1
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> HSETEX s EX abc FIELDS 1 a 1
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> HSETEX s NX FIELDS 1 a 1
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> HSETEX s FIELDS 0 a 1
(error) ERR numfields should be greater than 0 and match the provided number of fields
> HSET h a 1
(integer) 1
> HSETEX h NX EX abc FIELDS 1 a 1
(integer) 0
> HSETEX nokey XX EX abc FIELDS 1 a 1
(integer) 0
> HSETEX h FNX EX abc FIELDS 1 b 1
(error) ERR value is not an integer or out of range
```

**HS-17** key-level TTL is untouched by HSETEX
```text
> DEL h
(integer) 0
> HSET h a 1
(integer) 1
> EXPIRE h 1000
(integer) 1
> HSETEX h EX 50 FIELDS 1 a 2
(integer) 1
> TTL h
(integer) 1000
> HSETEX h FIELDS 1 b 2
(integer) 1
> TTL h
(integer) 1000
```

**HS-18** overwriting an expired-not-reaped field: counts as success; FNX sees it as absent
```text
> DEL h
(integer) 0
> HSETEX h PX 1 FIELDS 1 e v
(integer) 1
> HSET h k 1
(integer) 1
> HSETEX h FNX FIELDS 1 e new
(integer) 1
> HGETALL h
1) "e"
2) "new"
3) "k"
4) "1"
```

**HS-19** binary-safe / empty field and value
```text
> DEL h
(integer) 0
> HSETEX h FIELDS 1  
(integer) 1
> HGETALL h
1) ""
2) ""
```

### 4.5 Keyspace notifications (captured)
**EV-HSETEX**
```text
> HSETEX h FIELDS 1 a 1
    reply: ':1\r\n'
    events: hset h
> HSETEX h EX 100 FIELDS 1 a 1
    reply: ':1\r\n'
    events: hset h, hexpire h
> HSETEX h KEEPTTL FIELDS 1 a 2
    reply: ':1\r\n'
    events: hset h
> HSETEX h NX FIELDS 1 a 3
    reply: ':0\r\n'
    events: (none)
> HSETEX h FNX FIELDS 1 a 3
    reply: ':0\r\n'
    events: (none)
> HSETEX h EXAT 1 FIELDS 1 a 3
    reply: ':1\r\n'
    events: hset h, hexpire h, hexpired h, del h
> EXISTS h
    reply: ':0\r\n'
    events: (none)
> HSETEX n PXAT 1 FIELDS 1 a 3
    reply: ':1\r\n'
    events: hset n, hexpire n, hexpired n, del n
> EXISTS n
    reply: ':0\r\n'
    events: (none)
> HSETEX n EX -1 FIELDS 1 a 3
    reply: "-ERR invalid expire time in 'hsetex' command\r\n"
    events: (none)
```
**EV-HSETEX-overwrite-expired**
```text
(setup) > HSETEX h PX 1 FIELDS 1 e v
(setup) > HSET h k 1
(setup) > DEBUG SLEEP 0.05
> HSETEX h FIELDS 1 e new
    reply: ':1\r\n'
    events: hexpired h, hset h
> HSET h e x
    reply: ':0\r\n'
    events: hset h
```
**EV-active-field-expiry (active expire ON)**
```text
> HSETEX h PX 100 FIELDS 1 a 1
    reply: ':1\r\n'
    events: hset h, hexpire h, hexpired h, del h
> HSETEX h FIELDS 1 b 1
    reply: ':1\r\n'
    events: hset h
```
**EV-active-expiry-wait**
```text
(setup) > HSETEX h PX 100 FIELDS 2 a 1 b 2   (then wait 0.8 s, active expire ON)
    events after the wait: hset h, hexpire h, hexpired h, del h
    EXISTS h -> ':0\r\n'
```

---

## 5. DELIFEQ key value

### 5.1 Parsing / checks
1. Arity != 3 -> E_ARITY. There are no options.
2. Write lookup of the key (an expired key is treated as missing and lazily expired first).
3. Missing key -> `0`.
4. Key exists and is not a string (hash, list, set, ...) -> **E_WT**, not `0` (observed for hash, list, set).
5. Compare: binary-safe, case-sensitive string comparison of the stored value and `argv[2]`. Integer-encoded values compare by their decimal string: stored `123` does not match `0123`, `+123`, `123.0` (observed); a value produced by `INCR` matches its decimal text. Empty string matches empty string.

### 5.2 Reply and side effects
- Match: key is deleted (any TTL goes with it), reply integer `1`. Event `del <key>` only (observed). Propagated as `DEL key`.
- Mismatch or missing: reply integer `0`, no change, no event.
- Same bytes in RESP2 and RESP3 (`:1`/`:0`).

### 5.3 Test vectors
**DE-01** match: deletes, reply 1
```text
> SET k v
OK
> DELIFEQ k v
(integer) 1
> EXISTS k
(integer) 0
```

**DE-02** mismatch: key kept, reply 0
```text
> SET k v
OK
> DELIFEQ k V
(integer) 0
> DELIFEQ k vv
(integer) 0
> DELIFEQ k 
(integer) 0
> GET k
"v"
```

**DE-03** missing key: 0
```text
> DELIFEQ nokey v
(integer) 0
```

**DE-04** non-string key: WRONGTYPE (checked before comparing)
```text
> DEL h
(integer) 0
> HSET h a 1
(integer) 1
> DELIFEQ h x
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> LPUSH l a
(integer) 1
> DELIFEQ l a
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> SADD st a
(integer) 1
> DELIFEQ st a
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

**DE-05** arity
```text
> DELIFEQ
(error) ERR wrong number of arguments for 'delifeq' command
> DELIFEQ k
(error) ERR wrong number of arguments for 'delifeq' command
> DELIFEQ k v extra
(error) ERR wrong number of arguments for 'delifeq' command
```

**DE-06** integer-looking values compare as strings: 123 vs 0123 vs +123
```text
> SET n 123
OK
> DELIFEQ n 0123
(integer) 0
> DELIFEQ n +123
(integer) 0
> DELIFEQ n 123.0
(integer) 0
> DELIFEQ n 123
(integer) 1
> EXISTS n
(integer) 0
```

**DE-07** empty value and binary-ish value
```text
> SET e 
OK
> DELIFEQ e 
(integer) 1
> EXISTS e
(integer) 0
> SET b a b
OK
> DELIFEQ b a b
(integer) 1
> EXISTS b
(integer) 0
```

**DE-08** key with a TTL is deleted like any key; after match nothing remains
```text
> SET k v EX 100
OK
> DELIFEQ k v
(integer) 1
> TTL k
(integer) -2
```

**DE-09** long (non-embedded) values and INCR-made ints
```text
> SET big xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OK
> DELIFEQ big xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
(integer) 0
> DELIFEQ big xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
(integer) 1
> SET c 5
OK
> INCR c
(integer) 6
> DELIFEQ c 6
(integer) 1
> EXISTS c
(integer) 0
```

**DE-10** expired-not-reaped key counts as missing
```text
> SET k v PX 1
OK
> DELIFEQ k v
(integer) 0
```

### 5.4 Keyspace notifications (captured)
**EV-DELIFEQ**
```text
(setup) > SET k v
> DELIFEQ k x
    reply: ':0\r\n'
    events: (none)
> DELIFEQ k v
    reply: ':1\r\n'
    events: del k
> DELIFEQ k v
    reply: ':0\r\n'
    events: (none)
```

---

## 6. MSETEX numkeys key value [key value ...] [NX | XX] [EX s | PX ms | EXAT ts | PXAT tms | KEEPTTL]

### 6.1 Parsing order
1. Arity < 4 -> E_ARITY (`MSETEX 1 a` is arity).
2. `argv[1]` must be an integer in `[1, 2147483647]` -> else **E_NUMKEYS** (covers `0`, negatives, `abc`, `1.5`, `2147483648`, `99999999999999999999`; note this error is used instead of E_INT).
3. `args_start = 2 + 2*numkeys`; if `args_start > argc` -> E_SYNTAX (not enough key/value pairs, e.g. `MSETEX 2 a 1`, `MSETEX 2147483647 a 1`).
4. Options parsed from `args_start` to the end with the shared parser (0.3): NX, XX, KEEPTTL, EX/PX/EXAT/PXAT. Anything else (`FNX`, `FXX`, `PERSIST`, `GET`, unknown tokens, **extra key/value pairs beyond numkeys**) -> E_SYNTAX. Nothing is written when this fails.
5. Expire value (if EX/PX/EXAT/PXAT) via the MSETEX converter 0.4: E_INT (not an integer), E_EXP (`<= 0`, overflow). Validated **before** the NX/XX check, so a bad value beats a failing NX.
6. NX/XX: every key is checked, in order, before any write. `NX` and any key exists, or `XX` and any key missing -> reply `0`, nothing written (atomic all-or-nothing). Duplicate keys in the list do not interfere with NX (all checks happen before writes).

### 6.2 Reply
Integer `1` on success, integer `0` when NX/XX failed (`:1`/`:0` in both RESP versions).

### 6.3 Side effects
- Keys are written in argument order, each overwriting any existing value **of any type** (a hash becomes a string; no WRONGTYPE).
- TTL: no TTL option -> any old TTL is cleared; `KEEPTTL` -> old TTL kept (new keys have none); `EX/PX/EXAT/PXAT` -> the TTL of every key is the **same absolute ms timestamp** (`now + ms`, or the given absolute time), replacing old TTLs. (`EX n` gives `TTL` = n.)
- Duplicate keys in one call: last pair wins.
- A past absolute time (`PXAT 1`, `EXAT 1`) is accepted: reply `1`, the key is stored with a past expiry and is immediately invisible to reads (`EXISTS` 0, `GET` nil); the first access performs the lazy delete and emits `expired`.
- Events per key, in key order (observed): `set <key>`, then `expire <key>` if an expire option was given (also for a past time). No event for KEEPTTL beyond `set`. No events when NX/XX failed or on errors. For a key that was a hash the only event is `set`.
- DENYOOM: rejected with OOM when over `maxmemory` (from source/flags).

### 6.4 Test vectors
**MS-01** basic: two keys, no expiry
```text
> MSETEX 2 a 1 b 2
(integer) 1
> MGET a b
1) "1"
2) "2"
> TTL a
(integer) -1
> TTL b
(integer) -1
```

**MS-02** EXAT/PXAT give exact expiry for all keys; PX/EX relative
```text
> MSETEX 2 a 1 b 2 PXAT 4102444800000
(integer) 1
> PEXPIRETIME a
(integer) 4102444800000
> PEXPIRETIME b
(integer) 4102444800000
> MSETEX 2 a 1 b 2 EXAT 4102444800
(integer) 1
> PEXPIRETIME a
(integer) 4102444800000
> MSETEX 1 a 1 EX 100
(integer) 1
> TTL a
(integer) 100
> MSETEX 1 a 1 PX 100000
(integer) 1
> PTTL a
(integer) 99999
```

**MS-03** NX is all-or-nothing: one existing key blocks every key
```text
> DEL a b c
(integer) 0
> SET b old
OK
> MSETEX 3 a 1 b 2 c 3 NX
(integer) 0
> MGET a b c
1) (nil)
2) "old"
3) (nil)
> DEL b
(integer) 1
> MSETEX 3 a 1 b 2 c 3 NX
(integer) 1
> MGET a b c
1) "1"
2) "2"
3) "3"
```

**MS-04** XX is all-or-nothing: one missing key blocks every key
```text
> DEL a b c
(integer) 0
> SET a old
OK
> MSETEX 2 a 1 b 2 XX
(integer) 0
> MGET a b
1) "old"
2) (nil)
> SET b old
OK
> MSETEX 2 a 1 b 2 XX
(integer) 1
> MGET a b
1) "1"
2) "2"
```

**MS-05** numkeys errors
```text
> MSETEX 0 a 1
(error) ERR invalid numkeys value or out of range
> MSETEX -1 a 1
(error) ERR invalid numkeys value or out of range
> MSETEX abc a 1
(error) ERR invalid numkeys value or out of range
> MSETEX 1.5 a 1
(error) ERR invalid numkeys value or out of range
> MSETEX 2147483648 a 1
(error) ERR invalid numkeys value or out of range
> MSETEX 2147483647 a 1
(error) ERR syntax error
> MSETEX 99999999999999999999 a 1
(error) ERR invalid numkeys value or out of range
> MSETEX 2 a 1
(error) ERR syntax error
> MSETEX 3 a 1 b 2
(error) ERR syntax error
> MSETEX 2 a 1 b
(error) ERR syntax error
```

**MS-06** numkeys smaller than supplied pairs: extras parsed as options -> syntax error
```text
> MSETEX 1 a 1 b 2
(error) ERR syntax error
> MSETEX 1 a 1 b 2 EX 10
(error) ERR syntax error
> MGET a b
1) (nil)
2) (nil)
```

**MS-07** arity
```text
> MSETEX
(error) ERR wrong number of arguments for 'msetex' command
> MSETEX 1
(error) ERR wrong number of arguments for 'msetex' command
> MSETEX 1 a
(error) ERR wrong number of arguments for 'msetex' command
```

**MS-08** conflicting / invalid options -> syntax error
```text
> MSETEX 1 a 1 NX XX
(error) ERR syntax error
> MSETEX 1 a 1 XX NX
(error) ERR syntax error
> MSETEX 1 a 1 EX 10 PX 10
(error) ERR syntax error
> MSETEX 1 a 1 EX 10 KEEPTTL
(error) ERR syntax error
> MSETEX 1 a 1 KEEPTTL EX 10
(error) ERR syntax error
> MSETEX 1 a 1 PERSIST
(error) ERR syntax error
> MSETEX 1 a 1 GET
(error) ERR syntax error
> MSETEX 1 a 1 FNX
(error) ERR syntax error
> MSETEX 1 a 1 EX
(error) ERR syntax error
> MSETEX 1 a 1 FOO
(error) ERR syntax error
> MSETEX 1 a 1 EX 10 EX 20
(integer) 1
> TTL a
(integer) 20
> MSETEX 1 a 1 NX NX XX
(error) ERR syntax error
```

**MS-09** invalid expire values: 0 and negative ARE errors here
```text
> MSETEX 1 a 1 EX 0
(error) ERR invalid expire time in 'msetex' command
> MSETEX 1 a 1 PX 0
(error) ERR invalid expire time in 'msetex' command
> MSETEX 1 a 1 EXAT 0
(error) ERR invalid expire time in 'msetex' command
> MSETEX 1 a 1 EX -1
(error) ERR invalid expire time in 'msetex' command
> MSETEX 1 a 1 EX abc
(error) ERR value is not an integer or out of range
> MSETEX 1 a 1 EX 1.5
(error) ERR value is not an integer or out of range
> MSETEX 1 a 1 EX 9223372036854775807
(error) ERR invalid expire time in 'msetex' command
> MSETEX 1 a 1 PX 9223372036854775807
(error) ERR invalid expire time in 'msetex' command
> MSETEX 1 a 1 EX 9223372036854775808
(error) ERR value is not an integer or out of range
> MSETEX 1 a 1 EXAT 9223372036854775807
(error) ERR invalid expire time in 'msetex' command
> MSETEX 1 a 1 PXAT 9223372036854775807
(integer) 1
> EXISTS a
(integer) 1
```

**MS-10** past absolute time: reply 1, key is created then immediately invisible
```text
> DEL a
(integer) 0
> MSETEX 1 a 1 PXAT 1
(integer) 1
> EXISTS a
(integer) 0
> GET a
(nil)
> MSETEX 1 a 1 EXAT 1
(integer) 1
> EXISTS a
(integer) 0
```

**MS-11** KEEPTTL keeps old TTL; no option clears TTL; EX replaces TTL
```text
> SET a x PXAT 4102444800000
OK
> MSETEX 1 a 1 KEEPTTL
(integer) 1
> PEXPIRETIME a
(integer) 4102444800000
> MSETEX 1 a 2
(integer) 1
> PEXPIRETIME a
(integer) -1
> SET b x PXAT 4102444800000
OK
> MSETEX 1 b 2 EX 100
(integer) 1
> TTL b
(integer) 100
```

**MS-12** overwrites any type (like MSET); no WRONGTYPE
```text
> DEL a h
(integer) 0
> HSET h f v
(integer) 1
> MSETEX 1 h str
(integer) 1
> TYPE h
string
> GET h
"str"
```

**MS-13** duplicate keys in one call: last wins; NX still succeeds on absent key
```text
> DEL d
(integer) 0
> MSETEX 2 d 1 d 2 NX
(integer) 1
> GET d
"2"
> MSETEX 2 d 1 d 2 XX
(integer) 1
> GET d
"2"
```

**MS-14** expire value is validated before the NX/XX check (bad expire value beats a failing NX)
```text
> SET a old
OK
> MSETEX 1 a 1 NX EX abc
(error) ERR value is not an integer or out of range
> MSETEX 1 a 1 NX EX 0
(error) ERR invalid expire time in 'msetex' command
```

**MS-16** empty key and empty value; binary-safe
```text
> MSETEX 1  
(integer) 1
> EXISTS 
(integer) 1
> GET 
""
```

**MS-17** numkeys with option-looking keys (key named NX / EX)
```text
> MSETEX 2 NX 1 EX 2
(integer) 1
> MGET NX EX
1) "1"
2) "2"
```

### 6.5 Keyspace notifications (captured)
**EV-MSETEX**
```text
> MSETEX 2 a 1 b 2
    reply: ':1\r\n'
    events: set a, set b
> MSETEX 2 a 1 b 2 EX 100
    reply: ':1\r\n'
    events: set a, expire a, set b, expire b
> MSETEX 1 a 1 KEEPTTL
    reply: ':1\r\n'
    events: set a
> MSETEX 2 a 1 c 2 NX
    reply: ':0\r\n'
    events: (none)
> MSETEX 1 p 1 PXAT 1
    reply: ':1\r\n'
    events: set p, expire p
> GET p
    reply: '$-1\r\n'
    events: expired p
> MSETEX 1 a 1 EX 0
    reply: "-ERR invalid expire time in 'msetex' command\r\n"
    events: (none)
```
**EV-MSETEX-overwrite-hash**
```text
(setup) > HSET h f v
> MSETEX 1 h x
    reply: ':1\r\n'
    events: set h
```

---

## 7. Sharded pub/sub (non-cluster mode): SPUBLISH, SSUBSCRIBE, SUNSUBSCRIBE

### 7.1 Model
- Separate namespace: shard channels live in `server.pubsubshard_channels`, plain channels in `pubsub_channels`, patterns in `pubsub_patterns`. `PUBLISH` reaches only `SUBSCRIBE`/`PSUBSCRIBE` clients; `SPUBLISH` reaches only `SSUBSCRIBE` clients. A pattern subscription (`PSUBSCRIBE *`) never receives an `SPUBLISH`; shard subscribers never receive `PUBLISH`. `PUBSUB CHANNELS`/`NUMSUB`/`NUMPAT` ignore shard channels; `PUBSUB SHARDCHANNELS`/`SHARDNUMSUB` ignore the others. (SP-03, SP-04, SP-12)
- Non-cluster: no hash-slot or CROSSSLOT checks; all shard channels are in one table (slot 0) (from source). No pattern support for shard channels; channel names are binary-safe (empty name, spaces and CRLF in payload all work, SP-15).
- Subscribe-mode flag is shared: after any of SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE the client is in "pubsub" mode until its **total** subscription count (channels + patterns + shard channels) is 0.

### 7.2 Frames (RESP2 = array `*3`, RESP3 = push `>3`; identical payloads)
| Frame | Elements | Count element |
|---|---|---|
| ssubscribe | `"ssubscribe"`, channel | number of **shard channels** this client now has (patterns and plain channels not counted) |
| sunsubscribe | `"sunsubscribe"`, channel (or nil) | number of shard channels left |
| smessage | `"smessage"`, channel, payload | - |
| subscribe/unsubscribe (for contrast) | name, channel | channels **plus patterns**, shard excluded |

Examples (RESP2): `*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n`; RESP3 replaces the leading `*3` with `>3`. `smessage`: `*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n`. nil channel: RESP2 `$-1\r\n`, RESP3 `_\r\n`.

### 7.3 Command rules
- **SSUBSCRIBE ch [ch ...]** (arity -2; E_ARITY if no channel): one `ssubscribe` frame per argument, in order, even for duplicates (`SSUBSCRIBE ch ch` -> two frames, both count 1; the client is counted once by SHARDNUMSUB). Inside `MULTI`, queued as usual but at `EXEC` the element is the error E_DENY (unlike SUBSCRIBE, which works inside MULTI; observed SP-14). The `EXEC` array contains the error as that element. (E_DENY is also returned for any client with the deny-blocking flag, e.g. scripts - from source.)
- **SUNSUBSCRIBE [ch ...]** (arity -1): with names: one `sunsubscribe` frame per name in order, **including names the client is not subscribed to** (count = current shard count, so `0` for a fresh client). With no names: one frame per currently subscribed shard channel (iteration order unspecified; tests must sort), the count falling to 0. With no names and no shard subscriptions: a **single** frame `sunsubscribe`, nil, `0` (RESP2 `*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n`, RESP3 `>3\r\n$12\r\nsunsubscribe\r\n_\r\n:0\r\n`) - this works on a connection that has never subscribed and leaves it in normal mode (a following `GET` works). After the call, if total subscriptions are 0 the client leaves pubsub mode; if plain channels/patterns remain it stays in pubsub mode and the count in the frame still reports shard channels only (SP-08).
- **SPUBLISH ch msg** (arity 3): reply integer = number of **clients** currently subscribed to exactly that shard channel (0 if none; also 0 if only SUBSCRIBE/PSUBSCRIBE listeners exist). Each receiver gets one `smessage` push. In RESP3, a client that publishes to a channel it subscribes to receives its own reply `:1` first and then the push (SP-10). In cluster mode there is an extra message-size check and cluster propagation (not covered). In non-cluster mode the command is propagated to replicas.
- **RESP2 context restriction applies to SSUBSCRIBE**: while the client is in pubsub mode (any subscription kind) and uses RESP2, only `PING`, `SUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `SUNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `QUIT`, `RESET` run; everything else (including `SPUBLISH`, `GET`, `PUBSUB ...`) -> E_SUBCTX with the lowercase full command name (`'get'`, `'spublish'`, `'pubsub|shardnumsub'`). `PING` replies as a pubsub array: `*2\r\n$4\r\npong\r\n$0\r\n\r\n` (or the echoed argument). In RESP3 there is no restriction (`GET` works, `PING` -> `+PONG`) (SP-09, SP-10).
- `RESET` and disconnect drop all subscriptions (SP-13).
- `PUBSUB SHARDCHANNELS [glob]`: flat array of channel names that have >= 1 subscriber, optionally filtered by a case-sensitive glob; order unspecified; more than one argument -> `ERR unknown subcommand or wrong number of arguments for 'SHARDCHANNELS'. Try PUBSUB HELP.` (subcommand echoed as typed); empty -> `*0`.
- `PUBSUB SHARDNUMSUB [ch ...]`: flat array `[ch, n, ch, n ...]` in argument order (duplicates repeated, `0` for unknown); no arguments -> `*0`. A flat array in both RESP versions (from source: `addReplyArrayLen`, no map). Unknown subcommand -> `ERR unknown subcommand 'SHARDFOO'. Try PUBSUB HELP.`
- Info surfaces (observed SP-17): `INFO stats` -> `pubsubshard_channels:<n>` (also `pubsub_channels`, `pubsub_patterns`); `CLIENT LIST` -> `sub=<channels> psub=<patterns> ssub=<shard>` and `flags=P` for a client in pubsub mode.
- Sharded pub/sub does not emit keyspace events.

### 7.4 Test vectors
**SP-01** RESP2: SSUBSCRIBE, SPUBLISH, smessage frame
```text
[A] > SSUBSCRIBE ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[B] > SPUBLISH ch hi
    <- ':1\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n'
[B] > SPUBLISH other hi
    <- ':0\r\n'
```

**SP-02** RESP3: push frames (>)
```text
[A] > SSUBSCRIBE ch
    <- '>3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[B] > SPUBLISH ch hi
    <- ':1\r\n'
[A] (async) <- '>3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n'
[A] > GET x
    <- '_\r\n'
[A] > PING
    <- '+PONG\r\n'
```

**SP-03** Namespaces are separate: SUBSCRIBE ch vs SSUBSCRIBE ch
```text
[A] > SUBSCRIBE ch
    <- '*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n'
[B] > SSUBSCRIBE ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[C] > PUBLISH ch x
    <- ':1\r\n'
[C] > SPUBLISH ch y
    <- ':1\r\n'
[A] (async) <- '*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$1\r\nx\r\n'
[B] (async) <- '*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$1\r\ny\r\n'
[C] > PUBSUB CHANNELS
    <- '*1\r\n$2\r\nch\r\n'
[C] > PUBSUB SHARDCHANNELS
    <- '*1\r\n$2\r\nch\r\n'
[C] > PUBSUB NUMSUB ch
    <- '*2\r\n$2\r\nch\r\n:1\r\n'
[C] > PUBSUB SHARDNUMSUB ch
    <- '*2\r\n$2\r\nch\r\n:1\r\n'
```

**SP-04** No cross-delivery: only-SUBSCRIBE / only-SSUBSCRIBE / PSUBSCRIBE *
```text
[A] > SUBSCRIBE ch
    <- '*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n'
[C] > SPUBLISH ch y
    <- ':0\r\n'
[B] > PSUBSCRIBE *
    <- '*3\r\n$10\r\npsubscribe\r\n$1\r\n*\r\n:1\r\n'
[C] > SPUBLISH ch y
    <- ':0\r\n'
[B] (async) <- (nothing)
[A] (async) <- (nothing)
[B] > SSUBSCRIBE ch2
    <- '*3\r\n$10\r\nssubscribe\r\n$3\r\nch2\r\n:1\r\n'
[C] > PUBLISH ch2 z
    <- ':0\r\n'
[B] (async) <- (nothing)
```

**SP-05** subscription counts in frames: ssubscribe counts shard channels only; subscribe counts channels+patterns only
```text
[A] > SUBSCRIBE a
    <- '*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n'
[A] > SSUBSCRIBE s1 s2
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\ns1\r\n:1\r\n*3\r\n$10\r\nssubscribe\r\n$2\r\ns2\r\n:2\r\n'
[A] > SUBSCRIBE b
    <- '*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n'
[A] > PSUBSCRIBE p*
    <- '*3\r\n$10\r\npsubscribe\r\n$2\r\np*\r\n:3\r\n'
[A] > SSUBSCRIBE s3
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\ns3\r\n:3\r\n'
[A] > SUNSUBSCRIBE s1
    <- '*3\r\n$12\r\nsunsubscribe\r\n$2\r\ns1\r\n:2\r\n'
[A] > UNSUBSCRIBE a
    <- '*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:2\r\n'
```

**SP-06** duplicate SSUBSCRIBE and multi-channel SSUBSCRIBE
```text
[A] > SSUBSCRIBE ch ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[B] > PUBSUB SHARDNUMSUB ch
    <- '*2\r\n$2\r\nch\r\n:1\r\n'
[B] > SPUBLISH ch m
    <- ':1\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$1\r\nm\r\n'
```

**SP-07** SUNSUBSCRIBE with no args and nothing subscribed; with a name not subscribed
```text
[A] > SUNSUBSCRIBE
    <- '*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n'
[A] > SUNSUBSCRIBE ch
    <- '*3\r\n$12\r\nsunsubscribe\r\n$2\r\nch\r\n:0\r\n'
[A] > SUNSUBSCRIBE ch ch2
    <- '*3\r\n$12\r\nsunsubscribe\r\n$2\r\nch\r\n:0\r\n*3\r\n$12\r\nsunsubscribe\r\n$3\r\nch2\r\n:0\r\n'
[A] > GET x
    <- '$-1\r\n'
[A3] > SUNSUBSCRIBE
    <- '>3\r\n$12\r\nsunsubscribe\r\n_\r\n:0\r\n'
[A3] > SUNSUBSCRIBE ch
    <- '>3\r\n$12\r\nsunsubscribe\r\n$2\r\nch\r\n:0\r\n'
```

**SP-08** SUNSUBSCRIBE with no args while subscribed to two shard channels; leaving subscribe mode
```text
[A] > SSUBSCRIBE s1 s2
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\ns1\r\n:1\r\n*3\r\n$10\r\nssubscribe\r\n$2\r\ns2\r\n:2\r\n'
[A] > SUNSUBSCRIBE
    <- '*3\r\n$12\r\nsunsubscribe\r\n$2\r\ns1\r\n:1\r\n*3\r\n$12\r\nsunsubscribe\r\n$2\r\ns2\r\n:0\r\n'
[A] > GET x
    <- '$-1\r\n'
[A] > SSUBSCRIBE s1
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\ns1\r\n:1\r\n'
[A] > SUBSCRIBE c1
    <- '*3\r\n$9\r\nsubscribe\r\n$2\r\nc1\r\n:1\r\n'
[A] > SUNSUBSCRIBE
    <- '*3\r\n$12\r\nsunsubscribe\r\n$2\r\ns1\r\n:0\r\n'
[A] > GET x
    <- "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
[A] > UNSUBSCRIBE
    <- '*3\r\n$11\r\nunsubscribe\r\n$2\r\nc1\r\n:0\r\n'
[A] > GET x
    <- '$-1\r\n'
```

**SP-09** RESP2 subscribe-mode command restriction applies to SSUBSCRIBE
```text
[A] > SSUBSCRIBE ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[A] > GET x
    <- "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
[A] > SPUBLISH ch m
    <- "-ERR Can't execute 'spublish': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
[A] > PUBSUB SHARDNUMSUB ch
    <- "-ERR Can't execute 'pubsub|shardnumsub': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
[A] > PING
    <- '*2\r\n$4\r\npong\r\n$0\r\n\r\n'
[A] > PING hello
    <- '*2\r\n$4\r\npong\r\n$5\r\nhello\r\n'
[A] > SUBSCRIBE c
    <- '*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n'
[A] > PSUBSCRIBE p
    <- '*3\r\n$10\r\npsubscribe\r\n$1\r\np\r\n:2\r\n'
[A] > SSUBSCRIBE ch2
    <- '*3\r\n$10\r\nssubscribe\r\n$3\r\nch2\r\n:2\r\n'
[A] > UNSUBSCRIBE
    <- '*3\r\n$11\r\nunsubscribe\r\n$1\r\nc\r\n:1\r\n'
[A] > PUNSUBSCRIBE
    <- '*3\r\n$12\r\npunsubscribe\r\n$1\r\np\r\n:0\r\n'
[A] > GET x
    <- "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
[A] > SUNSUBSCRIBE
    <- '*3\r\n$12\r\nsunsubscribe\r\n$2\r\nch\r\n:1\r\n*3\r\n$12\r\nsunsubscribe\r\n$3\r\nch2\r\n:0\r\n'
[A] > GET x
    <- '$-1\r\n'
```

**SP-10** RESP3: no restriction; PING returns +PONG; publisher that is also subscriber
```text
[A] > SSUBSCRIBE ch
    <- '>3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[A] > SPUBLISH ch self
    <- ':1\r\n>3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$4\r\nself\r\n'
[A] > GET x
    <- '_\r\n'
[A] > PING
    <- '+PONG\r\n'
[A] > SUNSUBSCRIBE ch
    <- '>3\r\n$12\r\nsunsubscribe\r\n$2\r\nch\r\n:0\r\n'
[A] > SUNSUBSCRIBE
    <- '>3\r\n$12\r\nsunsubscribe\r\n_\r\n:0\r\n'
```

**SP-11** arity errors
```text
[A] > SPUBLISH
    <- "-ERR wrong number of arguments for 'spublish' command\r\n"
[A] > SPUBLISH ch
    <- "-ERR wrong number of arguments for 'spublish' command\r\n"
[A] > SPUBLISH ch m extra
    <- "-ERR wrong number of arguments for 'spublish' command\r\n"
[A] > SSUBSCRIBE
    <- "-ERR wrong number of arguments for 'ssubscribe' command\r\n"
[A] > PUBSUB SHARDNUMSUB
    <- '*0\r\n'
[A] > PUBSUB SHARDCHANNELS a b
    <- "-ERR unknown subcommand or wrong number of arguments for 'SHARDCHANNELS'. Try PUBSUB HELP.\r\n"
[A] > PUBSUB SHARDFOO
    <- "-ERR unknown subcommand 'SHARDFOO'. Try PUBSUB HELP.\r\n"
```

**SP-12** PUBSUB SHARDCHANNELS pattern filter; SHARDNUMSUB with several channels
```text
[A] > SSUBSCRIBE news.a news.b sport
    <- '*3\r\n$10\r\nssubscribe\r\n$6\r\nnews.a\r\n:1\r\n*3\r\n$10\r\nssubscribe\r\n$6\r\nnews.b\r\n:2\r\n*3\r\n$10\r\nssubscribe\r\n$5\r\nsport\r\n:3\r\n'
[B] > SSUBSCRIBE news.a
    <- '*3\r\n$10\r\nssubscribe\r\n$6\r\nnews.a\r\n:1\r\n'
[C] > PUBSUB SHARDCHANNELS news.*
    <- '*2\r\n$6\r\nnews.a\r\n$6\r\nnews.b\r\n'
[C] > PUBSUB SHARDCHANNELS nomatch*
    <- '*0\r\n'
[C] > PUBSUB SHARDNUMSUB news.a sport nope news.a
    <- '*8\r\n$6\r\nnews.a\r\n:2\r\n$5\r\nsport\r\n:1\r\n$4\r\nnope\r\n:0\r\n$6\r\nnews.a\r\n:2\r\n'
[C] > PUBSUB SHARDNUMSUB
    <- '*0\r\n'
[C] > PUBSUB NUMSUB news.a
    <- '*2\r\n$6\r\nnews.a\r\n:0\r\n'
[C] > PUBSUB NUMPAT
    <- ':0\r\n'
[C] > SPUBLISH news.a m
    <- ':2\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$6\r\nnews.a\r\n$1\r\nm\r\n'
[B] (async) <- '*3\r\n$8\r\nsmessage\r\n$6\r\nnews.a\r\n$1\r\nm\r\n'
```

**SP-13** disconnect removes subscription; RESET removes subscription
```text
[A] > SSUBSCRIBE ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[C] > SPUBLISH ch m
    <- ':1\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$1\r\nm\r\n'
[A] > RESET
    <- '+RESET\r\n'
[C] > SPUBLISH ch m
    <- ':0\r\n'
[C] > PUBSUB SHARDNUMSUB ch
    <- '*2\r\n$2\r\nch\r\n:0\r\n'
[A2] > SSUBSCRIBE ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[C] > PUBSUB SHARDCHANNELS
    <- '*0\r\n'
```

**SP-14** SSUBSCRIBE inside MULTI/EXEC
```text
[A] > MULTI
    <- '+OK\r\n'
[A] > SSUBSCRIBE ch
    <- '+QUEUED\r\n'
[A] > SUBSCRIBE c2
    <- '+QUEUED\r\n'
[A] > SPUBLISH ch m
    <- '+QUEUED\r\n'
[A] > EXEC
    <- "*3\r\n-ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client\r\n*3\r\n$9\r\nsubscribe\r\n$2\r\nc2\r\n:1\r\n:0\r\n"
[A] > GET x
    <- "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
[A] > PUBSUB SHARDCHANNELS
    <- "-ERR Can't execute 'pubsub|shardchannels': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
```

**SP-15** binary-safe: empty channel, empty message, space in channel, message with CRLF
```text
[A] > SSUBSCRIBE ""
    <- '*3\r\n$10\r\nssubscribe\r\n$0\r\n\r\n:1\r\n'
[B] > SPUBLISH "" ""
    <- ':1\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$0\r\n\r\n$0\r\n\r\n'
[A] > SSUBSCRIBE a b
    <- '*3\r\n$10\r\nssubscribe\r\n$3\r\na b\r\n:2\r\n'
[B] > SPUBLISH a b x
y
    <- ':1\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$3\r\na b\r\n$4\r\nx\r\ny\r\n'
```

**SP-16** multiple subscribers: SPUBLISH count and message fan-out
```text
[A] > SSUBSCRIBE ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[B] > SSUBSCRIBE ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[C] > SSUBSCRIBE ch
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n'
[P] > SPUBLISH ch m
    <- ':3\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$1\r\nm\r\n'
[B] (async) <- '*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$1\r\nm\r\n'
[C] (async) <- '*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$1\r\nm\r\n'
[P] > SPUBLISH ch m2
    <- ':2\r\n'
```

**SP-17** INFO counters (pubsub_shardchannels) and CLIENT INFO sub/ssub fields
```text
[A] > SSUBSCRIBE s1 s2
    <- '*3\r\n$10\r\nssubscribe\r\n$2\r\ns1\r\n:1\r\n*3\r\n$10\r\nssubscribe\r\n$2\r\ns2\r\n:2\r\n'
[A] > SUBSCRIBE c1
    <- '*3\r\n$9\r\nsubscribe\r\n$2\r\nc1\r\n:1\r\n'
INFO stats: pubsub_channels:1
INFO stats: pubsub_patterns:0
INFO stats: pubsubshard_channels:2
CLIENT LIST fields: sub=1 psub=0 ssub=2
CLIENT LIST flags: flags=P
```

---

## 8. Wire-format summary (RESP2 vs RESP3)

Only nil differs for these commands (`$-1` vs `_`); integers, errors and arrays are identical. The pubsub frames use `*` vs `>`.

Wire bytes, RESP2 connection
```text
[RESP2] > HGETDEL nokey FIELDS 2 a b
    <- '*2\r\n$-1\r\n$-1\r\n'
[RESP2] > HGETEX nokey FIELDS 1 a
    <- '*1\r\n$-1\r\n'
[RESP2] > HGETEX h FIELDS 2 a zz
    <- '*2\r\n$1\r\n1\r\n$-1\r\n'
[RESP2] > HSETEX h FIELDS 1 a 1
    <- ':1\r\n'
[RESP2] > HSETEX h NX FIELDS 1 a 1
    <- ':0\r\n'
[RESP2] > DELIFEQ h2 v
    <- ':0\r\n'
[RESP2] > MSETEX 1 k v NX
    <- ':1\r\n'
[RESP2] > MSETEX 1 k v NX
    <- ':0\r\n'
[RESP2] > HGETDEL k FIELDS 1 a
    <- '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
[RESP2] > HGETDEL h FIELDS 0 a
    <- '-ERR numfields should be greater than 0 and match the provided number of fields\r\n'
```

Wire bytes, RESP3 connection
```text
[RESP3] > HGETDEL nokey FIELDS 2 a b
    <- '*2\r\n_\r\n_\r\n'
[RESP3] > HGETEX nokey FIELDS 1 a
    <- '*1\r\n_\r\n'
[RESP3] > HGETEX h FIELDS 2 a zz
    <- '*2\r\n$1\r\n1\r\n_\r\n'
[RESP3] > HSETEX h FIELDS 1 a 1
    <- ':1\r\n'
[RESP3] > HSETEX h NX FIELDS 1 a 1
    <- ':0\r\n'
[RESP3] > DELIFEQ h2 v
    <- ':0\r\n'
[RESP3] > MSETEX 1 k v NX
    <- ':1\r\n'
[RESP3] > MSETEX 1 k v NX
    <- ':0\r\n'
[RESP3] > HGETDEL k FIELDS 1 a
    <- '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
[RESP3] > HGETDEL h FIELDS 0 a
    <- '-ERR numfields should be greater than 0 and match the provided number of fields\r\n'
```

## 9. Notable quirks to reproduce (checklist)

1. `EX 0` / `PX 0` / past `EXAT`/`PXAT` in HGETEX and HSETEX delete the listed fields and succeed; in MSETEX the same values are E_EXP (<= 0) or store an immediately-expired key.
2. HSETEX reports `1` in the past-time case even when nothing was set, and emits `hset`, `hexpire`, `hexpired` (+`del` if emptied) regardless.
3. HSETEX with no TTL option clears the field TTL; `KEEPTTL` keeps it.
4. HSETEX NX/XX are checked before the expire value is validated; type is checked before both.
5. HGETEX validates the expire value after the type check but even for a missing key.
6. HGETDEL's FIELDS/numfields checks precede the type check; HGETEX/HSETEX do the same for numfields but not for option errors vs type (option parse errors also precede the type check).
7. Hash-field TTL has no upper-bound validation in HGETEX/HSETEX.
8. MSETEX numkeys errors use E_NUMKEYS, and surplus key/value pairs are a syntax error (they are parsed as options).
9. SSUBSCRIBE inside MULTI errors at EXEC time; SUBSCRIBE does not.
10. `ssubscribe`/`sunsubscribe` counts exclude plain channels and patterns.

## 10. SET key value IFEQ comparison-value

Added to this spec on 2026-11-16, captured the same way from Valkey 9.1.3 (`valkey-cli --no-raw`, from `FLUSHALL`). `IFEQ` came in Valkey 8.1.0.

### 10.1 Parsing / checks
1. `IFEQ` takes the next argument as the value to compare with. It excludes `NX`, `XX` and a second `IFEQ`; any of those, or `IFEQ` with nothing after it, is `ERR syntax error`.
2. Missing key: nothing is set, reply `(nil)`.
3. Key that is not a string: `WRONGTYPE`, with or without `GET`.
4. Compare: binary-safe; an integer-encoded value compares as its decimal text.

### 10.2 Reply and side effects
- Match: the key is set with the expiry options as plain `SET` sets them (no option clears the TTL, `KEEPTTL` keeps it); reply `OK`, or the old value with `GET`.
- Mismatch: no change; reply `(nil)`, or the old value with `GET`.

### 10.3 Test vectors
**SI-01** match: sets, reply OK
```text
> SET k v
OK
> SET k w IFEQ v
OK
> GET k
"w"
```

**SI-02** mismatch: key kept, reply nil
```text
> SET k v
OK
> SET k w IFEQ V
(nil)
> SET k w IFEQ vv
(nil)
> GET k
"v"
```

**SI-03** missing key: nil, nothing made
```text
> DEL k
(integer) 0
> SET k w IFEQ v
(nil)
> EXISTS k
(integer) 0
```

**SI-04** with GET: the old value, set or not
```text
> SET k v
OK
> SET k w IFEQ v GET
"v"
> SET k z IFEQ v GET
"w"
> GET k
"w"
> DEL nokey
(integer) 0
> SET nokey a IFEQ b GET
(nil)
> EXISTS nokey
(integer) 0
```

**SI-05** non-string key: WRONGTYPE, with GET too
```text
> DEL h
(integer) 0
> HSET h a 1
(integer) 1
> SET h x IFEQ 1
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> SET h x IFEQ 1 GET
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

**SI-06** with an expiry, and KEEPTTL
```text
> SET k v
OK
> SET k w IFEQ v EX 100
OK
> TTL k
(integer) 100
> SET k u IFEQ w KEEPTTL
OK
> TTL k
(integer) 100
> GET k
"u"
> SET k t IFEQ u
OK
> TTL k
(integer) -1
```

**SI-07** IFEQ excludes NX and XX, and needs its value
```text
> SET k v NX IFEQ v
(error) ERR syntax error
> SET k v IFEQ v XX
(error) ERR syntax error
> SET k v IFEQ a IFEQ b
(error) ERR syntax error
> SET k v IFEQ
(error) ERR syntax error
> GET k
(nil)
```

**SI-08** integer values compare as their text
```text
> SET n 123
OK
> SET n 5 IFEQ 0123
(nil)
> SET n 5 IFEQ 123
OK
> INCR n
(integer) 6
> SET n 7 IFEQ 6
OK
> GET n
"7"
```
