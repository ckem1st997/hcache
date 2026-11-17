# Valkey 9.1.3 wave-1b behaviour spec

Target: reimplement `ROLE`, `LOLWUT`, `COMMANDLOG` (and the `SLOWLOG` alias), `DEBUG`, `SHUTDOWN`, `READONLY`/`READWRITE`/`ASKING`, the `CLUSTER` command family (on a non-cluster server and on a one-node cluster), `CLUSTERSCAN`, and the cluster-mode command rules (`CROSSSLOT` and friends) byte-for-byte, and write differential tests against Valkey 9.1.3.

Evidence labels: everything not marked is **observed** on a live Valkey 9.1.3 server (`valkey_version:9.1.3`). Statements marked "(from source)" come from reading `src/commandlog.c`, `src/debug.c`, `src/lolwut*.c`, `src/db.c`, `src/server.c`, `src/cluster.c`, `src/module.c`, `src/script.c`, `src/sort.c`, `src/replication.c` and `src/commands/*.json` and were not (or could not be) exercised. Where a source reading and an observation disagree, the observation is what the vectors show and the text says so.

## 0. Method and conventions

- Vectors were captured with a small RESP client that prints replies exactly as `valkey-cli --no-raw` does (`(integer) 1`, `1) "a"`, `(nil)`, `(empty array)`, `(error) ...`, bulk strings in double quotes with `\n`, `\r`, `\xNN` escapes). One vector is a line `**ID** short title` followed by a fenced `text` block. Arguments are split on single spaces (no spaces or quotes inside an argument). Each vector starts from `FLUSHALL` and was replayed twice on the same server; every vector in this file gave byte-identical output both times, and a full in-order replay of all vector files on freshly started servers also reproduced it.
- Servers (all started by the capture, WSL2 Ubuntu 24.04, loopback only):
  - **7401** standalone: `--save "" --appendonly no --enable-debug-command yes`. Vectors RO, LW, CM, DB-02..DB-23, SD, RW, CN run here.
  - **7402** standalone, default `enable-debug-command no` (DB-01 only).
  - **7403** one-node cluster: `--cluster-enabled yes --save "" --appendonly no --enable-debug-command yes`, then `CLUSTER ADDSLOTSRANGE 0 16383`, then wait for `cluster_state:ok`. Vectors CL, CS run here. The node has no IP of its own (see 8.2), epoch 0.
  - **7404/7405** throwaway servers for SHUTDOWN, ROLE with replicas, CLUSTERDOWN and announce-ip observations (section 5, 8.7). Their results are described in prose or in `shape` blocks, not as vectors, because a vector cannot start or kill a server.
- Wire blocks use `[A] > CMD` then `    <- '<python repr of the bytes>'`; a client whose name ends in `3` sent `HELLO 3` first; `[A] (read)` reads whatever the server pushed asynchronously. Only deterministic bytes are shown.
- Things that vary between runs (node ids, shard ids, client ids and addresses, timestamps, durations, `DEBUG OBJECT` addresses and `lru`, LOLWUT art with random constants, `ROLE` offsets) are never in a vector. They are described in prose and a `shape` block with `<placeholders>`.
- Command names in error text are lowercase full names (`'cluster|keyslot'`, `'commandlog|get'`). The container itself is just `'cluster'`, `'commandlog'`, `'slowlog'`, `'debug'`.
- Observed deviations from the requested setup: (1) the `valkey-server` on the PATH is 7.2.13, so the prebuilt 9.1.3 `valkey-server` and `valkey-cli` from the source tree (`/root/src/valkey-9.1.3/src/`, built by the owner, not rebuilt here) were copied to `/tmp/wave1b/bin` and used; (2) ports are 7401-7405; (3) `dir` of 7401 is a private directory so `DEBUG RELOAD` writes `dump.rdb` there.
- Every vector that contains `COMMANDLOG` or `CONFIG SET commandlog-*` restores the defaults before it ends, so the vectors can be replayed in any order.

### 0.1 Command table (from `src/commands/*.json`)

| Command | Arity | Flags | ACL categories | Since | Keys |
|---|---|---|---|---|---|
| CLUSTER ADDSLOTS | -3 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER ADDSLOTSRANGE | -4 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 7.0.0 | - |
| CLUSTER BUMPEPOCH | 2 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER CANCELSLOTMIGRATIONS | 2 | NO_ASYNC_LOADING ADMIN STALE ALL_DBS | ADMIN DANGEROUS SLOW | 9.0.0 | - |
| CLUSTER COUNT-FAILURE-REPORTS | 3 | ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER COUNTKEYSINSLOT | 3 | STALE | SLOW | 3.0.0 | - |
| CLUSTER DELSLOTS | -3 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER DELSLOTSRANGE | -4 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 7.0.0 | - |
| CLUSTER FAILOVER | -2 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER FLUSHSLOT | -3 | WRITE NO_ASYNC_LOADING ADMIN ALL_DBS | ADMIN DANGEROUS KEYSPACE SLOW WRITE | 9.0.0 | - |
| CLUSTER FLUSHSLOTS | 2 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER FORGET | 3 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER GETKEYSINSLOT | 4 | STALE | SLOW | 3.0.0 | - |
| CLUSTER GETSLOTMIGRATIONS | 2 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 9.0.0 | - |
| CLUSTER HELP | 2 | LOADING STALE | SLOW | 5.0.0 | - |
| CLUSTER INFO | 2 | LOADING STALE | SLOW | 3.0.0 | - |
| CLUSTER KEYSLOT | 3 | STALE | SLOW | 3.0.0 | - |
| CLUSTER LINKS | 2 | STALE | SLOW | 7.0.0 | - |
| CLUSTER MEET | -4 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER MIGRATESLOTS | -4 | NO_ASYNC_LOADING ADMIN STALE ALL_DBS | ADMIN DANGEROUS SLOW | 9.0.0 | - |
| CLUSTER MYID | 2 | LOADING STALE | SLOW | 3.0.0 | - |
| CLUSTER MYSHARDID | 2 | LOADING STALE | SLOW | 7.2.0 | - |
| CLUSTER NODES | 2 | LOADING STALE | SLOW | 3.0.0 | - |
| CLUSTER REPLICAS | 3 | ADMIN STALE | ADMIN DANGEROUS SLOW | 5.0.0 | - |
| CLUSTER REPLICATE | -3 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER RESET | -2 | ADMIN STALE NOSCRIPT | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER SAVECONFIG | 2 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER SET-CONFIG-EPOCH | 3 | NO_ASYNC_LOADING ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER SETSLOT | -4 | NO_ASYNC_LOADING ADMIN STALE MAY_REPLICATE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER SHARDS | 2 | LOADING STALE | SLOW | 7.0.0 | - |
| CLUSTER SLAVES | 3 | ADMIN STALE | ADMIN DANGEROUS SLOW | 3.0.0 | - |
| CLUSTER SLOT-STATS | -4 | STALE LOADING | SLOW | 8.0.0 | - |
| CLUSTER SLOTS | 2 | LOADING STALE | SLOW | 3.0.0 | - |
| CLUSTER SYNCSLOTS | -3 | NO_ASYNC_LOADING ADMIN STALE MAY_REPLICATE | ADMIN DANGEROUS SLOW | 9.0.0 | - |
| CLUSTER | -2 |  | SLOW | 3.0.0 | - |
| CLUSTERSCAN | -2 | READONLY TOUCHES_ARBITRARY_KEYS | KEYSPACE READ SLOW | 9.1.0 | - |
| READONLY | 1 | FAST LOADING STALE | CONNECTION FAST | 3.0.0 | - |
| READWRITE | 1 | FAST LOADING STALE | CONNECTION FAST | 3.0.0 | - |
| ASKING | 1 | FAST | CONNECTION FAST | 3.0.0 | - |
| ROLE | 1 | NOSCRIPT LOADING STALE FAST SENTINEL | ADMIN DANGEROUS FAST | 2.8.12 | - |
| LOLWUT | -1 | READONLY FAST | FAST READ | 5.0.0 | - |
| SHUTDOWN | -1 | ADMIN NOSCRIPT LOADING STALE NO_MULTI SENTINEL ALLOW_BUSY | ADMIN DANGEROUS SLOW | 1.0.0 | - |
| DEBUG | -2 | ADMIN NOSCRIPT LOADING STALE PROTECTED | ADMIN DANGEROUS SLOW | 1.0.0 | - |
| COMMANDLOG GET | 4 | ADMIN LOADING STALE | ADMIN DANGEROUS SLOW | 8.1.0 | - |
| COMMANDLOG HELP | 2 | LOADING STALE | SLOW | 8.1.0 | - |
| COMMANDLOG LEN | 3 | ADMIN LOADING STALE | ADMIN DANGEROUS SLOW | 8.1.0 | - |
| COMMANDLOG RESET | 3 | ADMIN LOADING STALE | ADMIN DANGEROUS SLOW | 8.1.0 | - |
| COMMANDLOG | -2 |  | SLOW | 8.1.0 | - |

Arity counts the container word and the subcommand word (`CLUSTER KEYSLOT k` has arity 3). Arity is checked before anything else, including "cluster support disabled" (CN-01: `CLUSTER CANCELSLOTMIGRATIONS ALL` on a standalone server is an arity error, `CLUSTER INFO` is not).

### 0.2 Error strings used (exact, without the leading `-`)

| Id | Text |
|---|---|
| E_ARITY | `ERR wrong number of arguments for '<cmd>' command` |
| E_SUBUNK | `ERR unknown subcommand '<arg as typed>'. Try <CONTAINER> HELP.` (CLUSTER, COMMANDLOG, SLOWLOG; container written upper case) |
| E_SUBARG | `ERR unknown subcommand or wrong number of arguments for '<SUB as typed>'. Try <CONTAINER> HELP.` (every DEBUG subcommand with a wrong argument count; also `CLUSTER RESET HARD SOFT`, `CLUSTER SLOT-STATS SLOTSRANGE 0`, `SLOWLOG GET 1 2`) |
| E_CLUSTER_OFF | `ERR This instance has cluster support disabled` |
| E_SYNTAX | `ERR syntax error` |
| E_INT | `ERR value is not an integer or out of range` |
| E_FLOAT | `ERR value is not a valid float` |
| E_DBRANGE | `ERR DB index is out of range` |
| E_CROSSSLOT | `CROSSSLOT Keys in request don't hash to the same slot` |
| E_SCRIPT_CROSS | `ERR Script attempted to access keys that do not hash to the same slot` (a `redis.call` raises it with the suffix ` script: on @user_script:<line>.`; `redis.pcall` returns it without the suffix) |
| E_NONLOCAL | `ERR Attempted to access a non local key in a cluster node` (same suffix rule; see 9.4, this is the poisoned state) |
| E_NOSCRIPT_CMD | `ERR This Valkey command is not allowed from script` (same suffix rule, so `redis.call` shows `... from script script: on @user_script:1.`) |
| E_NOTXN | `ERR Command '<cmd>' not allowed inside a transaction` |
| E_EXECABORT | `EXECABORT Transaction discarded because of previous errors.` |
| E_DEBUG_OFF | `ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.` |
| E_CL_UNKNOWN_NODE | `ERR Unknown node <arg as typed>` |
| E_SLOT | `ERR Invalid or out of range slot` |
| E_CLUSTERDOWN_UNBOUND | `CLUSTERDOWN Hash slot not served` |
| E_CLUSTERDOWN | `CLUSTERDOWN The cluster is down` |
| E_CLUSTERDOWN_RO | `CLUSTERDOWN The cluster is down and only accepts read commands` (from source, `clusterRedirectClient`) |
| E_MOVED / E_ASK | `MOVED <slot> <endpoint>:<port>`, `ASK <slot> <endpoint>:<port>` (from source; a one-node cluster never produces them) |
| E_TRYAGAIN | `TRYAGAIN Multiple keys request during rehashing of slot` (from source) |
| E_SHUTDOWN | `ERR Errors trying to SHUTDOWN. Check logs.` |
| E_NOSHUTDOWN | `ERR No shutdown in progress.` |

Section-specific messages (COMMANDLOG type/count errors, CLUSTER errors, CONFIG range errors) are listed where they are used.

---

## 1. ROLE

### 1.1 Rules

- Arity 1 (`ROLE x` is E_ARITY with `'role'`). Flags `NOSCRIPT LOADING STALE FAST SENTINEL`: it works while loading and on stale replicas, is queued normally in MULTI (RO-03), and is refused from scripts with E_NOSCRIPT_CMD (RO-03).
- Reply for a primary (`server.primary_host == NULL`) (from source `roleCommand`, confirmed on a live primary with one replica): array of 3: `"master"`, the integer `primary_repl_offset`, and an array of replicas that are **online** (`REPLICA_STATE_ONLINE`; a replica still syncing is not listed). Each replica is an array of 3 **bulk strings**: ip, listening port, last acknowledged offset (so the port and the offset are bulk strings, not integers).
- Reply for a replica: array of 5: `"slave"`, master host (bulk), master port (integer), state (bulk), offset (integer, `-1` unless the master link exists). State is `handshake` while the handshake is in progress, else `none`, `connect`, `connecting`, `sync` (transfer), `connected`, `unknown` (from source; `connect`, `handshake` and `connected` were all seen live).
- A fresh standalone server has offset 0 and no replicas (RO-01). RESP3 changes nothing: no maps, no verbatim strings (RO-04).

Shape of a primary with one online replica (observed) and of a replica (observed), the numbers vary:

```shape
> ROLE
1) "master"
2) (integer) <primary_repl_offset>
3) 1) 1) "127.0.0.1"
      2) "<replica-listening-port>"
      3) "<replica-ack-offset>"

> ROLE            (on the replica)
1) "slave"
2) "127.0.0.1"
3) (integer) <primary-port>
4) "connected"
5) (integer) <replica-offset>
```

Wire bytes of those two replies (observed): primary `*3\r\n$6\r\nmaster\r\n:64\r\n*1\r\n*3\r\n$9\r\n127.0.0.1\r\n$4\r\n7405\r\n$2\r\n14\r\n`; replica `*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:7404\r\n$9\r\nconnected\r\n:64\r\n`. A replica whose master is down reports state `connect` with offset `-1`, during the handshake `handshake` with `-1`. After `REPLICAOF NO ONE` the server is a primary again with its offset advanced and no replicas.

### 1.2 Vectors

**RO-01** ROLE on a master without replicas
```text
> ROLE
1) "master"
2) (integer) 0
3) (empty array)
```

**RO-02** ROLE arity
```text
> ROLE x
(error) ERR wrong number of arguments for 'role' command
> ROLE x y
(error) ERR wrong number of arguments for 'role' command
```

**RO-03** ROLE queued in MULTI, not allowed from scripts
```text
> MULTI
OK
> ROLE
QUEUED
> EXEC
1) 1) "master"
   2) (integer) 0
   3) (empty array)
> EVAL return(redis.call('role')) 0
(error) ERR This Valkey command is not allowed from script script: on @user_script:1.
> EVAL return(redis.pcall('role')) 0
(error) ERR This Valkey command is not allowed from script
```

**RO-04** ROLE wire bytes RESP2 and RESP3 (identical)
```text
[A] > ROLE
    <- '*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n'
[B3] > ROLE
    <- '*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n'
```


---

## 2. LOLWUT

### 2.1 Dispatch (from source, `lolwutCommand`, confirmed by LW-02, LW-04..LW-07)

`LOLWUT [VERSION <n>] [args...]`. Arity `-1`. If `argc >= 3` and argv[1] equals `VERSION` (case-insensitive), argv[2] is parsed as a C `long` (E_INT if not an integer or out of range) and the effective version string is `"%u.0.0"` of `(unsigned int)n`; the two tokens are removed before the implementation sees its arguments. Otherwise the version string is the server version, `9.1.3`. Then, by string prefix:

| Version string | Implementation |
|---|---|
| `5.x` where the third char is not `9`, or `4.9...` | **LOLWUT 5** (Georg Nees "schotter", braille) |
| `6.x` where the third char is not `9`, or `5.9...` | **LOLWUT 6** (8-bit skyline art, ANSI colours) |
| starts with `9` | **LOLWUT 9** (Julia set, default) |
| anything else | **unstable**: a single line |

Consequences (observed, LW-02/LW-04): `VERSION 5` and `VERSION 6` hit 5 and 6; `VERSION 9`, `90`, `99` hit 9 (so does no `VERSION`); `VERSION 0`, `1`, `4`, `7`, `8`, `10`, `49`, `59`, negatives and `9223372036854775807` hit the unstable one (negatives and huge numbers are truncated to `unsigned int` first: `-1` becomes `4294967295.0.0`). `LOLWUT VERSION` (one argument after LOLWUT) is **not** a version request: it is passed to the default implementation and fails with the syntax error (LW-01). The word `version` is case-insensitive (LW-04).

All variants end the reply with `Valkey ver. <VALKEY_VERSION>` (the unstable one prints `Valkey ver.<VALKEY_VERSION>\n` with no space). The vectors therefore contain the literal `9.1.3`; a reimplementation reports its own version string and the test should substitute it.

### 2.2 LOLWUT 9 (Julia set, the default implementation)

Syntax `LOLWUT [columns rows [real imaginary]]`. Arguments after LOLWUT:

- 0 args: random constants, 80 columns by 40 rows. 2 args: `columns rows` with random constants. 4 args: `columns rows real imaginary`. Any other count (1, 3, 5 or more) gives `ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]` (LW-01, including `LOLWUT FOO`, `LOLWUT 10`, `LOLWUT 5`).
- `columns` and `rows` are parsed with the usual integer rule (E_INT on `abc`, `1.5`) and clamped: `columns` to 1..160, `rows` to 1..80 (observed: `LOLWUT 200 100 ...` gives 160 columns and 80 rows; `0` and negatives give 1) (LW-03).
- `real` and `imaginary` are parsed as doubles; a non-number is E_FLOAT; `nan` is E_FLOAT, `inf` is accepted (LW-03: constant printed as `inf + 0.00i`).
- Reply: `rows` lines of exactly `columns` characters each (every line ends with `\n`), then `Ascii representation of Julia set with constant <real %.2f> + <imaginary %.2f>i\n` and `Don't forget to have fun! Valkey ver. 9.1.3\n`. The art alphabet is ` .:-=+*#%&@` (space is the outside of the set). With fixed constants the art is deterministic (LW-02). Reply type: bulk string in RESP2, verbatim string `txt:` in RESP3 (LW-08). With no constants given the art is random (observed: 40 art lines of width 80 plus two footer lines).

### 2.3 LOLWUT 5 and 6 (observed only to the extent shown)

- `LOLWUT VERSION 5 [columns squares-per-row squares-per-col]`: arguments are integers (E_INT on a non-integer); extra arguments are ignored (LW-05 last line). The default reply is random braille art (LW-06 shows the one-square case, which is stable: `"â 

Georg Nees - schotter, plotter on paper, 1968. Valkey ver. 9.1.3
"`), ended by the `Georg Nees - schotter...` line. Random unless one square (LW-06).
- `LOLWUT VERSION 6 [columns rows]`: integers (E_INT), extra arguments ignored (LW-07 last line), ANSI escape sequences (`\x1b[0;97;107m \x1b[0m`), footer `Dedicated to the 8 bit game developers of past and present.` and `Original 8 bit image from Plaguemon by hikikomori. Valkey ver. 9.1.3`. The default reply is random-sized and about 25 KB.
- These two are art toys: a reimplementation only needs the argument parsing, the footers and the line structure, not the random content.

### 2.4 Vectors

**LW-01** syntax errors of the default (version 9) implementation
```text
> LOLWUT FOO
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
> LOLWUT 10
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
> LOLWUT 10 5 1
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
> LOLWUT 1 1 1 1 1 1
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
> LOLWUT 10 5 0.5 0.5 7
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
> LOLWUT VERSION
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
> LOLWUT VERSION 9 10
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
> LOLWUT VERSION 9 1 1 1
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
> LOLWUT 5
(error) ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]
```

**LW-02** Julia set with fixed constants is deterministic (cols rows real imaginary)
```text
> LOLWUT 3 2 0.5 0.5
" = \n = \nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
> LOLWUT VERSION 9 3 2 0.5 0.5
" = \n = \nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
> LOLWUT VERSION 99 3 2 0.5 0.5
" = \n = \nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
> LOLWUT VERSION 90 3 2 0.5 0.5
" = \n = \nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
> LOLWUT 4 3 -0.8 0.156
"    \n-@@-\n    \nAscii representation of Julia set with constant -0.80 + 0.16i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
> LOLWUT 1 1 0.5 0.5
"=\nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
```

**LW-03** Julia: clamping (cols 1..160, rows 1..80) and number parsing
```text
> LOLWUT 0 0 0.5 0.5
"=\nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
> LOLWUT -5 -5 0.5 0.5
"=\nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
> LOLWUT 3 2 inf 0
"   \n   \nAscii representation of Julia set with constant inf + 0.00i\nDon't forget to have fun! Valkey ver. 9.1.3\n"
> LOLWUT 3 2 nan nan
(error) ERR value is not a valid float
> LOLWUT 3 2 x 0.5
(error) ERR value is not a valid float
> LOLWUT 3 2 0.5 y
(error) ERR value is not a valid float
> LOLWUT 3 abc 0.5 0.5
(error) ERR value is not an integer or out of range
> LOLWUT 1.5 2 0.5 0.5
(error) ERR value is not an integer or out of range
> LOLWUT FOO 6
(error) ERR value is not an integer or out of range
> LOLWUT VERSION abc
(error) ERR value is not an integer or out of range
> LOLWUT VERSION 1.5
(error) ERR value is not an integer or out of range
> LOLWUT VERSION 9223372036854775808
(error) ERR value is not an integer or out of range
```

**LW-04** versions that map to the unstable implementation: one line, arguments ignored
```text
> LOLWUT VERSION 1
"Valkey ver.9.1.3\n"
> LOLWUT version 1
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 0
"Valkey ver.9.1.3\n"
> LOLWUT VERSION -1
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 4
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 7
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 8
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 49
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 59
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 9223372036854775807
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 1 extra args
"Valkey ver.9.1.3\n"
> LOLWUT VERSION 7 x
"Valkey ver.9.1.3\n"
```

**LW-05** Schotter (VERSION 5, cols squares-per-row squares-per-col): argument errors
```text
> LOLWUT VERSION 5 x
(error) ERR value is not an integer or out of range
> LOLWUT VERSION 5 1 x
(error) ERR value is not an integer or out of range
> LOLWUT VERSION 5 1 1 x
(error) ERR value is not an integer or out of range
> LOLWUT VERSION 5 1 1 1 extra
"\xe2\xa0\x8b\n\nGeorg Nees - schotter, plotter on paper, 1968. Valkey ver. 9.1.3\n"
```

**LW-06** Schotter with a single square is stable
```text
> LOLWUT VERSION 5 1 1 1
"\xe2\xa0\x8b\n\nGeorg Nees - schotter, plotter on paper, 1968. Valkey ver. 9.1.3\n"
> LOLWUT VERSION 5 6 1 1
"\xe2\xa0\x80\xe2\xa1\xa4\xe2\xa0\xa4\xe2\xa0\xa4\xe2\xa0\xa4\xe2\xa1\x84\n\xe2\xa0\x80\xe2\xa1\x87\xe2\xa0\x80\xe2\xa0\x80\xe2\xa0\x80\xe2\xa1\x87\n\xe2\xa0\x80\xe2\xa0\xa7\xe2\xa0\xa4\xe2\xa0\xa4\xe2\xa0\xa4\xe2\xa0\x87\n\nGeorg Nees - schotter, plotter on paper, 1968. Valkey ver. 9.1.3\n"
```

**LW-07** Skyline (VERSION 6, cols rows): argument errors, clamping to a 1x1 cell grid
```text
> LOLWUT VERSION 6 x
(error) ERR value is not an integer or out of range
> LOLWUT VERSION 6 1 x
(error) ERR value is not an integer or out of range
> LOLWUT VERSION 6 1 1 extra
"\x1b[0;97;107m \x1b[0m\nDedicated to the 8 bit game developers of past and present.\nOriginal 8 bit image from Plaguemon by hikikomori. Valkey ver. 9.1.3\n"
```

**LW-08** wire: bulk string in RESP2, verbatim string in RESP3
```text
[A] > LOLWUT VERSION 1
    <- '$17\r\nValkey ver.9.1.3\n\r\n'
[B3] > LOLWUT VERSION 1
    <- '=21\r\ntxt:Valkey ver.9.1.3\n\r\n'
[A] > LOLWUT 3 2 0.5 0.5
    <- "$113\r\n = \n = \nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n\r\n"
[B3] > LOLWUT 3 2 0.5 0.5
    <- "=117\r\ntxt: = \n = \nAscii representation of Julia set with constant 0.50 + 0.50i\nDon't forget to have fun! Valkey ver. 9.1.3\n\r\n"
[A] > LOLWUT VERSION 9 1 1 x y
    <- '-ERR value is not a valid float\r\n'
[B3] > LOLWUT FOO
    <- '-ERR Syntax error. Use: LOLWUT [columns rows] [real imaginary]\r\n'
```


---

## 3. COMMANDLOG (and SLOWLOG)

### 3.1 Model (from source `commandlog.c`, confirmed by the vectors)

Three independent logs: `slow`, `large-request`, `large-reply`. `SLOWLOG` is the old name of the `slow` log: the three `slowlog-*` configs are aliases of the `commandlog-*` slow configs (CM-06) and `SLOWLOG` reads/resets the same list (CM-22).

Push rule, evaluated for every command **after it executed** (so a `CONFIG SET` that lowers a threshold is itself logged, and `COMMANDLOG LEN` does not count itself), per log type: do nothing when the threshold is `< 0` or the log's max-len is `0`; otherwise log when `value >= threshold` (CM-07 boundary: `SET k v` is exactly 27 bytes and is logged with threshold 27, not with 28; threshold 0 logs every command, CM-08). A log longer than max-len is trimmed from the old end **when the next entry is pushed**, and again when the config is changed via CONFIG SET (CM-20: lowering max-len to 1 gives LEN 1 immediately in this build; raising max-len to 0 stops new entries but keeps the old ones, CM-08).

Values:

| Type | `value` | Notes |
|---|---|---|
| `slow` | execution time in microseconds | `DEBUG SLEEP 0.02` gives `>= 20000` (CM-11) |
| `large-request` | bytes of the RESP request frame as received (`*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n` = 27) | an inline command counts the line it was sent as; a rewritten command (`SET k v EX 100`, `EXPIRE`) logs the original arguments and the original size (CM-19) |
| `large-reply` | bytes of the reply as sent (`+OK\r\n` = 5, `$1\r\nv\r\n` = 7, an error reply counts its text) (CM-10) | large bulk replies that the server sends without copying are counted too (CM-23: 208 for a 200 byte value) |

Entry format (`COMMANDLOG GET <count> <type>`, `SLOWLOG GET [count]`): array of entries, newest first; each entry is an array of 6: **id** (integer, per-log counter, starts at 0, never reset by `RESET`, `entry_id` is only zeroed at startup), **timestamp** (integer, unix seconds), **value** (integer), **argv** (array of bulk strings, trimmed), **peer** (bulk `ip:port` of the client), **client name** (bulk, empty string if none). `SLOWLOG GET` entries have the same 6 fields. Same bytes in RESP2 and RESP3 (arrays and integers only).

Argument trimming (CM-12, CM-13, from source): at most 32 arguments are kept; if there were more, the 32nd slot is replaced by the string `... (<N> more arguments)` where N is the number of arguments dropped **including the one replaced** (40 args become 31 + marker `(11 more arguments)`); each kept string is cut to 128 bytes and `... (<M> more bytes)` is appended where M is the number of bytes dropped (`129` bytes become `xxxx...(128) ... (1 more bytes)`, so a trimmed string is 128 + the marker).

Redaction (CM-14): arguments of commands flagged as sensitive are replaced by the literal string `(redacted)`: `AUTH` (all arguments), `HELLO ... AUTH user pass` (only `user` and `pass`), `ACL SETUSER` (everything after the subcommand name's arguments: here `(redacted)` x4), `MIGRATE ... AUTH`, `CONFIG SET` of sensitive configs. The request size is still the real size. Commands flagged `SKIP_COMMANDLOG` are not logged at all (`EXEC` is one of them: CM-15/CM-16).

Transactions (CM-15, CM-16): `EXEC` itself is skipped; each queued command is logged when it runs. The large-request value of each queued command is the size of the **EXEC frame** (14 bytes), and `MULTI` is logged with its own size. The large-reply value is cumulative: each queued command gets the number of reply bytes produced since `EXEC` started (so the array header `*2\r\n` is counted in the first one: SET 9 = 4 + 5, GET 16 = 9 + 7).

Scripts (CM-17, CM-18): the `EVAL` is logged as a normal command; every `redis.call` command inside it is logged as its own entry with the command name **as written in the script** (`set`, lowercase), request size 0 and reply size the size of its reply.

Defaults (CM-04): `commandlog-execution-slower-than 10000`, `commandlog-slow-execution-max-len 128`, `commandlog-request-larger-than 1048576`, `commandlog-large-request-max-len 128`, `commandlog-reply-larger-than 1048576`, `commandlog-large-reply-max-len 128`, `slowlog-log-slower-than 10000`, `slowlog-max-len 128`. `CONFIG GET commandlog*` returns the matching names **sorted alphabetically** (observed order in CM-04).

Subcommand errors (CM-02):

| Case | Error |
|---|---|
| unknown type | `ERR type should be one of the following: slow, large-request, large-reply` |
| bad count (not an integer, `< -1`, overflow) | `ERR count should be greater than or equal to -1` |
| bad subcommand | E_SUBUNK with `COMMANDLOG` / `SLOWLOG` |
| arity | E_ARITY with `'commandlog|get'` etc. |

The count error is checked **before** the type error (`COMMANDLOG GET abc foo` gives the count error). Types and subcommands are case-insensitive. `count` `-1` returns everything, `0` returns an empty array. `SLOWLOG GET` default count is 10; `SLOWLOG GET 1 2` is E_SUBARG. `SLOWLOG RESET` resets only the slow log.

Config range errors (CM-05): `ERR CONFIG SET failed (possibly related to argument '<name>') - argument must be between -1 and 9223372036854775807 inclusive` for the `*-slower-than` / `*-larger-than` configs (min `-1`), `... between 0 and 9223372036854775807 inclusive` for the three `*-max-len` configs (min 0), and `... - argument couldn't be parsed into an integer`.

Shape of an entry (values vary):

```shape
> COMMANDLOG GET 1 large-request
1) 1) (integer) <id>
   2) (integer) <unix-seconds>
   3) (integer) 27
   4) 1) "SET"
      2) "k"
      3) "v"
   5) "127.0.0.1:<client-port>"
   6) "<client-name or empty>"
```

Wire bytes of one slow entry (RESP3, identical in RESP2): `*1\r\n*6\r\n:113\r\n:1794835512\r\n:1\r\n*1\r\n$4\r\nPING\r\n$15\r\n127.0.0.1:44266\r\n$0\r\n\r\n`; an empty log is `*0\r\n`.

### 3.2 Vectors

CM-09 and later read entries back through `EVAL` + `redis.call('commandlog','get',...)` and project only the deterministic fields (value and argv), because the id, the timestamp and the peer are not stable. The Lua text contains no spaces so that it survives the space-splitting of the vector format.

**CM-01** HELP (COMMANDLOG and SLOWLOG)
```text
> COMMANDLOG HELP
 1) COMMANDLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
 2) GET <count> <type>
 3)     Return top <count> entries of the specified <type> from the commandlog (-1 mean all).
 4)     Entries are made of:
 5)     id, timestamp,
 6)         time in microseconds for type of slow,
 7)         or size in bytes for type of large-request,
 8)         or size in bytes for type of large-reply
 9)     arguments array, client IP and port,
10)     client name
11) LEN <type>
12)     Return the length of the specified type of commandlog.
13) RESET <type>
14)     Reset the specified type of commandlog.
15) HELP
16)     Print this help.
> COMMANDLOG help
 1) COMMANDLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
 2) GET <count> <type>
 3)     Return top <count> entries of the specified <type> from the commandlog (-1 mean all).
 4)     Entries are made of:
 5)     id, timestamp,
 6)         time in microseconds for type of slow,
 7)         or size in bytes for type of large-request,
 8)         or size in bytes for type of large-reply
 9)     arguments array, client IP and port,
10)     client name
11) LEN <type>
12)     Return the length of the specified type of commandlog.
13) RESET <type>
14)     Reset the specified type of commandlog.
15) HELP
16)     Print this help.
> SLOWLOG HELP
 1) SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
 2) GET [<count>]
 3)     Return top <count> entries from the slowlog (default: 10, -1 mean all).
 4)     Entries are made of:
 5)     id, timestamp, time in microseconds, arguments array, client IP and port,
 6)     client name
 7) LEN
 8)     Return the length of the slowlog.
 9) RESET
10)     Reset the slowlog.
11) HELP
12)     Print this help.
```

**CM-02** argument and type errors
```text
> COMMANDLOG
(error) ERR wrong number of arguments for 'commandlog' command
> COMMANDLOG GET
(error) ERR wrong number of arguments for 'commandlog|get' command
> COMMANDLOG GET 1
(error) ERR wrong number of arguments for 'commandlog|get' command
> COMMANDLOG GET 1 slow extra
(error) ERR wrong number of arguments for 'commandlog|get' command
> COMMANDLOG GET 1 foo
(error) ERR type should be one of the following: slow, large-request, large-reply
> COMMANDLOG GET abc slow
(error) ERR count should be greater than or equal to -1
> COMMANDLOG GET 1.5 slow
(error) ERR count should be greater than or equal to -1
> COMMANDLOG GET -2 slow
(error) ERR count should be greater than or equal to -1
> COMMANDLOG GET 9223372036854775808 slow
(error) ERR count should be greater than or equal to -1
> COMMANDLOG GET abc foo
(error) ERR count should be greater than or equal to -1
> COMMANDLOG LEN
(error) ERR wrong number of arguments for 'commandlog|len' command
> COMMANDLOG LEN foo
(error) ERR type should be one of the following: slow, large-request, large-reply
> COMMANDLOG LEN slow extra
(error) ERR wrong number of arguments for 'commandlog|len' command
> COMMANDLOG RESET
(error) ERR wrong number of arguments for 'commandlog|reset' command
> COMMANDLOG RESET foo
(error) ERR type should be one of the following: slow, large-request, large-reply
> COMMANDLOG HELP extra
(error) ERR wrong number of arguments for 'commandlog|help' command
> COMMANDLOG FOO
(error) ERR unknown subcommand 'FOO'. Try COMMANDLOG HELP.
> COMMANDLOG FOO bar
(error) ERR unknown subcommand 'FOO'. Try COMMANDLOG HELP.
```

**CM-03** types are case-insensitive, empty logs
```text
> COMMANDLOG RESET slow
OK
> COMMANDLOG RESET large-request
OK
> COMMANDLOG RESET large-reply
OK
> COMMANDLOG LEN SLOW
(integer) 0
> COMMANDLOG LEN Large-Request
(integer) 0
> COMMANDLOG LEN LARGE-REPLY
(integer) 0
> COMMANDLOG GET -1 slow
(empty array)
> COMMANDLOG GET 0 large-request
(empty array)
> COMMANDLOG GET 9223372036854775807 large-reply
(empty array)
```

**CM-04** default configuration
```text
> CONFIG GET commandlog*
 1) "commandlog-execution-slower-than"
 2) "10000"
 3) "commandlog-large-reply-max-len"
 4) "128"
 5) "commandlog-large-request-max-len"
 6) "128"
 7) "commandlog-reply-larger-than"
 8) "1048576"
 9) "commandlog-request-larger-than"
10) "1048576"
11) "commandlog-slow-execution-max-len"
12) "128"
> CONFIG GET slowlog*
1) "slowlog-log-slower-than"
2) "10000"
3) "slowlog-max-len"
4) "128"
> CONFIG GET commandlog-slow-execution-max-len slowlog-max-len
1) "commandlog-slow-execution-max-len"
2) "128"
3) "slowlog-max-len"
4) "128"
> CONFIG GET commandlog-execution-slower-than slowlog-log-slower-than
1) "commandlog-execution-slower-than"
2) "10000"
3) "slowlog-log-slower-than"
4) "10000"
```

**CM-05** config value errors
```text
> CONFIG SET commandlog-execution-slower-than -2
(error) ERR CONFIG SET failed (possibly related to argument 'commandlog-execution-slower-than') - argument must be between -1 and 9223372036854775807 inclusive
> CONFIG SET commandlog-request-larger-than -2
(error) ERR CONFIG SET failed (possibly related to argument 'commandlog-request-larger-than') - argument must be between -1 and 9223372036854775807 inclusive
> CONFIG SET commandlog-reply-larger-than abc
(error) ERR CONFIG SET failed (possibly related to argument 'commandlog-reply-larger-than') - argument couldn't be parsed into an integer
> CONFIG SET commandlog-slow-execution-max-len -1
(error) ERR CONFIG SET failed (possibly related to argument 'commandlog-slow-execution-max-len') - argument must be between 0 and 9223372036854775807 inclusive
> CONFIG SET commandlog-large-request-max-len -1
(error) ERR CONFIG SET failed (possibly related to argument 'commandlog-large-request-max-len') - argument must be between 0 and 9223372036854775807 inclusive
> CONFIG SET commandlog-large-reply-max-len abc
(error) ERR CONFIG SET failed (possibly related to argument 'commandlog-large-reply-max-len') - argument couldn't be parsed into an integer
> CONFIG SET slowlog-max-len abc
(error) ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument couldn't be parsed into an integer
> CONFIG SET slowlog-log-slower-than -5
(error) ERR CONFIG SET failed (possibly related to argument 'slowlog-log-slower-than') - argument must be between -1 and 9223372036854775807 inclusive
```

**CM-06** slowlog-* names are aliases of the commandlog slow settings
```text
> CONFIG SET slowlog-max-len 7
OK
> CONFIG GET commandlog-slow-execution-max-len
1) "commandlog-slow-execution-max-len"
2) "7"
> CONFIG SET commandlog-slow-execution-max-len 9
OK
> CONFIG GET slowlog-max-len
1) "slowlog-max-len"
2) "9"
> CONFIG SET slowlog-log-slower-than 1234
OK
> CONFIG GET commandlog-execution-slower-than
1) "commandlog-execution-slower-than"
2) "1234"
> CONFIG SET commandlog-execution-slower-than 4321
OK
> CONFIG GET slowlog-log-slower-than
1) "slowlog-log-slower-than"
2) "4321"
> CONFIG SET slowlog-max-len 128 slowlog-log-slower-than 10000
OK
> CONFIG GET commandlog-slow-execution-max-len
1) "commandlog-slow-execution-max-len"
2) "128"
> CONFIG GET commandlog-execution-slower-than
1) "commandlog-execution-slower-than"
2) "10000"
```

**CM-07** large-request threshold: entry when size >= threshold, boundary 27 vs 28 (SET k v is 27 bytes)
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 27
OK
> SET k v
OK
> SET k vv
OK
> DEL k
(integer) 1
> COMMANDLOG LEN large-request
(integer) 3
> CONFIG SET commandlog-request-larger-than 28
OK
> COMMANDLOG RESET large-request
OK
> SET k v
OK
> SET k vv
OK
> COMMANDLOG LEN large-request
(integer) 2
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
> COMMANDLOG LEN large-request
(integer) 0
```

**CM-08** -1 disables a type, 0 logs everything, max-len 0 disables too
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than -1
OK
> SET k v
OK
> COMMANDLOG LEN large-request
(integer) 0
> CONFIG SET commandlog-request-larger-than 0
OK
> COMMANDLOG RESET large-request
OK
> PING
PONG
> PING
PONG
> COMMANDLOG LEN large-request
(integer) 3
> CONFIG SET commandlog-large-request-max-len 0
OK
> PING
PONG
> COMMANDLOG LEN large-request
(integer) 4
> CONFIG SET commandlog-large-request-max-len 128
OK
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-09** large-request value is the RESP request frame size, argv, newest first
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> SET k v
OK
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 27
2) 1) "SET"
   2) "k"
   3) "v"
> PING
PONG
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 14
2) 1) "PING"
> GET k
"v"
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 20
2) 1) "GET"
   2) "k"
> EVAL return(function(e)return(e[1][1]-e[2][1])end)(redis.call('commandlog','get',2,'large-request')) 0
(integer) 1
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-10** large-reply value is the RESP reply size in bytes
```text
> COMMANDLOG RESET large-reply
OK
> CONFIG SET commandlog-reply-larger-than 0
OK
> SET k v
OK
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-reply')) 0
1) (integer) 5
2) 1) "SET"
   2) "k"
   3) "v"
> GET k
"v"
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-reply')) 0
1) (integer) 7
2) 1) "GET"
   2) "k"
> GET nokey
(nil)
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-reply')) 0
1) (integer) 5
2) 1) "GET"
   2) "nokey"
> PING
PONG
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-reply')) 0
1) (integer) 7
2) 1) "PING"
> ECHO hello
"hello"
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-reply')) 0
1) (integer) 11
2) 1) "ECHO"
   2) "hello"
> RPUSH lst a b
(integer) 2
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-reply')) 0
1) (integer) 4
2) 1) "RPUSH"
   2) "lst"
   3) "a"
   4) "b"
> LRANGE lst 0 -1
1) "a"
2) "b"
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-reply')) 0
1) (integer) 18
2) 1) "LRANGE"
   2) "lst"
   3) "0"
   4) "-1"
> LPUSH k x
(error) WRONGTYPE Operation against a key holding the wrong kind of value
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-reply')) 0
1) (integer) 68
2) 1) "LPUSH"
   2) "k"
   3) "x"
> CONFIG SET commandlog-reply-larger-than 1048576
OK
> COMMANDLOG RESET large-reply
OK
```

**CM-11** slow: value is the execution time in microseconds
```text
> COMMANDLOG RESET slow
OK
> DEBUG SLEEP 0.02
OK
> COMMANDLOG LEN slow
(integer) 1
> EVAL return(function(e)return((e[1][3]>=20000)and(1)or(0))end)(redis.call('commandlog','get',1,'slow')) 0
(integer) 1
> COMMANDLOG RESET slow
OK
> CONFIG SET commandlog-execution-slower-than 0
OK
> COMMANDLOG RESET slow
OK
> PING
PONG
> PING
PONG
> COMMANDLOG LEN slow
(integer) 3
> CONFIG SET commandlog-execution-slower-than -1
OK
> PING
PONG
> COMMANDLOG LEN slow
(integer) 4
> CONFIG SET commandlog-execution-slower-than 10000
OK
> COMMANDLOG RESET slow
OK
> COMMANDLOG LEN slow
(integer) 0
```

**CM-12** argument count trimmed to 32 entries, last one is the marker
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> RPUSH l a1 a2 a3 a4 a5 a6 a7 a8 a9 a10 a11 a12 a13 a14 a15 a16 a17 a18 a19 a20 a21 a22 a23 a24 a25 a26 a27 a28 a29 a30 a31 a32 a33 a34 a35 a36 a37 a38 a39 a40
(integer) 40
> EVAL return(function(e)return{e[1][3],#e[1][4],e[1][4][31],e[1][4][32]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 374
2) (integer) 32
3) "a29"
4) "... (11 more arguments)"
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-13** argument bytes trimmed to 128, then the marker
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> SET k xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OK
> EVAL return(function(e)return{#e[1][4][3],string.sub(e[1][4][3],127)}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 128
2) "xx"
> SET k xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OK
> EVAL return(function(e)return{#e[1][4][3],string.sub(e[1][4][3],127)}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 146
2) "xx... (1 more bytes)"
> SET k xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OK
> EVAL return(function(e)return{#e[1][4][3],string.sub(e[1][4][3],127)}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 147
2) "xx... (72 more bytes)"
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-14** redacted arguments
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> AUTH default pw
OK
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 35
2) 1) "AUTH"
   2) "(redacted)"
   3) "(redacted)"
> AUTH pw
(error) ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 22
2) 1) "AUTH"
   2) "(redacted)"
> HELLO 2 AUTH nouser nopw
(error) WRONGPASS invalid username-password pair or user is disabled.
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 54
2) 1) "HELLO"
   2) "2"
   3) "AUTH"
   4) "(redacted)"
   5) "(redacted)"
> ACL SETUSER cmtest on >secret +get
OK
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 69
2) 1) "ACL"
   2) "SETUSER"
   3) "(redacted)"
   4) "(redacted)"
   5) "(redacted)"
   6) "(redacted)"
> ACL DELUSER cmtest
(integer) 1
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-15** MULTI/EXEC reply sizes (EXEC itself is not logged; queued commands are logged with cumulative reply bytes including the array header)
```text
> COMMANDLOG RESET large-request
OK
> COMMANDLOG RESET large-reply
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> CONFIG SET commandlog-reply-larger-than 0
OK
> MULTI
OK
> SET a 1
QUEUED
> GET a
QUEUED
> EXEC
1) OK
2) "1"
> CONFIG SET commandlog-reply-larger-than 1048576
OK
> EVAL return(function(e)return{e[1][3],e[1][4],e[2][3],e[2][4],e[3][3],e[3][4]}end)(redis.call('commandlog','get',3,'large-reply')) 0
1) (integer) 16
2) 1) "GET"
   2) "a"
3) (integer) 9
4) 1) "SET"
   2) "a"
   3) "1"
5) (integer) 5
6) 1) "MULTI"
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
> COMMANDLOG RESET large-reply
OK
```

**CM-16** MULTI/EXEC request sizes (every queued command gets the size of the EXEC frame, 14 bytes)
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> MULTI
OK
> SET a 1
QUEUED
> GET a
QUEUED
> EXEC
1) OK
2) "1"
> EVAL return(function(e)return{e[1][3],e[1][4],e[2][3],e[2][4],e[3][3],e[3][4]}end)(redis.call('commandlog','get',3,'large-request')) 0
1) (integer) 14
2) 1) "GET"
   2) "a"
3) (integer) 14
4) 1) "SET"
   2) "a"
   3) "1"
5) (integer) 15
6) 1) "MULTI"
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-17** commands run by a script are logged separately (name as written, request size 0), request log
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> EVAL return(redis.call('set','k','v')) 0
OK
> EVAL return(function(e)return{e[1][3],e[1][4],e[2][3],e[2][4]}end)(redis.call('commandlog','get',2,'large-request')) 0
1) (integer) 61
2) 1) "EVAL"
   2) "return(redis.call('set','k','v'))"
   3) "0"
3) (integer) 0
4) 1) "set"
   2) "k"
   3) "v"
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-18** script-run commands, reply log
```text
> COMMANDLOG RESET large-reply
OK
> CONFIG SET commandlog-reply-larger-than 0
OK
> EVAL return(redis.call('set','k','v')) 0
OK
> EVAL return(function(e)return{e[1][3],e[1][4],e[2][3],e[2][4]}end)(redis.call('commandlog','get',2,'large-reply')) 0
1) (integer) 5
2) 1) "EVAL"
   2) "return(redis.call('set','k','v'))"
   3) "0"
3) (integer) 5
4) 1) "set"
   2) "k"
   3) "v"
> CONFIG SET commandlog-reply-larger-than 1048576
OK
> COMMANDLOG RESET large-reply
OK
```

**CM-19** rewritten commands are logged with their original arguments
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> SET k v EX 100
OK
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 44
2) 1) "SET"
   2) "k"
   3) "v"
   4) "EX"
   5) "100"
> EXPIRE k 100
(integer) 1
> EVAL return(function(e)return{e[1][3],e[1][4]}end)(redis.call('commandlog','get',1,'large-request')) 0
1) (integer) 32
2) 1) "EXPIRE"
   2) "k"
   3) "100"
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-20** max-len keeps the newest entries, trimming happens when the next entry is pushed
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-large-request-max-len 2
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> PING
PONG
> PING
PONG
> PING
PONG
> COMMANDLOG LEN large-request
(integer) 2
> CONFIG SET commandlog-large-request-max-len 1
OK
> COMMANDLOG LEN large-request
(integer) 1
> CONFIG SET commandlog-large-request-max-len 128
OK
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-21** count argument
```text
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> PING
PONG
> PING
PONG
> PING
PONG
> EVAL return(#redis.call('commandlog','get',2,'large-request')) 0
(integer) 2
> EVAL return(#redis.call('commandlog','get',-1,'large-request')) 0
(integer) 6
> EVAL return(#redis.call('commandlog','get',1000,'large-request')) 0
(integer) 8
> EVAL return(#redis.call('commandlog','get',0,'large-request')) 0
(integer) 0
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-22** SLOWLOG shares the slow log; RESET only touches slow
```text
> COMMANDLOG RESET slow
OK
> COMMANDLOG RESET large-request
OK
> CONFIG SET commandlog-request-larger-than 0
OK
> PING
PONG
> DEBUG SLEEP 0.02
OK
> SLOWLOG LEN
(integer) 1
> COMMANDLOG LEN slow
(integer) 1
> EVAL return(redis.call('slowlog','get',1)[1][1]==redis.call('commandlog','get',1,'slow')[1][1]) 0
(integer) 1
> EVAL return(redis.call('slowlog','get',1)[1][3]>=20000) 0
(integer) 1
> EVAL return(#redis.call('slowlog','get',0)) 0
(integer) 0
> EVAL return(#redis.call('slowlog','get',-1)) 0
(integer) 1
> SLOWLOG GET abc
(error) ERR count should be greater than or equal to -1
> SLOWLOG GET -2
(error) ERR count should be greater than or equal to -1
> SLOWLOG GET 1 2
(error) ERR unknown subcommand or wrong number of arguments for 'GET'. Try SLOWLOG HELP.
> SLOWLOG GET 1 2 3
(error) ERR unknown subcommand or wrong number of arguments for 'GET'. Try SLOWLOG HELP.
> SLOWLOG FOO
(error) ERR unknown subcommand 'FOO'. Try SLOWLOG HELP.
> SLOWLOG
(error) ERR wrong number of arguments for 'slowlog' command
> SLOWLOG HELP x
(error) ERR wrong number of arguments for 'slowlog|help' command
> SLOWLOG RESET
OK
> SLOWLOG LEN
(integer) 0
> COMMANDLOG LEN large-request
(integer) 20
> CONFIG SET commandlog-request-larger-than 1048576
OK
> COMMANDLOG RESET large-request
OK
```

**CM-23** reply copy avoidance: large bulk replies are counted too
```text
> COMMANDLOG RESET large-reply
OK
> CONFIG SET commandlog-reply-larger-than 100
OK
> SET big xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
OK
> GET big
"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
> EVAL return(redis.call('commandlog','get',1,'large-reply')[1][3]) 0
(integer) 208
> CONFIG SET commandlog-reply-larger-than 1048576
OK
> COMMANDLOG RESET large-reply
OK
> DEL big
(integer) 1
```


---

## 4. DEBUG

### 4.1 Rules

- Arity `-2` (`DEBUG` alone is E_ARITY with `'debug'`). Flags `ADMIN NOSCRIPT LOADING STALE PROTECTED`. On a server without `enable-debug-command` every `DEBUG <anything>` (including unknown subcommands and `DEBUG HELP`) fails with E_DEBUG_OFF **after** the arity check (DB-01: the config is `no` by default and immutable at runtime, `CONFIG SET enable-debug-command yes` fails with `ERR CONFIG SET failed (possibly related to argument 'enable-debug-command') - can't set immutable config`). Inside MULTI the refusal happens at queue time, so `EXEC` returns E_EXECABORT. The text mentions a `local` value (allow only loopback connections), from source; not exercised.
- Subcommand names are case-insensitive. A wrong argument count and an unknown subcommand both give E_SUBARG with the subcommand as typed (DB-03). `DEBUG` is refused from scripts with E_NOSCRIPT_CMD (DB-04).
- `DEBUG HELP` (DB-02) is a 136-line array of status strings (`DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:` then each subcommand and an indented description). The text is the same whether `DEBUG HELP` is sent in RESP2 or RESP3 (simple strings in an array).

### 4.2 Subcommands that matter for a test harness (observed)

| Subcommand | Reply |
|---|---|
| `SLEEP <seconds>` | `+OK` after sleeping. The argument is parsed with `atof`: `abc` sleeps 0, negatives sleep 0, decimals allowed (DB-04). Arity exactly 3. |
| `SET-ACTIVE-EXPIRE <0\|1>` | `+OK`; the value is `atoi`: `0`, `x` disable background expiry, any non-zero enables (DB-04). |
| `LOG <message>` | `+OK`, writes the message to the log. |
| `ERROR <string>` | an error reply whose text is exactly `<string>` with no `ERR` added: `DEBUG ERROR boom` gives `-boom` (shown as `(error) boom`). |
| `CHANGE-REPL-ID` | `+OK` (arity exactly 2). |
| `DIGEST` / `DIGEST-VALUE key [key ...]` | 40 hex chars. `DEBUG DIGEST` of an empty db is 40 zeros; `DIGEST-VALUE` returns one entry per key, 40 zeros for a missing key, an empty array with no keys (DB-06). The digest is order-independent over keys and depends on the type, the value and the expire (DB-06 shows that `EXPIRE` changes it and `PERSIST` restores it). Algorithm: from source `debug.c` (`xorDigest`, `mixDigest`, `xorObjectDigest`: SHA1 per key/value pair, XORed together; strings use `mixDigest` of type tag + value, collections XOR per-element digests when unordered). A reimplementation must port it from there to reproduce the hex values in DB-06 and DB-08. |
| `POPULATE <count> [<prefix>] [<size>]` | `+OK`. Creates `<prefix>:<i>` (default prefix `key`) with value `value:<i>`, truncated or zero-padded to `<size>` bytes when `<size>` is given; existing keys are skipped, not overwritten; `count 0` is fine; a negative or non-numeric count, or a negative or non-numeric size, gives `ERR value is out of range, must be positive` (DB-07). Not propagated. |
| `RELOAD [MERGE\|NOFLUSH\|NOSAVE ...]` | `+OK`, saves the RDB (unless `NOSAVE`), flushes (unless `NOFLUSH`) and reloads; the digest is unchanged (DB-08). An unknown option gives `ERR DEBUG RELOAD only supports the MERGE, NOFLUSH and NOSAVE options.` |
| `OBJECT <key> [fast]` | see the shape block below; a missing key is `ERR no such key` (DB-05). |
| `SDSLEN <key>` | see below; `ERR no such key`, or `ERR Not an sds encoded string.` for an integer-encoded value (DB-05). |
| `LISTPACK <key>` | `+Listpack structure printed on stdout` for a listpack-encoded key, `ERR Not a listpack encoded object.` otherwise (DB-05); `ERR no such key`. |
| `QUICKLIST <key> [0\|1]` | `ERR no such key` on a missing key (DB-05). |
| `STRUCTSIZE` | one bulk string: `bits:64 robj:16 dictentry:24 sdshdr5:1 sdshdr8:3 sdshdr16:5 sdshdr32:9 sdshdr64:17 ` (note the trailing space; the sizes are build-specific). Arity exactly 2. |
| `HTSTATS <dbid> [full]` | one bulk string such as `[Dictionary HT]\nHash table 0 stats (main hash table):\n table size: 7\n number of entries: 1\n rehashing index: -1\n[Expires HT]\n`; `ERR Out of range database` for a bad id, E_INT for a non-integer. |
| `PROTOCOL <type>` | a test value of the given RESP type (4.3). |

Not captured because they kill or damage the server or need infrastructure: `ASSERT`, `CRASH-AND-RECOVER`, `OOM`, `PANIC`, `SEGFAULT`, `RESTART`, `LEAK`, `LOADAOF`, `AOF-FLUSH-SLEEP`, `MALLCTL`, `MALLCTL-STR`, `REPLICATE`, `STRINGMATCH-TEST`, `CLIENT-EVICTION`, `PAUSE-CRON`, `REPLYBUFFER`, `PAUSE-AFTER-FORK`, `DICT-RESIZING`, the cluster-bus fault injectors (`DROP-CLUSTER-PACKET-FILTER`, `CLOSE-CLUSTER-LINK-ON-PACKET-DROP`, `DISABLE-CLUSTER-RANDOM-PING`, `DISABLE-CLUSTER-RECONNECTION`, `CLUSTERLINK KILL`, `SLOTMIGRATION ...`) and the remaining test toggles listed in `DEBUG HELP`. `DEBUG JMAP` does not exist (E_SUBARG).

Variable replies (shape):

```shape
> DEBUG OBJECT s            (SET s hello)
Value at:0x<address> refcount:1 encoding:embstr serializedlength:6 lru:<lru-clock> lru_seconds_idle:<n>
> DEBUG OBJECT s fast
Value at:0x<address> refcount:1 encoding:embstr lru:<lru-clock> lru_seconds_idle:<n>
```

`serializedlength` is the RDB-serialized size of the value (`hello` = 6, `12345` as `int` = 3, a 3-element list as `listpack` = 19, a one-field hash 14, a one-member set 11, a one-member zset 13); `fast` omits that field. Encodings seen: `embstr`, `int`, `listpack`. It is a status reply (`+Value at:...`). Quicklist and hashtable encodings add more fields (from source).

```shape
> DEBUG SDSLEN s            (SET s hello)
key_sds_len:1 key_sds_avail:0 obj_alloc:24 val_sds_len:5 val_sds_avail:3 val_alloc:0
```

`DEBUG SDSLEN`, `OBJECT`, `LISTPACK` and `HTSTATS` print allocator-dependent values; a test should check only the prefix up to the first number.

### 4.3 DEBUG PROTOCOL (wire bytes, DB-10 .. DB-23)

`DEBUG PROTOCOL <type>` with type one of `string integer double bignum null array set map attrib push verbatim true false` (case-insensitive: `STRING` works). It returns a fixed value whose RESP3 encoding depends on the type and whose RESP2 downgrade follows the usual rules:

| Type | RESP2 | RESP3 |
|---|---|---|
| string | `$11\r\nHello World\r\n` | same |
| integer | `:12345\r\n` | same |
| double | `$5\r\n3.141\r\n` | `,3.141\r\n` |
| bignum | `$37\r\n1234567999999999999999999999999999999\r\n` | `(1234567999999999999999999999999999999\r\n` |
| null | `$-1\r\n` | `_\r\n` |
| array | `*3\r\n:0\r\n:1\r\n:2\r\n` | same |
| set | `*3\r\n:0\r\n:1\r\n:2\r\n` | `~3\r\n:0\r\n:1\r\n:2\r\n` |
| map | `*6\r\n:0\r\n:0\r\n:1\r\n:1\r\n:2\r\n:0\r\n` | `%3\r\n:0\r\n#f\r\n:1\r\n#t\r\n:2\r\n#f\r\n` |
| attrib | only the real reply `$39\r\nSome real reply following the attribute\r\n` | `\|1\r\n$14\r\nkey-popularity\r\n*2\r\n$7\r\nkey:123\r\n:90\r\n` then the real reply |
| push | `-ERR RESP2 is not supported by this command\r\n` | `$40\r\nSome real reply following the push reply\r\n` then `>2\r\n$16\r\nserver-cpu-usage\r\n:42\r\n` (the push comes **after** the reply) |
| verbatim | `$25\r\nThis is a verbatim\nstring\r\n` | `=29\r\ntxt:This is a verbatim\nstring\r\n` |
| true | `:1\r\n` | `#t\r\n` |
| false | `:0\r\n` | `#f\r\n` |

A wrong type gives `ERR Wrong protocol type name. Please use one of the following: string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false`; no type or an extra argument is E_SUBARG (DB-23).

### 4.4 Vectors

**DB-01** DEBUG is refused on a server started without enable-debug-command (7402)
```text
> DEBUG HELP
(error) ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.
> DEBUG SLEEP 0
(error) ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.
> debug object x
(error) ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.
> DEBUG PROTOCOL string
(error) ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.
> DEBUG FOO
(error) ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.
> DEBUG
(error) ERR wrong number of arguments for 'debug' command
> DEBUG HELP x
(error) ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.
> CONFIG GET enable-debug-command
1) "enable-debug-command"
2) "no"
> CONFIG SET enable-debug-command yes
(error) ERR CONFIG SET failed (possibly related to argument 'enable-debug-command') - can't set immutable config
> MULTI
OK
> DEBUG SLEEP 0
(error) ERR DEBUG command not allowed. If the enable-debug-command option is set to "local", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.
> EXEC
(error) EXECABORT Transaction discarded because of previous errors.
```

**DB-02** DEBUG HELP
```text
> DEBUG HELP
  1) DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
  2) AOF-FLUSH-SLEEP <microsec>
  3)     Server will sleep before flushing the AOF, this is used for testing.
  4) ASSERT
  5)     Crash by assertion failed.
  6) CHANGE-REPL-ID
  7)     Change the replication IDs of the instance.
  8)     Dangerous: should be used only for testing the replication subsystem.
  9) CONFIG-REWRITE-FORCE-ALL
 10)     Like CONFIG REWRITE but writes all configuration options, including
 11)     keywords not listed in original configuration file or default values.
 12) CRASH-AND-RECOVER [<milliseconds>]
 13)     Hard crash and restart after a <milliseconds> delay (default 0).
 14) DIGEST
 15)     Output a hex signature representing the current DB content.
 16) DIGEST-VALUE <key> [<key> ...]
 17)     Output a hex signature of the values of all the specified keys.
 18) ERROR <string>
 19)     Return a RESP protocol error with <string> as message. Useful for clients
 20)     unit tests to simulate error replies.
 21) LEAK <string>
 22)     Create a memory leak of the input string.
 23) LOG <message>
 24)     Write <message> to the server log.
 25) HTSTATS <dbid> [full]
 26)     Return hash table statistics of the specified database.
 27) HTSTATS-KEY <key> [full]
 28)     Like HTSTATS but for the hash table stored at <key>'s value.
 29) LOADAOF
 30)     Flush the AOF buffers on disk and reload the AOF in memory.
 31) REPLICATE <string>
 32)     Replicates the provided string to replicas, allowing data divergence.
 33) MALLCTL <key> [<val>]
 34)     Get or set a malloc tuning integer.
 35) MALLCTL-STR <key> [<val>]
 36)     Get or set a malloc tuning string.
 37) OBJECT <key> [fast]
 38)     Show low level info about `key` and associated value.
 39)     Some fields of the default behavior may be time consuming to fetch,
 40)     and `fast` can be passed to avoid fetching them.
 41) DROP-CLUSTER-PACKET-FILTER <packet-type>
 42)     Drop all packets that match the filtered type. Set to -1 allow all packets or -2 to drop all packets.
 43) CLOSE-CLUSTER-LINK-ON-PACKET-DROP <0|1>
 44)     This is valid only when DROP-CLUSTER-PACKET-FILTER is set to a valid packet type.
 45)     When set to 1, the cluster link is closed after dropping a packet based on the filter.
 46) DISABLE-CLUSTER-RANDOM-PING <0|1>
 47)     Disable sending cluster ping to a random node every second.
 48) DISABLE-CLUSTER-RECONNECTION <0|1>
 49)     Disable cluster reconnection of cluster nodes.
 50) OOM
 51)     Crash the server simulating an out-of-memory error.
 52) PANIC
 53)     Crash the server simulating a panic.
 54) POPULATE <count> [<prefix>] [<size>]
 55)     Create <count> string keys named key:<num>. If <prefix> is specified then
 56)     it is used instead of the 'key' prefix. These are not propagated to
 57)     replicas. Cluster slots are not respected so keys not belonging to the
 58)     current node can be created in cluster mode.
 59) PROTOCOL <type>
 60)     Reply with a test value of the specified type. <type> can be: string,
 61)     integer, double, bignum, null, array, set, map, attrib, push, verbatim,
 62)     true, false.
 63) RELOAD [option ...]
 64)     Save the RDB on disk and reload it back to memory. Valid <option> values:
 65)     * MERGE: conflicting keys will be loaded from RDB.
 66)     * NOFLUSH: the existing database will not be removed before load, but
 67)       conflicting keys will generate an exception and kill the server.
 68)     * NOSAVE: the database will be loaded from an existing RDB file.
 69)     Examples:
 70)     * DEBUG RELOAD: verify that the server is able to persist, flush and reload
 71)       the database.
 72)     * DEBUG RELOAD NOSAVE: replace the current database with the contents of an
 73)       existing RDB file.
 74)     * DEBUG RELOAD NOSAVE NOFLUSH MERGE: add the contents of an existing RDB
 75)       file to the database.
 76) RESTART [<milliseconds>]
 77)     Graceful restart: save config, db, restart after a <milliseconds> delay (default 0).
 78) SDSLEN <key>
 79)     Show low level SDS string info representing `key` and value.
 80) SEGFAULT
 81)     Crash the server with sigsegv.
 82) SET-ACTIVE-EXPIRE <0|1>
 83)     Setting it to 0 disables expiring keys in background when they are not
 84)     accessed (otherwise the behavior). Setting it to 1 reenables back the
 85)     default.
 86) QUICKLIST-PACKED-THRESHOLD <size>
 87)     Sets the threshold for elements to be inserted as plain vs packed nodes
 88)     Default value is 1GB, allows values up to 4GB. Setting to 0 restores to default.
 89) SET-SKIP-CHECKSUM-VALIDATION <0|1>
 90)     Enables or disables checksum checks for RDB files and RESTORE's payload.
 91) SLEEP <seconds>
 92)     Stop the server for <seconds>. Decimals allowed.
 93) STRINGMATCH-TEST
 94)     Run a fuzz tester against the stringmatchlen() function.
 95) STRUCTSIZE
 96)     Return the size of different core C structures.
 97) LISTPACK <key>
 98)     Show low level info about the listpack encoding of <key>.
 99) QUICKLIST <key> [<0|1>]
100)     Show low level info about the quicklist encoding of <key>.
101)     The optional argument (0 by default) sets the level of detail
102) CLIENT-EVICTION
103)     Show low level client eviction pools info (maxmemory-clients).
104) PAUSE-CRON <0|1>
105)     Stop periodic cron job processing.
106) REPLYBUFFER PEAK-RESET-TIME <NEVER||RESET|time>
107)     Sets the time (in milliseconds) to wait between client reply buffer peak resets.
108)     In case NEVER is provided the last observed peak will never be reset
109)     In case RESET is provided the peak reset time will be restored to the default value
110) REPLYBUFFER RESIZING <0|1>
111)     Enable or disable the reply buffer resize cron job
112) PAUSE-AFTER-FORK <0|1>
113)     Stop the server's main process after fork.
114) DELAY-RDB-CLIENT-FREE-SECOND <seconds>
115)     Grace period in seconds for replica main channel to establish psync.
116) DICT-RESIZING <0|1>
117)     Enable or disable the main dict and expire dict resizing.
118) HASHTABLE-CAN-ABORT-SHRINK <0|1>
119)     Enable or disable the hashtable shrink abort.
120) CLIENT-ENFORCE-REPLY-LIST <0|1>
121)     When set to 1, it enforces the use of the client reply list directly
122)     and avoids using the client's static buffer.
123) SLOTMIGRATION PREVENT-PAUSE <0|1>
124)     When set to 1, slot migrations will be prevented from pausing on the source node.
125) SLOTMIGRATION PREVENT-FAILOVER <0|1>
126)     When set to 1, slot migrations will be prevented from performing the slot-level failover on the target node.
127) FORCE-FREE-PRIMARY-ASYNC <0|1>
128)     Force freeClient on primary to use async path.
129) PROTECT-CLIENT <id>
130)     Protect a client from being freed, forcing deferred close.
131) FORCE-TLS-WRITE-ERROR <0|1>
132)     Force TLS write error for testing.
133) CLUSTERLINK KILL <to|from|all> <node-id>
134)     Kills the link based on the direction to/from (both) with the provided node.
135) HELP
136)     Print this help.
> DEBUG help
  1) DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
  2) AOF-FLUSH-SLEEP <microsec>
  3)     Server will sleep before flushing the AOF, this is used for testing.
  4) ASSERT
  5)     Crash by assertion failed.
  6) CHANGE-REPL-ID
  7)     Change the replication IDs of the instance.
  8)     Dangerous: should be used only for testing the replication subsystem.
  9) CONFIG-REWRITE-FORCE-ALL
 10)     Like CONFIG REWRITE but writes all configuration options, including
 11)     keywords not listed in original configuration file or default values.
 12) CRASH-AND-RECOVER [<milliseconds>]
 13)     Hard crash and restart after a <milliseconds> delay (default 0).
 14) DIGEST
 15)     Output a hex signature representing the current DB content.
 16) DIGEST-VALUE <key> [<key> ...]
 17)     Output a hex signature of the values of all the specified keys.
 18) ERROR <string>
 19)     Return a RESP protocol error with <string> as message. Useful for clients
 20)     unit tests to simulate error replies.
 21) LEAK <string>
 22)     Create a memory leak of the input string.
 23) LOG <message>
 24)     Write <message> to the server log.
 25) HTSTATS <dbid> [full]
 26)     Return hash table statistics of the specified database.
 27) HTSTATS-KEY <key> [full]
 28)     Like HTSTATS but for the hash table stored at <key>'s value.
 29) LOADAOF
 30)     Flush the AOF buffers on disk and reload the AOF in memory.
 31) REPLICATE <string>
 32)     Replicates the provided string to replicas, allowing data divergence.
 33) MALLCTL <key> [<val>]
 34)     Get or set a malloc tuning integer.
 35) MALLCTL-STR <key> [<val>]
 36)     Get or set a malloc tuning string.
 37) OBJECT <key> [fast]
 38)     Show low level info about `key` and associated value.
 39)     Some fields of the default behavior may be time consuming to fetch,
 40)     and `fast` can be passed to avoid fetching them.
 41) DROP-CLUSTER-PACKET-FILTER <packet-type>
 42)     Drop all packets that match the filtered type. Set to -1 allow all packets or -2 to drop all packets.
 43) CLOSE-CLUSTER-LINK-ON-PACKET-DROP <0|1>
 44)     This is valid only when DROP-CLUSTER-PACKET-FILTER is set to a valid packet type.
 45)     When set to 1, the cluster link is closed after dropping a packet based on the filter.
 46) DISABLE-CLUSTER-RANDOM-PING <0|1>
 47)     Disable sending cluster ping to a random node every second.
 48) DISABLE-CLUSTER-RECONNECTION <0|1>
 49)     Disable cluster reconnection of cluster nodes.
 50) OOM
 51)     Crash the server simulating an out-of-memory error.
 52) PANIC
 53)     Crash the server simulating a panic.
 54) POPULATE <count> [<prefix>] [<size>]
 55)     Create <count> string keys named key:<num>. If <prefix> is specified then
 56)     it is used instead of the 'key' prefix. These are not propagated to
 57)     replicas. Cluster slots are not respected so keys not belonging to the
 58)     current node can be created in cluster mode.
 59) PROTOCOL <type>
 60)     Reply with a test value of the specified type. <type> can be: string,
 61)     integer, double, bignum, null, array, set, map, attrib, push, verbatim,
 62)     true, false.
 63) RELOAD [option ...]
 64)     Save the RDB on disk and reload it back to memory. Valid <option> values:
 65)     * MERGE: conflicting keys will be loaded from RDB.
 66)     * NOFLUSH: the existing database will not be removed before load, but
 67)       conflicting keys will generate an exception and kill the server.
 68)     * NOSAVE: the database will be loaded from an existing RDB file.
 69)     Examples:
 70)     * DEBUG RELOAD: verify that the server is able to persist, flush and reload
 71)       the database.
 72)     * DEBUG RELOAD NOSAVE: replace the current database with the contents of an
 73)       existing RDB file.
 74)     * DEBUG RELOAD NOSAVE NOFLUSH MERGE: add the contents of an existing RDB
 75)       file to the database.
 76) RESTART [<milliseconds>]
 77)     Graceful restart: save config, db, restart after a <milliseconds> delay (default 0).
 78) SDSLEN <key>
 79)     Show low level SDS string info representing `key` and value.
 80) SEGFAULT
 81)     Crash the server with sigsegv.
 82) SET-ACTIVE-EXPIRE <0|1>
 83)     Setting it to 0 disables expiring keys in background when they are not
 84)     accessed (otherwise the behavior). Setting it to 1 reenables back the
 85)     default.
 86) QUICKLIST-PACKED-THRESHOLD <size>
 87)     Sets the threshold for elements to be inserted as plain vs packed nodes
 88)     Default value is 1GB, allows values up to 4GB. Setting to 0 restores to default.
 89) SET-SKIP-CHECKSUM-VALIDATION <0|1>
 90)     Enables or disables checksum checks for RDB files and RESTORE's payload.
 91) SLEEP <seconds>
 92)     Stop the server for <seconds>. Decimals allowed.
 93) STRINGMATCH-TEST
 94)     Run a fuzz tester against the stringmatchlen() function.
 95) STRUCTSIZE
 96)     Return the size of different core C structures.
 97) LISTPACK <key>
 98)     Show low level info about the listpack encoding of <key>.
 99) QUICKLIST <key> [<0|1>]
100)     Show low level info about the quicklist encoding of <key>.
101)     The optional argument (0 by default) sets the level of detail
102) CLIENT-EVICTION
103)     Show low level client eviction pools info (maxmemory-clients).
104) PAUSE-CRON <0|1>
105)     Stop periodic cron job processing.
106) REPLYBUFFER PEAK-RESET-TIME <NEVER||RESET|time>
107)     Sets the time (in milliseconds) to wait between client reply buffer peak resets.
108)     In case NEVER is provided the last observed peak will never be reset
109)     In case RESET is provided the peak reset time will be restored to the default value
110) REPLYBUFFER RESIZING <0|1>
111)     Enable or disable the reply buffer resize cron job
112) PAUSE-AFTER-FORK <0|1>
113)     Stop the server's main process after fork.
114) DELAY-RDB-CLIENT-FREE-SECOND <seconds>
115)     Grace period in seconds for replica main channel to establish psync.
116) DICT-RESIZING <0|1>
117)     Enable or disable the main dict and expire dict resizing.
118) HASHTABLE-CAN-ABORT-SHRINK <0|1>
119)     Enable or disable the hashtable shrink abort.
120) CLIENT-ENFORCE-REPLY-LIST <0|1>
121)     When set to 1, it enforces the use of the client reply list directly
122)     and avoids using the client's static buffer.
123) SLOTMIGRATION PREVENT-PAUSE <0|1>
124)     When set to 1, slot migrations will be prevented from pausing on the source node.
125) SLOTMIGRATION PREVENT-FAILOVER <0|1>
126)     When set to 1, slot migrations will be prevented from performing the slot-level failover on the target node.
127) FORCE-FREE-PRIMARY-ASYNC <0|1>
128)     Force freeClient on primary to use async path.
129) PROTECT-CLIENT <id>
130)     Protect a client from being freed, forcing deferred close.
131) FORCE-TLS-WRITE-ERROR <0|1>
132)     Force TLS write error for testing.
133) CLUSTERLINK KILL <to|from|all> <node-id>
134)     Kills the link based on the direction to/from (both) with the provided node.
135) HELP
136)     Print this help.
> DEBUG HELP x
(error) ERR unknown subcommand or wrong number of arguments for 'HELP'. Try DEBUG HELP.
> CONFIG GET enable-debug-command
1) "enable-debug-command"
2) "yes"
```

**DB-03** unknown subcommand and argument-count errors share one message
```text
> DEBUG FOO
(error) ERR unknown subcommand or wrong number of arguments for 'FOO'. Try DEBUG HELP.
> DEBUG FOO bar
(error) ERR unknown subcommand or wrong number of arguments for 'FOO'. Try DEBUG HELP.
> DEBUG SLEEP
(error) ERR unknown subcommand or wrong number of arguments for 'SLEEP'. Try DEBUG HELP.
> DEBUG SLEEP 1 2
(error) ERR unknown subcommand or wrong number of arguments for 'SLEEP'. Try DEBUG HELP.
> DEBUG SET-ACTIVE-EXPIRE
(error) ERR unknown subcommand or wrong number of arguments for 'SET-ACTIVE-EXPIRE'. Try DEBUG HELP.
> DEBUG OBJECT
(error) ERR unknown subcommand or wrong number of arguments for 'OBJECT'. Try DEBUG HELP.
> DEBUG ERROR
(error) ERR unknown subcommand or wrong number of arguments for 'ERROR'. Try DEBUG HELP.
> DEBUG ERROR boom x
(error) ERR unknown subcommand or wrong number of arguments for 'ERROR'. Try DEBUG HELP.
> DEBUG LOG
(error) ERR unknown subcommand or wrong number of arguments for 'LOG'. Try DEBUG HELP.
> DEBUG LOG a b
(error) ERR unknown subcommand or wrong number of arguments for 'LOG'. Try DEBUG HELP.
> DEBUG CHANGE-REPL-ID x
(error) ERR unknown subcommand or wrong number of arguments for 'CHANGE-REPL-ID'. Try DEBUG HELP.
> DEBUG JMAP
(error) ERR unknown subcommand or wrong number of arguments for 'JMAP'. Try DEBUG HELP.
> DEBUG STRUCTSIZE x
(error) ERR unknown subcommand or wrong number of arguments for 'STRUCTSIZE'. Try DEBUG HELP.
> DEBUG PROTOCOL
(error) ERR unknown subcommand or wrong number of arguments for 'PROTOCOL'. Try DEBUG HELP.
> DEBUG PROTOCOL string extra
(error) ERR unknown subcommand or wrong number of arguments for 'PROTOCOL'. Try DEBUG HELP.
> DEBUG SDSLEN
(error) ERR unknown subcommand or wrong number of arguments for 'SDSLEN'. Try DEBUG HELP.
> DEBUG POPULATE
(error) ERR unknown subcommand or wrong number of arguments for 'POPULATE'. Try DEBUG HELP.
> DEBUG POPULATE 1 pre 5 x
(error) ERR unknown subcommand or wrong number of arguments for 'POPULATE'. Try DEBUG HELP.
```

**DB-04** simple subcommands: SLEEP, SET-ACTIVE-EXPIRE, LOG, ERROR, CHANGE-REPL-ID, JMAP
```text
> DEBUG SLEEP 0
OK
> DEBUG SLEEP abc
OK
> DEBUG SLEEP -1
OK
> DEBUG sleep 0.001
OK
> DEBUG SET-ACTIVE-EXPIRE 0
OK
> DEBUG SET-ACTIVE-EXPIRE 1
OK
> DEBUG SET-ACTIVE-EXPIRE 2
OK
> DEBUG SET-ACTIVE-EXPIRE x
OK
> DEBUG SET-ACTIVE-EXPIRE 1
OK
> DEBUG LOG hello
OK
> DEBUG ERROR boom
(error) boom
> DEBUG ERROR -ERR
(error) -ERR
> DEBUG ERROR x
(error) x
> DEBUG CHANGE-REPL-ID
OK
> MULTI
OK
> DEBUG SLEEP 0
QUEUED
> EXEC
1) OK
> EVAL return(redis.call('debug','sleep','0')) 0
(error) ERR This Valkey command is not allowed from script script: on @user_script:1.
```

**DB-05** DEBUG OBJECT, SDSLEN, LISTPACK, QUICKLIST, HTSTATS: errors (the success replies are variable, see the shape block)
```text
> DEBUG OBJECT nokey
(error) ERR no such key
> DEBUG SDSLEN nokey
(error) ERR no such key
> SET i 12345
OK
> DEBUG SDSLEN i
(error) ERR Not an sds encoded string.
> SET s hello
OK
> DEBUG LISTPACK s
(error) ERR Not a listpack encoded object.
> DEBUG LISTPACK nokey
(error) ERR no such key
> DEBUG QUICKLIST nokey
(error) ERR no such key
> DEBUG HTSTATS 99
(error) ERR Out of range database
> DEBUG HTSTATS abc
(error) ERR value is not an integer or out of range
```

**DB-06** DEBUG DIGEST and DIGEST-VALUE
```text
> DEBUG DIGEST
0000000000000000000000000000000000000000
> DEBUG DIGEST-VALUE nokey
1) 0000000000000000000000000000000000000000
> DEBUG DIGEST-VALUE
(empty array)
> SET s hello
OK
> DEBUG DIGEST-VALUE s nokey s
1) 36b23a1456b2dce2c3ed252c456761301dba8060
2) 0000000000000000000000000000000000000000
3) 36b23a1456b2dce2c3ed252c456761301dba8060
> DEBUG DIGEST
a500c6b54937da22f2ac1167da1da852479c7339
> SET s hello
OK
> DEBUG DIGEST
a500c6b54937da22f2ac1167da1da852479c7339
> SET s hellO
OK
> DEBUG DIGEST
d6b5674bb049a8263c9539b4bdb43288a0f8e92c
> SET s hello
OK
> DEBUG DIGEST
a500c6b54937da22f2ac1167da1da852479c7339
> RPUSH l a b c
(integer) 3
> HSET h f v
(integer) 1
> SADD st a
(integer) 1
> ZADD z 1 a
(integer) 1
> DEBUG DIGEST-VALUE s l h st z
1) 36b23a1456b2dce2c3ed252c456761301dba8060
2) 8bf72d812571eea9b927f3c11beb0c4165a6ff89
3) 90c76e9e9f4c62d642a34fc97c7dad503b51f906
4) 344e2dce84032272e136053b2083009290148e5e
5) 430190c6ed5abafff87d97b4f51a92796ed05895
> DEBUG DIGEST
495d33afb56df31b3c03b9427bd83d8b43af5b24
> EXPIRE s 1000
(integer) 1
> DEBUG DIGEST
bba99c9cc43588814256d612db4ad1ae6f391c17
> PERSIST s
(integer) 1
> DEBUG DIGEST
495d33afb56df31b3c03b9427bd83d8b43af5b24
```

**DB-07** DEBUG POPULATE
```text
> DEBUG POPULATE 0
OK
> DBSIZE
(integer) 0
> DEBUG POPULATE -1
(error) ERR value is out of range, must be positive
> DEBUG POPULATE abc
(error) ERR value is out of range, must be positive
> DEBUG POPULATE 3
OK
> DBSIZE
(integer) 3
> EXISTS key:0 key:1 key:2 key:3
(integer) 3
> GET key:0
"value:0"
> GET key:2
"value:2"
> TYPE key:0
string
> DEBUG POPULATE 2 pre 5
OK
> GET pre:0
"value"
> GET pre:1
"value"
> DEBUG POPULATE 2 pre abc
(error) ERR value is out of range, must be positive
> DEBUG POPULATE 2 pre -1
(error) ERR value is out of range, must be positive
> DEBUG POPULATE 5 key
OK
> DBSIZE
(integer) 7
> DEBUG POPULATE 1 big 12
OK
> GET big:0
"value:0\x00\x00\x00\x00\x00"
> STRLEN big:0
(integer) 12
> DEBUG POPULATE 1 bigger 20
OK
> GET bigger:0
"value:0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
> STRLEN bigger:0
(integer) 20
```

**DB-08** DEBUG RELOAD keeps the data set
```text
> SET s hello
OK
> RPUSH l a b c
(integer) 3
> DEBUG DIGEST
6d6afcc403e8c2cd1b9bc0b7a6393b9ed78c70c0
> DEBUG RELOAD
OK
> GET s
"hello"
> LRANGE l 0 -1
1) "a"
2) "b"
3) "c"
> DEBUG DIGEST
6d6afcc403e8c2cd1b9bc0b7a6393b9ed78c70c0
> DEBUG RELOAD NOSAVE
OK
> DEBUG DIGEST
6d6afcc403e8c2cd1b9bc0b7a6393b9ed78c70c0
> DEBUG RELOAD FOO
(error) ERR DEBUG RELOAD only supports the MERGE, NOFLUSH and NOSAVE options.
> DEBUG RELOAD NOSAVE NOFLUSH MERGE
OK
> DEBUG DIGEST
6d6afcc403e8c2cd1b9bc0b7a6393b9ed78c70c0
> DBSIZE
(integer) 2
```


**DB-10** DEBUG PROTOCOL string, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL string
    <- '$11\r\nHello World\r\n'
[B3] > DEBUG PROTOCOL string
    <- '$11\r\nHello World\r\n'
```

**DB-11** DEBUG PROTOCOL integer, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL integer
    <- ':12345\r\n'
[B3] > DEBUG PROTOCOL integer
    <- ':12345\r\n'
```

**DB-12** DEBUG PROTOCOL double, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL double
    <- '$5\r\n3.141\r\n'
[B3] > DEBUG PROTOCOL double
    <- ',3.141\r\n'
```

**DB-13** DEBUG PROTOCOL bignum, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL bignum
    <- '$37\r\n1234567999999999999999999999999999999\r\n'
[B3] > DEBUG PROTOCOL bignum
    <- '(1234567999999999999999999999999999999\r\n'
```

**DB-14** DEBUG PROTOCOL null, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL null
    <- '$-1\r\n'
[B3] > DEBUG PROTOCOL null
    <- '_\r\n'
```

**DB-15** DEBUG PROTOCOL array, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL array
    <- '*3\r\n:0\r\n:1\r\n:2\r\n'
[B3] > DEBUG PROTOCOL array
    <- '*3\r\n:0\r\n:1\r\n:2\r\n'
```

**DB-16** DEBUG PROTOCOL set, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL set
    <- '*3\r\n:0\r\n:1\r\n:2\r\n'
[B3] > DEBUG PROTOCOL set
    <- '~3\r\n:0\r\n:1\r\n:2\r\n'
```

**DB-17** DEBUG PROTOCOL map, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL map
    <- '*6\r\n:0\r\n:0\r\n:1\r\n:1\r\n:2\r\n:0\r\n'
[B3] > DEBUG PROTOCOL map
    <- '%3\r\n:0\r\n#f\r\n:1\r\n#t\r\n:2\r\n#f\r\n'
```

**DB-18** DEBUG PROTOCOL attrib, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL attrib
    <- '$39\r\nSome real reply following the attribute\r\n'
[B3] > DEBUG PROTOCOL attrib
    <- '|1\r\n$14\r\nkey-popularity\r\n*2\r\n$7\r\nkey:123\r\n:90\r\n$39\r\nSome real reply following the attribute\r\n'
```

**DB-19** DEBUG PROTOCOL push, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL push
    <- '-ERR RESP2 is not supported by this command\r\n'
[B3] > DEBUG PROTOCOL push
    <- '$40\r\nSome real reply following the push reply\r\n>2\r\n$16\r\nserver-cpu-usage\r\n:42\r\n'
```

**DB-20** DEBUG PROTOCOL verbatim, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL verbatim
    <- '$25\r\nThis is a verbatim\nstring\r\n'
[B3] > DEBUG PROTOCOL verbatim
    <- '=29\r\ntxt:This is a verbatim\nstring\r\n'
```

**DB-21** DEBUG PROTOCOL true, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL true
    <- ':1\r\n'
[B3] > DEBUG PROTOCOL true
    <- '#t\r\n'
```

**DB-22** DEBUG PROTOCOL false, RESP2 then RESP3
```text
[A] > DEBUG PROTOCOL false
    <- ':0\r\n'
[B3] > DEBUG PROTOCOL false
    <- '#f\r\n'
```

**DB-23** DEBUG PROTOCOL: bad type, case, arity
```text
[A] > DEBUG PROTOCOL foo
    <- '-ERR Wrong protocol type name. Please use one of the following: string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false\r\n'
[B3] > DEBUG PROTOCOL STRING
    <- '$11\r\nHello World\r\n'
[A] > DEBUG PROTOCOL
    <- "-ERR unknown subcommand or wrong number of arguments for 'PROTOCOL'. Try DEBUG HELP.\r\n"
[A] > DEBUG PROTOCOL string extra
    <- "-ERR unknown subcommand or wrong number of arguments for 'PROTOCOL'. Try DEBUG HELP.\r\n"
```


---

## 5. SHUTDOWN

### 5.1 Flag parsing (from source `shutdownCommand`, confirmed by SD-01/SD-02 and scenario A)

`SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]` plus the cluster flags `SAFE` and `FAILOVER` (the JSON lists no tokens, arity `-1`). Flags are case-insensitive and may appear in any order. Rules:

- Unknown token, a number, or an empty-looking extra word: E_SYNTAX (SD-01).
- `SAVE` together with `NOSAVE`: E_SYNTAX in either order. The same flag may be repeated (`SAVE SAVE`, `FORCE FORCE`, `ABORT ABORT` are fine).
- `ABORT` must be the **only** token (`ABORT NOW`, `ABORT SAVE`, `NOW ABORT`, `SAFE ABORT`, `ABORT x` are all E_SYNTAX; `FAILOVER ABORT` too). `SHUTDOWN ABORT` with no shutdown pending is E_NOSHUTDOWN (SD-02).
- `FAILOVER` on a standalone server: `ERR SHUTDOWN FAILOVER is only supported in cluster mode.` (SD-02, also with `NOW`). `SAFE` is accepted on a standalone server and does nothing there.
- Inside MULTI: refused at queue time with E_NOTXN (`'shutdown'`), the transaction aborts (SD-03). From a script: E_NOSCRIPT_CMD (SD-03).

### 5.2 What the flags mean (observed with throwaway servers on 7404/7405)

| Flag | Effect |
|---|---|
| (none) | Save an RDB if at least one `save` point is configured (`--save "900 1"`), do not save otherwise (`--save ""`). Wait for lagging replicas up to `shutdown-timeout` (10 s). |
| `SAVE` | Save even when no save points are configured (observed: `--save ""` plus `SHUTDOWN SAVE` logs `Saving the final RDB snapshot before exiting.`). |
| `NOSAVE` | Never save. |
| `NOW` | Do not wait for lagging replicas. Does **not** bypass a failing save. |
| `FORCE` | Ignore errors: if the save fails the server logs `Error trying to save the DB. Exit anyway.` and exits; with `SAVE` too it is the same. |
| `SAFE` (cluster only) | Refuse to exit when this node is a voting primary that would break the cluster. |
| `FAILOVER` (cluster only) | Try to hand the primaryship to a replica first (`Unable to find a replica to perform the auto failover on shutdown.` when there is none, then exits). |
| `ABORT` | Cancel a shutdown that is waiting for replicas. |

### 5.3 Outcomes (observed)

| Situation | Reply and effect |
|---|---|
| Successful shutdown (every flag combination in the table that exits) | **No reply bytes**: the connection is closed (`<- ''  [closed]`), the process exits with status 0. Log lines in order: `User requested shutdown...`, optionally `Saving the final RDB snapshot before exiting.` / `DB saved on disk`, then `Valkey is now ready to exit, bye bye...`. The data set is saved to `dump.rdb` in `dir` when a save happened (and loses data written since the last save when `NOSAVE`). |
| Save fails (`dump.rdb` is a directory, or `dir` is unwritable) with no `FORCE` and no `NOSAVE` (plain, `SAVE`, `NOW`) | reply E_SHUTDOWN, the **server keeps running** and accepts commands (PING works). Log: `Error moving temp DB file ... Is a directory` / `Failed opening the temp RDB file ...`, `Error trying to save the DB, can't exit.`, `Errors trying to shut down the server. Check the logs for more information.` |
| Same failure with `NOSAVE` | exits normally, no reply, status 0 |
| Same failure with `FORCE` (or `SAVE FORCE`) | exits, no reply, status 0 (`Error trying to save the DB. Exit anyway.`) |
| Shutdown waiting for a lagging replica (master with a frozen replica, plain `SHUTDOWN`) | the calling client is **blocked**; other clients keep being served (`PING` answers `PONG` meanwhile). Log: `Waiting for replicas before shutting down.` |
| `SHUTDOWN ABORT` from another client while that is pending | `+OK` to the aborter; the blocked client then receives E_SHUTDOWN; the server continues (log `Shutdown manually aborted.`); a second `SHUTDOWN ABORT` is E_NOSHUTDOWN. |
| Cluster node, plain `SHUTDOWN` / `SHUTDOWN NOSAVE` on a one-node cluster | exits, no reply. |
| Cluster node, `SHUTDOWN SAFE`, `SAFE NOSAVE`, `NOW SAFE` | E_SHUTDOWN, server keeps running. Log: `This is a voting primary. Shutting down may cause the cluster to go down. Can't exit.` |
| Cluster node, `SHUTDOWN SAFE FORCE` | exits (`... Exit anyway.`). |
| Cluster node, `SHUTDOWN FAILOVER` with no replica | exits (a replica-less primary just logs that no failover target was found). |

Vectors cover only the error paths that leave the server up (the vector runner cannot restart a server):

### 5.4 Vectors

**SD-01** flag syntax errors (the server must stay up)
```text
> SHUTDOWN FOO
(error) ERR syntax error
> SHUTDOWN SAVE NOSAVE
(error) ERR syntax error
> SHUTDOWN NOSAVE SAVE
(error) ERR syntax error
> SHUTDOWN ABORT NOW
(error) ERR syntax error
> SHUTDOWN ABORT SAVE
(error) ERR syntax error
> SHUTDOWN NOW ABORT
(error) ERR syntax error
> SHUTDOWN SAFE ABORT
(error) ERR syntax error
> SHUTDOWN 1
(error) ERR syntax error
> SHUTDOWN ABORT x
(error) ERR syntax error
> SHUTDOWN FOO BAR
(error) ERR syntax error
> PING
PONG
```

**SD-02** ABORT with nothing to abort, FAILOVER on a standalone server
```text
> SHUTDOWN ABORT
(error) ERR No shutdown in progress.
> SHUTDOWN abort
(error) ERR No shutdown in progress.
> SHUTDOWN ABORT ABORT
(error) ERR No shutdown in progress.
> SHUTDOWN FAILOVER
(error) ERR SHUTDOWN FAILOVER is only supported in cluster mode.
> SHUTDOWN NOW FAILOVER
(error) ERR SHUTDOWN FAILOVER is only supported in cluster mode.
> SHUTDOWN FAILOVER ABORT
(error) ERR syntax error
> PING
PONG
```

**SD-03** SHUTDOWN is refused inside MULTI and from scripts
```text
> MULTI
OK
> SHUTDOWN
(error) ERR Command 'shutdown' not allowed inside a transaction
> EXEC
(error) EXECABORT Transaction discarded because of previous errors.
> MULTI
OK
> SHUTDOWN NOSAVE
(error) ERR Command 'shutdown' not allowed inside a transaction
> DISCARD
OK
> EVAL return(redis.call('shutdown')) 0
(error) ERR This Valkey command is not allowed from script script: on @user_script:1.
> EVAL return(redis.pcall('shutdown')) 0
(error) ERR This Valkey command is not allowed from script
> PING
PONG
```


---

## 6. READONLY, READWRITE, ASKING

### 6.1 Rules

- All three have arity 1 (`READONLY x` is E_ARITY with `'readonly'`) and are `FAST LOADING STALE` (`ASKING` is `FAST`), ACL `@connection @fast`.
- On a **standalone** server `READONLY` and `READWRITE` are accepted and reply `+OK` (they only set or clear a client flag), repeated calls are fine, and the commands are queued normally in MULTI. `ASKING` fails with E_CLUSTER_OFF (RW-01). `ASKING` is also an error from a script on a standalone server, while `READONLY` is allowed from a script (RW-04). RESP3 does not change any of this (RW-05).
- In **cluster mode** all three reply `+OK` (CL-11). `READONLY` lets the connection read from a replica (no observable effect on a primary), `ASKING` marks the **next** command only as an asking command (it is cleared after the next command; from source, not observable on a one-node cluster); `READWRITE` clears `READONLY`. In MULTI they are queued (`QUEUED`, then two `OK`s at `EXEC`).

### 6.2 Vectors

**RW-01** standalone: READONLY and READWRITE succeed, ASKING fails
```text
> READONLY
OK
> READWRITE
OK
> ASKING
(error) ERR This instance has cluster support disabled
> readonly
OK
> READONLY
OK
> READONLY
OK
> READWRITE
OK
> READWRITE
OK
```

**RW-02** standalone arity errors
```text
> READONLY x
(error) ERR wrong number of arguments for 'readonly' command
> READWRITE x
(error) ERR wrong number of arguments for 'readwrite' command
> ASKING x
(error) ERR wrong number of arguments for 'asking' command
> READONLY x y
(error) ERR wrong number of arguments for 'readonly' command
```

**RW-03** standalone inside MULTI: queued, errors surface at EXEC
```text
> MULTI
OK
> READONLY
QUEUED
> READWRITE
QUEUED
> ASKING
QUEUED
> EXEC
1) OK
2) OK
3) (error) ERR This instance has cluster support disabled
```

**RW-04** standalone: ASKING from a script
```text
> EVAL return(redis.call('readonly')) 0
OK
> EVAL return(redis.pcall('asking')) 0
(error) ERR This instance has cluster support disabled
```

**RW-05** standalone wire bytes
```text
[A] > READONLY
    <- '+OK\r\n'
[A] > READWRITE
    <- '+OK\r\n'
[A] > ASKING
    <- '-ERR This instance has cluster support disabled\r\n'
[B3] > ASKING
    <- '-ERR This instance has cluster support disabled\r\n'
```


---

## 7. CLUSTER on a non-cluster server

### 7.1 Rules

- `cluster-enabled` is `no`, `INFO cluster` is `# Cluster\r\ncluster_enabled:0\r\n` (CN-03).
- Every CLUSTER subcommand except `KEYSLOT` (and arity errors, `HELP`'s arity, unknown subcommand) fails with E_CLUSTER_OFF: `INFO SLOTS SHARDS NODES MYID MYSHARDID HELP LINKS COUNTKEYSINSLOT GETKEYSINSLOT REPLICAS SLAVES COUNT-FAILURE-REPORTS FORGET REPLICATE MEET ADDSLOTS ADDSLOTSRANGE DELSLOTS DELSLOTSRANGE FLUSHSLOTS FLUSHSLOT SETSLOT SAVECONFIG BUMPEPOCH SET-CONFIG-EPOCH FAILOVER RESET SLOT-STATS GETSLOTMIGRATIONS MIGRATESLOTS SYNCSLOTS` (CN-01), and so do `CLUSTERSCAN` and `ASKING`. Arguments are **not** validated first (`CLUSTER REPLICAS abc` gives E_CLUSTER_OFF), but arity is (`CLUSTER CANCELSLOTMIGRATIONS ALL` is E_ARITY: that subcommand takes no argument, so its arity is 2).
- `CLUSTER HELP` itself fails with E_CLUSTER_OFF, so the help text can only be read in cluster mode (CL-02).
- `CLUSTER KEYSLOT <key>` works everywhere (CN-02); arity is checked first, so does `CLUSTER COUNTKEYSINSLOT` etc.; an unknown subcommand error uses the subcommand as typed (`CLUSTER cluster` -> `unknown subcommand 'cluster'`).
- Database commands behave normally: `SELECT 1..15`, `SWAPDB`, `MOVE`, `COPY ... DB` work; `cluster-databases` is `1` and `databases` is `16` (CN-04).

### 7.2 Vectors

**CN-01** CLUSTER subcommands on a non-cluster server: disabled error
```text
> CLUSTER INFO
(error) ERR This instance has cluster support disabled
> CLUSTER SLOTS
(error) ERR This instance has cluster support disabled
> CLUSTER SHARDS
(error) ERR This instance has cluster support disabled
> CLUSTER NODES
(error) ERR This instance has cluster support disabled
> CLUSTER MYID
(error) ERR This instance has cluster support disabled
> CLUSTER MYSHARDID
(error) ERR This instance has cluster support disabled
> CLUSTER HELP
(error) ERR This instance has cluster support disabled
> CLUSTER LINKS
(error) ERR This instance has cluster support disabled
> CLUSTER COUNTKEYSINSLOT 0
(error) ERR This instance has cluster support disabled
> CLUSTER GETKEYSINSLOT 0 1
(error) ERR This instance has cluster support disabled
> CLUSTER REPLICAS abc
(error) ERR This instance has cluster support disabled
> CLUSTER SLAVES abc
(error) ERR This instance has cluster support disabled
> CLUSTER COUNT-FAILURE-REPORTS abc
(error) ERR This instance has cluster support disabled
> CLUSTER FORGET abc
(error) ERR This instance has cluster support disabled
> CLUSTER REPLICATE abc
(error) ERR This instance has cluster support disabled
> CLUSTER MEET 127.0.0.1 1
(error) ERR This instance has cluster support disabled
> CLUSTER ADDSLOTS 1
(error) ERR This instance has cluster support disabled
> CLUSTER ADDSLOTSRANGE 1 2
(error) ERR This instance has cluster support disabled
> CLUSTER DELSLOTS 1
(error) ERR This instance has cluster support disabled
> CLUSTER DELSLOTSRANGE 1 2
(error) ERR This instance has cluster support disabled
> CLUSTER FLUSHSLOTS
(error) ERR This instance has cluster support disabled
> CLUSTER FLUSHSLOT 1
(error) ERR This instance has cluster support disabled
> CLUSTER SETSLOT 1 STABLE
(error) ERR This instance has cluster support disabled
> CLUSTER SAVECONFIG
(error) ERR This instance has cluster support disabled
> CLUSTER BUMPEPOCH
(error) ERR This instance has cluster support disabled
> CLUSTER SET-CONFIG-EPOCH 1
(error) ERR This instance has cluster support disabled
> CLUSTER FAILOVER
(error) ERR This instance has cluster support disabled
> CLUSTER RESET
(error) ERR This instance has cluster support disabled
> CLUSTER SLOT-STATS SLOTSRANGE 0 1
(error) ERR This instance has cluster support disabled
> CLUSTER GETSLOTMIGRATIONS
(error) ERR This instance has cluster support disabled
> CLUSTER CANCELSLOTMIGRATIONS ALL
(error) ERR wrong number of arguments for 'cluster|cancelslotmigrations' command
> CLUSTER MIGRATESLOTS SLOTSRANGE 0 1 NODE abc
(error) ERR This instance has cluster support disabled
> CLUSTER SYNCSLOTS ESTABLISH
(error) ERR This instance has cluster support disabled
> CLUSTER info
(error) ERR This instance has cluster support disabled
```

**CN-02** CLUSTER KEYSLOT works without cluster mode; arity is checked first
```text
> CLUSTER KEYSLOT foo
(integer) 12182
> CLUSTER KEYSLOT bar
(integer) 5061
> CLUSTER KEYSLOT {user1000}.following
(integer) 3443
> CLUSTER KEYSLOT
(error) ERR wrong number of arguments for 'cluster|keyslot' command
> CLUSTER KEYSLOT a b
(error) ERR wrong number of arguments for 'cluster|keyslot' command
> CLUSTER COUNTKEYSINSLOT
(error) ERR wrong number of arguments for 'cluster|countkeysinslot' command
> CLUSTER GETKEYSINSLOT 0
(error) ERR wrong number of arguments for 'cluster|getkeysinslot' command
> CLUSTER INFO x
(error) ERR wrong number of arguments for 'cluster|info' command
> CLUSTER HELP x
(error) ERR wrong number of arguments for 'cluster|help' command
> CLUSTER
(error) ERR wrong number of arguments for 'cluster' command
> CLUSTER FOO
(error) ERR unknown subcommand 'FOO'. Try CLUSTER HELP.
> CLUSTER FOO bar
(error) ERR unknown subcommand 'FOO'. Try CLUSTER HELP.
> CLUSTER cluster
(error) ERR unknown subcommand 'cluster'. Try CLUSTER HELP.
```

**CN-03** INFO cluster on a non-cluster server
```text
> INFO cluster
"# Cluster\r\ncluster_enabled:0\r\n"
> INFO CLUSTER
"# Cluster\r\ncluster_enabled:0\r\n"
> CONFIG GET cluster-enabled
1) "cluster-enabled"
2) "no"
```

**CN-04** related commands without cluster mode
```text
> CLUSTERSCAN 0
(error) ERR This instance has cluster support disabled
> CLUSTER SLOTS
(error) ERR This instance has cluster support disabled
> SELECT 1
OK
> SELECT 15
OK
> SELECT 16
(error) ERR DB index is out of range
> SELECT 0
OK
> SWAPDB 0 1
OK
> SWAPDB 0 0
OK
> MOVE nokey 1
(integer) 0
> COPY a b DB 1
(integer) 0
> CONFIG GET cluster-databases
1) "cluster-databases"
2) "1"
> CONFIG GET databases
1) "databases"
2) "16"
```


---

## 8. Cluster mode (one node owning all 16384 slots, port 7403)

Only what a single node can show was captured: no peer, no replica, no `MOVED`/`ASK`, no failover, no gossip (`cluster_stats_*` stay 0, `CLUSTER LINKS` is empty). Everything below that is not labelled otherwise was observed on that node.

### 8.1 INFO and CLUSTER INFO

- `INFO cluster` is `# Cluster\r\ncluster_enabled:1\r\n`; `INFO server` has `server_mode:cluster`; `HELLO 3` reports `mode` `cluster` (instead of `standalone`); `CONFIG GET cluster-enabled` is `yes` (CL-01).
- `CLUSTER INFO` is a bulk string in RESP2 and a verbatim `txt:` string in RESP3: 21 lines `name:value\r\n` in this order: `cluster_state` (`ok`/`fail`), `cluster_slots_assigned`, `cluster_slots_ok`, `cluster_slots_pfail`, `cluster_slots_fail`, `cluster_nodes_pfail`, `cluster_nodes_fail`, `cluster_voting_nodes_pfail`, `cluster_voting_nodes_fail`, `cluster_known_nodes`, `cluster_size`, `cluster_current_epoch`, `cluster_my_epoch`, `cluster_stats_messages_sent`, `cluster_stats_messages_received`, `cluster_stats_bytes_sent`, `cluster_stats_bytes_received`, `cluster_stats_pubsub_bytes_sent`, `cluster_stats_pubsub_bytes_received`, `cluster_stats_module_bytes_sent`, `cluster_stats_module_bytes_received`, `total_cluster_links_buffer_limit_exceeded` (CL-01 shows the full text). A fresh one-node cluster has state `ok`, 16384 slots assigned, `known_nodes 1`, `size 1`, epochs 0.
- `cluster_state` becomes `fail` when a slot has no owner and `cluster-require-full-coverage yes` (default); with `no` it stays `ok` (8.7).

### 8.2 Identity: MYID, MYSHARDID, NODES, SLOTS, SHARDS (variable: ids)

- `CLUSTER MYID`: 40 lowercase hex chars (bulk). `CLUSTER MYSHARDID`: 40 hex chars, a different value (a shard id is generated separately). Both change on every fresh start (not in vectors).
- `CLUSTER NODES`: one line per known node, newline-terminated, bulk in RESP2 and verbatim `txt:` in RESP3. Format `<id> <ip>:<port>@<bus-port>[,<hostname>] <flags> <primary-id or -> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-ranges...>`. A fresh node has no IP (it only learns its own IP from a peer), so the address is **empty**: `:7403@17403`. Observed lines:

```shape
> CLUSTER NODES                              (fresh node, no announce ip)
<40-hex-id> :7403@17403 myself,master - 0 0 0 connected 0-16383
> CLUSTER NODES        (--cluster-announce-ip 10.1.2.3 --cluster-announce-hostname myhost.example)
<40-hex-id> 10.1.2.3:7405@17405,myhost.example myself,master - 0 0 0 connected 0-16383
```

  The bus port is port + 10000 (`cluster-port 0`). The config epoch column is 0 right after `ADDSLOTSRANGE`; the first epoch bump (`CLUSTER BUMPEPOCH`, `SET-CONFIG-EPOCH 1`, or the node settling) makes it 1, which is why epochs are not in vectors after the first one.
- `CLUSTER SLOTS`: array of `[start, end, node, replica...]`; `node` = `[endpoint, port, id, metadata]`. Observed RESP2 wire for the fresh node: `*1\r\n*3\r\n:0\r\n:16383\r\n*4\r\n$0\r\n\r\n:7403\r\n$40\r\n<id>\r\n*0\r\n`; RESP3 differs only in the last element: `%0\r\n` (a map) instead of `*0\r\n`. With an announced ip the endpoint is `"10.1.2.3"`, and a hostname gives metadata `["hostname","myhost.example"]` (a map in RESP3). With `cluster-preferred-endpoint-type hostname` the endpoint becomes the hostname and the metadata is `["ip","10.1.2.3"]`; with `unknown-endpoint` the endpoint is **null** (`(nil)`) and the metadata carries both `ip` and `hostname`. An empty endpoint with type `ip` is the empty bulk string `""`.
- `CLUSTER SHARDS`: array of maps. Per shard: `slots` (flat array `[start, end, ...]`), `nodes` (array of node maps), `id` (the shard id). Per node: `id`, `port`, `ip`, `endpoint`, then `hostname` only when one is set (and `tls-port` first when TLS is on, from source), then `role` (`master`/`replica`), `replication-offset` (integer), `health` (`online`/`failed`/`loading`). The `endpoint` follows the preferred endpoint type. RESP2 uses flat arrays everywhere, RESP3 uses maps (`%3` for the shard, `%7` for the node). Observed RESP2 wire for the fresh node (ids vary):

```shape
*1\r\n*6\r\n$5\r\nslots\r\n*2\r\n:0\r\n:16383\r\n$5\r\nnodes\r\n*1\r\n*14\r\n
$2\r\nid\r\n$40\r\n<node-id>\r\n$4\r\nport\r\n:7403\r\n$2\r\nip\r\n$0\r\n\r\n$8\r\nendpoint\r\n$0\r\n\r\n
$4\r\nrole\r\n$6\r\nmaster\r\n$18\r\nreplication-offset\r\n:0\r\n$6\r\nhealth\r\n$6\r\nonline\r\n
$2\r\nid\r\n$40\r\n<shard-id>\r\n
```

  (the line breaks above are only for display). RESP3: `*1\r\n%3\r\n$5\r\nslots\r\n*2\r\n:0\r\n:16383\r\n$5\r\nnodes\r\n*1\r\n%7\r\n...` with the same key/value order.
- `CLUSTER LINKS`: array of maps, one per link (`direction`, `node`, `create-time`, `events`, `send-buffer-allocated`, `send-buffer-used`, from source); empty on a node with no peer: `*0\r\n` in both protocols (CL-05, CS-15).
- Preferred endpoint knobs (observed on 7405): `cluster-announce-ip`, `cluster-announce-hostname`, `cluster-announce-human-nodename`, `cluster-preferred-endpoint-type ip|hostname|unknown-endpoint` (a `CONFIG SET` takes effect immediately).

### 8.3 Key-space subcommands (CL-03, CL-04)

- `CLUSTER KEYSLOT <key>`: CRC16 (XMODEM) of the key, or of the first `{...}` with a non-empty body, mod 16384. Edge cases (CL-03): `foo{}{bar}` hashes the whole key (empty tag), `foo{{bar}}zap` hashes `{bar`, `foo{bar}{zap}` hashes `bar`, `{bar` and `foo}bar` hash the whole key, `{}` hashes `{}`. The reply is an integer.
- `CLUSTER COUNTKEYSINSLOT <slot>`: integer. `CLUSTER GETKEYSINSLOT <slot> <count>`: array of keys (in key-insertion/hash order inside the slot; a test should sort), `count 0` is an empty array. Errors: slot out of 0..16383 -> `ERR Invalid slot` (COUNTKEYSINSLOT) / `ERR Invalid slot or number of keys` (GETKEYSINSLOT, also for a negative count); non-integers -> E_INT; wrong arity -> E_ARITY (all in CL-04).
- `CLUSTER SLOT-STATS` (CL-06): `SLOTSRANGE <start> <end>` returns `[[slot, ["key-count", n]], ...]` (RESP3: the inner list is a map `%1`); `ORDERBY KEY-COUNT [LIMIT n] [ASC|DESC]` returns the top slots, default order **descending**, limit 1..16384 (default 16384, so `ORDERBY KEY-COUNT` alone returns 16384 entries: not in vectors), ties are returned in ascending slot order. Only `KEY-COUNT` exists with the default `cluster-slot-stats-enabled no`; `CPU-USEC` and friends are rejected with `ERR Unrecognized sort metric for ORDERBY.`. Errors: `ERR Start slot number 5 is greater than end slot number 3`, E_SLOT (`Invalid or out of range slot`), `ERR Limit has to lie in between 1 and 16384 (maximum number of slots).` (also for non-numeric limits), E_SYNTAX for a bad direction token; `CLUSTER SLOT-STATS` and `... ORDERBY` alone are E_ARITY, an unknown first word (`FOO`) is E_ARITY (3 words) and `SLOTSRANGE 0` is E_SUBARG.
- `CLUSTER GETSLOTMIGRATIONS` is an empty array; `CLUSTER CANCELSLOTMIGRATIONS` with nothing running is `ERR No migrations ongoing` (CL-05).

### 8.4 State-changing subcommands (observed, not vectors)

These succeed on a one-node cluster but change the node, so they are described instead of replayed:

| Command | Reply |
|---|---|
| `CLUSTER SETSLOT 1 STABLE` | `+OK` |
| `CLUSTER SET-CONFIG-EPOCH 1` | `+OK` (only while the epoch is 0 and the node is alone) |
| `CLUSTER SAVECONFIG` | `+OK` |
| `CLUSTER BUMPEPOCH` | `+BUMPED 1` (or `+STILL <epoch>` when the epoch is already the highest) |
| `CLUSTER FLUSHSLOT <slot> [ASYNC\|SYNC]` | `+OK`, deletes the keys of one slot; bad token E_SYNTAX |
| `CLUSTER DELSLOTS <slot>` / `ADDSLOTS <slot>` | `+OK`; the cluster state follows (8.7) |

### 8.5 Argument and state errors that do not mutate (CL-07)

`CLUSTER REPLICAS|SLAVES|COUNT-FAILURE-REPORTS|FORGET|REPLICATE <unknown-id>` -> `ERR Unknown node <id>`; `SETSLOT` with an unknown action or without the node id -> `ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP`; `SETSLOT <slot> IMPORTING <id>` on a slot this node owns -> `ERR I'm already the owner of hash slot <slot>`; `MIGRATING <unknown>` -> `ERR I don't know about node <id>`; `NODE <unknown>` -> `ERR Unknown node <id>`; `ADDSLOTS` of an assigned slot -> `ERR Slot <n> is already busy`; `ADDSLOTSRANGE a b` with `a > b` -> `ERR start slot number 5 is greater than end slot number 1` (an odd number of slot arguments is E_ARITY); `MEET abc 1` -> `ERR Invalid node address specified: abc:1`, `MEET 127.0.0.1 abc` -> `ERR Invalid base port specified: abc`, port out of range -> `ERR Port number is out of range`; `SET-CONFIG-EPOCH -1` -> `ERR Invalid config epoch specified: -1`; `FAILOVER` (any flag combination) on a primary -> `ERR You should send CLUSTER FAILOVER to a replica`, unknown flag E_SYNTAX; `RESET FOO` E_SYNTAX; `MIGRATESLOTS` without `NODE` E_SYNTAX, with an unknown node `ERR Invalid node name: abc`; `SYNCSLOTS FOO` and `ESTABLISH` alone E_SYNTAX; `FLUSHSLOT abc` / `16384` E_SLOT.

### 8.6 Arity table and unknown subcommands (CL-08, CL-09)

Every subcommand's arity (section 0.1) is enforced before any argument is looked at; CL-09 sends one argument too few and one too many for each subcommand. `CLUSTER` alone is E_ARITY with `'cluster'`; an unknown subcommand is E_SUBUNK with `CLUSTER`.

### 8.7 When the cluster is down (observed on a separate node, 7405; not vectors)

Started like 7403, then `CLUSTER DELSLOTS 15495` (the slot of key `a`), waited 0.5 s:

| Config | Command | Reply |
|---|---|---|
| `cluster-require-full-coverage yes` (default) | `CLUSTER INFO` | `cluster_state:fail`, `cluster_slots_assigned:16383` |
| | `GET a` (unserved slot) | E_CLUSTERDOWN_UNBOUND |
| | `GET b`, `SET b 1`, `GET {t}x` (any key command) | E_CLUSTERDOWN |
| | `MSET a 1 b 2` | E_CROSSSLOT (cross-slot is checked before down) |
| | `PING`, `EVAL return(1) 0`, `KEYS *`, `DBSIZE`, `PUBLISH a x` | normal replies |
| | `SPUBLISH a x`, `SSUBSCRIBE a` | E_CLUSTERDOWN_UNBOUND |
| | after `CLUSTER ADDSLOTS 15495` | everything works again |
| `cluster-require-full-coverage no` | `CLUSTER INFO` | `cluster_state:ok` |
| | `GET a`, `SET a 1`, `EXISTS a` (unserved slot) | E_CLUSTERDOWN_UNBOUND |
| | `GET b`, `SET b 1` | normal |
| | a script touching key `a` | `ERR Attempted to access a non local key in a cluster node script: on @user_script:1.` |
| | `SPUBLISH a x` | E_CLUSTERDOWN_UNBOUND |

(`cluster-allow-reads-when-down yes` would turn E_CLUSTERDOWN into E_CLUSTERDOWN_RO for writes: from source, not exercised.)

### 8.8 Vectors

**CL-01** INFO cluster and CLUSTER INFO on a fresh one-node cluster
```text
> INFO cluster
"# Cluster\r\ncluster_enabled:1\r\n"
> CLUSTER INFO
"cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_nodes_pfail:0\r\ncluster_nodes_fail:0\r\ncluster_voting_nodes_pfail:0\r\ncluster_voting_nodes_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:0\r\ncluster_my_epoch:0\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\ncluster_stats_bytes_sent:0\r\ncluster_stats_bytes_received:0\r\ncluster_stats_pubsub_bytes_sent:0\r\ncluster_stats_pubsub_bytes_received:0\r\ncluster_stats_module_bytes_sent:0\r\ncluster_stats_module_bytes_received:0\r\ntotal_cluster_links_buffer_limit_exceeded:0\r\n"
> CLUSTER info
"cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_nodes_pfail:0\r\ncluster_nodes_fail:0\r\ncluster_voting_nodes_pfail:0\r\ncluster_voting_nodes_fail:0\r\ncluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:0\r\ncluster_my_epoch:0\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\ncluster_stats_bytes_sent:0\r\ncluster_stats_bytes_received:0\r\ncluster_stats_pubsub_bytes_sent:0\r\ncluster_stats_pubsub_bytes_received:0\r\ncluster_stats_module_bytes_sent:0\r\ncluster_stats_module_bytes_received:0\r\ntotal_cluster_links_buffer_limit_exceeded:0\r\n"
> CONFIG GET cluster-enabled
1) "cluster-enabled"
2) "yes"
> CLUSTER INFO x
(error) ERR wrong number of arguments for 'cluster|info' command
```

**CL-02** CLUSTER HELP
```text
> CLUSTER HELP
 1) CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
 2) COUNTKEYSINSLOT <slot>
 3)     Return the number of keys in <slot>.
 4) GETKEYSINSLOT <slot> <count>
 5)     Return key names stored by current node in a slot.
 6) INFO
 7)     Return information about the cluster.
 8) KEYSLOT <key>
 9)     Return the hash slot for <key>.
10) MYID
11)     Return the node id.
12) MYSHARDID
13)     Return the node's shard id.
14) NODES
15)     Return cluster configuration seen by node. Output format:
16)     <id> <ip:port@bus-port[,hostname]> <flags> <primary> <pings> <pongs> <epoch> <link> <slot> ...
17) REPLICAS <node-id>
18)     Return <node-id> replicas.
19) SLOTS
20)     Return information about slots range mappings. Each range is made of:
21)     start, end, primary and replicas IP addresses, ports and ids
22) SLOT-STATS
23)     Return an array of slot usage statistics for slots assigned to the current node.
24) SHARDS
25)     Return information about slot range mappings and the nodes associated with them.
26) ADDSLOTS <slot> [<slot> ...]
27)     Assign slots to current node.
28) ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]
29)     Assign slots which are between <start-slot> and <end-slot> to current node.
30) BUMPEPOCH
31)     Advance the cluster config epoch.
32) COUNT-FAILURE-REPORTS <node-id>
33)     Return number of failure reports for <node-id>.
34) DELSLOTS <slot> [<slot> ...]
35)     Delete slots information from current node.
36) DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]
37)     Delete slots information which are between <start-slot> and <end-slot> from current node.
38) FAILOVER [FORCE|TAKEOVER]
39)     Promote current replica node to being a master.
40) FORGET <node-id>
41)     Remove a node from the cluster.
42) FLUSHSLOTS
43)     Delete current node own slots information.
44) MEET <ip> <port> [<bus-port>]
45)     Connect nodes into a working cluster.
46) REPLICATE <node-id>
47)     Configure current node as replica to <node-id>.
48) RESET [HARD|SOFT]
49)     Reset current node (default: soft).
50) SET-CONFIG-EPOCH <epoch>
51)     Set config epoch of current node.
52) SETSLOT <slot> (IMPORTING <node-id>|MIGRATING <node-id>|STABLE|NODE <node-id>)
53)     Set slot state.
54) SAVECONFIG
55)     Force saving cluster configuration on disk.
56) LINKS
57)     Return information about all network links between this node and its peers.
58)     Output format is an array where each array element is a map containing attributes of a link
59) MIGRATESLOTS SLOTSRANGE start-slot end-slot [start-slot end-slot ...] NODE node-id [SLOTSRANGE start-slot end-slot [start-slot end-slot ...] NODE node-id ...]
60)     Migrate the specified slot ranges from this node to the specified node.
61) CANCELSLOTMIGRATIONS ALL
62)     Cancel all migrations.
63) GETSLOTMIGRATIONS
64)     Get information about ongoing and recently finished slot imports and exports.
65) HELP
66)     Print this help.
> CLUSTER help
 1) CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:
 2) COUNTKEYSINSLOT <slot>
 3)     Return the number of keys in <slot>.
 4) GETKEYSINSLOT <slot> <count>
 5)     Return key names stored by current node in a slot.
 6) INFO
 7)     Return information about the cluster.
 8) KEYSLOT <key>
 9)     Return the hash slot for <key>.
10) MYID
11)     Return the node id.
12) MYSHARDID
13)     Return the node's shard id.
14) NODES
15)     Return cluster configuration seen by node. Output format:
16)     <id> <ip:port@bus-port[,hostname]> <flags> <primary> <pings> <pongs> <epoch> <link> <slot> ...
17) REPLICAS <node-id>
18)     Return <node-id> replicas.
19) SLOTS
20)     Return information about slots range mappings. Each range is made of:
21)     start, end, primary and replicas IP addresses, ports and ids
22) SLOT-STATS
23)     Return an array of slot usage statistics for slots assigned to the current node.
24) SHARDS
25)     Return information about slot range mappings and the nodes associated with them.
26) ADDSLOTS <slot> [<slot> ...]
27)     Assign slots to current node.
28) ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]
29)     Assign slots which are between <start-slot> and <end-slot> to current node.
30) BUMPEPOCH
31)     Advance the cluster config epoch.
32) COUNT-FAILURE-REPORTS <node-id>
33)     Return number of failure reports for <node-id>.
34) DELSLOTS <slot> [<slot> ...]
35)     Delete slots information from current node.
36) DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]
37)     Delete slots information which are between <start-slot> and <end-slot> from current node.
38) FAILOVER [FORCE|TAKEOVER]
39)     Promote current replica node to being a master.
40) FORGET <node-id>
41)     Remove a node from the cluster.
42) FLUSHSLOTS
43)     Delete current node own slots information.
44) MEET <ip> <port> [<bus-port>]
45)     Connect nodes into a working cluster.
46) REPLICATE <node-id>
47)     Configure current node as replica to <node-id>.
48) RESET [HARD|SOFT]
49)     Reset current node (default: soft).
50) SET-CONFIG-EPOCH <epoch>
51)     Set config epoch of current node.
52) SETSLOT <slot> (IMPORTING <node-id>|MIGRATING <node-id>|STABLE|NODE <node-id>)
53)     Set slot state.
54) SAVECONFIG
55)     Force saving cluster configuration on disk.
56) LINKS
57)     Return information about all network links between this node and its peers.
58)     Output format is an array where each array element is a map containing attributes of a link
59) MIGRATESLOTS SLOTSRANGE start-slot end-slot [start-slot end-slot ...] NODE node-id [SLOTSRANGE start-slot end-slot [start-slot end-slot ...] NODE node-id ...]
60)     Migrate the specified slot ranges from this node to the specified node.
61) CANCELSLOTMIGRATIONS ALL
62)     Cancel all migrations.
63) GETSLOTMIGRATIONS
64)     Get information about ongoing and recently finished slot imports and exports.
65) HELP
66)     Print this help.
> CLUSTER HELP x
(error) ERR wrong number of arguments for 'cluster|help' command
```

**CL-03** CLUSTER KEYSLOT: hash tag rules
```text
> CLUSTER KEYSLOT foo
(integer) 12182
> CLUSTER KEYSLOT bar
(integer) 5061
> CLUSTER KEYSLOT 123456789
(integer) 12739
> CLUSTER KEYSLOT {user1000}.following
(integer) 3443
> CLUSTER KEYSLOT {user1000}.followers
(integer) 3443
> CLUSTER KEYSLOT foo{}{bar}
(integer) 8363
> CLUSTER KEYSLOT foo{{bar}}zap
(integer) 4015
> CLUSTER KEYSLOT foo{bar}{zap}
(integer) 5061
> CLUSTER KEYSLOT {bar
(integer) 4015
> CLUSTER KEYSLOT foo}bar
(integer) 7223
> CLUSTER KEYSLOT {}
(integer) 15257
> CLUSTER KEYSLOT a
(integer) 15495
> CLUSTER KEYSLOT KEYSLOT
(integer) 820
> CLUSTER KEYSLOT b{a}
(integer) 15495
> CLUSTER KEYSLOT {a}{b}
(integer) 15495
> CLUSTER KEYSLOT x{}
(integer) 2608
> CLUSTER KEYSLOT {{a}}
(integer) 10276
> CLUSTER KEYSLOT {a}}
(integer) 15495
> CLUSTER KEYSLOT {}a{b}
(integer) 16175
> CLUSTER keyslot foo
(integer) 12182
> CLUSTER KEYSLOT
(error) ERR wrong number of arguments for 'cluster|keyslot' command
> CLUSTER KEYSLOT a b
(error) ERR wrong number of arguments for 'cluster|keyslot' command
```

**CL-04** COUNTKEYSINSLOT and GETKEYSINSLOT
```text
> SET foo 1
OK
> SET bar 2
OK
> SET {foo}x 3
OK
> CLUSTER COUNTKEYSINSLOT 12182
(integer) 2
> CLUSTER COUNTKEYSINSLOT 5061
(integer) 1
> CLUSTER COUNTKEYSINSLOT 0
(integer) 0
> CLUSTER COUNTKEYSINSLOT 16383
(integer) 0
> CLUSTER GETKEYSINSLOT 12182 10
1) "foo"
2) "{foo}x"
> CLUSTER GETKEYSINSLOT 12182 1
1) "foo"
> CLUSTER GETKEYSINSLOT 12182 0
(empty array)
> CLUSTER GETKEYSINSLOT 5061 1
1) "bar"
> CLUSTER GETKEYSINSLOT 0 10
(empty array)
> DEL foo
(integer) 1
> CLUSTER COUNTKEYSINSLOT 12182
(integer) 1
> CLUSTER GETKEYSINSLOT 12182 10
1) "{foo}x"
> CLUSTER COUNTKEYSINSLOT -1
(error) ERR Invalid slot
> CLUSTER COUNTKEYSINSLOT 16384
(error) ERR Invalid slot
> CLUSTER COUNTKEYSINSLOT abc
(error) ERR value is not an integer or out of range
> CLUSTER COUNTKEYSINSLOT 1.5
(error) ERR value is not an integer or out of range
> CLUSTER COUNTKEYSINSLOT
(error) ERR wrong number of arguments for 'cluster|countkeysinslot' command
> CLUSTER COUNTKEYSINSLOT 1 2
(error) ERR wrong number of arguments for 'cluster|countkeysinslot' command
> CLUSTER GETKEYSINSLOT -1 1
(error) ERR Invalid slot or number of keys
> CLUSTER GETKEYSINSLOT 16384 1
(error) ERR Invalid slot or number of keys
> CLUSTER GETKEYSINSLOT abc 1
(error) ERR value is not an integer or out of range
> CLUSTER GETKEYSINSLOT 12182 -1
(error) ERR Invalid slot or number of keys
> CLUSTER GETKEYSINSLOT 12182 abc
(error) ERR value is not an integer or out of range
> CLUSTER GETKEYSINSLOT 12182
(error) ERR wrong number of arguments for 'cluster|getkeysinslot' command
> CLUSTER GETKEYSINSLOT 12182 1 2
(error) ERR wrong number of arguments for 'cluster|getkeysinslot' command
```

**CL-05** LINKS, GETSLOTMIGRATIONS, CANCELSLOTMIGRATIONS on a node with no peers
```text
> CLUSTER LINKS
(empty array)
> CLUSTER GETSLOTMIGRATIONS
(empty array)
> CLUSTER CANCELSLOTMIGRATIONS
(error) ERR No migrations ongoing
> CLUSTER CANCELSLOTMIGRATIONS ALL
(error) ERR wrong number of arguments for 'cluster|cancelslotmigrations' command
> CLUSTER LINKS x
(error) ERR wrong number of arguments for 'cluster|links' command
> CLUSTER GETSLOTMIGRATIONS x
(error) ERR wrong number of arguments for 'cluster|getslotmigrations' command
```

**CL-06** CLUSTER SLOT-STATS
```text
> CLUSTER SLOT-STATS SLOTSRANGE 0 1
1) 1) (integer) 0
   2) 1) "key-count"
      2) (integer) 0
2) 1) (integer) 1
   2) 1) "key-count"
      2) (integer) 0
> CLUSTER SLOT-STATS SLOTSRANGE 16383 16383
1) 1) (integer) 16383
   2) 1) "key-count"
      2) (integer) 0
> SET foo 1
OK
> SET bar 2
OK
> CLUSTER SLOT-STATS SLOTSRANGE 12182 12182
1) 1) (integer) 12182
   2) 1) "key-count"
      2) (integer) 1
> CLUSTER SLOT-STATS SLOTSRANGE 5061 5061
1) 1) (integer) 5061
   2) 1) "key-count"
      2) (integer) 1
> CLUSTER SLOT-STATS ORDERBY KEY-COUNT LIMIT 2 DESC
1) 1) (integer) 5061
   2) 1) "key-count"
      2) (integer) 1
2) 1) (integer) 12182
   2) 1) "key-count"
      2) (integer) 1
> CLUSTER SLOT-STATS ORDERBY KEY-COUNT LIMIT 2 ASC
1) 1) (integer) 0
   2) 1) "key-count"
      2) (integer) 0
2) 1) (integer) 1
   2) 1) "key-count"
      2) (integer) 0
> CLUSTER SLOT-STATS ORDERBY KEY-COUNT LIMIT 1
1) 1) (integer) 5061
   2) 1) "key-count"
      2) (integer) 1
> CLUSTER SLOT-STATS ORDERBY key-count LIMIT 1 desc
1) 1) (integer) 5061
   2) 1) "key-count"
      2) (integer) 1
> CLUSTER SLOT-STATS ORDERBY KEY-COUNT LIMIT 0
(error) ERR Limit has to lie in between 1 and 16384 (maximum number of slots).
> CLUSTER SLOT-STATS ORDERBY KEY-COUNT LIMIT 16385
(error) ERR Limit has to lie in between 1 and 16384 (maximum number of slots).
> CLUSTER SLOT-STATS ORDERBY KEY-COUNT LIMIT abc
(error) ERR Limit has to lie in between 1 and 16384 (maximum number of slots).
> CLUSTER SLOT-STATS ORDERBY KEY-COUNT LIMIT 1 FOO
(error) ERR syntax error
> CLUSTER SLOT-STATS ORDERBY FOO
(error) ERR Unrecognized sort metric for ORDERBY.
> CLUSTER SLOT-STATS ORDERBY CPU-USEC LIMIT 1
(error) ERR Unrecognized sort metric for ORDERBY.
> CLUSTER SLOT-STATS SLOTSRANGE 5 3
(error) ERR Start slot number 5 is greater than end slot number 3
> CLUSTER SLOT-STATS SLOTSRANGE 0 16384
(error) ERR Invalid or out of range slot
> CLUSTER SLOT-STATS SLOTSRANGE a b
(error) ERR Invalid or out of range slot
> CLUSTER SLOT-STATS SLOTSRANGE -1 5
(error) ERR Invalid or out of range slot
> CLUSTER SLOT-STATS FOO
(error) ERR wrong number of arguments for 'cluster|slot-stats' command
> CLUSTER SLOT-STATS
(error) ERR wrong number of arguments for 'cluster|slot-stats' command
> CLUSTER SLOT-STATS ORDERBY
(error) ERR wrong number of arguments for 'cluster|slot-stats' command
> CLUSTER SLOT-STATS SLOTSRANGE 0
(error) ERR unknown subcommand or wrong number of arguments for 'SLOT-STATS'. Try CLUSTER HELP.
```

**CL-07** errors that do not change cluster state
```text
> CLUSTER REPLICAS abc
(error) ERR Unknown node abc
> CLUSTER SLAVES abc
(error) ERR Unknown node abc
> CLUSTER COUNT-FAILURE-REPORTS abc
(error) ERR Unknown node abc
> CLUSTER FORGET abc
(error) ERR Unknown node abc
> CLUSTER REPLICATE abc
(error) ERR Unknown node abc
> CLUSTER SETSLOT 1 FOO
(error) ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP
> CLUSTER SETSLOT 1
(error) ERR wrong number of arguments for 'cluster|setslot' command
> CLUSTER SETSLOT abc STABLE
(error) ERR Invalid or out of range slot
> CLUSTER SETSLOT 16384 STABLE
(error) ERR Invalid or out of range slot
> CLUSTER SETSLOT 1 MIGRATING abc
(error) ERR I don't know about node abc
> CLUSTER SETSLOT 1 IMPORTING abc
(error) ERR I'm already the owner of hash slot 1
> CLUSTER SETSLOT 1 NODE abc
(error) ERR Unknown node abc
> CLUSTER ADDSLOTS 1
(error) ERR Slot 1 is already busy
> CLUSTER ADDSLOTS abc
(error) ERR Invalid or out of range slot
> CLUSTER ADDSLOTS -1
(error) ERR Invalid or out of range slot
> CLUSTER ADDSLOTS 16384
(error) ERR Invalid or out of range slot
> CLUSTER ADDSLOTSRANGE 5 1
(error) ERR start slot number 5 is greater than end slot number 1
> CLUSTER ADDSLOTSRANGE 1 2 3
(error) ERR wrong number of arguments for 'cluster|addslotsrange' command
> CLUSTER ADDSLOTSRANGE 0 16384
(error) ERR Invalid or out of range slot
> CLUSTER DELSLOTS abc
(error) ERR Invalid or out of range slot
> CLUSTER DELSLOTS 99999
(error) ERR Invalid or out of range slot
> CLUSTER DELSLOTSRANGE 5 1
(error) ERR start slot number 5 is greater than end slot number 1
> CLUSTER DELSLOTSRANGE 1 2 3
(error) ERR wrong number of arguments for 'cluster|delslotsrange' command
> CLUSTER MEET abc 1
(error) ERR Invalid node address specified: abc:1
> CLUSTER MEET 127.0.0.1 abc
(error) ERR Invalid base port specified: abc
> CLUSTER MEET 127.0.0.1 0
(error) ERR Port number is out of range
> CLUSTER MEET 127.0.0.1 70000
(error) ERR Port number is out of range
> CLUSTER SET-CONFIG-EPOCH abc
(error) ERR value is not an integer or out of range
> CLUSTER SET-CONFIG-EPOCH -1
(error) ERR Invalid config epoch specified: -1
> CLUSTER FAILOVER
(error) ERR You should send CLUSTER FAILOVER to a replica
> CLUSTER FAILOVER FOO
(error) ERR syntax error
> CLUSTER FAILOVER FORCE
(error) ERR You should send CLUSTER FAILOVER to a replica
> CLUSTER FAILOVER TAKEOVER
(error) ERR You should send CLUSTER FAILOVER to a replica
> CLUSTER FAILOVER FORCE TAKEOVER
(error) ERR You should send CLUSTER FAILOVER to a replica
> CLUSTER RESET FOO
(error) ERR syntax error
> CLUSTER RESET HARD SOFT
(error) ERR unknown subcommand or wrong number of arguments for 'RESET'. Try CLUSTER HELP.
> CLUSTER MIGRATESLOTS SLOTSRANGE 0 1
(error) ERR syntax error
> CLUSTER MIGRATESLOTS SLOTSRANGE 0 1 NODE abc
(error) ERR Invalid node name: abc
> CLUSTER MIGRATESLOTS FOO 0 1 NODE abc
(error) ERR syntax error
> CLUSTER SYNCSLOTS FOO
(error) ERR syntax error
> CLUSTER SYNCSLOTS ESTABLISH
(error) ERR syntax error
> CLUSTER FLUSHSLOT abc
(error) ERR Invalid or out of range slot
> CLUSTER FLUSHSLOT 16384
(error) ERR Invalid or out of range slot
> CLUSTER FLUSHSLOT 5 FOO
(error) ERR syntax error
```

**CL-08** unknown subcommand
```text
> CLUSTER FOO
(error) ERR unknown subcommand 'FOO'. Try CLUSTER HELP.
> CLUSTER FOO bar
(error) ERR unknown subcommand 'FOO'. Try CLUSTER HELP.
> CLUSTER cluster
(error) ERR unknown subcommand 'cluster'. Try CLUSTER HELP.
> CLUSTER SLOTSX
(error) ERR unknown subcommand 'SLOTSX'. Try CLUSTER HELP.
> CLUSTER
(error) ERR wrong number of arguments for 'cluster' command
```

**CL-09** arity errors for every CLUSTER subcommand (checked before anything else)
```text
> CLUSTER ADDSLOTS
(error) ERR wrong number of arguments for 'cluster|addslots' command
> CLUSTER ADDSLOTSRANGE a
(error) ERR wrong number of arguments for 'cluster|addslotsrange' command
> CLUSTER BUMPEPOCH a
(error) ERR wrong number of arguments for 'cluster|bumpepoch' command
> CLUSTER CANCELSLOTMIGRATIONS a
(error) ERR wrong number of arguments for 'cluster|cancelslotmigrations' command
> CLUSTER COUNT-FAILURE-REPORTS
(error) ERR wrong number of arguments for 'cluster|count-failure-reports' command
> CLUSTER COUNT-FAILURE-REPORTS a a
(error) ERR wrong number of arguments for 'cluster|count-failure-reports' command
> CLUSTER COUNTKEYSINSLOT
(error) ERR wrong number of arguments for 'cluster|countkeysinslot' command
> CLUSTER COUNTKEYSINSLOT a a
(error) ERR wrong number of arguments for 'cluster|countkeysinslot' command
> CLUSTER DELSLOTS
(error) ERR wrong number of arguments for 'cluster|delslots' command
> CLUSTER DELSLOTSRANGE a
(error) ERR wrong number of arguments for 'cluster|delslotsrange' command
> CLUSTER FLUSHSLOT
(error) ERR wrong number of arguments for 'cluster|flushslot' command
> CLUSTER FLUSHSLOTS a
(error) ERR wrong number of arguments for 'cluster|flushslots' command
> CLUSTER FORGET
(error) ERR wrong number of arguments for 'cluster|forget' command
> CLUSTER FORGET a a
(error) ERR wrong number of arguments for 'cluster|forget' command
> CLUSTER GETKEYSINSLOT a
(error) ERR wrong number of arguments for 'cluster|getkeysinslot' command
> CLUSTER GETKEYSINSLOT a a a
(error) ERR wrong number of arguments for 'cluster|getkeysinslot' command
> CLUSTER GETSLOTMIGRATIONS a
(error) ERR wrong number of arguments for 'cluster|getslotmigrations' command
> CLUSTER HELP a
(error) ERR wrong number of arguments for 'cluster|help' command
> CLUSTER INFO a
(error) ERR wrong number of arguments for 'cluster|info' command
> CLUSTER KEYSLOT
(error) ERR wrong number of arguments for 'cluster|keyslot' command
> CLUSTER KEYSLOT a a
(error) ERR wrong number of arguments for 'cluster|keyslot' command
> CLUSTER LINKS a
(error) ERR wrong number of arguments for 'cluster|links' command
> CLUSTER MEET a
(error) ERR wrong number of arguments for 'cluster|meet' command
> CLUSTER MIGRATESLOTS a
(error) ERR wrong number of arguments for 'cluster|migrateslots' command
> CLUSTER MYID a
(error) ERR wrong number of arguments for 'cluster|myid' command
> CLUSTER MYSHARDID a
(error) ERR wrong number of arguments for 'cluster|myshardid' command
> CLUSTER NODES a
(error) ERR wrong number of arguments for 'cluster|nodes' command
> CLUSTER REPLICAS
(error) ERR wrong number of arguments for 'cluster|replicas' command
> CLUSTER REPLICAS a a
(error) ERR wrong number of arguments for 'cluster|replicas' command
> CLUSTER REPLICATE
(error) ERR wrong number of arguments for 'cluster|replicate' command
> CLUSTER SAVECONFIG a
(error) ERR wrong number of arguments for 'cluster|saveconfig' command
> CLUSTER SET-CONFIG-EPOCH
(error) ERR wrong number of arguments for 'cluster|set-config-epoch' command
> CLUSTER SET-CONFIG-EPOCH a a
(error) ERR wrong number of arguments for 'cluster|set-config-epoch' command
> CLUSTER SETSLOT a
(error) ERR wrong number of arguments for 'cluster|setslot' command
> CLUSTER SHARDS a
(error) ERR wrong number of arguments for 'cluster|shards' command
> CLUSTER SLAVES
(error) ERR wrong number of arguments for 'cluster|slaves' command
> CLUSTER SLAVES a a
(error) ERR wrong number of arguments for 'cluster|slaves' command
> CLUSTER SLOT-STATS a
(error) ERR wrong number of arguments for 'cluster|slot-stats' command
> CLUSTER SLOTS a
(error) ERR wrong number of arguments for 'cluster|slots' command
> CLUSTER SYNCSLOTS
(error) ERR wrong number of arguments for 'cluster|syncslots' command
```

**CL-10** CLUSTERSCAN
```text
> CLUSTERSCAN
(error) ERR wrong number of arguments for 'clusterscan' command
> CLUSTERSCAN 0
1) "0-{06S}-0"
2) (empty array)
> CLUSTERSCAN 0 COUNT 10
1) "0-{06S}-0"
2) (empty array)
> CLUSTERSCAN 0 MATCH x
1) "0-{x}-0"
2) (empty array)
> CLUSTERSCAN 0 TYPE string
1) "0-{06S}-0"
2) (empty array)
> CLUSTERSCAN 0 COUNT 0
(error) ERR syntax error
> CLUSTERSCAN 0 FOO
(error) ERR syntax error
> CLUSTERSCAN abc
(error) ERR Invalid cursor
> CLUSTERSCAN 0-{06S}-0
1) "0"
2) (empty array)
```

**CL-11** READONLY, READWRITE and ASKING in cluster mode
```text
> READONLY
OK
> READWRITE
OK
> ASKING
OK
> READONLY
OK
> ASKING
OK
> GET a
(nil)
> READWRITE
OK
> MULTI
OK
> ASKING
QUEUED
> READONLY
QUEUED
> EXEC
1) OK
2) OK
```


---

## 9. Cluster-mode command rules (port 7403)

### 9.1 The rule (from source `getNodeByQuery`, confirmed by CS-01..CS-12)

Before a command runs, the slots of all its keys (from the command's key specs, after the hash-tag rule) are compared. If they differ the reply is E_CROSSSLOT; if the slot is unserved E_CLUSTERDOWN_UNBOUND; if the cluster is down E_CLUSTERDOWN; on a one-node cluster owning everything nothing else can happen. The check is on **keys**, not on every argument: `SET a 1`, `GET {t}x`, `BITOP AND dest a b` (all three are keys), `EVAL script 2 a b` (`numkeys` keys), `SORT k STORE d` (`d` is a key), `ZUNIONSTORE d 2 a b`, `SINTERCARD 2 a b`, `XREAD STREAMS a b 0 0`, `LMPOP`, `BLPOP`, `PFMERGE`, `LCS`, `WATCH`, `SSUBSCRIBE`, `SPUBLISH` are all slot-checked. Commands without keys (`PING`, `KEYS`, `SCAN`, `DBSIZE`, `RANDOMKEY`, `FLUSHALL`, `INFO`, `PUBLISH`, `PUBSUB ...`, `ECHO`) are never redirected and never cross-slot. `OBJECT FREQ a`/`ENCODING a` are just key commands (nil on a missing key).

Hash tags: only the first `{` and the first following `}` count, and only when the body is non-empty; same-tag keys share a slot (`{t}a`, `{t}b`).

### 9.2 Per command family (CS-01..CS-06)

- Multi-key string/key commands, set/list/zset/hll/geo/stream/bit commands, blocking commands: E_CROSSSLOT for different slots, normal behaviour with a shared tag (`RENAME {t}x {t}y` on a missing key is the normal `ERR no such key`). The cross-slot check happens before blocking, so `BLPOP a b 0.01` fails immediately (CS-03).
- `SORT`/`SORT_RO` (CS-04, from source `sort.c`): with `BY pattern` (other than `nosort`) or `GET pattern` (other than `#`), the pattern must contain a hash tag that puts it in the same slot as the sorted key, else `ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.` / `ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.`. The pattern's tag is the first `{`..`}` **before** the first `*` with a non-empty body (`{t}w*` yes, `{t}*` yes, `*{t}` no, `{}*` no, `{u}w*` for key `{t}k` no). `STORE dest` is a key (E_CROSSSLOT when `dest` hashes elsewhere). Even `SORT k GET #` and `BY nosort` pass.
- One database (CS-05): `cluster-databases` defaults to `1` (immutable); `SELECT n` for `n != 0` and negatives is E_DBRANGE (`SELECT 0` is fine, `SELECT abc` is E_INT); `MOVE key 1` is E_DBRANGE, `MOVE key 0` is `ERR source and destination objects are the same`; `SWAPDB a b` is **always** `ERR SWAPDB is not allowed in cluster mode` (even with non-numeric arguments); `COPY a b DB 1` is E_CROSSSLOT if the keys differ and E_DBRANGE otherwise, `COPY {t}a {t}b DB 0` works.
- Commands with no keys: all work (CS-06), `INFO keyspace` is just `# Keyspace\r\n` on an empty node, `DEBUG POPULATE` ignores slots (keys land in whatever slots).
- `PUBLISH` is cluster-wide and unrestricted (CS-10: `PUBLISH a x` replies `0`); `SPUBLISH ch msg` and `SSUBSCRIBE ch...` are slot-checked like keys (a subscription to channels in different slots is E_CROSSSLOT); `PUBSUB SHARDCHANNELS` and `SHARDNUMSUB` take no slot check (observed before subscribing: `PUBSUB SHARDNUMSUB a b` -> `a 0 b 0`). Once a connection has an active subscription only the pub/sub context commands are allowed on it (CS-10, CS-13).

### 9.3 MULTI, WATCH (CS-07..CS-09)

- Each queued command is slot-checked **when queued**: a cross-slot single command (`MSET a 1 b 2`) is refused immediately with E_CROSSSLOT and the transaction is flagged, so `EXEC` returns E_EXECABORT and nothing runs (CS-08).
- Commands queued without error but spanning different slots (`SET a 1`, `SET b 2`) are accepted by the queue (`QUEUED`) and `EXEC` then fails with E_CROSSSLOT and **discards** the transaction (nothing was executed: `GET a` is `(nil)`) (CS-07). Keyless commands (`PING`, `ECHO`) in the queue do not count toward the slot check. An empty `MULTI`/`EXEC` is `(empty array)`.
- `SELECT 1` and `SWAPDB` are queued and fail individually inside `EXEC` (CS-08: errors appear as elements of the `EXEC` array).
- `WATCH k1 k2` is a normal key command: E_CROSSSLOT for different slots; `WATCH` inside MULTI is E_NOTXN (CS-09).

### 9.4 Scripts and functions (CS-11, CS-12; legacy scripts are vectors, `#!lua` scripts are not)

- The declared `numkeys` keys of `EVAL`, `EVALSHA`, `EVAL_RO`, `FCALL`, `FCALL_RO` are slot-checked **before** the script (or function name) is looked up: `EVALSHA <unknown sha> 2 a b` is E_CROSSSLOT, `EVALSHA <unknown sha> 2 {t}a {t}b` is `NOSCRIPT No matching script.`; `FCALL nofunc 2 a b` is E_CROSSSLOT, with the same tag `ERR Function not found`. `numkeys` errors are unchanged: `ERR Number of keys can't be negative`, E_INT, `ERR Number of keys can't be greater than number of args`.
- **Legacy scripts** (no `#!` line) run in compatibility mode: `redis.call`/`pcall` may touch any key that this node owns, declared or not, in any slots (CS-12: `set a`, `set b` with `KEYS` = `a` works, two commands on different slots work, `select 1` is E_DBRANGE with the `script: on @user_script:1.` suffix, `swapdb` is `SWAPDB is not allowed in cluster mode`, `publish`/`spublish` of any channel work).
- **Scripts with a `#!lua` first line and functions** (observed with a raw client, because the script body contains a newline and cannot be a vector; `script` = `#!lua\n<body>`):

| Script body (numkeys, KEYS) | Result |
|---|---|
| `redis.call('set','a','1');redis.call('set','b','2');return(1)` (0) | `ERR Script attempted to access keys that do not hash to the same slot script: on @user_script:2.` (the check is per script: the second key is in another slot than the first) |
| `redis.call('set','a','1');redis.call('set','{a}b','2');return(1)` (0) | `(integer) 1` |
| `return(redis.call('set','b','v'))` (1, `a`) | the same cross-slot error: **the script's slot is fixed by the first key touched or the declared keys** (declared key `a` fixes the slot, so touching `b` fails); `set a` with KEYS `a` works |
| `return(redis.call('set','a','v'))` (0) and `return(redis.call('get','a'))` (0) | work (the first touched key fixes the slot) |
| `return(redis.call('ping'))` (0) | `PONG` (keyless commands are fine) |
| `redis.call('get','a');return(redis.call('get','b'))` (0) | cross-slot error with suffix; with `redis.pcall` the same error **without** the suffix |
| `return(pcall(redis.call,'get','a'))` (0) | `(integer) 1` (a caught error is true/false) |
| `#!lua flags=allow-cross-slot-keys` + two sets in different slots | `(integer) 1` (the flag lifts the check) |
| `return(1)` with 2 declared keys `a b` | E_CROSSSLOT (declared keys are checked first, before the script) |
| `return(redis.call('get','{t}c'))` with declared keys `{t}a {t}b` | works; `get 'c'` (other slot) is the cross-slot error |
| `return(redis.call('keys','*'))`, `dbsize`, `flushall`, `publish`, `spublish` (0 keys) | work; `spublish 'b'` with declared key `a` is the cross-slot error |
| `EVAL_RO` of a script without `flags=no-writes` | `ERR Can not execute a script with write flag using *_ro command.` |
| `FUNCTION LOAD` of `#!lua name=f1` with a function that sets `b`; `FCALL ff 0` | `OK`; `FCALL ff 1 a` and `FCALL ff 1 b`: cross-slot error for `a`, OK for `b`; a function registered with `flags={'allow-cross-slot-keys'}` can touch anything |

- **Reference bug: one cross-slot command inside a script poisons every later script call on that server** (observed on 9.1.3; the vectors deliberately avoid it). A multi-key command with keys in different slots issued **by `redis.call` inside a script**, such as `redis.call('mset','a','1','b','2')`, fails with E_NONLOCAL (the generic text used for every `getNodeByQuery` failure on this path). From then on **every** `redis.call`/`pcall` of **every** later script on that server process, even `redis.call('ping')`, fails with the same E_NONLOCAL error, on every connection, until the server is restarted. Plain (non-script) commands keep working. The cause is in `module.c` `VM_Call...` (`c->slot = clusterSlotByCommand(..., &c->read_flags)`): `read_flags` is OR-ed into a pooled temporary client and never reset, so a sticky `READ_FLAGS_CROSSSLOT` is seen by all later calls (inferred from source and from this behaviour). Sequence (observed, fresh node):

```shape
EVAL return(redis.call('ping')) 0                         -> PONG
EVAL return(redis.call('mset','a','1','b','2')) 0         -> (error) ERR Attempted to access a non local key in a cluster node script: on @user_script:1.
EVAL return(redis.call('ping')) 0                         -> (error) ERR Attempted to access a non local key in a cluster node script: on @user_script:1.
EVAL return(redis.call('set','a','1')) 0                  -> (error) the same
(new connection) EVAL return(redis.call('ping')) 0        -> (error) the same
SET a 1                                                   -> OK   (non-script commands still work)
```

  A reimplementation should **not** copy this: tests must avoid cross-slot commands inside scripts (or run them last on a node that is restarted afterwards). The same E_NONLOCAL appears legitimately when a script touches a key of an **unserved** slot (8.7) and for any cross-slot multi-key command run by a script.

### 9.5 Vectors

**CS-01** multi-key string and key commands: cross-slot is rejected, a shared hash tag is accepted
```text
> MSET a 1 b 2
(error) CROSSSLOT Keys in request don't hash to the same slot
> MSET {t}a 1 {t}b 2
OK
> MGET a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> MGET {t}a {t}b
1) "1"
2) "2"
> MSETNX a 1 b 2
(error) CROSSSLOT Keys in request don't hash to the same slot
> MSETNX {t}c 1 {t}d 2
(integer) 1
> DEL a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> DEL {t}a {t}b
(integer) 2
> EXISTS a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> EXISTS {t}c {t}d
(integer) 2
> UNLINK a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> TOUCH a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> RENAME a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> RENAME {t}x {t}y
(error) ERR no such key
> RENAMENX a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> COPY a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> COPY {t}x {t}y
(integer) 0
> BITOP AND d a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> BITOP AND {t}d {t}c {t}e
(integer) 1
> GETDEL a
(nil)
> LCS a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> LCS {t}a {t}b
""
```

**CS-02** multi-key collection commands
```text
> SUNIONSTORE d1 s1 s2
(error) CROSSSLOT Keys in request don't hash to the same slot
> SUNIONSTORE {t}d {t}s1 {t}s2
(integer) 0
> SUNION a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> SINTER a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> SDIFF a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> SINTERCARD 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> SINTERCARD 2 {t}a {t}b
(integer) 0
> SMOVE s1 s2 x
(error) CROSSSLOT Keys in request don't hash to the same slot
> SMOVE {t}s1 {t}s2 x
(integer) 0
> LMOVE l1 l2 LEFT RIGHT
(error) CROSSSLOT Keys in request don't hash to the same slot
> RPOPLPUSH l1 l2
(error) CROSSSLOT Keys in request don't hash to the same slot
> LMPOP 2 a b LEFT
(error) CROSSSLOT Keys in request don't hash to the same slot
> ZMPOP 2 a b MIN
(error) CROSSSLOT Keys in request don't hash to the same slot
> ZUNIONSTORE d 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> ZINTER 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> ZINTER 2 {t}a {t}b
(empty array)
> ZDIFF 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> PFMERGE d {t}a
(error) CROSSSLOT Keys in request don't hash to the same slot
> PFCOUNT a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> PFCOUNT {t}a {t}b
(integer) 0
> GEOSEARCHSTORE d a FROMLONLAT 0 0 BYRADIUS 1 km
(error) CROSSSLOT Keys in request don't hash to the same slot
> XREAD STREAMS a b 0 0
(error) CROSSSLOT Keys in request don't hash to the same slot
> XREAD STREAMS {t}a {t}b 0 0
(nil)
> XREAD BLOCK 1 STREAMS a b 0 0
(error) CROSSSLOT Keys in request don't hash to the same slot
```

**CS-03** blocking commands with several keys
```text
> BLPOP a b 0.01
(error) CROSSSLOT Keys in request don't hash to the same slot
> BLPOP {t}a {t}b 0.01
(nil)
> BRPOPLPUSH a b 0.01
(error) CROSSSLOT Keys in request don't hash to the same slot
> BRPOPLPUSH {t}a {t}b 0.01
(nil)
> BLMOVE a b LEFT LEFT 0.01
(error) CROSSSLOT Keys in request don't hash to the same slot
> BLMPOP 0.01 2 a b LEFT
(error) CROSSSLOT Keys in request don't hash to the same slot
> BLMPOP 0.01 2 {t}a {t}b LEFT
(nil)
> BZMPOP 0.01 2 a b MIN
(error) CROSSSLOT Keys in request don't hash to the same slot
> BZPOPMIN a b 0.01
(error) CROSSSLOT Keys in request don't hash to the same slot
```

**CS-04** SORT: BY and GET patterns must stay in the key slot, STORE must too
```text
> SORT k BY w*
(error) ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.
> SORT k GET g*
(error) ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.
> SORT k GET #
(empty array)
> SORT k BY nosort
(empty array)
> SORT k BY nosort GET #
(empty array)
> SORT {t}k BY {t}w*
(empty array)
> SORT {t}k GET {t}g*
(empty array)
> SORT {t}k BY w*
(error) ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.
> SORT {t}k GET g*
(error) ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.
> SORT {t}k BY {u}w*
(error) ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.
> SORT {t}k BY {t}w* GET # GET {t}g*
(empty array)
> SORT {t}k STORE d
(error) CROSSSLOT Keys in request don't hash to the same slot
> SORT {t}k STORE {t}d
(integer) 0
> SORT k STORE k2
(error) CROSSSLOT Keys in request don't hash to the same slot
> SORT_RO k BY w*
(error) ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.
> SORT_RO {t}k BY {t}w*
(empty array)
> SORT {t}k BY {t}*
(empty array)
> SORT {t}k BY *{t}
(error) ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.
> SORT {t}k BY {t}
(empty array)
> SORT {t}k BY {}*
(error) ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.
```

**CS-05** one database only: SELECT, MOVE, SWAPDB, COPY ... DB
```text
> SELECT 0
OK
> SELECT 1
(error) ERR DB index is out of range
> SELECT abc
(error) ERR value is not an integer or out of range
> SELECT -1
(error) ERR DB index is out of range
> SELECT 16
(error) ERR DB index is out of range
> MOVE a 1
(error) ERR DB index is out of range
> MOVE a 0
(error) ERR source and destination objects are the same
> MOVE a abc
(error) ERR value is not an integer or out of range
> SWAPDB 0 1
(error) ERR SWAPDB is not allowed in cluster mode
> SWAPDB 0 0
(error) ERR SWAPDB is not allowed in cluster mode
> SWAPDB a b
(error) ERR SWAPDB is not allowed in cluster mode
> COPY a b DB 1
(error) CROSSSLOT Keys in request don't hash to the same slot
> COPY {t}a {t}b DB 1
(error) ERR DB index is out of range
> COPY {t}a {t}b DB 0
(integer) 0
> COPY {t}a {t}b DB abc
(error) ERR value is not an integer or out of range
> CONFIG GET cluster-databases
1) "cluster-databases"
2) "1"
> CONFIG GET databases
1) "databases"
2) "16"
> CONFIG SET cluster-databases 2
(error) ERR CONFIG SET failed (possibly related to argument 'cluster-databases') - can't set immutable config
> CONFIG SET databases 4
(error) ERR CONFIG SET failed (possibly related to argument 'databases') - can't set immutable config
```

**CS-06** commands without keys are served locally
```text
> PING
PONG
> DBSIZE
(integer) 0
> RANDOMKEY
(nil)
> KEYS *
(empty array)
> SCAN 0
1) "0"
2) (empty array)
> SCAN 0 MATCH {t}* COUNT 10
1) "0"
2) (empty array)
> SCAN 0 TYPE string
1) "0"
2) (empty array)
> FLUSHDB
OK
> FLUSHALL
OK
> OBJECT FREQ a
(nil)
> OBJECT ENCODING a
(nil)
> OBJECT IDLETIME a
(nil)
> INFO keyspace
"# Keyspace\r\n"
> ECHO hi
"hi"
> DEBUG POPULATE 3
OK
> DBSIZE
(integer) 3
> CLUSTER COUNTKEYSINSLOT 1
(integer) 0
> FLUSHALL
OK
```

**CS-07** MULTI: queued commands are slot-checked individually; EXEC fails when the queue spans slots
```text
> MULTI
OK
> SET a 1
QUEUED
> SET b 2
QUEUED
> GET a
QUEUED
> EXEC
(error) CROSSSLOT Keys in request don't hash to the same slot
> GET a
(nil)
> GET b
(nil)
> MULTI
OK
> SET {t}a 1
QUEUED
> SET {t}b 2
QUEUED
> GET {t}a
QUEUED
> EXEC
1) OK
2) OK
3) "1"
> MULTI
OK
> PING
QUEUED
> SET a 1
QUEUED
> ECHO x
QUEUED
> EXEC
1) PONG
2) OK
3) "x"
> GET a
"1"
> MULTI
OK
> SET a 1
QUEUED
> PING
QUEUED
> SET b 1
QUEUED
> EXEC
(error) CROSSSLOT Keys in request don't hash to the same slot
> PING
PONG
```

**CS-08** MULTI: a cross-slot command is refused when queued and the transaction is aborted
```text
> MULTI
OK
> MSET a 1 b 2
(error) CROSSSLOT Keys in request don't hash to the same slot
> SET {t}x 1
QUEUED
> EXEC
(error) EXECABORT Transaction discarded because of previous errors.
> GET {t}x
(nil)
> MULTI
OK
> SET a 1
QUEUED
> MSET a 1 b 2
(error) CROSSSLOT Keys in request don't hash to the same slot
> EXEC
(error) EXECABORT Transaction discarded because of previous errors.
> MULTI
OK
> SET a 1
QUEUED
> DISCARD
OK
> GET a
(nil)
> MULTI
OK
> EXEC
(empty array)
> MULTI
OK
> SELECT 1
QUEUED
> EXEC
1) (error) ERR DB index is out of range
> MULTI
OK
> SWAPDB 0 1
QUEUED
> EXEC
1) (error) ERR SWAPDB is not allowed in cluster mode
```

**CS-09** WATCH
```text
> WATCH a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> WATCH {t}a {t}b
OK
> UNWATCH
OK
> WATCH a
OK
> WATCH b
OK
> UNWATCH
OK
> MULTI
OK
> WATCH a
(error) ERR Command 'watch' not allowed inside a transaction
> EXEC
(error) EXECABORT Transaction discarded because of previous errors.
```

**CS-10** PUBLISH is cluster-wide and unrestricted, SPUBLISH/SSUBSCRIBE are slot-checked
```text
> PUBLISH a x
(integer) 0
> PUBLISH {t}a x
(integer) 0
> SPUBLISH a x
(integer) 0
> SPUBLISH {t}a x
(integer) 0
> PUBSUB SHARDCHANNELS
(empty array)
> PUBSUB SHARDNUMSUB a b
1) "a"
2) (integer) 0
3) "b"
4) (integer) 0
> PUBSUB SHARDNUMSUB
(empty array)
> PUBSUB CHANNELS
(empty array)
> PUBSUB NUMSUB a b
1) "a"
2) (integer) 0
3) "b"
4) (integer) 0
> SSUBSCRIBE a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> SSUBSCRIBE {t}a {t}b
1) "ssubscribe"
2) "{t}a"
3) (integer) 1
1) "ssubscribe"
2) "{t}b"
3) (integer) 2
> PING
1) "pong"
2) ""
> GET a
(error) ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context
```

**CS-11** EVAL/EVALSHA/FCALL: declared keys must share a slot (checked before the script runs)
```text
> EVAL return(1) 0
(integer) 1
> EVAL return(1) 1 a
(integer) 1
> EVAL return(1) 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> EVAL return(1) 2 {t}a {t}b
(integer) 1
> EVAL return(1) 3 a b c
(error) CROSSSLOT Keys in request don't hash to the same slot
> EVALSHA ffffffffffffffffffffffffffffffffffffffff 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> EVALSHA ffffffffffffffffffffffffffffffffffffffff 2 {t}a {t}b
(error) NOSCRIPT No matching script.
> EVAL_RO return(1) 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> FCALL nofunc 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> FCALL nofunc 2 {t}a {t}b
(error) ERR Function not found
> FCALL_RO nofunc 2 a b
(error) CROSSSLOT Keys in request don't hash to the same slot
> EVAL return(1) -1 a
(error) ERR Number of keys can't be negative
> EVAL return(1) abc a
(error) ERR value is not an integer or out of range
> EVAL return(1) 2 a
(error) ERR Number of keys can't be greater than number of args
```

**CS-12** legacy scripts (no #! line) may touch any key of this node
```text
> EVAL return(redis.call('set','a','v')) 0
OK
> EVAL return(redis.call('set','b','v')) 1 a
OK
> EVAL redis.call('set','c','1');redis.call('set','d','2');return(1) 0
(integer) 1
> EVAL return(redis.call('get','a')) 0
"v"
> EVAL return(redis.call('get','d')) 2 {t}a {t}b
"2"
> EVAL return(redis.pcall('get','b')) 1 a
"v"
> EVAL return(redis.call('ping')) 0
PONG
> EVAL return(redis.call('dbsize')) 0
(integer) 4
> EVAL return(#redis.call('keys','*')) 0
(integer) 4
> EVAL return(redis.call('select',1)) 0
(error) ERR DB index is out of range script: on @user_script:1.
> EVAL return(redis.call('select',0)) 0
OK
> EVAL return(redis.call('publish','a','b')) 0
(integer) 0
> EVAL return(redis.call('spublish','a','b')) 0
(integer) 0
> EVAL return(redis.call('swapdb',0,1)) 0
(error) ERR SWAPDB is not allowed in cluster mode script: on @user_script:1.
> EVAL return(redis.call('move','a',1)) 0
(error) ERR DB index is out of range script: on @user_script:1.
> EVAL return(redis.call('flushall')) 0
OK
> DBSIZE
(integer) 0
```


Wire vectors (sharded pub/sub in cluster mode, error bytes, RESP2 and RESP3 identical except where shown):

**CS-13** SSUBSCRIBE and SPUBLISH in cluster mode (RESP2): channels are slot-checked like keys
```text
[A] > SSUBSCRIBE a
    <- '*3\r\n$10\r\nssubscribe\r\n$1\r\na\r\n:1\r\n'
[B] > SPUBLISH a hi
    <- ':1\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$1\r\na\r\n$2\r\nhi\r\n'
[B] > SPUBLISH {t}a hi
    <- ':0\r\n'
[A] > SSUBSCRIBE {t}a {t}b
    <- '*3\r\n$10\r\nssubscribe\r\n$4\r\n{t}a\r\n:2\r\n*3\r\n$10\r\nssubscribe\r\n$4\r\n{t}b\r\n:3\r\n'
[B] > SPUBLISH {t}b hi
    <- ':1\r\n'
[A] (async) <- '*3\r\n$8\r\nsmessage\r\n$4\r\n{t}b\r\n$2\r\nhi\r\n'
[B] > SSUBSCRIBE a b
    <- "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
[B] > SSUBSCRIBE {t}x {t}y
    <- '*3\r\n$10\r\nssubscribe\r\n$4\r\n{t}x\r\n:1\r\n*3\r\n$10\r\nssubscribe\r\n$4\r\n{t}y\r\n:2\r\n'
[B] > PUBSUB SHARDNUMSUB a
    <- "-ERR Can't execute 'pubsub|shardnumsub': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"
```

**CS-14** SSUBSCRIBE and SPUBLISH in cluster mode (RESP3)
```text
[A3] > SSUBSCRIBE a
    <- '>3\r\n$10\r\nssubscribe\r\n$1\r\na\r\n:1\r\n'
[B3] > SPUBLISH a hi
    <- ':1\r\n'
[A3] (async) <- '>3\r\n$8\r\nsmessage\r\n$1\r\na\r\n$2\r\nhi\r\n'
[A3] > SSUBSCRIBE a b
    <- "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
```

**CS-15** error and reply bytes in cluster mode (RESP2 and RESP3)
```text
[A] > MSET a 1 b 2
    <- "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
[B3] > MSET a 1 b 2
    <- "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
[A] > EVAL return(1) 2 a b
    <- "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
[A] > SSUBSCRIBE a b
    <- "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
[A] > ASKING
    <- '+OK\r\n'
[B3] > READONLY
    <- '+OK\r\n'
[A] > CLUSTER FOO
    <- "-ERR unknown subcommand 'FOO'. Try CLUSTER HELP.\r\n"
[B3] > CLUSTER KEYSLOT foo
    <- ':12182\r\n'
[A] > CLUSTER COUNTKEYSINSLOT 1
    <- ':0\r\n'
[A] > CLUSTER LINKS
    <- '*0\r\n'
[B3] > CLUSTER LINKS
    <- '*0\r\n'
[A] > CLUSTER GETKEYSINSLOT 0 1
    <- '*0\r\n'
[A] > SELECT 1
    <- '-ERR DB index is out of range\r\n'
[A] > MULTI
    <- '+OK\r\n'
[A] > MSET a 1 b 2
    <- "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
[A] > EXEC
    <- '-EXECABORT Transaction discarded because of previous errors.\r\n'
```


---

## 10. Wire-format summary (RESP2 vs RESP3)

| Reply | RESP2 | RESP3 |
|---|---|---|
| `ROLE`, `COMMANDLOG GET`, `SLOWLOG GET` | arrays of integers/bulks | identical |
| `LOLWUT` | bulk | verbatim string `=<n>\r\ntxt:...` |
| `CLUSTER INFO`, `NODES` | bulk | verbatim string `txt:` |
| `CLUSTER SLOTS` metadata, `SHARDS`, `SLOT-STATS` inner list | flat arrays | maps (`%`) |
| `CLUSTER SLOTS` null endpoint (`unknown-endpoint`) | `$-1` | `_` |
| `DEBUG PROTOCOL` | see 4.3 | see 4.3 |
| `SSUBSCRIBE`/`smessage` frames | `*3` arrays | `>3` pushes (CS-13, CS-14) |
| Errors, `+OK`, integers, `QUEUED` | identical | identical |

## 11. Notable quirks to reproduce (checklist)

1. `COMMANDLOG` pushes **after** execution: a `CONFIG SET` that lowers a threshold is itself logged; `LEN`/`GET` never include themselves; `value >= threshold` logs, threshold `-1` or max-len `0` disables.
2. Request size is the raw frame size, every queued command in `MULTI/EXEC` gets the **EXEC** frame size, script-run commands get size 0 and the name as written; rewritten commands log the original arguments and size.
3. Argument trimming: 32 arguments (the last slot becomes `... (N more arguments)`, N counts the replaced one) and 128 bytes per argument (`... (M more bytes)` appended).
4. `COMMANDLOG GET` checks the **count** before the **type**; ids never reset; `slowlog-*` configs and `SLOWLOG` are aliases of the slow log.
5. Redaction replaces `AUTH` args, `HELLO ... AUTH` credentials and `ACL SETUSER` arguments by `(redacted)`.
6. `DEBUG` on a server without `enable-debug-command` is refused after the arity check, also for `DEBUG HELP`; wrong argument count and unknown subcommand share one error; `DEBUG ERROR x` returns the raw `-x` with no `ERR`.
7. `DEBUG PROTOCOL push` in RESP3 sends the real reply first and the push frame after it.
8. `LOLWUT VERSION n` only counts when it has a number after it; version string `"%u.0.0"` of an `unsigned int`; `LOLWUT VERSION` alone is the syntax error of LOLWUT 9. The unstable reply has no space in `Valkey ver.9.1.3`.
9. `SHUTDOWN` success closes the connection without a reply; a failed save keeps the server alive and replies `Errors trying to SHUTDOWN. Check logs.`; `NOSAVE` or `FORCE` still exit; `ABORT` must be alone; `SAVE`+`NOSAVE` is a syntax error; it is refused inside MULTI and scripts.
10. On a standalone server `READONLY`/`READWRITE` succeed and only `ASKING` and the CLUSTER subcommands report `This instance has cluster support disabled`; `CLUSTER KEYSLOT` works and arity is checked before the disabled error.
11. A fresh one-node cluster has an **empty** node IP (`:7403@17403`, `""` in SLOTS/SHARDS); `CLUSTER MYSHARDID` is not the node id; `CLUSTER CANCELSLOTMIGRATIONS` takes no argument.
12. Cross-slot is a **key** property: keyless commands never fail; `EVAL`/`FCALL` check declared keys before the script lookup; `MULTI` checks at queue time per command and again at `EXEC` for the whole transaction; `SWAPDB` is always refused; `SELECT`/`MOVE`/`COPY DB` fail for any non-zero database (`cluster-databases 1`); `SORT BY/GET` need a hash-tagged pattern.
13. Legacy scripts may touch any key of the node; `#!lua` scripts and functions are limited to one slot unless `allow-cross-slot-keys`; the first cross-slot **command** run by a script poisons all later script calls until restart (do not reproduce).
