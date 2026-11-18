<p align="center">
  <img src="docs/assets/hotstash-logo.png" alt="Hotstash logo" width="128">
</p>

# Hotstash

Hotstash is an in-memory cache written in Rust that speaks the Redis and Valkey protocol (RESP2 and RESP3) and can also be embedded in a Rust program as a library. It runs as a TCP server that existing Redis and Valkey clients can talk to, and as an in-process engine with the same command set.

**Status: early prototype.** This is phase-0 spike code, written to measure and to decide, not a finished product. It is not production-ready. On-disk formats, command behaviour and the crate APIs may change without notice.

Hotstash was called HCache until 2026-11-17. The name was shared with too many other projects.

Tiếng Việt: [README.vi.md](README.vi.md)

## Quick start with Docker

Images for linux/amd64 and linux/arm64 are built from the `main` branch and published to GitHub Container Registry:

```bash
docker run -d --name hotstash -p 6379:6379 -v hotstash-data:/data \
  -e HOTSTASH_REQUIREPASS='choose-a-password' ghcr.io/ckem1st997/hotstash:latest
```

Then connect with any Redis or Valkey client, for example `valkey-cli -a 'choose-a-password' PING`.

- The container listens on all interfaces, so it refuses to start without a password.
- Data is kept in the `/data` volume. `docker stop` writes every change and exits cleanly.
- Settings go in environment variables, for example `-e HOTSTASH_WORKERS=4`; see [Configuration](#configuration). Prometheus metrics: `-p 9121:9121 -e HOTSTASH_METRICS_PORT=9121`.
- Tags: `latest` and `main` follow the main branch, `sha-<commit>` pins one build. There are no releases yet.
- On Kubernetes, the Helm chart in `deploy/helm/hotstash` uses this image.

## Configuration

Every setting can be given as a command-line flag or as an environment variable: `HOTSTASH_` plus the flag name in capitals, with dashes turned into underscores. When both are set, the command line wins. A bad value or an unknown flag stops the server from starting.

| Environment variable | Flag | Default | What it does |
|---|---|---|---|
| `HOTSTASH_REQUIREPASS` | `--requirepass` | none | Password of the `default` user. Prefer the variable: the server removes it from its own environment after reading it, while a command line is visible to other users of the machine. |
| `HOTSTASH_BIND` | `--bind` | `127.0.0.1` | Address to listen on. |
| `HOTSTASH_PORT` | `--port` | `6379` | RESP port. `0` together with a TLS port means TLS only. |
| `HOTSTASH_PROTECTED_MODE` | `--protected-mode` | `yes` | `yes` refuses to listen beyond loopback without a password. |
| `HOTSTASH_WORKERS` | `--workers` | number of CPUs | Threads serving connections. `1` runs every command on one thread. |
| `HOTSTASH_SHARDS` | `--shards` | `64` | Shards per database. |
| `HOTSTASH_DATABASES` | `--databases` | `16` | Number of databases for `SELECT`. |
| `HOTSTASH_HASH_TAGS` | `--hash-tags` (no value) | `no` | `yes` (or the flag) places `a{tag}b` by `tag`, as Redis Cluster does. |
| `HOTSTASH_MAXMEMORY` | `--maxmemory` | `auto` | Memory limit: bytes, or `256mb`, `1gb`. `auto`: under a container memory limit, leave a quarter of it (at least 128 MiB, at most half) to the rest of the process; with no limit, none. |
| `HOTSTASH_MAXMEMORY_POLICY` | `--maxmemory-policy` | `noeviction` | `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`. |
| `HOTSTASH_DATA` | `--data` | none | Data file. Without it, nothing survives a restart. |
| `HOTSTASH_APPENDFSYNC` | `--appendfsync` | `everysec` | `always` answers a write only once it is on disk; `everysec` syncs every `--sync-interval-ms`; `no` leaves it to the system. |
| `HOTSTASH_SYNC_INTERVAL_MS` | `--sync-interval-ms` | `100` | How often `everysec` syncs, in milliseconds. |
| `HOTSTASH_MEMORY_DBS` | `--memory-dbs` | none | Databases kept out of the data file, for example `0,2`. |
| `HOTSTASH_DIR` | `--dir` | none | Folder of the RDB file that `SAVE` and `BGSAVE` write and a start with empty databases loads. |
| `HOTSTASH_DBFILENAME` | `--dbfilename` | `dump.rdb` | Name of the RDB file. |
| `HOTSTASH_TIMEOUT` | `--timeout` | none | Close clients idle for this many seconds. |
| `HOTSTASH_THP` | `--thp` | `off` | `off` turns transparent huge pages off for the process; `inherit` keeps the system's setting. |
| `HOTSTASH_TLS_PORT` | `--tls-port` | none | TLS port. |
| `HOTSTASH_TLS_CERT_FILE` | `--tls-cert-file` | none | Server certificate (PEM). |
| `HOTSTASH_TLS_KEY_FILE` | `--tls-key-file` | none | Server private key (PEM). |
| `HOTSTASH_TLS_CA_CERT_FILE` | `--tls-ca-cert-file` | none | CA that client certificates must chain to. |
| `HOTSTASH_TLS_AUTH_CLIENTS` | `--tls-auth-clients` | `yes` when a CA is given | `yes`, `optional` or `no`. |
| `HOTSTASH_MEMCACHED_PORT` | `--memcached-port` | off | Port for the Memcached text protocol (database 0). |
| `HOTSTASH_METRICS_PORT` | `--metrics-port` | off | Port for Prometheus `GET /metrics`. |
| `HOTSTASH_CLUSTER_ENABLED` | `--cluster-enabled` | `no` | `yes` answers as a one-node Redis Cluster. |
| `HOTSTASH_CLUSTER_DATABASES` | `--cluster-databases` | `1` | Databases in cluster mode. |
| `HOTSTASH_ENABLE_DEBUG_COMMAND` | `--enable-debug-command` | `no` | `no`, `yes`, or `local` (only connections from this machine). |

Any setting that `CONFIG SET` accepts can be given at start the same way, for example `HOTSTASH_NOTIFY_KEYSPACE_EVENTS=KEA`, `HOTSTASH_SLOWLOG_MAX_LEN=512` or `HOTSTASH_LAZYFREE_LAZY_USER_DEL=no`.

In the Docker image the command line already sets `--bind 0.0.0.0 --data /data/hotstash.aof`, so `HOTSTASH_BIND` and `HOTSTASH_DATA` have no effect there unless you replace the command. Variables that Kubernetes or Docker links create for a service named `hotstash` (`HOTSTASH_SERVICE_*`, `HOTSTASH_PORT_*`, `HOTSTASH_PORT=tcp://...`) are ignored.

## Features

What works today (each item is covered by tests in this repository; see [Known limitations](#known-limitations) for what does not):

- **Commands.** About 228 commands (per `COMMAND COUNT`): strings, bitmaps, GEO, hashes (including per-field expiry), HyperLogLog, lists, sets, sorted sets, streams with consumer groups, blocking commands, key and expiry commands, `MULTI`/`EXEC`/`WATCH`, Lua scripts (`EVAL`, `EVALSHA`, with `cjson`, `cmsgpack`, `struct`, `bit`) and Lua functions (`FUNCTION`, `FCALL`), pub/sub including sharded pub/sub, keyspace notifications, `DUMP`/`RESTORE`, `SORT`, `LCS`, `SLOWLOG`, `LATENCY`, `MONITOR`, and client-side caching (`CLIENT TRACKING`).
- **Valkey 9 commands.** `HGETDEL`, `HGETEX`, `HSETEX`, `DELIFEQ`, `MSETEX`, `SET ... IFEQ`, `SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, checked against recordings from Valkey 9.1.3.
- **Databases, auth, ACL.** 16 databases by default (`--databases N`, 1 to 256), `SWAPDB`, `AUTH`, and `ACL` users with command, key and channel rules. Users are stored in the data file when `--data` is used.
- **Eviction.** `--maxmemory` with eight policies, including `allkeys-lfu` and `volatile-lfu`. In containers, `--maxmemory auto` derives a limit from the cgroup memory limit.
- **Persistence.** A data file (`--data`) with `--appendfsync always|everysec|no`, `WAITAOF`, and memory-only databases (`--memory-dbs`).
- **RDB files.** `--dir` and `--dbfilename` select an RDB file. `SAVE` and `BGSAVE` write RDB 11 (as Redis 7.2) or RDB 80 (as Valkey 9) when hashes carry field expiry. The file is loaded at startup if the databases are empty. Redis up to 7.2 and all Valkey versions can read and write it; this was tested against Valkey 9.1.3 in both directions, and with randomly corrupted files.
- **Replication.** `REPLICAOF`/`SLAVEOF`, partial resynchronisation from a backlog, replicas of replicas, `WAIT`, `FAILOVER` (with `TO`, `TIMEOUT`, `FORCE`, `ABORT`), `ROLE`, and the `SYNC`/`PSYNC`/`REPLCONF` protocol side. Primary and replica can each be Hotstash or Valkey. On 2026-11-16 this was run against Valkey 9.1.3 in both directions, including with a password and with writes during the sync, and with `valkey-cli --rdb` and `valkey-cli --replica` against Hotstash.
- **Sentinel compatibility.** A real Valkey Sentinel 9.1.3 managed a group of Hotstash nodes through the full cycle (2026-11-17): detection, automatic failover when the primary dies, reconfiguring the old primary as a replica when it returns, `SENTINEL FAILOVER` and `SENTINEL FAILOVER ... COORDINATED`. The same scenario on Valkey nodes gave the same results and timings. Details and how to rerun: [docs/sentinel-trial.md](docs/sentinel-trial.md).
- **Memcached port.** `--memcached-port N` opens a second port speaking the Memcached text protocol (classic and meta commands), sharing data with database 0 of the RESP side. Replies are byte-identical to memcached 1.6.24 on 311 comparison requests (plus 27 login requests), and a 45-step pymemcache 4.0 scenario gave identical results on both.
- **Bloom and JSON.** `BF.*` and `JSON.*` are built in, with no module to load. They follow valkey-bloom 1.0.1 and valkey-json (same command names, replies and error strings), based on reading the modules' source and test suites; they have not been run against the real modules. JSON supports both legacy paths and JSONPath. Bloom and JSON keys are written to RDB under the valkey-bloom and valkey-json module types.
- **TLS.** `--tls-port` with PEM certificate and key, optional client certificates, TLS 1.2 and 1.3 (rustls with `ring`).
- **Prometheus and Helm.** `--metrics-port N` serves `GET /metrics` with `INFO` values under the names redis_exporter uses, with HTTP Basic auth when a password is set. `deploy/helm/hotstash` is a minimal Helm chart (StatefulSet, PVC, Service, password Secret, optional ServiceMonitor, NetworkPolicy, restricted `securityContext`). The chart has not been rendered or installed yet.
- **Containers.** A `Dockerfile` (distroless runtime image), `deploy/k8s.yaml`, a `healthcheck` subcommand, graceful shutdown on `SIGTERM`/`SIGINT`. CI builds the image for amd64 and arm64 and checks that each one starts, stores a key and stops cleanly before publishing it. The Kubernetes files have not been run yet.
- **Single-node cluster mode.** `CLUSTER` info commands, `READONLY`, `READWRITE`, `ASKING` with `--cluster-enabled yes`. Multi-node cluster is only a design document.

## Crates

| Crate | Contents |
|---|---|
| `hotstash-core` | RESP parser, command table, shards, execution gate, engine, Lua |
| `hotstash-server` | TCP server: `mio` event loop, a separate thread for exclusive work |
| `hotstash` | Embedded library: `Db::open`, `Session::execute`, `Db::listen` |
| `hload` | Load generator for seven adverse workloads; speaks plain RESP, so it also works against Valkey |
| `spikeb` | Spike B (two hash table prototypes, measuring bytes per key at 10 million keys); results in [crates/spikeb/RESULTS.md](crates/spikeb/RESULTS.md) |

The project also contains two experiments: Spike A is a minimal RESP server built on six architecture rules (below), with the embedded library and `hload`; Spike B is the hash table comparison above.

The six rules and where they live in the code:

1. The event loop never waits for a lock. It uses `try_lock`; if the lock is busy the command joins the shard queue and the connection is parked (`engine.rs`, `submit`).
2. The lock holder processes a bounded batch of the queue before releasing it (`drain_batch`, default 32).
3. Connection-level commands run before any lock (`PING`, `INFO`, `SCRIPT KILL`, ...), so they still answer while a script holds the whole node.
4. Long scans release the lock periodically (`KEYS`, `HGETALL`: every `scan_chunk` items, default 256).
5. Pipelines are capped per turn (`pipeline_cap`, default 64).
6. Shard code is written as `&mut Shard` and knows nothing about locking.

Exclusive work (`EVAL`, `FLUSHALL`, escalated multi-key commands) goes through the execution gate on the write side and runs on its own thread, not on the event loop.

## Build and run

Run the commands below from the repository root. Hotstash builds with stable Rust on Linux and Windows. On Windows it is built and tested with the MSVC toolchain (add `+stable-x86_64-pc-windows-msvc` after `cargo` if that is not your default).

```bash
cargo build --release --workspace
```

Run the server (default `127.0.0.1:6379`, 64 shards):

```bash
target/release/hotstash-server --port 6400 --workers 4
```

Connect with any Redis or Valkey client, for example `valkey-cli -p 6400` or `redis-cli -p 6400`.

Main flags:

- `--port N`, `--workers N` (`--workers 1` runs all commands on one thread), `--databases N`.
- `--requirepass` is accepted, but other users on the machine can read the command line. Prefer the environment variable, which the server removes from its own environment right after reading it:

  ```bash
  HOTSTASH_REQUIREPASS='your-password' target/release/hotstash-server --port 6400
  ```

- `--data <file>` enables persistence; `--appendfsync always|everysec|no` sets durability (`everysec` is the default and syncs every `--sync-interval-ms`, default 100 ms):

  ```bash
  target/release/hotstash-server --port 6400 --data hotstash.hc --appendfsync always
  ```

- `--memory-dbs 0,2` keeps those databases out of the data file (in-memory cache next to persisted databases).
- `--maxmemory`, `--maxmemory-policy`, `--timeout <seconds>`, `--protected-mode yes|no` (default `yes`: the server refuses to start on a non-loopback address without a password).
- TLS:

  ```bash
  target/release/hotstash-server --port 6400 --tls-port 6401 --tls-cert-file server.crt --tls-key-file server.key
  ```

  Also `--tls-ca-cert-file`, `--tls-auth-clients optional|no`. `--port 0` with `--tls-port` listens on TLS only.
- `--memcached-port N`, `--metrics-port N`:

  ```bash
  target/release/hotstash-server --port 6400 --memcached-port 11211
  target/release/hotstash-server --port 6400 --metrics-port 9121
  ```

- `--dir <directory>` and `--dbfilename <name>` for an RDB file (default name `dump.rdb`).
- `--cluster-enabled yes`, `--enable-debug-command`, `--hash-tags`, `--thp off|inherit`.

Every flag also has an environment variable: `HOTSTASH_` plus the flag name in upper case with dashes turned into underscores (`HOTSTASH_WORKERS=4` is `--workers 4`). The command line wins when both are set. Any setting that `CONFIG SET` accepts can also be given at startup as `--name value` or `HOTSTASH_NAME=value`. A bad value or an unknown flag stops the server from starting.

Health check, for images without a shell: `hotstash-server healthcheck [--port N]` exits 0 if the local server answers `PING`. Without `--port` it asks the port in `HOTSTASH_PORT`, else 6379.

Docker image of your own (CI publishes one, see [Quick start with Docker](#quick-start-with-docker)):

```bash
docker build -t hotstash .
```

PGO build (Linux, needs the rustup `llvm-tools` component), output in `target/pgo/hotstash-server`:

```bash
bash bench/pgo_build.sh
```

Load generator (run `hload` with no arguments for all options):

```bash
target/release/hload baseline --addr 127.0.0.1:6400 --clients 16 --secs 5
```

Spike B, all 16 configurations at 10 million keys (takes a few minutes, needs about 2.5 GB of RAM):

```bash
target/release/spikeb --all
```

## Tests

```bash
cargo test --workspace --no-fail-fast
cargo clippy --workspace --all-targets -- -D warnings
```

As of 2026-11-18 the suite has 1055 tests (plus 4 that are ignored by default). All 1055 pass on Windows and on Linux (WSL2). `clippy -D warnings` and `rustfmt` are clean. The WSL2 wall clock sometimes jumps by more than 10 seconds, and a test that compares TTLs (from the wall clock) with a monotonic clock can then fail once; such a failure has always passed on rerun.

The tests are written to find bugs: comparison against reference models with random data, packets cut at every position, corrupt input, many threads racing, scripts that never end, and shutting the server down mid-operation. Some suites compare against recordings from Valkey 9.1.3 (`valkey9.rs`), and `rdb.rs` writes a file for Valkey 9.1.3 to read and reads back one Valkey wrote. Test files are under `crates/*/tests/`.

A differential fuzzer sent the same random commands to Hotstash and to Valkey 9.1.3 and compared every reply. It covered strings, bits, hashes, lists, sets, sorted sets, HyperLogLog, `SORT` and `LCS`, with values chosen to hit edge cases (`-0`, `nan`, `1e300`, hex, empty strings). Its first run found 20 kinds of difference. Each is fixed and has a test made from Valkey's replies. Eight seeds, 288,000 commands in all, now show none. The fuzzer is not in the repository yet.

### Tests with real client libraries

`clients/` holds small projects that each run the same 38 to 43 checks through a popular Redis client library: strings, hashes, lists, sets, sorted sets, transactions, expiry, `SCAN`, scripts, pub/sub. Start the server on port 6400 first. `HOTSTASH_PORT` changes the port; `HOTSTASH_PROTO=2` or `3` forces the protocol (empty lets the library choose). The Python suite can also run over TLS: set `HOTSTASH_TLS_CA` to the CA certificate path and `HOTSTASH_PORT` to the TLS port. The test certificates in `crates/hotstash/tests/tls/` are self-signed and for testing only.

| Library | Directory | Command (run inside it) | RESP2 | RESP3 |
|---|---|---|---|---|
| Python, redis-py | `clients/python` | `PYTHONIOENCODING=utf-8 python test_client.py` | 43/43 | 43/43 |
| Node, node-redis | `clients/node` | `node test_node_redis.mjs` | 39/39 | 39/39 |
| Node, ioredis | `clients/node` | `node test_ioredis.mjs` | 38/38 | 38/38 |
| Go, go-redis | `clients/go` | `go run .` | 39/39 | 39/39 |
| C#, StackExchange.Redis | `clients/dotnet` | `dotnet run -c Release` | 38/38 | 38/38 |
| Rust, redis-rs | `clients/rust` | `cargo run --release -q` | 38/38 | 38/38 |

Measured on 2026-11-16 against a release build running with `--appendfsync always`. The Python suite also passes 43/43 over TLS in both protocols; the other libraries have not been run over TLS. They have not been run against a Linux server either.

## Performance

All of this is a first look, not a benchmark result. Treat it as a signal, not a verdict.

**Setup.** 2026-11-16, WSL2 (Ubuntu 24.04, kernel 6.6, 8 vCPUs of an i7-9700K, 8 GB RAM). Valkey 9.1.3 built from source, with `--io-threads` equal to the core count; Hotstash with `--workers` equal to the core count. Both log with `appendfsync everysec`; Valkey syncs every second and Hotstash every 100 ms, so Hotstash runs under the stricter condition. The server is pinned to the first N vCPUs with `taskset`, and `hload` runs on the remaining ones. `hload baseline`: 64 connections, 10% writes, 100 000 keys, 64-byte values, 10 seconds per cell, each cell run 3 times on a fresh server. Values are median commands per second, p99 in brackets. Scripts: `bench/spike_a_wsl.sh`; raw numbers in `bench/results/`.

| Cores | Keys | Pipeline | Valkey 9.1.3 | Hotstash | Hotstash / Valkey |
|---|---|---|---|---|---|
| 1 | uniform | 1 | 47 557 (2.8 ms) | 42 618 (2.8 ms) | 0.90 |
| 1 | uniform | 16 | 568 323 (3.3 ms) | 422 085 (5.2 ms) | 0.74 |
| 1 | Zipf 0.99 | 1 | 51 604 (2.4 ms) | 44 433 (2.9 ms) | 0.86 |
| 1 | Zipf 0.99 | 16 | 607 774 (2.5 ms) | 473 240 (3.7 ms) | 0.78 |
| 2 | uniform | 1 | 56 350 (1.6 ms) | 91 976 (1.2 ms) | 1.63 |
| 2 | uniform | 16 | 771 242 (2.0 ms) | 856 323 (3.5 ms) | 1.11 |
| 2 | Zipf 0.99 | 1 | 56 697 (1.6 ms) | 93 660 (1.1 ms) | 1.65 |
| 2 | Zipf 0.99 | 16 | 769 904 (2.0 ms) | 927 206 (3.0 ms) | 1.20 |
| 4 | uniform | 1 | 400 870 (1.1 ms) | 487 454 (0.6 ms) | 1.22 |
| 4 | uniform | 16 | 1 893 018 (1.3 ms) | 1 871 438 (1.6 ms) | 0.99 |
| 4 | Zipf 0.99 | 1 | 432 561 (0.6 ms) | 538 364 (0.3 ms) | 1.24 |
| 4 | Zipf 0.99 | 16 | 1 992 056 (1.2 ms) | 2 356 069 (1.2 ms) | 1.18 |

The three runs of each cell are within 10% of each other, except one Valkey run at 1 core without pipelining.

- At one core Hotstash was slower than Valkey. With pipelining it reached 0.74 to 0.78 of Valkey, below the 0.9 threshold used as the target. This was the most trustworthy bad result of the round.
- At two and four cores Hotstash was faster, 1.1 to 1.65 times, except 4 cores with uniform keys and pipeline 16 (0.99).
- With pipelining, Hotstash's tail latency was worse: p99 higher than Valkey's at 1 and 2 cores.
- The non-pipelined numbers at 1 and 2 cores are limited by the virtual machine. Valkey on any 1 or 2 vCPUs gave about 57 500 commands per second and jumped to about 410 000 at 4 vCPUs, which points to Hyper-V vCPU wake-up cost rather than a server limit (`bench/vcpu_check.sh`).

**Caveats.** These runs do not answer the question the project set out to answer. That needs 8 and 16 cores, a separate load-generator machine, and Linux running directly on the hardware instead of in a virtual machine. This machine has only 8 vCPUs shared by server and client. The 1-core result is a warning sign: if it also comes out below 0.9 on rented hardware, Spike A misses the 1-core threshold.

**Why one core was slower, and what changed.** Profiling at one core showed the gap was not logging, system calls or the threading model. Most of it was time inside the program waiting on memory during key lookup (roughly four scattered memory regions per `GET`, against about two in Valkey). A sequence of optimisations followed: inline short keys, the mimalloc allocator, whole-program LTO, smaller command structs, batch prefetch, direct `GET` replies, a fast path for plain `GET`/`SET`, a tagged-bucket hash table, and an extra PGO build. Ratio Hotstash / Valkey 9.1.3, 1 vCPU, pipeline 16, 64 connections, 10% writes, 64-byte values, interleaved runs measured in the same round:

| Step | 1 000 keys | 100 000 keys | 1 000 000 keys |
|---|---|---|---|
| Before these changes (64-byte hash table entry) | 0.856 | 0.788 | 0.740 |
| Through the second round of changes, regular build | 1.005 | 0.994 | 0.935 |
| Same, PGO build (`bench/pgo_build.sh`) | 1.082 | 1.045 | 0.997 |

Ratios vary by a few percent between rounds; only compare numbers taken in the same round. At 100 000 keys the regular build reached 1.525 at 2 cores and 1.902 at 4 cores (PGO: 1.665 and 2.094). The `Dockerfile` builds with PGO; `--build-arg PGO=0` gives the regular build. Memory per key (1 million string keys, 64-byte values) was 160 bytes in Hotstash after the allocator change, against 125 in Valkey.

**Against Redis too.** 2026-11-17, Redis 8.10.2 and Valkey 9.1.3 built from source, the same settings as above (pipeline 16, 64 connections, 10% writes, 64-byte values), servers pinned with `taskset` and given `--io-threads` or `--workers` equal to the core count, none writing to disk, rounds interleaved with `bench/ab_cores.sh` (6 rounds at 1 core, 5 at 4; raw output in `bench/results/2026-11-17-wsl2-redis.txt`). Ratio to Valkey 9.1.3:

| Server | 1 core, 100 000 keys | 1 core, 1 000 000 keys | 4 cores, 100 000 keys | 4 cores, 1 000 000 keys |
|---|---|---|---|---|
| Valkey 9.1.3 | 1.000 | 1.000 | 1.000 | 1.000 |
| Redis 8.10.2 | 1.029 | 0.997 | 0.997 | 0.915 |
| Hotstash, regular build | 0.955 | 0.902 | 1.575 | 1.399 |
| Hotstash, PGO build | 1.066 | 1.013 | 1.854 | 1.769 |

At 1 core Valkey 9.1.3 did 625 377 commands per second at 100 000 keys; at 4 cores, 2 026 378. Redis stays within 3% of Valkey except at 4 cores with 1 000 000 keys. Hotstash runs commands on all its workers, while Redis and Valkey run them on one main thread and use the other threads for I/O; that is the likely reason for the gap at 4 cores. The standard deviation was under 3% of the mean except for Hotstash PGO at 4 cores, 100 000 keys (11%). Same machine and caveats as above: the load generator shares the 8 vCPUs.

### Write speed by fsync mode

2026-11-16, on the Windows development machine (NVMe Samsung 970 EVO Plus), `hload baseline --write-pct 100`, 5 seconds per cell, 100 000 keys, 64-byte values, no pipelining. The load generator ran on the same machine as the server, so absolute numbers are only for comparing modes with each other.

| Mode | 1 connection | 16 connections | 64 connections |
|---|---|---|---|
| No data file | 26 886 cmd/s, p99 0.08 ms | 69 862, p99 0.8 ms | 85 323, p99 4.5 ms |
| `no` | 26 979, p99 0.08 ms | 69 368, p99 1.0 ms | 84 580, p99 4.5 ms |
| `everysec` | 26 737, p99 0.09 ms | 70 498, p99 0.7 ms | 82 650, p99 3.5 ms |
| `always` | 651, p99 4.5 ms | 6 505, p99 5.5 ms | 22 591, p99 6.0 ms |

- `no` and `everysec` are no slower than running without a data file, within measurement error.
- `always` with one connection is about 40 times slower: every command waits for an `fsync` (about 1.4 ms). More connections share one `fsync`, so 64 connections reach about a quarter of the `everysec` speed.
- `always` with 16 connections and a 16-command pipeline reached 88 602 cmd/s (p99 6.6 ms). With 16 connections and 10% writes it reached 45 746 cmd/s; reads do not wait for the disk (p50 0.11 ms).

Each cell was measured once and not repeated, so run-to-run variation is unknown.

## Known limitations

This list is condensed; the full list of deviations from Redis is long.

- **Platforms.** Linux has only been run inside WSL2. The WSL wall clock sometimes jumps by more than 10 seconds, which makes TTL- and Sentinel-based tests flaky now and then. The client-library suites have not run on Linux.
- **Performance evidence.** There are no numbers yet from bare-metal Linux with 8 and 16 cores and a separate load generator; see [Performance](#performance).
- **Not implemented.** Multi-node cluster (only a design: [docs/cluster-design.md](docs/cluster-design.md)), Search, TimeSeries, probabilistic structures other than Bloom (`CF.*`, `CMS.*`, `TOPK.*`), an SSD tier, Memcached binary protocol, UDP and SASL, diskless replication as an option, `min-replicas-to-write`, `replica-announce-ip`, a configuration file (`CONFIG REWRITE` returns an error, as Valkey does without one).
- **Durability.** By default data is written every 100 ms, so a killed server loses up to that much; `--appendfsync always` loses no answered command at the cost of waiting for the disk. With `always`, other connections may see a change (or receive its pub/sub message) before it reaches disk. If an `fsync` fails, the waiting connection is closed without a reply and writes in `always` mode keep failing until the server can rewrite a whole new file (retried every second); Redis exits instead. Only process kills have been tested, not real power loss. A large collection is rewritten as a whole block on each change.
- **Security.** The non-TLS port is open by default and passwords cross it in clear text (use `--port 0` with `--tls-port`). The data file holds unsalted SHA-256 hashes of passwords, as Redis does, so keep it readable only by the server account. There is no limit on `AUTH` attempts. Worker threads do not catch panics. TLS certificates cannot be reloaded without a restart, there is no `tls-protocols` or `tls-ciphers`, and client certificates are checked against the CA only, not tied to an ACL user. The metrics port takes the password over unencrypted HTTP Basic.
- **ACL.** No selectors, no separate ACL file, `ACL LOG` is always empty.
- **Data types and commands.** `DUMP` writes RDB version 8; streams and hashes with field expiry use an Hotstash-specific format that Redis cannot read. HyperLogLog strings are byte for byte those of Valkey 9.1: sparse until a register passes 32 or the string passes 3000 bytes, with the count `PFCOUNT` caches. `PFDEBUG` and `PFSELFTEST` are missing. `MEMORY USAGE` is an estimate. `OBJECT IDLETIME` and `FREQ` always return 0, and LFU counters decay with lookups, not time. `LATENCY` returns static replies. `SCAN` and `HSCAN` return at most 256 elements per call. `WATCH` sometimes aborts `EXEC` although the watched key did not change. `INFO` reports `redis_version:7.4.0` for framework compatibility but lacks many sections. Keyspace notifications are not emitted for `FLUSHALL`/`FLUSHDB`.
- **Bloom, JSON and RDB.** `BF.*` and `JSON.*` were written from the modules' source and tests and not run against the real modules. A Valkey that reads Hotstash's RDB file or replicates from it must load valkey-bloom and valkey-json.
- **Lua and scripts.** `redis.call('FLUSHALL')` is rejected; a script that has written data cannot be stopped with `SCRIPT KILL` (as in Redis) and holds the whole node if it loops forever; a long-running C function inside Lua (for example `string.find` with a bad pattern) cannot be interrupted. Floats smaller than about 2e-16 become 0 through `cjson`, `cmsgpack` and `struct`.
- **Concurrency details.** `KEYS` and `HGETALL` on large data are not atomic: they release the lock between batches. `DEL` and `UNLINK` of a very large hash free memory under the shard lock, and the hash table rehashes all at once. The embedded `Session` takes locks directly instead of through the shard queue, and has no background expiry thread (expired keys are collected on access; the sweep thread only runs with `Db::listen`).
- **Memory ordering.** The execution gate omits a memory fence on x86. The argument holds for the x86 memory model but has not been checked with tools such as loom or miri.
- **Test flake.** One full run saw `endless_pipeline_does_not_starve_another_connection_on_the_same_worker` miss its 500 ms threshold once; it has not been reproduced.
- **Environment.** Not yet verified: `deploy/k8s.yaml` and the Helm chart (no `helm` or Kubernetes on the development machine).

## Documentation

The `docs/` folder holds design notes and behaviour specifications.

| File | Language | Contents |
|---|---|---|
| [docs/bloom-spec.md](docs/bloom-spec.md) | English | Behaviour spec for the `BF.*` commands, target valkey-bloom 1.0.1 |
| [docs/json-spec.md](docs/json-spec.md) | English | Behaviour spec for `JSON.*`, target valkey-json 1.1.0, with test vectors |
| [docs/json-path.md](docs/json-path.md) | Vietnamese | Supported JSONPath subset and differences from valkey-json |
| [docs/valkey9-wave1-spec.md](docs/valkey9-wave1-spec.md) | English | Valkey 9.1.3 behaviour spec for `HGETDEL`, `HGETEX`, `HSETEX`, `DELIFEQ`, `MSETEX`, sharded pub/sub |
| [docs/valkey9-wave1b-spec.md](docs/valkey9-wave1b-spec.md) | English | Valkey 9.1.3 behaviour spec for `ROLE`, `COMMANDLOG`, `DEBUG`, `SHUTDOWN`, `CLUSTER` family and cluster-mode rules |
| [docs/sentinel-spec.md](docs/sentinel-spec.md) | English | Implementation spec for Sentinel mode, referenced to Valkey 9.1.3 `sentinel.c` |
| [docs/sentinel-trial.md](docs/sentinel-trial.md) | Vietnamese | Results of running real Valkey Sentinel 9.1.3 against Hotstash nodes |
| [docs/cluster-design.md](docs/cluster-design.md) | Vietnamese | Design draft for multi-node cluster (no code yet) |

Some of these documents cite paths from the author's working environment for reference sources; those paths do not exist in this repository.

## License

Apache-2.0, see [LICENSE](LICENSE). Parts of this project are derived from Valkey, valkey-json, valkey-bloom and the `bloomfilter` crate, which are used under their BSD licences. See [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).
