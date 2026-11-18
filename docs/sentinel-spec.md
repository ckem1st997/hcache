# Hotstash sentinel mode: implementation spec (reference: Valkey 9.1.3 `src/sentinel.c`)

Reference tree: `valkey-9.1.3/src/`. Every `sentinel.c:N` reference below was checked against the file (5479 lines). Other files are cited as `server.c:N`, `networking.c:N`, `pubsub.c:N`, `replication.c:N`, `config.c:N`, `server.h:N`, `commands/<name>.json`.

Conventions:
- "primary" and "replica" are the internal names. The wire strings still use the legacy words: instance type strings are `master` / `slave` / `sentinel` (`sentinel.c:1427-1436`), event names say `slave`/`master`, and INFO fields are `master_*` / `slave_*`.
- Times are in milliseconds unless stated.
- Valkey-9-specific items are marked **[V9]**. Items whose introduction version cannot be read from the source are marked **[?ver]**.

---

## 1. Overview and instance model

### 1.1 Instances and flags

A sentinel keeps a dictionary `sentinel.primaries` (name to instance) (`sentinel.c:270`). Each primary owns two child dictionaries: `replicas` (key `ip:port`) and `sentinels` (key = sentinel runid) (`sentinel.c:228-229`, `1289-1303`). The three instance kinds are `SRI_PRIMARY`, `SRI_REPLICA`, `SRI_SENTINEL` (`sentinel.c:64-66`).

| Flag | Bit | Meaning | Line |
|---|---|---|---|
| `SRI_PRIMARY` / `SRI_REPLICA` / `SRI_SENTINEL` | 0/1/2 | kind | 64-66 |
| `SRI_S_DOWN` | 3 | subjectively down | 67 |
| `SRI_O_DOWN` | 4 | objectively down | 68 |
| `SRI_PRIMARY_DOWN` | 5 | set on a *sentinel* instance: that sentinel says the primary is down | 69 |
| `SRI_FAILOVER_IN_PROGRESS` | 6 | failover running for this primary | 71 |
| `SRI_PROMOTED` | 7 | replica selected for promotion | 73 |
| `SRI_RECONF_SENT` / `SRI_RECONF_INPROG` / `SRI_RECONF_DONE` | 8/9/10 | replica reconfiguration progress | 74-76 |
| `SRI_FORCE_FAILOVER` | 11 | `SENTINEL FAILOVER` without election | 77 |
| `SRI_SCRIPT_KILL_SENT` | 12 | `SCRIPT KILL` already sent after `-BUSY` | 78 |
| `SRI_PRIMARY_REBOOT` | 13 | primary seen rebooting (runid changed) | 79 |
| `SRI_COORD_FAILOVER` | 14 | coordinated failover **[V9]** | 80 |

Flag names in replies: `s_down o_down master slave sentinel disconnected master_down failover_in_progress promoted reconf_sent reconf_inprog reconf_done force_failover coordinated_failover script_kill_sent master_reboot` (`sentinel.c:3337-3352`, in exactly this order, comma-joined, `disconnected` derived from `link->disconnected`).

Instance creation (`sentinel.c:1270-1371`):
- Address is resolved first (`createSentinelAddr`, `sentinel.c:571-594`). Port must be 0..65535 (`sentinel.c:575`). Without `resolve-hostnames yes` only literal IPs are accepted (`ANET_IP_ONLY`, `sentinel.c:579`).
- A replica's name is `ip:port` (`[ip]:port` if the host contains `:`) (`sentinel.c:644-650`, `1289-1290`).
- A sentinel's name is its runid (`sentinel.c:2829`).
- Duplicates return `EBUSY`: "Duplicate master name." / "Duplicate hostname and port for replica." / "Duplicate runid for sentinel." (`sentinel.c:1304-1309`, `1709-1723`).
- `down_after_period` is inherited from the primary (`sentinel.c:1326`). Default `failover_timeout` is 180000 (`sentinel.c:1353`). `replica_priority` defaults to 100 and `replica_announced` to 1 (`sentinel.c:1331-1332`). `last_pub_time` is initialised to now, so the first hello goes out about 2 s after creation (`sentinel.c:1321`).

### 1.2 Links (connections)

Each instance has an `instanceLink` (`sentinel.c:164-189`) holding two async connections:
- `cc`: command connection, always used.
- `pc`: Pub/Sub connection, created **only for primaries and replicas** (`sentinel.c:2391`). Sentinel instances have `cc` only.

Sentinel-kind instances that represent the same physical sentinel under different primaries share one link (refcounted) (`sentinel.c:1097-1125`). The pending-command cap is scaled by refcount (`sentinel.c:3027`).

Reconnect logic is `sentinelReconnectInstance` (`sentinel.c:2342-2424`):
- Skipped when `link->disconnected == 0` or port is 0 (`sentinel.c:2343-2344`).
- Rate-limited: no attempt if `now - last_reconn_time < sentinel_ping_period` (1000) (`sentinel.c:2348-2349`).
- Connects with `valkeyAsyncConnectBind(ip, port, server.bind_source_addr)` (`sentinel.c:2365`, `2392`).
- With `resolve-hostnames yes` it re-resolves the hostname before each `cc` reconnect (`sentinel.c:2357-2363`).
- TLS is negotiated if `server.tls_replication` is set (`sentinel.c:2370`, `2396`, `2323-2337`).
- `link->disconnected` is cleared only when `cc` exists and (the instance is a sentinel or `pc` exists) (`sentinel.c:2423`).

### 1.3 What a sentinel sends right after connecting

There is no `HELLO`: the connection stays on RESP2 (`HELLO` is never sent; the only `valkeyAsyncCommand` format strings in the file are listed in section 4.5). Replies to all setup commands are discarded (`sentinelDiscardReplyCallback`, `sentinel.c:2709-2715`).

Command connection `cc`, in this order (`sentinel.c:2383-2387`):
1. `AUTH ...` if credentials apply (`sentinel.c:2270-2305`).
2. `CLIENT SETNAME sentinel-<first 8 chars of myid>-cmd` (`sentinel.c:2313-2321`, format `"sentinel-%.8s-%s"` at `sentinel.c:2316`).
3. `PING` (`sentinelSendPing`, `sentinel.c:2387`).

Pub/Sub connection `pc`, in this order (`sentinel.c:2408-2412`):
1. `AUTH ...` (same rule).
2. `CLIENT SETNAME sentinel-<myid8>-pubsub`.
3. `SUBSCRIBE __sentinel__:hello`. The callback is `sentinelReceiveHelloMessages`. If queuing fails the pc is closed (`sentinel.c:2411-2418`).

AUTH selection (`sentinel.c:2270-2305`):

| Target | Credentials | Wire form |
|---|---|---|
| primary | its `auth-pass` / `auth-user` | `AUTH <pass>` or `AUTH <user> <pass>` |
| replica | **the primary's** `auth-pass` / `auth-user` | same |
| sentinel | `sentinel-pass` / `sentinel-user`; if `sentinel-pass` is unset, the local `requirepass` (no user) | same |

The command names (`AUTH`, `CLIENT`, `PING`, `INFO`, `SLAVEOF`, `CONFIG`, `PUBLISH`, `SUBSCRIBE`, `SCRIPT`, `MULTI`, `EXEC`, `FAILOVER`, `SENTINEL`) go through `sentinelInstanceMapCommand`, which honours per-primary `rename-command` (replicas use their primary's table) (`sentinel.c:1697-1703`).

Link health callbacks: any connect error or disconnect sets the link to `disconnected` (`sentinel.c:1221-1244`).

---

## 2. Timers and constants

### 2.1 Constants

| Name (variable) | Default | Adjustable via | Line |
|---|---|---|---|
| `VALKEY_SENTINEL_PORT` | 26379 | config `port` | 52, 497 |
| `SENTINEL_PING_PERIOD` | 1000 | (const) | 84 |
| `sentinel_info_period` | 10000 | `SENTINEL DEBUG info-period` | 86 |
| `sentinel_ping_period` | 1000 | `SENTINEL DEBUG ping-period` | 87 |
| `sentinel_ask_period` | 1000 | `SENTINEL DEBUG ask-period` | 88 |
| `sentinel_publish_period` (hello period) | 2000 | `SENTINEL DEBUG publish-period` | 89 |
| `sentinel_default_down_after` | 30000 | `SENTINEL DEBUG default-down-after` | 90 |
| `sentinel_tilt_trigger` | 2000 | `SENTINEL DEBUG tilt-trigger` | 91 |
| `sentinel_tilt_period` | 30000 (`PING_PERIOD*30`) | `SENTINEL DEBUG tilt-period` | 92 |
| `sentinel_replica_reconf_timeout` | 10000 | `SENTINEL DEBUG slave-reconf-timeout` | 93 |
| `sentinel_min_link_reconnect_period` | 15000 | `SENTINEL DEBUG min-link-reconnect-period` | 94 |
| `sentinel_election_timeout` | 10000 | `SENTINEL DEBUG election-timeout` | 95 |
| `sentinel_script_max_runtime` | 60000 | `SENTINEL DEBUG script-max-runtime` | 96 |
| `sentinel_script_retry_delay` | 30000 | `SENTINEL DEBUG script-retry-delay` | 97 |
| `sentinel_default_failover_timeout` | 180000 (`60*3*1000`) | `SENTINEL DEBUG default-failover-timeout` | 98 |
| `SENTINEL_HELLO_CHANNEL` | `__sentinel__:hello` | | 100 |
| `SENTINEL_DEFAULT_REPLICA_PRIORITY` | 100 | | 101 |
| `SENTINEL_DEFAULT_PARALLEL_SYNCS` | 1 | | 102 |
| `SENTINEL_MAX_PENDING_COMMANDS` | 100 (times link refcount) | | 103, 3027 |
| `SENTINEL_MAX_DESYNC` | 1000 | | 105, 4654, 4946 |
| `SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG` | 1 | | 106 |
| `SENTINEL_DEFAULT_RESOLVE_HOSTNAMES` / `ANNOUNCE_HOSTNAMES` | 0 / 0 | | 107-108 |
| `SENTINEL_SCRIPT_MAX_QUEUE` / `MAX_RUNNING` / `MAX_RETRY` | 256 / 16 / 10 | | 133-135 |
| `SENTINEL_SCRIPT_MAX_ARGS` | 16 | | 755 |
| `CONFIG_RUN_ID_SIZE` (runid length) | 40 | | `server.h:153` |
| `CONFIG_DEFAULT_HZ` | 10 | | `server.h:139` |

The `SENTINEL DEBUG` option names are matched case-insensitively (`sentinel.c:3515-3630`). All take an integer greater than 0 (`sentinel.c:3518` and following).

### 2.2 Order of operations

The sentinel timer is called from `serverCron` on every cron tick (`server.c:1673`). After each tick `server.hz` is set to `10 + rand() % 10`, so ticks come every roughly 50-100 ms (`sentinel.c:5472-5478`).

`sentinelTimer` (`sentinel.c:5465-5479`):
1. `sentinelCheckTiltCondition()` (`sentinel.c:5452-5463`).
2. `sentinelHandleDictOfValkeyInstances(sentinel.primaries)` (`sentinel.c:5407-5431`). For every primary:
   - `sentinelHandleValkeyInstance(primary)`.
   - recurse into its `replicas`, then its `sentinels`.
   - if `failover_state == UPDATE_CONFIG`, call `sentinelFailoverSwitchToPromotedReplica` (`sentinel.c:5421-5427`).
3. `sentinelRunPendingScripts()`, `sentinelCollectTerminatedScripts()`, `sentinelKillTimedoutScripts()`.

`sentinelHandleValkeyInstance(ri)` (`sentinel.c:5377-5403`):
- Monitoring half, run in every mode including TILT:
  1. `sentinelReconnectInstance`
  2. `sentinelSendPeriodicCommands`
- Acting half:
  - If TILT: return unless `now - tilt_start_time >= sentinel_tilt_period`, in which case clear TILT and fire `-tilt` (`sentinel.c:5387-5391`). Note this exit check only runs when at least one instance exists.
  - `sentinelCheckSubjectivelyDown` (all kinds).
  - Primaries only, in this order: `sentinelCheckObjectivelyDown`; `sentinelStartFailoverIfNeeded` (if it returns 1, `sentinelAskPrimaryStateToOtherSentinels(FORCED)`); `sentinelFailoverStateMachine`; `sentinelAskPrimaryStateToOtherSentinels(NO_FLAGS)`.

### 2.3 Adaptive periodic-command rules (`sentinelSendPeriodicCommands`, `sentinel.c:3012-3072`)

Nothing is sent if the link is disconnected (`sentinel.c:3019`) or `pending_commands >= 100 * refcount` (`sentinel.c:3027`).

- **INFO period** (`sentinel.c:3037-3042`): `1000` if the instance is a replica AND (its primary has `O_DOWN` or `FAILOVER_IN_PROGRESS`, OR the replica's `primary_link_down_time != 0`). Otherwise `sentinel_info_period` (10000). Note this only shortens the period for the replicas of a down primary. **The primary itself keeps the 10 s period.**
- **PING period** (`sentinel.c:3047-3048`): `ping_period = min(down_after_period, sentinel_ping_period)`, so at most 1000.
- **INFO** is sent only to primaries and replicas, never to sentinels, when `info_refresh == 0 || now - info_refresh > info_period` (`sentinel.c:3051-3055`). INFO has no "already pending" check.
- **PING** is sent to all three kinds when `now - last_pong_time > ping_period && now - last_ping_time > ping_period/2` (`sentinel.c:3058-3060`). `act_ping_time` is only refreshed when the previous pong arrived (`sentinel.c:3003`).
- **PUBLISH hello** is sent to all three kinds when `now - last_pub_time > sentinel_publish_period`, unless (`sentinel.c:3063-3071`):
  - the instance has `SRI_COORD_FAILOVER` and `failover_state <= WAIT_PROMOTION`, or
  - the instance reports `master_failover_state` other than `no-failover` (`monitored_instance_failover_state == FAILOVER`). **[V9]**
- `last_pub_time` is updated only in the PUBLISH reply callback, and only if the reply is not an error (`sentinel.c:2764`). Until then the hello is re-sent on every timer tick (the code comment says "retry in 100 ms"). `sentinelForceHelloUpdateForPrimary` subtracts `publish_period+1` from `last_pub_time` on the primary, its replicas and its sentinels to force an immediate hello (`sentinel.c:2960-2987`).

Reply acceptance for PING (`sentinelPingReplyCallback`, `sentinel.c:2717-2749`):
- Only a status or error reply is examined (`sentinel.c:2726`). A reply counts as "available" if its text starts with `PONG`, `LOADING` or `MASTERDOWN` (`strncmp`, `sentinel.c:2729-2730`). Then `last_avail_time = now`, `act_ping_time = 0`.
- A `PONG` also clears `SRI_PRIMARY_REBOOT` (`sentinel.c:2734`).
- Any other reply: if it starts with `BUSY` and the instance is `S_DOWN` and `SCRIPT_KILL_SENT` is unset, send `SCRIPT KILL` and set the flag (`sentinel.c:2739-2745`).
- `last_pong_time = now` for every reply of any type (`sentinel.c:2748`).
- A bulk-string PONG would not count. Hotstash must answer PING with `+PONG` in RESP2.

### 2.4 TILT (`sentinel.c:5452-5463`)

On every timer tick `delta = now - previous_time`. If `delta < 0 || delta > sentinel_tilt_trigger` (2000), then `tilt = 1`, `tilt_start_time = now`, `total_tilt++`, and event `+tilt` with payload `#tilt mode entered` is fired. `previous_time` is updated at the end (`sentinel.c:5462`).

In TILT:
- Monitoring half still runs (pings, INFO, hello).
- Acting half is skipped (`sentinel.c:5387-5391`): no SDOWN/ODOWN evaluation, no failover logic.
- INFO parsing returns early after updating the role bookkeeping (`sentinel.c:2598`).
- `is-master-down-by-addr` always answers down=0 (`sentinel.c:4885`).
- TILT ends when `now - tilt_start_time >= sentinel_tilt_period` (30000), event `-tilt` with `#tilt mode exited` (`sentinel.c:5388-5390`).

---

## 3. The hello message on `__sentinel__:hello`

### 3.1 Payload

Channel: `__sentinel__:hello` (`sentinel.c:100`). Payload, built in `sentinelSendHello` (`sentinel.c:2944-2950`):

```
"%s,%d,%s,%llu,%s,%s,%d,%llu"
 sentinel_ip,sentinel_port,sentinel_runid,current_epoch,primary_name,primary_ip,primary_port,primary_config_epoch
```

Exactly 8 comma-separated fields, in this order (`sentinel.c:2773-2774`, `2912-2913`):

| # | Field | Source |
|---|---|---|
| 0 | sentinel ip | `announce-ip` if set (`sentinel.c:2930-2931`), else the local address of the `cc` socket obtained with `anetFdToString` (`sentinel.c:2933`); if that fails nothing is sent (`sentinel.c:2933`) |
| 1 | sentinel port | `announce-port` if set; else `server.tls_port` if `tls_replication && tls_port`; else `server.port` (`sentinel.c:2936-2941`) |
| 2 | sentinel runid | `sentinel.myid` (40 hex chars) |
| 3 | current epoch | `sentinel.current_epoch` |
| 4 | primary name | `primary->name` |
| 5 | primary ip | `announceSentinelAddr(sentinelGetCurrentPrimaryAddress(primary))`, which is the hostname if `announce-hostnames yes`, else the IP (`sentinel.c:637-639`) |
| 6 | primary port | same address object |
| 7 | primary config epoch | `primary->config_epoch` |

`sentinelGetCurrentPrimaryAddress` returns the **promoted replica's address** once a failover is in progress, a promoted replica exists, and `failover_state >= RECONF_REPLICAS`; otherwise the primary's own address (`sentinel.c:1658-1670`). This is how the new address is gossiped during the failover.

The buffer is `NET_IP_STR_LEN + 1024` (`sentinel.c:2919`). IPv6 addresses are sent bare (the format has no brackets), and commas never occur in them.

The command sent is `PUBLISH __sentinel__:hello <payload>` through the instance's `cc` (via the renamed-command map) (`sentinel.c:2951-2952`).

### 3.2 To whom it is sent

To **every** instance of the primary: the primary itself, each replica, and each other sentinel, every `publish_period` (2000), with the exceptions in 2.3 (`sentinel.c:3062-3071`).
- For primaries and replicas, the data node relays it to all subscribers of the channel, so every sentinel that subscribed (all sentinels monitoring that node, including the sender) receives it via its `pc`.
- For sentinels, the target's fake `PUBLISH` handler processes it directly (`sentinel.c:4431-4438`).
- The hello for a replica instance describes the replica's primary (`sentinel.c:2923`).

### 3.3 Receiving through Pub/Sub (`sentinelReceiveHelloMessages`, `sentinel.c:2878-2904`)

- Every reply on `pc` sets `link->pc_last_activity = now` (`sentinel.c:2889`), including the initial subscribe confirmation.
- The reply is processed only if it is an array (or RESP3 push) of exactly 3 string elements whose first is `message` (`sentinel.c:2895-2898`). The subscribe confirmation has an integer third element and is therefore ignored. **A data node must deliver `["message", "__sentinel__:hello", payload]` as 3 bulk strings.**
- Self-messages are dropped: if the payload contains `sentinel.myid` as a substring (`strstr`), it is ignored (`sentinel.c:2901`). This check applies only on the Pub/Sub path, not in the fake PUBLISH.

### 3.4 Processing (`sentinelProcessHelloMessage`, `sentinel.c:2772-2873`)

1. Split on `,`. If not exactly 8 tokens, silently ignored (`sentinel.c:2778-2781`).
2. Look up the primary by name (`token[4]`). Unknown name: drop (`sentinel.c:2783-2784`).
3. Look for an existing sentinel in `primary->sentinels` matching **both runid (`token[2]`) and address** (`token[0]:token[1]`) (`sentinel.c:2789`). Address equality is by port plus (IP equal or hostname equal case-insensitively) (`sentinel.c:617-619`).
4. If not found (`sentinel.c:2793-2841`):
   - Remove every sentinel entry with the same runid (address change). If any was removed, publish `+sentinel-address-switch` with format `"%@ ip %s port %d for %s"` args (`token[0]`, port, `token[2]`) on the primary (`sentinel.c:2797-2800`).
   - Otherwise, if a sentinel with the same address but a different runid exists, publish `+sentinel-invalid-addr` for it and remove that runid from **all** primaries (`sentinel.c:2806-2825`).
   - Create the sentinel instance (name = runid) (`sentinel.c:2829`). If creation succeeded: publish `+sentinel` (only when nothing was removed), set `runid`, try link sharing, and if it was an address switch call `sentinelUpdateSentinelAddressInAllPrimaries` (which closes the matching links and copies the new address, event `+sentinel-address-update`, `sentinel.c:1183-1213`). Then `sentinelFlushConfig()` (`sentinel.c:2831-2840`).
5. If `token[3] > sentinel.current_epoch`: adopt it, flush config, publish `+new-epoch <epoch>` (`sentinel.c:2844-2848`).
6. **Config-epoch rule** (`sentinel.c:2851-2865`), only if the sentinel entry exists and `primary->config_epoch < token[7]` (strictly greater):
   - `primary->config_epoch = token[7]`.
   - If the advertised address differs (port differs or `!sentinelAddrEqualsHostname`): publish `+config-update-from` (on the sentinel instance), then `+switch-master` with `"%s %s %d %s %d"` = (name, old ip, old port, new ip `token[5]`, new port `token[6]`), then `sentinelResetPrimaryAndChangeAddress` (flushes config), then the client-reconfig script with role `observer`.
   - If the address is the same, only the epoch is raised (no event, no flush at that point).
   - A hello with an equal or lower config epoch changes nothing. There is no other mismatch handling.
7. `si->last_hello_time = now` (`sentinel.c:2868`).

Discovery summary: sentinels discover one another **only** through hello messages (relayed through a shared data node or sent directly). There is no seed list besides `known-sentinel` lines in the config file (`sentinel.c:1920-1933`).

---

## 4. Commands exchanged

### 4.1 PING, INFO, PUBLISH
See 2.3 and 3. INFO is sent as the bare command `INFO` (no section argument) (`sentinel.c:3052-3053`) and the full reply is parsed. Only a bulk string reply (or RESP3 verbatim) is accepted (`sentinel.c:2704`).

### 4.2 `SENTINEL is-master-down-by-addr` (sentinel to sentinel)

Sent on the peer's `cc` (`sentinelAskPrimaryStateToOtherSentinels`, `sentinel.c:4581-4621`), exact format (`sentinel.c:4615`):

```
SENTINEL is-master-down-by-addr <primary-ip> <primary-port> <current-epoch> <runid-or-*>
```

- The subcommand name is always the legacy spelling, lowercase.
- `<primary-ip>` is `announceSentinelAddr(primary->addr)` (`sentinel.c:4616`).
- `<current-epoch>` is `sentinel.current_epoch` (`sentinel.c:4617`).
- The last argument is the sender's `myid` if `primary->failover_state > NONE` (this is a vote request), else `*` (`sentinel.c:4617`).

Conditions to send (`sentinel.c:4583-4611`):
- Only if the primary is `S_DOWN` (`sentinel.c:4583`).
- The peer link must be connected.
- Unless `SENTINEL_ASK_FORCED`, only if `now - last_primary_down_reply_time >= ask_period` (1000).
- Before that, stale state is cleared: if `now - last_primary_down_reply_time > ask_period * 5`, clear the peer's `SRI_PRIMARY_DOWN` flag and its stored `leader` (`sentinel.c:4598-4602`).

Receiver (`sentinelCommand`, `sentinel.c:3851-3899`):
- Exactly 6 args, `port` and `epoch` must be integers.
- Looks up the primary by address (`getSentinelValkeyInstanceByAddrAndRunID(sentinel.primaries, ip, port, NULL)`), not by name.
- `down = 1` iff not in TILT and the primary exists and is `S_DOWN` and is a primary (`sentinel.c:4885`).
- If the primary exists and the runid argument is not `*` (`strcasecmp`), call `sentinelVoteLeader` (section 7) (`sentinel.c:4889-4891`).

Reply, a 3-element array (`sentinel.c:4895-4898`):
1. integer `0`/`1` (down state, `shared.czero/cone`)
2. bulk string: the leader runid this sentinel voted for, or `*` if no vote info
3. integer: leader epoch (0 if none)

Sender parsing (`sentinelReceiveIsPrimaryDownReply`, `sentinel.c:4544-4575`): must be an array of 3 with types (integer, string, integer); otherwise ignored. Then:
- `last_primary_down_reply_time = now`.
- `SRI_PRIMARY_DOWN` set iff `element[0] == 1`.
- If `element[1] != "*"`: store it as that sentinel's `leader` and `element[2]` as `leader_epoch` (`sentinel.c:4564-4572`). Log line `"%s voted for %s %llu"` if the epoch changed.

There are no other "failover-related" sentinel to sentinel messages: election is carried entirely by this command (with a runid) and the config is carried by hello.

### 4.3 Commands sent to monitored data nodes

| Purpose | Exact command | Sent to | Line |
|---|---|---|---|
| Auth | `AUTH <pass>` / `AUTH <user> <pass>` | all | 2295, 2301 |
| Name connection | `CLIENT SETNAME sentinel-<id8>-cmd` / `-pubsub` | all | 2317 |
| Liveness | `PING` | all | 2996 |
| Info | `INFO` | primaries, replicas | 3052 |
| Hello | `PUBLISH __sentinel__:hello <payload>` | all | 2951 |
| Hello subscribe | `SUBSCRIBE __sentinel__:hello` | primaries, replicas (on `pc`) | 2411 |
| Unstick busy script | `SCRIPT KILL` | the instance, when it answers PING with `BUSY...` while `S_DOWN` | 2740 |
| Reconfigure / promote (see 4.4) | `MULTI`, optional `FAILOVER ABORT`, `SLAVEOF <host> <port>` (or `SLAVEOF NO ONE`), `CONFIG REWRITE`, `CLIENT KILL TYPE normal`, `CLIENT KILL TYPE pubsub`, `EXEC` | replica/promoted replica/old primary | 4895-4930 |
| Coordinated failover **[V9]** | `MULTI`, `CLIENT PAUSE <ms> WRITE`, `FAILOVER TO <host> <port> TIMEOUT <ms>`, `EXEC` | the old primary | 4772-4797 |
| Post-promotion cleanup (coordinated) **[V9]** | `MULTI`, `CONFIG REWRITE`, `CLIENT KILL TYPE normal`, `CLIENT KILL TYPE pubsub`, `CLIENT UNPAUSE`, `EXEC` | old primary and promoted replica | 4822-4853, 2624-2627 |

Notes:
- The reconfiguration command is the literal word **`SLAVEOF`**, not `REPLICAOF` (`sentinel.c:4908`). Hotstash must accept `SLAVEOF host port` and `SLAVEOF NO ONE`.
- `SLAVEOF NO ONE` is built as host `NO` + port `ONE` (`sentinel.c:4875-4878`).
- `host` is `announceSentinelAddr(addr)`, i.e. hostname if `announce-hostnames yes`, else IP.
- The `FAILOVER ABORT` line is sent only when the instance reported a failover state (`monitored_instance_failover_state == FAILOVER`) (`sentinel.c:4900-4905`). **[V9]**
- The MULTI/EXEC is sent as separate pipelined commands on `cc`. Every reply (including `QUEUED`) is discarded; the sentinel only checks the effects in the next INFO (`sentinel.c:4883-4894`).
- `CLIENT KILL TYPE <type>` relies on Valkey's default of not killing the calling client. `CLIENT KILL TYPE pubsub` **will kill the sentinel's own `pc`**, which is then reconnected (`sentinel.c:4917-4927`, `2391`). The code notes that a node not understanding the subcommand just fails that one queued command.
- The sentinel never sends `ROLE`, `HELLO`, `CONFIG SET` or `REPLCONF` (grep of all 24 `valkeyAsyncCommand` call sites).

### 4.4 Which commands a data node must support to be managed

`PING`, `INFO`, `AUTH`, `CLIENT SETNAME`, `SUBSCRIBE`, `PUBLISH` (with fan-out to subscribers), `MULTI`/`EXEC`, `SLAVEOF host port`, `SLAVEOF NO ONE`, `CONFIG REWRITE`, `CLIENT KILL TYPE normal|pubsub`. All of these must be accepted **at queue time** (known command, valid arity): in Valkey/Redis a command rejected while queuing (unknown command, wrong arity) makes `EXEC` fail with `EXECABORT` and then `SLAVEOF` would never run. An error produced at `EXEC` time (for example `CONFIG REWRITE` failing because there is no config file) is harmless, since the sentinel discards every reply. The sentinel's own comment on this point only covers `CLIENT KILL TYPE` on old servers (`sentinel.c:4917-4921`); this paragraph about `EXECABORT` is standard server behaviour, not something read in `sentinel.c`. Optional: `SCRIPT KILL`, `FAILOVER ABORT`, `FAILOVER TO ... TIMEOUT`, `CLIENT PAUSE ... WRITE`, `CLIENT UNPAUSE` **[V9]**.

### 4.5 Complete list of `valkeyAsyncCommand` format strings in `sentinel.c`
`"%s"` (PING, INFO, MULTI, EXEC), `"%s %s"` (AUTH pass; SUBSCRIBE channel), `"%s %s %s"` (AUTH user pass; PUBLISH channel payload; SLAVEOF host port), `"%s SETNAME %s"`, `"%s KILL"` (SCRIPT), `"%s REWRITE"`, `"%s KILL TYPE %s"`, `"%s ABORT"`, `"%s UNPAUSE"`, `"%s PAUSE %d WRITE"`, `"%s TO %s %s TIMEOUT %d"`, and `"%s is-master-down-by-addr %s %s %llu %s"`.

### 4.6 INFO fields parsed from nodes (`sentinelRefreshInstanceInfo`, `sentinel.c:2440-2691`)

The reply is split on `\r\n` (`sentinel.c:2454`) and the whole text is cached for `SENTINEL INFO-CACHE` (`sentinel.c:2446-2447`). Matching is by line prefix.

| Line | Accepted when | Effect | Missing means | Line |
|---|---|---|---|---|
| `run_id:<40 chars>` | line length >= 47 | first value is stored; a later different value fires `+reboot` and, for primaries with `primary-reboot-down-after-period != 0`, sets `SRI_PRIMARY_REBOOT` + `primary_reboot_since_time`; runid updated | `runid` stays NULL: no `+reboot` detection, `SENTINEL REPLICAS/MASTER` show `runid ""` (`sentinel.c:3333`), replica tie-break sorts NULL runid last (`sentinel.c:5036-5044`). No functional failure beyond that | 2460-2476 |
| `slaveN:` (digit after `slave`) | **primaries only**; line length >= 7 | if the line contains `ip=` it is the new format: reads `ip=` and `port=` values (terminated by `,`); else old format `slaveN:<ip>,<port>,<state>` (needs both commas). Unknown replica `ip:port` creates a replica instance, fires `+slave`, flushes config | No replicas discovered (nothing to fail over to) | 2480-2519 |
| `master_link_down_since_seconds:<n>` | any role | `primary_link_down_time = n * 1000`. The field is reset to 0 on every INFO if the line is absent | 0 | 2451, 2522-2524 |
| `role:master` / `role:slave` | prefix match | sets `role` (`SRI_PRIMARY` / `SRI_REPLICA`). If neither appears `role` stays 0 | `role_reported` becomes 0, event `-role-change` ("new reported role is slave"), and the node is treated as not primary | 2527-2530 |
| `master_host:<h>` | role == replica seen earlier in the same reply | stored as `replica_primary_host`; changes update `replica_conf_change_time` | host NULL | 2534-2540 |
| `master_port:<p>` | same | `replica_primary_port = atoi` | 0 | 2543-2550 |
| `master_link_status:<s>` | same | `up` (case-insens) means UP, anything else DOWN (shown `ok` / `err`) | DOWN | 2553-2556 |
| `slave_priority:<n>` | same | `replica_priority` (0 excludes from promotion) | 100 | 2559 |
| `slave_repl_offset:<n>` | same | `replica_repl_offset` (unsigned) | 0 | 2562-2563 |
| `replica_announced:<0/1>` | same | 0 hides the replica from `SENTINEL REPLICAS` | 1 | 2566 |
| `master_failover_state:<s>` **[V9]** | any role | `no-failover` means "supported, idle"; **any other value means "failover ongoing"**; absent means "not supported" and `SENTINEL FAILOVER ... COORDINATED` is refused | not supported | 2570-2576, 3931 |

Order dependency: the replica-only fields are honoured only after a `role:slave` line has been seen in the same reply (`role` is a running variable, `sentinel.c:2532`). `role:` is the first line of the `# Replication` section, so the usual layout works. Hotstash must emit `role:` before `master_host:` etc.

`info_refresh = now` after parsing (`sentinel.c:2578`).

Acting half of INFO processing (skipped entirely in TILT, `sentinel.c:2598`):
- **Role change bookkeeping** (`sentinel.c:2586-2594`): on role change, set `role_reported`/`role_reported_time`; for replicas also `replica_conf_change_time`; event `+role-change` if the new role matches the configured kind, else `-role-change`, both with payload `"%@ new reported role is master|slave"`, level VERBOSE (published, not scripted).
- **Primary reporting `role:slave`**: nothing here; it later makes it SDOWN (section 6) (`sentinel.c:2601-2605`).
- **Replica now reports `role:master`** (`sentinel.c:2608-2643`):
  - If it is the promoted replica and the primary is in `WAIT_PROMOTION`: promotion confirmed (section 9).
  - Otherwise wait `wait_time = publish_period * 4 = 8000`; if not `PROMOTED`, its primary `looks sane`, the replica had no SDOWN/ODOWN within `wait_time` and `now - role_reported_time > wait_time`: send the reconfigure transaction pointing at the primary (`sentinelSendReplicaOf(ri, primary->addr)`) and fire `+convert-to-slave`. This is how an old primary that returns is turned into a replica.
  - `sentinelPrimaryLooksSane` (`sentinel.c:2433-2437`): is a primary, `role_reported == PRIMARY`, no `S_DOWN|O_DOWN`, and `now - info_refresh < 2 * sentinel_info_period`.
- **Replica reports `master_failover_state` != `no-failover` for longer than the primary's `failover_timeout`** [V9]: same sane checks, then `SLAVEOF` to the right primary, event `+fix-slave-config` (`sentinel.c:2646-2655`).
- **Replica replicating the wrong address** (`master_port` differs or `master_host` does not match the primary's address): after `failover_timeout` since `replica_conf_change_time`, with the same sane checks, send `SLAVEOF` to the correct primary, event `+fix-slave-config` (`sentinel.c:2658-2670`).
- **Reconfiguration tracking during failover** (`sentinel.c:2674-2690`): `RECONF_SENT -> RECONF_INPROG` when the replica reports the promoted replica's host/port (event `+slave-reconf-inprog`); `RECONF_INPROG -> RECONF_DONE` when `master_link_status` is up (event `+slave-reconf-done`).

---

## 5. Client-facing commands

### 5.1 Top-level commands available in sentinel mode

Commands carry the flag `SENTINEL` and are the only ones registered (`server.c:3317-3318`); commands with `ONLY_SENTINEL` exist only in sentinel mode (`server.c:3320-3321`). The set (from `commands/*.json` with `"SENTINEL"` in `command_flags`): `ACL` (and subcommands), `AUTH`, `CLIENT` (caching, getname, getredir, help, id, info, kill, list, no-evict, pause, reply, setinfo, setname, tracking, trackinginfo, unblock, unpause), `COMMAND` (and subcommands), `HELLO`, `INFO`, `PING`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `ROLE`, `SHUTDOWN`, and `SENTINEL` with its subcommands. Everything else is an unknown command. In particular there is no `CONFIG`, `MULTI`, `DEBUG`, `SELECT`, `SCRIPT`, `MODULE`.

- `PING`: standard.
- `INFO`: replaced by `sentinelInfoCommand` (`server.c:6794-6797`), see section 11.
- `ROLE`: `sentinelRoleCommand` (`replication.c:4683-4684`; `sentinel.c:4236-4251`): array of 2: bulk `sentinel`, then an array of the monitored primary names (bulk strings).
- `PUBLISH`: `sentinelPublishCommand` (`pubsub.c:655-658`; `sentinel.c:4431-4438`). Channel must equal `__sentinel__:hello` exactly, else error `ERR Only HELLO messages are accepted by Sentinel instances.`. Payload is processed as a hello (no self-id check), reply `:1`.
- `SUBSCRIBE`/`PSUBSCRIBE`: normal. Events are delivered to subscribers via `pubsubPublishMessage` (`sentinel.c:714`).
- `HELLO`: standard, but the map has `server, version, proto, id, mode ("sentinel"), modules` and no `role` field (`networking.c:5935-5962`).
- `SHUTDOWN`: SAVE is forced off (`server.c:4742`).
- `AUTH`, `CLIENT`, `ACL`, `COMMAND`: standard behaviour. Sentinel requires a config file it can write (`sentinel.c:528-537`).

### 5.2 `SENTINEL` subcommand table (`commands/sentinel-*.json`, `commands.def`)

All have `ADMIN|SENTINEL|ONLY_SENTINEL` flags except `HELP`.

| Subcommand | Arity (incl. `SENTINEL`) | Notes |
|---|---|---|
| `CKQUORUM name` | 3 | |
| `CONFIG GET\|SET ...` | -4 | |
| `DEBUG [opt val ...]` | -2 | since 7.0.0 |
| `FAILOVER name [COORDINATED]` | -3 | `COORDINATED` since 9.0.0 **[V9]** |
| `FLUSHCONFIG` | 2 | |
| `GET-PRIMARY-ADDR-BY-NAME name` | 3 | since 8.0.0 |
| `GET-MASTER-ADDR-BY-NAME name` | 3 | deprecated since 8.0.0 alias |
| `HELP` | 2 | flags `LOADING STALE` |
| `INFO-CACHE name [name ...]` | -3 | |
| `IS-PRIMARY-DOWN-BY-ADDR ip port epoch runid` | 6 | since 8.0.0 |
| `IS-MASTER-DOWN-BY-ADDR ...` | 6 | deprecated alias |
| `PRIMARY name` / `MASTER name` | 3 | `MASTER` deprecated alias |
| `PRIMARIES` / `MASTERS` | 2 | `MASTERS` deprecated alias |
| `MONITOR name host port quorum` | 6 | |
| `MYID` | 2 | |
| `PENDING-SCRIPTS` | 2 | |
| `REMOVE name` | 3 | |
| `REPLICAS name` / `SLAVES name` | 3 | `SLAVES` deprecated since 5.0.0 |
| `RESET pattern` | 3 | |
| `SENTINELS name` | 3 | |
| `SET name opt val [opt val ...]` | -5 | |
| `SIMULATE-FAILURE flag [flag ...]` | -3 | |

The code dispatch is by `strcasecmp` on `argv[1]` (`sentinel.c:3772-4150`). Unknown subcommand: `ERR unknown subcommand or wrong number of arguments for '<sub>'. Try SENTINEL HELP.` (`networking.c:1678-1684`). Arity errors use `ERR wrong number of arguments for '<fullname>' command` (`networking.c:1076-1078`). Errors beginning with `-` in the source (for example `-INPROG`) are sent as-is, with no `ERR ` prefix (`networking.c:855-860`).

### 5.3 Reply shapes

**Instance map** (used by `PRIMARY`, and as list elements by `PRIMARIES`, `REPLICAS`, `SENTINELS`) (`sentinel.c:3313-3502`). In RESP2 it is a flat array of key/value bulk strings; in RESP3 a map. Field order, exactly:

| Order | Field | Value type | Present for |
|---|---|---|---|
| 1 | `name` | bulk | all |
| 2 | `ip` | bulk (`announceSentinelAddr`) | all |
| 3 | `port` | bulk-int | all |
| 4 | `runid` | bulk (`""` if NULL) | all |
| 5 | `flags` | bulk, comma list (empty string if none) | all |
| 6 | `link-pending-commands` | bulk-int | all |
| 7 | `link-refcount` | bulk-int | all |
| 8 | `failover-state` | bulk: `none`, `wait_start`, `select_slave`, `send_slaveof_noone`, `wait_promotion`, `reconf_slaves`, `update_config` | only if `FAILOVER_IN_PROGRESS` (`sentinel.c:3299-3310`, `3367-3371`) |
| 9 | `last-ping-sent` | bulk-int ms (`now - act_ping_time`, 0 if no ping pending) | all |
| 10 | `last-ok-ping-reply` | bulk-int ms since `last_avail_time` | all |
| 11 | `last-ping-reply` | bulk-int ms since `last_pong_time` | all |
| 12 | `s-down-time` | bulk-int | only if `S_DOWN` |
| 13 | `o-down-time` | bulk-int | only if `O_DOWN` |
| 14 | `down-after-milliseconds` | bulk-int | all |
| 15 | `info-refresh` | bulk-int ms (0 if never) | primary, replica |
| 16 | `role-reported` | bulk `master`/`slave` | primary, replica |
| 17 | `role-reported-time` | bulk-int ms | primary, replica |
| primary | `config-epoch`, `num-slaves`, `num-other-sentinels`, `quorum`, `failover-timeout`, `parallel-syncs`, then `notification-script` and `client-reconfig-script` only if set | bulk-int / bulk | primary |
| replica | `master-link-down-time`, `master-link-status` (`ok`/`err`), `master-host` (`?` if unknown), `master-port`, `slave-priority`, `slave-repl-offset`, `replica-announced` | bulk / bulk-int | replica |
| sentinel | `last-hello-message` (ms), `voted-leader` (`?` if none), `voted-leader-epoch` | bulk / bulk-int | sentinel |

`PRIMARIES`/`REPLICAS`/`SENTINELS` return an array of such maps; replicas with `replica_announced == 0` are skipped (`sentinel.c:3713-3730`). Unknown name error: `ERR No such master with that name` (`sentinel.c:3740`).

**`GET-PRIMARY-ADDR-BY-NAME name`** (`sentinel.c:3904-3919`): unknown name returns a **null array** (`addReplyNullArray`: `*-1` in RESP2, `_` in RESP3). Known name returns array of 2: bulk ip (`announceSentinelAddr`), bulk port (as bulk string). During a failover that has reached `RECONF_REPLICAS` or later it returns the **promoted replica's** address (`sentinelGetCurrentPrimaryAddress`).

**`CKQUORUM name`** (`sentinel.c:4016-4040`, `3749-3769`): `usable` starts at 1 (self) plus known sentinels with neither `S_DOWN` nor `O_DOWN`. `voters = known sentinels + 1`.
- OK: simple string `+OK <usable> usable Sentinels. Quorum and failover authorization can be reached`.
- Else error `-NOQUORUM <usable> usable Sentinels. ` followed by: `Not enough available Sentinels to reach the specified quorum for this master` (when `usable < quorum`), then `. ` if both, then `Not enough available Sentinels to reach the majority and authorize a failover` (when `usable < voters/2 + 1`).

**`FAILOVER name [COORDINATED]`** (`sentinel.c:3920-3962`), checks in order:
1. unknown name: `ERR No such master with that name`.
2. 4th arg other than `coordinated` (case-insens): `ERR Unknown failover option specified`.
3. coordinated and the primary has never reported `master_failover_state`: `-NOGOODPRIMARY Primary does not support FAILOVER command` **[V9]**.
4. failover already running: `-INPROG Failover already in progress`.
5. no selectable replica: `-NOGOODSLAVE No suitable replica to promote`.
6. Else: log, set `s_down_since_time = now`, set `S_DOWN`, `sentinelStartFailover`. If coordinated: set `SRI_COORD_FAILOVER` and ask other sentinels for votes with `SENTINEL_ASK_FORCED` (election required). Otherwise `SRI_FORCE_FAILOVER` (no election). Reply `+OK`.

**`MONITOR name ip port quorum`** (`sentinel.c:3968-4002`): args checked for control characters (`<= 0x1F` or `0x7F`) (`sentinel.c:3093-3112`), error `ERR Invalid argument at index <i> for SENTINEL MONITOR: control characters are not allowed`. Then: `ERR Invalid quorum`, `ERR Invalid port`, `ERR Quorum must be 1 or greater.`, `ERR Invalid IP address or hostname specified`, or the create errors (`Duplicate master name.` etc.). Success flushes config, replies `+OK`, publishes `+monitor` (`%@ quorum %d`).

**`REMOVE name`** (`sentinel.c:4007-4015`): publishes `-monitor`, deletes the primary, flushes config, `+OK`.

**`RESET pattern`** (`sentinel.c:3900-3903`, `1528-1580`): glob match on primary names; for each it drops replicas and sentinels, closes links, clears flags (keeps only `SRI_PRIMARY`), clears leader/failover state/promoted replica/runid, resets ping times, `failover_start_time = 0`, publishes `+reset-master`. Reply is an **integer**: number of primaries reset.

**`SET name opt val [...]`** (`sentinel.c:4254-4423`): options (case-insens.) and validation:

| Option | Validation | Line |
|---|---|---|
| `down-after-milliseconds` | integer > 0; propagated to replicas and sentinels | 4273-4282 |
| `failover-timeout` | integer > 0 | 4283-4291 |
| `parallel-syncs` | integer > 0 (note: the config file accepts any integer, `sentinel.c:1864`) | 4292-4300 |
| `notification-script`, `client-reconfig-script` | rejected with `Reconfiguration of scripts path is denied for security reasons. Check the deny-scripts-reconfig configuration directive in your Sentinel configuration` if `deny-scripts-reconfig yes` (default); path must be executable unless empty (empty clears) | 4301-4335 |
| `auth-pass`, `auth-user` | empty clears; drops the primary and replica connections to force re-auth; `+set` payload redacts the password as `******` | 4336-4350 |
| `quorum` | integer > 0 | 4351-4359 |
| `rename-command old new` | both non-empty; replaces any previous mapping; no mapping stored if identical case-insensitively | 4360-4380 |
| `primary-reboot-down-after-period` / `master-reboot-down-after-period` | integer >= 0 | 4381-4391 |

Each applied pair publishes `+set` (`"%@ <opt> <val>"`, two-argument form, or `"%@ <opt> <a> <b>"` for `rename-command`). If any change was made, the config is flushed and `+OK` returned. Errors: `ERR Invalid argument '<arg>' for SENTINEL SET '<option>'` or `ERR Unknown option or number of arguments for SENTINEL SET '<option>'`. Options already applied before an error stay applied and the config is flushed (`sentinel.c:4419-4422`). Arguments are control-char checked first.

**`CONFIG SET opt val [opt val ...]`** (`sentinel.c:3115-3238`): options `announce-ip`, `sentinel-user`, `sentinel-pass`, `resolve-hostnames`, `announce-port`, `announce-hostnames`, `loglevel` (`debug|verbose|notice|warning|nothing`). All pairs are validated first. Errors: `Invalid argument '<x>' to SENTINEL CONFIG SET`, `Duplicate argument '<x>' to SENTINEL CONFIG SET`, `Missing argument '<x>' value`, `Invalid value '<v>' to SENTINEL CONFIG SET '<opt>'`. Success flushes config and replies `+OK`; changing `sentinel-user`/`sentinel-pass` drops all sentinel connections (`sentinel.c:3228-3229`, `1150-1175`). `announce-port` must be 0..65535 (`sentinel.c:3169`).

**`CONFIG GET pattern [pattern ...]`** (`sentinel.c:3241-3297`): glob patterns, replies a map (flat array in RESP2) of name/value bulk strings. For each pattern, matching names are emitted in this fixed order: `resolve-hostnames`, `announce-hostnames`, `announce-ip`, `announce-port`, `sentinel-user`, `sentinel-pass`, `loglevel`; duplicates across patterns are suppressed. Values: `yes`/`no`, string (`""` if unset), integer as bulk. Any other `CONFIG` form: `ERR Only SENTINEL CONFIG GET <param> [<param> <param> ...] / SET <param> <value> [<param> <value> ...] are supported.`

**`INFO-CACHE [name ...]`** (`sentinel.c:4052-4116`): with no names, all primaries; unknown names are ignored. Reply: array of length `2 * N`: for each primary a bulk name followed by an array of `1 + #replicas` entries, each an array `[integer ms since info_refresh (0 if never), bulk INFO text or null]` (primary first, then replicas).

**`DEBUG`** (`sentinel.c:3504-3709`): no args returns a map with keys, in order: `INFO-PERIOD`, `PING-PERIOD`, `ASK-PERIOD`, `PUBLISH-PERIOD`, `DEFAULT-DOWN-AFTER`, `DEFAULT-FAILOVER-TIMEOUT`, `TILT-TRIGGER`, `TILT-PERIOD`, `SLAVE-RECONF-TIMEOUT`, `MIN-LINK-RECONNECT-PERIOD`, `ELECTION-TIMEOUT`, `SCRIPT-MAX-RUNTIME`, `SCRIPT-RETRY-DELAY` (values as bulk-int). With args, option/value pairs, replies `+OK`. Errors: `Invalid argument '<v>' for SENTINEL DEBUG '<opt>'`, `Unknown option or number of arguments for SENTINEL DEBUG '<opt>'`.

**`SIMULATE-FAILURE`** (`sentinel.c:4117-4142`): flags `crash-after-election`, `crash-after-promotion`; `HELP` returns array of those 2 names; unknown: `ERR Unknown failure simulation specified`; reply `+OK`. Crash is `exit(99)` (`sentinel.c:4626-4629`).

**`PENDING-SCRIPTS`** (`sentinel.c:940-977`): array of maps (5 pairs each): `argv` (array of bulk), `flags` (`running` | `scheduled`), `pid`, then `run-time` (if running) or `run-delay` (if not), then `retry-num`.

**`MYID`**: bulk string of 40 chars (`sentinel.c:3848-3850`). **`FLUSHCONFIG`**: `+OK`, or `ERR Failed to save config file. Check server logs.` (`sentinel.c:2249-2254`). **`HELP`**: standard help array (`sentinel.c:3772-3822`).

**`IS-PRIMARY-DOWN-BY-ADDR` / `IS-MASTER-DOWN-BY-ADDR`**: see 4.2 (`sentinel.c:3851-3899`).

Config-sync related reply: the `SENTINEL SENTINELS` list is how clients and tools enumerate quorum members.

---

## 6. SDOWN / ODOWN

### 6.1 `sentinelCheckSubjectivelyDown` (`sentinel.c:4443-4500`), every tick, every instance kind

`elapsed` (`sentinel.c:4444-4449`):
- if `act_ping_time != 0`: `now - act_ping_time` (a ping is outstanding);
- else if the link is disconnected: `now - last_avail_time`;
- else 0.

Link recycling (`sentinel.c:4457-4474`):
- Close `cc` if it was connected for more than `min_link_reconnect_period` (15000), a ping is pending, and both `now - act_ping_time` and `now - last_pong_time` exceed `down_after_period / 2`.
- Close `pc` if connected for more than 15000 and `now - pc_last_activity > publish_period * 3` (6000).

SDOWN is set when any of (`sentinel.c:4482-4486`):
1. `elapsed > down_after_period`.
2. The instance is configured as a **primary**, reports `role:slave`, and has done so for more than `down_after_period + 2 * sentinel_info_period` (default `down_after + 20000`) measured from `role_reported_time`.
3. `SRI_PRIMARY_REBOOT` is set and `now - primary_reboot_since_time > primary_reboot_down_after_period`.

On transition up to SDOWN: `+sdown` (`%@`), `s_down_since_time = now`, set flag (`sentinel.c:4488-4492`). On recovery: `-sdown`, clear `S_DOWN` and `SCRIPT_KILL_SENT` (`sentinel.c:4495-4498`). SDOWN is evaluated for primaries, replicas and sentinels.

There is no "primary reports slave for too long" other than rule 2 above, and rule 2 does not apply to replicas.

### 6.2 `sentinelCheckObjectivelyDown` (`sentinel.c:4508-4540`), primaries only

If the primary is `S_DOWN`: `votes = 1 (self) + number of known sentinels with SRI_PRIMARY_DOWN`. `ODOWN` iff `votes >= quorum`. If not SDOWN, ODOWN is cleared. Events: `+odown` with `"%@ #quorum %d/%d"` (votes/quorum) and `o_down_since_time = now`; `-odown` with `%@`.

The `SRI_PRIMARY_DOWN` flags come only from `is-master-down-by-addr` replies (4.2). They are cleared when no reply arrived for `5 * ask_period` (5 s), but that cleanup runs only while the primary is `S_DOWN` (`sentinel.c:4583`, `4598-4600`). ODOWN needs SDOWN anyway (`sentinel.c:4513`). A sentinel that is itself SDOWN still counts in the quorum if its stale flag has not yet expired; sentinels are never expired from the dictionary.

---

## 7. Leader election and epochs

State:
- `sentinel.current_epoch` (global, persisted as `sentinel current-epoch`) (`sentinel.c:269`).
- per primary: `config_epoch` (epoch of the current configuration), `leader` + `leader_epoch` (this sentinel's own vote), `failover_epoch` (epoch of the failover this sentinel started) (`sentinel.c:195`, `246-251`).
- per sentinel instance: `leader` + `leader_epoch` = that sentinel's last reported vote (`sentinel.c:246-250`).

### 7.1 Starting (`sentinelStartFailoverIfNeeded`, `sentinel.c:4961-4984`)

Start when all hold: primary `O_DOWN`; no failover in progress; and `now - failover_start_time >= failover_timeout * 2` (otherwise logs `"Next failover delay: I will not start a failover before %s"` once).

`sentinelStartFailover` (`sentinel.c:4938-4948`):
- state `WAIT_START`, set `FAILOVER_IN_PROGRESS`;
- `failover_epoch = ++current_epoch`, publish `+new-epoch <epoch>`, publish `+try-failover` (`%@`);
- `failover_start_time = now + rand() % 1000`; `failover_state_change_time = now`.

The caller then immediately asks the other sentinels with `SENTINEL_ASK_FORCED` (`sentinel.c:5399`), and every later tick asks again subject to `ask_period` (`sentinel.c:5401`). Because the failover state is now `> NONE` the runid argument is this sentinel's id, so each ask is a vote request for `current_epoch` (`sentinel.c:4617`).

The random `failover_start_time` is **not** a start delay in `WAIT_START`: that state does not look at it. It only (a) gates the next failover attempt (`failover_timeout * 2` window) and (b) is the origin of the election timeout.

### 7.2 Voting (`sentinelVoteLeader`, `sentinel.c:4636-4659`)

Called with `(primary, req_epoch, req_runid)`:
1. If `req_epoch > current_epoch`: `current_epoch = req_epoch`, flush config, publish `+new-epoch`.
2. If `primary->leader_epoch < req_epoch && current_epoch <= req_epoch`: record the vote: `leader = req_runid`, `leader_epoch = current_epoch`, flush config, publish `+vote-for-leader` with `"%s %llu"` (runid, epoch). If the vote is not for itself (`strcasecmp(leader, myid)`), `failover_start_time = now + rand() % 1000` to delay its own failover for this primary.
3. Return `primary->leader` (a copy, possibly from an older vote) and `*leader_epoch = primary->leader_epoch`, or NULL when there is no leader.

So a sentinel votes at most once per epoch per primary, and replies with its existing vote when it has already voted (possibly an older epoch). The requester counts the reply only if the epoch matches (7.3).

### 7.3 Counting (`sentinelGetLeader`, `sentinel.c:4690-4751`)

Called each tick from `WAIT_START` with `failover_epoch` (`sentinel.c:5095`):
- `voters = known sentinels + 1` (`sentinel.c:4703`).
- Other sentinels' votes count only if `leader != NULL && leader_epoch == sentinel.current_epoch` (`sentinel.c:4709`).
- Pick the runid with the most votes (first one wins ties, dictionary order, so not deterministic) (`sentinel.c:4716-4725`).
- This sentinel's own vote: via `sentinelVoteLeader`, voting for the current winner if there is one, else for itself (`sentinel.c:4730-4733`). Counted if `leader_epoch == epoch` (`sentinel.c:4735-4742`).
- The winner needs `votes >= voters/2 + 1` **and** `votes >= primary->quorum`, i.e. majority = max(quorum, voters/2+1) (`sentinel.c:4744-4745`); otherwise no leader.

### 7.4 Election timeout and retry (`sentinelFailoverWaitStart`, `sentinel.c:5090-5119`)

Not elected and not `FORCE_FAILOVER`: if `now - failover_start_time > min(sentinel_election_timeout 10000, failover_timeout)`, publish `-failover-abort-not-elected` and abort (`sentinel.c:5102-5111`). A later retry is gated by `failover_timeout * 2` from `failover_start_time` (7.1). Elected: `+elected-leader`, optional simulated crash, state `SELECT_REPLICA`, publish `+failover-state-select-slave`.

Epoch guarantees for interop: a vote request for an epoch greater than the receiver's `current_epoch` raises it and persists it; the hello message carries `current_epoch` and raises it everywhere (3.4 step 5). Both are needed for sentinels from different implementations to converge on epochs.

---

## 8. Replica selection (`sentinelSelectReplica`, `sentinel.c:5047-5087`)

A replica is a candidate only if **all** pass:
1. no `S_DOWN` / `O_DOWN` (`sentinel.c:5064`);
2. link not disconnected (`sentinel.c:5065`);
3. `now - last_avail_time <= sentinel_ping_period * 5` (5000) (`sentinel.c:5066`);
4. `replica_priority != 0` (`sentinel.c:5067`);
5. `now - info_refresh <= info_validity_time`, where `info_validity_time = ping_period * 5` (5000) if the primary is `S_DOWN`, else `info_period * 3` (30000) (`sentinel.c:5072-5076`);
6. `primary_link_down_time <= max_primary_down_time`, where `max_primary_down_time = (primary S_DOWN ? now - s_down_since_time : 0) + primary->down_after_period * 10` (`sentinel.c:5053-5056`, `5077`).

Not checked: `replica_announced`, `master_link_status`, `role_reported`, replication lag.

Order (`compareReplicasForPromotion`, `sentinel.c:5018-5045`), first of the sorted list wins:
1. lower `replica_priority`;
2. greater `replica_repl_offset`;
3. lexicographically smaller runid (`strcasecmp`); a NULL runid sorts after any non-NULL one.

---

## 9. Failover state machine

States (`sentinel.c:111-117`, names in `sentinel.c:3299-3310`):
`NONE=0 "none"`, `WAIT_START=1 "wait_start"`, `SELECT_REPLICA=2 "select_slave"`, `SEND_REPLICAOF_NOONE=3 "send_slaveof_noone"`, `WAIT_PROMOTION=4 "wait_promotion"`, `RECONF_REPLICAS=5 "reconf_slaves"`, `UPDATE_CONFIG=6 "update_config"`.

Dispatch is `sentinelFailoverStateMachine` (`sentinel.c:5334-5351`), run only for primaries with `FAILOVER_IN_PROGRESS`.

| State | Behaviour | Events | Lines |
|---|---|---|---|
| `WAIT_START` | election, see 7.3-7.4; forced failover skips the election | `+elected-leader`, `+failover-state-select-slave`; abort `-failover-abort-not-elected` | 5090-5119 |
| `SELECT_REPLICA` | run replica selection (section 8). None: abort. Else flag replica `PROMOTED`, `promoted_replica = replica`, next state | abort `-failover-abort-no-good-slave`; `+selected-slave` (WARNING, on the replica), `+failover-state-send-slaveof-noone` (NOTICE, on the replica) | 5121-5137 |
| `SEND_REPLICAOF_NOONE` (not coordinated) | if the promoted replica's link is down: wait, and abort after `failover_timeout` since state change. Else send the `SLAVEOF NO ONE` transaction (4.3) and move to `WAIT_PROMOTION` | `-failover-abort-slave-timeout`; `+failover-state-wait-promotion` (NOTICE, on the replica) | 5171-5194 |
| `SEND_REPLICAOF_NOONE` (coordinated) **[V9]** | abort if `failover_timeout - elapsed < 1000`; wait if the primary link is down; send the `CLIENT PAUSE/FAILOVER TO/EXEC` transaction to the **primary**; move to `WAIT_PROMOTION` | `-failover-abort-master-timeout`; `+failover-state-wait-promotion` | 5139-5168, 5342-5347 |
| `WAIT_PROMOTION` | only a timeout is handled here: abort after `failover_timeout` since state change | `-failover-abort-slave-timeout` | 5198-5205 |
| `RECONF_REPLICAS` | send reconfigs to replicas, check for end | below | 5269-5319 |
| `UPDATE_CONFIG` | handled by the dict walk: switch address | `+switch-master` | 5324-5332, 5421-5427 |

**Promotion detection** (`sentinel.c:2608-2632`): in INFO processing, a replica with `PROMOTED` whose primary is in `WAIT_PROMOTION`, now reporting `role:master` (and not in TILT):
- `primary->config_epoch = primary->failover_epoch`;
- state `RECONF_REPLICAS`, `failover_state_change_time = now`;
- flush config; publish `+promoted-slave` (WARNING, on the replica);
- **[V9]** if `SRI_COORD_FAILOVER`: send the cleanup transaction (`CONFIG REWRITE`, `CLIENT KILL`, `CLIENT UNPAUSE`) to the old primary and to the promoted replica (`sentinel.c:4809-4856`);
- publish `+failover-state-reconf-slaves` (on the primary);
- call the client-reconfig script (role `leader`, `start`, old addr, new addr);
- `sentinelForceHelloUpdateForPrimary` (immediate hello with the new config epoch and address, 3.2).

The failover can no longer be aborted from this point (`sentinelAbortFailover` asserts `state <= WAIT_PROMOTION`, `sentinel.c:5358-5369`). Abort clears `FAILOVER_IN_PROGRESS|FORCE_FAILOVER|COORD_FAILOVER`, resets to `NONE`, and un-flags the promoted replica.

**Reconfiguring the other replicas** (`sentinelFailoverReconfNextReplica`, `sentinel.c:5269-5319`), every tick:
- `in_progress` = number of replicas with `RECONF_SENT|RECONF_INPROG`.
- While `in_progress < parallel_syncs`, for each replica (skipping `PROMOTED` and `RECONF_DONE`):
  - if `RECONF_SENT` and `now - replica_reconf_sent_time > 10000`: publish `-slave-reconf-sent-timeout`, clear `RECONF_SENT`, set `RECONF_DONE` (it will be fixed later by the "wrong address" rule, `sentinel.c:2658-2670`);
  - skip if `RECONF_SENT|RECONF_INPROG` or the link is disconnected;
  - else send `SLAVEOF <new primary host> <port>` transaction (4.3), set `RECONF_SENT`, `replica_reconf_sent_time = now`, publish `+slave-reconf-sent`, `in_progress++`.
- Then `sentinelFailoverDetectEnd`.

Replica progress flags are advanced by INFO (4.6): `RECONF_SENT -> RECONF_INPROG -> RECONF_DONE`.

**End detection** (`sentinelFailoverDetectEnd`, `sentinel.c:5207-5265`):
- Do nothing if the promoted replica is missing or `S_DOWN`.
- `not_reconfigured` = replicas not (`PROMOTED` or `RECONF_DONE`) and not `S_DOWN`.
- If `now - failover_state_change_time > failover_timeout`: force end, publish `+failover-end-for-timeout`, and send best-effort `SLAVEOF` to every connected replica not already `PROMOTED|RECONF_DONE|RECONF_SENT`, flagging it `RECONF_SENT` and publishing `+slave-reconf-sent-be`.
- If `not_reconfigured == 0`: publish `+failover-end`, state `UPDATE_CONFIG`.

**Switch** (`sentinelFailoverSwitchToPromotedReplica`, `sentinel.c:5324-5332`): publish `+switch-master` (`"<name> <old ip> <old port> <new ip> <new port>"`), then `sentinelResetPrimaryAndChangeAddress(primary, promoted->hostname, promoted->port)` (`sentinel.c:1589-1644`):
- builds the new replica list = old replicas except the promoted address + **the old primary's address**, so the old primary is monitored as a replica and its `+slave` event fires for each;
- resets the primary (without discarding sentinels), clears SDOWN/ODOWN, swaps the address, flushes the config.
- `config_epoch` and `leader_epoch` are kept, so the new `config_epoch` (= `failover_epoch`) is what other sentinels adopt.

**How other sentinels learn the result**: hello messages carry the new primary address (once `RECONF_REPLICAS`) and the new `config_epoch`; receivers with a lower `config_epoch` run the `+config-update-from` / `+switch-master` path (3.4 step 6).

**Old primary comes back**: it is now a replica instance reporting `role:master`. After `8000 ms` of no down state and sane primary, every sentinel (leader or not) sends it `SLAVEOF <new primary>` and fires `+convert-to-slave` (4.6). If it reports `role:slave` of the wrong address, `+fix-slave-config` after `failover_timeout`.

**Manual failover** (`SENTINEL FAILOVER`, 5.3): sets `S_DOWN` and `FORCE_FAILOVER`, so `WAIT_START` treats the sentinel as leader without any vote (`sentinel.c:5101`), but still bumps the epoch via `sentinelStartFailover`. The `S_DOWN` flag is cleared on the next tick if the primary is actually healthy (`sentinel.c:4493-4499`, event `-sdown`). Since the primary is not `O_DOWN`, `sentinelGetLeader`'s assertion still holds because it also accepts `FAILOVER_IN_PROGRESS` (`sentinel.c:4700`). Coordinated mode instead needs an election, and asks only while the primary is `S_DOWN`, so it relies on the initial forced ask (`sentinel.c:3956`) plus replies from sentinels that vote regardless of their own SDOWN view (`sentinel.c:4889`); this follows from the code but was not exercised.

`failover_state_change_time` is set at every transition (lines 4947, 5117, 5134, 5167, 5193, 2620, 5239, 5364) and is the clock for the per-state timeouts above (`failover_timeout` default 180000).

---

## 10. Config file

Sentinel state is stored with the normal config rewrite machinery (`config.c:1808`, `sentinel.c:1992-2222`). Lines are written with the prefix `sentinel `. `sentinelFlushConfig` (`sentinel.c:2228-2244`) calls `rewriteConfig(server.configfile, 0)` with `hz` temporarily set to 10. A sentinel refuses to start without a writable config file (`sentinel.c:528-537`). Non-sentinel directives (port, dir, logfile, requirepass, ACL, etc.) are rewritten by the normal code path, and `save` is skipped in sentinel mode (`config.c:1428-1431`).

### 10.1 Lines written (`sentinelRewriteConfig`, `sentinel.c:1998-2221`)

| Line (printf) | When written | Line |
|---|---|---|
| `sentinel myid %s` | always | 1998-1999 |
| `sentinel deny-scripts-reconfig yes\|no` | only if != default `yes` | 2002-2004 |
| `sentinel resolve-hostnames yes\|no` | only if != default `no` | 2010-2012 |
| `sentinel announce-hostnames yes\|no` | only if != default `no` | 2015-2017 |
| `sentinel monitor %s %s %d %d` (name, **current** primary addr incl. promoted replica, port, quorum) | per primary | 2028-2030 |
| `sentinel down-after-milliseconds %s %ld` | if != default 30000 | 2034-2038 |
| `sentinel failover-timeout %s %ld` | if != default 180000 | 2042-2046 |
| `sentinel parallel-syncs %s %d` | if != 1 | 2050-2053 |
| `sentinel notification-script %s "<repr>"` | if set | 2057-2061 |
| `sentinel client-reconfig-script %s "<repr>"` | if set | 2065-2069 |
| `sentinel auth-pass %s "<repr>"` | if set | 2073-2077 |
| `sentinel auth-user %s "<repr>"` | if set | 2080-2084 |
| `sentinel primary-reboot-down-after-period %s %ld` | if != 0 | 2088-2092 |
| `sentinel config-epoch %s %llu` | per primary, always | 2096-2099 |
| `sentinel leader-epoch %s %llu` | per primary, always | 2103-2106 |
| `sentinel known-replica %s %s %d` (name, ip, port) | per replica (if the replica address equals the current primary address during failover, the old primary address is written instead); replaces an existing `known-slave` line if one is present | 2109-2131 |
| `sentinel known-sentinel %s %s %d "<runid>"` | per sentinel with a runid | 2135-2144 |
| `sentinel rename-command %s "<old>" "<new>"` | per mapping | 2148-2158 |
| `sentinel current-epoch %llu` | always | 2163-2164 |
| `sentinel announce-ip "<repr>"` | if set | 2167-2173 |
| `sentinel announce-port %d` | if non-zero | 2176-2181 |
| `sentinel sentinel-user "<repr>"`, `sentinel sentinel-pass "<repr>"` | if set | 2184-2199 |

`<repr>` is `sdscatrepr` quoting. Lines whose names are no longer produced are deleted (`rewriteConfigMarkAsProcessed`, `sentinel.c:2208-2221`).

### 10.2 When flushed

`sentinelFlushConfig` is called on: first start when `myid` is generated (`sentinel.c:553`); new replica discovered via INFO (`sentinel.c:2516`); new sentinel via hello (`sentinel.c:2839`); new epoch via hello (`sentinel.c:2846`); `+new-epoch` from votes (`sentinel.c:4639`); vote recorded (`sentinel.c:4647`); promotion detected (`sentinel.c:2621`); `sentinelResetPrimaryAndChangeAddress` (`sentinel.c:1642`); and the admin commands `MONITOR`, `REMOVE`, `SET`, `CONFIG SET`, `FLUSHCONFIG` (via `sentinelFlushConfigAndReply`, `sentinel.c:2249-2254`). Success logs `"Sentinel new configuration saved on disk"`; failure logs a warning (`sentinel.c:2236-2243`).

### 10.3 Directives accepted at load (`sentinelHandleConfiguration`, `sentinel.c:1836-1985`)

Lines are `sentinel <directive> ...`; the generic parser queues them (`config.c:588-596`) and they are applied in three batches: pre-monitor directives (`announce-ip`, `announce-port`, `deny-scripts-reconfig`, `sentinel-user`, `sentinel-pass`, `current-epoch`, `myid`, `resolve-hostnames`, `announce-hostnames`) then `monitor` then everything else (`sentinel.c:482-492`, `1782-1790`, `1806-1822`). A `sentinel` directive outside sentinel mode is an error (`config.c:592-594`).

| Directive | argc | Validation / error | Line |
|---|---|---|---|
| `monitor <name> <host> <port> <quorum>` | 5 | quorum > 0 ("Quorum must be 1 or greater."); create errors | 1839-1846 |
| `down-after-milliseconds <name> <ms>` | 3 | > 0; propagates | 1847-1853 |
| `failover-timeout <name> <ms>` | 3 | > 0 | 1854-1859 |
| `parallel-syncs <name> <n>` | 3 | any `atoi` | 1860-1864 |
| `notification-script <name> <path>`, `client-reconfig-script <name> <path>` | 3 | must be executable | 1865-1878 |
| `auth-pass <name> <pw>`, `auth-user <name> <user>` | 3 | | 1879-1888 |
| `current-epoch <n>` | 2 | raised only upward | 1889-1892 |
| `myid <40 chars>` | 2 | length must be exactly 40 ("Malformed Sentinel id in myid option.") | 1893-1895 |
| `config-epoch <name> <n>` | 3 | also raises `current_epoch` | 1896-1904 |
| `leader-epoch <name> <n>` | 3 | | 1905-1909 |
| `known-slave` / `known-replica <name> <ip> <port>` | 4 | creates replica | 1910-1919 |
| `known-sentinel <name> <ip> <port> <runid>` | 5 (the 4-arg legacy form is ignored) | creates sentinel and tries link sharing | 1920-1933 |
| `rename-command <name> <cmd> <new>` | 4 | duplicate error | 1934-1944 |
| `announce-ip <ip>`, `announce-port <port>` | 2 | | 1945-1950 |
| `deny-scripts-reconfig`, `resolve-hostnames`, `announce-hostnames` `<yes\|no>` | 2 | | 1951-1971 |
| `sentinel-user`, `sentinel-pass` | 2 | empty ignored | 1957-1962 |
| `primary-reboot-down-after-period` / `master-reboot-down-after-period <name> <ms>` | 3 | >= 0 | 1973-1980 |

Unknown directive: `Unrecognized sentinel configuration statement.` Unknown master name: `No such master with specified name.` (`sentinel.c:1850` etc.). The only aliases accepted at load are `known-slave` for `known-replica` and `master-reboot-down-after-period` for `primary-reboot-down-after-period`. All other directives have a single spelling, and there is no `sentinel primary ...` form in 9.1.3. When rewriting, the file always gets the `known-replica` and `primary-reboot-down-after-period` spellings.

A sentinel with no `myid` generates 40 random hex chars at startup and persists them immediately (`sentinel.c:541-562`), and then emits `+monitor` for every configured primary (`sentinel.c:732-742`).

---

## 11. The sentinel's INFO

`sentinelInfoCommand` (`sentinel.c:4160-4232`). Supported sections: `server`, `clients`, `cpu`, `stats`, `sentinel` (`sentinel.c:4161`). Requests for other sections are dropped; `all`/`everything`/no argument gives all five. The first four come from the standard generator (`genValkeyInfoString`); their field names are those of normal Valkey (for example `# Server` has `run_id:`, `tcp_port:` and `server_mode:sentinel`, `server.c:6103`, `6148-6149`). Sections are separated by a blank line (`\r\n`).

`# Sentinel` section, exact text (`sentinel.c:4199-4211`):

```
# Sentinel\r\n
sentinel_masters:%lu\r\n
sentinel_tilt:%d\r\n
sentinel_tilt_since_seconds:%jd\r\n      (-1 if not in tilt, else seconds since tilt start)
sentinel_total_tilt:%d\r\n
sentinel_running_scripts:%d\r\n
sentinel_scripts_queue_length:%ld\r\n
sentinel_simulate_failure_flags:%lu\r\n
```

Then one line per primary (`sentinel.c:4213-4227`):

```
master%d:name=%s,status=%s,address=%s:%d,slaves=%lu,sentinels=%lu\r\n
```
- index counts from 0;
- `status` is `odown` if `O_DOWN`, else `sdown` if `S_DOWN`, else `ok`;
- `address` is `announceSentinelAddr(addr):port` (no brackets for IPv6 here);
- `slaves` is the replica count (includes unannounced), `sentinels` is the other-sentinel count **+ 1** (self).

---

## 12. Pub/Sub events

Every event is published on a channel equal to its name, with the payload below (`sentinelEvent`, `sentinel.c:678-726`). DEBUG-level events are only logged, never published (`sentinel.c:711`). A script is queued only for `LL_WARNING` events that have an instance (`sentinel.c:720-725`).

**Instance description** `%@` (`sentinel.c:684-696`):
- primary: `master <name> <ip> <port>`
- replica/sentinel: `<slave|sentinel> <name> <ip> <port> @ <primary name> <primary ip> <primary port>`
- IPs use `announceSentinelAddr` (hostname when `announce-hostnames yes`). For a replica `<name>` is `ip:port`; for a sentinel it is the runid.

| Event | Level | Instance | Payload after description | When | Line |
|---|---|---|---|---|---|
| `+monitor` | WARNING | primary | `quorum <q>` | startup and `SENTINEL MONITOR` | 739, 4001 |
| `-monitor` | WARNING | primary | | `SENTINEL REMOVE` | 4013 |
| `+set` | WARNING | primary | `<opt> <val>` (or `<opt> <a> <b>`) | `SENTINEL SET` | 4404-4411 |
| `+reset-master` | WARNING | primary | | `SENTINEL RESET` | 1557 |
| `+slave` | NOTICE | replica | | new replica from INFO, or re-added after switch | 1635, 2515 |
| `+sentinel` | NOTICE | sentinel | | new sentinel via hello | 2832 |
| `+sentinel-address-switch` | NOTICE | primary | `ip <ip> port <port> for <runid>` | known runid with new address | 2799 |
| `+sentinel-invalid-addr` | NOTICE | sentinel | | another sentinel claims the same address | 2811 |
| `+sentinel-address-update` | NOTICE | sentinel | `<n> additional matching instances` | address copied to other primaries | 1211 |
| `+reboot` | NOTICE | instance | | `run_id` changed | 2465 |
| `+role-change` / `-role-change` | VERBOSE | instance | `new reported role is master\|slave` | reported role changed (+ if coherent) | 2592 |
| `+sdown` / `-sdown` | WARNING | instance | | SDOWN transitions | 4489, 4496 |
| `+odown` | WARNING | primary | `#quorum <votes>/<quorum>` | | 4530 |
| `-odown` | WARNING | primary | | | 4536 |
| `+new-epoch` | WARNING | primary | `<epoch>` (no description, `%@` not used) | epoch raised or incremented | 2847, 4640, 4944 |
| `+vote-for-leader` | WARNING | primary | `<runid> <epoch>` (no description) | recorded a vote | 4648 |
| `+try-failover` | WARNING | primary | | failover started | 4945 |
| `-failover-abort-not-elected` | WARNING | primary | | election timeout | 5109 |
| `+elected-leader` | WARNING | primary | | won (or forced) | 5114 |
| `+failover-state-select-slave` | WARNING | primary | | | 5118 |
| `-failover-abort-no-good-slave` | WARNING | primary | | | 5127 |
| `+selected-slave` | WARNING | replica | | | 5130 |
| `+failover-state-send-slaveof-noone` | NOTICE | replica | | | 5135 |
| `-failover-abort-master-timeout` | WARNING | primary | | coordinated, <1 s left | 5146 |
| `+failover-state-wait-promotion` | NOTICE | replica | | command sent | 5164, 5191 |
| `-failover-abort-slave-timeout` | WARNING | primary | | | 5179, 5202 |
| `+promoted-slave` | WARNING | replica | | replica reports master | 2622 |
| `+failover-state-reconf-slaves` | WARNING | primary | | | 2628 |
| `+slave-reconf-sent` / `+slave-reconf-sent-be` | NOTICE | replica | | SLAVEOF sent (be = best effort at timeout) | 5311, 5259 |
| `-slave-reconf-sent-timeout` | NOTICE | replica | | 10 s without progress | 5296 |
| `+slave-reconf-inprog` / `+slave-reconf-done` | NOTICE | replica | | | 2681, 2688 |
| `+failover-end` | WARNING | primary | | | 5237 |
| `+failover-end-for-timeout` | WARNING | primary | | | 5233 |
| `+switch-master` | WARNING | primary | `<name> <old ip> <old port> <new ip> <new port>` (no description) | end of failover (leader) or hello config update (observer) | 2857, 5327 |
| `+config-update-from` | WARNING | sentinel | | before observer `+switch-master` | 2856 |
| `+convert-to-slave` | NOTICE | replica | | old primary reconfigured | 2640 |
| `+fix-slave-config` | NOTICE | replica | | replica fixed | 2653, 2668 |
| `+tilt` / `-tilt` | WARNING | none | `#tilt mode entered` / `#tilt mode exited` | | 5460, 5390 |
| `-script-error` | WARNING | none | `<path> <signal> <exit code>` (fork error: `99 0`) | | 845, 912 |
| `-script-timeout` | WARNING | none | `<path> <pid>` | | 933 |
| `+script-child`, `-script-child`, `-cmd-link-reconnection`, `-pubsub-link-reconnection` | DEBUG | | not published | | 857, 891, 2369-2399 |

### 12.1 Scripts

Scheduled with `sentinelScheduleScriptExecution` (up to 16 args, queue limit 256, 16 concurrent, `fork` + `execve`) (`sentinel.c:755-860`).
- Notification script (per primary): `<script> <event-type> "<event description>"`, i.e. argv = `type`, `msg` (`sentinel.c:723`).
- Client reconfig script: `<script> <primary-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>`, with `role` = `leader` or `observer`, `state` always `start` (`sentinel.c:979-1004`). Called on promotion (leader, `sentinel.c:2629`) and from hello config update (observer, `sentinel.c:2862`).
- Retry: exit code 1 or a signal reschedules the script until 10 attempts, with delay `30 s * 2^(retry-1)` (`sentinel.c:869-874`, `901-908`). Runtime over 60 s is killed (`sentinel.c:932-935`).

---

## 13. Hotstash implementation checklist

### (a) To be monitored (and managed) by a Valkey sentinel

1. RESP2 server on the data port: `PING` returns `+PONG`; `AUTH [user] pass`; `CLIENT SETNAME`.
2. `INFO` (no section argument) returns a bulk string with `\r\n` line endings including:
   - `run_id:` exactly 40 chars, changes on restart;
   - `role:master` or `role:slave`, **before** the replica fields;
   - on a primary: `slaveN:ip=<ip>,port=<listening port>,state=...,offset=...,lag=...` (the `ip=` and `port=` values must be reachable from the sentinel);
   - on a replica: `master_host`, `master_port`, `master_link_status:up|down`, `slave_repl_offset`, `slave_priority`, `master_link_down_since_seconds` (when the link is down), optionally `replica_announced`.
   - `master_failover_state:no-failover` is optional **[V9]**. If emitted, any other value is read as "failover ongoing" and suppresses hellos; if omitted, `SENTINEL FAILOVER ... COORDINATED` is refused (`NOGOODPRIMARY`).
3. `SUBSCRIBE` and `PUBLISH` with fan-out as `["message", channel, payload]` to all subscribers (including the publisher's own subscription), so hello messages reach every sentinel.
4. `MULTI` / `EXEC` with `SLAVEOF host port`, `SLAVEOF NO ONE`, `CONFIG REWRITE` (may error), `CLIENT KILL TYPE normal|pubsub` (must skip the caller; killing pubsub clients is expected).
5. A correct `SLAVEOF NO ONE` promotion that flips `INFO role` to `master`, and `SLAVEOF` that updates `master_host`/`master_port` and later reports `master_link_status:up`.
6. Optional: `SCRIPT KILL`, `FAILOVER TO host port TIMEOUT ms`, `FAILOVER ABORT`, `CLIENT PAUSE ms WRITE`, `CLIENT UNPAUSE` **[V9]**.

### (b) To be a sentinel that shares a quorum with Valkey sentinels

1. Sentinel mode with the command set of 5.1 and the `SENTINEL` subcommands used between sentinels at minimum: `IS-MASTER-DOWN-BY-ADDR` (both spellings, 6 args, reply shape of 4.2), `MASTER`/`PRIMARY`, `MASTERS`/`PRIMARIES`, `REPLICAS`/`SLAVES`, `SENTINELS`, `GET-MASTER-ADDR-BY-NAME` (both spellings), `MYID`, `CKQUORUM`, `MONITOR`, `REMOVE`, `SET`, `RESET`, `FAILOVER`, `FLUSHCONFIG`.
2. Fake `PUBLISH`: accept only `__sentinel__:hello`, process it as a hello, reply `:1`. `INFO` with the `# Sentinel` section. `ROLE` returning `sentinel` + names.
3. Hello: send the 8-field payload to primaries, replicas and sentinels every 2000 ms; parse incoming hellos exactly as 3.4 (strict greater-than config epoch rule, address-switch handling, drop self by `myid` substring).
4. Persist `myid` (40 hex), `current-epoch`, per-primary `config-epoch`, `leader-epoch`, known replicas/sentinels, in the config file with the line formats of 10.1; read them back at load.
5. Epochs and votes: `sentinelVoteLeader` rules (7.2), majority `max(quorum, voters/2+1)` (7.3), `failover_epoch = ++current_epoch`, `failover_start_time = now + rand(0..999)`, retry gate `2 * failover_timeout`, election timeout `min(10000, failover_timeout)`.
6. SDOWN/ODOWN rules (section 6) with the same numbers, including the `ask_period * 5` staleness clear and TILT.
7. Failover state machine and replica selection (sections 8-9), using `SLAVEOF`-based transactions so that Valkey and Hotstash data nodes both behave. The coordinated path (`FAILOVER TO`) **[V9]** is optional; without it, `SENTINEL FAILOVER name COORDINATED` should simply not be offered.
8. Event publication (section 12) with exact payloads, because clients and tooling parse `+switch-master`, `+sdown`, `+odown`, `+failover-end`.

---

## Notes on uncertainty

- Tie-breaking among equal vote counts depends on dictionary iteration order (`sentinel.c:4716-4725`), so it is not specified.
- The behaviour of coordinated failover when the first vote round fails (single forced ask, no retry unless the primary is still `S_DOWN`) is inferred from reading the code, not from a test.
- The version in which `sentinel_total_tilt`, `SENTINEL CONFIG`'s `loglevel`, the control-character argument validation (`sentinel.c:3093-3112`) and `SENTINEL INFO-CACHE` multi-name support appeared cannot be read from the source and is marked **[?ver]**; all are present in 9.1.3.
- The set of top-level commands in 5.1 comes from grepping `commands/*.json` for the `SENTINEL` flag, not from reading `commands.def` row by row.
- "Server" section field names other than `run_id`, `tcp_port`, `server_mode` were not enumerated.

## 14. Findings from live tests

Live tests of Valkey Sentinel 9.1.3 (3 Sentinels, quorum 2, down-after 2000 ms) against Hotstash nodes, 2026-11-17. Full detail in `sentinel-trial.md`. These findings define what a data node must do to be managed (section 13a) and confirm the behaviour described above.

- Confirmed on the wire: the 8-field hello payload (for example `127.0.0.1,7510,<runid>,2,mymaster,127.0.0.1,7500,0`) published every 2 s to the primary and every replica by every Sentinel, one `SUBSCRIBE __sentinel__:hello` per Sentinel per node, `CLIENT SETNAME sentinel-<runid8>-cmd|-pubsub` on both links, `PING` about every second, plain `INFO` every 10 s.
- Confirmed: role-change commands are sent as one `MULTI`, `SLAVEOF host port|NO ONE`, `CONFIG REWRITE`, `CLIENT KILL TYPE normal`, `CLIENT KILL TYPE pubsub`, `EXEC` block with replies ignored (`sentinel.c:4868-4935`). A node that refuses any line (Hotstash refuses `CLIENT KILL` at queue time and `SLAVEOF` at `EXEC`) leaves the failover stuck in `wait-promotion` until `-failover-abort-slave-timeout` after `failover-timeout`, then retried after `2 x failover-timeout`.
- Confirmed: an empty `run_id` (Hotstash `INFO` has none) leaves `runid` blank in `SENTINEL master|replicas` and does not prevent SDOWN, ODOWN, election (`+vote-for-leader`, `+elected-leader`) or `+selected-slave`.
- Timings seen with Valkey nodes: `+sdown` 2.1 s after the kill, `+odown` within 0.1 s, `+switch-master` 2.2 s later (4 s total from the kill to the new address).
- TILT: a host stall over 2 s put all three Sentinels in TILT for 30 s (`+tilt`/`-tilt`), during which no SDOWN, election or failover step ran; an election started right after TILT found no usable replica (`-failover-abort-no-good-slave`) because replica INFO was stale. An implementation must keep this rule (section 6) or it will misbehave on a stalled host.
- Not exercised: a mixed quorum of Hotstash and Valkey Sentinels, `is-master-down-by-addr` between implementations, coordinated failover.

