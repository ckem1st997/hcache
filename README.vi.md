# HCache: mã thử nghiệm giai đoạn 0 (Spike A và Spike B)

Đây là mã dùng để đo và quyết định, không phải sản phẩm. Nó sẽ bị bỏ đi sau giai đoạn 0; chỉ kết luận và bài học được mang sang giai đoạn sau.

- **Spike A:** server RESP tối giản viết theo sáu nguyên tắc kiến trúc của kế hoạch bản 2 (tài liệu nội bộ), kèm bản nhúng và công cụ phát tải cho bảy ca xấu.
- **Spike B:** hai nguyên mẫu bảng băm (kiểu con trỏ như Redis, và kiểu gọn) để đo số byte mỗi khóa ở 10 triệu khóa.

## Chạy nhanh bằng Docker

Image cho linux/amd64 và linux/arm64 được build từ nhánh `main` và đăng lên GitHub Container Registry:

```bash
docker run -d --name hcache -p 6379:6379 -v hcache-data:/data \
  -e HCACHE_REQUIREPASS='mat-khau-cua-ban' ghcr.io/ckem1st997/hcache:latest
```

Rồi kết nối bằng client Redis hay Valkey bất kỳ, ví dụ `valkey-cli -a 'mat-khau-cua-ban' PING`.

- Container nghe trên mọi địa chỉ, nên không chịu chạy nếu thiếu mật khẩu.
- Dữ liệu nằm trong volume `/data`. `docker stop` ghi hết thay đổi rồi thoát gọn.
- Thiết lập truyền bằng biến môi trường, ví dụ `-e HCACHE_WORKERS=4`; danh sách đủ ở mục [Biến môi trường](#biến-môi-trường). Số liệu Prometheus: `-p 9121:9121 -e HCACHE_METRICS_PORT=9121`.
- Tag: `latest` và `main` theo nhánh main, `sha-<commit>` ghim một bản build. Chưa có bản phát hành.
- Trên Kubernetes, Helm chart ở `deploy/helm/hcache` dùng image này.

## Biến môi trường

Mọi thiết lập đều truyền được bằng cờ dòng lệnh hoặc bằng biến môi trường: `HCACHE_` cộng tên cờ viết hoa, gạch ngang đổi thành gạch dưới. Khi có cả hai, dòng lệnh thắng. Giá trị sai hoặc cờ lạ làm server không khởi động.

| Biến môi trường | Cờ | Mặc định | Tác dụng |
|---|---|---|---|
| `HCACHE_REQUIREPASS` | `--requirepass` | không có | Mật khẩu của người dùng `default`. Nên dùng biến: server xóa nó khỏi môi trường của mình ngay sau khi đọc, còn dòng lệnh thì người dùng khác trên máy đọc được. |
| `HCACHE_BIND` | `--bind` | `127.0.0.1` | Địa chỉ để nghe. |
| `HCACHE_PORT` | `--port` | `6379` | Cổng RESP. `0` kèm cổng TLS nghĩa là chỉ nghe TLS. |
| `HCACHE_PROTECTED_MODE` | `--protected-mode` | `yes` | `yes` từ chối nghe ngoài loopback khi chưa có mật khẩu. |
| `HCACHE_WORKERS` | `--workers` | số CPU | Số luồng phục vụ kết nối. `1` chạy mọi lệnh trên một luồng. |
| `HCACHE_SHARDS` | `--shards` | `64` | Số shard mỗi database. |
| `HCACHE_DATABASES` | `--databases` | `16` | Số database cho `SELECT`. |
| `HCACHE_HASH_TAGS` | `--hash-tags` (không có giá trị) | `no` | `yes` (hoặc có cờ) xếp `a{tag}b` theo `tag`, như Redis Cluster. |
| `HCACHE_MAXMEMORY` | `--maxmemory` | `auto` | Giới hạn bộ nhớ: số byte, hoặc `256mb`, `1gb`. `auto`: khi container có giới hạn bộ nhớ, chừa một phần tư (ít nhất 128 MiB, nhiều nhất một nửa) cho phần còn lại của tiến trình; không có giới hạn thì không giới hạn. |
| `HCACHE_MAXMEMORY_POLICY` | `--maxmemory-policy` | `noeviction` | `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`. |
| `HCACHE_DATA` | `--data` | không có | Tệp dữ liệu. Không có thì khởi động lại là mất hết. |
| `HCACHE_APPENDFSYNC` | `--appendfsync` | `everysec` | `always` chỉ trả lời lệnh ghi khi nó đã xuống đĩa; `everysec` đồng bộ mỗi `--sync-interval-ms`; `no` để hệ điều hành lo. |
| `HCACHE_SYNC_INTERVAL_MS` | `--sync-interval-ms` | `100` | Chu kỳ đồng bộ của `everysec`, tính bằng mili giây. |
| `HCACHE_MEMORY_DBS` | `--memory-dbs` | không có | Các database không ghi vào tệp dữ liệu, ví dụ `0,2`. |
| `HCACHE_DIR` | `--dir` | không có | Thư mục của tệp RDB mà `SAVE`, `BGSAVE` ghi ra và lần khởi động với database rỗng nạp vào. |
| `HCACHE_DBFILENAME` | `--dbfilename` | `dump.rdb` | Tên tệp RDB. |
| `HCACHE_TIMEOUT` | `--timeout` | không có | Đóng client không gửi gì trong chừng ấy giây. |
| `HCACHE_THP` | `--thp` | `off` | `off` tắt transparent huge pages cho tiến trình; `inherit` giữ thiết lập của hệ thống. |
| `HCACHE_TLS_PORT` | `--tls-port` | không có | Cổng TLS. |
| `HCACHE_TLS_CERT_FILE` | `--tls-cert-file` | không có | Chứng chỉ của server (PEM). |
| `HCACHE_TLS_KEY_FILE` | `--tls-key-file` | không có | Khóa riêng của server (PEM). |
| `HCACHE_TLS_CA_CERT_FILE` | `--tls-ca-cert-file` | không có | CA mà chứng chỉ client phải thuộc về. |
| `HCACHE_TLS_AUTH_CLIENTS` | `--tls-auth-clients` | `yes` khi có CA | `yes`, `optional` hoặc `no`. |
| `HCACHE_MEMCACHED_PORT` | `--memcached-port` | tắt | Cổng cho giao thức text của Memcached (database 0). |
| `HCACHE_METRICS_PORT` | `--metrics-port` | tắt | Cổng cho `GET /metrics` của Prometheus. |
| `HCACHE_CLUSTER_ENABLED` | `--cluster-enabled` | `no` | `yes` trả lời như một Redis Cluster một nút. |
| `HCACHE_CLUSTER_DATABASES` | `--cluster-databases` | `1` | Số database ở chế độ cluster. |
| `HCACHE_ENABLE_DEBUG_COMMAND` | `--enable-debug-command` | `no` | `no`, `yes`, hoặc `local` (chỉ kết nối từ chính máy này). |

Thiết lập nào `CONFIG SET` nhận thì cũng truyền được lúc khởi động theo cùng cách, ví dụ `HCACHE_NOTIFY_KEYSPACE_EVENTS=KEA`, `HCACHE_SLOWLOG_MAX_LEN=512`, `HCACHE_LAZYFREE_LAZY_USER_DEL=no`.

Trong image Docker, dòng lệnh đã đặt sẵn `--bind 0.0.0.0 --data /data/hcache.aof`, nên `HCACHE_BIND` và `HCACHE_DATA` không có tác dụng ở đó trừ khi bạn thay lệnh. Các biến mà Kubernetes hay Docker link tự tạo cho service tên `hcache` (`HCACHE_SERVICE_*`, `HCACHE_PORT_*`, `HCACHE_PORT=tcp://...`) bị bỏ qua.

## Lệnh

Máy này phải dùng toolchain MSVC; toolchain mặc định (gnu) không build được. Mọi lệnh chạy trong thư mục `spike`.

```bash
cargo +stable-x86_64-pc-windows-msvc test --workspace --no-fail-fast
```

```bash
cargo +stable-x86_64-pc-windows-msvc clippy --workspace --all-targets -- -D warnings
```

```bash
cargo +stable-x86_64-pc-windows-msvc build --release --workspace
```

Chạy server (mặc định `127.0.0.1:6379`, 64 shard):

```bash
target/release/hcache-server --port 6400 --workers 4
```

Số database mặc định là 16; `--databases N` đổi (1 đến 256).

Đặt mật khẩu cho người dùng `default` qua biến môi trường (server xóa biến này khỏi tiến trình ngay sau khi đọc):

```bash
HCACHE_REQUIREPASS='mật-khẩu-của-bạn' target/release/hcache-server --port 6400
```

Tham số `--requirepass` cũng dùng được, nhưng người dùng khác trên cùng máy đọc được dòng lệnh. Server nghe ở địa chỉ không phải loopback mà không có mật khẩu thì in cảnh báo khi khởi động.

Lưu đĩa và độ bền (cần `--data <tệp>`):

```bash
target/release/hcache-server --port 6400 --data hcache.hc --appendfsync always
```

| `--appendfsync` | Ý nghĩa | Mất gì khi máy sập |
|---|---|---|
| `always` | Lệnh ghi chỉ được trả lời sau khi thay đổi đã `fsync` xuống đĩa. Nhiều kết nối ghi cùng lúc dùng chung một lần `fsync` | Không mất lệnh nào đã được trả lời |
| `everysec` (mặc định) | Ghi và `fsync` theo chu kỳ `--sync-interval-ms` (mặc định 100 ms, không phải 1 giây như tên gọi) | Tối đa một chu kỳ |
| `no` | Ghi theo chu kỳ, để hệ điều hành tự quyết lúc `fsync` | Tùy hệ điều hành |

`CONFIG SET appendfsync` đổi chế độ lúc đang chạy. `SAVE` chờ đến khi mọi thay đổi đã nằm trên đĩa rồi mới trả `OK`.

Chọn cách chạy:

- **Biến môi trường.** Mỗi cờ đều có biến `HCACHE_` cộng tên cờ viết hoa, gạch ngang thành gạch dưới: `HCACHE_WORKERS=4` là `--workers 4`, `HCACHE_HASH_TAGS=yes` là `--hash-tags`. Nếu cả hai cùng có thì dòng lệnh thắng. Biến để trống được coi như không đặt.
- **Thiết lập lúc khởi động.** Mọi thiết lập mà `CONFIG SET` nhận đều đặt được bằng `--tên giá-trị` hoặc `HCACHE_TÊN=giá-trị`, ví dụ `--lazyfree-lazy-user-del no`, `--notify-keyspace-events KEA`, `HCACHE_SLOWLOG_MAX_LEN=512`. Giá trị sai thì server không khởi động. Tên sai trên dòng lệnh cũng vậy. Còn biến môi trường có tên lạ chỉ bị cảnh báo, vì có thể nó thuộc chương trình khác.
- **Đơn luồng hay đa luồng.** `--workers 1` chạy mọi lệnh trên một luồng; số lớn hơn thì chia kết nối cho nhiều luồng. Không có chế độ luồng riêng cho từng database hay từng khóa; lý do nằm ở mục "Chọn cách chạy" của kế hoạch.
- **Xóa bất đồng bộ.** `UNLINK`, cùng `FLUSHALL` và `FLUSHDB` với `ASYNC`, giải phóng giá trị lớn (trên 64 phần tử) ở một luồng nền thay vì bắt các client khác chờ. `DEL`, `FLUSHALL` không kèm tùy chọn, eviction, hết hạn và ghi đè làm theo năm thiết lập `lazyfree-lazy-user-del`, `-user-flush`, `-eviction`, `-expire`, `-server-del`, mặc định `yes` như Valkey. `INFO memory` có `lazyfree_pending_objects` và `lazyfreed_objects`.
- **Độ bền theo từng lệnh.** Chạy `--appendfsync everysec` cho nhanh, rồi sau các lệnh ghi quan trọng gửi `WAITAOF 1 0 <ms>`: lệnh này trả `[1, 0]` khi mọi thay đổi trước nó đã nằm trên đĩa. `WAIT <n> <ms>` chờ đến khi `n` replica đã nhận mọi lệnh ghi trước nó (mục "Sao chép").
- **Database chỉ trong RAM.** `--memory-dbs 0,2` giữ các database đó ngoài tệp dữ liệu: dùng làm cache bên cạnh các database được lưu. Với `--appendfsync always`, lệnh ghi vào chúng không phải chờ đĩa, trừ lệnh chạm nhiều database (`FLUSHALL`, `SWAPDB`, `MOVE`). Lưu ý: đổi một database đã có dữ liệu trên đĩa sang chỉ trong RAM thì các khóa cũ của nó bị xóa khỏi tệp ngay lúc mở, và mất hẳn.

Thời hạn kết nối: `--timeout <giây>` (hoặc `CONFIG SET timeout`) đóng kết nối không gửi gì quá lâu; mặc định 0 là không đóng. Kết nối đang chờ (`BLPOP`…), đang nghe kênh hoặc đang `MONITOR` không bị tính. Kết nối không chịu đọc dữ liệu trả về trong 60 giây liền thì bị đóng.

TLS (chứng chỉ và khóa ở dạng PEM):

```bash
target/release/hcache-server --port 6400 --tls-port 6401 --tls-cert-file server.crt --tls-key-file server.key
```

- `--port 0` cùng `--tls-port` thì chỉ nghe TLS.
- `--tls-ca-cert-file ca.crt` bắt client trình chứng chỉ do CA đó ký. Thêm `--tls-auth-clients optional` thì client không có chứng chỉ vẫn vào được, còn chứng chỉ sai vẫn bị từ chối; `--tls-auth-clients no` thì không hỏi chứng chỉ.
- Chỉ nhận TLS 1.2 và 1.3. Phía TLS dùng thư viện `rustls` với `ring`.
- Kết nối TLS phải bắt tay xong trong 10 giây.

Chạy trong container (Docker, Kubernetes, k3s):

- `--maxmemory auto` là mặc định. Khi tiến trình chạy dưới giới hạn bộ nhớ (cgroup của container: `memory.max`, `memory.high`, hoặc `memory.limit_in_bytes` của cgroup v1), server để một phần tư giới hạn cho phần còn lại của tiến trình (ít nhất 128 MiB nhưng không quá một nửa) và lấy phần còn lại làm `maxmemory`. Ví dụ giới hạn 1 GiB cho 768 MiB dữ liệu. Không có giới hạn thì không có `maxmemory`, như trước. Đặt `--maxmemory` sát giới hạn (trên 90%) thì server in cảnh báo.
- `--thp off` là mặc định: tắt transparent huge pages cho tiến trình (`prctl`), như Redis và Valkey. `--thp inherit` để nguyên theo hệ thống. Đo trên WSL2 (THP kiểu `madvise`), 1 triệu khóa: để THP bật thì nhanh hơn 1,5 đến 2% nhưng tốn 180 byte mỗi khóa (RSS) thay vì 147.
- `SIGTERM` hoặc `SIGINT`: server ngừng nhận kết nối, các worker dừng, mọi thay đổi được ghi và `fsync` xuống đĩa, rồi thoát với mã 0. Quá 20 giây thì thoát luôn với mã 1; tín hiệu thứ hai, hoặc tín hiệu đến khi đang nạp tệp dữ liệu, thì thoát ngay. Trên Windows server vẫn chạy đến khi bị tắt như cũ.
- `--protected-mode yes` là mặc định: server từ chối khởi động nếu nghe ở địa chỉ không phải loopback mà không có mật khẩu (trước đây chỉ in cảnh báo). `--protected-mode no` để bỏ qua.
- `hcache-server healthcheck [--port N]` thoát với mã 0 nếu server trên máy này trả lời `PING` (cả khi server đòi mật khẩu). Không có `--port` thì nó hỏi cổng trong `HCACHE_PORT`, không có nữa thì 6379. Dùng cho health check trong image không có shell.
- `Dockerfile` (build trên Debian 13, chạy trên `gcr.io/distroless/cc-debian13:nonroot`, người dùng 65532, dữ liệu ở `/data`) và `deploy/k8s.yaml` (StatefulSet một bản, PVC, probe, `securityContext` theo mức restricted, request bằng limit). CI build image cho amd64 và arm64, kiểm từng bản khởi động, ghi và đọc một khóa, dừng gọn, rồi mới đăng lên. `deploy/k8s.yaml` chưa được chạy thử.

```bash
docker build -t hcache .
```

Build có PGO (Linux, cần component `llvm-tools` của rustup), ra `target/pgo/hcache-server`:

```bash
bash bench/pgo_build.sh
```

Phát tải (xem `hload` không tham số để biết đủ tùy chọn):

```bash
target/release/hload baseline --addr 127.0.0.1:6400 --clients 16 --secs 5
```

Spike B, đủ 16 cấu hình ở 10 triệu khóa (mất vài phút, cần khoảng 2,5 GB RAM):

```bash
target/release/spikeb --all
```

## Các crate

| Crate | Nội dung |
|---|---|
| `hcache-core` | Bộ phân tích RESP, bảng lệnh, shard, cổng thực thi, engine, Lua |
| `hcache-server` | Server TCP: vòng lặp sự kiện `mio`, một luồng riêng cho việc độc quyền |
| `hcache` | Bản nhúng: `Db::open`, `Session::execute`, `Db::listen` |
| `hload` | Công cụ phát tải cho bảy ca xấu; nói RESP thuần nên chạy được với cả Valkey |
| `spikeb` | Spike B; kết quả ở [RESULTS.md](crates/spikeb/RESULTS.md) |

## Lệnh được hỗ trợ

| Nhóm | Lệnh |
|---|---|
| Kết nối | `PING`, `ECHO`, `QUIT`, `INFO` (nhận tên mục: `INFO replication memory`), `HELLO` (RESP2 và RESP3, có `AUTH`), `RESET`, `TIME`, `CLIENT SETNAME`/`GETNAME`/`ID`/`INFO`/`LIST`/`SETINFO`/`KILL`/`PAUSE`/`UNPAUSE` |
| Database | `SELECT`, `MOVE`, `COPY ... DB`, `SWAPDB`, `FLUSHDB`, `FLUSHALL` (cả hai nhận `ASYNC`/`SYNC`), `DBSIZE` (16 database mặc định) |
| Người dùng | `AUTH`, `ACL SETUSER`/`GETUSER`/`DELUSER`/`LIST`/`USERS`/`WHOAMI`/`CAT`/`GENPASS`/`DRYRUN`, `CONFIG SET requirepass` |
| Mô tả lệnh | `COMMAND`, `COMMAND COUNT`/`INFO`/`DOCS`/`LIST`/`GETKEYS` (228 lệnh) |
| Chuỗi | `GET`, `SET` (với `NX`, `XX`, `IFEQ`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`), `DELIFEQ`, `MSETEX`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `GETRANGE`, `SUBSTR`, `SETRANGE`, `MGET`, `MSET`, `MSETNX` |
| Bitmap | `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`, `BITFIELD` (`GET`, `SET`, `INCRBY`, `OVERFLOW WRAP`/`SAT`/`FAIL`), `BITFIELD_RO` |
| GEO | `GEOADD` (với `NX`, `XX`, `CH`), `GEOPOS`, `GEODIST`, `GEOHASH`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEORADIUS`, `GEORADIUSBYMEMBER` và hai dạng `_RO` |
| Hash | `HSET`, `HMSET`, `HSETNX`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY`, `HINCRBYFLOAT`, `HGETALL`, `HKEYS`, `HVALS`, `HSTRLEN`, `HRANDFIELD`, `HSCAN`, `HGETDEL`, `HGETEX`, `HSETEX`; thời hạn cho từng field: `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT` (với `NX`, `XX`, `GT`, `LT`), `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST` |
| HyperLogLog | `PFADD`, `PFCOUNT`, `PFMERGE` |
| List | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, `LRANGE`, `LSET`, `LREM`, `LTRIM`, `LINSERT`, `LPOS`, `LMOVE`, `RPOPLPUSH`, `LMPOP` |
| Set | `SADD`, `SREM`, `SCARD`, `SISMEMBER`, `SMISMEMBER`, `SMEMBERS`, `SPOP`, `SRANDMEMBER`, `SMOVE`, `SINTER`, `SUNION`, `SDIFF`, các dạng `...STORE`, `SINTERCARD`, `SSCAN` |
| Sorted set | `ZADD` (đủ cờ), `ZREM`, `ZCARD`, `ZSCORE`, `ZMSCORE`, `ZINCRBY`, `ZRANK`, `ZREVRANK`, `ZCOUNT`, `ZLEXCOUNT`, `ZRANGE` (với `BYSCORE`, `BYLEX`, `REV`, `LIMIT`), các lệnh `ZRANGEBY...` và `ZREVRANGE...` cũ, `ZRANGESTORE`, `ZREMRANGEBYRANK`/`BYSCORE`/`BYLEX`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `ZRANDMEMBER`, `ZUNION`, `ZINTER`, `ZDIFF`, các dạng `...STORE`, `ZINTERCARD`, `ZSCAN` |
| Stream | `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XDEL`, `XTRIM`, `XSETID`, `XREAD`, `XGROUP` (`CREATE`, `DESTROY`, `CREATECONSUMER`, `DELCONSUMER`, `SETID`), `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM`, `XINFO` |
| Lệnh chờ | `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `XREAD BLOCK`, `XREADGROUP BLOCK` |
| Khóa | `DEL`, `UNLINK`, `EXISTS`, `TOUCH`, `TYPE`, `KEYS`, `SCAN` (với `MATCH`, `COUNT`, `TYPE`), `RANDOMKEY`, `RENAME`, `RENAMENX`, `COPY`, `OBJECT` (kể cả `FREQ`), `SORT` (với `BY`, `GET`, `LIMIT`, `ALPHA`, `STORE`), `SORT_RO`, `DBSIZE`, `FLUSHALL`, `FLUSHDB` |
| Thời hạn | `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` (với `NX`, `XX`, `GT`, `LT`), `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST` |
| Giao dịch | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| Script | `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT LOAD`/`EXISTS`/`FLUSH`/`KILL`; trong Lua có `cjson`, `cmsgpack`, `struct`, `bit` và các hàm `redis.*` |
| Hàm Lua | `FUNCTION LOAD` (có `REPLACE`)/`DELETE`/`FLUSH`/`LIST`/`STATS`/`KILL`, `FCALL`, `FCALL_RO`; thư viện đăng ký hàm bằng `redis.register_function` |
| Pub/sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB CHANNELS`/`NUMSUB`/`NUMPAT`; theo shard: `SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, `PUBSUB SHARDCHANNELS`/`SHARDNUMSUB` |
| Thông báo keyspace | `CONFIG SET notify-keyspace-events <cờ>` (cờ `K E g $ l s h z x e t A`), phát lên `__keyspace@<db>__:<khóa>` và `__keyevent@<db>__:<sự kiện>` |
| Sao chép khóa, đo đạc | `DUMP`, `RESTORE` (với `REPLACE`, `ABSTTL`, `IDLETIME`, `FREQ`), `MEMORY USAGE` (với `SAMPLES`), `MEMORY DOCTOR`/`PURGE`/`MALLOC-STATS`, `LCS` (với `LEN`, `IDX`, `MINMATCHLEN`, `WITHMATCHLEN`) |
| Lưu đĩa | `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `LASTSAVE` (cần `--data <tệp>` hoặc `--dir`/`--dbfilename`), `WAITAOF` (chờ các lệnh ghi trước nó xuống đĩa) |
| Sao chép | `REPLICAOF`/`SLAVEOF`, `ROLE`, `WAIT`, `FAILOVER` (với `TO`, `TIMEOUT`, `FORCE`, `ABORT`), và phía giao thức `SYNC`, `PSYNC` (cả `PSYNC ... FAILOVER`), `REPLCONF` |
| Cluster một node | `CLUSTER INFO`/`MYID`/`NODES`/`SLOTS`/`SHARDS`/`KEYSLOT`/`COUNTKEYSINSLOT`/`GETKEYSINSLOT`, `READONLY`, `READWRITE`, `ASKING` (khi chạy `--cluster-enabled yes`) |
| Vận hành | `SHUTDOWN`, `COMMANDLOG`, `LOLWUT`, `DEBUG` (một tập con, cần `--enable-debug-command`) |
| Bộ nhớ | `--maxmemory`, `--maxmemory-policy` (tám chính sách, có `allkeys-lfu` và `volatile-lfu`), `CONFIG GET` |
| Theo dõi vận hành | `SLOWLOG GET`/`LEN`/`RESET` (`CONFIG SET slowlog-log-slower-than`, `slowlog-max-len`), `LATENCY` (`LATEST`, `HISTORY`, `HISTOGRAM`, `DOCTOR`, `GRAPH`, `RESET`), `MONITOR` |
| Cache phía client | `CLIENT TRACKING ON`/`OFF` (với `REDIRECT`, `BCAST`, `PREFIX`, `OPTIN`, `OPTOUT`, `NOLOOP`), `CLIENT CACHING`, `CLIENT TRACKINGINFO`, `CLIENT GETREDIR` |
| Bloom | `BF.ADD`, `BF.MADD`, `BF.EXISTS`, `BF.MEXISTS`, `BF.RESERVE`, `BF.INSERT`, `BF.INFO`, `BF.CARD`, `BF.LOAD` (như valkey-bloom 1.0.1) |
| JSON | `JSON.SET`, `GET`, `MGET`, `MSET`, `DEL`, `FORGET`, `MERGE`, `TYPE`, `CLEAR`, `TOGGLE`, `NUMINCRBY`, `NUMMULTBY`, `STRAPPEND`, `STRLEN`, `ARRAPPEND`, `ARRINDEX`, `ARRINSERT`, `ARRLEN`, `ARRPOP`, `ARRTRIM`, `OBJKEYS`, `OBJLEN`, `RESP`, `DEBUG` (như valkey-json, tập con JSONPath ở [docs/json-path.md](docs/json-path.md)) |

`CLIENT REPLY`, `UNBLOCK`, `NO-EVICT`, `NO-TOUCH` báo lỗi rõ ràng.

**Database.** Mỗi database là một không gian khóa riêng; mỗi kết nối nhớ database của nó. `SWAPDB` đổi chỗ hai database ngay lập tức, kể cả thời hạn và tệp dữ liệu. Trong script, `SELECT`, `MOVE`, `FLUSHDB` và `COPY ... DB` bị từ chối. Mỗi database tốn sẵn một dãy shard rỗng (16 × 64 theo mặc định). Tệp dữ liệu chỉ mở được khi số database cấu hình không ít hơn số database tệp đã dùng.

**Xác thực và ACL.**

- Khi `default` có mật khẩu, kết nối chưa đăng nhập chỉ chạy được `AUTH`, `HELLO`, `QUIT`, `RESET`, và mỗi lệnh của nó bị giới hạn ở 16 tham số và 16 KiB.
- Đặt mật khẩu lúc server đang chạy thì mọi kết nối chưa `AUTH` bị khóa ngay, kể cả kết nối vừa đặt mật khẩu.
- Người dùng có giới hạn bị kiểm ở từng lệnh: tên lệnh hoặc nhóm (`+get`, `-@dangerous`, `+config|get`), mẫu khóa (`~app:*`, `%R~`, `%W~`), mẫu kênh (`&news:*`). Kiểm cả lúc xếp lệnh vào `MULTI` và ở từng `redis.call` trong script.
- Xóa người dùng thì các kết nối của họ bị đóng ngay, kể cả kết nối đang chờ hoặc đang nghe kênh. Thu hồi kênh thì người đang nghe kênh đó bị đóng.
- Quyền của người dùng đổi giữa `MULTI` và `EXEC` thì `EXEC` bị hủy.
- Mật khẩu chỉ được lưu dưới dạng băm SHA-256. `CONFIG GET requirepass` luôn trả chuỗi rỗng.

- Khi có `--data`, mọi người dùng (kể cả `default` và mã băm mật khẩu của họ) được lưu trong tệp dữ liệu và có lại sau khi khởi động lại. Mật khẩu đưa qua biến môi trường hoặc `--requirepass` lúc khởi động thay cho mật khẩu của `default` đã lưu.

Khác Redis: không có selector (`(...)`); không có tệp ACL riêng (`ACL SAVE`/`LOAD` báo lỗi), người dùng nằm trong tệp dữ liệu; `ACL LOG` luôn rỗng; quy tắc khóa chặt hơn (lệnh ghi cần mẫu cho cả đọc lẫn ghi); người dùng bị tắt (`off`) mất quyền ngay trên kết nối đang mở; `AUTH` và `ACL` không được nằm trong `MULTI`; bản nhúng (`Session`) có toàn quyền và không cần đăng nhập.

**Lệnh chờ.** Chỉ kết nối TCP ngoài `MULTI` mới chờ. Trong `MULTI`, trong script và ở bản nhúng, lệnh chạy một lần rồi trả về rỗng nếu chưa có dữ liệu. Người chờ lâu nhất được phục vụ trước. `FLUSHALL`, `DEL` hay việc khóa đổi kiểu đánh thức người đang chờ `XREADGROUP` bằng một lỗi.

**RESP3.** Ngoài giá trị rỗng và map, server trả kiểu double cho điểm của sorted set, kiểu set cho `SMEMBERS`/`SINTER`/`SUNION`/`SDIFF`/`SPOP`, mảng các cặp cho `ZRANGE ... WITHSCORES`, và map cho `XREAD`, `XREADGROUP`, `XINFO STREAM`, kiểu double cho tọa độ của `GEOPOS` và `GEOSEARCH ... WITHCOORD`. Kiểu push dùng cho pub/sub và cho thông báo `invalidate` của `CLIENT TRACKING`.

Khóa hết hạn bị xóa theo hai cách: lệnh nào chạm vào nó cũng coi nó như không tồn tại, và server quét nền để thu những khóa không ai chạm. Một khối `MULTI`/`EXEC` chạy độc quyền trên cả nút, nên không lệnh nào khác chen vào giữa.

Bảng đối chiếu tính năng với Redis, Valkey và các cache khác, cùng danh sách việc còn thiếu theo mức ưu tiên, nằm ở sổ theo dõi giai đoạn 0 (tài liệu nội bộ).

## Tệp RDB

`--dir <thư mục>` và `--dbfilename <tên>` (mặc định `dump.rdb`) đặt một tệp RDB như Redis. `SAVE` và `BGSAVE` ghi nó: RDB 11 như Redis 7.2, hoặc RDB 80 như Valkey 9 khi có field của hash mang thời hạn. Lúc khởi động, tệp được nạp nếu database còn rỗng. Redis đến 7.2 và mọi bản Valkey đọc và ghi được tệp này; `tests/rdb.rs` ghi tệp cho Valkey 9.1.3 đọc rồi đọc lại tệp Valkey ghi, và cho dữ liệu hỏng ngẫu nhiên vào để chắc rằng server không panic, không nạp dở. Tệp RDB cũng là thứ primary gửi cho replica khi đồng bộ toàn bộ.

## Sao chép

`REPLICAOF <host> <port>` (hay `SLAVEOF`) biến nút thành replica của một primary; `REPLICAOF NO ONE` đưa nó lên làm primary. Giao thức là của Redis và Valkey, nên primary và replica có thể là HCache hay Valkey theo cả hai chiều. Ngày 2026-11-16 đã chạy thật với Valkey 9.1.3: HCache làm replica của Valkey và ngược lại, có mật khẩu, có lệnh ghi trong lúc đồng bộ, cùng `valkey-cli --rdb` và `valkey-cli --replica` với HCache.

- Lần đầu replica nhận một tệp RDB rồi luồng lệnh ghi. Mất kết nối thì nó nối tiếp từ chỗ dừng, lấy phần thiếu trong backlog của primary (`repl-backlog-size`, mặc định 10 MiB); quá xa thì đồng bộ lại từ đầu. Replica của một replica cũng được.
- Luồng gửi đi là kết quả của lệnh, như Redis từ 7.0: lệnh phụ thuộc đồng hồ hay số ngẫu nhiên đi dưới dạng trạng thái nó để lại (`PEXPIREAT`, `SET ... PXAT`, `RESTORE ... ABSTTL`), khóa hết hạn hay bị thu hồi đi dưới dạng `DEL`. `MULTI` và script đi trong một khối `MULTI`/`EXEC`.
- Primary có mật khẩu: đặt `CONFIG SET masterauth` (và `masteruser` nếu cần) trên replica. `CONFIG GET` không bao giờ hiện mật khẩu này.
- Replica từ chối lệnh ghi (`READONLY`); `replica-read-only` luôn là `yes`.
- `WAIT <n> <ms>` chờ đến khi `n` replica xác nhận đã có mọi lệnh ghi trước nó.
- `FAILOVER [TO <host> <port>] [TIMEOUT <ms>] [FORCE]` và `FAILOVER ABORT` như Valkey: primary dừng nhận lệnh ghi (lệnh đọc vẫn chạy), chờ replica theo kịp, trao vai primary bằng `PSYNC ... FAILOVER` rồi tự làm replica của nút mới. Kết nối đang chờ (`BLPOP`…) nhận lỗi `UNBLOCKED`. Hết giờ mà không có `FORCE` thì hủy; `INFO replication` có `master_failover_state`.
- `ROLE` và `INFO replication` trả các trường như Redis; `CLIENT KILL TYPE master`/`replica` cắt liên kết.

**Sentinel.** Valkey Sentinel 9.1.3 thật quản lý được nhóm nút HCache trọn vòng (2026-11-17): phát hiện, failover tự động khi primary chết, đưa primary cũ về làm replica khi nó chạy lại, `SENTINEL FAILOVER` và `SENTINEL FAILOVER ... COORDINATED`. Cùng kịch bản với nút Valkey cho kết quả và thời gian như nhau. Những gì Sentinel cần đều có:
- khối đổi vai `MULTI`/`SLAVEOF`/`CONFIG REWRITE`/`CLIENT KILL`/`EXEC` chạy được trong giao dịch;
- `INFO` có `run_id` và nhận tên mục;
- `replica-priority`, `replica-announced`;
- `CLIENT PAUSE WRITE|ALL` và `FAILOVER` trong `MULTI`.

Kết quả và cách chạy lại (`bench/sentinel_trial.sh`) ở [docs/sentinel-trial.md](docs/sentinel-trial.md).

Chưa có: đồng bộ không qua đĩa theo kiểu tùy chọn của Redis, `repl-timeout` đổi được (cố định 60 giây), `min-replicas-to-write`, `replica-announce-ip`. Không có tệp cấu hình, nên `CONFIG REWRITE` báo lỗi như Valkey chạy không có tệp: nút khởi động lại lên làm primary cho đến khi Sentinel đưa nó về replica, khoảng 8–12 giây.

## Cổng Memcached

`--memcached-port N` (hoặc `HCACHE_MEMCACHED_PORT`) mở thêm một cổng nói giao thức văn bản của Memcached, trên cùng địa chỉ `--bind`:

```bash
target/release/hcache-server --port 6400 --memcached-port 11211
```

- **Lệnh:** `set`, `add`, `replace`, `append`, `prepend`, `cas`, `get`, `gets`, `gat`, `gats`, `delete`, `incr`, `decr`, `touch`, `flush_all` (cả dạng hẹn giờ), `version`, `verbosity`, `stats` (và `stats reset`), `quit`, cùng các lệnh meta `mg`, `ms`, `md`, `ma`, `mn`. Câu trả lời, kể cả câu báo lỗi, giống memcached 1.6.24 từng byte.
- **Dùng chung dữ liệu với RESP:** cổng đọc và ghi các khóa của database 0. Giá trị ghi với client flags bằng 0 là đúng chuỗi mà RESP đọc được; flags khác 0 thì giá trị có thêm 8 byte ở đầu (`\0MC\x01` rồi flags dạng big-endian), RESP cũng thấy 8 byte này. Khóa không phải chuỗi được coi như không có. `incr`, `append`, `cas` đọc rồi ghi có điều kiện (`SET ... IFEQ`) và thử lại khi có người ghi chen vào, nên nhiều client cùng `incr` không mất lần nào.
- **CAS** là mã băm xxh3 của giá trị đang lưu, không phải số đếm tăng dần: ghi lại đúng các byte cũ thì CAS không đổi.
- **Đăng nhập và quyền:** khi `default` có mật khẩu, kết nối phải đăng nhập như với `memcached -Y`: một lệnh `set` bất kỳ mà giá trị là `<tên> <mật khẩu>`. Người dùng và quyền là của ACL: `get` bị kiểm như `GET`, `delete` như `DEL`, `flush_all` như `FLUSHDB`; `stats` như `INFO`, `stats reset` như `CONFIG RESETSTAT`; bị từ chối thì nhận `SERVER_ERROR NOPERM ...`. Xóa người dùng thì kết nối của họ bị đóng. Một `flush_all` hẹn giờ chỉ chạy nếu đến giờ người hẹn vẫn còn và vẫn có quyền `FLUSHDB`. Kết nối phải đăng nhập mà sau 10 giây chưa đăng nhập thì bị đóng, để những kết nối không bao giờ đăng nhập không chiếm hết chỗ.
- **Không có TLS:** cổng này chỉ nói văn bản thường, nên `--memcached-port` bị từ chối khi khởi động nếu đi cùng `--port 0` (server chỉ nhận TLS); nếu không, cổng này sẽ thành đường vòng qua TLS.
- **Sao chép và độ bền:** lệnh ghi qua cổng này đi vào tệp dữ liệu, theo `appendfsync`, và sang replica như lệnh RESP. Trên replica, lệnh ghi nhận `SERVER_ERROR READONLY ...`; trong lúc `FAILOVER` thì chờ.
- **Kiểm thử:** `tests/memcached.rs` (9 test); một bộ so sánh gửi 311 yêu cầu (và 27 yêu cầu đăng nhập) tới memcached 1.6.24 thật và tới HCache, câu trả lời giống hệt; thư viện pymemcache 4.0 chạy cùng một kịch bản 45 bước trên cả hai, kết quả giống nhau.

Khác memcached:

- Chưa có giao thức nhị phân, UDP, SASL, lệnh `me`, `lru_crawler`, `slabs`, `watch`. `stats` chỉ có các mục cơ bản; `stats settings`, `items`, `slabs`, `sizes`, `conns` trả rỗng.
- Cờ meta chưa có: `h`, `l`, `x`, `E`, `R`, `N` của `ms`, `I` của `md`. Dùng chúng thì nhận `CLIENT_ERROR invalid flag`.
- Exptime lớn hơn 2^31: memcached cắt về 32 bit, HCache giữ nguyên (trần 2^40).
- Đăng nhập: mật khẩu là toàn bộ phần sau dấu cách đầu tiên, như memcached 1.6.45; memcached 1.6.24 chỉ lấy từ thứ hai. Trước khi đăng nhập, memcached chỉ giữ câu trả lời cuối của nhiều lệnh gửi liền nhau; HCache trả lời từng lệnh. Kết nối chưa đăng nhập sau 10 giây thì bị đóng; memcached không đặt hạn. Đăng nhập sai không đóng kết nối và không bị làm chậm, như memcached và như `AUTH` của RESP.
- `flush_all` xóa cả database 0, kể cả khóa do RESP ghi. Một `flush_all` hẹn giờ chỉ nằm trong bộ nhớ: server khởi động lại trước giờ đó thì nó mất.
- Bộ nhớ theo `maxmemory` và chính sách thu hồi của HCache, không có slab. Giá trị tối đa 1 MiB (như `-I 1m` mặc định), khóa tối đa 250 byte.
- Mỗi kết nối có một luồng riêng.

## Bloom và JSON

Hai họ lệnh này có sẵn trong server, không cần nạp module. Chúng theo valkey-bloom 1.0.1 và valkey-json (cùng tên lệnh, câu trả lời và câu báo lỗi), đối chiếu theo mã nguồn và bộ test của hai module, chưa chạy với module thật.

- **Bloom (`BF.*`):** cách tính kích thước, hàm băm và bố cục bit giống valkey-bloom. Vì vậy với `bf.bloom-use-random-seed no`, một khóa trả lời y như ở valkey-bloom, kể cả các lần dương tính giả. Thiết lập đọc và đổi qua `CONFIG GET`/`CONFIG SET`:
  - `bf.bloom-capacity` (mặc định 100), `bf.bloom-expansion` (2);
  - `bf.bloom-fp-rate` (0,01), `bf.bloom-tightening-ratio` (0,5);
  - `bf.bloom-memory-usage-limit` (128 MiB mỗi filter), `bf.bloom-use-random-seed` (yes).
  
  Mô tả hành vi theo từng mục ở [docs/bloom-spec.md](docs/bloom-spec.md).
- **JSON (`JSON.*`):** cả cú pháp đường dẫn cũ (`.a.b`) lẫn JSONPath (`$..a[?(@.x>1)]`). Tập con JSONPath được hỗ trợ, các giới hạn và mọi chỗ khác valkey-json ghi ở [docs/json-path.md](docs/json-path.md). Thiết lập: `json.max-document-size` (byte; mặc định 0 là không giới hạn) và `json.max-path-limit` (độ lồng, mặc định 128). `tests/json_upstream.rs` (151 test) là bộ test Python của valkey-json chuyển sang Rust. Bốn giới hạn valkey-json không có, thêm sau lượt rà bảo mật: công sức của một lần tìm theo đường (`..` lồng nhau), câu trả lời `JSON.GET` tối đa 512 MiB, một lệnh ghi thêm tối đa 512 MiB kể cả các bản sao ở mọi chỗ khớp, và số vượt số thực lớn nhất bị từ chối như RapidJSON. `BF.LOAD` từ client nhận tối đa 4096 filter con.
- **Tệp RDB và sao chép:** khóa Bloom và JSON được ghi vào RDB dưới kiểu module của valkey-bloom (`bloomfltr`, bản mã hóa 1) và valkey-json (`ReJSON-RL`, bản 3). Vì vậy một Valkey làm replica của HCache, hoặc đọc tệp RDB của HCache, phải nạp hai module đó. Định dạng viết theo mã nguồn của module, chưa đối chiếu với tệp do module thật ghi.

## Prometheus và Helm

`--metrics-port N` (hoặc `HCACHE_METRICS_PORT`) mở một cổng HTTP trên cùng địa chỉ `--bind`:
- `GET /metrics` trả số liệu của `INFO` dưới đúng tên và dạng mà redis_exporter dùng, nên dashboard và cảnh báo làm cho redis_exporter dùng được nguyên.
- Khi người dùng `default` có mật khẩu, cổng này đòi HTTP Basic với người dùng và mật khẩu của RESP, và người dùng phải có quyền `INFO`.
- Mỗi kết nối hỏi một lần rồi bị đóng.

```bash
target/release/hcache-server --port 6400 --metrics-port 9121
```

`deploy/helm/hcache` là một Helm chart tối giản gồm:
- StatefulSet một bản, PVC và Service;
- Secret mật khẩu, tự tạo từ `auth.password` hoặc lấy `auth.existingSecret` có sẵn;
- cổng metrics 9121, cùng ServiceMonitor tùy chọn cho Prometheus Operator;
- NetworkPolicy chỉ cho pod có nhãn `hcache-client: "true"` nối vào RESP, và chỉ pod trong namespace `monitoring` nối vào cổng metrics;
- `securityContext` theo mức restricted.

Cổng metrics nhận mật khẩu qua HTTP Basic không mã hóa. Mặc định ServiceMonitor dùng người dùng `default`, có mọi quyền. Nên tạo một người dùng chỉ đọc được `INFO` (`ACL SETUSER metrics on >MẬT_KHẨU -@all +info`, được lưu trong tệp dữ liệu) rồi đặt Secret của nó vào `metrics.serviceMonitor.authSecret`. Đổi mật khẩu xong phải `kubectl rollout restart` pod: chart không còn ghi mã băm của mật khẩu vào annotation của pod, vì người đọc được pod thường nhiều hơn người đọc được Secret, và mã băm ngắn không có muối thì dò ngược được.

Chart chưa được render hay cài thử vì máy này chưa có `helm` và Kubernetes.

## Sáu nguyên tắc, và chỗ chúng nằm trong mã

1. **Vòng lặp sự kiện không bao giờ chờ khóa.** Nó dùng `try_lock`; khóa bận thì lệnh vào hàng đợi của shard và kết nối được gác lại (`engine.rs`, `submit`).
2. **Người giữ khóa xử lý một lô có giới hạn** của hàng đợi trước khi nhả (`drain_batch`, mặc định 32).
3. **Lệnh mức kết nối chạy trước mọi khóa** (`PING`, `INFO`, `SCRIPT KILL`…), nên vẫn trả lời khi một script đang giữ cả nút.
4. **Quét dài nhả khóa theo chu kỳ** (`KEYS`, `HGETALL`: mỗi `scan_chunk` phần tử, mặc định 256).
5. **Pipeline bị chặn trần** mỗi lượt (`pipeline_cap`, mặc định 64).
6. **Mã của shard viết dưới dạng `&mut Shard`**, không biết gì về khóa.

Việc độc quyền (`EVAL`, `FLUSHALL`, lệnh nhiều khóa bị leo thang) đi qua cổng thực thi ở phía ghi và chạy trên một luồng riêng, không chạy trên vòng lặp sự kiện.

## Bộ test

Cuối ngày 2026-11-17, sau đợt 3, cả bộ có 1041 test (thêm 4 test bỏ qua theo mặc định). Trên Windows cả 1041 đều đạt. Trên Linux (WSL2) 1040 đạt; test mô hình ngẫu nhiên của `collections.rs` trượt một lần vì đồng hồ thực của WSL nhảy khoảng 23,7 giây giữa lúc chạy (engine tính TTL theo đồng hồ thực, mô hình trong test theo đồng hồ monotonic), chạy lại riêng ba lần đều đạt. `clippy -D warnings` và `rustfmt` sạch. Đợt 3 thêm `metrics.rs`, `bloom.rs`, `json.rs`, `json_upstream.rs` và `sentinel.rs`. Trước đó, cũng ngày 2026-11-17, cả bộ có 818 test, đều đạt trên hai hệ. Đợt bù tính năng thêm `valkey9.rs` (lệnh mới của Valkey 9 so với bản ghi lấy từ Valkey 9.1.3 thật), `cluster.rs` (cluster một node và lệnh vận hành), `rdb.rs` (tệp RDB, kể cả tệp hỏng ngẫu nhiên), `replication.rs` (sao chép và `FAILOVER`) và `memcached.rs` (cổng Memcached). Ngày 2026-11-16 có 710 test, đều đạt trên Windows và Linux. Trong đó `durability.rs` (25 test) kiểm ba chế độ `appendfsync` (đọc lại đúng tệp trên đĩa ngay sau khi lệnh được trả lời), `SAVE`, hàm Lua và người dùng ACL qua khởi động lại, qua lần ghi lại tệp và khi tệp bị cắt cụt, `CLIENT KILL` với mọi bộ lọc, thời hạn kết nối rảnh và client không đọc; `tls.rs` (17 test) kiểm TLS: giá trị 9 MiB và pipeline dài, gói tin vụn, dữ liệu không phải TLS, bắt tay dở dang, chứng chỉ client đúng, sai và thiếu, client biến mất giữa chừng; `p2.rs` (39 test) kiểm nhóm P2: `BITFIELD` so với mô hình từng bit, GEO so với công thức haversine tính thẳng, `SORT`, `SWAPDB` (kể cả `WATCH`, người chờ, khởi động lại), `SLOWLOG`, `MONITOR`, `CLIENT TRACKING` qua RESP3; `p1.rs` (28 test) kiểm nhóm P1: thời hạn của field, HyperLogLog, `DUMP`/`RESTORE` với payload hỏng, thông báo keyspace, `MEMORY USAGE`, `LCS`, `FUNCTION`; `databases.rs` (13 test) kiểm nhiều database, `auth.rs` (17 test) kiểm xác thực và ACL, `hardening.rs` (6 test) kiểm các lỗi mà lượt rà bảo mật tìm ra.

Test được viết để tìm lỗi: đối chiếu với mô hình tham chiếu bằng dữ liệu ngẫu nhiên, cắt gói tin ở mọi vị trí, đầu vào hỏng, nhiều luồng tranh nhau, script chạy vô hạn, tắt server giữa chừng.

| Tệp | Kiểm gì |
|---|---|
| `hcache-core/tests/resp.rs` | Bộ phân tích: cắt gói ở mọi byte, số âm, số quá lớn, lệnh inline, fuzz |
| `hcache-core/tests/dict.rs`, `glob.rs` | Bảng băm so với mô hình; mẫu glob kể cả mẫu gây bùng nổ |
| `hcache/tests/model.rs` | Mọi lệnh so với một mô hình tham chiếu đơn luồng |
| `hcache/tests/concurrency.rs` | Tính nguyên tử của lệnh nhiều khóa, không lệnh nào bị bỏ quên trong hàng đợi |
| `hcache/tests/script.rs` | Hộp cát Lua, giới hạn bộ nhớ, `SCRIPT KILL`, script lỗi |
| `hcache/tests/tcp.rs` | Server thật qua socket: pipeline lớn, client chậm, ngắt giữa chừng, tắt server |
| `hcache/tests/memory.rs` | Kết nối trả lại bộ nhớ sau một yêu cầu và một trả lời 48 MiB (tệp riêng để không test nào khác cấp phát lúc đo) |
| `hcache/tests/mixed.rs` | Bản nhúng và TCP dùng chung một dữ liệu |
| `hcache/tests/expiry.rs` | Thời hạn, tùy chọn của `SET`, `SCAN`/`HSCAN`, `MULTI`/`EXEC`/`WATCH`: so với mô hình có đồng hồ ảo, giá trị biên, nhiều luồng tranh nhau |
| `hcache/tests/expiry_tcp.rs` | Các phần trên qua socket: RESP2 và RESP3, ngắt kết nối giữa `MULTI`, pipeline vượt trần, quét nền |
| `hcache/tests/compat.rs` | Những gì thư viện client thật gửi: `HELLO` với phiên bản sai, RESP3, `EVALSHA`, `SCRIPT LOAD/EXISTS/FLUSH`, bảng script đầy |
| `hcache/tests/collections.rs`, `coll_smoke.rs` | List, set, sorted set, hash: so với mô hình, giá trị biên, nhiều luồng |
| `hcache/tests/extras.rs` | Phép toán trên sorted set, khoảng theo từ điển, bitmap so với mô hình, `EVAL_RO`, lệnh nhiều khóa qua nhiều shard |
| `hcache/tests/pubsub.rs` | Pub/sub qua socket, kể cả người đăng ký chậm và ngắt kết nối |
| `hcache/tests/eviction.rs` | `maxmemory` với tám chính sách, kể cả LFU |
| `hcache/tests/p2.rs` | Nhóm P2: `BITFIELD`, GEO, `SORT`, `SWAPDB`, `SLOWLOG`, `LATENCY`, `MONITOR`, `OBJECT FREQ`, `CLIENT TRACKING` (mọi chế độ, đua giữa đọc và ghi, người dùng bị giới hạn) |
| `hcache/tests/persist.rs` | Tệp dữ liệu: khởi động lại, cắt tệp ở mọi vị trí, tệp hỏng, sao chép lúc đang ghi |
| `hcache/tests/blocking.rs` | Lệnh chờ: thứ tự người chờ, hết giờ, người chờ biến mất, nhiều người chờ nhiều khóa |
| `hcache/tests/streams.rs` | Stream qua socket: `XREAD`/`XREADGROUP` có và không `BLOCK`, nhóm tiêu thụ dưới tải (không mất, không lặp), tệp dữ liệu, nhiều kiểu người chờ trên một khóa |
| `hcache-core/src/stream.rs` (test đơn vị) | Stream so với mô hình ngẫu nhiên, ID ở biên `u64`, giải mã dữ liệu hỏng |
| `hcache/tests/resp3_shapes.rs` | Kiểu trả lời RESP2 và RESP3, đúng từng byte |
| `hcache/tests/conn_cmds.rs` | `CLIENT`, `RESET`, các mục của `INFO` |
| `hcache/tests/sentinel.rs` | Những gì Sentinel gửi: khối đổi vai trong `MULTI`, failover `COORDINATED` (`CLIENT PAUSE` và `FAILOVER` trong một `EXEC`), `INFO` có `run_id` và theo mục, `replica-priority`, `CLIENT PAUSE WRITE`/`ALL`, `CONFIG REWRITE` |
| `hcache/tests/metrics.rs` | Cổng `/metrics`: tên theo redis_exporter, HTTP Basic khi có mật khẩu và quyền `INFO`, chỉ phục vụ `GET`/`HEAD /metrics`, client im lặng bị ngắt mà không chặn người khác |
| `hcache/tests/bloom.rs` | `BF.*`: câu trả lời như valkey-bloom, mở rộng filter, thiết lập `bf.*`, `BF.LOAD` của filter valkey-bloom ghi, `DUMP`/`RESTORE`, RDB, tệp dữ liệu, sao chép, thông báo keyspace |
| `hcache/tests/json.rs`, `json_upstream.rs` | `JSON.*`: kiểu khóa riêng, lỗi trong `MULTI`, `DUMP`/`RESTORE`, RDB, tệp dữ liệu, sao chép, thông báo keyspace; bộ test Python của valkey-json (151 test) chuyển sang Rust |
| `hcache/tests/script_libs.rs` | `cjson`, `cmsgpack`, `struct`, `bit` trong Lua: đáp án đã biết, dữ liệu hỏng, lồng sâu, giới hạn bộ nhớ |
| `hcache/tests/valkey9.rs` | `HGETDEL`, `HGETEX`, `HSETEX`, `DELIFEQ`, `MSETEX`, `SET IFEQ`, pub/sub theo shard: câu trả lời, sự kiện keyspace và byte trên dây so với bản ghi từ Valkey 9.1.3 |
| `hcache/tests/cluster.rs` | Cluster một node (`CROSSSLOT`, `CLUSTER ...`), `ROLE`, `COMMANDLOG`, `DEBUG`, `SHUTDOWN`, `LOLWUT` |
| `hcache/tests/rdb.rs` | Tệp RDB: mọi kiểu dữ liệu, thư viện hàm, tệp của Valkey, tệp hỏng ngẫu nhiên không làm panic và không nạp dở |
| `hcache/tests/replication.rs` | Sao chép: đồng bộ, nối tiếp sau khi đứt, replica của replica, mật khẩu, `SYNC`/`PSYNC`, `FAILOVER` (chờ, hủy, `FORCE`, hết giờ) |
| `hcache/tests/memcached.rs` | Cổng Memcached: dùng chung dữ liệu với RESP, CAS, `incr` từ nhiều client, lệnh meta, `noreply`, đăng nhập và ACL, replica, trần kết nối, `flush_all` hẹn giờ |

## Test bằng thư viện client thật

Thư mục `clients/` có năm dự án nhỏ, mỗi dự án chạy cùng một bộ 38 đến 43 phép kiểm qua một thư viện Redis phổ biến: chuỗi, hash, list, set, sorted set, giao dịch, thời hạn, `SCAN`, script, pub/sub. Bộ của Python có thêm một phép kiểm stream và lệnh chờ, và một phép kiểm nhiều database, đăng nhập bằng người dùng ACL và `COMMAND`, và một phép kiểm nhóm P1 (thời hạn field, HyperLogLog, `DUMP`/`RESTORE`, `LCS`, `FUNCTION`/`FCALL`, thông báo keyspace), và một phép kiểm nhóm P2 (GEO, `BITFIELD`, `SORT`, `SWAPDB`, `SLOWLOG`, `OBJECT FREQ`, `MONITOR`, `CLIENT TRACKING` với `REDIRECT`), và một phép kiểm `appendfsync`, `SAVE`, `CLIENT KILL`.

Chạy server ở cổng 6400 trước (lệnh ở mục "Lệnh"). Biến `HCACHE_PORT` đổi cổng; `HCACHE_PROTO=2` hoặc `3` ép giao thức, để trống thì thư viện tự bắt tay theo mặc định của nó. Riêng bộ Python chạy được qua TLS: đặt `HCACHE_TLS_CA` là đường dẫn tới chứng chỉ CA và `HCACHE_PORT` là cổng TLS. Chứng chỉ thử nghiệm nằm ở `crates/hcache/tests/tls/` (tự ký, chỉ để test, không dùng cho việc gì khác).

| Thư viện | Thư mục | Lệnh chạy (trong thư mục đó) | RESP2 | RESP3 |
|---|---|---|---|---|
| Python, redis-py | `clients/python` | `PYTHONIOENCODING=utf-8 python test_client.py` | 43/43 | 43/43 |
| Node, node-redis | `clients/node` | `node test_node_redis.mjs` | 39/39 | 39/39 |
| Node, ioredis | `clients/node` | `node test_ioredis.mjs` | 38/38 | 38/38 |
| Go, go-redis | `clients/go` | `go run .` | 39/39 | 39/39 |
| C#, StackExchange.Redis | `clients/dotnet` | `dotnet run -c Release` | 38/38 | 38/38 |
| Rust, redis-rs | `clients/rust` | `cargo +stable-x86_64-pc-windows-msvc run --release -q` | 38/38 | 38/38 |

Kết quả trên đo ngày 2026-11-16 với server bản release chạy `--appendfsync always`. Bộ Python qua cổng TLS cũng đạt 43/43 ở cả hai giao thức; năm thư viện còn lại chưa chạy qua TLS. Chạy lại đủ sáu thư viện, cả hai giao thức, sau khi đổi bảng băm, thêm đường nhanh GET/SET và so tag bằng SSE2: vẫn đạt hết (Python 43, node-redis 39, ioredis 38, Go 39, .NET 38, Rust 38).

## Tốc độ ghi theo chế độ fsync

Đo ngày 2026-11-16 trên máy này (Windows, ổ NVMe Samsung 970 EVO Plus), `hload baseline --write-pct 100`, 5 giây mỗi ô, 100 000 khóa, giá trị 64 byte, không pipeline. Máy phát tải chạy cùng máy với server, nên số tuyệt đối chỉ để so các chế độ với nhau.

| Chế độ | 1 kết nối | 16 kết nối | 64 kết nối |
|---|---|---|---|
| Không có tệp dữ liệu | 26 886 lệnh/giây, p99 0,08 ms | 69 862, p99 0,8 ms | 85 323, p99 4,5 ms |
| `no` | 26 979, p99 0,08 ms | 69 368, p99 1,0 ms | 84 580, p99 4,5 ms |
| `everysec` | 26 737, p99 0,09 ms | 70 498, p99 0,7 ms | 82 650, p99 3,5 ms |
| `always` | 651, p99 4,5 ms | 6 505, p99 5,5 ms | 22 591, p99 6,0 ms |

- `no` và `everysec` không chậm hơn chạy không có tệp dữ liệu, trong sai số của phép đo.
- `always` với một kết nối chậm hơn khoảng 40 lần: mỗi lệnh chờ một lần `fsync` (khoảng 1,4 ms). Càng nhiều kết nối thì càng nhiều lệnh dùng chung một lần `fsync`, nên 64 kết nối đạt khoảng một phần tư tốc độ của `everysec`.
- `always` với 16 kết nối và pipeline 16 lệnh đạt 88 602 lệnh/giây (p99 6,6 ms).
- `always` với 16 kết nối và 10% lệnh ghi đạt 45 746 lệnh/giây; lệnh đọc không chờ đĩa (p50 0,11 ms).

Mỗi ô chỉ đo một lần, chưa lặp lại để lấy độ dao động.

## Spike A, lượt đầu trên WSL2 (so với Valkey 9.1.3)

Đo ngày 2026-11-16 trong WSL2 (Ubuntu 24.04, nhân 6.6, 8 vCPU của i7-9700K, 8 GB RAM). Valkey 9.1.3 build từ mã nguồn, chạy với `--io-threads` bằng số nhân. HCache chạy với `--workers` bằng số nhân. Cả hai ghi log với `appendfsync everysec`; Valkey fsync mỗi giây, HCache mỗi 100 ms (mặc định của `--sync-interval-ms`), tức HCache chịu điều kiện chặt hơn. Server được ghim vào N vCPU đầu bằng `taskset`, `hload` chạy trên các vCPU còn lại. `hload baseline`: 64 kết nối, 10% lệnh ghi, 100 000 khóa, giá trị 64 byte, 10 giây mỗi ô. Mỗi ô chạy 3 lần, mỗi lần trên một server mới. Bảng ghi trung vị, đơn vị lệnh/giây, trong ngoặc là p99. Kịch bản đo: `bench/spike_a_wsl.sh`; số thô: `bench/results/2026-11-16-wsl2-grid.txt`.

| Nhân | Khóa | Pipeline | Valkey 9.1.3 | HCache | HCache / Valkey |
|---|---|---|---|---|---|
| 1 | đều | 1 | 47 557 (2,8 ms) | 42 618 (2,8 ms) | 0,90 |
| 1 | đều | 16 | 568 323 (3,3 ms) | 422 085 (5,2 ms) | **0,74** |
| 1 | Zipf 0,99 | 1 | 51 604 (2,4 ms) | 44 433 (2,9 ms) | 0,86 |
| 1 | Zipf 0,99 | 16 | 607 774 (2,5 ms) | 473 240 (3,7 ms) | **0,78** |
| 2 | đều | 1 | 56 350 (1,6 ms) | 91 976 (1,2 ms) | 1,63 |
| 2 | đều | 16 | 771 242 (2,0 ms) | 856 323 (3,5 ms) | 1,11 |
| 2 | Zipf 0,99 | 1 | 56 697 (1,6 ms) | 93 660 (1,1 ms) | 1,65 |
| 2 | Zipf 0,99 | 16 | 769 904 (2,0 ms) | 927 206 (3,0 ms) | 1,20 |
| 4 | đều | 1 | 400 870 (1,1 ms) | 487 454 (0,6 ms) | 1,22 |
| 4 | đều | 16 | 1 893 018 (1,3 ms) | 1 871 438 (1,6 ms) | 0,99 |
| 4 | Zipf 0,99 | 1 | 432 561 (0,6 ms) | 538 364 (0,3 ms) | 1,24 |
| 4 | Zipf 0,99 | 16 | 1 992 056 (1,2 ms) | 2 356 069 (1,2 ms) | 1,18 |

Ba lần đo của mỗi ô cách nhau không quá 10%, trừ một lần của Valkey ở 1 nhân không pipeline.

- **Một nhân: HCache chậm hơn Valkey.** Có pipeline (lúc server chỉ còn tốn CPU, ít phụ thuộc mạng ảo) HCache đạt 0,74 đến 0,78 lần Valkey, dưới ngưỡng 0,9 của tiêu chí dừng. Đây là kết quả xấu đáng tin nhất của lượt này. Nguyên nhân chưa tìm; một khả năng là luồng ghi log và các luồng nền của HCache dùng chung một nhân với worker.
- **Hai và bốn nhân: HCache nhanh hơn**, 1,1 đến 1,65 lần, trừ ô 4 nhân, khóa đều, pipeline 16 (0,99). Với khóa Zipf HCache không tụt (1,18 đến 1,65).
- **Đuôi trễ của HCache xấu hơn khi có pipeline:** p99 cao hơn Valkey ở 1 và 2 nhân, và lần chậm nhất của HCache thường 20 đến 40 ms, của Valkey 10 đến 20 ms.
- **Số không pipeline ở 1 và 2 nhân bị máy ảo giới hạn.** Cho Valkey chạy trên bất kỳ 1 hay 2 vCPU nào (0, 1, 2, 3, 7, 2-3, 6-7) với client trên 4 vCPU khác đều ra khoảng 57 500 lệnh/giây; lên 4 vCPU thì nhảy lên khoảng 410 000. Vậy đó là chi phí đánh thức vCPU của Hyper-V, không phải giới hạn của server. Kịch bản kiểm: `bench/vcpu_check.sh`.

**Lượt này chưa trả lời được tiêu chí dừng.** Tiêu chí cần 8 và 16 nhân, máy phát tải tách riêng, và Linux chạy thẳng trên máy, không qua máy ảo. Máy này chỉ có 8 vCPU dùng chung cho server và client. Số ở 1 nhân là tín hiệu cảnh báo: nếu đo trên máy thuê cũng ra dưới 0,9 thì Spike A trượt ngưỡng 1 nhân.

### Vì sao HCache chậm hơn ở 1 nhân

Đo ngày 2026-11-16 trong WSL2, server ghim vào 1 vCPU, pipeline 16, 64 kết nối, 10% lệnh ghi, không ghi log. Số đo một lần dao động tới 10% theo máy chủ Windows, nên các so sánh dưới đây chạy xen kẽ: mỗi vòng chạy mọi server một lần, 5 hoặc 6 vòng, mỗi lần 4 giây (độ lệch chuẩn khoảng 1%). Kịch bản: `bench/ab_one_core.sh`, `bench/cost_per_op.sh`, `bench/mem_per_key.sh`, `bench/one_core.sh`.

**Không phải do ghi log, system call hay mô hình luồng.**

- Tắt ghi log ở cả hai bên, khoảng cách vẫn vậy (0,77 đến 0,80 lần với 0%, 10% và 100% lệnh ghi).
- Phần chạy trong nhân hệ điều hành ngang nhau: 2 900 chu kỳ CPU mỗi lệnh ở HCache, 2 700 ở Valkey.
- Bỏ bớt lần `read` thừa làm số system call của HCache thấp hơn Valkey (70 600 so với 81 500 mỗi giây) nhưng chỉ nhanh thêm 0,9%. Thay đổi này còn làm mất tín hiệu đóng kết nối trên Linux (7 test hỏng), nên đã hoàn lại.
- Đường đi qua engine (hàng đợi, cổng thực thi, khóa shard) tốn khoảng 150 đến 180 ns mỗi lệnh, chưa tới một phần ba khoảng chênh.

**Nguyên nhân chính: chi phí trong chính chương trình, phần lớn là chờ bộ nhớ khi tra khóa.** HCache tốn 4 460 chu kỳ mỗi lệnh ngoài nhân hệ điều hành, Valkey 2 600. HCache chạy ít lệnh máy hơn (4 400 so với 5 700 mỗi lệnh) nhưng mỗi chu kỳ làm được ít hơn hẳn (IPC 0,74 so với 1,32), tức CPU đứng chờ bộ nhớ.

| Phần việc (ns mỗi lệnh, 100 000 khóa) | HCache | Valkey 9.1.3 |
|---|---|---|
| Tra khóa trong bảng băm | 368 (`Dict::find`, gồm 113 so khóa) | khoảng 190 (`hashtableFind` 78 và prefetch 112) |
| Chép giá trị ra câu trả lời | 132 (`memmove`) | 96 (`addReplyBulk`, gồm cả ghi câu trả lời) |
| Cấp phát và giải phóng | khoảng 115 | không đáng kể trong profile |
| Phân tích lệnh | 129 (`Parser::feed`) và 77 (`Cmd::parse`) | 105 (`parseInputBuffer`) |

Một lệnh GET của HCache chạm khoảng bốn vùng nhớ nằm rời nhau: mảng bucket, bản ghi, khóa (một vùng cấp phát riêng) và giá trị (một vùng cấp phát riêng nữa). Valkey 9 chạm khoảng hai: bucket, và một đối tượng chứa cả khóa lẫn giá trị ngắn. Valkey còn prefetch khóa theo lô khi có pipeline, nhưng phần này chỉ đóng góp 1,8% (tắt bằng `--prefetch-batch-max-size 0`). Bằng chứng khác: khoảng cách lớn dần theo số khóa, đúng như khi nút thắt là bộ nhớ.

| Số khóa | HCache / Valkey trước khi sửa | Sau khi sửa |
|---|---|---|
| 1 000 (vừa cache) | 0,852 | 0,856 |
| 100 000 | 0,774 | 0,788 |
| 1 000 000 | 0,725 | 0,740 |

Phần còn lại khi khóa vừa cache (khoảng 15%) là chi phí cố định mỗi lệnh: cấp phát cho từng tham số, chép giá trị hai lần, lấy giờ để đo lệnh chậm, đường đi qua engine.

**Đã sửa ngay: bản ghi của bảng băm từ 128 xuống 64 byte.** Các kiểu tập hợp lớn của `Value` (hash, set, sorted set) giờ nằm sau một con trỏ, nên `Value` còn 32 byte. Mỗi bản ghi chỉ giữ 32 bit thấp của mã băm, đủ để chọn bucket. Kết quả: nhanh thêm 0,5 đến 2,1%. Lợi lớn hơn nằm ở bộ nhớ: 1 triệu khóa chuỗi giá trị 64 byte tốn 187 byte mỗi khóa (RSS), trước là 251; Valkey tốn 125.

**Việc nên làm tiếp (giai đoạn 4 trong kế hoạch, chưa làm):**

1. Đặt khóa và giá trị ngắn chung một vùng cấp phát. Ước tính bớt được một đến hai lần chờ bộ nhớ mỗi lệnh và khoảng 50 byte mỗi khóa.
2. Trả giá trị thẳng vào bộ đệm câu trả lời, không chép ra một `Vec` trung gian.
3. Đọc tham số thành lát cắt của bộ đệm đầu vào thay vì cấp phát mỗi tham số một `Vec`.
4. Prefetch khóa theo lô khi có pipeline, như Valkey 9.

Mô hình luồng không phải thứ thua ở 1 nhân: chi phí của nó (khoảng 150 đến 180 ns mỗi lệnh) nhỏ hơn nhiều so với phần bảng băm và cấp phát. Vì vậy kết quả 1 nhân không bác bỏ mô hình luồng, nhưng tiêu chí 0,9 lần ở 1 và 2 nhân sẽ chỉ đạt khi làm các việc trên.

### Tối ưu 1 nhân

Đo như trên (1 vCPU, pipeline 16, 64 kết nối, 10% ghi, giá trị 64 byte), xen kẽ 5 vòng, số là HCache / Valkey 9.1.3 đo cùng vòng. Mỗi bước giữ các bước trước. Nghiên cứu đứng sau các bước này (mã nguồn Valkey, Dragonfly, Garnet, memcached, Pelikan, CacheLib; phần cứng; container) nằm trong một báo cáo nội bộ, chưa công khai.

| Bước | 1 000 khóa | 100 000 khóa | 1 000 000 khóa |
|---|---|---|---|
| Trước đợt này (entry 64 byte) | 0,856 | 0,788 | 0,740 |
| Khóa ngắn (đến 18 byte) nằm trong entry, không cấp phát riêng | 0,873 | 0,787 | 0,736 |
| Bộ cấp phát mimalloc | 0,864 | 0,799 | 0,767 |
| LTO toàn chương trình, 1 codegen unit | 0,905 | 0,837 | 0,798 |
| `Cmd` từ 160 xuống 96 byte (biến thể lớn nằm sau `Box`) | 0,902 đến 0,931 | 0,857 đến 0,880 | 0,794 đến 0,818 |
| Prefetch theo lô và GET ghi thẳng vào bộ đệm trả lời | 0,927 | 0,942 | 0,893 |
| Bỏ `read` thừa (Linux) và một lần đọc đồng hồ mỗi lệnh | 0,988 | 0,943 | 0,954 |
| Đợt hai: đường nhanh GET/SET, hạn trong entry, bảng bucket so tag SSE2, LFU không chia, THP tắt | 1,005 | 0,994 | 0,935 |
| Như trên, build PGO (`bench/pgo_build.sh`) | 1,082 | 1,045 | 0,997 |

Dòng "Bỏ `read` thừa" đo xen kẽ 10 vòng khi máy Windows bên ngoài đang bận hơn (độ lệch chuẩn 3 đến 8%, các dòng trên 1 đến 3%). Trong cùng lượt đó, bản ngay trước nó ra 0,965 / 0,874 / 0,923, nên bước này thêm khoảng 2 đến 8%. Hai dòng đợt hai đo cùng lượt 5 vòng; trong lượt đó bản cuối đợt một (cùng tắt THP) ra 0,942 / 0,926 / 0,894. Tỉ lệ so với Valkey xê dịch vài phần trăm giữa các lượt; chỉ nên so các bản đo trong cùng một lượt.

- **Khóa trong entry** (`dict.rs`, `Key`): khóa ngắn (nay đến 16 byte, trước là 18) nằm ngay trong entry 64 byte, so sánh khóa không đọc thêm vùng nhớ nào. Về tốc độ, chênh lệch nằm trong độ nhiễu; lợi chính là bớt một khối cấp phát mỗi khóa.
- **mimalloc** (`mem.rs`): hàng triệu khối nhỏ được cấp nhanh hơn và không có đầu khối riêng; bộ nhớ 188 xuống 160 byte mỗi khóa (Valkey 125).
- **LTO** (`Cargo.toml`): đường đi nóng của một lệnh đi qua nhiều crate; gom vào một đơn vị dịch giảm một nửa số lần trượt iTLB mỗi lệnh.
- **`Cmd` nhỏ lại** (`cmd.rs`): lệnh được chép qua nhiều tầng; trên 128 byte mỗi lần chép là một lời gọi `memmove`, mà các nhánh so kích thước của nó chiếm một phần năm số lần đoán nhánh sai của một GET.
- **Prefetch theo lô** (`resp::peek_keys`, `Engine::prefetch`, `Shard::prefetch`): trước khi chạy, worker đọc lướt tới 16 lệnh đang chờ trong bộ đệm vào, lấy khóa của từng lệnh và yêu cầu CPU nạp trước theo ba bước (ô bucket, rồi entry, rồi giá trị). Các lần trượt cache của 16 lệnh chạy song song thay vì nối tiếp. Đây là cách của Valkey (`prefetchCommandQueueKeys`). Lợi lớn nhất khi bảng không vừa cache: 100 000 khóa +8%, 1 000 000 khóa +10%.
- **GET ghi thẳng** (`Engine::submit_into`, `Shard::get_into`): giá trị được mã hóa thẳng từ bảng vào bộ đệm trả lời, bỏ một lần cấp phát, một lần chép và một lần giải phóng mỗi GET.
- **Bỏ `read` thừa** (`Conn::fill_to`): trên Linux, một lần `read` trả về ít hơn bộ đệm nghĩa là socket đã cạn, nên không gọi thêm một lần chỉ để nhận `EAGAIN`. Tín hiệu đóng kết nối không mất: mio đăng ký `EPOLLRDHUP`, và khi sự kiện báo phía kia đã đóng (`read_closed`) thì vẫn đọc tới hết. Không áp dụng cho TLS (thư viện TLS có thể còn giữ dữ liệu đã giải mã) và Windows (mio trên Windows chỉ đăng ký lại sau `WouldBlock`). Lần thử đầu, thiếu kiểm tra `read_closed`, đã làm hỏng 7 test trên Linux.
- **Bảng băm kiểu bucket có tag** (`dict.rs`): mỗi bucket đúng 64 byte (một dòng cache) gồm 12 byte tag, 12 chỉ số entry và một liên kết tới bucket tràn. Tag là 8 bit của mã băm; một lệnh SSE2 so cả 12 tag cùng lúc (máy không phải x86_64 so 8 byte một lần, SWAR), nên phần lớn lần tra chỉ đọc một bucket rồi một entry. Bảng nhân đôi khi trung bình 8 khóa mỗi bucket. Entry nằm trong các khối 1 024 cái, không di chuyển khi bảng lớn lên. Con trỏ `SCAN` vẫn đi theo thứ tự bit đảo như Redis.
- **Hạn trong entry** (`dict.rs`, `shard.rs`): hạn của khóa (48 bit, mili giây) nằm ngay trong entry, nên đọc một khóa có hạn không phải tra thêm bảng `expires`. Bảng `expires` vẫn giữ để quét khóa hết hạn và chọn khóa đẩy ra theo `volatile-*`.
- **Đường nhanh GET/SET** (`resp::read_fast`, `Conn::read_fast`): `GET k` và `SET k v` không tùy chọn được đọc thẳng từ bộ đệm vào, bỏ qua bộ phân tích chung (mỗi tham số một `Vec`) và chuỗi so tên lệnh của `Cmd::parse`. Chỉ dùng khi người dùng không bị giới hạn quyền, đã đăng nhập, không đang nghe kênh kiểu RESP2 và không có `MONITOR`; khi slowlog bật (mặc định) tham số được giữ lại phòng lệnh chậm. Các trường hợp khác đi đường cũ. Bản đầu bỏ qua đường nhanh mỗi khi slowlog bật, tức là gần như không bao giờ chạy; số đo lần đầu vì thế thấp hơn bản trước. Test fuzz so đường nhanh với bộ phân tích chung, kể cả khi vượt giới hạn kích thước.
- **LFU không chia** (`Dict::find`): xác suất tăng bộ đếm tính bằng phép nhân thay vì phép chia lấy dư.
- **Một lần đọc đồng hồ** (`Shared::slow`, `pump`): khi slowlog bật, giờ kết thúc của lệnh trước được dùng làm giờ bắt đầu của lệnh sau trong cùng lô, trừ khi lệnh trước vừa ghi slowlog hoặc đang có `MONITOR` (khi đó đọc lại đồng hồ). Hai thay đổi này cùng giảm số lệnh máy mỗi lệnh ở 100 000 khóa từ 3 936 xuống 3 580.

Ở 2 và 4 nhân, prefetch không gây tranh chấp khóa shard vì nó chỉ dùng `try_lock`: shard nào đang bận thì bỏ qua. Bảng đầy đủ nằm ở báo cáo. HCache / Valkey: trước đợt một → cuối đợt một (lượt 6 vòng) → cuối đợt hai, build thường / PGO (lượt 4 vòng):

| Số nhân | 100 000 khóa | 1 000 000 khóa |
|---|---|---|
| 2 nhân | 1,278 → 1,403 → 1,525 / 1,665 | 1,210 → 1,436 → 1,437 / 1,561 |
| 4 nhân | 1,258 → 1,631 → 1,902 / 2,094 | 1,145 → 1,524 → 1,604 / 1,826 |

PGO (tối ưu theo profile chạy thật): `bench/pgo_build.sh` build bản có đo đếm, chạy `hload` qua các kiểu tải (GET/SET có và không pipeline, ghi 100%, SCAN, hashtag, EVAL, MULTI, khóa thay liên tục), rồi build lại theo profile. Lần thử đầu (trước đợt hai) chỉ thêm 1 đến 2%. Trên bản cuối, PGO thêm 5 đến 8% ở 1 nhân và 9 đến 14% ở 2 và 4 nhân, giảm chu kỳ mỗi lệnh từ 3 026 xuống 2 428 và trượt iTLB từ 13,9 xuống 9,2. Chưa nằm trong `Dockerfile`.

## Giới hạn đã biết

- **Linux mới chạy trong WSL2.** Ngày 2026-11-17 bộ 1041 test chạy trên Ubuntu 24.04 trong WSL2 (đường `epoll` chạy qua các test TCP và TLS); đồng hồ thực của WSL có lúc nhảy hơn 10 giây, làm test dựa vào TTL hay Sentinel thỉnh thoảng trượt. Sáu thư viện client chưa chạy trên Linux.
- **Chưa có số đo dùng được cho tiêu chí dừng.** Tiêu chí của Spike A cần Linux chạy thẳng trên máy 8 và 16 nhân, máy phát tải tách riêng và Valkey chạy cùng điều kiện. Số trên WSL2 là lượt đầu, có giới hạn của máy ảo.
- **Chưa có** (xếp theo đợt ở mục "Bù các tính năng hệ khác có" của kế hoạch): cluster nhiều node (thiết kế ở [docs/cluster-design.md](docs/cluster-design.md)), Search, TimeSeries, các cấu trúc xác suất ngoài Bloom (`CF.*`, `CMS.*`, `TOPK.*`), tầng SSD.
- **Độ bền, TLS, `CLIENT KILL`, chỗ khác Redis:**
  - `appendfsync always` chỉ giữ lại câu trả lời của chính lệnh ghi. Kết nối khác có thể đọc thấy thay đổi, nhận tin pub/sub hoặc thông báo `invalidate` của thay đổi đó trước khi nó xuống đĩa. Redis giữ mọi câu trả lời của cùng một vòng lặp.
  - Khi một lần `fsync` thất bại, kết nối đang chờ nó bị đóng mà không nhận câu trả lời; thay đổi vẫn còn trong bộ nhớ. Từ đó tệp không được ghi nối thêm, mọi lệnh ghi ở chế độ `always` đều thất bại theo cách này, cho đến khi server ghi lại được nguyên một tệp mới (thử lại mỗi giây). Redis thì thoát hẳn.
  - Bản nhúng (`Session`) ở chế độ `always` chờ đĩa ngay trong `execute`; nếu đang có script chạy lâu thì nó chờ script xong.
  - `SAVE` không tạo ảnh chụp riêng, nó chỉ chờ tệp dữ liệu cập nhật xong. `SAVE` bị từ chối trong `MULTI` và trong script. Các lệnh `CLIENT` bị từ chối trong script. Trong `MULTI`, `CLIENT KILL`/`PAUSE`/`UNPAUSE`/`SETNAME`, `REPLICAOF` và `FAILOVER` chạy đúng chỗ của chúng trong `EXEC`, như Sentinel cần. Riêng `CLIENT TRACKING`/`CACHING` vẫn bị từ chối.
  - `CLIENT KILL`: nhận dạng cũ (`addr:port`) và các bộ lọc `ID`, `ADDR`, `LADDR`, `USER`, `TYPE`, `MAXAGE`, `SKIPME`. `TYPE master` khớp liên kết của replica tới primary, `TYPE replica` khớp các replica đang theo. Chưa có `CLIENT NO-EVICT`, `CLIENT UNBLOCK`.
  - `CLIENT PAUSE <ms> WRITE` giữ lệnh ghi và lệnh được nhân bản (`PUBLISH`, `SPUBLISH`, script không chỉ đọc); `ALL` giữ mọi lệnh trừ `CLIENT UNPAUSE`. Trong lúc dừng không có hết hạn chủ động, như Valkey.
  - Thời hạn kết nối rảnh kiểm mỗi giây một lần, nên kết nối bị đóng trễ tối đa chừng một giây. Client đọc rất chậm nhưng vẫn đọc thì không bị coi là đứng.
  - TLS: không đổi chứng chỉ lúc đang chạy (phải khởi động lại); chưa có `tls-protocols`, `tls-ciphers`, danh sách thu hồi chứng chỉ; chứng chỉ client chỉ được kiểm là do CA ký, chưa gắn với người dùng ACL. Khi server đầy kết nối, client TLS bị ngắt mà không có câu báo lỗi.
  - Tệp dữ liệu có thêm bốn loại bản ghi (thư viện hàm, người dùng); bản HCache cũ hơn không đọc được tệp có các bản ghi này.
- **Nhóm P1, chỗ khác Redis:**
  - Thời hạn của field: `KEYS`, `SCAN`, `DBSIZE`, `RANDOMKEY` có thể còn thấy một hash mà mọi field đã hết hạn, cho đến khi có lệnh chạm vào nó hoặc lượt quét dọn tới.
  - HyperLogLog luôn lưu ở dạng dày (12 KiB mỗi khóa); dạng thưa của Redis đọc được và được đổi sang dạng dày khi ghi. Chưa có `PFDEBUG`, `PFSELFTEST`.
  - `DUMP` ghi RDB phiên bản 8 và `RESTORE` nhận phiên bản tới 12, cho chuỗi, list, set, sorted set, hash. Stream và hash có thời hạn field dùng định dạng riêng của HCache (kiểu `0xE0`, `0xE1`), Redis không đọc được; payload stream của Redis bị từ chối. `IDLETIME` và `FREQ` được nhận rồi bỏ qua. Mã giải mã viết theo tài liệu và trí nhớ về mã Redis, mới kiểm bằng hai vector trong tài liệu Redis, chưa đối chiếu với Redis thật.
  - Thông báo keyspace: không phát cho `FLUSHALL`/`FLUSHDB`, cho khóa đích của `COPY ... DB n`, và không có các lớp `n` (khóa mới), `m` (đọc trượt), `d`. Bản nhúng không có listener thì không phát gì. Mỗi shard giữ một số có hạn sự kiện hết hạn chờ phát; vượt quá thì bỏ bớt.
  - `FUNCTION`: thư viện được lưu trong tệp dữ liệu khi có `--data` và được nạp lại lúc khởi động (một thư viện không nạp lại được thì server không khởi động); không có `FUNCTION DUMP`/`RESTORE`; tối đa 1000 thư viện, mỗi thư viện 1 MiB mã. Khi trạng thái Lua bị dựng lại (sau lỗi hết bộ nhớ), thư viện được chạy lại từ mã nguồn; nếu lần chạy lại đăng ký tập hàm khác lần đầu thì `FCALL` báo lỗi.
  - `MEMORY USAGE` là ước lượng theo kích thước dữ liệu, không phải số byte bộ cấp phát thật sự dùng.
  - `LCS` từ chối khi tích độ dài hai chuỗi vượt 16 triệu ô (`ERR String too long for LCS`); Redis thì cứ tính.
  - `RESTORE` giới hạn tổng dữ liệu giải nén LZF của một payload ở 512 MiB.
- **Nhóm P2, chỗ khác Redis:**
  - LFU: bộ đếm giảm dần theo số lượt tra cứu, không theo thời gian; không có `lfu-log-factor`, `lfu-decay-time`. `OBJECT FREQ` không tính là một lần dùng. `OBJECT IDLETIME` vẫn luôn là 0.
  - `LATENCY` nhận đủ lệnh con nhưng trả lời tĩnh (không ghi nhận sự kiện nào).
  - `SLOWLOG`: `slowlog-max-len` tối đa 65536; không đo lệnh chờ (`BLPOP`…). `SLOWLOG` và `MONITOR` không bao giờ hiện `AUTH`, `HELLO`, `ACL`, và `CONFIG SET` của thiết lập có chữ `pass` hoặc `auth`.
  - `MONITOR`: cần quyền `monitor`; mỗi dòng bị cắt ở khoảng 64 KiB; kết nối không kịp đọc thì bị đóng thay vì bỏ bớt dòng. Tham số của `EVAL` vẫn hiện nguyên, nên bí mật truyền qua `ARGV` sẽ lộ cho người đang `MONITOR`.
  - `CLIENT TRACKING`: `BCAST` bị từ chối với người dùng có giới hạn khóa; tối đa 256 tiền tố mỗi kết nối; bảng theo dõi có trần (1 triệu khóa, 64 MiB tên khóa), vượt trần thì khóa cũ bị loại và người đọc nó nhận `invalidate`. `REDIRECT` nhận id của bất kỳ kết nối nào, như Redis. Đổi quyền của người dùng thì kết nối `MONITOR` hoặc `BCAST` không còn đủ quyền bị đóng.
  - `SORT`: kết quả của `GET` bị chặn ở 256 MiB (`ERR the result of SORT with these GET patterns is too large`).
  - `GEOADD` phát thông báo keyspace là `zadd`.
- **Bảo mật, chưa làm:** cổng không TLS vẫn mở theo mặc định, mật khẩu đi qua cổng đó ở dạng rõ (dùng `--port 0` cùng `--tls-port` để tắt); tệp dữ liệu chứa mã băm SHA-256 không có muối của mật khẩu (như Redis), nên cần giữ quyền đọc tệp cho riêng tài khoản chạy server (trên Linux server tự đặt quyền 0600; trên Windows tệp theo quyền của thư mục); không giới hạn số lần thử `AUTH`; một kết nối không giới hạn số khóa của một lệnh chờ, và mọi lệnh chờ dùng chung một khóa nội bộ; luồng xử lý không bắt panic; người dùng có giới hạn với rất nhiều mẫu khóa dài làm mỗi lệnh của chính họ chậm theo.
- **Lưu đĩa:** ở chế độ mặc định dữ liệu được ghi theo chu kỳ (100 ms), nên server bị giết đột ngột mất tối đa chừng đó; `--appendfsync always` thì không mất lệnh đã trả lời, đổi lại mỗi lượt ghi phải chờ đĩa. Một collection lớn bị ghi lại nguyên cả khối mỗi lần đổi. Tốc độ của từng chế độ ở mục "Tốc độ ghi theo chế độ fsync". Chưa thử mất điện thật (mới thử giết tiến trình).
- **Stream:** cắt xấp xỉ (`~`) bỏ từng khối 100 phần tử; vài trường của `XINFO` là ước lượng; `XINFO GROUPS`/`CONSUMERS` trả mảng phẳng cả ở RESP3; `XREAD` chưa nhận ID `+`. Nếu `XREADGROUP` trên nhiều khóa gặp lỗi ở khóa sau, các khóa trước đã được giao.
- **Lệnh chờ:** mỗi lần một stream đổi, mọi người đang chờ stream đó đều thử lại lần lượt, nên rất nhiều người chờ trên một stream thì mỗi `XADD` tốn theo số người chờ. Với list và sorted set, chỉ người đầu hàng thử lại.
- **Lua:** số thực nhỏ hơn khoảng 2e-16 đi qua `cjson`, `cmsgpack`, `struct` thành 0; lồng sâu quá 1000 mức bị từ chối; `redis.breakpoint`, `redis.debug`, `redis.set_repl` nhận nhưng không làm gì.
- `DBSIZE` đếm cả khóa đã hết hạn nhưng chưa được thu dọn, nên có thể lớn hơn số khóa đọc được trong chốc lát.
- `SCAN` và `HSCAN` trả tối đa 256 phần tử mỗi lần gọi dù `COUNT` lớn hơn, và có thể trả danh sách rỗng kèm con trỏ khác 0. Client phải lặp đến khi con trỏ về 0, như với Redis.
- `WATCH` không bỏ sót thay đổi nào, nhưng đôi khi hủy `EXEC` dù khóa được theo dõi không đổi (ví dụ sau `FLUSHALL` hoặc khi khóa khác cùng nhóm bị ghi).
- Hàng đợi của `MULTI` không có giới hạn số lệnh.
- Bản nhúng không có luồng quét nền: khóa hết hạn được thu khi có lệnh chạm tới shard của nó. Luồng quét chỉ chạy khi gọi `Db::listen`.
- Bảng script của `EVALSHA` bị xóa sạch khi vượt 10 000 script hoặc 64 MiB; Redis thì giữ đến khi `SCRIPT FLUSH`. Client gặp `NOSCRIPT` sẽ tự gửi lại bằng `EVAL`.
- `INFO` phần Keyspace luôn ghi `expires=0`.
- `redis.call('FLUSHALL')` trong script bị từ chối.
- Một script đã ghi dữ liệu thì không bị `SCRIPT KILL` (giống Redis). Nếu nó lặp vô hạn, nó giữ cả nút; `shutdown` chờ 5 giây rồi trả về và để luồng đó lại.
- Hàm C chạy lâu bên trong Lua (ví dụ `string.find` với mẫu xấu) không ngắt được.
- Một engine chỉ phục vụ một listener tại một thời điểm; gọi `listen` lần hai trả về lỗi.
- Phiên nhúng (`Session`) chờ khóa trực tiếp, không đi qua hàng đợi của shard.
- Nguyên tắc 5 được làm đơn giản: trần tính theo số lệnh mỗi lượt của một kết nối, không theo thời gian giữ khóa.
- `KEYS` và `HGETALL` trên dữ liệu lớn không nguyên tử: chúng nhả khóa giữa các lô, nên client có thể thấy trạng thái trộn của trước và sau một lệnh ghi xen vào. Nếu hash bị xóa rồi tạo lại giữa hai lô, kết quả ghép từ cả hai. Redis không có hiện tượng này.
- `DEL` và `UNLINK` một hash rất lớn giải phóng bộ nhớ ngay dưới khóa của shard; `UNLINK` chạy đồng bộ như `DEL`. Bảng băm cũng rehash một lần, không rehash dần.
- Chỗ lệch với Redis: `INFO` báo `redis_version:7.4.0` để các framework nhận ra, nhưng thiếu nhiều mục (tên mục lạ trả rỗng); `OBJECT IDLETIME` và `FREQ` luôn trả 0; số thực trả từ Lua định dạng khác `%.17g`; mẫu `KEYS` dài hơn 1024 byte bị từ chối; một số câu báo lỗi của stream, `cjson`, `cmsgpack` viết theo trí nhớ về mã Redis, chưa đối chiếu với Redis thật.
- Script dài hơn 16 KiB không được lưu vào bộ đệm script, nên bị biên dịch lại mỗi lần gọi.
- Cổng thực thi bỏ một hàng rào bộ nhớ trên x86; lập luận đúng với mô hình TSO của x86 nhưng chưa được kiểm bằng công cụ (loom, miri).
- Nếu tạo luồng thất bại giữa lúc `Server::start`, engine giữ chỗ của listener đó cho tới khi bị hủy.
- Có một lần chạy cả bộ test mà `endless_pipeline_does_not_starve_another_connection_on_the_same_worker` trượt ngưỡng 500 ms; chưa tái hiện được (xem sổ theo dõi).
