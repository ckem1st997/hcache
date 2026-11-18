# Cluster nhiều node cho Hotstash: thiết kế

Ngày 2026-11-17, đợt 3. Trạng thái: bản thảo để chủ dự án duyệt. Chưa có dòng code nào; tài liệu này chọn hướng, chia giai đoạn và nêu rủi ro.

Quy ước trích dẫn:
- Đường dẫn Hotstash tính từ `spike/`.
- Đường dẫn mã tham chiếu tính từ thư mục `ref-src/` (mã nguồn Valkey, Dragonfly, Garnet tải về, chỉ đọc, không dựng, không chạy).
- Chỗ ghi **[suy luận]** là điều tôi suy ra nhưng chưa kiểm chứng bằng mã hay bằng chạy thử.

## 1. Kết luận

- **Đích cuối là hướng (a):** cluster bus tương thích wire với Valkey. Nút Hotstash tham gia được cluster Valkey; `valkey-cli --cluster` và các operator đang dùng quản lý được nó.
- **Đường đi:** làm trước phần dùng chung cho mọi hướng (bảng slot, chuyển hướng MOVED/ASK, `MIGRATE`), chạy với topology tĩnh kiểu Dragonfly (hướng c). Sau đó mới làm bus.
- **Không làm hướng (b)** (bus riêng của Hotstash như Garnet). Nó tốn gần bằng (a) mà không được công cụ nào của hệ sinh thái.
- **Chuyển slot:** kiểu cũ (`SETSLOT` + `MIGRATE` + `ASK`) trước, kiểu nguyên tử của Valkey 9 sau.
- **Công sức:** khoảng 76–105 ngày công cho một kỹ sư, chia 5 giai đoạn. Có một điểm dừng sau giai đoạn 2: tới đó Hotstash đã là cluster dùng được với topology tĩnh, nhưng chưa tự failover.

## 2. Hotstash đang có gì

Kế hoạch bản 2 xếp "Cluster nhiều node: cluster bus, chuyển slot, `MIGRATE`" vào đợt 3, sau replication (kế hoạch nội bộ, chưa công khai). Kế hoạch cũng ghi rõ giả lập một node hiện tại không qua các test về chuyển slot, failover của cluster và đọc từ replica.

Những điểm trong mã liên quan trực tiếp tới thiết kế:

| Chỗ | Hiện trạng | Hệ quả cho cluster thật |
|---|---|---|
| `crates/hotstash-core/src/cluster.rs:1-4` | Một node giữ cả 16384 slot, không bao giờ chuyển hướng | Cần một bảng topology thật |
| `cluster.rs:49-51`, `:56-77` | `key_slot` (CRC16, có hash tag), `slot_of` gom slot của khóa và kênh `SSUBSCRIBE`/`SPUBLISH` | Dùng lại nguyên |
| `crates/hotstash-core/src/engine.rs:1132-1148` | Ở chế độ cluster, shard nội bộ = `slot * shards / 16384`, chỉ số `db * shards + local` | Một slot nằm trọn trong một shard nội bộ; một shard chứa một dải slot liền nhau, trộn chung một bảng băm |
| `engine.rs:3956-3975`, `crates/hotstash-core/src/cmd.rs:1273` | `COUNTKEYSINSLOT`/`GETKEYSINSLOT` quét cả shard, chạy ở phía exclusive của gate | Quá chậm cho cluster thật, xem mục 5.3 |
| `crates/hotstash-core/src/txn.rs:50-75`, `:151-160` | `CROSSSLOT` được kiểm trong `TxnState::feed`; transaction nhớ slot của lệnh đầu | Đây là chỗ đặt kiểm tra chuyển hướng |
| `crates/hotstash-server/src/lib.rs:3351` | Mọi lệnh của client đi qua `conn.txn.feed(engine, parsed)` | Một điểm chặn duy nhất cho mọi lệnh |
| `engine.rs:3757`, `cmd.rs:858` | `ASKING` trả `OK` nhưng không nhớ gì; `READONLY`/`READWRITE` là `Cmd::Static(Reply::Ok)` | Cần cờ theo từng kết nối |
| `cluster.rs:218-292` | `CLUSTER NODES/SLOTS/SHARDS` viết cứng cho một node; các lệnh quản trị khác trả lỗi "not supported" | Sinh từ bảng topology |
| `lib.rs:3936-3937`, `lib.rs:4030-4032` | `REPLICAOF` và `FAILOVER` bị từ chối ở chế độ cluster, như Valkey (`valkey/src/replication.c:5275-5278`) | Replica trong cluster phải đi qua `CLUSTER REPLICATE` |
| `crates/hotstash-core/src/repl.rs:1-23` | Dòng replication là lệnh RESP theo hiệu ứng; thứ tự đảm bảo theo shard (dòng 17-20) | Dùng lại cho luồng chuyển slot |
| `engine.rs:1377-1389` | Snapshot cho full sync lấy gate exclusive của cả node (không fork) | Không dùng nguyên cho chuyển slot được, xem mục 5.3 |
| `crates/hotstash-core/src/shard/dump.rs:1-5`, `:266-313` | `DUMP`/`RESTORE` cùng định dạng Valkey | Nền cho `MIGRATE` |
| `cmdtable.rs:241`, `ops.rs:875`, `ops.rs:1039` | `RESTORE` có; `MIGRATE` chưa có (chỉ có tên `MIGRATESLOTS` trong danh sách lệnh con CLUSTER) | Phải viết `MIGRATE` |
| `lib.rs:584-600` | `Shared::holds`: tạm giữ lệnh ghi khi FAILOVER hoặc `CLIENT PAUSE` | Dùng lại cho bước chốt khi chuyển slot và manual failover |
| `crates/hotstash-core/src/persist.rs:1-30` | File append-only theo khóa, có `<file>.lock` | `nodes.conf` là file riêng |
| `crates/hotstash-core/src/script/functions.rs:31` | Có cờ `no-cluster`; không kiểm khóa ngoài slot bên trong script | Phải thêm kiểm tra như Valkey |

## 3. Ba hướng và lựa chọn

### 3.1 Client cần gì

Thư viện client ở chế độ cluster (redis-py `RedisCluster`, ioredis `Cluster`, Lettuce, Jedis, go-redis) cần bốn thứ:
1. một lệnh hỏi topology: `CLUSTER SLOTS` hoặc `CLUSTER SHARDS`, đôi khi `CLUSTER NODES`;
2. lỗi `MOVED` và `ASK` đúng định dạng;
3. lệnh `ASKING`;
4. `READONLY` để đọc từ replica.

Client không nói chuyện với cluster bus. **[suy luận: dựa trên hiểu biết chung về các thư viện này; đợt này tôi chưa đọc mã client.]** Vì thế về phía client, ba hướng như nhau. Chúng chỉ khác ở phía vận hành: ai giữ topology, ai quyết định failover, công cụ nào dùng được.

### 3.2 So sánh

**(a) Bus tương thích wire với Valkey.**
- **Định dạng gói tin:** header nhị phân cố định `clusterMsg`, chữ ký `RCmb` (`valkey/src/cluster_legacy.h:358-384`). Offset từng trường bị khóa bằng `static_assert` (`:395-415`) để các bản Valkey khác nhau nói chuyện được với nhau.
- **Loại gói:** 12 loại (`:162-174`). Ngoài ra có phần mở rộng cho hostname, shard id, cổng client, availability zone, replica priority (`:243-252`) và header "light" cho pub/sub (`:175-186`).
- **Phần phải viết:** gossip, phát hiện lỗi theo quorum của primary (`valkey/src/cluster_legacy.c:2828-2850`), bầu chọn replica (`:6350-6590`), config epoch (`:2615-2705`), `nodes.conf` (`:1088-1096`, `:1224`).
- **Quy mô ở Valkey:** `cluster_legacy.c` dài 9719 dòng, `cluster_migrateslots.c` dài 2787 dòng.

**(b) Bus riêng, client thấy như Redis (Garnet).**
- **Gossip:** Garnet gửi qua lệnh RESP `CLUSTER GOSSIP`, mang cả cấu hình đã tuần tự hóa (`garnet/libs/cluster/Session/RespClusterBasicCommands.cs:362-410`).
- **Xác thực bus:** bằng user/mật khẩu riêng của cluster (`Server/Gossip/GarnetServerNode.cs:125-126`, `Server/ClusterAuthContainer.cs:10-13`).
- **Epoch:** chép cách xử lý va chạm epoch của Redis (`Server/ClusterConfig.cs:1577-1602`).
- **Failover:** Garnet không bầu chọn. Mã ghi rõ: "An external coordinator authorizes and serializes promotions; Garnet does not perform voting" (`Server/Failover/ReplicaFailoverSession.cs:517-518`).

Vậy Garnet trả phần lớn chi phí hệ phân tán của (a) mà vẫn cần bộ điều phối bên ngoài, lại không ghép được với nút Valkey.

**(c) Topology do bên ngoài quản lý (Dragonfly).**
- **Đẩy cấu hình:** `DFLYCLUSTER CONFIG <json>` thay toàn bộ cấu hình (`dragonfly/src/server/cluster/cluster_family.cc:556-647`). Lệnh này kiểm cấu hình, hủy các lệnh chặn trên khóa đã chuyển đi (`:599-616`), xóa dữ liệu của slot không còn sở hữu (`:627-637`) rồi mới khởi động các lần chuyển slot ghi trong cấu hình (`:641`).
- **Cấu hình:** JSON gồm các shard, mỗi shard có `slot_ranges`, `master`, `replicas`, `migrations`; mỗi node có `id`, `ip`, `port`, `health` (`cluster_config.cc:226-358`).
- **Chuyển hướng:** chỉ có `MOVED`, không có `ASK` (`main_service.cc:1288-1299`, `cluster_defs.cc:67-79`).
- **Quyền:** lệnh mang cờ `ADMIN` (`cluster_family.cc:1283`).

| Tiêu chí | (a) Bus Valkey | (b) Bus riêng | (c) Topology ngoài |
|---|---|---|---|
| Công sức (ngày, gồm phần dùng chung ~25) | 76–105 | 65–90 **[suy luận]** | 25–35 |
| Rủi ro đúng đắn | Cao: bầu chọn, epoch, phân vùng mạng | Cao như (a) | Thấp trong node; dồn sang bộ điều khiển |
| Rủi ro tương thích | Phải theo kịp định dạng bus của từng bản Valkey | Không có | Không có |
| `valkey-cli --cluster create` | Chạy | Có thể **[suy luận]** | Không: cần gossip để hội tụ |
| `valkey-cli --cluster check/reshard` | Chạy | Có thể | Chạy được nếu nút nhận `SETSLOT ... NODE` |
| Tự failover | Có (giai đoạn 4) | Không (như Garnet) | Không |
| Ghép với nút Valkey trong một cluster | Có | Không | Không |
| Chạy được `tests/unit/cluster` của Valkey | Phần lớn | Không | Rất ít |

Vì sao `--cluster create` cần gossip:
- `valkey-cli` chỉ gửi `CLUSTER MEET` từ mọi node tới node đầu tiên (`valkey/src/valkey-cli.c:7370-7406`).
- Sau đó nó chờ tới khi `CLUSTER NODES` của mọi node có cùng chữ ký (`:5593-5596`, `:6030-6045`).
- Không có gossip thì các node khác không bao giờ biết nhau.

Vì sao reshard kiểu cũ chạy được không cần bus: sau mỗi slot, `valkey-cli` tự gửi `CLUSTER SETSLOT <slot> NODE <id>` cho mọi primary, kể cả các nút không tham gia (`valkey-cli.c:5540-5551`).

Bộ test cluster của Valkey (69 file trong `valkey/tests/unit/cluster/`) dựng cluster bằng `CLUSTER MEET` từ node 0 (`valkey/tests/support/cluster_util.tcl:219-248`). Nên hầu hết chỉ chạy được khi đã có bus.

### 3.3 Khuyến nghị

Đi (c) làm bước đệm, rồi lên (a). Lý do:
- **Đúng luận điểm của dự án.** Hotstash bán sự thay thế không cần sửa client và công cụ (kế hoạch, dòng 13). Phần Sentinel cũng đã làm theo kiểu tương thích wire với Valkey thật (`docs/sentinel-trial.md`).
- **Có đường di chuyển.** Với (a), người dùng thêm nút Hotstash làm replica vào cluster Valkey đang chạy, rồi failover sang. Hai hướng còn lại không cho làm điều này.
- **Bỏ (b).** Nó có chi phí của (a) mà không có lợi ích của (a).
- **Không phí công.** Giai đoạn 1–2 dùng chung cho mọi hướng và tự đứng được: topology tĩnh cộng một bộ điều khiển bên ngoài.

Một điều kiện để không phải làm lại: bảng topology ở giai đoạn 1 phải theo đúng mô hình của Valkey:
- node ID 40 ký tự hex, shard ID, cờ;
- `configEpoch` cho từng primary, `currentEpoch` cho cả cluster;
- file topology tĩnh dùng đúng định dạng `nodes.conf`, tức output của `CLUSTER NODES` cộng dòng `vars currentEpoch … lastVoteEpoch …` (`cluster_legacy.c:1088-1096`).

Khi đó file người vận hành viết tay ở giai đoạn 1 chính là file bus ghi ra ở giai đoạn 3.

## 4. Những gì client thấy, và chỗ đặt trong Hotstash

Ở Valkey, kiểm tra chuyển hướng nằm trong `processCommand`. Nó bỏ qua lệnh đến từ primary và lệnh không có khóa (`valkey/src/server.c:4793-4811`), rồi gọi `getNodeByQuery` (`valkey/src/cluster.c:1066`).

Ở Hotstash, điểm tương ứng là `TxnState::feed` (`txn.rs:62-75`), ngay sau `slot_of`. Luồng replication (`Replay::apply`, `repl.rs:931`) không đi qua `feed`, nên tự nhiên được miễn như "obey client" của Valkey.

Đề xuất một hàm `cluster::route(slot, cờ kết nối, lệnh) -> Ok | Redirect(Reply)` thuần, không I/O, để kiểm bằng unit test theo đúng bảng quyết định của `getNodeByQuery`.

| Hạng mục | Quy tắc của Valkey | Việc trong Hotstash |
|---|---|---|
| `MOVED` | Slot thuộc node khác: `-MOVED <slot> <host>:<port>` (`cluster.c:1335-1338`, định dạng `:1362-1367`). Client TLS được báo cổng TLS (`:1363-1365`) | Tra bảng slot; chọn cổng theo kết nối có TLS hay không (`tls.rs`) |
| Slot không ai giữ | `-CLUSTERDOWN Hash slot not served` (`cluster.c:1119-1124`, `:1360-1361`) | Như Valkey |
| Cluster down | Khi không lành, chặn tất cả hoặc chỉ cho đọc, tùy `cluster-allow-reads-when-down` (`cluster.c:1270-1290`). Trạng thái "fail" khi số primary liên lạc được dưới quorum (`cluster_legacy.c:7474-7476`), hoặc thiếu slot khi bật `cluster-require-full-coverage` (`:7432`) | Giai đoạn 1–2: trạng thái do topology quyết định (đủ slot là `ok`). Từ giai đoạn 3: theo bus |
| `ASK` / `TRYAGAIN` | Slot đang MIGRATING: thiếu mọi khóa thì `ASK`, thiếu một phần thì `TRYAGAIN` (`cluster.c:1298-1311`). Slot đang IMPORTING và client đã `ASKING`: phục vụ, trừ lệnh nhiều khóa thiếu khóa thì `TRYAGAIN` (`:1312-1323`). Trong lúc chuyển, `MOVE` và `COPY` khác DB bị chặn (`:1220-1240`). `MIGRATE` luôn chạy tại chỗ (`:1292-1297`) | Giai đoạn 2. Đếm khóa có/không có bằng lookup không tác dụng phụ, theo shard nội bộ của slot |
| `ASKING` | Cờ một lần; xóa sau lệnh kế tiếp, trừ khi đang trong MULTI (`valkey/src/networking.c:3890`, `cluster.c:1626-1633`) | Thêm cờ vào `Conn`; thay chỗ `engine.rs:3757` |
| `READONLY` | Replica phục vụ lệnh đọc cho slot của primary của nó nếu kết nối đã `READONLY`; lệnh pub/sub shard cũng vậy (`cluster.c:1325-1333`, `:1638-1648`) | Cờ trong `Conn` thay cho `Cmd::Static` ở `cmd.rs:858`; lệnh ghi vẫn bị `on_replica` chặn (`cmd.rs:1387`) |
| Lệnh đang chặn (BLPOP…) | Khi slot đổi chủ: gỡ chặn với `MOVED` hoặc `CLUSTERDOWN` (`cluster.c:1384-1430`). Dragonfly hủy lệnh chặn khi đổi cấu hình (`cluster_family.cc:599-613`) | Đã có đường gỡ chặn khi đổi vai (`lib.rs:3416-3425`); mở rộng cho đổi chủ slot |
| Nhiều khóa | `CROSSSLOT` khi khác slot (`cluster.c:1088-1110`) | Đã có (`txn.rs:62-69`) |
| Transaction | Slot của EXEC là slot chung của các lệnh trong hàng (`cluster.c:1088-1107`). Bị chuyển hướng lúc EXEC thì cả transaction bị hủy; lúc xếp hàng thì transaction bị đánh dấu hỏng (`server.c:4801-4805`) | `TxnState` đã nhớ slot (`txn.rs:30-33`, `:151-160`). Thêm: chuyển hướng lúc xếp hàng thì đặt `dirty`; lúc EXEC thì kiểm lại vì slot có thể đã đổi chủ |
| Script | Khóa khai báo đi qua cùng đường. Script chạm khóa khác slot: "Script attempted to access keys that do not hash to the same slot" (`valkey/src/module.c:7081`). Chạm khóa không thuộc node: "non local key" (`:6984-7001`) | Thêm kiểm tra vào đường `redis.call` của `script.rs`; hiện chưa có |
| Pub/sub | `PUBLISH` lan ra mọi node qua bus; `SPUBLISH` chỉ tới các node của shard mình (`cluster_legacy.c:5770-5800`) | Giai đoạn 1–2 (chưa có bus): chỉ có subscriber cục bộ. Ghi rõ đây là giới hạn. Giai đoạn 3: gói PUBLISH/PUBLISHSHARD |
| Thông báo keyspace | Chỉ cục bộ: `notifyKeyspaceEvent` không gửi gì qua cluster (`valkey/src/notify.c:107`) | Giữ nguyên (`notify.rs`); client phải đăng ký trên từng primary |
| `CLUSTER SLOTS` | Mỗi dải: đầu, cuối, primary, rồi các replica còn sống (`cluster.c:1515-1534`); reply được cache và xóa khi topology đổi (`:1537`) | Sinh từ bảng; cache theo phiên bản topology |
| `CLUSTER SHARDS`, `NODES` | `cluster_legacy.c:8090`; `NODES` cùng định dạng `nodes.conf` (`:7851`) | Thay phần viết cứng ở `cluster.rs:229-268` |
| Nhiều database | Valkey 9 cho cluster nhiều DB; chuyển slot duyệt mọi DB (`cluster_migrateslots.c:1667`) | Hotstash đã có `--cluster-databases` (`crates/hotstash-server/src/main.rs:435-438`); mọi đường chuyển slot phải lặp theo DB |

## 5. Chuyển slot

### 5.1 Kiểu cũ: SETSLOT, MIGRATE, ASK

Trình tự `valkey-cli` dùng cho mỗi slot (`valkey-cli.c:5484-5551`):
1. `SETSLOT IMPORTING` ở nút đích, `SETSLOT MIGRATING` ở nút nguồn.
2. Lặp `GETKEYSINSLOT` và `MIGRATE ... KEYS ...` cho tới khi hết khóa.
3. `SETSLOT NODE` ở nút đích, rồi nút nguồn, rồi mọi primary còn lại.

`MIGRATE` làm ba việc:
- gửi `SELECT` (khi cần) và `RESTORE-ASKING` kèm payload `DUMP` cho từng khóa (`cluster.c:558-602`);
- xóa khóa ở nguồn, và lệnh này đi vào dòng replication dưới dạng `DEL` (`:648-676`);
- giữ kết nối tới nút đích trong một cache socket: tối đa 64 kết nối, đóng sau 10 giây không dùng (`:325-430`).

`MIGRATE` có các tùy chọn `COPY`, `REPLACE`, `AUTH`, `AUTH2`, `KEYS`. Mật khẩu được che trước khi phân tích lệnh (`:456-460`).

Mặc định, `valkey-cli --cluster reshard` vẫn dùng kiểu cũ; kiểu nguyên tử chỉ chạy khi có cờ `--cluster-use-atomic-slot-migration` (`valkey-cli.c:2828-2829`, `:7948-7949`).

Nhược điểm của kiểu cũ:
- Client thấy `ASK` và `TRYAGAIN`.
- Lệnh nhiều khóa có thể hỏng giữa chừng.
- `MIGRATE` một khóa lớn chặn nút nguồn trong lúc gửi **[suy luận, theo cách `MIGRATE` gửi đồng bộ trên luồng chính]**.

### 5.2 Kiểu nguyên tử của Valkey 9

Chạy `CLUSTER MIGRATESLOTS` trên nút nguồn. Các bước:
1. **Kết nối:** nguồn mở kết nối tới **cổng client** của đích, `AUTH`, rồi `CLUSTER SYNCSLOTS ESTABLISH`. Toàn bộ máy trạng thái ở `cluster_migrateslots.c:1114-1170` (nguồn) và `:440-510` (đích).
2. **Snapshot:** nguồn fork một tiến trình con, ghi các slot thành lệnh kiểu AOF, kết thúc bằng `SYNCSLOTS SNAPSHOT-EOF` (`:1664-1686`, `:1712-1780`).
3. **Stream:** từ đó, mọi lệnh ghi vào các slot này được chuyển tiếp (`clusterFeedSlotExportJobs`, `:1843-1870`). Lệnh không khóa, kể cả `MULTI`/`EXEC`, bị bỏ qua (`:1845-1848`).
4. **Chốt:** đích gửi `REQUEST-PAUSE`. Nguồn tạm dừng ghi khi phần còn chờ gửi nhỏ hơn `slot-migration-max-failover-repl-bytes` (`:1555-1572`), báo `PAUSED`. Đích gửi `REQUEST-FAILOVER`, nguồn trả `FAILOVER-GRANTED`.
5. **Nhận slot:** đích tự tăng epoch không cần đồng thuận và nhận slot (`performSlotImportJobFailover`, `:897-911`). Lúc này mới cần bus để lan cấu hình mới.

Ở đây không có `ASK`: trong suốt quá trình, nút nguồn vẫn là chủ slot nên client vẫn được phục vụ ở nguồn **[suy luận từ `getNodeByQuery`; chưa chạy thử]**.

Dragonfly làm tương tự:
- mỗi luồng có một "flow" riêng (`dragonfly/src/server/cluster/outgoing_slot_migration.cc:82-97`), stream bằng journal;
- lúc chốt, tạm dừng **mọi** client, và để TODO "chỉ chặn các slot đang chuyển" (`:428-435`);
- lặp lại `DFLYMIGRATE ACK` theo số lần thử (`:380-395`, `:457-500`).

### 5.3 Ánh xạ vào shard nội bộ và dòng replication của Hotstash

**Chỉ mục theo slot.**
- Valkey giữ một bảng băm cho mỗi slot (`kvstore`), nên đếm khóa của slot là O(1) (`cluster.c:846-847`).
- Hotstash trộn mọi slot của một shard trong một bảng, và đếm bằng cách quét (`engine.rs:3966-3975`).
- `valkey-cli` gọi `COUNTKEYSINSLOT` cho từng slot khi check và fix (`valkey-cli.c:6198`, `:6502`), và `GETKEYSINSLOT` liên tục khi reshard. 16384 lần quét cả shard là không chấp nhận được.

Đề xuất theo hai bước:
1. **Giai đoạn 1:** bộ đếm khóa theo slot trong mỗi shard (tăng/giảm ở chỗ thêm/xóa khóa).
2. **Giai đoạn 2:** duyệt khóa của một slot theo từng đoạn bằng `scan_chunk` có lọc slot, không giữ gate exclusive. Sau khi đo, nếu cần mới tách bảng băm của shard thành bảng theo slot như `kvstore`.

**Snapshot không fork.** `repl_snapshot` dừng cả node trong lúc tạo RDB (`engine.rs:1377-1389`); dùng cho chuyển slot thì quá đắt. Đề xuất cắt theo từng shard nội bộ:
1. Giữ khóa của shard S.
2. Ghi từng khóa thuộc các slot cần chuyển thành `RESTORE ... REPLACE ABSTTL`; mã mã hóa đã có ở `encode_state` (`repl.rs:385`).
3. Đăng ký bộ lọc chuyển tiếp cho S, rồi thả khóa.

Vì lệnh ghi lên S được ghi vào dòng replication trong lúc còn giữ S (`repl.rs:17-20`), mỗi shard có một điểm cắt nhất quán. Transaction và script chạy ở gate exclusive nên không xen vào giữa. **[suy luận; cần test chứng minh, kể cả với khóa hết hạn trong lúc cắt.]** Cách đơn giản nhất là giữ S trong cả lúc ghi. Nếu đo thấy trễ, chuyển sang cắt theo con trỏ: khóa sau con trỏ chưa cần chuyển tiếp, khóa trước con trỏ thì chuyển tiếp.

**Luồng ghi.**
- `Repl::append(db, body)` hiện không biết slot (`repl.rs:719`). Thêm tham số slot từ chỗ `Plan` mã hóa hiệu ứng; mỗi job chuyển slot lọc theo dải slot của mình.
- Phía đích dùng lại `Replay::apply` (`repl.rs:931`), vì `SYNCSLOTS` gửi lệnh RESP thường (`cluster_migrateslots.c:1680-1685`).
- Đích ghi vào dòng replication của chính nó, nên replica của đích nhận theo **[suy luận]**. Valkey có trạng thái riêng cho replica của đích (`:487-508`).

**Dọn dẹp.** Nguồn xóa khóa của slot đã chuyển. Các lệnh `DEL` đi vào dòng replication và file bền vững (`persist.rs` ghi bản ghi `DEL key`).

### 5.4 Làm kiểu nào trước

**Kiểu cũ trước**, vì:
- `valkey-cli` mặc định dùng nó;
- `MIGRATE` là lệnh riêng đã có trong kế hoạch (dòng 681);
- các test `slot-migration.tcl`, `half-migrated-slot.tcl` cần nó;
- `ASK`/`TRYAGAIN` phải có để làm việc chung với nút Valkey đang chuyển slot kiểu cũ.

Kiểu nguyên tử cần tăng epoch và lan cấu hình, nên đặt sau bus. Các lệnh `SYNCSLOTS` chạy trên cổng client nên phần stream làm được trước, phần chốt chờ bus.

## 6. Failover trong cluster

### 6.1 Valkey làm thế nào

- **Phát hiện lỗi:** một node bị coi là PFAIL khi quá `cluster-node-timeout`. Nó thành FAIL khi đủ quorum primary báo (`size/2+1`), rồi tin FAIL được phát cho mọi node (`cluster_legacy.c:2828-2850`).
- **Bầu chọn:** replica chờ một khoảng gồm độ trễ cố định, phần ngẫu nhiên, và phần tỉ lệ với hạng của nó; hạng xếp theo offset (`:6436-6459`, `:6133`). Sau đó gửi `FAILOVER_AUTH_REQUEST`.
- **Bỏ phiếu:** mỗi primary bỏ một phiếu cho mỗi epoch (`lastVoteEpoch`, `:5935`, `:6004`). Đủ `size/2+1` phiếu (`:6354`, `:6574`) thì replica lấy epoch của cuộc bầu làm `configEpoch` và nhận các slot của primary cũ (`:6574-6588`, `:6298-6340`). Lý do từ chối được trả bằng NACK (`cluster_legacy.h:427-433`).
- **`CLUSTER FAILOVER` (mặc định):** replica gửi MFSTART; primary dừng client (`:4846-4848`); replica bắt kịp offset rồi bầu với cờ FORCEACK (`:5821-5832`).
- **`FORCE`:** bỏ bước phối hợp với primary nhưng vẫn bầu (`:9084-9095`).
- **`TAKEOVER`:** không bầu; tự tăng epoch không cần đồng thuận rồi nhận slot (`:9076-9083`, `:2615-2644`). Chính chú thích của Valkey cảnh báo cách này có thể vi phạm quy tắc "failover sau cùng thắng" (`:2610-2614`).

### 6.2 Quan hệ với mã FAILOVER/PSYNC hiện có

- **Đường dữ liệu dùng lại hết.** Trong cluster của Valkey, primary và replica vẫn dùng replication thường. `follow.rs` (PSYNC, partial resync, `Replay`) và việc đổi replication ID (`repl.rs:840-851`) dùng nguyên. `CLUSTER REPLICATE <id>` chỉ cần mở link `follow.rs` tới địa chỉ của node đó, vì `REPLICAOF` bị cấm ở chế độ cluster.
- **Manual failover dùng lại một phần.** `FAILOVER` của Hotstash do primary chủ động, qua `PSYNC FAILOVER` (`lib.rs:665-790`, `follow.rs:63-95`). `CLUSTER FAILOVER` thì do replica chủ động, qua bus. Phần dùng lại được là cơ chế giữ lệnh ghi (`lib.rs:584-600`) và phép kiểm "replica đã có toàn bộ dòng" của `failover_tick` (`lib.rs:665-703`).
- **Bầu chọn và epoch là mã mới hoàn toàn,** chỉ có từ giai đoạn 4.
- **Giai đoạn 1–2 (chưa có bus):** bộ điều khiển bên ngoài quyết định, như Garnet và Dragonfly. Nó gửi `CLUSTER FAILOVER TAKEOVER` cho replica (chỉ đổi bảng cục bộ), rồi đẩy topology mới cho mọi node. Node thấy mình không còn là chủ và được ghi là replica của node mới thì tự mở link `follow.rs`. Dragonfly có bước tương tự: `ReconcileMasterSlots`/`ReconcileReplicaSlots` (`cluster_family.cc:1195-1278`).

Sentinel không giúp được: Sentinel đổi vai bằng `REPLICAOF`, mà lệnh này bị cấm ở chế độ cluster ở cả Valkey và Hotstash.

## 7. Trạng thái bền vững, bảo mật, các kiểu hỏng

### 7.1 `nodes.conf`

- **Nội dung:** cùng định dạng `CLUSTER NODES`, thêm dòng `vars currentEpoch … lastVoteEpoch …` (`cluster_legacy.c:1088-1096`).
- **Ghi đĩa:** ghi và fsync mỗi khi epoch đổi, trước khi hành động tiếp (ví dụ `:2638`, `:6578-6582`). Có file khóa `nodes.conf.lock` (`:1281-1290`). File hỏng thì dừng tiến trình (`:1085-1086`).
- **Vì sao fsync quan trọng:** tính đúng của bỏ phiếu dựa vào việc `lastVoteEpoch` đã nằm trên đĩa. Node bỏ phiếu, khởi động lại, rồi quên thì có thể bỏ phiếu hai lần trong một epoch.
- **Cách ghi trong Hotstash:** file tạm, fsync, rename, fsync thư mục; khóa giống `persist.rs`. Không trộn vào file dữ liệu.

### 7.2 Bảo mật

- **Bus của Valkey không xác thực.** Trong `cluster_legacy.c`, tìm `auth` chỉ ra các biến `failover_auth_*`. Ai tới được cổng bus thì gửi được MEET và các gói khác. Valkey trông vào cô lập mạng.
- **TLS trên bus:** bật bằng `tls-cluster` (`:182`, `:202`, `:891`).
- **Ngược lại:**
  - Chuyển slot nguyên tử `AUTH` trên cổng client và che mật khẩu (`cluster_migrateslots.c:95-98`, `:1208-1216`).
  - Garnet xác thực gossip bằng user riêng của cluster.
  - Dragonfly yêu cầu quyền ADMIN cho `DFLYCLUSTER`.
- **Đề xuất cho Hotstash:**
  - Bus chỉ bind vào giao diện nội bộ. Khi không cấu hình `bind`, từ chối mở bus ra ngoài; Valkey chỉ cảnh báo trường hợp này (`:1637`).
  - Có TLS cho bus, dựa trên `tls.rs`.
  - Các lệnh quản trị cluster (`SETSLOT`, `ADDSLOTS`, lệnh đẩy topology) thuộc nhóm `@admin` của ACL.
  - Một phần mở rộng xác thực riêng của Hotstash (ví dụ HMAC với khóa chung) sẽ phá cluster trộn với Valkey, nên chỉ có thể là tùy chọn khi cả cluster là Hotstash. Xem câu hỏi mở.
- **Kiểm gói tin.** Valkey kiểm độ dài từng loại gói và từng phần mở rộng trước khi đọc (`clusterIsValidPacket`, `:3990-4095`); phần mở rộng lạ thì ghi log rồi bỏ qua (`:3854-3856`). Bộ đọc gói của Hotstash phải fuzz ngay từ đầu, cùng mức với các đợt rà soát bảo mật trước.
- **Chế độ bảo vệ:** chưa kiểm Valkey có áp protected mode lên cổng bus hay không. **[chưa kiểm chứng]**

### 7.3 Các kiểu hỏng

- **Split brain.** Primary nằm ở phía thiểu số vẫn nhận ghi tới khi hết `node-timeout`. Ghi trong khoảng đó mất sau khi phía đa số failover. Đây là hành vi của chính thuật toán Valkey, không phải lỗi cài đặt (kế hoạch, dòng 104). Phải nói rõ trong tài liệu người dùng.
- **Va chạm epoch.** Hai primary cùng `configEpoch`: node có ID nhỏ hơn tự tăng epoch (`cluster_legacy.c:2692-2705`; Garnet chép lại ở `ClusterConfig.cs:1577-1602`).
- **Node cũ quay lại với cấu hình cũ.** Gói UPDATE và `clusterUpdateSlotsConfigWith` sửa nó (`:5648`, `:3285`).
- **Topology tĩnh (giai đoạn 1–2).** Bộ điều khiển đẩy hai cấu hình khác nhau cho hai node thì có hai chủ cho một slot. Dragonfly chỉ so cấu hình mới có giống cấu hình cũ không (`cluster_family.cc:566-569`). Hotstash nên kèm một số phiên bản tăng dần trong cấu hình đẩy vào, và từ chối bản cũ hơn.

## 8. Kế hoạch theo giai đoạn

Ước lượng cho một kỹ sư, sai số khoảng ±40%. Chưa tính 20% dự phòng cho gỡ lỗi tương thích.

| GĐ | Nội dung | Ngày | Test |
|---|---|---|---|
| 1. Bảng topology và chuyển hướng | Bảng node/slot theo mô hình Valkey; đọc file `nodes.conf` viết tay (`--cluster-config-file`); một lệnh đẩy topology nguyên khối, có phiên bản; `ADDSLOTS[RANGE]`, `DELSLOTS[RANGE]`, `SETSLOT … NODE`, `FLUSHSLOTS`, `REPLICATE` (mở `follow.rs`), `RESET`; `cluster::route` với `MOVED`/`CLUSTERDOWN`; cờ `READONLY`/`ASKING`; gỡ lệnh chặn khi đổi chủ slot; kiểm khóa trong script; `CLUSTER SLOTS/SHARDS/NODES/INFO` sinh từ bảng và cache; bộ đếm khóa theo slot | 14–18 | Unit: bảng quyết định chép từ các nhánh của `getNodeByQuery`; định dạng `SLOTS/SHARDS/NODES` so với Valkey. Tích hợp: 3 primary + 3 replica Hotstash với file tĩnh; redis-py `RedisCluster` và ioredis `Cluster` ghi/đọc 100k khóa trên mọi slot, pipeline, transaction có hash tag, `MOVED` sau khi đẩy topology mới, đọc replica với `READONLY`; `valkey-cli --cluster check`. Mở rộng `crates/hotstash/tests/cluster.rs` |
| 2. Chuyển slot kiểu cũ | `SETSLOT IMPORTING/MIGRATING/STABLE`; `MIGRATE` đủ cú pháp và cache socket; `RESTORE-ASKING`; `ASK`/`TRYAGAIN`; `GETKEYSINSLOT` không quét cả shard; xóa khóa của slot không còn sở hữu | 10–14 | `valkey-cli --cluster reshard/rebalance/fix` giữa các nút Hotstash, trong lúc redis-py và ioredis chạy tải ghi: không mất lần ghi nào đã được xác nhận, client đi theo `ASK`; `MIGRATE` Hotstash→Valkey và Valkey→Hotstash. **Điểm dừng: chủ dự án quyết có đi tiếp không** |
| 3. Cluster bus tương thích Valkey | Codec nhị phân và fuzz; MEET/PING/PONG, gossip, các phần mở rộng, header light; PFAIL/FAIL; UPDATE; PUBLISH/PUBLISHSHARD; va chạm epoch; ghi `nodes.conf`; `FORGET` và blacklist; trạng thái cluster. Chưa có bầu chọn | 25–35 | `valkey-cli --cluster create/check/add-node/del-node`; cluster trộn 3 Valkey 9.1.3 + 3 Hotstash. Valkey `tests/unit/cluster`: `cluster-slots`, `cluster-shards`, `cluster-nodes-slots`, `cluster-multiple-meets`, `cluster-reliable-meet`, `packet`, `links`, `info`, `hostnames`, `multidb`, `pubsub`, `sharded-pubsub`, `faildet`, `failure-marking`, `update-msg` |
| 4. Failover trong cluster | Bầu chọn, phiếu, NACK, `lastVoteEpoch` bền vững, hạng replica, `cluster-replica-validity-factor`; `CLUSTER FAILOVER [FORCE\|TAKEOVER]` với MFSTART và tạm dừng ghi | 12–18 | `failover`, `failover2`, `manual-failover`, `manual-takeover`, `faster-failover`, `failover-nack`, `replica-priority`, `no-failover-option`, `slave-selection`; kịch bản kill và chia mạng trên WSL2 có tải client, đếm số lần ghi mất, so với cùng kịch bản trên Valkey |
| 5. Chuyển slot nguyên tử | `CLUSTER MIGRATESLOTS/SYNCSLOTS/GETSLOTMIGRATIONS/CANCELSLOTMIGRATIONS`, tương thích ở mức lệnh `SYNCSLOTS`; snapshot theo shard không fork; lọc dòng ghi theo slot; bước chốt | 15–20 | `cluster-migrateslots`, `many-slot-migration`, `pubsubshard-slot-migration`; `valkey-cli --cluster reshard --cluster-use-atomic-slot-migration`; chuyển Hotstash↔Valkey cả hai chiều |
| | **Tổng** | **76–105** | |

Ngoài phạm vi (ghi rõ để không trôi):
- `CLUSTER SLOT-STATS` và tối ưu theo availability zone (vẫn lưu và gossip trường AZ, nhưng không dùng);
- gói tin module (loại 9);
- tự chuyển replica sang primary thiếu replica (`cluster-allow-replica-migration`), có thể làm sau giai đoạn 4;
- proxy cluster, lệnh nhiều khóa xuyên slot, active-active;
- operator Kubernetes riêng, tự động chia lại slot.

## 9. Rủi ro và câu hỏi mở

### Rủi ro

1. **Đúng đắn của phần phân tán.** Lỗi bầu chọn, epoch, hoặc fsync `nodes.conf` chỉ lộ ra khi mạng bị chia hay node chết đúng lúc. Ngay thuật toán gốc cũng có thể mất ghi đã xác nhận. Cách giảm: chạy bộ test của Valkey, thêm kịch bản tiêm lỗi, và luôn so với Valkey trên cùng kịch bản.
2. **Định dạng bus thay đổi theo bản Valkey.** Header light, phần mở rộng, NACK là các phần đã được thêm vào định dạng gốc (`cluster_legacy.h:175-186`, `:243-252`, `:427-433`); thời điểm thêm từng phần tôi chưa kiểm. Hotstash phải theo từng bản minor. Cách giảm: chốt một bản Valkey để đối chiếu (9.1.3); bỏ qua phần mở rộng lạ như Valkey; CI có cluster trộn.
3. **Không có fork.** Snapshot slot và full sync phải giữ khóa shard hoặc dừng cả node (`engine.rs:1377-1379`). Trong cluster, replica mới và failover xảy ra thường hơn, nên trễ đuôi khi chuyển slot hoặc full sync sẽ lộ rõ hơn. Phải đo ngay ở giai đoạn 2.
4. Chỉ mục theo slot thêm chi phí vào đường nóng (thêm/xóa khóa).
5. Ước lượng còn thô. Phần bus là mã mới hoàn toàn, chưa có tiền lệ trong Hotstash.

### Câu hỏi cho chủ dự án

1. Topology tĩnh cộng bộ điều khiển bên ngoài (giai đoạn 1–2) có đủ cho nhóm người dùng đầu không? Hay `valkey-cli --cluster create` phải chạy ngay? Câu trả lời quyết định giai đoạn 3 có nằm trước 1.0 không.
2. Lệnh đẩy topology: lấy tên và định dạng nào? Tôi đề xuất một lệnh riêng nhận đúng văn bản `nodes.conf` kèm số phiên bản (tên dự kiến `HCLUSTER SETCONFIG`), thay vì JSON như Dragonfly.
3. Ở giai đoạn 1–2, ai là bộ điều khiển? Viết một công cụ nhỏ `hotstash-cluster`, hay chỉ viết tài liệu thao tác tay?
4. Cluster trộn Valkey + Hotstash là cấu hình được hỗ trợ, hay chỉ dùng để test?
5. Chấp nhận thêm khoảng 8–16 byte mỗi khóa cho chỉ mục theo slot nếu bộ đếm và duyệt theo đoạn không đủ nhanh không? **[con số là suy luận]**
6. Có muốn xác thực bus riêng của Hotstash không, biết rằng bật nó thì không trộn với Valkey được?
7. Ở giai đoạn 1–2, `PUBLISH`/`SPUBLISH` chỉ tới subscriber cục bộ. Chấp nhận giới hạn này tới khi có bus không?

### Câu trả lời mặc định

Chủ dự án để tôi quyết (2026-11-17). Nếu chưa có câu trả lời khác, giai đoạn 1 làm theo các mặc định sau:

1. Topology tĩnh đủ cho bản đầu. Bus (giai đoạn 3) quyết sau điểm dừng của giai đoạn 2.
2. Dùng `HCLUSTER SETCONFIG <phiên bản> <văn bản nodes.conf>`. Phiên bản nhỏ hơn hoặc bằng phiên bản đang giữ thì bị từ chối.
3. Chỉ viết tài liệu thao tác tay kèm file mẫu cho 3 primary + 3 replica. Chưa viết công cụ `hotstash-cluster`.
4. Cluster trộn Valkey + Hotstash chỉ dùng để test, cho tới khi xong giai đoạn 4 và qua bộ test cluster của Valkey.
5. Làm bộ đếm khóa theo slot trước và đo. Chỉ thêm chỉ mục theo slot nếu đo thấy cần, và chỉ khi bật chế độ cluster.
6. Không có xác thực bus riêng. Bảo vệ bus như Valkey: mạng riêng và TLS.
7. Chấp nhận, và ghi rõ giới hạn này trong README.
