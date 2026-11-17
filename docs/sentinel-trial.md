# Valkey Sentinel 9.1.3 quản lý nút HCache: kết quả chạy thử

Ngày 2026-11-17, đợt 3. Môi trường: WSL2 Ubuntu 24.04, 8 vCPU. Dùng Valkey 9.1.3 (`valkey-sentinel`, `valkey-server`, `valkey-cli`) dựng từ mã nguồn. HCache là bản release dựng từ `spike/` hiện tại.

## Kết luận

Một Sentinel thật của Valkey 9.1.3 giờ quản lý được nhóm ba nút HCache trọn vòng:
- phát hiện các nút;
- failover tự động khi primary chết;
- đưa primary cũ về làm replica khi nó chạy lại;
- `SENTINEL FAILOVER`;
- `SENTINEL FAILOVER ... COORDINATED`.

Cùng kịch bản chạy với ba nút Valkey cho kết quả và thời gian như nhau: mỗi bên qua 19/19 bước.

Trước đợt 3, Sentinel phát hiện được nút HCache nhưng không failover được: lần nào cũng kết thúc bằng `-failover-abort-slave-timeout`. Có hai chỗ chặn:
- `CLIENT KILL` trong `MULTI` bị từ chối, nên cả khối đổi vai bị `EXECABORT`;
- `SLAVEOF`/`REPLICAOF` trong `EXEC` không chạy (`ERR REPLICAOF needs the server`).

Mục "Đã sửa trong HCache" liệt kê các thay đổi.

## Cách chạy lại

```bash
bench/sentinel_trial.sh hcache /root/src/valkey-9.1.3/src /root/spike-sentinel/target/release/hcache-server
bench/sentinel_trial.sh valkey /root/src/valkey-9.1.3/src
```

- **Cổng:** 7500–7502 là các nút; 7510–7512 là ba Sentinel, đều trên 127.0.0.1.
- **Cấu hình Sentinel:** `sentinel monitor mymaster 127.0.0.1 7500 2`, `down-after-milliseconds 2000`, `failover-timeout 10000`, `parallel-syncs 1`.
- **HCache:** chạy với `--workers 2`, không có file dữ liệu.
- **Kết quả:** mỗi bước in `PASS` hoặc `FAIL`, cuối cùng là các sự kiện trong log Sentinel.

## Kết quả (lần chạy cuối, 2026-11-17 15:37–15:39)

| Bước | HCache | Valkey |
|---|---|---|
| Phát hiện: 2 replica, 2 Sentinel khác, run ID 40 ký tự, `SENTINEL CKQUORUM` OK | PASS | PASS |
| `kill -9` primary 7500, Sentinel chuyển sang primary mới | PASS, 5 s (các lần trước: 4 s, 5 s) | PASS, 5 s (các lần trước: 4 s, 5 s, 7 s) |
| Khóa ghi trước khi primary chết có trên primary mới; replica còn lại theo primary mới | PASS | PASS |
| Chạy lại 7500: Sentinel gửi `+convert-to-slave`, 7500 thành replica | PASS, 8 s (lần trước: 12 s) | PASS, 8 s (lần trước: 12 s) |
| `SENTINEL FAILOVER mymaster`: promote, reconf replica, primary cũ thành replica | PASS, 3 s | PASS, 3 s |
| `SENTINEL FAILOVER mymaster COORDINATED` | PASS, 3 s | PASS, 3 s |
| Sau COORDINATED: nút cũ là replica, trả `READONLY`, không còn pause (PUBLISH trả lời ngay) | PASS | PASS |
| Sau COORDINATED: khóa ghi ngay trước đó có trên primary mới; khóa ghi sau đó có trên cả 3 nút | PASS | PASS |

Thời gian 8–12 s ở bước chạy lại primary cũ là do Sentinel: nó chỉ đổi vai một nút báo `master` sau khi nút đó báo như vậy liên tục 8 s (`sentinel_publish_period * 4`).

Một lần chạy với HCache ngay trước đó (15:34–15:37) trượt 3 bước: failover tự động, dữ liệu trên primary mới, và 7500 về làm replica. Log của Sentinel cho thấy nó vào TILT hai lần (15:34:47 và 15:35:11) dù `tilt-trigger` đã là 10 s, tức đồng hồ thực nhảy hơn 10 s. Khi ra khỏi TILT, các replica đã mất liên kết quá lâu nên Sentinel báo `-failover-abort-no-good-slave`, như mô tả ở mục "Môi trường". Các bước `SENTINEL FAILOVER` và COORDINATED của lần đó vẫn đạt. Cùng lúc đó bộ test Linux cũng có một test trượt vì đồng hồ thực nhảy khoảng 23,7 s. Lần chạy kế tiếp với Valkey rồi với HCache, không có TILT, đều đạt 19/19.

Ở lần chạy thủ công trước đó (cùng ngày, 14:51), trạng thái cuối của HCache khớp Valkey: 7502 là primary, 7500 và 7501 là replica đã kết nối. Lúc chụp, replica của Valkey còn ở `handshake` vì đang đồng bộ toàn phần, còn của HCache đã `connected`.

## Sentinel gửi gì cho nút

Sentinel không đọc trả lời của các khối này. Nó chỉ xem hiệu quả qua `INFO` lần sau, nên một lệnh hỏng chỉ lộ ra thành timeout.

- **Đổi vai** (`sentinelSendReplicaOf`): `MULTI`, `SLAVEOF host port` hoặc `SLAVEOF NO ONE`, `CONFIG REWRITE`, `CLIENT KILL TYPE normal`, `CLIENT KILL TYPE pubsub`, `EXEC`. Nếu nút báo `master_failover_state` khác `no-failover`, Sentinel chèn thêm `FAILOVER ABORT`.
- **COORDINATED, gửi cho primary** (`sentinelFailoverTo`): `MULTI`, `CLIENT PAUSE <t> WRITE`, `FAILOVER TO host port TIMEOUT <t>`, `EXEC`.
- **COORDINATED, sau khi promote, gửi cho cả hai nút** (`sentinelKillClients`): `MULTI`, `CONFIG REWRITE`, `CLIENT KILL TYPE normal`, `CLIENT KILL TYPE pubsub`, `CLIENT UNPAUSE`, `EXEC`.
- **Thường xuyên:** `PING` khoảng mỗi 1 s và `INFO` mỗi 10 s (mỗi 1 s cho replica của primary đã chết). Cứ 2 s một lần có `PUBLISH __sentinel__:hello ...`; Sentinel dừng gửi lệnh này khi nút đang FAILOVER. Kết nối pub/sub dùng `SUBSCRIBE __sentinel__:hello`. Mỗi kết nối mở đầu bằng `CLIENT SETNAME sentinel-<id>-cmd|pubsub`.

## Đã sửa trong HCache (đợt 3)

1. **`CLIENT KILL` được dùng trong `MULTI`** (`hcache-core/src/txn.rs`). Mặc định nó bỏ qua chính kết nối gọi, như Valkey.
2. **`EXEC` giao cho server phần chỉ server làm được: `CLIENT ...`, `REPLICAOF`/`SLAVEOF` và `FAILOVER`.** Engine chạy phần còn lại trước, giữ chỗ cho từng lệnh này. Sau đó server chạy chúng đúng thứ tự xếp hàng và điền trả lời vào đúng vị trí (`split_exec`, `finish_exec` trong `hcache-server/src/lib.rs`).
   - `REPLICAOF` đổi vai trên luồng riêng; `EXEC` đợi xong mới chạy tiếp.
   - Nếu `WATCH` hỏng thì không lệnh nào chạy, kể cả phần của server.
3. **`INFO` có `run_id`:** 40 chữ số hex, mới mỗi lần khởi động. Sentinel dùng nó để nhận ra nút đã khởi động lại (`+reboot`) và để phá thế hòa khi chọn replica.
4. **`INFO <mục> [mục ...]` chỉ trả các mục được hỏi.** `default`, `all`, `everything` trả tất cả; tên lạ trả rỗng.
   - Bỏ mục `# Memory` bị lặp.
   - `tracking_clients` chuyển về mục `# Clients` như Valkey.
5. **`CONFIG GET/SET` có `replica-priority`, `replica-announced` và các tên cũ.**
   - `replica-priority` (tên cũ `slave-priority`) nhận 0..2147483647; mức 0 nghĩa là không bao giờ được promote.
   - `replica-announced` (tên cũ `slave-announced`) nhận yes/no.
   - `INFO replication` của replica in `slave_priority` và `replica_announced` theo giá trị đang đặt.
6. **`CLIENT PAUSE <ms> [WRITE|ALL]` và `CLIENT UNPAUSE` chạy thật.**
   - `WRITE` giữ lệnh ghi và các lệnh Valkey gọi là "may-replicate": `PUBLISH`, `SPUBLISH`, script không read-only.
   - `ALL` (mặc định) giữ mọi lệnh, trừ `CLIENT UNPAUSE` và `REPLCONF` của replica.
   - Hai lần pause gộp lại tới mốc muộn hơn; `WRITE` không rút ngắn một `ALL` đang chạy.
   - Như Valkey, trong lúc pause không có expire chủ động, không có PING tới replica và không có `REPLCONF GETACK`.
   - Như Valkey, replica trả `READONLY` cho lệnh ghi trước khi xét pause.
7. **`FAILOVER` giữ cả `PUBLISH`/`SPUBLISH`**, trước đây chỉ giữ lệnh ghi. Lý do: HCache nhân bản PUBLISH sang replica, nên nếu PUBLISH chạy giữa lúc bàn giao thì offset sẽ lệch.
8. **`CONFIG REWRITE` trả `ERR The server is running without a config file`**, như Valkey khi không có file cấu hình. Trước đây nó trả `OK`.

Test: `crates/hcache/tests/sentinel.rs` có 7 test, gồm cả hai khối đổi vai và khối COORDINATED đúng như Sentinel gửi. Test chạy qua trên Windows và WSL.

## Môi trường: đồng hồ WSL và TILT

Đo trong WSL bằng một vòng lặp ngủ 50 ms, 80 s cho mỗi trường hợp:
- **Không chạy gì:** wall-clock nhảy tới khoảng 3 s, đều đặn mỗi khoảng 35 s.
- **Ba nút HCache:** như trên.
- **Ba nút Valkey:** như trên.
- **Đồng hồ monotonic:** đều trong cả ba trường hợp; bước lớn nhất là 0,05 s.

Sentinel tính khoảng thời gian bằng wall-clock. Một bước lớn hơn `tilt-trigger` (mặc định 2000 ms) đẩy nó vào TILT: 30 s không hành động. Ở lần chạy đầu, TILT làm Sentinel phát hiện primary chết trễ khoảng 35 s. Đến lúc đó, các replica đã mất liên kết lâu hơn 10 lần `down-after-milliseconds`, nên Sentinel loại cả hai và báo `-failover-abort-no-good-slave`. Đây là cơ chế của Sentinel, không phụ thuộc loại nút.

Kịch bản đặt `SENTINEL DEBUG tilt-trigger 10000` cho từng Sentinel. Mức này đủ cho các bước nhảy khoảng 3 s, nhưng có lúc đồng hồ nhảy hơn 10 s (lần chạy 15:34 ở trên), và khi đó Sentinel vẫn vào TILT. Chỉnh này nằm trong Sentinel, không đụng tới hệ thống. Khi chạy thật, cần giữ giờ của máy ổn định; việc đó không làm ở đây vì là thiết lập hệ thống.

## Còn khác Valkey hoặc chưa kiểm

- **Không có file cấu hình.**
  - `CONFIG REWRITE` luôn trả lỗi, nên vai trò Sentinel đặt không còn sau khi khởi động lại. Nút chạy lại lên làm primary, rồi Sentinel đưa nó về replica sau khoảng 8–12 s (đã kiểm).
  - Nếu nút có `--data`, trong khoảng đó client nối thẳng vào nó vẫn ghi được, và các ghi này mất khi nút đồng bộ lại.
  - Valkey không có file cấu hình cũng vậy.
- **Chưa có `min-replicas-to-write` / `min-replicas-max-lag`.** Vì vậy chưa chặn được ghi vào một primary bị cô lập (split-brain). Valkey dùng hai thiết lập này cùng Sentinel để giảm mất dữ liệu.
- **Chưa có `replica-announce-ip` / `replica-announce-port`.** Sau NAT hoặc Docker, Sentinel thấy địa chỉ replica theo IP kết nối cộng `listening-port`.
- **Chưa thử:**
  - Sentinel với mật khẩu (`requirepass` cùng `sentinel auth-pass`, `masterauth`);
  - TLS;
  - nhiều máy thật;
  - phân vùng mạng (mới chỉ thử `kill -9`);
  - `replica-priority 0` với Sentinel thật (mới thử bằng test).
- **HCache chưa tự làm Sentinel.** Thiết kế chế độ sentinel nằm ở [sentinel-spec.md](sentinel-spec.md).
- **`MONITOR` của HCache hiện cả lệnh quản trị** (`SLAVEOF`, `CONFIG`), Valkey thì giấu. Điểm này không ảnh hưởng Sentinel.

## Dọn dẹp

Kịch bản dừng mọi tiến trình nó mở theo PID: `kill`, rồi `kill -9`, không dùng `pkill`. Thư mục làm việc là `/tmp/hcache-sentinel-<loại>` trong WSL. Các bản sao dùng khi thử là `/root/spike-sentinel`, `/root/sent` và `/root/hcache-server.*`; có thể xóa.
