# JSON.* trong HCache: tập con JSONPath hỗ trợ và chỗ khác valkey-json

HCache làm JSON.* theo valkey-json 1.0/1.1: cùng tên lệnh, cờ, nhóm ACL, cấu hình, cú pháp đường dẫn, câu trả lời và chuỗi lỗi. Kiểu dữ liệu tên `ReJSON-RL`, RDB module ID `0x45e25238df912c03` (encver 3), nên file RDB và `DUMP`/`RESTORE` qua lại được với Valkey có nạp valkey-json.

Nguồn: mã nguồn valkey-json (`src/json/selector.cc`, `json.cc`, `dom.cc`) và bộ test `tst/integration/test_json_basic.py`, chỉ đọc chứ chưa chạy module thật; chi tiết từng lệnh ở [json-spec.md](json-spec.md). Phần RedisJSON lấy từ tài liệu redis.io.

## 1. Hai kiểu đường dẫn

| | Legacy | JSONPath (v2) |
|---|---|---|
| Nhận ra bằng | ký tự đầu **không** phải `$` (`.a`, `a.b`, `[0]`) | ký tự đầu là `$` |
| Gốc | `.` | `$` |
| Giá trị được chọn | lệnh làm trên giá trị đầu tiên (đọc) hoặc tất cả (ghi), trả một giá trị | lệnh làm trên tất cả, trả mảng (mỗi chỗ khớp một phần tử) |
| Không khớp gì | lỗi `NONEXISTENT JSON path does not exist` | mảng rỗng, hoặc `null` ở chỗ sai kiểu |
| Sai kiểu | lỗi `WRONGTYPE ...` nếu không giá trị nào đúng kiểu | `null` cho chỗ đó |

- Bỏ đường dẫn thì mặc định là `.` (legacy), nên `JSON.GET k` trả nguyên văn bản tài liệu, không bọc mảng.
- `$` đứng ở chỗ khác vị trí đầu: `SYNTAXERR Dollar sign cannot apply to non-root element`.
- `JSON.GET` nhiều đường dẫn: có một đường `$` thì cả câu trả lời theo kiểu JSONPath (một object `{path: [...]}`).

## 2. Cú pháp hỗ trợ

| Cú pháp | Ý nghĩa |
|---|---|
| `$`, `.` | gốc |
| `.name`, `name` (đầu đường legacy) | member của object |
| `['name']`, `["name"]` | member, tên có ký tự đặc biệt; khoảng trắng trong ngoặc được phép (`$.[ "a" ]`). Ngoặc kép đọc như chuỗi JSON (đủ escape, cả `\uXXXX`); ngoặc đơn nhận `\'`, `\\`, `\t`, `\b`, `\f`, `\n`, `\r` |
| `..name`, `..[...]`, `..*` | đi xuống mọi tầng (recursive descent), tối đa **20** lần `..` trong một đường |
| `*`, `[*]` | mọi member của object hoặc mọi phần tử của mảng |
| `[n]`, `[-n]` | chỉ số, âm thì đếm từ cuối |
| `[start:end:step]` | lát cắt; phần nào cũng có thể bỏ; cho phép dấu `+` và khoảng trắng; `start`/`end` âm cộng thêm độ dài rồi kẹp vào mảng; `step` bằng 0 là lỗi `SYNTAXERR Step in the slice cannot be zero` |
| `[a,b,c]`, `['x','y']` | hợp (union) chỉ số hoặc tên, theo thứ tự viết, giữ trùng lặp, bỏ chỉ số ngoài mảng; không trộn lát cắt với chỉ số; không có phần tử rỗng (`[0,,4]`, `[4,]` là lỗi cú pháp) |
| `[?(expr)]` | bộ lọc, xem mục 3 |

Tên không đặt trong ngoặc kết thúc ở một trong các ký tự `. [ ] ( ) < > = ! ' " khoảng-trắng | &`. Tên Unicode viết thẳng được (`.a.愛`). Ba dấu chấm liền nhau trở lên: `SYNTAXERR Invalid dot sequence`.

## 3. Bộ lọc `[?(...)]`

- Toán tử: `==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`, ngoặc tròn. `&&` ưu tiên hơn `||`.
- Toán hạng:
  - `@` (chính phần tử), `@.member`, `@['member']`, `@["member"]`, `@.member[i]` và đường dẫn tương đối dài hơn;
  - hằng: `null`, `true`, `false`, số (cả dạng mũ `1.0E1`, `-1.23e1`), chuỗi trong ngoặc đơn hoặc kép;
  - đường dẫn bắt đầu bằng `$` trỏ tới đúng một giá trị vô hướng.
- `@.member` đứng một mình là phép thử "có member này không" (`$..book[?(@.isbn)]`).
- Hằng bên trái được phép (`1 < @` hiểu là `@ > 1`).
- So sánh:
  - hai vế khác kiểu thì mọi toán tử đều sai, kể cả `!=`; `true` và `false` cùng một kiểu;
  - `null == null` đúng; so thứ tự trên `null`, object, mảng luôn sai;
  - chuỗi so theo byte;
  - số so bằng số thực nếu một vế là số thực, còn lại so số nguyên.
- Bộ lọc áp lên phần tử của mảng và giá trị member của object. Kết quả theo thứ tự trong tài liệu, không theo nhánh `||` nào khớp trước.
- Bộ lọc dùng được trong lệnh ghi: `JSON.SET`/`JSON.DEL` với bộ lọc sửa hoặc xoá mọi chỗ khớp.

## 4. Không hỗ trợ (giống valkey-json)

Các cú pháp sau của RedisJSON hoặc JSONPath đầy đủ **không** có, và báo lỗi cú pháp:

- `~` (lấy tên member), `=~` (regex), `!` (phủ định);
- `in`, `nin`, `size`, `empty`, `subsetof`, `anyof`, `noneof`;
- phép tính số học, biểu thức script `(...)` ngoài bộ lọc;
- mọi hàm: `length()`, `min()`, `max()`, `avg()`...;
- tuỳ chọn `FPHA` của `JSON.SET` (Redis 8.8).

## 5. Giới hạn

| Giới hạn | Giá trị | Vượt thì |
|---|---|---|
| Độ dài đường dẫn | 128 KB | `LIMIT Query string size limit is exceeded` |
| Số `..` trong một đường | 20 | `LIMIT Total number of recursive descent tokens in the query string exceeds the limit` |
| Độ sâu đệ quy khi phân tích đường (bộ lọc lồng, hợp) | 200 | `LIMIT Parser recursion depth is exceeded` |
| Độ sâu lồng của tài liệu | `json.max-path-limit`, mặc định 128 | `LIMIT Document path nesting limit is exceeded` |
| Kích thước tài liệu | `json.max-document-size`, mặc định 0 = không giới hạn | `LIMIT Document size limit is exceeded` |
| Công sức của một lần tìm theo đường (chỉ HCache) | 2^20 lần chạy phần còn lại của đường từ một giá trị, cộng 4 lần mỗi giá trị trong tài liệu; tính cả đường `$` trong bộ lọc | `LIMIT Path search work limit is exceeded` |
| Độ sâu cộng dồn của các lượt `..` lồng nhau (chỉ HCache) | 2048 | `LIMIT Parser recursion depth is exceeded` |
| Câu trả lời của `JSON.GET`, `JSON.MGET` (chỉ HCache) | 512 MiB, như `proto-max-bulk-len` | `LIMIT Reply size limit is exceeded` |
| Phần một lệnh ghi thêm vào tài liệu (chỉ HCache) | 512 MiB, mọi bản sao cộng lại, kể cả khi `json.max-document-size` là 0 | `LIMIT Document size limit is exceeded` |

Cả hai cấu hình đọc và đặt bằng `CONFIG GET/SET`. Trên replica các giới hạn tài liệu không áp (lệnh từ master luôn được nhận), như valkey-json.

Bốn giới hạn "chỉ HCache" có từ lượt rà bảo mật đợt 3:
- **Công sức tìm.** Mỗi `..` chạy phần còn lại của đường từ mọi giá trị bên dưới, nên `..` sau `..` nhân lên. Trên một tài liệu sâu 100 tầng, `$..a..a..a..a..a..a..a..a` cần khoảng 10^11 lượt và giữ một shard hàng giờ; valkey-json cũng vậy. Đường thường (một `..`, `[*]`, bộ lọc) tốn cỡ số giá trị trong tài liệu, không chạm giới hạn này.
- **Câu trả lời.** `INDENT`, `NEWLINE`, `SPACE` lặp lại theo từng giá trị và từng tầng, nên một `INDENT` dài trên tài liệu nhỏ đủ dựng câu trả lời hàng chục GB và làm tiến trình chết vì hết bộ nhớ.
- **Bản sao.** `JSON.SET k '$[*]' <giá trị>` chép giá trị vào mọi chỗ khớp. Trước đây giới hạn kích thước chỉ tính một bản; nay tính đủ số chỗ khớp ở `JSON.SET`, `JSON.MSET`, `JSON.MERGE`, `JSON.STRAPPEND`, `JSON.ARRAPPEND`, `JSON.ARRINSERT`.
- Số lớn hơn số thực lớn nhất (`9.9e309`, hay `1` theo sau 400 số 0) bị từ chối khi đọc JSON, như RapidJSON (`Number too big`). Trước đây nó thành vô cực, rồi `0 * vô cực` thành NaN, được lưu thành `nan` mà tệp dữ liệu, RDB và `RESTORE` không đọc lại được.

## 6. Lỗi nào làm hỏng cả lệnh

- **Legacy**: mọi lỗi đều trả về: không khớp gì (`NONEXISTENT`), sai kiểu (`WRONGTYPE`), chỉ số ngoài mảng (`OUTOFBOUNDARIES Array index is out of bounds`), wildcard trên giá trị vô hướng (`SYNTAXERR Invalid JSON path`).
- **JSONPath**: chỉ lỗi "cú pháp" làm hỏng lệnh: đường sai, tên member sai, số sai, dấu chấm sai, biểu thức rỗng, chỉ số không phải số, `step` 0, thiếu phần tử cha, và các lỗi giới hạn của đường ở mục 5. Lỗi khác chỉ làm chỗ đó không khớp (mảng rỗng hoặc `null`).
- Ngoại lệ, như valkey-json: NUMINCRBY, NUMMULTBY, STRAPPEND, ARRAPPEND, ARRPOP, ARRINSERT, ARRTRIM và CLEAR trả nguyên lỗi khi tìm đường thất bại, kể cả với `$`. Ví dụ ARRINSERT với chỉ số sai dưới `$.[*]` trả `OUTOFBOUNDARIES`.

## 7. Ghi bằng `JSON.SET`

- Đường khớp giá trị có sẵn: thay tất cả.
- Bước **cuối** là tên member chưa có, cha có và là object: thêm member vào cuối object.
- Thiếu member ở bước giữa: `NONEXISTENT JSON path does not exist`. Cha không phải object: `ERROR Cannot insert a member into a non-object value`. Chỉ số ngoài mảng: `OUTOFBOUNDARIES`. Không tạo phần tử mảng bằng chỉ số; dùng ARRINSERT/ARRAPPEND.
- Wildcard, bộ lọc, hợp, lát cắt và `..` chỉ sửa chỗ có sẵn, không bao giờ thêm.
- `NX`: không làm gì (trả `nil`) nếu đường khớp bất kỳ giá trị nào. `XX`: không làm gì nếu đường sẽ thêm member mới.
- Khoá chưa có thì đường phải là gốc: `SYNTAXERR A new Valkey key's path must be root`.
- `$..a` có chỗ khớp lồng nhau: sửa tổ tiên trước, chỗ con đã bị thay thì bỏ qua (`{"a":{"a":1}}` thành `{"a":5}`).
- Lệnh ghi nào cũng kiểm tra hết trước khi đổi; bị từ chối thì tài liệu giữ nguyên.

## 8. Chỗ HCache khác valkey-json

Các điểm dưới đây tìm ra khi đọc mã nguồn valkey-json; chưa đối chiếu với module chạy thật.

| Chỗ | valkey-json | HCache | Vì sao |
|---|---|---|---|
| Độ lồng tối đa | chỉ theo `json.max-path-limit` | thêm trần cứng 1024 tầng; đặt `json.max-path-limit` lớn hơn cũng như 1024 | chặn tràn ngăn xếp khi đọc, ghi, giải phóng tài liệu |
| Thêm nhiều member trong một lệnh (`$['x','y']` khi cả hai chưa có) | thứ tự theo `unordered_set` (thứ tự băm) | theo thứ tự trong đường dẫn | thứ tự xác định |
| Thêm member khi cha đã bị thay trong cùng lệnh | JSON Pointer `Set` tạo lại cả nhánh | bỏ qua chỗ thêm đó | không đẻ ra nhánh mà lệnh không nhắm tới |
| Tên member mới có `/` | thêm qua JSON Pointer ghép từ tên chưa escape, nên `$.x/y` thành hai tầng `x` rồi `y` (đọc từ mã, chưa kiểm) | thêm đúng một member tên `x/y` | không tạo nhánh ngầm |
| Tên member mới có `~` | `~1` thành `/`, `~0` thành `~` (test upstream `$.b~1c` ghi vào `b/c`); `~` khác làm Pointer hỏng | như valkey-json; `~` khác báo `SYNTAXERR Invalid JSON path` trước khi đổi gì | |
| `JSON.SET` vừa sửa đúng một chỗ vừa thêm member | sau `Swap`, biến giá trị mới giữ giá trị **cũ**, nên member thêm vào nhận giá trị cũ | member thêm vào nhận giá trị mới | coi là lỗi của valkey-json |
| NUMINCRBY/NUMMULTBY/ARRINSERT trên nhiều chỗ, một chỗ lỗi (tràn số, chỉ số sai) | các chỗ trước đó đã bị sửa | không chỗ nào bị sửa | lệnh nguyên tử |
| In số thực đã tính (khi GET) | Grisu2 của RapidJSON, hiếm khi dài hơn chữ số ngắn nhất | luôn chữ số ngắn nhất đọc lại đúng giá trị | Rust; kết quả NUMINCRBY/NUMMULTBY vẫn `%.17g` như valkey-json |
| `JSON.DEBUG` | MEMORY, FIELDS, DEPTH, HELP và các lệnh dev (MAX-DEPTH-KEY, KEYTABLE-*...) | chỉ MEMORY, FIELDS, DEPTH, HELP; HELP liệt kê 4 dòng này | lệnh dev quét toàn bộ khoá, không dùng cho máy chạy thật |
| Số byte của `JSON.DEBUG MEMORY`, `MEMORY USAGE` | theo bộ cấp phát | ước lượng từ cấu trúc | không có bộ cấp phát riêng cho từng tài liệu |
| `JSON.RESP` legacy khi đường khớp nhiều chỗ | gửi nhiều câu trả lời liền nhau cho một lệnh, làm lệch giao thức | trả chỗ đầu tiên | giữ đúng một câu trả lời mỗi lệnh |
| Thứ tự khoá khi `JSON.GET` nhiều đường | test của valkey-json ghi là không cố định | theo thứ tự đối số | thứ tự xác định |
| RDB encver 0 và 1 (bản rất cũ) | nạp được | không nạp | chỉ ghi và đọc encver 3 |
| Đặt `json.max-document-size` khác 0 | cỡ tài liệu theo dõi sẵn qua bộ cấp phát, O(1) | mỗi lệnh ghi tính lại cỡ cả tài liệu, O(n) | chưa giữ cỡ cộng dồn |
| Lệnh ghi không đổi gì (ví dụ `JSON.DEL k $.x` khi không có `x`) | vẫn replicate và phát `json.del` | như valkey-json, và WATCH cũng coi khoá đã đổi | |
| File snapshot riêng của HCache (`persist`) | không có | lưu văn bản JSON gọn | RDB vẫn đúng định dạng valkey-json |

Thêm: số nguyên giữ dạng `i64`; số thực giữ nguyên văn bản như khi ghi (giống valkey-json); object giữ thứ tự thêm, khoá trùng khi đọc giữ vị trí đầu và giá trị sau.

## 9. Khác RedisJSON (giữ theo valkey-json)

- Đường mặc định là `.` (legacy), RedisJSON là `$`.
- Lỗi khoá mới ở đường không phải gốc, đường thiếu member giữa, chỉ số ngoài mảng: chuỗi lỗi của valkey-json (mục 7), không phải `ERR ...` của RedisJSON.
- `ARRTRIM`: `start` âm thành 0, `stop` âm không đổi, đủ 4 tham số.
- `JSON.DEL $..a` lồng nhau xoá mọi chỗ khớp (valkey-json đếm 4, RedisJSON 1).
- Bộ lọc và hàm của RedisJSON ở mục 4 không có.
