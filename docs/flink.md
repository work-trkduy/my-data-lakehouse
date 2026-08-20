# Flink — Stream processing (CDC → Iceberg)

## Vai trò
Flink SQL chạy job streaming đọc topic CDC từ Kafka, **append mỗi change event** (insert/update/delete) vào bảng Iceberg dạng **append-only event log** dưới catalog Polaris — mỗi dòng là một event giữ nguyên `op` + metadata nguồn. Job được nộp 1 lần (thông qua `sql-client`).

## Version sử dụng
- Image gốc: `flink:2.1.2-scala_2.12`
- Build của repo: `lakehouse/flink:2.1`
- Iceberg runtime: `iceberg-flink-runtime-2.1-1.11.0.jar`
- AWS bundle (S3FileIO): `iceberg-aws-bundle-1.11.0.jar`
- Kafka SQL connector: `flink-sql-connector-kafka-5.0.0-2.1.jar`
- Hadoop client: `hadoop-client-api-3.3.6.jar` + `hadoop-client-runtime-3.3.6.jar`

**Lý do chọn Flink 2.1:** user chọn thử nhánh mới; Iceberg hỗ trợ Flink 2.1 qua runtime artifact `-2.1` (đã xác nhận trên Maven Central). Connector SQL cho Flink 2.x không còn hậu tố `-flink-version` (chuyển sang version bản nhánh, ở đây Kafka dùng `5.0.0` + hậu tố `-2.1` cho catalogue Flink 2.1). Các jar này được **bake thẳng vào image** (xem `flink/Dockerfile`), không cần copy lúc chạy.

## Deploy / chạy job
```bash
docker compose up -d flink-jobmanager flink-taskmanager
# nộp job streaming (chạy từ host)
docker compose exec flink-jobmanager bash /opt/flink-sql/run-job.sh
# UI
open http://localhost:8081
```

### `run-job.sh` — STARTUP_MODE
- `STARTUP_MODE=auto` (mặc định): resume từ savepoint mới nhất trong `/opt/flink/savepoints`,
  không có savepoint thì cold start từ `earliest-offset`.
- `STARTUP_MODE=latest`: bắt đầu từ **đầu hiện tại của topic** (chỉ event mới). **Bắt buộc dùng
  sau một reconcile** — sink append-only không idempotent, cold start `earliest` sẽ replay toàn
  bộ topic (trùng dòng đã ingest + reconcile). Xem [backfill.md](backfill.md).
  ```bash
  docker compose exec -T -e STARTUP_MODE=latest flink-jobmanager bash /opt/flink-sql/run-job.sh
  ```

## Job một-lần: snapshot → staging (backfill diff)
`flink/sql/snapshot-to-staging.sql` là job **BATCH một-lần** đọc cửa sổ Kafka
`[SNAP_TS, SNAP_END_TS]`, lọc `op='r'`, tính `__hash__` (HASH CONTRACT, giống hệt init.sql) rồi
ghi vào bảng staging `transactions_snapshot`. Được chạy bởi `scripts/snapshot-to-staging.sh`
(backfill) hoặc tự động qua `scripts/watchdog.sh`. Điểm mấu chốt:
- Kafka source phải **bounded** (`scan.bounded.mode=timestamp` + `scan.bounded.timestamp-millis`)
  nếu không báo `Querying an unbounded table ... in batch mode is not allowed`.
- `SET 'sql-client.execution.result-mode'='TABLEAU'` để chạy không TTY.
- Cùng gotcha catalog: source ở default catalog, qualify đầy đủ.

## Cấu hình
### compose `flink-jobmanager:` / `flink-taskmanager:`
```yaml
environment:
  FLINK_JOBMANAGER_HOSTNAME: flink-jobmanager
  FLINK_PROPERTIES: |
    jobmanager.rpc.address: flink-jobmanager
    taskmanager.numberOfTaskSlots: 4
    parallelism.default: 2
volumes:
  - ./flink/sql:/opt/flink-sql:ro
command: jobmanager   # (hoặc taskmanager)
```

### `flink/sql/init.sql` (job SQL)
Đầy đủ chi tiết trong file, các điểm mấu chốt:
- 3 bước setup: `RESET;` → `SET STREAMING`, `parallelism=1`, checkpoint 10s, `EXACTLY_ONCE`.
- `CREATE CATALOG iceberg_catalog` → `RESTCatalog` `uri=http://polaris:8181/api/catalog`, `warehouse=quickstart_catalog`, `io-impl=S3FileIO`, `s3.*` trỏ `http://minio:9000`, `credential=root:s3cr3t`, header `Polaris-Realm=POLARIS`.
- Kafka source table `cdc_transactions_source`: `connector=kafka`, `topic=postgres.public.transactions`, `scan.startup.mode=earliest-offset`, **`format=json`** (raw envelope) — khai báo `before`/`after`/`source`/`op`/`ts_ms` thành **nested ROW** (backtick-quote từ khoá `before`/`after`/`source`/`db`/`schema`/`table`) để lấy `op`, `source.lsn`, `source.ts_ms`, `source.db|schema|table`; `amount` STRING (decimal string mode); `created_at`/`updated_at` `TIMESTAMP_LTZ(6)` parse ISO-8601.
- Iceberg sink `transactions_cdc_log`: **append-only** — `format-version=2`, **KHÔNG `PRIMARY KEY`**, **KHÔNG `write.upsert.enabled`**; 7 cột dữ liệu (trong đó `amount DECIMAL(12,2)`, cast từ STRING source) + **8 cột hệ thống** (`__op__`, `__source_ts__`, `__lsn__`, `__db__`, `__schema__`, `__table__`, `__hash__`, `__ingest_ts__`). `__hash__` là MD5 full-record (HASH CONTRACT — chỉ tính ở Flink, xem [backfill.md](backfill.md)) để reconcile so sánh diff mà **không hash ở Trino**.
- INSERT (job): đọc source từ **`default_catalog.default_database.cdc_transactions_source`** → ghi sink; `COALESCE(after.*, before.*)` giữ giá trị cũ cho DELETE; `amount` cast `AS DECIMAL(12,2)` (exact); `__source_ts__` = `TO_TIMESTAMP_LTZ(source.ts_ms, 3)` (deterministic, không dùng `CURRENT_TIMESTAMP`); `__ingest_ts__` = `TO_TIMESTAMP_LTZ(ts_ms, 3)` (envelope top-level `ts_ms`). Lưu ý: đối số thứ 2 `3` đã bảo Flink coi số là **millisecond** — KHÔNG viết `/ 1000` trước khi truyền vào.

### Giá trị có thể chỉnh
| Tham số | Chỉnh khi nào |
|---------|----------------|
| `scan.startup.mode` | `earliest-offset` = đọc từ đầu tất cả tin trong topic (đúng cho khởi tạo đầy đủ); `latest-offset` chỉ theo dõi tin mới |
| checkpoint interval | giảm (vd 5s) để dữ liệu hiện nhanh hơn, tăng nếu muốn giảm overhead |
| `parallelism` | tăng để xử lý nhiều luồng, cần đủ task slots |
| `transactions_cdc_log` | bảng log **append-only** — không PK, không upsert; mỗi change event là 1 dòng mới. **Không idempotent khi replay** → checkpoint phải bền (xem Lưu ý) |
| `json.timestamp-format.standard` | `ISO-8601` để parse `created_at`/`updated_at` dạng Debezium `"2024-05-01T12:34:56.123456Z"` |
| Kafka `group.id` | đổi group thì Flink đọc lại from đầu (nếu startup earliest) |

## Lưu ý khi config — QUAN TRỌNG
- **Kafka source phải ở catalog mặc định (in-memory), KHÔNG được ở `iceberg_catalog`.** Nếu tạo source sau `USE CATALOG iceberg_catalog`, Flink coi nó là bảng Iceberg rỗng → source enumerator báo **"No more splits" → job FINISH ngay** (thay vì RUNNING). → Source để trong default catalog, và trong `INSERT` phải **qualify đầy đủ** `default_catalog.default_database.cdc_transactions_source`.
- Dùng format **`json`** (raw envelope), KHÔNG phải `debezium-json`. Lý do: `debezium-json` không expose `op`/`source.lsn`/`source.ts_ms`/`source.db|schema|table` thành cột được. Với `json`, khai báo full envelope thành nested ROW và đọc các cột này trực tiếp. Vẫn **không dùng SMT unwrap** ở connector — nó xoá envelope (xem [debezium.md](debezium.md)).
- **Append-only KHÔNG idempotent.** Nếu job restore từ checkpoint cũ hoặc nộp lại từ đầu (`scan.startup.mode=earliest-offset`), các dòng bị ghi **lặp lại** (không có PK/upsert để ghi đè). → Bắt buộc checkpoint **persistent**: cấu hình `state.checkpoints.dir` trỏ nơi lưu bền (volume/object store) + giữ Kafka offsets; không để checkpoint dir mất giữa restart. `EXACTLY_ONCE` chỉ đảm bảo không ghi nửa chừng, không tự chống trùng khi replay.
- `scan.startup.mode` trong Flink 2.1 dùng giá trị enum `earliest-offset` (KHÔNG phải `earliest`); `earliest` gây `IllegalArgumentException`.

## Version mới / liên quan tech khác — rủi ro
- **Flink 2.1 là major version mới.** Nếu build image mới mà job fail vì thiếu class/SQL, khả năng do connector/runtime version mismatch → kiểm tra `iceberg-flink-runtime` & `flink-sql-connector-kafka` tương thích Flink 2.1.
- Phụ thuộc Polaris + MinIO phải health trước khi Flink khởi động (đã đặt `depends_on`).

## Lỗi có thể xảy ra / đã gặp
- **Job FINISHED ngay / "No more splits available for subtask 0"** → source đặt trong iceberg catalog (bug trên). Fix: đưa source về default catalog + qualify trong INSERT như trên, rồi nộp lại job.
- **`scan.startup.mode ... Expected one of [earliest-offset, latest-offset,...]`** → đang để `earliest`.
- **Các cột `before`/`after`/`op` toàn null / số dòng gấp bội** → SMT unwrap cũ ở connector hoặc dữ liệu cũ trong topic. Fix: xoá transforms, xoá topic Kafka `postgres.public.transactions`, rebuild + re-register Debezium, re-insert (hoặc `snapshot.mode=initial` để resync).
- **`amount` parse sai / decimal bị lệch** → nhớ `decimal.handling.mode=string` ở connector và `amount` STRING ở DDL; nếu connector vẫn `double` thì `amount` là JSON number → khai STRING sẽ lỗi.
- **`created_at`/`updated_at` null hoặc lỗi parse** → `json.timestamp-format.standard=ISO-8601` (chưa chắc áp dụng cho nested ROW field ở mọi phiên bản — nếu null, fallback khai `created_at`/`updated_at` STRING rồi `TO_TIMESTAMP_LTZ(CAST(... AS TIMESTAMP), 6)` ở SELECT; xem note runtime trong task).
- **`__source_ts__`/`__ingest_ts__` hiển thị `1970-01-21 ...`** → lỗi chia đôi `TO_TIMESTAMP_LTZ(ts_ms / 1000, 3)`. Đối số `3` đã bảo Flink coi `ts_ms` là millisecond (tự chia 1000), nên `/ 1000` thêm một lần nữa khiến giá trị bị chia 1 000 000 → ra ~20 ngày sau epoch. Fix: `TO_TIMESTAMP_LTZ(ts_ms, 3)` (bỏ `/ 1000`). Dữ liệu cũ đã ghi sai ts phải re-ingest mới đúng (ts không được lưu raw để suy ngược lại).
- **`Connect to localhost:9000 Connection refused`** khi job chạy → Polaris trả endpoint `localhost` (xem [polaris.md](polaris.md)); phải đổi `MINIO_EXTERNAL_ENDPOINT=http://minio:9000` + recreate catalog.

## Kiểm tra
```bash
# trạng thái job (REST path /v1/, KHÔNG phải /api/v1)
curl -s http://localhost:8081/v1/jobs/overview
# logs JM
docker logs --tail 40 lakehouse-flink-jm
```