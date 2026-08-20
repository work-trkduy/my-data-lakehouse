# Iceberg — Table format (lakehouse)

## Vai trò
Iceberg là định dạng bảng được Flink ghi vào (**append-only event log**) và Trino đọc. Không phải service riêng — nó là **thư viện + data layout** trên MinIO (`s3://warehouse`), với metadata managed bởi Polaris REST catalog. Dữ liệu & metadata nằm trong `./minio/data/warehouse/polaris/transactions_cdc_log/` (data files `.parquet` + `.metadata.json`).

## Version sử dụng (jars / khả năng tương thích)
- Iceberg runtime cho Flink: `iceberg-flink-runtime-2.1-1.11.0.jar` (Iceberg 1.11.0, compile cho Flink 2.1).
- AWS/Hadoop/S3 lớp: `iceberg-aws-bundle-1.11.0.jar`, `hadoop-client-api-3.3.6.jar`, `hadoop-client-runtime-3.3.6.jar` — được bake vào `flink/Dockerfile`.
- Trino connector Iceberg: có sẵn trong `trinodb/trino:483` (connect qua REST catalog).

**Lưu ý phiên bản:** Iceberg dùng bộ runtime riêng cho từng branch Flink (`-2.1`). Khi nâng Flink lên 2.x khác phải đổi sang runtime tương ứng; giữ version Iceberg đồng nhất (1.11.0) giữa Flink runtime và AWS bundle.

## Mapping trong stack
| Bảng / khái niệm | Nơi thể hiện |
|--------------------|---------------|
| Catalog | `quickstart_catalog` (trong Polaris) |
| Namespace/Schema | `polaris` |
| Table | `transactions_cdc_log` |
| Data files | `s3://warehouse/polaris/transactions_cdc_log/data/...parquet` (MinIO) |
| Metadata | `s3://warehouse/polaris/transactions_cdc_log/metadata/*.json + *.avro` |

## Cấu hình trong `flink/sql/init.sql` (sink table)
```sql
USE CATALOG iceberg_catalog;
CREATE TABLE IF NOT EXISTS transactions_cdc_log (
  id BIGINT, user_id BIGINT, amount DECIMAL(12,2), currency STRING, status STRING,
  created_at TIMESTAMP_LTZ(6), updated_at TIMESTAMP_LTZ(6),
  __op__ STRING, __source_ts__ TIMESTAMP_LTZ(3), __lsn__ BIGINT,
  __db__ STRING, __schema__ STRING, __table__ STRING, __ingest_ts__ TIMESTAMP_LTZ(3)
  -- KHÔNG PRIMARY KEY, KHÔNG write.upsert.enabled
) WITH (
  'format-version' = '2',                          -- v2 vẫn dùng cho IDs/deletes
  'write.metadata.delete-after-commit.enabled' = 'true'
);
```
Job: `INSERT INTO transactions_cdc_log (...) SELECT COALESCE(after.*, before.*), op, ... FROM default_catalog.default_database.cdc_transactions_source`.

**Bảng staging cho backfill diff** `transactions_snapshot` (tạo trong `flink/sql/snapshot-to-staging.sql`,
cũng `format-version=2`): cột dữ liệu + `__hash__` + `__source_ts__/__db__/__schema__/__table__`.
Mỗi lần backfill bị **DROP rồi tạo lại** (chỉ chứa snapshot mới nhất); `scripts/reconcile.sh` so
sánh `__hash__` của nó với `transactions_current` để append **chỉ diff** vào log (xem
[backfill.md](backfill.md)).

## Khả năng chỉnh / lưu ý
- **Append-only, không upsert:** mỗi change event (c/u/d/r/t) là **1 dòng mới**; `op` và các cột `__*` cho biết loại event + nguồn. Không có `PRIMARY KEY`, không `write.upsert.enabled`.
- **`amount` DECIMAL(12,2):** connector Debezium để `decimal.handling.mode=string` → source `amount` là STRING `"1234.56"` (exact, không lossy như `double`); INSERT `CAST(... AS DECIMAL(12,2))` để sink giữ đúng kiểu PG ban đầu.
- **`__source_ts__` deterministic:** lấy từ `source.ts_ms` (`TO_TIMESTAMP_LTZ(ts_ms, 3)`), không phải `CURRENT_TIMESTAMP` — để replay cho cùng kết quả. (`__ingest_ts__` = `TO_TIMESTAMP_LTZ(ts_ms, 3)` với `ts_ms` top-level.)
- **Không idempotent:** log append ghi lặp nếu job replay (checkpoint cũ / nộp lại từ `earliest-offset`). → checkpoint phải **persistent** (`state.checkpoints.dir` bền) + giữ Kafka offsets.
- `write.metadata.delete-after-commit.enabled=true`: dọn metadata phiền toái sau commit (gọn object, không mất snapshot chính). Muốn giữ mọi lịch sử metadata thì tắt.
- Chưa partition; khi volume tăng, `PARTITIONED BY (days(__source_ts__))` + retention/expiry (TODO trong `init.sql`).

## Lỗi có thể xảy ra / đã gặp
- **Job coi bảng Iceberg là nguồn rỗng → FINISH ngay** — xảy ra khi tạo Kafka source trong catalog Iceberg; chữa bằng cách để source ở default catalog (chi tiết [flink.md](flink.md)).
- **Số dòng gấp bội sau restart** → replay append-only (không upsert nên không ghi đè). Fix: checkpoint persistent + đừng nộp lại job từ đầu trừ khi chấp nhận trùng; hoặc dùng bảng upsert riêng nếu cần trạng thái mới nhất.
- **`__source_ts__`/`__ingest_ts__` rơi về `1970-01-21`** → dùng `TO_TIMESTAMP_LTZ(ts_ms / 1000, 3)` (chia đôi). `3` đã ngụ ý millisecond → Flink tự `/1000`; thêm `/ 1000` nữa là chia 1e6. Fix: `TO_TIMESTAMP_LTZ(ts_ms, 3)` (xem [flink.md](flink.md)). Dòng cũ ghi sai ts phải re-ingest.
- **Cannot find `S3FileIO` / ClassNotFound** → thiếu `iceberg-aws-bundle-1.11.0.jar` trong `/opt/flink/lib`.
- **Không đọc được data sau khi đổi MinIO credential/bucket** → đồng bộ theme giữa catalog (Polaris), Flink và Trino (xem `minio.md` chỉnh chung).

## Kiểm tra
```bash
# qua Trino (nơi duy nhất thấy data Iceberg)
curl -s -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: admin" -H "X-Trino-Catalog: iceberg" \
  -d "SELECT count(*), count(*) FILTER (WHERE __op__='d') FROM polaris.transactions_cdc_log"
# trực tiếp trong MinIO (data/metadata xuất hiện sau checkpoint)
ls minio/data/warehouse/polaris/transactions_cdc_log/data | head
```