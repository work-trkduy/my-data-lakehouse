# Docs — Tài liệu per-tech

Tài liệu 1 file cho mỗi tech trong stack CDC lakehouse (Postgres → Debezium → Kafka → Flink → Iceberg → MinIO → Polaris → Trino). Mỗi file gồm: vai trò, version, cách deploy, cấu hình, lý do chọn, giá trị chỉnh được, lỗi đã gặp, lỗi liên quan tech khác, rủi ro version.

## Nội dung (8 tech + orchestration)
| File | Tech | Vai trò |
|------|------|---------|
| [postgres.md](postgres.md) | PostgreSQL 16 | Nguồn dữ liệu CDC (WAL logical) |
| [kafka.md](kafka.md) | Kafka 4.3.1 KRaft | Bus CDC (topic `postgres.public.transactions`) |
| [debezium.md](debezium.md) | Debezium 3.0.0 / Kafka Connect | Capture CDC PG → Kafka |
| [flink.md](flink.md) | Flink 2.1.2 | Stream processing CDC → Iceberg |
| [iceberg.md](iceberg.md) | Iceberg 1.11.0 | Bảng lakehouse (data trên MinIO, metadata Polaris) |
| [minio.md](minio.md) | MinIO | Object storage S3 (data + metadata Iceberg) |
| [polaris.md](polaris.md) | Polaris 1.7.0 | REST catalog (namespace/table) |
| [trino.md](trino.md) | Trino 483 | Query engine (DBeaver xem lakehouse) |
| [docker-compose.md](docker-compose.md) | Compose | Orchestration, bind-mounts, env |
| [monitoring.md](monitoring.md) | Prometheus + Grafana | Giám sát stream: scrape Flink/Kafka/Polaris/Trino, dashboard + alert, nối watchdog |

## Tài liệu tổng quan kiến trúc
| File | Ngôn ngữ | Nội dung |
|------|----------|----------|
| [architecture.md](architecture.md) | English | Mô tả kiến trúc (Mermaid): Polaris catalog trên Postgres, Iceberg append-only CDC log, mô hình dữ liệu, bảng component, lộ trình production |
| [architecture.vi.md](architecture.vi.md) | Tiếng Việt | Bản dịch của `architecture.md` (sơ đồ Mermaid + code giữ nguyên) |

## Đọc nhanh các "bẫy" đã giải
- **Flink source phải ở default catalog** & qualify `default_catalog.default_database.cdc_transactions_source` — nếu không job FINISH ngay. ([flink.md](flink.md), [iceberg.md](iceberg.md))
- **Debezium không dùng SMT unwrap** — Flink `json` đọc full envelope (`before`/`after`/`source`/`op`/`ts_ms`) để lấy `op`/`lsn`. ([debezium.md](debezium.md), [flink.md](flink.md))
- **Sink append-only event log** (`transactions_cdc_log`), không PK/upsert; append **không idempotent** → cần checkpoint persistent. ([flink.md](flink.md), [iceberg.md](iceberg.md))
- **`decimal.handling.mode=string`** → `amount` STRING, giữ precision chính xác (không `double` lossy). ([debezium.md](debezium.md))
- **`TO_TIMESTAMP_LTZ(ts_ms / 1000, 3)` sai** → chia đôi (`3` đã ngụ ý ms, thêm `/1000` nữa → `1970-01-21`). Dùng `TO_TIMESTAMP_LTZ(ts_ms, 3)`. ([flink.md](flink.md), [iceberg.md](iceberg.md))
- **Trino dùng `s3.*` native + `fs.s3.enabled=true`**, `vended-credentials-enabled=false` + static S3 keys. ([trino.md](trino.md))
- **Polaris `MINIO_EXTERNAL_ENDPOINT` phải `http://minio:9000`**; thiếu grant `CATALOG_MANAGE_CONTENT`. ([polaris.md](polaris.md))
- **Polaris chặn `DROP TABLE`** (`Unable to purge entity`) → bật server-side `polaris.features."DROP_WITH_PURGE_ENABLED"=true`. ([polaris.md](polaris.md))
- **Kafka Connect 3 topic internal cần `cleanup.policy=compact`**. ([kafka.md](kafka.md))
- **`scan.startup.mode=earliest-offset`** (không `earliest`). ([flink.md](flink.md))
- **Restart toàn stack / rebuild frames** qua `docker-compose.md` → [docker-compose.md](docker-compose.md).
- **Backfill khi CDC bị rớt** — **diff-based**: incremental snapshot → staging (`scripts/snapshot-to-staging.sh`, hash `__hash__` tính ở Flink) → reconcile (`scripts/reconcile.sh`) so sánh hash, append chỉ record khác/xóa; `scripts/watchdog.sh` tự phát hiện stream rớt/checkpoint fail rồi chạy recovery + restart stream từ `latest`. ([backfill.md](backfill.md))