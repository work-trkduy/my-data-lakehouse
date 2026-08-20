# docker-compose.yml — Orchestration toàn stack

## Vai trò
File duy nhất dựng & khởi động 13 service: postgres, kafka(kraft), debezium, debezium-register, minio, minio-init, polaris-db, polaris-bootstrap, polaris, polaris-setup, flink-jobmanager, flink-taskmanager, trino. Mỗi service gắn riêng thư mục build + bind-mount config/data vào project (Không dùng named volumes).

## Cấu trúc services & `depends_on` (thứ tự khởi động)
| Service | Build (Dockerfile) / Image | depends_on | Ghi chú |
|---------|--------------------------|------------|---------|
| postgres | `./postgres` | — | init SQL, wal_level=logical |
| kafka | `apache/kafka:4.3.1` | — | KRaft |
| debezium | `./debezium` | kafka(healthy), postgres(healthy) | Connect |
| debezium-register | `curlimages/curl` | debezium(started) | POST connector |
| minio | `./minio` | — | S3 server |
| minio-init | `minio/mc:***` | minio(healthy) | tạo bucket |
| polaris-db | `postgres:16` | — | Postgres lưu catalog metadata (relational-jdbc) |
| polaris-bootstrap | `apache/polaris-admin-tool:1.7.0` | polaris-db(healthy) | bootstrap schema Postgres (one-shot, idempotent) |
| polaris | `./polaris` | polaris-bootstrap(completed), minio(healthy) | REST catalog, persistence relational-jdbc → polaris-db |
| polaris-setup | `./polaris` | polaris(started) | setup catalog + grant |
| flink-jobmanager | `./flink` | kafka(healthy), minio(healthy), polaris(started) | nộp job sql |
| flink-taskmanager | `./flink` | flink-jobmanager(started) | slots |
| trino | `./trino` | minio(healthy), polaris(started) | query |

## Bind-mounts (không named volumes) — KHAI báo theo quy tắc
```
./postgres/data:/var/lib/postgresql/data      # data PG
./postgres/init:/docker-entrypoint-initdb.d:ro # init SQL
./kafka/data:/mnt/kafka-logs                   # KRaft logs
./minio/data:/data                             # object storage
./minio/init.sh:/init.sh:ro                    # mc init script
./polaris-db/data:/var/lib/postgresql/data     # data PG của Polaris catalog metadata
./polaris:/scripts:ro                          # polaris setup.sh
./flink/sql:/opt/flink-sql:ro                  # SQL job + run-job.sh
./flink/checkpoints:/opt/flink/checkpoints     # Flink checkpoint state
./flink/savepoints:/opt/flink/savepoints       # Flink savepoint state (resume job)
./trino/catalog:/etc/trino/catalog:ro          # Iceberg catalog props
```
> Luật: **mọi state/config đều bind-mount từ project**; không khai block `volumes:` top-level. `docker compose down` (không `-v`) giữ data; reset dữ liệu thật bằng `rm -rf postgres/data kafka/data minio/data polaris-db/data flink/checkpoints flink/savepoints`.

## Các biến môi trường quan trọng (`/mnt/d/Projects/my_data_lakehouse/.env`)
- `POSTGRES_*`, `KAFKA_BOOTSTRAP`
- `MINIO_ROOT_USER/PASSWORD/BUCKET/REGION`, `MINIO_ENDPOINT=http://minio:9000`, **`MINIO_EXTERNAL_ENDPOINT=http://minio:9000`** (không localhost)
- `POLARIS_REALM/POLARIS_ROOT_USER/POLARIS_ROOT_PASSWORD/POLARIS_CATALOG`
- `POLARIS_DB_NAME/POLARIS_DB_USER/POLARIS_DB_PASSWORD` (Postgres persistence của Polaris)

> Đổi `.env` cần `docker compose up -d` để service đọc lại; với config baked trong image (flink jars) phải `docker compose build`.

## Lỗi có thể xảy ra / đã gặp
- **Trước đây Polaris `MINIO_EXTERNAL_ENDPOINT` để `http://localhost:9000`** → Flink/Trino không kết nối được MinIO (connection refused tới localhost). Fix: đặt `http://minio:9000` + recreate polaris + chạy lại setup.
- **Polaris server chết lúc boot nếu thiếu schema Postgres** → schema **không tự tạo**; phải chạy `polaris-bootstrap` (admin-tool `bootstrap`, idempotent) trước khi `polaris` bật (`depends_on: service_completed_successfully`). Chạy lại thủ công: `docker compose run --rm polaris-bootstrap`.
- **Catalog metadata giờ persist trong `polaris-db` (Postgres)** — hết ephemeral. Reset sạch catalog: `rm -rf polaris-db/data` rồi `docker compose up -d polaris-db polaris-bootstrap polaris polaris-setup`.
- **`debezium-register` bật quá sớm** → sidecar có loop wait (~120s); nếu timeout tăng số lần lặp trong `register-debezium.sh`.
- **Flink nộp job khi chưa có catalog/schema** → đã thêm `depends_on` polaris(minio healthy); nếu quá sớm vẫn có thể fail auth — chạy lại `run-job.sh`.
- **Flink job append-only không idempotent** → checkpoint/savepoint bind-mount vào `./flink/checkpoints` + `./flink/savepoints`; `run-job.sh` tự resume từ savepoint mới nhất nếu có.

## Command hữu ích
```bash
docker compose up -d                                      # dựng toàn stack
docker compose build flink debezium                       # rebuild hình thay đổi jars
docker compose ps                                         # tất cả healthy
docker compose exec flink-jobmanager bash /opt/flink-sql/run-job.sh   # (re)submit streaming job
docker compose down                                       # giữ data
docker compose rm -f && rm -rf postgres/data kafka/data minio/data polaris-db/data flink/checkpoints flink/savepoints   # reset full
```