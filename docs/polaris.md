# Polaris — Iceberg REST Catalog (namespace/table metadata)

## Vai trò
REST catalog quản lý metadata Iceberg: catalog `quickstart_catalog`, namespace `polaris`, bảng `transactions_cdc_log` (append-only CDC event log), bảng staging `transactions_snapshot` (chứa snapshot mới nhất cho backfill diff) + view `transactions_current` (latest-state, expose `__hash__`). Trino và Flink gọi Polaris qua HTTP để resolve schema; dữ liệu thật (data files) nằm trên MinIO. Chi tiết backfill: [backfill.md](backfill.md).

## Version sử dụng
- Image: `apache/polaris:1.7.0` (build của repo `lakehouse/polaris:1.7.0`). Pin stable, không dùng `:latest`.

## Deploy
```bash
docker compose up -d polaris polaris-setup
```
`polaris-bootstrap` (image `apache/polaris-admin-tool:1.7.0`) tạo schema Postgres trong `polaris-db` trước khi server chạy (one-shot). `polaris-setup` (cùng image `polaris`) chạy `polaris/setup.sh` để tạo catalog + grant. Health: port `8182` (`/q/health`), REST `8181`.

Catalog metadata lưu trong Postgres (`polaris-db`) qua backend **relational-jdbc** → bền vững khi recreate container.

## Cấu hình
### compose `polaris:`
```yaml
environment:
  POLARIS_BOOTSTRAP_CREDENTIALS: "${POLARIS_REALM},${POLARIS_ROOT_USER},${POLARIS_ROOT_PASSWORD}"  # POLARIS,root,s3cr3t
  "polaris.realm-context.realms": "${POLARIS_REALM}"
  POLARIS_SERVER_PORT: "8181"
  # Cho phép DROP TABLE ... PURGE (mặc định Polaris cấm, xem "Lỗi có thể xảy ra")
  'polaris.features."DROP_WITH_PURGE_ENABLED"': "true"
  # Persistence: relational-jdbc → Postgres (không còn in-memory/ephemeral)
  POLARIS_PERSISTENCE_TYPE: relational-jdbc
  POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATABASE_TYPE: postgresql
  QUARKUS_DATASOURCE_JDBC_URL: jdbc:postgresql://polaris-db:5432/${POLARIS_DB_NAME}
  QUARKUS_DATASOURCE_USERNAME: ${POLARIS_DB_USER}
  QUARKUS_DATASOURCE_PASSWORD: ${POLARIS_DB_PASSWORD}
  AWS_REGION: ${MINIO_REGION}
  AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER}
  AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD}
ports: ["8181:8181", "8182:8182"]
```
### compose `polaris-db:` (Postgres lưu catalog metadata)
```yaml
image: postgres:16
environment:
  POSTGRES_USER: ${POLARIS_DB_USER}
  POSTGRES_PASSWORD: ${POLARIS_DB_PASSWORD}
  POSTGRES_DB: ${POLARIS_DB_NAME}
volumes:
  - ./polaris-db/data:/var/lib/postgresql/data
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U ${POLARIS_DB_USER} -d ${POLARIS_DB_NAME}"]
```
### compose `polaris-bootstrap:` (one-shot, chạy TRƯỚC khi `polaris` server bật)
```yaml
image: apache/polaris-admin-tool:1.7.0
depends_on:
  polaris-db:
    condition: service_healthy
command:
  - "bootstrap"
  - "--realm=${POLARIS_REALM}"
  - "--credential=${POLARIS_REALM},${POLARIS_ROOT_USER},${POLARIS_ROOT_PASSWORD}"
environment:
  POLARIS_PERSISTENCE_TYPE: relational-jdbc
  POLARIS_PERSISTENCE_RELATIONAL_JDBC_DATABASE_TYPE: postgresql
  QUARKUS_DATASOURCE_JDBC_URL: jdbc:postgresql://polaris-db:5432/${POLARIS_DB_NAME}
  QUARKUS_DATASOURCE_USERNAME: ${POLARIS_DB_USER}
  QUARKUS_DATASOURCE_PASSWORD: ${POLARIS_DB_PASSWORD}
```
> Schema Postgres **không tự tạo** — phải chạy lệnh `bootstrap` (idempotent) với đúng 3 biến `QUARKUS_DATASOURCE_*` trước khi server khởi động.
### compose `polaris-setup:`
```yaml
entrypoint: ["/bin/bash", "/scripts/setup.sh"]
environment:
  POLARIS_CATALOG: ${POLARIS_CATALOG}         # quickstart_catalog
  MINIO_ENDPOINT: ${MINIO_ENDPOINT}           # endpointInternal
  MINIO_EXTERNAL_ENDPOINT: ${MINIO_EXTERNAL_ENDPOINT}
  ...
```
### `polaris/setup.sh` — tạo catalog
Tạo catalog `quickstart_catalog` trỏ `s3://warehouse`, storage `endpointInternal=${MINIO_INTERNAL}` (nội bộ), `endpoint=${MINIO_EXTERNAL}` (vended cho client), `pathStyleAccess=true`, `region=${S3_REGION}`.

### `.env`
```
POLARIS_REALM=POLARIS
POLARIS_ROOT_USER=root
POLARIS_ROOT_PASSWORD=s3cr3t
POLARIS_CATALOG=quickstart_catalog
# Polaris metadata persistence (Postgres)
POLARIS_DB_NAME=polaris
POLARIS_DB_USER=polaris
POLARIS_DB_PASSWORD=polaris
```

### Giá trị có thể chỉnh
| Biến | Chỉnh khi nào |
|------|----------------|
| `MINIO_EXTERNAL_ENDPOINT` | **PHẢI là `http://minio:9000`**, không `localhost`. Là endpoint Polaris vends cho client đọc/write; nếu sai → Flink/Trino connection refused tới localhost | 
| `POLARIS_CATALOG` | đổi tên catalog → đồng bộ vào `flink/init.sql` (`warehouse=`), `trino/catalog/iceberg.properties` (`iceberg.rest-catalog.warehouse`) |
| realm / root credential | đổi thì sửa đồng bộ trong `.env` + `flink/init.sql` `credential=root:...` + `trino` `CLIENT_ID/CLIENT_SECRET` | | `AWS_*` env | credential dùng cho metadata write ở phía catalog |

## Lỗi có thể xảy ra / đã gặp
- **Flink/Trino `Connect to localhost:9000 Connection refused`** → Polaris vends endpoint `localhost:9000` (setup đang ghi thẳng). Fix: đổi `.env` `MINIO_EXTERNAL_ENDPOINT=http://minio:9000`, rồi **recreate container** + chạy lại setup:
  ```bash
  docker compose up -d polaris-setup   # chạy lại setup; nếu catalog cũ vẫn trỏ endpoint cũ → xoá catalog rồi tạo lại (metadata giờ lưu trong polaris-db)
  ```
- **Catalog store giờ bền vững (relational-jdbc → Postgres).** Không còn mất metadata khi recreate `polaris`. Muốn reset sạch thật sự: `rm -rf polaris-db/data` (hoặc drop schema) rồi `docker compose up -d polaris-db polaris-bootstrap polaris polaris-setup`.
- **Schema Postgres không tự tạo / server chết lúc boot** → `polaris-bootstrap` chưa chạy hoặc thiếu 3 biến `QUARKUS_DATASOURCE_*`. Bootstrap idempotent, chạy lại an toàn:
  ```bash
  docker compose run --rm polaris-bootstrap
  ```
- **`NotAuthorizedException` khi CREATE DATABASE/TABLE** → thiếu grant `CATALOG_MANAGE_CONTENT`. `setup.sh` giờ tự cấp grant lên role `catalog_admin` sau khi tạo catalog (idempotent, chạy cả khi catalog đã tồn tại). Nếu cần cấp thủ công ngoài setup:
  ```bash
  TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
    --user root:s3cr3t -H "Polaris-Realm: POLARIS" \
    -d 'grant_type=client_credentials' -d 'scope=PRINCIPAL_ROLE:ALL' \
    | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
  curl -s -X PUT "http://localhost:8181/api/management/v1/catalogs/quickstart_catalog/catalog-roles/catalog_admin/grants" \
    -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" \
    -H "Content-Type: application/json" \
    -d '{"privilege":"CATALOG_MANAGE_CONTENT","type":"catalog"}'
  ```
- **`ForbiddenException: Unable to purge entity: <table>` khi `DROP TABLE`** → Polaris **mặc định cấm** drop-with-purge (`DROP_WITH_PURGE_ENABLED=false`, kể từ Polaris #1619). Flink/Trino `DROP TABLE` cố gắng purge data files → bị chặn. **Chỉ bật server-side**, không qua config catalog (thử `polaris.config.drop-with-purge.enabled=true` ở phía client KHÔNG đủ). Fix: thêm vào env của service `polaris` rồi recreate:
  ```yaml
  'polaris.features."DROP_WITH_PURGE_ENABLED"': "true"
  ```
  rồi `docker compose up -d polaris`. Lưu ý: bật flag này đồng nghĩa `DROP TABLE` sẽ **xoá cả data files** (không khôi phục được) — chỉ dùng cho reset/thử nghiệm, cẩn thận trong môi trường production.

## Lỗi còn tồn đọng / việc thiếu
- **Đã xử lý:** catalog metadata persist qua **relational-jdbc → Postgres** (`polaris-db`), bootstrap schema bằng `polaris-bootstrap` (admin-tool `bootstrap`) trước khi server chạy. Không còn mất catalog khi recreate container.
- Còn thiếu: quản lý vòng đời/migration của schema Postgres (hiện bootstrap idempotent là đủ); chưa test failover cho `polaris-db`.

## Kiểm tra
```bash
curl -sf http://localhost:8182/q/health
# token flow
curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t -H "Polaris-Realm: POLARIS" \
  -d 'grant_type=client_credentials' -d 'scope=PRINCIPAL_ROLE:ALL'
```