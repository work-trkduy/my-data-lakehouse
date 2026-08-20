# Trino — Query engine (DBeaver → Iceberg)

## Vai trò
Trino cho phép truy vấn bảng Iceberg (đã có data từ Flink) qua Polaris REST catalog. DBeaver kết nối tới `localhost:8080`, catalog `iceberg`, schema `polaris` → table `transactions_cdc_log`.

## Version sử dụng
- Image: `trinodb/trino:483` (build repo `lakehouse/trino:483`).

**Lý do chọn 483:** nhánh stable; connector Iceberg native hỗ trợ S3 (fs.s3) kèm. DBeaver thấy như JDBC/Trino driver.

## Deploy
```bash
docker compose up -d trino
# catalog config: ./trino/catalog/iceberg.properties (mount /etc/trino/catalog)
```

## Cấu hình
### compose `trino:`
```yaml
environment:
  CLIENT_ID: ${POLARIS_ROOT_USER}        # root
  CLIENT_SECRET: ${POLARIS_ROOT_PASSWORD} # s3cr3t
  MINIO_ROOT_USER: ${MINIO_ROOT_USER}
  MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
  CATALOG_MANAGEMENT: static              # BẮT BUỘC static (Trino 483 enum)
  MINIO_ENDPOINT: ${MINIO_ENDPOINT}       # http://minio:9000
ports: ["8080:8080"]
volumes:
  - ./trino/catalog:/etc/trino/catalog:ro
```

### `trino/catalog/iceberg.properties` (QUAN TRỌNG)
```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=${ENV:CLIENT_ID}:${ENV:CLIENT_SECRET}
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
iceberg.rest-catalog.warehouse=quickstart_catalog
iceberg.rest-catalog.vended-credentials-enabled=false
fs.s3.enabled=true
s3.aws-access-key=${ENV:MINIO_ROOT_USER}
s3.aws-secret-key=${ENV:MINIO_ROOT_PASSWORD}
s3.endpoint=${ENV:MINIO_ENDPOINT}
s3.path-style-access=true
s3.region=us-west-2
iceberg.rest-catalog.http-headers=Polaris-Realm: POLARIS
```

### Giá trị có thể chỉnh / Lưu ý QUAN TRỌNG
- **S3 props của Trino 483 dùng namespace `s3.*` (native), KHÔNG phải `iceberg.s3.*`** và **phải bật `fs.s3.enabled=true`**. Nếu dùng `iceberg.s3.*` hoặc `hive.s3.*`/`hive.s3.custom.*` → Trino `Exit(100) Configuration property 'XXX' was not used`.
- `iceberg.rest-catalog.vended-credentials-enabled=false`: tắt cơ chế credentials vended. Vì vended credentials cần role được grant read-delegation; root/service_admin không có → buộc dùng **static S3 keys** (`s3.aws-access-key/secret`) như trên. 
- Property `s3.aws-secret-key` (KHÔNG phải `s3.aws-secret-access-key`).

## Lỗi có thể xảy ra / đã gặp
- **Trino `Exit(100)` "Configuration property 'X' was not used"** → sai namespace prefix S3 (đã dùng `iceberg.s3.*`/`hive.s3.*`). Fix: đổi sang `s3.*` + `fs.s3.enabled=true`.
- **Trino `Exit(100)` invalid `catalog.management`='file'** → Trino 483 enum là `static` (không `file`). Fix: `CATALOG_MANAGEMENT: static`.
- **Truy vấn bảng không thấy dữ liệu / AccessDenied** → credentials static sai hoặc vended-credentials bật; hoặc `MINIO_ENDPOINT` trong `s3.endpoint` không phải host nội bộ đúng.

## Kiểm tra (qua REST / DBeaver)
```bash
curl -s -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: admin" -H "X-Trino-Catalog: iceberg" \
  -d "SELECT count(*) FROM polaris.transactions_cdc_log"
```
Trong DBeaver: Trino connector, `localhost:8080`, catalog `iceberg`, nhấn `Browse schema polaris → transactions_cdc_log`.