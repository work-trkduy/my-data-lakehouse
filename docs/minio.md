# MinIO — Object storage (Iceberg data files)

## Vai trò
Lưu bản thân dữ liệu Iceberg (data files) + metadata. Kết nối theo giao thức S3, dùng path-style addressing (`s3://warehouse/...`), chạy `mc` để tạo bucket.

## Version sử dụng
- Image: `minio/minio:RELEASE.2025-09-07T16-13-09Z`
- Sidecar init: `minio/mc:RELEASE.2025-08-13T08-35-41Z`

**Lý do chọn:** những bản RC gần nhất bị pin theo ngày; chọn bản release ổn định mới nhất có trên Docker Hub. MinIO gần như S3-compatible, phù hợp chạy local thay AWS S3.

## Deploy
```bash
docker compose up -d minio minio-init
# console: http://localhost:9001 (minioadmin/minioadmin)
# S3 API:  http://localhost:9000
```
`minio-init` (sidecar `mc`) chờ MinIO rồi `mc mb --ignore-existing minio/${MINIO_BUCKET}` để tạo bucket.

## Cấu hình
### compose `minio:`
```yaml
image: lakehouse/minio:latest
command: server /data --console-address ":9001"
environment:
  MINIO_ROOT_USER: ${MINIO_ROOT_USER}      # minioadmin
  MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}  # minioadmin
ports: ["9000:9000", "9001:9001"]
volumes:
  - ./minio/data:/data     # bind mount data (KHÔNG volume docker)
```
### compose `minio-init:`
```yaml
image: minio/mc:RELEASE.2025-08-13T08-35-41Z
entrypoint: ["/bin/sh", "/init.sh"]
environment:
  MINIO_ENDPOINT: http://minio:9000
  MINIO_ROOT_USER: ${MINIO_ROOT_USER}
  MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
  MINIO_BUCKET: ${MINIO_BUCKET}   # warehouse
```

### `.env`
```
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_REGION=us-west-2
MINIO_BUCKET=warehouse
MINIO_ENDPOINT=http://minio:9000
MINIO_EXTERNAL_ENDPOINT=http://minio:9000
```

### Giá trị có thể chỉnh
| Biến | Chỉnh khi nào |
|------|----------------|
| `MINIO_BUCKET` | đổi tên bucket (phải đồng bộ vào `polaris/setup.sh` `default-base-location` và `allowedLocations`, và `trino` `iceberg.rest-catalog.warehouse`) |
| `MINIO_ROOT_USER/PASSWORD` | đổi credential → phải đồng bộ vào `.env`, `init.sh`, Polaris env, `flink/init.sql` (`s3.access-key-id/secret`), `trino/catalog` |
| `MINIO_REGION` | phải trùng region Flink/Trino/Polaris trỏ tới |

## Lưu ý khi config
- **Endpoint nội bộ (docker net) dùng `http://minio:9000`, không `localhost`.** Nếu Polaris vends `localhost`, client (Flink/Trino) trong net khác sẽ connection refused (đã gặp — xem [polaris.md](polaris.md)).
- **PATH-STYLE:** các client (Polaris `pathStyleAccess=true`, Flink `s3.path-style-access=true`, Trino `s3.path-style-access=true`) đều phải path-style vì MinIO không phải AWS virtual-host.

## Lỗi có thể xảy ra / đã gặp
- My Trino/Flink `AccessDenied` / 403 khi đọc dữ liệu → gần như do credential/region không khớp giữa các layer, hoặc do `vended-credentials` bật (Trino) — giải thích trong [trino.md](trino.md).
- `mc` không connect được → endpoint sai hoặc MinIO chưa up; sidecar có vòng wait.
- Bucket không tồn tại khi Polaris/Flink ghi → chạy lại `minio-init` sau khi sửa `MINIO_BUCKET`.

## Kiểm tra
```bash
# list bucket qua mc (từ thử nghiệm host, hoặc exec vào minio-init)
docker compose exec lakehouse-minio ls /data/warehouse
curl -sf http://localhost:9000/minio/health/live   # healthy
```