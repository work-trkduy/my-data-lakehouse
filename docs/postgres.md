# PostgreSQL — Nguồn dữ liệu CDC

## Vai trò
Nguồn sự thật (`source of truth`). Table `transactions` được ghi/update bởi ứng dụng; Debezium bắt các thay đổi này qua **logical replication (WAL)** để đẩy xuống Kafka → Iceberg.

## Version sử dụng
- Image: `postgres:16`
- Bản thân build của repo: `lakehouse/postgres:16`

**Lý do chọn 16:** stable, được kế thừa rộng rãi. `wal_level=logical` + plugin decoding `pgoutput` sẵn có (không cần cài extension riêng). Debezium 3.x hỗ trợ PG 16.

## Deploy
```bash
# từ repo root
docker compose up -d postgres
# kiểm tra health
docker compose ps postgres
```
Chạy lần đầu chạy `postgres/init/init-db.sql` (mount vào `/docker-entrypoint-initdb.d`). Nếu `./postgres/data` đã có dữ liệu cũ, init SQL **không chạy lại** (chỉ chạy khi data dir rỗng).

## Cấu hình dùng trong repo (docker-compose.yml `postgres:`)
```yaml
environment:
  POSTGRES_USER: ${POSTGRES_USER}        # postgres
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD} # postgres
  POSTGRES_DB: ${POSTGRES_DB}            # transactions_db
command:
  - "postgres"
  - "-c" "wal_level=logical"            # bật logical replication
  - "-c" "max_wal_senders=10"           # số WAL sender chịu được cho Debezium
  - "-c" "max_replication_slots=10"     # số replication slot
ports: ["5432:5432"]
volumes:
  - ./postgres/data:/var/lib/postgresql/data        # dữ liệu (bind mount, KHÔNG volume docker)
  - ./postgres/init:/docker-entrypoint-initdb.d:ro  # init SQL
```

### Giá trị có thể chỉnh theo nhu cầu
| Biến | Nghĩa | Chỉnh khi nào |
|------|-------|----------------|
| `wal_level=logical` | bật logical decode | **Bắt buộc** cho CDC; đổi sang `replica` sẽ làm Debezium fail |
| `max_wal_senders` | bao nhiêu kết nối WAL song song | tăng khi thêm nhiều connector/consumer |
| `max_replication_slots` | bao nhiêu slot | ≥ số connector Debezium đang chạy |
| `POSTGRES_DB` | database nguồn | đổi tên DB thì phải sửa cả `debezium-register.json` (`database.dbname`) |
| `POSTGRES_*` pass | credential | sửa phải đồng bộ vào `debezium-register.json` |

## Schema / dữ liệu khởi tạo (`postgres/init/init-db.sql`)
- Bảng `public.transactions(id BIGSERIAL PK, user_id, amount NUMERIC(12,2), currency, status, created_at, updated_at)`.
- Chèn 5 hàng mẫu.
- Tạo `CREATE PUBLICATION flink_publication FOR TABLE public.transactions` — Debezium tiêu thụ publication này.

## Lưu ý khi config
- **Init SQL chỉ chạy khi `/var/lib/postgresql/data` rỗng.** Reset dữ liệu: `docker compose down` rồi `rm -rf postgres/data` (hoặc `down -v` — ở repo này không có named volume, `-v` chỉ xoá cùng loại).
- Nếu đổi schema/bảng quyết định capture, phải cập nhật lại:
  - `debezium/debezium-register.json`: `table.include.list`, `publication.name`.
  - publication trong `init-db.sql`.
- Không xoá replication slot khi connector đang chạy — bài toán WAL growth. Slot `flink_slot` được Debezium tạo tự động.

## Lỗi có thể xảy ra / đã gặp
- **Connector không bắt được thay đổi** → thường do `wal_level` không phải `logical` hoặc publication/table list sai. Kiểm tra: `SELECT * FROM pg_publication; SELECT * FROM pg_replication_slots;`
- **`init-db.sql` không chạy** → data dir đã tồn tại, xoá `postgres/data` và khởi động lại.
- **Data dir mount permission denied** (Windows WSL): nếu thấy lỗi quyền trên `/var/lib/postgresql/data`, chắc chắn bind-mount sang project dir dính quyền — cần set lại owner cho `postgres/data`.

## Chạy lệnh kiểm tra
```bash
docker exec -it lakehouse-postgres psql -U postgres -d transactions_db \
  -c "SELECT count(*) FROM transactions;"
# insert thử để feed CDC
docker exec -it lakehouse-postgres psql -U postgres -d transactions_db \
  -c "INSERT INTO transactions (user_id, amount, currency, status) VALUES (99, 1234.56, 'USD', 'completed');"
```