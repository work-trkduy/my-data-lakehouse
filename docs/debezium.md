# Debezium (Kafka Connect) — Capture CDC từ Postgres

## Vai trò
Connector Kafka Connect đọc **WAL logical replication** của Postgres, biên dịch thành message JSON **full envelope** (`before`/`after`/`source`/`op`/`ts_ms`), ghi vào Kafka topic `postgres.public.transactions`. Đây là nguồn cho Flink.

## Version sử dụng
- Image: `debezium/connect:3.0.0.Final` (bản build của repo: `lakehouse/debezium:3.0.0`)
- Runtime Kafka Connect: `KafkaConnectApi` bị deprecated từ Kafka 4.0, nhưng Debezium 3.0 setup dùng chuẩn `BOOTSTRAP_SERVERS`/storage topics như cũ.

**Lý do chọn 3.0.0.Final:** nhánh ổn định, hỗ trợ PG 16 + Kafka 4.x; không thấy tag 3.1+ (stack này chọn bản reachable ổn định).

## Deploy
```bash
docker compose up -d debezium debezium-register
```
- `debezium`: Connect server.
- `debezium-register`: sidecar (`curlimages/curl`), chờ REST API rồi `POST` connector config từ `debezium/debezium-register.json`.

## Cấu hình (compose + register JSON)

### compose `debezium:` key
```yaml
environment:
  BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP}          # kafka:9092
  GROUP_ID: 1
  CONFIG_STORAGE_TOPIC: connect-configs
  OFFSET_STORAGE_TOPIC: connect-offsets
  STATUS_STORAGE_TOPIC: connect-status
  KEY_CONVERTER / VALUE_CONVERTER:
    org.apache.kafka.connect.json.JsonConverter   # JSON
  KEY_CONVERTER_SCHEMAS_ENABLE / VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
```

### `debezium-register.json` (connector config)
```json
{
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.dbname": "transactions_db",
    "database.server.name": "postgres",
    "topic.prefix": "postgres",
    "table.include.list": "public.transactions",
    "publication.name": "flink_publication",
    "slot.name": "flink_slot",
    "decimal.handling.mode": "string",
    "tombstones.on.delete": "false",
    "snapshot.mode": "initial",
    "schema.history.internal.kafka.topic": "postgres-transactions-history",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092"
  }
}
```

### Giá trị có thể chỉnh
| Option | Chỉnh khi nào |
|--------|----------------|
| `table.include.list` | thêm nhiều bảng cần CDC |
| `decimal.handling.mode` | `string` (đang dùng) — xuất `NUMERIC(12,2)` thành JSON string `"1234.56"` giữ nguyên precision; `double` lossy (ví dụ `100.50` → `100.5`, sai số khi nhân/chia). Flink khai báo `amount` là STRING |
| `snapshot.mode` | `initial` = snapshot toàn bộ hiện có rồi theo dõi tiếp; `no_data` = chỉ theo dõi thay đổi mới |
| `slot.name` / `publication.name` | đổi tên để nhiều connector không xung đột |
| `plugin.name=pgoutput` | khớp plugin PG có sẵn; `decoderbufs`/`wal2json` cần cài extension |

## Lưu ý khi config — QUAN TRỌNG
- **KHÔNG bật transform `ExtractNewRecordState` (unwrap SMT).** Flink dùng format `json` đọc **full envelope** và cần `op`, `source.lsn`, `source.ts_ms`, `source.db|schema|table` làm cột. SMT unwrap làm phẳng record thành `{id, user_id, ...}` → các cột envelope (`before`/`after`/`source`/`op`) thành null → Flink ghi hàng toàn null (hoặc fail). → **Bỏ hẳn key `transforms`** (config hiện tại đã đúng).
- `value.converter.schemas.enable=false` để value là JSON thô không schema wrapper, khớp với Flink `json` format.
- Giữ `tombstones.on.delete=false`: với sink append-only, ta cần **hàng DELETE thật** (envelope `op='d'`, `before` chứa pre-image). Nếu bật tombstone, Kafka log-compaction chèn message null → không có envelope → Flink `json` format không đọc được. Giữ `false`.
- `decimal.handling.mode=string` phải đi đôi với DDL Flink: source `amount` khai STRING (vì JSON value là string `"1234.56"`; khai DECIMAL sẽ parse sai); sink `transactions_cdc_log` cast `AS DECIMAL(12,2)` để giữ đúng kiểu PG. Chi tiết [flink.md](flink.md).

## Lỗi có thể xảy ra / đã gặp
- **Hàng toàn null trong bảng log / các cột `before`/`after`/`op` null** → do SMT unwrap cũ vẫn còn hoặc topic chứa dữ liệu cũ đã unwrap. Fix: xoá transforms, `docker compose build debezium`, xoá topic Kafka `postgres.public.transactions`, restart `debezium-register` rồi re-insert (hoặc `snapshot.mode=initial` để resync).
- **Dữ liệu cũ (unwrap cũ) vẫn còn trong topic** → sau khi fix SMT, xoá topic Kafka `postgres.public.transactions` và re-insert để topic recreate clean; hoặc `snapshot.mode=initial` để resync.
- **Kafka Connect crash `cleanup.policy=compact required`** → xem [kafka.md](kafka.md). Xoá/recreate 3 topic internal với `cleanup.policy=compact` rồi restart.
- **Register không thành công do cổng chưa up** → sidecar đã có loop wait tối đa 60×2s; nếu timeout, bật: `docker logs lakehouse-debezium-register`.

## Kiểm tra
```bash
curl -s localhost:8083/connectors/postgres-connector/status | head -c 500
# mong đợi "state": "RUNNING"
docker logs --tail 50 lakehouse-debezium
```