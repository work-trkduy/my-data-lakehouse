# Kafka — Chặng vận chuyển CDC

## Vai trò
Kafka là lớp trung gian giữ lại luồng CDC từ Debezium (topic `postgres.public.transactions`), cho Flink tiêu thụ theo consumer group. Tách biệt nguồn (Postgres) khỏi sink (Iceberg).

## Version sử dụng
- Image: `apache/kafka:4.3.1` — KRaft single-node (broker + controller gộp, không Zookeeper).

**Lý do chọn KRaft 4.3.1:** Zookeeper mode deprecated; 4.3.x nhánh KRaft stable. 1 node đủ cho stack local.

## Deploy
```bash
docker compose up -d kafka
docker compose ps kafka   # đợi healthy (cả phút)
```

## Cấu hình (`docker-compose.yml` `kafka:`)
```yaml
image: apache/kafka:4.3.1
environment:
  KAFKA_NODE_ID: 1
  KAFKA_PROCESS_ROLES: broker,controller
  KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
  KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
  KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
  KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
  KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
  KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
ports: ["9092:9092"]
volumes:
  - ./kafka/data:/mnt/kafka-logs   # bind mount logs (KHÔNG volume docker)
```

### Giá trị có thể chỉnh
| Biến | Chỉnh khi nào |
|------|----------------|
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | để `3` nếu chạy cluster 3 node |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` trong prod, tạo topic thủ công |
| `KAFKA_ADVERTISED_LISTENERS` | đổi host nếu mesh khác (phải trùng hostname trong net docker) |

## Topic quan trọng
- `postgres.public.transactions` — data CDC (auto-create).
- `connect-configs` / `connect-offsets` / `connect-status` — internal Kafka Connect (Debezium). **Bắt buộc `cleanup.policy=compact`**.
- `postgres-transactions-history` — schema history Debezium.

## Lỗi có thể xảy ra / đã gặp
- **Debezium `Exited(1)` `Topic 'connect-offsets' ... cleanup.policy needed compact, found delete`** → 3 topic internal bị auto-create dạng `delete`. Fix:
  ```bash
  for t in connect-configs connect-offsets connect-status; do
    docker exec -it lakehouse-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --alter \
      --topic $t --config cleanup.policy=compact
  done
  docker compose restart debezium
  ```
  (Nếu không alter được do đã kẹt, xoá topic rồi cho Debezium recreate đúng policy.)
- **`connection refused` tới localhost:9092 từ container khác** → `KAFKA_ADVERTISED_LISTENERS` phải là hostname `kafka:9092`, không phải `localhost`.
- **consumer group rebalance chậm / văng** → STT group. Nếu không cần offset cũ, reset group.

## Kiểm tra
```bash
docker exec -it lakehouse-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic postgres.public.transactions \
  --from-beginning --max-messages 3
```