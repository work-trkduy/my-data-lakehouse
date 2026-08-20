#!/bin/bash
#
# Waits for the Debezium Connect REST API, then registers the Postgres CDC
# connector. Runs in the `debezium-register` sidecar container.
set -e

echo "[register] Waiting for Debezium Connect REST on http://debezium:8083 ..."
for i in $(seq 1 60); do
  if curl -sf http://debezium:8083/connectors >/dev/null 2>&1; then
    echo "[register] Connect API is up."
    break
  fi
  sleep 2
  if [ "$i" = "60" ]; then
    echo "[register] Connect API did not become ready in time" >&2
    exit 1
  fi
done

# Idempotent: if the connector is already registered, force-recreate it.
echo "[register] Registering postgres-connector..."
curl -sf -o /dev/null -X DELETE http://debezium:8083/connectors/postgres-connector 2>/dev/null || true

curl -sf -o /dev/null \
  -H "Content-Type: application/json" \
  -X POST \
  --data @/scripts/debezium-register.json \
  http://debezium:8083/connectors

echo "[register] postgres-connector registered."
echo "[register] Status check:"
curl -s http://debezium:8083/connectors/postgres-connector/status
echo
exit 0