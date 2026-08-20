#!/usr/bin/env bash
set -euo pipefail
#
# Trigger a Debezium INCREMENTAL SNAPSHOT (backfill) of a source table.
#
# The connector must be RUNNING and STREAMING (snapshot.mode must leave it
# streaming -- `initial`, `no_data`, etc.) for the signal to be processed.
# Signals are only seen once the connector is streaming; they are NOT
# processed retroactively.
#
# Usage (run from the project root, on the Docker host):
#   debezium/backfill.sh                       # backfill public.transactions
#   TABLE=public.transactions debezium/backfill.sh
#   SIGNAL_ID=my-backfill-1 debezium/backfill.sh
#
# Re-firing: each run INSERTs a NEW signal row (a fresh WAL change) -> a new
# signal. To re-fire with the same id, UPDATE the existing row instead.

DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-transactions_db}"
TABLE="${TABLE:-public.transactions}"
SIGNAL_ID="${SIGNAL_ID:-backfill-$(date +%s)}"

echo "[backfill] signal id        : ${SIGNAL_ID}"
echo "[backfill] data-collections : [\"${TABLE}\"]"
echo "[backfill] sending execute-snapshot (INCREMENTAL) to lakehouse-postgres ..."

docker exec -i lakehouse-postgres \
  psql -U "${DB_USER}" -d "${DB_NAME}" <<SQL
INSERT INTO debezium_signal (id, type, data)
VALUES ('${SIGNAL_ID}', 'execute-snapshot', '{"data-collections": ["${TABLE}"], "type": "INCREMENTAL"}');
SQL

echo "[backfill] done. Monitor progress in the Debezium log:"
echo "  docker logs -f lakehouse-debezium | grep -i 'incremental snapshot'"
