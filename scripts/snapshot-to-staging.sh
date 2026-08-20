#!/usr/bin/env bash
set -euo pipefail
#
# Diff-based backfill, part 1: load a Debezium INCREMENTAL snapshot into the
# staging table `transactions_snapshot` WITHOUT touching the CDC log.
#
# Flow (run from the Docker host, project root):
#   1. Capture SNAP_TS = now (epoch ms), BEFORE firing the signal.
#   2. Fire the Debezium `execute-snapshot` (INCREMENTAL) signal.
#   3. Wait for the snapshot to complete (Kafka topic end-offset goes stable).
#   4. SNAP_END_TS = now.
#   5. Run the one-shot BATCH Flink job (flink/sql/snapshot-to-staging.sql)
#      reading the Kafka window [SNAP_TS, SNAP_END_TS] for `op='r'`, computing
#      `__hash__` in Flink (same contract as init.sql), INSERT into
#      `transactions_snapshot`.
#
# The staging table is DROPPED first so each run holds only the LATEST snapshot
# (no accumulation across runs). scripts/reconcile.sh consumes it afterwards.
#
# Prereq: Debezium connector RUNNING (a signal only works while it is
# streaming). See docs/backfill.md.
#
# Usage:
#   scripts/snapshot-to-staging.sh
#   TABLE=public.transactions scripts/snapshot-to-staging.sh

# --- config ---
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-transactions_db}"
TABLE="${TABLE:-public.transactions}"
TOPIC="postgres.public.transactions"
SIGNAL_ID="backfill-$(date +%s%N)"
SNAP_TS="$(date +%s%3N)"          # epoch MILLISECONDS, before firing

echo "[snapshot-to-staging] SNAP_TS=${SNAP_TS} signal=${SIGNAL_ID} table=${TABLE}"

# --- 1) drop the staging table so this run is the only state ---
scripts/trino-run.sh "DROP TABLE IF EXISTS polaris.transactions_snapshot" \
  || { echo "[snapshot-to-staging] drop staging failed" >&2; exit 1; }

# --- 2) fire the incremental snapshot signal ---
docker exec -i lakehouse-postgres \
  psql -U "${DB_USER}" -d "${DB_NAME}" >/dev/null <<SQL
INSERT INTO debezium_signal (id, type, data)
VALUES ('${SIGNAL_ID}', 'execute-snapshot', '{"data-collections": ["${TABLE}"], "type": "INCREMENTAL"}');
SQL
echo "[snapshot-to-staging] signal sent."

# --- 3) wait for the snapshot to finish: end-offset stable for 3 polls ---
prev=""
stable=0
for i in $(seq 1 45); do            # 45 * 2s = 90s budget
  sleep 2
  end="$(docker exec lakehouse-kafka /opt/kafka/bin/kafka-get-offsets.sh \
          --bootstrap-server localhost:9092 --topic "${TOPIC}" 2>/dev/null \
          | sed -n 's/.*:0://p' || true)"
  if [ -n "${end}" ]; then
    if [ "${end}" = "${prev}" ]; then
      stable=$((stable + 1))
      [ "${stable}" -ge 3 ] && { echo "[snapshot-to-staging] snapshot done (end-offset ${end})."; break; }
    else
      stable=0
    fi
    prev="${end}"
  fi
done
if [ "${stable}" -lt 3 ]; then
  echo "[snapshot-to-staging] WARNING: offset never stabilised; proceeding anyway (last=${prev})." >&2
fi
SNAP_END_TS="$(date +%s%3N)"
echo "[snapshot-to-staging] SNAP_END_TS=${SNAP_END_TS}"

# --- 4) substitute the window into the one-shot job and run it ---
sed -e "s/__SNAP_TS__/${SNAP_TS}/g" \
    -e "s/__SNAP_END_TS__/${SNAP_END_TS}/g" \
    flink/sql/snapshot-to-staging.sql > /tmp/snapshot-to-staging.subst.sql

docker cp /tmp/snapshot-to-staging.subst.sql \
  lakehouse-flink-jm:/tmp/snapshot-to-staging.subst.sql

echo "[snapshot-to-staging] running one-shot Flink batch job..."
if ! docker compose exec -T flink-jobmanager \
      /opt/flink/bin/sql-client.sh -f /tmp/snapshot-to-staging.subst.sql; then
  echo "[snapshot-to-staging] Flink batch job FAILED." >&2
  exit 1
fi

# --- 5) report ---
echo "[snapshot-to-staging] staging rows:"
scripts/trino-run.sh "SELECT count(*) AS rows, count(DISTINCT __hash__) AS distinct_hash FROM polaris.transactions_snapshot"
echo "[snapshot-to-staging] done. Next: scripts/reconcile.sh"
