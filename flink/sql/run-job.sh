#!/bin/bash
#
# Submits the CDC streaming SQL job (init.sql) to the running Flink jobmanager.
# Run from YOUR HOST:
#
#   docker compose exec flink-jobmanager bash /opt/flink-sql/run-job.sh
#   # or, if not mounted with exec bit:
#   docker compose exec flink-jobmanager bash -lc 'bash /opt/flink-sql/run-job.sh'
#
# The Iceberg / Kafka connector jars are baked into the image at /opt/flink/lib
# by flink/Dockerfile -- no runtime copy is needed.
set -e

# ---------------------------------------------------------------------------
# STARTUP_MODE  auto   (default)  resume from the latest savepoint if one
#                                exists, else cold-start from earliest offset.
#               latest            start from the CURRENT topic end (new events
#                                only) -- the mode scripts/watchdog.sh uses after
#                                a reconcile.
#
# The Iceberg sink in init.sql is append-only (no PK, no write.upsert.enabled),
# so re-running from the earliest offset DUPLICATES rows — it is not idempotent.
# In `auto` mode we persist checkpoints/savepoints to /opt/flink/savepoints
# (bind-mounted from ./flink/savepoints) and, when a savepoint is present, start
# the new job from it instead of replaying the whole topic from the earliest
# offset. After a reconcile the log is ALREADY current, so `latest` skips the
# whole gap (reconciled rows live in the log, not in Kafka) and only new changes
# are ingested going forward.
# ---------------------------------------------------------------------------
STARTUP_MODE="${STARTUP_MODE:-auto}"

ARGS=(
  -D execution.checkpointing.interval=10s
  -D parallelism.default=1
)

if [ "${STARTUP_MODE}" = "latest" ]; then
  echo "[run-job] STARTUP_MODE=latest: starting from the CURRENT topic end."
  # Substitute the Kafka source startup mode in a temp copy (init.sql is a ro
  # bind-mount, so we cannot edit it in place).
  sed -e "s/'earliest-offset'/'latest-offset'/" \
      /opt/flink-sql/init.sql > /tmp/init-latest.sql
  ARGS+=( -f /tmp/init-latest.sql )
else
  ARGS+=( -f /opt/flink-sql/init.sql )
  SAVEPOINT_DIR=""
  if [ -d /opt/flink/savepoints ]; then
    # Savepoints are stored as <root>/savepoint-<jobid>-<rand>/; pick the newest.
    SAVEPOINT_DIR=$(ls -dt /opt/flink/savepoints/savepoint-* 2>/dev/null | head -n1 || true)
  fi

  if [ -n "${SAVEPOINT_DIR}" ] && [ -d "${SAVEPOINT_DIR}" ]; then
    echo "[run-job] Resuming from savepoint: ${SAVEPOINT_DIR}"
    ARGS+=(-D "execution.savepoint.path=${SAVEPOINT_DIR}")
  else
    echo "[run-job] No savepoint found under /opt/flink/savepoints; cold start."
  fi
fi

echo "[run-job] Submitting streaming job via sql-client..."
"/opt/flink/bin/sql-client.sh" "${ARGS[@]}"

echo "[run-job] Done."