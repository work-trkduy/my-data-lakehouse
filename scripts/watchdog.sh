#!/usr/bin/env bash
set -euo pipefail
#
# Watchdog for the CDC streaming pipeline: detect a dead / stalled stream and
# recover WITHOUT reloading 100% of the source (diff-based backfill).
#
# Detection (ANY one triggers recovery):
#   1. No RUNNING streaming job ("insert-into_iceberg_catalog.polaris.transactions_cdc_log").
#   2. Job RUNNING but no completed checkpoint in the last MAX_STREAM_STALL seconds.
#   3. Job RUNNING but the most recent checkpoint FAILED (failed newer than completed).
#
# Recovery (guard: the Debezium connector must be RUNNING, else an incremental
# snapshot signal cannot be processed):
#   a. Cancel the stalled streaming job (if any) so the log is quiescent.
#   b. scripts/snapshot-to-staging.sh   Debezium incremental snapshot -> staging
#                                       table (__hash__ computed in Flink).
#   c. scripts/reconcile.sh             append ONLY the diff to the log (SCD2).
#   d. Restart the streaming job from the LATEST Kafka offset (STARTUP_MODE=latest)
#      -- NOT earliest -- so it does NOT replay the topic it already ingested and
#      reconciled. run-job.sh's cold-start default (earliest) WOULD duplicate rows.
#
# Schedule from a cron / systemd timer, e.g. every 5 min:
#   */5 * * * *  cd /mnt/d/Projects/my_data_lakehouse && ./scripts/watchdog.sh
#
# Usage (Docker host, project root):
#   scripts/watchdog.sh            # check once; recover if a trigger fires
#   scripts/watchdog.sh --explain  # print health + trigger reason, NO recovery
#   FORCE=1 scripts/watchdog.sh    # always run the recovery path
#
# Exit codes: 0 = OK or recovered; 1 = explain mode, trigger would fire;
#             3 = guard blocked (Debezium not RUNNING); 4 = Flink REST down;
#             5/6 = snapshot/reconcile failed; 7 = restart did not come up.

# --- config ---
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR="${CONNECTOR:-postgres-connector}"
JOB_NAME="insert-into_iceberg_catalog.polaris.transactions_cdc_log"
MAX_STREAM_STALL="${MAX_STREAM_STALL:-300}"   # seconds without a completed checkpoint

PYJSON='
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print(""); raise SystemExit(0)
def g(*path):
    x = d
    for p in path:
        x = x.get(p) if isinstance(x, dict) else None
        if x is None:
            break
    return x
key = sys.argv[1]
if key == "running_job":
    name = sys.argv[2]
    j = [x for x in d.get("jobs", []) if x.get("name") == name and x.get("state") == "RUNNING"]
    print(j[0]["jid"] if j else "")
elif key == "completed_ts":
    print(g("latest", "completed", "trigger_timestamp") or "")
elif key == "failed_ts":
    print(g("latest", "failed", "trigger_timestamp") or "")
elif key == "job_state":
    print(d.get("state", ""))
elif key == "conn_state":
    print(g("connector", "state") or "")
else:
    print("")
'

explain=0
[ "${1:-}" = "--explain" ] && explain=1

now_ms() { date +%s%3N; }

# --- 0) Flink up? ---
if ! curl -sf -m 5 "${FLINK_URL}/jobs/overview" >/dev/null 2>&1; then
  echo "[watchdog] Flink REST unreachable at ${FLINK_URL} -- cannot assess or recover. No changes made."
  exit 4
fi

# --- 1) find the RUNNING streaming job ---
jobs_json="$(curl -sf -m 5 "${FLINK_URL}/jobs/overview")"
jid="$(printf '%s' "${jobs_json}" | python3 -c "${PYJSON}" running_job "${JOB_NAME}")"

# --- 2) decide the trigger reason ---
reason=""
if [ -z "${jid}" ]; then
  reason="no RUNNING streaming job '${JOB_NAME}'"
else
  cp_json="$(curl -sf -m 5 "${FLINK_URL}/jobs/${jid}/checkpoints" 2>/dev/null || echo '{}')"
  completed_ts="$(printf '%s' "${cp_json}" | python3 -c "${PYJSON}" completed_ts)"
  failed_ts="$(printf '%s' "${cp_json}" | python3 -c "${PYJSON}" failed_ts)"
  if [ -n "${completed_ts}" ]; then
    age=$(( ( $(now_ms) - completed_ts ) / 1000 ))
    if [ "${age}" -gt "${MAX_STREAM_STALL}" ]; then
      reason="job RUNNING but last completed checkpoint is ${age}s old (> ${MAX_STREAM_STALL}s)"
    fi
  fi
  if [ -z "${reason}" ] && [ -n "${failed_ts}" ]; then
    latest_ok="${completed_ts:-0}"
    if [ "${failed_ts}" -gt "${latest_ok}" ]; then
      reason="last checkpoint FAILED"
    fi
  fi
fi

# --- 3) no trigger -> healthy ---
if [ -z "${reason}" ] && [ "${FORCE:-0}" != "1" ]; then
  echo "[watchdog] OK: streaming job ${jid:-<none>} healthy, no trigger (stall threshold ${MAX_STREAM_STALL}s)."
  exit 0
fi

if [ "${explain}" = "1" ]; then
  echo "[watchdog] TRIGGER WOULD FIRE: ${reason:-FORCED}"
  exit 1
fi
echo "[watchdog] TRIGGER: ${reason:-FORCED}"

# --- 4) guard: Debezium connector must be RUNNING ---
conn_state="$(curl -sf -m 5 "${CONNECT_URL}/connectors/${CONNECTOR}/status" \
              | python3 -c "${PYJSON}" conn_state 2>/dev/null || true)"
if [ "${conn_state}" != "RUNNING" ]; then
  echo "[watchdog] Debezium '${CONNECTOR}' state=${conn_state:-?} -- an incremental-snapshot signal cannot be processed. Aborting recovery (no changes made)."
  exit 3
fi
echo "[watchdog] guard OK: Debezium '${CONNECTOR}' RUNNING."

# --- 5a) quiesce the stream: cancel the stalled job (if any) ---
if [ -n "${jid}" ]; then
  echo "[watchdog] canceling stalled streaming job ${jid}..."
  curl -sf -m 10 -X POST "${FLINK_URL}/jobs/${jid}/cancel" >/dev/null \
    || echo "[watchdog] cancel request failed (continuing anyway)"
  for _ in $(seq 1 30); do
    sleep 1
    st="$(curl -sf -m 5 "${FLINK_URL}/jobs/${jid}" 2>/dev/null | python3 -c "${PYJSON}" job_state || true)"
    [ -n "${st}" ] && [ "${st}" != "RUNNING" ] && { echo "[watchdog] job ${jid} -> ${st}."; break; }
  done
else
  echo "[watchdog] no streaming job running; log is quiescent."
fi

# --- 5b) snapshot -> staging ---
echo "[watchdog] step 1/3: incremental snapshot -> staging..."
if ! scripts/snapshot-to-staging.sh; then
  echo "[watchdog] snapshot-to-staging FAILED; recovery aborted." >&2
  exit 5
fi

# --- 5c) reconcile ---
echo "[watchdog] step 2/3: reconcile (append diff)..."
if ! scripts/reconcile.sh; then
  echo "[watchdog] reconcile FAILED; recovery aborted." >&2
  exit 6
fi

# --- 5d) restart the stream from the LATEST offset ---
echo "[watchdog] step 3/3: restarting streaming job from LATEST offset (skips the reconciled gap)..."
docker compose exec -T -d -e STARTUP_MODE=latest flink-jobmanager bash /opt/flink-sql/run-job.sh

for _ in $(seq 1 60); do        # 60 * 2s = 120s budget
  sleep 2
  new_jid="$(curl -sf -m 5 "${FLINK_URL}/jobs/overview" 2>/dev/null \
             | python3 -c "${PYJSON}" running_job "${JOB_NAME}" || true)"
  if [ -n "${new_jid}" ]; then
    echo "[watchdog] stream restarted: job ${new_jid} RUNNING."
    exit 0
  fi
done
echo "[watchdog] streaming job did not come up within 120s -- investigate." >&2
exit 7
