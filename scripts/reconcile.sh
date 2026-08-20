#!/usr/bin/env bash
set -euo pipefail
#
# Diff-based backfill, part 2: reconcile the CDC log against the source's
# CURRENT state WITHOUT recomputing hashes in Trino.
#
# Input : polaris.transactions_snapshot -- the incremental snapshot loaded by
#         scripts/snapshot-to-staging.sh (hash computed in Flink, same contract
#         as the log ingest in flink/sql/init.sql).
# Output: appends ONLY the diff to polaris.transactions_cdc_log:
#   * new OR changed rows  ->  __op__='r'  (id missing from current, or hash differs)
#   * deleted rows         ->  __op__='d'  (id present in current, absent from staging)
# Unchanged rows are NOT appended (SCD2-style diff, NOT a 100% reload).
#
# Every appended row gets a SYNTHETIC high __lsn__ (current max + offset) so it
# WINS the latest-state view (ORDER BY __lsn__ DESC). Real CDC events that
# follow (higher real LSN) take over again, and `transactions_current` keeps
# working. Staging snapshot `r` events carry source.lsn=NULL, hence the
# synthetic value (Trino sorts NULLS LAST in DESC, so NULL would LOSE the view).
#
# No hashing here: the comparison is `staging.__hash__ <> current.__hash__` on
# values already computed in Flink. See docs/backfill.md "HASH CONTRACT".
#
# Usage (Docker host, project root):
#   scripts/reconcile.sh
#   DRY_RUN=1 scripts/reconcile.sh      # print the diff, do NOT append
#
# Prereq: scripts/snapshot-to-staging.sh ran and populated the staging table.

# --- 1) compute the synthetic lsn base (monotonic; NULLs ignored by MAX) ---
SYNTH_LSN="$(scripts/trino-run.sh \
  "SELECT COALESCE(MAX(__lsn__), 0) + 1 FROM polaris.transactions_cdc_log" \
  | tail -n1)"
if ! [[ "${SYNTH_LSN}" =~ ^[0-9]+$ ]]; then
  echo "[reconcile] could not read max(__lsn__), got: ${SYNTH_LSN}" >&2
  exit 1
fi
echo "[reconcile] synthetic lsn base = ${SYNTH_LSN}"

# --- 2) show (or apply) the diff that WOULD be appended ---
DIFF_SQL="
SELECT 'r' AS op, s.id, s.__hash__, COALESCE(t.__hash__, '') AS cur_hash,
       CASE WHEN t.id IS NULL THEN 'new' ELSE 'changed' END AS reason
FROM   polaris.transactions_snapshot s
LEFT JOIN polaris.transactions_current t ON s.id = t.id
WHERE  t.id IS NULL OR s.__hash__ <> t.__hash__
UNION ALL
SELECT 'd', t.id, t.__hash__, '', 'deleted'
FROM   polaris.transactions_current t
LEFT JOIN polaris.transactions_snapshot s ON t.id = s.id
WHERE  s.id IS NULL
ORDER BY id;
"

echo "[reconcile] diff preview:"
scripts/trino-run.sh "${DIFF_SQL}" || true

if [ "${DRY_RUN:-0}" = "1" ]; then
  echo "[reconcile] DRY_RUN: not appending."
  exit 0
fi

# --- 3) append new/changed rows as 'r' (staging state + synthetic lsn) ---
echo "[reconcile] appending new/changed (op='r')..."
scripts/trino-run.sh "
INSERT INTO polaris.transactions_cdc_log (
  id, user_id, amount, currency, status, created_at, updated_at,
  __op__, __source_ts__, __lsn__, __db__, __schema__, __table__, __hash__, __ingest_ts__
)
SELECT
  s.id, s.user_id, s.amount, s.currency, s.status, s.created_at, s.updated_at,
  'r', s.__source_ts__, ${SYNTH_LSN},
  s.__db__, s.__schema__, s.__table__, s.__hash__, CURRENT_TIMESTAMP
FROM polaris.transactions_snapshot s
LEFT JOIN polaris.transactions_current t ON s.id = t.id
WHERE t.id IS NULL OR s.__hash__ <> t.__hash__;
"

# --- 4) append deleted rows as 'd' (last-known state from the view) ---
echo "[reconcile] appending deleted (op='d')..."
scripts/trino-run.sh "
INSERT INTO polaris.transactions_cdc_log (
  id, user_id, amount, currency, status, created_at, updated_at,
  __op__, __source_ts__, __lsn__, __db__, __schema__, __table__, __hash__, __ingest_ts__
)
SELECT
  t.id, t.user_id, t.amount, t.currency, t.status, t.created_at, t.updated_at,
  'd', t.last_change_ts, ${SYNTH_LSN} + 1,
  t.__db__, t.__schema__, t.__table__, t.__hash__, CURRENT_TIMESTAMP
FROM polaris.transactions_current t
LEFT JOIN polaris.transactions_snapshot s ON t.id = s.id
WHERE s.id IS NULL;
"

# --- 5) report ---
echo "[reconcile] log rows:"
scripts/trino-run.sh "SELECT __op__, count(*) AS n FROM polaris.transactions_cdc_log GROUP BY __op__ ORDER BY __op__"
echo "[reconcile] current state:"
scripts/trino-run.sh "SELECT count(*) AS current_rows FROM polaris.transactions_current"
echo "[reconcile] done."
