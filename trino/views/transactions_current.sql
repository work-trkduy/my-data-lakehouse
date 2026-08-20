-- ============================================================
-- Latest-state view over the APPEND-ONLY CDC log `transactions_cdc_log`.
--
-- The log keeps one row per change event, so "what is the current state of
-- each transaction id?" is answered by taking the most recent event per id
-- (highest __lsn__, tie-broken by __ingest_ts__) and dropping ids whose latest
-- event is a DELETE (`__op__ = 'd'`).
--
-- Why rank first, THEN filter deletes (not filter deletes first): if a row was
-- backfilled by a snapshot (`__op__ = 'r'`) and later deleted (`__op__ = 'd'`),
-- filtering deletes first would leave the stale snapshot row visible. Ranking
-- first keeps only the single newest event, then a delete removes the id.
--
-- Backfill rows (incremental snapshot) arrive with `__op__ = 'r'` and a fresh
-- __lsn__, so after a backfill this view reflects the snapshot's current state;
-- normal `c`/`u`/`d` events that follow (higher LSN) take over again.
--
-- `__hash__` is exposed so the reconcile job (scripts/reconcile.sh) can compare
-- staging-hash vs current-hash WITHOUT recomputing the hash on the Trino side
-- (the hash is always computed in Flink -- see flink/sql/init.sql "HASH
-- CONTRACT"). Deleted rows carry the last-known hash, used as an audit trail.
-- ============================================================

CREATE OR REPLACE VIEW polaris.transactions_current AS
SELECT
  id,
  user_id,
  amount,
  currency,
  status,
  created_at,
  updated_at,
  __op__        AS last_op,        -- 'c' create / 'u' update / 'r' snapshot-read / 'd' delete
  __source_ts__ AS last_change_ts,
  __hash__,
  __db__,
  __schema__,
  __table__,
  __ingest_ts__
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY id
           ORDER BY __lsn__ DESC, __ingest_ts__ DESC
         ) AS rn
  FROM polaris.transactions_cdc_log
) t
WHERE rn = 1
  AND __op__ <> 'd';

-- Usage (run in Trino, catalog `iceberg`):
--   SELECT * FROM polaris.transactions_current;
--
-- NOTE: __lsn__ ordering is approximate for backfill rows (incremental snapshot
-- chunks interleave with live streamed events). For exact reconciliation use a
-- downstream batch job keyed on (__db__, __schema__, __table__, __lsn__), or a
-- dedicated snapshot table instead of the shared log.
