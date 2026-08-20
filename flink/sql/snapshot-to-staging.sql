-- ============================================================
-- FLINK SQL (ONE-SHOT BATCH):  Debezium INCREMENTAL SNAPSHOT  ->  staging table
--
-- Companion job of init.sql for the diff-based backfill (see docs/backfill.md).
-- Flow: when the CDC stream is down/gappy, the watchdog fires a Debezium
-- INCREMENTAL snapshot via the `debezium_signal` table (debezium/backfill.sh).
-- Debezium emits the snapshot rows as `op='r'` events into the Kafka topic.
-- THIS job reads exactly those events (from SNAP_TS onward, bounded), computes
-- `__hash__` with the SAME contract as init.sql, and writes them into the
-- staging table `transactions_snapshot`. scripts/reconcile.sh then compares
-- staging hash vs the log's current hash and appends ONLY the diff.
--
-- Contract (MUST stay byte-identical to init.sql so the reconcile can compare
-- `__hash__` columns directly -- NO hashing on the Trino side):
--   MD5(LOWER(CONCAT_WS('||',
--       user_id, amount AS DECIMAL(12,2) AS STRING, currency, status)))
--   separator '||'; NULLs -> '^^'.
--
-- Why BATCH + bounded: a streaming Kafka source never terminates, but this job
-- is one-shot -- read the snapshot window [SNAP_TS, SNAP_END_TS] and stop.
--
-- Placeholders __SNAP_TS__ / __SNAP_END_TS__ (epoch milliseconds) are
-- substituted by scripts/snapshot-to-staging.sh.
-- ============================================================

RESET;
SET 'execution.runtime-mode' = 'BATCH';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- 1) Catalog pointing at Apache Polaris (Iceberg REST) -> MinIO.
--    Mirror of init.sql.
CREATE CATALOG iceberg_catalog
  WITH (
    'type'                       = 'iceberg',
    'catalog-impl'               = 'org.apache.iceberg.rest.RESTCatalog',
    'uri'                        = 'http://polaris:8181/api/catalog',
    'warehouse'                  = 'quickstart_catalog',
    'credential'                 = 'root:s3cr3t',
    'scope'                      = 'PRINCIPAL_ROLE:ALL',
    'io-impl'                    = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'                = 'http://minio:9000',
    's3.path-style-access'       = 'true',
    's3.access-key-id'           = 'minioadmin',
    's3.secret-access-key'       = 'minioadmin',
    'rest-catalog.http.headers.Polaris-Realm' = 'POLARIS'
  );

-- 2) Kafka source (default in-memory catalog, NOT iceberg -- same gotcha as
--    init.sql). Reads the FULL Debezium envelope as plain `json`; we only need
--    the `op='r'` snapshot rows produced after __SNAP_TS__.
--    Bounded via `scan.bounded.mode=timestamp` so the batch job terminates.
CREATE TABLE cdc_snapshot_source (
  `before` ROW<
    id          BIGINT,
    user_id     BIGINT,
    amount      STRING,
    currency    STRING,
    status      STRING,
    created_at  TIMESTAMP_LTZ(6),
    updated_at  TIMESTAMP_LTZ(6)
  >,
  `after` ROW<
    id          BIGINT,
    user_id     BIGINT,
    amount      STRING,
    currency    STRING,
    status      STRING,
    created_at  TIMESTAMP_LTZ(6),
    updated_at  TIMESTAMP_LTZ(6)
  >,
  `source` ROW<
    `db`     STRING,
    `schema` STRING,
    `table`  STRING,
    `lsn`    BIGINT,
    `ts_ms`  BIGINT
  >,
  `op`    STRING,
  `ts_ms` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic'     = 'postgres.public.transactions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode'          = 'timestamp',
  'scan.startup.timestamp-millis'      = '__SNAP_TS__',
  'scan.bounded.mode'          = 'timestamp',
  'scan.bounded.timestamp-millis'      = '__SNAP_END_TS__',
  'format'    = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- 3) Staging sink. Same data columns as the log + `__hash__` + source metadata.
--    `__source_ts__` = source.ts_ms of the snapshot event (deterministic).
--    NOTE: incremental-snapshot `r` events carry `source.lsn = NULL`, so the
--    staging table keeps lsn out of the comparison entirely; reconcile assigns
--    a synthetic high `__lsn__` when it appends diffs.
USE CATALOG iceberg_catalog;
USE polaris;

CREATE TABLE IF NOT EXISTS transactions_snapshot (
  id             BIGINT,
  user_id        BIGINT,
  amount         DECIMAL(12,2),
  currency       STRING,
  status         STRING,
  created_at     TIMESTAMP_LTZ(6),
  updated_at     TIMESTAMP_LTZ(6),
  __hash__       STRING,
  __source_ts__  TIMESTAMP_LTZ(3),
  __db__         STRING,
  __schema__     STRING,
  __table__      STRING
) WITH (
  'format-version' = '2'
);

-- 4) Insert the incremental-snapshot rows. Hash expression is byte-identical
--    to init.sql (HASH CONTRACT). Only `op='r'` rows are kept -- the window
--    [SNAP_TS, SNAP_END_TS] also contains any live c/u/d events that interleaved
--    with the snapshot, which the reconcile must NOT treat as state.
INSERT INTO transactions_snapshot
SELECT
  COALESCE(`after`.id,          `before`.id)          AS id,
  COALESCE(`after`.user_id,     `before`.user_id)     AS user_id,
  CAST(COALESCE(`after`.amount, `before`.amount) AS DECIMAL(12,2)) AS amount,
  COALESCE(`after`.currency,    `before`.currency)    AS currency,
  COALESCE(`after`.status,      `before`.status)      AS status,
  COALESCE(`after`.created_at,  `before`.created_at)  AS created_at,
  COALESCE(`after`.updated_at,  `before`.updated_at)  AS updated_at,
  MD5(LOWER(CONCAT_WS('||',
    CAST(COALESCE(`after`.user_id, `before`.user_id) AS STRING),
    CAST(CAST(COALESCE(`after`.amount, `before`.amount) AS DECIMAL(12,2)) AS STRING),
    COALESCE(COALESCE(`after`.currency, `before`.currency), '^^'),
    COALESCE(COALESCE(`after`.status,   `before`.status),   '^^')
  )))                                                     AS __hash__,
  TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                 AS __source_ts__,
  `source`.`db`                                       AS __db__,
  `source`.`schema`                                   AS __schema__,
  `source`.`table`                                    AS __table__
FROM default_catalog.default_database.cdc_snapshot_source
WHERE `op` = 'r';
