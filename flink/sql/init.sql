-- ============================================================
-- FLINK SQL:  Postgres (Debezium JSON on Kafka)  ->  Iceberg lakehouse
--
-- Reads the CDC topic written by Debezium and APPENDS every change event
-- (insert/update/delete) to an append-only Iceberg event log managed by
-- Polaris (REST catalog), stored on MinIO.
--
-- Uses Flink's `json` format against Debezium's FULL envelope
-- (before/after/op/source/ts_ms), declared as nested ROW columns so the
-- operation type (`op`) and source metadata (`source.lsn`, `source.ts_ms`,
-- `source.db|schema|table`) surface as regular columns. The `debezium-json`
-- format cannot expose them, which is why we read the raw JSON envelope.
--
-- Do NOT enable the ExtractNewRecordState SMT on the Debezium connector —
-- it strips the envelope we need.
--
-- The sink is APPEND-ONLY (no PRIMARY KEY, no write.upsert.enabled): every
-- change is a new row carrying the 8 CDC system columns (`__op__`,
-- `__source_ts__`, `__lsn__`, `__db__`, `__schema__`, `__table__`,
-- `__hash__`, `__ingest_ts__`). `__source_ts__` is derived deterministically
-- from source.ts_ms so replays are reproducible. `__hash__` is a
-- full-record MD5 fingerprint used by the reconcile/backfill job to append
-- only changed rows (SCD2-style diff) instead of reloading 100%.
-- ============================================================

RESET;
SET 'execution.runtime-mode' = 'STREAMING';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 1) Catalog pointing at Apache Polaris (Iceberg REST) -> MinIO
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

-- 2) Kafka source table (kept in the DEFAULT in-memory catalog, NOT iceberg).
--    If this were created after USE CATALOG iceberg_catalog it would be treated
--    as an Iceberg table (an empty source => "no more splits") and the job would
--    finish immediately. The 'connector'='kafka' option only works on a table
--    owned by a non-Iceberg catalog.
--
--    Format is plain `json` (NOT debezium-json) against Debezium's full
--    envelope, declared as nested ROW types. Reserved words (`before`,
--    `after`, `source`, `db`, `schema`, `table`) are backtick-quoted.
--    `amount` is STRING because the connector uses decimal.handling.mode=string
--    (exact decimals). `created_at`/`updated_at` are TIMESTAMP_LTZ(6) parsed
--    from the Debezium ISO-8601 string via
--    'json.timestamp-format.standard' = 'ISO-8601'.
CREATE TABLE IF NOT EXISTS cdc_transactions_source (
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
  'properties.group.id' = 'flink-lakehouse',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode'  = 'earliest-offset',
  'format'    = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- 3) Iceberg sink table (managed by Polaris, stored on MinIO).
--    APPEND-ONLY CDC event log: one row per change event (c/u/d/r/t), NO
--    PRIMARY KEY, NO write.upsert.enabled. Each row repeats the data columns
--    plus the 7 system columns so the full change history is preserved.
--    format-version=2 is retained (IDs/column-deletes + deletes support).
--    `amount` is DECIMAL(12,2): the connector emits it as an exact STRING and
--    the INSERT casts it back to DECIMAL (lossless), matching the source PG type.
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS polaris;
USE polaris;

CREATE TABLE IF NOT EXISTS transactions_cdc_log (
  id             BIGINT,
  user_id        BIGINT,
  amount         DECIMAL(12,2),
  currency       STRING,
  status         STRING,
  created_at     TIMESTAMP_LTZ(6),
  updated_at     TIMESTAMP_LTZ(6),
  __op__         STRING,
  __source_ts__  TIMESTAMP_LTZ(3),
  __lsn__        BIGINT,
  __db__         STRING,
  __schema__     STRING,
  __table__      STRING,
  __hash__       STRING,
  __ingest_ts__  TIMESTAMP_LTZ(3)
) WITH (
  'format-version'         = '2',
  'write.metadata.delete-after-commit.enabled' = 'true'
);
-- TODO: partition by days(__source_ts__) + add retention/expiry once daily
--       volume justifies it; leaving the log unpartitioned keeps the PoC simple.

-- 4) Continuous streaming job: Kafka CDC -> Iceberg append-only log.
--    NOTE: the Kafka source lives in the DEFAULT (in-memory) catalog. After
--    USE CATALOG iceberg_catalog, the unqualified name would resolve to an
--    (empty) Iceberg table, so we qualify it explicitly with its catalog.
--    COALESCE(after.*, before.*) carries the old values for DELETE events
--    (after is null, before holds the pre-image). `__source_ts__` comes from
--    source.ts_ms (epoch milliseconds), NOT CURRENT_TIMESTAMP, so replays are
--    deterministic. `__ingest_ts__` is the Debezium envelope top-level ts_ms.
--
--    BUG-GOTCHA: use TO_TIMESTAMP_LTZ(ts_ms, 3) — pass the RAW millisecond
--    value. The 2nd arg `3` already tells Flink to treat the number as ms
--    (divide by 1000 internally). Do NOT write TO_TIMESTAMP_LTZ(ts_ms/1000, 3):
--    that divides by 1000 TWICE, so timestamps land at 1970-01-21 (ms/1e6).
--
--    __hash__ : full-record fingerprint used by the reconcile job
--    (scripts/reconcile.sh) to append ONLY rows whose content differs from the
--    current state of the log (SCD2-style diff backfill) instead of reloading
--    100%. HASH CONTRACT (see docs/backfill.md) — computed ONLY in Flink, with
--    the SAME expression at streaming ingest (here) and at staging load
--    (flink/sql/snapshot-to-staging.sql); the reconcile then compares the two
--    __hash__ columns directly, so NO hash function is needed on the Trino side.
--      MD5(LOWER(CONCAT_WS('||',
--          user_id, amount AS DECIMAL(12,2) AS STRING, currency, status)))
--    - separator is '||'; NULL business columns are replaced by the sentinel
--      '^^' (real values like 'USD'/'pending' can never contain either).
--    - amount is pinned to scale 2 BEFORE stringification so both sides emit
--      '100.50' (not '100.5' / '100.500').
--    - created_at/updated_at are deliberately EXCLUDED from the hash: an edit
--      that changes ONLY updated_at (same business columns) is therefore NOT
--      detected as a change by reconcile — acceptable for this model.
INSERT INTO transactions_cdc_log (
  id, user_id, amount, currency, status, created_at, updated_at,
  __op__, __source_ts__, __lsn__, __db__, __schema__, __table__, __hash__, __ingest_ts__
)
SELECT
  COALESCE(`after`.id,          `before`.id)          AS id,
  COALESCE(`after`.user_id,     `before`.user_id)     AS user_id,
  CAST(COALESCE(`after`.amount, `before`.amount) AS DECIMAL(12,2)) AS amount,
  COALESCE(`after`.currency,    `before`.currency)    AS currency,
  COALESCE(`after`.status,      `before`.status)      AS status,
  COALESCE(`after`.created_at,  `before`.created_at)  AS created_at,
  COALESCE(`after`.updated_at,  `before`.updated_at)  AS updated_at,
  `op`                            AS __op__,
  TO_TIMESTAMP_LTZ(`source`.ts_ms, 3)                 AS __source_ts__,
  `source`.lsn                    AS __lsn__,
  `source`.`db`                   AS __db__,
  `source`.`schema`               AS __schema__,
  `source`.`table`                AS __table__,
  MD5(LOWER(CONCAT_WS('||',
    CAST(COALESCE(`after`.user_id, `before`.user_id) AS STRING),
    CAST(CAST(COALESCE(`after`.amount, `before`.amount) AS DECIMAL(12,2)) AS STRING),
    COALESCE(COALESCE(`after`.currency, `before`.currency), '^^'),
    COALESCE(COALESCE(`after`.status,   `before`.status),   '^^')
  )))                                                     AS __hash__,
  TO_TIMESTAMP_LTZ(ts_ms, 3)                          AS __ingest_ts__
FROM default_catalog.default_database.cdc_transactions_source;
