-- ============ SOURCE TABLE FOR CDC TEST ============
-- Runs on first Postgres init (docker-entrypoint-initdb.d).
-- Also enables the pgoutput decoding plugin used by Debezium.

-- NOTE: pgoutput is a built-in logical decoding plugin in postgres:16,
-- it is NOT a createable extension. Debezium selects it via plugin.name=pgoutput.

DROP TABLE IF EXISTS public.transactions;

CREATE TABLE public.transactions (
    id           BIGSERIAL PRIMARY KEY,
    user_id      BIGINT          NOT NULL,
    amount       NUMERIC(12,2)   NOT NULL DEFAULT 0,
    currency     VARCHAR(3)      NOT NULL DEFAULT 'USD',
    status       VARCHAR(20)     NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Sample rows so there is data to CDC before any inserts.
INSERT INTO public.transactions (user_id, amount, currency, status)
VALUES
    (1, 100.50, 'USD', 'completed'),
    (2, 250.75, 'EUR', 'pending'),
    (3,  39.99, 'GBP', 'completed'),
    (4, 1000.00, 'USD', 'failed'),
    (5,  12.25, 'USD', 'pending');

-- Signal table for Debezium SOURCE-channel signaling (incremental snapshot /
-- backfill). Columns must be exactly (id, type, data); id is VARCHAR(42) PK.
-- The connector ignores the id VALUE and uses it only as a key.
CREATE TABLE IF NOT EXISTS public.debezium_signal (
    id    VARCHAR(42)  PRIMARY KEY,
    type  VARCHAR(32)  NOT NULL,
    data  VARCHAR(2048) NULL
);

-- The publication Debezium will consume using pgoutput.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'flink_publication') THEN
        CREATE PUBLICATION flink_publication FOR TABLE public.transactions;
    END IF;
END
$$;

-- The signal table MUST be in the publication so its change events reach the
-- connector via pgoutput (pgoutput only decodes tables in the publication).
-- It is deliberately NOT added to table.include.list, so signal rows are
-- processed for signaling but NOT emitted to Kafka topics.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'flink_publication'
          AND schemaname = 'public' AND tablename = 'debezium_signal'
    ) THEN
        ALTER PUBLICATION flink_publication ADD TABLE public.debezium_signal;
    END IF;
END
$$;