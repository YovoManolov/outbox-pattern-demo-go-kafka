CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS orders (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_name TEXT NOT NULL,
    item          TEXT NOT NULL,
    amount        NUMERIC(10, 2) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The outbox table lives in the SAME database/schema as the business tables.
-- Writing here happens in the same transaction as the business write, which
-- is what gives the outbox pattern its atomicity guarantee.
CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type TEXT NOT NULL,
    aggregate_id   UUID NOT NULL,
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- Speeds up the relay's poll query: "give me unpublished events, oldest first"
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (created_at)
    WHERE published_at IS NULL;
