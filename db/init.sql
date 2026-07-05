CREATE TABLE IF NOT EXISTS orders (
    id          TEXT PRIMARY KEY,          -- ULID assigned at ingest
    store_id    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'new',
    customer    JSONB NOT NULL,
    lines       JSONB NOT NULL,
    total_cents INTEGER NOT NULL CHECK (total_cents >= 0),
    created_at  TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_store_created ON orders (store_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
