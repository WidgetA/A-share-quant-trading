CREATE TABLE IF NOT EXISTS v20.v22_alert_batches (
    batch_id TEXT PRIMARY KEY,
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    source_event_id TEXT NOT NULL REFERENCES v20.outbox_events(event_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(official_stream_id,lineage_id,trade_date)
);
CREATE TABLE IF NOT EXISTS v20.v22_alert_positions (
    position_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES v20.v22_alert_batches(batch_id),
    code TEXT NOT NULL,
    stock_name TEXT NOT NULL,
    entry_date DATE NOT NULL,
    entry_price DOUBLE PRECISION CHECK (entry_price>0 AND entry_price<'Infinity'::float8),
    quantity BIGINT CHECK (quantity>=0),
    price_source TEXT NOT NULL DEFAULT 'PENDING_REFERENCE',
    status TEXT NOT NULL DEFAULT 'MONITORING' CHECK(status IN ('MONITORING','NOT_BOUGHT','CLOSED')),
    revision BIGINT NOT NULL DEFAULT 0,
    calibrated BOOLEAN NOT NULL DEFAULT FALSE,
    extended BOOLEAN NOT NULL DEFAULT FALSE,
    alert_event_id TEXT REFERENCES v20.outbox_events(event_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(batch_id,code)
);
CREATE TABLE IF NOT EXISTS v20.v22_position_calibrations (
    request_id TEXT PRIMARY KEY,
    position_id TEXT NOT NULL REFERENCES v20.v22_alert_positions(position_id),
    request_hash TEXT NOT NULL,
    before_json JSONB NOT NULL,
    after_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
