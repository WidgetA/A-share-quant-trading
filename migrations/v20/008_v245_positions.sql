-- Notification references, never orders or evidence of execution.
ALTER TABLE v20.v22_alert_batches
    ADD COLUMN IF NOT EXISTS reference_kind TEXT NOT NULL DEFAULT 'STOCK'
        CHECK (reference_kind IN ('STOCK','BANK'));
-- Preserve the existing once-a-day STOCK slot, while allowing an independently
-- identifiable BANK recommendation on the same day after a fresh manual run.
ALTER TABLE v20.v22_alert_batches DROP CONSTRAINT IF EXISTS
    v22_alert_batches_official_stream_id_lineage_id_trade_date_key;
CREATE UNIQUE INDEX IF NOT EXISTS v22_alert_batches_reference_day
    ON v20.v22_alert_batches(official_stream_id,lineage_id,trade_date,reference_kind);

ALTER TABLE v20.v22_alert_positions
    ADD COLUMN IF NOT EXISTS asset_kind TEXT NOT NULL DEFAULT 'STOCK'
        CHECK (asset_kind IN ('STOCK','BANK')),
    ADD COLUMN IF NOT EXISTS strategy_version TEXT NOT NULL DEFAULT 'V22-slim',
    ADD COLUMN IF NOT EXISTS reference_clock TIME DEFAULT TIME '09:41:00';

CREATE TABLE IF NOT EXISTS v20.v245_switch_intents (
    intent_id TEXT PRIMARY KEY,
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    target TEXT NOT NULL CHECK (target IN ('BANK','STOCKS')),
    source_event_id TEXT NOT NULL REFERENCES v20.outbox_events(event_id),
    last_event_id TEXT NOT NULL REFERENCES v20.outbox_events(event_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (official_stream_id,lineage_id,trade_date,target)
);
ALTER TABLE v20.v245_switch_intents
    ADD COLUMN IF NOT EXISTS phase_event_id TEXT REFERENCES v20.outbox_events(event_id);

-- This obligation survives T+1 and an already-sent ordinary R33 alert.
-- It is closed only through the existing audited holding calibration.
CREATE TABLE IF NOT EXISTS v20.v245_position_switches (
    position_id TEXT PRIMARY KEY REFERENCES v20.v22_alert_positions(position_id),
    intent_id TEXT NOT NULL REFERENCES v20.v245_switch_intents(intent_id),
    requested_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE v20.v245_position_switches
    ADD COLUMN IF NOT EXISTS switch_alert_event_id TEXT REFERENCES v20.outbox_events(event_id),
    ADD COLUMN IF NOT EXISTS phase_event_id TEXT REFERENCES v20.outbox_events(event_id);
