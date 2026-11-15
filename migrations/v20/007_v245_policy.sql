-- Separate overlay state: never rewrite the original V22 health/risk lineage.
CREATE TABLE IF NOT EXISTS v20.v245_policy_state (
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    state_hash CHAR(64) NOT NULL,
    state_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (official_stream_id,lineage_id)
);

-- A fresh valid calculation after INPUT_INVALID supplies the original reference
-- basket without rewriting the failed daily slot or its official-state receipt.
CREATE TABLE IF NOT EXISTS v20.v245_reference_repairs (
    batch_id TEXT PRIMARY KEY REFERENCES v20.shadow_batches(batch_id),
    source_event_id TEXT NOT NULL REFERENCES v20.outbox_events(event_id),
    source_payload_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Suppression is an audited non-delivery outcome, never a fabricated SENT or
-- DELIVERY_UNKNOWN receipt. The immutable event and sealed message are kept.
CREATE TABLE IF NOT EXISTS v20.v245_delivery_suppressions (
    event_id TEXT PRIMARY KEY REFERENCES v20.outbox_events(event_id),
    reason TEXT NOT NULL,
    latest_intent_id TEXT,
    audit_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS v20.v245_policy_days (
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    signal_status TEXT NOT NULL CHECK (signal_status IN ('KNOWN','UNKNOWN')),
    raw_open BOOLEAN,
    state_before_hash CHAR(64) NOT NULL,
    state_before_json JSONB NOT NULL,
    transition_hash CHAR(64) NOT NULL,
    transition_json JSONB NOT NULL,
    first_event_id TEXT NOT NULL REFERENCES v20.outbox_events(event_id),
    effective_event_id TEXT NOT NULL REFERENCES v20.outbox_events(event_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (official_stream_id,lineage_id,trade_date),
    CHECK ((signal_status='KNOWN' AND raw_open IS NOT NULL)
        OR (signal_status='UNKNOWN' AND raw_open IS NULL))
);
