-- One inherited prestate per original stream/lineage. Historical slots, policy
-- days, messages, positions and the original official state are never rewritten.
CREATE TABLE IF NOT EXISTS v20.v245_policy_bootstraps (
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    target_trade_date DATE NOT NULL,
    history_until DATE NOT NULL,
    state_hash CHAR(64) NOT NULL,
    audit_hash CHAR(64) NOT NULL,
    audit_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (official_stream_id,lineage_id),
    FOREIGN KEY (official_stream_id,lineage_id)
        REFERENCES v20.v245_policy_state(official_stream_id,lineage_id),
    CHECK (history_until < target_trade_date)
);
