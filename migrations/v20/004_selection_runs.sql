-- Every timer/button invocation saves a normal selection result. Daily state
-- remains a once-per-session projection; reruns have independent delivery IDs.
CREATE TABLE IF NOT EXISTS v20.selection_runs (
    run_id TEXT PRIMARY KEY,
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    config_hash CHAR(64) NOT NULL,
    event_id TEXT NOT NULL UNIQUE REFERENCES v20.outbox_events(event_id),
    snapshot_id TEXT NOT NULL REFERENCES v20.input_snapshots(snapshot_id),
    proposal_hash CHAR(64) NOT NULL,
    proposal_json JSONB NOT NULL,
    advanced_daily_state BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_v20_selection_runs_date
    ON v20.selection_runs(official_stream_id,trade_date,created_at);
