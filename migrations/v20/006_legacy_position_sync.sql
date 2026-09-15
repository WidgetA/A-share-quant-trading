-- User-reported holdings are independent of frozen strategy prices and returns.
ALTER TABLE v20.model_legs
    ADD COLUMN IF NOT EXISTS user_position_status TEXT NOT NULL DEFAULT 'UNCONFIRMED'
        CHECK (user_position_status IN ('UNCONFIRMED','MONITORING','CLOSED','NOT_BOUGHT')),
    ADD COLUMN IF NOT EXISTS user_remaining_quantity BIGINT
        CHECK (user_remaining_quantity >= 0),
    ADD COLUMN IF NOT EXISTS user_position_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS user_position_updated_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS v20.legacy_position_calibrations (
    request_id TEXT PRIMARY KEY,
    model_leg_id TEXT NOT NULL REFERENCES v20.model_legs(model_leg_id),
    request_hash CHAR(64) NOT NULL,
    before_json JSONB NOT NULL,
    after_json JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
