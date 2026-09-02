-- One independent theoretical intent per durable canonical V16 signal date.
CREATE TABLE IF NOT EXISTS v20.rolling7_market_health (
    signal_date DATE PRIMARY KEY,
    canonical_available BOOLEAN NOT NULL,
    canonical_snapshot_id TEXT NOT NULL,
    canonical_snapshot_hash TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    recommendations JSONB NOT NULL,
    t2_date DATE,
    d0_references JSONB NOT NULL DEFAULT '{}'::jsonb,
    d2_closes JSONB NOT NULL DEFAULT '{}'::jsonb,
    batch_return DOUBLE PRECISION,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_rolling7_market_health_kinds
        CHECK (signal_kind IN ('SIGNAL', 'NO_SIGNAL', 'MISSING_CANONICAL')),
    CONSTRAINT ck_rolling7_market_health_status
        CHECK (status IN ('COMPLETE', 'DATA_GAP')),
    CONSTRAINT ck_rolling7_market_health_recommendations_json
        CHECK (jsonb_typeof(recommendations) = 'array'),
    CONSTRAINT ck_rolling7_market_health_d0_references_json
        CHECK (jsonb_typeof(d0_references) = 'object'),
    CONSTRAINT ck_rolling7_market_health_d2_closes_json
        CHECK (jsonb_typeof(d2_closes) = 'object'),
    CONSTRAINT ck_rolling7_market_health_d0_references_positive
        CHECK (
            NOT jsonb_path_exists(
                d0_references,
                '$.* ? (@.type() != "number" || @ <= 0)'
            )
        ),
    CONSTRAINT ck_rolling7_market_health_d2_closes_positive
        CHECK (
            NOT jsonb_path_exists(
                d2_closes,
                '$.* ? (@.type() != "number" || @ <= 0)'
            )
        ),
    CONSTRAINT ck_rolling7_market_health_canonical_shape
        CHECK (
            (NOT canonical_available) = (
                canonical_snapshot_id = ''
                AND canonical_snapshot_hash = ''
                AND signal_kind = 'MISSING_CANONICAL'
                AND recommendations = '[]'::jsonb
                AND status = 'DATA_GAP'
                AND reason = 'MISSING_CANONICAL'
                AND d0_references = '{}'::jsonb
                AND d2_closes = '{}'::jsonb
                AND batch_return IS NULL
            )
        ),
    CONSTRAINT ck_rolling7_market_health_canonical_identity
        CHECK (
            NOT canonical_available
            OR (
                canonical_snapshot_id <> ''
                AND canonical_snapshot_hash ~ '^[0-9a-f]{64}$'
                AND signal_kind IN ('SIGNAL', 'NO_SIGNAL')
            )
        ),
    CONSTRAINT ck_rolling7_market_health_no_signal
        CHECK (
            signal_kind <> 'NO_SIGNAL'
            OR (
                recommendations = '[]'::jsonb
                AND t2_date IS NULL
                AND status = 'COMPLETE'
                AND reason = 'NO_SIGNAL'
                AND d0_references = '{}'::jsonb
                AND d2_closes = '{}'::jsonb
                AND batch_return IS NULL
            )
        ),
    CONSTRAINT ck_rolling7_market_health_signal_recommendations
        CHECK (
            signal_kind <> 'SIGNAL'
            OR jsonb_array_length(recommendations) > 0
        ),
    CONSTRAINT ck_rolling7_market_health_complete_signal
        CHECK (
            NOT (signal_kind = 'SIGNAL' AND status = 'COMPLETE')
            OR (
                t2_date > signal_date
                AND d0_references <> '{}'::jsonb
                AND d2_closes <> '{}'::jsonb
                AND batch_return IS NOT NULL
            )
        ),
    CONSTRAINT ck_rolling7_market_health_data_gap_return
        CHECK (status <> 'DATA_GAP' OR batch_return IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_rolling7_market_health_maturity
    ON v20.rolling7_market_health(t2_date, signal_date);
