-- V20 default production schema.  The runtime repository renders the same DDL
-- with the validated database.v20.schema value from database-config.yaml.
CREATE SCHEMA IF NOT EXISTS v20;
CREATE SEQUENCE IF NOT EXISTS v20.commit_marker_seq;

CREATE TABLE IF NOT EXISTS v20.runtime_configs (
    config_id TEXT PRIMARY KEY,
    config_hash CHAR(64) NOT NULL UNIQUE,
    strategy_version TEXT NOT NULL,
    deployment_mode TEXT NOT NULL,
    effective_trade_date DATE NOT NULL,
    config_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS v20.official_state (
    lineage_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    state_hash CHAR(64) NOT NULL,
    state_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS v20.state_lineage_registry (
    lineage_id TEXT PRIMARY KEY,
    official_stream_id TEXT NOT NULL,
    genesis_state_hash CHAR(64) NOT NULL,
    state_semantics_hash CHAR(64) NOT NULL,
    bootstrap_mode TEXT NOT NULL
        CHECK (bootstrap_mode IN ('EMPTY_FORWARD_SHADOW','CHECKPOINT')),
    bootstrap_checkpoint_hash CHAR(64),
    bootstrap_predecessor_trade_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (bootstrap_mode='EMPTY_FORWARD_SHADOW' AND bootstrap_checkpoint_hash IS NULL)
        OR (bootstrap_mode='CHECKPOINT' AND bootstrap_checkpoint_hash IS NOT NULL)
    )
);

ALTER TABLE v20.state_lineage_registry
    ADD COLUMN IF NOT EXISTS bootstrap_predecessor_trade_date DATE;
ALTER TABLE v20.state_lineage_registry
    ADD COLUMN IF NOT EXISTS state_semantics_hash CHAR(64);
UPDATE v20.state_lineage_registry
SET bootstrap_predecessor_trade_date=
    (created_at AT TIME ZONE 'Asia/Shanghai')::date - 1
WHERE bootstrap_predecessor_trade_date IS NULL
  AND bootstrap_mode='EMPTY_FORWARD_SHADOW';

CREATE TABLE IF NOT EXISTS v20.decision_slots (
    official_stream_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    slot_id TEXT NOT NULL UNIQUE,
    strategy_version TEXT NOT NULL,
    config_id TEXT NOT NULL REFERENCES v20.runtime_configs(config_id),
    config_hash CHAR(64) NOT NULL,
    lineage_id TEXT NOT NULL,
    slot_status TEXT NOT NULL CHECK (slot_status IN ('OPEN','COMPLETED','FAILED')),
    slot_revision BIGINT NOT NULL DEFAULT 0,
    terminal_event_id TEXT,
    terminal_decision_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (official_stream_id, trade_date)
);

CREATE TABLE IF NOT EXISTS v20.input_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    snapshot_type TEXT NOT NULL,
    trade_date DATE NOT NULL,
    snapshot_hash CHAR(64) NOT NULL,
    snapshot_json JSONB NOT NULL,
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (snapshot_type, trade_date, snapshot_hash)
);

CREATE TABLE IF NOT EXISTS v20.entry_decisions (
    decision_id TEXT PRIMARY KEY,
    slot_id TEXT NOT NULL UNIQUE REFERENCES v20.decision_slots(slot_id),
    event_id TEXT NOT NULL UNIQUE,
    snapshot_id TEXT NOT NULL REFERENCES v20.input_snapshots(snapshot_id),
    action TEXT NOT NULL CHECK (action IN ('ENTER','BLOCK','NO_SIGNAL','INPUT_INVALID')),
    final_multiplier DOUBLE PRECISION NOT NULL
        CHECK (final_multiplier >= 0 AND final_multiplier <= 1),
    semantic_content_hash CHAR(64) NOT NULL,
    commit_fingerprint CHAR(64) NOT NULL,
    semantic_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS v20.shadow_batches (
    batch_id TEXT PRIMARY KEY,
    decision_id TEXT REFERENCES v20.entry_decisions(decision_id),
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    source_batch_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('HEALTH','ROLLING7')),
    signal_date DATE NOT NULL,
    t2_date DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','COMPLETE_VALID','COMPLETE_INVALID')),
    batch_json JSONB NOT NULL,
    batch_return DOUBLE PRECISION,
    reference_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (reference_status IN ('PENDING','LOCKED','UNAVAILABLE')),
    reference_prices_json JSONB,
    reference_snapshot_hash CHAR(64),
    reference_locked_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    UNIQUE (decision_id, kind),
    CHECK (
        (reference_status='PENDING' AND reference_prices_json IS NULL
            AND reference_snapshot_hash IS NULL AND reference_locked_at IS NULL)
        OR (reference_status='LOCKED' AND reference_prices_json IS NOT NULL
            AND reference_snapshot_hash IS NOT NULL AND reference_locked_at IS NOT NULL)
        OR (reference_status='UNAVAILABLE' AND reference_prices_json IS NULL
            AND reference_snapshot_hash IS NOT NULL AND reference_locked_at IS NOT NULL)
    ),
    CHECK (
        (status='PENDING' AND completed_at IS NULL)
        OR (status='COMPLETE_VALID' AND batch_return IS NOT NULL AND completed_at IS NOT NULL)
        OR (status='COMPLETE_INVALID' AND batch_return IS NULL AND completed_at IS NOT NULL)
    )
);

ALTER TABLE v20.shadow_batches
    ADD COLUMN IF NOT EXISTS official_stream_id TEXT;
ALTER TABLE v20.shadow_batches
    ADD COLUMN IF NOT EXISTS lineage_id TEXT;
ALTER TABLE v20.shadow_batches
    ADD COLUMN IF NOT EXISTS source_batch_id TEXT;
UPDATE v20.shadow_batches AS shadow
SET official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id
FROM v20.entry_decisions AS decision,
     v20.decision_slots AS slot
WHERE shadow.decision_id=decision.decision_id
  AND decision.slot_id=slot.slot_id
  AND (shadow.official_stream_id IS NULL OR shadow.lineage_id IS NULL);
ALTER TABLE v20.shadow_batches
    ALTER COLUMN official_stream_id SET NOT NULL;
ALTER TABLE v20.shadow_batches
    ALTER COLUMN lineage_id SET NOT NULL;
ALTER TABLE v20.shadow_batches
    ALTER COLUMN decision_id DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_v20_shadow_scope_maturity
    ON v20.shadow_batches(official_stream_id,lineage_id,t2_date,kind,status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_v20_shadow_source_mapping
    ON v20.shadow_batches(lineage_id,source_batch_id)
    WHERE source_batch_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS v20.model_batches (
    model_batch_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL UNIQUE REFERENCES v20.entry_decisions(decision_id),
    signal_date DATE NOT NULL,
    multiplier DOUBLE PRECISION NOT NULL CHECK (multiplier > 0 AND multiplier <= 1),
    evaluation_only BOOLEAN NOT NULL,
    reference_profile_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS v20.model_legs (
    model_leg_id TEXT PRIMARY KEY,
    model_batch_id TEXT NOT NULL REFERENCES v20.model_batches(model_batch_id),
    code VARCHAR(6) NOT NULL,
    stock_name TEXT NOT NULL,
    rank SMALLINT NOT NULL CHECK (rank > 0),
    relative_weight DOUBLE PRECISION NOT NULL
        CHECK (relative_weight > 0 AND relative_weight <= 1),
    d1 DATE NOT NULL,
    d2 DATE NOT NULL,
    reference_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (reference_status IN ('PENDING','LOCKED','UNAVAILABLE')),
    reference_price DOUBLE PRECISION,
    reference_snapshot_hash CHAR(64),
    reference_locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (model_batch_id, rank),
    UNIQUE (model_batch_id, code),
    CHECK (d2 > d1),
    CHECK (
        (reference_status='PENDING' AND reference_price IS NULL
            AND reference_snapshot_hash IS NULL AND reference_locked_at IS NULL)
        OR (reference_status='LOCKED' AND reference_price > 0
            AND reference_price < 'Infinity'::double precision
            AND reference_snapshot_hash IS NOT NULL
            AND reference_locked_at IS NOT NULL)
        OR (reference_status='UNAVAILABLE' AND reference_price IS NULL
            AND reference_snapshot_hash IS NOT NULL AND reference_locked_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS v20.mews_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    source_trade_date DATE NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_sealed_at TIMESTAMPTZ,
    fast_state TEXT NOT NULL,
    model_version TEXT NOT NULL,
    data_version TEXT NOT NULL,
    content_hash CHAR(64) NOT NULL UNIQUE,
    snapshot_json JSONB NOT NULL
);

ALTER TABLE v20.mews_snapshots
    ADD COLUMN IF NOT EXISTS receipt_sealed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS v20.leg_mews_selection (
    model_leg_id TEXT PRIMARY KEY REFERENCES v20.model_legs(model_leg_id),
    snapshot_id TEXT REFERENCES v20.mews_snapshots(snapshot_id),
    fast_state TEXT,
    cutoff_ts TIMESTAMPTZ NOT NULL,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    selection_reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS v20.exit_intents (
    exit_intent_id TEXT PRIMARY KEY,
    model_leg_id TEXT NOT NULL UNIQUE REFERENCES v20.model_legs(model_leg_id),
    event_id TEXT NOT NULL UNIQUE,
    signal_type TEXT NOT NULL,
    trigger_ts TIMESTAMPTZ NOT NULL,
    rule_actionable_from TIMESTAMPTZ NOT NULL,
    semantic_content_hash CHAR(64) NOT NULL,
    commit_fingerprint CHAR(64) NOT NULL,
    semantic_json JSONB NOT NULL,
    initial_exit_persisted_local_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS v20.outbox_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    route_id TEXT NOT NULL,
    official_stream_id TEXT NOT NULL,
    lineage_id TEXT NOT NULL,
    semantic_content_hash CHAR(64) NOT NULL,
    semantic_json JSONB NOT NULL,
    payload_json JSONB,
    payload_hash CHAR(64),
    seal_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (seal_status IN ('PENDING','SEALED')),
    seal_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (seal_attempt_count >= 0),
    seal_last_attempt_at TIMESTAMPTZ,
    seal_last_error TEXT,
    delivery_status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (delivery_status IN ('PENDING','LEASED','SENT')),
    action_expiry_ts TIMESTAMPTZ,
    generated_at TIMESTAMPTZ,
    commit_marker BIGINT UNIQUE,
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error TEXT,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((seal_status='PENDING' AND payload_json IS NULL AND payload_hash IS NULL)
        OR (seal_status='SEALED' AND payload_json IS NOT NULL AND payload_hash IS NOT NULL
            AND generated_at IS NOT NULL AND commit_marker IS NOT NULL)),
    CHECK ((delivery_status='LEASED' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR (delivery_status<>'LEASED' AND lease_owner IS NULL AND lease_until IS NULL)),
    CHECK (event_type<>'ENTRY_DECISION' OR action_expiry_ts IS NOT NULL)
);

-- Upgrade shared schemas without assigning legacy events to a live worker by
-- guesswork. Relationally bound rows are recovered; anything else is retained
-- in an explicit quarantine scope that no configured runtime may claim.
ALTER TABLE v20.outbox_events
    ADD COLUMN IF NOT EXISTS official_stream_id TEXT;
ALTER TABLE v20.outbox_events
    ADD COLUMN IF NOT EXISTS lineage_id TEXT;
ALTER TABLE v20.outbox_events
    ADD COLUMN IF NOT EXISTS action_expiry_ts TIMESTAMPTZ;
ALTER TABLE v20.outbox_events
    ADD COLUMN IF NOT EXISTS seal_attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE v20.outbox_events
    ADD COLUMN IF NOT EXISTS seal_last_attempt_at TIMESTAMPTZ;
ALTER TABLE v20.outbox_events
    ADD COLUMN IF NOT EXISTS seal_last_error TEXT;
UPDATE v20.outbox_events AS outbox
SET official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id
FROM v20.entry_decisions AS decision
JOIN v20.decision_slots AS slot ON slot.slot_id=decision.slot_id
WHERE outbox.event_id=decision.event_id
  AND (outbox.official_stream_id IS NULL OR outbox.lineage_id IS NULL);
UPDATE v20.outbox_events AS outbox
SET action_expiry_ts=(slot.trade_date + TIME '09:40') AT TIME ZONE 'Asia/Shanghai'
FROM v20.entry_decisions AS decision
JOIN v20.decision_slots AS slot ON slot.slot_id=decision.slot_id
WHERE outbox.event_id=decision.event_id
  AND outbox.event_type='ENTRY_DECISION' AND outbox.action_expiry_ts IS NULL;
UPDATE v20.outbox_events
SET action_expiry_ts=clock_timestamp()
WHERE event_type='ENTRY_DECISION' AND action_expiry_ts IS NULL;
UPDATE v20.outbox_events AS outbox
SET official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id
FROM v20.exit_intents AS intent
JOIN v20.model_legs AS leg ON leg.model_leg_id=intent.model_leg_id
JOIN v20.model_batches AS batch ON batch.model_batch_id=leg.model_batch_id
JOIN v20.entry_decisions AS decision ON decision.decision_id=batch.decision_id
JOIN v20.decision_slots AS slot ON slot.slot_id=decision.slot_id
WHERE outbox.event_id=intent.event_id
  AND (outbox.official_stream_id IS NULL OR outbox.lineage_id IS NULL);
CREATE TABLE IF NOT EXISTS v20.delivery_attempts (
    event_id TEXT NOT NULL REFERENCES v20.outbox_events(event_id),
    attempt_number INTEGER NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    succeeded BOOLEAN NOT NULL,
    error_text TEXT,
    PRIMARY KEY (event_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS v20.exit_reminders (
    reminder_id TEXT PRIMARY KEY,
    exit_intent_id TEXT NOT NULL REFERENCES v20.exit_intents(exit_intent_id),
    original_exit_event_id TEXT NOT NULL,
    reminder_trade_date DATE NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    semantic_content_hash CHAR(64) NOT NULL,
    semantic_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (exit_intent_id, reminder_trade_date)
);

UPDATE v20.outbox_events AS outbox
SET official_stream_id=slot.official_stream_id,
    lineage_id=slot.lineage_id
FROM v20.exit_reminders AS reminder
JOIN v20.exit_intents AS intent ON intent.exit_intent_id=reminder.exit_intent_id
JOIN v20.model_legs AS leg ON leg.model_leg_id=intent.model_leg_id
JOIN v20.model_batches AS batch ON batch.model_batch_id=leg.model_batch_id
JOIN v20.entry_decisions AS decision ON decision.decision_id=batch.decision_id
JOIN v20.decision_slots AS slot ON slot.slot_id=decision.slot_id
WHERE outbox.event_id=reminder.event_id
  AND (outbox.official_stream_id IS NULL OR outbox.lineage_id IS NULL);
UPDATE v20.outbox_events
SET official_stream_id=COALESCE(
        NULLIF(official_stream_id,''),
        NULLIF(semantic_json->>'official_stream_id',''),
        'LEGACY_UNSCOPED'
    ),
    lineage_id=COALESCE(
        NULLIF(lineage_id,''),
        NULLIF(semantic_json->>'state_lineage_id',''),
        'LEGACY_UNSCOPED'
    )
WHERE official_stream_id IS NULL OR official_stream_id=''
   OR lineage_id IS NULL OR lineage_id='';
ALTER TABLE v20.outbox_events
    ALTER COLUMN official_stream_id SET NOT NULL;
ALTER TABLE v20.outbox_events
    ALTER COLUMN lineage_id SET NOT NULL;

CREATE TABLE IF NOT EXISTS v20.minute_bars (
    code VARCHAR(6) NOT NULL,
    bar_end TIMESTAMPTZ NOT NULL,
    end_label CHAR(5) NOT NULL,
    source_hash CHAR(64) NOT NULL,
    bar_json JSONB NOT NULL,
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_sealed_at TIMESTAMPTZ,
    PRIMARY KEY (code, bar_end, source_hash)
);

ALTER TABLE v20.minute_bars
    ADD COLUMN IF NOT EXISTS receipt_sealed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_v20_minute_bar_time_code_label
    ON v20.minute_bars(bar_end,code,end_label)
    WHERE receipt_sealed_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS v20.daily_bar_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    trade_date DATE NOT NULL,
    source_hash CHAR(64) NOT NULL,
    snapshot_json JSONB NOT NULL,
    first_received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_sealed_at TIMESTAMPTZ,
    receipt_sequence BIGSERIAL NOT NULL UNIQUE,
    UNIQUE (trade_date, source_hash)
);

CREATE INDEX IF NOT EXISTS idx_v20_daily_snapshot_receipt
    ON v20.daily_bar_snapshots(trade_date,first_received_at,receipt_sequence);

CREATE TABLE IF NOT EXISTS v20.exit_scan_watermarks (
    model_leg_id TEXT NOT NULL REFERENCES v20.model_legs(model_leg_id),
    trade_date DATE NOT NULL,
    scanned_through_label CHAR(5) NOT NULL,
    source_hash CHAR(64) NOT NULL,
    first_scanned_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (model_leg_id, trade_date),
    CHECK (scanned_through_label ~ '^[0-9]{2}:[0-9]{2}$')
);

CREATE TABLE IF NOT EXISTS v20.reminder_stop_acks (
    ack_id TEXT PRIMARY KEY,
    original_exit_event_id TEXT NOT NULL REFERENCES v20.outbox_events(event_id),
    consumer_id TEXT NOT NULL,
    ack_ts TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    receipt_sealed_at TIMESTAMPTZ,
    auth_evidence_hash CHAR(64) NOT NULL,
    UNIQUE (original_exit_event_id, consumer_id)
);

ALTER TABLE v20.reminder_stop_acks
    ADD COLUMN IF NOT EXISTS receipt_sealed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_v20_outbox_ready
    ON v20.outbox_events(delivery_status, seal_status, available_at);
CREATE INDEX IF NOT EXISTS idx_v20_outbox_scope_ready
    ON v20.outbox_events(
        route_id,official_stream_id,lineage_id,available_at,created_at,event_id
    ) WHERE seal_status='SEALED' AND delivery_status <> 'SENT';
CREATE INDEX IF NOT EXISTS idx_v20_outbox_scope_unsealed
    ON v20.outbox_events(route_id,official_stream_id,lineage_id,event_id)
    WHERE seal_status='PENDING';
CREATE INDEX IF NOT EXISTS idx_v20_slots_scope_date
    ON v20.decision_slots(official_stream_id, lineage_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_v20_legs_dates ON v20.model_legs(d1, d2);
CREATE INDEX IF NOT EXISTS idx_v20_shadow_maturity ON v20.shadow_batches(status, t2_date);
CREATE INDEX IF NOT EXISTS idx_v20_exit_reminder_date
    ON v20.exit_reminders(reminder_trade_date, exit_intent_id);
