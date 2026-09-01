-- Durable at-most-once V20 Feishu dispatch boundary.
-- This migration is deliberately separate from the historical 001 baseline.

SET lock_timeout = '3s';

DO $v20_outbox_at_most_once$
DECLARE
    migration_checksum text := 'b37b755347ba6e25b4f66515359579487f512edecef56f2fc470ae57fc73beef';
    clean_status_definition text;
    clean_lease_definition text;
    preserved_attempt_definition text;
    rejected_status_definition text;
    rejected_lease_definition text;
    clean_status_name text;
    clean_lease_name text;
    rejected_status_name text;
    rejected_lease_name text;
    matching_constraints int;
    rogue_constraints int;
    clean_contract_valid boolean;
    rejected_contract_valid boolean;
    index_catalog_matches int;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('v20:outbox_at_most_once:002', 0)
    );

    CREATE TABLE IF NOT EXISTS v20.migration_receipts (
        version TEXT PRIMARY KEY,
        checksum TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS uq_v20_migration_receipt_version
        ON v20.migration_receipts(version);

    IF EXISTS (
        SELECT 1
        FROM v20.migration_receipts
        WHERE version = '002_outbox_at_most_once'
          AND checksum <> migration_checksum
    ) THEN
        RAISE EXCEPTION
            'V20 migration receipt checksum mismatch for 002_outbox_at_most_once';
    END IF;
    IF EXISTS (
        SELECT 1 FROM v20.migration_receipts WHERE version = '002_outbox_at_most_once'
    ) THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE _v20_001_delivery_reference (
        delivery_status TEXT,
        lease_owner TEXT,
        lease_until TIMESTAMPTZ,
        attempt_count INTEGER,
        CONSTRAINT v20_001_expected_status CHECK (
            delivery_status IN ('PENDING','LEASED','SENT')
        ),
        CONSTRAINT v20_001_expected_lease CHECK (
            (delivery_status='LEASED' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
            OR (delivery_status<>'LEASED' AND lease_owner IS NULL AND lease_until IS NULL)
        ),
        CONSTRAINT v20_001_expected_attempt_count CHECK (attempt_count >= 0)
    );
    CREATE TEMP TABLE _v20_rejected_delivery_reference (
       delivery_status TEXT,
       lease_owner TEXT,
       lease_until TIMESTAMPTZ,
       CONSTRAINT v20_rejected_expected_status CHECK (
           delivery_status IN ('PENDING','LEASED','DELIVERY_UNKNOWN','SENT')
       ),
       CONSTRAINT v20_rejected_expected_lease CHECK (
           (delivery_status IN ('LEASED','DELIVERY_UNKNOWN')
               AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
           OR (delivery_status NOT IN ('LEASED','DELIVERY_UNKNOWN')
               AND lease_owner IS NULL AND lease_until IS NULL)
       )
    );

    SELECT pg_get_constraintdef(oid) INTO clean_status_definition
    FROM pg_constraint
    WHERE conrelid = '_v20_001_delivery_reference'::regclass
      AND conname = 'v20_001_expected_status';
    SELECT pg_get_constraintdef(oid) INTO clean_lease_definition
    FROM pg_constraint
    WHERE conrelid = '_v20_001_delivery_reference'::regclass
      AND conname = 'v20_001_expected_lease';
    SELECT pg_get_constraintdef(oid) INTO preserved_attempt_definition
    FROM pg_constraint
    WHERE conrelid = '_v20_001_delivery_reference'::regclass
      AND conname = 'v20_001_expected_attempt_count';
    SELECT pg_get_constraintdef(oid) INTO rejected_status_definition
    FROM pg_constraint
    WHERE conrelid = '_v20_rejected_delivery_reference'::regclass
      AND conname = 'v20_rejected_expected_status';
    SELECT pg_get_constraintdef(oid) INTO rejected_lease_definition
    FROM pg_constraint
    WHERE conrelid = '_v20_rejected_delivery_reference'::regclass
      AND conname = 'v20_rejected_expected_lease';

    SELECT min(conname), count(*) INTO clean_status_name, matching_constraints
    FROM pg_constraint
    WHERE conrelid = 'v20.outbox_events'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) = clean_status_definition;
    clean_contract_valid := matching_constraints = 1;

    SELECT min(conname), count(*) INTO clean_lease_name, matching_constraints
    FROM pg_constraint
    WHERE conrelid = 'v20.outbox_events'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) = clean_lease_definition;
    clean_contract_valid := clean_contract_valid AND matching_constraints = 1;

    SELECT count(*) INTO matching_constraints
    FROM pg_constraint
    WHERE conrelid = 'v20.outbox_events'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) = preserved_attempt_definition;
    clean_contract_valid := clean_contract_valid AND matching_constraints = 1;

    SELECT min(conname), count(*) INTO rejected_status_name, matching_constraints
    FROM pg_constraint
    WHERE conrelid = 'v20.outbox_events'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) = rejected_status_definition;
    rejected_contract_valid := matching_constraints = 1;

    SELECT min(conname), count(*) INTO rejected_lease_name, matching_constraints
    FROM pg_constraint
    WHERE conrelid = 'v20.outbox_events'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) = rejected_lease_definition;
    rejected_contract_valid := rejected_contract_valid AND matching_constraints = 1;

    SELECT count(*) INTO matching_constraints
    FROM pg_constraint
    WHERE conrelid = 'v20.outbox_events'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) = preserved_attempt_definition;
    rejected_contract_valid := rejected_contract_valid AND matching_constraints = 1;

    IF clean_contract_valid THEN
       SELECT count(*) INTO rogue_constraints
       FROM pg_constraint
       WHERE conrelid = 'v20.outbox_events'::regclass
         AND contype = 'c'
         AND (
             pg_get_constraintdef(oid) ~
                 '(delivery_status|lease_owner|lease_until|delivered_at)'
             OR pg_get_constraintdef(oid) ~
                 '(^|[^[:alnum:]_])attempt_count([^[:alnum:]_]|$)'
         )
         AND pg_get_constraintdef(oid) NOT IN (
             clean_status_definition,
             clean_lease_definition,
             preserved_attempt_definition
         );
       IF rogue_constraints <> 0 THEN
           RAISE EXCEPTION
               'unknown V20 outbox delivery constraints: % rogue definition(s)',
               rogue_constraints;
       END IF;
       IF clean_status_name IS NULL OR clean_lease_name IS NULL THEN
           RAISE EXCEPTION
               'V20 clean delivery constraint names were not resolved';
       END IF;
       EXECUTE format(
           'ALTER TABLE %s DROP CONSTRAINT %I, DROP CONSTRAINT %I',
           'v20.outbox_events',
           clean_status_name,
           clean_lease_name
       );
    ELSIF rejected_contract_valid THEN
       SELECT count(*) INTO rogue_constraints
       FROM pg_constraint
       WHERE conrelid = 'v20.outbox_events'::regclass
         AND contype = 'c'
         AND (
             pg_get_constraintdef(oid) ~
                 '(delivery_status|lease_owner|lease_until|delivered_at)'
             OR pg_get_constraintdef(oid) ~
                 '(^|[^[:alnum:]_])attempt_count([^[:alnum:]_]|$)'
         )
         AND pg_get_constraintdef(oid) NOT IN (
             rejected_status_definition,
             rejected_lease_definition,
             preserved_attempt_definition
         );
       IF rogue_constraints <> 0 THEN
           RAISE EXCEPTION
               'unknown V20 outbox delivery constraints: % rogue definition(s)',
               rogue_constraints;
       END IF;
       IF rejected_status_name IS NULL OR rejected_lease_name IS NULL THEN
           RAISE EXCEPTION
               'V20 rejected delivery constraint names were not resolved';
       END IF;
       EXECUTE format(
           'ALTER TABLE %s DROP CONSTRAINT %I, DROP CONSTRAINT %I',
           'v20.outbox_events',
           rejected_status_name,
           rejected_lease_name
       );
    ELSE
       RAISE EXCEPTION
           'unknown V20 outbox delivery constraint definitions';
    END IF;

    DROP TABLE _v20_001_delivery_reference;
    DROP TABLE _v20_rejected_delivery_reference;

    CREATE TABLE IF NOT EXISTS v20.delivery_quarantine (
        event_id TEXT PRIMARY KEY,
        original_delivery_status TEXT NOT NULL,
        original_attempt_count INTEGER NOT NULL,
        reason TEXT NOT NULL,
        migrated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );

    ALTER TABLE v20.delivery_attempts
        ADD COLUMN IF NOT EXISTS phase TEXT;
    ALTER TABLE v20.delivery_attempts
        ADD COLUMN IF NOT EXISTS worker_id TEXT;
    ALTER TABLE v20.delivery_attempts
        ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
    ALTER TABLE v20.delivery_attempts
        ADD COLUMN IF NOT EXISTS delivery_variant TEXT;

    INSERT INTO v20.delivery_quarantine (
        event_id, original_delivery_status, original_attempt_count, reason
    )
    SELECT
        event_id,
        delivery_status,
        attempt_count,
        CASE
            WHEN delivery_status = 'LEASED' THEN 'LEGACY_LEASE_WITHOUT_DISPATCH_PROOF'
            WHEN delivery_status = 'DELIVERY_UNKNOWN' THEN 'LEGACY_UNKNOWN'
            WHEN delivery_status = 'SENT' AND delivered_at IS NULL THEN 'SENT_WITHOUT_RECEIPT_TIME'
            WHEN attempt_count > 0 THEN 'LEGACY_PENDING_AFTER_ATTEMPT'
            WHEN EXISTS (
                SELECT 1 FROM v20.delivery_attempts AS attempt
                WHERE attempt.event_id = outbox.event_id
            ) THEN 'LEGACY_PENDING_WITH_ATTEMPT_EVIDENCE'
            ELSE 'UNCLASSIFIED_LEGACY_DELIVERY_EVIDENCE'
        END
    FROM v20.outbox_events AS outbox
    WHERE delivery_status IN ('LEASED', 'DELIVERY_UNKNOWN')
       OR (delivery_status = 'SENT' AND delivered_at IS NULL)
       OR (
            delivery_status = 'PENDING'
            AND (
                attempt_count > 0
                OR EXISTS (
                    SELECT 1 FROM v20.delivery_attempts AS attempt
                    WHERE attempt.event_id = outbox.event_id
                )
            )
       )
    ON CONFLICT (event_id) DO NOTHING;

    UPDATE v20.outbox_events
    SET delivery_status = 'DELIVERY_UNKNOWN',
        lease_owner = NULL,
        lease_until = NULL
    WHERE event_id IN (SELECT event_id FROM v20.delivery_quarantine);

    ALTER TABLE v20.delivery_attempts
        ALTER COLUMN succeeded DROP NOT NULL;

    -- Old success rows are the only historically proved deliveries. Their
    -- completion timestamp is the real migration adjudication time; no historical
    -- send time is invented. Everything else is conservatively unknown.
    UPDATE v20.delivery_attempts AS attempt
    SET phase = CASE WHEN attempt.succeeded THEN 'DELIVERED' ELSE 'UNKNOWN' END,
        succeeded = CASE WHEN attempt.succeeded THEN TRUE ELSE NULL END,
        worker_id = COALESCE(attempt.worker_id, 'legacy-migration'),
        completed_at = COALESCE(attempt.completed_at, statement_timestamp()),
        error_text = CASE
            WHEN attempt.succeeded THEN NULL
            ELSE 'legacy delivery outcome unknown'
        END,
        delivery_variant = COALESCE(attempt.delivery_variant, 'LEGACY_UNKNOWN')
    WHERE phase IS DISTINCT FROM (
              CASE WHEN attempt.succeeded THEN 'DELIVERED' ELSE 'UNKNOWN' END
          )
       OR worker_id IS NULL
       OR completed_at IS NULL
       OR error_text IS DISTINCT FROM (
              CASE WHEN attempt.succeeded THEN NULL
                   ELSE 'legacy delivery outcome unknown' END
          )
       OR delivery_variant IS NULL;

    ALTER TABLE v20.outbox_events
        ADD CONSTRAINT ck_v20_outbox_delivery_status_v2
        CHECK (delivery_status IN ('PENDING','LEASED','DELIVERY_UNKNOWN','SENT')) NOT VALID;
    ALTER TABLE v20.outbox_events
        ADD CONSTRAINT ck_v20_outbox_delivery_lease_v2 CHECK (
            (delivery_status = 'LEASED'
                AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
            OR (delivery_status IN ('PENDING','SENT')
                AND lease_owner IS NULL AND lease_until IS NULL)
            OR (delivery_status = 'DELIVERY_UNKNOWN'
                AND (
                    (lease_owner IS NULL AND lease_until IS NULL)
                    OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL)
                ))
        ) NOT VALID;
    ALTER TABLE v20.outbox_events
        ADD CONSTRAINT ck_v20_outbox_delivery_sent_v2 CHECK (
            (delivery_status = 'SENT') = (delivered_at IS NOT NULL)
        ) NOT VALID;

    UPDATE v20.outbox_events
    SET delivered_at = clock_timestamp()
    WHERE delivery_status = 'SENT' AND delivered_at IS NULL;
    UPDATE v20.outbox_events
    SET delivered_at = NULL
    WHERE delivery_status <> 'SENT' AND delivered_at IS NOT NULL;

    ALTER TABLE v20.delivery_attempts
        ADD CONSTRAINT ck_v20_delivery_attempt_number_v2 CHECK (attempt_number > 0) NOT VALID;
    ALTER TABLE v20.delivery_attempts
        ADD CONSTRAINT ck_v20_delivery_attempt_phase_required_v2
        CHECK (phase IS NOT NULL) NOT VALID;
    ALTER TABLE v20.delivery_attempts
        ADD CONSTRAINT ck_v20_delivery_attempt_worker_required_v2
        CHECK (worker_id IS NOT NULL) NOT VALID;
    ALTER TABLE v20.delivery_attempts
        ADD CONSTRAINT ck_v20_delivery_attempt_phase_v2 CHECK (
            (phase = 'STARTED'
                AND succeeded IS NULL AND completed_at IS NULL AND error_text IS NULL)
            OR (phase = 'DELIVERED'
                AND succeeded IS TRUE AND completed_at IS NOT NULL AND error_text IS NULL)
            OR (phase = 'SAFE_RETRY'
                AND succeeded IS FALSE AND completed_at IS NOT NULL AND error_text IS NOT NULL)
            OR (phase = 'UNKNOWN'
                AND succeeded IS NULL AND completed_at IS NOT NULL AND error_text IS NOT NULL)
        ) NOT VALID;
    ALTER TABLE v20.delivery_attempts
        ADD CONSTRAINT ck_v20_delivery_attempt_completion_v2
        CHECK (completed_at IS NULL OR completed_at >= attempted_at) NOT VALID;
    ALTER TABLE v20.delivery_attempts
        ADD CONSTRAINT ck_v20_delivery_attempt_variant_v2 CHECK (
            delivery_variant IN (
                'PRIMARY','ACTIONABLE','EXPIRED_NOTICE','RELAY_ENFORCED','LEGACY_UNKNOWN'
            )
        ) NOT VALID;
    ALTER TABLE v20.delivery_attempts
        ADD CONSTRAINT ck_v20_delivery_attempt_variant_required_v2
        CHECK (delivery_variant IS NOT NULL) NOT VALID;

    CREATE FUNCTION v20.enforce_v20_outbox_delivery_lease_v2()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $v20_outbox_lease_trigger$
    BEGIN
        IF NEW.delivery_status = 'LEASED'
            AND (NEW.lease_owner IS NULL OR NEW.lease_until IS NULL) THEN
            RAISE EXCEPTION 'LEASED V20 outbox event requires a complete lease';
        END IF;
        IF NEW.delivery_status IN ('PENDING','SENT')
            AND (NEW.lease_owner IS NOT NULL OR NEW.lease_until IS NOT NULL) THEN
            RAISE EXCEPTION 'terminal/retryable V20 outbox event cannot retain a lease';
        END IF;
        IF NEW.delivery_status = 'DELIVERY_UNKNOWN'
           AND (NEW.lease_owner IS NULL) <> (NEW.lease_until IS NULL) THEN
            RAISE EXCEPTION
                'DELIVERY_UNKNOWN V20 outbox event has a half lease';
        END IF;
        IF NEW.delivery_status = 'DELIVERY_UNKNOWN' AND NEW.lease_owner IS NOT NULL THEN
            IF NOT EXISTS (
                SELECT 1
                    FROM v20.delivery_attempts AS attempt
                    WHERE attempt.event_id = NEW.event_id
                      AND attempt.phase = 'STARTED'
                      AND attempt.worker_id = NEW.lease_owner
                      AND attempt.attempt_number = NEW.attempt_count
               ) THEN
                RAISE EXCEPTION
                    'DELIVERY_UNKNOWN V20 outbox lease lacks an active STARTED attempt';
            END IF;
        END IF;
        RETURN NEW;
    END;
    $v20_outbox_lease_trigger$;

    CREATE FUNCTION v20.enforce_v20_delivery_attempt_identity_v2()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $v20_delivery_attempt_identity_trigger$
    BEGIN
        IF NEW.event_id <> OLD.event_id
           OR NEW.attempt_number <> OLD.attempt_number THEN
            RAISE EXCEPTION
                'V20 delivery attempt event_id/attempt_number is immutable';
        END IF;
        RETURN NEW;
    END;
    $v20_delivery_attempt_identity_trigger$;

    DROP TRIGGER IF EXISTS trg_v20_delivery_attempt_identity_v2
        ON v20.delivery_attempts;
    CREATE TRIGGER trg_v20_delivery_attempt_identity_v2
        BEFORE UPDATE OF event_id, attempt_number
        ON v20.delivery_attempts
        FOR EACH ROW
        EXECUTE FUNCTION v20.enforce_v20_delivery_attempt_identity_v2();

    DROP TRIGGER IF EXISTS trg_v20_outbox_delivery_lease_v2
        ON v20.outbox_events;
    CREATE TRIGGER trg_v20_outbox_delivery_lease_v2
        BEFORE INSERT OR UPDATE OF delivery_status, lease_owner, lease_until
        ON v20.outbox_events
        FOR EACH ROW
        EXECUTE FUNCTION v20.enforce_v20_outbox_delivery_lease_v2();

    CREATE FUNCTION v20.enforce_v20_outbox_attempt_count_v2()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $v20_outbox_attempt_count_trigger$
    BEGIN
        IF NEW.attempt_count <> OLD.attempt_count AND NOT (
            OLD.delivery_status = 'LEASED'
            AND NEW.delivery_status = 'DELIVERY_UNKNOWN'
            AND NEW.attempt_count = OLD.attempt_count + 1
            AND OLD.lease_owner IS NOT NULL
            AND NEW.lease_owner = OLD.lease_owner
            AND EXISTS (
                SELECT 1
                FROM v20.delivery_attempts AS attempt
                WHERE attempt.event_id = NEW.event_id
                  AND attempt.phase = 'STARTED'
                  AND attempt.worker_id = NEW.lease_owner
                  AND attempt.attempt_number = NEW.attempt_count
            )
        ) THEN
            RAISE EXCEPTION
                'V20 attempt_count may only advance through the dispatch CAS';
        END IF;
        RETURN NEW;
    END;
    $v20_outbox_attempt_count_trigger$;

    DROP TRIGGER IF EXISTS trg_v20_outbox_attempt_count_v2
        ON v20.outbox_events;
    CREATE TRIGGER trg_v20_outbox_attempt_count_v2
        BEFORE UPDATE OF attempt_count
        ON v20.outbox_events
        FOR EACH ROW
        EXECUTE FUNCTION v20.enforce_v20_outbox_attempt_count_v2();

    CREATE UNIQUE INDEX IF NOT EXISTS uq_v20_delivery_attempt_started
        ON v20.delivery_attempts(event_id)
        WHERE phase = 'STARTED';
    CREATE INDEX IF NOT EXISTS idx_v20_outbox_ready_v2
        ON v20.outbox_events(delivery_status, seal_status, available_at)
        WHERE seal_status = 'SEALED' AND delivery_status = 'PENDING';
    CREATE INDEX IF NOT EXISTS idx_v20_outbox_scope_ready_v2
        ON v20.outbox_events(
            route_id, official_stream_id, lineage_id, available_at, created_at, event_id
        )
        WHERE seal_status = 'SEALED' AND delivery_status = 'PENDING';
    CREATE INDEX IF NOT EXISTS idx_v20_outbox_unknown_v2
        ON v20.outbox_events(
            route_id, official_stream_id, lineage_id, created_at, event_id
        )
        WHERE seal_status = 'SEALED' AND delivery_status = 'DELIVERY_UNKNOWN';

    CREATE TEMP TABLE _v20_index_reference (
        event_id TEXT,
        attempt_number INTEGER,
        delivery_status TEXT,
        seal_status TEXT,
        available_at TIMESTAMPTZ,
        route_id TEXT,
        official_stream_id TEXT,
        lineage_id TEXT,
        created_at TIMESTAMPTZ,
        phase TEXT
    ) ON COMMIT DROP;
    CREATE UNIQUE INDEX _v20_reference_started
        ON _v20_index_reference(event_id)
        WHERE phase = 'STARTED';
    CREATE INDEX _v20_reference_ready
        ON _v20_index_reference(delivery_status, seal_status, available_at)
        WHERE seal_status = 'SEALED' AND delivery_status = 'PENDING';
    CREATE INDEX _v20_reference_scope_ready
        ON _v20_index_reference(
            route_id, official_stream_id, lineage_id, available_at, created_at, event_id
        )
        WHERE seal_status = 'SEALED' AND delivery_status = 'PENDING';
    CREATE INDEX _v20_reference_unknown
        ON _v20_index_reference(
            route_id, official_stream_id, lineage_id, created_at, event_id
        )
        WHERE seal_status = 'SEALED' AND delivery_status = 'DELIVERY_UNKNOWN';

    CREATE TEMP TABLE _v20_expected_index_pairs (
        reference_name TEXT,
        actual_name TEXT,
        actual_table REGCLASS
    ) ON COMMIT DROP;
    INSERT INTO _v20_expected_index_pairs
        (reference_name,actual_name,actual_table)
    VALUES
        (
            '_v20_reference_started',
            'uq_v20_delivery_attempt_started',
            'v20.delivery_attempts'::regclass
        ),
        (
            '_v20_reference_ready',
            'idx_v20_outbox_ready_v2',
            'v20.outbox_events'::regclass
        ),
        (
            '_v20_reference_scope_ready',
            'idx_v20_outbox_scope_ready_v2',
            'v20.outbox_events'::regclass
        ),
        (
            '_v20_reference_unknown',
            'idx_v20_outbox_unknown_v2',
            'v20.outbox_events'::regclass
        );

    IF (SELECT count(*) FROM _v20_expected_index_pairs) <> 4 THEN
        RAISE EXCEPTION
            'V20 outbox migration index contract must contain exactly four entries';
    END IF;

    SELECT count(*) INTO index_catalog_matches
    FROM _v20_expected_index_pairs AS expected
    JOIN pg_class AS reference_class
      ON reference_class.relname = expected.reference_name
    JOIN pg_index AS reference_index
      ON reference_index.indexrelid = reference_class.oid
    JOIN pg_class AS actual_class
      ON actual_class.relname = expected.actual_name
    JOIN pg_index AS actual_index
      ON actual_index.indexrelid = actual_class.oid
    JOIN LATERAL (
        SELECT array_agg(attribute.attname ORDER BY key_position.ord) AS key_columns
        FROM unnest(reference_index.indkey::smallint[])
            WITH ORDINALITY AS key_position(attnum, ord)
        JOIN pg_attribute AS attribute
          ON attribute.attrelid = reference_index.indrelid
         AND attribute.attnum = key_position.attnum
    ) AS reference_keys ON true
    JOIN LATERAL (
        SELECT array_agg(attribute.attname ORDER BY key_position.ord) AS key_columns
        FROM unnest(actual_index.indkey::smallint[])
            WITH ORDINALITY AS key_position(attnum, ord)
        JOIN pg_attribute AS attribute
          ON attribute.attrelid = actual_index.indrelid
         AND attribute.attnum = key_position.attnum
    ) AS actual_keys ON true
    WHERE reference_class.relnamespace = pg_my_temp_schema()
      AND actual_index.indrelid = expected.actual_table
      AND reference_class.relam = actual_class.relam
      AND reference_index.indisunique = actual_index.indisunique
      AND reference_index.indisprimary = actual_index.indisprimary
      AND reference_index.indisexclusion = actual_index.indisexclusion
      AND reference_index.indisvalid
      AND reference_index.indisready
      AND actual_index.indisvalid
      AND actual_index.indisready
      AND reference_keys.key_columns = actual_keys.key_columns
      AND reference_index.indclass = actual_index.indclass
      AND reference_index.indcollation = actual_index.indcollation
      AND reference_index.indoption = actual_index.indoption
      AND reference_index.indnatts = actual_index.indnatts
      AND reference_index.indnkeyatts = actual_index.indnkeyatts
      AND pg_get_expr(reference_index.indpred, reference_index.indrelid)
          IS NOT DISTINCT FROM
          pg_get_expr(actual_index.indpred, actual_index.indrelid)
      AND pg_get_expr(reference_index.indexprs, reference_index.indrelid)
          IS NOT DISTINCT FROM
          pg_get_expr(actual_index.indexprs, actual_index.indrelid);

    IF index_catalog_matches
       <> (SELECT count(*) FROM _v20_expected_index_pairs) THEN
        RAISE EXCEPTION
            'V20 outbox index definition catalog mismatch for 002';
    END IF;

    DROP TABLE _v20_index_reference;

    ALTER TABLE v20.outbox_events VALIDATE CONSTRAINT
        ck_v20_outbox_delivery_status_v2;
    ALTER TABLE v20.outbox_events VALIDATE CONSTRAINT
        ck_v20_outbox_delivery_lease_v2;
    ALTER TABLE v20.outbox_events VALIDATE CONSTRAINT
        ck_v20_outbox_delivery_sent_v2;
    ALTER TABLE v20.delivery_attempts VALIDATE CONSTRAINT
        ck_v20_delivery_attempt_number_v2;
    ALTER TABLE v20.delivery_attempts VALIDATE CONSTRAINT
        ck_v20_delivery_attempt_phase_v2;
    ALTER TABLE v20.delivery_attempts VALIDATE CONSTRAINT
        ck_v20_delivery_attempt_completion_v2;
    ALTER TABLE v20.delivery_attempts VALIDATE CONSTRAINT
        ck_v20_delivery_attempt_variant_v2;
    ALTER TABLE v20.delivery_attempts VALIDATE CONSTRAINT
        ck_v20_delivery_attempt_phase_required_v2;
    ALTER TABLE v20.delivery_attempts VALIDATE CONSTRAINT
        ck_v20_delivery_attempt_worker_required_v2;
    ALTER TABLE v20.delivery_attempts VALIDATE CONSTRAINT
        ck_v20_delivery_attempt_variant_required_v2;

    INSERT INTO v20.migration_receipts(version, checksum)
    VALUES ('002_outbox_at_most_once', migration_checksum);
END
$v20_outbox_at_most_once$;
