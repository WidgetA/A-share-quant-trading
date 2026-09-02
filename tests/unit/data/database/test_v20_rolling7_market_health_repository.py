import inspect
import json
import re
from datetime import date, datetime, timezone

import pytest

from src.data.database.v20_repository import (
    V20DatabaseConfig,
    V20Repository,
    V20SemanticConflict,
)
from src.strategy.v20.rolling7_market_health import (
    BatchStatus,
    CanonicalRecommendation,
    Rolling7Batch,
    Rolling7Leg,
    SignalKind,
    make_batch,
    make_missing_canonical_batch,
)

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)
INSERT_COLUMNS = (
    "signal_date",
    "canonical_available",
    "canonical_snapshot_id",
    "canonical_snapshot_hash",
    "signal_kind",
    "recommendations",
    "t2_date",
    "d0_references",
    "d2_closes",
    "batch_return",
    "status",
    "reason",
    "updated_at",
)
RECOMMENDATIONS = (
    CanonicalRecommendation(rank=1, code="000001"),
    CanonicalRecommendation(rank=2, code="000002"),
)


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class _StatefulConnection:
    def __init__(self, rows=None):
        self.rows = dict(rows or {})
        self.calls = []

    async def fetchrow(self, sql, *args):
        self.calls.append(("fetchrow", sql, args))
        if sql.lstrip().startswith("INSERT"):
            signal_date = args[0]
            existing = self.rows.get(signal_date)
            if existing is not None and not _upsert_permits(existing, args):
                return None
            decoded = _row_args_to_dict(signal_date, args)
            self.rows[signal_date] = decoded
            return _dict_to_row(decoded)
        return _dict_to_row(self.rows.get(args[0])) if args[0] in self.rows else None

    async def fetch(self, sql, *args):
        self.calls.append(("fetch", sql, args))
        decision_date = args[0]
        limit = args[1]
        recent = [
            (signal_date, row)
            for signal_date, row in self.rows.items()
            if row["t2_date"] is not None and row["t2_date"] < decision_date
        ]
        recent = sorted(recent, reverse=True)[:limit]
        return [_dict_to_row(row) for _, row in sorted(recent)]


class _FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _AsyncContext(self.connection)


def _repository(connection):
    repository = V20Repository(V20DatabaseConfig())
    repository._pool = _FakePool(connection)
    return repository


def _batch(
    *,
    signal_date=date(2026, 8, 1),
    t2_date=date(2026, 8, 4),
    recommendations=RECOMMENDATIONS,
    d0_references=None,
    d2_closes=None,
):
    return make_batch(
        signal_date=signal_date,
        canonical_snapshot_id="snapshot-01",
        canonical_snapshot_hash="1" * 64,
        recommendations=recommendations,
        t2_date=t2_date,
        d0_references=d0_references or {},
        d2_closes=d2_closes or {},
    )


def _null_t2_gap_batch(d0_references=None):
    references = d0_references or {}
    return Rolling7Batch(
        signal_date=date(2026, 8, 1),
        canonical_snapshot_id="snapshot-01",
        canonical_snapshot_hash="1" * 64,
        canonical_available=True,
        signal_kind=SignalKind.SIGNAL,
        recommendations=RECOMMENDATIONS,
        t2_date=None,
        legs=tuple(
            Rolling7Leg(
                rank=item.rank,
                code=item.code,
                d0_reference=references.get(item.code),
                d2_close=None,
            )
            for item in RECOMMENDATIONS
        ),
        status=BatchStatus.DATA_GAP,
        reason="INVALID_T2_SESSION",
    )


def _row_args_to_dict(signal_date, args):
    recommendations = tuple(
        CanonicalRecommendation(rank=item["rank"], code=item["code"])
        for item in json.loads(args[5])
    )
    return {
        "signal_date": signal_date,
        "canonical_available": args[1],
        "canonical_snapshot_id": args[2],
        "canonical_snapshot_hash": args[3],
        "signal_kind": args[4],
        "recommendations": recommendations,
        "t2_date": args[6],
        "d0_references": json.loads(args[7]),
        "d2_closes": json.loads(args[8]),
        "batch_return": args[9],
        "status": args[10],
        "reason": args[11],
        "updated_at": args[12],
    }


def _upsert_args(batch):
    recommendations = [{"rank": item.rank, "code": item.code} for item in batch.recommendations]
    return (
        batch.signal_date,
        batch.canonical_available,
        batch.canonical_snapshot_id,
        batch.canonical_snapshot_hash,
        batch.signal_kind.value,
        json.dumps(recommendations),
        batch.t2_date,
        json.dumps({leg.code: leg.d0_reference for leg in batch.legs if leg.d0_reference}),
        json.dumps({leg.code: leg.d2_close for leg in batch.legs if leg.d2_close}),
        batch.batch_return,
        batch.status.value,
        batch.reason,
        NOW,
    )


def _semantic_fields(row):
    return (
        row["canonical_available"],
        row["canonical_snapshot_id"],
        row["canonical_snapshot_hash"],
        row["signal_kind"],
        row["recommendations"],
        row["t2_date"],
        row["d0_references"],
        row["d2_closes"],
        row["batch_return"],
        row["status"],
        row["reason"],
    )


def _contains(existing, incoming):
    return all(item in incoming.items() for item in existing.items())


def _upsert_permits(existing, incoming):
    incoming_row = _row_args_to_dict(incoming[0], incoming)
    if _semantic_fields(existing) == _semantic_fields(incoming_row):
        return True
    if existing["status"] != "DATA_GAP":
        return False
    if existing["canonical_available"] is False:
        if incoming_row["canonical_available"] is False:
            return (
                existing["t2_date"] is None
                and incoming_row["t2_date"] is not None
                and existing["d0_references"] == incoming_row["d0_references"]
                and existing["d2_closes"] == incoming_row["d2_closes"]
                and existing["reason"] == incoming_row["reason"]
            )
        return incoming_row["signal_kind"] == "NO_SIGNAL" or (
            existing["t2_date"] is None or existing["t2_date"] == incoming_row["t2_date"]
        )
    same_identity = (
        existing["canonical_snapshot_id"] == incoming_row["canonical_snapshot_id"]
        and existing["canonical_snapshot_hash"] == incoming_row["canonical_snapshot_hash"]
        and existing["signal_kind"] == incoming_row["signal_kind"]
        and existing["recommendations"] == incoming_row["recommendations"]
    )
    t2_permitted = existing["t2_date"] == incoming_row["t2_date"] or (
        existing["t2_date"] is None and incoming_row["t2_date"] is not None
    )
    evidence_permitted = _contains(
        existing["d0_references"], incoming_row["d0_references"]
    ) and _contains(existing["d2_closes"], incoming_row["d2_closes"])
    if not (same_identity and t2_permitted and evidence_permitted):
        return False
    if incoming_row["status"] == "COMPLETE":
        return True
    return incoming_row["status"] == "DATA_GAP" and (
        (existing["t2_date"] is None and incoming_row["t2_date"] is not None)
        or existing["d0_references"] != incoming_row["d0_references"]
        or existing["d2_closes"] != incoming_row["d2_closes"]
    )


def _dict_to_row(row):
    legs = tuple(
        Rolling7Leg(
            rank=item.rank,
            code=item.code,
            d0_reference=row["d0_references"].get(item.code),
            d2_close=row["d2_closes"].get(item.code),
        )
        for item in row["recommendations"]
    )
    return {
        **row,
        "recommendations": [
            {"rank": item.rank, "code": item.code} for item in row["recommendations"]
        ],
        "legs": legs,
    }


@pytest.mark.asyncio
async def test_missing_placeholder_replays_and_uses_canonical_insert_shape():
    placeholder = make_missing_canonical_batch(
        signal_date=date(2026, 8, 1), t2_date=date(2026, 8, 4)
    )
    connection = _StatefulConnection()

    first = await _repository(connection).save_rolling7_market_health(placeholder, updated_at=NOW)
    replay = await _repository(connection).save_rolling7_market_health(placeholder, updated_at=NOW)
    first_sql, first_args = connection.calls[0][1:]
    insert_columns = first_sql.split("VALUES", 1)[0].split("(", 1)[1]
    assert re.findall(r"[A-Za-z_][A-Za-z0-9_]*", insert_columns) == list(INSERT_COLUMNS)
    values_sql = first_sql.split("VALUES", 1)[1].split("ON CONFLICT", 1)[0]
    assert re.findall(r"\$(\d+)", values_sql) == [str(number) for number in range(1, 14)]
    assert first_args == _upsert_args(placeholder)
    assert first.batch.signal_kind is SignalKind.MISSING_CANONICAL
    assert replay.batch == placeholder


@pytest.mark.asyncio
async def test_known_canonical_evidence_progresses_monotonically_to_complete():
    connection = _StatefulConnection()
    full_d0 = {"000001": 100.0, "000002": 200.0}
    progression = (
        _null_t2_gap_batch(),
        _null_t2_gap_batch(d0_references={"000001": 100.0}),
        _batch(d0_references=full_d0),
        _batch(d0_references=full_d0, d2_closes={"000001": 101.0}),
        _batch(d0_references=full_d0, d2_closes={"000001": 101.0, "000002": 202.0}),
    )
    repository = _repository(connection)

    for expected in progression:
        saved = await repository.save_rolling7_market_health(expected, updated_at=NOW)
        replay = await repository.save_rolling7_market_health(expected, updated_at=NOW)
        assert saved.batch == expected
        assert replay.batch == expected

    assert connection.rows[progression[-1].signal_date]["status"] == "COMPLETE"
    assert connection.rows[progression[-1].signal_date]["reason"] == progression[-1].reason


@pytest.mark.asyncio
async def test_known_canonical_gap_rejects_conflicting_and_removed_evidence():
    full_d0 = {"000001": 100.0, "000002": 200.0}
    partial_d2 = {"000001": 101.0}
    established = _batch(d0_references=full_d0, d2_closes=partial_d2)
    connection = _StatefulConnection()
    await _repository(connection).save_rolling7_market_health(established, updated_at=NOW)
    incoming = [
        _batch(d0_references={"000001": 99.0, "000002": 200.0}),
        _batch(d0_references=full_d0, d2_closes={"000001": 999.0}),
        _batch(d0_references={"000001": 100.0}),
        _batch(d0_references=full_d0, d2_closes={}),
    ]

    for batch in incoming:
        with pytest.raises(V20SemanticConflict, match="exists differently"):
            await _repository(connection).save_rolling7_market_health(batch, updated_at=NOW)


@pytest.mark.asyncio
async def test_known_canonical_partial_d2_grows_to_complete():
    full_d0 = {"000001": 100.0, "000002": 200.0}
    partial = _batch(d0_references=full_d0, d2_closes={"000001": 101.0})
    complete = _batch(d0_references=full_d0, d2_closes={"000001": 101.0, "000002": 202.0})
    connection = _StatefulConnection()
    repository = _repository(connection)

    saved = await repository.save_rolling7_market_health(partial, updated_at=NOW)
    completed = await repository.save_rolling7_market_health(complete, updated_at=NOW)

    assert saved.batch == partial
    assert completed.batch == complete
    assert connection.rows[complete.signal_date]["d2_closes"] == {
        "000001": 101.0,
        "000002": 202.0,
    }


@pytest.mark.asyncio
async def test_known_canonical_gap_grows_evidence_while_t2_remains_null():
    connection = _StatefulConnection()
    repository = _repository(connection)

    await repository.save_rolling7_market_health(_null_t2_gap_batch(), updated_at=NOW)
    partial_d0 = await repository.save_rolling7_market_health(
        _null_t2_gap_batch(d0_references={"000001": 100.0}), updated_at=NOW
    )
    full_d0 = await repository.save_rolling7_market_health(
        _null_t2_gap_batch(d0_references={"000001": 100.0, "000002": 200.0}),
        updated_at=NOW,
    )

    assert partial_d0.batch.t2_date is None
    assert full_d0.batch.t2_date is None
    assert connection.rows[full_d0.batch.signal_date]["d0_references"] == {
        "000001": 100.0,
        "000002": 200.0,
    }


@pytest.mark.asyncio
async def test_established_t2_cannot_change_even_with_evidence_growth():
    established = _batch(
        d0_references={"000001": 100.0, "000002": 200.0},
        d2_closes={"000001": 101.0},
    )
    changed = _batch(
        t2_date=date(2026, 8, 5),
        d0_references={"000001": 100.0, "000002": 200.0},
        d2_closes={"000001": 101.0, "000002": 202.0},
    )
    connection = _StatefulConnection()
    await _repository(connection).save_rolling7_market_health(established, updated_at=NOW)

    with pytest.raises(V20SemanticConflict, match="exists differently"):
        await _repository(connection).save_rolling7_market_health(changed, updated_at=NOW)


@pytest.mark.asyncio
async def test_restart_load_preserves_partial_evidence_then_completion_freezes():
    partial = _batch(
        d0_references={"000001": 100.0, "000002": 200.0},
        d2_closes={"000001": 101.0},
    )
    completed = _batch(
        d0_references={"000001": 100.0, "000002": 200.0},
        d2_closes={"000001": 101.0, "000002": 202.0},
    )
    connection = _StatefulConnection()
    await _repository(connection).save_rolling7_market_health(partial, updated_at=NOW)

    loaded = await _repository(connection).load_rolling7_market_health(
        before_t2=date(2026, 8, 5), limit=1
    )
    frozen = await _repository(connection).save_rolling7_market_health(completed, updated_at=NOW)
    replay = await _repository(connection).save_rolling7_market_health(completed, updated_at=NOW)

    assert loaded == (partial,)
    assert frozen.batch == completed
    assert replay.batch == completed
    with pytest.raises(V20SemanticConflict, match="exists differently"):
        await _repository(connection).save_rolling7_market_health(
            _batch(
                d0_references={"000001": 100.0, "000002": 200.0},
                d2_closes={"000001": 101.0, "000002": 203.0},
            ),
            updated_at=NOW,
        )


@pytest.mark.asyncio
async def test_missing_placeholder_t2_and_canonical_transitions():
    null_t2 = make_missing_canonical_batch(signal_date=date(2026, 8, 1), t2_date=None)
    filled_t2 = make_missing_canonical_batch(signal_date=date(2026, 8, 1), t2_date=date(2026, 8, 4))
    changed_t2 = make_missing_canonical_batch(
        signal_date=date(2026, 8, 1), t2_date=date(2026, 8, 5)
    )
    canonical_gap = _batch(d0_references={"000001": 100.0})
    connection = _StatefulConnection()
    repository = _repository(connection)

    await repository.save_rolling7_market_health(null_t2, updated_at=NOW)
    await repository.save_rolling7_market_health(filled_t2, updated_at=NOW)
    with pytest.raises(V20SemanticConflict, match="exists differently"):
        await repository.save_rolling7_market_health(changed_t2, updated_at=NOW)
    gap = await repository.save_rolling7_market_health(canonical_gap, updated_at=NOW)
    assert gap.batch == canonical_gap


@pytest.mark.asyncio
async def test_load_takes_latest_limit_then_returns_rows_ascending():
    batches = (
        _batch(signal_date=date(2026, 8, 1), t2_date=date(2026, 8, 4)),
        _batch(signal_date=date(2026, 8, 2), t2_date=date(2026, 8, 5)),
        _batch(signal_date=date(2026, 8, 3), t2_date=date(2026, 8, 6)),
    )
    rows = {
        batch.signal_date: _row_args_to_dict(batch.signal_date, _upsert_args(batch))
        for batch in batches
    }
    connection = _StatefulConnection(rows)

    loaded = await _repository(connection).load_rolling7_market_health(
        before_t2=date(2026, 9, 1), limit=2
    )

    assert [batch.signal_date for batch in loaded] == [date(2026, 8, 2), date(2026, 8, 3)]
    assert "t2_date < $1" in connection.calls[0][1]
    assert "ORDER BY signal_date DESC" in connection.calls[0][1]
    assert "ORDER BY recent.signal_date ASC" in connection.calls[0][1]
    assert "LIMIT $2" in connection.calls[0][1]
    assert connection.calls[0][2] == (date(2026, 9, 1), 2)

    signature = inspect.signature(V20Repository.load_rolling7_market_health)
    assert signature.parameters["limit"].default == 1000
    for invalid_before_t2 in ("2026-09-01", datetime(2026, 9, 1, tzinfo=timezone.utc)):
        with pytest.raises(ValueError, match="before_t2 must be a date"):
            await _repository(None).load_rolling7_market_health(before_t2=invalid_before_t2)

    for invalid_limit in (0, -1, 1.0, True, "1"):
        with pytest.raises(ValueError, match="limit must be a positive integer"):
            await _repository(None).load_rolling7_market_health(
                before_t2=date(2026, 9, 1), limit=invalid_limit
            )


@pytest.mark.asyncio
async def test_get_rolling7_market_health_for_date_is_exact_and_strict():
    batch = _batch(d0_references={"000001": 100.0})
    row = _row_args_to_dict(batch.signal_date, _upsert_args(batch))
    repository = _repository(_StatefulConnection({batch.signal_date: row}))

    loaded = await repository.get_rolling7_market_health_for_date(batch.signal_date)
    missing = await repository.get_rolling7_market_health_for_date(date(2026, 8, 2))

    assert loaded == batch
    assert missing is None

    row["d0_references"]["000001"] = float("nan")
    with pytest.raises(V20SemanticConflict, match="market evidence is invalid"):
        await repository.get_rolling7_market_health_for_date(batch.signal_date)
    with pytest.raises(ValueError, match="signal_date must be a date"):
        await repository.get_rolling7_market_health_for_date("2026-08-01")


@pytest.mark.parametrize(
    "value",
    [
        "100",
        0,
        0.0,
        -1,
        -1.5,
        None,
        {},
        [],
        True,
        float("nan"),
        float("inf"),
        10**400,
    ],
)
@pytest.mark.asyncio
async def test_readback_rejects_invalid_present_complete_evidence(value):
    row = _row_args_to_dict(date(2026, 8, 1), _upsert_args(_batch()))
    row["d0_references"]["000001"] = value
    connection = _StatefulConnection({date(2026, 8, 1): row})

    with pytest.raises(V20SemanticConflict, match="market evidence is invalid"):
        await _repository(connection).load_rolling7_market_health(
            before_t2=date(2026, 8, 5), limit=1
        )


@pytest.mark.asyncio
async def test_readback_rejects_invalid_present_gap_evidence_and_extra_keys():
    gap_row = _row_args_to_dict(date(2026, 8, 1), _upsert_args(_batch(d2_closes={})))
    gap_row["d2_closes"]["unrecommended"] = -1
    complete_row = _row_args_to_dict(date(2026, 8, 2), _upsert_args(_batch()))
    complete_row["d2_closes"]["unrecommended"] = "202"

    for row in (gap_row, complete_row):
        connection = _StatefulConnection({row["signal_date"]: row})
        with pytest.raises(V20SemanticConflict, match="market evidence is invalid"):
            await _repository(connection).load_rolling7_market_health(
                before_t2=date(2026, 8, 5), limit=1
            )
