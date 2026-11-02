"""V22 uses a real portable digest without computing legacy full-object hashes."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import src.strategy.v22_slim.selection as slim_selection
import src.web.v20_canonical_selection as canonical_module
import src.web.v20_service as service_module
from src.data.clients.tushare_realtime import TushareDailyBar, TushareEarlyMarketData
from src.data.database.v20_repository import MinuteBarRecord, V20SemanticConflict, sha256_json
from src.web.v20_service import _DayContext
from src.web.v20_v16_canonical_artifact import encode, hydrate
from tests.unit.web.test_v20_canonical_projection_acceptance import (
    FULL_EXCHANGE_CALENDAR,
    HISTORY_DATES,
    TRADE_DATE,
    TZ,
    _canonical,
)
from tests.unit.web.test_v20_service import _service
from tests.unit.web.test_v22_canonical_artifact import v22_canonical

RAW_RECEIPT = datetime(2026, 9, 1, 9, 40, tzinfo=TZ)
ARTIFACT_RECEIPT = RAW_RECEIPT + timedelta(seconds=1)


class _Repository:
    def __init__(self):
        self.records = {}

    async def record_minute_bars(self, payloads):
        for payload in payloads:
            digest = sha256_json(payload)
            self.records[digest] = MinuteBarRecord(
                code=payload["stock_code"],
                bar_end=datetime.fromisoformat(payload["bar_end"]),
                end_label=payload["end_label"],
                source_hash=digest,
                payload=dict(payload),
                first_received_at=RAW_RECEIPT,
            )
        return frozenset(sha256_json(payload) for payload in payloads)

    async def list_raw_minute_bar_records(self, codes, **_kwargs):
        return tuple(record for record in self.records.values() if record.code in codes)


class _PortableStore:
    """Real JSON codec and immutable slot semantics; no custom hydrate shortcut."""

    def __init__(self):
        self.record = None
        self.saves = 0

    def install(self, payload):
        self.record = SimpleNamespace(
            payload=deepcopy(payload),
            trade_date=TRADE_DATE,
            first_received_at=ARTIFACT_RECEIPT,
            snapshot_hash=sha256_json(payload),
        )

    async def load(self, **_kwargs):
        return self.record

    async def save_once(self, payload, **_kwargs):
        self.saves += 1
        if self.record is not None and self.record.payload != payload:
            raise V20SemanticConflict("canonical artifact slot collision")
        self.install(payload)


def _portable_service(monkeypatch):
    repository = _Repository()
    service = _service(monkeypatch, repository)
    service._scan_state.selection_version = "V22-slim"
    service._canonical_callbacks_open = True
    service._canonical_artifact_store = _PortableStore()
    service._clock = lambda: ARTIFACT_RECEIPT + timedelta(seconds=1)
    return service, repository, service._canonical_artifact_store


@pytest.mark.parametrize("has_state_version", [True, False])
@pytest.mark.parametrize("version,expected_legacy", [("V22-slim", False), ("V20", True)])
async def test_direct_compute_selects_only_v22_lightweight_identity(
    monkeypatch, version, expected_legacy, has_state_version
):
    service = _service(monkeypatch, object())
    if has_state_version:
        service._scan_state.selection_version = version
    else:
        service._scan_state = SimpleNamespace()
        service.config = SimpleNamespace(strategy_version=version)
    service._clock = lambda: ARTIFACT_RECEIPT
    frozen = SimpleNamespace(
        early_data_seed={},
        universe=(),
        clean_boards={},
        prev_closes={},
        history_raw={},
        names={},
        calendar=FULL_EXCHANGE_CALENDAR,
        prior_daily={},
        st_eligible_codes=(),
    )
    canonical = _canonical()
    observed = []

    async def compute(_state, day, **kwargs):
        assert day == TRADE_DATE
        assert kwargs["allow_realtime_fetch"] is False
        observed.append(kwargs.get("include_legacy_fingerprints", True))
        return canonical

    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", compute)
    service._historical_canonical_inputs = AsyncMock(return_value=frozen)
    service._persist_canonical_raw_minute_bars = AsyncMock()
    result = await service._compute_canonical_v16_from_persisted_raw(
        _DayContext(trade_date=TRADE_DATE, calendar=FULL_EXCHANGE_CALENDAR)
    )
    assert result is canonical
    assert observed == [expected_legacy]
    service._persist_canonical_raw_minute_bars.assert_awaited_once_with(canonical)


async def test_v22_new_portable_artifact_has_real_digest_and_restart_readback(monkeypatch):
    service, _repository, store = _portable_service(monkeypatch)
    canonical = replace(v22_canonical(), input_hash="", _integrity_hash="")
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)

    await service._persist_canonical_artifact_barrier(canonical)

    assert store.saves == 1
    assert store.record.payload["canonical_integrity_hash"] == projected.snapshot_hash
    assert projected.snapshot_hash not in {"", "0" * 64}
    assert hydrate(store.record.payload).bundle.snapshot == projected.snapshot
    restored, receipt = await service._load_canonical_artifact(TRADE_DATE)
    assert restored.snapshot == projected.snapshot
    assert restored.snapshot_hash == projected.snapshot_hash
    assert receipt == ARTIFACT_RECEIPT

    later = replace(canonical, computed_at=canonical.computed_at + timedelta(seconds=2))
    await service._persist_canonical_artifact_barrier(later)
    assert store.saves == 1
    assert store.record.first_received_at == ARTIFACT_RECEIPT


@pytest.mark.parametrize("legacy_schema", ["v1", "v2"])
async def test_v22_reuses_existing_legacy_digest_when_portable_snapshot_matches(
    monkeypatch, legacy_schema
):
    service, _repository, store = _portable_service(monkeypatch)
    legacy = v22_canonical()
    projected = service._project_canonical_v16(legacy, calendar=FULL_EXCHANGE_CALENDAR)
    payload = encode(
        projected,
        calendar=FULL_EXCHANGE_CALENDAR,
        canonical_integrity_hash=legacy._integrity_hash,
    )
    if legacy_schema == "v1":
        payload["schema_version"] = "v20-v16-portable-frozen/v1"
        payload.pop("legacy_recommendation")
    store.install(payload)
    canonical = replace(legacy, input_hash="", _integrity_hash="")

    await service._persist_canonical_artifact_barrier(canonical)

    assert store.saves == 0
    assert store.record.payload == payload
    restored, _receipt = await service._load_canonical_artifact(TRADE_DATE)
    assert restored.snapshot_hash == projected.snapshot_hash


@pytest.mark.parametrize("corruption", ["snapshot", "row_hash", "row_date", "raw"])
async def test_v22_reuse_still_validates_existing_artifact_and_raw(monkeypatch, corruption):
    service, repository, store = _portable_service(monkeypatch)
    canonical = replace(v22_canonical(), input_hash="", _integrity_hash="")
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    payload = encode(
        projected,
        calendar=FULL_EXCHANGE_CALENDAR,
        canonical_integrity_hash=projected.snapshot_hash,
    )
    store.install(payload)
    await service._persist_canonical_raw_minute_bars(canonical)
    if corruption == "snapshot":
        store.record.payload["v20_snapshot"]["v22_market"]["000001"]["amount"] += 1
        store.record.snapshot_hash = sha256_json(store.record.payload)
    elif corruption == "row_hash":
        store.record.snapshot_hash = "e" * 64
    elif corruption == "row_date":
        store.record.trade_date = TRADE_DATE + timedelta(days=1)
    else:
        repository.records.clear()

    with pytest.raises(V20SemanticConflict):
        await service._persist_canonical_artifact_barrier(canonical)
    assert store.saves == 0


@pytest.mark.parametrize("changed_field", ["score", "legacy_open_price"])
async def test_changed_v22_projection_does_not_reuse_an_existing_artifact(
    monkeypatch, changed_field
):
    service, _repository, store = _portable_service(monkeypatch)
    canonical = replace(v22_canonical(), input_hash="", _integrity_hash="")
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    payload = encode(
        projected,
        calendar=FULL_EXCHANGE_CALENDAR,
        canonical_integrity_hash=projected.snapshot_hash,
    )
    store.install(payload)
    changed = deepcopy(canonical)
    if changed_field == "score":
        changed.scan_result.recommended[0].score += 1
    else:
        top_code = changed.scan_result.recommended[0].code
        changed.stock_data[top_code].open_price += 1
    with pytest.raises(V20SemanticConflict, match="collision"):
        await service._persist_canonical_artifact_barrier(changed)


async def test_legacy_v20_artifact_keeps_its_full_integrity_digest(monkeypatch):
    service, repository, store = _portable_service(monkeypatch)
    service._scan_state.selection_version = "V20"
    canonical = _canonical()
    # This fixture has no raw minute rows; retain the raw barrier as a separate
    # existing acceptance responsibility while using the real portable codec.
    service._persist_canonical_raw_minute_bars = AsyncMock()
    service._hydrate_canonical_artifact_record = AsyncMock(
        side_effect=lambda record: hydrate(record.payload).bundle
    )
    await service._persist_canonical_artifact_barrier(canonical)
    assert store.record.payload["canonical_integrity_hash"] == canonical._integrity_hash
    await service._persist_canonical_artifact_barrier(canonical)
    assert store.saves == 1

    # Matching projection alone must not replace V20's full-object identity.
    with pytest.raises(V20SemanticConflict, match="collision"):
        await service._persist_canonical_artifact_barrier(
            replace(canonical, _integrity_hash="e" * 64)
        )


async def test_v20_empty_legacy_digest_is_not_silently_replaced(monkeypatch):
    service, _repository, _store = _portable_service(monkeypatch)
    service._scan_state.selection_version = "V20"
    service._persist_canonical_raw_minute_bars = AsyncMock()
    canonical = replace(_canonical(), input_hash="", _integrity_hash="")
    with pytest.raises(V20SemanticConflict, match="canonical integrity hash is invalid"):
        await service._persist_canonical_artifact_barrier(canonical)


async def test_real_v22_direct_compute_skips_legacy_hashes_and_preserves_scan(monkeypatch):
    service, _repository, _store = _portable_service(monkeypatch)
    seed = v22_canonical()
    codes = tuple(sorted(seed.stock_data))
    calendar = (*HISTORY_DATES, *FULL_EXCHANGE_CALENDAR[1:])

    def derive(_state, **_kwargs):
        # Keep this unit fixture small while running the real V22 projection,
        # stock builder, scanner, scorer assets and complete canonical compute.
        scanner, scorer, _boards = slim_selection.make_scanner()
        return scanner, scorer, {"fixture-board": [(code, code) for code in codes]}, codes

    monkeypatch.setattr(canonical_module, "derive_canonical_v16_universe", derive)
    frozen = SimpleNamespace(
        early_data_seed={
            code: TushareEarlyMarketData(
                quote=replace(seed.quotes[codes[0]], stock_code=code),
                early_bars=seed.early_bars[code],
                source_hash=seed.early_source_hashes[code],
            )
            for code in seed.early_bars
        },
        universe=codes,
        clean_boards={"fixture-board": [(code, code) for code in codes]},
        prev_closes={code: 10.0 for code in codes},
        history_raw=seed.history_raw,
        names={code: code for code in codes},
        calendar=calendar,
        prior_daily={
            code: TushareDailyBar(
                stock_code=code,
                trade_date=HISTORY_DATES[-1].isoformat(),
                close_price=10.0,
                amount_yuan=1_000_000.0,
            )
            for code in codes
        },
        st_eligible_codes=codes,
    )
    kwargs = {
        "early_data_seed": frozen.early_data_seed,
        "universe_override": frozen.universe,
        "clean_boards_override": frozen.clean_boards,
        "prev_closes_override": frozen.prev_closes,
        "history_raw_override": frozen.history_raw,
        "names_override": frozen.names,
        "calendar_override": frozen.calendar,
        "prior_daily_override": frozen.prior_daily,
        "st_eligible_codes_override": frozen.st_eligible_codes,
        "allow_realtime_fetch": False,
    }
    baseline = await canonical_module.compute_canonical_v16_scan(
        service._scan_state, TRADE_DATE, **kwargs
    )
    assert baseline.input_hash and baseline._integrity_hash

    def forbidden(*_args, **_kwargs):
        raise AssertionError("V22 direct compute must not calculate legacy full-object hashes")

    monkeypatch.setattr(canonical_module, "_stable_input_hash", forbidden)
    monkeypatch.setattr(canonical_module, "_bundle_fingerprint", forbidden)
    service._historical_canonical_inputs = AsyncMock(return_value=frozen)
    service._persist_canonical_raw_minute_bars = AsyncMock()
    actual = await service._compute_canonical_v16_from_persisted_raw(
        _DayContext(trade_date=TRADE_DATE, calendar=calendar)
    )
    assert actual.input_hash == actual._integrity_hash == ""
    assert actual.scan_result == baseline.scan_result
    assert actual.external_market_fact_hash == baseline.external_market_fact_hash
    assert set(actual.stock_data) == set(baseline.stock_data) == set(codes)
    before = service._project_canonical_v16(baseline, calendar=calendar)
    after = service._project_canonical_v16(actual, calendar=calendar)
    assert before.snapshot == after.snapshot
    assert before.snapshot_hash == after.snapshot_hash
