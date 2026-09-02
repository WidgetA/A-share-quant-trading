from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from src.web import v20_service as service_module
from src.web.v20_service import _DayContext
from tests.unit.web.test_v20_service import (
    _ENRICHED_LABELS,
    _HIST_TRADE_DATE,
    _LATE_REPLAY_CODES,
    _LEGACY_LABELS,
    _bar,
    _bar_payload,
    _historical_seed_service,
    _HistoricalSeedClient,
    _late_replay_service,
    _SeedRepository,
)


async def seed_legacy(repository: _SeedRepository) -> None:
    for code in _LATE_REPLAY_CODES:
        rows = [_bar_payload(_bar(code, label)) for label in _LEGACY_LABELS]
        await repository.record_minute_bars(rows)


async def test_fixed_nine_is_usable_without_special_label_refetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A legal target-date 09:39 is sufficient for historical V16 readiness."""
    repository = _SeedRepository()
    await seed_legacy(repository)
    repository.persist_calls.clear()
    client = _HistoricalSeedClient()
    service = _historical_seed_service(monkeypatch, repository, client)
    seed, universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert client.calls == []
    assert repository.list_calls == 1
    assert repository.persist_calls == []
    assert set(seed) == set(universe)
    assert all(
        [bar.end_label for bar in seed[code].early_bars] == list(_LEGACY_LABELS)
        for code in universe
    )


async def test_optional_preopen_labels_are_preserved_without_becoming_a_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persisted earlier labels survive hydration but do not create a new gate."""
    repository = _SeedRepository()
    await seed_legacy(repository)
    for code in _LATE_REPLAY_CODES:
        await repository.record_minute_bars([_bar_payload(_bar(code, "09:25"))])
    repository.persist_calls.clear()
    client = _HistoricalSeedClient()
    service = _historical_seed_service(monkeypatch, repository, client)
    seed, universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert client.calls == []
    assert repository.list_calls == 1
    assert set(seed) == set(universe)
    expected_labels = ("09:25", *_LEGACY_LABELS)
    assert all(
        [bar.end_label for bar in seed[code].early_bars] == list(expected_labels)
        for code in universe
    )


async def test_same_day_post_cutoff_uses_historical_adapter_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same-day replay backfills with stk_mins and never current-day history."""
    service, repository, client, calls, _observed, context = _late_replay_service(monkeypatch)
    for code in _LATE_REPLAY_CODES:
        repository.raw.pop((code, "09:39"))

    bundle = await service._compute_canonical_v16_from_persisted_raw(context)

    assert context.trade_date == date(2026, 8, 31)
    assert client.calls == []
    assert client.stk_mins_calls == [(tuple(sorted(_LATE_REPLAY_CODES)), context.trade_date)]
    assert calls == [context.trade_date]
    assert bundle.trade_date == context.trade_date
    assert {label for _code, label in repository.raw} == set(_ENRICHED_LABELS)


async def test_fully_enriched_evidence_avoids_vendor_refetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fully persisted enriched evidence is usable with no vendor access."""
    repository = _SeedRepository()
    for code in _LATE_REPLAY_CODES:
        rows = [
            _bar_payload(_bar(code, label, trade_date=_HIST_TRADE_DATE))
            for label in _ENRICHED_LABELS
        ]
        await repository.record_minute_bars(rows)
    repository.persist_calls.clear()

    class ForbiddenVendorClient:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"vendor refetch attempted through {name}")

    service = _historical_seed_service(monkeypatch, repository, ForbiddenVendorClient())

    async def coordinator_bomb(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("persisted replay must bypass the canonical coordinator")

    monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", coordinator_bomb)
    context = _DayContext(
        trade_date=_HIST_TRADE_DATE,
        calendar=(date(2026, 8, 28), _HIST_TRADE_DATE, date(2026, 9, 1)),
    )
    seed, universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert repository.list_calls == 1
    assert repository.persist_calls == []
    assert set(seed) == set(universe)
    assert all(
        [bar.end_label for bar in seed[code].early_bars] == list(_ENRICHED_LABELS)
        for code in universe
    )
    assert context.trade_date == _HIST_TRADE_DATE
