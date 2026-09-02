from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from src.data.database.v20_repository import V20RepositoryError
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


@pytest.mark.parametrize("special_label", ["09:25", "09:30"])
async def test_one_special_label_stays_unresolved(
    monkeypatch: pytest.MonkeyPatch,
    special_label: str,
) -> None:
    """Legacy fixed-nine plus one special label is not canonical V16."""
    repository = _SeedRepository()
    await seed_legacy(repository)
    repository.persist_calls.clear()

    class OneLabelClient(_HistoricalSeedClient):
        async def batch_get_early_minute_history_for_date(self, codes, trade_date):
            self.calls.append((tuple(codes), trade_date))
            missing_label = "09:30" if special_label == "09:25" else "09:25"
            labels = (special_label,) if len(self.calls) == 1 else (missing_label,)
            return {
                code: tuple(_bar(code, label, trade_date=trade_date) for label in labels)
                for code in codes
            }

    client = OneLabelClient()
    service = _historical_seed_service(monkeypatch, repository, client)
    try:
        await service._historical_early_evidence_seed(_HIST_TRADE_DATE)
    except V20RepositoryError as exc:
        assert str(exc) == (
            "canonical V16 historical backfill is incomplete: "
            "10/10 targets lack qualified persisted evidence "
            "or an explicitly empty vendor response"
        )
    else:
        pytest.fail(
            f"one non-legacy label {special_label} was treated complete; "
            "both 09:25 and 09:30 are required"
        )

    assert client.calls == [(tuple(sorted(_LATE_REPLAY_CODES)), _HIST_TRADE_DATE)]
    assert repository.list_calls == 2
    assert len(repository.persist_calls) == 1
    assert len(repository.persist_calls[0]) == len(_LATE_REPLAY_CODES)
    assert {row["end_label"] for row in repository.persist_calls[0]} == {special_label}
    for code in _LATE_REPLAY_CODES:
        labels = {label for stored, label in repository.raw if stored == code}
        assert labels == set(_LEGACY_LABELS) | {special_label}

    seed, universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)
    assert len(client.calls) == 2
    assert repository.list_calls == 4
    assert len(repository.persist_calls) == 2
    assert len(repository.persist_calls[1]) == len(_LATE_REPLAY_CODES)
    assert {row["end_label"] for row in repository.persist_calls[1]} == {
        "09:30" if special_label == "09:25" else "09:25"
    }
    assert client.calls[1] == (tuple(sorted(_LATE_REPLAY_CODES)), _HIST_TRADE_DATE)
    assert set(seed) == set(universe)
    assert {label for _code, label in repository.raw} == set(_ENRICHED_LABELS)


async def test_legacy_nine_completes_after_both_special_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical stk_mins evidence completes only with both special labels."""
    repository = _SeedRepository()
    await seed_legacy(repository)
    repository.persist_calls.clear()

    class BothLabelsClient(_HistoricalSeedClient):
        async def batch_get_early_minute_history_for_date(self, codes, trade_date):
            self.calls.append((tuple(codes), trade_date))
            bars = ("09:25", "09:30")
            return {
                code: tuple(_bar(code, label, trade_date=trade_date) for label in bars)
                for code in codes
            }

    client = BothLabelsClient()
    service = _historical_seed_service(monkeypatch, repository, client)
    seed, universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert client.calls == [(tuple(sorted(_LATE_REPLAY_CODES)), _HIST_TRADE_DATE)]
    assert repository.list_calls == 2
    assert set(seed) == set(universe)
    assert {label for _code, label in repository.raw} == set(_ENRICHED_LABELS)
    assert all(
        [bar.end_label for bar in seed[code].early_bars] == list(_ENRICHED_LABELS)
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
