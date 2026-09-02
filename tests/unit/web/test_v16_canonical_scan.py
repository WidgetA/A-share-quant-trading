"""Canonical V16 core must be single-flight, reusable, and message-exactly-once."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.data.clients.tushare_realtime import (
    BEIJING_TZ,
    TushareDailyBar,
    TushareEarlyMarketData,
    TushareMinuteBar,
    TushareQuote,
    tushare_minute_bars_to_early_market_data,
)
from src.strategy.strategies.v16_scanner import V16Scanner as RealV16Scanner
from src.web import v15_scan_service


def _clean_board(*codes: str) -> list[tuple[str, str]]:
    return [(code, f"{code}-name") for code in codes]


@pytest.fixture
def fakes(monkeypatch):
    """Set up a minimal scan state with fakes and recorders."""

    trade_date = datetime.now(v15_scan_service.BEIJING_TZ).date()
    prev_date = trade_date - timedelta(days=1)

    class FakeScorer:
        def __init__(self, *_args, **_kwargs):
            self.model_sha256 = "m" * 64
            self.feature_list_sha256 = "f" * 64

    class FakeScanner:
        scan_calls = 0
        fail_times = 0
        gate: asyncio.Event | None = None

        def __init__(self, **kwargs):  # noqa: ARG002
            pass

        def get_universe(self):
            return ({"board-a": _clean_board("600000")}, {"600000"})

        async def scan(self, stock_data, clean_boards):  # noqa: ARG002
            type(self).scan_calls += 1
            if type(self).fail_times > 0:
                type(self).fail_times -= 1
                raise RuntimeError("scan boom")
            if type(self).gate is not None:
                await type(self).gate.wait()
            await asyncio.sleep(0)
            top1 = SimpleNamespace(
                code="600000",
                name="cached-name",
                buy_price=12.345678,
                score=0.123456789,
            )
            return SimpleNamespace(
                recommended=[top1],
                all_scored=[top1],
                stock_best_board={"600000": "board-a"},
                stock_all_boards={"600000": ["board-a"]},
                step2_hot_board_count=3,
                final_candidates=5,
            )

    def _make_bars(code: str) -> tuple[TushareMinuteBar, ...]:
        bar_end = datetime.combine(trade_date, datetime.min.time()).replace(
            hour=9, minute=39, tzinfo=BEIJING_TZ
        )
        return (
            TushareMinuteBar(
                stock_code=code,
                bar_end=bar_end,
                end_label="09:39",
                open_price=11.0,
                close_price=12.3,
                high_price=12.4,
                low_price=10.9,
                volume=2000.0,
                amount=24000.0,
            ),
        )

    class FakeRTClient:
        def __init__(self):
            self.early_pull_calls = 0
            self.daily_pull_calls = 0

        async def stop(self):
            pass

        async def batch_get_early_market_data(self, codes: list[str], expected_trade_date=None):
            self.early_pull_calls += 1
            await asyncio.sleep(0)
            return {
                code: TushareEarlyMarketData(
                    quote=TushareQuote(
                        stock_code=code,
                        open_price=11.0,
                        latest_price=12.0,
                        high_price=12.5,
                        low_price=10.9,
                        volume=5000.0,
                        amount=60000.0,
                        early_close=12.3,
                        early_high=12.4,
                        early_low=11.5,
                        early_volume=3000.0,
                        volume_937=2000.0,
                    ),
                    early_bars=_make_bars(code),
                    source_hash=f"h-{code}",
                )
                for code in codes
            }

        async def fetch_prev_closes(self, ts_date: str):
            return {"600000": 10.5}

        async def fetch_daily_bars(self, trade_date: str):
            self.daily_pull_calls += 1
            closes = await self.fetch_prev_closes(trade_date)
            return {
                code: TushareDailyBar(
                    stock_code=code,
                    trade_date=trade_date,
                    close_price=close,
                    amount_yuan=1_234_567.0,
                )
                for code, close in closes.items()
            }

    class FakeFDB:
        async def batch_get_fundamentals(self, codes):
            return {}

        async def batch_current_names(self, codes):
            return {"600000": "fresh-name"}

        async def close(self):
            pass

    class FakeHistAdapter:
        async def history_quotes(
            self,
            *,
            codes: str,
            indicators: str,  # noqa: ARG002
            start_date: str,  # noqa: ARG002
            end_date: str,  # noqa: ARG002
        ):
            requested = [c.split(".")[0] for c in codes.split(",")]
            tables = []
            for code in requested:
                times = [
                    (prev_date - timedelta(days=39 - i)).strftime("%Y-%m-%d") for i in range(40)
                ]
                tables.append(
                    {
                        "thscode": f"{code}.SH",
                        "table": {
                            "time": times,
                            "open": [10.0] * 40,
                            "high": [10.5] * 40,
                            "low": [9.5] * 40,
                            "close": [10.0 + i * 0.01 for i in range(40)],
                            "volume": [1000.0] * 40,
                        },
                    }
                )
            return {"tables": tables}

    top10_calls: list[Any] = []
    error_calls: list[tuple[str, str]] = []
    daygate_calls: list[Any] = []

    async def record_top10(scan_result):
        top10_calls.append(scan_result)

    async def record_error(title, detail):
        error_calls.append((title, detail))

    async def no_refresh(*_args, **_kwargs):
        pass

    def record_daygate(snapshot, runtime):  # noqa: ARG001
        daygate_calls.append((snapshot, runtime))

    monkeypatch.setattr("src.strategy.lgbrank_scorer.LGBRankScorer", FakeScorer)
    monkeypatch.setattr("src.strategy.strategies.v16_scanner.V16Scanner", FakeScanner)

    async def fake_calendar():
        return sorted(
            {
                *(trade_date - timedelta(days=offset) for offset in range(1, 46)),
                trade_date,
                trade_date + timedelta(days=1),
                trade_date + timedelta(days=2),
            }
        )

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", fake_calendar)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_v16_top10", record_top10)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", record_error)
    monkeypatch.setattr(v15_scan_service, "_refresh_top10_names", no_refresh)
    monkeypatch.setattr(v15_scan_service, "_schedule_v16_day_gate_shadow", record_daygate)
    monkeypatch.setattr(
        "src.strategy.v16_day_gate_shadow.freeze_v16_day_gate_runtime",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        "src.strategy.v16_day_gate_shadow.freeze_v16_scan_snapshot",
        lambda *args, **kwargs: {"run_id": "test"},
    )

    rt = FakeRTClient()
    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=rt,
        fundamentals_db=FakeFDB(),
        historical_adapter=FakeHistAdapter(),
        concept_mapper=object(),
        stock_filter=object(),
        tushare_cache=None,
    )

    return SimpleNamespace(
        state=state,
        rt=rt,
        trade_date=trade_date,
        scanner=FakeScanner,
        fake_rt_class=FakeRTClient,
        top10_calls=top10_calls,
        error_calls=error_calls,
        daygate_calls=daygate_calls,
    )


@pytest.mark.parametrize("callers", [5, 20, 36])
@pytest.mark.asyncio
async def test_concurrent_canonical_calls_fetch_and_scan_once(fakes, callers: int):
    bundles = await asyncio.gather(
        *(
            v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
            for _ in range(callers)
        )
    )
    b1 = bundles[0]
    b2 = bundles[-1]

    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    assert fakes.rt.daily_pull_calls == 1
    assert b1.input_hash == b2.input_hash
    assert b1._integrity_hash == b2._integrity_hash
    assert all(bundle is not b1 for bundle in bundles[1:])


@pytest.mark.asyncio
async def test_36_concurrent_callers_use_one_breadth_union_and_one_daily(fakes):
    requested_early: list[list[str]] = []

    class UnionRT(fakes.fake_rt_class):
        async def fetch_prev_closes(self, _ts_date: str):
            return {"600000": 10.5, "000001": 9.5}

        async def batch_get_early_market_data(self, codes, expected_trade_date=None):
            requested_early.append(list(codes))
            return await super().batch_get_early_market_data(
                codes, expected_trade_date=expected_trade_date
            )

    rt = UnionRT()
    fakes.state.realtime_client = rt
    fakes.scanner.scan_calls = 0

    bundles = await asyncio.gather(
        *(
            v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
            for _ in range(36)
        )
    )

    assert requested_early == [["000001", "600000"]]
    assert rt.daily_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    assert len(bundles) == 36
    assert {bundle.breadth_valid_n for bundle in bundles} == {2}
    # Consumers receive isolated artifacts, not the master bundle.
    b1 = bundles[0]
    b2 = bundles[-1]
    assert all(bundle is not b1 for bundle in bundles[1:])
    master = fakes.state.canonical_coordinator.cache[fakes.trade_date]
    assert master is not b1
    assert b1.input_hash == b2.input_hash
    assert b1._integrity_hash == b2._integrity_hash
    assert b1.trade_date == fakes.trade_date
    assert b1.universe == ("600000",)
    assert b1.quotes["600000"].open_price == pytest.approx(11.0)
    assert b1.prev_closes == {"600000": 10.5, "000001": 9.5}
    assert "600000" in b1.stock_data
    assert b1.early_source_hashes == {
        "000001": "h-000001",
        "600000": "h-600000",
    }
    assert b1.model_sha256 == "m" * 64
    assert b1.feature_list_sha256 == "f" * 64
    assert len(b1.input_hash) == 64
    assert b1.computed_at.tzinfo is not None


@pytest.mark.asyncio
async def test_cross_date_singleflight_keeps_running_master_and_shares_waiters(
    fakes,
    monkeypatch,
):
    """A newer date must not evict or duplicate an older still-running master."""

    date_a = fakes.trade_date
    date_b = date_a + timedelta(days=1)
    scanner_entered = asyncio.Event()
    release_a = asyncio.Event()
    scanner_calls: list[date] = []
    compute_calls: dict[date, int] = {}
    rt_calls: dict[date, int] = {}

    class SequenceGatedScanner:
        def __init__(self, **_kwargs):  # noqa: ARG002
            pass

        def get_universe(self):
            return ({"board-a": _clean_board("600000")}, {"600000"})

        async def scan(self, stock_data, clean_boards):  # noqa: ARG002
            call_date = (date_a, date_b, date_a)[len(scanner_calls)]
            scanner_calls.append(call_date)
            if len(scanner_calls) == 1:
                scanner_entered.set()
                await asyncio.wait_for(release_a.wait(), timeout=1)
            await asyncio.sleep(0)
            top1 = SimpleNamespace(
                code="600000",
                name="cached-name",
                buy_price=12.345678,
                score=0.123456789,
            )
            return SimpleNamespace(
                recommended=[top1],
                all_scored=[top1],
                stock_best_board={"600000": "board-a"},
                stock_all_boards={"600000": ["board-a"]},
                step2_hot_board_count=3,
                final_candidates=5,
            )

    class TradeDateRTClient:
        async def stop(self):
            return None

        async def batch_get_early_market_data(self, codes, expected_trade_date=None):
            trade_date = expected_trade_date or date_a
            rt_calls[trade_date] = rt_calls.get(trade_date, 0) + 1
            await asyncio.sleep(0)
            return {
                code: TushareEarlyMarketData(
                    quote=TushareQuote(
                        stock_code=code,
                        open_price=11.0,
                        latest_price=12.0,
                        high_price=12.5,
                        low_price=10.9,
                        volume=5000.0,
                        amount=60000.0,
                        early_close=12.3,
                        early_high=12.4,
                        early_low=11.5,
                        early_volume=3000.0,
                        volume_937=2000.0,
                    ),
                    early_bars=(
                        TushareMinuteBar(
                            stock_code=code,
                            bar_end=datetime.combine(trade_date, time(9, 39), tzinfo=BEIJING_TZ),
                            end_label="09:39",
                            open_price=11.0,
                            close_price=12.3,
                            high_price=12.4,
                            low_price=10.9,
                            volume=2000.0,
                            amount=24000.0,
                        ),
                    ),
                    source_hash=f"h-{code}-{trade_date.isoformat()}",
                )
                for code in codes
            }

        async def fetch_prev_closes(self, _ts_date):
            return {"600000": 10.5}

        async def fetch_daily_bars(self, trade_date: str):
            return {
                "600000": TushareDailyBar(
                    stock_code="600000",
                    trade_date=trade_date,
                    close_price=10.5,
                    amount_yuan=1_000_000.0,
                )
            }

    class TradeDateHistAdapter:
        async def history_quotes(
            self,
            *,
            codes: str,
            indicators: str,  # noqa: ARG002
            start_date: str,  # noqa: ARG002
            end_date: str,
        ):
            requested = [code.split(".")[0] for code in codes.split(",")]
            final_day = datetime.strptime(end_date, "%Y-%m-%d").date()
            times = [(final_day - timedelta(days=39 - index)).isoformat() for index in range(40)]
            tables = []
            for code in requested:
                tables.append(
                    {
                        "thscode": f"{code}.SH",
                        "table": {
                            "time": times,
                            "open": [10.0] * 40,
                            "high": [10.5] * 40,
                            "low": [9.5] * 40,
                            "close": [10.0 + index * 0.01 for index in range(40)],
                            "volume": [1000.0] * 40,
                        },
                    }
                )
            return {"tables": tables}

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=TradeDateRTClient(),
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=TradeDateHistAdapter(),
        concept_mapper=object(),
        stock_filter=object(),
        tushare_cache=None,
    )

    async def calendar_with_both_dates():
        return sorted(
            {
                *(date_a - timedelta(days=offset) for offset in range(1, 46)),
                date_a,
                date_b,
                date_b + timedelta(days=1),
                date_b + timedelta(days=2),
            }
        )

    real_compute = v15_scan_service.compute_canonical_v16_scan

    async def counting_compute(scan_state, trade_date, **kwargs):
        compute_calls[trade_date] = compute_calls.get(trade_date, 0) + 1
        return await real_compute(scan_state, trade_date, **kwargs)

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar_with_both_dates)
    monkeypatch.setattr("src.strategy.strategies.v16_scanner.V16Scanner", SequenceGatedScanner)
    monkeypatch.setattr(v15_scan_service, "compute_canonical_v16_scan", counting_compute)

    first_a_waiter = asyncio.create_task(
        v15_scan_service.get_or_compute_canonical_v16(state, date_a)
    )
    await asyncio.wait_for(scanner_entered.wait(), timeout=1)
    coordinator = state.canonical_coordinator
    assert coordinator is not None
    master_a = coordinator.inflight[date_a]

    bundle_b = await asyncio.wait_for(
        v15_scan_service.get_or_compute_canonical_v16(state, date_b), timeout=1
    )
    contract_failures: list[str] = []
    if coordinator.inflight.get(date_a) is not master_a:
        contract_failures.append("date-B eviction removed or replaced running date-A master")

    second_a_waiter = asyncio.create_task(
        v15_scan_service.get_or_compute_canonical_v16(state, date_a)
    )
    await asyncio.sleep(0)
    joined_a_master = coordinator.inflight.get(date_a)
    if joined_a_master is not master_a:
        contract_failures.append("second date-A waiter did not join the original master")

    release_a.set()
    first_a_bundle, second_a_bundle = await asyncio.wait_for(
        asyncio.gather(first_a_waiter, second_a_waiter), timeout=1
    )

    if compute_calls != {date_a: 1, date_b: 1}:
        contract_failures.append(f"compute calls {compute_calls!r}")
    if rt_calls != {date_a: 1, date_b: 1}:
        contract_failures.append(f"realtime calls {rt_calls!r}")
    if scanner_calls != [date_a, date_b]:
        contract_failures.append(f"scanner calls {scanner_calls!r}")
    if first_a_bundle.input_hash != second_a_bundle.input_hash:
        contract_failures.append("date-A waiters received different input hashes")
    if first_a_bundle._integrity_hash != second_a_bundle._integrity_hash:
        contract_failures.append("date-A waiters received different integrity hashes")
    if coordinator.inflight:
        contract_failures.append(f"coordinator left inflight orphans {coordinator.inflight!r}")
    if set(coordinator.cache) != {date_a}:
        contract_failures.append(f"coordinator cache keys {set(coordinator.cache)!r}")
    if any(task.get_name().startswith("v20-") for task in asyncio.all_tasks()):
        contract_failures.append("canonical computation left a live scheduler task")

    assert not contract_failures, "\n".join(contract_failures)
    assert bundle_b.trade_date == date_b
    assert first_a_bundle.trade_date == date_a


@pytest.mark.asyncio
async def test_blocked_durable_sink_holds_cache_return_and_publish(fakes):
    sink_entered = asyncio.Event()
    release_sink = asyncio.Event()
    sink_calls = 0

    async def blocked_sink(_bundle):
        nonlocal sink_calls
        sink_calls += 1
        sink_entered.set()
        await asyncio.wait_for(release_sink.wait(), timeout=1)

    fakes.state.canonical_sink = blocked_sink
    caller = asyncio.create_task(
        v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    )
    await asyncio.wait_for(sink_entered.wait(), timeout=1)
    coordinator = fakes.state.canonical_coordinator

    assert sink_calls == 1
    assert caller.done() is False
    assert fakes.trade_date in coordinator.pending_persist
    assert fakes.trade_date in coordinator.inflight
    assert fakes.trade_date not in coordinator.cache
    cached = await v15_scan_service.get_cached_canonical_v16(fakes.state, fakes.trade_date)
    assert cached.status is v15_scan_service.CachedCanonicalV16Status.PERSISTENCE_PENDING
    assert fakes.top10_calls == []
    assert fakes.daygate_calls == []

    release_sink.set()
    bundle = await asyncio.wait_for(caller, timeout=1)
    assert bundle.trade_date == fakes.trade_date
    assert fakes.trade_date in coordinator.cache
    assert fakes.trade_date not in coordinator.pending_persist
    cached = await v15_scan_service.get_cached_canonical_v16(fakes.state, fakes.trade_date)
    assert cached.status is v15_scan_service.CachedCanonicalV16Status.AVAILABLE


@pytest.mark.asyncio
async def test_thirty_six_callers_compute_persist_and_publish_once(fakes):
    sink_calls = 0

    async def counting_sink(_bundle):
        nonlocal sink_calls
        sink_calls += 1

    fakes.state.canonical_sink = counting_sink
    recommendations = await asyncio.wait_for(
        asyncio.gather(*(v15_scan_service.run_v16_scan(fakes.state) for _ in range(36))),
        timeout=1,
    )

    assert len(recommendations) == 36
    assert all(item == recommendations[0] for item in recommendations[1:])
    assert sink_calls == 1
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    assert len(fakes.top10_calls) == 1
    assert len(fakes.daygate_calls) == 1


@pytest.mark.asyncio
async def test_waiter_cancellation_does_not_cancel_master_or_sink(fakes):
    sink_entered = asyncio.Event()
    release_sink = asyncio.Event()

    async def shielded_sink(_bundle):
        sink_entered.set()
        await asyncio.wait_for(release_sink.wait(), timeout=1)

    fakes.state.canonical_sink = shielded_sink
    first = asyncio.create_task(
        v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    )
    second = asyncio.create_task(
        v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    )
    await asyncio.wait_for(sink_entered.wait(), timeout=1)
    master = fakes.state.canonical_coordinator.inflight[fakes.trade_date]
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    assert master.cancelled() is False
    release_sink.set()
    bundle = await asyncio.wait_for(second, timeout=1)
    assert bundle.trade_date == fakes.trade_date
    assert master.done() is True
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1


@pytest.mark.asyncio
async def test_failed_sink_retries_without_vendor_or_scanner(fakes):
    sink_calls = 0

    async def failing_once_sink(_bundle):
        nonlocal sink_calls
        sink_calls += 1
        if sink_calls == 1:
            raise RuntimeError("durable store temporarily unavailable")

    fakes.state.canonical_sink = failing_once_sink
    with pytest.raises(v15_scan_service.CanonicalV16PersistencePendingError):
        await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    coordinator = fakes.state.canonical_coordinator
    assert sink_calls == 1
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    assert fakes.trade_date in coordinator.pending_persist
    assert fakes.trade_date not in coordinator.cache
    cached = await v15_scan_service.get_cached_canonical_v16(fakes.state, fakes.trade_date)
    assert cached.status is v15_scan_service.CachedCanonicalV16Status.PERSISTENCE_PENDING

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    assert bundle.trade_date == fakes.trade_date
    assert sink_calls == 2
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    assert fakes.trade_date not in coordinator.pending_persist
    assert fakes.trade_date in coordinator.cache


@pytest.mark.asyncio
async def test_cross_date_pending_is_retained_and_reused(fakes, monkeypatch):
    sink_calls: list[date] = []

    async def date_sink(_bundle):
        sink_calls.append(_bundle.trade_date)
        if _bundle.trade_date == fakes.trade_date and len(sink_calls) == 1:
            raise RuntimeError("date-A store unavailable")

    fakes.state.canonical_sink = date_sink
    with pytest.raises(v15_scan_service.CanonicalV16PersistencePendingError):
        await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    coordinator = fakes.state.canonical_coordinator
    pending_a = coordinator.pending_persist[fakes.trade_date]
    date_b = fakes.trade_date + timedelta(days=1)
    pending_b = replace(pending_a, trade_date=date_b, _integrity_hash="")
    pending_b = replace(
        pending_b,
        _integrity_hash=v15_scan_service._bundle_fingerprint(pending_b),
    )
    computed_dates: list[date] = []

    async def compute_date_b(_state, trade_date, **_kwargs):
        computed_dates.append(trade_date)
        return pending_b

    monkeypatch.setattr(v15_scan_service, "compute_canonical_v16_scan", compute_date_b)
    assert await v15_scan_service.get_or_compute_canonical_v16(fakes.state, date_b)
    assert computed_dates == [date_b]
    assert date_b in coordinator.cache
    assert fakes.trade_date in coordinator.pending_persist

    monkeypatch.undo()
    bundle_a = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    assert bundle_a.trade_date == fakes.trade_date
    assert computed_dates == [date_b]
    assert sink_calls == [fakes.trade_date, date_b, fakes.trade_date]
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    assert fakes.trade_date in coordinator.cache
    assert fakes.trade_date not in coordinator.pending_persist


@pytest.mark.asyncio
async def test_sink_cannot_silently_tamper_the_pending_master(fakes):
    sink_calls = 0

    async def tampering_once_sink(bundle):
        nonlocal sink_calls
        sink_calls += 1
        if sink_calls == 1:
            bundle.scan_result.recommended[0].name = "tampered-by-sink"

    fakes.state.canonical_sink = tampering_once_sink
    with pytest.raises(v15_scan_service.CanonicalV16PersistencePendingError):
        await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    pending = fakes.state.canonical_coordinator.pending_persist[fakes.trade_date]
    assert pending.scan_result.recommended[0].name == "cached-name"
    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    assert bundle.scan_result.recommended[0].name == "cached-name"
    assert sink_calls == 2
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1


@pytest.mark.asyncio
async def test_cleanup_cancels_durable_master_and_clears_pending(fakes):
    sink_entered = asyncio.Event()

    async def blocked_sink(_bundle):
        sink_entered.set()
        await asyncio.Event().wait()

    fakes.state.canonical_sink = blocked_sink
    caller = asyncio.create_task(
        v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    )
    await asyncio.wait_for(sink_entered.wait(), timeout=1)
    coordinator = fakes.state.canonical_coordinator
    master = coordinator.inflight[fakes.trade_date]

    await asyncio.wait_for(v15_scan_service.cleanup_scan_resources(fakes.state), timeout=1)

    assert master.cancelled() is True
    assert coordinator.pending_persist == {}
    assert fakes.state.canonical_coordinator is None
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(caller, timeout=1)


@pytest.mark.asyncio
async def test_scheduler_retries_pending_durable_sink_without_computing_again(
    fakes,
    monkeypatch,
):
    sink_calls = 0

    async def failing_once_sink(_bundle):
        nonlocal sink_calls
        sink_calls += 1
        if sink_calls == 1:
            raise RuntimeError("scheduler durable store unavailable")

    fakes.state.canonical_sink = failing_once_sink

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.combine(fakes.trade_date, time(9, 39), tzinfo=BEIJING_TZ)

    real_sleep = asyncio.sleep

    async def fast_sleep(_delay):
        await real_sleep(0)

    async def no_signal(_signal):
        return None

    monkeypatch.setattr(v15_scan_service, "datetime", FixedDateTime)
    monkeypatch.setattr(v15_scan_service.asyncio, "sleep", fast_sleep)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_signal", no_signal)
    scheduler = asyncio.create_task(v15_scan_service._scan_scheduler(fakes.state))

    async def recommendation_set():
        while fakes.state.today_recommendation is None:
            await real_sleep(0)

    await asyncio.wait_for(recommendation_set(), timeout=1)
    scheduler.cancel()
    await asyncio.wait_for(scheduler, timeout=1)

    assert sink_calls == 2
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    assert fakes.state.scan_done_date == fakes.trade_date.isoformat()
    assert fakes.state.scan_error is None


@pytest.mark.asyncio
async def test_cached_canonical_accessor_returns_isolated_verified_master(fakes, monkeypatch):
    master = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    cached_master = fakes.state.canonical_coordinator.cache[fakes.trade_date]

    async def compute_must_not_start(*_args, **_kwargs):
        raise AssertionError("cached-only accessor must not start computation")

    monkeypatch.setattr(v15_scan_service, "compute_canonical_v16_scan", compute_must_not_start)
    result = await v15_scan_service.get_cached_canonical_v16(fakes.state, fakes.trade_date)

    assert result.status is v15_scan_service.CachedCanonicalV16Status.AVAILABLE
    assert result.available is True
    assert result.bundle is not cached_master
    assert result.bundle is not master
    assert result.bundle._integrity_hash == cached_master._integrity_hash
    assert result.bundle.trade_date == fakes.trade_date
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1


@pytest.mark.asyncio
async def test_cached_canonical_accessor_reports_missing_and_inflight_without_cold_start(
    fakes, monkeypatch
):
    empty_state = v15_scan_service.V15ScanState()
    missing = await v15_scan_service.get_cached_canonical_v16(empty_state, fakes.trade_date)
    assert missing.status is v15_scan_service.CachedCanonicalV16Status.NOT_CACHED
    assert missing.available is False
    assert missing.bundle is None
    assert empty_state.canonical_coordinator is None

    gate = asyncio.Event()
    fakes.scanner.gate = gate
    inflight = asyncio.create_task(
        v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    )
    for _ in range(100):
        if fakes.rt.early_pull_calls == 1:
            break
        await asyncio.sleep(0)
    for _ in range(100):
        if fakes.scanner.scan_calls == 1:
            break
        await asyncio.sleep(0)

    async def compute_must_not_start(*_args, **_kwargs):
        raise AssertionError("cached-only accessor must not join or start computation")

    monkeypatch.setattr(v15_scan_service, "compute_canonical_v16_scan", compute_must_not_start)
    running = await v15_scan_service.get_cached_canonical_v16(fakes.state, fakes.trade_date)
    assert running.status is v15_scan_service.CachedCanonicalV16Status.IN_FLIGHT
    assert running.available is False
    assert running.bundle is None
    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1

    gate.set()
    await inflight


@pytest.mark.asyncio
async def test_cached_canonical_accessor_reports_prior_failure_without_retry(fakes, monkeypatch):
    fakes.scanner.fail_times = 1
    with pytest.raises(RuntimeError, match="scan boom"):
        await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    async def compute_must_not_start(*_args, **_kwargs):
        raise AssertionError("cached-only accessor must not retry a failed computation")

    monkeypatch.setattr(v15_scan_service, "compute_canonical_v16_scan", compute_must_not_start)
    failed = await v15_scan_service.get_cached_canonical_v16(fakes.state, fakes.trade_date)

    assert failed.status is v15_scan_service.CachedCanonicalV16Status.FAILED
    assert failed.available is False
    assert failed.bundle is None
    assert failed.detail == "RuntimeError: scan boom"
    assert fakes.scanner.scan_calls == 1


@pytest.mark.asyncio
async def test_cached_canonical_accessor_rejects_date_mismatch_and_bad_fingerprint(fakes):
    master = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    coord = fakes.state.canonical_coordinator
    coord.cache[fakes.trade_date] = replace(master, trade_date=fakes.trade_date + timedelta(days=1))
    mismatched = await v15_scan_service.get_cached_canonical_v16(fakes.state, fakes.trade_date)
    assert mismatched.status is v15_scan_service.CachedCanonicalV16Status.TRADE_DATE_MISMATCH
    assert mismatched.available is False
    assert mismatched.bundle is None

    coord.cache[fakes.trade_date] = replace(master, universe=())
    invalid = await v15_scan_service.get_cached_canonical_v16(fakes.state, fakes.trade_date)
    assert invalid.status is v15_scan_service.CachedCanonicalV16Status.INTEGRITY_INVALID
    assert invalid.available is False
    assert invalid.bundle is None


@pytest.mark.asyncio
async def test_failed_compute_is_not_cached_and_retry_succeeds(fakes):
    fakes.scanner.fail_times = 1

    with pytest.raises(RuntimeError, match="scan boom"):
        await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    assert fakes.scanner.scan_calls == 2
    assert bundle is not None
    assert bundle.trade_date == fakes.trade_date


@pytest.mark.asyncio
async def test_consumer_first_then_wrapper_sends_top10_and_day_gate_once(fakes):
    consumer_bundle = await v15_scan_service.get_or_compute_canonical_v16(
        fakes.state, fakes.trade_date
    )

    assert len(fakes.top10_calls) == 0
    assert len(fakes.daygate_calls) == 0

    rec1 = await v15_scan_service.run_v16_scan(fakes.state)
    assert len(fakes.top10_calls) == 1
    assert len(fakes.daygate_calls) == 1
    assert rec1 is not None

    rec2 = await v15_scan_service.run_v16_scan(fakes.state)
    assert len(fakes.top10_calls) == 1
    assert len(fakes.daygate_calls) == 1
    assert rec1 == rec2

    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    # The wrapper returns the legacy top-1 payload, not the bundle itself.
    assert isinstance(rec1, dict)
    assert rec1["stock_code"] == consumer_bundle.scan_result.recommended[0].code


@pytest.mark.asyncio
async def test_legacy_top1_payload_fields_are_compatible(fakes):
    rec = await v15_scan_service.run_v16_scan(fakes.state)

    assert rec == {
        "stock_code": "600000",
        "stock_name": "cached-name",
        "board_name": "board-a",
        "open_price": 11.0,
        "prev_close": 10.5,
        "latest_price": 12.3457,
        "lgb_score": 0.123457,
        "hot_board_count": 3,
        "final_candidates": 5,
    }
    assert not any("gate" in key for key in rec)


@pytest.mark.asyncio
async def test_waiter_cancellation_does_not_cancel_shared_compute(fakes):
    gate = asyncio.Event()
    fakes.scanner.gate = gate

    async def fetch():
        return await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    t1 = asyncio.create_task(fetch())
    t2 = asyncio.create_task(fetch())
    await asyncio.sleep(0.05)
    t1.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1

    gate.set()
    bundle = await t2

    assert fakes.rt.early_pull_calls == 1
    assert fakes.scanner.scan_calls == 1
    assert bundle is not None


@pytest.mark.asyncio
async def test_wrapper_sends_structured_error_notification(fakes):
    fakes.rt.batch_get_early_market_data = lambda codes, expected_trade_date=None: asyncio.sleep(
        0, result={}
    )

    with pytest.raises(v15_scan_service.CanonicalV16ScanError, match="0 quotes"):
        await v15_scan_service.run_v16_scan(fakes.state)

    assert len(fakes.error_calls) == 1
    title, _ = fakes.error_calls[0]
    assert "9:40行情全空" in title


@pytest.mark.asyncio
async def test_bundle_captures_data_error_codes_without_sending_them(fakes, monkeypatch):
    """When build skips codes, wrapper sends the notification exactly once."""
    # Use a six-stock universe so one missing prev_close is below the 20% halt.
    codes = ["600000", "000001", "000002", "000003", "000004", "000005"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )

    async def fake_scan(self, stock_data, clean_boards):  # noqa: ARG002
        code = sorted(stock_data)[0]
        top1 = SimpleNamespace(
            code=code,
            name="name",
            buy_price=12.345678,
            score=0.123456789,
        )
        return SimpleNamespace(
            recommended=[top1],
            all_scored=[top1],
            stock_best_board={code: "board-a"},
            stock_all_boards={code: ["board-a"]},
            step2_hot_board_count=3,
            final_candidates=5,
        )

    monkeypatch.setattr(fakes.scanner, "scan", fake_scan)

    # All codes except 600000 have prev_close.
    async def fake_prev(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes if code != "600000"}

    fakes.state.realtime_client.fetch_prev_closes = fake_prev

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    assert "600000" in bundle.failed_no_prev_close
    assert bundle.data_error_notification is not None

    # First wrapper send.
    await v15_scan_service.run_v16_scan(fakes.state)
    assert len(fakes.error_calls) == 1

    # Second wrapper must not resend the data-error notification.
    await v15_scan_service.run_v16_scan(fakes.state)
    assert len(fakes.error_calls) == 1


@pytest.mark.asyncio
async def test_concurrent_wrappers_share_publication_even_when_notifier_yields(fakes, monkeypatch):
    """Concurrent run_v16_scan calls must produce exactly one of each side effect."""
    gate = asyncio.Event()
    calls = []

    async def yielding_top10(scan_result):  # noqa: ARG001
        calls.append("top10")
        await asyncio.sleep(0.05)
        gate.set()

    async def yielding_error(title, detail):  # noqa: ARG001
        calls.append("error")
        await asyncio.sleep(0.05)

    monkeypatch.setattr(v15_scan_service, "_notify_feishu_v16_top10", yielding_top10)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", yielding_error)

    # Trigger a nonfatal data error so the data-error alert is also exercised.
    codes = ["600000", "000001", "000002", "000003", "000004", "000005"]

    async def fake_prev(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes if code != "000001"}

    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )
    fakes.state.realtime_client.fetch_prev_closes = fake_prev

    results = await asyncio.gather(
        v15_scan_service.run_v16_scan(fakes.state),
        v15_scan_service.run_v16_scan(fakes.state),
    )

    await gate.wait()
    assert results[0] == results[1]
    assert calls.count("top10") == 1
    assert calls.count("error") == 1
    assert len(fakes.daygate_calls) == 1


@pytest.mark.asyncio
async def test_concurrent_fatal_error_notification_sent_once(fakes):
    """Concurrent wrappers sharing one fatal error must notify exactly once."""

    async def empty_early(*_args, **_kwargs):
        return {}

    fakes.state.realtime_client.batch_get_early_market_data = empty_early

    async def run():
        return await v15_scan_service.run_v16_scan(fakes.state)

    results = await asyncio.gather(*[run() for _ in range(3)], return_exceptions=True)

    assert all(isinstance(r, Exception) for r in results)
    assert len(fakes.error_calls) == 1
    title, _ = fakes.error_calls[0]
    assert title == "9:40行情全空"


@pytest.mark.asyncio
async def test_prev_close_history_board_changes_alter_input_hash(fakes, monkeypatch):
    """Changing selection-relevant inputs changes input_hash; completion order does not."""

    def make_state(adapter=None):
        state = v15_scan_service.V15ScanState(
            initialized=True,
            realtime_client=type(fakes.rt)(),
            fundamentals_db=fakes.state.fundamentals_db,
            historical_adapter=adapter or fakes.state.historical_adapter,
            concept_mapper=fakes.state.concept_mapper,
            stock_filter=fakes.state.stock_filter,
            tushare_cache=None,
        )

        async def prev_close(ts_date):  # noqa: ARG001
            return {"600000": 10.5}

        state.realtime_client.fetch_prev_closes = prev_close
        return state

    base = await v15_scan_service.compute_canonical_v16_scan(make_state(), fakes.trade_date)

    # Change prev_close.
    prev_changed = make_state()

    async def prev_close_11(ts_date):  # noqa: ARG001
        return {"600000": 11.0}

    prev_changed.realtime_client.fetch_prev_closes = prev_close_11
    changed_prev = await v15_scan_service.compute_canonical_v16_scan(prev_changed, fakes.trade_date)
    assert changed_prev.input_hash != base.input_hash

    # Change a history value.
    prev_date = fakes.trade_date - timedelta(days=1)

    class HistAdapterChanged:
        async def history_quotes(
            self,
            *,
            codes: str,
            indicators: str,  # noqa: ARG002
            start_date: str,  # noqa: ARG002
            end_date: str,  # noqa: ARG002
        ):
            requested = [c.split(".")[0] for c in codes.split(",")]
            tables = []
            for code in requested:
                times = [
                    (prev_date - timedelta(days=39 - i)).strftime("%Y-%m-%d") for i in range(40)
                ]
                tables.append(
                    {
                        "thscode": f"{code}.SH",
                        "table": {
                            "time": times,
                            "open": [10.0] * 40,
                            "high": [10.5] * 40,
                            "low": [9.5] * 40,
                            "close": [20.0 + i * 0.01 for i in range(40)],
                            "volume": [1000.0] * 40,
                        },
                    }
                )
            return {"tables": tables}

    history_changed = make_state(HistAdapterChanged())
    changed_hist = await v15_scan_service.compute_canonical_v16_scan(
        history_changed, fakes.trade_date
    )
    assert changed_hist.input_hash != base.input_hash

    # Change board membership.
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-b": _clean_board("600000")}, {"600000"}),
    )
    board_changed = await v15_scan_service.compute_canonical_v16_scan(
        make_state(), fakes.trade_date
    )
    assert board_changed.input_hash != base.input_hash


@pytest.mark.asyncio
async def test_insertion_order_does_not_affect_input_hash(fakes, monkeypatch):
    """Response completion order must not change deterministic input_hash or scan semantic."""
    codes = ["000001", "600000", "300750"]

    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )

    calls_a: list[str] = []
    calls_b: list[str] = []

    async def ordered_early(codes: list[str], expected_trade_date=None):
        calls_a.extend(codes)
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.3,
                    early_high=12.4,
                    early_low=11.5,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=(
                    TushareMinuteBar(
                        stock_code=code,
                        bar_end=datetime.combine(fakes.trade_date, datetime.min.time()).replace(
                            hour=9, minute=39, tzinfo=BEIJING_TZ
                        ),
                        end_label="09:39",
                        open_price=11.0,
                        close_price=12.3,
                        high_price=12.4,
                        low_price=10.9,
                        volume=2000.0,
                        amount=24000.0,
                    ),
                ),
                source_hash=f"h-{code}",
            )
            for code in codes
        }

    async def reverse_early(codes: list[str], expected_trade_date=None):
        await asyncio.sleep(0)
        calls_b.extend(reversed(codes))
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.3,
                    early_high=12.4,
                    early_low=11.5,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=(
                    TushareMinuteBar(
                        stock_code=code,
                        bar_end=datetime.combine(fakes.trade_date, datetime.min.time()).replace(
                            hour=9, minute=39, tzinfo=BEIJING_TZ
                        ),
                        end_label="09:39",
                        open_price=11.0,
                        close_price=12.3,
                        high_price=12.4,
                        low_price=10.9,
                        volume=2000.0,
                        amount=24000.0,
                    ),
                ),
                source_hash=f"h-{code}",
            )
            for code in reversed(codes)
        }

    async def prev_for_codes(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes}

    state_a = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=fakes.rt,
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=fakes.state.historical_adapter,
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )
    state_a.realtime_client.batch_get_early_market_data = ordered_early
    state_a.realtime_client.fetch_prev_closes = prev_for_codes
    bundle_a = await v15_scan_service.compute_canonical_v16_scan(state_a, fakes.trade_date)

    state_b = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=fakes.rt,
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=fakes.state.historical_adapter,
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )
    state_b.realtime_client.batch_get_early_market_data = reverse_early
    state_b.realtime_client.fetch_prev_closes = prev_for_codes
    bundle_b = await v15_scan_service.compute_canonical_v16_scan(state_b, fakes.trade_date)

    assert calls_a == sorted(codes)
    assert calls_b == sorted(codes, reverse=True)
    assert bundle_a.input_hash == bundle_b.input_hash
    assert bundle_a._integrity_hash == bundle_b._integrity_hash
    assert list(bundle_a.stock_data) == list(bundle_b.stock_data) == sorted(codes)
    assert bundle_a.scan_result.step2_hot_board_count == bundle_b.scan_result.step2_hot_board_count


@pytest.mark.asyncio
async def test_missing_0939_is_not_cached_and_retry_succeeds(fakes, monkeypatch):
    """A one-stock bundle missing 09:39 is NOT_READY, uncached, then retry succeeds."""

    async def not_ready_early(codes: list[str], expected_trade_date=None):
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.0,
                    early_high=12.0,
                    early_low=12.0,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=(),  # no 09:39 bar
                source_hash="h-stale",
            )
            for code in codes
        }

    original = fakes.state.realtime_client.batch_get_early_market_data
    fakes.state.realtime_client.batch_get_early_market_data = not_ready_early

    with pytest.raises(v15_scan_service.CanonicalV16NotReadyError):
        await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    # Not cached: a later retry (same coordinator, no manual reset) can succeed
    # once 09:39 data is available.
    fakes.state.realtime_client.batch_get_early_market_data = original
    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    assert bundle is not None


@pytest.mark.asyncio
async def test_shared_task_cancellation_no_loop_callback_error_and_retry(fakes):
    """CancelledError must not escape the done callback; retry works."""
    gate = asyncio.Event()
    fakes.scanner.gate = gate

    async def fetch():
        return await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    t1 = asyncio.create_task(fetch())
    await asyncio.sleep(0.05)
    t1.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1

    # No loop exception handler is triggered (would fail the test if CancelledError
    # escaped the done callback). Retry succeeds.
    gate.set()
    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    assert bundle is not None


@pytest.mark.asyncio
async def test_cleanup_cancels_and_awaits_coordinator_tasks(fakes):
    """cleanup_scan_resources must cancel/await in-flight compute before stopping resources."""
    gate = asyncio.Event()
    fakes.scanner.gate = gate

    task = asyncio.create_task(
        v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    )
    await asyncio.sleep(0.05)

    await v15_scan_service.cleanup_scan_resources(fakes.state)

    assert task.cancelled()
    assert fakes.state.canonical_coordinator is None


@pytest.mark.asyncio
async def test_scanner_failure_after_nonfatal_data_errors_reports_data_alert(fakes, monkeypatch):
    """If build has nonfatal missing-data errors and scanner.scan fails, the data-error
    alert is still emitted exactly once before/with the propagated failure."""
    fakes.scanner.fail_times = 1

    codes = ["600000", "000001", "000002", "000003", "000004", "000005"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )

    async def fake_prev(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes if code != "000001"}  # one nonfatal error

    fakes.state.realtime_client.fetch_prev_closes = fake_prev

    with pytest.raises(v15_scan_service.CanonicalV16ScanError, match="scan boom"):
        await v15_scan_service.run_v16_scan(fakes.state)

    # Data-error alert is emitted unchanged, followed by the scanner fatal alert.
    assert len(fakes.error_calls) == 2
    data_title, data_detail = fakes.error_calls[0]
    assert data_title == "数据缺失报警"
    assert "000001" in data_detail
    fatal_title, fatal_detail = fakes.error_calls[1]
    assert fatal_title == "V16扫描失败"
    assert "scan boom" in fatal_detail


@pytest.mark.asyncio
async def test_bundle_outer_mappings_are_read_only(fakes):
    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    with pytest.raises(TypeError):
        bundle.quotes["new"] = object()  # type: ignore[index]
    with pytest.raises(TypeError):
        bundle.prev_closes["new"] = 1.0  # type: ignore[index]


@pytest.mark.asyncio
async def test_bundle_integrity_check_detects_mutation(fakes):
    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    # Mutate an inner object that the integrity fingerprint covers.
    quote = bundle.quotes["600000"]
    quote.open_price = 999.0

    with pytest.raises(RuntimeError, match="integrity check failed"):
        v15_scan_service._verify_bundle_integrity(bundle)


@pytest.mark.asyncio
async def test_ready_codes_only_count_current_date_0939(fakes):
    """A 09:39 bar stamped yesterday must not count as ready."""
    wrong_date = fakes.trade_date - timedelta(days=1)

    def make_bar(trade_date, label="09:39"):
        bar_end = datetime.combine(trade_date, datetime.min.time()).replace(
            hour=9, minute=39, tzinfo=BEIJING_TZ
        )
        return TushareMinuteBar(
            stock_code="600000",
            bar_end=bar_end,
            end_label=label,
            open_price=11.0,
            close_price=12.3,
            high_price=12.4,
            low_price=10.9,
            volume=2000.0,
            amount=24000.0,
        )

    def q(code: str) -> TushareQuote:
        return TushareQuote(
            stock_code=code,
            open_price=11.0,
            latest_price=12.0,
            high_price=12.5,
            low_price=10.9,
            volume=5000.0,
            amount=60000.0,
            early_close=12.3,
            early_high=12.4,
            early_low=11.5,
            early_volume=3000.0,
            volume_937=2000.0,
        )

    ready = TushareEarlyMarketData(
        quote=q("ready"),
        early_bars=(make_bar(fakes.trade_date),),
        source_hash="h-ready",
    )
    wrong_day = TushareEarlyMarketData(
        quote=q("wrong"),
        early_bars=(make_bar(wrong_date),),
        source_hash="h-wrong",
    )
    no_0939 = TushareEarlyMarketData(
        quote=q("no0939"),
        early_bars=(make_bar(fakes.trade_date, label="09:38"),),
        source_hash="h-no0939",
    )

    data = {"ready": ready, "wrong": wrong_day, "no0939": no_0939}
    assert v15_scan_service._ready_codes(data, fakes.trade_date) == {"ready"}


@pytest.mark.asyncio
async def test_compute_only_admits_ready_codes_to_scanner(fakes, monkeypatch):
    """Codes without a current-date 09:39 bar must not reach scanner.scan."""
    codes = ["000001", "000002", "000003", "000004", "000005"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )

    received_codes: set[str] = set()

    async def recording_scan(self, stock_data, clean_boards):  # noqa: ARG002
        received_codes.update(stock_data)
        code = sorted(stock_data)[0]
        top1 = SimpleNamespace(
            code=code,
            name="name",
            buy_price=12.345678,
            score=0.123456789,
        )
        return SimpleNamespace(
            recommended=[top1],
            all_scored=[top1],
            stock_best_board={code: "board-a"},
            stock_all_boards={code: ["board-a"]},
            step2_hot_board_count=3,
            final_candidates=5,
        )

    monkeypatch.setattr(fakes.scanner, "scan", recording_scan)

    async def daily_for_codes(trade_date: str):
        return {
            code: TushareDailyBar(
                stock_code=code,
                trade_date=trade_date,
                close_price=10.5,
                amount_yuan=1_000_000.0,
            )
            for code in codes
        }

    fakes.state.realtime_client.fetch_daily_bars = daily_for_codes

    def make_bars(trade_date, ready: bool):
        if not ready:
            return ()
        bar_end = datetime.combine(trade_date, datetime.min.time()).replace(
            hour=9, minute=39, tzinfo=BEIJING_TZ
        )
        return (
            TushareMinuteBar(
                stock_code="x",
                bar_end=bar_end,
                end_label="09:39",
                open_price=11.0,
                close_price=12.3,
                high_price=12.4,
                low_price=10.9,
                volume=2000.0,
                amount=24000.0,
            ),
        )

    async def mixed_early(codes: list[str], expected_trade_date=None):
        ready_codes = {"000001", "000002", "000003", "000004"}
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.3,
                    early_high=12.4,
                    early_low=11.5,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=make_bars(expected_trade_date or fakes.trade_date, code in ready_codes),
                source_hash=f"h-{code}",
            )
            for code in codes
        }

    fakes.state.realtime_client.batch_get_early_market_data = mixed_early

    await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)

    assert "000005" not in received_codes
    assert received_codes == {"000001", "000002", "000003", "000004"}


@pytest.mark.asyncio
async def test_partial_evidence_via_coordinator(fakes, monkeypatch):
    """Coordinator retains partial evidence and retry pulls only unresolved codes."""
    codes = ["000001", "000002", "000003", "000004", "600000"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )

    call_log: list[list[str]] = []

    def make_bars(trade_date, ready: bool):
        if not ready:
            return ()
        bar_end = datetime.combine(trade_date, datetime.min.time()).replace(
            hour=9, minute=39, tzinfo=BEIJING_TZ
        )
        return (
            TushareMinuteBar(
                stock_code="x",
                bar_end=bar_end,
                end_label="09:39",
                open_price=11.0,
                close_price=12.3,
                high_price=12.4,
                low_price=10.9,
                volume=2000.0,
                amount=24000.0,
            ),
        )

    async def staged_early(codes: list[str], expected_trade_date=None):
        call_log.append(list(codes))
        trade_date = expected_trade_date or fakes.trade_date
        if len(call_log) == 1:
            ready_codes = {"000001", "000002"}
        else:
            ready_codes = set(codes)
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.3,
                    early_high=12.4,
                    early_low=11.5,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=make_bars(trade_date, code in ready_codes),
                source_hash=f"h-{code}",
            )
            for code in codes
        }

    daily_calls: list[str] = []

    async def partial_daily_for_codes(trade_date: str):
        daily_calls.append(trade_date)
        return {
            code: TushareDailyBar(
                stock_code=code,
                trade_date=trade_date,
                close_price=10.5,
                amount_yuan=1_000_000.0,
            )
            for code in codes
        }

    fakes.state.realtime_client.fetch_daily_bars = partial_daily_for_codes
    fakes.state.realtime_client.batch_get_early_market_data = staged_early

    with pytest.raises(v15_scan_service.CanonicalV16NotReadyError):
        await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)

    assert set(call_log[0]) == set(codes)

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    assert bundle is not None
    assert len(call_log) == 2
    assert set(call_log[1]) == {"000003", "000004", "600000"}
    assert daily_calls == [(fakes.trade_date - timedelta(days=1)).strftime("%Y%m%d")]


@pytest.mark.asyncio
async def test_not_ready_deadline_clears_recommendation_and_alerts_once(fakes):
    """At 10:00, NOT_READY becomes a single fatal audit alert and clears recs."""
    fakes.state.today_recommendation = {"stock_code": "stale"}
    deadline = datetime.combine(fakes.trade_date, datetime.min.time()).replace(
        hour=10, minute=1, tzinfo=BEIJING_TZ
    )

    await v15_scan_service._fail_not_ready_deadline(fakes.state, fakes.trade_date, deadline)

    assert fakes.state.today_recommendation is None
    assert fakes.state.scan_error is not None
    assert len(fakes.error_calls) == 1
    title, detail = fakes.error_calls[0]
    assert title == "9:39数据未就绪截止"
    assert "10:01" in detail

    # Second call is deduplicated.
    await v15_scan_service._fail_not_ready_deadline(fakes.state, fakes.trade_date, deadline)
    assert len(fakes.error_calls) == 1


@pytest.mark.asyncio
async def test_consumer_mutation_does_not_corrupt_master(fakes):
    """Mutating a consumer-isolated bundle must not affect the cached master."""
    bundle1 = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    bundle1.quotes["600000"].open_price = 999.0

    bundle2 = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    assert bundle2.quotes["600000"].open_price == pytest.approx(11.0)
    v15_scan_service._verify_bundle_integrity(bundle2)
    master = fakes.state.canonical_coordinator.cache[fakes.trade_date]
    assert master.quotes["600000"].open_price == pytest.approx(11.0)


@pytest.mark.asyncio
async def test_publish_uses_isolated_artifact(fakes, monkeypatch):
    """The publication task works on its own deep copy of the bundle."""
    gate = asyncio.Event()

    async def mutating_top10(scan_result):  # noqa: ARG001
        scan_result.recommended[0].buy_price = 999.0
        await asyncio.sleep(0.01)
        gate.set()

    monkeypatch.setattr(v15_scan_service, "_notify_feishu_v16_top10", mutating_top10)

    await v15_scan_service.run_v16_scan(fakes.state)
    await gate.wait()

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    assert bundle.scan_result.recommended[0].buy_price == pytest.approx(12.345678)


@pytest.mark.asyncio
async def test_fingerprint_detects_stock_data_history_scan_result_changes(fakes):
    """Integrity check covers stock_data, history_df, and full scan_result."""
    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    bundle.stock_data["600000"].price_940 = 999.0
    with pytest.raises(RuntimeError, match="integrity check failed"):
        v15_scan_service._verify_bundle_integrity(bundle)

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    df = bundle.stock_data["600000"].history_df
    df.iloc[0, df.columns.get_loc("close")] = 999.0
    with pytest.raises(RuntimeError, match="integrity check failed"):
        v15_scan_service._verify_bundle_integrity(bundle)

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    bundle.scan_result.recommended[0].buy_price = 999.0
    with pytest.raises(RuntimeError, match="integrity check failed"):
        v15_scan_service._verify_bundle_integrity(bundle)

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    bundle.scan_result.step2_hot_board_count = 999
    with pytest.raises(RuntimeError, match="integrity check failed"):
        v15_scan_service._verify_bundle_integrity(bundle)


@pytest.mark.asyncio
async def test_same_date_different_fatals_are_both_sent(fakes, monkeypatch):
    """Stable incident identity means different fatals on the same date both emit."""

    async def empty_early(*_args, **_kwargs):
        return {}

    original_rt = fakes.state.realtime_client.batch_get_early_market_data
    fakes.state.realtime_client.batch_get_early_market_data = empty_early

    with pytest.raises(v15_scan_service.CanonicalV16ScanError):
        await v15_scan_service.run_v16_scan(fakes.state)

    assert len(fakes.error_calls) == 1
    assert fakes.error_calls[0][0] == "9:40行情全空"

    # Now trigger a different fatal (scanner failure) on the same date.
    fakes.state.realtime_client.batch_get_early_market_data = original_rt
    fakes.scanner.fail_times = 1

    with pytest.raises(v15_scan_service.CanonicalV16ScanError, match="scan boom"):
        await v15_scan_service.run_v16_scan(fakes.state)

    assert len(fakes.error_calls) == 2
    assert fakes.error_calls[1][0] == "V16扫描失败"


@pytest.mark.asyncio
async def test_cancellation_of_waiter_does_not_duplicate_fatal_alert(fakes, monkeypatch):
    """Cancelling one waiter while the shared compute fails must not double-send fatal."""
    gate = asyncio.Event()

    async def slow_error(title, detail):
        fakes.error_calls.append((title, detail))
        await gate.wait()

    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", slow_error)

    async def slow_empty(codes, expected_trade_date=None):  # noqa: ARG001
        await asyncio.sleep(0.05)
        return {}

    fakes.state.realtime_client.batch_get_early_market_data = slow_empty

    async def run():
        return await v15_scan_service.run_v16_scan(fakes.state)

    t1 = asyncio.create_task(run())
    t2 = asyncio.create_task(run())
    await asyncio.sleep(0.02)
    t1.cancel()

    with pytest.raises(asyncio.CancelledError):
        await t1

    gate.set()

    results = await asyncio.gather(t2, return_exceptions=True)
    assert any(isinstance(r, v15_scan_service.CanonicalV16ScanError) for r in results)
    # Allow the single notification task to complete.
    await asyncio.sleep(0.1)
    assert len(fakes.error_calls) == 1


@pytest.mark.asyncio
async def test_coordinator_state_is_bound_to_most_recent_trade_date():
    """Completed stale state is dropped while registered masters are retained."""

    coord = v15_scan_service._CanonicalV16Coordinator()
    d0 = date(2025, 12, 31)
    d1 = date(2026, 1, 1)
    d2 = date(2026, 1, 2)

    coord.cache[d0] = object()  # type: ignore[arg-type]
    coord.cache[d1] = object()  # type: ignore[arg-type]
    coord.cache[d2] = object()  # type: ignore[arg-type]
    coord.inflight[d1] = object()  # type: ignore[arg-type]
    coord.publish[d1] = object()  # type: ignore[arg-type]
    coord.publish[d0] = object()  # type: ignore[arg-type]
    coord.partial[d0] = {}
    coord.partial[d1] = {}
    coord.partial[d2] = {}
    coord.published.update({d0, d1, d2})
    coord.data_errors_sent.update({d0, d1, d2})
    coord.not_ready_alert_sent.update({d0, d1, d2})
    coord.fatal_errors_sent.add((d0, "t0", "h0"))
    coord.fatal_errors_sent.add((d1, "t1", "h1"))
    coord.fatal_errors_sent.add((d2, "t2", "h2"))

    v15_scan_service._evict_stale_dates(coord, d2)

    assert d0 not in coord.cache
    assert d0 not in coord.publish
    assert d0 not in coord.partial
    assert d2 in coord.cache
    assert d1 in coord.inflight
    assert d1 in coord.publish
    assert d1 in coord.partial
    assert d2 in coord.partial
    assert coord.published == {d1, d2}
    assert coord.data_errors_sent == {d1, d2}
    assert coord.not_ready_alert_sent == {d1, d2}
    assert coord.fatal_errors_sent == {(d1, "t1", "h1"), (d2, "t2", "h2")}


# --- Phase 1 regression tests: deterministic scanner inputs and hashes --------


@pytest.mark.asyncio
async def test_clean_board_and_member_order_are_canonicalized(fakes, monkeypatch):
    """clean_boards passed to scanner must be in a unique deterministic order.

    Before the fix, two mapper orders with the same contents produced the same
    input_hash but different scan semantics; after canonicalization both the hash
    and the full semantic must match.
    """
    codes = ["600000", "000001"]
    scanned_keys: list[list[str]] = []
    scanned_members: list[dict[str, list[str]]] = []

    async def scanning_scan(self, stock_data, clean_boards):  # noqa: ARG002
        scanned_keys.append(list(clean_boards.keys()))
        scanned_members.append({b: [c for c, _ in clean_boards[b]] for b in clean_boards})
        code = sorted(stock_data)[0]
        top1 = SimpleNamespace(
            code=code,
            name="name",
            buy_price=12.345678,
            score=0.123456789,
        )
        # Expose any remaining order sensitivity: best board is the first key.
        best_board = list(clean_boards.keys())[0]
        return SimpleNamespace(
            recommended=[top1],
            all_scored=[top1],
            stock_best_board={code: best_board},
            stock_all_boards={code: list(clean_boards.keys())},
            step2_hot_board_count=len(clean_boards),
            final_candidates=5,
        )

    monkeypatch.setattr(fakes.scanner, "scan", scanning_scan)

    async def prev_for_codes(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes}

    async def early_for_codes(codes: list[str], expected_trade_date=None):  # noqa: ARG001
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.3,
                    early_high=12.4,
                    early_low=11.5,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=(
                    TushareMinuteBar(
                        stock_code=code,
                        bar_end=datetime.combine(fakes.trade_date, datetime.min.time()).replace(
                            hour=9, minute=39, tzinfo=BEIJING_TZ
                        ),
                        end_label="09:39",
                        open_price=11.0,
                        close_price=12.3,
                        high_price=12.4,
                        low_price=10.9,
                        volume=2000.0,
                        amount=24000.0,
                    ),
                ),
                source_hash=f"h-{code}",
            )
            for code in codes
        }

    order_a = {
        "board-z": [("000001", "n1"), ("600000", "n2")],
        "board-a": [("600000", "n2")],
    }
    order_b = {
        "board-a": [("600000", "n2")],
        "board-z": [("600000", "n2"), ("000001", "n1")],
    }

    async def make_bundle(order: dict[str, list[tuple[str, str]]]):
        monkeypatch.setattr(
            fakes.scanner,
            "get_universe",
            lambda self: (order, set(codes)),
        )
        state = v15_scan_service.V15ScanState(
            initialized=True,
            realtime_client=fakes.rt,
            fundamentals_db=fakes.state.fundamentals_db,
            historical_adapter=fakes.state.historical_adapter,
            concept_mapper=fakes.state.concept_mapper,
            stock_filter=fakes.state.stock_filter,
            tushare_cache=None,
        )
        state.realtime_client.batch_get_early_market_data = early_for_codes
        state.realtime_client.fetch_prev_closes = prev_for_codes
        return await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)

    bundle_a = await make_bundle(order_a)
    bundle_b = await make_bundle(order_b)

    assert bundle_a.input_hash == bundle_b.input_hash
    assert bundle_a.scan_result.stock_best_board == bundle_b.scan_result.stock_best_board
    assert bundle_a.scan_result.step2_hot_board_count == bundle_b.scan_result.step2_hot_board_count
    # Both scanner invocations received the same canonical order.
    assert scanned_keys[-2] == ["board-a", "board-z"]
    assert scanned_keys[-1] == ["board-a", "board-z"]
    assert scanned_members[-1] == {"board-a": ["600000"], "board-z": ["000001", "600000"]}


@pytest.mark.asyncio
async def test_ready_codes_and_stock_data_follow_universe_order(fakes, monkeypatch):
    """quotes and stock_data must iterate in sorted universe order, not set order."""
    codes = ["300750", "000001", "600000"]

    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )

    async def prev_for_codes(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes}

    async def early_for_codes(codes: list[str], expected_trade_date=None):  # noqa: ARG001
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.3,
                    early_high=12.4,
                    early_low=11.5,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=(
                    TushareMinuteBar(
                        stock_code=code,
                        bar_end=datetime.combine(fakes.trade_date, datetime.min.time()).replace(
                            hour=9, minute=39, tzinfo=BEIJING_TZ
                        ),
                        end_label="09:39",
                        open_price=11.0,
                        close_price=12.3,
                        high_price=12.4,
                        low_price=10.9,
                        volume=2000.0,
                        amount=24000.0,
                    ),
                ),
                source_hash=f"h-{code}",
            )
            for code in codes
        }

    fakes.state.realtime_client.batch_get_early_market_data = early_for_codes
    fakes.state.realtime_client.fetch_prev_closes = prev_for_codes

    bundle = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)

    assert list(bundle.quotes) == sorted(codes)
    assert list(bundle.stock_data) == sorted(codes)
    assert bundle.universe == tuple(sorted(codes))


@pytest.mark.asyncio
async def test_input_hash_covers_stock_data_scalars(fakes, monkeypatch):
    """Changing only an effective stock_data scalar or source hash changes input_hash."""
    base = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)

    async def modified_early(codes: list[str], expected_trade_date=None):  # noqa: ARG001
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=99.0,
                    early_high=99.0,
                    early_low=99.0,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=(
                    TushareMinuteBar(
                        stock_code=code,
                        bar_end=datetime.combine(fakes.trade_date, datetime.min.time()).replace(
                            hour=9, minute=39, tzinfo=BEIJING_TZ
                        ),
                        end_label="09:39",
                        open_price=11.0,
                        close_price=99.0,
                        high_price=99.0,
                        low_price=99.0,
                        volume=2000.0,
                        amount=24000.0,
                    ),
                ),
                source_hash="h-changed",
            )
            for code in codes
        }

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=fakes.rt,
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=fakes.state.historical_adapter,
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )

    async def prev_for_modified(ts_date):  # noqa: ARG001
        return {"600000": 10.5}

    state.realtime_client.batch_get_early_market_data = modified_early
    state.realtime_client.fetch_prev_closes = prev_for_modified

    changed = await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)
    assert changed.input_hash != base.input_hash

    source_only = copy.copy(changed)
    source_only.stock_data["600000"].price_940 = base.stock_data["600000"].price_940
    assert v15_scan_service._stable_input_hash(
        source_only.trade_date,
        source_only.universe,
        source_only.clean_boards,
        source_only.model_sha256,
        source_only.feature_list_sha256,
        {"600000": "source-only"},
        source_only.prev_closes,
        source_only.history_raw,
        source_only.stock_data,
        source_only.failed_no_prev_close,
        source_only.failed_no_history,
        source_only.failed_build,
        source_only.skipped_new_listings,
        {},
        getattr(source_only.scan_result, "st_eligible_codes", []),
    ) != v15_scan_service._stable_input_hash(
        base.trade_date,
        base.universe,
        base.clean_boards,
        base.model_sha256,
        base.feature_list_sha256,
        {"600000": "h-600000"},
        base.prev_closes,
        base.history_raw,
        base.stock_data,
        base.failed_no_prev_close,
        base.failed_no_history,
        base.failed_build,
        base.skipped_new_listings,
        {},
        getattr(base.scan_result, "st_eligible_codes", []),
    )

    scalar_only = copy.copy(base)
    scalar_only.stock_data["600000"].price_940 += 0.5
    assert (
        v15_scan_service._stable_input_hash(
            scalar_only.trade_date,
            scalar_only.universe,
            scalar_only.clean_boards,
            scalar_only.model_sha256,
            scalar_only.feature_list_sha256,
            dict(scalar_only.early_source_hashes),
            scalar_only.prev_closes,
            scalar_only.history_raw,
            scalar_only.stock_data,
            scalar_only.failed_no_prev_close,
            scalar_only.failed_no_history,
            scalar_only.failed_build,
            scalar_only.skipped_new_listings,
            {},
            getattr(scalar_only.scan_result, "st_eligible_codes", []),
        )
        != base.input_hash
    )


@pytest.mark.asyncio
async def test_real_scanner_st_evidence_changes_input_hash_once(fakes, monkeypatch):
    """Real scanner ST response is explicit selection evidence and is queried once."""
    codes = ["000001", "600000"]
    calls: list[list[str]] = []

    async def prev_for_codes(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes}

    fakes.state.realtime_client.fetch_prev_closes = prev_for_codes

    class STFDB:
        def __init__(self, eligible: set[str]):
            self.eligible = eligible

        async def batch_filter_st(self, requested):
            calls.append(list(requested))
            return sorted(self.eligible)

    def install(eligible: set[str]):
        scanner = RealV16Scanner(
            STFDB(eligible),
            fakes.state.concept_mapper,
            fakes.state.stock_filter,
            None,
        )
        monkeypatch.setattr(
            scanner,
            "get_universe",
            lambda: ({"board-a": _clean_board(*codes)}, set(codes)),
        )
        return scanner

    monkeypatch.setattr(
        "src.strategy.strategies.v16_scanner.V16Scanner",
        lambda *_args, **_kwargs: install(set(codes)),
    )
    full = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)

    monkeypatch.setattr(
        "src.strategy.strategies.v16_scanner.V16Scanner",
        lambda *_args, **_kwargs: install({"600000"}),
    )
    filtered_state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=type(fakes.rt)(),
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=fakes.state.historical_adapter,
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )
    filtered_state.realtime_client.fetch_prev_closes = prev_for_codes
    filtered = await v15_scan_service.compute_canonical_v16_scan(filtered_state, fakes.trade_date)

    assert calls == [sorted(codes), sorted(codes)]
    assert full.scan_result.st_eligible_codes == sorted(codes)
    assert filtered.scan_result.st_eligible_codes == ["600000"]
    assert filtered.input_hash != full.input_hash


@pytest.mark.asyncio
async def test_history_normalization_before_scanner(fakes, monkeypatch):
    """History facts are normalized before builder, hash, and scanner."""
    import numpy as np
    import pandas as pd

    prev_date = fakes.trade_date - timedelta(days=1)

    def make_history(numeric_factory, time_factory):
        days = 40
        return {
            "time": [time_factory(prev_date - timedelta(days=days - i - 1)) for i in range(days)],
            "open": [numeric_factory(10.0)] * days,
            "high": [numeric_factory(10.5)] * days,
            "low": [numeric_factory(9.5)] * days,
            "close": [None] + [numeric_factory(10.0 + i * 0.01) for i in range(days - 1)],
            "volume": [numeric_factory(1000.0)] * days,
        }

    class HistAdapter:
        def __init__(self, factory=None, time_factory=lambda value: value.strftime("%Y-%m-%d")):
            self.factory = factory
            self.time_factory = time_factory

        async def history_quotes(self, *, codes, indicators, start_date, end_date):  # noqa: ARG002
            factory = self.factory or float
            return {
                "tables": [
                    {
                        "thscode": f"{code}.SH",
                        "table": make_history(factory, self.time_factory),
                    }
                    for code in codes.split(",")
                ]
            }

    def state(adapter):
        result = v15_scan_service.V15ScanState(
            initialized=True,
            realtime_client=type(fakes.rt)(),
            fundamentals_db=fakes.state.fundamentals_db,
            historical_adapter=adapter,
            concept_mapper=fakes.state.concept_mapper,
            stock_filter=fakes.state.stock_filter,
            tushare_cache=None,
        )
        return result

    plain = await v15_scan_service.compute_canonical_v16_scan(
        state(HistAdapter()), fakes.trade_date
    )
    numeric = await v15_scan_service.compute_canonical_v16_scan(
        state(HistAdapter(lambda value: np.float64(value))), fakes.trade_date
    )
    timestamped = await v15_scan_service.compute_canonical_v16_scan(
        state(
            HistAdapter(
                lambda value: np.int64(value) if float(value).is_integer() else np.float64(value),
                pd.Timestamp,
            )
        ),
        fakes.trade_date,
    )
    date_like = await v15_scan_service.compute_canonical_v16_scan(
        state(HistAdapter(lambda value: value, lambda value: value)), fakes.trade_date
    )
    np_datetime = await v15_scan_service.compute_canonical_v16_scan(
        state(
            HistAdapter(
                lambda value: value,
                lambda value: np.datetime64(value.isoformat(), "D"),
            )
        ),
        fakes.trade_date,
    )
    assert (
        {code: dict(hist) for code, hist in plain.history_raw.items()}
        == {code: dict(hist) for code, hist in numeric.history_raw.items()}
        == {code: dict(hist) for code, hist in timestamped.history_raw.items()}
    )
    pd.testing.assert_frame_equal(
        plain.stock_data["600000"].history_df,
        numeric.stock_data["600000"].history_df,
    )
    pd.testing.assert_frame_equal(
        plain.stock_data["600000"].history_df,
        timestamped.stock_data["600000"].history_df,
    )
    assert plain.input_hash == numeric.input_hash == timestamped.input_hash
    assert plain.input_hash == date_like.input_hash == np_datetime.input_hash
    assert all(bundle.history_raw["600000"]["time"][0].count("-") == 2 for bundle in (plain,))
    assert fakes.scanner.scan_calls >= 3

    for invalid_name, invalid in (
        ("nan", float("nan")),
        ("inf", float("inf")),
        ("nat", pd.NaT),
    ):
        fakes.scanner.scan_calls = 0
        adapter = HistAdapter(
            (lambda value: invalid) if invalid_name != "nat" else float,
            (lambda value: invalid) if invalid_name == "nat" else pd.Timestamp,
        )
        with pytest.raises(
            v15_scan_service.CanonicalV16ScanError,
            match="history input normalization failed",
        ):
            await v15_scan_service.compute_canonical_v16_scan(state(adapter), fakes.trade_date)
        assert fakes.scanner.scan_calls == 0


@pytest.mark.asyncio
async def test_old_stock_short_timestamp_history_fails_structured(fakes, monkeypatch):
    """Old-stock short history remains a structured error before scanner."""
    import pandas as pd

    prev_date = fakes.trade_date - timedelta(days=1)
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board("600000")}, {"600000"}),
    )

    class ShortHistAdapter:
        async def history_quotes(self, *, codes, indicators, start_date, end_date):  # noqa: ARG002
            times = [pd.Timestamp(prev_date - timedelta(days=3 - i)) for i in range(4)]
            return {
                "tables": [
                    {
                        "thscode": "600000.SH",
                        "table": {
                            "time": times,
                            "open": [10.0] * 4,
                            "high": [10.5] * 4,
                            "low": [9.5] * 4,
                            "close": [10.0] * 4,
                            "volume": [1000.0] * 4,
                        },
                    }
                ]
            }

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=type(fakes.rt)(),
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=ShortHistAdapter(),
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )
    fakes.scanner.scan_calls = 0
    with pytest.raises(v15_scan_service.CanonicalV16ScanError):
        await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)
    assert fakes.scanner.scan_calls == 0


@pytest.mark.asyncio
async def test_history_none_nan_np_timestamp_are_canonicalized(fakes, monkeypatch):
    """History arrays with None/NaN/Inf/Timestamp must not leak TypeError."""
    import numpy as np
    import pandas as pd

    codes = ["600000"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )

    prev_date = fakes.trade_date - timedelta(days=1)

    class WeirdHistAdapter:
        async def history_quotes(
            self,
            *,
            codes: str,  # noqa: ARG002
            indicators: str,  # noqa: ARG002
            start_date: str,  # noqa: ARG002
            end_date: str,  # noqa: ARG002
        ):
            return {
                "tables": [
                    {
                        "thscode": "600000.SH",
                        "table": {
                            # First row has a None close; _build_stock_data skips it.
                            # The remaining rows exercise np scalar / Timestamp / NaN handling
                            # in the hash/fingerprint path.
                            "time": [
                                (prev_date - timedelta(days=2)).strftime("%Y-%m-%d"),
                                (prev_date - timedelta(days=1)).strftime("%Y-%m-%d"),
                                pd.Timestamp(prev_date),
                            ],
                            "open": [10.0, np.float64(10.0), 10.0],
                            "high": [10.5, 10.5, 10.5],
                            "low": [9.5, 9.5, 9.5],
                            "close": [None, 10.0, 10.0],
                            "volume": [1000.0, float("nan"), 1000.0],
                        },
                    }
                ]
            }

    async def prev_for_weird(ts_date):  # noqa: ARG001
        return {"600000": 10.5}

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=fakes.rt,
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=WeirdHistAdapter(),
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )
    state.realtime_client.fetch_prev_closes = prev_for_weird

    # NaN in volume must fail closed with a structured canonical error, not TypeError.
    with pytest.raises(v15_scan_service.CanonicalV16ScanError):
        await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)


@pytest.mark.asyncio
async def test_bundle_clean_boards_are_canonical_sorted(fakes, monkeypatch):
    """The bundle must expose clean boards in deterministic sorted order."""
    codes = ["600000"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: (
            {"z-board": _clean_board(*codes), "a-board": _clean_board(*codes)},
            set(codes),
        ),
    )
    bundle = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)
    assert list(bundle.clean_boards) == ["a-board", "z-board"]
    for members in bundle.clean_boards.values():
        assert members == tuple(sorted(members))


@pytest.mark.asyncio
async def test_fingerprint_is_explicit_schema_and_covers_boards_and_early_bars(fakes):
    """The integrity fingerprint must use an explicit V16ScanResult schema and cover
    clean boards and early evidence (including bar stock_code/bar_end).
    """
    import copy
    from dataclasses import replace

    bundle = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    base_fp = v15_scan_service._bundle_fingerprint(bundle)

    # Mutate a primitive scan-result field that is part of the explicit schema.
    mutated_scan = copy.copy(bundle.scan_result)
    mutated_scan.step2_hot_board_count = 999999
    mutated_bundle = replace(bundle, scan_result=mutated_scan)
    assert v15_scan_service._bundle_fingerprint(mutated_bundle) != base_fp

    mutated_scan.st_eligible_codes = ["999999"]
    mutated_bundle_st = replace(bundle, scan_result=mutated_scan)
    assert v15_scan_service._bundle_fingerprint(mutated_bundle_st) != base_fp

    # Mutate clean board membership.
    mutated_boards = dict(bundle.clean_boards)
    board_name = list(mutated_boards)[0]
    mutated_boards[board_name] = (("999999", "fake"),)
    mutated_bundle2 = replace(bundle, clean_boards=mutated_boards)
    assert v15_scan_service._bundle_fingerprint(mutated_bundle2) != base_fp

    # Mutate an early bar's stock_code.
    early = dict(bundle.early_bars)
    bar = early["600000"][0]
    early["600000"] = (replace(bar, stock_code="999999"),)
    mutated_bundle3 = replace(bundle, early_bars=early)
    assert v15_scan_service._bundle_fingerprint(mutated_bundle3) != base_fp


# --- C1 regression tests: canonical history/date/numeric/notification semantics ----


def test_normalize_history_date_semantics() -> None:
    """date/date-only string/naive datetime/naive Timestamp/np.datetime64 are all
    interpreted as Asia/Shanghai local dates; aware datetimes/Timestamps are converted
    to Shanghai before taking the date.
    """
    # UTC 2026-08-31 16:30 == Shanghai 2026-09-01 00:30
    utc_1630 = datetime(2026, 8, 31, 16, 30, tzinfo=timezone.utc)
    sh_0030 = datetime(2026, 9, 1, 0, 30, tzinfo=BEIJING_TZ)

    history = {
        "600000": {
            "time": [
                utc_1630,
                sh_0030,
                "2026-09-01",
                pd.Timestamp("2026-09-01"),
                datetime(2026, 9, 1),
                np.datetime64("2026-09-01"),
                pd.Timestamp("2026-08-31 16:30:00", tz="UTC"),
            ],
            "open": [10.0] * 7,
            "high": [10.5] * 7,
            "low": [9.5] * 7,
            "close": [10.0] * 7,
            "volume": [1000.0] * 7,
        }
    }
    normalized = v15_scan_service._normalize_history_inputs(history)
    assert normalized["600000"]["time"] == ["2026-09-01"] * 7


def test_canonical_json_value_naive_timestamp_localizes_to_shanghai() -> None:
    """Naive pd.Timestamp must not raise and must localize to Asia/Shanghai."""
    ts = pd.Timestamp("2026-09-01")
    dt = datetime(2026, 9, 1)
    expected = "2026-09-01T00:00:00+08:00"
    assert v15_scan_service._canonical_json_value(ts) == expected
    assert v15_scan_service._canonical_json_value(dt) == expected


def test_canonical_json_value_rejects_nat() -> None:
    """pd.NaT and np.datetime64('NaT') must be rejected, not encoded as strings."""
    with pytest.raises(ValueError, match="NaT"):
        v15_scan_service._canonical_json_value(pd.NaT)
    with pytest.raises(ValueError, match="NaT"):
        v15_scan_service._canonical_json_value(np.datetime64("NaT"))


def test_normalize_history_accepts_decimal_and_numeric_strings() -> None:
    """Decimal and numeric strings acceptable to float() must be normalized."""
    history = {
        "600000": {
            "time": ["2026-08-31", "2026-09-01"],
            "open": [Decimal("10.0"), "10.0"],
            "high": [10.5, 10.5],
            "low": [9.5, 9.5],
            "close": [10.0, 10.0],
            "volume": [1000, "1000"],
        }
    }
    normalized = v15_scan_service._normalize_history_inputs(history)
    assert normalized["600000"]["open"] == [10.0, 10.0]
    assert normalized["600000"]["volume"] == [1000.0, 1000.0]


def test_normalize_history_rejects_bool_and_non_numeric_strings() -> None:
    """bool and non-numeric strings must still be rejected."""
    for bad_value in (True, "not-a-number"):
        history = {
            "600000": {
                "time": ["2026-09-01"],
                "open": [bad_value],
                "high": [10.5],
                "low": [9.5],
                "close": [10.0],
                "volume": [1000.0],
            }
        }
        with pytest.raises(ValueError):
            v15_scan_service._normalize_history_inputs(history)


@pytest.mark.asyncio
async def test_history_none_values_are_skipped_successfully(fakes, monkeypatch):
    """A None value in a numeric column is skipped by _build_stock_data; scan succeeds."""
    codes = ["600000"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )
    prev_date = fakes.trade_date - timedelta(days=1)

    class NoneHistAdapter:
        async def history_quotes(self, *, codes, indicators, start_date, end_date):  # noqa: ARG002
            return {
                "tables": [
                    {
                        "thscode": "600000.SH",
                        "table": {
                            # 40 rows, one None close in the middle.
                            "time": [
                                (prev_date - timedelta(days=39 - i)).strftime("%Y-%m-%d")
                                for i in range(40)
                            ],
                            "open": [10.0] * 40,
                            "high": [10.5] * 40,
                            "low": [9.5] * 40,
                            "close": [10.0] * 19 + [None] + [10.0] * 20,
                            "volume": [1000.0] * 40,
                        },
                    }
                ]
            }

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=type(fakes.rt)(),
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=NoneHistAdapter(),
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )

    async def prev_for_none(ts_date):  # noqa: ARG001
        return {"600000": 10.5}

    state.realtime_client.fetch_prev_closes = prev_for_none

    bundle = await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)
    assert bundle is not None
    assert "600000" in bundle.stock_data


@pytest.mark.asyncio
async def test_history_nan_inf_nat_structured_error_has_notification(fakes, monkeypatch):
    """NaN/±Inf/NaT in history must raise CanonicalV16ScanError with non-empty
    notify_title and notify_detail so the done callback emits exactly one alert.
    """
    codes = ["600000"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )
    for bad_value, time_value in (
        (float("nan"), "2026-08-31"),
        (float("inf"), "2026-08-31"),
        (float("-inf"), "2026-08-31"),
        (10.0, pd.NaT),
    ):

        class BadHistAdapter:
            async def history_quotes(self, *, codes, indicators, start_date, end_date):  # noqa: ARG002
                return {
                    "tables": [
                        {
                            "thscode": "600000.SH",
                            "table": {
                                "time": [time_value],
                                "open": [10.0],
                                "high": [10.5],
                                "low": [9.5],
                                "close": [bad_value],
                                "volume": [1000.0],
                            },
                        }
                    ]
                }

        state = v15_scan_service.V15ScanState(
            initialized=True,
            realtime_client=type(fakes.rt)(),
            fundamentals_db=fakes.state.fundamentals_db,
            historical_adapter=BadHistAdapter(),
            concept_mapper=fakes.state.concept_mapper,
            stock_filter=fakes.state.stock_filter,
            tushare_cache=None,
        )

        async def prev_for_bad(ts_date):  # noqa: ARG001
            return {"600000": 10.5}

        state.realtime_client.fetch_prev_closes = prev_for_bad

        with pytest.raises(v15_scan_service.CanonicalV16ScanError) as exc_info:
            await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)
        assert exc_info.value.notify_title == "V16 history normalization failed"
        assert "normalization failures: 1" in exc_info.value.notify_detail
        assert fakes.scanner.scan_calls == 0


def test_mixed_none_and_nan_is_rejected_without_row_skip() -> None:
    """A nullable row must not make a later non-finite value disappear."""
    history = {
        "600000": {
            "time": ["2026-08-30", "2026-08-31"],
            "open": [10.0, 10.0],
            "high": [10.5, 10.5],
            "low": [9.5, 9.5],
            "close": [None, float("nan")],
            "volume": [1000.0, 1000.0],
        }
    }
    with pytest.raises(ValueError, match="non-finite close"):
        v15_scan_service._normalize_history_inputs(history)


@pytest.mark.asyncio
async def test_minority_history_normalization_failure_is_ticket_scoped(fakes, monkeypatch):
    """One bad ticket is reported and skipped; five good tickets still scan once."""
    codes = ["000001", "000002", "000003", "000004", "000005", "600000"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )
    prev_date = fakes.trade_date - timedelta(days=1)

    class MixedHistAdapter:
        async def history_quotes(self, *, codes, indicators, start_date, end_date):  # noqa: ARG002
            tables = []
            for code in [value.split(".")[0] for value in codes.split(",")]:
                close_values = [10.0 + i * 0.01 for i in range(40)]
                if code == "000001":
                    close_values[0] = None
                    close_values[1] = float("nan")
                tables.append(
                    {
                        "thscode": f"{code}.SH",
                        "table": {
                            "time": [
                                (prev_date - timedelta(days=39 - i)).strftime("%Y-%m-%d")
                                for i in range(40)
                            ],
                            "open": [10.0] * 40,
                            "high": [10.5] * 40,
                            "low": [9.5] * 40,
                            "close": close_values,
                            "volume": [1000.0] * 40,
                        },
                    }
                )
            return {"tables": tables}

    async def prev_for_codes(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes}

    fakes.state.realtime_client.fetch_prev_closes = prev_for_codes
    original_state_adapter = fakes.state.historical_adapter
    fakes.state.historical_adapter = MixedHistAdapter()
    fakes.scanner.scan_calls = 0
    try:
        bundle = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)
    finally:
        fakes.state.historical_adapter = original_state_adapter

    assert len(bundle.stock_data) == 5
    assert "000001" not in bundle.stock_data
    assert len(bundle.failed_no_history) == 1
    assert bundle.failed_no_history[0].startswith("000001: 000001 history input")
    assert bundle.data_error_notification is not None
    assert fakes.scanner.scan_calls == 1


async def _compute_whole_ticket_history_case(fakes, monkeypatch, total_tickets, failed_codes):
    codes = [f"60000{index}" for index in range(total_tickets)]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )
    received: list[list[str]] = []

    async def recording_scan(self, stock_data, clean_boards):  # noqa: ARG002
        type(self).scan_calls += 1
        received.append(sorted(stock_data))
        top1 = SimpleNamespace(
            code=sorted(stock_data)[0],
            name="cached-name",
            buy_price=12.345678,
            score=0.123456789,
        )
        return SimpleNamespace(
            recommended=[top1],
            all_scored=[top1],
            stock_best_board={code: "board-a" for code in stock_data},
            stock_all_boards={code: ["board-a"] for code in stock_data},
            step2_hot_board_count=1,
            final_candidates=1,
            stock_cci={},
            stock_early_vol={},
        )

    monkeypatch.setattr(fakes.scanner, "scan", recording_scan)
    prev_date = fakes.trade_date - timedelta(days=1)

    class PartialHistAdapter:
        async def history_quotes(self, *, codes, indicators, start_date, end_date):  # noqa: ARG002
            tables = []
            for code in [value.split(".")[0] for value in codes.split(",")]:
                invalid = code in failed_codes
                tables.append(
                    {
                        "thscode": f"{code}.SH",
                        "table": {
                            "time": [
                                (prev_date - timedelta(days=39 - i)).strftime("%Y-%m-%d")
                                for i in range(40)
                            ],
                            "open": [10.0] * 40,
                            "high": [10.5] * 40,
                            "low": [9.5] * 40,
                            "close": (
                                [float("nan")] * 40
                                if invalid
                                else [10.0 + i * 0.01 for i in range(40)]
                            ),
                            "volume": [1000.0] * 40,
                        },
                    }
                )
            return {"tables": tables}

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=type(fakes.rt)(),
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=PartialHistAdapter(),
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )

    async def prev_for_codes(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes}

    state.realtime_client.fetch_prev_closes = prev_for_codes
    fakes.scanner.scan_calls = 0
    return state, received


@pytest.mark.asyncio
async def test_one_of_five_whole_ticket_history_failures_is_exactly_at_threshold(
    fakes, monkeypatch
):
    state, received = await _compute_whole_ticket_history_case(fakes, monkeypatch, 5, {"600000"})
    bundle = await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)

    assert len(bundle.stock_data) == 4
    assert "600000" not in bundle.stock_data
    assert len(bundle.failed_no_history) == 1
    assert bundle.failed_no_history[0].startswith("600000: 600000 history input")
    assert bundle.data_error_notification is not None
    assert fakes.scanner.scan_calls == 1
    assert len(received[0]) == 4
    assert "600000" not in received[0]


@pytest.mark.asyncio
async def test_one_of_six_whole_ticket_history_failure_is_minority_success(fakes, monkeypatch):
    state, received = await _compute_whole_ticket_history_case(fakes, monkeypatch, 6, {"600000"})
    bundle = await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)

    assert sorted(bundle.stock_data) == [f"60000{index}" for index in range(1, 6)]
    assert bundle.failed_no_history == (
        "600000: 600000 history input normalization failed: non-finite close",
    )
    assert bundle.data_error_notification is not None
    assert fakes.scanner.scan_calls == 1
    assert received[0] == sorted(bundle.stock_data)


@pytest.mark.asyncio
async def test_two_of_six_whole_ticket_history_failures_stop_before_scanner(fakes, monkeypatch):
    state, received = await _compute_whole_ticket_history_case(
        fakes, monkeypatch, 6, {"600000", "600001"}
    )

    with pytest.raises(v15_scan_service.CanonicalV16ScanError) as exc_info:
        await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)

    assert exc_info.value.notify_title
    assert exc_info.value.notify_detail
    assert "history coverage 4/6" in str(exc_info.value)
    assert fakes.scanner.scan_calls == 0
    assert received == []


@pytest.mark.asyncio
@pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
async def test_full_compute_rejects_mixed_none_and_non_finite_old_history(
    fakes, monkeypatch, non_finite
):
    """Origin None skipping cannot mask a later non-finite value."""
    codes = ["600000"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )
    prev_date = fakes.trade_date - timedelta(days=1)
    close_values = [10.0 + i * 0.01 for i in range(40)]
    close_values[10] = None
    close_values[11] = non_finite

    class MixedHistAdapter:
        async def history_quotes(self, *, codes, indicators, start_date, end_date):  # noqa: ARG002
            return {
                "tables": [
                    {
                        "thscode": "600000.SH",
                        "table": {
                            "time": [
                                (prev_date - timedelta(days=39 - i)).strftime("%Y-%m-%d")
                                for i in range(40)
                            ],
                            "open": [10.0] * 40,
                            "high": [10.5] * 40,
                            "low": [9.5] * 40,
                            "close": close_values,
                            "volume": [1000.0] * 40,
                        },
                    }
                ]
            }

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=type(fakes.rt)(),
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=MixedHistAdapter(),
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )

    async def prev_for_mixed(ts_date):  # noqa: ARG001
        return {"600000": 10.5}

    state.realtime_client.fetch_prev_closes = prev_for_mixed
    fakes.scanner.scan_calls = 0
    with pytest.raises(v15_scan_service.CanonicalV16ScanError) as exc_info:
        await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)

    assert exc_info.value.notify_title == "V16 history normalization failed"
    assert "non-finite close" in exc_info.value.notify_detail
    assert fakes.scanner.scan_calls == 0


@pytest.mark.asyncio
async def test_returned_bundle_deeply_isolates_cached_and_sibling_state(fakes):
    """Mutating nested consumer state cannot affect the master or a sibling."""
    first = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    second = await v15_scan_service.get_or_compute_canonical_v16(fakes.state, fakes.trade_date)
    master = fakes.state.canonical_coordinator.cache[fakes.trade_date]

    assert first.stock_data["600000"] is not second.stock_data["600000"]
    assert first.stock_data["600000"] is not master.stock_data["600000"]
    assert first.stock_data["600000"].history_df is not second.stock_data["600000"].history_df
    assert first.stock_data["600000"].history_df is not master.stock_data["600000"].history_df
    assert first.scan_result is not second.scan_result
    assert first.scan_result is not master.scan_result
    assert first.history_raw is not second.history_raw
    assert first.history_raw is not master.history_raw
    assert first.history_raw["600000"] is not second.history_raw["600000"]
    assert first.history_raw["600000"] is not master.history_raw["600000"]
    assert first.history_raw["600000"]["time"] is not second.history_raw["600000"]["time"]
    assert first.history_raw["600000"]["time"] is not master.history_raw["600000"]["time"]

    original_open = first.stock_data["600000"].history_df.loc[0, "open"]
    first.stock_data["600000"].history_df.loc[0, "open"] = original_open + 999.0
    first.history_raw["600000"]["time"][0] = "1900-01-01"
    first.scan_result.recommended[0].name = "mutated-consumer"

    assert second.stock_data["600000"].history_df.loc[0, "open"] == original_open
    assert master.stock_data["600000"].history_df.loc[0, "open"] == original_open
    assert second.history_raw["600000"]["time"][0] != "1900-01-01"
    assert master.history_raw["600000"]["time"][0] != "1900-01-01"
    assert second.scan_result.recommended[0].name == "cached-name"
    assert master.scan_result.recommended[0].name == "cached-name"


@pytest.mark.asyncio
async def test_fingerprint_covers_only_data_error_notification_change(fakes):
    from dataclasses import replace

    bundle = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)
    base = v15_scan_service._bundle_fingerprint(bundle)
    changed = replace(
        bundle,
        data_error_notification=("V16 data error", "operator review required"),
    )
    assert v15_scan_service._bundle_fingerprint(changed) != base
    assert changed.scan_result == bundle.scan_result
    assert changed.stock_data == bundle.stock_data


@pytest.mark.asyncio
async def test_st_evidence_mutation_uses_independent_pre_mutation_results(fakes):
    bundle = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)
    full_scan = copy.deepcopy(bundle.scan_result)
    filtered_scan = copy.deepcopy(bundle.scan_result)
    full_scan.st_eligible_codes = ["600000"]
    filtered_scan.st_eligible_codes = []

    assert full_scan is not filtered_scan
    for field_name, full_value in full_scan.__dict__.items():
        if field_name != "st_eligible_codes":
            assert filtered_scan.__dict__[field_name] == full_value

    full_bundle = replace(bundle, scan_result=full_scan)
    filtered_bundle = replace(bundle, scan_result=filtered_scan)
    full_fingerprint = v15_scan_service._bundle_fingerprint(full_bundle)
    filtered_fingerprint = v15_scan_service._bundle_fingerprint(filtered_bundle)
    assert full_fingerprint != filtered_fingerprint

    common_hash_args = (
        bundle.trade_date,
        bundle.universe,
        bundle.clean_boards,
        bundle.model_sha256,
        bundle.feature_list_sha256,
        bundle.early_source_hashes,
        bundle.prev_closes,
        bundle.history_raw,
        bundle.stock_data,
        bundle.failed_no_prev_close,
        bundle.failed_no_history,
        bundle.failed_build,
        bundle.skipped_new_listings,
        {},
    )
    assert v15_scan_service._stable_input_hash(*common_hash_args, ["600000"]) != (
        v15_scan_service._stable_input_hash(*common_hash_args, [])
    )


@pytest.mark.asyncio
async def test_source_only_change_changes_both_canonical_identities(fakes, monkeypatch):
    base = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)
    original_early = fakes.rt.batch_get_early_market_data

    async def source_only_early(codes, expected_trade_date=None):  # noqa: ARG001
        data = await original_early(codes, expected_trade_date)
        data["600000"] = replace(data["600000"], source_hash="source-only")
        return data

    monkeypatch.setattr(
        fakes.state.realtime_client,
        "batch_get_early_market_data",
        source_only_early,
    )
    changed = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)

    def non_source_facts(bundle):
        stock_facts = {}
        for code, stock in sorted(bundle.stock_data.items()):
            stock_facts[code] = (
                stock.code,
                stock.name,
                stock.open_price,
                stock.prev_close,
                stock.price_940,
                stock.high_940,
                stock.low_940,
                stock.volume_940,
                stock.volume_937,
                stock.avg_daily_volume,
                stock.trend_5d,
                stock.trend_10d,
                stock.avg_daily_return_20d,
                stock.volatility_20d,
                stock.consecutive_up_days,
                stock.history_df.to_dict(orient="records"),
            )
        return (
            bundle.universe,
            {board: tuple(members) for board, members in sorted(bundle.clean_boards.items())},
            {code: quote.__dict__ for code, quote in sorted(bundle.quotes.items())},
            {code: tuple(bars) for code, bars in sorted(bundle.early_bars.items())},
            dict(bundle.prev_closes),
            {
                code: {field: list(values) for field, values in sorted(history.items())}
                for code, history in sorted(bundle.history_raw.items())
            },
            stock_facts,
            bundle.failed_no_prev_close,
            bundle.failed_no_history,
            bundle.failed_build,
            bundle.skipped_new_listings,
            copy.deepcopy(bundle.scan_result.__dict__),
            bundle.model_sha256,
            bundle.feature_list_sha256,
            bundle.data_error_notification,
        )

    assert non_source_facts(changed) == non_source_facts(base)
    assert changed.early_source_hashes == {"600000": "source-only"}
    assert changed.quotes == base.quotes
    assert changed.early_bars == base.early_bars
    assert changed.history_raw == base.history_raw
    assert changed.input_hash != base.input_hash
    assert changed._integrity_hash != base._integrity_hash


@pytest.mark.asyncio
@pytest.mark.parametrize("valid_rows", [5, 6, 13, 14, 36])
async def test_origin_low_row_history_still_builds_scanner_input(fakes, monkeypatch, valid_rows):
    """Origin V16 permits 5-36 rows and computes reduced-window features."""
    codes = ["600000"]
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )
    first_date = fakes.trade_date - timedelta(days=100)

    class LowRowHistAdapter:
        async def history_quotes(self, *, codes, indicators, start_date, end_date):  # noqa: ARG002
            times = [
                (first_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(valid_rows - 1)
            ] + [(fakes.trade_date - timedelta(days=1)).isoformat()]
            return {
                "tables": [
                    {
                        "thscode": "600000.SH",
                        "table": {
                            "time": times,
                            "open": [10.0] * valid_rows,
                            "high": [10.5] * valid_rows,
                            "low": [9.5] * valid_rows,
                            "close": [10.0 + i * 0.01 for i in range(valid_rows)],
                            "volume": [1000.0] * valid_rows,
                        },
                    }
                ]
            }

    state = v15_scan_service.V15ScanState(
        initialized=True,
        realtime_client=type(fakes.rt)(),
        fundamentals_db=fakes.state.fundamentals_db,
        historical_adapter=LowRowHistAdapter(),
        concept_mapper=fakes.state.concept_mapper,
        stock_filter=fakes.state.stock_filter,
        tushare_cache=None,
    )

    async def prev_for_low_rows(ts_date):  # noqa: ARG001
        return {"600000": 10.5}

    state.realtime_client.fetch_prev_closes = prev_for_low_rows
    received: list[list[str]] = []

    class LowRowSTFDB:
        async def batch_filter_st(self, codes):  # noqa: ARG002
            return list(codes)

    real_scanner = RealV16Scanner(
        LowRowSTFDB(),
        fakes.state.concept_mapper,
        fakes.state.stock_filter,
        None,
    )
    monkeypatch.setattr(
        real_scanner,
        "get_universe",
        lambda: ({"board-a": _clean_board(*codes)}, set(codes)),
    )
    monkeypatch.setattr(
        real_scanner,
        "_step2_hot_boards",
        lambda clean_boards, stock_data: (
            {"board-a": ["600000"]},
            {"600000": ["board-a"]},
            0,
            {"board-a": 5.0},
            {"board-a": 5.0},
        ),
    )
    monkeypatch.setattr(
        real_scanner,
        "_step5_volume_filter",
        lambda candidates, stock_data: candidates,
    )

    async def keep_reversal(candidates, stock_data):  # noqa: ARG002
        return candidates

    monkeypatch.setattr(real_scanner, "_step6_reversal_filter", keep_reversal)
    monkeypatch.setattr(
        real_scanner,
        "_step6_5_limit_up_filter",
        lambda candidates, stock_data: candidates,
    )
    monkeypatch.setattr(
        real_scanner,
        "_step7_lgbrank",
        lambda candidates, stock_data: [
            SimpleNamespace(
                code="600000",
                name="low-history",
                buy_price=12.0,
                score=1.0,
            )
        ],
    )

    async def recording_real_scan(stock_data, clean_boards):  # noqa: ARG002
        received.append(sorted(stock_data))
        return await RealV16Scanner.scan(real_scanner, stock_data, clean_boards)

    monkeypatch.setattr(real_scanner, "scan", recording_real_scan)
    monkeypatch.setattr(
        "src.strategy.strategies.v16_scanner.V16Scanner",
        lambda *_args, **_kwargs: real_scanner,
    )

    bundle = await v15_scan_service.compute_canonical_v16_scan(state, fakes.trade_date)
    assert len(bundle.stock_data["600000"].history_df) == valid_rows
    assert received == [["600000"]]
    if valid_rows < 14:
        assert bundle.scan_result.stock_cci == {}
    else:
        assert set(bundle.scan_result.stock_cci) == {"600000"}
        assert np.isfinite(bundle.scan_result.stock_cci["600000"])


@pytest.mark.asyncio
@pytest.mark.parametrize("ready_n", [79, 80])
async def test_readiness_boundary_79_of_100_not_ready_80_of_100_scans(
    fakes, monkeypatch, ready_n: int
):
    """The canonical 09:39 readiness gate is exactly 80%: 79/100 fails, 80/100 scans."""
    codes = [f"60{index:04d}" for index in range(100)]
    ready_codes = set(codes[:ready_n])
    monkeypatch.setattr(
        fakes.scanner,
        "get_universe",
        lambda self: ({"board-a": _clean_board(*codes)}, set(codes)),
    )

    async def prev_for_codes(ts_date):  # noqa: ARG001
        return {code: 10.5 for code in codes}

    fakes.state.realtime_client.fetch_prev_closes = prev_for_codes

    def make_bars(code: str, ready: bool):
        if not ready:
            return ()
        bar_end = datetime.combine(fakes.trade_date, datetime.min.time()).replace(
            hour=9, minute=39, tzinfo=BEIJING_TZ
        )
        return (
            TushareMinuteBar(
                stock_code=code,
                bar_end=bar_end,
                end_label="09:39",
                open_price=11.0,
                close_price=12.3,
                high_price=12.4,
                low_price=10.9,
                volume=2000.0,
                amount=24000.0,
            ),
        )

    async def boundary_early(requested: list[str], expected_trade_date=None):
        return {
            code: TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.3,
                    early_high=12.4,
                    early_low=11.5,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=make_bars(code, code in ready_codes),
                source_hash=f"h-{code}",
            )
            for code in requested
        }

    fakes.state.realtime_client.batch_get_early_market_data = boundary_early
    fakes.scanner.scan_calls = 0

    if ready_n < 80:
        with pytest.raises(v15_scan_service.CanonicalV16NotReadyError) as exc_info:
            await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)
        assert "79/100" in str(exc_info.value)
        assert fakes.scanner.scan_calls == 0
    else:
        bundle = await v15_scan_service.compute_canonical_v16_scan(fakes.state, fakes.trade_date)
        assert fakes.scanner.scan_calls == 1
        assert sorted(bundle.stock_data) == sorted(ready_codes)


_REAL_SEED_TRADE_DATE = date(2026, 8, 31)
_REAL_SEED_LABELS = ("09:25", "09:30", *(f"09:{minute:02d}" for minute in range(31, 40)))


def _real_seed_raw_bars(code: str, index: int, *, bump_939: float = 0.0):
    """Deterministic raw 09:25/09:30/09:31..09:39 minute bars for one code."""
    base = 12.0 + index * 0.05
    bars = []
    for label in _REAL_SEED_LABELS:
        if label == "09:25":
            open_price = close = base
        elif label == "09:30":
            open_price, close = base, base + 0.02
        else:
            open_price = base + 0.02 + (int(label[3:]) - 31) * 0.01
            close = open_price + 0.01
        if label == "09:39":
            close += bump_939
        hour, minute = int(label[:2]), int(label[3:])
        volume = 1000.0 + index * 100.0 + minute
        bars.append(
            TushareMinuteBar(
                stock_code=code,
                bar_end=datetime.combine(
                    _REAL_SEED_TRADE_DATE, time(hour, minute), tzinfo=BEIJING_TZ
                ),
                end_label=label,
                open_price=open_price,
                close_price=close,
                high_price=max(open_price, close) + 0.01,
                low_price=min(open_price, close) - 0.01,
                volume=volume,
                amount=volume * close,
            )
        )
    return tuple(bars)


class _RealSeedFundamentals:
    async def batch_filter_st(self, codes):
        return list(codes)

    async def batch_get_fundamentals(self, codes):  # noqa: ARG002
        return {}

    async def close(self):
        pass


class _RealSeedBombRT:
    """Every realtime method is a bomb; any touch is recorded and fails."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str):
        async def _bomb(*_args, **_kwargs):
            self.calls.append(name)
            raise AssertionError(f"seeded compute touched realtime method {name}")

        return _bomb


@pytest.mark.asyncio
async def test_seeded_historical_compute_reproduces_live_normalized_canonical_scan(monkeypatch):
    """A persisted-raw seed reproduces the live-normalized canonical scan exactly.

    The real ``compute_canonical_v16_scan`` runs the real V16Scanner with the
    real LGBRank model twice over the same raw 09:25/09:30/09:31..09:39 bars:
    once with the live client normalizing them on the fly, once hydrated from a
    repository/historical seed with every realtime method bombed.  Static inputs
    (universe, boards, prev closes, history, names, calendar) are pinned by
    overrides so only the early evidence path differs.
    """
    codes = tuple(f"6000{index:02d}" for index in range(10))
    universe = tuple(sorted(codes))
    names = {code: f"name-{code}" for code in codes}
    clean_boards = {"board-a": tuple((code, names[code]) for code in universe)}
    prev_closes = {code: 12.0 for code in codes}
    calendar = tuple(
        sorted(
            {
                *(date(2026, 8, 28) - timedelta(days=offset) for offset in range(45)),
                _REAL_SEED_TRADE_DATE,
                date(2026, 9, 1),
                date(2026, 9, 2),
            }
        )
    )
    history_days = [
        (date(2026, 8, 28) - timedelta(days=39 - offset)).isoformat() for offset in range(40)
    ]
    history_raw = {
        code: {
            "time": history_days,
            "open": [11.5] * 40,
            "high": [11.6] * 40,
            "low": [11.4] * 40,
            "close": [11.5 + offset * 0.01 for offset in range(40)],
            "volume": [1_000_000.0] * 40,
        }
        for code in codes
    }

    def normalized(bump_code: str | None = None) -> dict[str, TushareEarlyMarketData]:
        return {
            code: tushare_minute_bars_to_early_market_data(
                code,
                _real_seed_raw_bars(code, index, bump_939=0.5 if code == bump_code else 0.0),
                _REAL_SEED_TRADE_DATE,
            )
            for index, code in enumerate(codes)
        }

    class _LiveNormalizedRT(_RealSeedBombRT):
        def __init__(self, early: dict[str, TushareEarlyMarketData]) -> None:
            super().__init__()
            self._early = early

        async def batch_get_early_market_data(self, requested, expected_trade_date=None):
            assert expected_trade_date == _REAL_SEED_TRADE_DATE
            self.calls.append("batch_get_early_market_data")
            return {code: self._early[code] for code in requested}

    # Board plumbing and orthogonal tail filters are pinned, but the scanner
    # genuinely builds stock_data from the normalized raw bars, applies the ST /
    # gain / price filters, and scores with the real LGBRank model.
    def pinned_hot_boards(self, boards_arg, stock_data):  # noqa: ARG001
        return (
            {
                board: sorted(code for code, _name in members)
                for board, members in boards_arg.items()
            },
            {code: ["board-a"] for code in universe},
            0,
            {"board-a": 1.0},
            {"board-a": 1.0},
        )

    monkeypatch.setattr(RealV16Scanner, "_step2_hot_boards", pinned_hot_boards)
    monkeypatch.setattr(
        RealV16Scanner, "_step5_volume_filter", lambda self, candidates, stock_data: candidates
    )

    async def keep_candidates(self, candidates, stock_data):  # noqa: ARG001
        return candidates

    monkeypatch.setattr(RealV16Scanner, "_step6_reversal_filter", keep_candidates)
    monkeypatch.setattr(
        RealV16Scanner, "_step6_5_limit_up_filter", lambda self, candidates, stock_data: candidates
    )
    monkeypatch.setattr(
        RealV16Scanner,
        "_step6_6_upper_shadow_filter",
        lambda self, candidates, stock_data: candidates,
    )

    async def no_name_refresh(*_args, **_kwargs):
        pass

    monkeypatch.setattr(v15_scan_service, "_refresh_top10_names", no_name_refresh)

    def make_state(rt_client) -> v15_scan_service.V15ScanState:
        return v15_scan_service.V15ScanState(
            initialized=True,
            realtime_client=rt_client,
            fundamentals_db=_RealSeedFundamentals(),
            historical_adapter=object(),
            concept_mapper=object(),
            stock_filter=object(),
            tushare_cache=None,
        )

    static_inputs = {
        "universe_override": universe,
        "clean_boards_override": clean_boards,
        "prev_closes_override": prev_closes,
        "prior_daily_override": {
            code: TushareDailyBar(
                stock_code=code,
                trade_date=date(2026, 8, 28).strftime("%Y%m%d"),
                close_price=prev_closes[code],
                amount_yuan=1_000_000.0,
            )
            for code in codes
        },
        "st_eligible_codes_override": universe,
        "history_raw_override": history_raw,
        "names_override": names,
        "calendar_override": calendar,
    }

    # Baseline: the live client normalizes the raw bars on the fly.
    live_rt = _LiveNormalizedRT(normalized())
    baseline = await v15_scan_service.compute_canonical_v16_scan(
        make_state(live_rt), _REAL_SEED_TRADE_DATE, **static_inputs
    )
    assert live_rt.calls == ["batch_get_early_market_data"]

    # Seeded: the same raw bars arrive through the repository/historical seed
    # path (the same shared normalizer) with realtime fetches forbidden.
    bomb_rt = _RealSeedBombRT()
    seeded = await v15_scan_service.compute_canonical_v16_scan(
        make_state(bomb_rt),
        _REAL_SEED_TRADE_DATE,
        early_data_seed=normalized(),
        allow_realtime_fetch=False,
        **static_inputs,
    )
    assert bomb_rt.calls == []

    assert seeded.input_hash == baseline.input_hash
    assert seeded.early_source_hashes == baseline.early_source_hashes

    def critical_stock_fields(bundle):
        return {
            code: (
                sd.open_price,
                sd.prev_close,
                sd.price_940,
                sd.high_940,
                sd.low_940,
                sd.volume_937,
                sd.volume_940,
                sd.avg_daily_volume,
                len(sd.history_df),
            )
            for code, sd in bundle.stock_data.items()
        }

    assert critical_stock_fields(seeded) == critical_stock_fields(baseline)

    def ordered_top10(bundle):
        return [
            (
                rank,
                stock.code,
                stock.score,
                stock.buy_price,
                tuple(bundle.scan_result.stock_all_boards.get(stock.code, ())),
            )
            for rank, stock in enumerate(bundle.scan_result.recommended, start=1)
        ]

    baseline_top10 = ordered_top10(baseline)
    assert len(baseline_top10) == 10
    # The real model genuinely ranks: scores are finite and not all identical.
    assert len({entry[2] for entry in baseline_top10}) > 1
    assert ordered_top10(seeded) == baseline_top10

    # The outputs are a function of the raw bars, not fixed: perturbing one
    # raw 09:39 close changes the canonical input hash.
    perturbed_rt = _LiveNormalizedRT(normalized(bump_code=codes[0]))
    perturbed = await v15_scan_service.compute_canonical_v16_scan(
        make_state(perturbed_rt), _REAL_SEED_TRADE_DATE, **static_inputs
    )
    assert perturbed.input_hash != baseline.input_hash
