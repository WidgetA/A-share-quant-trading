from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.data.clients.tushare_realtime import (
    TushareDailyBar,
    TushareEarlyMarketData,
    TushareMinuteBar,
    TushareQuote,
    TushareRealtimeClient,
)
from src.web import v15_scan_service

TZ = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 9, 1)
PRIOR_DATE = date(2026, 8, 31)
CALENDAR = tuple(
    sorted(
        {
            *(TRADE_DATE - timedelta(days=offset) for offset in range(1, 46)),
            TRADE_DATE,
            TRADE_DATE + timedelta(days=1),
            TRADE_DATE + timedelta(days=2),
        }
    )
)


class VendorBudgetHarness:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, codes: tuple[str, ...]) -> None:
        self.codes = codes
        self.early_calls: list[tuple[str, ...]] = []
        self.daily_calls: list[str] = []
        self.prev_close_calls: list[str] = []
        self.daily_entered = asyncio.Event()
        self.daily_gate: asyncio.Event | None = None
        self.ready_codes: set[str] | None = None
        self.scanner_calls: list[tuple[str, ...]] = []
        self.network_calls: list[str] = []
        self.invalid_history_date: str | None = None

        harness = self

        class BudgetScanner:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                return

            def get_universe(self) -> tuple[dict[str, list[tuple[str, str]]], set[str]]:
                return (
                    {"board-a": [(code, f"name-{code}") for code in harness.codes]},
                    set(harness.codes),
                )

            async def scan(self, stock_data: dict[str, Any], _clean_boards: Any) -> Any:
                received = tuple(sorted(stock_data))
                harness.scanner_calls.append(received)
                recommended = [
                    SimpleNamespace(
                        code=code,
                        name=f"name-{code}",
                        buy_price=12.0,
                        score=0.9 - index * 0.01,
                        rank=index + 1,
                    )
                    for index, code in enumerate(received[:10])
                ]
                return SimpleNamespace(
                    recommended=recommended,
                    all_scored=recommended,
                    stock_best_board={code: "board-a" for code in received},
                    stock_all_boards={code: ["board-a"] for code in received},
                    step2_hot_board_count=1,
                    final_candidates=len(recommended),
                )

        class BudgetRealtimeClient:
            async def stop(self) -> None:
                return None

            async def fetch_prev_closes(self, trade_date: str) -> dict[str, float]:
                harness.prev_close_calls.append(trade_date)
                raise AssertionError(
                    "canonical V16 must derive prior closes from its one daily snapshot"
                )

            async def fetch_daily_bars(self, trade_date: str) -> dict[str, TushareDailyBar]:
                harness.daily_calls.append(trade_date)
                if harness.daily_gate is not None:
                    harness.daily_entered.set()
                    await harness.daily_gate.wait()
                return {
                    code: TushareDailyBar(
                        stock_code=code,
                        trade_date=trade_date,
                        close_price=close,
                        amount_yuan=1_000_000.0,
                    )
                    for code, close in harness.prev_closes.items()
                }

            async def batch_get_early_market_data(
                self, requested: list[str], expected_trade_date: date | None = None
            ) -> dict[str, TushareEarlyMarketData]:
                assert expected_trade_date == TRADE_DATE
                requested_tuple = tuple(requested)
                harness.early_calls.append(requested_tuple)
                ready = harness.ready_codes if harness.ready_codes is not None else set(requested)
                return {
                    code: harness.early_data(code, code in ready)
                    for code in requested
                    if code in ready
                }

        class BudgetFundamentals:
            async def batch_get_fundamentals(self, codes: list[str]) -> dict[str, Any]:
                return {code: SimpleNamespace(company_name=f"name-{code}") for code in codes}

            async def batch_current_names(self, codes: list[str]) -> dict[str, str]:
                return {code: f"name-{code}" for code in codes}

            async def close(self) -> None:
                return None

        class BudgetHistoryAdapter:
            async def history_quotes(
                self,
                *,
                codes: str,
                indicators: str,
                start_date: str,
                end_date: str,
            ) -> dict[str, Any]:
                requested = tuple(item.split(".")[0] for item in codes.split(","))
                dates = [
                    (PRIOR_DATE - timedelta(days=39 - index)).isoformat() for index in range(40)
                ]
                highs = [9.0 if day == harness.invalid_history_date else 10.5 for day in dates]
                return {
                    "tables": [
                        {
                            "thscode": f"{code}.SH",
                            "table": {
                                "time": dates,
                                "open": [10.0] * 40,
                                "high": highs,
                                "low": [9.5] * 40,
                                "close": [10.0 + index * 0.01 for index in range(40)],
                                "volume": [100_000.0] * 40,
                            },
                        }
                        for code in requested
                    ]
                }

        self.rt = BudgetRealtimeClient()
        self.prev_closes = {code: 10.5 for code in codes}
        # Breadth deliberately adds a code outside the scanner ticket universe.
        self.prev_closes.setdefault("000001", 9.5)
        self.state = v15_scan_service.V15ScanState(
            initialized=True,
            realtime_client=self.rt,
            fundamentals_db=BudgetFundamentals(),
            historical_adapter=BudgetHistoryAdapter(),
            concept_mapper=object(),
            stock_filter=object(),
            tushare_cache=None,
        )
        from src.strategy.strategies import v16_scanner

        monkeypatch.setattr(v16_scanner, "V16Scanner", BudgetScanner)

        async def no_name_refresh(*_args: Any, **_kwargs: Any) -> None:
            return None

        monkeypatch.setattr(v15_scan_service, "_refresh_top10_names", no_name_refresh)

    @property
    def early_universe(self) -> tuple[str, ...]:
        return tuple(sorted(set(self.codes) | set(self.prev_closes)))

    @staticmethod
    def early_data(code: str, ready: bool) -> TushareEarlyMarketData:
        bars: tuple[TushareMinuteBar, ...] = ()
        if ready:
            bars = (
                TushareMinuteBar(
                    stock_code=code,
                    bar_end=datetime.combine(TRADE_DATE, datetime.min.time()).replace(
                        hour=9, minute=39, tzinfo=TZ
                    ),
                    end_label="09:39",
                    open_price=11.0,
                    close_price=12.3,
                    high_price=12.4,
                    low_price=10.9,
                    volume=2000.0,
                    amount=24_000.0,
                ),
            )
        return TushareEarlyMarketData(
            quote=TushareQuote(
                stock_code=code,
                open_price=11.0,
                latest_price=12.0,
                high_price=12.5,
                low_price=10.9,
                volume=5000.0,
                amount=60_000.0,
                early_close=12.3 if ready else None,
                early_high=12.4,
                early_low=11.5,
                early_volume=3000.0,
                volume_937=2000.0,
            ),
            early_bars=bars,
            source_hash=f"h-{code}",
        )


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> VendorBudgetHarness:
    async def calendar() -> list[date]:
        return list(CALENDAR)

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", calendar)
    return VendorBudgetHarness(monkeypatch, codes=("600000",))


async def test_thirty_six_callers_share_one_union_early_one_scanner_and_one_daily(
    harness: VendorBudgetHarness,
) -> None:
    bundles = await asyncio.wait_for(
        asyncio.gather(
            *(
                v15_scan_service.get_or_compute_canonical_v16(harness.state, TRADE_DATE)
                for _ in range(36)
            )
        ),
        timeout=2.0,
    )

    assert harness.early_calls == [("000001", "600000")]
    assert harness.daily_calls == [PRIOR_DATE.strftime("%Y%m%d")]
    assert harness.prev_close_calls == []
    assert harness.scanner_calls == [("600000",)]
    assert len(bundles) == 36
    assert {bundle.breadth_valid_n for bundle in bundles} == {2}
    assert {bundle.input_hash for bundle in bundles} == {bundles[0].input_hash}
    assert harness.state.canonical_coordinator is not None
    assert harness.state.canonical_coordinator.daily_tasks == {}
    assert set(harness.state.canonical_coordinator.daily_bars) == {PRIOR_DATE}


async def test_thirty_six_callers_share_physical_rt_min_daily_once_per_unique_code(
    harness: VendorBudgetHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count physical Tushare calls, not only the canonical adapter invocation."""

    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    physical_calls: list[tuple[str, str]] = []

    async def physical_api_call(
        api_name: str,
        params: dict[str, Any],
        fields: str = "",
    ) -> dict[str, Any]:
        del fields
        physical_calls.append((api_name, str(params.get("ts_code", ""))))
        if api_name == "daily":
            requested_date = str(params["trade_date"])
            return {
                "data": {
                    "fields": ["ts_code", "trade_date", "close", "amount"],
                    "items": [
                        ["000001.SZ", requested_date, 9.5, 1_000.0],
                        ["600000.SH", requested_date, 10.5, 1_000.0],
                    ],
                }
            }
        assert api_name == "rt_min_daily"
        ts_code = str(params["ts_code"])
        base = 11.0 if ts_code.startswith("000001") else 12.0
        return {
            "data": {
                "fields": [
                    "ts_code",
                    "time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ],
                "items": [
                    [
                        ts_code,
                        f"{TRADE_DATE.isoformat()} 09:{minute:02d}:00",
                        base,
                        base + 0.1,
                        base + 0.2,
                        base - 0.1,
                        2_000.0,
                        24_000.0,
                    ]
                    for minute in range(31, 40)
                ],
            }
        }

    monkeypatch.setattr(client, "_api_call", physical_api_call)
    harness.state.realtime_client = client

    bundles = await asyncio.gather(
        *(
            v15_scan_service.get_or_compute_canonical_v16(harness.state, TRADE_DATE)
            for _ in range(36)
        )
    )

    assert len(bundles) == 36
    assert physical_calls.count(("daily", "")) == 1
    early_calls = [call for call in physical_calls if call[0] == "rt_min_daily"]
    assert sorted(early_calls) == [
        ("rt_min_daily", "000001.SZ"),
        ("rt_min_daily", "600000.SH"),
    ]
    assert len({ts_code for _api_name, ts_code in early_calls}) == len(early_calls)
    assert harness.scanner_calls == [("600000",)]


async def test_partial_oss_daily_cache_cannot_shrink_or_override_authoritative_daily(
    harness: VendorBudgetHarness,
) -> None:
    class PartialStaleCache:
        is_ready = True

        @staticmethod
        def get_all_codes_with_daily(_trade_date: str) -> dict[str, dict[str, float]]:
            return {"600000": {"close": 1.0, "amount": 1.0}}

    harness.state.tushare_cache = PartialStaleCache()

    bundle = await v15_scan_service.get_or_compute_canonical_v16(harness.state, TRADE_DATE)

    assert harness.daily_calls == [PRIOR_DATE.strftime("%Y%m%d")]
    assert harness.early_calls == [("000001", "600000")]
    assert bundle.prev_closes == {"000001": 9.5, "600000": 10.5}
    assert bundle.breadth_valid_n == 2


async def test_not_ready_retry_reuses_daily_and_preserves_breadth_partial_facts(
    harness: VendorBudgetHarness,
) -> None:
    codes = ("000001", "000002", "000003", "000004", "000005")
    harness.codes = codes
    harness.prev_closes = {code: 10.5 for code in codes}
    ready_first = {"000001", "000002"}
    harness.ready_codes = ready_first

    with pytest.raises(v15_scan_service.CanonicalV16NotReadyError):
        await v15_scan_service.get_or_compute_canonical_v16(harness.state, TRADE_DATE)

    assert harness.early_calls == [codes]
    assert harness.daily_calls == [PRIOR_DATE.strftime("%Y%m%d")]
    assert harness.prev_close_calls == []
    coordinator = harness.state.canonical_coordinator
    assert coordinator is not None
    assert set(coordinator.partial[TRADE_DATE]) == ready_first

    harness.ready_codes = set(codes)
    bundle = await v15_scan_service.get_or_compute_canonical_v16(harness.state, TRADE_DATE)

    assert harness.early_calls == [
        codes,
        ("000003", "000004", "000005"),
    ]
    assert harness.daily_calls == [PRIOR_DATE.strftime("%Y%m%d")]
    assert harness.prev_close_calls == []
    assert harness.scanner_calls == [codes]
    assert bundle.breadth_valid_n == len(codes)
    for code in ready_first:
        assert bundle.early_bars[code] == harness.early_data(code, True).early_bars
        assert bundle.early_source_hashes[code] == f"h-{code}"


async def test_seeded_replay_with_realtime_forbidden_makes_zero_vendor_calls(
    harness: VendorBudgetHarness,
) -> None:
    seed = {code: harness.early_data(code, True) for code in ("000001", "600000")}
    history = {
        code: {
            "time": [(PRIOR_DATE - timedelta(days=index)).isoformat() for index in range(40)],
            "open": [10.0] * 40,
            "high": [10.5] * 40,
            "low": [9.5] * 40,
            "close": [10.0 + index * 0.01 for index in range(40)],
            "volume": [100_000.0] * 40,
        }
        for code in ("000001", "600000")
    }

    bundle = await v15_scan_service.compute_canonical_v16_scan(
        harness.state,
        TRADE_DATE,
        universe_override=("000001", "600000"),
        clean_boards_override={"board-a": (("000001", "name-000001"), ("600000", "name-600000"))},
        prev_closes_override={"000001": 9.5, "600000": 10.5},
        history_raw_override=history,
        names_override={"000001": "name-000001", "600000": "name-600000"},
        calendar_override=CALENDAR,
        prior_daily_override={
            code: TushareDailyBar(
                stock_code=code,
                trade_date=PRIOR_DATE.strftime("%Y%m%d"),
                close_price=close,
                amount_yuan=1_000_000.0,
            )
            for code, close in {"000001": 9.5, "600000": 10.5}.items()
        },
        st_eligible_codes_override=("000001", "600000"),
        early_data_seed=seed,
        allow_realtime_fetch=False,
    )

    assert harness.early_calls == []
    assert harness.daily_calls == []
    assert harness.prev_close_calls == []
    assert bundle.breadth_valid_n == 2
    assert set(bundle.early_bars) == {"000001", "600000"}


async def test_empty_calendar_override_fails_closed_without_network(
    harness: VendorBudgetHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def network_bomb() -> list[date]:
        harness.network_calls.append("trade_calendar")
        raise AssertionError("empty calendar override must not fall back to network")

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", network_bomb)

    with pytest.raises(
        (v15_scan_service.CanonicalV16ScanError, RuntimeError),
        match="calendar|previous trading day",
    ):
        await v15_scan_service.compute_canonical_v16_scan(
            harness.state,
            TRADE_DATE,
            universe_override=("600000",),
            clean_boards_override={"board-a": (("600000", "name-600000"),)},
            prev_closes_override={"600000": 10.5},
            history_raw_override={},
            names_override={"600000": "name-600000"},
            calendar_override=(),
            allow_realtime_fetch=False,
        )

    assert harness.network_calls == []
    assert harness.early_calls == []
    assert harness.daily_calls == []


async def test_cleanup_cancels_and_awaits_daily_singleflight_task(
    harness: VendorBudgetHarness,
) -> None:
    harness.daily_gate = asyncio.Event()
    daily_task = asyncio.create_task(
        v15_scan_service._fetch_prior_daily_once(
            harness.state,
            PRIOR_DATE,
            owner_date=TRADE_DATE,
        )
    )
    await asyncio.wait_for(harness.daily_entered.wait(), timeout=1.0)
    coordinator = harness.state.canonical_coordinator
    assert coordinator is not None
    master = coordinator.daily_tasks[PRIOR_DATE]
    assert master is not daily_task
    assert not master.done()

    try:
        await asyncio.wait_for(v15_scan_service.cleanup_scan_resources(harness.state), timeout=1.0)
        assert daily_task.done()
        assert daily_task.cancelled()
        assert master.done()
        assert master.cancelled()
        results = await asyncio.gather(daily_task, master, return_exceptions=True)
        assert len(results) == 2
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
    finally:
        harness.daily_gate.set()
        daily_task.cancel()
        await asyncio.gather(daily_task, return_exceptions=True)


async def test_per_date_history_health_is_frozen_without_replacing_v16_gate(
    harness: VendorBudgetHarness,
) -> None:
    harness.invalid_history_date = PRIOR_DATE.isoformat()

    bundle = await v15_scan_service.get_or_compute_canonical_v16(harness.state, TRADE_DATE)

    assert harness.scanner_calls == [("600000",)]
    assert bundle.history_date_valid_counts[PRIOR_DATE.isoformat()] == 0
    assert bundle.history_min_date_coverage == 0.0
