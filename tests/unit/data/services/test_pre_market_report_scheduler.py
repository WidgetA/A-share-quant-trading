# === MODULE PURPOSE ===
# Tests for PreMarketReportScheduler (ANA-002).
# Mocks out tushare/lambda/feishu/LLM — only verifies scheduler logic:
#   - disabled toggle skips
#   - non-trading day skip vs manual override
#   - storage / broker error paths
#   - no-positions silent skip
#   - per-stock failure isolation

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.data.services.pre_market_report_scheduler import (
    PreMarketReportScheduler,
    _latest_trading_day_with_data,
)


def _make_app_state(
    *,
    storage=None,
    broker=None,
    broker_last_error=None,
    broker_positions=None,
):
    return SimpleNamespace(
        storage=storage,
        broker=broker,
        broker_last_error=broker_last_error,
        broker_positions=broker_positions or [],
    )


def _make_storage(*, is_ready=True, existing_dates=None):
    storage = MagicMock()
    storage.is_ready = is_ready
    storage.get_existing_daily_dates = AsyncMock(return_value=set(existing_dates or []))
    storage.get_last_scheduler_run = AsyncMock(return_value=None)
    storage.log_scheduler_run = AsyncMock(return_value=None)
    return storage


@pytest.mark.asyncio
async def test_latest_trading_day_with_data_finds_recent():
    storage = _make_storage(existing_dates=[date(2026, 4, 30), date(2026, 5, 1)])
    today = date(2026, 5, 5)  # Tue, Mon-was-holiday
    result = await _latest_trading_day_with_data(storage, today)
    assert result == date(2026, 5, 1)


@pytest.mark.asyncio
async def test_latest_trading_day_with_data_empty():
    storage = _make_storage(existing_dates=[])
    result = await _latest_trading_day_with_data(storage, date(2026, 5, 5))
    assert result is None


@pytest.mark.asyncio
async def test_latest_trading_day_with_data_lookback_limit():
    """If the latest cached date is > 14 days ago, return None (likely stale cache)."""
    storage = _make_storage(existing_dates=[date(2026, 4, 1)])
    result = await _latest_trading_day_with_data(storage, date(2026, 5, 5))
    assert result is None


@pytest.mark.asyncio
async def test_run_once_scheduled_skipped_when_disabled():
    """When the toggle is off, the 8am scheduled trigger skips."""
    state = _make_app_state(storage=_make_storage())
    sched = PreMarketReportScheduler(state)

    with patch(
        "src.common.config.get_pre_market_report_enabled",
        return_value=False,
    ):
        await sched._run_once("scheduled")

    assert sched.last_run_result == "skipped"
    assert "已关闭" in sched.last_run_message


@pytest.mark.asyncio
async def test_run_once_manual_ignores_disabled_toggle():
    """Manual trigger always runs, even when the scheduled toggle is off —
    user explicitly clicked the button, signal of intent we don't override."""
    state = _make_app_state(
        storage=_make_storage(existing_dates=[date(2026, 5, 1)]),
        broker=MagicMock(),
        broker_positions=[],  # no positions → reaches no_positions branch
    )
    sched = PreMarketReportScheduler(state)

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=False,  # toggle OFF
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=False),  # also non-trading day
        ),
    ):
        await sched._run_once("manual")

    # Reached no-positions branch — proves both toggle and trading-day filter
    # were bypassed for manual trigger.
    assert sched.last_run_result == "no_positions"


@pytest.mark.asyncio
async def test_run_once_scheduled_skips_non_trading_day():
    state = _make_app_state(storage=_make_storage())
    sched = PreMarketReportScheduler(state)

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=True,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=False),
        ),
    ):
        await sched._run_once("scheduled")

    assert sched.last_run_result == "skipped"
    assert "非交易日" in sched.last_run_message


@pytest.mark.asyncio
async def test_run_once_manual_runs_on_non_trading_day():
    """Manual trigger ignores trading-day filter — used for 补发 / testing."""
    state = _make_app_state(
        storage=_make_storage(existing_dates=[date(2026, 5, 1)]),
        broker=MagicMock(),  # broker present
        broker_positions=[],  # but no positions → no_positions
    )
    sched = PreMarketReportScheduler(state)

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=True,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=False),
        ),
    ):
        await sched._run_once("manual")

    # Reaches the no-positions branch, proving trading-day filter was bypassed
    assert sched.last_run_result == "no_positions"


@pytest.mark.asyncio
async def test_run_once_failed_when_storage_not_ready():
    state = _make_app_state(storage=_make_storage(is_ready=False))
    sched = PreMarketReportScheduler(state)

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=True,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu",
            AsyncMock(),
        ),
    ):
        await sched._run_once("scheduled")

    assert sched.last_run_result == "failed"
    assert "GreptimeDB" in sched.last_run_message


@pytest.mark.asyncio
async def test_run_once_failed_when_broker_missing():
    state = _make_app_state(
        storage=_make_storage(existing_dates=[date(2026, 5, 1)]),
        broker=None,
    )
    sched = PreMarketReportScheduler(state)

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=True,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu",
            AsyncMock(),
        ),
    ):
        await sched._run_once("scheduled")

    assert sched.last_run_result == "failed"
    assert "Broker" in sched.last_run_message


@pytest.mark.asyncio
async def test_run_once_failed_on_broker_error_with_no_positions():
    """Empty positions + broker_last_error means we lost connectivity — fail loud,
    don't silently skip (trading-safety)."""
    state = _make_app_state(
        storage=_make_storage(existing_dates=[date(2026, 5, 1)]),
        broker=MagicMock(),
        broker_last_error="connection refused",
        broker_positions=[],
    )
    sched = PreMarketReportScheduler(state)

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=True,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu",
            AsyncMock(),
        ),
    ):
        await sched._run_once("scheduled")

    assert sched.last_run_result == "failed"
    assert "connection refused" in sched.last_run_message


@pytest.mark.asyncio
async def test_run_once_no_positions_silent_skip():
    state = _make_app_state(
        storage=_make_storage(existing_dates=[date(2026, 5, 1)]),
        broker=MagicMock(),
        broker_last_error=None,
        broker_positions=[],
    )
    sched = PreMarketReportScheduler(state)
    feishu_mock = AsyncMock()

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=True,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu",
            feishu_mock,
        ),
    ):
        await sched._run_once("scheduled")

    assert sched.last_run_result == "no_positions"
    # Silent: no Feishu notification at all
    feishu_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_once_success_per_stock_serial():
    state = _make_app_state(
        storage=_make_storage(existing_dates=[date(2026, 5, 1)]),
        broker=MagicMock(),
        broker_positions=[
            {"code": "000001.SZ", "volume": 1000, "avg_price": 12.5, "market_value": 13000.0},
            {"code": "600519.SH", "volume": 100, "avg_price": 1700.0, "market_value": 175000.0},
        ],
    )
    sched = PreMarketReportScheduler(state)

    analyze_mock = AsyncMock(
        return_value={
            "image_url": "https://example.com/k.png",
            "analysis": "测试分析文本",
        }
    )
    text_mock = AsyncMock()
    image_mock = AsyncMock()
    md_mock = AsyncMock()

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=True,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu",
            text_mock,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu_image",
            image_mock,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu_markdown",
            md_mock,
        ),
        patch("src.analysis.kline_llm.analyze_kline", analyze_mock),
    ):
        await sched._run_once("scheduled")

    assert sched.last_run_result == "success"
    assert "成功 2/2" in sched.last_run_message
    # 1 text header + 2 image + 2 markdown per stock
    assert text_mock.await_count == 1
    assert image_mock.await_count == 2
    assert md_mock.await_count == 2
    assert analyze_mock.await_count == 2


@pytest.mark.asyncio
async def test_run_once_partial_failure_one_stock_errors():
    state = _make_app_state(
        storage=_make_storage(existing_dates=[date(2026, 5, 1)]),
        broker=MagicMock(),
        broker_positions=[
            {"code": "000001.SZ", "volume": 1000, "avg_price": 12.5, "market_value": 13000.0},
            {"code": "999999.SZ", "volume": 100, "avg_price": 10.0, "market_value": 1000.0},
        ],
    )
    sched = PreMarketReportScheduler(state)

    async def _analyze(storage, code, days=30, **kw):
        if code.startswith("999"):
            raise RuntimeError("no daily data")
        return {"image_url": "https://example.com/k.png", "analysis": "ok"}

    text_mock = AsyncMock()
    image_mock = AsyncMock()
    md_mock = AsyncMock()

    with (
        patch(
            "src.common.config.get_pre_market_report_enabled",
            return_value=True,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._is_trading_day",
            AsyncMock(return_value=True),
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu",
            text_mock,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu_image",
            image_mock,
        ),
        patch(
            "src.data.services.pre_market_report_scheduler._notify_feishu_markdown",
            md_mock,
        ),
        patch("src.analysis.kline_llm.analyze_kline", AsyncMock(side_effect=_analyze)),
    ):
        await sched._run_once("scheduled")

    assert sched.last_run_result == "success"
    assert "成功 1/2" in sched.last_run_message
    assert "失败 1" in sched.last_run_message
    # 1 header + 1 error text for failed stock
    assert text_mock.await_count == 2
    # Successful stock got both image + markdown; failed stock got neither
    assert image_mock.await_count == 1
    assert md_mock.await_count == 1


@pytest.mark.asyncio
async def test_concurrent_run_returns_skipped():
    """If a run is in-flight, a second _run_once returns skipped without queuing."""
    state = _make_app_state(storage=_make_storage())
    sched = PreMarketReportScheduler(state)

    # Hold the lock manually
    await sched._lock.acquire()
    try:
        with (
            patch(
                "src.common.config.get_pre_market_report_enabled",
                return_value=True,
            ),
            patch(
                "src.data.services.pre_market_report_scheduler._is_trading_day",
                AsyncMock(return_value=True),
            ),
        ):
            await sched._run_once("manual")
    finally:
        sched._lock.release()

    assert sched.last_run_result == "skipped"
    assert "尚未结束" in sched.last_run_message


def test_is_running_reflects_lock():
    sched = PreMarketReportScheduler(_make_app_state())
    assert sched.is_running() is False


@pytest.mark.asyncio
async def test_get_status_shape():
    sched = PreMarketReportScheduler(_make_app_state())
    with patch(
        "src.common.config.get_pre_market_report_enabled",
        return_value=True,
    ):
        status = sched.get_status()
    assert set(status.keys()) == {
        "enabled",
        "running",
        "next_run_time",
        "last_run_time",
        "last_run_result",
        "last_run_message",
    }
    assert status["enabled"] is True
    assert status["running"] is False
