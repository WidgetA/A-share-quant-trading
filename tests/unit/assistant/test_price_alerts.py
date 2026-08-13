from __future__ import annotations

import time as wall_time
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.assistant.price_alerts import (
    PriceAlertMonitor,
    PriceAlertStore,
    format_trigger_message,
    price_to_fen,
)

BJ = ZoneInfo("Asia/Shanghai")


def _store(tmp_path) -> PriceAlertStore:
    return PriceAlertStore(tmp_path / "alerts.sqlite3")


def _create(store: PriceAlertStore, *, code="603261", direction="below", price="68"):
    return store.create_alert(
        stock_code=code,
        stock_name="立航科技",
        direction=direction,
        threshold=price,
        now=datetime(2026, 8, 13, 9, 0, tzinfo=BJ),
    )


def test_price_to_fen_is_exact_and_rejects_bad_values():
    assert price_to_fen("68") == 6800
    assert price_to_fen(68.005) == 6801
    with pytest.raises(ValueError):
        price_to_fen(0)
    with pytest.raises(ValueError):
        price_to_fen("not-a-price")


def test_store_create_is_idempotent_and_cancel_is_persistent(tmp_path):
    store = _store(tmp_path)
    first, created = _create(store)
    duplicate, duplicate_created = _create(store)

    assert created is True
    assert duplicate_created is False
    assert duplicate["alert_id"] == first["alert_id"]
    assert store.list_alerts(status="active") == [first]

    assert store.cancel_alerts(alert_id=first["alert_id"]) == 1
    assert store.cancel_alerts(alert_id=first["alert_id"]) == 0
    assert store.list_alerts(status="active") == []
    assert store.list_alerts(status="cancelled")[0]["alert_id"] == first["alert_id"]


def test_active_alert_survives_store_reopen(tmp_path):
    path = tmp_path / "alerts.sqlite3"
    first_store = PriceAlertStore(path)
    alert, _ = _create(first_store)

    reopened_store = PriceAlertStore(path)
    active = reopened_store.list_alerts(status="active")

    assert len(active) == 1
    assert active[0]["alert_id"] == alert["alert_id"]


def test_store_claims_below_and_above_once_only_for_current_positions(tmp_path):
    store = _store(tmp_path)
    below, _ = _create(store, price="68")
    above, _ = _create(store, direction="above", price="72")
    other, _ = _create(store, code="000001", price="10")
    now = datetime(2026, 8, 13, 10, 0, tzinfo=BJ)

    first = store.claim_triggered([{"code": "603261.SH", "last_price": 67.99}], now=now)
    assert [item["alert_id"] for item in first] == [below["alert_id"]]
    assert store.claim_triggered([{"code": "603261.SH", "last_price": 67.50}], now=now) == []

    second = store.claim_triggered([{"code": "603261.SH", "last_price": 72.01}], now=now)
    assert [item["alert_id"] for item in second] == [above["alert_id"]]
    assert store.list_alerts(status="active")[0]["alert_id"] == other["alert_id"]


async def test_monitor_triggers_and_sends_feishu_once(tmp_path):
    store = _store(tmp_path)
    alert, _ = _create(store)
    state = SimpleNamespace(
        broker_positions=[{"code": "603261.SH", "last_price": 67.88}],
        broker_positions_updated_at=wall_time.time(),
    )
    messages: list[str] = []

    async def send(message: str) -> bool:
        messages.append(message)
        return True

    async def trading_day(_target: date) -> bool:
        return True

    monitor = PriceAlertMonitor(
        state,
        store=store,
        send_notification=send,
        trading_day_checker=trading_day,
    )
    result = await monitor.check_once(now=datetime(2026, 8, 13, 10, 0, tzinfo=BJ))

    assert result == {"result": "checked", "triggered": 1, "notified": 1}
    assert len(messages) == 1
    assert "603261 立航科技 已跌破 68.00 元" in messages[0]
    assert "未执行任何交易" in messages[0]
    saved = store.list_alerts(status="triggered")[0]
    assert saved["alert_id"] == alert["alert_id"]
    assert saved["notification_status"] == "sent"

    state.broker_positions_updated_at = wall_time.time() + 1
    again = await monitor.check_once(now=datetime(2026, 8, 13, 10, 1, tzinfo=BJ))
    assert again["triggered"] == 0
    assert len(messages) == 1


async def test_monitor_skips_outside_market_and_stale_snapshot(tmp_path):
    store = _store(tmp_path)
    _create(store)
    state = SimpleNamespace(
        broker_positions=[{"code": "603261", "last_price": 60}],
        broker_positions_updated_at=wall_time.time() - 600,
    )

    async def trading_day(_target: date) -> bool:
        return True

    monitor = PriceAlertMonitor(state, store=store, trading_day_checker=trading_day)
    outside = await monitor.check_once(now=datetime(2026, 8, 13, 8, 0, tzinfo=BJ))
    stale = await monitor.check_once(now=datetime(2026, 8, 13, 10, 0, tzinfo=BJ))

    assert outside["result"] == "outside_market"
    assert stale["result"] == "stale_broker_snapshot"
    assert store.list_alerts(status="active")


async def test_failed_feishu_delivery_stays_pending_and_retries_later(tmp_path):
    store = _store(tmp_path)
    _create(store)
    state = SimpleNamespace(
        broker_positions=[{"code": "603261", "last_price": 67}],
        broker_positions_updated_at=wall_time.time(),
    )
    outcomes = [False, True]
    calls: list[str] = []

    async def send(message: str) -> bool:
        calls.append(message)
        return outcomes.pop(0)

    async def trading_day(_target: date) -> bool:
        return True

    monitor = PriceAlertMonitor(
        state,
        store=store,
        send_notification=send,
        trading_day_checker=trading_day,
    )
    t0 = datetime(2026, 8, 13, 10, 0, tzinfo=BJ)
    first = await monitor.check_once(now=t0)
    assert first["triggered"] == 1
    assert first["notified"] == 0
    assert store.pending_notifications(now=t0 + timedelta(seconds=30)) == []

    state.broker_positions_updated_at = wall_time.time() + 1
    retried = await monitor.check_once(now=t0 + timedelta(seconds=61))
    assert retried["notified"] == 1
    assert len(calls) == 2
    assert store.list_alerts(status="triggered")[0]["notification_status"] == "sent"


def test_trigger_message_for_above_rule(tmp_path):
    store = _store(tmp_path)
    alert, _ = _create(store, direction="above", price="72.5")
    claimed = store.claim_triggered(
        [{"code": "603261", "last_price": "72.50"}],
        now=datetime(2026, 8, 13, 14, 0, tzinfo=BJ),
    )[0]
    message = format_trigger_message(claimed)
    assert alert["alert_id"] in message
    assert "已突破 72.50 元" in message
