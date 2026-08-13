from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import pytest

from src.assistant.price_alerts import PriceAlertStore

SCRIPT = (
    Path(__file__).parents[3]
    / "kimi-skills"
    / "manage-price-alerts"
    / "scripts"
    / "manage_price_alert.py"
)
SPEC = importlib.util.spec_from_file_location("manage_price_alert", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _holding(code="603261.SH", name="立航科技", last_price=69.0):
    return {"code": code, "name": name, "last_price": last_price, "quantity": 100}


def test_create_resolves_current_holding_and_deduplicates(monkeypatch, tmp_path):
    store = PriceAlertStore(tmp_path / "alerts.sqlite3")
    monkeypatch.setattr(MODULE, "_fetch_holdings", lambda: [_holding()])
    args = argparse.Namespace(stock="立航科技", direction="below", price="68")

    first = MODULE._create(args, store)
    second = MODULE._create(args, store)

    assert first["created"] is True
    assert first["alert"]["stock_code"] == "603261"
    assert first["already_matches"] is False
    assert second["created"] is False
    assert first["alert"]["alert_id"] == second["alert"]["alert_id"]


def test_create_rejects_stock_not_in_current_holdings(monkeypatch, tmp_path):
    store = PriceAlertStore(tmp_path / "alerts.sqlite3")
    monkeypatch.setattr(MODULE, "_fetch_holdings", lambda: [_holding()])
    args = argparse.Namespace(stock="贵州茅台", direction="below", price="1400")

    with pytest.raises(ValueError, match="当前持仓里没找到"):
        MODULE._create(args, store)


def test_cancel_by_stock_name_cancels_all_active_rules(monkeypatch, tmp_path):
    store = PriceAlertStore(tmp_path / "alerts.sqlite3")
    monkeypatch.setattr(MODULE, "_fetch_holdings", lambda: [_holding()])
    MODULE._create(argparse.Namespace(stock="立航科技", direction="below", price="68"), store)
    MODULE._create(argparse.Namespace(stock="立航科技", direction="above", price="75"), store)

    result = MODULE._cancel(argparse.Namespace(alert_id=None, stock="立航科技"), store)

    assert result["cancelled"] == 2
    assert store.list_alerts(status="active") == []
