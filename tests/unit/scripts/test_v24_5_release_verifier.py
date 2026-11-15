"""A healthy server, accepted request, or closure skip is not delivery proof."""

from copy import deepcopy

import pytest

from scripts.verify_v24_5_release import assess_status, assess_task

SHA = "e80abb8ebc9324de08a2943c3489e7b75e9fbdb6"


def _status():
    return {
        "status": "ok",
        "git_commit": SHA,
        "v20": {
            "strategy_version": "V24.5",
            "configured": True,
            "enabled": True,
            "started": True,
            "healthy": True,
            "running": True,
            "retrying": False,
        },
    }


def _task():
    return {
        "trade_date": "2026-11-16",
        "selection_version": "V24.5",
        "orders_changed": False,
        "calculation_result": "SUCCESS",
        "entry_event_id": "entry-persisted",
        "entry_timing_alert_event_id": "timing-persisted",
        "retrospective_expired": False,
        "exact_automatic_message": True,
        "delivery_status": "SENT",
        "entry_timing_alert_delivery_status": "SENT",
        "feishu_delivery_confirmed": True,
        "task_success": True,
    }


def test_health_is_exact_commit_and_strategy_bound():
    assert assess_status(_status(), SHA, "V24.5")["passed"]
    assert not assess_status(_status(), "a" * 40, "V24.5")["passed"]
    assert not assess_status(_status(), SHA, "V22-slim")["passed"]


@pytest.mark.parametrize("field", ["started", "healthy", "running", "enabled"])
def test_runtime_not_ready_is_not_release_success(field):
    status = _status()
    status["v20"][field] = False
    assert not assess_status(status, SHA, "V24.5")["passed"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("delivery_status", "PENDING"),
        ("delivery_status", "DELIVERY_UNKNOWN"),
        ("entry_timing_alert_delivery_status", "PENDING"),
        ("entry_timing_alert_delivery_status", "NOT_REQUIRED"),
        ("entry_event_id", None),
        ("orders_changed", True),
        ("trade_date", "2026-11-13"),
        ("selection_version", "V22-slim"),
    ],
)
def test_incomplete_or_stale_task_cannot_pass(field, value):
    payload = deepcopy(_task())
    payload[field] = value
    result = assess_task(payload, "2026-11-16", "V24.5")
    assert not result["passed"]
    assert not result["trading_day_acceptance"]


def test_both_receipts_and_current_result_are_required():
    assert assess_task(_task(), "2026-11-16", "V24.5")["trading_day_acceptance"]
    assert not assess_task({"accepted": True}, "2026-11-16", "V24.5")["passed"]


def test_sunday_skip_is_valid_skip_and_never_trading_day_acceptance():
    payload = {
        "trade_date": "2026-11-15",
        "selection_version": "V24.5",
        "orders_changed": False,
        "cycle_result": "NON_TRADING_DAY",
        "calculation_result": "NOT_RUN",
        "delivery_status": "NOT_REQUIRED",
        "symbols": [],
        "reference_symbols": [],
        "official_state_changed": False,
        "task_success": False,
    }
    result = assess_task(payload, "2026-11-15", "V24.5")
    assert result["passed"] and not result["trading_day_acceptance"]
