from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.common.v20_feishu import _render_entry_strategy_body, render_entry_message


def semantic(permit=False):
    return {
        "strategy_version": "V24.5",
        "trade_date": "2026-11-16",
        "deployment_mode": "forward_shadow",
        "entry_only": True,
        "action": "ENTER" if permit else "BLOCK",
        "final_multiplier": 1 if permit else 0,
        "symbols": [],
        "reference_symbols": [],
        "original_final_open": True,
        "v245_transition": {
            "raw_open": True,
            "permit": permit,
            "state_after": {
                "mode": "NORMAL" if permit else "TURTLE",
                "open_streak": 3 if permit else 1,
                "closed_streak": 0,
            },
        },
        "v245_switch_plan": {
            "target": "STOCKS" if permit else "BANK",
            "sell_positions": [
                {
                    "code": "601398" if permit else "600000",
                    "stock_name": "工商银行" if permit else "浦发银行",
                    "entry_date": "2026-11-13",
                    "quantity": None,
                    "t1_eligible": True,
                }
            ],
        },
    }


def render(value):
    return render_entry_message(
        value,
        generated_at=datetime(2026, 11, 16, 9, 45, tzinfo=ZoneInfo("Asia/Shanghai")),
        commit_marker=1,
        on_time=True,
    )


def test_waiting_for_three_days_explains_bank_target_and_unknown_quantity():
    message = render(semantic())
    assert "V24.5" in message and "乌龟" in message and "1/3" in message
    assert "601398 工商银行" in message and "可用资金" in message
    assert "600000 浦发银行" in message and "实际" in message
    assert "保留现金" not in message
    assert "V22-slim选股" not in message


def test_normal_recovery_sells_bank_before_original_allocation():
    message = render(semantic(True))
    assert "先卖出工商银行" in message and "资金" in message
    assert "原 v22" in message and "正常" in message
    assert "09:41" not in message


def test_t1_deferred_position_has_an_explicit_next_session_date():
    value = semantic()
    value["v245_switch_plan"]["sell_positions"][0].update(
        t1_eligible=False, earliest_sell_date="2026-11-17"
    )
    message = render(value)
    assert "2026-11-17" in message and "T+1" in message


@pytest.mark.parametrize("reason", ["ENTRY_INPUT_UNAVAILABLE", "SLOT_FINALIZED_FAILED"])
def test_invalid_selection_is_error_instead_of_a_normal_no_buy_decision(reason):
    value = semantic()
    value.update(
        action="INPUT_INVALID",
        reason_codes=[reason],
        failure_detail="internal traceback with private transport settings",
    )
    message = render(value)
    assert "ERROR" in message.splitlines()[0]
    assert "计算结论：ERROR" in message
    assert "原因：" in message
    assert "NO_SIGNAL" not in message
    assert "买入或继续持有" not in message
    assert value["failure_detail"] not in message


def test_invalid_selection_body_does_not_describe_an_empty_successful_stock_list():
    value = semantic()
    value.update(action="INPUT_INVALID", reason_codes=["ENTRY_INPUT_UNAVAILABLE"])
    body = _render_entry_strategy_body(value)
    assert "计算结论：ERROR" in body
    assert "原因：" in body
    assert "最终倍率" not in body
    assert "资金目标为工商银行" not in body


@pytest.mark.parametrize("action", ["NO_SIGNAL", "BLOCK"])
def test_valid_no_candidate_or_gate_decision_is_not_an_error(action):
    value = semantic()
    value["action"] = action
    message = render(value)
    assert f"计算结论：{action}" in message
    assert "ERROR" not in message
