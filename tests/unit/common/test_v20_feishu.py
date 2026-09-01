import asyncio
import hashlib
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.common.v20_feishu import (
    V20_RELAY_REQUEST_SCHEMA,
    V20_RELAY_RESPONSE_SCHEMA,
    V20LegacyRelayClient,
    V20OutboxPublisher,
    V20RelayClient,
    V20RelayContractError,
    load_legacy_embedded_v20_route,
    load_v20_feishu_routes,
    render_entry_message,
    render_exit_message,
    seal_v20_payload,
)
from src.data.database.v20_repository import OutboxRecord, V20StateConflict, sha256_json
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_EXIT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_FEISHU_PAYLOAD_SCHEMA,
)

TZ = ZoneInfo("Asia/Shanghai")


def _outbox_record(
    event_type: str,
    semantic: dict,
    *,
    event_id: str | None = None,
) -> OutboxRecord:
    resolved_event_id = event_id or str(semantic.get("event_id", "event"))
    return OutboxRecord(
        event_id=resolved_event_id,
        event_type=event_type,
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        payload=None,
        payload_hash=None,
        generated_at=None,
        commit_marker=None,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
    )


def test_v20_routes_never_fall_back_to_legacy_feishu_credentials(monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_APP_ID", "legacy-app")
    monkeypatch.setenv("FEISHU_APP_SECRET", "legacy-secret")
    monkeypatch.setenv("FEISHU_CHAT_ID", "legacy-chat")
    monkeypatch.setenv("FEISHU_BOT_URL", "https://legacy-relay.example")
    for prefix in ("V20_SHADOW_FEISHU", "V20_FEISHU"):
        monkeypatch.delenv(f"{prefix}_APP_ID", raising=False)
        monkeypatch.delenv(f"{prefix}_APP_SECRET", raising=False)
        monkeypatch.delenv(f"{prefix}_CHAT_ID", raising=False)
        monkeypatch.delenv(f"{prefix}_BOT_URL", raising=False)

    routes = load_v20_feishu_routes()

    assert not routes["V20_SHADOW_FEISHU"].is_configured()
    assert not routes["V20_FORMAL_FEISHU"].is_configured()


def test_explicit_embedded_route_reuses_legacy_main_feishu(monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_APP_ID", "legacy-app")
    monkeypatch.setenv("FEISHU_APP_SECRET", "legacy-secret")
    monkeypatch.setenv("FEISHU_CHAT_ID", "legacy-chat")
    monkeypatch.setenv("FEISHU_BOT_URL", "https://legacy-relay.example")

    route = load_legacy_embedded_v20_route()

    assert route.is_configured()
    assert route.route_id == "V20_SHADOW_FEISHU"
    assert route.transport == "legacy_send"
    assert route.bot_origin == "https://legacy-relay.example"
    assert route.app_id == "legacy-app"
    assert route.chat_id == "legacy-chat"


def test_v20_route_requires_its_own_https_relay_url(monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_BOT_URL", "https://legacy-relay.example")
    monkeypatch.setenv("V20_FEISHU_APP_ID", "formal-app")
    monkeypatch.setenv("V20_FEISHU_APP_SECRET", "formal-secret")
    monkeypatch.setenv("V20_FEISHU_CHAT_ID", "formal-chat")
    monkeypatch.delenv("V20_FEISHU_BOT_URL", raising=False)

    route = load_v20_feishu_routes()["V20_FORMAL_FEISHU"]

    assert route.bot_url == ""
    assert not route.is_configured()


def test_v20_route_rejects_whitespace_credentials_and_invalid_relay_url(monkeypatch) -> None:
    monkeypatch.setenv("V20_SHADOW_FEISHU_BOT_URL", "not-a-url")
    monkeypatch.setenv("V20_SHADOW_FEISHU_APP_ID", "app")
    monkeypatch.setenv("V20_SHADOW_FEISHU_APP_SECRET", "secret")
    monkeypatch.setenv("V20_SHADOW_FEISHU_CHAT_ID", "   ")

    route = load_v20_feishu_routes()["V20_SHADOW_FEISHU"]

    assert not route.is_configured()


def test_v20_routes_reject_shared_shadow_and_formal_credentials(monkeypatch) -> None:
    monkeypatch.setenv("V20_SHADOW_FEISHU_APP_ID", "shared-app")
    monkeypatch.setenv("V20_SHADOW_FEISHU_APP_SECRET", "shared-secret")
    monkeypatch.setenv("V20_SHADOW_FEISHU_CHAT_ID", "shadow-chat")
    monkeypatch.setenv("V20_FEISHU_APP_ID", "shared-app")
    monkeypatch.setenv("V20_FEISHU_APP_SECRET", "shared-secret")
    monkeypatch.setenv("V20_FEISHU_CHAT_ID", "formal-chat")

    with pytest.raises(ValueError, match="cannot share Feishu app credentials"):
        load_v20_feishu_routes()


def test_entry_message_is_explicit_and_keeps_full_v16_style_rows() -> None:
    symbols = [
        {
            "rank": rank,
            "code": f"{rank:06d}",
            "name": f"股票{rank}",
            "score": 2.0 - rank / 100,
            "snapshot_price": 10.0 + rank / 10,
            "boards": ["银行"],
            "best_board": "银行",
            "is_driver": True,
            "cci": 80.0 + rank,
            "volume_937": 100000.0 + rank * 10000,
            "history_hash": "a" * 64,
        }
        for rank in range(1, 11)
    ]
    semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "e" * 64,
        "deployment_mode": "production_push",
        "trade_date": "2026-08-31",
        "action": "ENTER",
        "final_multiplier": 0.25,
        "health_state": "PAUSED_R1",
        "base_multiplier": 0.5,
        "rolling7_state": "BAD",
        "rolling7_r7": -0.12,
        "rolling7_l7": 5,
        "g_state": "CLEAR",
        "defense_multiplier": 0.5,
        "reason_codes": ["BASE_PAUSED_BREADTH_HALF", "ROLLING7_BAD_HALF"],
        "last_complete_bar": "09:39",
        "v16_funnel": {
            "step0_universe_count": 3210,
            "step2_hot_board_count": 8,
            "final_candidates": 10,
        },
        "v16_board_avg_gains": {"银行": 1.23},
        "scheduled_exits_today": [
            {
                "model_leg_id": "old-leg",
                "code": "600000",
                "stock_name": "浦发银行",
                "rank": 2,
                "signal_date": "2026-08-27",
                "relative_weight": 0.05,
                "plan_time": "14:57",
            }
        ],
        "symbols": symbols,
    }

    payload = seal_v20_payload(
        _outbox_record("ENTRY_DECISION", semantic),
        datetime(2026, 8, 31, 9, 39, 50, tzinfo=TZ),
        7,
        True,
    )
    message = str(payload["message"])

    assert "25%标准批次" in message
    assert "股票池 3210只 | 热门板块 8个 | 最终 10只" in message
    assert "V16完整推荐（10只）" in message
    assert "推荐 Top-1: 000001 股票1" in message
    expected_rows = [
        (
            f"{item['rank']}. {item['code']} {item['name']}  "
            f"LGB={item['score']:.4f}  "
            f"09:39快照:{item['snapshot_price']:.2f}  "
            f"[带动]银行(+1.23%)  CCI={item['cci']:.0f}  "
            f"7min={item['volume_937'] / 10000:.0f}万"
        )
        for item in symbols
    ]
    message_lines = message.splitlines()
    score_header_index = message_lines.index("评分前10:")
    assert message_lines[score_header_index + 1 : score_header_index + 11] == expected_rows
    assert len({item["code"] for item in symbols}) == 10
    assert expected_rows[-1] == message_lines[score_header_index + 10]
    assert message.count(expected_rows[-1]) == 1
    assert "[带动]银行(+1.23%)" in message
    assert "[带动]=个股自身涨幅已达热门板块门槛" in message
    assert "09:41结束标签" in message
    assert "账户金额或股数" in message
    assert "D0=2026-08-27 / rank=2 / 腿份额=5.00%" in message


def test_entry_current_contract_seals_and_legacy_or_partial_contracts_fail_closed() -> None:
    semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "entry",
        "deployment_mode": "production_push",
        "trade_date": "2026-08-31",
        "action": "ENTER",
        "final_multiplier": 1.0,
        "base_multiplier": 1.0,
        "defense_multiplier": 1.0,
        "health_state": "HEALTHY",
        "rolling7_state": "NON_BAD",
        "rolling7_r7": 0.1,
        "rolling7_l7": 1,
        "g_state": "CLEAR",
        "reason_codes": [],
        "last_complete_bar": "09:39",
        "v16_funnel": {
            "step0_universe_count": 100,
            "step2_hot_board_count": 1,
            "final_candidates": 1,
        },
        "v16_board_avg_gains": {"bank": 1.23},
        "symbols": [
            {
                "rank": 1,
                "code": "000001",
                "name": "stock",
                "score": 1.0,
                "snapshot_price": 10.0,
                "boards": ["bank"],
                "best_board": "bank",
                "is_driver": True,
                "cci": 88.0,
                "volume_937": 120000.0,
                "history_hash": "a" * 64,
            }
        ],
        "scheduled_exits_today": [],
    }
    generated_at = datetime(2026, 8, 31, 9, 39, 50, tzinfo=TZ)

    payload = seal_v20_payload(_outbox_record("ENTRY_DECISION", semantic), generated_at, 1, True)
    assert payload["schema_version"] == V20_FEISHU_PAYLOAD_SCHEMA
    assert payload["feishu_formatter_profile"] == V20_FEISHU_FORMATTER_PROFILE

    unknown_rolling = {
        **semantic,
        "event_id": "entry-unknown-rolling",
        "rolling7_state": "UNKNOWN",
        "rolling7_r7": None,
        "rolling7_l7": None,
    }
    unknown_payload = seal_v20_payload(
        _outbox_record("ENTRY_DECISION", unknown_rolling), generated_at, 2, True
    )
    assert "亏损批次=-/7" in str(unknown_payload["message"])
    assert "None/7" not in str(unknown_payload["message"])

    legacy = {**semantic, "schema_version": "v20-entry-semantic/v1"}
    with pytest.raises(ValueError, match="legacy semantics cannot be upgraded"):
        seal_v20_payload(_outbox_record("ENTRY_DECISION", legacy), generated_at, 2, True)

    partial = dict(semantic)
    partial.pop("v16_board_avg_gains")
    with pytest.raises(ValueError, match="frozen V16 formatter evidence"):
        seal_v20_payload(_outbox_record("ENTRY_DECISION", partial), generated_at, 3, True)

    wrong_profile = {**semantic, "feishu_formatter_profile": "legacy-profile"}
    with pytest.raises(ValueError, match="formatter profile"):
        seal_v20_payload(_outbox_record("ENTRY_DECISION", wrong_profile), generated_at, 4, True)


def test_input_invalid_current_contract_leads_with_operator_action() -> None:
    semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "invalid-entry",
        "deployment_mode": "production_push",
        "trade_date": "2026-08-31",
        "action": "INPUT_INVALID",
        "final_multiplier": 0.0,
        "base_multiplier": 0.0,
        "defense_multiplier": 0.0,
        "health_state": None,
        "rolling7_state": None,
        "rolling7_r7": None,
        "rolling7_l7": None,
        "g_state": "NOT_EVALUATED",
        "reason_codes": ["ENTRY_INPUT_UNAVAILABLE"],
        "failure_detail": "quote coverage 70/100 below threshold",
        "last_complete_bar": None,
        "symbols": [],
        "scheduled_exits_today": [],
    }

    payload = seal_v20_payload(
        _outbox_record("ENTRY_DECISION", semantic),
        datetime(2026, 8, 31, 9, 45, 1, tzinfo=TZ),
        5,
        False,
    )

    assert "🔴 现在操作：不开仓，不补买，不追买" in payload["message"]
    assert "关键入场数据不完整" in payload["message"]
    assert "quote coverage 70/100 below threshold" not in payload["message"]


def _manual_trigger_receipt_semantic(*, event_id: str = "manual-event") -> dict:
    return {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": event_id,
        "strategy_version": "V20",
        "config_hash": "a" * 64,
        "deployment_mode": "production_push",
        "official_stream_id": "formal-stream",
        "state_lineage_id": "formal-lineage",
        "alert_code": "MANUAL_TRIGGER_RECEIPT",
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "manual_request_id": "deploy-12345678",
        "event_trade_date": "2026-08-31",
        "cycle_result": "ALREADY_TERMINAL",
        "formal_decision_available": True,
        "entry_action": "ENTER",
        "entry_event_id": "entry-event",
        "formal_semantic_hash": "b" * 64,
        "official_state_changed": False,
        "non_actionable": True,
        "message": (
            "仅用于部署验收；不会创建或修改订单、持仓、卖出信号或券商侧状态。\n"
            "幂等请求: deploy-12345678\n"
            "本轮结果: ALREADY_TERMINAL"
        ),
    }


def test_manual_trigger_receipt_is_visibly_non_actionable_and_seals_as_data_alert() -> None:
    semantic = _manual_trigger_receipt_semantic()

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic),
        datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
        17,
        True,
    )

    assert payload["event_type"] == "DATA_ALERT"
    assert "actionable_from" not in payload
    assert "expired_delivery_message" not in payload
    lines = str(payload["message"]).splitlines()
    assert lines[0] == "[V20] 人工触发回执｜非交易指令"
    assert "🔴 现在操作：不开仓，不补买，不追买" in payload["message"]
    assert "验收结果：已读取今天已经冻结的结果" in payload["message"]
    assert "ALREADY_TERMINAL" not in payload["message"]


def _manual_monitor_armed_semantic() -> dict:
    return {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "manual-monitor-armed-event",
        "strategy_version": "V20",
        "config_hash": "a" * 64,
        "deployment_mode": "production_push",
        "official_stream_id": "formal-stream",
        "state_lineage_id": "formal-lineage",
        "alert_code": "MANUAL_MONITOR_ARMED",
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "enrollment_id": "manual-monitor-enrollment-001",
        "source_event_id": "frozen-morning-source-event",
        "model_batch_id": "manual-monitor-batch-001",
        "signal_date": "2026-08-31",
        "d1": "2026-09-01",
        "d2": "2026-09-02",
        "activation_cutoff_ts": "2026-09-01T09:30:00+08:00",
        "reference_profile_id": "D0_0941_OPEN_D1_0930_ARBITRATION_V1",
        "reference_evidence_status": "COMPLETE_PENDING_D1_ARBITRATION",
        "armed_leg_count": 2,
        "symbols": [
            {"code": "000001", "name": "平安银行"},
            {"code": "600000", "name": "浦发银行"},
        ],
        "official_state_changed": False,
        "orders_changed": False,
        "message": "manual monitor enrollment confirmation",
    }


def test_manual_monitor_armed_seals_as_explicit_monitor_only_confirmation() -> None:
    semantic = _manual_monitor_armed_semantic()

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id=semantic["event_id"]),
        datetime(2026, 8, 31, 16, 30, tzinfo=TZ),
        18,
        True,
    )
    message = str(payload["message"])

    assert payload["event_type"] == "DATA_ALERT"
    assert "actionable_from" not in payload
    assert "expired_delivery_message" not in payload
    assert message.splitlines()[0] == "[V20] 人工补挂卖出监控已启用"
    assert "🟢 已启用：2 只模型腿" in message
    assert "票单：000001 平安银行、600000 浦发银行" in message
    assert "D0 原始 09:41 bar.open" in message
    assert "D1 09:30" in message
    assert "D1 保护：任一有效分钟 bar.close ≤ 参考价 92%" in message
    assert "D2 保护：任一有效分钟 bar.close ≤ 参考价 88%" in message
    assert "合格 MEWS=DANGER 时提高到 95%；14:57 无条件提醒退出" in message
    assert "只新增卖出监控腿" in message
    assert "未修改正式入场决定，也未创建订单、持仓或成交" in message
    assert "已下单" not in message
    assert "已持仓" not in message


@pytest.mark.parametrize(
    ("updates", "error"),
    [
        ({"event_id": "wrong-event"}, "inconsistent"),
        ({"delivery_priority_class": "ACTIONABLE_ENTRY"}, "inconsistent"),
        ({"reference_evidence_status": "LOCKED"}, "inconsistent"),
        ({"official_state_changed": True}, "inconsistent"),
        ({"orders_changed": True}, "inconsistent"),
        ({"armed_leg_count": 1}, "inconsistent"),
        ({"armed_leg_count": True}, "inconsistent"),
        ({"symbols": "000001"}, "inconsistent"),
        ({"signal_date": "not-a-date"}, "dates are invalid"),
        ({"d2": "2026-09-01"}, "activation boundary"),
        ({"activation_cutoff_ts": "2026-09-01T09:31:00+08:00"}, "activation boundary"),
        ({"activation_cutoff_ts": "2026-09-01T09:30:00"}, "activation boundary"),
    ],
)
def test_manual_monitor_armed_rejects_inconsistent_or_unsafe_semantics(
    updates: dict,
    error: str,
) -> None:
    semantic = {**_manual_monitor_armed_semantic(), **updates}

    with pytest.raises(ValueError, match=error):
        seal_v20_payload(
            _outbox_record(
                "DATA_ALERT",
                semantic,
                event_id="manual-monitor-armed-event",
            ),
            datetime(2026, 8, 31, 16, 30, tzinfo=TZ),
            19,
            True,
        )


def test_late_0939_replay_has_dedicated_expired_non_actionable_title() -> None:
    semantic = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "late-replay-event",
        "strategy_version": "V20",
        "config_hash": "a" * 64,
        "deployment_mode": "forward_shadow",
        "official_stream_id": "formal-stream",
        "state_lineage_id": "formal-lineage",
        "alert_code": "LATE_0939_REPLAY_RESULT",
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "event_trade_date": "2026-08-31",
        "replay_kind": "RETROSPECTIVE_POST_CUTOFF",
        "non_actionable": True,
        "official_entry_action": "INPUT_INVALID",
        "official_entry_event_id": "failed-entry-event",
        "replay_action": "ENTER",
        "final_multiplier": 1.0,
        "symbols": [{"code": "000001"}],
        "data_cutoff": "09:39",
        "data_receipt_timeliness": "POST_CUTOFF",
        "computed_at": "2026-08-31T15:30:00+08:00",
        "state_replay_profile": "DEPLOYED_RUNTIME_LINEAGE",
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "pit_limitations": ["POST_CUTOFF_REPLAY"],
        "message": "⛔ 已过期不可追买；这不是交易指令。",
    }

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id="late-replay-event"),
        datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
        19,
        True,
    )

    assert payload["event_type"] == "DATA_ALERT"
    assert payload["timeliness_status"] == "ON_TIME"
    assert "actionable_from" not in payload
    lines = payload["message"].splitlines()
    assert lines[0] == "[V20][SHADOW] 现在不开仓｜09:39复盘已过期"
    assert lines[2] == "🔴 现在操作：不开仓，不补买，不追买"
    assert lines[3] == "🕘 当时本应：正常开仓（策略倍率100%）；结果已过期"
    assert "早盘正式记录：未形成按时有效的入场决策" in payload["message"]
    assert "INPUT_INVALID" not in payload["message"]
    assert "ENTER" not in payload["message"]
    assert "ON_TIME" not in payload["message"]


@pytest.mark.parametrize(
    ("replay_action", "expected"),
    [
        ("BLOCK", "🕘 当时本应：不开仓（风控拦截）；结果已过期"),
        ("NO_SIGNAL", "🕘 当时本应：不开仓（没有合格候选票）；结果已过期"),
    ],
)
def test_late_0939_replay_translates_non_entry_outcomes(
    replay_action: str,
    expected: str,
) -> None:
    semantic = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": f"late-{replay_action.lower()}",
        "strategy_version": "V20",
        "deployment_mode": "forward_shadow",
        "alert_code": "LATE_0939_REPLAY_RESULT",
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "event_trade_date": "2026-08-31",
        "replay_kind": "RETROSPECTIVE_POST_CUTOFF",
        "non_actionable": True,
        "official_entry_action": "INPUT_INVALID",
        "official_entry_event_id": "failed-entry-event",
        "replay_action": replay_action,
        "final_multiplier": 0.0,
        "symbols": [],
        "data_cutoff": "09:39",
        "data_receipt_timeliness": "POST_CUTOFF",
        "computed_at": "2026-08-31T15:30:00+08:00",
        "state_replay_profile": "DEPLOYED_RUNTIME_LINEAGE",
        "bootstrap_mode": "EMPTY_FORWARD_SHADOW",
        "pit_limitations": ["POST_CUTOFF_REPLAY"],
        "message": "retrospective audit detail",
    }

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id=semantic["event_id"]),
        datetime(2026, 8, 31, 15, 31, tzinfo=TZ),
        20,
        True,
    )

    assert expected in payload["message"]
    assert "现在操作：不开仓，不补买，不追买" in payload["message"]


def _manual_0939_chain_probe_semantic(
    *,
    event_id: str = "manual-chain-probe-event",
    probe_result: str = "PASS",
) -> dict:
    symbols = [
        {
            "rank": 1,
            "code": "000001",
            "name": "平安银行",
            "score": 0.81234,
            "snapshot_price": 10.26,
            "boards": ["银行"],
            "best_board": "银行",
            "is_driver": True,
            "cci": 88.0,
            "volume_937": 120000.0,
            "history_hash": "c" * 64,
        }
    ]
    entry_render_semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "hypothetical-entry-event",
        "deployment_mode": "forward_shadow",
        "trade_date": "2026-08-31",
        "action": "ENTER",
        "final_multiplier": 1.0,
        "base_multiplier": 1.0,
        "defense_multiplier": 1.0,
        "health_state": "HEALTHY",
        "rolling7_state": "NON_BAD",
        "rolling7_r7": 0.1,
        "rolling7_l7": 1,
        "g_state": "NOT_EVALUATED",
        "reason_codes": [],
        "last_complete_bar": "09:39",
        "v16_funnel": {
            "step0_universe_count": 100,
            "step2_hot_board_count": 1,
            "final_candidates": 1,
        },
        "v16_board_avg_gains": {"银行": 1.23},
        "symbols": symbols,
        "scheduled_exits_today": [],
    }
    semantic = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": event_id,
        "strategy_version": "V20",
        "config_hash": "a" * 64,
        "state_semantics_hash": "b" * 64,
        "deployment_mode": "forward_shadow",
        "official_stream_id": "formal-stream",
        "state_lineage_id": "formal-lineage",
        "alert_code": "MANUAL_0939_CHAIN_PROBE_RESULT",
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "event_trade_date": "2026-08-31",
        "manual_request_id": "deploy-current-build-001",
        "probe_profile": "CURRENT_DEPLOYED_CODE_EXACT_0939_ENTRY_RENDER_V2",
        "probe_result": probe_result,
        "current_version_recomputed": True,
        "replay_reused": False,
        "data_source": "PERSISTED_09:31_09:39",
        "data_window_start": "09:31",
        "data_window_end": "09:39",
        "quote_coverage": 1.0,
        "raw_fact_n": 18,
        "v16_count": 1,
        "v20_action": "ENTER",
        "final_multiplier": 1.0,
        "symbols": symbols,
        "entry_render_semantic": entry_render_semantic,
        "official_entry_action": "INPUT_INVALID",
        "official_entry_event_id": "failed-entry-event",
        "official_state_changed": False,
        "orders_changed": False,
        "non_actionable": True,
        "retrospective_expired": True,
        "visible_message_mode": "AUTOMATIC_ENTRY_RENDER",
        "computed_at": "2026-08-31T15:30:00+08:00",
        "message": "formatter-owned manual full-chain probe detail",
    }
    if probe_result == "FAIL":
        semantic.update(
            {
                "current_version_recomputed": False,
                "v16_count": 0,
                "v20_action": None,
                "final_multiplier": None,
                "symbols": [],
                "quote_coverage": None,
                "failure_stage": "V16_SCAN",
                "failure_reason": "09:39原始行情覆盖不足",
                "visible_message_mode": "FAILURE_ALERT",
            }
        )
    return semantic


def test_manual_0939_chain_probe_pass_message_proves_fresh_current_version_run() -> None:
    semantic = _manual_0939_chain_probe_semantic()
    generated_at = datetime(2026, 8, 31, 15, 30, tzinfo=TZ)

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id=semantic["event_id"]),
        generated_at,
        21,
        True,
    )

    assert payload["event_type"] == "DATA_ALERT"
    assert payload["timeliness_status"] == "ON_TIME"
    assert "actionable_from" not in payload
    assert "expired_delivery_message" not in payload
    assert payload["message"] == render_entry_message(
        semantic["entry_render_semantic"],
        generated_at=generated_at,
        commit_marker=21,
        on_time=True,
    )
    assert str(payload["message"]).startswith("[V20][SHADOW] 每日决策")
    assert "当前版本早盘链路重算" not in payload["message"]
    assert "验收" not in payload["message"]


def test_manual_0939_chain_probe_can_verify_previous_session_after_midnight() -> None:
    semantic = {
        **_manual_0939_chain_probe_semantic(event_id="manual-chain-probe-after-midnight"),
        "computed_at": "2026-09-01T00:05:00+08:00",
    }

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id=semantic["event_id"]),
        datetime(2026, 9, 1, 0, 5, tzinfo=TZ),
        24,
        True,
    )

    assert payload["message"] == render_entry_message(
        semantic["entry_render_semantic"],
        generated_at=datetime(2026, 9, 1, 0, 5, tzinfo=TZ),
        commit_marker=24,
        on_time=True,
    )


def test_manual_0939_chain_probe_discloses_when_exact_coverage_was_not_frozen() -> None:
    semantic = {
        **_manual_0939_chain_probe_semantic(event_id="manual-chain-probe-coverage-note"),
        "quote_coverage": None,
        "quote_coverage_note": "NOT_EXPOSED_BY_EXISTING_REPLAY_HELPER",
    }

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id=semantic["event_id"]),
        datetime(2026, 8, 31, 15, 31, tzinfo=TZ),
        25,
        True,
    )

    assert payload["message"] == render_entry_message(
        semantic["entry_render_semantic"],
        generated_at=datetime(2026, 8, 31, 15, 31, tzinfo=TZ),
        commit_marker=25,
        on_time=True,
    )


def test_manual_0939_chain_probe_failure_message_is_explicit_and_non_actionable() -> None:
    semantic = _manual_0939_chain_probe_semantic(
        event_id="manual-chain-probe-failed",
        probe_result="FAIL",
    )

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id=semantic["event_id"]),
        datetime(2026, 8, 31, 15, 31, tzinfo=TZ),
        22,
        True,
    )

    lines = str(payload["message"]).splitlines()
    assert lines[0] == "[V20][SHADOW] 当前版本早盘链路重算｜❌ 失败"
    assert "当前部署版本未能完成全链路重新计算" in payload["message"]
    assert "失败阶段：V16_SCAN" in payload["message"]
    assert "失败原因：09:39原始行情覆盖不足" in payload["message"]
    assert "本验收消息不能用于下单" in payload["message"]
    assert "当时本应" not in payload["message"]


def test_frozen_official_entry_replay_preserves_message_bytes_exactly() -> None:
    source_message = "[V20] 每日决策 (2026-08-31 09:40)\n逐字原样：空格、换行、✅"
    semantic = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "frozen-replay-event",
        "strategy_version": "V20",
        "config_hash": "a" * 64,
        "state_semantics_hash": "b" * 64,
        "deployment_mode": "production_push",
        "official_stream_id": "formal-stream",
        "state_lineage_id": "formal-lineage",
        "alert_code": "MANUAL_MORNING_ENTRY_MESSAGE_REPLAY",
        "delivery_priority_class": "OPERATOR_NOTIFICATION",
        "manual_request_id": "manual-replay-001",
        "event_trade_date": "2026-08-31",
        "replay_profile": "FROZEN_OFFICIAL_ENTRY_MESSAGE_V1",
        "visible_message_mode": "FROZEN_OFFICIAL_PAYLOAD",
        "exact_automatic_message": True,
        "retrospective_expired": True,
        "source_entry_event_id": "source-entry-event",
        "source_entry_action": "ENTER",
        "source_final_multiplier": 1.0,
        "source_semantic_content_hash": "c" * 64,
        "source_payload_hash": "d" * 64,
        "message_sha256": hashlib.sha256(source_message.encode("utf-8")).hexdigest(),
        "symbols": [
            {
                "rank": 1,
                "code": "000001",
                "name": "平安银行",
                "snapshot_price": 10.26,
            }
        ],
        "official_state_changed": False,
        "orders_changed": False,
        "non_actionable": True,
        "message": source_message,
    }

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id=semantic["event_id"]),
        datetime(2026, 9, 1, 0, 5, tzinfo=TZ),
        26,
        True,
    )

    assert payload["message"] == source_message
    assert payload["message"].encode("utf-8") == source_message.encode("utf-8")


@pytest.mark.parametrize(
    ("updates", "error"),
    [
        ({"replay_reused": True}, "exact persisted 09:39 window"),
        ({"current_version_recomputed": False}, "current version"),
        ({"official_state_changed": True}, "state-preserving"),
        ({"orders_changed": True}, "state-preserving"),
        ({"v16_count": 2}, "V16 count"),
    ],
)
def test_manual_0939_chain_probe_cannot_masquerade_old_or_stateful_work_as_pass(
    updates: dict,
    error: str,
) -> None:
    semantic = {**_manual_0939_chain_probe_semantic(), **updates}

    with pytest.raises(ValueError, match=error):
        seal_v20_payload(
            _outbox_record("DATA_ALERT", semantic, event_id=semantic["event_id"]),
            datetime(2026, 8, 31, 15, 32, tzinfo=TZ),
            23,
            True,
        )


@pytest.mark.parametrize(
    ("alert_code", "expected_reason"),
    [
        (
            "ENTRY_CALENDAR_UNKNOWN_NO_BUY",
            "09:40仍无法确认交易日历，系统不能安全运行入场策略",
        ),
        ("SLOT_FINALIZED_FAILED", "系统直到09:45仍未完成并冻结早盘入场决策"),
    ],
)
def test_other_entry_failure_alerts_keep_the_same_no_buy_operator_action(
    alert_code: str,
    expected_reason: str,
) -> None:
    semantic = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "strategy_version": "V20",
        "deployment_mode": "forward_shadow",
        "alert_code": alert_code,
        "event_trade_date": "2026-08-31",
        "message": "raw engineering detail",
    }

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id=f"alert-{alert_code}"),
        datetime(2026, 8, 31, 9, 45, tzinfo=TZ),
        20,
        True,
    )

    assert "现在操作：不开仓，不补买，不追买" in payload["message"]
    assert expected_reason in payload["message"]
    assert "raw engineering detail" not in payload["message"]


def test_entry_cutoff_data_alert_is_rendered_as_a_human_no_buy_notice() -> None:
    semantic = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "strategy_version": "V20",
        "deployment_mode": "forward_shadow",
        "alert_code": "ENTRY_CUTOFF_NO_BUY",
        "event_trade_date": "2026-08-31",
        "message": "09:40 截止仍没有 durable 正常入场决定；今天不买，不要追买。",
    }

    payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic, event_id="cutoff-alert"),
        datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
        20,
        True,
    )

    lines = payload["message"].splitlines()
    assert lines[0] == "[V20][SHADOW] 入场报警｜不开仓"
    assert lines[2] == "🔴 现在操作：不开仓，不补买，不追买"
    assert "09:40截止前没有冻结出可执行的入场决策" in payload["message"]
    assert "DATA_ALERT" not in payload["message"]
    assert "durable" not in payload["message"]


@pytest.mark.parametrize(
    ("updates", "error"),
    [
        ({"event_id": "another-event"}, "event_id does not match"),
        ({"non_actionable": False}, "must be non-actionable"),
        ({"delivery_priority_class": "RUNTIME_CRITICAL_ALERT"}, "delivery priority"),
        ({"manual_request_id": ""}, "requires a request id"),
        ({"formal_decision_available": "yes"}, "decision flags"),
        ({"official_state_changed": 0}, "decision flags"),
    ],
)
def test_manual_trigger_receipt_semantics_fail_closed(updates: dict, error: str) -> None:
    semantic = {**_manual_trigger_receipt_semantic(), **updates}

    with pytest.raises(ValueError, match=error):
        seal_v20_payload(
            _outbox_record("DATA_ALERT", semantic, event_id="manual-event"),
            datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
            18,
            True,
        )


def test_late_entry_message_never_suggests_buying() -> None:
    message = render_entry_message(
        {
            "event_id": "late",
            "deployment_mode": "production_push",
            "trade_date": "2026-08-31",
            "action": "ENTER",
            "final_multiplier": 1.0,
            "symbols": [],
            "reason_codes": [],
        },
        generated_at=datetime(2026, 8, 31, 9, 40, 0, tzinfo=TZ),
        commit_marker=8,
        on_time=False,
    )

    assert "今天不要追买" in message
    assert "正常建立" not in message


def test_input_invalid_message_translates_cutoff_failure_for_operator() -> None:
    message = render_entry_message(
        {
            "event_id": "invalid",
            "deployment_mode": "production_push",
            "trade_date": "2026-08-31",
            "action": "INPUT_INVALID",
            "final_multiplier": 0.0,
            "reason_codes": ["ENTRY_INPUT_UNAVAILABLE_BY_0940"],
            "failure_detail": "09:39 terminal coverage 73/100",
            "symbols": [],
        },
        generated_at=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
        commit_marker=9,
        on_time=False,
    )

    assert "🔴 现在操作：不开仓，不补买，不追买" in message
    assert "09:40截止前没有形成完整、可靠的V16入场结果" in message
    assert "ENTRY_INPUT_UNAVAILABLE_BY_0940" not in message
    assert "09:39 terminal coverage 73/100" not in message
    assert "建立新模型批次" not in message


def test_exit_is_scoped_to_one_model_leg_not_account_holding() -> None:
    message = render_exit_message(
        {
            "event_id": "exit",
            "deployment_mode": "production_push",
            "exit_signal_type": "D1_CLOSE_CONFIRM_08",
            "code": "000001",
            "stock_name": "平安银行",
            "signal_date": "2026-08-28",
            "rank": 2,
            "model_leg_id": "leg-1234567890abcdef",
            "reference_entry_price": 10.0,
            "observed_close": 9.2,
            "wealth_factor": 0.92,
            "origin_final_relative_weight": 0.05,
            "rule_actionable_from": "2026-08-31T10:01:00+08:00",
            "reason_codes": [],
        },
        generated_at=datetime(2026, 8, 31, 10, 0, 5, tzinfo=TZ),
        commit_marker=9,
    )

    assert "建议退出该模型腿100%" in message
    assert "账户全部持仓" in message
    assert "D1 恐慌下杀 -8%" in message
    assert "该模型腿相对标准批次份额: 5.00%" in message


def test_manual_monitor_exit_identifies_its_origin_without_claiming_order_or_holding() -> None:
    semantic = {
        "schema_version": V20_EXIT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "manual-monitor-exit",
        "event_type": "EXIT_SIGNAL",
        "deployment_mode": "production_push",
        "exit_signal_type": "D1_CLOSE_CONFIRM_08",
        "origin_kind": "MANUAL_MONITOR",
        "source_event_id": "manual-monitor-armed-event",
        "code": "000001",
        "stock_name": "平安银行",
        "signal_date": "2026-08-31",
        "rank": 1,
        "model_leg_id": "manual-monitor-leg-000001",
        "reference_entry_price": 10.0,
        "observed_close": 9.2,
        "wealth_factor": 0.92,
        "origin_final_relative_weight": 0.1,
        "rule_actionable_from": "2026-09-01T10:01:00+08:00",
        "detection_trade_date": "2026-09-01",
        "detection_is_trading_day": True,
        "market_restriction": "TRADABLE",
        "reason_codes": [],
    }

    payload = seal_v20_payload(
        _outbox_record("EXIT_SIGNAL", semantic, event_id=semantic["event_id"]),
        datetime(2026, 9, 1, 10, 1, 5, tzinfo=TZ),
        20,
        True,
    )
    message = str(payload["message"])

    assert "来源：人工补挂的冻结票单监控腿" in message
    assert "只发卖出提醒，不代表系统已下单或持仓" in message
    assert "建议退出该模型腿100%（不是账户全部持仓）" in message
    assert "仅提示，不代表券商成交确认" in message
    assert "已创建订单" not in message
    assert "已确认持仓" not in message
    assert "已经卖出" not in message


def test_shadow_exit_and_reminder_are_explicitly_observation_only() -> None:
    semantic = {
        "schema_version": V20_EXIT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "shadow-exit",
        "event_type": "EXIT_SIGNAL",
        "deployment_mode": "forward_shadow",
        "exit_signal_type": "PLAN_1457",
        "code": "000001",
        "stock_name": "平安银行",
        "signal_date": "2026-08-27",
        "rank": 1,
        "model_leg_id": "leg-shadow-1234567890",
        "reference_entry_price": 10.0,
        "observed_close": None,
        "wealth_factor": None,
        "origin_final_relative_weight": 0.05,
        "rule_actionable_from": "2026-08-31T14:57:00+08:00",
        "reason_codes": [],
    }

    exit_message = render_exit_message(
        semantic,
        generated_at=datetime(2026, 8, 31, 14, 57, 1, tzinfo=TZ),
        commit_marker=10,
    )
    reminder_record = OutboxRecord(
        event_id="shadow-reminder",
        event_type="EXIT_REMINDER",
        route_id="V20_SHADOW_FEISHU",
        official_stream_id="shadow-stream",
        lineage_id="shadow-lineage",
        semantic={
            **semantic,
            "event_type": "EXIT_REMINDER",
            "original_exit_event_id": "shadow-exit",
        },
        semantic_content_hash="a" * 64,
        payload=None,
        payload_hash=None,
        generated_at=None,
        commit_marker=None,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
    )
    reminder = seal_v20_payload(
        reminder_record,
        datetime(2026, 9, 1, 9, 35, tzinfo=TZ),
        11,
        True,
    )

    assert "[V20][SHADOW]" in exit_message
    assert "前向观察：不替代当前正式策略建议" in exit_message
    assert "前向观察：不替代当前正式策略建议" in reminder["message"]


def test_exit_public_action_time_uses_durable_seal_clock_and_session_boundary() -> None:
    semantic = {
        "schema_version": V20_EXIT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "exit",
        "event_type": "EXIT_SIGNAL",
        "deployment_mode": "production_push",
        "exit_signal_type": "D1_CLOSE_CONFIRM_08",
        "code": "000001",
        "stock_name": "平安银行",
        "signal_date": "2026-08-28",
        "rank": 2,
        "model_leg_id": "leg-1234567890abcdef",
        "reference_entry_price": 10.0,
        "observed_close": 9.2,
        "wealth_factor": 0.92,
        "origin_final_relative_weight": 0.05,
        "rule_actionable_from": "2026-08-31T10:01:00+08:00",
        "detection_trade_date": "2026-08-31",
        "detection_is_trading_day": True,
        "reason_codes": [],
    }
    record = OutboxRecord(
        event_id="exit",
        event_type="EXIT_SIGNAL",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic=semantic,
        semantic_content_hash="a" * 64,
        payload=None,
        payload_hash=None,
        generated_at=None,
        commit_marker=None,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
    )

    lunch = seal_v20_payload(
        record,
        datetime(2026, 8, 31, 11, 45, tzinfo=TZ),
        10,
        True,
    )
    recovered_unknown_next_day = seal_v20_payload(
        record,
        datetime(2026, 9, 1, 9, 20, tzinfo=TZ),
        11,
        True,
    )

    assert lunch["actionable_from"] == "2026-08-31T13:00:00+08:00"
    assert recovered_unknown_next_day["actionable_from"] == "NEXT_TRADING_SESSION"
    assert lunch["schema_version"] == V20_FEISHU_PAYLOAD_SCHEMA
    assert lunch["feishu_formatter_profile"] == V20_FEISHU_FORMATTER_PROFILE


def test_d1_advance_notice_recovered_on_d2_keeps_the_frozen_d2_action_time() -> None:
    semantic = {
        "schema_version": V20_EXIT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "d1-plan",
        "event_type": "EXIT_SIGNAL",
        "deployment_mode": "production_push",
        "exit_signal_type": "D1_CLOSE_CONFIRM_08",
        "code": "000001",
        "stock_name": "平安银行",
        "signal_date": "2026-08-28",
        "rank": 1,
        "model_leg_id": "leg-d1-plan",
        "reference_entry_price": 10.0,
        "observed_close": 9.1,
        "wealth_factor": 0.91,
        "origin_final_relative_weight": 0.05,
        "rule_actionable_from": "2026-09-01T09:31:00+08:00",
        "detection_trade_date": "2026-08-31",
        "detection_is_trading_day": True,
        "next_confirmed_trade_date": "2026-09-01",
        "reason_codes": [],
    }
    record = OutboxRecord(
        event_id="d1-plan",
        event_type="EXIT_SIGNAL",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic=semantic,
        semantic_content_hash="a" * 64,
        payload=None,
        payload_hash=None,
        generated_at=None,
        commit_marker=None,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
    )

    before_open = seal_v20_payload(
        record,
        datetime(2026, 9, 1, 9, 20, tzinfo=TZ),
        12,
        True,
    )
    during_session = seal_v20_payload(
        record,
        datetime(2026, 9, 1, 10, 0, tzinfo=TZ),
        13,
        True,
    )

    assert before_open["actionable_from"] == "2026-09-01T09:31:00+08:00"
    assert during_session["actionable_from"] == "2026-09-01T10:00:00+08:00"


def test_non_trading_day_recovery_never_publishes_a_weekend_action_time() -> None:
    message = render_exit_message(
        {
            "event_id": "weekend-exit",
            "deployment_mode": "production_push",
            "exit_signal_type": "PLAN_1457",
            "code": "000001",
            "stock_name": "平安银行",
            "signal_date": "2026-08-27",
            "rank": 1,
            "model_leg_id": "leg-weekend",
            "reference_entry_price": 10.0,
            "observed_close": None,
            "wealth_factor": None,
            "rule_actionable_from": "2026-08-28T14:57:00+08:00",
            "detection_trade_date": "2026-08-29",
            "detection_is_trading_day": False,
            "next_confirmed_trade_date": None,
            "reason_codes": ["EXIT_SIGNAL_LATE_FORMATION"],
        },
        generated_at=datetime(2026, 8, 29, 10, 0, tzinfo=TZ),
        commit_marker=12,
    )

    assert "公开建议生效: 下一交易时段开始后" in message
    assert "公开建议生效: 2026-08-29" not in message


def test_unknown_calendar_stale_exit_is_actionable_without_guessing_session_state() -> None:
    message = render_exit_message(
        {
            "event_id": "unknown-calendar-exit",
            "deployment_mode": "production_push",
            "exit_signal_type": "PLAN_1457",
            "code": "000001",
            "stock_name": "平安银行",
            "signal_date": "2026-08-27",
            "rank": 1,
            "model_leg_id": "leg-unknown-calendar",
            "reference_entry_price": 10.0,
            "observed_close": None,
            "wealth_factor": None,
            "rule_actionable_from": "2026-08-28T14:57:00+08:00",
            "detection_trade_date": "2026-08-31",
            "detection_is_trading_day": False,
            "detection_calendar_status": "UNKNOWN",
            "next_confirmed_trade_date": None,
            "reason_codes": ["EXIT_SIGNAL_LATE_FORMATION"],
        },
        generated_at=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        commit_marker=13,
    )

    assert "若当前可交易则立即执行；否则下一交易时段开始后执行" in message


class _PublisherRepository:
    def __init__(self, record: OutboxRecord) -> None:
        self.record = record
        self.completed = None
        self.lease_kwargs = None

    async def lease_outbox(self, **kwargs):
        self.lease_kwargs = kwargs
        return [self.record]

    async def complete_delivery(self, event_id, **kwargs):
        self.completed = (event_id, kwargs)


class _CapturingRelay:
    def __init__(self) -> None:
        self.envelopes: list[dict] = []

    def is_configured(self):
        return True

    async def send_delivery(self, envelope):
        self.envelopes.append(dict(envelope))
        return True


class _FailingRelay(_CapturingRelay):
    async def send_delivery(self, envelope):
        self.envelopes.append(dict(envelope))
        return False


def _route(relay, *, transport="v20_relay"):
    return SimpleNamespace(
        is_configured=lambda: True,
        destination_fingerprint="d" * 64,
        transport=transport,
        relay=lambda: relay,
    )


async def test_publisher_gives_relay_both_buy_and_expired_notice_with_expiry() -> None:
    expiry = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    record = OutboxRecord(
        event_id="entry-event",
        event_type="ENTRY_DECISION",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic={"action": "ENTER", "final_multiplier": 1.0},
        semantic_content_hash="a" * 64,
        payload={
            "message": "100%正常建立",
            "expired_delivery_message": "已过期；今天不要据此追买",
        },
        payload_hash="b" * 64,
        generated_at=datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ),
        commit_marker=1,
        action_expiry_ts=expiry,
        delivery_status="PENDING",
        attempt_count=0,
        lease_db_ts=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
    )
    repository = _PublisherRepository(record)
    relay = _CapturingRelay()
    route = _route(relay)
    publisher = V20OutboxPublisher(
        repository,
        {"route": route},
        worker_id="worker",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        clock=lambda: datetime(2026, 8, 31, 9, 30, tzinfo=TZ),
    )

    sent = await publisher.publish_once()

    assert sent == 1
    assert repository.lease_kwargs == {
        "worker_id": "worker",
        "route_id": "route",
        "official_stream_id": "formal-stream",
        "lineage_id": "formal-lineage",
        "limit": 1,
    }
    assert repository.completed[1]["route_id"] == "route"
    assert repository.completed[1]["official_stream_id"] == "formal-stream"
    assert repository.completed[1]["lineage_id"] == "formal-lineage"
    assert relay.envelopes == [
        {
            "schema_version": V20_RELAY_REQUEST_SCHEMA,
            "event_id": "entry-event",
            "event_type": "ENTRY_DECISION",
            "route_id": "route",
            "idempotency_key": "route:entry-event",
            "payload_hash": "b" * 64,
            "delivery_class": "ACTIONABLE_ENTRY",
            "action_expiry_ts": expiry.isoformat(),
            "message": "100%正常建立",
            "expired_delivery_message": "已过期；今天不要据此追买",
            "destination_fingerprint": "d" * 64,
        }
    ]


async def test_embedded_legacy_delivery_is_downgraded_by_database_clock_after_expiry() -> None:
    expiry = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    record = OutboxRecord(
        event_id="late-entry-event",
        event_type="ENTRY_DECISION",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic={"action": "ENTER", "final_multiplier": 1.0},
        semantic_content_hash="a" * 64,
        payload={
            "message": "buy",
            "expired_delivery_message": "do not buy",
        },
        payload_hash="b" * 64,
        generated_at=datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ),
        commit_marker=1,
        action_expiry_ts=expiry,
        delivery_status="PENDING",
        attempt_count=0,
        lease_db_ts=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
    )
    repository = _PublisherRepository(record)
    relay = _CapturingRelay()
    publisher = V20OutboxPublisher(
        repository,
        {"route": _route(relay, transport="legacy_send")},
        worker_id="worker",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
    )

    assert await publisher.publish_once() == 1
    assert relay.envelopes[0]["delivery_class"] == "NON_ACTIONABLE_ENTRY"
    assert relay.envelopes[0]["action_expiry_ts"] is None
    assert relay.envelopes[0]["message"] == "do not buy"
    assert relay.envelopes[0]["expired_delivery_message"] is None


async def test_publisher_preserves_late_input_invalid_reason_message() -> None:
    expiry = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    record = OutboxRecord(
        event_id="invalid-entry-event",
        event_type="ENTRY_DECISION",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic={
            "action": "INPUT_INVALID",
            "final_multiplier": 0.0,
            "reason_codes": ["ENTRY_INPUT_UNAVAILABLE_BY_0940"],
        },
        semantic_content_hash="a" * 64,
        payload={
            "message": "输入异常：ENTRY_INPUT_UNAVAILABLE_BY_0940，今天不买",
            "expired_delivery_message": "通用过期文案",
        },
        payload_hash="b" * 64,
        generated_at=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
        commit_marker=1,
        action_expiry_ts=expiry,
        delivery_status="PENDING",
        attempt_count=0,
        lease_db_ts=datetime(2026, 8, 31, 9, 40, 1, tzinfo=TZ),
    )
    repository = _PublisherRepository(record)
    relay = _CapturingRelay()
    route = _route(relay)
    publisher = V20OutboxPublisher(
        repository,
        {"route": route},
        worker_id="worker",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
    )

    assert await publisher.publish_once() == 1
    assert relay.envelopes[0]["delivery_class"] == "NON_ACTIONABLE_ENTRY"
    assert relay.envelopes[0]["action_expiry_ts"] is None
    assert relay.envelopes[0]["expired_delivery_message"] is None
    assert "输入异常" in relay.envelopes[0]["message"]


async def test_manual_trigger_receipt_is_published_only_as_notification() -> None:
    semantic = _manual_trigger_receipt_semantic()
    sealed_payload = seal_v20_payload(
        _outbox_record("DATA_ALERT", semantic),
        datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
        19,
        True,
    )
    record = OutboxRecord(
        event_id="manual-event",
        event_type="DATA_ALERT",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        payload=sealed_payload,
        payload_hash=sha256_json(sealed_payload),
        generated_at=datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
        commit_marker=19,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
        lease_db_ts=datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
    )
    repository = _PublisherRepository(record)
    relay = _CapturingRelay()
    publisher = V20OutboxPublisher(
        repository,
        {"route": _route(relay)},
        worker_id="worker",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
    )

    assert await publisher.publish_once() == 1

    assert repository.completed[1]["succeeded"] is True
    assert relay.envelopes == [
        {
            "schema_version": V20_RELAY_REQUEST_SCHEMA,
            "event_id": "manual-event",
            "event_type": "DATA_ALERT",
            "route_id": "route",
            "idempotency_key": "route:manual-event",
            "payload_hash": sha256_json(sealed_payload),
            "delivery_class": "NOTIFICATION",
            "action_expiry_ts": None,
            "message": sealed_payload["message"],
            "expired_delivery_message": None,
            "destination_fingerprint": "d" * 64,
        }
    ]


async def test_publisher_fails_closed_if_repository_returns_another_scope() -> None:
    record = OutboxRecord(
        event_id="shadow-event",
        event_type="DATA_ALERT",
        route_id="shadow-route",
        official_stream_id="shadow-stream",
        lineage_id="shadow-lineage",
        semantic={},
        semantic_content_hash="a" * 64,
        payload={"message": "shadow"},
        payload_hash="b" * 64,
        generated_at=datetime(2026, 8, 31, 9, 0, tzinfo=TZ),
        commit_marker=1,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
    )
    repository = _PublisherRepository(record)
    relay = _CapturingRelay()
    route = _route(relay)
    publisher = V20OutboxPublisher(
        repository,
        {"formal-route": route},
        worker_id="formal-worker",
        route_id="formal-route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
    )

    with pytest.raises(V20StateConflict, match="escaped publisher scope"):
        await publisher.publish_once()

    assert relay.envelopes == []
    assert repository.completed is None


async def test_publisher_exposes_durable_relay_failure_to_runtime_health() -> None:
    record = OutboxRecord(
        event_id="exit-event",
        event_type="EXIT_SIGNAL",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic={},
        semantic_content_hash="a" * 64,
        payload={"message": "exit"},
        payload_hash="b" * 64,
        generated_at=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        commit_marker=1,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
    )
    repository = _PublisherRepository(record)
    relay = _FailingRelay()
    route = _route(relay)
    publisher = V20OutboxPublisher(
        repository,
        {"route": route},
        worker_id="worker",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
    )

    assert await publisher.publish_once() == 0

    assert publisher.last_cycle_error == "Feishu relay returned failure"
    assert repository.completed[1]["succeeded"] is False


async def test_publisher_cycle_guard_fails_before_outbox_lease() -> None:
    record = OutboxRecord(
        event_id="guarded-event",
        event_type="DATA_ALERT",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
        semantic={},
        semantic_content_hash="a" * 64,
        payload={"message": "alert"},
        payload_hash="b" * 64,
        generated_at=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        commit_marker=1,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
    )
    repository = _PublisherRepository(record)
    route = _route(_CapturingRelay())
    publisher = V20OutboxPublisher(
        repository,
        {"route": route},
        worker_id="worker",
        route_id="route",
        official_stream_id="formal-stream",
        lineage_id="formal-lineage",
    )

    async def lost_leadership() -> None:
        raise RuntimeError("leader lost")

    with pytest.raises(RuntimeError, match="leader lost"):
        await publisher.run(asyncio.Event(), before_cycle=lost_leadership)

    assert repository.lease_kwargs is None


class _RelayResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _RelayHttpClient:
    response_payload: dict = {}
    request_json: dict | None = None
    request_url: str | None = None

    def __init__(self, **_kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args) -> None:
        return None

    async def post(self, url: str, *, json: dict):
        type(self).request_url = url
        type(self).request_json = json
        return _RelayResponse(type(self).response_payload)


def _relay_envelope() -> dict:
    return {
        "schema_version": V20_RELAY_REQUEST_SCHEMA,
        "event_id": "event-1",
        "event_type": "ENTRY_DECISION",
        "route_id": "formal-route",
        "idempotency_key": "formal-route:event-1",
        "payload_hash": "a" * 64,
        "delivery_class": "ACTIONABLE_ENTRY",
        "action_expiry_ts": "2026-08-31T09:40:00+08:00",
        "message": "buy",
        "expired_delivery_message": "do not buy",
        "destination_fingerprint": "d" * 64,
    }


def _relay_receipt(**updates) -> dict:
    receipt = {
        "schema_version": V20_RELAY_RESPONSE_SCHEMA,
        "code": 0,
        "event_id": "event-1",
        "route_id": "formal-route",
        "idempotency_key": "formal-route:event-1",
        "payload_hash": "a" * 64,
        "delivery_status": "DELIVERED_ACTIONABLE",
        "duplicate": False,
        "accepted_at": "2026-08-31T09:39:59+08:00",
        "destination_fingerprint": "d" * 64,
    }
    receipt.update(updates)
    return receipt


async def test_v20_relay_uses_versioned_endpoint_and_strict_idempotency_envelope(
    monkeypatch,
) -> None:
    monkeypatch.setattr("src.common.v20_feishu.httpx.AsyncClient", _RelayHttpClient)
    _RelayHttpClient.response_payload = _relay_receipt(duplicate=True)
    client = V20RelayClient(
        bot_origin="https://relay.internal",
        app_id="app",
        app_secret="secret",
        chat_id="chat",
        destination_fingerprint="d" * 64,
    )

    assert await client.send_delivery(_relay_envelope()) is True
    assert _RelayHttpClient.request_url == "https://relay.internal/api/v20/send"
    assert _RelayHttpClient.request_json == {
        **_relay_envelope(),
        "app_id": "app",
        "app_secret": "secret",
        "chat_id": "chat",
    }


@pytest.mark.parametrize(
    ("now", "expected_message"),
    [
        (datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ), "buy"),
        (datetime(2026, 8, 31, 9, 40, tzinfo=TZ), "do not buy"),
    ],
)
async def test_legacy_embedded_relay_uses_existing_endpoint_and_honors_entry_expiry(
    monkeypatch,
    now,
    expected_message,
) -> None:
    monkeypatch.setattr("src.common.v20_feishu.httpx.AsyncClient", _RelayHttpClient)
    _RelayHttpClient.response_payload = {"code": 0, "msg": "success"}
    client = V20LegacyRelayClient(
        bot_origin="https://legacy-relay.example",
        app_id="legacy-app",
        app_secret="legacy-secret",
        chat_id="legacy-chat",
        destination_fingerprint="d" * 64,
        clock=lambda: now,
    )

    assert await client.send_delivery(_relay_envelope()) is True
    assert _RelayHttpClient.request_url == "https://legacy-relay.example/api/send"
    assert _RelayHttpClient.request_json == {
        "app_id": "legacy-app",
        "app_secret": "legacy-secret",
        "chat_id": "legacy-chat",
        "message": expected_message,
    }


@pytest.mark.parametrize(
    "updates,match",
    [
        ({"code": False}, "exact success code"),
        ({"code": 1}, "exact success code"),
        ({"duplicate": 1}, "duplicate flag must be boolean"),
        ({"event_id": "other"}, "event_id echo mismatch"),
        ({"route_id": "other"}, "route_id echo mismatch"),
        ({"idempotency_key": "other"}, "idempotency_key echo mismatch"),
        ({"payload_hash": "b" * 64}, "payload_hash echo mismatch"),
        ({"destination_fingerprint": "e" * 64}, "destination_fingerprint echo"),
        ({"accepted_at": "2026-08-31T09:39:59"}, "timezone-aware"),
        (
            {
                "delivery_status": "DELIVERED_ACTIONABLE",
                "accepted_at": "2026-08-31T09:40:00+08:00",
            },
            "after expiry",
        ),
        (
            {
                "delivery_status": "DELIVERED_EXPIRED_NOTICE",
                "accepted_at": "2026-08-31T09:39:59+08:00",
            },
            "before expiry",
        ),
    ],
)
async def test_v20_relay_rejects_unproven_or_inconsistent_receipts(
    monkeypatch,
    updates,
    match,
) -> None:
    monkeypatch.setattr("src.common.v20_feishu.httpx.AsyncClient", _RelayHttpClient)
    _RelayHttpClient.response_payload = _relay_receipt(**updates)
    client = V20RelayClient(
        bot_origin="https://relay.internal",
        app_id="app",
        app_secret="secret",
        chat_id="chat",
        destination_fingerprint="d" * 64,
    )

    with pytest.raises(V20RelayContractError, match=match):
        await client.send_delivery(_relay_envelope())


async def test_v20_relay_accepts_expired_notice_only_at_or_after_expiry(monkeypatch) -> None:
    monkeypatch.setattr("src.common.v20_feishu.httpx.AsyncClient", _RelayHttpClient)
    _RelayHttpClient.response_payload = _relay_receipt(
        delivery_status="DELIVERED_EXPIRED_NOTICE",
        accepted_at="2026-08-31T09:40:00+08:00",
    )
    client = V20RelayClient(
        bot_origin="https://relay.internal",
        app_id="app",
        app_secret="secret",
        chat_id="chat",
        destination_fingerprint="d" * 64,
    )

    assert await client.send_delivery(_relay_envelope()) is True


async def test_v20_relay_requires_plain_delivery_for_non_actionable_entry(
    monkeypatch,
) -> None:
    monkeypatch.setattr("src.common.v20_feishu.httpx.AsyncClient", _RelayHttpClient)
    envelope = {
        **_relay_envelope(),
        "delivery_class": "NON_ACTIONABLE_ENTRY",
        "action_expiry_ts": None,
        "expired_delivery_message": None,
    }
    _RelayHttpClient.response_payload = _relay_receipt(
        delivery_status="DELIVERED",
        accepted_at="2026-08-31T10:00:00+08:00",
    )
    client = V20RelayClient(
        bot_origin="https://relay.internal",
        app_id="app",
        app_secret="secret",
        chat_id="chat",
        destination_fingerprint="d" * 64,
    )

    assert await client.send_delivery(envelope) is True
