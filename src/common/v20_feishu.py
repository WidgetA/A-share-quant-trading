"""Deterministic V20 Feishu messages and durable-outbox publisher."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any, Callable, Literal
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

from src.data.database.v20_repository import OutboxRecord, V20Repository, V20StateConflict
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_EXIT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_FEISHU_PAYLOAD_SCHEMA,
)

logger = logging.getLogger(__name__)
_ENTRY_ACTION_SEND_GUARD_SECONDS = 1.0
_NON_EXPIRING_SEND_TIMEOUT_SECONDS = 2.0
SHANGHAI = ZoneInfo("Asia/Shanghai")
V20_RELAY_REQUEST_SCHEMA = "v20-relay-request/v1"
V20_RELAY_RESPONSE_SCHEMA = "v20-relay-response/v1"
_DELIVERY_CLASSES = {
    "ACTIONABLE_ENTRY",
    "NON_ACTIONABLE_ENTRY",
    "NOTIFICATION",
}
_DELIVERY_STATUSES = {
    "DELIVERED_ACTIONABLE",
    "DELIVERED_EXPIRED_NOTICE",
    "DELIVERED",
}


class V20RelayContractError(RuntimeError):
    """The dedicated relay did not prove the exact V20 delivery contract."""


def _canonical_relay_origin(value: str) -> str | None:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        return None
    try:
        port = parsed.port
    except ValueError:
        return None
    hostname = parsed.hostname.lower()
    if ":" in hostname:
        hostname = f"[{hostname}]"
    return f"https://{hostname}{f':{port}' if port is not None else ''}"


def _destination_fingerprint(*, route_id: str, bot_origin: str, app_id: str, chat_id: str) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "schema_version": "v20-destination-binding/v1",
                "route_id": route_id,
                "expected_bot_origin": bot_origin,
                "expected_app_id_sha256": hashlib.sha256(app_id.encode("utf-8")).hexdigest(),
                "expected_chat_id_sha256": hashlib.sha256(chat_id.encode("utf-8")).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _pct(value: float | None, digits: int = 2) -> str:
    return "-" if value is None else f"{value * 100:.{digits}f}%"


def _entry_action_text(multiplier: float, *, on_time: bool) -> str:
    if not on_time:
        return "⛔ 已过09:40：仅记入评价账本，今天不要追买"
    mapping = {
        1.0: "✅ 100%标准批次：正常建立",
        0.5: "🟡 50%标准批次：防御建立",
        0.25: "🟠 25%标准批次：极谨慎建立",
        0.0: "⛔ 0%：今天不建立新模型批次",
    }
    try:
        return mapping[float(multiplier)]
    except KeyError as exc:
        raise ValueError(f"unsupported V20 final multiplier: {multiplier}") from exc


def render_entry_message(
    semantic: Mapping[str, Any],
    *,
    generated_at: datetime,
    commit_marker: int,
    on_time: bool,
) -> str:
    """Render the one daily V20 entry-decision message."""
    mode = str(semantic.get("deployment_mode", "forward_shadow"))
    shadow = mode == "forward_shadow"
    title = "[V20][SHADOW] 每日决策" if shadow else "[V20] 每日决策"
    trade_date = str(semantic["trade_date"])
    action = str(semantic.get("action", "INPUT_INVALID"))
    multiplier = float(semantic.get("final_multiplier", 0.0))
    lines = [f"{title} ({trade_date} 09:40)"]
    if shadow:
        lines.append("⚪ 前向观察：不替代当前正式策略建议")
    if action == "NO_SIGNAL":
        lines.append("今日V16完整扫描合法无票，不建立新模型批次")
    elif action == "INPUT_INVALID":
        lines.append("🚨 输入异常：本日不给入场建议")
        if semantic.get("failure_detail"):
            lines.append(f"故障详情: {semantic['failure_detail']}")
    else:
        lines.append(_entry_action_text(multiplier, on_time=on_time))

    lines.extend(
        [
            "",
            (
                f"BASE: {semantic.get('health_state', '-')} / "
                f"基础倍率 {_pct(semantic.get('base_multiplier'), 0)}"
            ),
            (
                f"滚动7: {semantic.get('rolling7_state', '-')} | "
                f"R7={_pct(semantic.get('rolling7_r7'))} | "
                f"亏损批次={semantic.get('rolling7_l7', '-')}/7"
            ),
            (
                f"极端门G: {semantic.get('g_state', 'NOT_EVALUATED')} | "
                f"防御倍率 {_pct(semantic.get('defense_multiplier'), 0)} | "
                f"最终 {_pct(multiplier, 0)}"
            ),
        ]
    )
    reasons = semantic.get("reason_codes") or []
    if reasons:
        lines.append("原因: " + " / ".join(str(item) for item in reasons))

    funnel = semantic.get("v16_funnel") or {}
    if funnel:
        lines.append(
            "V16扫描: "
            f"股票池 {funnel.get('step0_universe_count', '-')}只 | "
            f"热门板块 {funnel.get('step2_hot_board_count', '-')}个 | "
            f"最终 {funnel.get('final_candidates', '-')}只"
        )

    symbols = semantic.get("symbols") or []
    if symbols:
        from src.strategy.filters.board_filter import BROAD_CONCEPT_BOARDS

        board_gains = semantic.get("v16_board_avg_gains") or {}
        broad_shown = False
        driver_tag_shown = False

        def format_boards(item: Mapping[str, Any]) -> str:
            nonlocal broad_shown
            boards = item.get("boards") or []
            if not boards and item.get("best_board"):
                boards = [item["best_board"]]
            if not boards:
                return "-"
            parts: list[str] = []
            for board in boards:
                board_text = str(board)
                star = "⭐" if board_text in BROAD_CONCEPT_BOARDS else ""
                broad_shown = broad_shown or bool(star)
                gain = board_gains.get(board_text, 0.0)
                parts.append(f"{star}{board_text}({float(gain):+.2f}%)")
            return "、".join(parts)

        def driver_tag(item: Mapping[str, Any]) -> str:
            nonlocal driver_tag_shown
            is_driver = item.get("is_driver")
            if is_driver is None:
                return ""
            driver_tag_shown = True
            return "[带动]" if is_driver is True else "[扩增]"

        def optional_metrics(item: Mapping[str, Any]) -> str:
            cci = item.get("cci")
            volume_937 = item.get("volume_937")
            cci_part = f"  CCI={float(cci):.0f}" if cci is not None else ""
            volume_part = f"  7min={float(volume_937) / 10000:.0f}万" if volume_937 else ""
            return cci_part + volume_part

        top1 = symbols[0]
        lines.extend(
            [
                "",
                f"V16完整推荐（{len(symbols)}只）:",
                f"推荐 Top-1: {top1['code']} {top1.get('name', '')}",
                (
                    f"  板块: {driver_tag(top1)}{format_boards(top1)} | "
                    f"LGB: {float(top1['score']):.4f} | "
                    f"09:39快照: {float(top1['snapshot_price']):.2f}"
                    f"{optional_metrics(top1)}"
                ),
                "",
                "评分前10:",
            ]
        )
        for item in symbols:
            lines.append(
                f"{int(item['rank'])}. {item['code']} {item.get('name', '')}  "
                f"LGB={float(item['score']):.4f}  "
                f"09:39快照:{float(item['snapshot_price']):.2f}  "
                f"{driver_tag(item)}{format_boards(item)}"
                f"{optional_metrics(item)}"
            )
        if broad_shown:
            lines.extend(["", "⭐=宽泛板块(成分≥400,题材偏泛,仅供参考)"])
        if driver_tag_shown:
            lines.append(
                "[带动]=个股自身涨幅已达热门板块门槛(0.8%),自己就能带火板块 | "
                "[扩增]=仅个股涨幅未到0.8%,靠板块内其他股票拉高均值才被纳入"
            )
        if on_time and multiplier > 0:
            per_leg = multiplier / len(symbols)
            lines.append(f"每只模型腿相对份额: {_pct(per_leg)}（不代表账户金额或股数）")
            lines.append("09:40参考价将在原始09:41结束标签出现后单独锁定")

    scheduled_exits = semantic.get("scheduled_exits_today") or []
    if scheduled_exits:
        lines.extend(["", f"今天已有模型腿计划退出（{len(scheduled_exits)}只）:"])
        for item in scheduled_exits:
            lines.append(
                f"- {item['code']} {item.get('stock_name', '')}  "
                f"D0={item.get('signal_date', '-')} / rank={item.get('rank', '-')} / "
                f"腿份额={_pct(item.get('relative_weight'))}；"
                f"最迟{item.get('plan_time', '14:57')}整腿退出，保护线命中则提前通知"
            )

    lines.extend(
        [
            "",
            "有效期: 本入场建议仅在当日09:40前有效；迟到消息不得据此追买",
            f"数据边界: raw {semantic.get('last_complete_bar', '-')}结束标签",
            f"生成: {generated_at.isoformat()} | marker={commit_marker}",
            f"事件: {semantic.get('event_id', '-')}",
        ]
    )
    return "\n".join(lines)


def render_expired_entry_delivery_message(semantic: Mapping[str, Any]) -> str:
    """Render a fail-closed notice when transport misses the entry expiry."""

    mode = str(semantic.get("deployment_mode", "forward_shadow"))
    title = (
        "[V20][SHADOW] 入场消息投递已过期"
        if mode == "forward_shadow"
        else "[V20] 入场消息投递已过期"
    )
    return "\n".join(
        [
            f"{title} ({semantic.get('trade_date', '-')})",
            "⚠️ 投递时已经达到或超过09:40；本条只作审计，今天不要据此追买。",
            (
                f"原计算动作: {semantic.get('action', '-')} | "
                f"原最终倍率: {_pct(semantic.get('final_multiplier'), 0)}"
            ),
            f"事件: {semantic.get('event_id', '-')}",
        ]
    )


_EXIT_LABELS = {
    "D1_CLOSE_CONFIRM_08": "D1 恐慌下杀 -8%",
    "D2_ENTRY_12": "D2 常驻底线 -12%",
    "D2_MEWS_DANGER_ENTRY_05": "D2 MEWS危险线 -5%",
    "PLAN_1457": "D2 14:57计划退出",
}


def _public_actionable_from(
    semantic: Mapping[str, Any],
    generated_at: datetime,
) -> str:
    """Derive a public action time from the durable seal clock, never worker time."""

    rule = datetime.fromisoformat(str(semantic["rule_actionable_from"])).astimezone(SHANGHAI)
    generated = generated_at.astimezone(SHANGHAI)
    detection_date = str(semantic.get("detection_trade_date", generated.date().isoformat()))
    generated_date = generated.date().isoformat()
    next_confirmed = semantic.get("next_confirmed_trade_date")

    # D1/D2 are frozen from the exchange calendar when the model leg is born.
    # A crash-delayed seal on the rule's own date may therefore use that date
    # even though it differs from the original detection date.  For any later
    # date we need explicit calendar evidence; otherwise say only "next trading
    # session" and never fabricate a weekend/current-session instruction.
    if generated.date() <= rule.date():
        candidate = max(rule, generated)
    elif (
        semantic.get("detection_is_trading_day") is True and generated_date == detection_date
    ) or generated_date == next_confirmed:
        candidate = generated
    elif semantic.get("detection_calendar_status") == "UNKNOWN":
        return "IMMEDIATE_IF_TRADABLE_ELSE_NEXT_SESSION"
    else:
        return "NEXT_TRADING_SESSION"
    wall = candidate.timetz().replace(tzinfo=None)
    if wall < time(9, 30):
        return candidate.replace(hour=9, minute=30, second=0, microsecond=0).isoformat()
    if time(11, 30) <= wall < time(13, 0):
        return candidate.replace(hour=13, minute=0, second=0, microsecond=0).isoformat()
    if wall >= time(15, 0):
        return "NEXT_TRADING_SESSION"
    return candidate.isoformat()


def render_exit_message(
    semantic: Mapping[str, Any],
    *,
    generated_at: datetime,
    commit_marker: int,
) -> str:
    mode = str(semantic.get("deployment_mode", "forward_shadow"))
    title = "[V20][SHADOW] 退出观察" if mode == "forward_shadow" else "[V20] 退出建议"
    signal_type = str(semantic["exit_signal_type"])
    actionable_from = _public_actionable_from(semantic, generated_at)
    actionable_text = {
        "NEXT_TRADING_SESSION": "下一交易时段开始后",
        "IMMEDIATE_IF_TRADABLE_ELSE_NEXT_SESSION": (
            "若当前可交易则立即执行；否则下一交易时段开始后执行"
        ),
    }.get(actionable_from, actionable_from)
    lines = [
        title,
        (
            "⚪ 前向观察：不替代当前正式策略建议"
            if mode == "forward_shadow"
            else "⚠️ 正式策略退出建议"
        ),
        "建议退出该模型腿100%（不是账户全部持仓）",
        f"股票: {semantic['code']} {semantic.get('stock_name', '')}",
        (
            f"模型腿: D0={semantic['signal_date']} / rank={semantic['rank']} / "
            f"{str(semantic['model_leg_id'])[:16]}"
        ),
        f"触发: {_EXIT_LABELS.get(signal_type, signal_type)}",
    ]
    if semantic.get("origin_final_relative_weight") is not None:
        lines.append(
            f"该模型腿相对标准批次份额: {_pct(float(semantic['origin_final_relative_weight']))}"
        )
    reference = semantic.get("reference_entry_price")
    observed = semantic.get("observed_close")
    wealth_factor = semantic.get("wealth_factor")
    if reference is not None:
        lines.append(f"参考价: {float(reference):.2f}")
    else:
        lines.append("参考价: 不可用（计划退出不因此留豁口）")
    if observed is not None:
        if wealth_factor is None:
            raise ValueError("observed exit price requires a wealth factor")
        lines.append(
            f"触发分钟收盘: {float(observed):.2f} | 相对参考: {_pct(float(wealth_factor) - 1)}"
        )
    if semantic.get("mews_fast_state") is not None:
        lines.append(
            f"MEWS: {semantic['mews_fast_state']} / {semantic.get('mews_source_trade_date', '-')}"
        )
    lines.append(
        "市场可成交状态: "
        f"{semantic.get('market_restriction', 'UNKNOWN')}（仅提示，不代表券商成交确认）"
    )
    lines.extend(
        [
            f"规则可行动时点: {semantic['rule_actionable_from']}",
            f"公开建议生效: {actionable_text}",
            f"生成: {generated_at.isoformat()} | marker={commit_marker}",
            f"事件: {semantic.get('event_id', '-')}",
        ]
    )
    if "EXIT_SIGNAL_LATE_FORMATION" in (semantic.get("reason_codes") or []):
        lines.append("注意: 历史触发被迟到识别，生效时间没有回填到过去")
    gap_reasons = {
        "D1_WINDOW_INCOMPLETE",
        "D2_WINDOW_INCOMPLETE",
        "EXIT_BAR_INPUT_UNAVAILABLE",
        "EXIT_WATERMARK_UNAVAILABLE",
    }.intersection(semantic.get("reason_codes") or [])
    if gap_reasons:
        lines.append("数据提示: 分钟保护窗口存在缺口；本条D2计划退出仍然有效")
    return "\n".join(lines)


def _finite_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _require_fields(
    value: Mapping[str, Any],
    required: set[str],
    *,
    subject: str,
) -> None:
    missing = sorted(required - set(value))
    if missing:
        raise ValueError(f"{subject} is missing required fields: {missing}")


def _validate_entry_formatter_semantic(
    record: OutboxRecord,
    semantic: Mapping[str, Any],
) -> None:
    _require_fields(
        semantic,
        {
            "event_id",
            "deployment_mode",
            "trade_date",
            "action",
            "final_multiplier",
            "base_multiplier",
            "defense_multiplier",
            "health_state",
            "rolling7_state",
            "rolling7_r7",
            "rolling7_l7",
            "g_state",
            "reason_codes",
            "last_complete_bar",
            "symbols",
            "scheduled_exits_today",
        },
        subject="V20 entry semantic",
    )
    if semantic["event_id"] != record.event_id:
        raise ValueError("V20 entry semantic event_id does not match outbox event")
    action = semantic["action"]
    if action not in {"ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"}:
        raise ValueError("V20 entry semantic action is unsupported")
    multiplier = semantic["final_multiplier"]
    if not _finite_number(multiplier) or not 0 <= float(multiplier) <= 1:
        raise ValueError("V20 entry semantic final_multiplier is invalid")
    symbols = semantic["symbols"]
    if not isinstance(symbols, list):
        raise ValueError("V20 entry semantic symbols must be an array")
    if not isinstance(semantic["scheduled_exits_today"], list):
        raise ValueError("V20 entry semantic scheduled_exits_today must be an array")
    if action == "ENTER" and (float(multiplier) <= 0 or not symbols):
        raise ValueError("V20 ENTER semantic requires a positive multiplier and symbols")
    if action != "ENTER" and float(multiplier) != 0:
        raise ValueError("non-ENTER V20 semantic must have a zero multiplier")
    if action in {"NO_SIGNAL", "INPUT_INVALID"} and symbols:
        raise ValueError(f"V20 {action} semantic cannot contain symbols")
    if action == "INPUT_INVALID":
        detail = semantic.get("failure_detail")
        if not isinstance(detail, str) or not detail:
            raise ValueError("V20 INPUT_INVALID semantic requires failure_detail")
        return

    funnel = semantic.get("v16_funnel")
    board_gains = semantic.get("v16_board_avg_gains")
    if not isinstance(funnel, Mapping) or not isinstance(board_gains, Mapping):
        raise ValueError("V20 entry semantic lacks frozen V16 formatter evidence")
    for field in ("step0_universe_count", "step2_hot_board_count", "final_candidates"):
        value = funnel.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"V20 entry semantic funnel.{field} is invalid")
    if any(
        not isinstance(board, str) or not board or not _finite_number(gain)
        for board, gain in board_gains.items()
    ):
        raise ValueError("V20 entry semantic board gains are invalid")

    required_symbol_fields = {
        "rank",
        "code",
        "name",
        "score",
        "snapshot_price",
        "boards",
        "best_board",
        "is_driver",
        "cci",
        "volume_937",
        "history_hash",
    }
    for item in symbols:
        if not isinstance(item, Mapping) or not required_symbol_fields.issubset(item):
            raise ValueError("V20 entry symbol formatter evidence is incomplete")
        boards = item["boards"]
        if (
            not isinstance(boards, list)
            or not boards
            or any(not isinstance(board, str) or not board for board in boards)
            or len(set(boards)) != len(boards)
        ):
            raise ValueError("V20 entry symbol boards are invalid")
        if item["best_board"] not in boards:
            raise ValueError("V20 entry symbol best_board is invalid")
        if any(board not in board_gains for board in boards):
            raise ValueError("V20 entry symbol board lacks frozen average gain")
        if not isinstance(item["is_driver"], bool):
            raise ValueError("V20 entry symbol is_driver is invalid")
        for field in ("score", "snapshot_price", "cci", "volume_937"):
            if not _finite_number(item[field]):
                raise ValueError(f"V20 entry symbol {field} is invalid")
        if float(item["snapshot_price"]) <= 0 or float(item["volume_937"]) <= 0:
            raise ValueError("V20 entry symbol price/volume must be positive")
        history_hash = item["history_hash"]
        if (
            not isinstance(history_hash, str)
            or len(history_hash) != 64
            or any(character not in "0123456789abcdef" for character in history_hash)
        ):
            raise ValueError("V20 entry symbol history_hash is invalid")


def _validate_formatter_semantic(record: OutboxRecord, semantic: Mapping[str, Any]) -> None:
    expected_schema = {
        "ENTRY_DECISION": V20_ENTRY_SEMANTIC_SCHEMA,
        "EXIT_SIGNAL": V20_EXIT_SEMANTIC_SCHEMA,
        "EXIT_REMINDER": V20_EXIT_SEMANTIC_SCHEMA,
        "DATA_ALERT": V20_DATA_ALERT_SEMANTIC_SCHEMA,
    }.get(record.event_type)
    if expected_schema is None:
        raise ValueError(f"unsupported V20 outbox event_type: {record.event_type}")
    if semantic.get("schema_version") != expected_schema:
        raise ValueError(
            f"unsupported {record.event_type} semantic schema_version; "
            "legacy semantics cannot be upgraded during delivery"
        )
    if semantic.get("feishu_formatter_profile") != V20_FEISHU_FORMATTER_PROFILE:
        raise ValueError("unsupported V20 Feishu formatter profile")
    if record.event_type == "ENTRY_DECISION":
        _validate_entry_formatter_semantic(record, semantic)
    elif record.event_type in {"EXIT_SIGNAL", "EXIT_REMINDER"}:
        _require_fields(
            semantic,
            {
                "deployment_mode",
                "exit_signal_type",
                "code",
                "stock_name",
                "model_leg_id",
                "signal_date",
                "rank",
                "origin_final_relative_weight",
                "rule_actionable_from",
                "reason_codes",
            },
            subject="V20 exit semantic",
        )
        if record.event_type == "EXIT_SIGNAL":
            if semantic.get("event_id") != record.event_id:
                raise ValueError("V20 exit semantic event_id does not match outbox event")
            if semantic.get("event_type") != "EXIT_SIGNAL":
                raise ValueError("V20 exit semantic event_type is invalid")
        elif semantic.get("event_type") != "EXIT_REMINDER" or not semantic.get(
            "original_exit_event_id"
        ):
            raise ValueError("V20 exit reminder semantic binding is invalid")
    else:
        message = semantic.get("message", semantic.get("reason"))
        if not isinstance(message, str) or not message:
            raise ValueError("V20 DATA_ALERT semantic requires a message")
        if semantic.get("alert_code") == "MANUAL_TRIGGER_RECEIPT":
            _require_fields(
                semantic,
                {
                    "event_id",
                    "manual_request_id",
                    "cycle_result",
                    "formal_decision_available",
                    "official_state_changed",
                    "non_actionable",
                    "delivery_priority_class",
                },
                subject="V20 manual trigger receipt",
            )
            if semantic["event_id"] != record.event_id:
                raise ValueError("V20 manual trigger event_id does not match outbox event")
            if semantic["non_actionable"] is not True:
                raise ValueError("V20 manual trigger receipt must be non-actionable")
            if semantic["delivery_priority_class"] != "OPERATOR_NOTIFICATION":
                raise ValueError("V20 manual trigger receipt has invalid delivery priority")
            if (
                not isinstance(semantic["manual_request_id"], str)
                or not semantic["manual_request_id"]
            ):
                raise ValueError("V20 manual trigger receipt requires a request id")
            if not isinstance(semantic["formal_decision_available"], bool) or not isinstance(
                semantic["official_state_changed"], bool
            ):
                raise ValueError("V20 manual trigger receipt has invalid decision flags")


def seal_v20_payload(
    record: OutboxRecord,
    generated_at: datetime,
    commit_marker: int,
    on_time: bool,
) -> Mapping[str, Any]:
    semantic = dict(record.semantic)
    _validate_formatter_semantic(record, semantic)
    title_prefix = (
        "[V20][SHADOW]"
        if str(semantic.get("deployment_mode", "forward_shadow")) == "forward_shadow"
        else "[V20]"
    )
    if record.event_type == "ENTRY_DECISION":
        message = render_entry_message(
            semantic,
            generated_at=generated_at,
            commit_marker=commit_marker,
            on_time=on_time,
        )
    elif record.event_type == "EXIT_SIGNAL":
        actionable_from = _public_actionable_from(semantic, generated_at)
        message = render_exit_message(
            semantic,
            generated_at=generated_at,
            commit_marker=commit_marker,
        )
    elif record.event_type == "EXIT_REMINDER":
        exit_signal = semantic.get("exit_signal_type", "-")
        exit_label = _EXIT_LABELS.get(str(exit_signal), exit_signal)
        shadow_notice = (
            "⚪ 前向观察：不替代当前正式策略建议\n"
            if str(semantic.get("deployment_mode", "forward_shadow")) == "forward_shadow"
            else ""
        )
        message = (
            f"{title_prefix} 退出提醒（尚未确认停止提醒）\n"
            f"{shadow_notice}"
            f"股票: {semantic.get('code', '-')} {semantic.get('stock_name', '')}\n"
            f"模型腿: {str(semantic.get('model_leg_id', '-'))[:16]}\n"
            f"原退出规则: {exit_label}\n"
            "建议仍为退出该模型腿100%。若已处理，请通过V20确认接口停止后续提醒。\n"
            f"原事件: {semantic.get('original_exit_event_id', '-')}"
        )
    elif (
        record.event_type == "DATA_ALERT" and semantic.get("alert_code") == "MANUAL_TRIGGER_RECEIPT"
    ):
        message = (
            f"{title_prefix} 人工触发验证（非交易指令）\n"
            f"{semantic['message']}\n"
            f"事件: {record.event_id[:16]}"
        )
    else:
        message = (
            f"{title_prefix} {record.event_type}\n"
            f"{semantic.get('message', semantic.get('reason', ''))}\n"
            f"事件: {record.event_id[:16]}"
        )
    payload = {
        "schema_version": V20_FEISHU_PAYLOAD_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": record.event_id,
        "event_type": record.event_type,
        "route_id": record.route_id,
        "semantic_content_hash": record.semantic_content_hash,
        "generated_at": generated_at.isoformat(),
        "durable_commit_marker": commit_marker,
        "timeliness_status": ("ON_TIME" if record.action_expiry_ts is None or on_time else "LATE"),
        "message": message,
    }
    if record.event_type == "EXIT_SIGNAL":
        payload["actionable_from"] = actionable_from
    if record.event_type == "ENTRY_DECISION":
        payload["expired_delivery_message"] = render_expired_entry_delivery_message(semantic)
    return payload


@dataclass(frozen=True)
class V20RelayClient:
    """Client for the V20-only idempotent relay endpoint."""

    bot_origin: str
    app_id: str
    app_secret: str
    chat_id: str
    destination_fingerprint: str

    async def send_delivery(self, envelope: Mapping[str, Any]) -> bool:
        expected_request_fields = {
            "schema_version",
            "event_id",
            "event_type",
            "route_id",
            "idempotency_key",
            "payload_hash",
            "delivery_class",
            "action_expiry_ts",
            "message",
            "expired_delivery_message",
            "destination_fingerprint",
        }
        if set(envelope) != expected_request_fields:
            raise V20RelayContractError("V20 relay request field set mismatch")
        if envelope["schema_version"] != V20_RELAY_REQUEST_SCHEMA:
            raise V20RelayContractError("unsupported V20 relay request schema")
        delivery_class = envelope["delivery_class"]
        if delivery_class not in _DELIVERY_CLASSES:
            raise V20RelayContractError("unsupported V20 relay delivery class")
        if envelope["destination_fingerprint"] != self.destination_fingerprint:
            raise V20RelayContractError("V20 relay request destination binding mismatch")

        request_body = {
            **dict(envelope),
            "app_id": self.app_id,
            "app_secret": self.app_secret,
            "chat_id": self.chat_id,
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.bot_origin}/api/v20/send",
                json=request_body,
            )
        response.raise_for_status()
        try:
            receipt = response.json()
        except ValueError as exc:
            raise V20RelayContractError("V20 relay response is not JSON") from exc
        expected_response_fields = {
            "schema_version",
            "code",
            "event_id",
            "route_id",
            "idempotency_key",
            "payload_hash",
            "delivery_status",
            "duplicate",
            "accepted_at",
            "destination_fingerprint",
        }
        if not isinstance(receipt, Mapping) or set(receipt) != expected_response_fields:
            raise V20RelayContractError("V20 relay response field set mismatch")
        if receipt["schema_version"] != V20_RELAY_RESPONSE_SCHEMA:
            raise V20RelayContractError("unsupported V20 relay response schema")
        if type(receipt["code"]) is not int or receipt["code"] != 0:
            raise V20RelayContractError("V20 relay did not return an exact success code")
        for field in (
            "event_id",
            "route_id",
            "idempotency_key",
            "payload_hash",
            "destination_fingerprint",
        ):
            if receipt[field] != envelope[field]:
                raise V20RelayContractError(f"V20 relay {field} echo mismatch")
        if type(receipt["duplicate"]) is not bool:
            raise V20RelayContractError("V20 relay duplicate flag must be boolean")
        delivery_status = receipt["delivery_status"]
        if delivery_status not in _DELIVERY_STATUSES:
            raise V20RelayContractError("unsupported V20 relay delivery status")
        try:
            accepted_at = datetime.fromisoformat(str(receipt["accepted_at"]))
        except ValueError as exc:
            raise V20RelayContractError("V20 relay accepted_at is invalid") from exc
        if accepted_at.tzinfo is None or accepted_at.utcoffset() is None:
            raise V20RelayContractError("V20 relay accepted_at must be timezone-aware")

        expiry_value = envelope["action_expiry_ts"]
        if delivery_class == "ACTIONABLE_ENTRY":
            if not isinstance(expiry_value, str):
                raise V20RelayContractError("actionable V20 relay entry requires an expiry")
            try:
                expiry = datetime.fromisoformat(expiry_value)
            except ValueError as exc:
                raise V20RelayContractError("V20 relay action expiry is invalid") from exc
            if expiry.tzinfo is None or expiry.utcoffset() is None:
                raise V20RelayContractError("V20 relay action expiry must be timezone-aware")
            if delivery_status == "DELIVERED_ACTIONABLE" and accepted_at >= expiry:
                raise V20RelayContractError("relay claims an actionable delivery after expiry")
            if delivery_status == "DELIVERED_EXPIRED_NOTICE" and accepted_at < expiry:
                raise V20RelayContractError("relay claims an expired notice before expiry")
            if delivery_status not in {
                "DELIVERED_ACTIONABLE",
                "DELIVERED_EXPIRED_NOTICE",
            }:
                raise V20RelayContractError("actionable entry has an incompatible receipt")
        elif expiry_value is not None or delivery_status != "DELIVERED":
            raise V20RelayContractError("non-actionable delivery has an incompatible receipt")
        return True


@dataclass(frozen=True)
class V20LegacyRelayClient:
    """Adapter for the relay already used by the legacy main container.

    PostgreSQL still owns the V20 outbox lease and retry lifecycle.  This
    adapter changes only the final HTTP shape from the dedicated
    ``/api/v20/send`` contract to the deployed ``/api/send`` contract.
    """

    bot_origin: str
    app_id: str
    app_secret: str
    chat_id: str
    destination_fingerprint: str
    clock: Callable[[], datetime] = lambda: datetime.now(SHANGHAI)

    async def send_delivery(self, envelope: Mapping[str, Any]) -> bool:
        expected_request_fields = {
            "schema_version",
            "event_id",
            "event_type",
            "route_id",
            "idempotency_key",
            "payload_hash",
            "delivery_class",
            "action_expiry_ts",
            "message",
            "expired_delivery_message",
            "destination_fingerprint",
        }
        if set(envelope) != expected_request_fields:
            raise V20RelayContractError("V20 relay request field set mismatch")
        if envelope["schema_version"] != V20_RELAY_REQUEST_SCHEMA:
            raise V20RelayContractError("unsupported V20 relay request schema")
        if envelope["destination_fingerprint"] != self.destination_fingerprint:
            raise V20RelayContractError("V20 relay request destination binding mismatch")
        delivery_class = envelope["delivery_class"]
        if delivery_class not in _DELIVERY_CLASSES:
            raise V20RelayContractError("unsupported V20 relay delivery class")

        message = str(envelope["message"])
        expiry_value = envelope["action_expiry_ts"]
        if delivery_class == "ACTIONABLE_ENTRY":
            if not isinstance(expiry_value, str):
                raise V20RelayContractError("actionable V20 relay entry requires an expiry")
            try:
                expiry = datetime.fromisoformat(expiry_value)
            except ValueError as exc:
                raise V20RelayContractError("V20 relay action expiry is invalid") from exc
            if expiry.tzinfo is None or expiry.utcoffset() is None:
                raise V20RelayContractError("V20 relay action expiry must be timezone-aware")
            if self.clock().astimezone(SHANGHAI) >= expiry.astimezone(SHANGHAI):
                message = str(envelope["expired_delivery_message"] or "")
                if not message:
                    raise V20RelayContractError("expired V20 entry lacks a safe notice")
        elif expiry_value is not None:
            raise V20RelayContractError("non-actionable V20 delivery cannot have an expiry")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.bot_origin}/api/send",
                json={
                    "app_id": self.app_id,
                    "app_secret": self.app_secret,
                    "chat_id": self.chat_id,
                    "message": message,
                },
            )
        response.raise_for_status()
        try:
            receipt = response.json()
        except ValueError as exc:
            raise V20RelayContractError("legacy Feishu relay response is not JSON") from exc
        if not isinstance(receipt, Mapping) or receipt.get("code") != 0:
            raise V20RelayContractError("legacy Feishu relay did not return success")
        return True


@dataclass(frozen=True)
class V20FeishuRoute:
    route_id: str
    bot_url: str
    app_id: str
    app_secret: str
    chat_id: str
    transport: Literal["v20_relay", "legacy_send"] = "v20_relay"

    @property
    def bot_origin(self) -> str:
        return _canonical_relay_origin(self.bot_url) or ""

    @property
    def destination_fingerprint(self) -> str:
        if not self.is_configured():
            return ""
        return _destination_fingerprint(
            route_id=self.route_id,
            bot_origin=self.bot_origin,
            app_id=self.app_id.strip(),
            chat_id=self.chat_id.strip(),
        )

    def is_configured(self) -> bool:
        return bool(
            self.bot_origin
            and self.app_id.strip()
            and self.app_secret.strip()
            and self.chat_id.strip()
        )

    def relay(self) -> V20RelayClient | V20LegacyRelayClient:
        if not self.is_configured():
            raise V20RelayContractError(f"V20 route {self.route_id!r} is not configured")
        relay_type = V20LegacyRelayClient if self.transport == "legacy_send" else V20RelayClient
        return relay_type(
            bot_origin=self.bot_origin,
            app_id=self.app_id.strip(),
            app_secret=self.app_secret.strip(),
            chat_id=self.chat_id.strip(),
            destination_fingerprint=self.destination_fingerprint,
        )


def load_v20_feishu_routes() -> dict[str, V20FeishuRoute]:
    def _route(route_id: str, prefix: str) -> V20FeishuRoute:
        return V20FeishuRoute(
            route_id=route_id,
            bot_url=os.getenv(f"{prefix}_BOT_URL", ""),
            app_id=os.getenv(f"{prefix}_APP_ID", ""),
            app_secret=os.getenv(f"{prefix}_APP_SECRET", ""),
            chat_id=os.getenv(f"{prefix}_CHAT_ID", ""),
        )

    routes = {
        "V20_SHADOW_FEISHU": _route("V20_SHADOW_FEISHU", "V20_SHADOW_FEISHU"),
        "V20_FORMAL_FEISHU": _route("V20_FORMAL_FEISHU", "V20_FEISHU"),
    }
    shadow = routes["V20_SHADOW_FEISHU"]
    formal = routes["V20_FORMAL_FEISHU"]
    if shadow.chat_id and formal.chat_id and shadow.chat_id == formal.chat_id:
        raise ValueError("V20 shadow and formal routes cannot share a Feishu chat_id")
    if shadow.app_id and formal.app_id and shadow.app_id == formal.app_id:
        raise ValueError("V20 shadow and formal routes cannot share Feishu app credentials")
    if shadow.app_secret and formal.app_secret and shadow.app_secret == formal.app_secret:
        raise ValueError("V20 shadow and formal routes cannot share Feishu app credentials")
    return routes


def load_legacy_embedded_v20_route() -> V20FeishuRoute:
    """Bind embedded shadow V20 to the same relay destination as V16."""
    from src.common.config import get_feishu_config

    config = get_feishu_config()
    return V20FeishuRoute(
        route_id="V20_SHADOW_FEISHU",
        bot_url=config["bot_url"],
        app_id=config["app_id"],
        app_secret=config["app_secret"],
        chat_id=config["chat_id"],
        transport="legacy_send",
    )


class V20OutboxPublisher:
    """At-least-once publisher whose retries are controlled by PostgreSQL."""

    def __init__(
        self,
        repository: V20Repository,
        routes: Mapping[str, V20FeishuRoute],
        *,
        worker_id: str,
        route_id: str,
        official_stream_id: str,
        lineage_id: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not worker_id or not route_id or not official_stream_id or not lineage_id:
            raise ValueError("V20 publisher worker and outbox scope cannot be empty")
        if route_id not in routes:
            raise ValueError(f"V20 publisher route {route_id!r} is not registered")
        self._repository = repository
        self._routes = dict(routes)
        self._worker_id = worker_id
        self._route_id = route_id
        self._official_stream_id = official_stream_id
        self._lineage_id = lineage_id
        self._clock = clock or (lambda: datetime.now(SHANGHAI))
        self._last_cycle_error: str | None = None

    @property
    def last_cycle_error(self) -> str | None:
        """Return the delivery failure observed by the most recent lease cycle."""

        return self._last_cycle_error

    async def publish_once(self) -> int:
        self._last_cycle_error = None
        lease_started_monotonic = asyncio.get_running_loop().time()
        records = await self._repository.lease_outbox(
            worker_id=self._worker_id,
            route_id=self._route_id,
            official_stream_id=self._official_stream_id,
            lineage_id=self._lineage_id,
            limit=1,
        )
        sent = 0
        for record in records:
            if (
                record.route_id,
                record.official_stream_id,
                record.lineage_id,
            ) != (self._route_id, self._official_stream_id, self._lineage_id):
                raise V20StateConflict(
                    f"outbox lease escaped publisher scope for {record.event_id}"
                )
            route = self._routes.get(record.route_id)
            if route is None:
                self._last_cycle_error = f"unknown route_id {record.route_id}"
                await self._repository.complete_delivery(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
                    succeeded=False,
                    error=f"unknown route_id {record.route_id}",
                    retry_after_seconds=300,
                )
                continue
            if not route.is_configured():
                self._last_cycle_error = f"route {record.route_id} is not configured"
                await self._repository.complete_delivery(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
                    succeeded=False,
                    error=f"route {record.route_id} is not configured",
                    retry_after_seconds=300,
                )
                continue
            payload = record.payload or {}
            message = str(payload.get("message", ""))
            action = record.semantic.get("action")
            multiplier = record.semantic.get("final_multiplier")
            normalized_multiplier: float | None = None
            if _finite_number(multiplier):
                assert isinstance(multiplier, (int, float)) and not isinstance(multiplier, bool)
                normalized_multiplier = float(multiplier)
            actionable_entry = (
                record.event_type == "ENTRY_DECISION"
                and action == "ENTER"
                and normalized_multiplier is not None
                and normalized_multiplier > 0
            )
            terminal_no_buy = (
                record.event_type == "ENTRY_DECISION"
                and action in {"BLOCK", "NO_SIGNAL", "INPUT_INVALID"}
                and normalized_multiplier == 0.0
            )
            if record.event_type == "ENTRY_DECISION" and not (actionable_entry or terminal_no_buy):
                raise V20StateConflict(
                    f"entry outbox has ambiguous delivery class for {record.event_id}"
                )
            delivery_class = (
                "ACTIONABLE_ENTRY"
                if actionable_entry
                else "NON_ACTIONABLE_ENTRY"
                if record.event_type == "ENTRY_DECISION"
                else "NOTIFICATION"
            )
            expired_message = (
                str(payload.get("expired_delivery_message", "")) if actionable_entry else None
            )
            if not message:
                raise V20StateConflict(f"outbox payload lacks message for {record.event_id}")
            if actionable_entry and not expired_message:
                raise V20StateConflict(
                    f"actionable entry lacks expired notice for {record.event_id}"
                )
            action_expiry = record.action_expiry_ts
            if actionable_entry and action_expiry is None:
                raise V20StateConflict(
                    f"actionable entry lacks action expiry for {record.event_id}"
                )
            if not isinstance(record.payload_hash, str) or len(record.payload_hash) != 64:
                raise V20StateConflict(f"outbox payload hash is invalid for {record.event_id}")

            action_expiry_iso: str | None = None
            if actionable_entry:
                assert action_expiry is not None
                action_expiry_iso = action_expiry.isoformat()
            envelope = {
                "schema_version": V20_RELAY_REQUEST_SCHEMA,
                "event_id": record.event_id,
                "event_type": record.event_type,
                "route_id": record.route_id,
                "idempotency_key": f"{record.route_id}:{record.event_id}",
                "payload_hash": record.payload_hash,
                "delivery_class": delivery_class,
                "action_expiry_ts": action_expiry_iso,
                "message": message,
                "expired_delivery_message": expired_message,
                "destination_fingerprint": route.destination_fingerprint,
            }
            actionable_timeout: float | None = None
            if actionable_entry:
                assert action_expiry is not None
                # PostgreSQL's lease clock, not a potentially skewed app-host
                # clock, is authoritative.  Bound the in-flight HTTP request by
                # the remaining monotonic duration as well, so a slow relay
                # cannot keep an actionable send alive across the cutoff.
                leased_at = record.lease_db_ts
                remaining = (
                    (action_expiry - leased_at).total_seconds() if leased_at is not None else 0.0
                )
                remaining -= asyncio.get_running_loop().time() - lease_started_monotonic
                if remaining > _ENTRY_ACTION_SEND_GUARD_SECONDS:
                    actionable_timeout = remaining - _ENTRY_ACTION_SEND_GUARD_SECONDS
                else:
                    actionable_timeout = 0.0
                    if getattr(route, "transport", "v20_relay") == "legacy_send":
                        # The legacy relay cannot enforce V20's server-side
                        # expiry contract.  PostgreSQL's lease clock already
                        # proves this suggestion is too late, so downgrade the
                        # outgoing envelope to a plain non-actionable notice
                        # before any HTTP request is created.
                        envelope = {
                            **envelope,
                            "delivery_class": "NON_ACTIONABLE_ENTRY",
                            "action_expiry_ts": None,
                            "message": expired_message,
                            "expired_delivery_message": None,
                        }
            error: str | None
            try:
                send = route.relay().send_delivery(envelope)
                if actionable_timeout is None or actionable_timeout <= 0.0:
                    succeeded = await asyncio.wait_for(
                        send, timeout=_NON_EXPIRING_SEND_TIMEOUT_SECONDS
                    )
                else:
                    succeeded = await asyncio.wait_for(send, timeout=actionable_timeout)
            except Exception as exc:  # the durable outbox owns every retry
                logger.warning("V20 Feishu delivery failed", exc_info=True)
                succeeded = False
                error = f"{type(exc).__name__}: {exc}"
            else:
                error = None if succeeded else "Feishu relay returned failure"
            if not succeeded:
                self._last_cycle_error = error or "Feishu relay returned failure"
            await self._repository.complete_delivery(
                record.event_id,
                worker_id=self._worker_id,
                route_id=self._route_id,
                official_stream_id=self._official_stream_id,
                lineage_id=self._lineage_id,
                succeeded=succeeded,
                error=error,
                retry_after_seconds=min(300, 2 ** min(record.attempt_count, 8)),
            )
            sent += int(succeeded)
        return sent

    async def run(
        self,
        stop_event: asyncio.Event,
        *,
        before_cycle: Callable[[], Awaitable[None]] | None = None,
        on_cycle_success: Callable[[], None] | None = None,
        on_cycle_error: Callable[[str], None] | None = None,
    ) -> None:
        while not stop_event.is_set():
            # Leadership is a public-side-effect fence, not merely a decision
            # lane concern.  Keep the guard outside the delivery retry handler
            # so losing it terminates this runtime instead of looking like a
            # recoverable relay failure.
            if before_cycle is not None:
                await before_cycle()
            try:
                sent = await self.publish_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("V20 outbox publisher iteration failed")
                if on_cycle_error is not None:
                    on_cycle_error(f"{type(exc).__name__}: {exc}")
                sent = 0
            else:
                if self._last_cycle_error is not None:
                    if on_cycle_error is not None:
                        on_cycle_error(self._last_cycle_error)
                elif on_cycle_success is not None:
                    on_cycle_success()
            if sent:
                # Drain a healthy backlog immediately.  Leasing one event at a
                # time lets a newly arrived LIVE_EXIT preempt older stale exits
                # at the very next query instead of sitting behind a leased batch.
                continue
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=2.0)
            except TimeoutError:
                pass


__all__ = [
    "V20FeishuRoute",
    "V20LegacyRelayClient",
    "V20OutboxPublisher",
    "V20RelayClient",
    "V20RelayContractError",
    "V20_RELAY_REQUEST_SCHEMA",
    "V20_RELAY_RESPONSE_SCHEMA",
    "load_legacy_embedded_v20_route",
    "load_v20_feishu_routes",
    "render_entry_message",
    "render_exit_message",
    "seal_v20_payload",
]
