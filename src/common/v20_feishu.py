"""Deterministic V20 Feishu messages and durable-outbox publisher."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, time
from typing import Any, Callable, Literal, TypeGuard
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

from src.common.feishu_bot import (
    LEGACY_FEISHU_HTTP_PHASE_TIMEOUT_SECONDS,
    post_message_once,
)
from src.data.database.v20_repository import OutboxRecord, V20Repository, V20StateConflict
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_EXIT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_FEISHU_PAYLOAD_SCHEMA,
)

logger = logging.getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")
V20_RELAY_REQUEST_SCHEMA = "v20-relay-request/v1"
V20_RELAY_RESPONSE_SCHEMA = "v20-relay-response/v1"
_LEGACY_OUTWARD_CALL_DEADLINE_SECONDS = LEGACY_FEISHU_HTTP_PHASE_TIMEOUT_SECONDS + 1.0
_LEGACY_OUTBOUND_DEADLINE_SAFETY_SECONDS = 1.0
_LEGACY_ACTION_RESERVE_SECONDS = (
    _LEGACY_OUTWARD_CALL_DEADLINE_SECONDS + _LEGACY_OUTBOUND_DEADLINE_SAFETY_SECONDS
)
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


def _rolling7_line(semantic: Mapping[str, Any]) -> str:
    status = str(semantic.get("rolling7_state", "-"))
    reason = semantic.get("rolling7_reason")
    window_ids = semantic.get("rolling7_window_ids")
    if status == "WARMUP":
        mature_n: int | None = None
        if isinstance(window_ids, list):
            mature_n = len(window_ids)
        if mature_n is None and isinstance(reason, str):
            matched = re.fullmatch(r"WARMUP:(\d+)/7", reason)
            if matched is not None:
                mature_n = int(matched.group(1))
        count = "-" if mature_n is None else str(mature_n)
        return f"滚动7: WARMUP | 成熟批次={count}/7 | 尚未形成完整7批窗口"
    if status == "DATA_GAP":
        gap_ids: tuple[str, ...] = ()
        if isinstance(reason, str) and reason.startswith("DATA_GAP:"):
            gap_ids = tuple(item for item in reason.removeprefix("DATA_GAP:").split(",") if item)
        detail = f"{len(gap_ids)}批待补齐（{', '.join(gap_ids)}）" if gap_ids else "待补齐"
        return f"滚动7: DATA_GAP | 数据缺口={detail}"
    if status == "UNKNOWN":
        detail = str(reason) if reason else "信息时钟无效"
        return f"滚动7: UNKNOWN | 无法评估={detail}"
    return (
        f"滚动7: {status} | R7={_pct(semantic.get('rolling7_r7'))} | "
        f"亏损批次={semantic.get('rolling7_l7', '-')}/7"
    )


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


def _operator_entry_decision(action: object, multiplier: object = None) -> str:
    """Translate a machine entry result into one unambiguous operator sentence."""

    action_text = str(action)
    if action_text == "ENTER":
        if (
            isinstance(multiplier, bool)
            or not isinstance(multiplier, (int, float))
            or not math.isfinite(multiplier)
        ):
            return "开仓（仓位见每日决策消息）"
        value = float(multiplier)
        labels = {
            1.0: "正常开仓（策略倍率100%）",
            0.5: "减半开仓（策略倍率50%）",
            0.25: "谨慎开仓（策略倍率25%）",
        }
        return labels.get(value, f"按策略倍率{value:.0%}开仓")
    if action_text == "BLOCK":
        return "不开仓（风控拦截）"
    if action_text == "NO_SIGNAL":
        return "不开仓（没有合格候选票）"
    if action_text == "INPUT_INVALID":
        return "未形成按时有效的入场决策"
    return "未形成可用结论"


def _render_late_replay_for_operator(
    semantic: Mapping[str, Any],
    *,
    title_prefix: str,
    event_id: str,
) -> str:
    """Render a late replay without mixing retrospective and current actions."""

    replay_decision = _operator_entry_decision(
        semantic.get("replay_action"), semantic.get("final_multiplier")
    )
    trade_date = str(semantic.get("event_trade_date", "-"))
    computed_at = datetime.fromisoformat(str(semantic["computed_at"])).astimezone(SHANGHAI)
    replay_time = computed_at.strftime("%H:%M")
    lines = [
        f"{title_prefix} 现在不开仓｜09:39复盘已过期",
        "",
        "🔴 现在操作：不开仓，不补买，不追买",
        f"🕘 当时本应：{replay_decision}；结果已过期",
    ]

    symbols = semantic.get("symbols") or []
    if isinstance(symbols, list) and symbols:
        ticket_text = "、".join(
            f"{item.get('code', '-')} {item.get('name', '')}".rstrip()
            for item in symbols
            if isinstance(item, Mapping)
        )
        lines.append(f"当时票单：{ticket_text}")
    else:
        lines.append("当时票单：无")

    official_decision = _operator_entry_decision(semantic.get("official_entry_action"))
    lines.extend(
        [
            f"早盘正式记录：{official_decision}。",
            "说明：数据在截止后取得并还原09:39截面，不代表早上已生成或送达。",
            "已有持仓不因本消息处理，继续按既定卖出规则管理。",
            "",
            f"交易日：{trade_date}｜复盘计算：{replay_time}｜事件：{event_id[:16]}",
        ]
    )
    return "\n".join(lines)


def _render_manual_receipt_for_operator(
    semantic: Mapping[str, Any],
    *,
    title_prefix: str,
    event_id: str,
    generated_at: datetime,
) -> str:
    """Keep deployment verification visibly separate from trading instructions."""

    cycle = str(semantic.get("cycle_result", "-"))
    cycle_text = {
        "ALREADY_TERMINAL": "已读取今天已经冻结的结果",
        "DECISION_COMMITTED": "已生成并冻结今天的正式结果",
        "LATE_0939_REPLAY_READY": "已读取正式结果，并完成09:39迟到复盘",
        "NON_TRADING_DAY": "今天不是交易日",
        "BEFORE_WINDOW": "尚未到运行窗口",
        "COLLECTING": "正在采集决策数据",
        "DECISION_PENDING": "正式决策尚未冻结",
        "CUTOFF_WITHOUT_DURABLE_DECISION": "已过截止时间，但正式决策缺失",
    }.get(cycle, cycle)
    trade_date = str(semantic.get("event_trade_date", "-"))
    generated_local = generated_at.astimezone(SHANGHAI)
    receipt_time = generated_local.strftime("%H:%M")
    after_entry_cutoff = generated_local.timetz().replace(tzinfo=None) >= time(9, 40)
    if after_entry_cutoff:
        current_action = "🔴 现在操作：不开仓，不补买，不追买"
    elif semantic.get("formal_decision_available"):
        current_action = "现在操作：以另发的“每日决策”为准；不要根据本回执下单"
    else:
        current_action = "现在操作：暂不开仓，等待“每日决策”"
    lines = [
        f"{title_prefix} 人工触发回执｜非交易指令",
        "",
        current_action,
        f"验收结果：{cycle_text}。",
    ]

    if semantic.get("formal_decision_available"):
        if semantic.get("entry_action") == "ENTER":
            if after_entry_cutoff:
                lines.append("早盘正式记录：曾给出开仓建议，现已过期（详见每日决策消息）。")
            else:
                lines.append("早盘正式结果已冻结；倍率和票单以另发的“每日决策”为准。")
        else:
            lines.append(
                "早盘正式记录：" + _operator_entry_decision(semantic.get("entry_action")) + "。"
            )
    else:
        lines.append("早盘正式记录：不可用。")

    if semantic.get("late_0939_replay_available"):
        replay_decision = _operator_entry_decision(
            semantic.get("late_0939_replay_action"),
            semantic.get("late_0939_replay_multiplier"),
        )
        lines.append(f"09:39还原：当时本应：{replay_decision}；结果已过期，现在不要补买。")
    elif semantic.get("late_0939_replay_error"):
        lines.append(f"09:39还原：未完成（{semantic['late_0939_replay_error']}）。")

    lines.extend(
        [
            "",
            (
                f"交易日：{trade_date}｜回执生成：{receipt_time}｜"
                f"请求：{semantic.get('manual_request_id', '-')}｜事件：{event_id[:16]}"
            ),
        ]
    )
    return "\n".join(lines)


def _render_manual_0939_chain_probe_for_operator(
    semantic: Mapping[str, Any],
    *,
    title_prefix: str,
    event_id: str,
) -> str:
    """Render an exact-current-code chain probe without presenting it as a trade signal."""

    passed = semantic.get("probe_result") == "PASS"
    result_mark = "✅ 通过" if passed else "❌ 失败"
    trade_date = str(semantic.get("event_trade_date", "-"))
    computed_at = datetime.fromisoformat(str(semantic["computed_at"])).astimezone(SHANGHAI)
    v16_count = int(semantic.get("v16_count", 0))
    raw_fact_n = int(semantic.get("raw_fact_n", 0))
    coverage = semantic.get("quote_coverage")
    if _finite_number(coverage):
        assert coverage is not None
        coverage_text = _pct(float(coverage), 1)
    elif semantic.get("quote_coverage_note") == "NOT_EXPOSED_BY_EXISTING_REPLAY_HELPER":
        coverage_text = "已通过生产≥80%门槛（精确比例未冻结）"
    else:
        coverage_text = "未完成"
    lines = [
        f"{title_prefix} 当前版本早盘链路重算｜{result_mark}",
        "",
    ]
    if passed:
        lines.extend(
            [
                "✅ 验收结论：当前部署版本已完成一次全链路重新计算。",
                "收到本条，说明“已落库原始数据 → V16 → V20 → 结果持久化 → 飞书投递”已经跑通。",
            ]
        )
    else:
        lines.append("❌ 验收结论：当前部署版本未能完成全链路重新计算。")
        failure_stage = semantic.get("failure_stage")
        failure_reason = semantic.get("failure_reason") or semantic.get("message")
        if isinstance(failure_stage, str) and failure_stage:
            stage_text = {
                "PERSISTED_FACT_LOAD": "读取已落库原始数据",
                "V16_SCAN": "V16选股",
                "V20_DECISION": "V20决策",
                "PERSIST_RESULT": "结果持久化",
                "FEISHU_SEAL": "飞书消息生成",
            }.get(failure_stage)
            lines.append(f"失败阶段：{failure_stage}{f'（{stage_text}）' if stage_text else ''}")
        if isinstance(failure_reason, str) and failure_reason:
            lines.append(f"失败原因：{failure_reason}")

    computation_scope = (
        "计算口径：当前部署代码重新读取持久化的09:31–09:39原始事实并完整重算；"
        "不是昨天的冻结结果，未复用旧回放。"
        if passed
        else "验收口径：只运行当前部署代码；链路未完成，未复用旧回放或旧决策兜底。"
    )
    lines.extend(
        [
            computation_scope,
            "",
            (
                f"V16选股：{v16_count}只"
                if passed
                else f"V16数量：{v16_count}只（链路失败，不能视为合法无票）"
            ),
        ]
    )
    if passed:
        lines.append(
            "V20重算结论："
            + _operator_entry_decision(semantic.get("v20_action"), semantic.get("final_multiplier"))
        )
    else:
        lines.append("V20重算结论：未形成可用结论")

    symbols = semantic.get("symbols") or []
    if isinstance(symbols, list) and symbols:
        ticket_text = "、".join(
            f"{item.get('code', '-')} {item.get('name', '')}".rstrip()
            for item in symbols
            if isinstance(item, Mapping)
        )
        lines.append(f"重算票单：{ticket_text}")
    else:
        lines.append("重算票单：无")

    lines.extend(
        [
            (
                f"数据窗口：{semantic.get('data_window_start', '-')}–"
                f"{semantic.get('data_window_end', '-')}｜原始事实：{raw_fact_n}条｜"
                f"行情覆盖：{coverage_text}"
            ),
            "",
            "安全边界：本次验收未修改正式决策、正式策略状态、订单、持仓或卖出信号。",
            "明早执行口径：只认09:40前送达的“V20每日决策”；"
            "本验收消息不能用于下单，迟到重算也不能追买。",
            "",
            (
                f"交易日：{trade_date}｜重算完成：{computed_at.strftime('%H:%M:%S')}｜"
                f"请求：{semantic.get('manual_request_id', '-')}｜事件：{event_id[:16]}"
            ),
        ]
    )
    return "\n".join(lines)


_MANUAL_CHECK_CURRENT_ACTION = "当前操作：不生成新的入场指令，不补买，不追买"
_MANUAL_CHECK_CURRENT_REASON = "原因：手工触发时间已过当日入场时点。"
_FROZEN_REPLAY_SOURCE_BEGIN = "----- 早盘封存原文开始（逐字节未改动，仅供核查） -----"
_FROZEN_REPLAY_SOURCE_END = "----- 早盘封存原文结束 -----"


def _render_entry_strategy_body(semantic: Mapping[str, Any]) -> str:
    """Render the one canonical strategy-output body for every entry surface."""

    action = str(semantic.get("action", "INPUT_INVALID"))
    multiplier = float(semantic.get("final_multiplier", 0.0))
    lines = [
        f"计算结论：{action}｜最终倍率 {_pct(multiplier, 0)}",
        (
            f"BASE: {semantic.get('health_state', '-')} / "
            f"基础倍率 {_pct(semantic.get('base_multiplier'), 0)}"
        ),
        _rolling7_line(semantic),
        (
            f"极端门G: {semantic.get('g_state', 'NOT_EVALUATED')} | "
            f"防御倍率 {_pct(semantic.get('defense_multiplier'), 0)} | "
            f"最终 {_pct(multiplier, 0)}"
        ),
    ]
    reasons = semantic.get("reason_codes") or []
    if reasons:
        lines.append("原因: " + " / ".join(str(item) for item in reasons))

    funnel = semantic.get("v16_funnel") or {}
    if isinstance(funnel, Mapping) and funnel:
        lines.append(
            "V16扫描: "
            f"股票池 {funnel.get('step0_universe_count', '-')}只 | "
            f"热门板块 {funnel.get('step2_hot_board_count', '-')}个 | "
            f"最终 {funnel.get('final_candidates', '-')}只"
        )

    symbols = semantic.get("symbols") or []
    if isinstance(symbols, list) and symbols:
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
        if multiplier > 0:
            per_leg = multiplier / len(symbols)
            lines.append(f"每只模型腿相对份额: {_pct(per_leg)}（不代表账户金额或股数）")
            lines.append("参考价规则: 使用原始09:41结束标签的bar.open锁定09:40参考价")
    else:
        lines.append("今日V16完整扫描合法无票，不建立新模型批次")

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
    return "\n".join(lines)


def _render_manual_entry_check_for_operator(
    semantic: Mapping[str, Any],
    *,
    title_prefix: str,
    event_id: str,
) -> str:
    """Render a fresh post-cutoff recomputation as check-only, never as a decision.

    The wrapper changes actionability only.  The embedded strategy body is
    rendered by the same helper used for the automatic morning message.
    """

    entry = semantic["entry_render_semantic"]
    assert isinstance(entry, Mapping)
    trade_date = str(semantic.get("event_trade_date", "-"))
    computed_at = datetime.fromisoformat(str(semantic["computed_at"])).astimezone(SHANGHAI)
    lines = [
        f"{title_prefix} 手工触发结果｜仅核查",
        "",
        _MANUAL_CHECK_CURRENT_ACTION,
        _MANUAL_CHECK_CURRENT_REASON,
    ]
    if semantic.get("calculation_result") == "SUCCESS":
        comparison = semantic.get("official_comparison_result")
        lines.extend(["", "本次计算：成功"])
        if comparison == "MATCH":
            lines.append("与早盘正式结果对比：一致")
        elif comparison == "DIFFERENT":
            mismatch_fields = semantic.get("official_mismatch_fields") or []
            lines.append("与早盘正式结果对比：不一致（仅为对比结果，不代表本次计算失败）")
            lines.append("差异字段：" + " / ".join(str(item) for item in mismatch_fields))
        else:
            lines.append("与早盘正式结果对比：无可比较的正式结果")
    elif semantic.get("probe_result") == "FAIL":
        mismatch_fields = semantic.get("probe_mismatch_fields") or []
        lines.extend(
            [
                "",
                "核查结论：FAIL（重算结果与冻结证据不一致）",
                "差异字段：" + " / ".join(str(item) for item in mismatch_fields),
            ]
        )
    else:
        lines.extend(["", "核查结论：PASS"])
    lines.extend(
        [
            "",
            "策略计算结果（当前V20代码按正式分钟/D1边界重算，并核验重取的历史输入）：",
            _render_entry_strategy_body(entry),
        ]
    )
    lines.extend(
        [
            "",
            "边界：本消息不创建模型批次、模型腿、持仓或订单；正式策略状态与早盘正式消息不变。",
            (
                f"交易日：{trade_date}｜计算完成：{computed_at.strftime('%H:%M:%S')}｜"
                f"请求：{semantic.get('manual_request_id', '-')}｜事件：{event_id[:16]}"
            ),
        ]
    )
    return "\n".join(lines)


def _render_frozen_entry_check_for_operator(
    semantic: Mapping[str, Any],
    *,
    title_prefix: str,
    event_id: str,
) -> str:
    """Wrap the sealed official bytes in an unmissable check-only banner.

    The verbatim source stays extractable byte-for-byte between the explicit
    sealed-source markers; the outer banner and footer keep the manual
    identity so the replay can never be mistaken for a new instruction.
    """

    source_message = str(semantic["message"])
    trade_date = str(semantic.get("event_trade_date", "-"))
    source_event = str(semantic.get("source_entry_event_id", "-"))
    return "\n".join(
        [
            f"{title_prefix} 手工触发结果｜仅核查",
            "",
            _MANUAL_CHECK_CURRENT_ACTION,
            _MANUAL_CHECK_CURRENT_REASON,
            "",
            "以下为早盘封存原文（已密封正式消息的逐字节副本，仅供核查，不是新指令）：",
            _FROZEN_REPLAY_SOURCE_BEGIN,
            source_message,
            _FROZEN_REPLAY_SOURCE_END,
            "",
            "说明：封存原文只在当日09:40截止前有效；现在不能据此下单、补买或追买。",
            f"交易日：{trade_date}｜来源事件：{source_event[:16]}｜事件：{event_id[:16]}",
        ]
    )


def _render_data_alert_for_operator(
    semantic: Mapping[str, Any],
    *,
    title_prefix: str,
    event_id: str,
    generated_at: datetime,
) -> str:
    """Give runtime alerts a human title and an explicit operator impact."""

    code = str(semantic.get("alert_code", "UNKNOWN"))
    if code == "MANUAL_MONITOR_ARMED":
        symbols = semantic.get("symbols") or []
        tickets = "、".join(
            f"{item.get('code', '-')} {item.get('name', '')}".rstrip()
            for item in symbols
            if isinstance(item, Mapping)
        )
        return "\n".join(
            [
                f"{title_prefix} 人工补挂卖出监控已启用",
                "",
                f"🟢 已启用：{semantic.get('armed_leg_count', 0)} 只模型腿",
                f"票单：{tickets or '无'}",
                ("参考价：D0 原始 09:41 bar.open；在 D1 09:30 按固定截止时间统一仲裁锁定"),
                "D1 保护：任一有效分钟 bar.close ≤ 参考价 92%，触发整腿卖出提醒",
                (
                    "D2 保护：任一有效分钟 bar.close ≤ 参考价 88%；"
                    "合格 MEWS=DANGER 时提高到 95%；14:57 无条件提醒退出"
                ),
                "",
                "边界：只新增卖出监控腿；未修改正式入场决定，也未创建订单、持仓或成交。",
                (
                    f"D0={semantic.get('signal_date', '-')} | "
                    f"D1={semantic.get('d1', '-')} | D2={semantic.get('d2', '-')}"
                ),
                f"来源事件：{str(semantic.get('source_event_id', '-'))[:16]}",
                f"确认事件：{event_id[:16]}",
            ]
        )
    no_buy_reasons = {
        "ENTRY_CALENDAR_UNKNOWN_NO_BUY": "09:40仍无法确认交易日历，系统不能安全运行入场策略。",
        "ENTRY_CUTOFF_NO_BUY": "09:40截止前没有冻结出可执行的入场决策。",
        "ENTRY_INPUT_UNAVAILABLE_BY_0940": "09:40截止前没有形成完整、可靠的V16入场结果。",
        "SLOT_FINALIZED_FAILED": "系统直到09:45仍未完成并冻结早盘入场决策。",
    }
    trade_date = str(semantic.get("event_trade_date", "-"))
    alert_time = generated_at.astimezone(SHANGHAI).strftime("%H:%M")
    if code in no_buy_reasons:
        return "\n".join(
            [
                f"{title_prefix} 入场报警｜不开仓",
                "",
                "🔴 现在操作：不开仓，不补买，不追买",
                f"原因：{no_buy_reasons[code]}",
                "已有持仓不因本报警处理，继续按既定卖出规则管理。",
                "若稍后收到09:39复盘，它也只用于核查，不能据此追买。",
                "",
                f"交易日：{trade_date}｜消息生成：{alert_time}｜事件：{event_id[:16]}",
            ]
        )

    detail = str(semantic.get("message", semantic.get("reason", "系统运行异常")))
    return "\n".join(
        [
            f"{title_prefix} 系统报警｜需要检查",
            f"影响：{detail}",
            f"报警类别：{code}",
            f"交易日：{trade_date}｜消息生成：{alert_time}｜事件：{event_id[:16]}",
        ]
    )


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
    if action == "INPUT_INVALID":
        reason_codes = {str(item) for item in (semantic.get("reason_codes") or [])}
        if "SLOT_FINALIZED_FAILED" in reason_codes:
            reason = "系统直到09:45仍未完成并冻结早盘入场决策。"
        elif "ENTRY_INPUT_UNAVAILABLE_BY_0940" in reason_codes:
            reason = "09:40截止前没有形成完整、可靠的V16入场结果。"
        else:
            reason = "关键入场数据不完整，系统无法安全给出买入结论。"
        lines = [
            f"{title}｜不开仓 ({trade_date} 09:40)",
            "",
            "🔴 现在操作：不开仓，不补买，不追买",
            f"原因：{reason}",
            "若稍后收到09:39复盘，它也只用于核查，不能据此追买。",
        ]
        if shadow:
            lines.append("⚪ 当前为前向观察，不替代V16正式建议。")
        scheduled_exits = semantic.get("scheduled_exits_today") or []
        if scheduled_exits:
            lines.extend(["", "已有模型腿的卖出计划不受影响："])
            for item in scheduled_exits:
                lines.append(
                    f"- {item['code']} {item.get('stock_name', '')}："
                    f"最迟{item.get('plan_time', '14:57')}整腿退出，保护线命中会提前通知"
                )
        lines.extend(
            [
                "",
                f"生成时间：{generated_at.astimezone(SHANGHAI).strftime('%H:%M:%S')}｜"
                f"事件：{str(semantic.get('event_id', '-'))[:16]}",
            ]
        )
        return "\n".join(lines)

    lines = [f"{title} ({trade_date} 09:40)"]
    if shadow:
        lines.append("⚪ 前向观察：不替代当前正式策略建议")
    if action != "NO_SIGNAL":
        lines.append(_entry_action_text(multiplier, on_time=on_time))
    lines.extend(["", _render_entry_strategy_body(semantic)])

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
    if semantic.get("origin_kind") == "MANUAL_MONITOR":
        lines.append("来源：人工补挂的冻结票单监控腿（只发卖出提醒，不代表系统已下单或持仓）")
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


def _finite_number(value: object) -> TypeGuard[int | float]:
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
        "early_source_hash",
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
        for field in ("score", "snapshot_price"):
            if not _finite_number(item[field]):
                raise ValueError(f"V20 entry symbol {field} is invalid")
        if float(item["snapshot_price"]) <= 0:
            raise ValueError("V20 entry symbol snapshot_price must be positive")
        for field in ("cci", "volume_937"):
            value = item[field]
            if value is not None and not _finite_number(value):
                raise ValueError(f"V20 entry symbol {field} is invalid")
        if item["volume_937"] is not None and float(item["volume_937"]) <= 0:
            raise ValueError("V20 entry symbol volume_937 must be positive")
        history_hash = item["history_hash"]
        if (
            not isinstance(history_hash, str)
            or len(history_hash) != 64
            or any(character not in "0123456789abcdef" for character in history_hash)
        ):
            raise ValueError("V20 entry symbol history_hash is invalid")
        early_source_hash = item["early_source_hash"]
        if (
            not isinstance(early_source_hash, str)
            or len(early_source_hash) != 64
            or any(character not in "0123456789abcdef" for character in early_source_hash)
        ):
            raise ValueError("V20 entry symbol early_source_hash is invalid")


def _validate_manual_0939_chain_probe(
    record: OutboxRecord,
    semantic: Mapping[str, Any],
) -> None:
    """Fail closed if a stale replay could be presented as a current-code chain probe."""

    _require_fields(
        semantic,
        {
            "event_id",
            "manual_request_id",
            "event_trade_date",
            "probe_profile",
            "probe_result",
            "current_version_recomputed",
            "replay_reused",
            "data_source",
            "data_window_start",
            "data_window_end",
            "v16_count",
            "v20_action",
            "final_multiplier",
            "symbols",
            "raw_fact_n",
            "quote_coverage",
            "computed_at",
            "config_hash",
            "state_semantics_hash",
            "official_entry_action",
            "official_entry_event_id",
            "official_state_changed",
            "orders_changed",
            "non_actionable",
            "retrospective_expired",
            "visible_message_mode",
            "delivery_priority_class",
        },
        subject="V20 manual 09:39 chain probe",
    )
    if semantic["event_id"] != record.event_id:
        raise ValueError("V20 chain probe event_id does not match outbox event")
    if not isinstance(semantic["manual_request_id"], str) or not semantic["manual_request_id"]:
        raise ValueError("V20 chain probe requires a manual request id")
    if semantic["probe_profile"] != "CURRENT_DEPLOYED_CODE_EXACT_0939_ENTRY_RENDER_V2":
        raise ValueError("V20 chain probe has an unsupported computation profile")
    if (
        semantic["replay_reused"] is not False
        or semantic["data_source"] != "PERSISTED_CANONICAL_EARLY_THROUGH_09:39"
        or semantic["data_window_start"] != "00:00"
        or semantic["data_window_end"] != "09:39"
    ):
        raise ValueError("V20 chain probe must recompute from the persisted canonical early window")
    if (
        semantic["official_state_changed"] is not False
        or semantic["orders_changed"] is not False
        or semantic["non_actionable"] is not True
        or semantic["retrospective_expired"] is not True
        or semantic["delivery_priority_class"] != "OPERATOR_NOTIFICATION"
    ):
        raise ValueError("V20 chain probe must remain non-actionable and state-preserving")
    result = semantic["probe_result"]
    if result not in {"PASS", "FAIL"}:
        raise ValueError("V20 chain probe result must be PASS or FAIL")
    if not isinstance(semantic["current_version_recomputed"], bool):
        raise ValueError("V20 chain probe recomputation flag must be boolean")
    comparison_fields = {
        "calculation_result",
        "official_comparison_result",
        "official_mismatch_fields",
    }
    if comparison_fields.intersection(semantic):
        _require_fields(
            semantic,
            comparison_fields,
            subject="V20 manual 09:39 chain probe comparison",
        )
        if semantic["calculation_result"] != "SUCCESS":
            raise ValueError("completed V20 chain probe calculation must be SUCCESS")
        comparison = semantic["official_comparison_result"]
        if comparison not in {"MATCH", "DIFFERENT", "NOT_AVAILABLE"}:
            raise ValueError("V20 chain probe official comparison result is invalid")
        mismatch_fields = semantic["official_mismatch_fields"]
        if not isinstance(mismatch_fields, list) or any(
            not isinstance(item, str) or not item for item in mismatch_fields
        ):
            raise ValueError("V20 chain probe official mismatch fields are invalid")
        if (comparison == "DIFFERENT") != bool(mismatch_fields):
            raise ValueError("V20 chain probe official comparison fields are inconsistent")
        if result != "PASS":
            raise ValueError("a successful V20 calculation must retain legacy PASS semantics")
        if semantic.get("probe_mismatch_fields") != []:
            raise ValueError(
                "a successful V20 calculation cannot expose official differences "
                "as probe mismatches"
            )
        if "failure_stage" in semantic or "failure_reason" in semantic:
            raise ValueError("an official comparison difference is not a calculation failure")

    for field in ("v16_count", "raw_fact_n"):
        value = semantic[field]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"V20 chain probe {field} is invalid")
    symbols = semantic["symbols"]
    if not isinstance(symbols, list) or any(
        not isinstance(item, Mapping)
        or not isinstance(item.get("code"), str)
        or not item.get("code")
        for item in symbols
    ):
        raise ValueError("V20 chain probe symbols are invalid")
    if semantic["v16_count"] != len(symbols):
        raise ValueError("V20 chain probe V16 count does not match its frozen symbols")

    for field in ("config_hash", "state_semantics_hash"):
        value = semantic[field]
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError(f"V20 chain probe {field} is invalid")
    try:
        trade_date = date.fromisoformat(str(semantic["event_trade_date"]))
        computed_at = datetime.fromisoformat(str(semantic["computed_at"]))
    except ValueError as exc:
        raise ValueError("V20 chain probe has invalid date/time fields") from exc
    earliest_probe_time = datetime.combine(trade_date, time(9, 40), tzinfo=SHANGHAI)
    if (
        computed_at.tzinfo is None
        or computed_at.utcoffset() is None
        or computed_at.astimezone(SHANGHAI) < earliest_probe_time
    ):
        raise ValueError("V20 chain probe computed_at precedes the frozen 09:39 window")

    recomputation_completed = semantic["current_version_recomputed"] is True
    if result == "PASS" or recomputation_completed:
        if not recomputation_completed:
            raise ValueError("a passing V20 chain probe must be recomputed by the current version")
        action = semantic["v20_action"]
        multiplier = semantic["final_multiplier"]
        coverage = semantic["quote_coverage"]
        if action not in {"ENTER", "BLOCK", "NO_SIGNAL"}:
            raise ValueError("passing V20 chain probe action is invalid")
        if not _finite_number(multiplier) or not 0 <= float(multiplier) <= 1:
            raise ValueError("passing V20 chain probe multiplier is invalid")
        if (action == "ENTER") != (float(multiplier) > 0):
            raise ValueError("passing V20 chain probe action/multiplier are inconsistent")
        if action == "ENTER" and not symbols:
            raise ValueError("passing ENTER chain probe requires symbols")
        if coverage is None:
            if semantic.get("quote_coverage_note") != ("NOT_EXPOSED_BY_EXISTING_REPLAY_HELPER"):
                raise ValueError("passing V20 chain probe lacks honest coverage disclosure")
        elif not _finite_number(coverage) or not 0 <= float(coverage) <= 1:
            raise ValueError("passing V20 chain probe quote coverage is invalid")
        if semantic["raw_fact_n"] <= 0:
            raise ValueError("passing V20 chain probe requires persisted raw facts")
        if semantic["visible_message_mode"] != "MANUAL_OPERATOR_RENDER":
            raise ValueError("passing V20 chain probe must use the manual check-only renderer")
        entry_semantic = semantic.get("entry_render_semantic")
        if not isinstance(entry_semantic, Mapping):
            raise ValueError("passing V20 chain probe lacks entry formatter evidence")
        entry_record = replace(
            record,
            event_id=str(entry_semantic.get("event_id", "")),
            event_type="ENTRY_DECISION",
            semantic=dict(entry_semantic),
        )
        _validate_entry_formatter_semantic(
            entry_record,
            entry_semantic,
        )
        if (
            entry_semantic.get("action") != action
            or entry_semantic.get("final_multiplier") != multiplier
            or entry_semantic.get("symbols") != symbols
        ):
            raise ValueError("V20 chain probe entry formatter evidence differs from its result")
        if result == "FAIL":
            mismatch_fields = semantic.get("probe_mismatch_fields")
            if (
                not isinstance(mismatch_fields, list)
                or not mismatch_fields
                or any(not isinstance(item, str) or not item for item in mismatch_fields)
            ):
                raise ValueError("failed comparison probe requires mismatch fields")
            if semantic.get("failure_stage") != "OFFICIAL_RESULT_COMPARISON":
                raise ValueError("failed comparison probe has an invalid failure stage")
            if not isinstance(semantic.get("failure_reason"), str) or not semantic.get(
                "failure_reason"
            ):
                raise ValueError("failed comparison probe requires a failure reason")
    else:
        if semantic["v20_action"] is not None or semantic["final_multiplier"] is not None:
            raise ValueError("a failed V20 chain probe cannot reuse an old decision result")
        for field in ("failure_stage", "failure_reason"):
            value = semantic.get(field)
            if not isinstance(value, str) or not value:
                raise ValueError(f"failed V20 chain probe requires {field}")
        coverage = semantic["quote_coverage"]
        if coverage is not None and (not _finite_number(coverage) or not 0 <= float(coverage) <= 1):
            raise ValueError("failed V20 chain probe quote coverage is invalid")
        if semantic["visible_message_mode"] != "FAILURE_ALERT":
            raise ValueError("failed V20 chain probe must use the failure alert renderer")


def _validate_frozen_entry_message_replay(
    record: OutboxRecord,
    semantic: Mapping[str, Any],
) -> None:
    """Authenticate a byte-for-byte replay of a sealed official entry message."""

    _require_fields(
        semantic,
        {
            "event_id",
            "manual_request_id",
            "event_trade_date",
            "replay_profile",
            "visible_message_mode",
            "exact_automatic_message",
            "retrospective_expired",
            "source_entry_event_id",
            "source_entry_action",
            "source_final_multiplier",
            "source_semantic_content_hash",
            "source_payload_hash",
            "message_sha256",
            "symbols",
            "official_state_changed",
            "orders_changed",
            "non_actionable",
            "delivery_priority_class",
            "message",
        },
        subject="V20 frozen morning entry message replay",
    )
    if semantic["event_id"] != record.event_id:
        raise ValueError("V20 frozen morning replay event_id does not match outbox event")
    if (
        semantic["replay_profile"] != "FROZEN_OFFICIAL_ENTRY_MESSAGE_V1"
        or semantic["visible_message_mode"] != "FROZEN_OFFICIAL_PAYLOAD"
        or semantic["exact_automatic_message"] is not True
        or semantic["retrospective_expired"] is not True
        or semantic["official_state_changed"] is not False
        or semantic["orders_changed"] is not False
        or semantic["non_actionable"] is not True
        or semantic["delivery_priority_class"] != "OPERATOR_NOTIFICATION"
    ):
        raise ValueError("V20 frozen morning replay must remain exact and non-actionable")
    message = semantic["message"]
    if not isinstance(message, str) or not message:
        raise ValueError("V20 frozen morning replay message is empty")
    if hashlib.sha256(message.encode("utf-8")).hexdigest() != semantic["message_sha256"]:
        raise ValueError("V20 frozen morning replay message hash is invalid")
    for field in ("source_semantic_content_hash", "source_payload_hash"):
        value = semantic[field]
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError(f"V20 frozen morning replay {field} is invalid")
    if semantic["source_entry_action"] not in {
        "ENTER",
        "BLOCK",
        "NO_SIGNAL",
        "INPUT_INVALID",
    }:
        raise ValueError("V20 frozen morning replay source action is invalid")
    multiplier = semantic["source_final_multiplier"]
    if not _finite_number(multiplier) or not 0 <= float(multiplier) <= 1:
        raise ValueError("V20 frozen morning replay multiplier is invalid")
    if not isinstance(semantic["symbols"], list):
        raise ValueError("V20 frozen morning replay symbols must be an array")


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
        if semantic.get("alert_code") == "MANUAL_MONITOR_ARMED":
            _require_fields(
                semantic,
                {
                    "event_id",
                    "enrollment_id",
                    "source_event_id",
                    "model_batch_id",
                    "signal_date",
                    "d1",
                    "d2",
                    "activation_cutoff_ts",
                    "reference_profile_id",
                    "reference_evidence_status",
                    "armed_leg_count",
                    "symbols",
                    "official_state_changed",
                    "orders_changed",
                    "delivery_priority_class",
                },
                subject="V20 manual monitor confirmation",
            )
            if (
                semantic["event_id"] != record.event_id
                or semantic["delivery_priority_class"] != "OPERATOR_NOTIFICATION"
                or semantic["reference_evidence_status"] != "COMPLETE_PENDING_D1_ARBITRATION"
                or semantic["official_state_changed"] is not False
                or semantic["orders_changed"] is not False
                or not isinstance(semantic["symbols"], list)
                or isinstance(semantic["armed_leg_count"], bool)
                or semantic["armed_leg_count"] != len(semantic["symbols"])
                or semantic["armed_leg_count"] <= 0
            ):
                raise ValueError("V20 manual monitor confirmation is inconsistent")
            try:
                signal_date = date.fromisoformat(str(semantic["signal_date"]))
                d1 = date.fromisoformat(str(semantic["d1"]))
                d2 = date.fromisoformat(str(semantic["d2"]))
                activation_cutoff = datetime.fromisoformat(str(semantic["activation_cutoff_ts"]))
            except ValueError as exc:
                raise ValueError("V20 manual monitor dates are invalid") from exc
            if (
                not signal_date < d1 < d2
                or activation_cutoff.tzinfo is None
                or activation_cutoff.utcoffset() is None
                or activation_cutoff.astimezone(SHANGHAI).date() != d1
                or activation_cutoff.astimezone(SHANGHAI).time().replace(tzinfo=None) != time(9, 30)
            ):
                raise ValueError("V20 manual monitor activation boundary is invalid")
        elif semantic.get("alert_code") == "MANUAL_0939_CHAIN_PROBE_RESULT":
            _validate_manual_0939_chain_probe(record, semantic)
        elif semantic.get("alert_code") == "MANUAL_MORNING_ENTRY_MESSAGE_REPLAY":
            _validate_frozen_entry_message_replay(record, semantic)
        elif semantic.get("alert_code") == "MANUAL_TRIGGER_RECEIPT":
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
        elif semantic.get("alert_code") == "LATE_0939_REPLAY_RESULT":
            _require_fields(
                semantic,
                {
                    "event_id",
                    "event_trade_date",
                    "replay_kind",
                    "official_entry_action",
                    "official_entry_event_id",
                    "replay_action",
                    "final_multiplier",
                    "symbols",
                    "non_actionable",
                    "delivery_priority_class",
                    "data_cutoff",
                    "data_receipt_timeliness",
                    "computed_at",
                    "state_replay_profile",
                    "bootstrap_mode",
                    "pit_limitations",
                },
                subject="V20 late 09:39 replay",
            )
            if semantic["event_id"] != record.event_id:
                raise ValueError("V20 late replay event_id does not match outbox event")
            if (
                semantic["replay_kind"] != "RETROSPECTIVE_POST_CUTOFF"
                or semantic["official_entry_action"]
                not in {"ENTER", "BLOCK", "NO_SIGNAL", "INPUT_INVALID"}
                or semantic["non_actionable"] is not True
            ):
                raise ValueError("V20 late replay must remain retrospective and non-actionable")
            if semantic["delivery_priority_class"] != "OPERATOR_NOTIFICATION":
                raise ValueError("V20 late replay has invalid delivery priority")
            if (
                semantic["data_cutoff"] != "09:39"
                or semantic["data_receipt_timeliness"] != "POST_CUTOFF"
                or semantic["state_replay_profile"] != "CURRENT_CODE_CANONICAL_V16_CHECK_ONLY"
            ):
                raise ValueError("V20 late replay has invalid retrospective boundary")
            if semantic["bootstrap_mode"] not in {"EMPTY_FORWARD_SHADOW", "CHECKPOINT"}:
                raise ValueError("V20 late replay has invalid bootstrap mode")
            try:
                computed_at = datetime.fromisoformat(str(semantic["computed_at"]))
                replay_date = date.fromisoformat(str(semantic["event_trade_date"]))
            except ValueError as exc:
                raise ValueError("V20 late replay has invalid date/time fields") from exc
            if (
                computed_at.tzinfo is None
                or computed_at.utcoffset() is None
                or computed_at.astimezone(SHANGHAI).date() != replay_date
                or computed_at.astimezone(SHANGHAI).timetz().replace(tzinfo=None) < time(9, 40)
            ):
                raise ValueError("V20 late replay computed_at is not post-cutoff")
            if not isinstance(semantic["pit_limitations"], list) or not semantic["pit_limitations"]:
                raise ValueError("V20 late replay must disclose PIT limitations")
            if semantic["replay_action"] not in {"ENTER", "BLOCK", "NO_SIGNAL"}:
                raise ValueError("V20 late replay action is invalid")
            multiplier = semantic["final_multiplier"]
            if not _finite_number(multiplier) or not 0 <= float(multiplier) <= 1:
                raise ValueError("V20 late replay multiplier is invalid")
            if (semantic["replay_action"] == "ENTER") != (float(multiplier) > 0):
                raise ValueError("V20 late replay action/multiplier are inconsistent")
            if not isinstance(semantic["symbols"], list):
                raise ValueError("V20 late replay symbols must be an array")


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
        record.event_type == "DATA_ALERT"
        and semantic.get("alert_code") == "MANUAL_0939_CHAIN_PROBE_RESULT"
    ):
        if semantic.get("probe_result") == "PASS" or (
            semantic.get("current_version_recomputed") is True
            and isinstance(semantic.get("entry_render_semantic"), Mapping)
        ):
            # A passing post-cutoff recomputation runs the same formal
            # computation chain as the morning, but the visible message must
            # stay a dedicated manual check-only render.  Calling
            # render_entry_message(..., on_time=True) here would masquerade
            # as the official daily decision and present "正常建立" as the
            # current action, which this trigger never is.
            message = _render_manual_entry_check_for_operator(
                semantic,
                title_prefix=title_prefix,
                event_id=record.event_id,
            )
        else:
            message = _render_manual_0939_chain_probe_for_operator(
                semantic,
                title_prefix=title_prefix,
                event_id=record.event_id,
            )
    elif (
        record.event_type == "DATA_ALERT"
        and semantic.get("alert_code") == "MANUAL_MORNING_ENTRY_MESSAGE_REPLAY"
    ):
        # The source is an already sealed official ENTRY_DECISION payload.
        # Its bytes stay verbatim inside a clearly labeled sealed-source
        # region under an unmissable check-only banner, so the whole message
        # can never be read as a new instruction while the original remains
        # extractable byte-for-byte for audit.
        message = _render_frozen_entry_check_for_operator(
            semantic,
            title_prefix=title_prefix,
            event_id=record.event_id,
        )
    elif (
        record.event_type == "DATA_ALERT" and semantic.get("alert_code") == "MANUAL_TRIGGER_RECEIPT"
    ):
        message = _render_manual_receipt_for_operator(
            semantic,
            title_prefix=title_prefix,
            event_id=record.event_id,
            generated_at=generated_at,
        )
    elif (
        record.event_type == "DATA_ALERT"
        and semantic.get("alert_code") == "LATE_0939_REPLAY_RESULT"
    ):
        message = _render_late_replay_for_operator(
            semantic,
            title_prefix=title_prefix,
            event_id=record.event_id,
        )
    else:
        message = _render_data_alert_for_operator(
            semantic,
            title_prefix=title_prefix,
            event_id=record.event_id,
            generated_at=generated_at,
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

    async def send_delivery(
        self,
        envelope: Mapping[str, Any],
        *,
        delivery_variant: str = "RELAY_ENFORCED",  # noqa: ARG002
    ) -> bool:
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
    """Adapter for the legacy one-shot ``/api/send`` relay contract."""

    bot_origin: str
    app_id: str
    app_secret: str
    chat_id: str
    destination_fingerprint: str
    clock: Callable[[], datetime] = lambda: datetime.now(SHANGHAI)

    async def send_delivery(
        self,
        envelope: Mapping[str, Any],
        *,
        delivery_variant: str = "PRIMARY",
    ) -> bool:
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

        message = str(
            envelope["expired_delivery_message"] or ""
            if delivery_class == "ACTIONABLE_ENTRY" and delivery_variant == "EXPIRED_NOTICE"
            else envelope["message"]
        )
        if delivery_class == "ACTIONABLE_ENTRY":
            expiry_value = envelope["action_expiry_ts"]
            if not isinstance(expiry_value, str):
                raise V20RelayContractError("actionable V20 relay entry requires an expiry")
            try:
                expiry = datetime.fromisoformat(expiry_value)
            except ValueError as exc:
                raise V20RelayContractError("V20 relay action expiry is invalid") from exc
            if expiry.tzinfo is None or expiry.utcoffset() is None:
                raise V20RelayContractError("V20 relay action expiry must be timezone-aware")
        elif envelope["action_expiry_ts"] is not None:
            raise V20RelayContractError("non-actionable V20 delivery cannot have an expiry")
        if not message:
            raise V20RelayContractError("legacy Feishu relay message is empty")

        try:
            return await post_message_once(
                bot_url=self.bot_origin,
                app_id=self.app_id,
                app_secret=self.app_secret,
                chat_id=self.chat_id,
                message=message,
            )
        except ValueError as exc:
            raise V20RelayContractError("legacy Feishu relay response is not JSON") from exc
        except RuntimeError as exc:
            raise V20RelayContractError("legacy Feishu relay did not return success") from exc


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
    """Durable at-most-once publisher whose dispatch boundary is PostgreSQL."""

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
                await self._repository.defer_before_dispatch(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
                    error=f"unknown route_id {record.route_id}",
                    retry_after_seconds=300,
                )
                continue
            if not route.is_configured():
                self._last_cycle_error = f"route {record.route_id} is not configured"
                await self._repository.defer_before_dispatch(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
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
                error = f"entry outbox has ambiguous delivery class for {record.event_id}"
                self._last_cycle_error = error
                await self._repository.defer_before_dispatch(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
                    error=error,
                    retry_after_seconds=300,
                )
                continue
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
                error = f"outbox payload lacks message for {record.event_id}"
                self._last_cycle_error = error
                await self._repository.defer_before_dispatch(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
                    error=error,
                    retry_after_seconds=300,
                )
                continue
            if actionable_entry and not expired_message:
                error = f"actionable entry lacks expired notice for {record.event_id}"
                self._last_cycle_error = error
                await self._repository.defer_before_dispatch(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
                    error=error,
                    retry_after_seconds=300,
                )
                continue
            action_expiry = record.action_expiry_ts
            if actionable_entry and action_expiry is None:
                error = f"actionable entry lacks action expiry for {record.event_id}"
                self._last_cycle_error = error
                await self._repository.defer_before_dispatch(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
                    error=error,
                    retry_after_seconds=300,
                )
                continue
            if not isinstance(record.payload_hash, str) or len(record.payload_hash) != 64:
                error = f"outbox payload hash is invalid for {record.event_id}"
                self._last_cycle_error = error
                await self._repository.defer_before_dispatch(
                    record.event_id,
                    worker_id=self._worker_id,
                    route_id=self._route_id,
                    official_stream_id=self._official_stream_id,
                    lineage_id=self._lineage_id,
                    error=error,
                    retry_after_seconds=300,
                )
                continue

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
            if actionable_entry:
                assert action_expiry is not None
            attempt = await self._repository.begin_delivery_attempt(
                record.event_id,
                worker_id=self._worker_id,
                route_id=self._route_id,
                official_stream_id=self._official_stream_id,
                lineage_id=self._lineage_id,
                action_reserve_seconds=(
                    0.0
                    if getattr(route, "transport", "v20_relay") == "v20_relay"
                    else _LEGACY_ACTION_RESERVE_SECONDS
                ),
                relay_enforced=getattr(route, "transport", "v20_relay") == "v20_relay",
            )
            send_error: str | None
            send_started_monotonic = asyncio.get_running_loop().time()
            legacy_transport = getattr(route, "transport", "v20_relay") == "legacy_send"
            try:
                if legacy_transport:
                    async with asyncio.timeout(_LEGACY_OUTWARD_CALL_DEADLINE_SECONDS):
                        succeeded = await route.relay().send_delivery(
                            envelope,
                            delivery_variant=attempt.delivery_variant,
                        )
                else:
                    succeeded = await route.relay().send_delivery(
                        envelope,
                        delivery_variant=attempt.delivery_variant,
                    )
            except asyncio.CancelledError:
                try:
                    await self._repository.complete_delivery(
                        record.event_id,
                        attempt_number=attempt.attempt_number,
                        worker_id=self._worker_id,
                        route_id=self._route_id,
                        official_stream_id=self._official_stream_id,
                        lineage_id=self._lineage_id,
                        outcome="UNKNOWN",
                        error="relay_send cancelled after dispatch boundary",
                        retry_after_seconds=300,
                    )
                except Exception:
                    logger.exception("V20 Feishu cancellation could not finalize unknown state")
                raise
            except Exception as exc:  # the durable outbox owns every retry
                logger.warning("V20 Feishu delivery failed", exc_info=True)
                succeeded = False
                send_error = f"{type(exc).__name__}: {exc}"
                before_total_deadline = (
                    asyncio.get_running_loop().time() - send_started_monotonic
                ) < _LEGACY_OUTWARD_CALL_DEADLINE_SECONDS
                retryable = before_total_deadline and isinstance(
                    exc,
                    (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout),
                )
            else:
                send_error = None if succeeded else "Feishu relay returned failure"
                retryable = False
            if not succeeded:
                self._last_cycle_error = send_error or "Feishu relay returned failure"
            await self._repository.complete_delivery(
                record.event_id,
                attempt_number=attempt.attempt_number,
                worker_id=self._worker_id,
                route_id=self._route_id,
                official_stream_id=self._official_stream_id,
                lineage_id=self._lineage_id,
                outcome="DELIVERED" if succeeded else ("SAFE_RETRY" if retryable else "UNKNOWN"),
                error=send_error,
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
