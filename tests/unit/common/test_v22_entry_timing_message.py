from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.common.v20_feishu import seal_v20_payload
from src.data.database.v20_repository import OutboxRecord, sha256_json
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
)


def test_entry_timing_notice_precedes_ordinary_stock_list_without_system_alarm_language():
    semantic = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "strategy_version": "V22-slim",
        "event_id": "a" * 64,
        "entry_event_id": "b" * 64,
        "alert_code": "V22_ENTRY_TIMING_ALERT",
        "event_trade_date": "2026-09-23",
        "message": "今天可以考虑不在开盘进。",
        "symbols": [{"code": "600001", "name": "示例股票"}],
    }
    record = OutboxRecord(
        event_id=semantic["event_id"],
        event_type="DATA_ALERT",
        route_id="route",
        official_stream_id="stream",
        lineage_id="lineage",
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
    now = datetime(2026, 9, 23, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai"))
    text = seal_v20_payload(record, now, 1, True)["message"]
    assert text.splitlines()[0] == "今天可以考虑不在开盘进。"
    assert "适用交易日：2026-09-23" in text
    assert "以下股票可以考虑等待更低价再介入：" in text
    assert "600001 示例股票" in text
    assert (
        "参考：9:40价格下方1%。上午到价，13:30仍到价再考虑，否则继续等到收盘；"
        "上午未到价，下午13:00后首次到价再考虑。" in text
    )
    assert all(word not in text for word in ("系统报警", "需要检查", "自动成交", "rank", "D0"))


@pytest.mark.parametrize(
    ("status", "reason", "headline", "explanation"),
    [
        (
            "NO_WAIT",
            "ORIGINAL_GATE_BLOCKED",
            "今天不满足延后入场条件。",
            "原策略未放行，今天不新开仓。",
        ),
        (
            "NO_WAIT",
            "NO_CANDIDATES",
            "今天不满足延后入场条件。",
            "本次没有可买入股票。",
        ),
        (
            "NO_WAIT",
            "INDEX_NOT_GREEN",
            "今天不满足延后入场条件。",
            "9:40上证指数没有低于昨收。",
        ),
        (
            "NO_WAIT",
            "NO_STOCK_RULE_MATCH",
            "今天不满足延后入场条件。",
            "计划买入股票均未命中等待低价的条件。",
        ),
        (
            "UNAVAILABLE",
            "INDEX_0940_PENDING",
            "本次入场时点暂时无法判断。",
            "9:40的指数数据尚未齐备。",
        ),
        (
            "UNAVAILABLE",
            "STOCK_FEATURES_UNAVAILABLE",
            "本次入场时点暂时无法判断。",
            "个股开盘分钟数据不足或无效。",
        ),
        (
            "UNAVAILABLE",
            "PRIOR_INDEX_DAILY_UNAVAILABLE",
            "本次入场时点暂时无法判断。",
            "指数昨收数据不可用。",
        ),
    ],
)
def test_nonmatching_and_unavailable_runs_explain_their_result_before_stock_list(
    status, reason, headline, explanation
):
    semantic = {
        "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "strategy_version": "V22-slim",
        "event_id": "a" * 64,
        "entry_event_id": "b" * 64,
        "alert_code": "V22_ENTRY_TIMING_ALERT",
        "event_trade_date": "2026-09-24",
        "message": "入场时点判断",
        "symbols": [],
        "entry_timing_advisory": {
            "schema": "v22-entry-timing/v1",
            "status": status,
            "reason": reason,
            "symbols": [],
        },
    }
    record = OutboxRecord(
        event_id=semantic["event_id"],
        event_type="DATA_ALERT",
        route_id="route",
        official_stream_id="stream",
        lineage_id="lineage",
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
    now = datetime(2026, 9, 24, 9, 40, tzinfo=ZoneInfo("Asia/Shanghai"))
    text = seal_v20_payload(record, now, 1, True)["message"]
    assert text.splitlines()[0] == headline
    assert "适用交易日：2026-09-24" in text
    assert explanation in text
    assert "今天可以考虑不在开盘进" not in text
    assert "9:40价格下方1%" not in text
    assert reason not in text
