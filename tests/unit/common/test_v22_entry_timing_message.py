from datetime import datetime
from zoneinfo import ZoneInfo

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
