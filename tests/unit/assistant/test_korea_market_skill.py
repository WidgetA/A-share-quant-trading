from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPT = (
    Path(__file__).parents[3]
    / "kimi-skills"
    / "check-korea-market"
    / "scripts"
    / "query_korea_market.py"
)
SPEC = importlib.util.spec_from_file_location("query_korea_market", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def provider_values(
    previous: float = 100.0,
    official_open: float = 101.0,
    snapshot: float = 102.0,
):
    return {
        "target_date": "2026-08-13",
        "previous_date": "2026-08-12",
        "previous_close": previous,
        "official_open": official_open,
        "snapshot_open": snapshot,
        "snapshot_close": snapshot,
    }


def test_missing_time_defaults_to_beijing_0900():
    now = datetime(2026, 8, 13, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    target, mode = MODULE.parse_target(None, now)
    assert target.strftime("%Y-%m-%d %H:%M %z") == "2026-08-13 09:00 +0800"
    assert mode == "default_09_00"


def test_now_defers_to_latest_common_minute():
    now = datetime(2026, 8, 13, 12, 0, 42, tzinfo=ZoneInfo("Asia/Shanghai"))
    target, mode = MODULE.parse_target("now", now)
    assert target is None
    assert mode == "latest_common_minute"


def test_latest_common_minute_uses_both_indexes_and_sources():
    target_kst = datetime(2026, 8, 13, 10, 1, tzinfo=ZoneInfo("Asia/Seoul"))
    epoch_1000 = int((target_kst.replace(minute=0)).timestamp())
    epoch_1001 = int(target_kst.timestamp())

    def naver_payload():
        return {
            "tradeBaseAt": "20260813",
            "priceInfos": [
                {"localDateTime": "20260813100000", "openPrice": 100.0},
                {"localDateTime": "20260813100100", "openPrice": 101.0},
            ],
        }

    def yahoo_payload():
        return {
            "minute": {
                "chart": {
                    "result": [
                        {
                            "timestamp": [epoch_1000, epoch_1001],
                            "indicators": {"quote": [{"open": [100.0, 101.0]}]},
                        }
                    ]
                }
            }
        }

    sources = {}
    for key in ("kospi", "kosdaq"):
        sources[f"naver:{key}"] = {"payload": naver_payload()}
        sources[f"yahoo:{key}"] = {"payload": yahoo_payload()}
    target = MODULE.latest_common_target(sources, target_kst.date())
    assert target.strftime("%Y-%m-%d %H:%M %z") == "2026-08-13 09:01 +0800"


def test_matching_sources_produce_red_snapshot_and_open():
    result, errors = MODULE.evaluate_index(
        "KOSPI", provider_values(), provider_values(snapshot=102.01)
    )
    assert errors == []
    assert result["snapshot_red"] is True
    assert result["open_red"] is True


def test_snapshot_can_be_red_while_open_was_not_red():
    result, errors = MODULE.evaluate_index(
        "KOSDAQ",
        provider_values(official_open=99.0, snapshot=101.0),
        provider_values(official_open=99.01, snapshot=101.01),
    )
    assert errors == []
    assert result["snapshot_red"] is True
    assert result["open_red"] is False


def test_cross_source_mismatch_forces_error():
    _result, errors = MODULE.evaluate_index(
        "KOSDAQ", provider_values(), provider_values(snapshot=103.0)
    )
    assert any("双源指定分钟差" in error for error in errors)
