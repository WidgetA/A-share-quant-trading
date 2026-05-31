# === MODULE PURPOSE ===
# Tests for the gap-diagnosis report (scripts/diagnose_gaps.py): the pure
# report builder (problem → root cause → what it should be → how to fix) and
# the orchestration wiring (classify each missing stock) with a fake store.

from __future__ import annotations

from datetime import date

import pytest

from scripts.diagnose_gaps import (
    build_detail_markdown,
    build_report,
    diagnose_gaps,
    write_report_files,
)


def _diag(**over):
    base = {
        "all_time_count": 5659,
        "per_day_min": 4900,
        "per_day_max": 5527,
        "per_day_recent": 5523,
        "delisted_in_listing": 140,
        "listing_total": 5659,
        "listing_verified": 5659,
        "daily_gap_days": 2,
        "daily_days": [
            {
                "date": "2024-01-03",
                "expected_count": 5521,
                "actual_count": 5519,
                "reported_missing_count": 2,
                "classified_missing_count": 2,
                "correct_count_after_fix": 5520,
                "rows_to_add": 1,
                "class_counts": {"A": 1, "B": 1, "C": 0, "PENDING": 0},
                "missing_by_class": {
                    "A": [{"code": "000001", "reason": "已退市"}],
                    "B": [{"code": "600000", "reason": "真缺"}],
                    "C": [],
                    "PENDING": [],
                },
                "problem": "名单误报 000001；日线真缺 600000",
                "root_cause": (
                    "stock_snapshot/effective universe 含当天不该在册股票；"
                    "Tushare daily 入库漏了在册且未停牌股票"
                ),
                "correct_number": "当天正确日线股票数应为 5520 只;库内现有 5519 只,需补 1 条。",
                "fix": "A: 按已核上市/退市日修正名单；B: 对这些(日期,股票)精准重下日线",
            }
        ],
        "daily_items": [
            {"date": "2024-01-03", "code": "000001", "cls": "A", "reason": "已退市"},
            {"date": "2024-01-03", "code": "600000", "cls": "B", "reason": "真缺"},
        ],
        "minute_gap_days": 740,
        "minute_detail_days": 50,
        "minute_detail_is_full": False,
        "minute_days": [
            {
                "date": "2023-01-03",
                "expected_stock_count": 4900,
                "complete_stock_count": 4899,
                "reported_incomplete_count": 1,
                "classified_incomplete_count": 1,
                "correct_complete_count_after_fix": 4899,
                "bars_to_add": 0,
                "class_counts": {"A": 0, "B": 0, "C": 1, "PENDING": 0},
                "missing_by_class": {
                    "A": [],
                    "B": [],
                    "C": [
                        {
                            "code": "300001",
                            "reason": "源头不足",
                            "local_bar_count": 200,
                            "source_bar_count": 200,
                        }
                    ],
                    "PENDING": [],
                },
                "problem": "口径误报/源头不足 300001",
                "root_cause": "源头本身不足 241 根、上市首日半天或低成交量按成交分钟返回",
                "correct_number": "当天可修真错修完后,完整股票数应至少为 4899 只;",
                "fix": "C: 记录为源头不足/半天/低成交量口径,不要按 241 根继续报警",
            }
        ],
        "minute_items": [
            {
                "date": "2023-01-03",
                "code": "300001",
                "cls": "C",
                "local_bar_count": 200,
                "source_bar_count": 200,
                "reason": "源头不足",
            },
        ],
    }
    base.update(over)
    return base


def test_report_has_three_problems_and_fix_lines():
    r = build_report(_diag())
    # three problems present
    assert "问题1" in r and "问题2" in r and "问题3" in r
    # each problem states root cause + what-it-should-be + how-to-fix
    assert "根因" in r and "真实应该" in r and "怎么修" in r
    # the headline numbers appear
    assert "5659" in r and "5527" in r and "740" in r
    # class breakdown for daily gaps
    assert "A 名单错 1" in r and "B 真缺 1" in r
    assert "源头也不足 241 根 1" in r


def test_report_shows_sampling_transparency():
    r = build_report(_diag())
    assert "抽样分类 50 天" in r  # minute detail sampling disclosed, not silent
    assert "完整逐天明细" in r


def test_report_lists_daily_problems_by_day():
    r = build_report(_diag())
    assert "2024-01-03: 名单误报 000001；日线真缺 600000" in r
    assert "正确数字: 当天正确日线股票数应为 5520" in r
    assert "怎么修:" in r


def test_detail_markdown_contains_full_daily_and_minute_sections(tmp_path):
    diag = _diag()
    md = build_detail_markdown(diag)
    assert "## 日线问题日" in md
    assert "### 2024-01-03" in md
    assert "## 分钟问题日" in md
    assert "300001" in md

    files = write_report_files(
        diag,
        json_file=tmp_path / "gap.json",
        markdown_file=tmp_path / "gap.md",
    )
    assert (tmp_path / "gap.json").exists()
    assert (tmp_path / "gap.md").read_text(encoding="utf-8").startswith("# 数据缺口按天详报")
    assert files["markdown"].endswith("gap.md")


class _FakeStore:
    def __init__(self):
        self._listing = {
            "000001": {
                "list_date": date(2010, 1, 1),
                "delist_date": date(2023, 6, 1),
                "verified": True,
            },  # delisted before the gap day -> A
            "600000": {"list_date": date(2000, 1, 1), "delist_date": None, "verified": True},
        }

    async def get_listing_info_all(self):
        return self._listing

    async def get_daily_stock_count(self):
        return 5659

    async def get_daily_counts_per_day(self):
        return {date(2024, 1, 3): 5519}

    async def audit_daily_gaps(self):
        return [(date(2024, 1, 3), 5521, 5519)]

    async def get_effective_universe_for_date(self, d):
        return {"000001", "600000", "000002"}

    async def get_codes_for_daily_date(self, d):
        return {"000002"}  # so 000001 (delisted->A) and 600000 (listed->B) are missing

    async def audit_minute_gaps(self):
        return [(date(2023, 1, 3), 4900, 4899)]

    async def find_missing_minute_stocks(self, d):
        return {"600000"}, 4900

    async def get_minute_bar_counts(self, d):
        return {"600000": 0}  # zero bars -> B


@pytest.mark.asyncio
async def test_diagnose_classifies_each_missing_stock():
    store = _FakeStore()
    source_calls = []

    async def fetch_suspended(day):
        return set()

    async def fetch_minute_source_count(day, code):
        source_calls.append((day, code))
        return 241

    diag = await diagnose_gaps(
        store, fetch_suspended, fetch_minute_source_count, minute_detail_days=10
    )

    by_code = {it["code"]: it["cls"] for it in diag["daily_items"]}
    assert by_code["000001"] == "A"  # delisted -> name list wrong
    assert by_code["600000"] == "B"  # listed, not suspended, missing -> real gap
    # minute: source has 241 bars but local has zero -> DB missing -> B
    assert diag["minute_items"][0]["cls"] == "B"
    assert diag["minute_items"][0]["source_bar_count"] == 241
    assert diag["daily_days"][0]["correct_count_after_fix"] == 5520
    assert diag["daily_days"][0]["rows_to_add"] == 1
    assert diag["minute_days"][0]["correct_complete_count_after_fix"] == 4900
    assert source_calls == [(date(2023, 1, 3), "600000")]
    # report renders without error
    assert "问题1" in build_report(diag)


@pytest.mark.asyncio
async def test_diagnose_marks_minute_source_short_as_not_wrong():
    class _SourceShortStore(_FakeStore):
        async def get_minute_bar_counts(self, d):
            return {"600000": 200}

    store = _SourceShortStore()

    async def fetch_suspended(day):
        return set()

    async def fetch_minute_source_count(day, code):
        return 200

    diag = await diagnose_gaps(
        store, fetch_suspended, fetch_minute_source_count, minute_detail_days=10
    )

    item = diag["minute_items"][0]
    assert item["cls"] == "C"
    assert item["local_bar_count"] == 200
    assert item["source_bar_count"] == 200
    assert "源头不足" in item["reason"]
