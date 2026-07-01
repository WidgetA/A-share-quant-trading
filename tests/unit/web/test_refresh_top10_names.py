"""Tests for `_refresh_top10_names` — overriding stale display names.

The offline board JSON name can lag reality (e.g. a de-ST'd stock still carried
as "*ST华微"). Before the top-10 report is sent, the final picks' display names
are refreshed from live Tushare. This is display-only and best-effort: on any
Tushare failure the cached names must be kept, and the report still sends.
"""

from unittest.mock import AsyncMock

import pytest

from src.strategy.lgbrank_scorer import ScoredStock
from src.web.v15_scan_service import _refresh_top10_names


def _picks() -> list[ScoredStock]:
    return [
        # stale name from offline board JSON — 摘帽 already but still "*ST华微"
        ScoredStock(code="600360", name="*ST华微", score=0.16, rank=1, buy_price=13.98),
        ScoredStock(code="002654", name="万润科技", score=0.16, rank=2, buy_price=21.08),
    ]


@pytest.mark.asyncio
async def test_stale_name_overridden_with_live_tushare_name():
    picks = _picks()
    fdb = AsyncMock()
    fdb.batch_current_names = AsyncMock(return_value={"600360": "华微电子", "002654": "万润科技"})

    await _refresh_top10_names(fdb, picks)

    fdb.batch_current_names.assert_awaited_once_with(["600360", "002654"])
    assert picks[0].name == "华微电子"  # de-ST name applied
    assert picks[1].name == "万润科技"


@pytest.mark.asyncio
async def test_missing_code_keeps_cached_name():
    """Codes Tushare doesn't return keep their cached name (no wipe)."""
    picks = _picks()
    fdb = AsyncMock()
    fdb.batch_current_names = AsyncMock(return_value={"002654": "万润科技"})

    await _refresh_top10_names(fdb, picks)

    assert picks[0].name == "*ST华微"  # absent from Tushare -> unchanged
    assert picks[1].name == "万润科技"


@pytest.mark.asyncio
async def test_tushare_failure_is_swallowed_and_names_kept():
    """Best-effort: a Tushare error must not raise and must keep cached names."""
    picks = _picks()
    fdb = AsyncMock()
    fdb.batch_current_names = AsyncMock(side_effect=RuntimeError("tushare down"))

    await _refresh_top10_names(fdb, picks)  # must not raise

    assert picks[0].name == "*ST华微"
    assert picks[1].name == "万润科技"


@pytest.mark.asyncio
async def test_noops_on_empty_inputs():
    fdb = AsyncMock()
    fdb.batch_current_names = AsyncMock()

    await _refresh_top10_names(fdb, [])
    await _refresh_top10_names(None, _picks())

    fdb.batch_current_names.assert_not_awaited()
