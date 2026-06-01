"""Tests for ST-filter Tushare ``stock_basic`` batching.

Regression guard for Tushare error 50101 ("列表个数超过限制1000个"): a single
``stock_basic`` request must never carry more than 1000 codes. ``_fetch_tushare_names``
chunks the codes into ``_STOCK_BASIC_BATCH_SIZE`` requests and merges the results.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.data.database.fundamentals_db import (
    _STOCK_BASIC_BATCH_SIZE,
    _fetch_tushare_names,
)


def _make_post_mock(captured_batches: list[list[str]]):
    """Build an AsyncMock POST that echoes each requested ts_code back as a name.

    Records the per-request code lists into ``captured_batches`` so tests can
    assert the chunking behaviour.
    """

    async def _post(url, json):  # noqa: A002 - mirror httpx.post signature
        ts_codes = json["params"]["ts_code"].split(",")
        captured_batches.append(ts_codes)
        items = [[tc, f"name-{tc[:6]}"] for tc in ts_codes]
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"code": 0, "data": {"items": items}}
        return resp

    return AsyncMock(side_effect=_post)


@pytest.mark.asyncio
async def test_fetch_names_chunks_above_1000_codes():
    """>1000 codes must be split into multiple requests, none exceeding the cap."""
    codes = [f"{i:06d}" for i in range(2500)]
    captured: list[list[str]] = []

    with (
        patch("src.data.database.fundamentals_db.get_tushare_token", return_value="tok"),
        patch("httpx.AsyncClient") as mock_client,
    ):
        mock_client.return_value.__aenter__.return_value.post = _make_post_mock(captured)
        names = await _fetch_tushare_names(codes)

    # 2500 codes / 900 per batch -> 3 requests (900, 900, 700).
    assert len(captured) == 3
    assert [len(b) for b in captured] == [900, 900, 700]
    assert all(len(b) <= _STOCK_BASIC_BATCH_SIZE for b in captured)
    assert all(len(b) <= 1000 for b in captured)
    # All codes resolved and merged across batches.
    assert len(names) == 2500
    assert names["000000"] == "name-000000"
    assert names["002499"] == "name-002499"


@pytest.mark.asyncio
async def test_fetch_names_single_request_under_cap():
    """A small list stays a single request."""
    codes = [f"{i:06d}" for i in range(10)]
    captured: list[list[str]] = []

    with (
        patch("src.data.database.fundamentals_db.get_tushare_token", return_value="tok"),
        patch("httpx.AsyncClient") as mock_client,
    ):
        mock_client.return_value.__aenter__.return_value.post = _make_post_mock(captured)
        names = await _fetch_tushare_names(codes)

    assert len(captured) == 1
    assert len(names) == 10


@pytest.mark.asyncio
async def test_fetch_names_empty_makes_no_request():
    """Empty input short-circuits without hitting the network."""
    captured: list[list[str]] = []

    with (
        patch("src.data.database.fundamentals_db.get_tushare_token", return_value="tok"),
        patch("httpx.AsyncClient") as mock_client,
    ):
        mock_client.return_value.__aenter__.return_value.post = _make_post_mock(captured)
        names = await _fetch_tushare_names([])

    assert names == {}
    assert captured == []
