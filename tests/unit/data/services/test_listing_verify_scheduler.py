# === MODULE PURPOSE ===
# Tests for ListingVerifyScheduler.verify_unverified (path B core).
# Mocks kimi + feishu; verifies: success rows written verified=true,
# failures written as verified=false placeholders, counts correct, and
# the per-run cap truncates with a reported remainder.

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.data.services import listing_verify_scheduler as mod
from src.data.services.listing_verify_scheduler import ListingVerifyScheduler


class _FakeStorage:
    def __init__(self, unverified, failed=None):
        self.is_ready = True
        self._unverified = set(unverified)
        self._failed = set(failed or [])
        self.written: list[dict] = []

    async def get_unverified_codes_in_snapshot(self):
        return set(self._unverified)

    async def get_failed_verified_codes(self):
        return set(self._failed)

    async def upsert_listing_info(self, entries):
        self.written.extend(entries)
        return len(entries)


def _scheduler(storage):
    state = SimpleNamespace(storage=storage, cache_fill_running=False)
    return ListingVerifyScheduler(state)


def _kimi_stub(found: dict[str, str]):
    """Return a fake run_kimi_for_code that resolves `found[code]` → a real
    list_date, anything else → None (parse failure)."""

    async def _run(code, timeout_sec=180, raw_dir=None):
        if code in found:
            return {
                "code": code,
                "name": f"N{code}",
                "list_date": found[code],
                "source": "http://x",
            }
        return None

    return _run


@pytest.mark.asyncio
async def test_verified_and_failed_split(monkeypatch):
    storage = _FakeStorage(["600519", "000001", "999999"])
    sched = _scheduler(storage)
    monkeypatch.setattr(
        mod, "run_kimi_for_code", _kimi_stub({"600519": "2001-08-27", "000001": "1991-04-03"})
    )
    with patch.object(mod, "_notify_feishu", new=AsyncMock()) as feishu:
        result = await sched.verify_unverified()

    assert result["checked"] == 3
    assert result["verified"] == 2
    assert result["failed"] == 1
    # All three rows written (2 verified + 1 placeholder).
    assert len(storage.written) == 3
    by_code = {e["code"]: e for e in storage.written}
    assert by_code["600519"]["verified"] is True
    assert by_code["600519"]["list_date"] == "2001-08-27"
    assert by_code["999999"]["verified"] is False
    assert by_code["999999"]["list_date"] is None
    assert by_code["999999"]["source"] == mod._FAILED_SOURCE
    # Feishu summary fired and names the failed code.
    feishu.assert_awaited_once()
    assert "999999" in feishu.await_args.args[0]


@pytest.mark.asyncio
async def test_no_codes_short_circuits(monkeypatch):
    storage = _FakeStorage([])
    sched = _scheduler(storage)
    monkeypatch.setattr(mod, "run_kimi_for_code", _kimi_stub({}))
    with patch.object(mod, "_notify_feishu", new=AsyncMock()):
        result = await sched.verify_unverified()
    assert result == {"checked": 0, "verified": 0, "failed": 0, "remaining": 0}
    assert storage.written == []


@pytest.mark.asyncio
async def test_per_run_cap_truncates(monkeypatch):
    codes = [f"{600000 + i:06d}" for i in range(5)]
    storage = _FakeStorage(codes)
    sched = _scheduler(storage)
    monkeypatch.setattr(mod, "MAX_CODES_PER_RUN", 2)
    monkeypatch.setattr(mod, "run_kimi_for_code", _kimi_stub({c: "2020-01-01" for c in codes}))
    with patch.object(mod, "_notify_feishu", new=AsyncMock()):
        result = await sched.verify_unverified()
    assert result["checked"] == 2
    assert result["remaining"] == 3
    assert len(storage.written) == 2


@pytest.mark.asyncio
async def test_include_failed_merges_placeholder_codes(monkeypatch):
    storage = _FakeStorage(unverified=["600519"], failed=["999999"])
    sched = _scheduler(storage)
    monkeypatch.setattr(mod, "run_kimi_for_code", _kimi_stub({"600519": "2001-08-27"}))
    with patch.object(mod, "_notify_feishu", new=AsyncMock()):
        result = await sched.verify_unverified(include_failed=True)
    assert result["checked"] == 2  # snapshot-unverified ∪ failed placeholder


@pytest.mark.asyncio
async def test_storage_not_ready_returns_error(monkeypatch):
    storage = _FakeStorage([])
    storage.is_ready = False
    sched = _scheduler(storage)
    result = await sched.verify_unverified()
    assert result["error"]
