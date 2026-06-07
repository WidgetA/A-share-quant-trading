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
from src.data.services.kimi_listing_verifier import KimiToolError
from src.data.services.listing_verify_scheduler import ListingVerifyScheduler


class _FakeStorage:
    def __init__(self, unverified, failed=None):
        self.is_ready = True
        self._unverified = set(unverified)
        self._failed = set(failed or [])
        self.written: list[dict] = []
        self.aliases: list[dict] = []

    async def get_unverified_codes_in_snapshot(self):
        return set(self._unverified)

    async def get_failed_verified_codes(self):
        return set(self._failed)

    async def upsert_listing_info(self, entries):
        self.written.extend(entries)
        return len(entries)

    async def upsert_code_alias(self, entries):
        self.aliases.extend(entries)
        return len(entries)


def _scheduler(storage):
    state = SimpleNamespace(storage=storage, cache_fill_running=False)
    return ListingVerifyScheduler(state)


def _kimi_stub(found: dict[str, str]):
    """Fake run_kimi_for_code: `found[code]` → a real date; anything else →
    a genuine 'searched but not found' answer (NOT a tool error)."""

    async def _run(code, timeout_sec=180, raw_dir=None):
        if code in found:
            return {
                "code": code,
                "name": f"N{code}",
                "list_date": found[code],
                "source": "http://x",
            }
        return {"code": code, "name": None, "list_date": None, "source": None, "error": "not found"}

    return _run


def _kimi_tool_error_stub(reason="kimi 未授权/登录或凭证问题"):
    """Fake run_kimi_for_code that always raises KimiToolError (kimi broken)."""

    async def _run(code, timeout_sec=180, raw_dir=None):
        raise KimiToolError(f"{reason} — code={code}")

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
    assert result == {"checked": 0, "verified": 0, "failed": 0, "remaining": 0, "findings": []}
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


@pytest.mark.asyncio
async def test_kimi_tool_error_aborts_with_real_reason(monkeypatch):
    """When kimi itself fails, DON'T mask it as '查不到': abort, write NO placeholders,
    and surface the REAL reason (here a timeout) — never a hardcoded '凭证'."""
    codes = [f"{1 + i:06d}" for i in range(20)]
    storage = _FakeStorage(codes)
    sched = _scheduler(storage)
    monkeypatch.setattr(mod, "run_kimi_for_code", _kimi_tool_error_stub("kimi 超时 (>600s)"))
    with patch.object(mod, "_notify_feishu", new=AsyncMock()) as feishu:
        result = await sched.verify_unverified()

    assert result["verified"] == 0
    assert result["failed"] == 0  # NOT counted as 查不到
    assert result["tool_errors"] >= mod._TOOL_ERROR_ABORT
    # the ACTUAL reason flows through honestly — timeout is reported as 超时, not 凭证
    assert "超时" in result["error"]
    assert "超时" in (result.get("tool_error_sample") or "")
    assert "凭证" not in result["error"]
    assert storage.written == []  # no bogus placeholders written
    # checked stops early at the abort threshold, not the full batch
    assert result["checked"] <= mod._TOOL_ERROR_ABORT
    msg = feishu.await_args.args[0]
    assert "已中止" in msg and "未写任何占位" in msg and "超时" in msg


@pytest.mark.asyncio
async def test_migration_writes_alias_and_excluded_marker_not_listing(monkeypatch):
    """A 迁号/换号 finding (new_code present) must NOT write the old code as a verified live
    listing row (that caused the source_none loop). It writes a list_date=None excluded
    marker (roster_for_day drops it) + a code_alias old→new, counted as 'migrated'."""
    storage = _FakeStorage(["830779"])
    sched = _scheduler(storage)

    async def _mig(code, timeout_sec=180, raw_dir=None):
        return {
            "code": code,
            "name": "武汉蓝电",
            "list_date": "2023-06-01",
            "delist_date": "2025-10-09",
            "status": "迁移新代码",
            "new_code": "920779",
            "note": "迁到 920779",
            "source": "http://x",
        }

    monkeypatch.setattr(mod, "run_kimi_for_code", _mig)
    with patch.object(mod, "_notify_feishu", new=AsyncMock()):
        result = await sched.verify_unverified()

    # old code written as an EXCLUDED marker (list_date=None), NOT a verified live listing
    assert len(storage.written) == 1
    row = storage.written[0]
    assert row["code"] == "830779"
    assert row["verified"] is False
    assert row["list_date"] is None  # roster_for_day excludes list_date=None
    assert row["source"] == mod._MIGRATED_SOURCE
    # code_alias old→new recorded so load_listing drops 830779 entirely next reload
    assert len(storage.aliases) == 1
    assert storage.aliases[0]["old_code"] == "830779"
    assert storage.aliases[0]["new_code"] == "920779"
    # categorized as migrated, NOT verified / not-found
    assert result["migrated"] == 1
    assert result["verified"] == 0
    assert result["failed"] == 0


@pytest.mark.asyncio
async def test_genuine_not_found_writes_placeholder(monkeypatch):
    """kimi searched and genuinely didn't find → verified=false placeholder."""
    storage = _FakeStorage(["999999"])
    sched = _scheduler(storage)
    monkeypatch.setattr(mod, "run_kimi_for_code", _kimi_stub({}))  # all not-found
    with patch.object(mod, "_notify_feishu", new=AsyncMock()):
        result = await sched.verify_unverified()
    assert result["verified"] == 0
    assert result["failed"] == 1  # genuine not-found
    assert result["tool_errors"] == 0
    assert storage.written[0]["verified"] is False
