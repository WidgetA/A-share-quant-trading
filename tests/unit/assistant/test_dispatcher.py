# === MODULE PURPOSE ===
# Unit tests for the Feishu assistant dispatcher's pure logic and main-loop
# behaviour (AST-001 M1): text extraction, slash routing, whitelist, event_id
# dedup, queue-full honesty, ack wording. The lark SDK and kimi are NOT touched
# — replies and kimi runs are monkeypatched; the ws thread is never started.

from __future__ import annotations

import asyncio
import json

import pytest

from src.assistant import dispatcher as disp
from src.assistant.dispatcher import (
    AssistantDispatcher,
    IncomingMessage,
    build_task_prompt,
    extract_text,
    route,
)

OWNER = "ou_owner"


def _msg(
    text: str,
    *,
    event_id: str = "evt_1",
    open_id: str = OWNER,
    message_type: str = "text",
) -> IncomingMessage:
    return IncomingMessage(
        event_id=event_id,
        open_id=open_id,
        message_id="om_1",
        chat_type="group",
        message_type=message_type,
        content=json.dumps({"text": text}, ensure_ascii=False),
    )


def _dispatcher(
    monkeypatch: pytest.MonkeyPatch,
    queue_size: int = 4,
    allowed: frozenset[str] = frozenset({OWNER}),
) -> tuple[AssistantDispatcher, list[str]]:
    # Whitelist + read-only key are read from config at use time (hot-apply
    # from the Settings page) — stub the getters the dispatcher module uses.
    monkeypatch.setattr(disp, "get_assistant_allowed_users", lambda: allowed)
    monkeypatch.setattr(disp, "get_assistant_readonly_key", lambda: "ro-key")
    d = AssistantDispatcher(
        app_id="cli_x",
        app_secret="secret",
        api_base="http://127.0.0.1:8000",
        queue_size=queue_size,
    )
    replies: list[str] = []

    async def fake_reply(message_id: str, text: str) -> None:
        replies.append(text)

    d._reply = fake_reply  # type: ignore[method-assign]
    return d, replies


# ── pure helpers ─────────────────────────────────────────────────────────


def test_extract_text_strips_mention_placeholder():
    content = json.dumps({"text": "@_user_1 /持仓"})
    assert extract_text("text", content) == "/持仓"


def test_extract_text_rejects_non_text_and_bad_json():
    assert extract_text("image", "{}") is None
    assert extract_text("text", "not json") is None


def test_route_classification():
    assert route("/持仓") == ("slash", "/持仓")
    assert route("/持仓 请查一下") == ("slash", "/持仓")
    assert route("/不存在") == ("unknown_slash", "/不存在")
    assert route("帮我看看大盘") == ("free", "帮我看看大盘")
    assert route("") == ("empty", "")


def test_build_task_prompt_pins_skill_and_rules():
    prompt = build_task_prompt("/持仓")
    assert "check-holdings" in prompt
    assert "只做只读查询" in prompt  # task book actually embedded


# ── _handle behaviour ────────────────────────────────────────────────────


async def test_slash_command_acks_and_enqueues(monkeypatch: pytest.MonkeyPatch):
    d, replies = _dispatcher(monkeypatch)
    await d._handle(_msg("@_user_1 /持仓"))
    assert d._queue.qsize() == 1
    assert len(replies) == 1
    assert "查询" in replies[0]


async def test_empty_whitelist_allows_everyone(monkeypatch: pytest.MonkeyPatch):
    """白名单功能屏蔽中(用户拍板 2026-07-07):没配白名单 = 群里都能用。"""
    d, replies = _dispatcher(monkeypatch, allowed=frozenset())
    await d._handle(_msg("/持仓", open_id="ou_anyone"))
    assert d._queue.qsize() == 1
    assert len(replies) == 1


async def test_configured_whitelist_still_enforces(monkeypatch: pytest.MonkeyPatch):
    """休眠≠删除:显式配了白名单(env/接口)仍然生效,以后要启用直接填。"""
    d, replies = _dispatcher(monkeypatch)  # allowed={OWNER}
    await d._handle(_msg("/持仓", event_id="e1", open_id="ou_stranger"))
    assert d._queue.qsize() == 0
    assert replies == []
    await d._handle(_msg("/持仓", event_id="e2"))
    assert len(replies) == 1


async def test_duplicate_event_id_processed_once(monkeypatch: pytest.MonkeyPatch):
    d, replies = _dispatcher(monkeypatch)
    await d._handle(_msg("/持仓", event_id="evt_dup"))
    await d._handle(_msg("/持仓", event_id="evt_dup"))
    assert d._queue.qsize() == 1
    assert len(replies) == 1


async def test_queue_full_gets_honest_reply_not_silent_drop(monkeypatch: pytest.MonkeyPatch):
    d, replies = _dispatcher(monkeypatch, queue_size=1)
    await d._handle(_msg("/持仓", event_id="e1"))
    await d._handle(_msg("/持仓", event_id="e2"))
    assert d._queue.qsize() == 1
    assert len(replies) == 2
    assert "排队满了" in replies[1]


async def test_free_text_and_unknown_slash_get_help(monkeypatch: pytest.MonkeyPatch):
    d, replies = _dispatcher(monkeypatch)
    await d._handle(_msg("大盘怎么样", event_id="e1"))
    await d._handle(_msg("/涨停", event_id="e2"))
    await d._handle(_msg("图片消息", event_id="e3", message_type="image"))
    assert d._queue.qsize() == 0
    assert len(replies) == 3
    assert all("/持仓" in r for r in replies)


# ── worker behaviour ─────────────────────────────────────────────────────


async def test_worker_replies_with_kimi_result(monkeypatch: pytest.MonkeyPatch):
    d, replies = _dispatcher(monkeypatch)

    async def fake_run(task_prompt: str, readonly_key: str | None, api_base: str) -> str:
        assert "check-holdings" in task_prompt
        assert readonly_key == "ro-key"
        return "当前持仓:工商银行 1000 股。"

    monkeypatch.setattr(disp, "run_kimi_assistant_task", fake_run)
    await d._handle(_msg("/持仓"))
    worker = asyncio.get_running_loop().create_task(d._worker())
    try:
        for _ in range(100):
            if len(replies) >= 2:
                break
            await asyncio.sleep(0.01)
    finally:
        worker.cancel()
    assert any("工商银行" in r for r in replies)


async def test_worker_reports_failure_honestly(monkeypatch: pytest.MonkeyPatch):
    d, replies = _dispatcher(monkeypatch)

    async def fake_run(task_prompt: str, readonly_key: str | None, api_base: str) -> str:
        raise RuntimeError("kimi 跑完但没写回复文件(无任何输出)")

    monkeypatch.setattr(disp, "run_kimi_assistant_task", fake_run)
    await d._handle(_msg("/持仓"))
    worker = asyncio.get_running_loop().create_task(d._worker())
    try:
        for _ in range(100):
            if len(replies) >= 2:
                break
            await asyncio.sleep(0.01)
    finally:
        worker.cancel()
    assert any("没查成" in r for r in replies)
