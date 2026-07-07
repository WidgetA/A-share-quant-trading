# === MODULE PURPOSE ===
# AST-001 M1 — Feishu assistant dispatcher: receive group @bot messages over the
# official long-connection (WebSocket, no public callback URL), route slash
# commands to kimi skills, reply with the result.
#
# Threading model (CRITICAL — do not "simplify"):
#   lark_oapi's ws client captures an asyncio loop at MODULE IMPORT time and
#   client.start() drives that loop with run_until_complete (blocking forever).
#   Importing lark_oapi from the running uvicorn loop would bind the ws client
#   to uvicorn's loop and crash on start. Therefore:
#     - the ENTIRE lark_oapi first-import + ws client lifecycle lives in a
#       dedicated daemon thread which sets up its own event loop FIRST;
#     - events hop to the main uvicorn loop via run_coroutine_threadsafe
#       (main loop captured on start(), same lesson as the baostock thread);
#     - outbound replies use the sync REST client inside asyncio.to_thread —
#       the sync client never touches the ws module's loop, so it is safe from
#       the main-loop side once the ws thread did the first import.
#
# Safety posture:
#   - open_id whitelist; non-whitelisted messages are logged and ignored
#   - event_id dedup (Feishu re-pushes events not acked within 3s)
#   - bounded queue; when full the user is told honestly instead of silent drop
#   - kimi runs serialized process-wide (KIMI_GLOBAL_LOCK, in kimi_runner)

import asyncio
import json
import logging
import re
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from src.assistant.kimi_runner import SKILLS_DIR, run_kimi_assistant_task

logger = logging.getLogger(__name__)

_INSTRUCTIONS_PATH = Path(__file__).parent / "assistant_instructions.md"

# Slash command → (skill name, one-line ack). The skill must exist under
# kimi-skills/ (CI-validated); the dispatcher pins it explicitly in the task
# prompt so routing never depends on the model guessing.
SLASH_COMMANDS: dict[str, dict[str, str]] = {
    "/持仓": {
        "skill": "check-holdings",
        "ack": "收到,正在查询当前持仓,大约要一两分钟,查到就回在这里。",
    },
}

_HELP_TEXT = "目前支持的命令:" + "、".join(SLASH_COMMANDS) + "。在群里 @我 后发命令即可。"

# Mention placeholders in text content look like "@_user_1" (the SDK keeps the
# display name in message.mentions, the body carries the placeholder).
_MENTION_RE = re.compile(r"@_user_\d+")

_MAX_REPLY_CHARS = 4000  # keep replies chat-sized; full text is in the log


@dataclass(frozen=True)
class IncomingMessage:
    """The subset of a Feishu message event the dispatcher needs (plain data,
    extracted on the ws thread so the main loop never touches SDK objects)."""

    event_id: str
    open_id: str
    message_id: str
    chat_type: str
    message_type: str
    content: str  # raw JSON string, e.g. '{"text": "@_user_1 /持仓"}'


def extract_text(message_type: str, content: str) -> str | None:
    """Return the human text of a message, or None when it isn't plain text."""
    if message_type != "text":
        return None
    try:
        text = json.loads(content).get("text", "")
    except (json.JSONDecodeError, AttributeError):
        return None
    return _MENTION_RE.sub("", text).strip()


def route(text: str) -> tuple[str, str]:
    """Classify a message text → ("slash", command) | ("unknown_slash", word) |
    ("free", text) | ("empty", "")."""
    if not text:
        return ("empty", "")
    first = text.split()[0]
    if first in SLASH_COMMANDS:
        return ("slash", first)
    if first.startswith("/"):
        return ("unknown_slash", first)
    return ("free", text)


def build_task_prompt(command: str) -> str:
    """Task prompt for one slash command: task book + pinned skill."""
    instructions = _INSTRUCTIONS_PATH.read_text(encoding="utf-8")
    skill = SLASH_COMMANDS[command]["skill"]
    return (
        f"{instructions}\n\n"
        f"## 本次任务\n\n"
        f"用户命令:{command}\n"
        f"指定执行的技能:{skill}(已在你的技能列表里,技能文件在 "
        f"{SKILLS_DIR / skill / 'SKILL.md'},先读它,严格照做)。\n"
    )


class AssistantDispatcher:
    """Owns the ws thread, the dedup window, the job queue and the worker."""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        allowed_users: frozenset[str],
        readonly_key: str | None,
        api_base: str,
        queue_size: int = 4,
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._allowed_users = allowed_users
        self._readonly_key = readonly_key
        self._api_base = api_base
        self._main_loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[IncomingMessage] = asyncio.Queue(maxsize=queue_size)
        self._worker_task: asyncio.Task | None = None
        self._ws_thread: threading.Thread | None = None
        self._seen_ids: set[str] = set()
        self._seen_order: deque[str] = deque(maxlen=512)
        self._api_client = None  # lazy lark.Client, built inside to_thread
        self._api_client_lock = threading.Lock()

    # ── lifecycle (main loop) ────────────────────────────────────────────

    def start(self) -> None:
        """Called from FastAPI startup (inside the running uvicorn loop)."""
        self._main_loop = asyncio.get_running_loop()
        self._worker_task = self._main_loop.create_task(self._worker())
        self._ws_thread = threading.Thread(
            target=self._ws_thread_main, name="feishu-assistant-ws", daemon=True
        )
        self._ws_thread.start()
        logger.info("Feishu assistant dispatcher started (ws thread + worker)")

    def stop(self) -> None:
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
        # The ws thread is a daemon blocked in the SDK's run_until_complete —
        # the SDK offers no stop API; it dies with the process. Acceptable for
        # a container whose lifecycle IS the process lifecycle.

    # ── ws thread side ───────────────────────────────────────────────────

    def _ws_thread_main(self) -> None:
        # Own loop FIRST, then first-ever lark_oapi import — the ws module
        # binds to whatever loop is current at import time (see module header).
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            import lark_oapi as lark

            handler = (
                lark.EventDispatcherHandler.builder("", "")
                .register_p2_im_message_receive_v1(self._on_event)
                .build()
            )
            client = lark.ws.Client(
                self._app_id,
                self._app_secret,
                event_handler=handler,
                log_level=lark.LogLevel.INFO,
                auto_reconnect=True,
            )
            client.start()  # blocks forever; SDK auto-reconnects
        except Exception:
            logger.exception("Feishu assistant ws thread died — assistant is OFFLINE")
            if self._main_loop is not None:
                asyncio.run_coroutine_threadsafe(
                    _alert_feishu("[飞书助手] 长连接线程挂了,助手已下线,需要重启服务恢复。"),
                    self._main_loop,
                )

    def _on_event(self, event) -> None:
        """SDK handler — runs on the ws thread; must return fast (3s ack rule).
        Extract plain data, hand off to the main loop, never block here."""
        try:
            msg = IncomingMessage(
                event_id=(event.header.event_id or "") if event.header else "",
                open_id=(
                    event.event.sender.sender_id.open_id or ""
                    if event.event and event.event.sender and event.event.sender.sender_id
                    else ""
                ),
                message_id=event.event.message.message_id or "",
                chat_type=event.event.message.chat_type or "",
                message_type=event.event.message.message_type or "",
                content=event.event.message.content or "",
            )
        except AttributeError:
            logger.warning("Feishu event missing expected fields, ignored")
            return
        if self._main_loop is not None:
            asyncio.run_coroutine_threadsafe(self._handle(msg), self._main_loop)

    # ── main loop side ───────────────────────────────────────────────────

    def _is_duplicate(self, event_id: str) -> bool:
        if not event_id or event_id in self._seen_ids:
            return True
        if len(self._seen_order) == self._seen_order.maxlen:
            self._seen_ids.discard(self._seen_order[0])
        self._seen_order.append(event_id)
        self._seen_ids.add(event_id)
        return False

    async def _handle(self, msg: IncomingMessage) -> None:
        if self._is_duplicate(msg.event_id):
            return
        if msg.open_id not in self._allowed_users:
            # Holdings are sensitive — silently ignore, but leave an audit line
            # (also how the operator harvests open_ids to whitelist).
            logger.warning(
                "Feishu assistant: message from non-whitelisted open_id=%s ignored",
                msg.open_id,
            )
            return

        text = extract_text(msg.message_type, msg.content)
        if text is None:
            await self._reply(msg.message_id, "目前只认识文字消息。" + _HELP_TEXT)
            return

        kind, value = route(text)
        if kind == "empty":
            await self._reply(msg.message_id, _HELP_TEXT)
        elif kind == "unknown_slash":
            await self._reply(msg.message_id, f"不认识 {value} 这个命令。{_HELP_TEXT}")
        elif kind == "free":
            await self._reply(
                msg.message_id, "自由问答还没开通,现在只会执行固定命令。" + _HELP_TEXT
            )
        else:  # slash
            try:
                self._queue.put_nowait(msg)
            except asyncio.QueueFull:
                await self._reply(msg.message_id, "排队满了(前面还有任务在跑),请过几分钟再试。")
                return
            await self._reply(msg.message_id, SLASH_COMMANDS[value]["ack"])

    async def _worker(self) -> None:
        while True:
            msg = await self._queue.get()
            text = extract_text(msg.message_type, msg.content) or ""
            command = route(text)[1]
            try:
                reply = await run_kimi_assistant_task(
                    build_task_prompt(command),
                    readonly_key=self._readonly_key,
                    api_base=self._api_base,
                )
                if len(reply) > _MAX_REPLY_CHARS:
                    logger.info("Assistant reply truncated (%d chars); full: %s", len(reply), reply)
                    reply = reply[:_MAX_REPLY_CHARS] + "\n…(太长截断了,完整内容在服务日志里)"
                await self._reply(msg.message_id, reply)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Honest failure report, plain Chinese — never a made-up answer.
                logger.exception("Assistant task failed for %s", command)
                await self._reply(msg.message_id, f"这次没查成:{exc}")

    # ── outbound replies ─────────────────────────────────────────────────

    async def _reply(self, message_id: str, text: str) -> None:
        try:
            await asyncio.to_thread(self._reply_sync, message_id, text)
        except Exception:
            # A failed reply must never kill the worker; the task result is
            # already in the log.
            logger.exception("Feishu assistant: reply to %s failed", message_id)

    def _reply_sync(self, message_id: str, text: str) -> None:
        # Safe to import here: the ws thread performed the first import, and
        # the sync REST client does not touch the ws module's captured loop.
        import lark_oapi as lark
        from lark_oapi.api.im.v1 import (
            ReplyMessageRequest,
            ReplyMessageRequestBody,
        )

        with self._api_client_lock:
            if self._api_client is None:
                self._api_client = (
                    lark.Client.builder().app_id(self._app_id).app_secret(self._app_secret).build()
                )
        request = (
            ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                ReplyMessageRequestBody.builder()
                .content(json.dumps({"text": text}, ensure_ascii=False))
                .msg_type("text")
                .build()
            )
            .build()
        )
        response = self._api_client.im.v1.message.reply(request)
        if not response.success():
            raise RuntimeError(f"飞书回复接口失败 code={response.code} msg={response.msg}")


async def _alert_feishu(message: str) -> None:
    """One-shot ops alert through the existing relay bot (best-effort)."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_alert("飞书助手", message)
    except Exception:
        logger.exception("Assistant ops alert failed")
