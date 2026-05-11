# === MODULE PURPOSE ===
# logging.Handler that forwards ERROR / CRITICAL records to Feishu so any
# silent failure surfaces as an alert. Previously each component had to
# manually call `notify_feishu(...)` in its except blocks — easy to forget
# (which is how tsanghi 余额不足 stayed invisible for days). This handler
# wires alerting to the standard logging facility instead, so any
# `logger.error(...)` automatically alerts.
#
# Throttling:
#   - Same (module, lineno, level) message deduped within 5 minutes
#   - Global cap of N records per minute (sliding window) to prevent storms
#   - Records from feishu_bot itself ignored to break recursion
#
# Threading:
#   - Sending runs in a daemon background thread with its own asyncio loop
#   - emit() is non-blocking: drops records via queue.Full if backlogged
#   - Never raises into the caller (logging handlers must not crash code)
from __future__ import annotations

import asyncio
import collections
import logging
import queue
import threading
import time
from collections import deque

_DEFAULT_DEDUPE_WINDOW_SEC = 300  # same message: 5 min between sends
_DEFAULT_MAX_PER_MINUTE = 10  # global cap to prevent log storms
_QUEUE_MAX = 200

# Loggers that must never trigger feishu alerts (recursion / noise prevention).
_BLOCKED_LOGGER_PREFIXES = (
    "src.common.feishu_bot",
    "src.common.feishu_log_handler",
    "httpx",  # FeishuBot uses httpx; its errors come back via FeishuBot anyway
    "httpcore",
    "asyncio",  # CancelledError noise during shutdown
)


class FeishuLogHandler(logging.Handler):
    """Forward ERROR/CRITICAL log records to Feishu with throttling."""

    def __init__(
        self,
        level: int = logging.ERROR,
        dedupe_window_sec: int = _DEFAULT_DEDUPE_WINDOW_SEC,
        max_per_minute: int = _DEFAULT_MAX_PER_MINUTE,
    ) -> None:
        super().__init__(level)
        self._dedupe_window = dedupe_window_sec
        self._max_per_minute = max_per_minute
        self._last_sent: dict[str, float] = {}
        self._send_timestamps: deque[float] = collections.deque()
        self._lock = threading.Lock()
        self._queue: queue.Queue[str | None] = queue.Queue(maxsize=_QUEUE_MAX)
        self._worker = threading.Thread(
            target=self._run,
            daemon=True,
            name="FeishuLogHandler",
        )
        self._worker.start()

    # ------------------------------------------------------------------
    # logging.Handler API
    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if any(record.name.startswith(p) for p in _BLOCKED_LOGGER_PREFIXES):
                return

            try:
                body = self.format(record)
            except Exception:
                return

            level_emoji = "🚨" if record.levelno >= logging.CRITICAL else "⚠️"
            message = f"{level_emoji} [{record.levelname}] {record.name}\n{body}"

            key = f"{record.module}:{record.lineno}:{record.levelname}"
            now = time.monotonic()

            with self._lock:
                last = self._last_sent.get(key)
                if last is not None and (now - last) < self._dedupe_window:
                    return

                cutoff = now - 60
                while self._send_timestamps and self._send_timestamps[0] < cutoff:
                    self._send_timestamps.popleft()
                if len(self._send_timestamps) >= self._max_per_minute:
                    return

                self._last_sent[key] = now
                self._send_timestamps.append(now)

            try:
                self._queue.put_nowait(message)
            except queue.Full:
                pass  # drop, can't do anything sensible from inside a log handler
        except Exception:
            # Logging handlers must never raise into the caller.
            pass

    def close(self) -> None:
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        super().close()

    # ------------------------------------------------------------------
    # Background worker
    # ------------------------------------------------------------------

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while True:
                msg = self._queue.get()
                if msg is None:
                    break
                try:
                    loop.run_until_complete(self._send(msg))
                except Exception:
                    pass
        finally:
            try:
                loop.close()
            except Exception:
                pass

    async def _send(self, message: str) -> None:
        # Import lazily so a misconfigured FeishuBot can't crash module import.
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if not bot.is_configured():
            return
        await bot.send_message(message)


def install_root_handler(
    level: int = logging.ERROR,
    dedupe_window_sec: int = _DEFAULT_DEDUPE_WINDOW_SEC,
    max_per_minute: int = _DEFAULT_MAX_PER_MINUTE,
) -> FeishuLogHandler | None:
    """Attach a FeishuLogHandler to the root logger (idempotent).

    Returns the handler instance, or None if one was already installed.
    Safe to call multiple times — pytest, reloads, etc.
    """
    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, FeishuLogHandler):
            return None

    handler = FeishuLogHandler(
        level=level,
        dedupe_window_sec=dedupe_window_sec,
        max_per_minute=max_per_minute,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(handler)
    return handler
