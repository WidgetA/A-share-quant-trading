"""Singleton background download task state management.

The ActiveDownload object lives in ``app.state.active_download`` and keeps
strong references to both asyncio Tasks so they cannot be garbage-collected.
The SSE stream is a *subscriber* to the task — disconnecting from the stream
has no effect on the running download.
"""
from __future__ import annotations

import asyncio
import enum
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

_BEIJING_TZ = ZoneInfo("Asia/Shanghai")

LOG_BUFFER_SIZE = 300  # messages kept for SSE reconnect replay


class DownloadState(enum.Enum):
    RUNNING = "running"
    FAILED = "failed"


@dataclass
class ActiveDownload:
    """All state for the singleton background download task.

    ``asyncio_task`` and ``watchdog_task`` are stored here (strong references)
    so they survive browser disconnects — the event loop cannot GC them while
    this object is reachable from ``app.state``.
    """

    state: DownloadState
    asyncio_task: asyncio.Task          # strong ref — prevents GC
    watchdog_task: asyncio.Task         # strong ref
    cancel_event: threading.Event
    log_buffer: deque                   # last LOG_BUFFER_SIZE msgs for reconnect
    last_event_at: list                 # [float] monotonic ts of last progress event
    start_date: str = ""
    end_date: str = ""
    error_msg: str | None = None
    started_at: datetime | None = None
    _subscribers: set = field(default_factory=set)

    def broadcast(self, msg: dict) -> None:
        """Append *msg* to log_buffer and fan-out to all live SSE subscribers."""
        self.log_buffer.append(msg)
        self.last_event_at[0] = time.monotonic()
        for q in list(self._subscribers):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                # Slow subscriber — drop; replay from log_buffer on reconnect.
                pass

    def subscribe(self) -> asyncio.Queue:
        """Create and register a per-connection subscriber queue."""
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)
