"""Own replacement V20 generations outside their fail-closed scheduler group."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class V20RuntimeSupervisor:
    def __init__(
        self,
        service: Any,
        factory: Callable[[], Any],
        install: Callable[[Any, bool], None],
        *,
        retry_seconds: float = 5.0,
        max_retry_seconds: float = 60.0,
        alert_timeout: float = 25.0,
    ) -> None:
        self.service = service
        self._factory = factory
        self._install = install
        self._alerts = service.runtime_alerts()
        self._retry_seconds = retry_seconds
        self._max_retry_seconds = max_retry_seconds
        self._alert_timeout = alert_timeout
        self._notices: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._tasks: list[asyncio.Task[None]] = []
        self._closing = False
        self.recovering = False
        self.attempts = 0
        self.last_error: str | None = None
        self.incident_id: str | None = None
        self.alert_error: str | None = None
        self.alert_delivered_at: str | None = None

    def start(self) -> None:
        if self._tasks:
            return
        self._tasks = [
            asyncio.create_task(self._monitor(), name="v20-runtime-supervisor"),
            asyncio.create_task(self._deliver_alerts(), name="v20-operational-alerts"),
        ]

    def status(self) -> dict[str, Any]:
        return {
            "running": bool(self._tasks) and all(not task.done() for task in self._tasks),
            "recovering": self.recovering,
            "attempts": self.attempts,
            "last_error_code": self.last_error,
            "incident_id": self.incident_id,
            "alert_error_code": self.alert_error,
            "alert_delivered_at": self.alert_delivered_at,
            "pending_alerts": self._notices.qsize(),
        }

    def _notice(self, text: str) -> None:
        self._notices.put_nowait((str(uuid4()), text))

    async def _deliver_alerts(self) -> None:
        while True:
            notification_id, text = await self._notices.get()
            delay = self._retry_seconds
            while True:
                try:
                    async with asyncio.timeout(self._alert_timeout):
                        await self._alerts.send(text, notification_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Only type names: a transport exception can contain credentials.
                    self.alert_error = type(exc).__name__
                    logger.error("V20 operational alert delivery failed: %s", self.alert_error)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._max_retry_seconds)
                else:
                    self.alert_error = None
                    self.alert_delivered_at = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()
                    self._notices.task_done()
                    break

    async def _monitor(self) -> None:
        while not self._closing:
            reason = await self.service.wait_runtime_failure()
            self.recovering = True
            self.incident_id = str(uuid4())
            self.last_error = reason
            self._install(self.service, False)
            self._notice(
                f"V20 运行中断\n时间：{datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()}"
                f"\n故障编号：{self.incident_id}\n故障类型：{reason}"
                "\n选股、退出监控及策略推送已暂停，正在清理并自动重连。"
            )
            delay = self._retry_seconds
            while not self._closing:
                self.attempts += 1
                try:
                    # No new generation until old tasks and private resources settle.
                    await self.service.stop()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = f"CLEANUP_FAILED:{type(exc).__name__}"
                    logger.exception("V20 recovery cleanup failed; replacement is blocked")
                    self._notice(
                        f"V20 自动恢复受阻\n故障编号：{self.incident_id}"
                        "\n旧运行资源未能完成清理，需人工检查；未启动第二个实例。"
                    )
                    # A repeated stop can hide an incomplete cleanup. Fail closed
                    # instead of claiming the old generation was safely released.
                    await asyncio.Event().wait()
                await asyncio.sleep(delay)
                replacement = None
                try:
                    replacement = self._factory()
                    self.service = replacement
                    self._install(replacement, False)
                    await replacement.start()
                    await replacement.assert_runtime_ready()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = f"RESTART_FAILED:{type(exc).__name__}"
                    logger.warning("V20 runtime recovery attempt failed: %s", self.last_error)
                    delay = min(delay * 2, self._max_retry_seconds)
                    continue
                self._install(replacement, True)
                self.recovering = False
                self.last_error = None
                self._notice(
                    f"V20 运行已恢复\n故障编号：{self.incident_id}"
                    "\n数据库运行锁已重新取得，调度任务已启动。"
                    "\n行情完整性继续独立检查，已过期的开仓建议不会补成有效建议。"
                )
                break

    async def stop(self) -> None:
        self._closing = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        await self.service.stop()


def attach_v20_supervisor(app: Any, service: Any, factory: Callable[[], Any]) -> None:
    """Attach only to services implementing the explicit supervision contract."""
    if not callable(getattr(service, "wait_runtime_failure", None)):
        return
    existing = getattr(app.state, "v20_supervisor", None)
    if existing is not None:
        return

    def install(replacement: Any, started: bool) -> None:
        app.state.v20_service = replacement
        app.state.v20_service_started = started

    supervisor = V20RuntimeSupervisor(service, factory, install)
    app.state.v20_supervisor = supervisor
    supervisor.start()
