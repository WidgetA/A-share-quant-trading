from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from src.web.v20_runtime_supervisor import V20RuntimeSupervisor, attach_v20_supervisor


class Alerts:
    def __init__(self):
        self.calls = []
        self.fail = False

    async def send(self, text, notification_id):
        self.calls.append((text, notification_id))
        if self.fail:
            raise ConnectionRefusedError("secret must not be exposed")


class Runtime:
    def __init__(self, alerts, *, start_error=False, cleanup_error=False):
        self.alerts = alerts
        self.failure = asyncio.Event()
        self.started = False
        self.stopped = False
        self.stop_calls = 0
        self.start_error = start_error
        self.cleanup_error = cleanup_error
        self.leader_checked = False

    def runtime_alerts(self):
        return self.alerts

    async def wait_runtime_failure(self):
        await self.failure.wait()
        return "v20-outbox-publisher:V20LeadershipLost"

    async def start(self):
        if self.start_error:
            raise ConnectionRefusedError("database still down")
        self.started = True

    async def assert_runtime_ready(self):
        assert self.started
        self.leader_checked = True

    async def stop(self):
        self.stop_calls += 1
        if self.cleanup_error:
            raise RuntimeError("cannot release old generation")
        self.stopped = True


async def eventually(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.001)


async def test_outage_alert_survives_dead_database_and_recovery_uses_new_generation():
    alerts = Alerts()
    original = Runtime(alerts)
    attempts = []
    installed = []
    v16_state = SimpleNamespace(running=True, pool=object(), cache={"live": "unchanged"})
    before = vars(v16_state).copy()

    def factory():
        assert original.stopped
        replacement = Runtime(alerts, start_error=len(attempts) < 2)
        attempts.append(replacement)
        return replacement

    supervisor = V20RuntimeSupervisor(
        original,
        factory,
        lambda service, started: installed.append((service, started)),
        retry_seconds=0.001,
        max_retry_seconds=0.002,
    )
    supervisor.start()
    try:
        original.failure.set()
        await eventually(lambda: len(alerts.calls) >= 2 and not supervisor.recovering)
        assert len(attempts) == 3
        assert all(item.stopped for item in attempts[:-1])
        assert attempts[-1].leader_checked
        assert installed[-1] == (attempts[-1], True)
        assert "运行中断" in alerts.calls[0][0]
        assert "运行已恢复" in alerts.calls[1][0]
        assert supervisor.incident_id in alerts.calls[0][0]
        assert supervisor.incident_id in alerts.calls[1][0]
        assert vars(v16_state) == before
    finally:
        await supervisor.stop()
    assert attempts[-1].stopped
    assert vars(v16_state) == before


async def test_unavailable_feishu_does_not_block_recovery_and_retries_same_uuid():
    alerts = Alerts()
    alerts.fail = True
    original = Runtime(alerts)
    replacement = Runtime(alerts)
    supervisor = V20RuntimeSupervisor(
        original,
        lambda: replacement,
        lambda *_: None,
        retry_seconds=0.001,
        max_retry_seconds=0.003,
    )
    supervisor.start()
    try:
        original.failure.set()
        await eventually(lambda: replacement.leader_checked and len(alerts.calls) >= 2)
        assert not supervisor.recovering
        assert supervisor.alert_error == "ConnectionRefusedError"
        assert "secret" not in str(supervisor.status())
        assert alerts.calls[0][1] == alerts.calls[1][1]
        alerts.fail = False
        await eventually(lambda: supervisor.alert_delivered_at is not None)
        assert supervisor.alert_error is None
    finally:
        await supervisor.stop()


async def test_shutdown_during_backoff_never_creates_replacement():
    alerts = Alerts()
    original = Runtime(alerts)
    factory_calls = []
    supervisor = V20RuntimeSupervisor(
        original,
        lambda: factory_calls.append(True),
        lambda *_: None,
        retry_seconds=60,
    )
    supervisor.start()
    original.failure.set()
    await eventually(lambda: original.stopped)
    await supervisor.stop()
    assert factory_calls == []


async def test_shutdown_of_healthy_runtime_has_no_incident_or_restart():
    alerts = Alerts()
    original = Runtime(alerts)
    supervisor = V20RuntimeSupervisor(original, lambda: pytest.fail("restart"), lambda *_: None)
    supervisor.start()
    await supervisor.stop()
    assert original.stopped
    assert alerts.calls == []


async def test_failed_cleanup_blocks_second_runtime_and_reports_it():
    alerts = Alerts()
    original = Runtime(alerts, cleanup_error=True)
    supervisor = V20RuntimeSupervisor(
        original,
        lambda: pytest.fail("must not overlap generations"),
        lambda *_: None,
        retry_seconds=0.001,
    )
    supervisor.start()
    original.failure.set()
    await eventually(lambda: len(alerts.calls) == 2)
    assert supervisor.last_error == "CLEANUP_FAILED:RuntimeError"
    assert supervisor.recovering
    original.cleanup_error = False
    await supervisor.stop()


async def test_host_installs_new_service_for_routes_without_mutating_v16():
    alerts = Alerts()
    original = Runtime(alerts)
    replacement = Runtime(alerts)
    app = SimpleNamespace(state=SimpleNamespace(v20_service=original, v16=object()))
    v16 = app.state.v16
    attach_v20_supervisor(app, original, lambda: replacement)
    supervisor = app.state.v20_supervisor
    supervisor._retry_seconds = 0.001
    attach_v20_supervisor(app, original, lambda: pytest.fail("duplicate supervisor"))
    assert app.state.v20_supervisor is supervisor
    try:
        original.failure.set()
        await eventually(lambda: getattr(app.state, "v20_service_started", False))
        assert app.state.v20_service is replacement
        assert app.state.v16 is v16
    finally:
        await supervisor.stop()


async def test_shutdown_during_replacement_start_cleans_partial_generation():
    alerts = Alerts()
    original = Runtime(alerts)
    starting = asyncio.Event()
    replacement = Runtime(alerts)

    async def blocked_start():
        starting.set()
        await asyncio.Event().wait()

    replacement.start = blocked_start
    supervisor = V20RuntimeSupervisor(
        original,
        lambda: replacement,
        lambda *_: None,
        retry_seconds=0.001,
    )
    supervisor.start()
    original.failure.set()
    await asyncio.wait_for(starting.wait(), 2)
    await supervisor.stop()
    assert replacement.stopped
