from __future__ import annotations

import asyncio
import hashlib
import json
import pickle
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import pytest

import src.web.v15_scan_service as v15_scan_service
import src.web.v20_service as service_module
from src.common.v20_feishu import V20FeishuRoute
from src.data.clients.mews_snapshot import MewsSnapshotSourceError
from src.data.clients.tushare_realtime import (
    TushareDailyBar,
    TushareMinuteBar,
    TushareRealtimeClient,
    tushare_minute_bars_to_early_market_data,
)
from src.data.database.fundamentals_db import FundamentalsDBConfig
from src.data.database.v20_repository import (
    ActiveModelLeg,
    EntryStatus,
    ManualMonitorEnrollmentRecord,
    MinuteBarRecord,
    OutboxRecord,
    StateRecord,
    V20DatabaseConfig,
    V20LeadershipLost,
    V20MinuteBarIntegrityConflict,
    V20RepositoryError,
    V20SemanticConflict,
    V20StateConflict,
    sha256_json,
)
from src.strategy.lgbrank_scorer import ScoredStock
from src.strategy.strategies.v16_scanner import V16ScanResult
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import genesis_state
from src.strategy.v20.identity import named_hash
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_EXIT_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
    V20_V16_SNAPSHOT_SCHEMA,
)
from src.strategy.v20.runtime_config import (
    V20ConfigError,
    V20RouteBinding,
    load_v20_runtime_config,
)
from src.web.v15_scan_service import (
    CanonicalV16ScanBundle,
    V15ScanState,
    _bundle_fingerprint,
    _initialize_scan_resources_once,
    cleanup_scan_resources,
)
from src.web.v20_service import (
    FULL_EXIT_LABELS,
    V20LiveExitStageTimeout,
    V20Service,
    _bar_payload,
    _bootstrap_bundle,
    _cleanup_embedded_v20_scan_resources,
    _cleanup_v20_scan_resources,
    _DayContext,
    _embedded_runtime_config,
    _init_embedded_v20_scan_resources,
    _init_owned_embedded_v20_scan_resources,
    _init_v20_scan_resources,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TZ = ZoneInfo("Asia/Shanghai")


class _UnusedMewsSource:
    async def fetch_snapshot(self, **_kwargs):
        raise AssertionError("test did not expect a MEWS fetch")


def _config(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.setenv("V20_MODE", "forward_shadow")
    monkeypatch.setenv("DB_SSLROOTCERT_SHA256", "c" * 64)
    monkeypatch.setenv("V20_INGEST_API_KEY", "i" * 32)
    monkeypatch.setenv("V20_STATUS_API_KEY", "s" * 32)
    monkeypatch.delenv("V20_ALLOW_PRODUCTION_PUSH", raising=False)
    config = load_v20_runtime_config(PROJECT_ROOT)
    binding = V20RouteBinding(
        route_id=config.route_id,
        expected_bot_origin="https://relay.internal",
        expected_app_id_sha256=hashlib.sha256(b"shadow-app").hexdigest(),
        expected_chat_id_sha256=hashlib.sha256(b"shadow-chat").hexdigest(),
    )
    return replace(
        config,
        enabled=True,
        route_binding=binding,
        route_bindings={**config.route_bindings, "forward_shadow": binding},
        v20_db_ca_sha256="d" * 64,
        fundamentals_db_ca_sha256="c" * 64,
    )


def _service(monkeypatch: pytest.MonkeyPatch, repository: Any, client: Any = None) -> V20Service:
    config = _config(monkeypatch)
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    scan_state = V15ScanState(initialized=True, realtime_client=client)
    return V20Service(
        config=config,
        repository=repository,
        scan_state=scan_state,
        artifacts=artifacts,
        publisher=SimpleNamespace(),
        routes={},
        mews_source=_UnusedMewsSource(),
    )


async def test_0910_mews_refresh_caches_once_and_opening_paths_use_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        def __init__(self) -> None:
            self.payloads = []
            self.eligibility_checks = []

        async def assert_runtime_leader(self):
            return None

        async def find_eligible_mews_snapshot(self, **_kwargs):
            return None

        async def record_mews_snapshot(self, payload):
            self.payloads.append(dict(payload))
            return "a" * 64

        async def mews_snapshot_is_eligible(
            self,
            snapshot_id,
            *,
            source_trade_date,
            cutoff,
        ):
            self.eligibility_checks.append((snapshot_id, source_trade_date, cutoff))
            return True

    class _Source:
        def __init__(self) -> None:
            self.calls = []

        async def fetch_snapshot(self, *, source_trade_date, availability_date):
            self.calls.append((source_trade_date, availability_date))
            return {
                "snapshot_id": "mews-v2-2026-08-31-deadbeef",
                "source_trade_date": "2026-08-31",
                "generated_at": "2026-09-01T09:15:00+08:00",
                "fast_state": "DANGER",
                "model_version": "mews_v2",
                "data_version": "d" * 64,
                "evidence": {"signal_available_date": "2026-09-01"},
            }

    repository = _Repository()
    source = _Source()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = source
    calendar = (
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
        date(2026, 9, 4),
    )
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=calendar),
    )
    now = datetime(2026, 9, 1, 9, 16, tzinfo=TZ)

    assert await service._refresh_mews_cache_once(now, calendar) is True
    assert await service._refresh_mews_cache_once(now, calendar) is False

    assert source.calls == [(date(2026, 8, 31), date(2026, 9, 1))]
    assert len(repository.payloads) == 1
    assert repository.payloads[0]["fast_state"] == "DANGER"
    assert repository.eligibility_checks[0][1] == date(2026, 8, 31)
    assert repository.eligibility_checks[0][2] == datetime(
        2026,
        9,
        1,
        9,
        40,
        tzinfo=TZ,
    )
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_snapshot_id == "mews-v2-2026-08-31-deadbeef"


async def test_mews_missing_at_runtime_is_calculated_then_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        def __init__(self) -> None:
            self.payloads = []

        async def assert_runtime_leader(self):
            return None

        async def find_eligible_mews_snapshot(self, **_kwargs):
            return None

        async def record_mews_snapshot(self, payload):
            self.payloads.append(dict(payload))

        async def mews_snapshot_is_eligible(self, *_args, **_kwargs):
            return True

    class _LocalCalculator:
        def __init__(self) -> None:
            self.calls = []

        async def fetch_snapshot(self, *, source_trade_date, availability_date):
            self.calls.append((source_trade_date, availability_date))
            return {
                "snapshot_id": "mews-v2-2026-08-31-local",
                "source_trade_date": "2026-08-31",
                "generated_at": "2026-09-01T09:18:00+08:00",
                "fast_state": "NORMAL",
                "model_version": "mews_v2",
                "data_version": "d" * 64,
                "evidence": {"profile": "LOCAL_TUSHARE_MEWS_V2_0910_V1"},
            }

    repository = _Repository()
    source = _LocalCalculator()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = source
    calendar = (date(2026, 8, 31), date(2026, 9, 1))
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=calendar),
    )

    cached = await service._refresh_mews_cache_once(
        datetime(2026, 9, 1, 9, 18, tzinfo=TZ),
        calendar,
    )

    assert cached is True
    assert source.calls == [(date(2026, 8, 31), date(2026, 9, 1))]
    assert repository.payloads[0]["source_trade_date"] == "2026-08-31"


async def test_mews_local_calculation_failure_is_not_written_as_a_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            raise MewsSnapshotSourceError("Tushare margin is missing SSE or SZSE")

    class _Repository:
        def __init__(self) -> None:
            self.payloads = []

        async def assert_runtime_leader(self):
            return None

        async def find_eligible_mews_snapshot(self, **_kwargs):
            return None

        async def record_mews_snapshot(self, payload):
            self.payloads.append(dict(payload))

    repository = _Repository()
    source = _Source()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = source
    alerts = _alert_recorder(monkeypatch, service)
    calendar = (date(2026, 8, 31), date(2026, 9, 1))
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=calendar),
    )

    # A genuine calculation failure settles one daily idempotent alert and the
    # scheduled refresh returns False instead of raising; nothing is written.
    assert (
        await service._refresh_mews_cache_once(
            datetime(2026, 9, 1, 9, 18, tzinfo=TZ),
            calendar,
        )
        is False
    )
    assert repository.payloads == []
    assert source.calls == 1
    assert len(alerts) == 1
    assert alerts[0]["code"] == "MEWS_CALCULATION_FAILED"
    assert "SCHEDULED_0910" in alerts[0]["message"]

    # The finished task was cleared, so the next in-window tick retries the
    # attempt without doubling the daily alert.
    assert service._mews_singleflight_task is None
    assert (
        await service._refresh_mews_cache_once(
            datetime(2026, 9, 1, 9, 19, tzinfo=TZ),
            calendar,
        )
        is False
    )
    assert source.calls == 2
    assert len(alerts) == 1


async def test_mews_refresh_never_calls_upstream_outside_0910_to_0940(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        async def fetch_snapshot(self, **_kwargs):
            raise AssertionError("MEWS must not be pulled outside its cache window")

    service = _service(monkeypatch, SimpleNamespace())
    service._mews_source = _Source()
    calendar = (
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
    )

    assert (
        await service._refresh_mews_cache_once(
            datetime(2026, 9, 1, 9, 9, 59, tzinfo=TZ),
            calendar,
        )
        is False
    )
    assert (
        await service._refresh_mews_cache_once(
            datetime(2026, 9, 1, 9, 40, tzinfo=TZ),
            calendar,
        )
        is False
    )


async def test_selection_trigger_calculates_missing_mews_after_cutoff_and_clears_lane_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        def __init__(self) -> None:
            self.payloads = []
            self.leader_calls = 0

        async def assert_runtime_leader(self):
            self.leader_calls += 1

        async def find_eligible_mews_snapshot(self, **_kwargs):
            return None

        async def record_mews_snapshot(self, payload):
            self.payloads.append(dict(payload))

        async def mews_snapshot_is_eligible(self, *_args, **_kwargs):
            return False

    class _Source:
        def __init__(self) -> None:
            self.calls = []

        async def fetch_snapshot(self, *, source_trade_date, availability_date):
            self.calls.append((source_trade_date, availability_date))
            return {
                "snapshot_id": "mews-v2-2026-08-31-trigger",
                "source_trade_date": "2026-08-31",
                "generated_at": "2026-09-01T13:30:00+08:00",
                "fast_state": "NORMAL",
                "model_version": "mews_v2",
                "data_version": "d" * 64,
                "evidence": {"profile": "LOCAL_TUSHARE_MEWS_V2_0910_V1"},
            }

    now = datetime(2026, 9, 1, 13, 30, tzinfo=TZ)
    repository = _Repository()
    source = _Source()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._clock = lambda: now
    service._mews_source = source
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(
            0,
            result=(date(2026, 8, 31), date(2026, 9, 1)),
        ),
    )
    service._record_lane_error("mews_cache", "MEWS_CACHE_FAILED: missing", now)

    assert await service.ensure_mews_for_selection_trigger(now) is True
    assert service._mews_singleflight_task is None

    assert repository.leader_calls == 1
    assert source.calls == [(date(2026, 8, 31), date(2026, 9, 1))]
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_snapshot_id == "mews-v2-2026-08-31-trigger"
    assert service._lane_health["mews_cache"].last_error is None

    # A cached day never re-attempts.
    assert await service.ensure_mews_for_selection_trigger(now) is True
    assert len(source.calls) == 1


async def test_mews_cache_restart_restores_postgres_snapshot_without_refetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        async def find_eligible_mews_snapshot(
            self,
            *,
            source_trade_date,
            cutoff,
            availability_date=None,
        ):
            assert source_trade_date == date(2026, 8, 31)
            assert cutoff == datetime(2026, 9, 1, 9, 40, tzinfo=TZ)
            assert availability_date == date(2026, 9, 1)
            return "mews-v2-2026-08-31-restored"

        async def mews_snapshot_is_eligible(
            self,
            snapshot_id,
            *,
            source_trade_date,
            cutoff,
        ) -> bool:
            assert snapshot_id == "mews-v2-2026-08-31-restored"
            assert source_trade_date == date(2026, 8, 31)
            assert cutoff == datetime(2026, 9, 1, 9, 40, tzinfo=TZ)
            return True

    class _Source:
        async def fetch_snapshot(self, **_kwargs):
            raise AssertionError("a restart must reuse the sealed PostgreSQL snapshot")

    service = _service(monkeypatch, _Repository())
    service._mews_source = _Source()
    calendar = (
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
    )
    now = datetime(2026, 9, 1, 9, 25, tzinfo=TZ)

    assert await service._restore_mews_cache_once(now, calendar) is True
    assert await service._refresh_mews_cache_once(now, calendar) is False
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_source_trade_date == date(2026, 8, 31)
    assert service._mews_snapshot_id == "mews-v2-2026-08-31-restored"


class _AfterCutoffMewsRepository:
    def __init__(self, *, restored_id: str | None = None) -> None:
        self.restored_id = restored_id
        self.payloads: list[dict[str, Any]] = []
        self.find_calls: list[dict[str, Any]] = []

    async def assert_runtime_leader(self) -> None:
        return None

    async def find_eligible_mews_snapshot(self, **kwargs):
        self.find_calls.append(kwargs)
        return self.restored_id

    async def record_mews_snapshot(self, payload):
        self.payloads.append(dict(payload))
        return "a" * 64

    async def mews_snapshot_is_eligible(self, *_args, **_kwargs):
        return False

    async def close(self) -> None:
        return None


def _late_mews_payload() -> dict[str, Any]:
    return {
        "snapshot_id": "mews-v2-2026-08-31-late",
        "source_trade_date": "2026-08-31",
        "generated_at": "2026-09-01T14:04:00+08:00",
        "fast_state": "NORMAL",
        "model_version": "mews_v2",
        "data_version": "d" * 64,
        "evidence": {
            "profile": "LOCAL_TUSHARE_MEWS_V2_0910_V1",
            "signal_available_date": "2026-09-01",
        },
    }


def _alert_recorder(monkeypatch: pytest.MonkeyPatch, service: V20Service) -> list[dict]:
    alerts: list[dict] = []

    async def _record(**kwargs):
        alerts.append(kwargs)
        return True

    monkeypatch.setattr(service, "_safe_alert", _record)
    return alerts


async def test_mews_after_cutoff_first_tick_calculates_caches_and_never_alerts_missed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        def __init__(self) -> None:
            self.calls = []

        async def fetch_snapshot(self, *, source_trade_date, availability_date):
            self.calls.append((source_trade_date, availability_date))
            return _late_mews_payload()

    repository = _AfterCutoffMewsRepository()
    source = _Source()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = source
    alerts = _alert_recorder(monkeypatch, service)
    calendar = (
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
    )
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=calendar),
    )
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)

    assert await service._recover_mews_after_cutoff_once(now, calendar) is True
    assert await service._recover_mews_after_cutoff_once(now, calendar) is True

    assert source.calls == [(date(2026, 8, 31), date(2026, 9, 1))]
    assert len(repository.payloads) == 1
    assert repository.payloads[0]["generated_at"] == "2026-09-01T14:04:00+08:00"
    assert alerts == []
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_snapshot_id == "mews-v2-2026-08-31-late"
    assert service._lane_health["mews_cache"].last_error is None


async def test_concurrent_selection_triggers_calculate_missing_mews_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        def __init__(self) -> None:
            self.calls = []

        async def fetch_snapshot(self, *, source_trade_date, availability_date):
            self.calls.append((source_trade_date, availability_date))
            await asyncio.sleep(0.05)
            return _late_mews_payload()

    repository = _AfterCutoffMewsRepository()
    source = _Source()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    service._clock = lambda: now
    service._mews_source = source
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=(date(2026, 8, 31), date(2026, 9, 1))),
    )

    first, second = await asyncio.gather(
        service.ensure_mews_for_selection_trigger(now),
        service.ensure_mews_for_selection_trigger(now),
    )

    # Both triggers joined the same per-date singleflight task and awaited it:
    # exactly one overlapping raw attempt, and both see the persisted success.
    assert (first, second) == (True, True)
    assert len(source.calls) == 1
    assert len(repository.payloads) == 1
    assert service._mews_singleflight_task is None

    assert await service.ensure_mews_for_selection_trigger(now) is True
    assert len(source.calls) == 1
    assert service._mews_cached_for == date(2026, 9, 1)


async def test_after_cutoff_recovery_failure_alerts_once_and_stays_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            raise MewsSnapshotSourceError("Tushare margin is missing SSE or SZSE")

    repository = _AfterCutoffMewsRepository()
    source = _Source()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = source
    alerts = _alert_recorder(monkeypatch, service)
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=(date(2026, 8, 31), date(2026, 9, 1))),
    )
    calendar = (
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
    )
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    service._clock = lambda: now

    assert await service._recover_mews_after_cutoff_once(now, calendar) is False

    assert repository.payloads == []
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert["code"] == "MEWS_CALCULATION_FAILED"
    assert alert["entity_id"] == "2026-09-01"
    message = alert["message"]
    assert "SCHEDULED_AFTER_CUTOFF_RECOVERY" in message
    assert "MewsSnapshotSourceError" in message
    assert "2026-08-31" in message
    assert service._lane_health["mews_cache"].last_error is not None

    # Later ticks neither retry the calculator nor repeat the alert.
    assert await service._recover_mews_after_cutoff_once(now, calendar) is False
    assert source.calls == 1
    assert len(alerts) == 1

    # A selection trigger discovers the same gap, retries once, fails closed,
    # and never doubles the daily alert.  The trigger awaits the attempt, so
    # the failure is already settled when it returns.
    assert await service.ensure_mews_for_selection_trigger(now) is False
    assert source.calls == 2
    assert len(alerts) == 1
    assert service._mews_singleflight_task is None

    # No permanent daily trigger skip: a later distinct trigger retries again.
    assert await service.ensure_mews_for_selection_trigger(now) is False
    assert source.calls == 3
    assert len(alerts) == 1


async def test_restart_recovers_late_calculated_snapshot_without_recalculation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        async def fetch_snapshot(self, **_kwargs):
            raise AssertionError("a restart must reuse the sealed PostgreSQL snapshot")

    repository = _AfterCutoffMewsRepository(restored_id="mews-v2-2026-08-31-late")
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = _Source()
    alerts = _alert_recorder(monkeypatch, service)
    calendar = (date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2))

    async def not_on_time(*_args: Any, **_kwargs: Any) -> bool:
        return False

    class SealedLateGuard:
        async def is_eligible(self, *_args: Any, **_kwargs: Any) -> bool:
            return True

    repository.mews_snapshot_is_eligible = not_on_time
    repository.schema = "v20"
    repository.pool = object()
    monkeypatch.setattr(
        service_module,
        "V20MewsReceiptGuard",
        lambda _repository: SealedLateGuard(),
    )
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=calendar),
    )
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)

    assert await service._recover_mews_after_cutoff_once(now, calendar) is True

    assert repository.find_calls[0]["source_trade_date"] == date(2026, 8, 31)
    assert repository.find_calls[0]["availability_date"] == date(2026, 9, 1)
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_source_trade_date == date(2026, 8, 31)
    assert service._mews_snapshot_id == "mews-v2-2026-08-31-late"
    assert alerts == []
    assert await service._refresh_mews_cache_once(now, calendar) is False


def _set_v20_consumer_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "V20_DB_HOST": "writer.internal",
        "V20_DB_PORT": "5433",
        "V20_DB_NAME": "strategy",
        "V20_DB_USER": "v20_writer",
        "V20_DB_PASSWORD": "writer-secret",
        "V20_DB_SSLMODE": "verify-full",
        "V20_DB_SSLROOTCERT": "/certs/writer.pem",
        "V20_DB_SSLROOTCERT_SHA256": "d" * 64,
        "V20_DB_CONNECT_TIMEOUT_SECONDS": "5",
        "V20_DB_COMMAND_TIMEOUT_SECONDS": "15",
        "DB_HOST": "reader.internal",
        "DB_PORT": "5434",
        "DB_NAME": "fundamentals",
        "DB_USER": "fundamentals_reader",
        "DB_PASSWORD": "reader-secret",
        "DB_SSLMODE": "verify-full",
        "DB_SSLROOTCERT": "/certs/reader.pem",
        "DB_SSLROOTCERT_SHA256": "c" * 64,
        "DB_CONNECT_TIMEOUT_SECONDS": "6",
        "DB_COMMAND_TIMEOUT_SECONDS": "16",
        "TUSHARE_TOKEN": "environment-tushare-token",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _writer_config() -> V20DatabaseConfig:
    return V20DatabaseConfig(
        host="writer.internal",
        port=5433,
        database="strategy",
        user="v20_writer",
        password="writer-secret",
        ssl_root_cert="/certs/writer.pem",
        ssl_root_cert_sha256="d" * 64,
        connect_timeout_seconds=5,
        command_timeout_seconds=15,
    )


def _fundamentals_config() -> FundamentalsDBConfig:
    return FundamentalsDBConfig(
        host="reader.internal",
        port=5434,
        database="fundamentals",
        user="fundamentals_reader",
        password="reader-secret",
        ssl_mode="verify-full",
        ssl_root_cert="/certs/reader.pem",
        ssl_root_cert_sha256="c" * 64,
        connect_timeout_seconds=6,
        command_timeout_seconds=16,
    )


def test_embedded_runtime_config_binds_legacy_destination_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.setenv("V20_MODE", "forward_shadow")
    base = load_v20_runtime_config(PROJECT_ROOT)
    route = V20FeishuRoute(
        route_id=base.route_id,
        bot_url="https://legacy-relay.example",
        app_id="legacy-app",
        app_secret="legacy-secret",
        chat_id="legacy-chat",
        transport="legacy_send",
    )

    embedded = _embedded_runtime_config(base, route)

    assert embedded.enabled is True
    assert embedded.deployment_mode == "forward_shadow"
    assert embedded.route_binding.destination_fingerprint == route.destination_fingerprint
    assert embedded.config_hash == sha256_json(embedded.frozen_payload)
    serialized = json.dumps(embedded.frozen_payload, sort_keys=True)
    assert "legacy-secret" not in serialized
    assert "legacy-app" not in serialized
    assert "legacy-chat" not in serialized
    assert embedded.frozen_payload["integration_profile"] == "legacy_main_embedded/v1"


def test_legacy_runtime_factory_wires_existing_main_infrastructure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.database import fundamentals_db as fundamentals_module
    from src.web import v20_service as service_module

    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.setenv("V20_MODE", "forward_shadow")
    base = load_v20_runtime_config(PROJECT_ROOT)
    repository = SimpleNamespace(
        config=V20DatabaseConfig(
            schema="v20",
            pool_min_size=1,
            pool_max_size=8,
            ssl_mode="require",
            connection_profile="legacy_embedded",
        )
    )
    fundamentals = SimpleNamespace()
    route = V20FeishuRoute(
        route_id=base.route_id,
        bot_url="https://legacy-relay.example",
        app_id="legacy-app",
        app_secret="legacy-secret",
        chat_id="legacy-chat",
        transport="legacy_send",
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(service_module, "load_v20_runtime_config", lambda _root: base)

    repository_pools: list[object | None] = []

    def create_repository(path, *, shared_pool=None):
        captured["database_path"] = path
        repository_pools.append(shared_pool)
        return repository

    monkeypatch.setattr(
        service_module,
        "create_embedded_v20_repository_from_config",
        create_repository,
    )
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "persisted-token")

    def create_fundamentals(path, *, tushare_token):
        captured["fundamentals_path"] = path
        captured["token"] = tushare_token
        return fundamentals

    monkeypatch.setattr(
        fundamentals_module,
        "create_fundamentals_db_from_config",
        create_fundamentals,
    )
    monkeypatch.setattr(service_module, "load_legacy_embedded_v20_route", lambda: route)
    monkeypatch.setattr(service_module, "load_g_artifacts", lambda *_args, **_kwargs: object())

    service = V20Service.from_legacy_runtime()

    assert service.config.enabled is True
    assert service.config.deployment_mode == "forward_shadow"
    assert service._repository is repository
    assert service._scan_state.fundamentals_db is fundamentals
    assert service._routes == {route.route_id: route}
    assert service._embedded_legacy is True
    assert service._initialize_resources is _init_owned_embedded_v20_scan_resources
    assert service._cleanup_resources is _cleanup_v20_scan_resources
    assert captured["token"] == "persisted-token"
    assert captured["database_path"] == captured["fundamentals_path"]
    assert repository_pools == [None]

    shared_pool = object()
    shared_fundamentals = SimpleNamespace(connection_pool=shared_pool)
    shared_service = V20Service.from_legacy_runtime(fundamentals_db=shared_fundamentals)

    assert shared_service._scan_state.fundamentals_db is shared_fundamentals
    assert shared_service._initialize_resources is _init_embedded_v20_scan_resources
    assert shared_service._cleanup_resources is _cleanup_embedded_v20_scan_resources
    assert repository_pools == [None, shared_pool]


async def test_embedded_runtime_reuses_shared_v16_trade_calendar_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cold 14:04 trigger after V16 ran must not fail on an empty V20 calendar."""
    from src.common import config as common_config
    from src.data.database import fundamentals_db as fundamentals_module
    from src.web import v15_scan_service
    from src.web import v20_service as service_module

    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.setenv("V20_MODE", "forward_shadow")
    base = load_v20_runtime_config(PROJECT_ROOT)
    repository = SimpleNamespace(
        config=V20DatabaseConfig(
            schema="v20",
            pool_min_size=1,
            pool_max_size=8,
            ssl_mode="require",
            connection_profile="legacy_embedded",
        )
    )
    fundamentals = SimpleNamespace()
    route = V20FeishuRoute(
        route_id=base.route_id,
        bot_url="https://legacy-relay.example",
        app_id="legacy-app",
        app_secret="legacy-secret",
        chat_id="legacy-chat",
        transport="legacy_send",
    )
    monkeypatch.setattr(service_module, "load_v20_runtime_config", lambda _root: base)
    monkeypatch.setattr(
        service_module,
        "create_embedded_v20_repository_from_config",
        lambda _path, *, shared_pool=None: repository,
    )
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "persisted-token")
    monkeypatch.setattr(
        fundamentals_module,
        "create_fundamentals_db_from_config",
        lambda _path, *, tushare_token: fundamentals,
    )
    monkeypatch.setattr(service_module, "load_legacy_embedded_v20_route", lambda: route)
    monkeypatch.setattr(service_module, "load_g_artifacts", lambda *_args, **_kwargs: object())

    service = V20Service.from_legacy_runtime()

    # The embedded service is wired to the exact shared V16 provider — not a
    # separate Tushare calendar adapter (the scan state has no client yet).
    assert service._calendar_provider is v15_scan_service.get_trade_calendar
    assert service._scan_state.realtime_client is None
    assert service._calendar_cache == ()

    calendar_days = [date(2026, 9, 1) - timedelta(days=offset) for offset in range(9, -1, -1)]
    monkeypatch.setattr(
        v15_scan_service,
        "_trade_calendar_cache",
        sorted(calendar_days + [date(2026, 9, 2), date(2026, 9, 3)]),
    )
    loaded = await service._load_trade_calendar(date(2026, 9, 1))
    assert date(2026, 9, 1) in loaded
    assert service._calendar_loaded_for == date(2026, 9, 1)

    # Concurrent cold callers share one provider call through the V20
    # singleflight wrapper around the shared provider.
    provider_calls = 0
    real_shared_provider = service._calendar_provider

    async def counted_provider() -> list[date]:
        nonlocal provider_calls
        provider_calls += 1
        await asyncio.sleep(0.05)
        return sorted(calendar_days + [date(2026, 9, 2), date(2026, 9, 3)])

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", counted_provider)
    second_service = V20Service.from_legacy_runtime()
    assert second_service._calendar_provider is counted_provider
    assert real_shared_provider is not counted_provider
    first, second = await asyncio.gather(
        second_service._load_trade_calendar(date(2026, 9, 1)),
        second_service._load_trade_calendar(date(2026, 9, 1)),
    )
    assert first == second
    assert provider_calls == 1

    # Genuinely invalid shared calendar data still fails closed.
    async def invalid_provider() -> list[date]:
        return [date(2026, 9, 1), date(2026, 9, 1)]

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", invalid_provider)
    third_service = V20Service.from_legacy_runtime()
    assert third_service._calendar_provider is invalid_provider
    with pytest.raises(V20RepositoryError, match="unsorted, or duplicated"):
        await third_service._load_trade_calendar(date(2026, 9, 1))
    assert third_service._calendar_cache == ()


@pytest.mark.parametrize(
    ("consumer", "field", "drifted"),
    [
        ("writer", "host", "other-writer.internal"),
        ("writer", "password", "other-writer-secret"),
        ("writer", "ssl_root_cert", "/certs/other-writer.pem"),
        ("fundamentals", "database", "other-fundamentals"),
        ("fundamentals", "password", "other-reader-secret"),
        ("fundamentals", "command_timeout_seconds", 17.0),
    ],
)
def test_default_factory_rejects_resolved_database_yaml_drift_before_connect(
    monkeypatch: pytest.MonkeyPatch,
    consumer: str,
    field: str,
    drifted: object,
) -> None:
    from src.data.database import fundamentals_db as fundamentals_module
    from src.web import v20_service as service_module

    _set_v20_consumer_environment(monkeypatch)
    runtime = _config(monkeypatch)
    writer_config = _writer_config()
    reader_config = _fundamentals_config()
    if consumer == "writer":
        writer_config = replace(writer_config, **{field: drifted})
    else:
        reader_config = replace(reader_config, **{field: drifted})

    class _NeverConnected:
        def __init__(self, config: object) -> None:
            self.config = config
            self.connect_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1

    writer = _NeverConnected(writer_config)
    fundamentals = _NeverConnected(reader_config)
    monkeypatch.setattr(service_module, "load_v20_runtime_config", lambda _root: runtime)
    monkeypatch.setattr(
        service_module,
        "create_v20_repository_from_config",
        lambda _path: writer,
    )
    monkeypatch.setattr(
        fundamentals_module,
        "create_fundamentals_db_from_config",
        lambda _path, *, tushare_token: fundamentals,
    )

    with pytest.raises(V20ConfigError, match="differs from explicit environment"):
        V20Service.from_default_config()

    assert writer.connect_calls == 0
    assert fundamentals.connect_calls == 0


def test_literal_database_yaml_drift_fails_before_asyncpg_connect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from src.data.database import fundamentals_db as fundamentals_module
    from src.data.database import v20_repository as repository_module
    from src.web import v20_service as service_module

    _set_v20_consumer_environment(monkeypatch)
    runtime = _config(monkeypatch)
    source = PROJECT_ROOT / "config" / "database-config.yaml"
    drifted_path = tmp_path / "database-config.yaml"
    drifted_path.write_text(
        source.read_text(encoding="utf-8").replace(
            'host: "${V20_DB_HOST:localhost}"',
            'host: "literal-writer.internal"',
        ),
        encoding="utf-8",
    )
    create_pool_calls = 0
    real_create_fundamentals = fundamentals_module.create_fundamentals_db_from_config

    async def forbidden_create_pool(**_kwargs: object) -> None:
        nonlocal create_pool_calls
        create_pool_calls += 1

    monkeypatch.setattr(service_module, "load_v20_runtime_config", lambda _root: runtime)
    monkeypatch.setattr(
        service_module,
        "create_v20_repository_from_config",
        lambda _path: repository_module.create_v20_repository_from_config(drifted_path),
    )
    monkeypatch.setattr(
        fundamentals_module,
        "create_fundamentals_db_from_config",
        lambda _path, *, tushare_token: real_create_fundamentals(
            drifted_path,
            tushare_token=tushare_token,
        ),
    )
    monkeypatch.setattr("asyncpg.create_pool", forbidden_create_pool)

    with pytest.raises(V20ConfigError, match="actual V20_DB host differs"):
        V20Service.from_default_config()

    assert create_pool_calls == 0


async def test_v20_scan_resources_use_explicit_environment_token_for_all_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module
    from src.data.sources import local_concept_mapper as concept_module
    from src.strategy.filters import stock_filter as stock_filter_module

    monkeypatch.setenv("TUSHARE_TOKEN", "environment-token")
    captured: dict[str, str] = {}

    class _Realtime:
        def __init__(self, *, token: str) -> None:
            captured["realtime"] = token

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        def as_ifind_format(self, *_args: object, **_kwargs: object) -> dict:
            return {}

    class _Historical:
        def __init__(
            self,
            _client: object,
            cache: object = None,
            *,
            tushare_token: str | None = None,
        ) -> None:
            del cache
            captured["historical"] = str(tushare_token)

    class _Fundamentals:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", _Realtime)
    monkeypatch.setattr(historical_module, "IQuantHistoricalAdapter", _Historical)
    monkeypatch.setattr(concept_module, "LocalConceptMapper", lambda: object())
    monkeypatch.setattr(stock_filter_module, "StockFilter", lambda _config: object())
    state = V15ScanState(fundamentals_db=_Fundamentals())

    await _init_v20_scan_resources(state)

    assert captured == {
        "realtime": "environment-token",
        "historical": "environment-token",
    }


async def test_embedded_v20_scan_resources_use_the_same_persisted_token_path_as_v16(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module
    from src.data.sources import local_concept_mapper as concept_module
    from src.strategy.filters import stock_filter as stock_filter_module

    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "persisted-v16-token")
    captured: dict[str, str] = {}

    class _Realtime:
        def __init__(self, *, token: str) -> None:
            captured["realtime"] = token

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    class _Historical:
        def __init__(
            self,
            _client: object,
            cache: object = None,
            *,
            tushare_token: str | None = None,
        ) -> None:
            del cache
            captured["historical"] = str(tushare_token)

    class _Fundamentals:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", _Realtime)
    monkeypatch.setattr(historical_module, "IQuantHistoricalAdapter", _Historical)
    monkeypatch.setattr(concept_module, "LocalConceptMapper", lambda: object())
    monkeypatch.setattr(stock_filter_module, "StockFilter", lambda _config: object())
    state = V15ScanState(fundamentals_db=_Fundamentals())

    await _init_embedded_v20_scan_resources(state)

    assert captured == {
        "realtime": "persisted-v16-token",
        "historical": "persisted-v16-token",
    }


async def test_embedded_cleanup_preserves_main_shared_fundamentals_pool() -> None:
    class _Realtime:
        stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1

        def as_ifind_format(self, *_args, **_kwargs):
            return {}

    class _SharedFundamentals:
        close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    realtime = _Realtime()
    fundamentals = _SharedFundamentals()
    state = V15ScanState(
        initialized=True,
        realtime_client=realtime,
        fundamentals_db=fundamentals,
    )

    await _cleanup_embedded_v20_scan_resources(state)

    assert realtime.stop_calls == 1
    assert fundamentals.close_calls == 0
    assert state.initialized is False


@pytest.mark.asyncio
async def test_v20_retry_reuses_resources_and_cleanup_enables_v16_takeover() -> None:
    class Realtime:
        def __init__(self) -> None:
            self.stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1

    realtime = Realtime()

    class Fundamentals:
        close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    fundamentals = Fundamentals()
    state = V15ScanState(fundamentals_db=fundamentals)
    constructions = 0

    async def initialize() -> None:
        nonlocal constructions
        constructions += 1
        state.realtime_client = realtime
        state.initialized = True

    await _initialize_scan_resources_once(state, "V20", initialize)
    await _initialize_scan_resources_once(state, "V20", initialize)
    assert constructions == 1
    assert state.resource_owner == "V20"

    await cleanup_scan_resources(state, owner="V20", close_fundamentals=False)
    assert realtime.stop_calls == 1
    assert state.realtime_client is None
    assert state.resource_owner is None

    replacement = Realtime()

    async def initialize_v16() -> None:
        state.realtime_client = replacement
        state.initialized = True

    await _initialize_scan_resources_once(state, "V16", initialize_v16)
    assert state.resource_owner == "V16"
    await cleanup_scan_resources(state, owner="V16")
    assert replacement.stop_calls == 1
    assert fundamentals.close_calls == 1


@pytest.mark.asyncio
async def test_v20_public_initializer_retry_is_singleflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    state = V15ScanState(fundamentals_db=object())
    calls = 0

    async def initialize_once(*_args: object, **_kwargs: object) -> None:
        nonlocal calls
        calls += 1
        state.initialized = True

    monkeypatch.setattr(
        "src.web.v20_service._init_v20_scan_resources_with_token",
        initialize_once,
    )

    await asyncio.gather(
        _init_embedded_v20_scan_resources(state),
        _init_embedded_v20_scan_resources(state),
    )
    await _init_embedded_v20_scan_resources(state)

    assert calls == 1
    assert state.resource_owner == "V20"


@pytest.mark.asyncio
async def test_v16_public_initializer_is_singleflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.web import v15_scan_service

    state = V15ScanState()
    calls = 0

    async def initialize_once(_state: V15ScanState) -> None:
        nonlocal calls
        calls += 1
        state.initialized = True

    monkeypatch.setattr(v15_scan_service, "_initialize_v16_scan_resources", initialize_once)

    await asyncio.gather(
        v15_scan_service.init_scan_resources(state),
        v15_scan_service.init_scan_resources(state),
    )
    await v15_scan_service.init_scan_resources(state)

    assert calls == 1
    assert state.resource_owner == "V16"


@pytest.mark.asyncio
async def test_v16_initializer_reuses_existing_fundamentals_even_if_factory_returns_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module
    from src.data.database import fundamentals_db as fundamentals_module
    from src.data.database import v15_scan_db as v15_scan_db_module
    from src.data.sources import local_concept_mapper as concept_module
    from src.strategy.filters import stock_filter as stock_filter_module
    from src.web import v15_scan_service

    monkeypatch.setenv("TUSHARE_TOKEN", "shared-pool-token")
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "shared-pool-token")

    class Realtime:
        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    class SharedFundamentals:
        connect_calls = 0
        close_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

    class FactoryFundamentals:
        def __init__(self) -> None:
            self.connect_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1

    class ScanDB:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

    shared = SharedFundamentals()
    factory_calls = 0

    def factory() -> FactoryFundamentals:
        nonlocal factory_calls
        factory_calls += 1
        return FactoryFundamentals()

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", lambda **_kwargs: Realtime())
    monkeypatch.setattr(
        historical_module,
        "IQuantHistoricalAdapter",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(concept_module, "LocalConceptMapper", lambda: object())
    monkeypatch.setattr(stock_filter_module, "StockFilter", lambda _config: object())
    monkeypatch.setattr(fundamentals_module, "create_fundamentals_db_from_config", factory)
    monkeypatch.setattr(v15_scan_db_module, "create_v15_scan_db_from_config", ScanDB)

    state = V15ScanState(fundamentals_db=shared)
    await v15_scan_service.init_scan_resources(state)

    assert factory_calls == 0
    assert shared.connect_calls == 0
    assert state.fundamentals_db is shared


@pytest.mark.asyncio
async def test_v16_cleanup_closes_fundamentals_and_restart_connects_new_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module
    from src.data.database import fundamentals_db as fundamentals_module
    from src.data.database import v15_scan_db as v15_scan_db_module
    from src.data.sources import local_concept_mapper as concept_module
    from src.strategy.filters import stock_filter as stock_filter_module
    from src.web import v15_scan_service

    monkeypatch.setenv("TUSHARE_TOKEN", "restart-token")
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "restart-token")

    class Realtime:
        def __init__(self, *, token: str) -> None:
            assert token == "restart-token"

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    class ClosedFundamentals:
        def __init__(self) -> None:
            self.closed = False

        async def connect(self) -> None:
            raise AssertionError("closed pool must not be reused")

        async def close(self) -> None:
            self.closed = True

    class RestartFundamentals:
        def __init__(self) -> None:
            self.connect_calls = 0
            self.closed = False

        async def connect(self) -> None:
            self.connect_calls += 1

        async def close(self) -> None:
            self.closed = True

    class ScanDB:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

    closed = ClosedFundamentals()
    restarted: list[RestartFundamentals] = []

    def factory() -> RestartFundamentals:
        created = RestartFundamentals()
        restarted.append(created)
        return created

    monkeypatch.setattr(
        realtime_module,
        "TushareRealtimeClient",
        lambda **kwargs: Realtime(**kwargs),
    )
    monkeypatch.setattr(
        historical_module,
        "IQuantHistoricalAdapter",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(concept_module, "LocalConceptMapper", lambda: object())
    monkeypatch.setattr(stock_filter_module, "StockFilter", lambda _config: object())
    monkeypatch.setattr(fundamentals_module, "create_fundamentals_db_from_config", factory)
    monkeypatch.setattr(v15_scan_db_module, "create_v15_scan_db_from_config", ScanDB)

    state = V15ScanState(
        initialized=True,
        resource_owner="V16",
        realtime_client=Realtime(token="restart-token"),
        fundamentals_db=closed,
    )
    await cleanup_scan_resources(state, owner="V16")
    assert closed.closed is True
    assert state.fundamentals_db is None

    await v15_scan_service.init_scan_resources(state)
    assert len(restarted) == 1
    assert restarted[0].connect_calls == 1
    assert state.fundamentals_db is restarted[0]
    assert state.fundamentals_db is not closed


@pytest.mark.asyncio
async def test_v16_partial_failure_rolls_back_and_retry_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module
    from src.data.database import fundamentals_db as fundamentals_module
    from src.data.database import v15_scan_db as v15_scan_db_module
    from src.data.sources import local_concept_mapper as concept_module
    from src.strategy.filters import stock_filter as stock_filter_module
    from src.web import v15_scan_service

    async def no_notify(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(v15_scan_service, "_notify_feishu_error", no_notify)
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "test-token")

    for failure in ("start", "connect", "adapter"):
        state = V15ScanState()
        attempts = 0
        has_failed = False

        class Realtime:
            def __init__(self, *, token: str) -> None:
                assert token == "test-token"
                self.stop_calls = 0

            async def start(self) -> None:
                nonlocal attempts
                nonlocal has_failed
                attempts += 1
                if failure == "start" and not has_failed:
                    has_failed = True
                    raise RuntimeError("rt start failed")

            async def stop(self) -> None:
                self.stop_calls += 1

            def as_ifind_format(self, *_args: object, **_kwargs: object) -> dict:
                return {}

        class Fundamentals:
            def __init__(self) -> None:
                self.close_calls = 0

            async def connect(self) -> None:
                nonlocal attempts
                nonlocal has_failed
                attempts += 1
                if failure == "connect" and not has_failed:
                    has_failed = True
                    raise RuntimeError("fundamentals connect failed")

            async def close(self) -> None:
                self.close_calls += 1

        class ScanDB:
            def __init__(self) -> None:
                self.close_calls = 0

            async def connect(self) -> None:
                return None

            async def close(self) -> None:
                self.close_calls += 1

        realtime = Realtime(token="test-token")
        fundamentals = Fundamentals()

        def historical(*_args: object, **_kwargs: object) -> object:
            nonlocal attempts
            nonlocal has_failed
            attempts += 1
            if failure == "adapter" and not has_failed:
                has_failed = True
                raise RuntimeError("historical constructor failed")
            return object()

        monkeypatch.setattr(realtime_module, "TushareRealtimeClient", lambda **_kwargs: realtime)
        monkeypatch.setattr(
            fundamentals_module, "create_fundamentals_db_from_config", lambda: fundamentals
        )
        monkeypatch.setattr(v15_scan_db_module, "create_v15_scan_db_from_config", ScanDB)
        monkeypatch.setattr(historical_module, "IQuantHistoricalAdapter", historical)
        monkeypatch.setattr(concept_module, "LocalConceptMapper", lambda: object())
        monkeypatch.setattr(stock_filter_module, "StockFilter", lambda _config: object())

        with pytest.raises(RuntimeError, match="failed"):
            await v15_scan_service.init_scan_resources(state)

        assert realtime.stop_calls == 1
        if failure in {"connect", "adapter"}:
            assert fundamentals.close_calls == 1
        assert state.realtime_client is None
        assert state.fundamentals_db is None
        assert state.initialized is False

        await v15_scan_service.init_scan_resources(state)
        assert state.initialized is True
        assert state.realtime_client is not None
        await cleanup_scan_resources(state, owner="V16")


@pytest.mark.asyncio
async def test_v16_initializer_cancellation_rolls_back_partial_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import tushare_realtime as realtime_module
    from src.web import v15_scan_service

    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "test-token")
    entered = asyncio.Event()

    class Realtime:
        def __init__(self, *, token: str) -> None:
            self.stop_calls = 0

        async def start(self) -> None:
            entered.set()
            await asyncio.Event().wait()

        async def stop(self) -> None:
            self.stop_calls += 1

    realtime = Realtime(token="test-token")
    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", lambda **_kwargs: realtime)
    state = V15ScanState()
    task = asyncio.create_task(v15_scan_service._initialize_v16_scan_resources(state))
    await asyncio.wait_for(entered.wait(), timeout=1.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert realtime.stop_calls == 1
    assert state.realtime_client is None
    assert state.initialized is False


@pytest.mark.asyncio
async def test_cleanup_runs_when_initialized_false_and_tasks_remain() -> None:
    class Realtime:
        stop_calls = 0

        async def stop(self) -> None:
            self.stop_calls += 1

    realtime = Realtime()
    scheduler_started = asyncio.Event()

    async def scheduler() -> None:
        scheduler_started.set()
        await asyncio.Event().wait()

    state = V15ScanState(initialized=False, realtime_client=realtime)
    state.scheduler_task = asyncio.create_task(scheduler())
    canonical_started = asyncio.Event()

    async def canonical_compute() -> None:
        canonical_started.set()
        await asyncio.Event().wait()

    canonical_task = asyncio.create_task(canonical_compute())
    state.canonical_coordinator = SimpleNamespace(
        inflight={"scan": canonical_task},
        publish={},
        pending_persist={},
    )
    await asyncio.wait_for(scheduler_started.wait(), timeout=1.0)
    await asyncio.wait_for(canonical_started.wait(), timeout=1.0)

    await cleanup_scan_resources(state, owner="V16")

    assert state.scheduler_task is None
    assert canonical_task.cancelled() is True
    assert state.canonical_coordinator is None
    assert realtime.stop_calls == 1
    assert state.realtime_client is None


def test_canonical_v20_projection_is_lossless_and_bypasses_old_pipeline() -> None:
    class Bomb:
        def __call__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("old V20 scan path must not run")

    first = ScoredStock(
        code="603068",
        name="酒钢宏兴",
        score=0.987654321,
        rank=1,
        buy_price=2.345678,
    )
    second = ScoredStock(
        code="605299",
        name="葫芦股份",
        score=0.876543210,
        rank=2,
        buy_price=12.678901,
    )
    histories = {
        "603068": {"time": ["2026-08-31"], "close": [2.3]},
        "605299": {"time": ["2026-08-31"], "close": [12.5]},
    }
    early_bars = {
        code: (_bar(code, "09:39", trade_date=date(2026, 9, 1)),) for code in ("603068", "605299")
    }
    early_source_hashes = {
        code: sha256_json([_bar_payload(bar) for bar in bars]) for code, bars in early_bars.items()
    }
    result = V16ScanResult(
        recommended=[first, second],
        all_scored=[first, second],
        step0_universe_count=3,
        step2_hot_board_count=1,
        step2_filtered_by_avg_gain=1,
        step3_count=2,
        step4_count=2,
        step5_count=2,
        step6_count=2,
        step6_5_count=2,
        step6_6_count=2,
        final_candidates=2,
        step0_codes=["603068", "605299", "000001"],
        step2_boards_detail={"board-b": ["603068"], "board-a": ["605299"]},
        step2_codes=["603068", "605299"],
        st_eligible_codes=["603068"],
        step3_codes=["603068", "605299"],
        step4_codes=["603068", "605299"],
        step5_codes=["603068", "605299"],
        step6_codes=["603068", "605299"],
        step6_5_codes=["603068", "605299"],
        step6_6_codes=["603068", "605299"],
        stock_best_board={"603068": "board-b", "605299": "board-a"},
        stock_all_boards={"603068": ["board-b"], "605299": ["board-a"]},
        step2_board_avg_gains={"board-a": 1.25, "board-b": 1.5},
        stock_is_driver={"603068": True, "605299": False},
        stock_cci={"603068": 88.5},
        stock_early_vol={"603068": 12345.0},
    )
    canonical = CanonicalV16ScanBundle(
        trade_date=date(2026, 9, 1),
        scan_result=result,
        stock_data={
            "603068": SimpleNamespace(volume_937=12345.0),
            "605299": SimpleNamespace(volume_937=None),
        },
        clean_boards={"board-a": [("605299", "葫芦股份")]},
        universe=("603068", "605299", "000001"),
        quotes={},
        prev_closes={},
        history_raw=histories,
        early_bars=early_bars,
        early_source_hashes=early_source_hashes,
        failed_no_prev_close=("000001",),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="m" * 64,
        feature_list_sha256="f" * 64,
        computed_at=datetime(2026, 9, 1, 9, 39, 59, tzinfo=TZ),
        input_hash="i" * 64,
        _integrity_hash="c" * 64,
        computation_calendar=(
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
            date(2026, 9, 3),
        ),
        prior_trade_date=date(2026, 8, 31),
    )
    service = V20Service.__new__(V20Service)
    service.config = SimpleNamespace(clock=SimpleNamespace(decision_bar_label="09:39"))
    service._scan_state = V15ScanState(
        realtime_client=Bomb(),
        historical_adapter=Bomb(),
    )

    projected = service._project_canonical_v16(
        canonical,
        calendar=(
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
            date(2026, 9, 3),
        ),
    )

    symbols = projected.snapshot["symbols"]
    assert [item["code"] for item in symbols] == ["603068", "605299"]
    assert [item["score"] for item in symbols] == [0.987654321, 0.876543210]
    assert [item["snapshot_price"] for item in symbols] == [2.345678, 12.678901]
    assert [item["history_hash"] for item in symbols] == [
        sha256_json(histories["603068"]),
        sha256_json(histories["605299"]),
    ]
    assert [item["early_source_hash"] for item in symbols] == [
        early_source_hashes["603068"],
        early_source_hashes["605299"],
    ]
    assert symbols[0]["cci"] == 88.5
    assert symbols[0]["volume_937"] == 12345.0
    assert symbols[1]["boards"] == ["board-a"]
    assert symbols[1]["best_board"] == "board-a"
    assert symbols[1]["is_driver"] is False
    assert symbols[1]["cci"] is None
    assert symbols[1]["volume_937"] is None
    assert projected.snapshot["board_avg_gains"] == {
        "board-a": 1.25,
        "board-b": 1.5,
    }
    assert projected.snapshot["funnel"]["step3_count"] == 2
    assert projected.snapshot["stages"]["step2_codes"] == ["603068", "605299"]
    assert projected.snapshot["scan_input_failure_codes"] == ["000001"]
    assert projected.snapshot_hash == sha256_json(projected.snapshot)


class _StrictTestArtifactStore:
    def __init__(self, timeline: list[str], official_stream_id: str) -> None:
        self.timeline = timeline
        self.official_stream_id = official_stream_id
        self.record: Any | None = None

    async def save_once(
        self,
        payload: Mapping[str, Any],
        *,
        official_stream_id: str,
        trade_date: date,
        event: str,
    ) -> Any:
        assert official_stream_id == self.official_stream_id
        assert event == "V16_CANONICAL_MASTER_V1"
        self.timeline.append("artifact-save")
        portable = dict(payload)
        record = SimpleNamespace(
            payload=portable,
            snapshot_hash=sha256_json(portable),
            trade_date=trade_date,
            first_received_at=datetime.combine(trade_date, time(9, 39, 20), tzinfo=TZ),
        )
        if self.record is None:
            self.record = record
        else:
            assert self.record.payload == record.payload
        return self.record

    async def load(
        self,
        *,
        official_stream_id: str,
        trade_date: date,
        event: str,
    ) -> Any | None:
        assert official_stream_id == self.official_stream_id
        assert event == "V16_CANONICAL_MASTER_V1"
        self.timeline.append(
            "artifact-load-hit" if self.record is not None else "artifact-load-miss"
        )
        return self.record


def _strict_barrier_canonical(trade_date: date) -> CanonicalV16ScanBundle:
    predecessor = date(2026, 8, 28)
    successors = (date(2026, 9, 1), date(2026, 9, 2))
    codes = sorted(_LATE_REPLAY_CODES)
    scan_result = _late_replay_scan_result()
    (board_name,) = scan_result.step2_board_avg_gains
    scan_result = replace(
        scan_result,
        step0_codes=codes,
        step2_boards_detail={board_name: codes},
        step2_codes=codes,
        st_eligible_codes=codes,
        step3_codes=codes,
        step4_codes=codes,
        step5_codes=codes,
        step6_codes=codes,
        step6_5_codes=codes,
        step6_6_codes=codes,
        step3_count=len(codes),
        step4_count=len(codes),
        step5_count=len(codes),
        step6_count=len(codes),
        step6_5_count=len(codes),
        step6_6_count=len(codes),
    )
    history_dates = tuple(predecessor - timedelta(days=offset) for offset in range(36, -1, -1))
    history_raw = {
        code: {
            "time": [day.isoformat() for day in history_dates],
            "open": [10.0] * len(history_dates),
            "high": [10.2] * len(history_dates),
            "low": [9.8] * len(history_dates),
            "close": [10.0] * len(history_dates),
            "volume": [1_000.0] * len(history_dates),
        }
        for code in _LATE_REPLAY_CODES
    }
    stock_data = {
        code: SimpleNamespace(
            code=code,
            name=f"name-{code}",
            open_price=10.0,
            prev_close=10.0,
            price_940=10.1,
            high_940=10.2,
            low_940=9.9,
            volume_940=1_000.0,
            volume_937=900.0,
            avg_daily_volume=800.0,
            trend_5d=0.01,
            trend_10d=0.02,
            avg_daily_return_20d=0.001,
            volatility_20d=0.01,
            consecutive_up_days=1,
            history_df=None,
        )
        for code in _LATE_REPLAY_CODES
    }
    base = replace(
        _entry_cycle_bundle(trade_date),
        scan_result=scan_result,
        stock_data=stock_data,
        history_raw=history_raw,
        computed_at=datetime.combine(trade_date, time(9, 39, 10), tzinfo=TZ),
        computation_calendar=(predecessor, trade_date, *successors),
        prior_trade_date=predecessor,
        prior_amount_yuan={code: 1_000_000.0 for code in _LATE_REPLAY_CODES},
        breadth_valid_n=len(_LATE_REPLAY_CODES),
        breadth_down_n=1,
        breadth_market_source_hash="b" * 64,
        history_date_valid_counts={
            day.isoformat(): len(_LATE_REPLAY_CODES) for day in history_dates
        },
        history_min_date_coverage=1.0,
        _integrity_hash="",
    )
    return replace(base, _integrity_hash=_bundle_fingerprint(base))


def _install_strict_durable_barrier(
    monkeypatch: pytest.MonkeyPatch,
    service: V20Service,
    repository: Any,
    timeline: list[str],
) -> _StrictTestArtifactStore:
    store = _StrictTestArtifactStore(timeline, service.config.official_stream_id)
    service._canonical_artifact_store = store
    service._canonical_callbacks_open = True
    service._canonical_artifact_lock = asyncio.Lock()
    service._canonical_raw_persisted_dates = set()
    service._canonical_barrier_completed_at = {}
    raw_records: dict[tuple[str, str], MinuteBarRecord] = {}

    async def record_minute_bars(rows: Sequence[Mapping[str, Any]]) -> frozenset[str]:
        timeline.append("durable-raw")
        hashes: set[str] = set()
        for item in rows:
            payload = dict(item)
            code = str(payload["stock_code"])
            label = str(payload["end_label"])
            digest = sha256_json(payload)
            hashes.add(digest)
            raw_records[(code, label)] = MinuteBarRecord(
                code=code,
                bar_end=datetime.fromisoformat(str(payload["bar_end"])),
                end_label=label,
                source_hash=digest,
                payload=payload,
                first_received_at=datetime.combine(
                    date.fromisoformat(str(payload["bar_end"])[:10]),
                    time(9, 39, 15),
                    tzinfo=TZ,
                ),
            )
        return frozenset(hashes)

    async def list_raw_minute_bar_records(
        codes: Sequence[str],
        *,
        trade_date: date,
        end_labels: Sequence[str],
    ) -> tuple[MinuteBarRecord, ...]:
        allowed_codes = set(codes)
        allowed_labels = set(end_labels)
        return tuple(
            record
            for (code, label), record in sorted(raw_records.items())
            if code in allowed_codes
            and label in allowed_labels
            and record.bar_end.astimezone(TZ).date() == trade_date
        )

    monkeypatch.setattr(repository, "record_minute_bars", record_minute_bars, raising=False)
    monkeypatch.setattr(
        repository,
        "list_raw_minute_bar_records",
        list_raw_minute_bar_records,
        raising=False,
    )
    production_hydrate = service._hydrate_canonical_artifact_record

    async def observed_hydrate(record: Any) -> Any:
        timeline.append("artifact-hydrate")
        return await production_hydrate(record)

    monkeypatch.setattr(service, "_hydrate_canonical_artifact_record", observed_hydrate)
    service._scan_state.canonical_sink = service._persist_canonical_artifact_barrier
    return store


@pytest.mark.asyncio
async def test_resource_waiter_cancellation_does_not_cancel_owner() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    state = V15ScanState()
    constructions = 0

    async def initialize() -> None:
        nonlocal constructions
        constructions += 1
        started.set()
        await release.wait()
        state.initialized = True

    owner = asyncio.create_task(_initialize_scan_resources_once(state, "V20", initialize))
    await asyncio.wait_for(started.wait(), timeout=1.0)
    borrower = asyncio.create_task(_initialize_scan_resources_once(state, "V20", initialize))
    await asyncio.sleep(0)
    borrower.cancel()
    with pytest.raises(asyncio.CancelledError):
        await borrower

    release.set()
    await owner
    assert constructions == 1
    assert state.initialized is True


@pytest.mark.asyncio
async def test_entry_collection_never_touches_old_scan_pipeline_or_vendor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def bomb(*_args: object, **_kwargs: object):
        raise AssertionError("old V20 scan path must not run")

    trade_date = date(2026, 8, 31)
    canonical = _strict_barrier_canonical(trade_date)

    async def canonical_once(state, requested_date):
        assert state is service._scan_state
        assert requested_date is trade_date
        assert state.canonical_sink is not None
        await state.canonical_sink(canonical)
        return canonical

    monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", canonical_once)
    service = V20Service.__new__(V20Service)
    service._scan_state = V15ScanState(
        realtime_client=SimpleNamespace(batch_get_minute_history=bomb)
    )

    async def get_entry_status(*_args: object, **_kwargs: object) -> None:
        return None

    repository = SimpleNamespace(get_entry_status=get_entry_status)
    service._repository = repository
    # Today's MEWS cache is already present, so the cycle skips the join.
    service._mews_cached_for = trade_date
    service._clock = lambda: datetime(2026, 8, 31, 9, 39, 30, tzinfo=TZ)
    service.config = SimpleNamespace(
        official_stream_id="stream",
        clock=SimpleNamespace(
            decision_bar_label="09:39",
            publish_deadline=time(9, 40),
            decision_finalization_deadline=time(9, 45),
        ),
    )
    context = _DayContext(
        trade_date=trade_date,
        calendar=(
            date(2026, 8, 28),
            trade_date,
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
    )
    timeline: list[str] = []
    store = _install_strict_durable_barrier(monkeypatch, service, repository, timeline)

    await service._run_entry_collection_cycle(context, datetime(2026, 8, 31, 9, 31, tzinfo=TZ))
    assert context.canonical_bundle is None

    await service._run_entry_collection_cycle(context, datetime(2026, 8, 31, 9, 39, tzinfo=TZ))
    assert context.canonical_bundle is not None
    assert context.canonical_bundle.snapshot_hash == store.record.payload["v20_snapshot_hash"]
    assert timeline == [
        "artifact-load-miss",
        "durable-raw",
        "artifact-load-miss",
        "artifact-save",
        "artifact-load-hit",
        "artifact-hydrate",
        "artifact-load-hit",
        "artifact-hydrate",
    ]


@pytest.mark.asyncio
async def test_bind_preserves_already_owned_scan_resources() -> None:
    owned_fundamentals = object()
    realtime = object()
    historical = object()
    owner_task = asyncio.get_running_loop().create_future()
    service = V20Service.__new__(V20Service)
    service._resources_started = False
    service._started = False
    service._scan_state = V15ScanState(
        initialized=True,
        realtime_client=realtime,
        fundamentals_db=owned_fundamentals,
        historical_adapter=historical,
        resource_owner="V20",
        resource_init_task=owner_task,
    )
    shared = V15ScanState()

    service.bind_shared_v15_scan_state(shared)

    assert service._scan_state is shared
    assert shared.fundamentals_db is owned_fundamentals
    assert shared.realtime_client is realtime
    assert shared.historical_adapter is historical
    assert shared.resource_owner == "V20"
    assert shared.resource_init_task is owner_task
    assert shared.initialized is True
    owner_task.cancel()


@pytest.mark.asyncio
async def test_slow_but_finite_selection_mews_completes_and_stop_cancels_managed_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No artificial outer budget: a slow-but-finite attempt is never cut off."""
    # The singleflight await carries no wall-clock cap at all.
    assert not hasattr(service_module, "MEWS_SINGLEFLIGHT_ATTEMPT_TIMEOUT_SECONDS")
    released = asyncio.Event()

    class _HangingSource:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            await released.wait()
            raise AssertionError("a cancelled attempt must never complete")

    # Phase 1: a legitimately slow attempt (many bounded provider calls and
    # retries would look like this) runs to completion and persists.
    class _SlowSource:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            await asyncio.sleep(0.2)
            return _late_mews_payload()

    repository = _AfterCutoffMewsRepository()
    slow_source = _SlowSource()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = slow_source
    alerts = _alert_recorder(monkeypatch, service)
    now = datetime(2026, 9, 1, 9, 39, tzinfo=TZ)
    service._clock = lambda: now
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=(date(2026, 8, 31), date(2026, 9, 1))),
    )

    assert await service.ensure_mews_for_selection_trigger(now) is True
    assert slow_source.calls == 1
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == date(2026, 9, 1)
    assert alerts == []
    assert service._mews_singleflight_task is None

    # Phase 2: a wedged attempt is still a managed task — stop() cancels and
    # awaits it, and the joined caller settles without an orphan.
    hanging_source = _HangingSource()
    service._mews_source = hanging_source
    service._mews_cached_for = None
    waiter = asyncio.create_task(service.ensure_mews_for_selection_trigger(now))
    for _ in range(200):
        if hanging_source.calls == 1:
            break
        await asyncio.sleep(0.005)
    assert hanging_source.calls == 1
    task = service._mews_singleflight_task
    assert task is not None and not task.done()
    service._repository_started = False
    await service.stop()
    assert task.cancelled() is True
    assert await waiter is False
    assert service._mews_singleflight_task is None
    released.set()


@pytest.mark.asyncio
async def test_failed_selection_mews_attempt_is_retried_by_a_later_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            raise MewsSnapshotSourceError("mews unavailable")

    repository = _AfterCutoffMewsRepository()
    source = _Source()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = source
    alerts = _alert_recorder(monkeypatch, service)
    now = datetime(2026, 9, 1, 9, 39, tzinfo=TZ)
    service._clock = lambda: now
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=(date(2026, 8, 31), date(2026, 9, 1))),
    )

    # The trigger awaits the attempt; the failure settles one daily idempotent
    # alert before the independent entry path continues.
    assert await service.ensure_mews_for_selection_trigger(now) is False
    assert source.calls == 1
    assert repository.payloads == []
    assert len(alerts) == 1
    assert alerts[0]["code"] == "MEWS_CALCULATION_FAILED"
    assert "SELECTION_TRIGGER" in alerts[0]["message"]
    assert service._lane_health["mews_cache"].last_error is not None
    assert service._mews_singleflight_task is None

    # The finished task was cleared: a later distinct trigger retries instead
    # of hitting a permanent daily failure skip, and never doubles the alert.
    assert await service.ensure_mews_for_selection_trigger(now) is False
    assert source.calls == 2
    assert len(alerts) == 1
    assert repository.payloads == []


async def test_mews_failure_alert_stays_bound_to_attempt_date_not_ambient_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        async def fetch_snapshot(self, **_kwargs):
            raise MewsSnapshotSourceError("target-date outage")

    service = _service(monkeypatch, _AfterCutoffMewsRepository())
    service._repository_started = True
    service._mews_source = _Source()
    alerts = _alert_recorder(monkeypatch, service)
    target_now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    ambient_now = datetime(2026, 9, 2, 9, 10, tzinfo=TZ)
    service._clock = lambda: ambient_now
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(
            0, result=(date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2))
        ),
    )

    assert await service.ensure_mews_for_selection_trigger(target_now) is False
    assert len(alerts) == 1
    assert alerts[0]["entity_id"] == "2026-09-01"
    assert alerts[0]["now"] == target_now
    assert alerts[0]["event_id"] == named_hash(
        "V20_MEWS_CALCULATION_FAILED_EVENT_ID_V1",
        {
            "alert_code": "MEWS_CALCULATION_FAILED",
            "entity_id": "2026-09-01",
            "route_id": service.config.route_id,
            "official_stream_id": service.config.official_stream_id,
            "state_lineage_id": service.config.state_lineage_id,
            "trade_date": "2026-09-01",
        },
    )
    assert service._mews_alerted_for == date(2026, 9, 1)


async def test_stop_cancels_and_awaits_hanging_calendar_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _AfterCutoffMewsRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    started = asyncio.Event()
    cancel_entered = asyncio.Event()

    async def provider() -> list[date]:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancel_entered.set()
            raise

    service._calendar_provider = provider
    waiter = asyncio.create_task(service._load_trade_calendar(date(2026, 9, 1)))
    await asyncio.wait_for(started.wait(), timeout=1)

    await asyncio.wait_for(service.stop(), timeout=1)
    await asyncio.wait_for(cancel_entered.wait(), timeout=1)
    assert waiter.cancelled() is True
    assert service._calendar_tasks == {}
    assert not any(
        task.get_name().startswith("v20-calendar-")
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    )


async def test_calendar_date_rollover_cancels_old_master_and_stop_rejects_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _AfterCutoffMewsRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    old_started = asyncio.Event()
    old_cancel_entered = asyncio.Event()
    calendar = (
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
        date(2026, 9, 4),
    )

    async def provider() -> list[date]:
        if not old_started.is_set():
            old_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                old_cancel_entered.set()
                raise
        return list(calendar)

    service._calendar_provider = provider
    old_waiter = asyncio.create_task(service._load_trade_calendar(date(2026, 9, 1)))
    await asyncio.wait_for(old_started.wait(), timeout=1)

    assert await service._load_trade_calendar(date(2026, 9, 2)) == calendar
    await asyncio.wait_for(old_cancel_entered.wait(), timeout=1)
    assert old_waiter.cancelled() is True
    await service.stop()
    service._calendar_cache = ()
    service._calendar_loaded_for = None
    with pytest.raises(V20RepositoryError, match="trade-calendar task lane is stopped"):
        await service._load_trade_calendar(date(2026, 9, 2))


class _GatedMewsSource:
    def __init__(self, gate: asyncio.Event) -> None:
        self.calls = 0
        self.entered = asyncio.Event()
        self._gate = gate

    async def fetch_snapshot(self, *, source_trade_date, availability_date):
        self.calls += 1
        self.entered.set()
        await self._gate.wait()
        return _late_mews_payload()


def _singleflight_service(
    monkeypatch: pytest.MonkeyPatch,
    repository: Any,
    source: Any,
    now: datetime,
) -> V20Service:
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._mews_source = source
    service._clock = lambda: now
    monkeypatch.setattr(
        service,
        "_load_trade_calendar",
        lambda _day: asyncio.sleep(0, result=(date(2026, 8, 31), date(2026, 9, 1))),
    )
    return service


@pytest.mark.asyncio
async def test_selection_trigger_awaits_attempt_and_persists_before_returning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    repository = _AfterCutoffMewsRepository()
    source = _GatedMewsSource(gate)
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    service = _singleflight_service(monkeypatch, repository, source, now)

    waiter = asyncio.create_task(service.ensure_mews_for_selection_trigger(now))
    await asyncio.wait_for(source.entered.wait(), timeout=1.0)
    await asyncio.sleep(0)
    # The selection path blocks on the attempt: no result, nothing persisted.
    assert not waiter.done()
    assert repository.payloads == []

    gate.set()
    assert await asyncio.wait_for(waiter, timeout=1.0) is True
    # Success persisted before the trigger returned.
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_singleflight_task is None


@pytest.mark.asyncio
async def test_scheduler_and_triggers_share_one_singleflight_attempt_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    repository = _AfterCutoffMewsRepository()
    source = _GatedMewsSource(gate)
    now = datetime(2026, 9, 1, 9, 10, tzinfo=TZ)
    service = _singleflight_service(monkeypatch, repository, source, now)
    calendar = (date(2026, 8, 31), date(2026, 9, 1))

    scheduled = asyncio.create_task(service._refresh_mews_cache_once(now, calendar))
    first = asyncio.create_task(service.ensure_mews_for_selection_trigger(now))
    second = asyncio.create_task(service.ensure_mews_for_selection_trigger(now))
    await asyncio.wait_for(source.entered.wait(), timeout=1.0)
    await asyncio.sleep(0)
    # The 09:10 scheduler and both triggers joined one overlapping raw attempt.
    assert source.calls == 1

    gate.set()
    results = await asyncio.wait_for(
        asyncio.gather(scheduled, first, second),
        timeout=1.0,
    )
    assert results == [True, True, True]
    assert source.calls == 1
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_singleflight_task is None


@pytest.mark.asyncio
async def test_scheduler_and_triggers_share_one_singleflight_attempt_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingSource:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            raise MewsSnapshotSourceError("Tushare margin is missing SSE or SZSE")

    repository = _AfterCutoffMewsRepository()
    source = _FailingSource()
    now = datetime(2026, 9, 1, 9, 10, tzinfo=TZ)
    service = _singleflight_service(monkeypatch, repository, source, now)
    alerts = _alert_recorder(monkeypatch, service)
    calendar = (date(2026, 8, 31), date(2026, 9, 1))

    results = await asyncio.gather(
        service._refresh_mews_cache_once(now, calendar),
        service.ensure_mews_for_selection_trigger(now),
        service.ensure_mews_for_selection_trigger(now),
    )

    # One overlapping raw attempt, one shared failure, one daily alert.
    assert results == [False, False, False]
    assert source.calls == 1
    assert repository.payloads == []
    assert len(alerts) == 1
    assert alerts[0]["code"] == "MEWS_CALCULATION_FAILED"
    assert service._lane_health["mews_cache"].last_error is not None
    assert service._mews_singleflight_task is None

    # The cleared task lets a later distinct trigger retry while cache is missing.
    assert await service.ensure_mews_for_selection_trigger(now) is False
    assert source.calls == 2
    assert len(alerts) == 1


@pytest.mark.asyncio
async def test_after_cutoff_recovery_and_triggers_share_one_singleflight_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The after-cutoff scheduler tick joins the same per-date task as triggers."""
    gate = asyncio.Event()
    repository = _AfterCutoffMewsRepository()
    source = _GatedMewsSource(gate)
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    service = _singleflight_service(monkeypatch, repository, source, now)
    calendar = (date(2026, 8, 31), date(2026, 9, 1))

    recovery = asyncio.create_task(service._recover_mews_after_cutoff_once(now, calendar))
    first = asyncio.create_task(service.ensure_mews_for_selection_trigger(now))
    second = asyncio.create_task(service.ensure_mews_for_selection_trigger(now))
    await asyncio.wait_for(source.entered.wait(), timeout=1.0)
    await asyncio.sleep(0)
    # Exactly one overlapping raw attempt across the scheduler tick and both
    # triggers.
    assert source.calls == 1

    gate.set()
    results = await asyncio.wait_for(
        asyncio.gather(recovery, first, second),
        timeout=1.0,
    )
    assert results == [True, True, True]
    assert source.calls == 1
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_failed_for is None
    assert service._mews_singleflight_task is None


@pytest.mark.asyncio
async def test_after_cutoff_recovery_and_triggers_share_one_singleflight_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingSource:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            raise MewsSnapshotSourceError("Tushare margin is missing SSE or SZSE")

    repository = _AfterCutoffMewsRepository()
    source = _FailingSource()
    now = datetime(2026, 9, 1, 14, 4, tzinfo=TZ)
    service = _singleflight_service(monkeypatch, repository, source, now)
    alerts = _alert_recorder(monkeypatch, service)
    calendar = (date(2026, 8, 31), date(2026, 9, 1))

    results = await asyncio.gather(
        service._recover_mews_after_cutoff_once(now, calendar),
        service.ensure_mews_for_selection_trigger(now),
        service.ensure_mews_for_selection_trigger(now),
    )

    # One overlapping raw attempt, one shared failure, one daily idempotent
    # alert; the scheduler lane latches against a tight retry loop.
    assert results == [False, False, False]
    assert source.calls == 1
    assert repository.payloads == []
    assert len(alerts) == 1
    assert alerts[0]["code"] == "MEWS_CALCULATION_FAILED"
    assert service._mews_failed_for == date(2026, 9, 1)
    assert service._mews_singleflight_task is None

    # The latch only stops scheduler looping; it must not gate a manual
    # trigger, which retries a fresh attempt without doubling the alert.
    assert await service._recover_mews_after_cutoff_once(now, calendar) is False
    assert source.calls == 1
    assert await service.ensure_mews_for_selection_trigger(now) is False
    assert source.calls == 2
    assert len(alerts) == 1


@pytest.mark.asyncio
async def test_0910_scheduler_entry_calculates_and_caches_through_singleflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Source:
        def __init__(self) -> None:
            self.calls = []

        async def fetch_snapshot(self, *, source_trade_date, availability_date):
            self.calls.append((source_trade_date, availability_date))
            return _late_mews_payload()

    repository = _AfterCutoffMewsRepository()
    source = _Source()
    now = datetime(2026, 9, 1, 9, 10, tzinfo=TZ)
    service = _singleflight_service(monkeypatch, repository, source, now)
    alerts = _alert_recorder(monkeypatch, service)
    service._calendar_provider = lambda: asyncio.sleep(
        0, result=[date(2026, 8, 31), date(2026, 9, 1)]
    )

    scheduler = asyncio.create_task(service._run_mews_cache_scheduler())
    for _ in range(200):
        if repository.payloads:
            break
        await asyncio.sleep(0.005)
    service._stop_event.set()
    await asyncio.wait_for(scheduler, timeout=2.0)

    assert source.calls == [(date(2026, 8, 31), date(2026, 9, 1))]
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_snapshot_id == "mews-v2-2026-08-31-late"
    assert service._lane_health["mews_cache"].last_error is None
    assert alerts == []
    assert service._mews_singleflight_task is None


@pytest.mark.asyncio
@pytest.mark.parametrize("wall", [time(9, 5), time(9, 39), time(14, 4)])
async def test_selection_trigger_calculates_without_cutoff_eligibility_at_any_wall_time(
    monkeypatch: pytest.MonkeyPatch,
    wall: time,
) -> None:
    class _Source:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            return _late_mews_payload()

    repository = _AfterCutoffMewsRepository()
    source = _Source()
    now = datetime.combine(date(2026, 9, 1), wall, tzinfo=TZ)
    service = _singleflight_service(monkeypatch, repository, source, now)

    # 09:05, 09:39 and 14:04 all share the same awaited singleflight behavior;
    # the repository's eligibility probe returns False here, and the trigger
    # calculation deliberately does not use cutoff eligibility.
    assert await service.ensure_mews_for_selection_trigger(now) is True
    assert source.calls == 1
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_singleflight_task is None


_ENTRY_CYCLE_DATE = date(2026, 9, 1)
_ENTRY_CYCLE_NOW = datetime(2026, 9, 1, 9, 39, 30, tzinfo=TZ)


def _entry_cycle_bundle(trade_date: date) -> CanonicalV16ScanBundle:
    labels = ("09:25", "09:30", *(f"09:{minute:02d}" for minute in range(31, 40)))
    early_bars = {
        code: tuple(_bar(code, label, trade_date=trade_date) for label in labels)
        for code in _LATE_REPLAY_CODES
    }
    return CanonicalV16ScanBundle(
        trade_date=trade_date,
        scan_result=_late_replay_scan_result(),
        stock_data={code: SimpleNamespace(volume_937=900.0) for code in _LATE_REPLAY_CODES},
        clean_boards={},
        universe=tuple(sorted(_LATE_REPLAY_CODES)),
        quotes={},
        prev_closes={code: 10.0 for code in _LATE_REPLAY_CODES},
        history_raw={},
        early_bars=early_bars,
        early_source_hashes={
            code: sha256_json([_bar_payload(bar) for bar in bars])
            for code, bars in early_bars.items()
        },
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="a" * 64,
        feature_list_sha256="b" * 64,
        computed_at=datetime(2026, 9, 1, 9, 39, 30, tzinfo=TZ),
        input_hash="c" * 64,
        _integrity_hash="",
        computation_calendar=(
            date(2026, 8, 31),
            trade_date,
            date(2026, 9, 2),
            date(2026, 9, 3),
        ),
        prior_trade_date=date(2026, 8, 31),
    )


class _EntryCycleRepository(_AfterCutoffMewsRepository):
    async def get_entry_status(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def record_minute_bars(self, rows):
        return frozenset(sha256_json(dict(row)) for row in rows)


def _entry_cycle_service(
    monkeypatch: pytest.MonkeyPatch,
    repository: Any,
    source: Any,
    order: list[str],
) -> V20Service:
    service = _singleflight_service(monkeypatch, repository, source, _ENTRY_CYCLE_NOW)

    async def recorded_compute(*_args: Any, **_kwargs: Any) -> CanonicalV16ScanBundle:
        order.append("canonical-compute")
        return _entry_cycle_bundle(_ENTRY_CYCLE_DATE)

    monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", recorded_compute)
    return service


@pytest.mark.asyncio
async def test_entry_collection_kicks_mews_without_blocking_canonical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    order: list[str] = []

    class _Source:
        def __init__(self) -> None:
            self.calls = 0
            self.entered = asyncio.Event()

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            order.append("mews-fetch")
            self.entered.set()
            await gate.wait()
            return _late_mews_payload()

    repository = _EntryCycleRepository()

    original_record = repository.record_mews_snapshot

    async def recorded_persist(payload: Mapping[str, Any]) -> None:
        order.append("mews-persist")
        await original_record(payload)

    repository.record_mews_snapshot = recorded_persist  # type: ignore[method-assign]
    source = _Source()
    service = _entry_cycle_service(monkeypatch, repository, source, order)
    context = _DayContext(
        trade_date=_ENTRY_CYCLE_DATE,
        calendar=(
            date(2026, 8, 31),
            _ENTRY_CYCLE_DATE,
            date(2026, 9, 2),
            date(2026, 9, 3),
        ),
    )

    # Entry must finish while the independently managed MEWS provider is still
    # blocked.  This is the production trigger path, not a fake direct compute.
    await asyncio.wait_for(
        service._run_entry_collection_cycle(context, _ENTRY_CYCLE_NOW),
        timeout=1.0,
    )
    await asyncio.wait_for(source.entered.wait(), timeout=1.0)

    assert context.canonical_bundle is not None
    assert context.last_phase == "CANONICAL_0939_READY"
    assert service._lane_health["decision"].last_error is None
    assert order.count("canonical-compute") == 1
    assert source.calls == 1
    assert repository.payloads == []
    master = service._mews_singleflight_task
    assert master is not None and not master.done()
    trigger_tasks = tuple(service._mews_trigger_tasks)
    assert len(trigger_tasks) == 1
    assert all(not task.done() for task in trigger_tasks)

    # Releasing the provider settles the owned task, persists exactly once,
    # and leaves neither a singleflight master nor a trigger-task orphan.
    gate.set()
    assert await asyncio.wait_for(
        asyncio.gather(*trigger_tasks),
        timeout=1.0,
    ) == [True]
    await asyncio.sleep(0)
    assert order[-1] == "mews-persist"
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == _ENTRY_CYCLE_DATE
    assert service._mews_singleflight_task is None
    assert service._mews_trigger_tasks == set()


@pytest.mark.asyncio
async def test_entry_collection_continues_after_genuine_mews_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    order: list[str] = []

    class _Source:
        def __init__(self) -> None:
            self.calls = 0
            self.entered = asyncio.Event()

        async def fetch_snapshot(self, **_kwargs):
            self.calls += 1
            self.entered.set()
            await gate.wait()
            raise MewsSnapshotSourceError("Tushare margin is missing SSE or SZSE")

    repository = _EntryCycleRepository()
    source = _Source()
    service = _entry_cycle_service(monkeypatch, repository, source, order)
    alerts = _alert_recorder(monkeypatch, service)
    context = _DayContext(
        trade_date=_ENTRY_CYCLE_DATE,
        calendar=(
            date(2026, 8, 31),
            _ENTRY_CYCLE_DATE,
            date(2026, 9, 2),
            date(2026, 9, 3),
        ),
    )

    # A genuine MEWS failure settles one idempotent daily alert and the
    # independent entry computation continues — never ENTRY_COLLECTION_FAILED.
    await asyncio.wait_for(
        service._run_entry_collection_cycle(context, _ENTRY_CYCLE_NOW),
        timeout=1.0,
    )
    await asyncio.wait_for(source.entered.wait(), timeout=1.0)
    assert order == ["canonical-compute"]
    assert source.calls == 1
    assert repository.payloads == []
    assert alerts == []
    assert context.canonical_bundle is not None
    assert context.last_phase == "CANONICAL_0939_READY"
    assert service._lane_health["decision"].last_error is None
    trigger_tasks = tuple(service._mews_trigger_tasks)
    assert len(trigger_tasks) == 1
    assert service._mews_singleflight_task is not None

    gate.set()
    assert await asyncio.wait_for(
        asyncio.gather(*trigger_tasks),
        timeout=1.0,
    ) == [False]
    await asyncio.sleep(0)
    assert len(alerts) == 1
    assert alerts[0]["code"] == "MEWS_CALCULATION_FAILED"
    assert alerts[0]["entity_id"] == _ENTRY_CYCLE_DATE.isoformat()
    assert "SELECTION_TRIGGER" in alerts[0]["message"]
    assert "MewsSnapshotSourceError" in alerts[0]["message"]
    assert not any(alert["code"] == "ENTRY_COLLECTION_FAILED" for alert in alerts)
    assert service._mews_singleflight_task is None
    assert service._mews_trigger_tasks == set()


@pytest.mark.asyncio
async def test_entry_collection_cycles_share_one_mews_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gate = asyncio.Event()
    order: list[str] = []
    repository = _EntryCycleRepository()
    source = _GatedMewsSource(gate)
    service = _entry_cycle_service(monkeypatch, repository, source, order)

    def new_context() -> _DayContext:
        return _DayContext(
            trade_date=_ENTRY_CYCLE_DATE,
            calendar=(
                date(2026, 8, 31),
                _ENTRY_CYCLE_DATE,
                date(2026, 9, 2),
                date(2026, 9, 3),
            ),
        )

    first_context = new_context()
    second_context = new_context()
    first = asyncio.create_task(
        service._run_entry_collection_cycle(first_context, _ENTRY_CYCLE_NOW)
    )
    second = asyncio.create_task(
        service._run_entry_collection_cycle(second_context, _ENTRY_CYCLE_NOW)
    )
    await asyncio.wait_for(source.entered.wait(), timeout=1.0)
    # Both entry cycles finish canonical work without waiting for the one shared
    # raw MEWS attempt.
    await asyncio.wait_for(asyncio.gather(first, second), timeout=1.0)
    assert source.calls == 1
    assert first_context.canonical_bundle is not None
    assert second_context.canonical_bundle is not None
    assert repository.payloads == []
    trigger_tasks = tuple(service._mews_trigger_tasks)
    assert len(trigger_tasks) == 2
    assert service._mews_singleflight_task is not None

    gate.set()
    assert await asyncio.wait_for(
        asyncio.gather(*trigger_tasks),
        timeout=1.0,
    ) == [True, True]
    await asyncio.sleep(0)
    # Exactly one overlapping raw MEWS attempt fed both entry cycles.
    assert source.calls == 1
    assert len(repository.payloads) == 1
    assert order == ["canonical-compute", "canonical-compute"]
    assert service._mews_cached_for == _ENTRY_CYCLE_DATE
    assert service._mews_singleflight_task is None
    assert service._mews_trigger_tasks == set()


@pytest.mark.asyncio
async def test_live_exit_stage_with_expired_deadline_never_invokes_the_factory() -> None:
    """An exhausted tick deadline raises before any DB/provider coroutine exists."""
    service = V20Service.__new__(V20Service)
    loop = asyncio.get_running_loop()
    tick_started_at = loop.time() - 5.0
    factory_calls = 0

    def provider_factory() -> Any:
        nonlocal factory_calls
        factory_calls += 1

        async def _operation() -> None:
            return None

        return _operation()

    with pytest.raises(V20LiveExitStageTimeout) as exc_info:
        await service._run_live_exit_stage(
            provider_factory,
            stage="latest",
            stage_cap=2.0,
            deadline=loop.time() - 0.001,
            tick_started_at=tick_started_at,
            symbols=("600000",),
            provider="tushare_rt",
        )
    assert exc_info.value.stage == "latest"
    assert exc_info.value.provider == "tushare_rt"
    # The provider was never called and no coroutine/orphan was created.
    assert factory_calls == 0

    # A live deadline still runs the stage exactly once and returns its value.
    async def _answer() -> str:
        return "rows"

    result = await service._run_live_exit_stage(
        lambda: _answer(),
        stage="latest",
        stage_cap=2.0,
        deadline=loop.time() + 5.0,
        tick_started_at=tick_started_at,
        symbols=("600000",),
        provider="tushare_rt",
    )
    assert result == "rows"
    assert factory_calls == 0


@pytest.mark.asyncio
async def test_cleanup_singleflight_awaits_scheduler_and_allows_cancellation() -> None:
    class Realtime:
        stop_calls = 0

        async def stop(self) -> None:
            await asyncio.sleep(0)
            self.stop_calls += 1

    class Scheduler:
        cancelled = False

        async def run(self) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    realtime = Realtime()
    scheduler = Scheduler()
    state = V15ScanState(
        initialized=True,
        resource_owner="V16",
        realtime_client=realtime,
    )
    state.scheduler_task = asyncio.create_task(scheduler.run())
    owner = asyncio.create_task(cleanup_scan_resources(state, owner="V16"))
    await asyncio.sleep(0)
    borrower = asyncio.create_task(cleanup_scan_resources(state, owner="V16"))
    await asyncio.sleep(0)
    borrower.cancel()
    with pytest.raises(asyncio.CancelledError):
        await borrower

    await owner
    assert scheduler.cancelled is True
    assert state.scheduler_task is None
    assert realtime.stop_calls == 1
    assert state.initialized is False


async def test_embedded_initializer_failure_preserves_shared_fundamentals_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module

    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "persisted-v16-token")

    class _Realtime:
        instance: _Realtime | None = None

        def __init__(self, *, token: str) -> None:
            assert token == "persisted-v16-token"
            self.stop_calls = 0
            type(self).instance = self

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            self.stop_calls += 1

    class _BrokenHistorical:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("historical adapter failed")

    class _SharedFundamentals:
        def __init__(self) -> None:
            self.connect_calls = 0
            self.close_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", _Realtime)
    monkeypatch.setattr(historical_module, "IQuantHistoricalAdapter", _BrokenHistorical)
    fundamentals = _SharedFundamentals()
    state = V15ScanState(fundamentals_db=fundamentals)

    with pytest.raises(RuntimeError, match="historical adapter failed"):
        await _init_embedded_v20_scan_resources(state)

    assert _Realtime.instance is not None
    assert _Realtime.instance.stop_calls == 1
    assert fundamentals.connect_calls == 0
    assert fundamentals.close_calls == 0
    assert state.realtime_client is None
    assert state.initialized is False


async def test_cancelling_embedded_initializer_waiter_does_not_cancel_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import tushare_realtime as realtime_module

    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "persisted-v16-token")
    start_entered = asyncio.Event()
    release_start = asyncio.Event()

    class _Realtime:
        instance: _Realtime | None = None

        def __init__(self, *, token: str) -> None:
            assert token == "persisted-v16-token"
            self.stop_calls = 0
            type(self).instance = self

        async def start(self) -> None:
            start_entered.set()
            await release_start.wait()

        async def stop(self) -> None:
            self.stop_calls += 1

        def as_ifind_format(self, *_args: object, **_kwargs: object) -> dict:
            return {}

    class _SharedFundamentals:
        def __init__(self) -> None:
            self.connect_calls = 0
            self.close_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1

        async def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", _Realtime)
    fundamentals = _SharedFundamentals()
    state = V15ScanState(fundamentals_db=fundamentals)
    owner = asyncio.create_task(_init_embedded_v20_scan_resources(state))
    await asyncio.wait_for(start_entered.wait(), timeout=1.0)
    borrower = asyncio.create_task(_init_embedded_v20_scan_resources(state))
    await asyncio.sleep(0)

    borrower.cancel()
    with pytest.raises(asyncio.CancelledError):
        await borrower

    assert _Realtime.instance is not None
    assert _Realtime.instance.stop_calls == 0
    assert fundamentals.connect_calls == 0
    assert fundamentals.close_calls == 0

    release_start.set()
    await owner
    assert _Realtime.instance.stop_calls == 0
    assert state.realtime_client is _Realtime.instance
    assert state.initialized is True


async def test_embedded_tushare_start_failure_is_rolled_back_without_shared_pool_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import tushare_realtime as realtime_module

    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "persisted-v16-token")

    class _Realtime:
        instance: _Realtime | None = None

        def __init__(self, *, token: str) -> None:
            assert token == "persisted-v16-token"
            self.stop_calls = 0
            type(self).instance = self

        async def start(self) -> None:
            raise RuntimeError("Tushare start failed after allocation")

        async def stop(self) -> None:
            self.stop_calls += 1

    class _SharedFundamentals:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", _Realtime)
    fundamentals = _SharedFundamentals()
    state = V15ScanState(fundamentals_db=fundamentals)

    with pytest.raises(RuntimeError, match="Tushare start failed"):
        await _init_embedded_v20_scan_resources(state)

    assert _Realtime.instance is not None
    assert _Realtime.instance.stop_calls == 1
    assert fundamentals.close_calls == 0
    assert state.realtime_client is None
    assert state.initialized is False


def _entry_status(config, *, action: str = "BLOCK") -> EntryStatus:
    snapshot = {
        "schema_version": V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
        "v16_snapshot_schema_version": V20_V16_SNAPSHOT_SCHEMA,
        "state_semantics_hash": config.state_semantics_hash,
        "comparison_pool_codes": ["000001", "000002"],
        "symbols": [{"code": "000001"}],
    }
    semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "strategy_version": config.strategy_version,
        "config_hash": config.config_hash,
        "official_stream_id": config.official_stream_id,
        "state_lineage_id": config.state_lineage_id,
        "state_semantics_hash": config.state_semantics_hash,
        "action": action,
    }
    return EntryStatus(
        official_stream_id=config.official_stream_id,
        trade_date=date(2026, 8, 31),
        slot_id="slot",
        slot_status="COMPLETED",
        slot_revision=1,
        strategy_version=config.strategy_version,
        config_id=config.config_hash[:24],
        config_hash=config.config_hash,
        lineage_id=config.state_lineage_id,
        decision_id="decision",
        event_id="event",
        action=action,
        final_multiplier=0.0,
        semantic_content_hash=sha256_json(semantic),
        semantic=semantic,
        snapshot_id="snapshot",
        snapshot_hash=sha256_json(snapshot),
        snapshot=snapshot,
        action_expiry_ts=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
    )


def test_entry_binding_rejects_legacy_semantic_and_snapshot_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    current = _entry_status(service.config)
    service._verify_entry_binding(current)

    invalid_semantic = {**current.semantic, "action": "INPUT_INVALID"}
    invalid_snapshot = {
        "schema_version": V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
        "state_semantics_hash": service.config.state_semantics_hash,
    }
    service._verify_entry_binding(
        replace(
            current,
            action="INPUT_INVALID",
            semantic=invalid_semantic,
            semantic_content_hash=sha256_json(invalid_semantic),
            snapshot=invalid_snapshot,
            snapshot_hash=sha256_json(invalid_snapshot),
        )
    )

    legacy_semantic = {**current.semantic, "schema_version": "v20-entry-semantic/v1"}
    with pytest.raises(V20ConfigError, match="semantic contract is incompatible"):
        service._verify_entry_binding(
            replace(
                current,
                semantic=legacy_semantic,
                semantic_content_hash=sha256_json(legacy_semantic),
            )
        )

    legacy_snapshot = {
        **current.snapshot,
        "schema_version": "v20-decision-input-snapshot/v1",
    }
    with pytest.raises(V20ConfigError, match="snapshot contract is incompatible"):
        service._verify_entry_binding(
            replace(
                current,
                snapshot=legacy_snapshot,
                snapshot_hash=sha256_json(legacy_snapshot),
            )
        )


def test_entry_binding_accepts_historical_terminal_explicit_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    current = _entry_status(service.config)
    historical_config_hash = "c" * 64
    historical_state_hash = "d" * 64
    semantic = {
        **current.semantic,
        "config_hash": historical_config_hash,
        "state_semantics_hash": historical_state_hash,
    }
    snapshot = {
        **current.snapshot,
        "state_semantics_hash": historical_state_hash,
    }
    historical = replace(
        current,
        config_id=historical_config_hash[:24],
        config_hash=historical_config_hash,
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
    )
    service._verify_entry_binding(historical)

    with pytest.raises(V20ConfigError, match="another config/lineage"):
        service._verify_entry_binding(replace(historical, slot_status="OPEN"))

    tampered_semantic = {**semantic, "config_hash": "e" * 64}
    with pytest.raises(V20ConfigError, match="another config/lineage"):
        service._verify_entry_binding(
            replace(
                historical,
                semantic=tampered_semantic,
                semantic_content_hash=sha256_json(tampered_semantic),
            )
        )


async def test_enabled_start_wires_all_runtime_lanes_and_stop_releases_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        def __init__(self) -> None:
            self.connected = False
            self.closed = False
            self.probes_started = 0
            self.probe_block = asyncio.Event()

        async def connect(self) -> None:
            self.connected = True

        async def acquire_runtime_leader(self, **_kwargs) -> None:
            return None

        async def register_config(self, **_kwargs) -> None:
            return None

        async def ensure_genesis_state(self, *_args, **_kwargs) -> None:
            return None

        async def load_state(self, lineage_id):
            return StateRecord(
                lineage_id=lineage_id,
                revision=0,
                state_hash="a" * 64,
                payload={},
            )

        async def get_outbox_health(self, **_kwargs):
            return {"delivery_error_n": 0}

        async def assert_runtime_leader(self) -> None:
            self.probes_started += 1
            await self.probe_block.wait()

        async def close(self) -> None:
            self.closed = True

    class _Publisher:
        async def run(
            self,
            stop_event: asyncio.Event,
            *,
            before_cycle=None,
            on_cycle_success=None,
            on_cycle_error=None,
        ) -> None:
            del on_cycle_error
            assert before_cycle is not None
            await before_cycle()
            if on_cycle_success is not None:
                on_cycle_success()
            await stop_event.wait()

    repository = _Repository()
    resources_started = False
    resources_stopped = False

    async def initialize(state: V15ScanState) -> None:
        nonlocal resources_started
        resources_started = True
        state.initialized = True

    async def cleanup(state: V15ScanState) -> None:
        nonlocal resources_stopped
        resources_stopped = True
        state.initialized = False

    config = _config(monkeypatch)
    artifacts = load_g_artifacts(
        config.artifact_manifest_path.parent,
        expected_manifest_sha256=config.artifact_manifest_sha256,
    )
    route = SimpleNamespace(
        chat_id="shadow-chat",
        app_id="shadow-app",
        app_secret="shadow-secret",
        destination_fingerprint=config.route_binding.destination_fingerprint,
        is_configured=lambda: True,
    )
    service = V20Service(
        config=config,
        repository=repository,
        scan_state=V15ScanState(),
        artifacts=artifacts,
        publisher=_Publisher(),
        routes={config.route_id: route},
        initialize_resources=initialize,
        cleanup_resources=cleanup,
        mews_source=_UnusedMewsSource(),
    )

    await service.start()
    for _ in range(20):
        if repository.probes_started >= 6:
            break
        await asyncio.sleep(0)

    assert repository.connected is True
    assert resources_started is True
    assert repository.probes_started == 6
    assert {task.get_name() for task in service._tasks} == {
        "v20-decision-scheduler",
        "v20-live-exit-scheduler",
        "v20-stale-exit-scheduler",
        "v20-outbox-recovery-scheduler",
        "v20-outbox-publisher",
        "v20-mews-cache-scheduler",
        "v20-rolling7-recovery-scheduler",
    }
    assert all(not task.done() for task in service._tasks)
    assert service.startup_stage == "RUNNING"

    sampled_at = service._aware_now()
    for lane_name in service._lane_health:
        service._record_lane_success(lane_name, sampled_at)
    await service._refresh_status_snapshot()
    assert (await service.status())["healthy"] is True
    await service._require_manual_trigger_ready()

    await service.stop()

    assert resources_stopped is True
    assert repository.closed is True
    assert service._tasks == []
    assert service.startup_stage == "STOPPED"


async def test_enabled_start_requires_local_mews_calculator_before_database_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        connect_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1

    repository = _Repository()
    service = _service(monkeypatch, repository)
    service._mews_source = None
    route = SimpleNamespace(
        chat_id="shadow-chat",
        app_id="shadow-app",
        app_secret="shadow-secret",
        destination_fingerprint=service.config.route_binding.destination_fingerprint,
        is_configured=lambda: True,
    )
    service._routes = {service.config.route_id: route}

    with pytest.raises(V20ConfigError, match="local MEWS calculator is required"):
        await service.start()

    assert repository.connect_calls == 0
    assert service.startup_stage == "VALIDATING_RUNTIME"


@pytest.mark.parametrize("failure", ["api_key", "destination", "fundamentals_ca"])
async def test_startup_security_binding_fails_before_database_connect(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    class _Repository:
        connect_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1

    repository = _Repository()
    service = _service(monkeypatch, repository)
    fingerprint = service.config.route_binding.destination_fingerprint
    if failure == "api_key":
        monkeypatch.delenv("V20_STATUS_API_KEY")
    elif failure == "destination":
        fingerprint = "f" * 64
    else:
        monkeypatch.setenv("DB_SSLROOTCERT_SHA256", "f" * 64)
    route = SimpleNamespace(
        chat_id="shadow-chat",
        app_id="shadow-app",
        app_secret="shadow-secret",
        destination_fingerprint=fingerprint,
        is_configured=lambda: True,
    )
    service._routes = {service.config.route_id: route}

    with pytest.raises(V20ConfigError):
        await service.start()

    assert repository.connect_calls == 0


async def test_embedded_start_reaches_legacy_database_without_v20_api_or_ca_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        connect_calls = 0

        async def connect(self) -> None:
            self.connect_calls += 1
            raise RuntimeError("embedded database probe")

    repository = _Repository()
    service = _service(monkeypatch, repository)
    service._embedded_legacy = True
    monkeypatch.delenv("V20_STATUS_API_KEY", raising=False)
    monkeypatch.delenv("V20_INGEST_API_KEY", raising=False)
    monkeypatch.delenv("DB_SSLROOTCERT_SHA256", raising=False)
    service._routes = {
        service.config.route_id: SimpleNamespace(
            chat_id="legacy-chat",
            app_id="legacy-app",
            app_secret="legacy-secret",
            destination_fingerprint=service.config.route_binding.destination_fingerprint,
            is_configured=lambda: True,
        )
    }

    with pytest.raises(RuntimeError, match="embedded database probe"):
        await service.start()

    assert repository.connect_calls == 1
    assert service.startup_stage == "CONNECTING_LEDGER"


class _AckRepository:
    def __init__(self) -> None:
        self.kwargs: dict[str, Any] | None = None

    async def record_reminder_stop_ack(self, event_id, consumer_id, **kwargs):
        self.kwargs = {"event_id": event_id, "consumer_id": consumer_id, **kwargs}
        return True


async def test_ack_uses_server_owned_auth_evidence_without_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _AckRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True

    result = await service.record_reminder_stop_ack(
        {
            "ack_id": "ack-1",
            "original_exit_event_id": "exit-event",
            "consumer_id": "operator",
            "ack_ts": "2026-08-31T10:00:00+08:00",
        }
    )

    assert result == {"ack_id": "ack-1", "accepted": True, "created": True}
    assert repository.kwargs is not None
    assert repository.kwargs["ack_id"] == "ack-1"
    assert len(repository.kwargs["auth_evidence_hash"]) == 64
    assert "secret" not in str(repository.kwargs)


class _ManualTriggerRepository:
    def __init__(
        self,
        entry_status: EntryStatus | None = None,
        *,
        leadership_error: Exception | None = None,
    ) -> None:
        self.entry_status = entry_status
        self.leadership_error = leadership_error
        self.events: dict[str, OutboxRecord] = {}
        self.leader_calls = 0
        self.entry_status_calls = 0
        self.enqueue_calls = 0
        self.seal_calls = 0

    async def assert_runtime_leader(self) -> None:
        self.leader_calls += 1
        if self.leadership_error is not None:
            raise self.leadership_error

    async def get_outbox_event(self, event_id: str, **_kwargs) -> OutboxRecord | None:
        return self.events.get(event_id)

    async def get_entry_status(
        self,
        _official_stream_id: str,
        _trade_date: date,
    ) -> EntryStatus | None:
        self.entry_status_calls += 1
        return self.entry_status

    async def enqueue_alert(
        self,
        event_id: str,
        route_id: str,
        semantic: dict[str, Any],
        semantic_hash: str,
        *,
        official_stream_id: str,
        lineage_id: str,
    ) -> bool:
        self.enqueue_calls += 1
        if event_id in self.events:
            return False
        assert sha256_json(semantic) == semantic_hash
        self.events[event_id] = OutboxRecord(
            event_id=event_id,
            event_type="DATA_ALERT",
            route_id=route_id,
            official_stream_id=official_stream_id,
            lineage_id=lineage_id,
            semantic=dict(semantic),
            semantic_content_hash=semantic_hash,
            payload=None,
            payload_hash=None,
            generated_at=None,
            commit_marker=None,
            action_expiry_ts=None,
            delivery_status="PENDING",
            attempt_count=0,
        )
        return True

    async def seal_event(self, event_id: str, builder) -> OutboxRecord:
        self.seal_calls += 1
        current = self.events[event_id]
        if current.payload is not None:
            return current
        generated_at = datetime(2026, 8, 31, 10, 1, tzinfo=TZ)
        payload = dict(builder(current, generated_at, 91, True))
        sealed = replace(
            current,
            payload=payload,
            payload_hash=sha256_json(payload),
            generated_at=generated_at,
            commit_marker=91,
        )
        self.events[event_id] = sealed
        return sealed


def _rich_entry_status(config) -> EntryStatus:
    current = _entry_status(config, action="ENTER")
    semantic = {
        **current.semantic,
        "health_state": "HEALTHY",
        "rolling7_state": "GOOD",
        "g_state": "PASS",
        "reason_codes": ["BASE_HEALTHY", "ROLLING7_GOOD"],
        "symbols": [
            {
                "rank": 1,
                "code": "000001",
                "name": "平安银行",
                "score": 0.81234,
                "snapshot_price": 10.26,
                "boards": ["银行", "高股息"],
            }
        ],
    }
    return replace(
        current,
        action="ENTER",
        final_multiplier=1.0,
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
    )


def _arm_manual_trigger_runtime(service: V20Service) -> list[asyncio.Task[Any]]:
    service._repository_started = True
    service._started = True
    service._stop_event.clear()
    blocker = asyncio.Event()
    tasks = [
        asyncio.create_task(blocker.wait(), name=task_name)
        for task_name in sorted(service_module.V20_RUNTIME_TASK_NAMES)
    ]
    service._tasks = tasks
    sampled_at = service._aware_now()
    for lane_name in service._lane_health:
        service._record_lane_success(lane_name, sampled_at)
    service._status_snapshot = {
        "sampled_at": sampled_at,
        "ledger": {},
        "outbox": {"delivery_error_n": 0},
    }
    service._status_snapshot_error = None
    return tasks


async def _disarm_manual_trigger_runtime(
    service: V20Service,
    tasks: list[asyncio.Task[Any]],
) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    service._tasks = []


async def test_manual_trigger_after_cutoff_only_copies_frozen_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    repository.entry_status = _rich_entry_status(service.config)
    service._clock = lambda: datetime(2026, 8, 31, 10, 1, tzinfo=TZ)
    tasks = _arm_manual_trigger_runtime(service)
    decision_calls = 0

    async def forbidden_late_decision(_now: datetime) -> None:
        nonlocal decision_calls
        decision_calls += 1
        raise AssertionError("post-cutoff manual trigger must not run a decision cycle")

    monkeypatch.setattr(service, "_run_decision_iteration_with_cutoff", forbidden_late_decision)
    try:
        first = await service.trigger_manual_scan("deploy-20260831-001")
        second = await service.trigger_manual_scan("deploy-20260831-001")
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert decision_calls == 0
    assert first["accepted"] is True
    assert first["created"] is True
    assert first["formal_decision_available"] is True
    assert first["entry_action"] == "ENTER"
    assert first["official_state_changed"] is False
    assert first["manual_notice_actionable"] is False
    assert first["feishu_delivery_confirmed"] is False
    assert second == {**first, "created": False}
    assert repository.enqueue_calls == 1
    assert repository.seal_calls == 1
    record = next(iter(repository.events.values()))
    assert record.action_expiry_ts is None
    assert record.semantic["delivery_priority_class"] == "OPERATOR_NOTIFICATION"
    assert "正式冻结 V16 票单（1只）" in str(record.semantic["message"])
    assert "000001 平安银行" in str(record.semantic["message"])
    assert "人工触发回执｜非交易指令" in str(record.payload["message"])
    assert "现在操作：不开仓，不补买，不追买" in str(record.payload["message"])
    assert "早盘正式记录：曾给出开仓建议，现已过期" in str(record.payload["message"])


async def test_manual_trigger_after_cutoff_never_backfills_missing_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    service._clock = lambda: datetime(2026, 8, 31, 15, 30, tzinfo=TZ)
    tasks = _arm_manual_trigger_runtime(service)
    decision_calls = 0

    async def forbidden_late_decision(_now: datetime) -> None:
        nonlocal decision_calls
        decision_calls += 1

    monkeypatch.setattr(service, "_run_decision_iteration_with_cutoff", forbidden_late_decision)
    try:
        result = await service.trigger_manual_scan("deploy-20260831-002")
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert decision_calls == 0
    assert result["formal_decision_available"] is False
    assert result["cycle_result"] == "CUTOFF_WITHOUT_DURABLE_DECISION"
    record = next(iter(repository.events.values()))
    assert "exact-09:39 正式结果" in str(record.semantic["message"])
    assert "不会使用晚到行情补算" in str(record.semantic["message"])


async def test_manual_trigger_after_cutoff_reports_independent_0939_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    current = _entry_status(service.config)
    invalid_semantic = {
        **current.semantic,
        "action": "INPUT_INVALID",
        "state_after_hash": "a" * 64,
        "policy_input_hash": "b" * 64,
        "scheduled_exits_today": [],
    }
    invalid_snapshot = {
        "schema_version": V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
        "state_semantics_hash": service.config.state_semantics_hash,
    }
    invalid_status = replace(
        current,
        slot_status="FAILED",
        action="INPUT_INVALID",
        semantic=invalid_semantic,
        semantic_content_hash=sha256_json(invalid_semantic),
        snapshot=invalid_snapshot,
        snapshot_hash=sha256_json(invalid_snapshot),
    )
    repository.entry_status = invalid_status
    service._clock = lambda: datetime(2026, 8, 31, 15, 30, tzinfo=TZ)
    service._context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)),
        entry_status=invalid_status,
    )
    replay_semantic = {
        "event_id": "late-replay-event",
        "replay_action": "ENTER",
        "final_multiplier": 1.0,
    }
    replay_record = OutboxRecord(
        event_id="late-replay-event",
        event_type="DATA_ALERT",
        route_id=service.config.route_id,
        official_stream_id=service.config.official_stream_id,
        lineage_id=service.config.state_lineage_id,
        semantic=replay_semantic,
        semantic_content_hash=sha256_json(replay_semantic),
        payload={"message": "late replay"},
        payload_hash=sha256_json({"message": "late replay"}),
        generated_at=datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
        commit_marker=92,
        action_expiry_ts=None,
        delivery_status="PENDING",
        attempt_count=0,
    )
    replay_calls = 0

    async def replay(_context: _DayContext, _now: datetime) -> OutboxRecord:
        nonlocal replay_calls
        replay_calls += 1
        return replay_record

    async def forbidden_late_decision(_now: datetime) -> None:
        raise AssertionError("late replay must not run the official decision lane")

    monkeypatch.setattr(service, "_ensure_late_0939_replay", replay)
    monkeypatch.setattr(service, "_run_decision_iteration_with_cutoff", forbidden_late_decision)
    tasks = _arm_manual_trigger_runtime(service)
    try:
        result = await service.trigger_manual_scan("deploy-20260831-replay")
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert replay_calls == 1
    assert repository.entry_status is invalid_status
    assert result["cycle_result"] == "LATE_0939_REPLAY_READY"
    assert result["entry_action"] == "INPUT_INVALID"
    assert result["official_state_changed"] is False
    assert result["late_0939_replay_available"] is True
    assert result["late_0939_replay_event_id"] == "late-replay-event"
    assert result["late_0939_replay_action"] == "ENTER"
    assert result["late_0939_replay_multiplier"] == 1.0
    manual_record = next(iter(repository.events.values()))
    assert "已过期不可追买" in str(manual_record.semantic["message"])


async def test_manual_trigger_inside_window_uses_serialized_official_decision_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    service._clock = lambda: datetime(2026, 8, 31, 9, 39, 5, tzinfo=TZ)
    tasks = _arm_manual_trigger_runtime(service)
    decision_calls: list[datetime] = []

    async def commit_official_decision(now: datetime) -> None:
        assert service._decision_cycle_lock.locked()
        decision_calls.append(now)
        repository.entry_status = _rich_entry_status(service.config)

    monkeypatch.setattr(service, "_run_decision_iteration_with_cutoff", commit_official_decision)
    try:
        result = await service.trigger_manual_scan("deploy-20260831-003")
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert decision_calls == [datetime(2026, 8, 31, 9, 39, 5, tzinfo=TZ)]
    assert result["cycle_result"] == "DECISION_COMMITTED"
    assert result["formal_decision_available"] is True
    assert result["official_state_changed"] is True


async def test_morning_selection_trigger_uses_only_official_entry_message_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    service._clock = lambda: datetime(2026, 8, 31, 9, 39, 5, tzinfo=TZ)
    tasks = _arm_manual_trigger_runtime(service)
    decision_calls: list[datetime] = []

    async def commit_official_decision(now: datetime) -> None:
        assert service._decision_cycle_lock.locked()
        decision_calls.append(now)
        status = _rich_entry_status(service.config)
        repository.entry_status = status
        payload = {"message": "the automatic entry message"}
        repository.events[status.event_id] = OutboxRecord(
            event_id=status.event_id,
            event_type="ENTRY_DECISION",
            route_id=service.config.route_id,
            official_stream_id=service.config.official_stream_id,
            lineage_id=service.config.state_lineage_id,
            semantic=status.semantic,
            semantic_content_hash=status.semantic_content_hash,
            payload=payload,
            payload_hash=sha256_json(payload),
            generated_at=now,
            commit_marker=101,
            action_expiry_ts=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
            delivery_status="PENDING",
            attempt_count=0,
        )

    monkeypatch.setattr(service, "_run_decision_iteration_with_cutoff", commit_official_decision)
    try:
        result = await service.trigger_morning_selection("deploy-20260831-exact")
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert decision_calls == [datetime(2026, 8, 31, 9, 39, 5, tzinfo=TZ)]
    assert result["cycle_result"] == "DECISION_COMMITTED"
    assert result["entry_action"] == "ENTER"
    assert result["exact_automatic_message"] is True
    assert result["retrospective_expired"] is False
    assert result["symbols"] == [
        {
            "rank": 1,
            "code": "000001",
            "name": "平安银行",
            "snapshot_price": 10.26,
        }
    ]
    assert repository.enqueue_calls == 0
    assert set(repository.events) == {repository.entry_status.event_id}


async def test_manual_trigger_rejects_failed_runtime_before_any_side_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    service._started = True

    with pytest.raises(V20RepositoryError, match="not healthy enough"):
        await service.trigger_manual_scan("deploy-20260831-004")

    assert repository.leader_calls == 0
    assert repository.enqueue_calls == 0


@pytest.mark.parametrize("delivery_fault", ("publisher_lane", "outbox_delivery"))
async def test_manual_trigger_readiness_does_not_confuse_delivery_health_with_computation(
    monkeypatch: pytest.MonkeyPatch,
    delivery_fault: str,
) -> None:
    repository = _ManualTriggerRepository(
        leadership_error=V20LeadershipLost("readiness passed and leader probe was reached")
    )
    service = _service(monkeypatch, repository)
    tasks = _arm_manual_trigger_runtime(service)
    now = service._aware_now()
    if delivery_fault == "publisher_lane":
        service._record_lane_error("publisher", "relay unavailable", now)
    else:
        assert service._status_snapshot is not None
        service._status_snapshot = {
            **service._status_snapshot,
            "outbox": {"delivery_error_n": 1},
        }
    try:
        status = await service.status()
        with pytest.raises(V20LeadershipLost, match="leader probe was reached"):
            await service.trigger_manual_scan(f"deploy-20260831-{delivery_fault}")
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert status["healthy"] is False
    assert repository.leader_calls == 1
    assert repository.enqueue_calls == 0


async def test_manual_trigger_rejects_stale_database_status_before_any_side_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    tasks = _arm_manual_trigger_runtime(service)
    now = service._aware_now()
    assert service._status_snapshot is not None
    service._status_snapshot = {
        **service._status_snapshot,
        "sampled_at": now - timedelta(seconds=60),
    }
    try:
        status = await service.status()
        with pytest.raises(V20RepositoryError, match="database status evidence is unavailable"):
            await service.trigger_manual_scan("deploy-20260831-stale-status")
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert status["healthy"] is False
    assert repository.leader_calls == 0
    assert repository.enqueue_calls == 0


@pytest.mark.parametrize(
    "request_id",
    ("short", "-leading-hyphen", "contains space", "x" * 129),
)
async def test_manual_trigger_rejects_invalid_idempotency_key_before_leader_probe(
    monkeypatch: pytest.MonkeyPatch,
    request_id: str,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    tasks = _arm_manual_trigger_runtime(service)
    try:
        with pytest.raises(ValueError, match="Idempotency-Key"):
            await service.trigger_manual_scan(request_id)
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert repository.leader_calls == 0
    assert repository.enqueue_calls == 0


async def test_manual_trigger_leadership_loss_precedes_outbox_side_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ManualTriggerRepository(
        leadership_error=V20LeadershipLost("leader session was replaced")
    )
    service = _service(monkeypatch, repository)
    tasks = _arm_manual_trigger_runtime(service)
    try:
        with pytest.raises(V20LeadershipLost, match="replaced"):
            await service.trigger_manual_scan("deploy-20260831-005")
    finally:
        await _disarm_manual_trigger_runtime(service, tasks)

    assert repository.enqueue_calls == 0
    assert repository.seal_calls == 0


async def test_manual_trigger_does_not_race_busy_automatic_decision_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.web import v20_service as service_module

    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    service._clock = lambda: datetime(2026, 8, 31, 9, 39, 5, tzinfo=TZ)
    tasks = _arm_manual_trigger_runtime(service)
    monkeypatch.setattr(service_module, "MANUAL_TRIGGER_DECISION_LOCK_TIMEOUT_SECONDS", 0.01)
    await service._decision_cycle_lock.acquire()
    try:
        with pytest.raises(V20StateConflict, match="decision lane is busy"):
            await service.trigger_manual_scan("deploy-20260831-006")
    finally:
        service._decision_cycle_lock.release()
        await _disarm_manual_trigger_runtime(service, tasks)

    assert repository.enqueue_calls == 0
    assert repository.seal_calls == 0


class _LateReplayRepository(_ManualTriggerRepository):
    def __init__(self, status: EntryStatus, state: StateRecord) -> None:
        super().__init__(status)
        self.state = state
        self.raw: dict[tuple[str, str], Any] = {}
        self.official_write_calls = 0

    async def load_state(self, _lineage_id: str) -> StateRecord:
        return self.state

    async def list_raw_minute_bar_records(
        self,
        codes,
        *,
        trade_date: date,
        end_labels,
    ):
        allowed_codes = set(codes)
        allowed_labels = set(end_labels)
        return [
            record
            for (code, label), record in sorted(self.raw.items())
            if code in allowed_codes
            and label in allowed_labels
            and record.bar_end.astimezone(TZ).date() == trade_date
        ]

    async def record_minute_bars(self, rows):
        hashes: set[str] = set()
        for payload in rows:
            payload = dict(payload)
            code = str(payload["stock_code"])
            label = str(payload["end_label"])
            key = (code, label)
            source_hash = sha256_json(payload)
            current = self.raw.get(key)
            if current is not None and current.source_hash != source_hash:
                raise V20SemanticConflict("conflicting replay minute fact")
            bar_end = datetime.fromisoformat(str(payload["bar_end"]))
            self.raw[key] = SimpleNamespace(
                code=code,
                bar_end=bar_end,
                end_label=label,
                source_hash=source_hash,
                payload=payload,
                first_received_at=datetime(2026, 8, 31, 15, 30, 1, tzinfo=TZ),
            )
            hashes.add(source_hash)
        return frozenset(hashes)

    async def record_daily_bar_snapshot(self, trade_date, payload):
        return SimpleNamespace(
            trade_date=trade_date,
            payload=dict(payload),
            first_received_at=datetime(2026, 8, 31, 15, 30, 1, tzinfo=TZ),
        )

    async def commit_entry(self, *_args, **_kwargs):
        self.official_write_calls += 1
        raise AssertionError("late replay must not commit an official entry")

    async def commit_exit(self, *_args, **_kwargs):
        self.official_write_calls += 1
        raise AssertionError("late replay must not commit an exit")

    async def select_mews_for_leg(self, *_args, **_kwargs):
        self.official_write_calls += 1
        raise AssertionError("MEWS is not a 09:39 replay input")


class _LateReplayClient:
    def __init__(self, *, missing_label: str | None = None) -> None:
        self.missing_label = missing_label
        # Current-day live endpoints (rt_min_daily & friends); a post-cutoff
        # replay must never touch them.
        self.calls: list[tuple[str, ...]] = []
        # Bounded historical stk_mins backfill — the only vendor path a replay
        # may use, and only for codes without qualified persisted evidence.
        self.stk_mins_calls: list[tuple[tuple[str, ...], date]] = []

    async def batch_get_minute_history(self, codes):
        self.calls.append(tuple(codes))
        labels = [f"09:{minute:02d}" for minute in range(31, 41)]
        return {
            code: tuple(
                _bar(code, label, close=10.0 + index / 100)
                for index, label in enumerate(labels, start=1)
                if label != self.missing_label
            )
            for code in codes
        }

    async def batch_get_early_minute_history_for_date(self, codes, trade_date):
        self.stk_mins_calls.append((tuple(codes), trade_date))
        labels = ["09:25", "09:30"] + [f"09:{minute:02d}" for minute in range(31, 40)]
        # The vendor restates the same facts the morning persisted.
        close_by_label = {"09:25": 10.0, "09:30": 10.0} | {
            f"09:{minute:02d}": 10.0 + (minute - 30) / 100 for minute in range(31, 40)
        }
        return {
            code: tuple(
                _bar(code, label, close=close_by_label[label], trade_date=trade_date)
                for label in labels
                if label != self.missing_label
            )
            for code in codes
        }

    async def fetch_daily_bars(self, trade_date):
        return {
            code: TushareDailyBar(
                stock_code=code,
                trade_date=trade_date,
                close_price=10.0,
                amount_yuan=1_000_000.0,
            )
            for code in _LATE_REPLAY_CODES
        }

    async def fetch_stock_names_for_date(self, trade_date):
        assert trade_date == "20260831"
        return {code: f"fresh-{code}" for code in _LATE_REPLAY_CODES}


class _ReplayHistoricalAdapter:
    async def history_quotes(self, *, codes, **_kwargs):
        return {
            "tables": [
                {
                    "thscode": ts_code,
                    "table": {
                        "time": ["2026-08-28"],
                        "open": [10.0],
                        "high": [10.1],
                        "low": [9.9],
                        "close": [10.0],
                        "volume": [1_000.0],
                    },
                }
                for ts_code in codes.split(",")
            ]
        }


class _ReplayCurrentNames:
    async def batch_current_names(self, codes):
        return {code: f"fresh-{code}" for code in codes}


_LATE_REPLAY_CODES = (
    "603068",
    "605299",
    "603990",
    "603232",
    "605098",
    "603193",
    "001238",
    "002368",
    "600486",
    "600557",
)


def _late_replay_scan_result() -> V16ScanResult:
    stocks = [
        ScoredStock(
            code=code,
            name=f"fresh-{code}",
            score=0.9 - index * 0.01,
            rank=index + 1,
            buy_price=10.09,
        )
        for index, code in enumerate(_LATE_REPLAY_CODES)
    ]
    return V16ScanResult(
        recommended=stocks,
        final_candidates=10,
        step0_universe_count=10,
        step2_hot_board_count=1,
        stock_best_board={code: "银行" for code in _LATE_REPLAY_CODES},
        stock_all_boards={code: ["银行"] for code in _LATE_REPLAY_CODES},
        stock_is_driver={code: True for code in _LATE_REPLAY_CODES},
        stock_cci={code: 1.0 for code in _LATE_REPLAY_CODES},
        stock_early_vol={code: 900.0 for code in _LATE_REPLAY_CODES},
        step2_board_avg_gains={"银行": 1.2},
    )


def _install_late_replay_canonical(
    monkeypatch: pytest.MonkeyPatch,
    service: V20Service,
    trade_date: date,
) -> tuple[list[date], dict[str, Any]]:
    """Bind replay to the seeded canonical V16 contract; coordinator is a bomb.

    The post-cutoff replay never uses the live coordinator: it seeds from
    persisted early (<=09:39) raw evidence and calls the real
    ``compute_canonical_v16_scan`` entry point directly with realtime fetches
    forbidden.  This double records the call and derives its bundle from the
    seed it was given, so outputs stay a function of the persisted raw bars.
    """
    compute_calls: list[date] = []
    observed: dict[str, Any] = {}

    async def compute(
        state: V15ScanState,
        requested_date: date,
        partial: Any = None,
        **kwargs: Any,
    ) -> CanonicalV16ScanBundle:
        assert state is service._scan_state
        assert requested_date == trade_date
        assert partial is None
        assert kwargs["allow_realtime_fetch"] is False
        assert kwargs["universe_override"] == tuple(sorted(_LATE_REPLAY_CODES))
        seed = kwargs["early_data_seed"]
        assert set(seed) <= set(_LATE_REPLAY_CODES)
        compute_calls.append(requested_date)
        early_bars = {code: seed[code].early_bars for code in seed}
        observed["early_volume"] = sum(bar.volume for bar in early_bars["603068"])
        observed["early_close"] = early_bars["603068"][-1].close_price
        return CanonicalV16ScanBundle(
            trade_date=requested_date,
            scan_result=_late_replay_scan_result(),
            stock_data={code: SimpleNamespace(volume_937=900.0) for code in _LATE_REPLAY_CODES},
            clean_boards={},
            universe=tuple(sorted(_LATE_REPLAY_CODES)),
            quotes={},
            prev_closes={code: 10.0 for code in _LATE_REPLAY_CODES},
            history_raw={},
            early_bars=early_bars,
            early_source_hashes={
                code: sha256_json([_bar_payload(bar) for bar in bars])
                for code, bars in early_bars.items()
            },
            failed_no_prev_close=(),
            failed_no_history=(),
            failed_build=(),
            skipped_new_listings=(),
            model_sha256="a" * 64,
            feature_list_sha256="b" * 64,
            computed_at=datetime(2026, 8, 31, 15, 30, 2, tzinfo=TZ),
            input_hash="c" * 64,
            _integrity_hash="",
            computation_calendar=(
                date(2026, 8, 28),
                requested_date,
                date(2026, 9, 1),
                date(2026, 9, 2),
            ),
            prior_trade_date=date(2026, 8, 28),
        )

    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", compute)

    async def coordinator_bomb(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("post-cutoff replay must bypass the canonical coordinator")

    monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", coordinator_bomb)
    return compute_calls, observed


def _late_replay_status_and_state(service: V20Service) -> tuple[EntryStatus, StateRecord]:
    trade_date = date(2026, 8, 31)
    before = genesis_state()
    before_hash = sha256_json(before)
    policy_inputs = {
        "schema_version": "v20-policy-input-snapshot/v1",
        "completed_health": [],
        "completed_rolling": [],
        "maturity_gaps": [],
    }
    policy_hash = sha256_json(policy_inputs)
    failure_gap_id = named_hash(
        "V20_OFFICIAL_SHADOW_GAP_ID_V1",
        {
            "official_stream_id": service.config.official_stream_id,
            "trade_date": trade_date.isoformat(),
        },
    )
    after = {
        **json.loads(json.dumps(before)),
        "state_revision": 1,
        "official_rolling_gaps": [
            {
                "gap_id": failure_gap_id,
                "signal_date": trade_date.isoformat(),
                "maturity_date": "2026-09-02",
                "closed": False,
                "aged_out": False,
            }
        ],
        "last_terminal_slot_id": "failed-slot",
        "last_terminal_trade_date": trade_date.isoformat(),
    }
    after_hash = sha256_json(after)
    semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "strategy_version": service.config.strategy_version,
        "config_hash": service.config.config_hash,
        "action": "INPUT_INVALID",
        "state_semantics_hash": service.config.state_semantics_hash,
        "state_before_hash": before_hash,
        "state_after_hash": after_hash,
        "policy_input_hash": policy_hash,
        "scheduled_exits_today": [],
    }
    snapshot = {
        "schema_version": V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
        "trade_date": trade_date.isoformat(),
        "reason_code": "SLOT_FINALIZED_FAILED",
        "detail": "late deployment",
        "state_before_hash": before_hash,
        "state_semantics_hash": service.config.state_semantics_hash,
        "policy_input_hash": policy_hash,
        "policy_inputs": policy_inputs,
    }
    status = EntryStatus(
        official_stream_id=service.config.official_stream_id,
        trade_date=trade_date,
        slot_id="failed-slot",
        slot_status="FAILED",
        slot_revision=1,
        strategy_version=service.config.strategy_version,
        config_id=service.config.config_hash[:24],
        config_hash=service.config.config_hash,
        lineage_id=service.config.state_lineage_id,
        decision_id="failed-decision",
        event_id="failed-entry-event",
        action="INPUT_INVALID",
        final_multiplier=0.0,
        semantic_content_hash=sha256_json(semantic),
        semantic=semantic,
        snapshot_id="failed-snapshot",
        snapshot_hash=sha256_json(snapshot),
        snapshot=snapshot,
        action_expiry_ts=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
    )
    state = StateRecord(
        lineage_id=service.config.state_lineage_id,
        revision=1,
        state_hash=after_hash,
        payload=after,
    )
    return status, state


def _late_replay_service(
    monkeypatch: pytest.MonkeyPatch,
    *,
    missing_label: str | None = None,
) -> tuple[
    V20Service,
    _LateReplayRepository,
    _LateReplayClient,
    list[date],
    dict[str, Any],
    _DayContext,
]:
    seed = _service(monkeypatch, SimpleNamespace())
    status, state = _late_replay_status_and_state(seed)
    repository = _LateReplayRepository(status, state)
    client = _LateReplayClient(missing_label=missing_label)
    service = _service(monkeypatch, repository, client)
    service._clock = lambda: datetime(2026, 8, 31, 15, 30, 3, tzinfo=TZ)
    replay_calendar = (
        date(2026, 8, 28),
        status.trade_date,
        date(2026, 9, 1),
        date(2026, 9, 2),
    )

    async def calendar_provider():
        return list(replay_calendar)

    service._calendar_provider = calendar_provider
    service._scan_state.historical_adapter = _ReplayHistoricalAdapter()
    service._scan_state.fundamentals_db = _ReplayCurrentNames()
    # Durable morning evidence: the full early raw bars (09:25/09:30 strategy
    # inputs included) were persisted at 09:39, so the post-cutoff replay
    # rehydrates from the database alone.
    replay_labels = ("09:25", "09:30") + tuple(f"09:{minute:02d}" for minute in range(31, 40))
    close_by_label = {"09:25": 10.0, "09:30": 10.0} | {
        f"09:{minute:02d}": 10.0 + (minute - 30) / 100 for minute in range(31, 40)
    }
    for code in _LATE_REPLAY_CODES:
        for label in replay_labels:
            if label == missing_label:
                continue
            bar = _bar(code, label, close=close_by_label[label])
            payload = _bar_payload(bar)
            repository.raw[(code, label)] = SimpleNamespace(
                code=code,
                bar_end=bar.bar_end,
                end_label=label,
                source_hash=sha256_json(payload),
                payload=payload,
                first_received_at=datetime(2026, 8, 31, 9, 39, tzinfo=TZ),
            )
    boards = {"board-a": tuple((code, f"name-{code}") for code in _LATE_REPLAY_CODES)}

    def derive_universe(
        _state,
        *,
        universe_override=None,
        clean_boards_override=None,
    ):
        selected_universe = tuple(
            sorted(_LATE_REPLAY_CODES if universe_override is None else universe_override)
        )
        selected_boards = boards if clean_boards_override is None else clean_boards_override
        return None, None, selected_boards, selected_universe

    monkeypatch.setattr(
        service_module,
        "derive_canonical_v16_universe",
        derive_universe,
    )
    # The post-cutoff replay attests V16 DayGate evidence after recompute; the
    # evidence store is outside these fixtures, so stub a PASS attestation.
    monkeypatch.setattr(
        service_module,
        "attest_post_cutoff_v16_day_gate",
        lambda *_args, **_kwargs: {
            "status": "PASS",
            "schema_version": "v16-day-gate-attestation/v1",
            "trade_date": status.trade_date.isoformat(),
        },
        raising=False,
    )
    compute_calls, observed = _install_late_replay_canonical(
        monkeypatch,
        service,
        status.trade_date,
    )
    context = _DayContext(
        trade_date=status.trade_date,
        calendar=replay_calendar,
        entry_status=status,
    )
    return service, repository, client, compute_calls, observed, context


async def test_late_0939_replay_core_is_durable_idempotent_and_officially_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, client, compute_calls, observed, context = _late_replay_service(
        monkeypatch
    )
    state_before = json.loads(json.dumps(repository.state.payload))
    now = datetime(2026, 8, 31, 15, 30, tzinfo=TZ)
    first = await service._ensure_late_0939_replay(context, now)
    second = await service._ensure_late_0939_replay(context, now + timedelta(minutes=1))

    assert first == second
    assert first.semantic["replay_kind"] == "RETROSPECTIVE_POST_CUTOFF"
    assert first.semantic["data_receipt_timeliness"] == "POST_CUTOFF"
    assert first.semantic["data_cutoff"] == "09:39"
    assert first.semantic["replay_action"] == "ENTER"
    assert first.semantic["final_multiplier"] == 1.0
    assert first.semantic["health_state"] == "WARMUP"
    assert first.semantic["rolling7_state"] == "WARMUP"
    assert first.semantic["breadth_replay_mode"] == ("SKIPPED_NOT_USED_BY_BASE_WARMUP_OR_HEALTHY")
    assert first.semantic["raw_fact_n"] == 110
    assert first.semantic["raw_post_cutoff_n"] == 110
    assert first.semantic["pit_limitations"][-1] == (
        "OFFICIAL_INPUT_INVALID_SLOT_HAS_NO_FROZEN_MORNING_CANONICAL_IDENTITY"
    )
    assert "MEWS_IS_NOT_A_09:39_ENTRY_INPUT" in first.semantic["pit_limitations"]
    assert first.semantic["state_replay_profile"] == "CURRENT_CODE_CANONICAL_V16_CHECK_ONLY"
    assert first.semantic["bootstrap_mode"] == "EMPTY_FORWARD_SHADOW"
    assert "decision_id" not in first.semantic
    assert "state_after_hash" not in first.semantic
    assert first.payload is not None
    assert "现在不开仓｜09:39复盘已过期" in str(first.payload["message"])
    assert "现在操作：不开仓，不补买，不追买" in str(first.payload["message"])
    # The replay lane never pulls from the vendor; it recomputes once from
    # durable raw facts through the shared canonical V16 contract.
    assert client.calls == []
    assert client.stk_mins_calls == []
    assert compute_calls == [context.trade_date]
    assert observed["early_volume"] == 1100.0
    assert observed["early_close"] == pytest.approx(10.09)
    assert len(repository.raw) == 110
    assert {label for _code, label in repository.raw} == {
        "09:25",
        "09:30",
        *{f"09:{minute:02d}" for minute in range(31, 40)},
    }
    assert repository.enqueue_calls == 1
    assert repository.seal_calls == 1
    assert repository.official_write_calls == 0
    assert repository.state.payload == state_before
    assert context.late_0939_replay_completed is True


async def test_late_0939_replay_rejects_state_that_moved_past_failed_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, client, compute_calls, _observed, context = _late_replay_service(
        monkeypatch
    )
    moved = {**dict(repository.state.payload), "state_revision": 2}
    repository.state = StateRecord(
        lineage_id=repository.state.lineage_id,
        revision=2,
        state_hash=sha256_json(moved),
        payload=moved,
    )

    with pytest.raises(V20StateConflict, match="moved beyond"):
        await service._ensure_late_0939_replay(
            context,
            datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
        )

    assert client.calls == []
    assert compute_calls == []
    assert repository.events == {}
    assert repository.official_write_calls == 0


async def test_late_0939_replay_can_recover_entirely_from_durable_raw_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, client, compute_calls, _observed, context = _late_replay_service(
        monkeypatch
    )
    record = await service._ensure_late_0939_replay(
        context,
        datetime(2026, 8, 31, 15, 31, tzinfo=TZ),
    )

    assert record.semantic["raw_fact_n"] == 110
    assert client.calls == []
    assert client.stk_mins_calls == []
    assert compute_calls == [context.trade_date]
    assert repository.official_write_calls == 0


async def test_late_0939_replay_missing_nonterminal_early_bar_still_replays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing non-terminal label (09:38) is not a fixed-nine failure."""
    service, repository, client, compute_calls, _observed, context = _late_replay_service(
        monkeypatch,
        missing_label="09:38",
    )

    record = await service._ensure_late_0939_replay(
        context,
        datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
    )

    assert record.semantic["replay_action"] == "ENTER"
    assert record.semantic["raw_fact_n"] == 100
    assert client.calls == []
    assert client.stk_mins_calls == []
    assert compute_calls == [context.trade_date]
    assert len(repository.raw) == 100
    assert {label for _code, label in repository.raw} == {
        "09:25",
        "09:30",
        *{f"09:{minute:02d}" for minute in range(31, 40) if minute != 38},
    }
    assert repository.official_write_calls == 0


async def test_late_0939_replay_missing_terminal_0939_bar_fails_without_official_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical readiness owns 09:39: a ready code without it fails closed.

    The persisted evidence and one bounded stk_mins backfill both lack the
    09:39 bar, so the replay fails before compute — and never touches a
    current-day live endpoint.
    """
    service, repository, client, compute_calls, _observed, context = _late_replay_service(
        monkeypatch,
        missing_label="09:39",
    )

    with pytest.raises(V20RepositoryError, match="backfill is incomplete"):
        await service._ensure_late_0939_replay(
            context,
            datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
        )

    assert client.calls == []
    assert client.stk_mins_calls == [(tuple(sorted(_LATE_REPLAY_CODES)), context.trade_date)]
    assert compute_calls == []
    assert repository.events == {}
    assert repository.official_write_calls == 0


async def test_automatic_late_replay_task_is_not_a_formal_runtime_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _repository, _client, _compute_calls, _observed, context = _late_replay_service(
        monkeypatch
    )
    started = asyncio.Event()
    blocked = asyncio.Event()

    async def background(_context: _DayContext, _now: datetime) -> None:
        started.set()
        await blocked.wait()

    monkeypatch.setattr(service, "_maybe_run_late_0939_replay", background)
    formal_tasks_before = tuple(service._tasks)
    service._schedule_late_0939_replay(
        context,
        datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
    )
    await started.wait()
    replay_task = service._late_0939_replay_task
    assert replay_task is not None
    assert replay_task not in service._tasks
    assert tuple(service._tasks) == formal_tasks_before
    replay_task.cancel()
    await asyncio.gather(replay_task, return_exceptions=True)
    service._late_0939_replay_task = None


class _SeedRepository:
    """Minimal raw minute-bar store for historical seed tests."""

    def __init__(self) -> None:
        self.raw: dict[tuple[str, str], Any] = {}
        self.list_calls = 0
        self.persist_calls: list[tuple[Mapping[str, Any], ...]] = []

    async def list_raw_minute_bar_records(self, codes, *, trade_date, end_labels):
        self.list_calls += 1
        allowed_codes = set(codes)
        allowed_labels = set(end_labels)
        return [
            record
            for (code, label), record in sorted(self.raw.items())
            if code in allowed_codes
            and label in allowed_labels
            and record.bar_end.astimezone(TZ).date() == trade_date
        ]

    async def record_minute_bars(self, rows):
        self.persist_calls.append(tuple(dict(payload) for payload in rows))
        hashes: set[str] = set()
        for payload in rows:
            payload = dict(payload)
            key = (str(payload["stock_code"]), str(payload["end_label"]))
            source_hash = sha256_json(payload)
            current = self.raw.get(key)
            if current is not None and current.source_hash != source_hash:
                raise V20SemanticConflict("conflicting replay minute fact")
            self.raw[key] = SimpleNamespace(
                code=key[0],
                bar_end=datetime.fromisoformat(str(payload["bar_end"])),
                end_label=key[1],
                source_hash=source_hash,
                payload=payload,
                first_received_at=datetime(2026, 9, 1, 15, 30, 1, tzinfo=TZ),
            )
            hashes.add(source_hash)
        return frozenset(hashes)

    async def record_daily_bar_snapshot(self, trade_date, payload):
        return SimpleNamespace(
            trade_date=trade_date,
            payload=dict(payload),
            first_received_at=datetime(2026, 9, 1, 15, 30, 1, tzinfo=TZ),
        )


class _HistoricalSeedClient:
    """Frozen historical adapters; current-day RT endpoints still bomb."""

    def __init__(self, bars_by_code: Mapping[str, tuple[TushareMinuteBar, ...]] | None = None):
        self.bars_by_code = dict(bars_by_code or {})
        self.calls: list[tuple[tuple[str, ...], date]] = []
        self.daily_calls: list[str] = []
        self.name_calls: list[str] = []

    async def batch_get_early_minute_history_for_date(self, codes, trade_date):
        self.calls.append((tuple(codes), trade_date))
        return {code: self.bars_by_code.get(code, ()) for code in codes}

    async def fetch_daily_bars(self, trade_date):
        self.daily_calls.append(trade_date)
        return {
            code: TushareDailyBar(
                stock_code=code,
                trade_date=trade_date,
                close_price=10.0,
                amount_yuan=1_000_000.0,
            )
            for code in _LATE_REPLAY_CODES
        }

    async def fetch_stock_names_for_date(self, trade_date):
        self.name_calls.append(trade_date)
        return {code: f"fresh-{code}" for code in _LATE_REPLAY_CODES}

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"past-date replay touched a live/vendor boundary: {name}")


def _historical_seed_service(
    monkeypatch: pytest.MonkeyPatch,
    repository: Any,
    client: Any,
    universe: Sequence[str] = _LATE_REPLAY_CODES,
) -> V20Service:
    service = _service(monkeypatch, repository, client)
    boards = {"board-a": tuple((code, f"name-{code}") for code in universe)}

    def derive_universe(
        _state,
        *,
        universe_override=None,
        clean_boards_override=None,
    ):
        selected_universe = tuple(
            sorted(universe if universe_override is None else universe_override)
        )
        selected_boards = boards if clean_boards_override is None else clean_boards_override
        return None, None, selected_boards, selected_universe

    monkeypatch.setattr(
        service_module,
        "derive_canonical_v16_universe",
        derive_universe,
    )
    service._clock = lambda: datetime(2026, 9, 1, 15, 30, 3, tzinfo=TZ)
    replay_calendar = (
        date(2026, 8, 28),
        _HIST_TRADE_DATE,
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
    )

    async def calendar_provider():
        return list(replay_calendar)

    service._calendar_provider = calendar_provider
    service._scan_state.historical_adapter = _ReplayHistoricalAdapter()
    service._scan_state.fundamentals_db = _ReplayCurrentNames()
    return service


_ENRICHED_LABELS = ("09:25", "09:30") + tuple(f"09:{minute:02d}" for minute in range(31, 40))
_LEGACY_LABELS = tuple(f"09:{minute:02d}" for minute in range(31, 40))
_HIST_TRADE_DATE = date(2026, 8, 31)


async def test_historical_seed_preserves_0925_and_0930_through_persist_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 09:25/09:30 strategy inputs survive persistence, fold and hydration."""
    repository = _SeedRepository()
    for code in _LATE_REPLAY_CODES:
        await repository.record_minute_bars(
            [_bar_payload(_bar(code, label)) for label in _ENRICHED_LABELS]
        )
    repository.persist_calls.clear()
    client = _HistoricalSeedClient()
    service = _historical_seed_service(monkeypatch, repository, client)

    seed, universe, _clean_boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    # Full usable coverage: a single database read, no vendor call, no readback.
    assert client.calls == []
    assert repository.list_calls == 1
    assert set(seed) == set(universe)
    for code in universe:
        expected = tushare_minute_bars_to_early_market_data(
            code,
            tuple(_bar(code, label) for label in _ENRICHED_LABELS),
            _HIST_TRADE_DATE,
        )
        assert seed[code] == expected
        assert [bar.end_label for bar in seed[code].early_bars] == list(_ENRICHED_LABELS)


async def test_past_date_replay_reruns_scanner_directly_and_never_touches_coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh fixed-nine history enters the same scanner and is then reused durably."""
    today = date(2026, 9, 1)
    repository = _SeedRepository()
    vendor_bars = {
        code: tuple(_bar(code, label) for label in _LEGACY_LABELS) for code in _LATE_REPLAY_CODES
    }
    client = _HistoricalSeedClient(vendor_bars)
    service = _historical_seed_service(monkeypatch, repository, client)

    d0_bundle = SimpleNamespace(marker="d0-canonical-cache-entry")
    service._scan_state.canonical_coordinator = SimpleNamespace(
        cache={today: d0_bundle},
        inflight={},
        partial={},
    )
    coordinator_cache = service._scan_state.canonical_coordinator.cache
    d0_cache_bytes = pickle.dumps(coordinator_cache)

    compute_calls: list[dict[str, Any]] = []

    async def fake_compute(state, requested_date, partial=None, **kwargs):
        assert state is service._scan_state
        assert requested_date == _HIST_TRADE_DATE
        assert partial is None
        assert kwargs["allow_realtime_fetch"] is False
        assert kwargs["universe_override"] == tuple(sorted(_LATE_REPLAY_CODES))
        assert "board-a" in kwargs["clean_boards_override"]
        seed = kwargs["early_data_seed"]
        assert set(seed) == set(_LATE_REPLAY_CODES)
        assert [bar.end_label for bar in seed["603068"].early_bars] == list(_LEGACY_LABELS)
        compute_calls.append(kwargs)
        early_bars = {
            code: tuple(_bar(code, label) for label in _LEGACY_LABELS)
            for code in _LATE_REPLAY_CODES
        }
        return CanonicalV16ScanBundle(
            trade_date=requested_date,
            scan_result=_late_replay_scan_result(),
            stock_data={code: SimpleNamespace(volume_937=900.0) for code in _LATE_REPLAY_CODES},
            clean_boards={},
            universe=tuple(sorted(_LATE_REPLAY_CODES)),
            quotes={},
            prev_closes={code: 10.0 for code in _LATE_REPLAY_CODES},
            history_raw={},
            early_bars=early_bars,
            early_source_hashes={
                code: sha256_json([_bar_payload(bar) for bar in bars])
                for code, bars in early_bars.items()
            },
            failed_no_prev_close=(),
            failed_no_history=(),
            failed_build=(),
            skipped_new_listings=(),
            model_sha256="a" * 64,
            feature_list_sha256="b" * 64,
            computed_at=datetime(2026, 9, 1, 15, 30, 2, tzinfo=TZ),
            input_hash="c" * 64,
            _integrity_hash="",
            computation_calendar=(
                date(2026, 8, 28),
                requested_date,
                date(2026, 9, 1),
                date(2026, 9, 2),
            ),
            prior_trade_date=date(2026, 8, 28),
        )

    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", fake_compute)

    async def coordinator_bomb(*_args, **_kwargs):
        raise AssertionError("historical replay must bypass the canonical coordinator")

    monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", coordinator_bomb)

    context = _DayContext(
        trade_date=_HIST_TRADE_DATE,
        calendar=(date(2026, 8, 28), _HIST_TRADE_DATE, today),
    )
    first = await service._compute_canonical_v16_from_persisted_raw(context)
    second = await service._compute_canonical_v16_from_persisted_raw(context)

    assert first.trade_date == second.trade_date == _HIST_TRADE_DATE
    assert len(compute_calls) == 2
    assert client.calls == [(tuple(sorted(_LATE_REPLAY_CODES)), _HIST_TRADE_DATE)]
    assert repository.list_calls == 3
    assert pickle.dumps(coordinator_cache) == d0_cache_bytes
    assert service._scan_state.canonical_coordinator.cache[today] is d0_bundle


async def test_past_date_bootstrap_fetches_only_missing_codes_and_reuses_fixed_nine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persisted legal 09:39 prevents refetch while missing codes are restored."""
    repository = _SeedRepository()
    legacy_codes = _LATE_REPLAY_CODES[:2]
    for code in legacy_codes:
        await repository.record_minute_bars(
            [_bar_payload(_bar(code, label)) for label in _LEGACY_LABELS]
        )
    suspended = "600557"
    vendor_bars: dict[str, tuple[TushareMinuteBar, ...]] = {}
    for code in _LATE_REPLAY_CODES:
        if code == suspended:
            continue
        bars = [_bar(code, label) for label in _LEGACY_LABELS]
        if code == "603990":
            # Late and wrong-date vendor rows must be truncated away BEFORE
            # the normalizer and persistence.
            bars += [
                _bar(code, "09:40"),
                _bar(code, "09:39", trade_date=date(2026, 8, 28)),
            ]
        vendor_bars[code] = tuple(bars)
    client = _HistoricalSeedClient(vendor_bars)
    service = _historical_seed_service(monkeypatch, repository, client)
    repository.persist_calls.clear()

    seed, universe, _clean_boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    missing_codes = tuple(sorted(set(universe) - set(legacy_codes)))
    # Only genuinely missing codes enter the deterministic batch.  The two
    # already-usable fixed-nine codes are neither fetched nor rewritten.
    assert client.calls == [(missing_codes, _HIST_TRADE_DATE)]
    assert repository.list_calls == 2
    assert len(repository.persist_calls) == 1
    persisted_now = repository.persist_calls[0]
    assert all(str(payload["stock_code"]) not in legacy_codes for payload in persisted_now)
    assert suspended not in seed
    assert set(seed) == set(universe) - {suspended}
    for code in seed:
        assert [bar.end_label for bar in seed[code].early_bars] == list(_LEGACY_LABELS)
    persisted_labels = {label for _code, label in repository.raw}
    assert "09:40" not in persisted_labels
    assert all(
        record.bar_end.astimezone(TZ).date() == _HIST_TRADE_DATE
        for record in repository.raw.values()
    )

    # The 80% gate never stops evidence fetching: the second seed build
    # re-requests only the still-missing suspended code, and its explicitly
    # empty response completes the round without an error.
    seed_again, _universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)
    assert set(seed_again) == set(seed)
    assert client.calls == [
        (missing_codes, _HIST_TRADE_DATE),
        ((suspended,), _HIST_TRADE_DATE),
    ]
    assert repository.list_calls == 4


async def test_past_date_seed_reads_back_even_when_bootstrap_seals_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The database readback is mandatory after a stk_mins attempt, even empty."""
    repository = _SeedRepository()
    client = _HistoricalSeedClient()  # vendor has no bars at all
    service = _historical_seed_service(monkeypatch, repository, client)

    seed, universe, _clean_boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert seed == {}
    assert client.calls == [(tuple(sorted(universe)), _HIST_TRADE_DATE)]
    assert repository.persist_calls == []
    assert repository.list_calls == 2


async def test_past_date_seed_uses_25_physical_batch_calls_for_3195_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production-shape Rolling7 recovery never falls back to per-symbol history."""
    codes = tuple(f"{index:06d}" for index in range(1, 3_196))
    repository = _SeedRepository()
    client = TushareRealtimeClient("token")
    client._client = object()  # type: ignore[assignment]
    physical_calls: list[dict[str, str]] = []

    async def api_call(api_name, params, **_kwargs):
        assert api_name == "stk_mins"
        physical_calls.append(dict(params))
        return {
            "data": {
                "fields": [
                    "ts_code",
                    "trade_time",
                    "open",
                    "close",
                    "high",
                    "low",
                    "vol",
                    "amount",
                ],
                "items": [],
            }
        }

    async def legacy_per_symbol_bomb(*_args, **_kwargs):
        raise AssertionError("Rolling7 recovery called the per-symbol full-day adapter")

    monkeypatch.setattr(client, "_api_call", api_call)
    monkeypatch.setattr(client, "batch_get_minute_history_for_date", legacy_per_symbol_bomb)
    service = _historical_seed_service(
        monkeypatch,
        repository,
        client,
        universe=codes,
    )

    seed, universe, _clean_boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert seed == {}
    assert universe == codes
    assert len(physical_calls) == (len(codes) + 127) // 128 == 25
    requested = [
        ts_code.split(".")[0]
        for params in physical_calls
        for ts_code in params["ts_code"].split(",")
    ]
    assert requested == list(codes)
    assert max(len(params["ts_code"].split(",")) for params in physical_calls) == 128
    assert repository.list_calls == 2
    assert repository.persist_calls == []


async def test_past_date_bootstrap_chunk_cancellation_resumes_from_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancelled chunk keeps completed chunks; the next call fetches the rest."""
    codes = tuple(f"{index:06d}" for index in range(200))
    repository = _SeedRepository()

    class _CancellingClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, ...]] = []

        async def batch_get_early_minute_history_for_date(self, requested, trade_date):
            self.calls.append(tuple(requested))
            if len(self.calls) == 2:
                raise asyncio.CancelledError()
            return {
                code: tuple(_bar(code, label, trade_date=trade_date) for label in _ENRICHED_LABELS)
                for code in requested
            }

        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"past-date replay touched a live/vendor boundary: {name}")

    client = _CancellingClient()
    service = _historical_seed_service(monkeypatch, repository, client, universe=codes)

    with pytest.raises(asyncio.CancelledError):
        await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    # The first 128-code chunk completed and is durable; the second chunk was
    # cancelled before producing anything.
    assert [len(call) for call in client.calls] == [128, 72]
    assert len(repository.raw) == 128 * len(_ENRICHED_LABELS)

    seed, _universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    # The resume re-derives pending from the database: only the 72 unfinished
    # codes are requested again.
    assert set(seed) == set(codes)
    assert [len(call) for call in client.calls] == [128, 72, 72]


async def test_past_date_bootstrap_failed_code_raises_instead_of_fake_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A target with a failed (keyless) response fails closed; no scan runs."""
    dropped = "600557"
    vendor_bars = {
        code: tuple(_bar(code, label) for label in _ENRICHED_LABELS)
        for code in _LATE_REPLAY_CODES
        if code != dropped
    }
    repository = _SeedRepository()

    class _FlakyClient(_HistoricalSeedClient):
        async def batch_get_early_minute_history_for_date(self, codes, trade_date):
            self.calls.append((tuple(codes), trade_date))
            # A missing key means the per-code API call failed.
            return {code: self.bars_by_code[code] for code in codes if code != dropped}

    client = _FlakyClient(vendor_bars)
    service = _historical_seed_service(monkeypatch, repository, client)

    with pytest.raises(V20RepositoryError, match="backfill is incomplete"):
        await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert client.calls == [(tuple(sorted(_LATE_REPLAY_CODES)), _HIST_TRADE_DATE)]
    # The successful chunk rows are still durable, and the mandatory readback ran.
    assert repository.list_calls == 2
    assert len(repository.raw) == 9 * len(_ENRICHED_LABELS)


async def test_past_date_replay_rejects_future_trade_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _SeedRepository()
    client = _HistoricalSeedClient()
    service = _historical_seed_service(monkeypatch, repository, client)
    future = date(2026, 9, 2)
    context = _DayContext(
        trade_date=future,
        calendar=(_HIST_TRADE_DATE, date(2026, 9, 1), future),
    )

    with pytest.raises(V20StateConflict, match="future trade date"):
        await service._compute_canonical_v16_from_persisted_raw(context)

    assert client.calls == []
    assert repository.list_calls == 0


def test_fold_universe_raw_records_identical_misbound_conflicted_and_missing() -> None:
    """Identical revisions fold; unequal or misbound revisions conflict a code."""

    def record(code: str, label: str, *, payload=None, bar_end=None) -> MinuteBarRecord:
        payload = payload if payload is not None else _bar_payload(_bar(code, label))
        return MinuteBarRecord(
            code=code,
            bar_end=(
                bar_end if bar_end is not None else datetime.fromisoformat(str(payload["bar_end"]))
            ),
            end_label=label,
            source_hash=sha256_json(payload),
            payload=payload,
            first_received_at=datetime(2026, 9, 1, 15, 30, tzinfo=TZ),
        )

    universe = ("000001", "000002", "000003", "000004", "000005")
    records = [
        # 000001: identical duplicate revisions fold into one usable bar.
        record("000001", "09:39"),
        record("000001", "09:39"),
        # 000002: unequal revisions for one label conflict the whole code.
        record("000002", "09:39"),
        record(
            "000002",
            "09:39",
            payload=_bar_payload(_bar("000002", "09:39", close=11.0)),
        ),
        # 000003: record label disagrees with the payload label (misbound).
        record("000003", "09:38", payload=_bar_payload(_bar("000003", "09:39"))),
        # 000004: record bar_end is not the label's Shanghai HH:MM instant.
        record(
            "000004",
            "09:39",
            bar_end=datetime(2026, 8, 31, 9, 38, tzinfo=TZ),
        ),
        # 000005: no rows at all.
    ]

    usable, missing, conflicted = V20Service._fold_universe_raw_records(
        records, universe, _HIST_TRADE_DATE
    )

    assert [bar.end_label for bar in usable["000001"]] == ["09:39"]
    assert conflicted == frozenset({"000002", "000003", "000004"})
    assert missing == frozenset({"000005"})


async def test_canonical_raw_persistence_seals_every_ready_code_not_just_top10(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persistence is scoped to the full ready universe, never only the Top10."""
    extra_codes = ("300001", "300002")
    all_codes = tuple(sorted(_LATE_REPLAY_CODES + extra_codes))
    early_bars = {
        code: tuple(_bar(code, label) for label in _ENRICHED_LABELS) for code in all_codes
    }
    canonical = CanonicalV16ScanBundle(
        trade_date=_HIST_TRADE_DATE,
        scan_result=_late_replay_scan_result(),
        stock_data={code: SimpleNamespace(volume_937=900.0) for code in all_codes},
        clean_boards={},
        universe=all_codes,
        quotes={},
        prev_closes={code: 10.0 for code in all_codes},
        history_raw={},
        early_bars=early_bars,
        early_source_hashes={
            code: sha256_json([_bar_payload(bar) for bar in bars])
            for code, bars in early_bars.items()
        },
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="a" * 64,
        feature_list_sha256="b" * 64,
        computed_at=datetime(2026, 8, 31, 15, 30, 2, tzinfo=TZ),
        input_hash="c" * 64,
        _integrity_hash="",
        computation_calendar=(
            date(2026, 8, 28),
            _HIST_TRADE_DATE,
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
        prior_trade_date=date(2026, 8, 28),
    )
    repository = _SeedRepository()
    service = _service(monkeypatch, repository)

    await service._persist_canonical_raw_minute_bars(canonical)

    assert len(repository.persist_calls) == 1
    persisted = repository.persist_calls[0]
    assert len(persisted) == len(all_codes) * len(_ENRICHED_LABELS)
    assert {str(payload["stock_code"]) for payload in persisted} == set(all_codes)
    for code in all_codes:
        assert {
            str(payload["end_label"]) for payload in persisted if str(payload["stock_code"]) == code
        } == set(_ENRICHED_LABELS)

    # A late 09:40+ row inside the canonical evidence fails closed.
    tainted = CanonicalV16ScanBundle(
        trade_date=date(2026, 9, 2),
        scan_result=canonical.scan_result,
        stock_data=canonical.stock_data,
        clean_boards=canonical.clean_boards,
        universe=canonical.universe,
        quotes=canonical.quotes,
        prev_closes=canonical.prev_closes,
        history_raw=canonical.history_raw,
        early_bars={
            code: tuple(
                _bar(code, label, trade_date=date(2026, 9, 2)) for label in _ENRICHED_LABELS
            )
            + (_bar(code, "09:40", trade_date=date(2026, 9, 2)),)
            for code in all_codes
        },
        early_source_hashes=canonical.early_source_hashes,
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="a" * 64,
        feature_list_sha256="b" * 64,
        computed_at=datetime(2026, 9, 2, 15, 30, 2, tzinfo=TZ),
        input_hash="d" * 64,
        _integrity_hash="",
        computation_calendar=(
            date(2026, 9, 1),
            date(2026, 9, 2),
            date(2026, 9, 3),
            date(2026, 9, 4),
        ),
        prior_trade_date=date(2026, 9, 1),
    )
    with pytest.raises(V20SemanticConflict, match="early raw bar is invalid"):
        await service._persist_canonical_raw_minute_bars(tainted)
    assert all(
        str(payload["end_label"]) != "09:40"
        for call in repository.persist_calls
        for payload in call
    )


def _seed_record(code: str, label: str, *, close: float = 10.0) -> MinuteBarRecord:
    payload = _bar_payload(_bar(code, label, close=close))
    return MinuteBarRecord(
        code=code,
        bar_end=datetime.fromisoformat(str(payload["bar_end"])),
        end_label=label,
        source_hash=sha256_json(payload),
        payload=payload,
        first_received_at=datetime(2026, 9, 1, 15, 30, tzinfo=TZ),
    )


async def test_past_date_bootstrap_empty_first_chunk_continues_to_later_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful all-empty chunk never breaks; later chunks are still fetched."""
    codes = tuple(f"{index:06d}" for index in range(200))
    populated = codes[128:]
    vendor_bars = {
        code: tuple(_bar(code, label) for label in _ENRICHED_LABELS) for code in populated
    }
    repository = _SeedRepository()
    # Keys are present for every requested code; the first chunk answers with
    # explicit empty tuples (a successful "no bars" response, not a failure).
    client = _HistoricalSeedClient(vendor_bars)
    service = _historical_seed_service(monkeypatch, repository, client, universe=codes)

    seed, _universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert [len(call) for call, _day in client.calls] == [128, 72]
    assert set(seed) == set(populated)
    assert repository.list_calls == 2
    assert len(repository.persist_calls) == 1
    assert len(repository.persist_calls[0]) == 72 * len(_ENRICHED_LABELS)


async def test_past_date_seed_initial_conflict_outside_top10_blocks_vendor_and_compute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Any conflicted universe code (even non-Top10) blocks fetch and compute."""
    conflicted_code = "300001"
    universe = tuple(sorted(_LATE_REPLAY_CODES + (conflicted_code,)))

    class _InitialConflictRepository(_SeedRepository):
        async def list_raw_minute_bar_records(self, codes, *, trade_date, end_labels):
            records = await super().list_raw_minute_bar_records(
                codes, trade_date=trade_date, end_labels=end_labels
            )
            # Two unequal persisted revisions of the same 09:39 bar.
            return [
                *records,
                _seed_record(conflicted_code, "09:39"),
                _seed_record(conflicted_code, "09:39", close=11.0),
            ]

    repository = _InitialConflictRepository()
    client = _HistoricalSeedClient()
    service = _historical_seed_service(monkeypatch, repository, client, universe=universe)

    compute_calls: list[Any] = []

    async def fake_compute(*_args: Any, **_kwargs: Any) -> Any:
        compute_calls.append(1)
        raise AssertionError("compute must never run after a seed conflict")

    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", fake_compute)
    context = _DayContext(
        trade_date=_HIST_TRADE_DATE,
        calendar=(date(2026, 8, 28), _HIST_TRADE_DATE, date(2026, 9, 1)),
    )

    with pytest.raises(V20SemanticConflict, match="initial fold has 1 conflicted") as exc_info:
        await service._compute_canonical_v16_from_persisted_raw(context)

    assert conflicted_code in str(exc_info.value)
    assert client.calls == []
    assert compute_calls == []
    assert repository.list_calls == 1


async def test_past_date_seed_readback_conflict_blocks_compute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A conflict appearing only in the mandatory readback still blocks compute."""

    class _ReadbackConflictRepository(_SeedRepository):
        async def list_raw_minute_bar_records(self, codes, *, trade_date, end_labels):
            records = await super().list_raw_minute_bar_records(
                codes, trade_date=trade_date, end_labels=end_labels
            )
            if self.list_calls >= 2:
                # A second, unequal 09:39 revision surfaces only on readback.
                records = [
                    *records,
                    _seed_record("603068", "09:39", close=99.0),
                ]
            return records

    repository = _ReadbackConflictRepository()
    vendor_bars = {
        code: tuple(_bar(code, label) for label in _ENRICHED_LABELS) for code in _LATE_REPLAY_CODES
    }
    client = _HistoricalSeedClient(vendor_bars)
    service = _historical_seed_service(monkeypatch, repository, client)

    compute_calls: list[Any] = []

    async def fake_compute(*_args: Any, **_kwargs: Any) -> Any:
        compute_calls.append(1)
        raise AssertionError("compute must never run after a readback conflict")

    monkeypatch.setattr(service_module, "compute_canonical_v16_scan", fake_compute)
    context = _DayContext(
        trade_date=_HIST_TRADE_DATE,
        calendar=(date(2026, 8, 28), _HIST_TRADE_DATE, date(2026, 9, 1)),
    )

    with pytest.raises(V20SemanticConflict, match="readback fold has 1 conflicted") as exc_info:
        await service._compute_canonical_v16_from_persisted_raw(context)

    assert "603068" in str(exc_info.value)
    assert compute_calls == []
    assert repository.list_calls == 2


async def test_past_date_seed_reuses_499_fixed_nine_codes_without_vendor_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The old 499-code partial backfill is already canonical under the 09:39 rule."""
    codes = tuple(f"{index:06d}" for index in range(1, 500))
    repository = _SeedRepository()
    for code in codes:
        await repository.record_minute_bars(
            [_bar_payload(_bar(code, label)) for label in _LEGACY_LABELS]
        )
    repository.persist_calls.clear()
    client = _HistoricalSeedClient()
    service = _historical_seed_service(monkeypatch, repository, client, universe=codes)

    seed, universe, _boards = await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert universe == codes
    assert set(seed) == set(codes)
    assert client.calls == []
    assert repository.list_calls == 1
    assert repository.persist_calls == []
    assert all(
        [bar.end_label for bar in seed[code].early_bars] == list(_LEGACY_LABELS) for code in codes
    )


async def test_past_date_seed_does_not_loosen_0939_date_or_validity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Earlier-only, wrong-date and invalid rows cannot satisfy readiness."""
    codes = ("000001", "000002", "000003")
    repository = _SeedRepository()
    vendor_bars = {
        "000001": tuple(_bar("000001", label) for label in _LEGACY_LABELS[:-1]),
        "000002": (_bar("000002", "09:39", trade_date=date(2026, 8, 28)),),
        "000003": (replace(_bar("000003", "09:39"), volume=-1.0),),
    }
    client = _HistoricalSeedClient(vendor_bars)
    service = _historical_seed_service(monkeypatch, repository, client, universe=codes)

    with pytest.raises(V20RepositoryError, match="backfill is incomplete: 3/3"):
        await service._historical_early_evidence_seed(_HIST_TRADE_DATE)

    assert client.calls == [(codes, _HIST_TRADE_DATE)]
    assert repository.list_calls == 2
    assert {(code, label) for code, label in repository.raw} == {
        ("000001", label) for label in _LEGACY_LABELS[:-1]
    }


class _SealRepository:
    def __init__(self) -> None:
        self.calls = 0
        self.sealed: list[str] = []
        self.scan_kwargs: dict[str, Any] | None = None

    async def list_unsealed_outbox_event_ids(self, **kwargs):
        self.scan_kwargs = kwargs
        self.calls += 1
        return ("entry-event", "exit-event") if self.calls == 1 else ()

    async def seal_event(self, event_id, builder):
        self.sealed.append(event_id)


async def test_crash_recovery_seals_committed_entry_and_exit_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _SealRepository()
    service = _service(monkeypatch, repository)

    await service._seal_pending_outbox()

    assert repository.sealed == ["entry-event", "exit-event"]
    assert repository.scan_kwargs is not None
    assert repository.scan_kwargs["route_id"] == service.config.route_id
    assert repository.scan_kwargs["official_stream_id"] == service.config.official_stream_id
    assert repository.scan_kwargs["lineage_id"] == service.config.state_lineage_id


class _PartlyBrokenSealRepository:
    def __init__(self) -> None:
        self.calls = 0
        self.sealed: list[str] = []

    async def list_unsealed_outbox_event_ids(self, **kwargs):
        self.calls += 1
        return ("bad-event", "good-event") if self.calls == 1 else ()

    async def seal_event(self, event_id, builder):
        if event_id == "bad-event":
            raise V20SemanticConflict("corrupt old payload")
        self.sealed.append(event_id)


async def test_bad_historical_outbox_event_does_not_starve_valid_sibling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _PartlyBrokenSealRepository()
    service = _service(monkeypatch, repository)

    await service._seal_pending_outbox()

    assert repository.sealed == ["good-event"]


async def test_scan_cleanup_closes_fundamentals_even_when_realtime_stop_fails() -> None:
    class _Realtime:
        async def stop(self):
            raise RuntimeError("stop failed")

    class _Fundamentals:
        def __init__(self) -> None:
            self.closed = False

        async def close(self):
            self.closed = True

    fundamentals = _Fundamentals()
    state = V15ScanState(
        initialized=True,
        realtime_client=_Realtime(),
        fundamentals_db=fundamentals,
    )

    with pytest.raises(RuntimeError, match="stop failed"):
        await _cleanup_v20_scan_resources(state)

    assert fundamentals.closed is True
    assert state.initialized is False


@pytest.mark.parametrize(
    "field,logical_path",
    [
        ("scorer_model_sha256", "models/lgbrank_latest.txt"),
        ("scorer_feature_sha256", "models/feature_list.json"),
    ],
)
def test_runtime_model_or_feature_drift_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    logical_path: str,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    prewarmed = SimpleNamespace(
        scorer_model_sha256=service.config.strategy_dependency_hashes["models/lgbrank_latest.txt"],
        scorer_feature_sha256=service.config.strategy_dependency_hashes["models/feature_list.json"],
    )
    setattr(prewarmed, field, "0" * 64)

    with pytest.raises(V20SemanticConflict, match=logical_path):
        service._verify_prewarm_dependencies(prewarmed)


async def test_service_stop_closes_repository_after_resource_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        def __init__(self) -> None:
            self.closed = False

        async def close(self):
            self.closed = True

    repository = _Repository()

    async def broken_cleanup(_state):
        raise RuntimeError("cleanup failed")

    service = _service(monkeypatch, repository)
    service._cleanup_resources = broken_cleanup
    service._resources_started = True
    service._repository_started = True
    service._started = True

    with pytest.raises(RuntimeError, match="cleanup failed"):
        await service.stop()

    assert repository.closed is True
    assert service._started is False


async def test_shutdown_cancellation_still_releases_resources_and_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    repository = _Repository()
    resource_closed = False
    child_cancelling = asyncio.Event()

    async def cleanup(_state):
        nonlocal resource_closed
        resource_closed = True

    async def child():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            child_cancelling.set()
            await asyncio.Event().wait()

    service = _service(monkeypatch, repository)
    service._cleanup_resources = cleanup
    service._resources_started = True
    service._repository_started = True
    service._started = True
    service._tasks = [asyncio.create_task(child())]

    stopping = asyncio.create_task(service.stop())
    await asyncio.wait_for(child_cancelling.wait(), timeout=1.0)
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping

    assert resource_closed is True
    assert repository.closed is True
    assert service._started is False


async def test_scheduler_terminates_after_leadership_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _LostLeaderRepository:
        async def assert_runtime_leader(self) -> None:
            raise V20LeadershipLost("leader connection lost")

    service = _service(monkeypatch, _LostLeaderRepository())

    with pytest.raises(V20LeadershipLost):
        await service._run_scheduler()

    assert service._stop_event.is_set()
    assert service._last_error is not None
    assert "LEADERSHIP_LOST" in service._last_error


async def test_live_exit_lane_keeps_ticking_while_decision_lane_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        async def assert_runtime_leader(self) -> None:
            return None

        async def database_cutoff_reached(self, _cutoff: datetime) -> bool:
            return True

    service = _service(monkeypatch, _Repository())
    decision_started = asyncio.Event()
    release_decision = asyncio.Event()
    two_exit_ticks = asyncio.Event()
    exit_calls = 0

    async def blocked_decision(_now, **_kwargs):
        decision_started.set()
        await release_decision.wait()

    async def live_tick(_context, _now):
        nonlocal exit_calls
        exit_calls += 1
        if exit_calls >= 2:
            two_exit_ticks.set()

    async def immediate_tick(_started_at, _cadence):
        await asyncio.sleep(0)

    monkeypatch.setattr(service, "_run_decision_iteration_with_cutoff", blocked_decision)
    monkeypatch.setattr(service, "_run_live_exit_tick", live_tick)
    monkeypatch.setattr(service, "_wait_for_runtime_tick", immediate_tick)
    decision_task = asyncio.create_task(service._run_scheduler())
    exit_task = asyncio.create_task(service._run_live_exit_scheduler())

    await asyncio.wait_for(decision_started.wait(), timeout=1.0)
    await asyncio.wait_for(two_exit_ticks.wait(), timeout=1.0)

    assert exit_calls >= 2
    assert not decision_task.done()
    service._stop_event.set()
    release_decision.set()
    await asyncio.gather(decision_task, exit_task)


async def test_live_exit_tick_timeout_cancels_slow_vendor_and_allows_next_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    context = _DayContext(trade_date=date(2026, 8, 31), calendar=())
    cancelled = asyncio.Event()
    calls = 0

    async def exit_cycle(*_args, deadline, tick_started_at, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:

            async def slow_vendor() -> None:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            await service._run_live_exit_stage(
                slow_vendor,
                stage="latest",
                stage_cap=1.0,
                deadline=deadline,
                tick_started_at=tick_started_at,
                symbols=("000001",),
                provider="TushareRealtimeClient",
            )

    incidents: list[V20LiveExitStageTimeout] = []

    async def record_incident(
        _context: _DayContext,
        _now: datetime,
        exc: V20LiveExitStageTimeout,
    ) -> None:
        incidents.append(exc)
        exc.diagnostic_alert_emitted = True

    monkeypatch.setattr(service, "_run_exit_cycle", exit_cycle)
    monkeypatch.setattr(service, "_record_live_exit_stage_incident", record_incident)
    monkeypatch.setattr(service, "_live_exit_tick_budget", lambda: 0.2)

    with pytest.raises(V20LiveExitStageTimeout, match="stage latest"):
        await service._run_live_exit_tick(context, datetime(2026, 8, 31, 10, 0, tzinfo=TZ))
    await asyncio.wait_for(cancelled.wait(), timeout=1.0)
    await service._run_live_exit_tick(context, datetime(2026, 8, 31, 10, 0, 15, tzinfo=TZ))

    assert calls == 2
    assert [incident.stage for incident in incidents] == ["latest"]


async def test_latest_minute_vendor_call_has_its_own_hard_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = asyncio.Event()

    class _Client:
        async def batch_get_latest_minute_bars(self, _codes):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    import src.web.v20_service as module

    monkeypatch.setattr(module, "LATEST_MINUTE_POLL_TIMEOUT_SECONDS", 0.01)
    service = _service(monkeypatch, SimpleNamespace(), _Client())

    with pytest.raises(V20LiveExitStageTimeout, match="stage latest") as caught:
        await service._poll_latest(
            _DayContext(trade_date=date(2026, 8, 31), calendar=()),
            ["000001"],
            observed_at=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        )

    assert cancelled.is_set()
    assert caught.value.stage == "latest"
    assert caught.value.provider == "tushare_rt"


async def test_fatal_runtime_lane_cancels_blocked_siblings_even_after_stop_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    service._startup_stage = "RUNNING"
    sibling_started = asyncio.Event()
    sibling_cancelled = asyncio.Event()

    async def sibling() -> None:
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    async def lose_leader() -> None:
        await sibling_started.wait()
        service._stop_event.set()
        raise V20LeadershipLost("leader connection lost")

    blocked = asyncio.create_task(sibling(), name="blocked-decision")
    fatal = asyncio.create_task(lose_leader(), name="fatal-exit")
    service._tasks = [blocked, fatal]
    for task in service._tasks:
        task.add_done_callback(service._runtime_task_finished)

    results = await asyncio.gather(blocked, fatal, return_exceptions=True)

    assert isinstance(results[0], asyncio.CancelledError)
    assert isinstance(results[1], V20LeadershipLost)
    assert sibling_cancelled.is_set()
    assert service._stop_event.is_set()
    assert service.startup_stage == "RUNTIME_FAILED"


async def test_outbox_recovery_lane_reseals_exit_while_decision_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sealed = asyncio.Event()

    class _Repository:
        def __init__(self) -> None:
            self.seal_calls = 0

        async def assert_runtime_leader(self) -> None:
            return None

        async def list_unsealed_outbox_event_ids(self, **_kwargs):
            return () if sealed.is_set() else ("exit-event",)

        async def seal_event(self, _event_id, _builder) -> None:
            self.seal_calls += 1
            if self.seal_calls == 1:
                raise RuntimeError("post-commit seal interrupted")
            sealed.set()

        async def record_outbox_seal_error(self, *_args, **_kwargs) -> bool:
            return True

    repository = _Repository()
    service = _service(monkeypatch, repository)

    async def no_alert(**_kwargs):
        return None

    async def no_status_refresh():
        return None

    async def immediate_tick(_started_at, _cadence):
        if sealed.is_set():
            service._stop_event.set()
        await asyncio.sleep(0)

    monkeypatch.setattr(service, "_safe_alert", no_alert)
    monkeypatch.setattr(service, "_refresh_status_snapshot", no_status_refresh)
    monkeypatch.setattr(service, "_wait_for_runtime_tick", immediate_tick)

    await asyncio.wait_for(service._run_outbox_recovery_scheduler(), timeout=1.0)

    assert sealed.is_set()
    assert repository.seal_calls == 2
    assert service._lane_health["outbox_recovery"].last_error is None


async def test_decision_success_cannot_clear_live_exit_lane_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    now = service._aware_now()

    for lane_name in ("decision", "stale_exit", "outbox_recovery"):
        service._record_lane_success(lane_name, now)
    service._record_lane_error("live_exit", "LIVE_EXIT_CYCLE_TIMEOUT", now)
    service._record_lane_success("decision", now)

    blocker = asyncio.create_task(asyncio.Event().wait())
    service._tasks = [blocker]
    try:
        status = await service.status()
    finally:
        blocker.cancel()
        await asyncio.gather(blocker, return_exceptions=True)

    assert status["healthy"] is False
    assert status["runtime_lanes"]["live_exit"]["healthy"] is False
    assert status["runtime_lanes"]["live_exit"]["last_error"] == "LIVE_EXIT_CYCLE_TIMEOUT"


async def test_decision_retry_error_is_not_cleared_at_end_of_same_scheduler_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def assert_leader() -> None:
        return None

    async def database_cutoff_reached(_cutoff: datetime) -> bool:
        return True

    async def get_entry_status(_stream_id: str, _trade_date: date) -> None:
        return None

    service = _service(
        monkeypatch,
        SimpleNamespace(
            assert_runtime_leader=assert_leader,
            database_cutoff_reached=database_cutoff_reached,
            get_entry_status=get_entry_status,
        ),
    )
    now = service._aware_now()
    service._calendar_loaded_for = now.date()
    service._calendar_cache = ()

    async def retrying_iteration(_now, **_kwargs):
        service._record_lane_error("decision", "ENTRY_MARKET_RETRY: vendor unavailable", now)
        service._stop_event.set()

    monkeypatch.setattr(service, "run_once", retrying_iteration)

    await service._run_scheduler()

    lane = service._lane_health["decision"]
    assert lane.last_error == "ENTRY_MARKET_RETRY: vendor unavailable"
    assert lane.last_success_at is None
    assert lane.error_revision == 1


async def test_publisher_failure_is_unhealthy_until_a_successful_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = asyncio.Event()
    recover = asyncio.Event()

    class _Repository:
        async def assert_runtime_leader(self) -> None:
            return None

    class _Publisher:
        async def run(
            self,
            stop_event: asyncio.Event,
            *,
            before_cycle=None,
            on_cycle_success=None,
            on_cycle_error=None,
        ) -> None:
            assert before_cycle is not None
            await before_cycle()
            assert on_cycle_error is not None
            assert on_cycle_success is not None
            on_cycle_error("Feishu relay returned failure")
            failed.set()
            await recover.wait()
            on_cycle_success()
            stop_event.set()

    service = _service(monkeypatch, _Repository())
    service._publisher = _Publisher()
    task = asyncio.create_task(service._run_publisher_scheduler())

    await asyncio.wait_for(failed.wait(), timeout=1.0)
    assert service._lane_health["publisher"].last_error == (
        "PUBLISH_FAILED: Feishu relay returned failure"
    )

    recover.set()
    await asyncio.wait_for(task, timeout=1.0)
    assert service._lane_health["publisher"].last_error is None
    assert service._lane_health["publisher"].last_success_at is not None


async def test_publisher_leadership_loss_stops_runtime_before_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = False

    class _Repository:
        async def assert_runtime_leader(self) -> None:
            raise V20LeadershipLost("leader connection lost")

    class _Publisher:
        async def run(
            self,
            stop_event: asyncio.Event,
            *,
            before_cycle=None,
            on_cycle_success=None,
            on_cycle_error=None,
        ) -> None:
            nonlocal published
            assert before_cycle is not None
            await before_cycle()
            published = True

    service = _service(monkeypatch, _Repository())
    service._publisher = _Publisher()

    with pytest.raises(V20LeadershipLost):
        await service._run_publisher_scheduler()

    assert published is False
    assert service._stop_event.is_set()
    assert service._lane_health["publisher"].last_error is not None
    assert "LEADERSHIP_LOST" in service._lane_health["publisher"].last_error


async def test_durable_delivery_failure_keeps_status_unhealthy_between_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        delivery_error_n = 1

        async def load_state(self, lineage_id: str) -> StateRecord:
            return StateRecord(lineage_id, 0, "a" * 64, genesis_state())

        async def get_outbox_health(self, **_kwargs):
            return {
                "unsealed_n": 0,
                "pending_delivery_n": self.delivery_error_n,
                "leased_n": 0,
                "seal_error_n": 0,
                "delivery_error_n": self.delivery_error_n,
                "max_seal_attempt_count": 0,
                "max_delivery_attempt_count": 3 if self.delivery_error_n else 0,
                "last_seal_attempt_at": None,
                "oldest_unsent_at": None,
                "last_delivered_at": None,
            }

    repository = _Repository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    now = service._aware_now()
    for lane_name in service._lane_health:
        service._record_lane_success(lane_name, now)
    blocker = asyncio.create_task(asyncio.Event().wait())
    service._tasks = [blocker]
    try:
        await service._refresh_status_snapshot()
        failed = await service.status()
        repository.delivery_error_n = 0
        await service._refresh_status_snapshot()
        recovered = await service.status()
    finally:
        blocker.cancel()
        await asyncio.gather(blocker, return_exceptions=True)

    assert failed["healthy"] is False
    assert failed["runtime_lanes"]["publisher"]["healthy"] is False
    assert failed["runtime_lanes"]["publisher"]["durable_delivery_failures"] == 1
    assert recovered["healthy"] is True
    assert recovered["runtime_lanes"]["publisher"]["healthy"] is True


async def test_runtime_alerts_are_durably_classified_ahead_of_stale_exit_backlog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        semantic: dict[str, Any] | None = None

        async def enqueue_alert(self, _event_id, _route_id, semantic, _hash, **_kwargs):
            self.semantic = dict(semantic)
            return True

        async def seal_event(self, _event_id, _builder):
            return None

    repository = _Repository()
    service = _service(monkeypatch, repository)
    service._repository_started = True

    await service._safe_alert(
        code="LIVE_EXIT_MARKET_DATA_UNAVAILABLE",
        entity_id="2026-08-31:000001",
        message="no legal current-day market evidence",
        now=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
    )

    assert repository.semantic is not None
    assert repository.semantic["delivery_priority_class"] == "RUNTIME_CRITICAL_ALERT"
    assert repository.semantic["schema_version"] == V20_DATA_ALERT_SEMANTIC_SCHEMA
    assert repository.semantic["feishu_formatter_profile"] == V20_FEISHU_FORMATTER_PROFILE


class _MissedSlotRepository:
    def __init__(self, config) -> None:
        payload = genesis_state()
        payload.update(
            state_revision=1,
            last_terminal_slot_id="previous-slot",
            last_terminal_trade_date="2026-08-28",
        )
        self.state = StateRecord(
            config.state_lineage_id,
            1,
            sha256_json(payload),
            payload,
        )
        self.predecessor = replace(
            _entry_status(config),
            trade_date=date(2026, 8, 28),
        )
        self.commits = []
        self.sealed: list[str] = []

    async def load_state(self, lineage_id):
        return self.state

    async def get_entry_status(self, official_stream_id, trade_date):
        if trade_date == date(2026, 8, 28):
            return self.predecessor
        return None

    async def list_active_legs(self, trade_date, **kwargs):
        return []

    async def commit_entry(self, commit):
        self.commits.append(commit)
        self.state = StateRecord(
            commit.lineage_id,
            commit.expected_state_revision + 1,
            commit.next_state_hash,
            commit.next_state,
        )

    async def seal_event(self, event_id, builder):
        self.sealed.append(event_id)


async def test_recovery_finalizes_each_missed_trade_day_before_today(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(monkeypatch)
    repository = _MissedSlotRepository(config)
    service = _service(monkeypatch, repository)
    historical_config_hash = "c" * 64
    historical_state_hash = "d" * 64
    historical_semantic = {
        **repository.predecessor.semantic,
        "config_hash": historical_config_hash,
        "state_semantics_hash": historical_state_hash,
    }
    historical_snapshot = {
        **repository.predecessor.snapshot,
        "state_semantics_hash": historical_state_hash,
    }
    repository.predecessor = replace(
        repository.predecessor,
        config_id=historical_config_hash[:24],
        config_hash=historical_config_hash,
        semantic=historical_semantic,
        semantic_content_hash=sha256_json(historical_semantic),
        snapshot=historical_snapshot,
        snapshot_hash=sha256_json(historical_snapshot),
    )

    async def expire(context, now):
        return None

    async def mature(context, now):
        context.maturity_done = True

    async def policy_inputs(trade_date):
        return [], [], []

    monkeypatch.setattr(service, "_expire_reference_gaps", expire)
    monkeypatch.setattr(service, "_process_mature_shadow", mature)
    monkeypatch.setattr(service, "_policy_inputs", policy_inputs)
    calendar = (
        date(2026, 8, 28),
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
    )

    await service._reconcile_missed_slots(
        datetime(2026, 9, 1, 9, 15, tzinfo=TZ),
        calendar,
    )

    assert [commit.trade_date for commit in repository.commits] == [date(2026, 8, 31)]
    assert repository.commits[0].action == "INPUT_INVALID"
    assert repository.commits[0].semantic["reason_codes"] == ["MISSED_TRADING_DAY_DOWNTIME"]
    assert repository.state.payload["last_terminal_trade_date"] == "2026-08-31"
    assert repository.sealed == [repository.commits[0].event_id]


class _GenesisMissedSlotRepository(_MissedSlotRepository):
    def __init__(self, config) -> None:
        payload = genesis_state()
        self.state = StateRecord(
            config.state_lineage_id,
            0,
            sha256_json(payload),
            payload,
        )
        self.predecessor = None
        self.bootstrap_predecessor = date(2026, 8, 28)
        self.commits = []
        self.sealed = []

    async def load_bootstrap_predecessor_trade_date(self, **scope):
        return self.bootstrap_predecessor

    async def get_entry_status(self, official_stream_id, trade_date):
        return None


async def test_revision_zero_recovery_uses_persisted_genesis_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(monkeypatch)
    repository = _GenesisMissedSlotRepository(config)
    service = _service(monkeypatch, repository)

    async def expire(context, now):
        return None

    async def mature(context, now):
        context.maturity_done = True

    async def policy_inputs(trade_date):
        return [], [], []

    monkeypatch.setattr(service, "_expire_reference_gaps", expire)
    monkeypatch.setattr(service, "_process_mature_shadow", mature)
    monkeypatch.setattr(service, "_policy_inputs", policy_inputs)
    calendar = (
        date(2026, 8, 28),
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
    )

    await service._reconcile_missed_slots(
        datetime(2026, 9, 1, 9, 15, tzinfo=TZ),
        calendar,
    )

    assert [commit.trade_date for commit in repository.commits] == [date(2026, 8, 31)]
    recovered = repository.commits[0]
    assert recovered.action == "INPUT_INVALID"
    assert recovered.shadow_batches == ()
    assert recovered.model_batch is None
    assert recovered.invalid_commit_not_before_ts == datetime(2026, 8, 31, 9, 45, tzinfo=TZ)
    assert recovered.next_state["official_rolling_gaps"] == []


def test_empty_forward_shadow_genesis_has_explicit_first_day_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(monkeypatch)
    predecessor = date(2026, 8, 30)

    bootstrap = _bootstrap_bundle(
        config,
        empty_predecessor_trade_date=predecessor,
    )

    assert bootstrap.predecessor_trade_date == predecessor
    assert bootstrap.state["state_revision"] == 0


def _checkpoint_payload(config, state: Mapping[str, Any], as_of: date) -> dict[str, Any]:
    """Return a valid v3 checkpoint: the exact exporter keyset, no retired fields."""
    return {
        "schema_version": "v20-bootstrap-checkpoint/v3",
        "target_official_stream_id": config.official_stream_id,
        "state_lineage_id": config.state_lineage_id,
        "source_official_stream_id": "shadow-stream",
        "source_lineage_id": "shadow-lineage",
        "as_of_trade_date": as_of.isoformat(),
        "source_state_revision": 42,
        "source_state_hash": "e" * 64,
        "source_bootstrap_mode": "CHECKPOINT",
        "source_bootstrap_checkpoint_hash": None,
        "source_last_terminal_slot_id": "shadow-slot-1",
        "source_last_terminal_trade_date": as_of.isoformat(),
        "batch_id_migration": {},
        "official_state": state,
        "official_state_hash": sha256_json(state),
        "state_shadow_batches": [
            {
                "kind": "ROLLING7",
                "status": "COMPLETE_VALID",
                "signal_date": date(2026, 8, 3).isoformat(),
            }
        ],
    }


_RETIRED_CHECKPOINT_FIELDS = (
    "source_config_hash",
    "source_state_semantics_hash",
    "resolved_state_semantics_hash",
)


def test_checkpoint_v3_valid_shape_carries_no_retired_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(monkeypatch)
    as_of = date(2026, 8, 28)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )
    state = genesis_state()
    state["official_rolling_gaps"] = [
        {
            "gap_id": "legacy-gap",
            "signal_date": "2026-08-01",
            "maturity_date": "2026-08-03",
            "closed": False,
            "aged_out": False,
        }
    ]
    checkpoint = _checkpoint_payload(config, state, as_of)
    assert not set(checkpoint) & set(_RETIRED_CHECKPOINT_FIELDS)
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    bootstrap = _bootstrap_bundle(
        checkpoint_config,
        empty_predecessor_trade_date=date(1999, 1, 1),
    )

    assert bootstrap.predecessor_trade_date == as_of
    assert bootstrap.state["state_revision"] == 0
    assert bootstrap.state["official_rolling_gaps"] == []
    assert bootstrap.shadow_batches == ()


@pytest.mark.parametrize("field", [*_RETIRED_CHECKPOINT_FIELDS, "surprise_field"])
def test_checkpoint_v3_rejects_retired_and_unknown_top_level_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field: str,
) -> None:
    config = _config(monkeypatch)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )
    checkpoint = {
        **_checkpoint_payload(config, genesis_state(), date(2026, 8, 28)),
        field: "f" * 64,
    }
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(V20ConfigError, match="top-level field set mismatch"):
        _bootstrap_bundle(
            checkpoint_config,
            empty_predecessor_trade_date=date(1999, 1, 1),
        )


@pytest.mark.parametrize("field", ["official_state_hash", "state_shadow_batches"])
def test_checkpoint_v3_rejects_missing_top_level_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field: str,
) -> None:
    config = _config(monkeypatch)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )
    checkpoint = _checkpoint_payload(config, genesis_state(), date(2026, 8, 28))
    del checkpoint[field]
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(V20ConfigError, match="top-level field set mismatch"):
        _bootstrap_bundle(
            checkpoint_config,
            empty_predecessor_trade_date=date(1999, 1, 1),
        )


def test_checkpoint_v2_legacy_shape_ignores_arbitrary_retired_hash_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(monkeypatch)
    as_of = date(2026, 8, 28)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )
    state = genesis_state()
    v3_checkpoint = _checkpoint_payload(config, state, as_of)
    v2_checkpoint = {
        **v3_checkpoint,
        "schema_version": "v20-bootstrap-checkpoint/v2",
        # Legacy provenance values are opaque to authorization: any bytes go.
        "source_config_hash": "not-a-real-hash",
        "source_state_semantics_hash": "",
        "resolved_state_semantics_hash": 12345,
    }
    checkpoint_path.write_text(json.dumps(v2_checkpoint), encoding="utf-8")

    v2_bootstrap = _bootstrap_bundle(
        checkpoint_config,
        empty_predecessor_trade_date=date(1999, 1, 1),
    )
    checkpoint_path.write_text(json.dumps(v3_checkpoint), encoding="utf-8")
    v3_bootstrap = _bootstrap_bundle(
        checkpoint_config,
        empty_predecessor_trade_date=date(1999, 1, 1),
    )

    assert v2_bootstrap.predecessor_trade_date == as_of
    assert v2_bootstrap == v3_bootstrap


def test_checkpoint_v2_early_shape_without_resolved_matches_v3(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(monkeypatch)
    as_of = date(2026, 8, 28)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )
    state = genesis_state()
    v3_checkpoint = _checkpoint_payload(config, state, as_of)
    v2_checkpoint = {
        **v3_checkpoint,
        "schema_version": "v20-bootstrap-checkpoint/v2",
        # Early v2 exports predate resolved_state_semantics_hash; the retired
        # provenance values stay opaque to authorization.
        "source_config_hash": "not-a-real-hash",
        "source_state_semantics_hash": "",
    }
    checkpoint_path.write_text(json.dumps(v2_checkpoint), encoding="utf-8")

    v2_bootstrap = _bootstrap_bundle(
        checkpoint_config,
        empty_predecessor_trade_date=date(1999, 1, 1),
    )
    checkpoint_path.write_text(json.dumps(v3_checkpoint), encoding="utf-8")
    v3_bootstrap = _bootstrap_bundle(
        checkpoint_config,
        empty_predecessor_trade_date=date(1999, 1, 1),
    )

    assert v2_bootstrap.predecessor_trade_date == as_of
    assert v2_bootstrap == v3_bootstrap


@pytest.mark.parametrize("field", ["source_config_hash", "source_state_semantics_hash"])
def test_checkpoint_v2_rejects_missing_required_retired_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    field: str,
) -> None:
    config = _config(monkeypatch)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )
    checkpoint = {
        **_checkpoint_payload(config, genesis_state(), date(2026, 8, 28)),
        "schema_version": "v20-bootstrap-checkpoint/v2",
        "source_config_hash": "c" * 64,
        "source_state_semantics_hash": "a" * 64,
    }
    del checkpoint[field]
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(V20ConfigError, match="top-level field set mismatch"):
        _bootstrap_bundle(
            checkpoint_config,
            empty_predecessor_trade_date=date(1999, 1, 1),
        )


@pytest.mark.parametrize("with_resolved", [False, True])
def test_checkpoint_v2_rejects_unknown_top_level_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    with_resolved: bool,
) -> None:
    config = _config(monkeypatch)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )
    checkpoint = {
        **_checkpoint_payload(config, genesis_state(), date(2026, 8, 28)),
        "schema_version": "v20-bootstrap-checkpoint/v2",
        "source_config_hash": "c" * 64,
        "source_state_semantics_hash": "a" * 64,
        "surprise_field": "f" * 64,
    }
    if with_resolved:
        checkpoint["resolved_state_semantics_hash"] = "b" * 64
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(V20ConfigError, match="top-level field set mismatch"):
        _bootstrap_bundle(
            checkpoint_config,
            empty_predecessor_trade_date=date(1999, 1, 1),
        )


def test_checkpoint_as_of_date_is_the_revision_zero_predecessor_anchor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(monkeypatch)
    state = genesis_state()
    as_of = date(2026, 8, 28)
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )

    def checkpoint_payload(schema_version: str) -> dict[str, Any]:
        checkpoint = {
            **_checkpoint_payload(config, state, as_of),
            "schema_version": schema_version,
        }
        if schema_version == "v20-bootstrap-checkpoint/v2":
            checkpoint.update(
                source_config_hash="c" * 64,
                source_state_semantics_hash="a" * 64,
                resolved_state_semantics_hash="b" * 64,
            )
        return checkpoint

    bootstraps = []
    for schema_version in ("v20-bootstrap-checkpoint/v2", "v20-bootstrap-checkpoint/v3"):
        checkpoint_path.write_text(json.dumps(checkpoint_payload(schema_version)), encoding="utf-8")
        bootstraps.append(
            _bootstrap_bundle(
                checkpoint_config,
                empty_predecessor_trade_date=date(1999, 1, 1),
            )
        )

    assert all(bootstrap.predecessor_trade_date == as_of for bootstrap in bootstraps)
    assert bootstraps[0] == bootstraps[1]

    for schema_version in ("v20-bootstrap-checkpoint/v2", "v20-bootstrap-checkpoint/v3"):
        checkpoint = checkpoint_payload(schema_version)
        invalid_schema_state = {**state, "schema_version": "legacy"}
        mutations = (
            ({**checkpoint, "target_official_stream_id": "other-stream"}, "target stream"),
            ({**checkpoint, "state_lineage_id": "other-lineage"}, "checkpoint lineage"),
            (
                {
                    **checkpoint,
                    "official_state": invalid_schema_state,
                    "official_state_hash": sha256_json(invalid_schema_state),
                },
                "official_state schema",
            ),
            ({**checkpoint, "official_state_hash": "0" * 64}, "official_state_hash mismatch"),
        )
        for mutated_checkpoint, expected_error in mutations:
            checkpoint_path.write_text(json.dumps(mutated_checkpoint), encoding="utf-8")
            with pytest.raises(V20ConfigError, match=expected_error):
                _bootstrap_bundle(
                    checkpoint_config,
                    empty_predecessor_trade_date=date(1999, 1, 1),
                )


async def test_checkpoint_as_of_day_is_already_consumed_by_target_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _GenesisMissedSlotRepository(_config(monkeypatch))
    repository.bootstrap_predecessor = date(2026, 8, 31)
    service = _service(monkeypatch, repository)

    assert await service._bootstrap_anchor_covers(date(2026, 8, 31))
    assert not await service._bootstrap_anchor_covers(date(2026, 9, 1))

    with pytest.raises(V20RepositoryError, match="later than the runtime date"):
        await service._bootstrap_anchor_covers(date(2026, 8, 28))


class _LateNormalEntryRepository:
    def __init__(self, config) -> None:
        payload = genesis_state()
        self.state = StateRecord(
            config.state_lineage_id,
            0,
            sha256_json(payload),
            payload,
        )
        self.commits = []
        self.sealed = []

    async def get_entry_status(self, official_stream_id, trade_date):
        if not self.commits:
            return None
        return SimpleNamespace(action=self.commits[-1].action)

    async def load_recent_completed(self, kind, **kwargs):
        return []

    async def load_rolling7_market_health(self, **_kwargs):
        return ()

    async def list_pending_shadow_batches(self, trade_date, **kwargs):
        return []

    async def load_state(self, lineage_id):
        return self.state

    async def list_active_legs(self, trade_date, **kwargs):
        return []

    async def commit_entry(self, commit):
        self.commits.append(commit)
        self.state = StateRecord(
            commit.lineage_id,
            commit.expected_state_revision + 1,
            commit.next_state_hash,
            commit.next_state,
        )

    async def seal_event(self, event_id, builder):
        self.sealed.append(event_id)


@pytest.mark.parametrize(
    ("formed_at", "observed_at"),
    [
        (
            datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
            datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ),
        ),
        (
            datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ),
            datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
        ),
    ],
)
async def test_late_normal_v16_candidate_becomes_gap_without_consumable_batches(
    monkeypatch: pytest.MonkeyPatch,
    formed_at: datetime,
    observed_at: datetime,
) -> None:
    config = _config(monkeypatch)
    repository = _LateNormalEntryRepository(config)

    service = _service(monkeypatch, repository)
    service._clock = lambda: observed_at
    collector = SimpleNamespace(
        complete_codes=lambda: {"000001"},
        codes_with_label=lambda label: {"000001"},
        incomplete_codes=lambda: (),
        freeze=lambda: object(),
        freeze_terminal=lambda: object(),
    )
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
        canonical_bundle=SimpleNamespace(frozen_at=formed_at),
        prewarmed=SimpleNamespace(
            required_minute_codes=("000001", "600000"),
            universe_codes=("000001",),
        ),
        collector=collector,
        breadth_collector=collector,
        early_stored_history_loaded=True,
    )

    await service._attempt_entry(
        context,
        observed_at,
    )

    assert len(repository.commits) == 1
    commit = repository.commits[0]
    assert commit.action == "INPUT_INVALID"
    assert commit.semantic["reason_codes"] == ["INPUT_TIME_BOUNDARY_VIOLATION"]
    assert commit.shadow_batches == ()
    assert commit.model_batch is None
    assert commit.invalid_commit_not_before_ts == datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    assert commit.next_state["official_rolling_gaps"] == []


async def test_missing_0939_coverage_finalizes_no_buy_at_0940_idempotently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(monkeypatch)
    repository = _LateNormalEntryRepository(config)
    service = _service(monkeypatch, repository)
    service._repository_started = True
    scan_called = False

    async def no_collection(*_args, **_kwargs) -> None:
        return None

    async def scan_must_not_run(*_args, **_kwargs):
        nonlocal scan_called
        scan_called = True
        raise AssertionError("a post-cutoff normal V16 scan must not run")

    async def no_alert(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(
        service_module,
        "get_or_compute_canonical_v16",
        scan_must_not_run,
    )
    monkeypatch.setattr(
        v15_scan_service,
        "get_or_compute_canonical_v16",
        scan_must_not_run,
    )
    monkeypatch.setattr(service, "_run_entry_collection_cycle", no_collection)
    monkeypatch.setattr(service, "_safe_alert", no_alert)
    collector = SimpleNamespace(
        complete_codes=lambda: set(),
        codes_with_label=lambda _label: set(),
        incomplete_codes=lambda: ("000001",),
    )
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
        prewarmed=SimpleNamespace(
            required_minute_codes=("000001",),
            universe_codes=("000001",),
        ),
        collector=collector,
        breadth_collector=collector,
        early_stored_history_loaded=True,
        last_phase="COLLECTING_0939",
    )

    cutoff = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    await service._run_entry_cycle(
        context,
        datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ),
    )
    assert repository.commits == []
    assert context.last_phase == "ENTRY_RETRY"
    await service._run_entry_cycle(context, cutoff)
    await service._run_entry_cycle(context, cutoff.replace(second=1))

    assert not scan_called
    assert len(repository.commits) == 1
    commit = repository.commits[0]
    assert commit.action == "INPUT_INVALID"
    assert commit.semantic["schema_version"] == V20_ENTRY_SEMANTIC_SCHEMA
    assert commit.semantic["feishu_formatter_profile"] == V20_FEISHU_FORMATTER_PROFILE
    assert commit.semantic["reason_codes"] == ["ENTRY_INPUT_UNAVAILABLE_BY_0940"]
    assert "canonical V16 09:39 result is unavailable" in commit.semantic["failure_detail"]
    assert commit.invalid_commit_not_before_ts == cutoff
    assert repository.sealed == [commit.event_id]


async def test_entry_collection_computes_canonical_only_at_or_after_0939(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(monkeypatch)
    service = _service(monkeypatch, _LateNormalEntryRepository(config))
    # The iteration timestamp is deliberately stale.  Any cutoff-sensitive
    # budget must be derived from a fresh clock sample, not this value.
    service._clock = lambda: datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ)
    # Today's MEWS cache is already present, so the cycle skips the join.
    service._mews_cached_for = date(2026, 8, 31)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
    )
    compute_calls: list[date] = []
    canonical = _strict_barrier_canonical(context.trade_date)

    async def compute(state: V15ScanState, requested_date: date):
        assert state is service._scan_state
        compute_calls.append(requested_date)
        assert state.canonical_sink is not None
        await state.canonical_sink(canonical)
        return canonical

    monkeypatch.setattr(service_module, "get_or_compute_canonical_v16", compute)
    timeline: list[str] = []
    store = _install_strict_durable_barrier(
        monkeypatch,
        service,
        service._repository,
        timeline,
    )

    await service._run_entry_collection_cycle(
        context,
        datetime(2026, 8, 31, 9, 38, 59, tzinfo=TZ),
    )
    assert compute_calls == []
    assert context.canonical_bundle is None

    await service._run_entry_collection_cycle(
        context,
        datetime(2026, 8, 31, 9, 39, 30, tzinfo=TZ),
    )
    assert compute_calls == [context.trade_date]
    assert context.canonical_bundle is not None
    assert context.canonical_bundle.snapshot_hash == store.record.payload["v20_snapshot_hash"]
    assert timeline == [
        "artifact-load-miss",
        "durable-raw",
        "artifact-load-miss",
        "artifact-save",
        "artifact-load-hit",
        "artifact-hydrate",
        "artifact-load-hit",
        "artifact-hydrate",
    ]
    assert context.last_phase == "CANONICAL_0939_READY"


async def test_decision_watchdog_preempts_blocked_reconciliation_at_0940(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def assert_leader() -> None:
        return None

    async def database_cutoff_reached(_cutoff: datetime) -> bool:
        return True

    async def repository_status(_stream_id: str, _trade_date: date) -> None:
        return None

    service = _service(
        monkeypatch,
        SimpleNamespace(
            assert_runtime_leader=assert_leader,
            database_cutoff_reached=database_cutoff_reached,
            get_entry_status=repository_status,
        ),
    )
    service._repository_started = True
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31),),
        last_phase="MISSED_SLOT_RECONCILIATION",
    )
    service._context = context
    service._calendar_loaded_for = context.trade_date
    service._calendar_cache = context.calendar
    clocks = iter(
        (
            datetime(2026, 8, 31, 9, 39, 59, 990000, tzinfo=TZ),
            datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
        )
    )
    service._clock = lambda: next(clocks)
    blocked = asyncio.Event()
    cancelled = False
    finalized: list[dict[str, Any]] = []

    async def blocked_reconciliation(*_args, **_kwargs) -> None:
        nonlocal cancelled
        try:
            await blocked.wait()
        except asyncio.CancelledError:
            cancelled = True
            raise

    async def no_status(*_args, **_kwargs):
        return None

    async def finalize(_context, now, **kwargs) -> None:
        finalized.append({"context": _context, "now": now, **kwargs})

    monkeypatch.setattr(service, "run_once", blocked_reconciliation)
    monkeypatch.setattr(service, "_refresh_entry_status", no_status)
    monkeypatch.setattr(service, "_finalize_invalid_entry", finalize)

    await service._run_decision_iteration_with_cutoff(datetime(2026, 8, 31, 9, 39, 50, tzinfo=TZ))

    assert cancelled is True
    assert len(finalized) == 1
    assert finalized[0]["context"] is context
    assert finalized[0]["now"] == datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    assert finalized[0]["reason"] == "ENTRY_INPUT_UNAVAILABLE_BY_0940"
    assert finalized[0]["invalid_commit_not_before_ts"] == datetime(2026, 8, 31, 9, 40, tzinfo=TZ)


async def test_decision_watchdog_cancels_only_canonical_waiter_and_master_is_reusable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def assert_leader() -> None:
        return None

    service = _service(monkeypatch, SimpleNamespace(assert_runtime_leader=assert_leader))
    trade_date = date(2026, 8, 31)
    before = datetime(2026, 8, 31, 9, 39, 59, 990000, tzinfo=TZ)
    cutoff = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    clocks = iter((before, cutoff))
    service._clock = lambda: next(clocks)
    master_started = asyncio.Event()
    release_master = asyncio.Event()
    master_cancelled = False
    compute_calls = 0

    base = CanonicalV16ScanBundle(
        trade_date=trade_date,
        scan_result=V16ScanResult(),
        stock_data={},
        clean_boards={},
        universe=(),
        quotes={},
        prev_closes={},
        history_raw={},
        early_bars={},
        early_source_hashes={},
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="a" * 64,
        feature_list_sha256="b" * 64,
        computed_at=cutoff,
        input_hash="c" * 64,
        _integrity_hash="",
        computation_calendar=(
            date(2026, 8, 28),
            trade_date,
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
        prior_trade_date=date(2026, 8, 28),
    )
    canonical = replace(base, _integrity_hash=_bundle_fingerprint(base))

    async def compute(*_args, **_kwargs) -> CanonicalV16ScanBundle:
        nonlocal compute_calls, master_cancelled
        compute_calls += 1
        master_started.set()
        try:
            await release_master.wait()
        except asyncio.CancelledError:
            master_cancelled = True
            raise
        return canonical

    async def run_once(*_args, **_kwargs) -> None:
        await v15_scan_service.get_or_compute_canonical_v16(service._scan_state, trade_date)

    cutoff_calls: list[datetime] = []

    async def enforce(_trade_date: date, *, now: datetime) -> bool:
        cutoff_calls.append(now)
        return True

    monkeypatch.setattr(v15_scan_service, "compute_canonical_v16_scan", compute)
    monkeypatch.setattr(service, "run_once", run_once)
    monkeypatch.setattr(service, "_enforce_or_alert_entry_cutoff", enforce)

    await service._run_decision_iteration_with_cutoff(before)

    assert master_started.is_set()
    assert master_cancelled is False
    coordinator = service._scan_state.canonical_coordinator
    assert coordinator is not None
    master = coordinator.inflight[trade_date]
    assert master.done() is False
    assert cutoff_calls == [cutoff]

    release_master.set()
    reused = await v15_scan_service.get_or_compute_canonical_v16(
        service._scan_state,
        trade_date,
    )
    assert reused.trade_date == trade_date
    assert compute_calls == 1
    assert master_cancelled is False
    assert not any(
        task.get_name().startswith("v20-decision-")
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    )


async def test_decision_watchdog_completed_before_cutoff_runs_once_without_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def assert_leader() -> None:
        return None

    service = _service(monkeypatch, SimpleNamespace(assert_runtime_leader=assert_leader))
    before = datetime(2026, 8, 31, 9, 39, 50, tzinfo=TZ)
    service._clock = lambda: before
    run_calls = 0

    async def run_once(*_args, **_kwargs) -> None:
        nonlocal run_calls
        run_calls += 1

    async def forbidden_cutoff(*_args, **_kwargs) -> bool:
        raise AssertionError("a decision completed before 09:40 must not enter cutoff handling")

    monkeypatch.setattr(service, "run_once", run_once)
    monkeypatch.setattr(service, "_enforce_or_alert_entry_cutoff", forbidden_cutoff)

    await service._run_decision_iteration_with_cutoff(before)

    assert run_calls == 1
    assert not any(
        task.get_name().startswith("v20-decision-")
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    )


async def test_decision_watchdog_boundary_completion_never_duplicates_terminal_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def assert_leader() -> None:
        return None

    service = _service(monkeypatch, SimpleNamespace(assert_runtime_leader=assert_leader))
    before = datetime(2026, 8, 31, 9, 39, 59, 999000, tzinfo=TZ)
    cutoff = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    clocks = iter((before, cutoff))
    service._clock = lambda: next(clocks)
    terminal_commits = 0
    cutoff_checks = 0
    duplicate_alerts = 0

    async def run_once(*_args, **_kwargs) -> None:
        nonlocal terminal_commits
        await asyncio.sleep(0)
        terminal_commits += 1

    async def enforce(_trade_date: date, *, now: datetime) -> bool:
        nonlocal cutoff_checks, duplicate_alerts
        assert now == cutoff
        cutoff_checks += 1
        if terminal_commits == 0:
            duplicate_alerts += 1
        return True

    monkeypatch.setattr(service, "run_once", run_once)
    monkeypatch.setattr(service, "_enforce_or_alert_entry_cutoff", enforce)

    await service._run_decision_iteration_with_cutoff(before)

    assert terminal_commits == 1
    assert cutoff_checks == 1
    assert duplicate_alerts == 0


@pytest.mark.parametrize(
    ("trade_date", "expected_alerts"),
    (
        (date(2026, 8, 31), 1),  # Monday: unresolved potential trading day.
        (date(2026, 8, 30), 0),  # Sunday: cannot be an A-share trading day.
    ),
)
async def test_decision_watchdog_fails_closed_when_first_calendar_load_crosses_cutoff(
    monkeypatch: pytest.MonkeyPatch,
    trade_date: date,
    expected_alerts: int,
) -> None:
    async def assert_leader() -> None:
        return None

    async def database_cutoff_reached(_cutoff: datetime) -> bool:
        return True

    async def get_entry_status(_stream_id: str, _trade_date: date) -> None:
        return None

    service = _service(
        monkeypatch,
        SimpleNamespace(
            assert_runtime_leader=assert_leader,
            database_cutoff_reached=database_cutoff_reached,
            get_entry_status=get_entry_status,
        ),
    )
    service._repository_started = True
    before = datetime.combine(
        trade_date,
        datetime.min.time().replace(hour=9, minute=39, second=59, microsecond=990000),
        tzinfo=TZ,
    )
    cutoff = datetime.combine(
        trade_date,
        datetime.min.time().replace(hour=9, minute=40),
        tzinfo=TZ,
    )
    clocks = iter((before, cutoff))
    service._clock = lambda: next(clocks)
    calendar_cancelled = False
    alerts: list[dict[str, Any]] = []
    never = asyncio.Event()

    async def blocked_calendar(*_args, **_kwargs) -> None:
        nonlocal calendar_cancelled
        try:
            await never.wait()
        except asyncio.CancelledError:
            calendar_cancelled = True
            raise

    async def capture_alert(**kwargs) -> None:
        alerts.append(kwargs)

    monkeypatch.setattr(service, "run_once", blocked_calendar)
    monkeypatch.setattr(service, "_safe_alert", capture_alert)
    error_revision_before = service._lane_health["decision"].error_revision

    await service._run_decision_iteration_with_cutoff(before)

    assert calendar_cancelled is True
    assert len(alerts) == expected_alerts
    if alerts:
        assert alerts[0]["code"] == "ENTRY_CUTOFF_NO_BUY"
        assert alerts[0]["entity_id"] == trade_date.isoformat()
        assert "今天不买，不要追买" in alerts[0]["message"]
        assert service._lane_health["decision"].error_revision == error_revision_before + 1
    else:
        assert service._lane_health["decision"].error_revision == error_revision_before


class _CutoffAlertRepository:
    """Repository double that dedupes alert rows by id like Postgres does."""

    def __init__(self) -> None:
        self.alert_rows: dict[str, dict[str, Any]] = {}

    async def assert_runtime_leader(self) -> None:
        return None

    async def database_cutoff_reached(self, _cutoff: datetime) -> bool:
        return True

    async def get_entry_status(self, _stream_id: str, _trade_date: date) -> None:
        return None

    async def enqueue_alert(
        self,
        alert_id,
        _route_id,
        semantic,
        _semantic_hash,
        **_kwargs,
    ) -> None:
        self.alert_rows.setdefault(alert_id, semantic)

    async def seal_event(self, _event_id, _sealer) -> None:
        return None


async def test_entry_cutoff_cold_start_success_loads_calendar_and_enforces_normally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 14:04 cold start: the in-memory calendar cache is empty, but one bounded
    # load succeeds and the normal cutoff path runs instead of the alert.
    repository = _CutoffAlertRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    trade_date = date(2026, 8, 31)  # Monday
    now = datetime(2026, 8, 31, 14, 4, tzinfo=TZ)
    provider_calls = 0

    async def calendar_provider():
        nonlocal provider_calls
        provider_calls += 1
        return (
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        )

    service._calendar_provider = calendar_provider
    assert service._calendar_loaded_for is None
    alerts: list[dict[str, Any]] = []
    finalized: list[dict[str, Any]] = []

    async def capture_alert(**kwargs) -> None:
        alerts.append(kwargs)

    async def forbidden_finalize(*_args, **kwargs) -> None:
        finalized.append(kwargs)

    monkeypatch.setattr(service, "_safe_alert", capture_alert)
    monkeypatch.setattr(service, "_finalize_invalid_entry", forbidden_finalize)

    cutoff_reached = await service._enforce_or_alert_entry_cutoff(trade_date, now=now)

    assert cutoff_reached is True
    assert provider_calls == 1
    assert service._calendar_loaded_for == trade_date
    # The normal cutoff path emitted the durable no-buy fact; the
    # calendar-unknown alert was not raised and no formal entry was created.
    assert [alert["code"] for alert in alerts] == ["ENTRY_CUTOFF_NO_BUY"]
    assert finalized == []
    assert "ENTRY_CALENDAR_UNKNOWN_AT_0940" not in (
        service._lane_health["decision"].last_error or ""
    )


async def test_entry_cutoff_cold_start_failure_alerts_once_with_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _CutoffAlertRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    trade_date = date(2026, 8, 31)  # Monday
    now = datetime(2026, 8, 31, 14, 4, tzinfo=TZ)

    async def failing_provider():
        raise RuntimeError("vendor calendar endpoint down")

    service._calendar_provider = failing_provider

    for _ in range(2):
        assert await service._enforce_or_alert_entry_cutoff(trade_date, now=now) is True

    assert len(repository.alert_rows) == 1
    semantic = next(iter(repository.alert_rows.values()))
    assert semantic["alert_code"] == "ENTRY_CUTOFF_NO_BUY"
    assert semantic["entity_id"] == trade_date.isoformat()
    assert "今天不买，不要追买" in semantic["message"]
    last_error = service._lane_health["decision"].last_error or ""
    assert "ENTRY_CALENDAR_UNKNOWN_AT_0940" in last_error
    assert "RuntimeError" in last_error
    assert "vendor calendar endpoint down" in last_error
    # A raised load never marks the day as loaded, so the next scheduler
    # iteration naturally retries.
    assert service._calendar_loaded_for is None


async def test_entry_cutoff_cold_start_timeout_alerts_once_and_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _CutoffAlertRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    trade_date = date(2026, 8, 31)  # Monday
    now = datetime(2026, 8, 31, 14, 4, tzinfo=TZ)
    monkeypatch.setattr(
        "src.web.v20_service._CALENDAR_CUTOFF_LOAD_BUDGET_SECONDS",
        0.05,
    )
    never = asyncio.Event()

    async def blocked_provider():
        await never.wait()
        return ()

    service._calendar_provider = blocked_provider

    # The outer bound proves the watchdog call returns instead of hanging on
    # the blocked vendor request.
    cutoff_reached = await asyncio.wait_for(
        service._enforce_or_alert_entry_cutoff(trade_date, now=now),
        timeout=5.0,
    )

    assert cutoff_reached is True
    assert len(repository.alert_rows) == 1
    semantic = next(iter(repository.alert_rows.values()))
    assert semantic["alert_code"] == "ENTRY_CUTOFF_NO_BUY"
    last_error = service._lane_health["decision"].last_error or ""
    assert "ENTRY_CALENDAR_UNKNOWN_AT_0940" in last_error
    assert "TimeoutError" in last_error
    assert service._calendar_loaded_for is None


async def test_post_cutoff_watchdog_lost_leader_cannot_write_or_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    side_effects: list[str] = []

    async def assert_leader() -> None:
        raise V20LeadershipLost("leader session was replaced")

    async def database_cutoff_reached(_cutoff: datetime) -> bool:
        side_effects.append("database_cutoff")
        return True

    service = _service(
        monkeypatch,
        SimpleNamespace(
            assert_runtime_leader=assert_leader,
            database_cutoff_reached=database_cutoff_reached,
        ),
    )
    cutoff = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    service._clock = lambda: cutoff

    async def forbidden_run(*_args, **_kwargs) -> None:
        side_effects.append("run_once")

    async def forbidden_alert(**_kwargs) -> None:
        side_effects.append("alert")

    monkeypatch.setattr(service, "run_once", forbidden_run)
    monkeypatch.setattr(service, "_safe_alert", forbidden_alert)

    with pytest.raises(V20LeadershipLost, match="leader session was replaced"):
        await service._run_decision_iteration_with_cutoff(cutoff)

    assert side_effects == []


@pytest.mark.parametrize("calendar_known", (True, False))
async def test_fast_application_clock_waits_for_database_before_cutoff_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    calendar_known: bool,
) -> None:
    leader_checks = 0
    database_cutoffs: list[datetime] = []
    side_effects: list[str] = []

    async def assert_leader() -> None:
        nonlocal leader_checks
        leader_checks += 1

    async def database_cutoff_reached(cutoff: datetime) -> bool:
        database_cutoffs.append(cutoff)
        return False

    service = _service(
        monkeypatch,
        SimpleNamespace(
            assert_runtime_leader=assert_leader,
            database_cutoff_reached=database_cutoff_reached,
        ),
    )
    cutoff = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    service._clock = lambda: cutoff
    if calendar_known:
        service._calendar_loaded_for = cutoff.date()
        service._calendar_cache = (cutoff.date(),)
        service._context = _DayContext(
            trade_date=cutoff.date(),
            calendar=(cutoff.date(),),
        )

    async def forbidden_run(*_args, **_kwargs) -> None:
        side_effects.append("run_once")

    async def forbidden_finalize(*_args, **_kwargs) -> None:
        side_effects.append("finalize")

    async def forbidden_alert(**_kwargs) -> None:
        side_effects.append("alert")

    monkeypatch.setattr(service, "run_once", forbidden_run)
    monkeypatch.setattr(service, "_enforce_entry_cutoff", forbidden_finalize)
    monkeypatch.setattr(service, "_safe_alert", forbidden_alert)

    await service._run_decision_iteration_with_cutoff(cutoff)

    assert leader_checks == 2
    assert database_cutoffs == [cutoff]
    assert side_effects == []


async def test_entry_cutoff_commit_failure_emits_stable_idempotent_no_buy_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alert_ids: list[str] = []
    semantics: list[dict[str, Any]] = []

    class _Repository:
        async def enqueue_alert(
            self,
            alert_id,
            _route_id,
            semantic,
            _semantic_hash,
            **_kwargs,
        ) -> None:
            alert_ids.append(alert_id)
            semantics.append(semantic)

        async def seal_event(self, _event_id, _sealer) -> None:
            return None

    service = _service(monkeypatch, _Repository())
    service._repository_started = True
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31),),
        last_phase="ENTRY_BLOCKED_BY_STATE_RECONCILIATION",
    )
    service._context = context

    async def no_status(*_args, **_kwargs):
        return None

    async def commit_failure(*_args, **_kwargs) -> None:
        raise V20RepositoryError("missing predecessor state")

    monkeypatch.setattr(service, "_refresh_entry_status", no_status)
    monkeypatch.setattr(service, "_finalize_invalid_entry", commit_failure)
    cutoff = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    error_revision_before = service._lane_health["decision"].error_revision

    await service._enforce_entry_cutoff(context.trade_date, now=cutoff)
    await service._enforce_entry_cutoff(context.trade_date, now=cutoff)

    assert len(alert_ids) == 2
    assert alert_ids[0] == alert_ids[1]
    assert semantics[0] == semantics[1]
    assert semantics[0]["alert_code"] == "ENTRY_CUTOFF_NO_BUY"
    assert "今天不买，不要追买" in semantics[0]["message"]
    assert service._lane_health["decision"].error_revision == error_revision_before + 2
    assert service._lane_health["decision"].last_error is not None
    assert "ENTRY_CUTOFF_FINALIZATION_FAILED" in service._lane_health["decision"].last_error


async def test_0940_cutoff_does_not_wait_for_health_maturity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    service._repository_started = True
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
    )
    entry_times: list[datetime] = []

    async def no_op(*_args, **_kwargs) -> None:
        return None

    async def false_phase(*_args, **_kwargs) -> bool:
        return False

    async def calendar_provider():
        return list(context.calendar)

    async def ensure_context(_current, _calendar):
        return context

    async def bootstrap_not_covering(*_args, **_kwargs):
        return False

    async def entry(_context, current):
        entry_times.append(current)

    service._repository.assert_runtime_leader = no_op
    service._calendar_provider = calendar_provider
    monkeypatch.setattr(service, "_ensure_context", ensure_context)
    monkeypatch.setattr(service, "_bootstrap_anchor_covers", bootstrap_not_covering)
    monkeypatch.setattr(service, "_run_entry_collection_cycle", no_op)
    monkeypatch.setattr(service, "_reconcile_missed_slots", no_op)
    monkeypatch.setattr(service, "_expire_reference_gaps", no_op)
    monkeypatch.setattr(service, "_process_mature_shadow", false_phase)
    monkeypatch.setattr(service, "_run_entry_cycle", entry)
    monkeypatch.setattr(service, "_run_reference_cycle", no_op)
    monkeypatch.setattr(service, "_run_reminders", no_op)

    cutoff = datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    await service.run_once(
        cutoff,
        include_exit_cycles=False,
        include_outbox_recovery=False,
    )

    assert entry_times == [cutoff]


async def test_entry_poll_coverage_cannot_be_filled_by_breadth_only_codes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def assert_leader() -> None:
        return None

    service = _service(
        monkeypatch,
        SimpleNamespace(assert_runtime_leader=assert_leader),
    )
    universe = tuple(f"{index:06d}" for index in range(10))
    breadth_only = tuple(f"6{index:05d}" for index in range(100))
    required = tuple(sorted(set(universe).union(breadth_only)))
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31),),
        prewarmed=SimpleNamespace(
            universe_codes=universe,
            breadth_codes=breadth_only,
            required_minute_codes=required,
        ),
        collector=SimpleNamespace(ingest=lambda _rows: None),
        breadth_collector=SimpleNamespace(ingest=lambda _rows: None),
    )
    for code in (*universe[:7], *breadth_only):
        context.minute_rows[(context.trade_date, code, "09:38")] = None  # type: ignore[assignment]
    polls: list[tuple[str, ...]] = []

    async def poll_latest(_context, codes, *, observed_at):
        polls.append(tuple(codes))
        return {}

    monkeypatch.setattr(service, "_poll_latest", poll_latest)

    await service._poll_entry_market(
        context,
        datetime(2026, 8, 31, 9, 38, 30, tzinfo=TZ),
    )

    assert polls == [required]


class _MaturityClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def fetch_daily_bars(self, trade_date: str):
        self.calls.append(trade_date)
        if self.fail:
            raise RuntimeError("daily source unavailable")
        return {}


class _MaturityRepository:
    def __init__(self, *, snapshot: Any = None) -> None:
        self.snapshot = snapshot
        self.requested_cutoffs: list[datetime | None] = []
        self.recorded: list[tuple[date, Any]] = []
        self.completed: list[dict[str, Any]] = []
        self.recent_completed_calls = 0
        self.batch = SimpleNamespace(
            batch_id="health-1",
            kind="HEALTH",
            signal_date=date(2026, 8, 27),
            t2_date=date(2026, 8, 28),
            status="PENDING",
            payload={
                "top3": [
                    {"code": "000001"},
                    {"code": "000002"},
                    {"code": "000003"},
                ],
                "comparison_pool_codes": ["000001", "000002", "000003"],
            },
            batch_return=None,
            reference_status="LOCKED",
            reference_prices={"000001": 10.0, "000002": 10.0, "000003": 10.0},
            reference_snapshot_hash="a" * 64,
        )

    async def list_pending_shadow_batches(self, trade_date, **kwargs):
        return [self.batch]

    async def load_recent_completed(self, *args, **kwargs):
        self.recent_completed_calls += 1
        return []

    async def database_cutoff_reached(self, cutoff):
        return True

    async def record_daily_bar_snapshot(self, trade_date, payload):
        self.recorded.append((trade_date, payload))

    async def load_latest_daily_bar_snapshot(self, trade_date, *, received_before=None):
        self.requested_cutoffs.append(received_before)
        if self.snapshot is None:
            return None
        if received_before is not None and self.snapshot.first_received_at > received_before:
            return None
        return self.snapshot

    async def list_daily_bar_snapshots(self, trade_date, *, received_before=None):
        self.requested_cutoffs.append(received_before)
        if self.snapshot is None:
            return [], ()
        if received_before is not None and self.snapshot.first_received_at > received_before:
            return [], ()
        return [self.snapshot], ()

    async def complete_shadow_batch(self, batch_id, **kwargs):
        self.completed.append({"batch_id": batch_id, **kwargs})
        return True


@pytest.mark.parametrize(
    ("now", "expected_completed", "expected_maturity_done"),
    [
        (datetime(2026, 8, 31, 9, 38, tzinfo=TZ), 0, False),
        (datetime(2026, 8, 31, 9, 39, tzinfo=TZ), 1, True),
        (datetime(2026, 9, 2, 9, 15, tzinfo=TZ), 1, True),
    ],
)
async def test_health_maturity_uses_fixed_d3_0939_cutoff_without_late_lookahead(
    monkeypatch: pytest.MonkeyPatch,
    now: datetime,
    expected_completed: int,
    expected_maturity_done: bool,
) -> None:
    late_snapshot = SimpleNamespace(
        snapshot_id="daily-after-cutoff",
        source_hash="c" * 64,
        first_received_at=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
        payload={"bars": {}},
    )
    repository = _MaturityRepository(snapshot=late_snapshot)
    service = _service(monkeypatch, repository, _MaturityClient())
    context = _DayContext(
        trade_date=now.date(),
        calendar=(
            date(2026, 8, 27),
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
    )

    await service._process_mature_shadow(context, now)

    fixed_cutoff = datetime(2026, 8, 31, 9, 39, tzinfo=TZ)
    assert repository.requested_cutoffs == ([] if now < fixed_cutoff else [fixed_cutoff])
    assert len(repository.completed) == expected_completed
    assert context.maturity_done is expected_maturity_done
    if repository.completed:
        completed = repository.completed[0]
        assert completed["status"] == "COMPLETE_INVALID"
        assert completed["payload_update"]["health_maturity_cutoff_ts"] == fixed_cutoff.isoformat()
        assert completed["payload_update"]["daily_snapshot_id"] is None


async def test_health_maturity_source_failure_does_not_hide_persisted_cutoff_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_cutoff = datetime(2026, 8, 31, 9, 39, tzinfo=TZ)
    snapshot = SimpleNamespace(
        snapshot_id="daily-before-cutoff",
        source_hash="b" * 64,
        first_received_at=datetime(2026, 8, 31, 9, 38, tzinfo=TZ),
        payload={"bars": {}},
    )
    repository = _MaturityRepository(snapshot=snapshot)
    client = _MaturityClient(fail=True)
    service = _service(monkeypatch, repository, client)
    context = _DayContext(
        trade_date=date(2026, 9, 2),
        calendar=(
            date(2026, 8, 27),
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
    )

    await service._process_mature_shadow(
        context,
        datetime(2026, 9, 2, 9, 15, tzinfo=TZ),
    )

    assert repository.requested_cutoffs == [fixed_cutoff]
    assert len(repository.completed) == 1
    assert repository.completed[0]["payload_update"]["daily_snapshot_id"] == snapshot.snapshot_id
    assert context.maturity_done is True


async def test_legacy_rolling_maturity_row_is_inert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received = datetime(2026, 8, 31, 15, 5, tzinfo=TZ)
    newer_empty = SimpleNamespace(
        snapshot_id="daily-newer-empty",
        source_hash="d" * 64,
        first_received_at=received,
        payload={"trade_date": "2026-08-28", "bars": {}},
    )
    older_complete = SimpleNamespace(
        snapshot_id="daily-older-complete",
        source_hash="e" * 64,
        first_received_at=received - timedelta(seconds=1),
        payload={
            "trade_date": "2026-08-28",
            "bars": {
                "000001": {
                    "stock_code": "000001",
                    "trade_date": "20260828",
                    "close_price": 11.0,
                    "amount_yuan": 1_000_000.0,
                }
            },
        },
    )

    class _CandidateRepository(_MaturityRepository):
        async def list_daily_bar_snapshots(self, trade_date, *, received_before=None):
            self.requested_cutoffs.append(received_before)
            return [newer_empty, older_complete], ()

    repository = _CandidateRepository()
    repository.batch.kind = "ROLLING7"
    repository.batch.payload = {"symbols": [{"code": "000001"}]}
    repository.batch.reference_prices = {"000001": 10.0}
    client = _MaturityClient(fail=True)
    service = _service(monkeypatch, repository, client)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 31)),
    )

    await service._process_mature_shadow(context, received)

    assert client.calls == []
    assert repository.recorded == []
    assert repository.requested_cutoffs == []
    assert repository.completed == []
    assert repository.recent_completed_calls == 0
    assert context.maturity_done is True


def _bar(
    code: str,
    label: str,
    *,
    open_price: float = 10.0,
    close: float = 10.0,
    trade_date: date = date(2026, 8, 31),
):
    return TushareMinuteBar(
        stock_code=code,
        bar_end=datetime.fromisoformat(f"{trade_date.isoformat()}T{label}:00").replace(tzinfo=TZ),
        end_label=label,
        open_price=open_price,
        high_price=max(open_price, close),
        low_price=min(open_price, close),
        close_price=close,
        volume=100.0,
        amount=1_000.0,
    )


class _ReferenceClient:
    async def batch_get_latest_minute_bars(self, codes):
        return {code: _bar(code, "09:41", open_price=10.0) for code in codes}


class _ReferenceRepository:
    def __init__(self) -> None:
        self.locked: dict[str, float] | None = None
        self.recorded: list[dict[str, Any]] = []

    async def record_minute_bars(self, rows):
        self.recorded.extend(rows)
        return frozenset(sha256_json(row) for row in rows)

    async def list_raw_minute_bar_records(self, *args, **kwargs):
        return []

    async def get_shadow_reference_status(self, signal_date, **kwargs):
        return "PENDING"

    async def update_shadow_references(
        self, signal_date, *, reference_prices, snapshot_hash, **kwargs
    ):
        self.locked = dict(reference_prices)
        return ("health", "rolling")

    async def list_pending_reference_legs(self, signal_date, **kwargs):
        return []


class _DurableReferenceRepository(_ReferenceRepository):
    def __init__(self, status: EntryStatus) -> None:
        super().__init__()
        self.status = status

    async def get_entry_status(self, official_stream_id, trade_date):
        return self.status


async def test_same_day_context_recovers_durable_entry_and_stages_0941_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(monkeypatch)
    repository = _DurableReferenceRepository(_entry_status(config))
    service = _service(monkeypatch, repository, _ReferenceClient())
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31),),
        entry_status=None,
    )
    service._context = context

    recovered = await service._ensure_context(
        datetime(2026, 8, 31, 9, 41, 10, tzinfo=TZ),
        context.calendar,
    )
    await service._run_reference_cycle(
        recovered,
        datetime(2026, 8, 31, 9, 41, 10, tzinfo=TZ),
    )

    assert recovered.entry_status is repository.status
    assert {row["stock_code"] for row in repository.recorded} == {"000001", "000002"}


async def test_reference_cycle_stages_raw_0941_without_early_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ReferenceRepository()
    service = _service(monkeypatch, repository, _ReferenceClient())
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31),),
        entry_status=_entry_status(service.config),
    )

    await service._run_reference_cycle(
        context,
        datetime(2026, 8, 31, 9, 41, 10, tzinfo=TZ),
    )

    assert context.reference_finalized is False
    assert context.last_phase == "REFERENCE_EVIDENCE_STAGED"
    assert repository.locked is None
    assert {row["stock_code"] for row in repository.recorded} == {"000001", "000002"}


class _ChangingReferenceClient:
    def __init__(self) -> None:
        self.price = 10.0

    async def batch_get_latest_minute_bars(self, codes):
        return {
            code: _bar(code, "09:41", open_price=self.price, close=self.price) for code in codes
        }

    async def batch_get_minute_history(self, codes):
        return {}


async def test_reference_cycle_keeps_staging_revisions_until_collection_window_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ReferenceRepository()
    client = _ChangingReferenceClient()
    service = _service(monkeypatch, repository, client)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31),),
        entry_status=_entry_status(service.config),
    )

    await service._run_reference_cycle(
        context,
        datetime(2026, 8, 31, 9, 41, 10, tzinfo=TZ),
    )
    client.price = 10.5
    await service._run_reference_cycle(
        context,
        datetime(2026, 8, 31, 9, 41, 13, tzinfo=TZ),
    )

    assert context.reference_finalized is False
    assert {row["open"] for row in repository.recorded} == {10.0, 10.5}


class _RestartReferenceRepository:
    def __init__(self, *, origin_kind: str = "OFFICIAL_ENTRY") -> None:
        self.locked: tuple[str, float] | None = None
        self.received_before: datetime | None = None
        self.finalized = False
        self.rows: list[Any] | None = None
        self.origin_kind = origin_kind
        self.reference_status = "PENDING"
        self.reference_price: float | None = None
        self.reference_snapshot_hash: str | None = None
        self.list_active_calls = 0

    async def list_active_legs(self, trade_date, **kwargs):
        self.list_active_calls += 1
        return [
            ActiveModelLeg(
                model_leg_id="leg",
                model_batch_id="batch",
                decision_id=("decision" if self.origin_kind == "OFFICIAL_ENTRY" else None),
                signal_date=date(2026, 8, 28),
                code="000001",
                stock_name="测试股",
                rank=1,
                relative_weight=1.0,
                d1=date(2026, 8, 31),
                d2=date(2026, 9, 1),
                reference_status=self.reference_status,
                reference_price=self.reference_price,
                reference_snapshot_hash=self.reference_snapshot_hash,
                evaluation_only=False,
                mews_snapshot_id=None,
                mews_fast_state=None,
                exit_intent_id=None,
                origin_kind=self.origin_kind,
                source_event_id=(
                    "entry-source" if self.origin_kind == "OFFICIAL_ENTRY" else "d" * 64
                ),
            )
        ]

    async def database_cutoff_reached(self, cutoff):
        return True

    async def list_pending_shadow_reference_batches(self, before_signal_date, **kwargs):
        return []

    async def list_pending_reference_legs(self, signal_date, **kwargs):
        return [
            SimpleNamespace(
                model_leg_id="leg",
                signal_date=signal_date,
                code="000001",
            )
        ]

    async def list_raw_minute_bar_records(
        self, codes, *, trade_date, end_labels, received_before=None
    ):
        self.received_before = received_before
        if self.rows is not None:
            return self.rows
        payload = {
            "stock_code": "000001",
            "bar_end": "2026-08-28T09:41:00+08:00",
            "end_label": "09:41",
            "open": 10.2,
            "high": 10.3,
            "low": 10.1,
            "close": 10.2,
            "volume": 100.0,
            "amount": 1_000.0,
            "source_confirms_complete": True,
        }
        return [
            SimpleNamespace(
                code="000001",
                payload=payload,
                source_hash=sha256_json(payload),
                first_received_at=datetime(2026, 8, 28, 9, 42, tzinfo=TZ),
            )
        ]

    async def lock_reference_price(
        self, model_leg_id, *, reference_profile_id, price, snapshot_hash, **kwargs
    ):
        self.locked = (model_leg_id, price)
        self.reference_status = "LOCKED"
        self.reference_price = price
        self.reference_snapshot_hash = snapshot_hash

    async def finalize_pending_references_unavailable(self, *args, **kwargs):
        self.finalized = True
        return ()


async def test_d1_restart_recovers_only_reference_received_before_fixed_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _RestartReferenceRepository()
    service = _service(monkeypatch, repository)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31), date(2026, 9, 1)),
    )

    await service._expire_reference_gaps(
        context,
        datetime(2026, 8, 31, 9, 30, tzinfo=TZ),
    )

    assert repository.received_before == datetime(2026, 8, 31, 9, 30, tzinfo=TZ)
    assert repository.locked == ("leg", 10.2)
    assert repository.finalized is True


async def test_manual_monitor_restart_locks_reference_and_enters_ordinary_exit_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _RestartReferenceRepository(origin_kind="MANUAL_MONITOR")
    service = _service(monkeypatch, repository)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31), date(2026, 9, 1)),
    )
    now = datetime(2026, 8, 31, 9, 30, tzinfo=TZ)

    await service._expire_reference_gaps(context, now)

    evaluated: list[ActiveModelLeg] = []

    async def capture(active, _now, _calendar=(), **_kwargs):
        evaluated.extend(active)

    monkeypatch.setattr(service, "_evaluate_active_exits", capture)
    await service._run_exit_cycle(context, now, include_stale=False)

    assert repository.received_before == datetime(2026, 8, 31, 9, 30, tzinfo=TZ)
    assert repository.locked == ("leg", 10.2)
    assert repository.list_active_calls >= 2
    assert evaluated
    assert all(leg.origin_kind == "MANUAL_MONITOR" for leg in evaluated)
    assert all(leg.decision_id is None for leg in evaluated)
    assert all(leg.reference_status == "LOCKED" for leg in evaluated)
    assert all(leg.reference_price == 10.2 for leg in evaluated)


def _reference_record(price: float, received_at: datetime) -> Any:
    payload = {
        "stock_code": "000001",
        "bar_end": "2026-08-28T09:41:00+08:00",
        "end_label": "09:41",
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 100.0,
        "amount": 1_000.0,
        "source_confirms_complete": True,
    }
    return SimpleNamespace(
        code="000001",
        payload=payload,
        source_hash=sha256_json(payload),
        first_received_at=received_at,
    )


async def test_reference_deadline_chooses_latest_eligible_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _RestartReferenceRepository()
    repository.rows = [
        _reference_record(10.0, datetime(2026, 8, 28, 9, 42, tzinfo=TZ)),
        _reference_record(10.8, datetime(2026, 8, 28, 9, 44, tzinfo=TZ)),
    ]
    service = _service(monkeypatch, repository)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31), date(2026, 9, 1)),
    )

    await service._expire_reference_gaps(
        context,
        datetime(2026, 8, 31, 9, 30, tzinfo=TZ),
    )

    assert repository.locked == ("leg", 10.8)
    assert repository.finalized is True


async def test_reference_deadline_ignores_later_illegal_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _RestartReferenceRepository()
    legal = _reference_record(10.0, datetime(2026, 8, 28, 9, 42, tzinfo=TZ))
    illegal = _reference_record(10.8, datetime(2026, 8, 28, 9, 44, tzinfo=TZ))
    illegal.payload["volume"] = 0.0
    illegal.source_hash = sha256_json(illegal.payload)
    repository.rows = [legal, illegal]
    service = _service(monkeypatch, repository)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31), date(2026, 9, 1)),
    )

    await service._expire_reference_gaps(
        context,
        datetime(2026, 8, 31, 9, 30, tzinfo=TZ),
    )

    assert repository.locked == ("leg", 10.0)


class _ShadowReferenceRepository:
    def __init__(self, *, kind: str = "HEALTH") -> None:
        self.kind = kind
        self.cutoff: datetime | None = None
        self.locked: dict[str, float] | None = None
        self.cutoff_checks = 0
        self.pending_leg_calls = 0

    async def list_active_legs(self, trade_date, **kwargs):
        return []

    async def database_cutoff_reached(self, cutoff):
        self.cutoff_checks += 1
        return True

    async def list_pending_shadow_reference_batches(self, before_signal_date, **kwargs):
        return [
            SimpleNamespace(
                batch_id="health",
                kind=self.kind,
                signal_date=date(2026, 8, 28),
                payload={
                    "d1": "2026-08-31",
                    "top3": [{"code": "000001"}],
                    "comparison_pool_codes": ["000001"],
                },
            )
        ]

    async def list_pending_reference_legs(self, signal_date, **kwargs):
        self.pending_leg_calls += 1
        return []

    async def list_raw_minute_bar_records(
        self, codes, *, trade_date, end_labels, received_before=None
    ):
        self.cutoff = received_before
        return [_reference_record(10.0, datetime(2026, 8, 28, 9, 42, tzinfo=TZ))]

    async def update_shadow_references(
        self, signal_date, *, reference_prices, snapshot_hash, **kwargs
    ):
        self.locked = dict(reference_prices)
        return ("health",)

    async def finalize_pending_references_unavailable(self, *args, **kwargs):
        return ()


async def test_shadow_reference_uses_explicit_d0_0945_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ShadowReferenceRepository()
    service = _service(monkeypatch, repository)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31), date(2026, 9, 1)),
    )

    await service._expire_reference_gaps(
        context,
        datetime(2026, 8, 31, 9, 30, tzinfo=TZ),
    )

    assert repository.cutoff == datetime(2026, 8, 28, 9, 45, tzinfo=TZ)
    assert repository.locked == {"000001": 10.0}


async def test_legacy_rolling_reference_gap_is_inert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ShadowReferenceRepository(kind="ROLLING7")
    service = _service(monkeypatch, repository)
    alerts: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def capture_alert(*args: Any, **kwargs: Any) -> None:
        alerts.append((args, kwargs))

    monkeypatch.setattr(service, "_safe_alert", capture_alert)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31), date(2026, 9, 1)),
    )

    await service._expire_reference_gaps(
        context,
        datetime(2026, 8, 31, 9, 30, tzinfo=TZ),
    )

    assert repository.cutoff_checks == 0
    assert repository.pending_leg_calls == 0
    assert repository.cutoff is None
    assert repository.locked is None
    assert alerts == []


async def test_reference_equal_latest_receipt_with_different_hashes_is_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _RestartReferenceRepository()
    equal_receipt = datetime(2026, 8, 28, 9, 44, tzinfo=TZ)
    repository.rows = [
        _reference_record(10.0, datetime(2026, 8, 28, 9, 42, tzinfo=TZ)),
        _reference_record(10.8, equal_receipt),
        _reference_record(11.2, equal_receipt),
    ]
    service = _service(monkeypatch, repository)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31), date(2026, 9, 1)),
    )

    await service._expire_reference_gaps(
        context,
        datetime(2026, 8, 31, 9, 30, tzinfo=TZ),
    )

    assert repository.locked is None
    assert repository.finalized is True


class _ExitRepository:
    def __init__(self) -> None:
        self.commit = None
        self.sealed = None

    async def select_mews_for_leg(self, *args, **kwargs):
        return None, None, "MEWS_UNAVAILABLE_FALLBACK_12"

    async def load_selected_mews_for_leg(self, model_leg_id):
        return None

    async def get_exit_scan_watermarks(self, model_leg_id, **kwargs):
        return {}

    async def list_minute_bars(self, code, *, trade_dates, end_cutoff):
        rows = []
        for label in FULL_EXIT_LABELS:
            rows.append(
                SimpleNamespace(
                    payload={
                        "stock_code": code,
                        "bar_end": f"2026-08-28T{label}:00+08:00",
                        "open": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "close": 10.0,
                        "volume": 100.0,
                        "amount": 1_000.0,
                        "source_confirms_complete": True,
                    }
                )
            )
        for label in FULL_EXIT_LABELS:
            if label > "10:00":
                break
            close = 8.8 if label == "10:00" else 10.0
            rows.append(
                SimpleNamespace(
                    payload={
                        "stock_code": code,
                        "bar_end": f"2026-08-31T{label}:00+08:00",
                        "open": 10.0,
                        "high": 10.0,
                        "low": close,
                        "close": close,
                        "volume": 100.0,
                        "amount": 1_000.0,
                        "source_confirms_complete": True,
                    }
                )
            )
        return rows

    async def commit_exit(self, commit):
        self.commit = commit
        return True

    async def seal_event(self, event_id, builder):
        self.sealed = event_id


async def test_d2_minus_12_creates_and_seals_full_model_leg_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ExitRepository()
    service = _service(monkeypatch, repository)
    leg = ActiveModelLeg(
        model_leg_id="leg",
        model_batch_id="batch",
        decision_id="decision",
        signal_date=date(2026, 8, 27),
        code="000001",
        stock_name="测试股",
        rank=1,
        relative_weight=0.05,
        d1=date(2026, 8, 28),
        d2=date(2026, 8, 31),
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="a" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )

    await service._evaluate_one_exit(
        leg,
        datetime(2026, 8, 31, 10, 0, 30, tzinfo=TZ),
    )

    assert repository.commit is not None
    assert repository.commit.signal_type == "D2_ENTRY_12"
    assert repository.commit.semantic["exit_scope"] == "FULL_MODEL_LEG"
    assert repository.commit.semantic["recommended_exit_fraction"] == 1.0
    assert repository.commit.semantic["delivery_priority_class"] == "LIVE_EXIT"
    assert repository.commit.semantic["schema_version"] == V20_EXIT_SEMANTIC_SCHEMA
    assert repository.commit.semantic["feishu_formatter_profile"] == V20_FEISHU_FORMATTER_PROFILE
    assert repository.commit.semantic["rule_actionable_from"] == "2026-08-31T10:01:00+08:00"
    assert "actionable_from" not in repository.commit.semantic
    assert repository.sealed == repository.commit.event_id


class _SelectionSpyExitRepository(_ExitRepository):
    def __init__(self) -> None:
        super().__init__()
        self.selection_calls: list[dict[str, Any]] = []

    async def select_mews_for_leg(self, model_leg_id, **kwargs):
        self.selection_calls.append({"model_leg_id": model_leg_id, **kwargs})
        return None, None, "MEWS_UNAVAILABLE_FALLBACK_12"


async def test_d2_selection_offers_late_same_day_mews_window_from_calendar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _SelectionSpyExitRepository()
    service = _service(monkeypatch, repository)
    leg = ActiveModelLeg(
        model_leg_id="leg",
        model_batch_id="batch",
        decision_id="decision",
        signal_date=date(2026, 8, 27),
        code="000001",
        stock_name="测试股",
        rank=1,
        relative_weight=0.05,
        d1=date(2026, 8, 28),
        d2=date(2026, 8, 31),
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="a" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )
    calendar = (
        date(2026, 8, 27),
        date(2026, 8, 28),
        date(2026, 8, 31),
        date(2026, 9, 1),
    )

    await service._evaluate_one_exit(
        leg,
        datetime(2026, 8, 31, 10, 0, 30, tzinfo=TZ),
        calendar=calendar,
    )

    assert len(repository.selection_calls) == 1
    call = repository.selection_calls[0]
    assert call["model_leg_id"] == "leg"
    assert call["d1"] == date(2026, 8, 28)
    assert call["cutoff"] == datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    assert call["late_source_trade_date"] == date(2026, 8, 28)
    assert call["late_availability_date"] == date(2026, 8, 31)


class _SparseExitRepository(_ExitRepository):
    async def list_minute_bars(self, code, *, trade_dates, end_cutoff):
        return [
            SimpleNamespace(
                payload={
                    "stock_code": code,
                    "bar_end": "2026-08-31T09:32:00+08:00",
                    "open": 10.0,
                    "high": 10.0,
                    "low": 8.0,
                    "close": 8.0,
                    "volume": 100.0,
                    "amount": 1_000.0,
                    "source_confirms_complete": True,
                }
            )
        ]


async def test_sparse_history_does_not_suppress_later_valid_d2_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _SparseExitRepository()
    service = _service(monkeypatch, repository)
    leg = ActiveModelLeg(
        model_leg_id="leg-sparse",
        model_batch_id="batch-sparse",
        decision_id="decision-sparse",
        signal_date=date(2026, 8, 27),
        code="000001",
        stock_name="测试股",
        rank=1,
        relative_weight=1.0,
        d1=date(2026, 8, 28),
        d2=date(2026, 8, 31),
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="a" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )

    await service._evaluate_one_exit(
        leg,
        datetime(2026, 8, 31, 9, 32, 30, tzinfo=TZ),
    )

    assert repository.commit is not None
    assert repository.commit.signal_type == "D2_ENTRY_12"
    assert "D1_WINDOW_INCOMPLETE" in repository.commit.semantic["reason_codes"]


class _CorruptSiblingExitRepository(_ExitRepository):
    async def list_minute_bars(self, code, *, trade_dates, end_cutoff):
        bar_end = datetime(2026, 8, 31, 9, 32, tzinfo=TZ)
        payload = {
            "stock_code": code,
            "bar_end": bar_end.isoformat(),
            "open": 10.0,
            "high": 10.0,
            "low": 8.8,
            "close": 8.8,
            "volume": 100.0,
            "amount": 1_000.0,
            "source_confirms_complete": True,
        }
        legal = SimpleNamespace(
            code=code,
            bar_end=bar_end,
            end_label="09:32",
            source_hash=sha256_json(payload),
            payload=payload,
            first_received_at=bar_end + timedelta(seconds=1),
        )
        raise V20MinuteBarIntegrityConflict(
            "one corrupt sibling",
            partial_records=[legal],
            corrupt_labels=[(code, bar_end.date(), "09:32")],
        )


async def test_corrupt_minute_sibling_cannot_erase_legal_stop_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _CorruptSiblingExitRepository()
    service = _service(monkeypatch, repository)
    leg = ActiveModelLeg(
        model_leg_id="leg-corrupt-sibling",
        model_batch_id="batch-corrupt-sibling",
        decision_id="decision-corrupt-sibling",
        signal_date=date(2026, 8, 27),
        code="000001",
        stock_name="测试股",
        rank=1,
        relative_weight=1.0,
        d1=date(2026, 8, 28),
        d2=date(2026, 8, 31),
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="a" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )

    async def no_alert(**_kwargs):
        return None

    monkeypatch.setattr(service, "_safe_alert", no_alert)
    await service._evaluate_one_exit(
        leg,
        datetime(2026, 8, 31, 9, 32, 30, tzinfo=TZ),
    )

    assert repository.commit is not None
    assert repository.commit.signal_type == "D2_ENTRY_12"


def _closed_history_leg() -> ActiveModelLeg:
    return ActiveModelLeg(
        model_leg_id="leg-history",
        model_batch_id="batch-history",
        decision_id="decision-history",
        signal_date=date(2026, 8, 28),
        code="000001",
        stock_name="测试股",
        rank=1,
        relative_weight=1.0,
        d1=date(2026, 8, 31),
        d2=date(2026, 9, 1),
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="a" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )


class _LiveExitDataRepository:
    def __init__(self, legs: list[ActiveModelLeg]) -> None:
        self.legs = legs

    async def list_active_legs(self, _trade_date, **_kwargs):
        return list(self.legs)

    async def record_minute_bars(self, rows):
        return frozenset(sha256_json(row) for row in rows)

    async def record_exit_scan_watermark(self, *_args, **_kwargs):
        return True


class _LiveExitDataClient:
    def __init__(
        self,
        *,
        latest: dict[str, TushareMinuteBar] | BaseException,
        history: dict[str, tuple[TushareMinuteBar, ...]] | BaseException,
    ) -> None:
        self.latest = latest
        self.history = history
        self.latest_calls = 0
        self.history_calls = 0

    async def batch_get_latest_minute_bars(self, _codes):
        self.latest_calls += 1
        if isinstance(self.latest, BaseException):
            raise self.latest
        return dict(self.latest)

    async def batch_get_minute_history(self, _codes):
        self.history_calls += 1
        if isinstance(self.history, BaseException):
            raise self.history
        return dict(self.history)


def _second_live_leg(first: ActiveModelLeg) -> ActiveModelLeg:
    return replace(
        first,
        model_leg_id="leg-history-2",
        model_batch_id="batch-history-2",
        decision_id="decision-history-2",
        code="000002",
        stock_name="测试股2",
    )


async def _run_live_data_probe(
    monkeypatch: pytest.MonkeyPatch,
    legs: list[ActiveModelLeg],
    client: _LiveExitDataClient,
    *,
    now: datetime | None = None,
    preexisting_outage: bool = False,
    alert_payloads: list[dict[str, Any]] | None = None,
) -> tuple[V20Service, _DayContext, list[str], list[tuple[str, ...]], bool]:
    repository = _LiveExitDataRepository(legs)
    service = _service(monkeypatch, repository, client)
    now = now or datetime(2026, 9, 1, 10, 0, 15, tzinfo=TZ)
    context = _DayContext(trade_date=now.date(), calendar=(now.date(),))
    context.live_exit_market_data_outage = preexisting_outage
    alerts: list[str] = []
    evaluations: list[tuple[str, ...]] = []

    async def capture_alert(**kwargs):
        alerts.append(str(kwargs["code"]))
        if alert_payloads is not None:
            alert_payloads.append(dict(kwargs))

    async def evaluate(active, _now, _calendar=(), **_kwargs):
        evaluations.append(tuple(item.code for item in active))

    async def no_recovery(*_args, **_kwargs):
        return None

    monkeypatch.setattr(service, "_safe_alert", capture_alert)
    monkeypatch.setattr(service, "_evaluate_active_exits", evaluate)
    monkeypatch.setattr(service, "_recover_closed_exit_windows", no_recovery)

    succeeded = await service._run_phase_isolated(
        context,
        now,
        "LIVE_EXIT_CYCLE_FAILED",
        service._run_exit_cycle(context, now, include_stale=False),
        lane_name="live_exit",
    )
    return service, context, alerts, evaluations, succeeded


async def test_all_live_targets_without_latest_or_history_evidence_marks_lane_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)

    service, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        _LiveExitDataClient(latest={}, history={}),
    )

    assert succeeded is False
    assert context.live_exit_market_data_outage is True
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" in alerts
    assert service._lane_health["live_exit"].last_error is not None
    assert "all live exit targets" in service._lane_health["live_exit"].last_error


async def test_partial_cold_history_keeps_sibling_evaluation_without_global_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    history = {first.code: (_bar(first.code, "10:00", trade_date=date(2026, 9, 1)),)}

    service, context, alerts, evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        _LiveExitDataClient(latest={}, history=history),
    )

    assert succeeded is True
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts
    assert "LIVE_EXIT_SYMBOL_DATA_GAP" in alerts
    assert any(set(codes) == {first.code, second.code} for codes in evaluations)
    assert service._lane_health["live_exit"].last_error is None


async def test_symbol_data_gap_names_missing_001306_and_healthy_sibling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    healthy = _closed_history_leg()
    missing = replace(
        _second_live_leg(healthy),
        code="001306",
        stock_name="test-missing",
    )
    history = {healthy.code: (_bar(healthy.code, "10:00", trade_date=date(2026, 9, 1)),)}
    alert_payloads: list[dict[str, Any]] = []

    _service_value, _context, _alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [healthy, missing],
        _LiveExitDataClient(latest={}, history=history),
        alert_payloads=alert_payloads,
    )

    assert succeeded is True
    payload = next(item for item in alert_payloads if item["code"] == "LIVE_EXIT_SYMBOL_DATA_GAP")
    assert payload["message"].endswith(f"missing symbols=001306; healthy siblings={healthy.code}")
    assert payload["semantic_extras"] == {
        "missing_symbols": ["001306"],
        "healthy_siblings": [healthy.code],
    }


async def test_single_empty_symbol_is_diagnostic_not_global_feed_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _closed_history_leg()

    alert_payloads: list[dict[str, Any]] = []
    _service_value, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [leg],
        _LiveExitDataClient(latest={}, history={leg.code: ()}),
        alert_payloads=alert_payloads,
    )

    assert succeeded is True
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_SYMBOL_DATA_GAP" in alerts
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts
    payload = next(item for item in alert_payloads if item["code"] == "LIVE_EXIT_SYMBOL_DATA_GAP")
    assert payload["message"].endswith(f"missing symbols={leg.code}; healthy siblings=none")
    assert payload["semantic_extras"] == {
        "missing_symbols": [leg.code],
        "healthy_siblings": [],
    }


async def test_single_symbol_lunch_history_failure_is_global_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _closed_history_leg()

    service, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [leg],
        _LiveExitDataClient(
            latest={},
            history=RuntimeError("history transport down"),
        ),
        now=datetime(2026, 9, 1, 11, 31, 30, tzinfo=TZ),
    )

    assert succeeded is False
    assert context.live_exit_market_data_outage is True
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" in alerts
    assert service._lane_health["live_exit"].last_error is not None


async def test_stale_latest_bar_does_not_disguise_a_live_feed_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    stale_latest = {
        leg.code: _bar(leg.code, "09:59", trade_date=date(2026, 9, 1)) for leg in (first, second)
    }

    service, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        _LiveExitDataClient(latest=stale_latest, history={}),
    )

    assert succeeded is False
    assert context.live_exit_market_data_outage is True
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" in alerts
    assert service._lane_health["live_exit"].last_error is not None


async def test_stale_history_bar_does_not_disguise_a_live_feed_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    stale_history = {
        leg.code: (_bar(leg.code, "09:59", trade_date=date(2026, 9, 1)),) for leg in (first, second)
    }

    _service_value, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        _LiveExitDataClient(latest={}, history=stale_history),
    )

    assert succeeded is False
    assert context.live_exit_market_data_outage is True
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" in alerts


async def test_morning_close_publication_grace_keeps_1129_health_frontier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    client = _LiveExitDataClient(
        latest=RuntimeError("11:30 bar is still publishing"),
        history={
            leg.code: (_bar(leg.code, "11:29", trade_date=date(2026, 9, 1)),)
            for leg in (first, second)
        },
    )

    service, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        client,
        now=datetime(2026, 9, 1, 11, 30, 15, tzinfo=TZ),
    )

    assert succeeded is True
    assert client.latest_calls == 0
    assert client.history_calls == 1
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts
    assert service._lane_health["live_exit"].last_error is None


async def test_morning_close_publication_grace_accepts_arrived_1130_bar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    history = {
        leg.code: (_bar(leg.code, "11:30", trade_date=date(2026, 9, 1)),) for leg in (first, second)
    }

    service, context, alerts, evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        _LiveExitDataClient(latest={}, history=history),
        now=datetime(2026, 9, 1, 11, 30, 15, tzinfo=TZ),
    )

    assert succeeded is True
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts
    assert any(set(codes) == {first.code, second.code} for codes in evaluations)
    assert service._lane_health["live_exit"].last_error is None


async def test_lunch_history_originates_outage_after_1130_publication_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    client = _LiveExitDataClient(
        latest=RuntimeError("latest must not run during lunch"),
        history={
            leg.code: (_bar(leg.code, "11:29", trade_date=date(2026, 9, 1)),)
            for leg in (first, second)
        },
    )

    service, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        client,
        now=datetime(2026, 9, 1, 11, 31, tzinfo=TZ),
    )

    assert succeeded is False
    assert client.latest_calls == 0
    assert client.history_calls == 1
    assert context.live_exit_market_data_outage is True
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" in alerts
    assert service._lane_health["live_exit"].last_error is not None
    assert (
        "all live exit targets lack persisted legal current-day market evidence"
        in service._lane_health["live_exit"].last_error
    )


async def test_lunch_uses_1130_as_the_feed_freshness_frontier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    client = _LiveExitDataClient(
        latest=RuntimeError("latest must not run during lunch"),
        history={
            leg.code: (_bar(leg.code, "11:30", trade_date=date(2026, 9, 1)),)
            for leg in (first, second)
        },
    )

    service, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        client,
        now=datetime(2026, 9, 1, 12, 0, 15, tzinfo=TZ),
        preexisting_outage=True,
    )

    assert succeeded is True
    assert client.latest_calls == 0
    assert client.history_calls == 1
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts
    assert service._lane_health["live_exit"].last_error is None


async def test_lunch_stale_history_cannot_clear_an_existing_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    client = _LiveExitDataClient(
        latest=RuntimeError("latest must not run during lunch"),
        history={
            leg.code: (_bar(leg.code, "11:29", trade_date=date(2026, 9, 1)),)
            for leg in (first, second)
        },
    )

    _service_value, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        client,
        now=datetime(2026, 9, 1, 12, 0, 15, tzinfo=TZ),
        preexisting_outage=True,
    )

    assert succeeded is False
    assert client.latest_calls == 0
    assert client.history_calls == 1
    assert context.live_exit_market_data_outage is True
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" in alerts


async def test_preopen_requires_no_live_minute_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    client = _LiveExitDataClient(
        latest=RuntimeError("latest must not run before the open"),
        history=RuntimeError("history must not run before the open"),
    )

    service, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        client,
        now=datetime(2026, 9, 1, 9, 20, tzinfo=TZ),
    )

    assert succeeded is True
    assert client.latest_calls == 0
    assert client.history_calls == 0
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts
    assert service._lane_health["live_exit"].last_error is None


async def test_exit_leg_evaluation_failure_marks_live_lane_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    leg = _closed_history_leg()
    now = datetime(2026, 9, 1, 10, 0, tzinfo=TZ)

    async def fail_one(*_args, **_kwargs):
        raise RuntimeError("persisted exit evidence unavailable")

    async def no_alert(**_kwargs):
        return None

    monkeypatch.setattr(service, "_evaluate_one_exit", fail_one)
    monkeypatch.setattr(service, "_safe_alert", no_alert)
    context = _DayContext(trade_date=now.date(), calendar=(now.date(),))

    succeeded = await service._run_phase_isolated(
        context,
        now,
        "LIVE_EXIT_CYCLE_FAILED",
        service._evaluate_active_exits([leg], now, context.calendar),
        lane_name="live_exit",
    )

    assert succeeded is False
    assert service._lane_health["live_exit"].last_error is not None
    assert "could not be evaluated" in service._lane_health["live_exit"].last_error


class _ClosedHistoryClient:
    def __init__(self, rows_by_date: dict[date, list[TushareMinuteBar]]) -> None:
        self.rows_by_date = rows_by_date
        self.calls: list[tuple[tuple[str, ...], date]] = []

    async def batch_get_minute_history_for_date(self, codes, trade_date):
        self.calls.append((tuple(codes), trade_date))
        return {codes[0]: list(self.rows_by_date.get(trade_date, []))}


class _ClosedHistoryRepository:
    def __init__(self) -> None:
        self.recorded: list[dict[str, Any]] = []
        self.watermarks: list[dict[str, Any]] = []

    async def record_minute_bars(self, rows):
        self.recorded.extend(rows)
        return frozenset(sha256_json(row) for row in rows)

    async def record_exit_scan_watermark(self, model_leg_id, **kwargs):
        self.watermarks.append({"model_leg_id": model_leg_id, **kwargs})
        return True


@pytest.mark.parametrize("partial", [False, True])
async def test_empty_or_partial_closed_exit_history_does_not_advance_watermark(
    monkeypatch: pytest.MonkeyPatch,
    partial: bool,
) -> None:
    leg = _closed_history_leg()
    rows_by_date = {
        leg.d1: ([_bar(leg.code, "09:31", trade_date=leg.d1)] if partial else []),
        leg.d2: ([_bar(leg.code, "09:31", trade_date=leg.d2)] if partial else []),
    }
    repository = _ClosedHistoryRepository()
    service = _service(monkeypatch, repository, _ClosedHistoryClient(rows_by_date))
    context = _DayContext(
        trade_date=date(2026, 9, 2),
        calendar=(leg.d1, leg.d2, date(2026, 9, 2)),
    )

    await service._recover_closed_exit_windows(
        context,
        [leg],
        datetime(2026, 9, 2, 9, 15, tzinfo=TZ),
    )

    assert repository.watermarks == []
    assert context.exit_history_completed == set()


async def test_d3_recovery_advances_both_complete_d1_and_d2_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _closed_history_leg()
    rows_by_date = {
        leg.d1: [_bar(leg.code, label, trade_date=leg.d1) for label in FULL_EXIT_LABELS],
        leg.d2: [_bar(leg.code, label, trade_date=leg.d2) for label in FULL_EXIT_LABELS],
    }
    repository = _ClosedHistoryRepository()
    client = _ClosedHistoryClient(rows_by_date)
    service = _service(monkeypatch, repository, client)
    context = _DayContext(
        trade_date=date(2026, 9, 2),
        calendar=(leg.d1, leg.d2, date(2026, 9, 2)),
    )

    await service._recover_closed_exit_windows(
        context,
        [leg],
        datetime(2026, 9, 2, 9, 15, tzinfo=TZ),
    )

    observed_watermarks = {
        (item["trade_date"], item["scanned_through_label"]) for item in repository.watermarks
    }
    assert observed_watermarks == {
        (leg.d1, "14:57"),
        (leg.d2, "14:56"),
    }
    assert context.exit_history_completed == {(leg.code, leg.d1), (leg.code, leg.d2)}
    assert {trade_date for _codes, trade_date in client.calls} == {leg.d1, leg.d2}


class _ConflictingExitRepository(_ExitRepository):
    async def list_minute_bars(self, code, *, trade_dates, end_cutoff):
        raise V20SemanticConflict("conflicting minute revisions")

    async def list_raw_minute_bar_records(self, codes, *, trade_date, end_labels):
        if trade_date != date(2026, 8, 28):
            return []
        first = {
            "stock_code": "000001",
            "bar_end": "2026-08-28T09:31:00+08:00",
            "end_label": "09:31",
            "open": 10.0,
            "high": 10.0,
            "low": 9.0,
            "close": 9.0,
            "volume": 100.0,
            "amount": 1_000.0,
            "source_confirms_complete": True,
        }
        second = dict(first, close=8.0, low=8.0)
        bar_end = datetime(2026, 8, 28, 9, 31, tzinfo=TZ)
        return [
            SimpleNamespace(code="000001", bar_end=bar_end, end_label="09:31", payload=first),
            SimpleNamespace(code="000001", bar_end=bar_end, end_label="09:31", payload=second),
        ]


async def test_conflicting_minute_revision_cannot_suppress_d2_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _ConflictingExitRepository()
    service = _service(monkeypatch, repository)
    leg = ActiveModelLeg(
        model_leg_id="leg",
        model_batch_id="batch",
        decision_id="decision",
        signal_date=date(2026, 8, 27),
        code="000001",
        stock_name="测试股",
        rank=1,
        relative_weight=1.0,
        d1=date(2026, 8, 28),
        d2=date(2026, 8, 31),
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="a" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )

    await service._evaluate_one_exit(
        leg,
        datetime(2026, 8, 31, 14, 57, tzinfo=TZ),
    )

    assert repository.commit is not None
    assert repository.commit.signal_type == "PLAN_1457"
    assert "D1_WINDOW_INCOMPLETE" in repository.commit.semantic["reason_codes"]


class _BrokenAuxiliaryExitRepository(_ExitRepository):
    async def select_mews_for_leg(self, *args, **kwargs):
        raise V20SemanticConflict("broken mews selection")

    async def list_minute_bars(self, code, *, trade_dates, end_cutoff):
        raise RuntimeError("minute store unavailable")

    async def get_exit_scan_watermarks(self, model_leg_id, **kwargs):
        raise RuntimeError("watermark store unavailable")


async def test_d2_plan_survives_all_auxiliary_input_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _BrokenAuxiliaryExitRepository()
    service = _service(monkeypatch, repository)
    leg = ActiveModelLeg(
        model_leg_id="leg",
        model_batch_id="batch",
        decision_id="decision",
        signal_date=date(2026, 8, 27),
        code="000001",
        stock_name="测试股",
        rank=1,
        relative_weight=1.0,
        d1=date(2026, 8, 28),
        d2=date(2026, 8, 31),
        reference_status="LOCKED",
        reference_price=10.0,
        reference_snapshot_hash="a" * 64,
        evaluation_only=False,
        mews_snapshot_id=None,
        mews_fast_state=None,
        exit_intent_id=None,
    )

    await service._evaluate_one_exit(
        leg,
        datetime(2026, 8, 31, 14, 57, tzinfo=TZ),
    )

    assert repository.commit is not None
    assert repository.commit.signal_type == "PLAN_1457"
    reasons = repository.commit.semantic["reason_codes"]
    assert "MEWS_INPUT_UNAVAILABLE" in reasons
    assert "EXIT_BAR_INPUT_UNAVAILABLE" in reasons
    assert "EXIT_WATERMARK_UNAVAILABLE" in reasons


async def test_run_once_reconciliation_failure_does_not_starve_exit_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    service._repository_started = True
    now = datetime(2026, 8, 31, 14, 57, tzinfo=TZ)
    context = _DayContext(
        trade_date=now.date(),
        calendar=(
            date(2026, 8, 28),
            now.date(),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
    )
    exit_calls: list[datetime] = []
    exit_started = asyncio.Event()

    async def no_op(*args, **kwargs):
        return None

    async def calendar_provider():
        return list(context.calendar)

    async def ensure_context(current, calendar):
        return context

    async def reconcile(current, calendar):
        await asyncio.wait_for(exit_started.wait(), timeout=1.0)
        raise RuntimeError("state predecessor unavailable")

    async def exit_cycle(current_context, current, **_kwargs):
        exit_calls.append(current)
        exit_started.set()

    async def entry_must_not_run(*args, **kwargs):
        raise AssertionError("entry must remain blocked by failed reconciliation")

    monkeypatch.setattr(service, "_seal_pending_outbox", no_op)
    service._repository.assert_runtime_leader = no_op
    monkeypatch.setattr(service, "_calendar_provider", calendar_provider)
    monkeypatch.setattr(service, "_ensure_context", ensure_context)

    async def bootstrap_not_covering(*_args, **_kwargs):
        return False

    monkeypatch.setattr(service, "_bootstrap_anchor_covers", bootstrap_not_covering)
    monkeypatch.setattr(service, "_run_entry_collection_cycle", no_op)
    monkeypatch.setattr(service, "_reconcile_missed_slots", reconcile)
    monkeypatch.setattr(service, "_expire_reference_gaps", no_op)
    monkeypatch.setattr(service, "_run_exit_cycle", exit_cycle)
    monkeypatch.setattr(service, "_run_stale_exit_cycle", no_op)
    monkeypatch.setattr(service, "_run_entry_cycle", entry_must_not_run)
    monkeypatch.setattr(service, "_run_reference_cycle", no_op)
    monkeypatch.setattr(service, "_run_reminders", no_op)
    monkeypatch.setattr(service, "_safe_alert", no_op)

    await service.run_once(now)

    assert exit_calls == [now]
    assert context.last_phase == "ENTRY_BLOCKED_BY_STATE_RECONCILIATION"


async def test_entry_collection_runs_while_health_maturity_is_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    service._repository_started = True
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ),
    )
    collection_times: list[datetime] = []
    entry_times: list[datetime] = []
    maturity_calls = 0

    async def no_op(*_args, **_kwargs):
        return None

    async def calendar_provider():
        return list(context.calendar)

    async def ensure_context(_current, _calendar):
        return context

    async def bootstrap_not_covering(*_args, **_kwargs):
        return False

    async def collect(_context, current):
        collection_times.append(current)
        if _context.prewarmed is None:
            _context.prewarmed = SimpleNamespace(accumulated_from="09:31")
            _context.collector = SimpleNamespace()

    async def mature(_context, _current):
        nonlocal maturity_calls
        maturity_calls += 1
        _context.maturity_done = maturity_calls >= 2

    async def entry(_context, current):
        assert _context.prewarmed.accumulated_from == "09:31"
        assert _context.collector is not None
        entry_times.append(current)

    service._repository.assert_runtime_leader = no_op
    monkeypatch.setattr(service, "_seal_pending_outbox", no_op)
    monkeypatch.setattr(service, "_calendar_provider", calendar_provider)
    monkeypatch.setattr(service, "_ensure_context", ensure_context)
    monkeypatch.setattr(service, "_bootstrap_anchor_covers", bootstrap_not_covering)
    monkeypatch.setattr(service, "_run_exit_cycle", no_op)
    monkeypatch.setattr(service, "_run_entry_collection_cycle", collect)
    monkeypatch.setattr(service, "_reconcile_missed_slots", no_op)
    monkeypatch.setattr(service, "_expire_reference_gaps", no_op)
    monkeypatch.setattr(service, "_process_mature_shadow", mature)
    monkeypatch.setattr(service, "_run_entry_cycle", entry)
    monkeypatch.setattr(service, "_run_reference_cycle", no_op)
    monkeypatch.setattr(service, "_run_reminders", no_op)
    monkeypatch.setattr(service, "_safe_alert", no_op)

    await service.run_once(datetime(2026, 8, 31, 9, 31, 10, tzinfo=TZ))
    assert collection_times == [datetime(2026, 8, 31, 9, 31, 10, tzinfo=TZ)]
    assert entry_times == []

    await service.run_once(datetime(2026, 8, 31, 9, 39, 10, tzinfo=TZ))
    assert collection_times[-1] == datetime(2026, 8, 31, 9, 39, 10, tzinfo=TZ)
    assert entry_times == [datetime(2026, 8, 31, 9, 39, 10, tzinfo=TZ)]


async def test_exit_evaluates_actionable_state_before_closed_history_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _closed_history_leg()

    class _Repository:
        async def list_active_legs(self, trade_date, **kwargs):
            return [leg]

    service = _service(monkeypatch, _Repository())
    evaluated = asyncio.Event()
    release_evaluation = asyncio.Event()
    recovery_started = asyncio.Event()
    release_recovery = asyncio.Event()

    async def evaluate(_active, _now, _calendar=(), **_kwargs):
        evaluated.set()
        await release_evaluation.wait()

    async def recover(_context, _active, _now):
        recovery_started.set()
        await release_recovery.wait()

    monkeypatch.setattr(service, "_evaluate_active_exits", evaluate)
    monkeypatch.setattr(service, "_recover_closed_exit_windows", recover)
    context = _DayContext(
        trade_date=date(2026, 9, 2),
        calendar=(leg.d1, leg.d2, date(2026, 9, 2)),
    )

    task = asyncio.create_task(
        service._run_exit_cycle(
            context,
            datetime(2026, 9, 2, 9, 15, tzinfo=TZ),
        )
    )
    await asyncio.wait_for(evaluated.wait(), timeout=0.2)
    assert recovery_started.is_set() is False
    release_evaluation.set()
    await asyncio.wait_for(recovery_started.wait(), timeout=0.2)
    release_recovery.set()
    await task


async def test_cold_start_calendar_failure_does_not_suppress_exit_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = SimpleNamespace()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    exited = asyncio.Event()

    async def no_op(*_args, **_kwargs):
        return None

    async def calendar_failure(_date):
        await asyncio.sleep(0)
        raise RuntimeError("trade calendar unavailable")

    async def exit_cycle(_context, _now, **_kwargs):
        exited.set()

    repository.assert_runtime_leader = no_op
    monkeypatch.setattr(service, "_seal_pending_outbox", no_op)
    monkeypatch.setattr(service, "_load_trade_calendar", calendar_failure)
    monkeypatch.setattr(service, "_run_exit_cycle", exit_cycle)
    monkeypatch.setattr(service, "_run_stale_exit_cycle", no_op)
    monkeypatch.setattr(service, "_safe_alert", no_op)

    with pytest.raises(RuntimeError, match="trade calendar unavailable"):
        await service.run_once(datetime(2026, 8, 31, 14, 57, tzinfo=TZ))

    assert exited.is_set() is True


class _CalendarClient:
    def __init__(self) -> None:
        self.calls: list[tuple[date, date]] = []

    async def fetch_trade_calendar(self, start_date, end_date):
        self.calls.append((start_date, end_date))
        return (
            date(2026, 8, 28),
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
        )


async def test_v20_calendar_is_tushare_backed_bounded_and_cached_per_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _CalendarClient()
    service = _service(monkeypatch, SimpleNamespace(), client)

    first = await service._load_trade_calendar(date(2026, 8, 31))
    second = await service._load_trade_calendar(date(2026, 8, 31))

    assert first == second
    assert len(client.calls) == 1
    start_date, end_date = client.calls[0]
    assert start_date < date(2026, 8, 31) < end_date


async def test_v20_calendar_rejects_exhausted_or_unsorted_horizon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())

    async def exhausted():
        return [date(2026, 8, 28), date(2026, 8, 31), date(2026, 9, 1)]

    service._calendar_provider = exhausted
    with pytest.raises(V20RepositoryError, match="fewer than two future"):
        await service._load_trade_calendar(date(2026, 8, 31))

    async def unsorted():
        return [
            date(2026, 8, 31),
            date(2026, 8, 28),
            date(2026, 9, 1),
            date(2026, 9, 2),
        ]

    service._calendar_provider = unsorted
    with pytest.raises(V20RepositoryError, match="unsorted"):
        await service._load_trade_calendar(date(2026, 8, 31))


class _StatusRepository:
    def __init__(self) -> None:
        self.health_kwargs: dict[str, Any] | None = None
        self.load_calls = 0
        self.health_calls = 0

    async def load_state(self, lineage_id):
        self.load_calls += 1
        return StateRecord(lineage_id, 7, "a" * 64, {})

    async def get_outbox_health(self, **kwargs):
        self.health_calls += 1
        self.health_kwargs = kwargs
        return {"unsealed_n": 0, "pending_delivery_n": 0, "leased_n": 0}


async def test_status_exposes_ledger_and_outbox_but_no_execution_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _StatusRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    await service._refresh_status_snapshot()

    status = await service.status()

    assert status["ledger"]["revision"] == 7
    assert status["outbox"]["unsealed_n"] == 0
    assert status["config_hash"] == service.config.config_hash
    assert status["route_id"] == service.config.route_id
    assert status["official_stream_id"] == service.config.official_stream_id
    assert status["state_lineage_id"] == service.config.state_lineage_id
    assert status["order_execution_scope"] == "OUT_OF_SCOPE"
    assert not any(key in status for key in ("account", "position", "order", "fill"))
    assert repository.health_kwargs == {
        "route_id": service.config.route_id,
        "official_stream_id": service.config.official_stream_id,
        "lineage_id": service.config.state_lineage_id,
    }
    assert repository.load_calls == 1
    assert repository.health_calls == 1


async def test_concurrent_status_requests_never_touch_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _StatusRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    await service._refresh_status_snapshot()

    statuses = await asyncio.gather(*(service.status() for _ in range(1000)))

    assert len(statuses) == 1000
    assert repository.load_calls == 1
    assert repository.health_calls == 1


async def test_publisher_lane_is_red_when_unknown_exists_even_without_last_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class UnknownOutboxRepository(_StatusRepository):
        async def get_outbox_health(self, **kwargs):
            self.health_kwargs = kwargs
            self.health_calls += 1
            return {
                "unsealed_n": 0,
                "pending_delivery_n": 0,
                "leased_n": 0,
                "dispatching_n": 0,
                "stale_started_n": 0,
                "terminal_unknown_n": 1,
                "unknown_n": 1,
                "delivery_error_n": 0,
            }

    repository = UnknownOutboxRepository()
    service = _service(monkeypatch, repository)
    service._repository_started = True
    now = service._aware_now()
    for lane_name in service._lane_health:
        service._record_lane_success(lane_name, now)
    await service._refresh_status_snapshot()

    status = await service.status()

    assert status["healthy"] is False
    assert status["runtime_lanes"]["publisher"]["healthy"] is False
    assert status["runtime_lanes"]["publisher"]["last_error"] is None
    assert status["runtime_lanes"]["publisher"]["unknown_delivery_outcomes"] == 1


async def test_missing_or_failed_status_snapshot_is_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(monkeypatch, SimpleNamespace())
    now = service._aware_now()
    for lane_name in service._lane_health:
        service._record_lane_success(lane_name, now)
    blocker = asyncio.create_task(asyncio.Event().wait())
    service._tasks = [blocker]
    try:
        missing = await service.status()
        service._status_snapshot_error = "RuntimeError: database unavailable"
        failed = await service.status()
    finally:
        blocker.cancel()
        await asyncio.gather(blocker, return_exceptions=True)

    assert missing["healthy"] is False
    assert missing["status_snapshot"]["stale"] is True
    assert failed["healthy"] is False
    assert failed["status_snapshot"]["last_error"] == ("RuntimeError: database unavailable")


class _ManualMonitorRepository:
    def __init__(self, service: V20Service, *, source_config_hash: str | None = None) -> None:
        self.service = service
        self.source_event_id = "d" * 64
        self.source_config_hash = source_config_hash or service.config.config_hash
        self.official_config_hash = service.config.config_hash
        self.symbols = [
            {"rank": 1, "code": "605189", "name": "富春染织", "snapshot_price": 15.32},
            {"rank": 2, "code": "002860", "name": "星帅尔", "snapshot_price": 16.77},
        ]
        self.official_event_id = "official-failed-entry"
        entry_render = {
            "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": "retrospective-entry-render",
            "decision_id": "retrospective-decision",
            "strategy_version": service.config.strategy_version,
            "config_hash": self.source_config_hash,
            "state_semantics_hash": service.config.state_semantics_hash,
            "trade_date": "2026-08-31",
            "action": "ENTER",
            "final_multiplier": 1.0,
            "reference_profile_id": service.config.reference_profile_id,
            "symbols": self.symbols,
        }
        source_semantic = {
            "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": self.source_event_id,
            "strategy_version": service.config.strategy_version,
            "config_hash": self.source_config_hash,
            "state_semantics_hash": service.config.state_semantics_hash,
            "deployment_mode": service.config.deployment_mode,
            "official_stream_id": service.config.official_stream_id,
            "state_lineage_id": service.config.state_lineage_id,
            "alert_code": "MANUAL_0939_CHAIN_PROBE_RESULT",
            "probe_profile": "CURRENT_DEPLOYED_CODE_EXACT_0939_ENTRY_RENDER_V2",
            "probe_result": "PASS",
            "current_version_recomputed": True,
            "replay_reused": False,
            "replay_action": "ENTER",
            "v20_action": "ENTER",
            "final_multiplier": 1.0,
            "official_entry_action": "INPUT_INVALID",
            "official_entry_event_id": self.official_event_id,
            "official_entry_event_id_before": self.official_event_id,
            "official_entry_event_id_after": self.official_event_id,
            "official_state_changed": False,
            "orders_changed": False,
            "non_actionable": True,
            "retrospective_expired": True,
            "visible_message_mode": "MANUAL_OPERATOR_RENDER",
            "event_trade_date": "2026-08-31",
            "symbols": self.symbols,
            "entry_render_semantic": entry_render,
        }
        source_payload = {"message": "sealed retrospective morning result"}
        self.source = OutboxRecord(
            event_id=self.source_event_id,
            event_type="DATA_ALERT",
            route_id=service.config.route_id,
            official_stream_id=service.config.official_stream_id,
            lineage_id=service.config.state_lineage_id,
            semantic=source_semantic,
            semantic_content_hash=sha256_json(source_semantic),
            payload=source_payload,
            payload_hash=sha256_json(source_payload),
            generated_at=datetime(2026, 8, 31, 10, 1, tzinfo=TZ),
            commit_marker=10,
            action_expiry_ts=None,
            delivery_status="SENT",
            attempt_count=1,
        )
        failed_semantic = {
            "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "strategy_version": service.config.strategy_version,
            "config_hash": self.official_config_hash,
            "official_stream_id": service.config.official_stream_id,
            "state_lineage_id": service.config.state_lineage_id,
            "state_semantics_hash": service.config.state_semantics_hash,
            "action": "INPUT_INVALID",
            "state_after_hash": "e" * 64,
        }
        failed_snapshot = {
            "schema_version": V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
            "state_semantics_hash": service.config.state_semantics_hash,
        }
        self.official = EntryStatus(
            official_stream_id=service.config.official_stream_id,
            trade_date=date(2026, 8, 31),
            slot_id="failed-slot",
            slot_status="FAILED",
            slot_revision=1,
            strategy_version=service.config.strategy_version,
            config_id=self.official_config_hash[:24],
            config_hash=self.official_config_hash,
            lineage_id=service.config.state_lineage_id,
            decision_id="failed-decision",
            event_id=self.official_event_id,
            action="INPUT_INVALID",
            final_multiplier=0.0,
            semantic_content_hash=sha256_json(failed_semantic),
            semantic=failed_semantic,
            snapshot_id="failed-snapshot",
            snapshot_hash=sha256_json(failed_snapshot),
            snapshot=failed_snapshot,
            action_expiry_ts=datetime(2026, 8, 31, 9, 40, tzinfo=TZ),
        )
        self.records = [
            self._reference_record(item, 20.0 + index) for index, item in enumerate(self.symbols)
        ]
        self.enrollment_commit: Any | None = None
        self.enrollment: ManualMonitorEnrollmentRecord | None = None
        self.events: dict[str, OutboxRecord] = {self.source_event_id: self.source}
        self.alert_semantics: list[Mapping[str, Any]] = []
        self.manual_legs_exited = False
        self.fail_confirmation_seal_once = False
        self.active_leg_reads = 0
        self.batch_leg_reads = 0
        self.reference_reads: list[datetime | None] = []
        self.persisted_minute_payloads: list[Mapping[str, Any]] = []

    @staticmethod
    def _reference_record(item: Mapping[str, Any], open_price: float) -> MinuteBarRecord:
        bar = TushareMinuteBar(
            stock_code=str(item["code"]),
            bar_end=datetime(2026, 8, 31, 9, 41, tzinfo=TZ),
            end_label="09:41",
            open_price=open_price,
            high_price=open_price + 0.2,
            low_price=open_price - 0.2,
            close_price=open_price + 0.1,
            volume=100_000.0,
            amount=2_000_000.0,
        )
        payload = _bar_payload(bar)
        return MinuteBarRecord(
            code=bar.stock_code,
            bar_end=bar.bar_end,
            end_label=bar.end_label,
            source_hash=sha256_json(payload),
            payload=payload,
            first_received_at=datetime(2026, 8, 31, 15, 1, tzinfo=TZ),
        )

    async def assert_runtime_leader(self) -> None:
        return None

    async def get_outbox_event(self, event_id: str, **_kwargs: Any) -> OutboxRecord | None:
        return self.events.get(event_id)

    async def get_entry_status(self, _stream: str, trade_date: date) -> EntryStatus | None:
        return self.official if trade_date == self.official.trade_date else None

    async def list_raw_minute_bar_records(
        self,
        codes: Sequence[str],
        *,
        trade_date: date,
        end_labels: Sequence[str],
        received_before: datetime | None = None,
    ) -> list[Any]:
        self.reference_reads.append(received_before)
        return [
            record
            for record in self.records
            if record.code in codes
            and record.bar_end.astimezone(TZ).date() == trade_date
            and record.end_label in end_labels
            and (received_before is None or record.first_received_at < received_before)
        ]

    async def record_minute_bars(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> frozenset[str]:
        receipt = self.service._aware_now()
        sealed: set[str] = set()
        existing_hashes = {record.source_hash for record in self.records}
        for raw in rows:
            payload = dict(raw)
            source_hash = sha256_json(payload)
            sealed.add(source_hash)
            self.persisted_minute_payloads.append(payload)
            if source_hash in existing_hashes:
                continue
            bar_end = datetime.fromisoformat(str(payload["bar_end"]))
            self.records.append(
                MinuteBarRecord(
                    code=str(payload["stock_code"]),
                    bar_end=bar_end,
                    end_label=str(payload["end_label"]),
                    source_hash=source_hash,
                    payload=payload,
                    first_received_at=receipt,
                )
            )
            existing_hashes.add(source_hash)
        return frozenset(sealed)

    async def enroll_manual_monitor(self, commit: Any) -> bool:
        self.enrollment_commit = commit
        if self.enrollment is not None:
            return False
        self.enrollment = ManualMonitorEnrollmentRecord(
            enrollment_id=commit.enrollment_id,
            source_event_id=commit.source_event_id,
            official_entry_event_id=commit.official_entry_event_id,
            model_batch_id=commit.model_batch.model_batch_id,
            request_id=commit.request_id,
            signal_date=commit.signal_date,
            d1=commit.d1,
            d2=commit.d2,
            activation_cutoff_ts=commit.activation_cutoff_ts,
            source_semantic_content_hash=commit.source_semantic_content_hash,
            source_payload_hash=commit.source_payload_hash,
            calendar_evidence_hash=commit.calendar_evidence_hash,
            semantic=commit.enrollment_semantic,
            created_at=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
        )
        return True

    async def get_manual_monitor_enrollment(self, _source: str, **_kwargs: Any) -> Any:
        return self.enrollment

    def _manual_monitor_legs(self) -> list[ActiveModelLeg]:
        if self.enrollment_commit is None:
            return []
        batch = self.enrollment_commit.model_batch
        return [
            ActiveModelLeg(
                model_leg_id=leg.model_leg_id,
                model_batch_id=batch.model_batch_id,
                decision_id=None,
                signal_date=self.enrollment_commit.signal_date,
                code=leg.code,
                stock_name=leg.stock_name,
                rank=leg.rank,
                relative_weight=leg.relative_weight,
                d1=leg.d1,
                d2=leg.d2,
                reference_status="PENDING",
                reference_price=None,
                reference_snapshot_hash=None,
                evaluation_only=False,
                mews_snapshot_id=None,
                mews_fast_state=None,
                exit_intent_id=(f"exit-{leg.model_leg_id}" if self.manual_legs_exited else None),
                origin_kind="MANUAL_MONITOR",
                source_event_id=self.source_event_id,
            )
            for leg in batch.legs
        ]

    async def list_active_legs(self, _trade_date: date, **_kwargs: Any) -> list[Any]:
        self.active_leg_reads += 1
        return [leg for leg in self._manual_monitor_legs() if leg.exit_intent_id is None]

    async def list_manual_monitor_batch_legs(
        self,
        model_batch_id: str,
        **_kwargs: Any,
    ) -> list[ActiveModelLeg]:
        self.batch_leg_reads += 1
        return [leg for leg in self._manual_monitor_legs() if leg.model_batch_id == model_batch_id]

    async def enqueue_alert(
        self,
        event_id: str,
        route_id: str,
        semantic: Mapping[str, Any],
        semantic_hash: str,
        **scope: Any,
    ) -> bool:
        self.alert_semantics.append(semantic)
        if event_id in self.events:
            return False
        self.events[event_id] = OutboxRecord(
            event_id=event_id,
            event_type="DATA_ALERT",
            route_id=route_id,
            official_stream_id=scope["official_stream_id"],
            lineage_id=scope["lineage_id"],
            semantic=semantic,
            semantic_content_hash=semantic_hash,
            payload=None,
            payload_hash=None,
            generated_at=None,
            commit_marker=None,
            action_expiry_ts=None,
            delivery_status="PENDING",
            attempt_count=0,
        )
        return True

    async def seal_event(self, event_id: str, formatter: Any) -> OutboxRecord:
        record = self.events[event_id]
        if record.payload is not None:
            return record
        if self.fail_confirmation_seal_once:
            self.fail_confirmation_seal_once = False
            raise RuntimeError("injected manual-monitor confirmation seal failure")
        generated_at = datetime(2026, 9, 1, 2, 1, tzinfo=TZ)
        payload = formatter(record, generated_at, 11, True)
        sealed = replace(
            record,
            payload=payload,
            payload_hash=sha256_json(payload),
            generated_at=generated_at,
            commit_marker=11,
        )
        self.events[event_id] = sealed
        return sealed


class _ManualMonitorHistoryClient:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], date]] = []

    async def batch_get_minute_history_for_date(
        self,
        codes: Sequence[str],
        trade_date: date,
    ) -> Mapping[str, Sequence[TushareMinuteBar]]:
        normalized = tuple(codes)
        self.calls.append((normalized, trade_date))
        return {
            code: (
                _bar(
                    code,
                    "09:41",
                    open_price=30.0 + index,
                    close=30.0 + index,
                    trade_date=trade_date,
                ),
            )
            for index, code in enumerate(normalized)
        }


async def _manual_monitor_service(
    monkeypatch: pytest.MonkeyPatch,
    *,
    now: datetime,
    source_config_hash: str | None = None,
    client: Any = None,
) -> tuple[V20Service, _ManualMonitorRepository]:
    service = _service(monkeypatch, SimpleNamespace(), client)
    repository = _ManualMonitorRepository(service, source_config_hash=source_config_hash)
    service._repository = repository
    service._clock = lambda: now

    async def ready() -> None:
        return None

    async def calendar() -> tuple[date, ...]:
        return (
            date(2026, 8, 31),
            date(2026, 9, 1),
            date(2026, 9, 2),
            date(2026, 9, 3),
        )

    service._require_manual_trigger_ready = ready  # type: ignore[method-assign]
    service._calendar_provider = calendar
    return service, repository


async def test_manual_monitor_arms_complete_0941_evidence_without_using_snapshot_price(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
    )

    result = await service.enroll_manual_monitor(
        repository.source_event_id,
        "manual-monitor-20260831",
    )

    assert result["created"] is True
    assert result["armed"] is True
    assert result["armed_leg_count"] == 2
    assert result["reference_evidence_complete"] is True
    assert result["reference_locked"] is False
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    commit = repository.enrollment_commit
    assert commit is not None
    assert commit.model_batch.evaluation_only is False
    assert commit.model_batch.multiplier == 1.0
    assert [leg.relative_weight for leg in commit.model_batch.legs] == [0.5, 0.5]
    assert commit.enrollment_semantic["reference_evidence_status"] == (
        "COMPLETE_PENDING_D1_ARBITRATION"
    )
    assert all(
        record.payload["open"] != item["snapshot_price"]
        for record, item in zip(repository.records, repository.symbols, strict=True)
    )
    confirmation = repository.events[result["confirmation_event_id"]]
    assert confirmation.payload is not None
    assert "09:41 bar.open" in confirmation.payload["message"]
    assert "未创建订单、持仓或成交" in confirmation.payload["message"]


async def test_manual_monitor_recovers_and_persists_d0_0941_before_enrollment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _ManualMonitorHistoryClient()
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 9, 20, tzinfo=TZ),
        client=client,
    )
    repository.records.clear()

    result = await service.enroll_manual_monitor(
        repository.source_event_id,
        "manual-monitor-history-recovery",
    )

    expected_codes = tuple(item["code"] for item in repository.symbols)
    cutoff = datetime(2026, 9, 1, 9, 30, tzinfo=TZ)
    assert result["created"] is True
    assert result["armed_leg_count"] == len(expected_codes)
    assert client.calls == [(expected_codes, date(2026, 8, 31))]
    assert repository.reference_reads == [cutoff, cutoff]
    assert {row["stock_code"] for row in repository.persisted_minute_payloads} == set(
        expected_codes
    )
    assert {record.code for record in repository.records} == set(expected_codes)
    assert all(record.first_received_at < cutoff for record in repository.records)
    assert repository.enrollment_commit is not None
    assert repository.enrollment_commit.enrollment_semantic["reference_evidence_status"] == (
        "COMPLETE_PENDING_D1_ARBITRATION"
    )


async def test_manual_monitor_accepts_historical_explicit_source_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_hash = "7" * 64
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
        source_config_hash=old_hash,
    )
    result = await service.enroll_manual_monitor(
        repository.source_event_id,
        "manual-monitor-old-config",
    )

    assert result["created"] is True
    assert repository.enrollment_commit.source_config_hash == old_hash


async def test_manual_monitor_rejects_tampered_historical_source_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
        source_config_hash="7" * 64,
    )
    repository.events[repository.source.event_id] = replace(
        repository.source,
        payload={"message": "tampered retrospective result"},
    )

    with pytest.raises(V20SemanticConflict, match="source payload hash differs"):
        await service.enroll_manual_monitor(
            repository.source_event_id,
            "manual-monitor-unknown-config",
        )

    assert repository.enrollment_commit is None


def _reseal_manual_monitor_source_semantic(
    repository: _ManualMonitorRepository,
    semantic: Mapping[str, Any],
) -> None:
    repository.events[repository.source_event_id] = replace(
        repository.source,
        semantic=semantic,
        semantic_content_hash=sha256_json(semantic),
    )


@pytest.mark.parametrize("invalid_config_hash", ["A" * 64, "g" * 64])
async def test_manual_monitor_rejects_malformed_historical_source_config_hash(
    monkeypatch: pytest.MonkeyPatch,
    invalid_config_hash: str,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
        source_config_hash="7" * 64,
    )
    semantic = dict(repository.source.semantic)
    semantic["config_hash"] = invalid_config_hash
    _reseal_manual_monitor_source_semantic(repository, semantic)

    with pytest.raises(V20SemanticConflict, match="source config hash is invalid"):
        await service.enroll_manual_monitor(
            repository.source_event_id,
            "manual-monitor-invalid-config-hash",
        )

    assert repository.enrollment_commit is None


@pytest.mark.parametrize(
    ("field", "value"),
    [("schema_version", "legacy"), ("feishu_formatter_profile", "other-formatter")],
)
async def test_manual_monitor_rejects_incompatible_nested_entry_render_contract(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
        source_config_hash="7" * 64,
    )
    semantic = dict(repository.source.semantic)
    semantic["entry_render_semantic"] = {
        **semantic["entry_render_semantic"],
        field: value,
    }
    _reseal_manual_monitor_source_semantic(repository, semantic)

    with pytest.raises(V20SemanticConflict, match="ticket list is inconsistent"):
        await service.enroll_manual_monitor(
            repository.source_event_id,
            "manual-monitor-invalid-entry-render",
        )

    assert repository.enrollment_commit is None


@pytest.mark.parametrize(
    "field",
    ["strategy_version", "config_hash", "state_semantics_hash"],
)
async def test_manual_monitor_rejects_nested_identity_mismatch_with_source(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
        source_config_hash="7" * 64,
    )
    semantic = dict(repository.source.semantic)
    semantic["entry_render_semantic"] = {
        **semantic["entry_render_semantic"],
        field: "9" * 64 if field != "strategy_version" else "other-strategy",
    }
    _reseal_manual_monitor_source_semantic(repository, semantic)

    with pytest.raises(V20SemanticConflict, match="ticket list is inconsistent"):
        await service.enroll_manual_monitor(
            repository.source_event_id,
            "manual-monitor-source-mismatch",
        )

    assert repository.enrollment_commit is None


async def test_manual_monitor_rejects_exact_d1_cutoff_before_any_model_leg_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 9, 30, tzinfo=TZ),
    )

    with pytest.raises(V20StateConflict, match="before D1 09:30"):
        await service.enroll_manual_monitor(
            repository.source_event_id,
            "manual-monitor-too-late",
        )

    assert repository.enrollment_commit is None
    assert repository.alert_semantics == []


async def test_manual_monitor_retry_after_cutoff_recovers_existing_enrollment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
    )
    first = await service.enroll_manual_monitor(
        repository.source_event_id,
        "manual-monitor-before-cutoff",
    )
    service._clock = lambda: datetime(2026, 9, 1, 10, 0, tzinfo=TZ)

    retry = await service.enroll_manual_monitor(
        repository.source_event_id,
        "manual-monitor-retry-after-cutoff",
    )

    assert first["created"] is True
    assert retry["created"] is False
    assert retry["armed"] is True
    assert retry["enrollment_id"] == first["enrollment_id"]
    assert retry["confirmation_event_id"] == first["confirmation_event_id"]


async def test_manual_monitor_retry_seals_confirmation_after_every_leg_has_exited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
    )
    request_id = "manual-monitor-confirmation-recovery"
    repository.fail_confirmation_seal_once = True

    with pytest.raises(RuntimeError, match="confirmation seal failure"):
        await service.enroll_manual_monitor(repository.source_event_id, request_id)

    assert repository.enrollment is not None
    confirmation_ids = [
        event_id for event_id in repository.events if event_id != repository.source_event_id
    ]
    assert len(confirmation_ids) == 1
    confirmation_event_id = confirmation_ids[0]
    assert repository.events[confirmation_event_id].payload is None

    repository.manual_legs_exited = True
    service._clock = lambda: datetime(2026, 9, 1, 10, 0, tzinfo=TZ)
    recovered = await service.enroll_manual_monitor(repository.source_event_id, request_id)

    assert recovered["created"] is False
    assert recovered["armed"] is True
    assert recovered["confirmation_event_id"] == confirmation_event_id
    assert repository.events[confirmation_event_id].payload is not None
    assert repository.active_leg_reads == 0
    assert repository.batch_leg_reads == 2
    assert repository.alert_semantics[0] == repository.alert_semantics[1]


async def test_manual_monitor_requires_all_ticket_reference_rows_before_writing_legs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
    )
    repository.records.pop()

    with pytest.raises(V20RepositoryError, match="minute-history adapter"):
        await service.enroll_manual_monitor(
            repository.source_event_id,
            "manual-monitor-missing-reference",
        )

    assert repository.enrollment_commit is None
