from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import pytest

import src.strategy.v20.runtime_config as runtime_config_module
from src.common.v20_feishu import V20FeishuRoute
from src.data.clients.mews_snapshot import MewsSnapshotSourceError
from src.data.clients.tushare_realtime import TushareMinuteBar
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
from src.web.v15_scan_service import V15ScanState
from src.web.v20_scan_pipeline import FrozenV16ScanBundle
from src.web.v20_service import (
    FULL_EXIT_LABELS,
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
        scan_pipeline=SimpleNamespace(),
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
    service._mews_source = source
    calendar = (
        date(2026, 8, 31),
        date(2026, 9, 1),
        date(2026, 9, 2),
        date(2026, 9, 3),
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
    service._mews_source = source
    calendar = (date(2026, 8, 31), date(2026, 9, 1))

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
        async def fetch_snapshot(self, **_kwargs):
            raise MewsSnapshotSourceError("Tushare margin is missing SSE or SZSE")

    service = _service(monkeypatch, SimpleNamespace())
    service._mews_source = _Source()

    with pytest.raises(MewsSnapshotSourceError, match="missing SSE or SZSE"):
        await service._refresh_mews_cache_once(
            datetime(2026, 9, 1, 9, 18, tzinfo=TZ),
            (date(2026, 8, 31), date(2026, 9, 1)),
        )


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
    assert await service.ensure_mews_for_selection_trigger(now) is False

    assert repository.leader_calls == 2
    assert source.calls == [(date(2026, 8, 31), date(2026, 9, 1))]
    assert len(repository.payloads) == 1
    assert service._mews_cached_for == date(2026, 9, 1)
    assert service._mews_snapshot_id == "mews-v2-2026-08-31-trigger"
    assert service._lane_health["mews_cache"].last_error is None


async def test_mews_cache_restart_restores_postgres_snapshot_without_refetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Repository:
        async def find_eligible_mews_snapshot(
            self,
            *,
            source_trade_date,
            cutoff,
        ):
            assert source_trade_date == date(2026, 8, 31)
            assert cutoff == datetime(2026, 9, 1, 9, 40, tzinfo=TZ)
            return "mews-v2-2026-08-31-restored"

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
    monkeypatch.setattr(service_module, "V20ScanPipeline", lambda state, root: (state, root))
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


async def test_cancelling_embedded_initializer_stops_tushare_but_preserves_shared_pool(
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
    task = asyncio.create_task(_init_embedded_v20_scan_resources(state))
    await asyncio.wait_for(start_entered.wait(), timeout=1.0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert _Realtime.instance is not None
    assert _Realtime.instance.stop_calls == 1
    assert fundamentals.connect_calls == 0
    assert fundamentals.close_calls == 0
    assert state.realtime_client is None
    assert state.initialized is False


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


def test_entry_binding_reattaches_only_proven_historical_terminal_config(
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
    binding = (
        historical.config_id,
        historical.config_hash,
        historical_state_hash,
    )

    with pytest.raises(V20ConfigError, match="unproven historical config"):
        service._verify_entry_binding(historical)

    service._compatible_entry_bindings.add(binding)
    service._verify_entry_binding(historical)

    with pytest.raises(V20ConfigError, match="unproven historical config"):
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
        scan_pipeline=SimpleNamespace(),
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
    }
    assert all(not task.done() for task in service._tasks)
    assert service.startup_stage == "RUNNING"

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
        asyncio.create_task(blocker.wait(), name=f"v20-test-lane-{index}") for index in range(6)
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


@pytest.mark.parametrize("unhealthy_source", ("lane_error", "delivery_error", "stale_snapshot"))
async def test_manual_trigger_requires_the_same_green_health_reported_by_status(
    monkeypatch: pytest.MonkeyPatch,
    unhealthy_source: str,
) -> None:
    repository = _ManualTriggerRepository()
    service = _service(monkeypatch, repository)
    tasks = _arm_manual_trigger_runtime(service)
    now = service._aware_now()
    if unhealthy_source == "lane_error":
        service._record_lane_error("publisher", "relay unavailable", now)
    elif unhealthy_source == "delivery_error":
        assert service._status_snapshot is not None
        service._status_snapshot = {
            **service._status_snapshot,
            "outbox": {"delivery_error_n": 1},
        }
    else:
        assert service._status_snapshot is not None
        service._status_snapshot = {
            **service._status_snapshot,
            "sampled_at": now - timedelta(seconds=60),
        }
    try:
        status = await service.status()
        with pytest.raises(V20RepositoryError, match="not healthy enough"):
            await service.trigger_manual_scan("deploy-20260831-unhealthy")
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
        self.calls: list[tuple[str, ...]] = []

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


class _LateReplayPipeline:
    def __init__(self, trade_date: date) -> None:
        self.trade_date = trade_date
        self.scan_calls = 0
        self.observed_early_volume: float | None = None

    async def scan(
        self,
        _prewarmed,
        early,
        *,
        breadth_early,
        minimum_quote_coverage,
    ) -> FrozenV16ScanBundle:
        self.scan_calls += 1
        assert minimum_quote_coverage > 0
        quote = early.quotes.get("000001")
        if quote is None:
            raise RuntimeError("exact 09:31..09:39 path is incomplete")
        self.observed_early_volume = quote.volume
        assert quote.volume == 900.0
        assert quote.early_close == pytest.approx(10.09)
        assert breadth_early.quotes == {}
        stock = ScoredStock(
            code="000001",
            name="平安银行",
            score=0.8,
            rank=1,
            buy_price=float(quote.early_close),
        )
        scan = V16ScanResult(
            recommended=[stock],
            final_candidates=1,
            step0_universe_count=1,
            step2_hot_board_count=1,
            stock_best_board={"000001": "银行"},
            stock_all_boards={"000001": ["银行"]},
            stock_is_driver={"000001": True},
            stock_cci={"000001": 1.0},
            stock_early_vol={"000001": 700.0},
            step2_board_avg_gains={"银行": 1.2},
        )
        snapshot = {
            "schema_version": V20_V16_SNAPSHOT_SCHEMA,
            "trade_date": self.trade_date.isoformat(),
            "last_complete_bar": "09:39",
            "funnel": {
                "step0_universe_count": 1,
                "step2_hot_board_count": 1,
                "final_candidates": 1,
            },
            "board_avg_gains": {"银行": 1.2},
            "symbols": [
                {
                    "rank": 1,
                    "code": "000001",
                    "name": "平安银行",
                    "score": 0.8,
                    "snapshot_price": float(quote.early_close),
                    "boards": ["银行"],
                    "best_board": "银行",
                    "is_driver": True,
                    "cci": 1.0,
                    "volume_937": 700.0,
                    "history_hash": "a" * 64,
                }
            ],
        }
        return FrozenV16ScanBundle(
            trade_date=self.trade_date,
            frozen_at=datetime(2026, 8, 31, 15, 30, 2, tzinfo=TZ),
            scan_result=scan,
            stock_data={},
            comparison_pool_codes=("000001",),
            breadth_valid_n=0,
            breadth_down_n=0,
            prior_trade_date=date(2026, 8, 28),
            prior_amount_yuan={"000001": 1_000_000_000.0},
            snapshot=snapshot,
            snapshot_hash=sha256_json(snapshot),
        )


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
) -> tuple[V20Service, _LateReplayRepository, _LateReplayClient, _LateReplayPipeline, _DayContext]:
    seed = _service(monkeypatch, SimpleNamespace())
    status, state = _late_replay_status_and_state(seed)
    repository = _LateReplayRepository(status, state)
    client = _LateReplayClient(missing_label=missing_label)
    service = _service(monkeypatch, repository, client)
    pipeline = _LateReplayPipeline(status.trade_date)
    service._scan_pipeline = pipeline
    service._clock = lambda: datetime(2026, 8, 31, 15, 30, 3, tzinfo=TZ)
    monkeypatch.setattr(service, "_verify_prewarm_dependencies", lambda _prewarmed: None)
    prewarmed = SimpleNamespace(
        trade_date=status.trade_date,
        universe_codes=("000001",),
        breadth_codes=("000001", "000002"),
        required_minute_codes=("000001", "000002"),
    )
    context = _DayContext(
        trade_date=status.trade_date,
        calendar=(status.trade_date, date(2026, 9, 1), date(2026, 9, 2)),
        entry_status=status,
        prewarmed=prewarmed,
    )
    return service, repository, client, pipeline, context


async def test_late_0939_replay_core_is_durable_idempotent_and_officially_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, client, pipeline, context = _late_replay_service(monkeypatch)
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
    assert first.semantic["rolling7_state"] == "UNKNOWN"
    assert first.semantic["breadth_replay_mode"] == ("SKIPPED_NOT_USED_BY_BASE_WARMUP_OR_HEALTHY")
    assert first.semantic["raw_fact_n"] == 9
    assert first.semantic["raw_post_cutoff_n"] == 9
    assert first.semantic["pit_limitations"][-1] == "MEWS_IS_NOT_A_09:39_ENTRY_INPUT"
    assert first.semantic["state_replay_profile"] == "DEPLOYED_RUNTIME_LINEAGE"
    assert first.semantic["bootstrap_mode"] == "EMPTY_FORWARD_SHADOW"
    assert "decision_id" not in first.semantic
    assert "state_after_hash" not in first.semantic
    assert first.payload is not None
    assert "现在不开仓｜09:39复盘已过期" in str(first.payload["message"])
    assert "现在操作：不开仓，不补买，不追买" in str(first.payload["message"])
    assert client.calls == [("000001",)]
    assert pipeline.scan_calls == 1
    assert pipeline.observed_early_volume == 900.0
    assert sorted(label for _code, label in repository.raw) == [
        f"09:{minute:02d}" for minute in range(31, 40)
    ]
    assert repository.enqueue_calls == 1
    assert repository.seal_calls == 1
    assert repository.official_write_calls == 0
    assert repository.state.payload == state_before
    assert context.late_0939_replay_completed is True


async def test_late_0939_replay_rejects_state_that_moved_past_failed_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, client, pipeline, context = _late_replay_service(monkeypatch)
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
    assert pipeline.scan_calls == 0
    assert repository.events == {}
    assert repository.official_write_calls == 0


async def test_late_0939_replay_can_recover_entirely_from_durable_raw_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, client, pipeline, context = _late_replay_service(monkeypatch)
    await service._persist_history(
        context,
        {
            "000001": tuple(
                _bar("000001", f"09:{minute:02d}", close=10.0 + (minute - 30) / 100)
                for minute in range(31, 40)
            )
        },
        observed_at=datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
    )

    record = await service._ensure_late_0939_replay(
        context,
        datetime(2026, 8, 31, 15, 31, tzinfo=TZ),
    )

    assert record.semantic["raw_fact_n"] == 9
    assert client.calls == []
    assert pipeline.scan_calls == 1
    assert repository.official_write_calls == 0


async def test_late_0939_replay_missing_early_bar_fails_without_replay_or_official_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository, client, pipeline, context = _late_replay_service(
        monkeypatch,
        missing_label="09:38",
    )

    with pytest.raises(RuntimeError, match="incomplete"):
        await service._ensure_late_0939_replay(
            context,
            datetime(2026, 8, 31, 15, 30, tzinfo=TZ),
        )

    assert client.calls == [("000001",)]
    assert pipeline.scan_calls == 1
    assert repository.events == {}
    assert repository.official_write_calls == 0


async def test_automatic_late_replay_task_is_not_a_formal_runtime_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _repository, _client, _pipeline, context = _late_replay_service(monkeypatch)
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

    monkeypatch.setattr(service, "run_once", blocked_decision)
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

    async def exit_cycle(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    monkeypatch.setattr(service, "_run_exit_cycle", exit_cycle)
    monkeypatch.setattr(service, "_live_exit_tick_budget", lambda: 0.01)

    with pytest.raises(TimeoutError):
        await service._run_live_exit_tick(context, datetime(2026, 8, 31, 10, 0, tzinfo=TZ))
    await asyncio.wait_for(cancelled.wait(), timeout=1.0)
    await service._run_live_exit_tick(context, datetime(2026, 8, 31, 10, 0, 15, tzinfo=TZ))

    assert calls == 2


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

    with pytest.raises(TimeoutError):
        await service._poll_latest(
            _DayContext(trade_date=date(2026, 8, 31), calendar=()),
            ["000001"],
            observed_at=datetime(2026, 8, 31, 10, 0, tzinfo=TZ),
        )

    assert cancelled.is_set()


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

    service = _service(
        monkeypatch,
        SimpleNamespace(
            assert_runtime_leader=assert_leader,
            database_cutoff_reached=database_cutoff_reached,
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
    service._compatible_entry_bindings.add(
        (
            historical_config_hash[:24],
            historical_config_hash,
            historical_state_hash,
        )
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
    assert recovered.next_state["official_rolling_gaps"][-1]["signal_date"] == "2026-08-31"


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


def test_checkpoint_as_of_date_is_the_revision_zero_predecessor_anchor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(monkeypatch)
    state = genesis_state()
    as_of = date(2026, 8, 28)
    checkpoint = {
        "schema_version": "v20-bootstrap-checkpoint/v2",
        "target_official_stream_id": config.official_stream_id,
        "state_lineage_id": config.state_lineage_id,
        "source_state_semantics_hash": config.state_semantics_hash,
        "as_of_trade_date": as_of.isoformat(),
        "source_last_terminal_trade_date": as_of.isoformat(),
        "official_state": state,
        "official_state_hash": sha256_json(state),
        "state_shadow_batches": [
            {
                "kind": "ROLLING7",
                "status": "COMPLETE_VALID",
                "signal_date": date(2026, 8, 3 + index).isoformat(),
            }
            for index in range(7)
        ],
    }
    checkpoint_path = tmp_path / "checkpoint.json"
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
    checkpoint_config = replace(
        config,
        bootstrap_mode="CHECKPOINT",
        bootstrap_checkpoint_path=checkpoint_path,
    )

    mismatched = {**checkpoint, "source_state_semantics_hash": "0" * 64}
    checkpoint_path.write_text(json.dumps(mismatched), encoding="utf-8")
    with pytest.raises(V20ConfigError, match="state semantics"):
        _bootstrap_bundle(
            checkpoint_config,
            empty_predecessor_trade_date=date(1999, 1, 1),
        )
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    bootstrap = _bootstrap_bundle(
        checkpoint_config,
        empty_predecessor_trade_date=date(1999, 1, 1),
    )

    assert bootstrap.predecessor_trade_date == as_of

    audited_legacy_hash = "a" * 64
    monkeypatch.setattr(
        runtime_config_module,
        "_AUDITED_LEGACY_STATE_SEMANTICS_HASHES",
        frozenset({audited_legacy_hash}),
    )
    resolved_legacy = {
        **checkpoint,
        "source_state_semantics_hash": audited_legacy_hash,
        "resolved_state_semantics_hash": config.state_semantics_hash,
    }
    checkpoint_path.write_text(json.dumps(resolved_legacy), encoding="utf-8")
    assert (
        _bootstrap_bundle(
            checkpoint_config,
            empty_predecessor_trade_date=date(1999, 1, 1),
        ).predecessor_trade_date
        == as_of
    )

    tampered_resolution = {
        **resolved_legacy,
        "source_state_semantics_hash": "b" * 64,
    }
    checkpoint_path.write_text(json.dumps(tampered_resolution), encoding="utf-8")
    with pytest.raises(V20ConfigError, match="state semantics"):
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

    async def scan(*args, **kwargs):
        return SimpleNamespace(frozen_at=formed_at)

    service = _service(monkeypatch, repository)
    service._scan_pipeline = SimpleNamespace(scan=scan)
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
        datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ),
    )

    assert len(repository.commits) == 1
    commit = repository.commits[0]
    assert commit.action == "INPUT_INVALID"
    assert commit.semantic["reason_codes"] == ["INPUT_TIME_BOUNDARY_VIOLATION"]
    assert commit.shadow_batches == ()
    assert commit.model_batch is None
    assert commit.invalid_commit_not_before_ts == datetime(2026, 8, 31, 9, 40, tzinfo=TZ)
    assert commit.next_state["official_rolling_gaps"][-1]["signal_date"] == "2026-08-31"


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

    service._scan_pipeline = SimpleNamespace(scan=scan_must_not_run)
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
    assert "raw 09:39 terminal-bar coverage is not ready: 0/1" in commit.semantic["failure_detail"]
    assert commit.invalid_commit_not_before_ts == cutoff
    assert repository.sealed == [commit.event_id]


async def test_prewarm_near_cutoff_is_skipped_so_0940_can_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(monkeypatch)
    service = _service(monkeypatch, _LateNormalEntryRepository(config))
    prewarm_started = False

    async def prewarm(*_args, **_kwargs):
        nonlocal prewarm_started
        prewarm_started = True
        await asyncio.Event().wait()

    service._scan_pipeline = SimpleNamespace(prewarm=prewarm)
    # The iteration timestamp is deliberately stale.  The prewarm budget must
    # be derived from a fresh clock sample or this request could cross 09:40.
    service._clock = lambda: datetime(2026, 8, 31, 9, 39, 59, tzinfo=TZ)
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 31),),
    )

    await service._run_entry_collection_cycle(
        context,
        datetime(2026, 8, 31, 9, 39, 30, tzinfo=TZ),
    )

    assert not prewarm_started
    assert context.prewarmed is None
    assert context.last_phase == "PREWARM_RETRY"
    assert context.last_entry_failure_detail is not None
    assert "reserved 09:40 cutoff window" in context.last_entry_failure_detail


async def test_decision_watchdog_preempts_blocked_reconciliation_at_0940(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def assert_leader() -> None:
        return None

    async def database_cutoff_reached(_cutoff: datetime) -> bool:
        return True

    service = _service(
        monkeypatch,
        SimpleNamespace(
            assert_runtime_leader=assert_leader,
            database_cutoff_reached=database_cutoff_reached,
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

    service = _service(
        monkeypatch,
        SimpleNamespace(
            assert_runtime_leader=assert_leader,
            database_cutoff_reached=database_cutoff_reached,
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
        assert alerts[0]["code"] == "ENTRY_CALENDAR_UNKNOWN_NO_BUY"
        assert alerts[0]["entity_id"] == trade_date.isoformat()
        assert "今天不买，不要追买" in alerts[0]["message"]
        assert service._lane_health["decision"].error_revision == error_revision_before + 1
    else:
        assert service._lane_health["decision"].error_revision == error_revision_before


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


async def test_maturity_prefers_older_complete_candidate_over_newer_empty_candidate(
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
    service = _service(monkeypatch, repository, _MaturityClient(fail=True))
    context = _DayContext(
        trade_date=date(2026, 8, 31),
        calendar=(date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 31)),
    )

    await service._process_mature_shadow(context, received)

    assert len(repository.completed) == 1
    completed = repository.completed[0]
    assert completed["status"] == "COMPLETE_VALID"
    assert completed["batch_return"] == pytest.approx(0.1)
    assert completed["payload_update"]["daily_snapshot_id"] == "daily-older-complete"


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

    async def capture(active, _now, _calendar=()):
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
    def __init__(self) -> None:
        self.cutoff: datetime | None = None
        self.locked: dict[str, float] | None = None

    async def list_active_legs(self, trade_date, **kwargs):
        return []

    async def database_cutoff_reached(self, cutoff):
        return True

    async def list_pending_shadow_reference_batches(self, before_signal_date, **kwargs):
        return [
            SimpleNamespace(
                batch_id="health",
                signal_date=date(2026, 8, 28),
                payload={
                    "d1": "2026-08-31",
                    "top3": [{"code": "000001"}],
                    "comparison_pool_codes": ["000001"],
                },
            )
        ]

    async def list_pending_reference_legs(self, signal_date, **kwargs):
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

    async def evaluate(active, _now, _calendar=()):
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


async def test_partial_live_data_keeps_sibling_evaluation_without_global_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    latest = {first.code: _bar(first.code, "10:00", trade_date=date(2026, 9, 1))}

    service, context, alerts, evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        _LiveExitDataClient(latest=latest, history={}),
    )

    assert succeeded is True
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts
    assert "LIVE_EXIT_SYMBOL_DATA_GAP" in alerts
    assert any(set(codes) == {first.code, second.code} for codes in evaluations)
    assert service._lane_health["live_exit"].last_error is None


async def test_single_empty_symbol_is_diagnostic_not_global_feed_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _closed_history_leg()

    _service_value, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [leg],
        _LiveExitDataClient(latest={}, history={leg.code: ()}),
    )

    assert succeeded is True
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_SYMBOL_DATA_GAP" in alerts
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts


async def test_single_symbol_with_both_vendor_paths_failed_is_global_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _closed_history_leg()

    service, context, alerts, _evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [leg],
        _LiveExitDataClient(
            latest=RuntimeError("latest transport down"),
            history=RuntimeError("history transport down"),
        ),
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
    assert client.latest_calls == 1
    assert client.history_calls == 1
    assert context.live_exit_market_data_outage is False
    assert "LIVE_EXIT_MARKET_DATA_UNAVAILABLE" not in alerts
    assert service._lane_health["live_exit"].last_error is None


async def test_morning_close_publication_grace_accepts_arrived_1130_bar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _closed_history_leg()
    second = _second_live_leg(first)
    latest = {
        leg.code: _bar(leg.code, "11:30", trade_date=date(2026, 9, 1)) for leg in (first, second)
    }

    service, context, alerts, evaluations, succeeded = await _run_live_data_probe(
        monkeypatch,
        [first, second],
        _LiveExitDataClient(latest=latest, history={}),
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

    async def evaluate(_active, _now, _calendar=()):
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
            "visible_message_mode": "AUTOMATIC_ENTRY_RENDER",
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
        self.registered_config_compatible = True
        self.registered_config_checks = 0
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

    async def is_registered_source_config_compatible(self, *_args: Any, **_kwargs: Any) -> bool:
        self.registered_config_checks += 1
        return self.registered_config_compatible

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


async def test_manual_monitor_accepts_an_audited_previous_full_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_hash = "7" * 64
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
        source_config_hash=old_hash,
    )
    service._compatible_entry_bindings.add(
        ("unrelated-config-id", "6" * 64, service.config.state_semantics_hash)
    )

    result = await service.enroll_manual_monitor(
        repository.source_event_id,
        "manual-monitor-old-config",
    )

    assert result["created"] is True
    assert repository.enrollment_commit.source_config_hash == old_hash
    assert repository.registered_config_checks == 1


async def test_manual_monitor_rejects_an_unregistered_previous_full_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, repository = await _manual_monitor_service(
        monkeypatch,
        now=datetime(2026, 9, 1, 2, 0, tzinfo=TZ),
        source_config_hash="7" * 64,
    )
    repository.registered_config_compatible = False

    with pytest.raises(V20SemanticConflict, match="unaudited historical config"):
        await service.enroll_manual_monitor(
            repository.source_event_id,
            "manual-monitor-unknown-config",
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
