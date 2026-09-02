"""Production-boundary tests for the dedicated V20 ASGI host."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from src.web.v20_app import create_v20_app

PROJECT_ROOT = Path(__file__).resolve().parents[3]


class _Service:
    def __init__(self, *, enabled: bool) -> None:
        self.config = SimpleNamespace(enabled=enabled)
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self) -> None:
        self.stop_calls += 1

    async def status(self):
        return {"enabled": self.config.enabled, "mode": "test"}

    async def record_reminder_stop_ack(self, payload):
        return payload

    async def trigger_manual_scan(self, request_id):
        return {"manual_request_id": request_id}

    async def enroll_manual_monitor(self, source_event_id, request_id):
        return {
            "source_event_id": source_event_id,
            "manual_request_id": request_id,
        }


def test_dedicated_host_exposes_only_the_four_v20_routes() -> None:
    app = create_v20_app(v20_service=_Service(enabled=False))

    routes = {(route.path, frozenset(route.methods or ())) for route in app.routes}

    assert routes == {
        ("/api/v20/status", frozenset({"GET"})),
        ("/api/v20/reminder-stop-acks", frozenset({"POST"})),
        ("/api/v20/trigger-scan", frozenset({"POST"})),
        ("/api/v20/manual-monitor", frozenset({"POST"})),
    }


def test_dedicated_host_owns_enabled_v20_lifecycle(monkeypatch) -> None:
    monkeypatch.setenv("V20_STATUS_API_KEY", "status-secret")
    service = _Service(enabled=True)

    with TestClient(create_v20_app(v20_service=service)) as client:
        response = client.get(
            "/api/v20/status",
            headers={"X-V20-Status-Key": "status-secret"},
        )
        assert service.start_calls == 1
        assert response.status_code == 200

    assert service.stop_calls == 1


def test_disabled_default_does_not_start_background_workers(monkeypatch) -> None:
    monkeypatch.setenv("V20_STATUS_API_KEY", "status-secret")
    service = _Service(enabled=False)

    with TestClient(create_v20_app(v20_service=service)) as client:
        response = client.get(
            "/api/v20/status",
            headers={"X-V20-Status-Key": "status-secret"},
        )
        assert response.json()["enabled"] is False

    assert service.start_calls == 0
    assert service.stop_calls == 0


def test_importing_dedicated_host_does_not_import_execution_surface() -> None:
    code = """
import os
import sys
import hashlib
from pathlib import Path
os.environ['V20_ENABLED'] = 'false'
os.environ['V20_MODE'] = 'forward_shadow'
os.environ['V20_ALLOW_PRODUCTION_PUSH'] = 'false'
import src.strategy.v20.runtime_config as runtime_config
project_root = Path.cwd()
test_mixed_classes = {
    'src/web/v20_service.py': 'V20_SERVICE_STATE_ORCHESTRATION_V4',
    'src/data/database/v20_repository.py': 'V20_LEDGER_STATE_CONTRACT_V2',
}
for relative, reviewed_hashes in runtime_config._MIXED_STATE_SOURCE_CLASSES.items():
    reviewed_hashes[hashlib.sha256((project_root / relative).read_bytes()).hexdigest()] = (
        test_mixed_classes[relative]
    )
import src.web.v20_app as v20_host
service = v20_host._create_default_v20_service()
assert service.config.enabled is False
banned = [
    'src.web.app',
    'src.web.routes',
    'src.web.iquant_routes',
    'src.trading.position_manager',
]
loaded = [name for name in banned if name in sys.modules]
if loaded:
    raise SystemExit(','.join(loaded))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_enabled_host_startup_and_shutdown_do_not_import_execution_surface() -> None:
    code = """
import asyncio
import hashlib
import os
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

os.environ['V20_ENABLED'] = 'false'
os.environ['V20_MODE'] = 'forward_shadow'
os.environ['V20_ALLOW_PRODUCTION_PUSH'] = 'false'
os.environ['V20_INGEST_API_KEY'] = 'i' * 32
os.environ['V20_STATUS_API_KEY'] = 's' * 32
os.environ['DB_SSLROOTCERT_SHA256'] = 'c' * 64
os.environ['TUSHARE_TOKEN'] = 'test-token'

import src.common.config as common_config
import src.strategy.v20.runtime_config as runtime_config
import src.data.clients.iquant_historical_adapter as historical_module
import src.data.clients.tushare_realtime as realtime_module
import src.data.database.fundamentals_db as fundamentals_module
import src.data.sources.local_concept_mapper as concept_module
import src.strategy.filters.stock_filter as stock_filter_module
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.runtime_config import V20RouteBinding, load_v20_runtime_config
from src.web.v15_scan_service import V15ScanState
from src.web.v20_app import create_v20_app
from src.web.v20_service import V20Service, _cleanup_v20_scan_resources, _init_v20_scan_resources

project_root = Path.cwd()
test_mixed_classes = {
    'src/web/v20_service.py': 'V20_SERVICE_STATE_ORCHESTRATION_V4',
    'src/data/database/v20_repository.py': 'V20_LEDGER_STATE_CONTRACT_V2',
}
for relative, reviewed_hashes in runtime_config._MIXED_STATE_SOURCE_CLASSES.items():
    reviewed_hashes[hashlib.sha256((project_root / relative).read_bytes()).hexdigest()] = (
        test_mixed_classes[relative]
    )
owned = {}


class FakeRealtime:
    def __init__(self, *, token):
        assert token == 'test-token'
        self.started = False
        self.stopped = False
        owned['realtime'] = self

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    def as_ifind_format(self, *_args, **_kwargs):
        return {}


class FakeFundamentals:
    def __init__(self):
        self.connected = False
        self.closed = False
        owned['fundamentals'] = self

    async def connect(self):
        self.connected = True

    async def close(self):
        self.closed = True


class FakeHistorical:
    def __init__(self, realtime_client, cache=None, *, tushare_token=None):
        assert realtime_client is owned['realtime']
        assert tushare_token == 'test-token'
        self.cache = cache


class FakeConceptMapper:
    pass


class FakeStockFilter:
    def __init__(self, config):
        self.config = config


common_config.get_tushare_token = lambda: 'test-token'
realtime_module.TushareRealtimeClient = FakeRealtime
fundamentals_module.create_fundamentals_db_from_config = lambda _path: FakeFundamentals()
historical_module.IQuantHistoricalAdapter = FakeHistorical
concept_module.LocalConceptMapper = FakeConceptMapper
stock_filter_module.StockFilter = FakeStockFilter


class FakeRepository:
    def __init__(self):
        self.connected = False
        self.closed = False

    async def connect(self):
        self.connected = True

    async def acquire_runtime_leader(self, **_kwargs):
        return None

    async def register_config(self, **_kwargs):
        return None

    async def ensure_genesis_state(self, *_args, **_kwargs):
        return None

    async def load_state(self, lineage_id):
        return SimpleNamespace(lineage_id=lineage_id, revision=0, state_hash='a' * 64)

    async def get_outbox_health(self, **_kwargs):
        return {'delivery_error_n': 0}

    async def assert_runtime_leader(self):
        await asyncio.Event().wait()

    async def close(self):
        self.closed = True


class FakePublisher:
    async def run(
        self,
        stop_event,
        *,
        before_cycle=None,
        on_cycle_success=None,
        on_cycle_error=None,
    ):
        del stop_event, on_cycle_success, on_cycle_error
        assert before_cycle is not None
        await before_cycle()


config = load_v20_runtime_config(project_root)
binding = V20RouteBinding(
    route_id=config.route_id,
    expected_bot_origin='https://relay.internal',
    expected_app_id_sha256=hashlib.sha256(b'shadow-app').hexdigest(),
    expected_chat_id_sha256=hashlib.sha256(b'shadow-chat').hexdigest(),
)
config = replace(
    config,
    enabled=True,
    route_binding=binding,
    route_bindings={**config.route_bindings, 'forward_shadow': binding},
    v20_db_ca_sha256='d' * 64,
    fundamentals_db_ca_sha256='c' * 64,
)
artifacts = load_g_artifacts(
    config.artifact_manifest_path.parent,
    expected_manifest_sha256=config.artifact_manifest_sha256,
)
route = SimpleNamespace(
    chat_id='shadow-chat',
    app_id='shadow-app',
    app_secret='shadow-secret',
    destination_fingerprint=config.route_binding.destination_fingerprint,
    is_configured=lambda: True,
)
repository = FakeRepository()
scan_state = V15ScanState(fundamentals_db=FakeFundamentals())
service = V20Service(
    config=config,
    repository=repository,
    scan_state=scan_state,
    artifacts=artifacts,
    publisher=FakePublisher(),
    routes={config.route_id: route},
    initialize_resources=_init_v20_scan_resources,
    cleanup_resources=_cleanup_v20_scan_resources,
    mews_source=SimpleNamespace(),
)

with TestClient(create_v20_app(v20_service=service)):
    assert service._started is True
    assert repository.connected is True
    assert scan_state.initialized is True
    assert owned['realtime'].started is True
    assert owned['fundamentals'].connected is True

assert service._started is False
assert repository.closed is True
assert scan_state.initialized is False
assert owned['realtime'].stopped is True
assert owned['fundamentals'].closed is True

legacy_web = {'src.web.app', 'src.web.routes', 'src.web.iquant_routes'}
banned = sorted(
    name
    for name in sys.modules
    if name in legacy_web or name == 'src.trading' or name.startswith('src.trading.')
)
if banned:
    raise SystemExit(','.join(banned))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
