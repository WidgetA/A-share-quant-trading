from __future__ import annotations

import ast
import asyncio
import hashlib
import json
from dataclasses import dataclass
from dataclasses import replace as dataclasses_replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.data.clients.tushare_realtime import (
    BEIJING_TZ,
    TushareEarlyMarketData,
    TushareMinuteBar,
    TushareQuote,
)
from src.data.database.v20_repository import (
    EntryStatus,
    OutboxRecord,
    StateRecord,
    sha256_json,
)
from src.strategy.lgbrank_scorer import ScoredStock
from src.strategy.strategies.v16_scanner import V16ScanResult, V16StockData
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.decision_engine import (
    _validate_v16_snapshot_formatter_evidence,
    genesis_state,
    prepare_entry,
)
from src.strategy.v20.identity import named_hash
from src.strategy.v20.models import (
    V20_DATA_ALERT_SEMANTIC_SCHEMA,
    V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
    V20_ENTRY_SEMANTIC_SCHEMA,
    V20_FEISHU_FORMATTER_PROFILE,
    V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
    HealthObservation,
    HealthSnapshot,
    HealthStatus,
    serialize_health_snapshot,
)
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.web import app as web_app
from src.web import v15_scan_service
from src.web.v15_scan_service import (
    CanonicalV16ScanBundle,
    V15ScanState,
    get_or_compute_canonical_v16,
)
from src.web.v20_routes import _dispatch_manual_trigger, create_v20_router
from src.web.v20_service import (
    V20Service,
    _bar_payload,
    _DayContext,
    _init_embedded_v20_scan_resources,
)

TZ = ZoneInfo("Asia/Shanghai")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
FRESH_CODES = (
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


class Bomb:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"forbidden dependency boundary was touched: {name}")


def _assert_no_legacy_scan_pipeline_construction() -> None:
    """The production service module must not construct the old scan pipeline."""

    from src.web import v20_service as service_module

    assert not hasattr(service_module, "V20ScanPipeline")
    tree = ast.parse(Path(service_module.__file__).read_text(encoding="utf-8"))
    offenders = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.Name) and node.id == "V20ScanPipeline")
        or (isinstance(node, ast.Attribute) and node.attr == "V20ScanPipeline")
        or (isinstance(node, ast.ClassDef) and node.name == "V20ScanPipeline")
    ]
    assert not offenders, "service module still constructs the old V20ScanPipeline"


class FakeScorer:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.model_sha256 = "1" * 64
        self.feature_list_sha256 = "2" * 64


class FakeV16Scanner:
    scan_calls = 0

    def __init__(self, **_kwargs: Any) -> None:
        pass

    def get_universe(self) -> tuple[dict[str, list[tuple[str, str]]], set[str]]:
        return (
            {"board-a": [(code, f"fresh-{code}") for code in FRESH_CODES]},
            set(FRESH_CODES),
        )

    async def scan(self, stock_data: Any, clean_boards: Any) -> Any:
        type(self).scan_calls += 1
        top10 = [
            ScoredStock(
                code=code,
                name=f"fresh-{code}",
                score=0.9 - index * 0.01,
                rank=index + 1,
                buy_price=10.0 + index,
            )
            for index, code in enumerate(FRESH_CODES)
        ]
        return V16ScanResult(
            recommended=top10,
            all_scored=top10,
            stock_best_board={code: "board-a" for code in FRESH_CODES},
            stock_all_boards={code: ["board-a"] for code in FRESH_CODES},
            stock_is_driver={code: True for code in FRESH_CODES},
            stock_cci={code: 50.0 for code in FRESH_CODES},
            stock_early_vol={code: 1000.0 for code in FRESH_CODES},
            step0_codes=list(FRESH_CODES),
            step2_codes=list(FRESH_CODES),
            st_eligible_codes=list(FRESH_CODES),
            step3_codes=list(FRESH_CODES),
            step4_codes=list(FRESH_CODES),
            step5_codes=list(FRESH_CODES),
            step6_codes=list(FRESH_CODES),
            step6_5_codes=list(FRESH_CODES),
            step6_6_codes=list(FRESH_CODES),
            step0_universe_count=len(FRESH_CODES),
            step2_hot_board_count=2,
            step2_board_avg_gains={"board-a": 1.25},
            final_candidates=len(FRESH_CODES),
        )


class FakeRealtimeClient:
    early_calls = 0

    async def stop(self) -> None:
        return None

    async def batch_get_early_market_data(
        self,
        codes: list[str],
        expected_trade_date: date | None = None,
    ) -> dict[str, TushareEarlyMarketData]:
        type(self).early_calls += 1
        result: dict[str, TushareEarlyMarketData] = {}
        for code in codes:
            trade_date = datetime.now(BEIJING_TZ).date()
            bars = []
            for minute in range(31, 40):
                bars.append(
                    TushareMinuteBar(
                        stock_code=code,
                        bar_end=datetime.combine(trade_date, time(9, minute), TZ),
                        end_label=f"09:{minute:02d}",
                        open_price=11.0,
                        close_price=12.3,
                        high_price=12.4,
                        low_price=10.9,
                        volume=2000.0,
                        amount=24000.0,
                    )
                )
            bars.append(
                TushareMinuteBar(
                    stock_code=code,
                    bar_end=datetime.combine(trade_date, time(9, 41), TZ),
                    end_label="09:41",
                    open_price=999.0,
                    close_price=999.0,
                    high_price=999.1,
                    low_price=998.9,
                    volume=99_000.0,
                    amount=999_000.0,
                )
            )
            result[code] = TushareEarlyMarketData(
                quote=TushareQuote(
                    stock_code=code,
                    open_price=11.0,
                    latest_price=12.0,
                    high_price=12.5,
                    low_price=10.9,
                    volume=5000.0,
                    amount=60000.0,
                    early_close=12.3,
                    early_high=12.4,
                    early_low=11.5,
                    early_volume=3000.0,
                    volume_937=2000.0,
                ),
                early_bars=tuple(bars),
                source_hash=sha256_json(
                    [_bar_payload(bar) for bar in bars if bar.end_label <= "09:39"]
                ),
            )
        return result

    async def batch_get_minute_history(
        self, codes: list[str]
    ) -> dict[str, tuple[TushareMinuteBar, ...]]:
        result: dict[str, tuple[TushareMinuteBar, ...]] = {}
        for code in codes:
            bars = []
            for minute in range(31, 42):
                if minute == 40:
                    continue
                bars.append(
                    TushareMinuteBar(
                        stock_code=code,
                        bar_end=datetime.combine(
                            datetime.now(BEIJING_TZ).date(), time(9, minute), TZ
                        ),
                        end_label=f"09:{minute:02d}",
                        open_price=999.0 if minute == 41 else 11.0,
                        close_price=999.0 if minute == 41 else 12.3,
                        high_price=999.1 if minute == 41 else 12.4,
                        low_price=998.9 if minute == 41 else 10.9,
                        volume=99_000.0 if minute == 41 else 2000.0,
                        amount=999_000.0 if minute == 41 else 24000.0,
                    )
                )
            result[code] = tuple(bars)
        return result

    async def fetch_prev_closes(self, trade_date: str) -> dict[str, float]:
        return {code: 10.5 for code in FRESH_CODES}


class FakeFundamentalsDB:
    async def batch_get_fundamentals(self, codes: list[str]) -> dict[str, Any]:
        return {}

    async def batch_current_names(self, codes: list[str]) -> dict[str, str]:
        return {code: f"fresh-{code}" for code in codes}

    async def close(self) -> None:
        return None


class FakeHistoryAdapter:
    history_calls = 0

    async def history_quotes(
        self,
        *,
        codes: str,
        indicators: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        type(self).history_calls += 1
        today = datetime.now(BEIJING_TZ).date()
        dates = [(today - timedelta(days=41 - index)).isoformat() for index in range(40)]
        return {
            "tables": [
                {
                    "thscode": code,
                    "table": {
                        "time": dates,
                        "open": [10.0] * 40,
                        "high": [10.5] * 40,
                        "low": [9.5] * 40,
                        "close": [10.0 + index * 0.01 for index in range(40)],
                        "volume": [1000.0] * 40,
                    },
                }
                for code in codes.split(",")
            ]
        }


@dataclass
class FakeClient:
    started: bool = False
    stopped: bool = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


class FakeBacktestCache:
    @staticmethod
    def load_from_oss(*_args: Any, **_kwargs: Any) -> Any:
        return None


class FakeStartupFundamentals:
    def __init__(self) -> None:
        self.closed = False

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True


def _canonical_fixture(monkeypatch: pytest.MonkeyPatch) -> V15ScanState:
    FakeV16Scanner.scan_calls = 0
    FakeRealtimeClient.early_calls = 0
    FakeHistoryAdapter.history_calls = 0
    monkeypatch.setattr("src.strategy.lgbrank_scorer.LGBRankScorer", FakeScorer)
    monkeypatch.setattr("src.strategy.strategies.v16_scanner.V16Scanner", FakeV16Scanner)

    async def fake_calendar() -> list[date]:
        today = datetime.now(BEIJING_TZ).date()
        return [today - timedelta(days=1), today]

    monkeypatch.setattr(v15_scan_service, "get_trade_calendar", fake_calendar)

    async def no_notification(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(v15_scan_service, "_notify_feishu_v16_top10", no_notification)
    monkeypatch.setattr(v15_scan_service, "_notify_feishu_signal", no_notification)
    monkeypatch.setattr(v15_scan_service, "_schedule_v16_day_gate_shadow", lambda *_args: None)
    return V15ScanState(
        initialized=True,
        realtime_client=FakeRealtimeClient(),
        fundamentals_db=FakeFundamentalsDB(),
        historical_adapter=FakeHistoryAdapter(),
        concept_mapper=object(),
        stock_filter=object(),
        tushare_cache=None,
    )


def _v20_config(monkeypatch: pytest.MonkeyPatch) -> Any:
    for name in ("V20_ENABLED", "V20_ALLOW_PRODUCTION_PUSH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("V20_MODE", "forward_shadow")
    monkeypatch.setenv("DB_SSLROOTCERT_SHA256", "c" * 64)
    monkeypatch.setenv("V20_INGEST_API_KEY", "i" * 32)
    monkeypatch.setenv("V20_STATUS_API_KEY", "s" * 32)
    from src.strategy.v20 import runtime_config

    monkeypatch.setattr(runtime_config, "_dependency_hashes", lambda _root: {})
    monkeypatch.setattr(
        runtime_config, "_state_semantics_source", lambda _payload: {"accepted": True}
    )
    config = load_v20_runtime_config(PROJECT_ROOT)
    # The harness pins DB_SSLROOTCERT_SHA256 above; align the reviewed runtime
    # configuration with it so service.start() passes the fundamentals CA check.
    return dataclasses_replace(config, fundamentals_db_ca_sha256="c" * 64)


def _v20_service(
    monkeypatch: pytest.MonkeyPatch,
    repository: Any = None,
    scan_state: V15ScanState | None = None,
) -> V20Service:
    config = _v20_config(monkeypatch)
    state = scan_state or V15ScanState(initialized=True)
    return V20Service(
        config=config,
        repository=repository or SimpleNamespace(),
        scan_state=state,
        artifacts=load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        ),
        publisher=SimpleNamespace(),
        routes={},
        mews_source=Bomb(),
    )


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def _no_op_stop() -> None:
    return None


async def _no_op_start() -> None:
    return None


@pytest.mark.asyncio
async def test_app_startup_gives_v16_and_v20_one_scan_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[Any] = []

    def factory(*, fundamentals_db: object | None = None, scan_state: Any = None) -> Any:
        assert fundamentals_db is not None
        assert isinstance(scan_state, V15ScanState)
        service = SimpleNamespace(
            config=SimpleNamespace(enabled=True, deployment_mode="forward_shadow"),
            scan_state=scan_state,
            start=_no_op_start,
            stop=_no_op_stop,
        )
        created.append(service)
        return service

    monkeypatch.setattr(web_app, "_create_default_v20_service", factory)
    app = web_app.create_app()
    app.state.fundamentals_db = FakeStartupFundamentals()
    await web_app._start_v20_lifecycle(app)
    assert app.state.v20_service_started is True
    assert created[0].scan_state is app.state.v15_scan_state
    assert (
        created[0].scan_state.canonical_coordinator
        is app.state.v15_scan_state.canonical_coordinator
    )


@pytest.mark.asyncio
async def test_app_injection_shares_existing_fundamentals_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = object()

    class SharedFundamentals:
        connection_pool = pool

        async def close(self) -> None:
            return None

    shared = V15ScanState(
        initialized=True,
        fundamentals_db=SharedFundamentals(),
    )
    service = _v20_service(monkeypatch, scan_state=shared)
    app = web_app.create_app(v20_service=service)

    assert service._scan_state is app.state.v15_scan_state
    assert app.state.v15_scan_state.fundamentals_db is shared.fundamentals_db
    assert app.state.v15_scan_state.fundamentals_db.connection_pool is pool
    assert service._scan_state.fundamentals_db is shared.fundamentals_db
    assert not hasattr(service, "_scan_pipeline")


@pytest.mark.asyncio
async def test_real_factories_and_app_lifecycle_have_no_scan_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common.v20_feishu import V20FeishuRoute
    from src.strategy.v20.runtime_config import V20RouteBinding
    from src.web import v20_service as service_module

    base_config = _v20_config(monkeypatch)
    route = V20FeishuRoute(
        route_id=base_config.route_id,
        bot_url="https://relay.internal",
        app_id="app-id",
        app_secret="app-secret",
        chat_id="chat-id",
        transport="legacy_send",
    )
    binding = V20RouteBinding(
        route_id=route.route_id,
        expected_bot_origin=route.bot_origin,
        expected_app_id_sha256=hashlib.sha256(b"app-id").hexdigest(),
        expected_chat_id_sha256=hashlib.sha256(b"chat-id").hexdigest(),
    )
    strict_config = dataclasses_replace(
        base_config,
        enabled=True,
        route_binding=binding,
        route_bindings={**base_config.route_bindings, "forward_shadow": binding},
    )
    embedded_config = dataclasses_replace(base_config, enabled=False)
    repository_config = SimpleNamespace(
        schema=strict_config.database_schema,
        pool_min_size=strict_config.database_pool_min_size,
        pool_max_size=strict_config.database_pool_max_size,
        ssl_root_cert_sha256=strict_config.v20_db_ca_sha256,
    )

    class LifecycleRepository:
        compatible_entry_bindings: frozenset[Any] = frozenset()

        def __init__(self) -> None:
            self.config = repository_config

        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def acquire_runtime_leader(self, **_kwargs: Any) -> None:
            return None

        async def assert_runtime_leader(self) -> None:
            return None

        async def register_config(self, **_kwargs: Any) -> None:
            return None

        async def ensure_genesis_state(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def load_state(self, lineage_id: str) -> StateRecord:
            payload = genesis_state()
            return StateRecord(
                lineage_id=lineage_id,
                revision=0,
                state_hash=sha256_json(payload),
                payload=payload,
            )

        async def get_outbox_health(self, **_kwargs: Any) -> dict[str, int]:
            return {"unsealed_n": 0, "pending_delivery_n": 0, "leased_n": 0}

    class LifecycleFundamentals:
        def __init__(self) -> None:
            self.config = SimpleNamespace()
            self.connection_pool = object()
            self.closed = False

        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            self.closed = True

    class LifecycleRealtime:
        def __init__(self, *, token: str) -> None:
            self.token = token

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    repository = LifecycleRepository()

    assert not hasattr(service_module, "V20ScanPipeline")
    monkeypatch.setattr(service_module, "validate_v20_api_keys", lambda: None)
    monkeypatch.setattr(
        service_module,
        "validated_v20_tushare_token",
        lambda: "pipeline-bomb-token",
    )
    monkeypatch.setattr(service_module, "validate_v20_database_consumers", lambda *_args: None)
    monkeypatch.setattr(
        service_module,
        "create_v20_repository_from_config",
        lambda _path: repository,
    )
    monkeypatch.setattr(
        service_module,
        "create_embedded_v20_repository_from_config",
        lambda _path, *, shared_pool=None: repository,
    )
    monkeypatch.setattr(
        service_module,
        "load_v20_feishu_routes",
        lambda: {route.route_id: route},
    )
    monkeypatch.setattr(
        service_module,
        "load_legacy_embedded_v20_route",
        lambda: route,
    )
    monkeypatch.setattr(
        service_module, "_embedded_runtime_config", lambda _base, _route: embedded_config
    )

    class FakeMewsCalculator:
        @staticmethod
        def default_bootstrap_path(_root: Any) -> Any:
            return None

        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

    monkeypatch.setattr(
        service_module,
        "LocalMewsSnapshotCalculator",
        FakeMewsCalculator,
    )
    monkeypatch.setattr("src.common.config.get_tushare_token", lambda: "pipeline-bomb-token")
    monkeypatch.setattr(
        "src.data.database.fundamentals_db.create_fundamentals_db_from_config",
        lambda *_args, **_kwargs: LifecycleFundamentals(),
    )
    monkeypatch.setattr(
        "src.data.clients.tushare_realtime.TushareRealtimeClient",
        LifecycleRealtime,
    )
    monkeypatch.setattr(
        "src.data.clients.iquant_historical_adapter.IQuantHistoricalAdapter",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        "src.data.sources.local_concept_mapper.LocalConceptMapper", lambda: object()
    )
    monkeypatch.setattr("src.strategy.filters.stock_filter.StockFilter", lambda _config: object())

    async def call_factory(factory: Any, **kwargs: Any) -> Any:
        try:
            return await asyncio.to_thread(factory, **kwargs)
        except BaseException as exc:
            return exc

    shared = V15ScanState(
        initialized=True,
        fundamentals_db=SimpleNamespace(config=SimpleNamespace(), connection_pool=object()),
    )
    monkeypatch.setattr(service_module, "load_v20_runtime_config", lambda _root: strict_config)
    strict = await call_factory(V20Service.from_default_config, scan_state=shared)
    monkeypatch.setattr(service_module, "load_v20_runtime_config", lambda _root: embedded_config)
    embedded = await call_factory(
        V20Service.from_legacy_runtime,
        fundamentals_db=SimpleNamespace(connection_pool=object()),
        scan_state=shared,
    )
    app = web_app.create_app()
    app.state.fundamentals_db = SimpleNamespace(connection_pool=object())
    monkeypatch.setattr(service_module, "load_v20_runtime_config", lambda _root: strict_config)
    try:
        await web_app._start_v20_lifecycle(app)
        assert app.state.v20_service_started is True
    finally:
        await web_app._stop_v20_lifecycle(app)
    assert not isinstance(strict, BaseException), str(strict)
    assert not isinstance(embedded, BaseException), str(embedded)
    # The lifecycle stop owns the flag: started during the run, cleared after.
    assert app.state.v20_service_started is False
    _assert_no_legacy_scan_pipeline_construction()
    assert not hasattr(strict, "_scan_pipeline")
    assert not hasattr(embedded, "_scan_pipeline")
    assert strict._scan_state is shared
    assert embedded._scan_state is shared
    assert app.state.v20_service._scan_state is app.state.v15_scan_state
    assert not hasattr(app.state.v20_service, "_scan_pipeline")


@pytest.mark.asyncio
async def test_all_v20_trigger_modes_reuse_canonical_raw_0939(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _canonical_fixture(monkeypatch)
    today = datetime.now(BEIJING_TZ).date()
    pre_cutoff = datetime.combine(today, time(9, 39, 30), TZ)
    post_cutoff = datetime.combine(today, time(9, 40, 1), TZ)
    prior_trade_date = today - timedelta(days=1)
    expected_symbols = [
        {
            "rank": index + 1,
            "code": code,
            "name": f"fresh-{code}",
            "score": 0.9 - index * 0.01,
            "snapshot_price": 10.0 + index,
            "boards": ["board-a"],
            "best_board": "board-a",
            "is_driver": True,
            "cci": 50.0,
            "volume_937": 1000.0,
            "history_hash": sha256_json(
                {
                    "close": [10.0],
                    "high": [10.5],
                    "low": [9.5],
                    "open": [10.0],
                    "time": [prior_trade_date.isoformat()],
                    "volume": [1000.0],
                }
            ),
        }
        for index, code in enumerate(FRESH_CODES)
    ]

    class Repository:
        def __init__(self) -> None:
            self.events: dict[str, OutboxRecord] = {}
            self.raw_reads: list[tuple[tuple[str, ...], date]] = []
            self.raw_by_key: dict[tuple[str, str], Any] = {}
            self.persist_calls: list[tuple[Mapping[str, Any], ...]] = []

        async def assert_runtime_leader(self) -> None:
            return None

        async def get_entry_status(self, _stream: str, trade_date: date) -> Any:
            if trade_date != today:
                return None
            return status if service._aware_now() >= post_cutoff else None

        async def get_outbox_event(self, event_id: str, **_kwargs: Any) -> Any:
            return self.events.get(event_id)

        async def load_state(self, _lineage: str) -> StateRecord:
            return failed_state

        async def list_raw_minute_bar_records(
            self,
            codes: Any,
            *,
            trade_date: date,
            end_labels: Any,
            received_before: datetime | None = None,
        ) -> list[Any]:
            self.raw_reads.append((tuple(codes), trade_date))
            allowed_codes = set(codes)
            allowed_labels = set(end_labels)
            return [
                SimpleNamespace(
                    code=code,
                    bar_end=datetime.fromisoformat(str(record["payload"]["bar_end"])),
                    end_label=label,
                    source_hash=sha256_json(record["payload"]),
                    payload=record["payload"],
                    first_received_at=record["first_received_at"],
                )
                for (code, label), record in self.raw_by_key.items()
                if code in allowed_codes and label in allowed_labels
            ]

        async def record_minute_bars(self, payloads: list[Mapping[str, Any]]) -> frozenset[str]:
            normalized = tuple(payloads)
            self.persist_calls.append(normalized)
            sealed = set()
            for payload in normalized:
                key = (str(payload["stock_code"]), str(payload["end_label"]))
                self.raw_by_key[key] = {
                    "payload": dict(payload),
                    "first_received_at": pre_cutoff,
                }
                sealed.add(sha256_json(payload))
            return frozenset(sealed)

        async def enqueue_alert(
            self,
            event_id: str,
            route_id: str,
            semantic: Mapping[str, Any],
            semantic_hash: str,
            **scope: Any,
        ) -> bool:
            self.events[event_id] = OutboxRecord(
                event_id=event_id,
                event_type="DATA_ALERT",
                route_id=route_id,
                official_stream_id=scope["official_stream_id"],
                lineage_id=scope["lineage_id"],
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

        async def seal_event(self, event_id: str, formatter: Any) -> OutboxRecord:
            current = self.events[event_id]
            payload = dict(formatter(current, post_cutoff, 100, True))
            sealed = dataclasses_replace(
                current,
                payload=payload,
                payload_hash=sha256_json(payload),
                generated_at=post_cutoff,
                commit_marker=100,
            )
            self.events[event_id] = sealed
            return sealed

    repository = Repository()
    service = _v20_service(monkeypatch, scan_state=state)
    service._repository = repository
    service._started = True
    service._repository_started = True
    service._clock = lambda: pre_cutoff

    before_payload = genesis_state()
    before_hash = sha256_json(before_payload)
    failure_gap_id = named_hash(
        "V20_OFFICIAL_SHADOW_GAP_ID_V1",
        {"official_stream_id": service.config.official_stream_id, "trade_date": today.isoformat()},
    )
    after_payload = {
        **before_payload,
        "state_revision": 1,
        "official_rolling_gaps": [
            {
                "gap_id": failure_gap_id,
                "signal_date": prior_trade_date.isoformat(),
                "maturity_date": (today + timedelta(days=2)).isoformat(),
                "closed": False,
                "aged_out": False,
            }
        ],
        "last_terminal_slot_id": "failed-slot",
        "last_terminal_trade_date": today.isoformat(),
    }
    after_hash = sha256_json(after_payload)
    failed_state = StateRecord(
        lineage_id=service.config.state_lineage_id,
        revision=1,
        state_hash=after_hash,
        payload=after_payload,
    )
    policy_inputs = {
        "schema_version": "v20-policy-input-snapshot/v1",
        "completed_health": [],
        "completed_rolling": [],
        "maturity_gaps": [],
    }
    policy_hash = sha256_json(policy_inputs)
    failed_semantic = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "strategy_version": service.config.strategy_version,
        "config_hash": service.config.config_hash,
        "state_semantics_hash": service.config.state_semantics_hash,
        "action": "INPUT_INVALID",
        "state_before_hash": before_hash,
        "state_after_hash": after_hash,
        "policy_input_hash": policy_hash,
        "scheduled_exits_today": [],
        "symbols": expected_symbols,
    }
    failed_snapshot = {
        "schema_version": V20_INVALID_INPUT_SNAPSHOT_SCHEMA,
        "trade_date": today.isoformat(),
        "state_before_hash": before_hash,
        "state_semantics_hash": service.config.state_semantics_hash,
        "policy_input_hash": policy_hash,
        "policy_inputs": policy_inputs,
    }
    status = EntryStatus(
        official_stream_id=service.config.official_stream_id,
        trade_date=today,
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
        semantic_content_hash=sha256_json(failed_semantic),
        semantic=failed_semantic,
        snapshot_id="failed-snapshot",
        snapshot_hash=sha256_json(failed_snapshot),
        snapshot=failed_snapshot,
        action_expiry_ts=post_cutoff,
    )
    repository.events["failed-entry-event"] = OutboxRecord(
        event_id="failed-entry-event",
        event_type="ENTRY_DECISION",
        route_id=service.config.route_id,
        official_stream_id=service.config.official_stream_id,
        lineage_id=service.config.state_lineage_id,
        semantic=failed_semantic,
        semantic_content_hash=sha256_json(failed_semantic),
        payload={"message": "failed official entry"},
        payload_hash=sha256_json({"message": "failed official entry"}),
        generated_at=pre_cutoff,
        commit_marker=1,
        action_expiry_ts=None,
        delivery_status="SENT",
        attempt_count=1,
    )

    service_calendar = (
        prior_trade_date,
        today,
        today + timedelta(days=1),
        today + timedelta(days=2),
    )

    async def service_calendar_provider() -> list[date]:
        return list(service_calendar)

    async def no_existing_entry(target: _DayContext) -> None:
        target.entry_status = None

    monkeypatch.setattr(service, "_refresh_entry_status", no_existing_entry)

    async def ready() -> None:
        return None

    async def mews_ready(_now: datetime) -> bool:
        return True

    async def manual_decision(current: datetime) -> None:
        context = _DayContext(trade_date=today, calendar=service_calendar)
        await service._run_entry_collection_cycle(context, current)
        context.entry_status = status
        service._context = context

    service._calendar_provider = service_calendar_provider
    service.config = dataclasses_replace(service.config, enabled=True)
    monkeypatch.setattr(service, "_require_manual_trigger_ready", ready)
    monkeypatch.setattr(service, "ensure_mews_for_selection_trigger", mews_ready)
    monkeypatch.setattr(service, "_verify_entry_binding", lambda _status: None)
    monkeypatch.setattr(service, "_run_decision_iteration_with_cutoff", manual_decision)

    release = asyncio.Event()
    scanner_entered = asyncio.Event()
    original_scan = FakeV16Scanner.scan

    async def gated_scan(self: FakeV16Scanner, stock_data: Any, boards: Any) -> Any:
        scanner_entered.set()
        await release.wait()
        return await original_scan(self, stock_data, boards)

    monkeypatch.setattr(FakeV16Scanner, "scan", gated_scan)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return pre_cutoff if tz is BEIJING_TZ or tz is TZ else datetime.now(tz)

    monkeypatch.setattr(v15_scan_service, "datetime", FixedDateTime)

    scheduler_task = asyncio.create_task(
        v15_scan_service._scan_scheduler(state),
        name="real-v16-scheduler",
    )
    automatic_context = _DayContext(trade_date=today, calendar=service_calendar)

    async def automatic_entry_with_production_persist() -> None:
        await service._run_entry_collection_cycle(automatic_context, pre_cutoff)
        return automatic_context

    automatic_task = asyncio.create_task(
        automatic_entry_with_production_persist(),
        name="v20-automatic",
    )
    cancelled_waiter = asyncio.create_task(
        get_or_compute_canonical_v16(state, today),
        name="canonical-cancelled-waiter",
    )

    for _ in range(100):
        await asyncio.sleep(0)
        coordinator = state.canonical_coordinator
        if coordinator is not None and coordinator.inflight.get(today) and scanner_entered.is_set():
            break
    else:
        raise AssertionError("concurrent paths did not create one canonical scan task")
    master_task = state.canonical_coordinator.inflight[today]
    cancelled_waiter.cancel()
    await asyncio.gather(cancelled_waiter, return_exceptions=True)
    assert not master_task.cancelled()

    release.set()

    automatic_result = await automatic_task
    # Manual pre/post-cutoff triggers share this same service and scan state,
    # in order; only a restart would build a new state.
    manual_pre_result = await _dispatch_manual_trigger(service, "modes-manual-pre")
    service._clock = lambda: post_cutoff
    check_only_result = await _dispatch_manual_trigger(service, "modes-check-only")
    for _ in range(100):
        if state.scan_done_date == today.isoformat():
            break
        await asyncio.sleep(0)
    scheduler_task.cancel()
    await asyncio.gather(scheduler_task, return_exceptions=True)

    master = state.canonical_coordinator.cache[today]
    assert automatic_result.last_phase == "CANONICAL_0939_READY"
    assert manual_pre_result["accepted"] is True
    assert check_only_result["accepted"] is True
    assert check_only_result["current_version_recomputed"] is True
    assert check_only_result["replay_reused"] is False
    canonical_symbols = [
        {"rank": item.rank, "code": item.code, "name": item.name}
        for item in master.scan_result.recommended
    ]
    assert [item["code"] for item in canonical_symbols] == list(FRESH_CODES)
    assert [item["code"] for item in automatic_result.canonical_bundle.snapshot["symbols"]] == list(
        FRESH_CODES
    )
    assert [item["code"] for item in service._context.canonical_bundle.snapshot["symbols"]] == list(
        FRESH_CODES
    )
    assert [item["code"] for item in check_only_result["symbols"]] == list(FRESH_CODES)
    assert FakeV16Scanner.scan_calls == 1
    assert FakeRealtimeClient.early_calls == 1
    assert FakeHistoryAdapter.history_calls == 1
    assert len(repository.persist_calls) == 1
    persisted_payloads = repository.persist_calls[0]
    assert len(persisted_payloads) == len(FRESH_CODES) * 9
    assert {(str(bar["stock_code"]), str(bar["end_label"])) for bar in persisted_payloads} == {
        (code, f"09:{minute:02d}") for code in FRESH_CODES for minute in range(31, 40)
    }
    assert all("09:41" != bar["end_label"] for bar in persisted_payloads)
    assert all(item["payload"]["open"] != 999.0 for item in repository.raw_by_key.values())
    expected_selection_sources = {
        code: sha256_json(
            [
                _bar_payload(next(bar for bar in bars if bar.end_label == f"09:{minute:02d}"))
                for minute in range(31, 40)
            ]
        )
        for code, bars in master.early_bars.items()
    }
    assert master.early_source_hashes == expected_selection_sources
    assert all(item["snapshot_price"] != 999.0 for item in check_only_result["symbols"])
    assert state.canonical_coordinator.inflight == {}
    assert all(
        result.snapshot_hash == automatic_result.canonical_bundle.snapshot_hash
        for result in (
            automatic_result.canonical_bundle,
            service._context.canonical_bundle,
        )
    )
    assert repository.raw_reads


@pytest.mark.asyncio
async def test_canonical_scan_singleflight_and_owner_error_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _canonical_fixture(monkeypatch)
    today = datetime.now(BEIJING_TZ).date()
    release = asyncio.Event()
    original_scan = FakeV16Scanner.scan

    async def gated_scan(self: FakeV16Scanner, stock_data: Any, boards: Any) -> Any:
        await release.wait()
        return await original_scan(self, stock_data, boards)

    monkeypatch.setattr(FakeV16Scanner, "scan", gated_scan)
    callers = [asyncio.create_task(get_or_compute_canonical_v16(state, today)) for _ in range(36)]
    await asyncio.sleep(0)
    borrowers = callers[:20]
    for task in borrowers:
        task.cancel()
    await asyncio.gather(*borrowers, return_exceptions=True)
    release.set()
    results = await asyncio.gather(*callers[20:])

    assert FakeV16Scanner.scan_calls == 1
    assert all(result.input_hash == results[0].input_hash for result in results)

    coord = state.canonical_coordinator
    assert coord is not None
    coord.cache.clear()
    FakeV16Scanner.scan_calls = 0

    async def fail_once(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("owner boom")

    monkeypatch.setattr(FakeV16Scanner, "scan", fail_once)
    with pytest.raises(RuntimeError, match="owner boom"):
        await get_or_compute_canonical_v16(state, today)
    monkeypatch.setattr(FakeV16Scanner, "scan", original_scan)
    recovered = await get_or_compute_canonical_v16(state, today)
    assert recovered.input_hash == results[0].input_hash


@pytest.mark.asyncio
async def test_canonical_v20_snapshot_is_lossless_and_stable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.web import v20_service as service_module

    assert not hasattr(service_module, "V20ScanPipeline")
    monkeypatch.setattr("src.strategy.strategies.v16_scanner.V16Scanner", Bomb)
    monkeypatch.setattr("src.data.clients.tushare_realtime.TushareRealtimeClient", Bomb)
    monkeypatch.setattr(
        "src.data.clients.iquant_historical_adapter.IQuantHistoricalAdapter",
        Bomb,
    )
    service = _v20_service(monkeypatch)
    _assert_no_legacy_scan_pipeline_construction()
    service._scan_state.realtime_client = Bomb()
    service._scan_state.historical_adapter = Bomb()

    trade_date = date(2026, 9, 1)
    computed_at = datetime(2026, 9, 1, 9, 39, 59, tzinfo=TZ)
    first = ScoredStock(
        code="603068",
        name="酒钢宏兴",
        score=0.987654321,
        rank=1,
        buy_price=2.345678,
    )
    second = ScoredStock(
        code="605299",
        name="艾艾精工",
        score=0.876543210,
        rank=2,
        buy_price=12.678901,
    )
    scan_result = V16ScanResult(
        recommended=[first, second],
        all_scored=[first, second],
        step0_universe_count=5000,
        step2_hot_board_count=7,
        step2_filtered_by_avg_gain=11,
        step3_count=222,
        step4_count=111,
        step5_count=55,
        step6_count=33,
        step6_5_count=22,
        step6_6_count=10,
        final_candidates=2,
        step0_codes=["000001", "603068", "605299"],
        step2_boards_detail={"board-b": ["603068"], "board-a": ["605299"]},
        step2_codes=["603068", "605299"],
        st_eligible_codes=["603068", "605299"],
        step3_codes=["603068", "605299"],
        step4_codes=["603068", "605299"],
        step5_codes=["603068", "605299"],
        step6_codes=["603068", "605299"],
        step6_5_codes=["603068", "605299"],
        step6_6_codes=["603068", "605299"],
        stock_best_board={"603068": "board-b", "605299": "board-a"},
        stock_all_boards={"603068": ["board-b", "board-a"], "605299": ["board-a"]},
        step2_board_avg_gains={"board-a": 1.25, "board-b": 2.5},
        stock_gain_from_open={"603068": 3.5, "605299": 1.2},
        stock_is_driver={"603068": True, "605299": False},
        stock_cci={"603068": 88.5, "605299": None},
        stock_early_vol={"603068": 12345.0, "605299": None},
    )
    history_dates = [(date(2026, 7, 10) + timedelta(days=index)).isoformat() for index in range(37)]
    histories = {
        code: {
            "time": history_dates,
            "open": [10.0] * 37,
            "high": [10.5] * 37,
            "low": [9.5] * 37,
            "close": [10.0 + index * 0.01 for index in range(37)],
            "volume": [1000.0] * 37,
        }
        for code in ("603068", "605299")
    }
    history_df = pd.DataFrame(histories["603068"])
    early_bars = {
        code: (
            TushareMinuteBar(
                stock_code=code,
                bar_end=datetime(2026, 9, 1, 9, 39, tzinfo=TZ),
                end_label="09:39",
                open_price=10.0,
                close_price=10.2,
                high_price=10.3,
                low_price=9.9,
                volume=1000.0,
                amount=10000.0,
            ),
        )
        for code in ("603068", "605299")
    }
    quotes = {
        code: TushareQuote(
            stock_code=code,
            open_price=10.0,
            latest_price=10.2,
            high_price=10.3,
            low_price=9.9,
            volume=1000.0,
            amount=10000.0,
            early_close=10.2,
            early_high=10.3,
            early_low=9.9,
            early_volume=1000.0,
            volume_937=500.0,
        )
        for code in ("603068", "605299")
    }
    canonical = CanonicalV16ScanBundle(
        trade_date=trade_date,
        scan_result=scan_result,
        stock_data={
            "603068": V16StockData(
                code="603068",
                name=first.name,
                open_price=10.0,
                prev_close=9.9,
                price_940=first.buy_price,
                high_940=10.3,
                low_940=9.9,
                volume_940=1000.0,
                volume_937=12345.0,
                avg_daily_volume=1000.0,
                trend_5d=0.05,
                trend_10d=0.1,
                avg_daily_return_20d=0.001,
                volatility_20d=0.02,
                consecutive_up_days=2,
                history_df=history_df.copy(deep=True),
            ),
            "605299": V16StockData(
                code="605299",
                name=second.name,
                open_price=10.0,
                prev_close=9.9,
                price_940=second.buy_price,
                high_940=10.3,
                low_940=9.9,
                volume_940=1000.0,
                volume_937=None,
                avg_daily_volume=1000.0,
                trend_5d=0.05,
                trend_10d=0.1,
                avg_daily_return_20d=0.001,
                volatility_20d=0.02,
                consecutive_up_days=2,
                history_df=history_df.copy(deep=True),
            ),
        },
        clean_boards={
            "board-a": [("605299", second.name)],
            "board-b": [("603068", first.name)],
        },
        universe=("603068", "605299", "000001"),
        quotes=quotes,
        prev_closes={"603068": 9.9, "605299": 9.9},
        history_raw=histories,
        early_bars=early_bars,
        early_source_hashes={
            "603068": "a" * 64,
            "605299": "b" * 64,
        },
        failed_no_prev_close=("000001",),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="m" * 64,
        feature_list_sha256="f" * 64,
        computed_at=computed_at,
        input_hash="i" * 64,
        _integrity_hash="c" * 64,
    )

    bundle = service._project_canonical_v16(canonical, calendar=(date(2026, 8, 31), trade_date))
    symbols = bundle.snapshot["symbols"]
    assert [(item["rank"], item["code"], item["name"]) for item in symbols] == [
        (1, "603068", "酒钢宏兴"),
        (2, "605299", "艾艾精工"),
    ]
    assert symbols[0]["score"] == 0.987654321
    assert symbols[1]["score"] == 0.876543210
    assert [item["snapshot_price"] for item in symbols] == [2.345678, 12.678901]
    assert [item["history_hash"] for item in symbols] == [
        _stable_hash(histories["603068"]),
        _stable_hash(histories["605299"]),
    ]
    assert [item["early_source_hash"] for item in symbols] == ["a" * 64, "b" * 64]
    assert symbols[0]["boards"] == ["board-b", "board-a"]
    assert symbols[0]["best_board"] == "board-b"
    assert symbols[0]["cci"] == 88.5
    assert symbols[0]["volume_937"] == 12345.0
    assert symbols[1]["boards"] == ["board-a"]
    assert symbols[1]["best_board"] == "board-a"
    assert symbols[1]["is_driver"] is False
    assert symbols[1]["cci"] is None
    assert symbols[1]["volume_937"] is None
    assert bundle.snapshot["funnel"] == {
        "step0_universe_count": 5000,
        "step2_hot_board_count": 7,
        "step2_filtered_by_avg_gain": 11,
        "step3_count": 222,
        "step4_count": 111,
        "step5_count": 55,
        "step6_count": 33,
        "step6_5_count": 22,
        "step6_6_count": 10,
        "final_candidates": 2,
    }
    assert bundle.snapshot["stages"] == {
        "step0_codes": ["000001", "603068", "605299"],
        "step2_boards_detail": {"board-b": ["603068"], "board-a": ["605299"]},
        "step2_codes": ["603068", "605299"],
        "st_eligible_codes": ["603068", "605299"],
        "step3_codes": ["603068", "605299"],
        "step4_codes": ["603068", "605299"],
        "step5_codes": ["603068", "605299"],
        "step6_codes": ["603068", "605299"],
        "step6_5_codes": ["603068", "605299"],
        "step6_6_codes": ["603068", "605299"],
    }
    assert bundle.snapshot["board_avg_gains"] == {"board-a": 1.25, "board-b": 2.5}
    assert bundle.snapshot["scan_input_failure_codes"] == ["000001"]
    assert bundle.snapshot_hash == _stable_hash(bundle.snapshot)

    rerun = service._project_canonical_v16(canonical, calendar=(date(2026, 8, 31), trade_date))
    assert rerun.snapshot == bundle.snapshot
    assert rerun.snapshot_hash == bundle.snapshot_hash

    _validate_v16_snapshot_formatter_evidence(bundle.snapshot)
    state_payload = genesis_state()
    state = StateRecord(
        lineage_id=service.config.state_lineage_id,
        revision=0,
        state_hash=sha256_json(state_payload),
        payload=state_payload,
    )
    prepared = prepare_entry(
        config=service.config,
        state=state,
        bundle=bundle,
        completed_health=(),
        completed_rolling=(),
        maturity_gaps=(),
        artifacts=service._artifacts,
        calendar=(date(2026, 8, 31), trade_date, date(2026, 9, 2), date(2026, 9, 3)),
    )
    assert prepared.commit.semantic["symbols"] == bundle.snapshot["symbols"]
    second_prepared = prepared.commit.semantic["symbols"][1]
    assert second_prepared["cci"] is None
    assert second_prepared["volume_937"] is None


@pytest.mark.asyncio
async def test_calendar_singleflight_shields_waiter_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _v20_service(monkeypatch)
    calls = 0
    release = asyncio.Event()

    async def calendar() -> tuple[date, ...]:
        nonlocal calls
        calls += 1
        await release.wait()
        today = datetime.now(TZ).date()
        return (
            today - timedelta(days=1),
            today,
            today + timedelta(days=1),
            today + timedelta(days=2),
        )

    service._calendar_provider = calendar
    today = datetime.now(TZ).date()
    owner = asyncio.create_task(service._load_trade_calendar(today))
    waiter = asyncio.create_task(service._load_trade_calendar(today))
    await asyncio.sleep(0)
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)
    assert not owner.cancelled()
    release.set()
    assert await owner == await service._load_trade_calendar(today)
    assert calls == 1


@pytest.mark.asyncio
async def test_post_cutoff_manual_selection_has_mews_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today = date(2026, 9, 1)
    now = datetime(2026, 9, 1, 9, 40, 1, tzinfo=TZ)
    state_payload = genesis_state()
    state = StateRecord(
        lineage_id="lineage",
        revision=0,
        state_hash=sha256_json(state_payload),
        payload=state_payload,
    )
    terminal = SimpleNamespace(
        action="INPUT_INVALID",
        trade_date=today,
        event_id="failed-entry-event",
        semantic={"state_after_hash": state.state_hash},
    )

    class Repository:
        def __init__(self) -> None:
            self.events: dict[str, OutboxRecord] = {}

        async def assert_runtime_leader(self) -> None:
            return None

        async def load_state(self, lineage_id: str) -> StateRecord:
            return state

        async def get_entry_status(self, _stream: str, trade_date: date) -> Any:
            return terminal if trade_date == today else None

        async def get_outbox_event(self, event_id: str, **_kwargs: Any) -> Any:
            return self.events.get(event_id)

        async def get_outbox_health(self, **_kwargs: Any) -> dict[str, int]:
            return {
                "unsealed_n": 0,
                "pending_delivery_n": 0,
                "leased_n": 0,
            }

        async def find_eligible_mews_snapshot(self, **_kwargs: Any) -> None:
            return None

        async def enqueue_alert(
            self,
            event_id: str,
            route_id: str,
            semantic: Mapping[str, Any],
            semantic_hash: str,
            **scope: Any,
        ) -> bool:
            assert route_id == service.config.route_id
            assert scope == {
                "official_stream_id": service.config.official_stream_id,
                "lineage_id": service.config.state_lineage_id,
            }
            assert semantic_hash == sha256_json(semantic)
            self.events[event_id] = OutboxRecord(
                event_id=event_id,
                event_type="DATA_ALERT",
                route_id=route_id,
                official_stream_id=scope["official_stream_id"],
                lineage_id=scope["lineage_id"],
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

        async def seal_event(self, event_id: str, formatter: Any) -> OutboxRecord:
            current = self.events[event_id]
            payload = dict(formatter(current, now, 100, True))
            sealed = dataclasses_replace(
                current,
                payload=payload,
                payload_hash=sha256_json(payload),
                generated_at=now,
                commit_marker=100,
            )
            self.events[event_id] = sealed
            return sealed

        async def close(self) -> None:
            return None

    class HangingMewsSource:
        def __init__(self) -> None:
            self.calls = 0
            self.entered = asyncio.Event()

        async def fetch_snapshot(self, **_kwargs: Any) -> dict[str, Any]:
            self.calls += 1
            self.entered.set()
            await release.wait()
            raise RuntimeError("local MEWS calculator failed")

    release = asyncio.Event()
    repository = Repository()
    service = _v20_service(monkeypatch, repository=repository)
    service.config = dataclasses_replace(service.config, enabled=True)
    service._repository_started = True
    service._started = True
    service._clock = lambda: now

    async def calendar() -> list[date]:
        return [date(2026, 8, 31), today, date(2026, 9, 2), date(2026, 9, 3)]

    async def ready() -> None:
        return None

    symbols = [
        {
            "rank": 1,
            "code": "603068",
            "name": "fresh-name",
            "score": 0.9,
            "snapshot_price": 10.0,
            "boards": ["fresh-board"],
            "best_board": "fresh-board",
            "is_driver": True,
            "cci": 50.0,
            "volume_937": 1000.0,
            "history_hash": "0" * 64,
            "early_source_hash": "1" * 64,
        }
    ]
    entry_render = {
        "schema_version": V20_ENTRY_SEMANTIC_SCHEMA,
        "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
        "event_id": "hypothetical-entry-event",
        "deployment_mode": service.config.deployment_mode,
        "trade_date": today.isoformat(),
        "action": "ENTER",
        "final_multiplier": 1.0,
        "base_multiplier": 1.0,
        "defense_multiplier": 1.0,
        "health_state": "WARMUP",
        "rolling7_state": "NON_BAD",
        "rolling7_r7": 0.0,
        "rolling7_l7": 0,
        "g_state": "NOT_EVALUATED",
        "reason_codes": [],
        "last_complete_bar": "09:39",
        "v16_funnel": {
            "step0_universe_count": 1,
            "step2_hot_board_count": 1,
            "final_candidates": 1,
        },
        "v16_board_avg_gains": {"fresh-board": 1.0},
        "symbols": symbols,
        "scheduled_exits_today": [],
    }

    async def fresh_selection_result(
        context: Any,
        current: datetime,
        *,
        replay_event_id: str,
    ) -> Mapping[str, Any]:
        return {
            "schema_version": V20_DATA_ALERT_SEMANTIC_SCHEMA,
            "feishu_formatter_profile": V20_FEISHU_FORMATTER_PROFILE,
            "event_id": replay_event_id,
            "strategy_version": service.config.strategy_version,
            "config_hash": service.config.config_hash,
            "state_semantics_hash": service.config.state_semantics_hash,
            "deployment_mode": service.config.deployment_mode,
            "official_stream_id": service.config.official_stream_id,
            "state_lineage_id": service.config.state_lineage_id,
            "event_trade_date": context.trade_date.isoformat(),
            "official_entry_action": context.entry_status.action,
            "official_entry_event_id": context.entry_status.event_id,
            "replay_action": "ENTER",
            "final_multiplier": 1.0,
            "symbols": symbols,
            "entry_render_semantic": entry_render,
            "raw_fact_n": 9,
            "quote_coverage": 1.0,
            "computed_at": current.isoformat(),
        }

    service._calendar_provider = calendar
    monkeypatch.setattr(service, "_require_manual_trigger_ready", ready)
    monkeypatch.setattr(service, "_verify_entry_binding", lambda _status: None)
    monkeypatch.setattr(
        service,
        "_build_late_0939_replay_semantic",
        fresh_selection_result,
    )

    selection_source = HangingMewsSource()
    service._mews_source = selection_source
    result = await asyncio.wait_for(
        _dispatch_manual_trigger(service, "mews-budget-selection"),
        timeout=0.25,
    )
    assert result["accepted"] is True
    assert result["current_version_recomputed"] is True
    assert result["replay_reused"] is False
    selection_task = service._mews_selection_refresh_task
    assert selection_task is not None
    assert not selection_task.done()
    await asyncio.wait_for(selection_source.entered.wait(), timeout=0.25)

    await service.stop()
    assert selection_task.cancelled()
    assert service._mews_selection_refresh_task is None
    assert selection_source.calls == 1

    service._started = True
    service._repository_started = True
    service._stop_event.clear()
    service._mews_selection_refresh_failed_for = None
    startup_source = HangingMewsSource()
    service._mews_source = startup_source
    startup_calls_before = startup_source.calls
    assert await service.ensure_mews_for_selection_trigger(now) is False
    startup_task = service._mews_selection_refresh_task
    assert startup_task is not None
    await asyncio.wait_for(startup_source.entered.wait(), timeout=0.25)
    service._started = False
    with pytest.raises(RuntimeError, match="V20 Feishu route .* is not configured"):
        await service.start()
    assert startup_task.cancelled()
    assert service._mews_selection_refresh_task is None
    assert startup_source.calls - startup_calls_before == 1

    service._started = True
    service._repository_started = True
    service._stop_event.clear()
    service._mews_selection_refresh_failed_for = None
    failure_source = HangingMewsSource()
    service._mews_source = failure_source
    failure_calls_before = failure_source.calls
    assert await service.ensure_mews_for_selection_trigger(now) is False
    failure_task = service._mews_selection_refresh_task
    assert failure_task is not None
    await asyncio.wait_for(failure_source.entered.wait(), timeout=0.25)
    release.set()
    assert await failure_task is False
    assert service._mews_selection_refresh_failed_for == today
    assert failure_source.calls - failure_calls_before == 1
    assert service._lane_health["mews_cache"].last_error is not None
    assert service._lane_health["decision"].last_error is None

    readiness_tasks = [asyncio.create_task(asyncio.Event().wait()) for _ in range(6)]
    service._tasks = readiness_tasks
    service._record_lane_success("decision", now)
    await service._refresh_status_snapshot()
    monkeypatch.setattr(
        service,
        "_require_manual_trigger_ready",
        V20Service._require_manual_trigger_ready.__get__(service, V20Service),
    )
    await service._require_manual_trigger_ready()
    for task in readiness_tasks:
        task.cancel()
    await asyncio.gather(*readiness_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_shared_state_resource_ownership_is_singleflight_and_cleanup_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module
    from src.data.database import fundamentals_db as fundamentals_module
    from src.data.database import v15_scan_db as v15_scan_db_module
    from src.data.sources import local_concept_mapper as concept_module
    from src.strategy.filters import stock_filter as stock_filter_module

    monkeypatch.setenv("TUSHARE_TOKEN", "shared-state-token")
    created = 0
    started = 0
    stopped = 0
    fail_next_start = True
    start_gate = asyncio.Event()
    all_waiters_seen = asyncio.Event()
    waiter_count = 0

    class _Realtime:
        def __init__(self, *, token: str) -> None:
            assert token == "shared-state-token"
            nonlocal created
            created += 1

        async def start(self) -> None:
            nonlocal fail_next_start, started, waiter_count
            started += 1
            if fail_next_start:
                fail_next_start = False
                raise RuntimeError("first V20 market-client start fails")
            waiter_count += 1
            if waiter_count == 1:
                all_waiters_seen.set()
            await start_gate.wait()

        async def stop(self) -> None:
            nonlocal stopped
            stopped += 1

    class _Fundamentals:
        def __init__(self) -> None:
            self.connect_calls = 0
            self.close_calls = 0
            self.closed = False

        async def connect(self) -> None:
            assert self.closed is False
            self.connect_calls += 1

        async def close(self) -> None:
            assert self.closed is False
            self.close_calls += 1
            self.closed = True

    class _Historical:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

    class _ScanDB:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

    fundamentals = _Fundamentals()
    fundamentals_factory_calls = 0
    factory_fundamentals: list[_Fundamentals] = []

    def new_fundamentals() -> _Fundamentals:
        nonlocal fundamentals_factory_calls
        fundamentals_factory_calls += 1
        created_db = _Fundamentals()
        factory_fundamentals.append(created_db)
        return created_db

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", _Realtime)
    monkeypatch.setattr(historical_module, "IQuantHistoricalAdapter", _Historical)
    monkeypatch.setattr(concept_module, "LocalConceptMapper", lambda: object())
    monkeypatch.setattr(stock_filter_module, "StockFilter", lambda _config: object())
    monkeypatch.setattr(
        fundamentals_module,
        "create_fundamentals_db_from_config",
        new_fundamentals,
    )
    monkeypatch.setattr(v15_scan_db_module, "create_v15_scan_db_from_config", lambda: _ScanDB())
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "shared-state-token")

    state = V15ScanState(fundamentals_db=fundamentals)
    assert state.initialized is False
    assert state.resource_owner is None
    with pytest.raises(RuntimeError, match="first V20 market-client start fails"):
        await _init_embedded_v20_scan_resources(state)
    assert fundamentals_factory_calls == 0
    assert factory_fundamentals == []
    assert state.fundamentals_db is fundamentals
    assert fundamentals.connect_calls == 0
    assert state.initialized is False
    assert state.resource_owner is None
    assert (created, started, stopped) == (1, 1, 1)
    assert state.realtime_client is None
    assert state.initialized is False

    takeover_tasks = [
        asyncio.create_task(v15_scan_service.init_scan_resources(state)) for _ in range(5)
    ]
    for _ in range(100):
        await asyncio.sleep(0)
        if all_waiters_seen.is_set() or any(task.done() for task in takeover_tasks):
            break
    for waiter in takeover_tasks[1:]:
        waiter.cancel()
    await asyncio.gather(*takeover_tasks[1:], return_exceptions=True)
    start_gate.set()
    takeover_result = await takeover_tasks[0]
    assert not isinstance(takeover_result, BaseException), str(takeover_result)
    assert (created, started) == (2, 2)
    assert fundamentals_factory_calls == 0
    assert factory_fundamentals == []
    assert state.fundamentals_db is fundamentals
    assert fundamentals.connect_calls == 0
    assert all(item.connect_calls == 0 for item in factory_fundamentals)

    await _init_embedded_v20_scan_resources(state)
    await v15_scan_service.init_scan_resources(state)
    assert (created, started) == (2, 2)
    assert fundamentals_factory_calls == 0
    assert state.fundamentals_db is fundamentals
    assert fundamentals.connect_calls == 0

    scheduler_cancelled = asyncio.Event()

    async def scheduler() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            scheduler_cancelled.set()
            raise

    state.scheduler_task = asyncio.create_task(scheduler(), name="v16-scheduler-acceptance")
    captured_scheduler = state.scheduler_task
    await v15_scan_service.cleanup_scan_resources(state)
    assert captured_scheduler.done()
    assert state.scheduler_task is None
    assert scheduler_cancelled.is_set()
    assert stopped == 2
    assert state.initialized is False
    assert fundamentals.closed is True

    start_gate = asyncio.Event()
    waiter_count = 0
    all_waiters_seen.clear()
    restart_owner = asyncio.create_task(v15_scan_service.init_scan_resources(state))
    restart_waiters = [
        asyncio.create_task(v15_scan_service.init_scan_resources(state)) for _ in range(4)
    ]
    for _ in range(100):
        await asyncio.sleep(0)
        if (
            all_waiters_seen.is_set()
            or restart_owner.done()
            or any(task.done() for task in restart_waiters)
        ):
            break
    for waiter in restart_waiters:
        waiter.cancel()
    await asyncio.gather(*restart_waiters, return_exceptions=True)
    start_gate.set()
    restart_result = await restart_owner
    assert not isinstance(restart_result, BaseException), str(restart_result)
    assert (created, started) == (3, 3)
    restarted_fundamentals = state.fundamentals_db

    await v15_scan_service.cleanup_scan_resources(state)
    assert stopped == 3
    assert state.scheduler_task is None
    assert fundamentals_factory_calls == 1
    assert len(factory_fundamentals) == 1
    assert state.fundamentals_db is not fundamentals
    assert restarted_fundamentals is factory_fundamentals[0]
    assert factory_fundamentals[0].connect_calls == 1
    assert factory_fundamentals[0].closed is True


@pytest.mark.parametrize("failure", ["rt-start", "historical", "mapper", "filter"])
@pytest.mark.asyncio
async def test_v16_initializer_dependency_failure_preserves_shared_pool(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    from src.common import config as common_config
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module
    from src.data.sources import local_concept_mapper as concept_module
    from src.strategy.filters import stock_filter as stock_filter_module

    monkeypatch.setenv("TUSHARE_TOKEN", "rollback-token")
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "rollback-token")

    class Realtime:
        instances: list[Realtime] = []

        def __init__(self, *, token: str) -> None:
            assert token == "rollback-token"
            self.stop_calls = 0
            Realtime.instances.append(self)

        async def start(self) -> None:
            if failure == "rt-start":
                raise RuntimeError("RT start failed")

        async def stop(self) -> None:
            self.stop_calls += 1

    class Historical:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            if failure == "historical":
                raise RuntimeError("historical adapter failed")

    def mapper() -> Any:
        if failure == "mapper":
            raise RuntimeError("concept mapper failed")
        return object()

    def stock_filter(_config: Any) -> Any:
        if failure == "filter":
            raise RuntimeError("stock filter failed")
        return object()

    class SharedDB:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", Realtime)
    monkeypatch.setattr(historical_module, "IQuantHistoricalAdapter", Historical)
    monkeypatch.setattr(concept_module, "LocalConceptMapper", mapper)
    monkeypatch.setattr(stock_filter_module, "StockFilter", stock_filter)

    fundamentals = SharedDB()
    scan_db = SharedDB()
    state = V15ScanState(fundamentals_db=fundamentals, v15_scan_db=scan_db)
    expected_message = {
        "rt-start": "RT start failed",
        "historical": "historical adapter failed",
        "mapper": "concept mapper failed",
        "filter": "stock filter failed",
    }[failure]
    with pytest.raises(RuntimeError, match=expected_message):
        await v15_scan_service.init_scan_resources(state)

    assert len(Realtime.instances) == 1
    assert Realtime.instances[0].stop_calls == 1
    assert state.realtime_client is None
    assert state.historical_adapter is None
    assert state.concept_mapper is None
    assert state.stock_filter is None
    assert state.initialized is False
    assert state.fundamentals_db is fundamentals
    assert state.v15_scan_db is scan_db
    assert fundamentals.closed is False
    assert scan_db.closed is False


@pytest.mark.asyncio
async def test_cleanup_continues_remaining_resources_when_one_step_fails() -> None:
    class Realtime:
        def __init__(self) -> None:
            self.stopped = False

        async def stop(self) -> None:
            self.stopped = True
            raise RuntimeError("RT stop failed")

    class Database:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    realtime = Realtime()
    fundamentals = Database()
    scan_db = Database()
    state = V15ScanState(
        initialized=True,
        realtime_client=realtime,
        fundamentals_db=fundamentals,
        v15_scan_db=scan_db,
    )
    state.resource_owner = "V16"

    with pytest.raises(RuntimeError, match="scan resource cleanup failed"):
        await v15_scan_service.cleanup_scan_resources(state)

    assert realtime.stopped is True
    assert fundamentals.closed is True
    assert scan_db.closed is True
    assert state.realtime_client is None
    assert state.historical_adapter is None
    assert state.concept_mapper is None
    assert state.stock_filter is None
    assert state.fundamentals_db is None
    assert state.v15_scan_db is None
    assert state.initialized is False
    assert state.resource_owner is None


@pytest.mark.asyncio
async def test_cancelling_v20_owner_waiter_preserves_shielded_resource_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.common import config as common_config
    from src.data.clients import iquant_historical_adapter as historical_module
    from src.data.clients import tushare_realtime as realtime_module
    from src.data.sources import local_concept_mapper as concept_module
    from src.strategy.filters import stock_filter as stock_filter_module

    monkeypatch.setenv("TUSHARE_TOKEN", "owner-token")
    monkeypatch.setattr(common_config, "get_tushare_token", lambda: "owner-token")
    start_entered = asyncio.Event()
    release_start = asyncio.Event()

    class Realtime:
        def __init__(self, *, token: str) -> None:
            assert token == "owner-token"
            self.stop_calls = 0

        async def start(self) -> None:
            start_entered.set()
            await release_start.wait()

        async def stop(self) -> None:
            self.stop_calls += 1

    class Fundamentals:
        def __init__(self) -> None:
            self.close_calls = 0
            self.connection_pool = object()

        async def close(self) -> None:
            self.close_calls += 1

    realtime_holder: list[Realtime] = []

    def realtime_factory(**kwargs: Any) -> Realtime:
        client = Realtime(**kwargs)
        realtime_holder.append(client)
        return client

    monkeypatch.setattr(realtime_module, "TushareRealtimeClient", realtime_factory)
    monkeypatch.setattr(
        historical_module,
        "IQuantHistoricalAdapter",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(concept_module, "LocalConceptMapper", lambda: object())
    monkeypatch.setattr(stock_filter_module, "StockFilter", lambda _config: object())

    fundamentals = Fundamentals()
    state = V15ScanState(fundamentals_db=fundamentals)
    owner_waiter = asyncio.create_task(_init_embedded_v20_scan_resources(state))
    await asyncio.wait_for(start_entered.wait(), timeout=0.25)
    borrowers = [asyncio.create_task(_init_embedded_v20_scan_resources(state)) for _ in range(3)]
    await asyncio.sleep(0)
    owner_waiter.cancel()
    await asyncio.gather(owner_waiter, return_exceptions=True)
    release_start.set()

    master = state.resource_init_task
    assert master is not None
    await master
    assert not master.cancelled()
    assert state.resource_owner == "V20"
    assert state.initialized is True

    await v15_scan_service.cleanup_scan_resources(state, owner="V16")
    assert realtime_holder[0].stop_calls == 0
    assert fundamentals.close_calls == 0
    assert state.resource_owner == "V20"

    await v15_scan_service.cleanup_scan_resources(state, owner="V20", close_fundamentals=False)
    assert realtime_holder[0].stop_calls == 1
    assert fundamentals.close_calls == 0
    assert state.resource_owner is None
    assert state.initialized is False
    assert state.resource_init_task is None
    await asyncio.gather(*borrowers, return_exceptions=True)


@pytest.mark.asyncio
async def test_post_cutoff_terminal_enter_still_fresh_recomputes_check_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today = date(2026, 9, 1)
    old_codes = ["000001", "600000"]
    fresh_codes = [
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
    ]
    frozen_rolling = [
        {
            "batch_id": f"terminal-roll-{index}",
            "signal_date": date(2026, 8, 10 + index).isoformat(),
            "t2_date": date(2026, 8, 12 + index).isoformat(),
            "batch_return": 0.01 + index * 0.001,
        }
        for index in range(7)
    ]
    frozen_health = [
        {
            "batch_id": f"terminal-health-{index}",
            "signal_date": date(2026, 8, 20 + index).isoformat(),
            "t2_date": date(2026, 8, 24 + index).isoformat(),
            "relative_return": -0.01,
            "valid": True,
            "invalid_reason": None,
        }
        for index in range(3)
    ]
    frozen_gaps = [
        {
            "gap_id": f"terminal-gap-{index}",
            "signal_date": date(2026, 8, 12 + index).isoformat(),
            "maturity_date": date(2026, 8, 14 + index).isoformat(),
            "closed": index % 2 == 0,
            "aged_out": False,
        }
        for index in range(2)
    ]
    frozen_policy_inputs = {
        "schema_version": "v20-policy-input-snapshot/v1",
        "completed_health": frozen_health,
        "completed_rolling": frozen_rolling,
        "maturity_gaps": frozen_gaps,
    }
    frozen_policy_hash = sha256_json(frozen_policy_inputs)
    old_semantic = {
        "state_after_hash": "0" * 64,
        "state_before_hash": "9" * 64,
        "policy_input_hash": frozen_policy_hash,
        "symbols": [
            {"rank": index + 1, "code": code, "name": f"old-{code}", "snapshot_price": 10.0}
            for index, code in enumerate(old_codes)
        ],
    }
    old_payload = {"message": "old terminal ENTER payload must not be replayed"}
    old_terminal = SimpleNamespace(
        official_stream_id=None,
        trade_date=today,
        slot_id="old-slot",
        slot_status="COMMITTED",
        slot_revision=7,
        strategy_version="V20",
        config_id="old-config",
        config_hash="old-config-hash",
        lineage_id=None,
        decision_id="old-decision",
        event_id="old-terminal-enter",
        action="ENTER",
        final_multiplier=1.0,
        semantic_content_hash=sha256_json(old_semantic),
        semantic=old_semantic,
        snapshot_id="old-snapshot",
        snapshot_hash=sha256_json(
            {
                "schema_version": V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
                "trade_date": today.isoformat(),
                "state_before_hash": "9" * 64,
                "policy_input_hash": frozen_policy_hash,
                "policy_inputs": frozen_policy_inputs,
            }
        ),
        snapshot={
            "schema_version": V20_DECISION_INPUT_SNAPSHOT_SCHEMA,
            "trade_date": today.isoformat(),
            "state_before_hash": "9" * 64,
            "policy_input_hash": frozen_policy_hash,
            "policy_inputs": frozen_policy_inputs,
        },
        action_expiry_ts=datetime(2026, 9, 1, 9, 40, tzinfo=TZ),
    )

    class Repository:
        def __init__(self) -> None:
            self.terminal = old_terminal
            self.state: Any | None = None
            self.old_semantic = dict(old_semantic)
            self.formal_write_calls = 0
            self.raw_reads: list[tuple[tuple[str, ...], date]] = []
            self.raw_records: list[Any] = []
            self.persist_calls: list[tuple[Mapping[str, Any], ...]] = []
            self.alerts: list[Mapping[str, Any]] = []
            self.events: dict[str, OutboxRecord] = {}

        async def assert_runtime_leader(self) -> None:
            return None

        async def get_entry_status(self, _stream: str, trade_date: date) -> Any:
            return self.terminal if trade_date == today else None

        async def get_outbox_event(self, event_id: str, **_kwargs: Any) -> Any:
            return self.events.get(event_id)

        async def load_state(self, lineage_id: str) -> Any:
            assert lineage_id == service.config.state_lineage_id
            return self.state

        async def record_minute_bars(self, payloads: list[Mapping[str, Any]]) -> frozenset[str]:
            normalized = tuple(dict(payload) for payload in payloads)
            self.persist_calls.append(normalized)
            return frozenset(sha256_json(payload) for payload in normalized)

        async def list_raw_minute_bar_records(
            self,
            codes: Any,
            *,
            trade_date: date,
            end_labels: Any,
            received_before: datetime | None = None,
        ) -> list[Any]:
            self.raw_reads.append((tuple(codes), trade_date))
            assert trade_date == today
            assert tuple(end_labels)[-1] == "09:39"
            records = []
            for code in codes:
                for minute in range(31, 40):
                    label = f"09:{minute:02d}"
                    bar = TushareMinuteBar(
                        stock_code=code,
                        bar_end=datetime(2026, 9, 1, 9, minute, tzinfo=TZ),
                        end_label=label,
                        open_price=10.0,
                        close_price=10.1,
                        high_price=10.2,
                        low_price=9.9,
                        volume=1000.0,
                        amount=10000.0,
                    )
                    payload = _bar_payload(bar)
                    records.append(
                        SimpleNamespace(
                            code=code,
                            bar_end=bar.bar_end,
                            end_label=label,
                            source_hash=sha256_json(payload),
                            payload=payload,
                            first_received_at=datetime(2026, 9, 1, 9, 39, tzinfo=TZ),
                        )
                    )
            self.raw_records.extend(records)
            return records

        async def enqueue_alert(
            self,
            event_id: str,
            route_id: str,
            semantic: Mapping[str, Any],
            semantic_hash: str,
            **scope: Any,
        ) -> bool:
            assert event_id not in self.events
            assert route_id == service.config.route_id
            assert scope == {
                "official_stream_id": service.config.official_stream_id,
                "lineage_id": service.config.state_lineage_id,
            }
            assert semantic_hash == sha256_json(semantic)
            self.alerts.append(dict(semantic))
            self.events[event_id] = OutboxRecord(
                event_id=event_id,
                event_type="DATA_ALERT",
                route_id=route_id,
                official_stream_id=scope["official_stream_id"],
                lineage_id=scope["lineage_id"],
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

        async def seal_event(self, event_id: str, formatter: Any) -> Any:
            current = self.events[event_id]
            payload = dict(formatter(current, datetime.now(TZ), 100, True))
            sealed = dataclasses_replace(
                current,
                payload=payload,
                payload_hash=sha256_json(payload),
                generated_at=datetime.now(TZ),
                commit_marker=100,
            )
            self.events[event_id] = sealed
            return sealed

        async def commit_entry(self, *_args: Any, **_kwargs: Any) -> None:
            self.formal_write_calls += 1
            raise AssertionError("check-only manual trigger must not commit entry state")

        async def commit_exit(self, *_args: Any, **_kwargs: Any) -> None:
            self.formal_write_calls += 1
            raise AssertionError("check-only manual trigger must not commit exit state")

    repository = Repository()
    service = _v20_service(monkeypatch, repository=repository)
    service.config = dataclasses_replace(service.config, enabled=True)
    state_payload = genesis_state()
    state_payload["state_revision"] = 7
    health_observations = tuple(
        HealthObservation(
            batch_id=f"old-health-{index}",
            signal_date=date(2026, 8, 20 + index),
            t2_exit_date=date(2026, 8, 24 + index),
            relative_return=-0.01,
        )
        for index in range(3)
    )
    state_payload["health"] = serialize_health_snapshot(
        HealthSnapshot(
            status=HealthStatus.PAUSED_R2,
            recovery_count=2,
            recent_valid=health_observations,
            last_processed_key=(
                health_observations[-1].t2_exit_date,
                health_observations[-1].signal_date,
                health_observations[-1].batch_id,
            ),
        )
    )
    state_payload["last_terminal_slot_id"] = "old-slot"
    state_payload["last_terminal_trade_date"] = today.isoformat()
    repository.state = StateRecord(
        lineage_id=service.config.state_lineage_id,
        revision=7,
        state_hash=sha256_json(state_payload),
        payload=state_payload,
    )
    old_terminal.semantic["state_after_hash"] = repository.state.state_hash
    old_terminal.semantic_content_hash = sha256_json(old_terminal.semantic)
    # Snapshot the bound terminal semantic only after the harness finishes its
    # own setup mutations; the replay must leave these bytes untouched.
    repository.old_semantic = dict(old_semantic)
    old_terminal.official_stream_id = service.config.official_stream_id
    old_terminal.lineage_id = service.config.state_lineage_id
    old_terminal.strategy_version = service.config.strategy_version
    old_terminal.config_id = service.config.config_hash[:24]
    old_terminal.config_hash = service.config.config_hash
    verified_terminals: list[Any] = []

    def verify_entry_binding(status: Any) -> None:
        verified_terminals.append(status)
        if status.action in {"ENTER", "BLOCK", "NO_SIGNAL"}:
            assert status.semantic["state_after_hash"] == repository.state.state_hash

    repository.events["old-terminal-enter"] = OutboxRecord(
        event_id="old-terminal-enter",
        event_type="ENTRY_DECISION",
        route_id=service.config.route_id,
        official_stream_id=service.config.official_stream_id,
        lineage_id=service.config.state_lineage_id,
        semantic=old_semantic,
        semantic_content_hash=sha256_json(old_semantic),
        payload=old_payload,
        payload_hash=sha256_json(old_payload),
        generated_at=datetime(2026, 9, 1, 9, 40, tzinfo=TZ),
        commit_marker=7,
        action_expiry_ts=None,
        delivery_status="SENT",
        attempt_count=1,
    )
    service._started = True
    service._repository_started = True
    service._clock = lambda: datetime(2026, 9, 1, 9, 40, 1, tzinfo=TZ)

    async def calendar(_current: date) -> tuple[date, ...]:
        return (
            date(2026, 8, 28),
            date(2026, 8, 31),
            today,
            date(2026, 9, 2),
            date(2026, 9, 3),
        )

    async def ready() -> None:
        return None

    async def mews_already_recovered(_now: datetime) -> bool:
        return True

    monkeypatch.setattr(service, "_load_trade_calendar", calendar)
    monkeypatch.setattr(service, "_require_manual_trigger_ready", ready)
    monkeypatch.setattr(
        service,
        "ensure_mews_for_selection_trigger",
        mews_already_recovered,
    )
    monkeypatch.setattr(service, "_verify_entry_binding", verify_entry_binding)

    recommended = [
        ScoredStock(
            code=code,
            name=f"fresh-{code}",
            score=0.9 - index * 0.01,
            rank=index + 1,
            buy_price=10.0 + index,
        )
        for index, code in enumerate(fresh_codes)
    ]
    scan_result = V16ScanResult(
        recommended=recommended,
        all_scored=recommended,
        step0_universe_count=len(fresh_codes),
        step2_hot_board_count=1,
        step2_board_avg_gains={"fresh-board": 1.25},
        final_candidates=len(fresh_codes),
        stock_best_board={item.code: "fresh-board" for item in recommended},
        stock_all_boards={item.code: ["fresh-board"] for item in recommended},
        stock_is_driver={item.code: True for item in recommended},
        stock_cci={item.code: 50.0 for item in recommended},
        stock_early_vol={item.code: 1000.0 for item in recommended},
    )
    history_df = pd.DataFrame(
        {
            "time": [
                (date(2026, 7, 10) + timedelta(days=index)).isoformat() for index in range(37)
            ],
            "open": [10.0] * 37,
            "high": [10.5] * 37,
            "low": [9.5] * 37,
            "close": [10.0 + index * 0.01 for index in range(37)],
            "volume": [1000.0] * 37,
        }
    )
    history_rows = history_df.to_dict(orient="records")
    history_raw = {
        item.code: {
            "time": [row["time"] for row in history_rows],
            "open": [row["open"] for row in history_rows],
            "high": [row["high"] for row in history_rows],
            "low": [row["low"] for row in history_rows],
            "close": [row["close"] for row in history_rows],
            "volume": [row["volume"] for row in history_rows],
        }
        for item in recommended
    }
    early_bars = {
        item.code: tuple(
            TushareMinuteBar(
                stock_code=item.code,
                bar_end=datetime(2026, 9, 1, 9, minute, tzinfo=TZ),
                end_label=f"09:{minute:02d}",
                open_price=10.0,
                close_price=10.1,
                high_price=10.2,
                low_price=9.9,
                volume=1000.0,
                amount=10000.0,
            )
            for minute in range(31, 40)
        )
        for item in recommended
    }
    persisted_row_hashes = {
        item.code: [
            {"label": bar.end_label, "payload_hash": sha256_json(_bar_payload(bar))} for bar in bars
        ]
        for item, bars in ((item, early_bars[item.code]) for item in recommended)
    }
    early_source_hashes = {
        item.code: sha256_json(
            {
                "profile": "PERSISTED_0931_0939_ROWS_V1",
                "code": item.code,
                "rows": persisted_row_hashes[item.code],
            }
        )
        for item in recommended
    }
    canonical_raw_hashes = {
        item.code: {
            "early_source_hash": early_source_hashes[item.code],
            "bars": persisted_row_hashes[item.code],
        }
        for item in recommended
    }
    canonical_input_hash = sha256_json(canonical_raw_hashes)
    quotes = {
        item.code: TushareQuote(
            stock_code=item.code,
            open_price=10.0,
            latest_price=item.buy_price,
            high_price=10.8,
            low_price=9.8,
            volume=2000.0,
            amount=20000.0,
            early_close=item.buy_price,
            early_high=10.8,
            early_low=9.8,
            early_volume=2000.0,
            volume_937=1000.0,
        )
        for item in recommended
    }
    canonical_pre = CanonicalV16ScanBundle(
        trade_date=today,
        scan_result=scan_result,
        stock_data={
            item.code: V16StockData(
                code=item.code,
                name=item.name,
                open_price=10.0,
                prev_close=9.9,
                price_940=item.buy_price,
                high_940=10.8,
                low_940=9.8,
                volume_940=2000.0,
                volume_937=1000.0,
                avg_daily_volume=1000.0,
                trend_5d=0.05,
                trend_10d=0.10,
                avg_daily_return_20d=0.001,
                volatility_20d=0.02,
                consecutive_up_days=2,
                history_df=history_df.copy(deep=True),
            )
            for item in recommended
        },
        clean_boards={"fresh-board": [(item.code, item.name) for item in recommended]},
        universe=tuple(fresh_codes),
        quotes=quotes,
        prev_closes={item.code: 9.9 for item in recommended},
        history_raw=history_raw,
        early_bars=early_bars,
        early_source_hashes=early_source_hashes,
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="m" * 64,
        feature_list_sha256="f" * 64,
        computed_at=datetime(2026, 9, 1, 9, 39, 59, tzinfo=TZ),
        input_hash=canonical_input_hash,
        _integrity_hash="",
    )
    canonical = dataclasses_replace(
        canonical_pre,
        _integrity_hash=v15_scan_service._bundle_fingerprint(canonical_pre),
    )
    service._scan_state.initialized = True
    service._scan_state.canonical_coordinator = SimpleNamespace(
        cache={today: canonical},
        inflight={},
        publish={},
        published=set(),
        data_errors_sent=set(),
        fatal_errors_sent=set(),
        not_ready_alert_sent=set(),
        partial={},
        lock=asyncio.Lock(),
    )

    fresh_builder_inputs: list[Any] = []
    original_builder = service._build_late_0939_replay_semantic
    from src.web import v20_service as service_module

    prepare_entry_calls: list[Mapping[str, Any]] = []
    prepare_entry_results: list[Any] = []
    real_prepare_entry = service_module.prepare_entry

    def spied_prepare_entry(**kwargs: Any) -> Any:
        prepare_entry_calls.append(dict(kwargs))
        result = real_prepare_entry(**kwargs)
        prepare_entry_results.append(result)
        return result

    monkeypatch.setattr(service_module, "prepare_entry", spied_prepare_entry)

    async def spied_builder(
        context: Any,
        now: datetime,
        *,
        replay_event_id: str,
    ) -> Any:
        semantic = await original_builder(
            context,
            now,
            replay_event_id=replay_event_id,
        )
        fresh_builder_inputs.append(context)
        return semantic

    monkeypatch.setattr(service, "_build_late_0939_replay_semantic", spied_builder)

    result = await _dispatch_manual_trigger(service, "terminal-enter-fresh-001")

    assert result["visible_message_mode"] != "FROZEN_OFFICIAL_PAYLOAD"
    assert result["current_version_recomputed"] is True
    assert result["replay_reused"] is False
    assert [item["code"] for item in result["symbols"]] == fresh_codes
    assert result["v20_action"] == "BLOCK"
    assert result["final_multiplier"] == 0.0
    assert old_terminal.action == "ENTER"
    assert old_terminal.final_multiplier == 1.0
    assert len(prepare_entry_calls) == 1
    assert len(prepare_entry_results) == 1
    assert prepare_entry_results[0].commit.semantic["action"] == "BLOCK"
    assert prepare_entry_results[0].commit.semantic["final_multiplier"] == 0.0
    assert (
        prepare_entry_results[0].commit.snapshot["policy_input_hash"]
        == old_terminal.semantic["policy_input_hash"]
    )
    assert prepare_entry_calls[0]["bundle"] is fresh_builder_inputs[0].canonical_bundle
    assert result["official_state_changed"] is False
    assert result["orders_changed"] is False
    assert result["non_actionable"] is True
    assert repository.terminal is old_terminal
    assert repository.old_semantic == old_semantic
    assert repository.formal_write_calls == 0
    assert fresh_builder_inputs
    assert [
        item["code"] for item in fresh_builder_inputs[0].canonical_bundle.snapshot["symbols"]
    ] == fresh_codes
    assert fresh_builder_inputs[0].canonical_bundle.snapshot["early_market_source_hash"] == (
        canonical_input_hash
    )
    persisted_by_key = {
        (record.code, record.end_label): record for record in repository.raw_records
    }
    for code, bars in canonical.early_bars.items():
        for bar in bars:
            record = persisted_by_key[(code, bar.end_label)]
            expected_payload = _bar_payload(bar)
            assert record.payload == expected_payload
            assert record.source_hash == sha256_json(expected_payload)
    assert {
        item["code"]: item["early_source_hash"]
        for item in fresh_builder_inputs[0].canonical_bundle.snapshot["symbols"]
    } == early_source_hashes
    assert repository.raw_reads
    assert all(len(codes) == len(fresh_codes) for codes, _trade_date in repository.raw_reads)
    assert repository.alerts
    assert len(repository.persist_calls) == 1
    assert len(repository.persist_calls[0]) == len(fresh_codes) * 9
    alert_semantic = repository.alerts[0]
    # The fresh check-only replay must reuse the terminal slot's frozen policy
    # hash and the real official state revision/terminal binding (CAS-checked
    # before and after), never a zeroed revision or empty inputs.
    assert alert_semantic["policy_input_hash"] == frozen_policy_hash
    assert alert_semantic["official_state_revision_before"] == 7
    assert alert_semantic["official_state_revision_after"] == 7
    assert alert_semantic["official_state_hash_before"] == repository.state.state_hash
    assert alert_semantic["official_state_hash_after"] == repository.state.state_hash
    assert alert_semantic["official_entry_event_id_before"] == "old-terminal-enter"
    assert alert_semantic["official_entry_event_id_after"] == "old-terminal-enter"
    assert alert_semantic["raw_fact_n"] == len(fresh_codes) * 9
    assert alert_semantic["raw_pre_cutoff_n"] == len(fresh_codes) * 9
    assert alert_semantic["raw_post_cutoff_n"] == 0


@pytest.mark.asyncio
async def test_deployment_probe_rejects_incompatible_cache_without_second_algorithm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.web import v20_service as service_module

    today = date(2026, 9, 1)
    now = datetime(2026, 9, 1, 9, 40, 1, tzinfo=TZ)
    old_code = "000001"
    history_df = pd.DataFrame(
        {
            "time": [
                (date(2026, 7, 10) + timedelta(days=index)).isoformat() for index in range(37)
            ],
            "open": [10.0] * 37,
            "high": [10.5] * 37,
            "low": [9.5] * 37,
            "close": [10.0 + index * 0.01 for index in range(37)],
            "volume": [1000.0] * 37,
        }
    )
    old_top = ScoredStock(
        code=old_code,
        name="old-cache-name",
        score=0.1,
        rank=1,
        buy_price=10.2,
    )
    old_stock = V16StockData(
        code=old_code,
        name=old_top.name,
        open_price=10.0,
        prev_close=9.9,
        price_940=10.2,
        high_940=10.3,
        low_940=9.9,
        volume_940=1000.0,
        volume_937=500.0,
        avg_daily_volume=1000.0,
        trend_5d=0.05,
        trend_10d=0.1,
        avg_daily_return_20d=0.001,
        volatility_20d=0.02,
        consecutive_up_days=2,
        history_df=history_df.copy(deep=True),
    )
    old_result = V16ScanResult(
        recommended=[old_top],
        all_scored=[old_top],
        step0_universe_count=1,
        step2_hot_board_count=1,
        final_candidates=1,
        stock_best_board={old_code: "old-board"},
        stock_all_boards={old_code: ["old-board"]},
        stock_is_driver={old_code: True},
        stock_cci={old_code: 50.0},
        stock_early_vol={old_code: 500.0},
        step2_board_avg_gains={"old-board": 1.0},
    )
    old_early_bars = tuple(
        TushareMinuteBar(
            stock_code=old_code,
            bar_end=datetime(2026, 9, 1, 9, minute, tzinfo=TZ),
            end_label=f"09:{minute:02d}",
            open_price=10.0,
            close_price=10.0,
            high_price=10.1,
            low_price=9.9,
            volume=100.0,
            amount=1000.0,
        )
        for minute in range(31, 40)
    )
    old_pre = CanonicalV16ScanBundle(
        trade_date=today,
        scan_result=old_result,
        stock_data={old_code: old_stock},
        clean_boards={"old-board": [(old_code, old_top.name)]},
        universe=(old_code,),
        quotes={},
        prev_closes={old_code: 9.9},
        history_raw={},
        early_bars={old_code: old_early_bars},
        early_source_hashes={old_code: "a" * 64},
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256="1" * 64,
        feature_list_sha256="2" * 64,
        computed_at=datetime(2026, 9, 1, 9, 39, 59, tzinfo=TZ),
        input_hash="b" * 64,
        _integrity_hash="",
    )
    old_canonical = dataclasses_replace(
        old_pre,
        _integrity_hash=v15_scan_service._bundle_fingerprint(old_pre),
    )

    raw_records = []
    for code in FRESH_CODES:
        for minute in range(31, 40):
            bar = TushareMinuteBar(
                stock_code=code,
                bar_end=datetime(2026, 9, 1, 9, minute, tzinfo=TZ),
                end_label=f"09:{minute:02d}",
                open_price=10.0,
                close_price=10.1,
                high_price=10.2,
                low_price=9.9,
                volume=1000.0,
                amount=10000.0,
            )
            payload = _bar_payload(bar)
            raw_records.append(
                SimpleNamespace(
                    code=code,
                    bar_end=bar.bar_end,
                    end_label=bar.end_label,
                    source_hash=sha256_json(payload),
                    payload=payload,
                    first_received_at=datetime(2026, 9, 1, 9, 39, tzinfo=TZ),
                )
            )

    state_payload = genesis_state()
    observations = tuple(
        HealthObservation(
            batch_id=f"health-{index}",
            signal_date=date(2026, 8, 20 + index),
            t2_exit_date=date(2026, 8, 24 + index),
            relative_return=-0.01,
        )
        for index in range(3)
    )
    state_payload["health"] = serialize_health_snapshot(
        HealthSnapshot(
            status=HealthStatus.PAUSED_R2,
            recovery_count=2,
            recent_valid=observations,
            last_processed_key=(
                observations[-1].t2_exit_date,
                observations[-1].signal_date,
                observations[-1].batch_id,
            ),
        )
    )
    state_payload["state_revision"] = 7
    state_record = StateRecord(
        lineage_id="lineage",
        revision=7,
        state_hash=sha256_json(state_payload),
        payload=state_payload,
    )
    terminal = SimpleNamespace(
        action="ENTER",
        event_id="old-enter",
        trade_date=today,
        final_multiplier=1.0,
        semantic={"state_after_hash": state_record.state_hash},
        semantic_content_hash="c" * 64,
    )

    class Repository:
        def __init__(self) -> None:
            self.events = {}
            self.raw_calls = []

        async def assert_runtime_leader(self):
            return None

        async def load_state(self, _lineage):
            return state_record

        async def get_entry_status(self, _stream, trade_date):
            return terminal if trade_date == today else None

        async def get_outbox_event(self, event_id, **_kwargs):
            return self.events.get(event_id)

        async def list_raw_minute_bar_records(self, codes, **_kwargs):
            requested = tuple(codes)
            self.raw_calls.append(requested)
            allowed = set(requested)
            return [record for record in raw_records if record.code in allowed]

        async def enqueue_alert(self, event_id, route_id, semantic, semantic_hash, **scope):
            self.events[event_id] = OutboxRecord(
                event_id=event_id,
                event_type="DATA_ALERT",
                route_id=route_id,
                official_stream_id=scope["official_stream_id"],
                lineage_id=scope["lineage_id"],
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

        async def seal_event(self, event_id, formatter):
            current = self.events[event_id]
            payload = dict(formatter(current, now, 100, True))
            sealed = dataclasses_replace(
                current,
                payload=payload,
                payload_hash=sha256_json(payload),
                generated_at=now,
                commit_marker=100,
            )
            self.events[event_id] = sealed
            return sealed

    repository = Repository()
    service = _v20_service(monkeypatch, repository=repository)
    service.config = dataclasses_replace(service.config, enabled=True)
    service._started = True
    service._repository_started = True
    service._clock = lambda: now
    service._scan_state.realtime_client = Bomb()
    service._scan_state.canonical_coordinator = SimpleNamespace(
        cache={today: old_canonical},
        inflight={},
        publish={},
        published=set(),
        data_errors_sent=set(),
        fatal_errors_sent=set(),
        not_ready_alert_sent=set(),
        partial={},
        lock=asyncio.Lock(),
    )
    monkeypatch.setattr("src.data.clients.tushare_realtime.TushareRealtimeClient", Bomb)
    monkeypatch.setattr("src.data.clients.iquant_historical_adapter.IQuantHistoricalAdapter", Bomb)
    monkeypatch.setattr(service, "_verify_entry_binding", lambda _status: None)
    monkeypatch.setattr(service, "_require_manual_trigger_ready", _no_op_start)

    async def calendar():
        return [date(2026, 8, 31), today, date(2026, 9, 2), date(2026, 9, 3)]

    service._calendar_provider = calendar
    projection_calls = []
    original_projection = service._project_canonical_v16

    def spied_projection(canonical, **kwargs):
        projection_calls.append(canonical)
        return original_projection(canonical, **kwargs)

    monkeypatch.setattr(service, "_project_canonical_v16", spied_projection)
    prepare_calls = []
    prepare_results = []
    real_prepare = service_module.prepare_entry

    def spied_prepare(**kwargs):
        prepare_calls.append(dict(kwargs))
        result = real_prepare(**kwargs)
        prepare_results.append(result)
        return result

    monkeypatch.setattr(service_module, "prepare_entry", spied_prepare)
    result = await _dispatch_manual_trigger(service, "from-persisted-raw-current")

    assert result["current_version_recomputed"] is False
    assert result["replay_reused"] is False
    assert result["probe_result"] == "FAIL"
    assert result["symbols"] == []
    assert result["v20_action"] is None
    assert result["final_multiplier"] is None
    assert result["failure_stage"] == "RAW_V16_V20_RECOMPUTE"
    assert "exact unique Top10" in result["failure_reason"]
    assert prepare_calls == []
    assert prepare_results == []
    assert projection_calls == []
    assert service._scan_state.canonical_coordinator.cache[today] is old_canonical
    assert repository.raw_calls == []


def test_external_mews_snapshots_cannot_become_production_fact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Repository:
        async def record_mews_snapshot(self, payload: Mapping[str, Any]) -> str:
            raise AssertionError("external MEWS payload reached production ledger")

    service = _v20_service(monkeypatch, repository=Repository())
    app = FastAPI()
    app.state.v20_service = service
    app.include_router(create_v20_router())
    service._started = True
    client = TestClient(app)
    response = client.post(
        "/api/v20/mews-snapshots",
        headers={"X-V20-API-Key": "i" * 32},
        json={
            "snapshot_id": "ext",
            "source_trade_date": "2026-08-31",
            "availability_date": "2026-09-01",
            "fast_state": "SAFE",
            "score": 0.1,
            "generated_at": "2026-09-01T09:39:00+08:00",
            "payload_hash": "0" * 64,
        },
    )
    assert response.status_code in (403, 404, 405, 409, 410, 422, 501)
    assert response.json().get("accepted") is not True
