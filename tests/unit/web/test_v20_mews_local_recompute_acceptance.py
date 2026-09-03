"""Acceptance: V20 MEWS is a local, persisted, singleflight daily computation.

Semantics under acceptance (through the real V20Service MEWS coordinator and
the real LocalMewsSnapshotCalculator, with only a minimal fake repository and
deterministic local raw material):

1. MEWS is calculated locally at 09:10 and persisted; it never depends on an
   external computed-MEWS service, Greptime, or the old source API.
2. Any trigger that finds today's MEWS missing recomputes it on the spot,
   independent of wall-clock time (a first deploy at 14:04 must still compute
   and persist; entry-cutoff eligibility is a separate question and must never
   skip the recomputation itself).
3. Concurrent triggers (manual and automatic) singleflight: exactly one
   calculation and one persistence, all callers observe the same result.
4. A valid same-day cache is reused as-is; content/date/source validation
   failures fail closed instead of re-serving a bad source through another
   path.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace as dataclasses_replace
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pytest

from src.data.clients.mews_snapshot import LocalMewsSnapshotCalculator
from src.data.database.v20_repository import V20SemanticConflict, sha256_json
from src.strategy.v20 import runtime_config
from src.strategy.v20.artifacts import load_g_artifacts
from src.strategy.v20.runtime_config import load_v20_runtime_config
from src.web.v20_canonical_selection import V20CanonicalSelectionState
from src.web.v20_service import MEWS_CACHE_CUTOFF, V20Service

SHANGHAI = ZoneInfo("Asia/Shanghai")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
BOOTSTRAP_PATH = PROJECT_ROOT / "data" / "v20_mews_bootstrap.json.gz"

SOURCE_DATE = date(2026, 8, 31)
TODAY = date(2026, 9, 1)
CALENDAR = (SOURCE_DATE, TODAY, date(2026, 9, 2), date(2026, 9, 3))
T_0905 = datetime(2026, 9, 1, 9, 5, tzinfo=SHANGHAI)
T_0915 = datetime(2026, 9, 1, 9, 15, tzinfo=SHANGHAI)
T_1404 = datetime(2026, 9, 1, 14, 4, tzinfo=SHANGHAI)
CUTOFF_0940 = datetime.combine(TODAY, MEWS_CACHE_CUTOFF, SHANGHAI)

# Hard guard against deadlocks: no awaited step may block longer than this.
STEP_TIMEOUT = 20.0

ALLOWED_RAW_APIS = {"trade_cal", "stock_basic", "margin", "margin_detail", "daily_basic"}


class Bomb:
    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"forbidden dependency boundary was touched: {name}")


class FakeMewsRepository:
    """Minimal but honest stand-in for the V20 repository's MEWS surface.

    It mirrors the real repository's validation, monotonic checkpoint, and
    receipt/cutoff eligibility semantics; it does not implement any of the
    calculation, singleflight, or caching logic under acceptance.
    """

    def __init__(
        self,
        state: Mapping[str, Any] | None = None,
        *,
        sealed_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._state = json.loads(json.dumps(state)) if state is not None else None
        self._state_hash = sha256_json(self._state) if self._state is not None else None
        self._sealed_clock = sealed_clock or (lambda: datetime.now(SHANGHAI))
        self.saved_states: list[dict[str, Any]] = []
        self.record_calls: list[dict[str, Any]] = []
        self.snapshots: dict[str, dict[str, Any]] = {}
        self.alerts: dict[str, Mapping[str, Any]] = {}

    async def assert_runtime_leader(self) -> None:
        return None

    async def load_mews_calculation_state(self) -> dict[str, Any] | None:
        return self._state

    async def save_mews_calculation_state(self, state: Mapping[str, Any]) -> str:
        if state.get("schema") != "v20-mews-incremental-state/v1":
            raise ValueError("MEWS calculation state schema is invalid")
        if state.get("model_version") != "mews_v2":
            raise ValueError("MEWS calculation state model_version is invalid")
        state_date = date.fromisoformat(str(state["state_date"]))
        payload = json.loads(json.dumps(state))
        content_hash = sha256_json(payload)
        if self._state is not None:
            existing_date = date.fromisoformat(str(self._state["state_date"]))
            if existing_date > state_date:
                raise V20SemanticConflict("MEWS calculation state cannot regress")
            if existing_date == state_date and self._state_hash != content_hash:
                raise V20SemanticConflict(
                    "MEWS calculation state changed for an already sealed date"
                )
        self._state = payload
        self._state_hash = content_hash
        self.saved_states.append(payload)
        return content_hash

    async def record_mews_snapshot(self, payload: Mapping[str, Any]) -> str:
        required = {
            "snapshot_id",
            "source_trade_date",
            "generated_at",
            "fast_state",
            "model_version",
            "data_version",
        }
        missing = sorted(required - set(payload))
        if missing:
            raise ValueError(f"MEWS snapshot missing fields: {', '.join(missing)}")
        if payload["fast_state"] not in {"NORMAL", "DANGER"}:
            raise ValueError("MEWS fast_state must be NORMAL or DANGER")
        generated_at = datetime.fromisoformat(str(payload["generated_at"]))
        if generated_at.tzinfo is None or generated_at.utcoffset() is None:
            raise ValueError("MEWS generated_at must be timezone-aware")
        content_hash = sha256_json(payload)
        snapshot_id = str(payload["snapshot_id"])
        self.record_calls.append(json.loads(json.dumps(payload)))
        existing = self.snapshots.get(snapshot_id)
        if existing is not None and existing["content_hash"] != content_hash:
            raise V20SemanticConflict("MEWS snapshot_id collision")
        if existing is None:
            self.snapshots[snapshot_id] = {
                "payload": json.loads(json.dumps(payload)),
                "content_hash": content_hash,
                "source_trade_date": date.fromisoformat(str(payload["source_trade_date"])),
                "generated_at": generated_at,
                "receipt_sealed_at": self._sealed_clock(),
            }
        return content_hash

    async def mews_snapshot_is_eligible(
        self,
        snapshot_id: str,
        *,
        source_trade_date: date,
        cutoff: datetime,
    ) -> bool:
        record = self.snapshots.get(snapshot_id)
        return bool(
            record is not None
            and record["source_trade_date"] == source_trade_date
            and record["generated_at"] < cutoff
            and record["receipt_sealed_at"] < cutoff
        )

    async def find_eligible_mews_snapshot(
        self,
        *,
        source_trade_date: date,
        cutoff: datetime,
        availability_date: date | None = None,
    ) -> str | None:
        availability = availability_date.isoformat() if availability_date is not None else None
        candidates = []
        for snapshot_id, record in self.snapshots.items():
            if record["source_trade_date"] != source_trade_date:
                continue
            on_time = record["generated_at"] < cutoff and record["receipt_sealed_at"] < cutoff
            same_day_repair = (
                record["payload"].get("evidence", {}).get("signal_available_date") == availability
            )
            if on_time or same_day_repair:
                candidates.append((record["generated_at"], snapshot_id))
        if not candidates:
            return None
        return max(candidates)[1]

    async def enqueue_alert(
        self,
        event_id: str,
        route_id: str,
        semantic: Mapping[str, Any],
        semantic_hash: str,
        **_scope: Any,
    ) -> bool:
        self.alerts[event_id] = semantic
        return True

    async def seal_event(self, event_id: str, _formatter: Any) -> Any:
        return SimpleNamespace(event_id=event_id)


class DeterministicRawClient:
    """Deterministic local raw material for exactly one pending trading day."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def query(
        self,
        api_name: str,
        params: Mapping[str, Any],
        fields: Sequence[str],
        *,
        allow_empty: bool = False,
    ) -> list[dict[str, Any]]:
        assert self.started
        self.calls.append(api_name)
        if api_name == "trade_cal":
            return [
                {
                    "exchange": params["exchange"],
                    "cal_date": "20260831",
                    "is_open": "1",
                    "pretrade_date": "20260828",
                }
            ]
        if api_name == "stock_basic":
            if params["list_status"] in {"P", "G"}:
                assert allow_empty
                return []
            if params["list_status"] == "D":
                return [
                    {
                        "ts_code": f"900001.{'SH' if params['exchange'] == 'SSE' else 'SZ'}",
                        "symbol": "900001",
                        "name": "old",
                        "market": "主板",
                        "exchange": params["exchange"],
                        "list_status": "D",
                        "list_date": "20000101",
                        "delist_date": "20010101",
                    }
                ]
            if params["exchange"] == "SZSE":
                return [
                    {
                        "ts_code": "000001.SZ",
                        "symbol": "000001",
                        "name": "Ping An Bank",
                        "market": "主板",
                        "exchange": "SZSE",
                        "list_status": "L",
                        "list_date": "19910403",
                        "delist_date": None,
                    }
                ]
            return [
                {
                    "ts_code": "600001.SH",
                    "symbol": "600001",
                    "name": "SSE sample",
                    "market": "主板",
                    "exchange": "SSE",
                    "list_status": "L",
                    "list_date": "20000101",
                    "delist_date": None,
                }
            ]
        if api_name == "margin":
            return [
                {"exchange_id": "SSE", "rzye": 60, "rzmre": 2, "rzche": 3},
                {"exchange_id": "SZSE", "rzye": 60, "rzmre": 2, "rzche": 3},
            ]
        if api_name == "margin_detail":
            return [
                {"ts_code": "000001.SZ", "rzye": 50, "rzmre": 1, "rzche": 3},
                {"ts_code": "600001.SH", "rzye": 50, "rzmre": 1, "rzche": 3},
            ]
        if api_name == "daily_basic":
            return [
                {"ts_code": "000001.SZ", "close": 10, "free_share": 100},
                {"ts_code": "600001.SH", "close": 10, "free_share": 100},
            ]
        raise AssertionError(f"unexpected raw API {api_name}")


class GatedRawClient(DeterministicRawClient):
    """Raw client that blocks inside the first `margin` query until released."""

    def __init__(self) -> None:
        super().__init__()
        self.entered_margin = asyncio.Event()
        self.release = asyncio.Event()

    async def query(
        self,
        api_name: str,
        params: Mapping[str, Any],
        fields: Sequence[str],
        *,
        allow_empty: bool = False,
    ) -> list[dict[str, Any]]:
        if api_name == "margin":
            self.entered_margin.set()
            await asyncio.wait_for(self.release.wait(), timeout=STEP_TIMEOUT)
        return await super().query(api_name, params, fields, allow_empty=allow_empty)


def _deterministic_state() -> dict[str, Any]:
    history = []
    for index in range(550):
        stock_balance = 95.0 + (index % 17) * 0.5
        market_balance = stock_balance / (5.0 / 6.0)
        buy = 3.0 + (index % 9) * 0.1
        repay = 3.2 + ((index * 7) % 11) * 0.1
        history.append(
            {
                "trade_date": f"2024-01-{(index % 28) + 1:02d}",
                "market_total_margin_balance": market_balance,
                "market_total_financing_buy_amount": buy * 1.2,
                "market_total_financing_repayment_amount": repay * 1.2,
                "ordinary_a_share_margin_balance": stock_balance,
                "ordinary_a_share_financing_buy_amount": buy,
                "ordinary_a_share_financing_repayment_amount": repay,
                "ordinary_a_share_margin_coverage": 5.0 / 6.0,
                "ffmv_stock": 20_000_000.0 + index * 10_000,
                "ffmv_coverage": 1.0,
                "nib_breadth_v2": 40.0,
                "nib_magnitude_v2": 30.0,
                "deleveraging_breadth": 55.0,
                "data_status": "OK",
                "mews_v2_score": 50.0 + (index % 13),
                "exhaustion_path": 48.0,
                "persistent_deleveraging_path": 45.0,
                "net_outflow_level_score": 60.0,
                "risk_state_v2": "WATCH",
            }
        )
    history[-1]["trade_date"] = "2026-08-28"
    security_state = {
        "current_balance": 50.0,
        "ema_fast_state": -0.01,
        "ema_fast_old_weight": 1.0,
        "ema_slow_state": -0.005,
        "ema_slow_old_weight": 1.0,
        "valid_history": [True] * 25,
        "net_flow_history": [-1.0] * 4,
        "impulse_history": [(-0.02 + (index % 7) * 0.002) for index in range(59)],
    }
    return {
        "schema": "v20-mews-incremental-state/v1",
        "model_version": "mews_v2",
        "state_date": "2026-08-28",
        "market_history": history,
        "security_states": {
            "000001.SZ": json.loads(json.dumps(security_state)),
            "600001.SH": json.loads(json.dumps(security_state)),
        },
        "risk_state": "WATCH",
        "clear_streak": 0,
    }


def _v20_config(monkeypatch: pytest.MonkeyPatch) -> Any:
    for name in ("V20_ENABLED", "V20_ALLOW_PRODUCTION_PUSH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("V20_MODE", "forward_shadow")
    monkeypatch.setenv("DB_SSLROOTCERT_SHA256", "c" * 64)
    monkeypatch.setenv("V20_INGEST_API_KEY", "i" * 32)
    monkeypatch.setenv("V20_STATUS_API_KEY", "s" * 32)
    monkeypatch.setattr(runtime_config, "_dependency_hashes", lambda _root: {})
    monkeypatch.setattr(
        runtime_config, "_state_semantics_source", lambda _payload: {"accepted": True}
    )
    return load_v20_runtime_config(PROJECT_ROOT)


def _build_service(
    monkeypatch: pytest.MonkeyPatch,
    *,
    repository: FakeMewsRepository,
    raw_factory: Callable[[], Any],
    now: datetime,
) -> V20Service:
    config = dataclasses_replace(_v20_config(monkeypatch), enabled=True)
    calculator = LocalMewsSnapshotCalculator(
        "raw-tushare-token",
        repository,
        bootstrap_path=BOOTSTRAP_PATH,
        client_factory=raw_factory,
        clock=lambda: now,
    )
    service = V20Service(
        config=config,
        repository=repository,
        scan_state=V20CanonicalSelectionState(initialized=True),
        artifacts=load_g_artifacts(
            config.artifact_manifest_path.parent,
            expected_manifest_sha256=config.artifact_manifest_sha256,
        ),
        publisher=Bomb(),
        routes={},
        mews_source=calculator,
    )
    service._started = True
    service._repository_started = True
    service._clock = lambda: now

    async def calendar_provider() -> list[date]:
        return list(CALENDAR)

    service._calendar_provider = calendar_provider
    return service


def _plain_factory(created: list[DeterministicRawClient]) -> Callable[[], Any]:
    def factory() -> DeterministicRawClient:
        client = DeterministicRawClient()
        created.append(client)
        return client

    return factory


class _GuardAsyncContext:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    async def __aenter__(self) -> Any:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _GuardConnection:
    def __init__(self, row: Mapping[str, Any] | None) -> None:
        self.row = row
        self.calls = 0

    async def fetchrow(self, *_args: Any) -> Any:
        self.calls += 1
        return self.row


class _AsyncpgLikeRow:
    def __init__(self, values: Mapping[str, Any]) -> None:
        self._values = dict(values)

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)


class _GuardPool:
    def __init__(self, connection: _GuardConnection) -> None:
        self.connection = connection

    def acquire(self) -> _GuardAsyncContext:
        return _GuardAsyncContext(self.connection)


class _GuardedCandidateRepository(FakeMewsRepository):
    def __init__(
        self,
        *,
        connection: _GuardConnection,
        repository_eligible: bool,
    ) -> None:
        super().__init__(_deterministic_state(), sealed_clock=lambda: T_1404)
        self.schema = "v20"
        self.pool = _GuardPool(connection)
        self.repository_eligible = repository_eligible
        self.eligibility_calls = 0

    async def find_eligible_mews_snapshot(self, **_kwargs: Any) -> str:
        return "guarded-candidate"

    async def mews_snapshot_is_eligible(
        self,
        _snapshot_id: str,
        *,
        source_trade_date: date,
        cutoff: datetime,
    ) -> bool:
        self.eligibility_calls += 1
        assert source_trade_date == SOURCE_DATE
        assert cutoff == CUTOFF_0940
        return self.repository_eligible


async def test_scheduled_0910_computes_mews_locally_and_persists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = FakeMewsRepository(_deterministic_state(), sealed_clock=lambda: T_0915)
    created: list[DeterministicRawClient] = []
    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory(created),
        now=T_0915,
    )

    # Before the 09:10 publish time the scheduled attempt must not compute.
    early = await asyncio.wait_for(
        service._refresh_mews_cache_once(T_0905, CALENDAR), timeout=STEP_TIMEOUT
    )
    assert early is False
    assert created == []

    calculated = await asyncio.wait_for(
        service._refresh_mews_cache_once(T_0915, CALENDAR), timeout=STEP_TIMEOUT
    )
    assert calculated is True

    assert isinstance(service._mews_source, LocalMewsSnapshotCalculator)
    assert service._mews_cached_for == TODAY
    assert service._mews_source_trade_date == SOURCE_DATE
    assert service._mews_last_failure is None
    assert repository.alerts == {}

    snapshot_id = service._mews_snapshot_id
    assert snapshot_id in repository.snapshots
    record = repository.snapshots[snapshot_id]
    payload = record["payload"]
    assert payload["model_version"] == "mews_v2"
    assert payload["source_trade_date"] == SOURCE_DATE.isoformat()
    assert payload["fast_state"] in {"NORMAL", "DANGER"}
    evidence = payload["evidence"]
    assert evidence["profile"] == "LOCAL_TUSHARE_MEWS_V2_0910_V1"
    assert evidence["source_trade_date"] == SOURCE_DATE.isoformat()
    assert evidence["signal_available_date"] == TODAY.isoformat()
    assert 0.0 <= evidence["mews"] <= 100.0

    # Locally computed from raw Tushare-shaped material only; the old computed
    # source API must never be touched.
    assert len(created) == 1
    raw_calls = created[0].calls
    assert raw_calls
    assert set(raw_calls) <= ALLOWED_RAW_APIS
    assert "margin-risk-curve" not in raw_calls

    # Persisted once, checkpointed once, and sealed before the 09:40 cutoff.
    assert len(repository.record_calls) == 1
    assert len(repository.saved_states) == 1
    assert repository.saved_states[0]["state_date"] == SOURCE_DATE.isoformat()
    assert record["generated_at"] < CUTOFF_0940
    assert record["receipt_sealed_at"] < CUTOFF_0940
    eligible = await repository.mews_snapshot_is_eligible(
        snapshot_id, source_trade_date=SOURCE_DATE, cutoff=CUTOFF_0940
    )
    assert eligible is True

    # A repeated scheduled attempt reuses the cache without recomputing.
    again = await asyncio.wait_for(
        service._refresh_mews_cache_once(T_0915, CALENDAR), timeout=STEP_TIMEOUT
    )
    assert again is False
    assert len(created) == 1
    assert len(repository.record_calls) == 1
    assert len(repository.saved_states) == 1


async def test_first_deploy_1404_trigger_recomputes_identical_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Reference: the same source facts computed inside the 09:10 window.
    early_repository = FakeMewsRepository(_deterministic_state(), sealed_clock=lambda: T_0915)
    early_service = _build_service(
        monkeypatch,
        repository=early_repository,
        raw_factory=_plain_factory([]),
        now=T_0915,
    )
    assert await asyncio.wait_for(
        early_service._refresh_mews_cache_once(T_0915, CALENDAR), timeout=STEP_TIMEOUT
    )

    # First deploy at 14:04: long after the 09:40 cutoff, the trigger must
    # still recompute and persist today's MEWS on the spot.
    late_repository = FakeMewsRepository(_deterministic_state(), sealed_clock=lambda: T_1404)
    late_service = _build_service(
        monkeypatch,
        repository=late_repository,
        raw_factory=_plain_factory([]),
        now=T_1404,
    )
    kicked = await late_service.ensure_mews_for_selection_trigger(T_1404)
    assert kicked is True, "late trigger awaits the recomputation, no cache-missed"

    assert late_service._mews_cached_for == TODAY
    assert late_service._mews_source_trade_date == SOURCE_DATE
    assert late_service._mews_last_failure is None
    assert late_service._mews_failed_for is None
    assert late_service._lane_health["mews_cache"].last_error is None
    assert late_repository.alerts == {}

    snapshot_id = late_service._mews_snapshot_id
    assert snapshot_id is not None, "no -12% fallback / no MEWS_0910_CACHE_MISSED"
    assert snapshot_id in late_repository.snapshots
    late_record = late_repository.snapshots[snapshot_id]

    # Time-independence: identical source facts yield the identical value,
    # level, and content hash at any wall clock.
    early_record = early_repository.snapshots[snapshot_id]
    assert late_record["payload"]["data_version"] == early_record["payload"]["data_version"]
    assert late_record["payload"]["evidence"] == early_record["payload"]["evidence"]
    assert late_record["payload"]["fast_state"] == early_record["payload"]["fast_state"]
    assert late_record["payload"]["generated_at"] != early_record["payload"]["generated_at"]

    # The recomputation itself is never skipped by the 09:40 cutoff, while the
    # lateness stays visible for downstream PIT eligibility.
    assert late_record["generated_at"] >= CUTOFF_0940
    assert late_record["receipt_sealed_at"] >= CUTOFF_0940
    eligible = await late_repository.mews_snapshot_is_eligible(
        snapshot_id, source_trade_date=SOURCE_DATE, cutoff=CUTOFF_0940
    )
    assert eligible is False
    assert len(late_repository.record_calls) == 1
    assert len(late_repository.saved_states) == 1

    # Once computed, later triggers reuse the same-day cache directly.
    assert await late_service.ensure_mews_for_selection_trigger(T_1404) is True
    assert len(late_repository.record_calls) == 1


async def test_concurrent_triggers_and_scheduler_singleflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = FakeMewsRepository(_deterministic_state(), sealed_clock=lambda: T_1404)
    created: list[GatedRawClient] = []

    def gated_factory() -> GatedRawClient:
        client = GatedRawClient()
        created.append(client)
        return client

    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=gated_factory,
        now=T_1404,
    )

    first = asyncio.create_task(service.ensure_mews_for_selection_trigger(T_1404))

    # Wait until the single computation holds the refresh lock inside the raw
    # query, then unleash the competitors while it is still in flight.
    for _ in range(5000):
        if created and created[0].entered_margin.is_set():
            break
        await asyncio.sleep(0.001)
    else:
        raise AssertionError("MEWS computation never reached the raw source")

    second = asyncio.create_task(service.ensure_mews_for_selection_trigger(T_1404))
    recovery = asyncio.create_task(
        service._recover_mews_after_cutoff_once(T_1404, CALENDAR),
        name="v20-mews-scheduler-recovery",
    )
    await asyncio.sleep(0)
    created[0].release.set()

    first_result, second_result, recovered = await asyncio.wait_for(
        asyncio.gather(first, second, recovery), timeout=STEP_TIMEOUT
    )
    # Both triggers joined the same per-date singleflight task and awaited it.
    assert first_result is True
    assert second_result is True
    assert recovered is True
    assert service._mews_singleflight_task is None

    # Singleflight: exactly one raw computation, one checkpoint, one snapshot.
    assert len(created) == 1
    raw_calls = created[0].calls
    assert raw_calls.count("margin") == 1
    assert raw_calls.count("margin_detail") == 1
    assert raw_calls.count("daily_basic") == 1
    assert len(repository.saved_states) == 1
    assert len(repository.record_calls) == 1
    assert len(repository.snapshots) == 1
    assert repository.alerts == {}
    assert service._mews_last_failure is None

    # Every caller observes the same immutable result.
    snapshot_id = service._mews_snapshot_id
    assert snapshot_id == next(iter(repository.snapshots))
    assert repository.snapshots[snapshot_id]["payload"] == repository.record_calls[0]
    assert service._mews_cached_for == TODAY
    assert await service.ensure_mews_for_selection_trigger(T_1404) is True
    assert len(repository.record_calls) == 1


async def test_valid_persisted_cache_is_restored_without_recompute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = FakeMewsRepository(_deterministic_state(), sealed_clock=lambda: T_0915)
    created: list[DeterministicRawClient] = []
    producer = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory(created),
        now=T_0915,
    )
    assert await asyncio.wait_for(
        producer._refresh_mews_cache_once(T_0915, CALENDAR), timeout=STEP_TIMEOUT
    )
    original_snapshot_id = producer._mews_snapshot_id
    persisted_payload = repository.snapshots[original_snapshot_id]["payload"]
    assert len(repository.record_calls) == 1

    # A fresh process (empty in-memory cache) must reattach to the sealed
    # same-day snapshot instead of recomputing it.
    def forbidden_factory() -> Any:
        raise AssertionError("valid same-day MEWS cache must be reused, not recomputed")

    consumer = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=forbidden_factory,
        now=T_1404,
    )
    assert consumer._mews_cached_for is None
    # The awaited trigger restores the sealed same-day snapshot on the spot.
    assert await consumer.ensure_mews_for_selection_trigger(T_1404) is True

    assert consumer._mews_cached_for == TODAY
    assert consumer._mews_source_trade_date == SOURCE_DATE
    assert consumer._mews_snapshot_id == original_snapshot_id
    assert consumer._mews_last_failure is None
    assert repository.alerts == {}
    assert len(repository.record_calls) == 1
    assert len(repository.saved_states) == 1
    assert len(repository.snapshots) == 1
    assert repository.snapshots[original_snapshot_id]["payload"] == persisted_payload
    assert await consumer.ensure_mews_for_selection_trigger(T_1404) is True


async def test_on_time_candidate_uses_repository_eligibility_without_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _GuardConnection(None)
    repository = _GuardedCandidateRepository(
        connection=connection,
        repository_eligible=True,
    )
    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory([]),
        now=T_1404,
    )

    assert await service._restore_mews_cache_once(T_1404, CALENDAR) is True
    assert repository.eligibility_calls == 1
    assert connection.calls == 0
    assert service._mews_snapshot_id == "guarded-candidate"


async def test_late_sealed_same_day_candidate_uses_receipt_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _GuardConnection(
        _AsyncpgLikeRow(
            {
                "source_trade_date": SOURCE_DATE,
                "generated_at": T_1404,
                "receipt_sealed_at": T_1404,
                "signal_available_date": TODAY.isoformat(),
            }
        )
    )
    repository = _GuardedCandidateRepository(
        connection=connection,
        repository_eligible=False,
    )
    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory([]),
        now=T_1404,
    )

    assert await service._restore_mews_cache_once(T_1404, CALENDAR) is True
    assert repository.eligibility_calls == 1
    assert connection.calls == 1
    assert service._mews_snapshot_id == "guarded-candidate"


async def test_late_unsealed_same_day_candidate_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _GuardConnection(
        _AsyncpgLikeRow(
            {
                "source_trade_date": SOURCE_DATE,
                "generated_at": T_1404,
                "receipt_sealed_at": None,
                "signal_available_date": TODAY.isoformat(),
            }
        )
    )
    repository = _GuardedCandidateRepository(
        connection=connection,
        repository_eligible=False,
    )
    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory([]),
        now=T_1404,
    )

    assert await service._restore_mews_cache_once(T_1404, CALENDAR) is False
    assert service._mews_cached_for is None
    assert service._mews_snapshot_id is None


async def test_candidate_without_real_pool_does_not_invoke_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = _GuardedCandidateRepository(
        connection=_GuardConnection(None),
        repository_eligible=False,
    )
    repository.schema = None
    repository.pool = None
    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory([]),
        now=T_1404,
    )

    assert await service._restore_mews_cache_once(T_1404, CALENDAR) is False
    assert repository.eligibility_calls == 1
    assert service._mews_cached_for is None


async def test_receipt_guard_query_failure_fails_closed_and_alerts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingConnection(_GuardConnection):
        async def fetchrow(self, *_args: Any) -> Any:
            await super().fetchrow(*_args)
            raise RuntimeError("receipt storage unavailable")

    repository = _GuardedCandidateRepository(
        connection=FailingConnection(None),
        repository_eligible=False,
    )
    created: list[DeterministicRawClient] = []
    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory(created),
        now=T_1404,
    )

    assert await service.ensure_mews_for_selection_trigger(T_1404) is False
    assert created == []
    assert service._mews_cached_for is None
    assert service._mews_snapshot_id is None
    assert len(repository.alerts) == 1
    assert "MEWS same-day receipt guard failed" in service._mews_last_failure


def _corrupt_content(state: dict[str, Any]) -> None:
    state["market_history"] = {"not": "a list"}


def _corrupt_date(state: dict[str, Any]) -> None:
    state["state_date"] = "2026-09-03"


def _corrupt_source(state: dict[str, Any]) -> None:
    state["state_date"] = "2026-08-31"
    state["market_history"][-1]["trade_date"] = "2026-08-31"
    state["calculated_at"] = "2026-08-31T15:30:00+08:00"


@pytest.mark.parametrize(
    ("corruptor", "expected_error"),
    [
        (_corrupt_content, "payload is invalid"),
        (_corrupt_date, "ahead of the requested source date"),
        (_corrupt_source, "availability date is inconsistent"),
    ],
    ids=["content", "date", "stale-source"],
)
async def test_invalid_persisted_state_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    corruptor: Callable[[dict[str, Any]], None],
    expected_error: str,
) -> None:
    state = _deterministic_state()
    corruptor(state)
    repository = FakeMewsRepository(state, sealed_clock=lambda: T_1404)
    created: list[DeterministicRawClient] = []
    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory(created),
        now=T_1404,
    )

    kicked = await service.ensure_mews_for_selection_trigger(T_1404)
    assert kicked is False

    # Fail closed: no snapshot, no cache, no silent reuse of the bad source.
    assert service._mews_cached_for is None
    assert service._mews_snapshot_id is None
    assert repository.snapshots == {}
    assert repository.record_calls == []
    assert repository.saved_states == []
    assert created == [], "a state that fails validation must never reach the raw source"
    assert service._mews_last_failure is not None
    assert "MewsSnapshotSourceError" in service._mews_last_failure
    assert expected_error in service._mews_last_failure

    # Exactly one stable daily alert; the failure is idempotent.
    assert len(repository.alerts) == 1
    semantic = next(iter(repository.alerts.values()))
    assert semantic["alert_code"] == "MEWS_CALCULATION_FAILED"
    assert expected_error in str(semantic["message"])

    # A later distinct trigger retries the attempt (no permanent daily skip);
    # the corrupt state fails validation again before reaching the raw source,
    # and the daily alert stays idempotent.
    assert await service.ensure_mews_for_selection_trigger(T_1404) is False
    assert service._mews_singleflight_task is None
    assert created == []
    assert len(repository.alerts) == 1
    assert repository.snapshots == {}


async def test_unsealed_same_day_repair_candidate_is_rejected_and_locally_resealed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unsealed_snapshot_id = "unsealed-same-day-repair"

    class StrictUnsealedCandidateRepository(FakeMewsRepository):
        def __init__(self) -> None:
            super().__init__(_deterministic_state(), sealed_clock=lambda: T_1404)
            self.find_calls: list[dict[str, Any]] = []
            self.eligibility_calls: list[dict[str, Any]] = []
            self.snapshots[unsealed_snapshot_id] = {
                "payload": {
                    "snapshot_id": unsealed_snapshot_id,
                    "source_trade_date": SOURCE_DATE.isoformat(),
                    "generated_at": T_0915.isoformat(),
                    "evidence": {"signal_available_date": TODAY.isoformat()},
                },
                "content_hash": "unsealed" * 8,
                "source_trade_date": SOURCE_DATE,
                "generated_at": T_0915,
                "receipt_sealed_at": None,
            }

        async def find_eligible_mews_snapshot(
            self,
            *,
            source_trade_date: date,
            cutoff: datetime,
            availability_date: date | None = None,
        ) -> str | None:
            self.find_calls.append(
                {
                    "source_trade_date": source_trade_date,
                    "cutoff": cutoff,
                    "availability_date": availability_date,
                }
            )
            return unsealed_snapshot_id

        async def mews_snapshot_is_eligible(
            self,
            snapshot_id: str,
            *,
            source_trade_date: date,
            cutoff: datetime,
        ) -> bool:
            self.eligibility_calls.append(
                {
                    "snapshot_id": snapshot_id,
                    "source_trade_date": source_trade_date,
                    "cutoff": cutoff,
                }
            )
            record = self.snapshots.get(snapshot_id)
            return bool(
                record is not None
                and record["receipt_sealed_at"] is not None
                and record["source_trade_date"] == source_trade_date
            )

    repository = StrictUnsealedCandidateRepository()
    created: list[DeterministicRawClient] = []
    service = _build_service(
        monkeypatch,
        repository=repository,
        raw_factory=_plain_factory(created),
        now=T_1404,
    )
    source = service._mews_source
    assert isinstance(source, LocalMewsSnapshotCalculator)
    source_fetch = source.fetch_snapshot
    source_calls: list[dict[str, date]] = []

    async def tracked_fetch_snapshot(
        *, source_trade_date: date, availability_date: date
    ) -> Mapping[str, Any]:
        source_calls.append(
            {
                "source_trade_date": source_trade_date,
                "availability_date": availability_date,
            }
        )
        return await source_fetch(
            source_trade_date=source_trade_date,
            availability_date=availability_date,
        )

    monkeypatch.setattr(source, "fetch_snapshot", tracked_fetch_snapshot)

    repaired = await asyncio.wait_for(
        service.ensure_mews_for_selection_trigger(T_1404), timeout=STEP_TIMEOUT
    )
    assert repaired is True

    expected_find = {
        "source_trade_date": SOURCE_DATE,
        "cutoff": CUTOFF_0940,
        "availability_date": TODAY,
    }
    assert repository.find_calls == [expected_find]
    assert repository.eligibility_calls[0] == {
        "snapshot_id": unsealed_snapshot_id,
        "source_trade_date": SOURCE_DATE,
        "cutoff": CUTOFF_0940,
    }
    assert service._mews_snapshot_id != unsealed_snapshot_id
    assert service._mews_cached_for == TODAY
    assert service._mews_source_trade_date == SOURCE_DATE
    assert source_calls == [{"source_trade_date": SOURCE_DATE, "availability_date": TODAY}]
    assert len(created) == 1
    assert len(repository.record_calls) == 1
    assert repository.eligibility_calls[-1]["snapshot_id"] == service._mews_snapshot_id
    sealed_record = repository.snapshots[service._mews_snapshot_id]
    assert sealed_record["receipt_sealed_at"] is not None
    assert repository.snapshots[unsealed_snapshot_id]["receipt_sealed_at"] is None
    assert repository.alerts == {}

    class RepositorySnapshotConnection(_GuardConnection):
        def __init__(self, snapshot_repository: FakeMewsRepository) -> None:
            super().__init__(None)
            self.snapshot_repository = snapshot_repository

        async def fetchrow(self, snapshot_id: str, *_args: Any) -> Any:
            record = self.snapshot_repository.snapshots.get(snapshot_id)
            if record is None:
                return None
            return _AsyncpgLikeRow(
                {
                    "source_trade_date": record["source_trade_date"],
                    "generated_at": record["generated_at"],
                    "receipt_sealed_at": record["receipt_sealed_at"],
                    "signal_available_date": record["payload"]
                    .get("evidence", {})
                    .get("signal_available_date"),
                }
            )

    class NeverSealingRepository(StrictUnsealedCandidateRepository):
        async def record_mews_snapshot(self, payload: Mapping[str, Any]) -> str:
            content_hash = await super().record_mews_snapshot(payload)
            self.snapshots[str(payload["snapshot_id"])]["receipt_sealed_at"] = None
            return content_hash

        async def mews_snapshot_is_eligible(
            self,
            snapshot_id: str,
            *,
            source_trade_date: date,
            cutoff: datetime,
        ) -> bool:
            await super().mews_snapshot_is_eligible(
                snapshot_id,
                source_trade_date=source_trade_date,
                cutoff=cutoff,
            )
            return False

    never_sealing_repository = NeverSealingRepository()
    never_sealing_repository.schema = "v20"
    never_sealing_repository.pool = _GuardPool(
        RepositorySnapshotConnection(never_sealing_repository)
    )
    failing_created: list[DeterministicRawClient] = []
    never_sealing_service = _build_service(
        monkeypatch,
        repository=never_sealing_repository,
        raw_factory=_plain_factory(failing_created),
        now=T_1404,
    )
    unprovable = await asyncio.wait_for(
        never_sealing_service.ensure_mews_for_selection_trigger(T_1404),
        timeout=STEP_TIMEOUT,
    )
    assert unprovable is False
    assert never_sealing_service._mews_cached_for is None
    assert never_sealing_service._mews_snapshot_id is None
    assert len(failing_created) == 1
    assert len(never_sealing_repository.record_calls) == 1
    assert never_sealing_repository.record_calls[0]["snapshot_id"] != unsealed_snapshot_id
    assert never_sealing_repository.snapshots[unsealed_snapshot_id]["receipt_sealed_at"] is None
    assert len(never_sealing_repository.alerts) == 1

    assert service._mews_singleflight_task is None
    assert never_sealing_service._mews_singleflight_task is None
    current = asyncio.current_task()
    assert [task for task in asyncio.all_tasks() if task is not current and not task.done()] == []
