"""V22 must survive the actual production projection and portable codec."""

import json
from dataclasses import replace
from datetime import datetime

import pytest

from src.data.clients.tushare_realtime import TushareMinuteBar
from src.data.database.v20_repository import V20SemanticConflict, sha256_json
from src.web.v20_v16_canonical_artifact import encode, hydrate
from tests.unit.web.test_v20_canonical_projection_acceptance import (
    FULL_EXCHANGE_CALENDAR,
    RECOMMENDED,
    TRADE_DATE,
    TZ,
    _canonical,
    _rehash,
)
from tests.unit.web.test_v20_service import PROJECT_ROOT, _service


def v22_canonical(*, with_signal=True):
    original = _canonical(recommended=RECOMMENDED if with_signal else ())
    codes = sorted(set(original.early_bars) | {f"{index:06}" for index in range(1, 1001)})
    bars = {
        code: tuple(
            TushareMinuteBar(
                stock_code=code,
                bar_end=datetime.combine(TRADE_DATE, datetime.min.time(), tzinfo=TZ).replace(
                    hour=9, minute=minute
                ),
                end_label=f"09:{minute:02}",
                open_price=10.0,
                close_price=10.0,
                high_price=10.0,
                low_price=10.0,
                volume=100.0,
                amount=1000.0,
            )
            for minute in range(30, 40)
        )
        for code in codes
    }
    manifest = json.loads((PROJECT_ROOT / "models/v22_slim/manifest.json").read_text())
    return _rehash(
        replace(
            original,
            model_sha256=manifest["sha256"]["lgbrank_latest.txt"],
            feature_list_sha256=manifest["sha256"]["feature_list.json"],
            early_bars=bars,
            early_source_hashes={code: "e" * 64 for code in codes},
        )
    )


@pytest.mark.parametrize("with_signal", [True, False])
def test_v22_production_projection_survives_portable_save_and_restart(monkeypatch, with_signal):
    service = _service(monkeypatch, object())
    canonical = v22_canonical(with_signal=with_signal)
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    payload = encode(
        projected,
        calendar=FULL_EXCHANGE_CALENDAR,
        canonical_integrity_hash=canonical._integrity_hash,
    )
    # A JSON roundtrip models the persistence boundary, not a custom hydrate fake.
    restored = hydrate(json.loads(json.dumps(payload))).bundle
    assert restored.snapshot == projected.snapshot
    assert restored.snapshot_hash == projected.snapshot_hash
    assert restored.scan_result.recommended == canonical.scan_result.recommended
    assert len(restored.snapshot["v22_market"]) == len(canonical.early_bars)
    assert restored.snapshot["v22_market"]["000001"] == {"close": 10.0, "amount": 10000.0}


@pytest.mark.parametrize(
    "mutation",
    [
        lambda snapshot: snapshot.pop("v22_market"),
        lambda snapshot: snapshot.update(unknown_extension={}),
        lambda snapshot: snapshot.update(scorer_model_sha256="1" * 64),
        lambda snapshot: snapshot.update(v22_market={}),
        lambda snapshot: snapshot["v22_market"].update({"999999": {"close": 1, "amount": 1}}),
        lambda snapshot: snapshot["v22_market"]["000001"].update(extra=1),
        lambda snapshot: snapshot["v22_market"]["000001"].pop("close"),
        lambda snapshot: snapshot["v22_market"]["000001"].update(close=0),
        lambda snapshot: snapshot["v22_market"]["000001"].update(close=True),
        lambda snapshot: snapshot["v22_market"]["000001"].update(amount=-1),
        lambda snapshot: snapshot["v22_market"]["000001"].update(amount="100"),
        lambda snapshot: snapshot["v22_market"]["000001"].update(amount=float("nan")),
    ],
)
def test_v22_codec_still_rejects_missing_unknown_or_invalid_market_inputs(monkeypatch, mutation):
    service = _service(monkeypatch, object())
    canonical = v22_canonical()
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    payload = encode(
        projected,
        calendar=FULL_EXCHANGE_CALENDAR,
        canonical_integrity_hash=canonical._integrity_hash,
    )
    mutation(payload["v20_snapshot"])
    try:
        payload["v20_snapshot_hash"] = sha256_json(payload["v20_snapshot"])
    except ValueError:
        # Non-finite values must fail validation before JSON hashing.
        payload["v20_snapshot_hash"] = "f" * 64
    with pytest.raises(V20SemanticConflict):
        hydrate(payload)


def test_v22_market_values_remain_bound_to_snapshot_hash(monkeypatch):
    service = _service(monkeypatch, object())
    canonical = v22_canonical()
    projected = service._project_canonical_v16(canonical, calendar=FULL_EXCHANGE_CALENDAR)
    payload = encode(
        projected,
        calendar=FULL_EXCHANGE_CALENDAR,
        canonical_integrity_hash=canonical._integrity_hash,
    )
    payload["v20_snapshot"]["v22_market"]["000001"]["amount"] += 1
    with pytest.raises(V20SemanticConflict, match="snapshot hash"):
        hydrate(payload)
