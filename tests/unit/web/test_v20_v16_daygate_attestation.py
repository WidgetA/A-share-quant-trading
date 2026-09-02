from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.strategy.v16_day_gate_evidence import (
    append_v16_day_gate_evidence,
    build_v16_day_gate_evidence,
    compute_v16_day_gate_evidence_hash,
)
from src.strategy.v16_day_gate_shadow import (
    freeze_v16_day_gate_runtime,
    freeze_v16_scan_snapshot,
    load_shadow_config,
    prepare_shadow_decision,
    prepared_to_metadata,
)
from src.web import v20_v16_daygate_attestation as attestation
from src.web.v15_scan_service import (
    CanonicalV16ScanBundle,
    _build_v16_recommendation_payload,
)
from src.web.v20_v16_daygate_attestation import (
    V16DayGateAttestationError,
    attest_post_cutoff_v16_day_gate,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 8, 25)
FROZEN_AT = datetime(2026, 8, 25, 9, 40, tzinfo=SHANGHAI)
MODEL_HASH = "1" * 64
FEATURE_HASH = "2" * 64
CODES = tuple(f"{300000 + index:06d}" for index in range(10))


def _project(tmp_path: Path) -> Path:
    source = Path("config/v16-day-gate.yaml").read_text(encoding="utf-8")
    source = source.replace("data/v16_day_gate", str((tmp_path / "evidence").as_posix()))
    config = tmp_path / "config" / "v16-day-gate.yaml"
    config.parent.mkdir()
    config.write_text(source, encoding="utf-8")
    return tmp_path


def _scored(code: str, rank: int, score: float | None = None) -> SimpleNamespace:
    score = 10.0 - rank if score is None else score
    return SimpleNamespace(
        code=code, name=f"stock-{code}", score=score, rank=rank, buy_price=20.0 + rank
    )


def _stock() -> SimpleNamespace:
    return SimpleNamespace(
        open_price=20.0,
        prev_close=19.0,
        price_940=21.0,
        high_940=21.2,
        low_940=19.8,
        volume_940=100000.0,
        volume_0937=70000.0,
        volume_937=70000.0,
    )


def _scan(score: float = 9.0, board: str = "board-a") -> SimpleNamespace:
    rows = [_scored(code, rank) for rank, code in enumerate(CODES, 1)]
    rows[0].score = score
    return SimpleNamespace(
        recommended=rows,
        stock_best_board={code: board for code in CODES},
        stock_all_boards={code: [board, f"all-{code}"] for code in CODES},
        stock_gain_from_open={code: float(rank) for rank, code in enumerate(CODES, 1)},
        stock_is_driver={code: index == 0 for index, code in enumerate(CODES)},
        stock_cci={code: float(rank * 10) for rank, code in enumerate(CODES, 1)},
        stock_early_vol={code: float(rank) for rank, code in enumerate(CODES, 1)},
        step2_board_avg_gains={board: 1.1},
        step2_all_board_avg_gains={board: 1.2},
        step2_boards_detail={board: list(CODES)},
        step0_universe_count=100,
        step2_hot_board_count=1,
        step2_filtered_by_avg_gain=2,
        step3_count=30,
        step4_count=20,
        step5_count=15,
        step6_count=12,
        step6_5_count=11,
        step6_6_count=10,
        final_candidates=10,
    )


def _recommendation(scan: SimpleNamespace) -> dict[str, object]:
    return _build_v16_recommendation_payload(scan, {scan.recommended[0].code: _stock()})


def _canonical(scan: SimpleNamespace) -> CanonicalV16ScanBundle:
    return CanonicalV16ScanBundle(
        trade_date=TRADE_DATE,
        scan_result=scan,
        stock_data={code: _stock() for code in CODES},
        clean_boards={},
        universe=CODES,
        quotes={},
        prev_closes={code: 19.0 for code in CODES},
        history_raw={},
        early_bars={},
        early_source_hashes={},
        failed_no_prev_close=(),
        failed_no_history=(),
        failed_build=(),
        skipped_new_listings=(),
        model_sha256=MODEL_HASH,
        feature_list_sha256=FEATURE_HASH,
        computed_at=FROZEN_AT,
        input_hash="3" * 64,
        _integrity_hash="4" * 64,
    )


def _write_evidence(
    project: Path,
    scan: SimpleNamespace,
    frozen_at=FROZEN_AT,
    stock_data: dict[str, SimpleNamespace] | None = None,
) -> Path:
    if stock_data is None:
        stock_data = {code: _stock() for code in CODES}
    snapshot = freeze_v16_scan_snapshot(
        scan,
        stock_data,
        _recommendation(scan),
        frozen_at=frozen_at,
    )
    config = replace(load_shadow_config(project), send_feishu=False)
    runtime = freeze_v16_day_gate_runtime(
        project,
        ranking_model_sha256=MODEL_HASH,
        ranking_feature_list_sha256=FEATURE_HASH,
        captured_at=frozen_at,
        config=config,
    )
    assert runtime is not None
    prepared = prepare_shadow_decision(snapshot, runtime, project)
    snapshot["shadow_evaluation"] = prepared_to_metadata(prepared)
    record = build_v16_day_gate_evidence(
        gate_input=prepared.gate_input,
        decision=prepared.decision,
        frozen_snapshot=snapshot,
        evaluated_at=frozen_at,
        scanner_version="scanner-test",
        model_version=MODEL_HASH,
        taxonomy_version=None,
        policy_version=prepared.decision.policy_version,
    )
    return append_v16_day_gate_evidence(config.evidence_dir, record)


def _rewrite(path: Path, mutate, *, rehash: bool = True) -> None:
    record = json.loads(path.read_text(encoding="utf-8"))
    mutate(record)
    if rehash:
        record["content_sha256"] = compute_v16_day_gate_evidence_hash(record)
        path.unlink()
        append_v16_day_gate_evidence(path.parents[1], record)
    else:
        path.write_text(
            json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8"
        )


def _attest(project: Path, canonical: CanonicalV16ScanBundle):
    return attest_post_cutoff_v16_day_gate(project, canonical, TRADE_DATE, TRADE_DATE)


def test_passes_with_real_evidence_writer_reader_and_honest_limitation(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    result = _attest(project, _canonical(scan))
    assert result["status"] == "PASS"
    assert result["evidence_relative_path"] == path.parent.name + "/" + path.name
    assert result["limitation"]["code"] == (
        "V16_DAY_GATE_EVIDENCE_ATTESTS_ORDERED_OUTPUT_NOT_FULL_READY_UNIVERSE"
    )
    assert "full ready" in result["limitation"]["text"]


def test_missing_evidence_is_fail_closed(tmp_path):
    project = _project(tmp_path)
    with pytest.raises(V16DayGateAttestationError, match=r"^V16_DAY_GATE_EVIDENCE_MISSING:") as exc:
        _attest(project, _canonical(_scan()))
    assert exc.value.reason == "V16_DAY_GATE_EVIDENCE_MISSING"


@pytest.mark.parametrize(
    ("mutate", "label"),
    [
        (lambda r: r.__setitem__("content_sha256", "0" * 64), "bad_hash"),
        (lambda r: r["frozen_snapshot"].__setitem__("schema_version", "bad"), "schema"),
        (lambda r: r["frozen_snapshot"].__setitem__("frozen_at", "2026-08-25T09:40:00"), "naive"),
        (
            lambda r: r["frozen_snapshot"].__setitem__(
                "decision_cutoff", "2026-08-25T09:40:01+08:00"
            ),
            "instant",
        ),
        (lambda r: r["frozen_snapshot"]["top_k"].pop(), "top10"),
        (lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("rank", 2), "rank"),
        (lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("score", True), "bool"),
        (lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("score", 1e999), "nonfinite"),
        (
            lambda r: r["frozen_snapshot"]["hot_board_member_counts"].__setitem__("board-a", True),
            "members",
        ),
    ],
)
def test_invalid_candidate_and_crossmatch_failures(tmp_path, mutate, label):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    _rewrite(path, mutate, rehash=label not in {"bad_hash", "nonfinite"})
    with pytest.raises(V16DayGateAttestationError, match=r"^V16_DAY_GATE_EVIDENCE_INVALID:"):
        _attest(project, _canonical(scan))


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("name", "different"),
            id="name",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("best_board", "other"),
            id="best-board",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["top_k"][0]["all_hot_boards"].reverse(),
            id="boards",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("is_driver", False),
            id="driver",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("score", 8.0),
            id="numeric",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["funnel_counts"].__setitem__(
                "step0_universe_count", 101
            ),
            id="funnel-map",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["hot_board_member_counts"].__setitem__("board-a", 9),
            id="member-map",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["board_avg_gains"].__setitem__("board-a", 1.3),
            id="board-gain-map",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["all_board_avg_gains"].__setitem__("board-a", 1.4),
            id="all-board-gain-map",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["shadow_evaluation"]["provenance"].__setitem__(
                "model_hash", "5" * 64
            ),
            id="model-hash",
        ),
        pytest.param(
            lambda r: r["frozen_snapshot"]["shadow_evaluation"]["provenance"].__setitem__(
                "feature_hash", "6" * 64
            ),
            id="feature-hash",
        ),
    ],
)
def test_legal_evidence_mismatches_are_semantic_conflicts(tmp_path, mutate):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    _rewrite(path, mutate)
    with pytest.raises(
        V16DayGateAttestationError,
        match=r"^V16_DAY_GATE_EVIDENCE_MISMATCH:",
    ) as exc:
        _attest(project, _canonical(scan))
    assert exc.value.reason == "V16_DAY_GATE_EVIDENCE_MISMATCH"


def test_evidence_rank_must_match_canonical_scored_item(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    scan.recommended[1].rank = 3
    path = _write_evidence(project, scan)
    _rewrite(
        path,
        lambda r: r["frozen_snapshot"]["top_k"][1].__setitem__("rank", 2),
    )
    with pytest.raises(
        V16DayGateAttestationError,
        match=r"^V16_DAY_GATE_EVIDENCE_MISMATCH:",
    ):
        _attest(project, _canonical(scan))


def test_market_snapshot_null_shape_is_preserved(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    canonical = _canonical(scan)
    canonical.stock_data.pop(CODES[1])
    for code in CODES[2:]:
        canonical.stock_data.pop(code)
    path = _write_evidence(project, scan, stock_data={CODES[0]: _stock()})
    _rewrite(
        path,
        lambda r: r["frozen_snapshot"]["top_k"][1].__setitem__("market_snapshot", None),
    )
    with pytest.raises(
        V16DayGateAttestationError,
        match=r"^V16_DAY_GATE_EVIDENCE_INVALID:",
    ):
        _attest(project, canonical)


def test_bad_path_fails_before_crossmatch(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    renamed = path.with_name("999999_wrong.json")
    path.rename(renamed)
    with pytest.raises(V16DayGateAttestationError, match=r"^V16_DAY_GATE_EVIDENCE_INVALID:"):
        _attest(project, _canonical(scan))


def test_numeric_tolerance_boundary(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    _rewrite(path, lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("score", 9.0000000001))
    assert _attest(project, _canonical(scan))["status"] == "PASS"


def test_earliest_mismatch_is_not_skipped(tmp_path):
    project = _project(tmp_path)
    first_scan = _scan(score=9.0)
    second_scan = _scan(score=9.5)
    _write_evidence(project, first_scan, frozen_at=FROZEN_AT)
    _write_evidence(project, second_scan, frozen_at=FROZEN_AT.replace(second=1))
    with pytest.raises(V16DayGateAttestationError, match=r"^V16_DAY_GATE_EVIDENCE_MISMATCH:"):
        _attest(project, _canonical(second_scan))


def test_content_hash_breaks_frozen_at_order_tie(tmp_path):
    project = _project(tmp_path)
    high = _scan()
    low = _scan(score=8.5)
    low_path = _write_evidence(project, low)
    high_path = _write_evidence(project, high)
    evidence_by_path = {low_path: low, high_path: high}
    winner_path = min(evidence_by_path, key=lambda path: path.name.split("_")[1][:12])
    result = _attest(project, _canonical(evidence_by_path[winner_path]))
    assert result["status"] == "PASS"
    assert winner_path.name.split("_")[1][:12] in result["evidence_content_sha256"]


@pytest.mark.parametrize(
    ("constant", "value"),
    [
        ("_MAX_DATE_SPAN_DAYS", 0),
        ("_MAX_CANDIDATE_FILES", 0),
        ("_MAX_CANDIDATE_BYTES", 10),
        ("_MAX_TOTAL_CANDIDATE_BYTES", 10),
    ],
)
def test_named_bounds_fail_closed(tmp_path, monkeypatch, constant, value):
    project = _project(tmp_path)
    scan = _scan()
    _write_evidence(project, scan)
    monkeypatch.setattr(attestation, constant, value)
    with pytest.raises(V16DayGateAttestationError, match=r"^V16_DAY_GATE_EVIDENCE_INVALID:"):
        attest_post_cutoff_v16_day_gate(
            project, _canonical(scan), TRADE_DATE, TRADE_DATE + timedelta(days=1)
        )


def test_date_enumeration_crosses_midnight_and_json_native(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    _write_evidence(project, scan, frozen_at=FROZEN_AT)
    result = attest_post_cutoff_v16_day_gate(
        project, _canonical(scan), TRADE_DATE, TRADE_DATE + timedelta(days=1)
    )
    assert json.dumps(result, allow_nan=False)
    assert copy.deepcopy(result) == result


def _expect(project: Path, canonical: CanonicalV16ScanBundle, reason: str) -> None:
    with pytest.raises(V16DayGateAttestationError) as exc:
        _attest(project, canonical)
    assert exc.value.reason == reason


def test_valid_target_ignores_valid_other_date_sibling(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    _write_evidence(project, scan, frozen_at=FROZEN_AT - timedelta(days=1))
    _write_evidence(project, scan)
    assert _attest(project, _canonical(scan))["status"] == "PASS"


def test_corrupt_sibling_and_target_classification(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    corrupt = _write_evidence(project, scan, frozen_at=FROZEN_AT - timedelta(days=1))
    corrupt.write_text("corrupt", encoding="utf-8")
    valid = _write_evidence(project, scan)
    assert _attest(project, _canonical(scan))["status"] == "PASS"

    invalid = _write_evidence(project, scan)
    valid.write_text("corrupt", encoding="utf-8")
    invalid.write_text("corrupt", encoding="utf-8")
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_INVALID")


def test_only_valid_other_date_evidence_is_missing(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    _write_evidence(project, scan, frozen_at=FROZEN_AT - timedelta(days=1))
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_MISSING")


def test_model_identity_binds_versions_gate_input_and_canonical(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    changed = "7" * 64
    _rewrite(
        path,
        lambda r: (
            r["versions"].__setitem__("model", changed),
            r["gate_input"].__setitem__("model_version", changed),
        ),
    )
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_MISMATCH")


def test_canonical_trade_date_must_equal_requested_trade_date(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    _write_evidence(project, scan)
    _expect(
        project,
        replace(_canonical(scan), trade_date=TRADE_DATE + timedelta(days=1)),
        "V16_DAY_GATE_EVIDENCE_INVALID",
    )


@pytest.mark.parametrize("extra", [False, True])
def test_top_row_key_set_is_exact(tmp_path, extra):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)

    def mutation(record: dict) -> None:
        if extra:
            record["frozen_snapshot"]["top_k"][0]["extra"] = True
        else:
            record["frozen_snapshot"]["top_k"][0].pop("cci_14")

    _rewrite(path, mutation)
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_INVALID")


def test_absent_market_key_passes_and_explicit_null_is_invalid(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    canonical = _canonical(scan)
    canonical.stock_data.pop(CODES[1])
    for code in CODES[2:]:
        canonical.stock_data.pop(code)
    path = _write_evidence(project, scan, stock_data={CODES[0]: _stock()})
    assert _attest(project, canonical)["status"] == "PASS"

    _rewrite(path, lambda r: r["frozen_snapshot"]["top_k"][1].__setitem__("market_snapshot", None))
    _expect(project, canonical, "V16_DAY_GATE_EVIDENCE_INVALID")


@pytest.mark.parametrize("market_key", sorted(attestation._MARKET_SNAPSHOT_KEYS))
def test_stock_market_key_set_is_exact(tmp_path, market_key):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    _rewrite(path, lambda r: r["frozen_snapshot"]["top_k"][0]["market_snapshot"].pop(market_key))
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_INVALID")

    path = _write_evidence(project, scan)
    _rewrite(
        path,
        lambda r: r["frozen_snapshot"]["top_k"][0]["market_snapshot"].__setitem__("extra", 1.0),
    )
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_INVALID")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r["frozen_snapshot"].__setitem__("effective_action", "block"),
        lambda r: r["frozen_snapshot"]["shadow_evaluation"].__setitem__(
            "effective_action", "block"
        ),
        lambda r: r["gate_input"]["ranked_top_k"].reverse(),
        lambda r: r["frozen_snapshot"]["recommendation_payload"].__setitem__("extra", True),
    ],
    ids=["snapshot-action", "shadow-action", "ranked-order", "recommendation-extra"],
)
def test_internal_contract_failures_are_invalid(tmp_path, mutate):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    _rewrite(path, mutate)
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_INVALID")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: (
            r["frozen_snapshot"]["top_k"][0].__setitem__("code", "999999"),
            r["gate_input"]["ranked_top_k"].__setitem__(0, "999999"),
        ),
        lambda r: (
            r["frozen_snapshot"]["top_k"].reverse(),
            [
                row.__setitem__("rank", index + 1)
                for index, row in enumerate(r["frozen_snapshot"]["top_k"])
            ],
            r["gate_input"].__setitem__(
                "ranked_top_k",
                [row["code"] for row in r["frozen_snapshot"]["top_k"]],
            ),
        ),
        lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("score", 8.0),
    ],
    ids=["code", "order", "output"],
)
def test_coherent_target_mismatch_is_semantic_conflict(tmp_path, mutate):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    _rewrite(path, mutate)
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_MISMATCH")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r["frozen_snapshot"]["top_k"][0].__setitem__("score", 10.0),
        lambda r: r["frozen_snapshot"]["top_k"][0]["market_snapshot"].__setitem__("open", 20.1),
        lambda r: r["frozen_snapshot"]["board_avg_gains"].__setitem__("board-a", 2.1),
        lambda r: r["frozen_snapshot"]["all_board_avg_gains"].__setitem__("board-a", 2.2),
    ],
    ids=["row", "market", "board-map", "all-board-map"],
)
def test_numeric_mismatch_is_outside_tolerance(tmp_path, mutate):
    project = _project(tmp_path)
    scan = _scan()
    path = _write_evidence(project, scan)
    _rewrite(path, mutate)
    _expect(project, _canonical(scan), "V16_DAY_GATE_EVIDENCE_MISMATCH")


def test_frozen_later_than_nominal_cutoff_is_allowed(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    _write_evidence(
        project,
        scan,
        frozen_at=FROZEN_AT.replace(hour=10, minute=15, microsecond=999999),
    )
    assert _attest(project, _canonical(scan))["status"] == "PASS"


def test_bounded_search_covers_four_days_and_long_holiday(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    _write_evidence(project, scan)
    assert (
        attest_post_cutoff_v16_day_gate(
            project,
            _canonical(scan),
            TRADE_DATE,
            TRADE_DATE + timedelta(days=4),
        )["status"]
        == "PASS"
    )

    holiday = date(2026, 2, 17)
    holiday_scan = _scan()
    _write_evidence(
        project,
        holiday_scan,
        frozen_at=datetime(holiday.year, holiday.month, holiday.day, 9, 40, tzinfo=SHANGHAI),
    )
    assert (
        attest_post_cutoff_v16_day_gate(
            project,
            replace(_canonical(holiday_scan), trade_date=holiday),
            holiday,
            date(2026, 3, 2),
        )["status"]
        == "PASS"
    )


def test_date_span_boundary_is_day_31_inclusive(tmp_path):
    project = _project(tmp_path)
    scan = _scan()
    _write_evidence(project, scan)
    canonical = _canonical(scan)

    assert (
        attest_post_cutoff_v16_day_gate(
            project,
            canonical,
            TRADE_DATE,
            TRADE_DATE + timedelta(days=31),
        )["status"]
        == "PASS"
    )

    with pytest.raises(
        V16DayGateAttestationError,
        match=r"^V16_DAY_GATE_EVIDENCE_INVALID:trade/current date span must be from 0 to 31 days$",
    ):
        attest_post_cutoff_v16_day_gate(
            project,
            canonical,
            TRADE_DATE,
            TRADE_DATE + timedelta(days=32),
        )


def test_evaluated_at_microseconds_break_frozen_at_tie_deterministically(tmp_path):
    project = _project(tmp_path)
    first = _scan(score=9.0)
    second = _scan(score=9.5)
    first_path = _write_evidence(project, first)
    _rewrite(
        first_path,
        lambda r: r.__setitem__(
            "evaluated_at", (FROZEN_AT + timedelta(microseconds=1)).isoformat()
        ),
    )
    second_path = _write_evidence(
        project,
        second,
        frozen_at=FROZEN_AT,
    )
    result = _attest(project, _canonical(second))
    assert result["evidence_relative_path"] == (second_path.parent.name + "/" + second_path.name)
    assert result["evidence_relative_path"] != (first_path.parent.name + "/" + first_path.name)
