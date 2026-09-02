from __future__ import annotations

import copy
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from types import MappingProxyType
from typing import Any

from src.data.database.v20_repository import V20SemanticConflict, sha256_json
from src.strategy.lgbrank_scorer import ScoredStock
from src.strategy.strategies.v16_scanner import V16ScanResult
from src.strategy.v20.models import V20_V16_SNAPSHOT_SCHEMA
from src.web.v20_scan_pipeline import FrozenV16ScanBundle

PORTABLE_FROZEN_V16_SCHEMA = "v20-v16-portable-frozen/v1"
_TOP_FIELDS = frozenset(
    {
        "schema_version",
        "trade_date",
        "frozen_at",
        "canonical_integrity_hash",
        "calendar",
        "v20_snapshot",
        "v20_snapshot_hash",
    }
)
_SNAPSHOT_FIELDS = frozenset(
    {
        "schema_version",
        "trade_date",
        "last_complete_bar",
        "early_market_source_hash",
        "early_market_conflict_codes",
        "breadth_market_source_hash",
        "breadth_market_missing_codes",
        "breadth_market_conflict_codes",
        "scorer_model_sha256",
        "scorer_feature_sha256",
        "list_complete",
        "list_n",
        "symbols",
        "raw_evidence_codes",
        "scan_input_codes",
        "scan_input_failure_codes",
        "scan_input_coverage",
        "history_profile_id",
        "history_input_hashes",
        "history_date_valid_counts",
        "history_min_date_coverage",
        "comparison_pool_codes",
        "comparison_pool_hash",
        "breadth_valid_n",
        "breadth_down_n",
        "prior_trade_date",
        "prior_amount_yuan",
        "funnel",
        "stages",
        "board_avg_gains",
    }
)
_STAGE_FIELDS = frozenset(
    {
        "step0_codes",
        "step2_boards_detail",
        "step2_codes",
        "st_eligible_codes",
        "step3_codes",
        "step4_codes",
        "step5_codes",
        "step6_codes",
        "step6_5_codes",
        "step6_6_codes",
    }
)
_SYMBOL_FIELDS = frozenset(
    {
        "rank",
        "code",
        "name",
        "score",
        "snapshot_price",
        "boards",
        "best_board",
        "is_driver",
        "cci",
        "volume_937",
        "history_hash",
        "early_source_hash",
    }
)
_FUNNEL_REQUIRED = frozenset({"step0_universe_count", "step2_hot_board_count", "final_candidates"})
_FUNNEL_OPTIONAL = frozenset(
    {
        "step2_filtered_by_avg_gain",
        "step3_count",
        "step4_count",
        "step5_count",
        "step6_count",
        "step6_5_count",
        "step6_6_count",
    }
)
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_CODE_RE = re.compile(r"^[0-9]{6}$")


@dataclass(frozen=True)
class HydratedFrozenV16Artifact:
    bundle: FrozenV16ScanBundle
    calendar: tuple[date, date, date]
    canonical_integrity_hash: str
    _payload: Mapping[str, Any] = field(repr=False)

    @property
    def payload(self) -> Mapping[str, Any]:
        return MappingProxyType(copy.deepcopy(dict(self._payload)))


def encode(
    bundle: FrozenV16ScanBundle,
    *,
    calendar: Sequence[date],
    canonical_integrity_hash: str,
) -> dict[str, Any]:
    snapshot = copy.deepcopy(dict(bundle.snapshot))
    _validate_snapshot(snapshot, bundle.trade_date)
    if sha256_json(snapshot) != bundle.snapshot_hash:
        raise V20SemanticConflict("frozen V16 snapshot hash differs from its JSON")
    if bundle.frozen_at.tzinfo is None or bundle.frozen_at.utcoffset() is None:
        raise V20SemanticConflict("frozen V16 timestamp lacks a timezone")
    if bundle.frozen_at.date() < bundle.trade_date:
        raise V20SemanticConflict("frozen V16 timestamp predates its trade date")
    computation_calendar = tuple(getattr(bundle, "computation_calendar", ()))
    if computation_calendar:
        successors = [day for day in computation_calendar if day > bundle.trade_date]
        if len(successors) < 2:
            raise V20SemanticConflict("canonical computation calendar lacks D1/D2")
        calendar_tuple = _validate_calendar(
            (bundle.trade_date, successors[0], successors[1]),
            bundle.trade_date,
        )
        supplied = tuple(_required_date(day) for day in calendar)
        if any(day not in supplied for day in calendar_tuple):
            raise V20SemanticConflict("V20 calendar disagrees with canonical computation calendar")
    else:
        calendar_tuple = _validate_calendar(calendar, bundle.trade_date)
    _validate_hash(canonical_integrity_hash, "canonical integrity hash")
    _validate_bundle_projection(bundle, snapshot)

    return {
        "schema_version": PORTABLE_FROZEN_V16_SCHEMA,
        "trade_date": bundle.trade_date.isoformat(),
        "frozen_at": bundle.frozen_at.isoformat(),
        "canonical_integrity_hash": canonical_integrity_hash,
        "calendar": [day.isoformat() for day in calendar_tuple],
        "v20_snapshot": snapshot,
        "v20_snapshot_hash": bundle.snapshot_hash,
    }


def hydrate(payload: Mapping[str, Any]) -> HydratedFrozenV16Artifact:
    if not isinstance(payload, Mapping) or set(payload) != _TOP_FIELDS:
        raise V20SemanticConflict("portable frozen V16 payload field set is invalid")
    if payload["schema_version"] != PORTABLE_FROZEN_V16_SCHEMA:
        raise V20SemanticConflict("portable frozen V16 schema is unsupported")
    trade_date = _date(payload["trade_date"])
    frozen_at = _datetime(payload["frozen_at"])
    if frozen_at.date() < trade_date:
        raise V20SemanticConflict("portable frozen V16 timestamp predates its trade date")
    calendar_tuple = _validate_calendar(
        payload["calendar"],
        trade_date,
        portable=True,
    )
    canonical_integrity_hash = payload["canonical_integrity_hash"]
    _validate_hash(canonical_integrity_hash, "canonical integrity hash")
    raw_snapshot = payload["v20_snapshot"]
    if not isinstance(raw_snapshot, Mapping):
        raise V20SemanticConflict("V16 snapshot is not an object")
    snapshot = copy.deepcopy(dict(raw_snapshot))
    _validate_snapshot(snapshot, trade_date)
    snapshot_hash = sha256_json(snapshot)
    if payload["v20_snapshot_hash"] != snapshot_hash:
        raise V20SemanticConflict("portable frozen V16 snapshot hash is invalid")

    frozen = _frozen_from_snapshot(
        snapshot=snapshot,
        trade_date=trade_date,
        frozen_at=frozen_at,
        calendar=calendar_tuple,
    )
    portable = {
        "schema_version": PORTABLE_FROZEN_V16_SCHEMA,
        "trade_date": trade_date.isoformat(),
        "frozen_at": frozen_at.isoformat(),
        "canonical_integrity_hash": canonical_integrity_hash,
        "calendar": [day.isoformat() for day in calendar_tuple],
        "v20_snapshot": snapshot,
        "v20_snapshot_hash": snapshot_hash,
    }
    return HydratedFrozenV16Artifact(
        bundle=frozen,
        calendar=calendar_tuple,
        canonical_integrity_hash=canonical_integrity_hash,
        _payload=MappingProxyType(portable),
    )


def _validate_bundle_projection(
    bundle: FrozenV16ScanBundle,
    snapshot: Mapping[str, Any],
) -> None:
    symbols = snapshot["symbols"]
    expected_stocks = [
        ScoredStock(
            code=item["code"],
            name=item["name"],
            score=item["score"],
            rank=item["rank"],
            buy_price=item["snapshot_price"],
        )
        for item in symbols
    ]
    if bundle.scan_result.recommended != expected_stocks:
        raise V20SemanticConflict("frozen V16 recommendations differ from snapshot")
    result = bundle.scan_result
    for item in symbols:
        code = item["code"]
        checks = (
            (result.stock_best_board.get(code), item["best_board"]),
            (result.stock_all_boards.get(code), item["boards"]),
            (result.stock_is_driver.get(code), item["is_driver"]),
            (result.stock_cci.get(code), item["cci"]),
            (result.stock_early_vol.get(code), item["volume_937"]),
        )
        if any(actual != expected for actual, expected in checks):
            raise V20SemanticConflict("frozen V16 recommendation evidence differs from snapshot")
    funnel = snapshot["funnel"]
    for name in (*_FUNNEL_REQUIRED, *_FUNNEL_OPTIONAL):
        if name in funnel and getattr(result, name) != funnel[name]:
            raise V20SemanticConflict("frozen V16 funnel evidence differs from snapshot")
    if result.step2_board_avg_gains != dict(snapshot["board_avg_gains"]):
        raise V20SemanticConflict("frozen V16 board gains differ from snapshot")
    derived = (
        (bundle.comparison_pool_codes, tuple(snapshot["comparison_pool_codes"])),
        (bundle.breadth_valid_n, snapshot["breadth_valid_n"]),
        (bundle.breadth_down_n, snapshot["breadth_down_n"]),
        (bundle.prior_trade_date, _date(snapshot["prior_trade_date"])),
        (dict(bundle.prior_amount_yuan), dict(snapshot["prior_amount_yuan"])),
    )
    if any(actual != expected for actual, expected in derived):
        raise V20SemanticConflict("frozen V16 derived fields differ from snapshot")
    computation_calendar = tuple(getattr(bundle, "computation_calendar", ()))
    if computation_calendar:
        predecessors = [day for day in computation_calendar if day < bundle.trade_date]
        if not predecessors or bundle.prior_trade_date != predecessors[-1]:
            raise V20SemanticConflict("canonical prior date differs from computation calendar")


def _frozen_from_snapshot(
    *,
    snapshot: Mapping[str, Any],
    trade_date: date,
    frozen_at: datetime,
    calendar: tuple[date, date, date],
) -> FrozenV16ScanBundle:
    symbols = snapshot["symbols"]
    recommended = [
        ScoredStock(
            code=item["code"],
            name=item["name"],
            score=item["score"],
            rank=item["rank"],
            buy_price=item["snapshot_price"],
        )
        for item in symbols
    ]
    funnel = dict(snapshot["funnel"])
    stages = snapshot.get("stages") or {}
    scan_result = V16ScanResult(
        recommended=recommended,
        all_scored=[],
        step0_universe_count=funnel["step0_universe_count"],
        step2_hot_board_count=funnel["step2_hot_board_count"],
        step2_filtered_by_avg_gain=funnel.get("step2_filtered_by_avg_gain", 0),
        step3_count=funnel.get("step3_count", 0),
        step4_count=funnel.get("step4_count", 0),
        step5_count=funnel.get("step5_count", 0),
        step6_count=funnel.get("step6_count", 0),
        step6_5_count=funnel.get("step6_5_count", 0),
        step6_6_count=funnel.get("step6_6_count", 0),
        final_candidates=funnel["final_candidates"],
        step0_codes=list(stages.get("step0_codes", [])),
        step2_boards_detail={
            board: list(codes) for board, codes in stages.get("step2_boards_detail", {}).items()
        },
        step2_codes=list(stages.get("step2_codes", [])),
        st_eligible_codes=list(stages.get("st_eligible_codes", [])),
        step3_codes=list(stages.get("step3_codes", [])),
        step4_codes=list(stages.get("step4_codes", [])),
        step5_codes=list(stages.get("step5_codes", [])),
        step6_codes=list(stages.get("step6_codes", [])),
        step6_5_codes=list(stages.get("step6_5_codes", [])),
        step6_6_codes=list(stages.get("step6_6_codes", [])),
        stock_best_board={item["code"]: item["best_board"] for item in symbols},
        stock_all_boards={item["code"]: list(item["boards"]) for item in symbols},
        step2_board_avg_gains=dict(snapshot["board_avg_gains"]),
        stock_is_driver={item["code"]: item["is_driver"] for item in symbols},
        stock_cci={item["code"]: item["cci"] for item in symbols},
        stock_early_vol={item["code"]: item["volume_937"] for item in symbols},
    )
    return FrozenV16ScanBundle(
        trade_date=trade_date,
        frozen_at=frozen_at,
        scan_result=scan_result,
        stock_data=MappingProxyType({}),
        comparison_pool_codes=tuple(snapshot["comparison_pool_codes"]),
        breadth_valid_n=snapshot["breadth_valid_n"],
        breadth_down_n=snapshot["breadth_down_n"],
        prior_trade_date=_date(snapshot["prior_trade_date"]),
        prior_amount_yuan=MappingProxyType(dict(snapshot["prior_amount_yuan"])),
        snapshot=snapshot,
        snapshot_hash=sha256_json(snapshot),
        computation_calendar=calendar,
    )


def _validate_snapshot(value: Mapping[str, Any], trade_date: date) -> None:
    if not isinstance(value, Mapping):
        raise V20SemanticConflict("V16 snapshot is not an object")
    if set(value) != _SNAPSHOT_FIELDS:
        raise V20SemanticConflict("V16 snapshot field set is invalid")
    if value["schema_version"] != V20_V16_SNAPSHOT_SCHEMA:
        raise V20SemanticConflict("V16 snapshot schema is unsupported")
    if _date(value["trade_date"]) != trade_date:
        raise V20SemanticConflict("V16 snapshot trade date differs")
    if not isinstance(value["last_complete_bar"], str) or not value["last_complete_bar"]:
        raise V20SemanticConflict("V16 snapshot last bar is invalid")
    for name in (
        "early_market_source_hash",
        "breadth_market_source_hash",
        "scorer_model_sha256",
        "scorer_feature_sha256",
    ):
        _validate_hash(value[name], f"V16 snapshot {name}")
    for name in (
        "early_market_conflict_codes",
        "breadth_market_missing_codes",
        "breadth_market_conflict_codes",
        "raw_evidence_codes",
        "scan_input_codes",
        "scan_input_failure_codes",
    ):
        _validate_code_list(value[name], name)
    if value["list_complete"] is not True:
        raise V20SemanticConflict("V16 snapshot list is incomplete")
    symbols = value["symbols"]
    if not isinstance(symbols, list) or not 0 <= len(symbols) <= 10:
        raise V20SemanticConflict("V16 snapshot recommendation count is invalid")
    if _integer(value["list_n"]) != len(symbols):
        raise V20SemanticConflict("V16 snapshot list count differs")
    seen_codes: set[str] = set()
    for expected_rank, item in enumerate(symbols, start=1):
        _validate_symbol(item, expected_rank, value["board_avg_gains"])
        if item["code"] in seen_codes:
            raise V20SemanticConflict("V16 snapshot recommendation codes are duplicated")
        seen_codes.add(item["code"])
    comparison = value["comparison_pool_codes"]
    _validate_code_list(comparison, "comparison_pool_codes")
    if not comparison:
        raise V20SemanticConflict("V16 snapshot comparison pool is empty")
    if value["comparison_pool_hash"] != sha256_json(list(comparison)):
        raise V20SemanticConflict("V16 snapshot comparison pool hash differs")
    breadth_valid = _integer(value["breadth_valid_n"])
    breadth_down = _integer(value["breadth_down_n"])
    if breadth_valid < 0 or breadth_down < 0 or breadth_down > breadth_valid:
        raise V20SemanticConflict("V16 snapshot breadth counts are invalid")
    prior_trade_date = _date(value["prior_trade_date"])
    amounts = value["prior_amount_yuan"]
    if not isinstance(amounts, Mapping) or set(amounts) != seen_codes:
        raise V20SemanticConflict("V16 snapshot prior amounts do not cover recommendations")
    if any(not _number(amount) or amount <= 0 for amount in amounts.values()):
        raise V20SemanticConflict("V16 snapshot prior amounts are invalid")
    _validate_funnel(value["funnel"])
    board_gains = value["board_avg_gains"]
    if not isinstance(board_gains, Mapping) or any(
        not isinstance(board, str) or not board or not _number(gain)
        for board, gain in board_gains.items()
    ):
        raise V20SemanticConflict("V16 snapshot board gains are invalid")
    coverage = value["scan_input_coverage"]
    if not _number(coverage) or not 0 <= coverage <= 1:
        raise V20SemanticConflict("V16 snapshot scan coverage is invalid")
    scan_input_codes = value["scan_input_codes"]
    scan_failure_codes = value["scan_input_failure_codes"]
    raw_evidence_codes = value["raw_evidence_codes"]
    if not raw_evidence_codes:
        raise V20SemanticConflict("V16 snapshot raw evidence union is empty")
    comparison_set = set(comparison)
    if (
        set(scan_input_codes) & set(scan_failure_codes)
        or set(scan_input_codes) | set(scan_failure_codes) != comparison_set
        or not math.isclose(
            float(coverage),
            len(scan_input_codes) / len(comparison),
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise V20SemanticConflict("V16 snapshot scan input partition is inconsistent")
    if not set(scan_input_codes).issubset(raw_evidence_codes):
        raise V20SemanticConflict("V16 snapshot scan inputs lack durable raw evidence")
    if not isinstance(value["history_profile_id"], str) or not value["history_profile_id"]:
        raise V20SemanticConflict("V16 snapshot history profile is invalid")
    history_hashes = value["history_input_hashes"]
    if not isinstance(history_hashes, Mapping):
        raise V20SemanticConflict("V16 snapshot history hashes are invalid")
    for code, history_hash in history_hashes.items():
        _validate_code(code, "history hash code")
        _validate_hash(history_hash, "history hash")
    if not set(history_hashes).issubset(comparison_set):
        raise V20SemanticConflict("V16 snapshot history hashes have unknown codes")
    for item in symbols:
        if history_hashes.get(item["code"]) != item["history_hash"]:
            raise V20SemanticConflict("V16 snapshot symbol history hash differs")
    history_counts = value["history_date_valid_counts"]
    if not isinstance(history_counts, Mapping) or any(
        not isinstance(day, str) or _integer(count) < 0 for day, count in history_counts.items()
    ):
        raise V20SemanticConflict("V16 snapshot history counts are invalid")
    if len(history_counts) != 37 or list(history_counts) != sorted(history_counts):
        raise V20SemanticConflict("V16 snapshot history counts are not 37 ordered dates")
    try:
        parsed_history_dates = [_date(day) for day in history_counts]
    except V20SemanticConflict as exc:
        raise V20SemanticConflict("V16 snapshot history counts are not dates") from exc
    if (
        len(set(parsed_history_dates)) != 37
        or any(day >= _date(value["trade_date"]) for day in parsed_history_dates)
        or parsed_history_dates[-1] != prior_trade_date
        or any(count > len(comparison) for count in history_counts.values())
    ):
        raise V20SemanticConflict("V16 snapshot history dates are invalid")
    history_coverage = value["history_min_date_coverage"]
    if not _number(history_coverage) or not 0 <= history_coverage <= 1:
        raise V20SemanticConflict("V16 snapshot history coverage is invalid")
    expected_history_coverage = min(history_counts.values()) / len(comparison)
    if not math.isclose(
        float(history_coverage),
        expected_history_coverage,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise V20SemanticConflict("V16 snapshot history coverage differs from counts")
    _validate_stages(value["stages"])
    _validate_cross_field_semantics(value, seen_codes)


def _validate_symbol(
    item: Any,
    expected_rank: int,
    board_gains: Mapping[str, Any],
) -> None:
    if not isinstance(item, Mapping) or set(item) != _SYMBOL_FIELDS:
        raise V20SemanticConflict("V16 snapshot symbol field set is invalid")
    if _integer(item["rank"]) != expected_rank:
        raise V20SemanticConflict("V16 snapshot symbol ranks are non-contiguous")
    _validate_code(item["code"], "recommendation code")
    if not isinstance(item["name"], str):
        raise V20SemanticConflict("V16 snapshot symbol name is invalid")
    if not _number(item["score"]):
        raise V20SemanticConflict("V16 snapshot symbol score is invalid")
    if not _number(item["snapshot_price"]) or item["snapshot_price"] <= 0:
        raise V20SemanticConflict("V16 snapshot symbol price is invalid")
    boards = item["boards"]
    if (
        not isinstance(boards, list)
        or not boards
        or any(not isinstance(board, str) or not board for board in boards)
        or len(set(boards)) != len(boards)
    ):
        raise V20SemanticConflict("V16 snapshot symbol boards are invalid")
    if item["best_board"] not in boards:
        raise V20SemanticConflict("V16 snapshot symbol best board is invalid")
    if any(board not in board_gains for board in boards):
        raise V20SemanticConflict("V16 snapshot symbol board lacks a frozen gain")
    if type(item["is_driver"]) is not bool:
        raise V20SemanticConflict("V16 snapshot symbol driver flag is invalid")
    for name in ("cci", "volume_937"):
        value = item[name]
        if value is not None and not _number(value):
            raise V20SemanticConflict("V16 snapshot symbol numeric evidence is invalid")
    if item["volume_937"] is not None and item["volume_937"] <= 0:
        raise V20SemanticConflict("V16 snapshot symbol volume is invalid")
    _validate_hash(item["history_hash"], "symbol history hash")
    _validate_hash(item["early_source_hash"], "symbol early source hash")


def _validate_funnel(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise V20SemanticConflict("V16 snapshot funnel is invalid")
    allowed = _FUNNEL_REQUIRED | _FUNNEL_OPTIONAL
    if not _FUNNEL_REQUIRED.issubset(value) or any(key not in allowed for key in value):
        raise V20SemanticConflict("V16 snapshot funnel field set is invalid")
    if any(_integer(value[key]) < 0 for key in value):
        raise V20SemanticConflict("V16 snapshot funnel count is invalid")


def _validate_stages(value: Any) -> None:
    if not isinstance(value, Mapping) or set(value) != _STAGE_FIELDS:
        raise V20SemanticConflict("V16 snapshot stages field set is invalid")
    for name in _STAGE_FIELDS - {"step2_boards_detail"}:
        _validate_code_list(value[name], name)
    detail = value["step2_boards_detail"]
    if not isinstance(detail, Mapping) or any(
        not isinstance(board, str) or not board for board in detail
    ):
        raise V20SemanticConflict("V16 snapshot board detail is invalid")
    for codes in detail.values():
        _validate_code_list(codes, "step2 board members")


def _validate_cross_field_semantics(
    snapshot: Mapping[str, Any],
    recommended_codes: set[str],
) -> None:
    """Reject internally coherent JSON whose frozen facts contradict each other."""

    stages = snapshot["stages"]
    funnel = snapshot["funnel"]
    if stages["step0_codes"] != snapshot["scan_input_codes"]:
        raise V20SemanticConflict("V16 snapshot step0 differs from scan inputs")
    board_union = sorted(
        {code for codes in stages["step2_boards_detail"].values() for code in codes}
    )
    if board_union != stages["step2_codes"]:
        raise V20SemanticConflict("V16 snapshot hot-board membership differs")
    if funnel["step0_universe_count"] != len(stages["step0_codes"]):
        raise V20SemanticConflict("V16 snapshot step0 count differs")
    if funnel["step2_hot_board_count"] != len(stages["step2_boards_detail"]):
        raise V20SemanticConflict("V16 snapshot hot-board count differs")

    stage_count_fields = (
        ("step3_count", "step3_codes"),
        ("step4_count", "step4_codes"),
        ("step5_count", "step5_codes"),
        ("step6_count", "step6_codes"),
        ("step6_5_count", "step6_5_codes"),
        ("step6_6_count", "step6_6_codes"),
    )
    for count_name, codes_name in stage_count_fields:
        if funnel.get(count_name, 0) != len(stages[codes_name]):
            raise V20SemanticConflict(f"V16 snapshot {count_name} differs")

    chain = (
        set(stages["step2_codes"]),
        set(stages["st_eligible_codes"]),
        set(stages["step3_codes"]),
        set(stages["step4_codes"]),
        set(stages["step5_codes"]),
        set(stages["step6_codes"]),
        set(stages["step6_5_codes"]),
        set(stages["step6_6_codes"]),
    )
    if any(not child.issubset(parent) for parent, child in zip(chain[:-1], chain[1:], strict=True)):
        raise V20SemanticConflict("V16 snapshot funnel stages are not nested")
    if funnel["final_candidates"] != len(stages["step6_6_codes"]):
        raise V20SemanticConflict("V16 snapshot final candidate count differs")
    if not recommended_codes.issubset(set(stages["step6_6_codes"])):
        raise V20SemanticConflict("V16 snapshot recommendations are outside final candidates")
    if len(recommended_codes) > funnel["final_candidates"]:
        raise V20SemanticConflict("V16 snapshot recommendation count exceeds candidates")

    raw_codes = set(snapshot["raw_evidence_codes"])
    comparison_codes = set(snapshot["comparison_pool_codes"])
    if any(
        code not in comparison_codes and not code.startswith(("00", "60")) for code in raw_codes
    ):
        raise V20SemanticConflict("V16 snapshot raw evidence has an unknown non-breadth code")
    if set(snapshot["early_market_conflict_codes"]) & raw_codes:
        raise V20SemanticConflict("V16 snapshot conflicted early inputs entered raw evidence")
    breadth_unavailable = set(snapshot["breadth_market_missing_codes"]) | set(
        snapshot["breadth_market_conflict_codes"]
    )
    if breadth_unavailable & raw_codes:
        raise V20SemanticConflict("V16 snapshot unavailable breadth codes entered raw evidence")
    if set(snapshot["breadth_market_missing_codes"]) & set(
        snapshot["breadth_market_conflict_codes"]
    ):
        raise V20SemanticConflict("V16 snapshot breadth missing/conflict sets overlap")


def _validate_calendar(
    value: Any,
    trade_date: date,
    *,
    portable: bool = False,
) -> tuple[date, date, date]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != 3:
        raise V20SemanticConflict("portable V16 calendar must contain D0,D1,D2")
    parsed = tuple(_date(item) if portable else _required_date(item) for item in value)
    if parsed[0] != trade_date or not parsed[0] < parsed[1] < parsed[2]:
        raise V20SemanticConflict("portable V16 calendar is not strictly ordered")
    return parsed  # type: ignore[return-value]


def _validate_code_list(value: Any, name: str) -> None:
    if not isinstance(value, list):
        raise V20SemanticConflict(f"V16 snapshot {name} is invalid")
    for code in value:
        _validate_code(code, name)
    if len(set(value)) != len(value) or list(value) != sorted(value):
        raise V20SemanticConflict(f"V16 snapshot {name} is not canonical")


def _validate_code(value: Any, name: str) -> None:
    if not isinstance(value, str) or _CODE_RE.fullmatch(value) is None:
        raise V20SemanticConflict(f"V16 snapshot {name} is invalid")


def _validate_hash(value: Any, name: str) -> None:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise V20SemanticConflict(f"{name} is invalid")


def _number(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise V20SemanticConflict("V16 snapshot integer is invalid")
    return value


def _date(value: Any) -> date:
    if not isinstance(value, str):
        raise V20SemanticConflict("V16 snapshot date is invalid")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise V20SemanticConflict("V16 snapshot date is invalid") from exc


def _required_date(value: Any) -> date:
    if type(value) is not date:
        raise V20SemanticConflict("portable V16 calendar date is invalid")
    return value


def _datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise V20SemanticConflict("portable V16 timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise V20SemanticConflict("portable V16 timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise V20SemanticConflict("portable V16 timestamp lacks a timezone")
    return parsed
