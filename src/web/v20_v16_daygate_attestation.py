"""Fail-closed post-cutoff attestation for V16 DayGate evidence."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from src.strategy.v16_day_gate_evidence import read_v16_day_gate_evidence
from src.strategy.v16_day_gate_shadow import SNAPSHOT_SCHEMA_VERSION, load_shadow_config
from src.web.v20_canonical_selection import (
    CanonicalV16ScanBundle,
    _build_v16_recommendation_payload,
)

_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_MAX_DATE_SPAN_DAYS = 31
_MAX_CANDIDATE_FILES = 128
_MAX_CANDIDATE_BYTES = 2 * 1024 * 1024
_MAX_TOTAL_CANDIDATE_BYTES = 8 * 1024 * 1024
_REQUIRED_TOP_K = 10
_CODE_RE = re.compile(r"^[0-9]{6}$")
_FUNNEL_FIELDS = (
    "step0_universe_count",
    "step2_hot_board_count",
    "step2_filtered_by_avg_gain",
    "step3_count",
    "step4_count",
    "step5_count",
    "step6_count",
    "step6_5_count",
    "step6_6_count",
    "final_candidates",
)
_LIMITATION_CODE = "V16_DAY_GATE_EVIDENCE_ATTESTS_ORDERED_OUTPUT_NOT_FULL_READY_UNIVERSE"
_LIMITATION_TEXT = (
    "V16 DayGate evidence attests the frozen ordered Top-10 output and its gate inputs; "
    "it does not contain or attest the full ready or missing stock universe."
)
_SNAPSHOT_KEYS = frozenset(
    {
        "schema_version",
        "run_id",
        "trade_date",
        "frozen_at",
        "decision_cutoff",
        "nominal_quote_window_end",
        "effective_action",
        "recommendation_payload",
        "top_k",
        "board_avg_gains",
        "all_board_avg_gains",
        "hot_board_member_counts",
        "funnel_counts",
        "source_commit",
        "mews",
        "shadow_evaluation",
    }
)
_TOP_ROW_BASE_KEYS = frozenset(
    {
        "rank",
        "code",
        "name",
        "score",
        "buy_price_0940",
        "best_board",
        "all_hot_boards",
        "gain_from_open_pct",
        "is_driver",
        "cci_14",
        "early_volume_0937",
    }
)
_MARKET_SNAPSHOT_KEYS = frozenset(
    {
        "open",
        "prev_close",
        "price_0940",
        "high_0940",
        "low_0940",
        "volume_0940",
        "volume_0937",
    }
)
_RECOMMENDATION_KEYS = frozenset(
    {
        "stock_code",
        "stock_name",
        "board_name",
        "open_price",
        "prev_close",
        "latest_price",
        "lgb_score",
        "hot_board_count",
        "final_candidates",
    }
)


class V16DayGateAttestationError(ValueError):
    """A stable error later mapped to a V20 semantic conflict."""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}:{detail}")
        self.reason = reason
        self.detail = detail


def _invalid(detail: str) -> V16DayGateAttestationError:
    return V16DayGateAttestationError("V16_DAY_GATE_EVIDENCE_INVALID", detail)


def _missing(detail: str) -> V16DayGateAttestationError:
    return V16DayGateAttestationError("V16_DAY_GATE_EVIDENCE_MISSING", detail)


def _aware_datetime(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise _invalid(f"{field} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise _invalid(f"{field} is not a valid ISO-8601 datetime") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _invalid(f"{field} must be timezone-aware")
    return parsed


def _mismatch(detail: str) -> V16DayGateAttestationError:
    return V16DayGateAttestationError("V16_DAY_GATE_EVIDENCE_MISMATCH", detail)


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _invalid(f"{field} must be a non-boolean number")
    result = float(value)
    if not math.isfinite(result):
        raise _invalid(f"{field} must be finite")
    return result


def _close(left: Any, right: Any, field: str) -> None:
    _finite_number(left, field)
    _finite_number(right, field)
    if not math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-9):
        raise _mismatch(f"{field} does not match canonical output")


def _exact(left: Any, right: Any, field: str) -> None:
    if isinstance(left, bool) != isinstance(right, bool) or type(left) is not type(right):
        if left is None or right is None:
            raise _mismatch(f"{field} has mismatched null shape")
        raise _invalid(f"{field} has an invalid evidence type")
    if left != right:
        raise _mismatch(f"{field} does not match canonical output")


def _candidates(base: Path, trade_date: date, current_date: date) -> list[tuple[Path, int, date]]:
    span = (current_date - trade_date).days
    if span < 0 or span > _MAX_DATE_SPAN_DAYS:
        raise _invalid(f"trade/current date span must be from 0 to {_MAX_DATE_SPAN_DAYS} days")
    candidates: list[tuple[Path, int, date]] = []
    total = 0
    for offset in range(span + 1):
        directory = base / (trade_date + timedelta(days=offset)).strftime("%Y%m%d")
        if not directory.exists():
            continue
        if not directory.is_dir():
            raise _invalid(f"evidence date path is not a directory: {directory}")
        for candidate in sorted(directory.iterdir(), key=lambda item: item.name):
            if candidate.suffix != ".json":
                continue
            if not candidate.is_file():
                raise _invalid(f"evidence candidate is not a file: {candidate}")
            if len(candidates) == _MAX_CANDIDATE_FILES:
                raise _invalid(f"more than {_MAX_CANDIDATE_FILES} evidence candidate files")
            size = candidate.stat().st_size
            if size > _MAX_CANDIDATE_BYTES:
                raise _invalid(f"evidence candidate exceeds byte bound: {candidate.name}")
            total += size
            if total > _MAX_TOTAL_CANDIDATE_BYTES:
                raise _invalid("evidence candidates exceed total byte bound")
            candidates.append((candidate, size, trade_date + timedelta(days=offset)))
    return candidates


def _require_exact_keys(value: Mapping[str, Any], expected: frozenset[str], field: str) -> None:
    if set(value) != set(expected):
        raise _invalid(f"{field} keys do not match the frozen writer schema")


def _optional_number(value: Any, field: str) -> None:
    if value is not None:
        _finite_number(value, field)


def _structural(
    record: Mapping[str, Any], trade_date: date
) -> tuple[tuple[datetime, datetime, str], Mapping[str, Any]] | None:
    gate_input = record.get("gate_input")
    snapshot = record.get("frozen_snapshot")
    if not isinstance(gate_input, dict) or not isinstance(snapshot, dict):
        raise _invalid("gate_input and frozen_snapshot must be objects")
    raw_snapshot_date = snapshot.get("trade_date")
    if not isinstance(raw_snapshot_date, str):
        raise _invalid("frozen_snapshot.trade_date must be an ISO-8601 date string")
    try:
        snapshot_date = date.fromisoformat(raw_snapshot_date)
    except ValueError:
        raise _invalid("frozen_snapshot.trade_date must be an ISO-8601 date string") from None
    if snapshot_date != trade_date:
        return None
    _require_exact_keys(snapshot, _SNAPSHOT_KEYS, "frozen_snapshot")
    evaluated_at = _aware_datetime(record.get("evaluated_at"), "evaluated_at")
    cutoff = _aware_datetime(gate_input.get("cutoff_ts"), "gate_input.cutoff_ts")
    decision = _aware_datetime(snapshot.get("decision_cutoff"), "decision_cutoff")
    frozen_at = _aware_datetime(snapshot.get("frozen_at"), "frozen_at")
    if not cutoff == decision == frozen_at:
        raise _invalid("cutoff, decision_cutoff, and frozen_at are not the same instant")
    if cutoff.astimezone(_SHANGHAI_TZ).date() != trade_date:
        raise _invalid("cutoff instant is not on the target Shanghai trade date")
    if snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise _invalid("unsupported frozen_snapshot schema_version")
    _exact(snapshot.get("trade_date"), trade_date.isoformat(), "frozen_snapshot.trade_date")
    if not isinstance(gate_input.get("upstream_data_complete"), bool):
        raise _invalid("gate_input.upstream_data_complete must be a boolean")
    _exact(
        gate_input.get("upstream_data_complete"),
        True,
        "gate_input.upstream_data_complete",
    )
    if not isinstance(snapshot.get("effective_action"), str):
        raise _invalid("frozen_snapshot.effective_action must be a string")
    _exact(snapshot.get("effective_action"), "pass_through", "frozen_snapshot.effective_action")
    shadow = snapshot.get("shadow_evaluation")
    if not isinstance(shadow, dict):
        raise _invalid("shadow_evaluation must be an object")
    provenance = shadow.get("provenance")
    if not isinstance(provenance, dict):
        raise _invalid("shadow_evaluation.provenance must be an object")
    versions = record.get("versions")
    if not isinstance(versions, dict):
        raise _invalid("versions must be an object")
    evidence_model = versions.get("model")
    if not isinstance(evidence_model, str) or not evidence_model.strip():
        raise _invalid("versions.model must be a non-empty string")
    _exact(gate_input.get("model_version"), evidence_model, "gate_input.model_version")
    if not isinstance(shadow.get("effective_action"), str):
        raise _invalid("shadow_evaluation.effective_action must be a string")
    _exact(
        shadow.get("effective_action"),
        "pass_through",
        "shadow_evaluation.effective_action",
    )
    rows = snapshot.get("top_k")
    if not isinstance(rows, list) or len(rows) != _REQUIRED_TOP_K:
        raise _invalid("frozen_snapshot.top_k must contain exactly ten rows")
    codes: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise _invalid(f"frozen_snapshot.top_k[{index}] must be an object")
        _require_exact_keys(
            row,
            _TOP_ROW_BASE_KEYS | ({"market_snapshot"} if "market_snapshot" in row else frozenset()),
            f"frozen_snapshot.top_k[{index}]",
        )
        if row.get("rank") != index + 1:
            raise _invalid(f"frozen_snapshot.top_k[{index}].rank is invalid")
        if isinstance(row["rank"], bool) or not isinstance(row["rank"], int):
            raise _invalid(f"frozen_snapshot.top_k[{index}].rank must be an integer")
        if not isinstance(row["name"], str):
            raise _invalid(f"frozen_snapshot.top_k[{index}].name must be a string")
        if row["best_board"] is not None and not isinstance(row["best_board"], str):
            raise _invalid(f"frozen_snapshot.top_k[{index}].best_board must be a string or null")
        boards = row["all_hot_boards"]
        if not isinstance(boards, list) or not all(isinstance(board, str) for board in boards):
            raise _invalid(f"frozen_snapshot.top_k[{index}].all_hot_boards must be strings")
        if not isinstance(row["is_driver"], bool) and row["is_driver"] is not None:
            raise _invalid(f"frozen_snapshot.top_k[{index}].is_driver must be boolean or null")
        _finite_number(row["score"], f"frozen_snapshot.top_k[{index}].score")
        _finite_number(row["buy_price_0940"], f"frozen_snapshot.top_k[{index}].buy_price_0940")
        for field in ("gain_from_open_pct", "cci_14", "early_volume_0937"):
            _optional_number(row[field], f"frozen_snapshot.top_k[{index}].{field}")
        market = row.get("market_snapshot")
        if "market_snapshot" in row and market is None:
            raise _invalid(
                f"frozen_snapshot.top_k[{index}].market_snapshot must not be explicit null"
            )
        if market is not None:
            if not isinstance(market, dict):
                raise _invalid(f"frozen_snapshot.top_k[{index}].market_snapshot must be an object")
            _require_exact_keys(
                market,
                _MARKET_SNAPSHOT_KEYS,
                f"frozen_snapshot.top_k[{index}].market_snapshot",
            )
            for field in _MARKET_SNAPSHOT_KEYS:
                _finite_number(
                    market[field],
                    f"frozen_snapshot.top_k[{index}].market_snapshot.{field}",
                )
        code = row.get("code")
        if not isinstance(code, str) or not _CODE_RE.fullmatch(code):
            raise _invalid(f"frozen_snapshot.top_k[{index}].code must be six digits")
        codes.append(code)
    if len(set(codes)) != _REQUIRED_TOP_K:
        raise _invalid("frozen_snapshot.top_k codes must be unique")
    ranked_top_k = gate_input.get("ranked_top_k")
    if not isinstance(ranked_top_k, list) or not all(
        isinstance(code, str) for code in ranked_top_k
    ):
        raise _invalid("gate_input.ranked_top_k must be an array of strings")
    _exact(ranked_top_k, codes, "gate_input.ranked_top_k")
    funnel = snapshot["funnel_counts"]
    _require_exact_keys(funnel, frozenset(_FUNNEL_FIELDS), "frozen_snapshot.funnel_counts")
    if not all(isinstance(value, int) and not isinstance(value, bool) for value in funnel.values()):
        raise _invalid("frozen_snapshot.funnel_counts values must be integers")
    members = snapshot["hot_board_member_counts"]
    if not all(
        isinstance(value, int) and not isinstance(value, bool) for value in members.values()
    ):
        raise _invalid("frozen_snapshot.hot_board_member_counts values must be integers")
    for field in ("board_avg_gains", "all_board_avg_gains"):
        for key, value in snapshot[field].items():
            _finite_number(value, f"frozen_snapshot.{field}[{key!r}]")
    payload = snapshot["recommendation_payload"]
    if not isinstance(payload, dict):
        raise _invalid("recommendation_payload must be an object")
    _require_exact_keys(payload, _RECOMMENDATION_KEYS, "recommendation_payload")
    for field in ("stock_code", "stock_name", "board_name"):
        if not isinstance(payload[field], str):
            raise _invalid(f"recommendation_payload.{field} must be a string")
    for field in ("open_price", "prev_close", "latest_price", "lgb_score"):
        _finite_number(payload[field], f"recommendation_payload.{field}")
    for field in ("hot_board_count", "final_candidates"):
        if isinstance(payload[field], bool) or not isinstance(payload[field], int):
            raise _invalid(f"recommendation_payload.{field} must be an integer")
    return (frozen_at, evaluated_at, str(record["content_sha256"])), snapshot


def _canonical_rows(canonical: CanonicalV16ScanBundle) -> list[Any]:
    rows = getattr(canonical.scan_result, "recommended", ())
    if not isinstance(rows, Sequence) or len(rows) != _REQUIRED_TOP_K:
        raise _invalid("canonical recommended output must contain exactly ten rows")
    return list(rows)


def _mapping(mapping: Any, key: str, default: Any = None) -> Any:
    return mapping.get(key, default) if isinstance(mapping, Mapping) else default


def _compare_rows(canonical: CanonicalV16ScanBundle, rows: Any) -> None:
    if not isinstance(rows, list):
        raise _invalid("frozen_snapshot.top_k must be an array")
    result = canonical.scan_result
    numeric_fields = ("gain_from_open_pct", "cci_14", "early_volume_0937")
    for index, (item, row) in enumerate(zip(_canonical_rows(canonical), rows, strict=True)):
        prefix = f"top_k[{index}]"
        code = str(item.code)
        _exact(row.get("rank"), getattr(item, "rank", None), f"{prefix}.rank")
        _exact(row.get("code"), code, f"{prefix}.code")
        _exact(row.get("name"), str(item.name), f"{prefix}.name")
        _exact(
            row.get("best_board"),
            _mapping(getattr(result, "stock_best_board", {}), code),
            f"{prefix}.best_board",
        )
        _exact(
            row.get("all_hot_boards"),
            list(_mapping(getattr(result, "stock_all_boards", {}), code, ())),
            f"{prefix}.all_hot_boards",
        )
        _exact(
            row.get("is_driver"),
            _mapping(getattr(result, "stock_is_driver", {}), code),
            f"{prefix}.is_driver",
        )
        canonical_numeric = {
            "score": item.score,
            "buy_price_0940": item.buy_price,
            **{
                field: _mapping(getattr(result, map_field, {}), code)
                for field, map_field in zip(
                    numeric_fields,
                    ("stock_gain_from_open", "stock_cci", "stock_early_vol"),
                )
            },
        }
        for field, expected in canonical_numeric.items():
            actual = row.get(field)
            if actual is None or expected is None:
                if actual is not expected:
                    raise _mismatch(f"{prefix}.{field} has mismatched null shape")
                continue
            _close(actual, expected, f"{prefix}.{field}")
        market = row.get("market_snapshot")
        stock = _mapping(canonical.stock_data, code)
        if stock is None:
            if "market_snapshot" in row:
                raise _invalid(f"{prefix}.market_snapshot must be absent")
            continue
        if stock is not None:
            if not isinstance(market, dict):
                raise _invalid(f"{prefix}.market_snapshot must be an object")
            if set(market) != set(_MARKET_SNAPSHOT_KEYS):
                raise _invalid(f"{prefix}.market_snapshot keys do not match writer schema")
            for field in (
                "open",
                "prev_close",
                "price_0940",
                "high_0940",
                "low_0940",
                "volume_0940",
                "volume_0937",
            ):
                canonical_attr = {
                    "open": "open_price",
                    "prev_close": "prev_close",
                    "price_0940": "price_940",
                    "high_0940": "high_940",
                    "low_0940": "low_940",
                    "volume_0940": "volume_940",
                    "volume_0937": "volume_937",
                }[field]
                _close(market.get(field), getattr(stock, canonical_attr), f"{prefix}.{field}")


def _integer_map(evidence: Any, canonical: Any, field: str) -> None:
    if not isinstance(evidence, dict):
        raise _invalid(f"frozen_snapshot.{field} must be an object")
    expected = dict(canonical) if isinstance(canonical, Mapping) else {}
    _exact(set(evidence), set(expected), f"frozen_snapshot.{field} keys")
    for key, actual in evidence.items():
        if isinstance(actual, bool) or not isinstance(actual, int):
            raise _invalid(f"frozen_snapshot.{field}[{key!r}] must be an integer")
        _exact(actual, expected[key], f"frozen_snapshot.{field}[{key!r}]")


def _numeric_map(evidence: Any, canonical: Any, field: str) -> None:
    if not isinstance(evidence, dict):
        raise _invalid(f"frozen_snapshot.{field} must be an object")
    expected = dict(canonical) if isinstance(canonical, Mapping) else {}
    _exact(set(evidence), set(expected), f"frozen_snapshot.{field} keys")
    for key, actual in evidence.items():
        _close(actual, expected[key], f"frozen_snapshot.{field}[{key!r}]")


def _recommendation(canonical: CanonicalV16ScanBundle) -> dict[str, Any]:
    payload = _build_v16_recommendation_payload(
        canonical.scan_result,
        canonical.stock_data,
    )
    if payload is None:
        raise _invalid("canonical Top-1 lacks recommendation market data")
    return payload


def _crossmatch(canonical: CanonicalV16ScanBundle, record: Mapping[str, Any]) -> None:
    if canonical.trade_date.isoformat() != record["frozen_snapshot"]["trade_date"]:
        raise _invalid("canonical trade_date does not match the requested trade date")
    snapshot = record["frozen_snapshot"]
    gate_model = record["gate_input"]["model_version"]
    evidence_model = record["versions"]["model"]
    _exact(evidence_model, canonical.model_sha256, "versions.model")
    _exact(gate_model, canonical.model_sha256, "gate_input.model_version")
    _compare_rows(canonical, snapshot.get("top_k"))
    _integer_map(
        snapshot.get("funnel_counts"),
        {name: getattr(canonical.scan_result, name, 0) for name in _FUNNEL_FIELDS},
        "funnel_counts",
    )
    _integer_map(
        snapshot.get("hot_board_member_counts"),
        {
            str(key): len(value)
            for key, value in getattr(canonical.scan_result, "step2_boards_detail", {}).items()
        },
        "hot_board_member_counts",
    )
    _numeric_map(
        snapshot.get("board_avg_gains"),
        getattr(canonical.scan_result, "step2_board_avg_gains", {}),
        "board_avg_gains",
    )
    _numeric_map(
        snapshot.get("all_board_avg_gains"),
        getattr(canonical.scan_result, "step2_all_board_avg_gains", {}),
        "all_board_avg_gains",
    )
    provenance = snapshot["shadow_evaluation"]["provenance"]
    if not isinstance(provenance, dict):
        raise _invalid("shadow_evaluation.provenance must be an object")
    _exact(provenance.get("model_hash"), canonical.model_sha256, "provenance.model_hash")
    _exact(
        provenance.get("feature_hash"),
        canonical.feature_list_sha256,
        "provenance.feature_hash",
    )
    payload = snapshot.get("recommendation_payload")
    if not isinstance(payload, dict):
        raise _invalid("recommendation_payload must be an object")
    expected_payload = _recommendation(canonical)
    _exact(
        set(payload),
        set(expected_payload),
        "recommendation_payload keys",
    )
    for key, expected in expected_payload.items():
        _exact(payload.get(key), expected, f"recommendation_payload.{key}")


def attest_post_cutoff_v16_day_gate(
    project_root: Path,
    canonical: CanonicalV16ScanBundle,
    trade_date: date,
    current_shanghai_date: date,
) -> Mapping[str, Any]:
    """Attest canonical V16 output against mandatory local DayGate evidence."""

    try:
        base = Path(load_shadow_config(project_root).evidence_dir)
    except Exception as exc:
        raise _invalid(f"shadow config rejected: {type(exc).__name__}: {exc}") from exc
    try:
        candidates = _candidates(base, trade_date, current_shanghai_date)
    except V16DayGateAttestationError:
        raise
    except Exception as exc:
        raise _invalid(f"evidence enumeration failed: {type(exc).__name__}: {exc}") from exc
    if not candidates:
        raise _missing(f"no evidence exists for trade date {trade_date.isoformat()}")
    valid: list[tuple[tuple[datetime, datetime, str], Path, Mapping[str, Any]]] = []
    rejected: list[str] = []
    target_candidates: list[str] = []
    for path, _size, candidate_date in candidates:
        relative_path = path.relative_to(base).as_posix()
        if candidate_date == trade_date:
            target_candidates.append(relative_path)
        try:
            record = read_v16_day_gate_evidence(base, path)
            structural = _structural(record, trade_date)
        except V16DayGateAttestationError as exc:
            rejected.append(f"{relative_path}:{exc.detail}")
            continue
        except Exception as exc:
            rejected.append(f"{relative_path}:{type(exc).__name__}: {exc}")
            continue
        if structural is None:
            continue
        key, _snapshot = structural
        valid.append((key, path, record))
    if not valid:
        if target_candidates:
            raise _invalid(
                "all target-date evidence candidates are invalid: "
                + ", ".join(sorted(target_candidates))
                + ("; " + "; ".join(sorted(rejected)) if rejected else "")
            )
        raise _missing(f"no evidence exists for trade date {trade_date.isoformat()}")
    selected = min(valid, key=lambda item: item[0])
    _, path, selected_record = selected
    _crossmatch(canonical, selected_record)
    return {
        "status": "PASS",
        "schema_version": "v16-day-gate-attestation/v1",
        "trade_date": trade_date.isoformat(),
        "evidence_content_sha256": selected[0][2],
        "frozen_at": selected[0][0].isoformat(),
        "evaluated_at": selected[0][1].isoformat(),
        "evidence_relative_path": path.relative_to(base).as_posix(),
        "limitation": {"code": _LIMITATION_CODE, "text": _LIMITATION_TEXT},
    }


__all__ = ["V16DayGateAttestationError", "attest_post_cutoff_v16_day_gate"]
