"""Observation-only runtime adapter for the V16 day gate.

This module freezes a completed V16 scan, prepares deterministic graph inputs,
and evaluates a shadow policy.  It has no order API and accepts no live/enforce
mode.  The web service deliberately runs the I/O portion in a background task
so a taxonomy, evidence, or notification failure cannot alter the recommendation
seen by iQuant.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final

import yaml

from src.strategy.filters.board_filter import BROAD_CONCEPT_BOARDS
from src.strategy.v16_day_gate import (
    GateMode,
    V16DayGate,
    V16DayGateDecision,
    V16DayGateInput,
    V16DayGateMetrics,
    V16DayGatePolicy,
)
from src.strategy.v16_theme_semantics import build_approved_theme_index, parse_json_strict

RUNTIME_SCHEMA_VERSION: Final = "v16-day-gate-runtime/v1"
SNAPSHOT_SCHEMA_VERSION: Final = "v16-day-gate-snapshot/v1"
MEWS_UNKNOWN: Final[dict[str, Any]] = {
    "status": "unknown",
    "reason": "provider_not_available_on_main",
    "index_version": None,
    "source_trade_date": None,
    "signal_available_ts": None,
}


class V16DayGateShadowError(ValueError):
    """Raised for an invalid shadow configuration or frozen snapshot."""


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that refuses silent duplicate-key overwrite."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise V16DayGateShadowError("config contains an unhashable mapping key") from exc
        if duplicate:
            raise V16DayGateShadowError(f"config contains duplicate key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True)
class BoardRelevanceConfig:
    cache_path: Path
    allowed_levels: tuple[str, ...]
    exclude_unrated: bool
    exclude_broad_boards: bool


@dataclass(frozen=True)
class V16DayGateShadowConfig:
    """Strict phase-1 configuration.  ``mode`` is only off or shadow."""

    mode: str
    top_k: int
    evidence_dir: Path
    send_feishu: bool
    approved_taxonomy_artifact_path: Path | None
    approved_taxonomy_artifact_sha256: str | None
    board_relevance: BoardRelevanceConfig
    policy: V16DayGatePolicy
    config_path: Path
    config_hash: str

    @property
    def enabled(self) -> bool:
        return self.mode == "shadow"


@dataclass(frozen=True)
class PreparedShadowDecision:
    """Gate inputs, output, and provenance prepared from one frozen scan."""

    gate_input: V16DayGateInput
    decision: V16DayGateDecision
    taxonomy_hash: str | None
    taxonomy_approval_artifact_hash: str | None
    relevance_cache_hash: str | None
    relevance_filtered_metrics: V16DayGateMetrics
    edge_coverage: dict[str, Any]
    edge_audit: tuple[dict[str, Any], ...]
    provenance: dict[str, Any]


@dataclass(frozen=True)
class FrozenV16DayGateRuntime:
    """Point-in-time policy, taxonomy, code, and ranking artifact identity.

    The large legacy relevance cache is intentionally excluded: it drives only
    a parallel diagnostic graph and is loaded/hash-labelled at evaluation time.
    """

    captured_at: datetime
    config: V16DayGateShadowConfig
    canonical_theme_items: tuple[tuple[str, str], ...]
    taxonomy_excluded_aliases: tuple[str, ...]
    taxonomy_version: str | None
    taxonomy_hash: str | None
    taxonomy_approval_artifact_hash: str | None
    ranking_model_sha256: str
    ranking_feature_list_sha256: str
    filesystem_source_hashes_at_capture: tuple[tuple[str, str | None], ...]
    source_commit: str
    context_id: str


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    """Hash one file without applying text or newline normalization."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _exact_keys(value: Mapping[str, Any], expected: set[str], path: str) -> None:
    actual = set(value)
    if actual != expected:
        raise V16DayGateShadowError(
            f"{path} keys mismatch; missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise V16DayGateShadowError(f"{path} must be an object")
    return value


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise V16DayGateShadowError(f"{path} must be a boolean")
    return value


def _optional_float(value: Any, path: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise V16DayGateShadowError(f"{path} must be a number or null")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise V16DayGateShadowError(f"{path} must be finite")
    return parsed


def _resolve_project_path(project_root: Path, value: str, path: str) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = project_root / candidate
    resolved = candidate.resolve()
    root = project_root.resolve()
    if resolved != root and root not in resolved.parents:
        raise V16DayGateShadowError(f"{path} must stay inside project root")
    return resolved


def load_shadow_config(
    project_root: Path,
    config_path: Path | None = None,
) -> V16DayGateShadowConfig:
    """Load the strict phase-1 YAML config.

    The loader rejects a live/enforce value structurally; phase 1 cannot be
    turned into an execution gate through configuration alone.
    """

    root = project_root.resolve()
    path = (config_path or (root / "config" / "v16-day-gate.yaml")).resolve()
    if path != root and root not in path.parents:
        raise V16DayGateShadowError("config_path must stay inside project root")
    raw_bytes = path.read_bytes()
    value = yaml.load(raw_bytes.decode("utf-8"), Loader=_UniqueKeySafeLoader)
    config = _mapping(value, "config")
    _exact_keys(
        config,
        {
            "schema_version",
            "mode",
            "top_k",
            "evidence_dir",
            "send_feishu",
            "approved_taxonomy_artifact_path",
            "approved_taxonomy_artifact_sha256",
            "board_relevance",
            "policy",
        },
        "config",
    )
    if config["schema_version"] != RUNTIME_SCHEMA_VERSION:
        raise V16DayGateShadowError(f"unsupported config schema {config['schema_version']!r}")
    mode = config["mode"]
    if mode not in {"off", "shadow"}:
        raise V16DayGateShadowError("config.mode must be 'off' or 'shadow'")
    top_k = config["top_k"]
    if isinstance(top_k, bool) or not isinstance(top_k, int) or not 1 <= top_k <= 10:
        raise V16DayGateShadowError("config.top_k must be an integer from 1 to 10")

    evidence_dir_value = config["evidence_dir"]
    if not isinstance(evidence_dir_value, str) or not evidence_dir_value.strip():
        raise V16DayGateShadowError("config.evidence_dir must be a non-empty path")
    evidence_dir = _resolve_project_path(root, evidence_dir_value, "config.evidence_dir")

    taxonomy_value = config["approved_taxonomy_artifact_path"]
    taxonomy_hash_value = config["approved_taxonomy_artifact_sha256"]
    taxonomy_path: Path | None = None
    if taxonomy_value is not None:
        if not isinstance(taxonomy_value, str) or not taxonomy_value.strip():
            raise V16DayGateShadowError(
                "config.approved_taxonomy_artifact_path must be a non-empty path or null"
            )
        taxonomy_path = _resolve_project_path(
            root,
            taxonomy_value,
            "config.approved_taxonomy_artifact_path",
        )
    if (taxonomy_path is None) != (taxonomy_hash_value is None):
        raise V16DayGateShadowError(
            "approved_taxonomy_artifact_path and approved_taxonomy_artifact_sha256 "
            "must be set together"
        )
    taxonomy_hash: str | None = None
    if taxonomy_hash_value is not None:
        if (
            not isinstance(taxonomy_hash_value, str)
            or len(taxonomy_hash_value) != 64
            or any(character not in "0123456789abcdef" for character in taxonomy_hash_value)
        ):
            raise V16DayGateShadowError(
                "config.approved_taxonomy_artifact_sha256 must be 64 lowercase hexadecimal chars"
            )
        taxonomy_hash = taxonomy_hash_value

    relevance = _mapping(config["board_relevance"], "config.board_relevance")
    _exact_keys(
        relevance,
        {
            "cache_path",
            "allowed_levels",
            "exclude_unrated",
            "exclude_broad_boards",
        },
        "config.board_relevance",
    )
    relevance_path_value = relevance["cache_path"]
    if not isinstance(relevance_path_value, str) or not relevance_path_value.strip():
        raise V16DayGateShadowError("config.board_relevance.cache_path must be a non-empty path")
    levels = relevance["allowed_levels"]
    if (
        not isinstance(levels, list)
        or not levels
        or any(not isinstance(level, str) or not level.strip() for level in levels)
        or len(levels) != len(set(levels))
    ):
        raise V16DayGateShadowError(
            "config.board_relevance.allowed_levels must be a unique string array"
        )
    known_relevance_levels = {"高", "中", "低"}
    unknown_levels = sorted(set(levels) - known_relevance_levels)
    if unknown_levels:
        raise V16DayGateShadowError(
            f"config.board_relevance.allowed_levels contains unknown values: {unknown_levels}"
        )

    policy_value = _mapping(config["policy"], "config.policy")
    _exact_keys(
        policy_value,
        {
            "version",
            "min_largest_cluster_share",
            "max_effective_cluster_count",
            "min_top3_main_cluster_coverage",
            "min_driver_breadth",
        },
        "config.policy",
    )
    policy_version = policy_value["version"]
    if not isinstance(policy_version, str) or not policy_version.strip():
        raise V16DayGateShadowError("config.policy.version must be non-empty")
    policy = V16DayGatePolicy(
        version=policy_version,
        mode=GateMode.SHADOW,
        min_largest_cluster_share=_optional_float(
            policy_value["min_largest_cluster_share"],
            "config.policy.min_largest_cluster_share",
        ),
        max_effective_cluster_count=_optional_float(
            policy_value["max_effective_cluster_count"],
            "config.policy.max_effective_cluster_count",
        ),
        min_top3_main_cluster_coverage=_optional_float(
            policy_value["min_top3_main_cluster_coverage"],
            "config.policy.min_top3_main_cluster_coverage",
        ),
        min_driver_breadth=_optional_float(
            policy_value["min_driver_breadth"],
            "config.policy.min_driver_breadth",
        ),
    )
    return V16DayGateShadowConfig(
        mode=mode,
        top_k=top_k,
        evidence_dir=evidence_dir,
        send_feishu=_boolean(config["send_feishu"], "config.send_feishu"),
        approved_taxonomy_artifact_path=taxonomy_path,
        approved_taxonomy_artifact_sha256=taxonomy_hash,
        board_relevance=BoardRelevanceConfig(
            cache_path=_resolve_project_path(
                root,
                relevance_path_value,
                "config.board_relevance.cache_path",
            ),
            allowed_levels=tuple(levels),
            exclude_unrated=_boolean(
                relevance["exclude_unrated"],
                "config.board_relevance.exclude_unrated",
            ),
            exclude_broad_boards=_boolean(
                relevance["exclude_broad_boards"],
                "config.board_relevance.exclude_broad_boards",
            ),
        ),
        policy=policy,
        config_path=path,
        config_hash=_sha256_bytes(raw_bytes),
    )


def _json_copy(value: Any) -> Any:
    """Validate and detach a JSON-native metadata value."""

    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")))


def freeze_v16_scan_snapshot(
    scan_result: Any,
    stock_data: Mapping[str, Any],
    recommendation_payload: Mapping[str, Any] | None,
    *,
    frozen_at: datetime,
) -> dict[str, Any]:
    """Freeze all gate-relevant scan evidence without performing any I/O."""

    if frozen_at.tzinfo is None or frozen_at.utcoffset() is None:
        raise V16DayGateShadowError("frozen_at must be timezone-aware")
    recommended = list(getattr(scan_result, "recommended", ()))

    def optional_number(value: Any, field_name: str) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise V16DayGateShadowError(f"{field_name} must be numeric or null")
        parsed = float(value)
        if not math.isfinite(parsed):
            raise V16DayGateShadowError(f"{field_name} must be finite")
        return parsed

    def stock_row(scored: Any) -> dict[str, Any]:
        code = str(scored.code)
        raw_boards = getattr(scan_result, "stock_all_boards", {}).get(code, ())
        if isinstance(raw_boards, (str, bytes)) or not isinstance(raw_boards, Sequence):
            raise V16DayGateShadowError(f"stock_all_boards[{code!r}] must be a board-name sequence")
        boards: list[str] = []
        for raw_board in raw_boards:
            if not isinstance(raw_board, str) or not raw_board.strip():
                raise V16DayGateShadowError(
                    f"stock_all_boards[{code!r}] contains an invalid board name"
                )
            boards.append(raw_board)
        driver = getattr(scan_result, "stock_is_driver", {}).get(code)
        if driver is not None and not isinstance(driver, bool):
            raise V16DayGateShadowError(f"stock_is_driver[{code!r}] must be boolean or null")
        row: dict[str, Any] = {
            "rank": int(scored.rank),
            "code": code,
            "name": str(scored.name),
            "score": float(scored.score),
            "buy_price_0940": float(scored.buy_price),
            "best_board": getattr(scan_result, "stock_best_board", {}).get(code),
            "all_hot_boards": boards,
            "gain_from_open_pct": optional_number(
                getattr(scan_result, "stock_gain_from_open", {}).get(code),
                f"stock_gain_from_open[{code!r}]",
            ),
            "is_driver": driver,
        }
        item = stock_data.get(code)
        if item is not None:
            row["market_snapshot"] = {
                "open": float(item.open_price),
                "prev_close": float(item.prev_close),
                "price_0940": float(item.price_940),
                "high_0940": float(item.high_940),
                "low_0940": float(item.low_940),
                "volume_0940": float(item.volume_940),
                "volume_0937": float(item.volume_937),
            }
        row["cci_14"] = optional_number(
            getattr(scan_result, "stock_cci", {}).get(code),
            f"stock_cci[{code!r}]",
        )
        row["early_volume_0937"] = optional_number(
            getattr(scan_result, "stock_early_vol", {}).get(code),
            f"stock_early_vol[{code!r}]",
        )
        return row

    funnel_count_names = (
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
    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "run_id": str(uuid.uuid4()),
        "trade_date": frozen_at.date().isoformat(),
        "frozen_at": frozen_at.isoformat(),
        # Honest availability cutoff.  The upstream quote contract remains a
        # nominal 09:40 window and is recorded separately below.
        "decision_cutoff": frozen_at.isoformat(),
        "nominal_quote_window_end": frozen_at.replace(
            hour=9, minute=40, second=0, microsecond=0
        ).isoformat(),
        "effective_action": "pass_through",
        "recommendation_payload": dict(recommendation_payload)
        if recommendation_payload is not None
        else None,
        "top_k": [stock_row(item) for item in recommended],
        "board_avg_gains": {
            str(board): float(gain)
            for board, gain in getattr(scan_result, "step2_board_avg_gains", {}).items()
        },
        "all_board_avg_gains": {
            str(board): float(gain)
            for board, gain in getattr(scan_result, "step2_all_board_avg_gains", {}).items()
        },
        "hot_board_member_counts": {
            str(board): len(codes)
            for board, codes in getattr(scan_result, "step2_boards_detail", {}).items()
        },
        "funnel_counts": {name: int(getattr(scan_result, name, 0)) for name in funnel_count_names},
        "source_commit": os.getenv("GIT_COMMIT_SHA") or "unknown",
        "mews": dict(MEWS_UNKNOWN),
    }
    return payload


def _fallback_bundled_path(path: Path, project_root: Path) -> Path:
    if path.exists():
        return path
    bundled = project_root / "bundled_data" / path.name
    return bundled if bundled.exists() else path


def _load_relevance_cache(
    config: BoardRelevanceConfig,
    project_root: Path,
) -> tuple[dict[str, dict[str, str]], str | None]:
    path = _fallback_bundled_path(config.cache_path, project_root)
    if not path.exists():
        return {}, None
    raw = path.read_bytes()
    value = parse_json_strict(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise V16DayGateShadowError("board relevance cache must be an object")
    parsed: dict[str, dict[str, str]] = {}
    for key, row in value.items():
        if not isinstance(key, str) or not isinstance(row, dict):
            raise V16DayGateShadowError("board relevance cache has an invalid row")
        level = row.get("level")
        reason = row.get("reason")
        if not isinstance(level, str) or not isinstance(reason, str):
            raise V16DayGateShadowError(
                f"board relevance cache row {key!r} lacks string level/reason"
            )
        parsed[key] = {"level": level, "reason": reason}
    return parsed, _sha256_bytes(raw)


def _load_approved_taxonomy(
    path: Path | None,
    approved_artifact_hash: str | None,
) -> tuple[dict[str, str], set[str], str | None, str | None, str | None]:
    if path is None:
        if approved_artifact_hash is not None:
            raise V16DayGateShadowError(
                "taxonomy approval artifact hash configured without an artifact path"
            )
        return {}, set(), None, None, None
    if approved_artifact_hash is None:
        raise V16DayGateShadowError(
            "taxonomy approval artifact path configured without an approved hash"
        )
    raw = path.read_bytes()
    value = parse_json_strict(raw.decode("utf-8"))
    index = build_approved_theme_index(
        value,
        approved_artifact_hash=approved_artifact_hash,
    )
    board_map = {
        alias: theme_id
        for alias in index.raw_to_canonical_theme_id
        if (theme_id := index.bridge_theme_id(alias)) is not None
    }
    return (
        board_map,
        set(index.excluded_aliases),
        str(value["taxonomy"]["taxonomy_version"]),
        index.source_hash,
        approved_artifact_hash,
    )


def _prepare_board_edges(
    top_rows: Sequence[Mapping[str, Any]],
    config: V16DayGateShadowConfig,
    project_root: Path,
    canonical_theme_map: Mapping[str, str],
    taxonomy_excluded: set[str],
) -> tuple[
    dict[str, tuple[str, ...]],
    dict[str, tuple[str, ...]],
    tuple[dict[str, Any], ...],
    str | None,
    dict[str, Any],
]:
    """Build a primary specific-theme graph and a parallel relevance graph.

    The primary graph never treats an unrated legacy-LLM cache entry as a
    negative.  The relevance-filtered graph is diagnostic only; its coverage
    is persisted separately and cannot turn the primary decision into a data
    failure.  Broad/noise-only stocks become explicit singleton components,
    while truly missing upstream board membership remains missing.
    """
    relevance, relevance_hash = _load_relevance_cache(config.board_relevance, project_root)
    allowed = set(config.board_relevance.allowed_levels)
    primary_by_code: dict[str, tuple[str, ...]] = {}
    relevance_by_code: dict[str, tuple[str, ...]] = {}
    audit: list[dict[str, Any]] = []
    total_edges = 0
    primary_edges = 0
    relevance_edges = 0
    unrated_edges = 0
    excluded_specific_edges = 0
    filtered_relevance_edges = 0
    taxonomy_mapped_edges = 0
    primary_specific_stock_count = 0
    relevance_stock_count = 0
    taxonomy_mapped_stock_count = 0
    for row in top_rows:
        if not isinstance(row, Mapping):
            raise V16DayGateShadowError("snapshot.top_k rows must be objects")
        raw_code = row.get("code")
        if not isinstance(raw_code, str) or not raw_code.strip():
            raise V16DayGateShadowError("snapshot.top_k code must be a non-empty string")
        code = raw_code
        raw_boards = row.get("all_hot_boards")
        if not isinstance(raw_boards, list) or any(
            not isinstance(board, str) or not board.strip() for board in raw_boards
        ):
            raise V16DayGateShadowError(
                f"snapshot.top_k[{code}].all_hot_boards must be a board-name array"
            )
        primary: list[str] = []
        relevance_filtered: list[str] = []
        stock_has_taxonomy_mapping = False
        for board in raw_boards:
            total_edges += 1
            cache_row = relevance.get(f"{board}::{code}")
            level = cache_row["level"] if cache_row else None
            if board in taxonomy_excluded:
                classification = "excluded_taxonomy_label"
                primary_included = False
            elif config.board_relevance.exclude_broad_boards and board in BROAD_CONCEPT_BOARDS:
                classification = "excluded_broad_board"
                primary_included = False
            else:
                classification = "specific_board"
                primary_included = True
                primary.append(board)

            relevance_included = False
            if primary_included:
                if cache_row is None:
                    unrated_edges += 1
                    relevance_included = not config.board_relevance.exclude_unrated
                    relevance_status = (
                        "unrated_retained" if relevance_included else "unrated_excluded"
                    )
                elif level in allowed:
                    relevance_included = True
                    relevance_status = "allowed_relevance"
                else:
                    filtered_relevance_edges += 1
                    relevance_status = "filtered_relevance"
                if relevance_included:
                    relevance_filtered.append(board)
            else:
                excluded_specific_edges += 1
                relevance_status = "not_specific"

            primary_edges += int(primary_included)
            relevance_edges += int(relevance_included)
            taxonomy_mapped = primary_included and board in canonical_theme_map
            taxonomy_mapped_edges += int(taxonomy_mapped)
            stock_has_taxonomy_mapping = stock_has_taxonomy_mapping or taxonomy_mapped
            audit.append(
                {
                    "code": code,
                    "board": board,
                    "classification": classification,
                    "primary_included": primary_included,
                    "relevance_graph_included": relevance_included,
                    "taxonomy_mapped": taxonomy_mapped,
                    "relevance_status": relevance_status,
                    "relevance_level": level,
                    "relevance_reason": cache_row["reason"] if cache_row else None,
                }
            )
        primary = list(dict.fromkeys(primary))
        relevance_filtered = list(dict.fromkeys(relevance_filtered))
        if primary:
            primary_specific_stock_count += 1
        elif raw_boards:
            primary = [f"unlinked-specific-stock:{code}"]
        if relevance_filtered:
            relevance_stock_count += 1
        elif raw_boards:
            relevance_filtered = [f"unlinked-relevance-stock:{code}"]
        primary_by_code[code] = tuple(primary)
        relevance_by_code[code] = tuple(relevance_filtered)
        taxonomy_mapped_stock_count += int(stock_has_taxonomy_mapping)

    ranked_count = len(top_rows)
    coverage = {
        "ranked_stock_count": ranked_count,
        "total_raw_edges": total_edges,
        "primary_specific_edges": primary_edges,
        "relevance_filtered_edges": relevance_edges,
        "unrated_edges": unrated_edges,
        "filtered_relevance_edges": filtered_relevance_edges,
        "taxonomy_mapped_edges": taxonomy_mapped_edges,
        "excluded_specific_edges": excluded_specific_edges,
        "primary_specific_stock_count": primary_specific_stock_count,
        "primary_specific_stock_coverage": (
            primary_specific_stock_count / ranked_count if ranked_count else 0.0
        ),
        "relevance_stock_count": relevance_stock_count,
        "relevance_stock_coverage": (relevance_stock_count / ranked_count if ranked_count else 0.0),
        "taxonomy_mapped_stock_count": taxonomy_mapped_stock_count,
        "taxonomy_mapped_stock_coverage": (
            taxonomy_mapped_stock_count / ranked_count if ranked_count else 0.0
        ),
    }
    return (
        primary_by_code,
        relevance_by_code,
        tuple(audit),
        relevance_hash,
        coverage,
    )


def _hash_if_present(path: Path) -> str | None:
    return sha256_file(path) if path.exists() else None


def _require_sha256(value: str, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise V16DayGateShadowError(f"{field_name} must be 64 lowercase hexadecimal chars")
    return value


def _runtime_identity_payload(
    *,
    config: V16DayGateShadowConfig,
    taxonomy_version: str | None,
    taxonomy_hash: str | None,
    taxonomy_approval_artifact_hash: str | None,
    ranking_model_sha256: str,
    ranking_feature_list_sha256: str,
    filesystem_source_hashes_at_capture: tuple[tuple[str, str | None], ...],
    source_commit: str,
) -> dict[str, Any]:
    return {
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
        "config_hash": config.config_hash,
        "mode": config.mode,
        "top_k": config.top_k,
        "policy": {
            "version": config.policy.version,
            "mode": config.policy.mode.value,
            "thresholds": dict(config.policy.thresholds),
        },
        "taxonomy_version": taxonomy_version,
        "taxonomy_hash": taxonomy_hash,
        "taxonomy_approval_artifact_hash": taxonomy_approval_artifact_hash,
        "ranking_model_sha256": ranking_model_sha256,
        "ranking_feature_list_sha256": ranking_feature_list_sha256,
        "filesystem_source_hashes_at_capture": dict(filesystem_source_hashes_at_capture),
        "source_commit": source_commit,
    }


def freeze_v16_day_gate_runtime(
    project_root: Path,
    *,
    ranking_model_sha256: str,
    ranking_feature_list_sha256: str,
    captured_at: datetime,
    config: V16DayGateShadowConfig | None = None,
    config_path: Path | None = None,
) -> FrozenV16DayGateRuntime | None:
    """Freeze the small decision runtime before a shadow job is enqueued.

    Config and the optional approved taxonomy artifact are read and compiled
    here exactly once.  An ``off`` config returns ``None`` before a scan
    snapshot is built.  Relevance-cache I/O remains in the background because
    that legacy cache is diagnostic-only.
    """

    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise V16DayGateShadowError("captured_at must be timezone-aware")
    if config is not None and config_path is not None:
        raise V16DayGateShadowError("pass either config or config_path, not both")
    frozen_config = config or load_shadow_config(project_root, config_path)
    if frozen_config.mode == "off":
        return None
    if frozen_config.mode != "shadow" or frozen_config.policy.mode is not GateMode.SHADOW:
        raise V16DayGateShadowError("frozen runtime accepts shadow policy only")

    model_hash = _require_sha256(ranking_model_sha256, "ranking_model_sha256")
    feature_hash = _require_sha256(
        ranking_feature_list_sha256,
        "ranking_feature_list_sha256",
    )
    (
        canonical_map,
        taxonomy_excluded,
        taxonomy_version,
        taxonomy_hash,
        taxonomy_approval_artifact_hash,
    ) = _load_approved_taxonomy(
        frozen_config.approved_taxonomy_artifact_path,
        frozen_config.approved_taxonomy_artifact_sha256,
    )

    source_paths = {
        "board_filter": project_root / "src" / "strategy" / "filters" / "board_filter.py",
        "lgbrank_scorer": project_root / "src" / "strategy" / "lgbrank_scorer.py",
        "v16_day_gate": project_root / "src" / "strategy" / "v16_day_gate.py",
        "v16_day_gate_shadow": project_root / "src" / "strategy" / "v16_day_gate_shadow.py",
        "v16_scanner": project_root / "src" / "strategy" / "strategies" / "v16_scanner.py",
        "v16_theme_semantics": project_root / "src" / "strategy" / "v16_theme_semantics.py",
    }
    filesystem_source_hashes_at_capture = tuple(
        (name, _hash_if_present(path)) for name, path in sorted(source_paths.items())
    )
    source_commit = os.getenv("GIT_COMMIT_SHA") or "unknown"
    identity = _runtime_identity_payload(
        config=frozen_config,
        taxonomy_version=taxonomy_version,
        taxonomy_hash=taxonomy_hash,
        taxonomy_approval_artifact_hash=taxonomy_approval_artifact_hash,
        ranking_model_sha256=model_hash,
        ranking_feature_list_sha256=feature_hash,
        filesystem_source_hashes_at_capture=filesystem_source_hashes_at_capture,
        source_commit=source_commit,
    )
    context_id = _sha256_bytes(
        json.dumps(
            identity,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )
    return FrozenV16DayGateRuntime(
        captured_at=captured_at,
        config=frozen_config,
        canonical_theme_items=tuple(sorted(canonical_map.items())),
        taxonomy_excluded_aliases=tuple(sorted(taxonomy_excluded)),
        taxonomy_version=taxonomy_version,
        taxonomy_hash=taxonomy_hash,
        taxonomy_approval_artifact_hash=taxonomy_approval_artifact_hash,
        ranking_model_sha256=model_hash,
        ranking_feature_list_sha256=feature_hash,
        filesystem_source_hashes_at_capture=filesystem_source_hashes_at_capture,
        source_commit=source_commit,
        context_id=context_id,
    )


def frozen_runtime_manifest(runtime: FrozenV16DayGateRuntime) -> dict[str, Any]:
    """Return a detached, JSON-native manifest for evidence provenance."""

    identity = _runtime_identity_payload(
        config=runtime.config,
        taxonomy_version=runtime.taxonomy_version,
        taxonomy_hash=runtime.taxonomy_hash,
        taxonomy_approval_artifact_hash=runtime.taxonomy_approval_artifact_hash,
        ranking_model_sha256=runtime.ranking_model_sha256,
        ranking_feature_list_sha256=runtime.ranking_feature_list_sha256,
        filesystem_source_hashes_at_capture=runtime.filesystem_source_hashes_at_capture,
        source_commit=runtime.source_commit,
    )
    return {
        **identity,
        "context_id": runtime.context_id,
        "captured_at": runtime.captured_at.isoformat(),
    }


def prepare_shadow_decision(
    snapshot: Mapping[str, Any],
    runtime: FrozenV16DayGateRuntime,
    project_root: Path,
) -> PreparedShadowDecision:
    """Evaluate one frozen snapshot using only local, versioned artifacts."""

    if snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise V16DayGateShadowError("unsupported or missing snapshot schema")
    config = runtime.config
    if not config.enabled or config.policy.mode is not GateMode.SHADOW:
        raise V16DayGateShadowError("shadow evaluation requested with a non-shadow runtime")
    top_rows = snapshot.get("top_k")
    if not isinstance(top_rows, list):
        raise V16DayGateShadowError("snapshot.top_k must be an array")
    top_rows = top_rows[: config.top_k]

    canonical_map = dict(runtime.canonical_theme_items)
    taxonomy_excluded = set(runtime.taxonomy_excluded_aliases)
    taxonomy_version = runtime.taxonomy_version
    taxonomy_hash = runtime.taxonomy_hash
    taxonomy_approval_artifact_hash = runtime.taxonomy_approval_artifact_hash
    (
        boards_by_code,
        relevance_boards_by_code,
        edge_audit,
        relevance_hash,
        edge_coverage,
    ) = _prepare_board_edges(
        top_rows,
        config,
        project_root,
        canonical_map,
        taxonomy_excluded,
    )
    ranked_codes = tuple(str(row["code"]) for row in top_rows)
    drivers: dict[str, bool] = {}
    for row in top_rows:
        driver = row.get("is_driver")
        if driver is None:
            continue
        if not isinstance(driver, bool):
            raise V16DayGateShadowError(
                f"snapshot.top_k[{row.get('code')}].is_driver must be boolean or null"
            )
        drivers[str(row["code"])] = driver
    cutoff = datetime.fromisoformat(str(snapshot["decision_cutoff"]))
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise V16DayGateShadowError("snapshot.decision_cutoff must be timezone-aware")
    if cutoff != runtime.captured_at:
        raise V16DayGateShadowError(
            "snapshot.decision_cutoff does not match its frozen runtime capture"
        )
    model_hash = runtime.ranking_model_sha256
    feature_hash = runtime.ranking_feature_list_sha256
    gate_input = V16DayGateInput(
        cutoff_ts=cutoff,
        ranked_top_k=ranked_codes,
        stock_all_boards=boards_by_code,
        stock_is_driver=drivers,
        model_version=model_hash,
        canonical_theme_map=canonical_map,
        taxonomy_version=taxonomy_version,
        upstream_data_complete=True,
    )
    decision = V16DayGate(config.policy).evaluate(gate_input)
    relevance_gate_input = V16DayGateInput(
        cutoff_ts=cutoff,
        ranked_top_k=ranked_codes,
        stock_all_boards=relevance_boards_by_code,
        stock_is_driver=drivers,
        model_version=model_hash,
        canonical_theme_map=canonical_map,
        taxonomy_version=taxonomy_version,
        upstream_data_complete=True,
    )
    relevance_filtered_metrics = V16DayGate().evaluate(relevance_gate_input).metrics
    resolved_root = project_root.resolve()
    resolved_config = config.config_path.resolve()
    runtime_manifest = frozen_runtime_manifest(runtime)
    provenance = {
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
        "context_id": runtime.context_id,
        "context_captured_at": runtime.captured_at.isoformat(),
        "context_manifest": runtime_manifest,
        "config_path": str(resolved_config.relative_to(resolved_root)),
        "config_hash": config.config_hash,
        "source_commit": runtime.source_commit,
        "snapshot_source_commit": snapshot.get("source_commit", "unknown"),
        "scanner_runtime_version": (
            runtime.source_commit if runtime.source_commit != "unknown" else runtime.context_id
        ),
        "scanner_filesystem_hash_at_context_capture": dict(
            runtime.filesystem_source_hashes_at_capture
        ).get("v16_scanner"),
        "model_hash": model_hash,
        "feature_hash": feature_hash,
        "board_relevance_cache_hash": relevance_hash,
        "board_relevance_cache_provenance": {
            "provider": "dashscope",
            "model": "qwen-plus",
            "generation_version": "legacy_unversioned",
            "decision_role": "parallel_diagnostic_only",
            "timing": "evaluation_time_not_runtime_frozen",
        },
        "taxonomy_hash": taxonomy_hash,
        "taxonomy_approval_artifact_hash": taxonomy_approval_artifact_hash,
        "taxonomy_version": taxonomy_version,
    }
    return PreparedShadowDecision(
        gate_input=gate_input,
        decision=decision,
        taxonomy_hash=taxonomy_hash,
        taxonomy_approval_artifact_hash=taxonomy_approval_artifact_hash,
        relevance_cache_hash=relevance_hash,
        relevance_filtered_metrics=relevance_filtered_metrics,
        edge_coverage=edge_coverage,
        edge_audit=edge_audit,
        provenance=provenance,
    )


def shadow_message(
    snapshot: Mapping[str, Any],
    prepared: PreparedShadowDecision,
    evidence_path: Path | None,
) -> str:
    """Render a compact, explicit Feishu shadow message."""

    decision = prepared.decision
    metrics = decision.metrics
    hypothetical = _hypothetical_action(decision.state.value).upper()
    reasons = ", ".join(reason.value for reason in decision.reasons)
    path_text = str(evidence_path) if evidence_path is not None else "not_persisted"
    mews = snapshot.get("mews") or MEWS_UNKNOWN
    gains = [
        float(row["gain_from_open_pct"])
        for row in snapshot.get("top_k", ())
        if row.get("gain_from_open_pct") is not None
    ]
    mean_gain = sum(gains) / len(gains) if gains else 0.0
    gain_3_share = sum(gain >= 3.0 for gain in gains) / len(gains) if gains else 0.0
    funnel = snapshot.get("funnel_counts") or {}
    return "\n".join(
        [
            "[V16 DayGate] SHADOW（不影响下单）",
            f"run_id: {snapshot['run_id']}",
            f"cutoff: {snapshot['decision_cutoff']}",
            f"state: {decision.state.value} | reasons: {reasons}",
            f"hypothetical_action: {hypothetical}",
            "effective_action: PASS_THROUGH",
            (
                "metrics: "
                f"main_cluster={metrics.largest_cluster_share:.1%}, "
                f"effective_clusters={metrics.effective_cluster_count:.2f}, "
                f"top3_coverage={metrics.top3_main_cluster_coverage:.1%}, "
                f"driver_breadth={metrics.driver_breadth:.1%}"
            ),
            (
                "crowding diagnostics (not gated): "
                f"mean_gain={mean_gain:+.2f}%, gain>=3%={gain_3_share:.1%}, "
                f"hot_boards={funnel.get('step2_hot_board_count', '-')}, "
                f"final_candidates={funnel.get('final_candidates', '-')}"
            ),
            (
                "edge coverage: "
                f"specific={prepared.edge_coverage['primary_specific_stock_coverage']:.1%}, "
                f"relevance={prepared.edge_coverage['relevance_stock_coverage']:.1%}, "
                f"taxonomy={prepared.edge_coverage['taxonomy_mapped_stock_coverage']:.1%}"
            ),
            f"policy: {decision.policy_version or 'none'} | taxonomy: "
            f"{prepared.provenance.get('taxonomy_version') or 'raw-board'}",
            f"MEWS: {mews.get('status', 'unknown')} ({mews.get('reason', '-')})",
            f"evidence: {path_text}",
        ]
    )


def _hypothetical_action(state: str) -> str:
    if state == "trade":
        return "allow"
    if state == "watch":
        return "watch_undecided"
    if state == "no_trade":
        return "block_new_entry"
    raise V16DayGateShadowError(f"unsupported gate state: {state!r}")


def prepared_to_metadata(prepared: PreparedShadowDecision) -> dict[str, Any]:
    """Return JSON-native metadata for the evidence writer."""

    return {
        "provenance": _json_copy(prepared.provenance),
        "taxonomy_hash": prepared.taxonomy_hash,
        "taxonomy_approval_artifact_hash": prepared.taxonomy_approval_artifact_hash,
        "relevance_cache_hash": prepared.relevance_cache_hash,
        "edge_coverage": _json_copy(prepared.edge_coverage),
        "edge_audit": _json_copy(list(prepared.edge_audit)),
        "relevance_filtered_metrics": _json_copy(asdict(prepared.relevance_filtered_metrics)),
        "effective_action": "pass_through",
        "hypothetical_action": _hypothetical_action(prepared.decision.state.value),
    }


__all__ = [
    "FrozenV16DayGateRuntime",
    "MEWS_UNKNOWN",
    "PreparedShadowDecision",
    "RUNTIME_SCHEMA_VERSION",
    "SNAPSHOT_SCHEMA_VERSION",
    "V16DayGateShadowConfig",
    "V16DayGateShadowError",
    "freeze_v16_day_gate_runtime",
    "freeze_v16_scan_snapshot",
    "frozen_runtime_manifest",
    "load_shadow_config",
    "prepare_shadow_decision",
    "prepared_to_metadata",
    "sha256_file",
    "shadow_message",
]
