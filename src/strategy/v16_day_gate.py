"""Deterministic day-level trade gate for the V16 ranking strategy.

The LGBRank model answers a stock-level question: which surviving candidate
should rank ahead of another candidate on the same day.  This module answers a
separate, day-level question: whether the ranked basket is coherent enough to
be eligible for execution.

The gate is deliberately self-contained.  It performs no I/O, reads no current
board files, and owns no fitted defaults.  Callers must freeze all inputs at the
decision cutoff and explicitly inject a versioned policy.  Without a calibrated
policy the result is ``WATCH / POLICY_UNCALIBRATED``.  Policies default to
shadow mode, so evaluating a gate cannot change execution unless a caller
explicitly supplies a live policy and honours ``blocks_trade``.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

INPUT_SCHEMA_VERSION = "v16-day-gate-input/v1"
OUTPUT_SCHEMA_VERSION = "v16-day-gate-decision/v1"
POLICY_SCHEMA_VERSION = "v16-day-gate-policy/v1"


class GateState(str, Enum):
    """Day-level assessment returned by the gate."""

    TRADE = "trade"
    WATCH = "watch"
    NO_TRADE = "no_trade"


class GateMode(str, Enum):
    """Whether a decision is observational or executable."""

    SHADOW = "shadow"
    LIVE = "live"


class GateReason(str, Enum):
    """Stable, versioned reason codes for logging and downstream consumers."""

    PASS = "v1.pass"
    SHADOW_MODE = "v1.shadow_mode"
    POLICY_UNCALIBRATED = "v1.policy_uncalibrated"
    DATA_INCOMPLETE = "v1.data_incomplete"
    NO_RANKED_CANDIDATES = "v1.no_ranked_candidates"
    DUPLICATE_RANKED_CODE = "v1.duplicate_ranked_code"
    MISSING_BOARD_MEMBERSHIP = "v1.missing_board_membership"
    MISSING_DRIVER_FLAG = "v1.missing_driver_flag"
    LARGEST_CLUSTER_TOO_SMALL = "v1.largest_cluster_too_small"
    EFFECTIVE_CLUSTER_COUNT_TOO_HIGH = "v1.effective_cluster_count_too_high"
    TOP3_MAIN_CLUSTER_COVERAGE_TOO_LOW = "v1.top3_main_cluster_coverage_too_low"
    DRIVER_BREADTH_TOO_LOW = "v1.driver_breadth_too_low"


@dataclass(frozen=True)
class V16DayGateInput:
    """Point-in-time evidence needed to assess one ranked V16 basket.

    ``ranked_top_k`` is already ordered best-first by LambdaRank.  Board and
    driver mappings must cover those codes.  ``canonical_theme_map`` is an
    optional, point-in-time mapping from raw board name to a canonical theme or
    supply-chain name.  It may be partial; unmapped boards retain their raw
    names.
    """

    cutoff_ts: datetime
    ranked_top_k: tuple[str, ...]
    stock_all_boards: Mapping[str, Sequence[str]]
    stock_is_driver: Mapping[str, bool]
    model_version: str
    canonical_theme_map: Mapping[str, str] | None = None
    taxonomy_version: str | None = None
    upstream_data_complete: bool = True
    data_quality_issues: tuple[str, ...] = ()
    schema_version: str = INPUT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INPUT_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported input schema {self.schema_version!r}; "
                f"expected {INPUT_SCHEMA_VERSION!r}"
            )
        if not self.model_version.strip():
            raise ValueError("model_version must not be empty")


@dataclass(frozen=True)
class V16DayGatePolicy:
    """Explicitly injected policy; no empirical threshold has a code default."""

    version: str
    mode: GateMode = GateMode.SHADOW
    min_largest_cluster_share: float | None = None
    max_effective_cluster_count: float | None = None
    min_top3_main_cluster_coverage: float | None = None
    min_driver_breadth: float | None = None
    schema_version: str = POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_SCHEMA_VERSION:
            raise ValueError(
                f"unsupported policy schema {self.schema_version!r}; "
                f"expected {POLICY_SCHEMA_VERSION!r}"
            )
        if not self.version.strip():
            raise ValueError("policy version must not be empty")
        _validate_unit_interval("min_largest_cluster_share", self.min_largest_cluster_share)
        _validate_unit_interval(
            "min_top3_main_cluster_coverage", self.min_top3_main_cluster_coverage
        )
        _validate_unit_interval("min_driver_breadth", self.min_driver_breadth)
        if self.max_effective_cluster_count is not None and (
            isinstance(self.max_effective_cluster_count, bool)
            or not math.isfinite(self.max_effective_cluster_count)
            or self.max_effective_cluster_count <= 0
        ):
            raise ValueError("max_effective_cluster_count must be finite and greater than zero")

    @property
    def has_rules(self) -> bool:
        """Return whether at least one calibrated threshold is active."""

        return any(value is not None for _name, value in self.thresholds)

    @property
    def thresholds(self) -> tuple[tuple[str, float | None], ...]:
        """Return thresholds in a stable order for evaluation and audit."""

        return (
            ("min_largest_cluster_share", self.min_largest_cluster_share),
            ("max_effective_cluster_count", self.max_effective_cluster_count),
            ("min_top3_main_cluster_coverage", self.min_top3_main_cluster_coverage),
            ("min_driver_breadth", self.min_driver_breadth),
        )


@dataclass(frozen=True)
class V16DayGateMetrics:
    """Deterministic graph and breadth metrics for one ranked Top-K basket."""

    ranked_count: int
    themed_stock_count: int
    theme_coverage: float
    component_count: int
    largest_cluster_size: int
    largest_cluster_share: float
    largest_cluster_codes: tuple[str, ...]
    largest_cluster_themes: tuple[str, ...]
    effective_cluster_count: float
    top3_main_cluster_coverage: float
    driver_count: int
    driver_breadth: float


@dataclass(frozen=True)
class V16DayGateDecision:
    """Versioned gate result.

    ``state`` is the policy assessment.  ``blocks_trade`` also accounts for
    mode: shadow decisions are observable but never enforce a block.
    """

    state: GateState
    mode: GateMode
    reasons: tuple[GateReason, ...]
    metrics: V16DayGateMetrics
    policy_version: str | None
    applied_thresholds: tuple[tuple[str, float], ...]
    data_quality_issues: tuple[str, ...]
    input_schema_version: str = INPUT_SCHEMA_VERSION
    output_schema_version: str = OUTPUT_SCHEMA_VERSION

    @property
    def blocks_trade(self) -> bool:
        """Whether an execution consumer should block this ranked basket."""

        return self.mode is GateMode.LIVE and self.state is not GateState.TRADE


class V16DayGate:
    """Pure, deterministic evaluator for a frozen V16 day snapshot."""

    def __init__(self, policy: V16DayGatePolicy | None = None):
        self._policy = policy

    def evaluate(self, gate_input: V16DayGateInput) -> V16DayGateDecision:
        """Build the shared-theme graph, compute metrics, and apply the policy."""

        ranked_codes, duplicate_codes = _unique_ranked_codes(gate_input.ranked_top_k)
        themes_by_code = _themes_by_code(
            ranked_codes,
            gate_input.stock_all_boards,
            gate_input.canonical_theme_map,
        )
        components = _connected_components(ranked_codes, themes_by_code)
        metrics = _compute_metrics(
            ranked_codes,
            components,
            themes_by_code,
            gate_input.stock_is_driver,
        )

        missing_boards = tuple(code for code in ranked_codes if not themes_by_code[code])
        missing_drivers = tuple(
            code for code in ranked_codes if code not in gate_input.stock_is_driver
        )
        quality_issues = _quality_issues(
            gate_input,
            duplicate_codes,
            missing_boards,
            missing_drivers,
        )
        mode = self._policy.mode if self._policy is not None else GateMode.SHADOW

        if not ranked_codes:
            return self._decision(
                state=GateState.NO_TRADE,
                mode=mode,
                reasons=(GateReason.NO_RANKED_CANDIDATES,),
                metrics=metrics,
                quality_issues=quality_issues,
            )

        data_reasons: list[GateReason] = []
        if not gate_input.upstream_data_complete or gate_input.data_quality_issues:
            data_reasons.append(GateReason.DATA_INCOMPLETE)
        if duplicate_codes:
            data_reasons.extend((GateReason.DATA_INCOMPLETE, GateReason.DUPLICATE_RANKED_CODE))
        if missing_boards:
            data_reasons.extend((GateReason.DATA_INCOMPLETE, GateReason.MISSING_BOARD_MEMBERSHIP))
        if missing_drivers:
            data_reasons.extend((GateReason.DATA_INCOMPLETE, GateReason.MISSING_DRIVER_FLAG))
        if data_reasons:
            return self._decision(
                state=GateState.NO_TRADE,
                mode=mode,
                reasons=_deduplicate(data_reasons),
                metrics=metrics,
                quality_issues=quality_issues,
            )

        if self._policy is None or not self._policy.has_rules:
            return self._decision(
                state=GateState.WATCH,
                mode=mode,
                reasons=(GateReason.POLICY_UNCALIBRATED,),
                metrics=metrics,
                quality_issues=quality_issues,
            )

        failures = self._policy_failures(metrics, self._policy)
        if failures:
            reasons = failures
            state = GateState.NO_TRADE
        else:
            reasons = (GateReason.PASS,)
            state = GateState.TRADE

        if mode is GateMode.SHADOW:
            reasons = (*reasons, GateReason.SHADOW_MODE)

        return self._decision(
            state=state,
            mode=mode,
            reasons=reasons,
            metrics=metrics,
            quality_issues=quality_issues,
        )

    def _decision(
        self,
        *,
        state: GateState,
        mode: GateMode,
        reasons: tuple[GateReason, ...],
        metrics: V16DayGateMetrics,
        quality_issues: tuple[str, ...],
    ) -> V16DayGateDecision:
        policy_version = self._policy.version if self._policy is not None else None
        applied_thresholds: tuple[tuple[str, float], ...] = ()
        if self._policy is not None:
            applied_thresholds = tuple(
                (name, value) for name, value in self._policy.thresholds if value is not None
            )
        return V16DayGateDecision(
            state=state,
            mode=mode,
            reasons=reasons,
            metrics=metrics,
            policy_version=policy_version,
            applied_thresholds=applied_thresholds,
            data_quality_issues=quality_issues,
        )

    @staticmethod
    def _policy_failures(
        metrics: V16DayGateMetrics,
        policy: V16DayGatePolicy,
    ) -> tuple[GateReason, ...]:
        failures: list[GateReason] = []
        if (
            policy.min_largest_cluster_share is not None
            and metrics.largest_cluster_share < policy.min_largest_cluster_share
        ):
            failures.append(GateReason.LARGEST_CLUSTER_TOO_SMALL)
        if (
            policy.max_effective_cluster_count is not None
            and metrics.effective_cluster_count > policy.max_effective_cluster_count
        ):
            failures.append(GateReason.EFFECTIVE_CLUSTER_COUNT_TOO_HIGH)
        if (
            policy.min_top3_main_cluster_coverage is not None
            and metrics.top3_main_cluster_coverage < policy.min_top3_main_cluster_coverage
        ):
            failures.append(GateReason.TOP3_MAIN_CLUSTER_COVERAGE_TOO_LOW)
        if (
            policy.min_driver_breadth is not None
            and metrics.driver_breadth < policy.min_driver_breadth
        ):
            failures.append(GateReason.DRIVER_BREADTH_TOO_LOW)
        return tuple(failures)


def _validate_unit_interval(name: str, value: float | None) -> None:
    if value is not None and (
        isinstance(value, bool) or not math.isfinite(value) or not 0 <= value <= 1
    ):
        raise ValueError(f"{name} must be finite and between zero and one")


def _unique_ranked_codes(codes: Sequence[str]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    unique: list[str] = []
    duplicates: list[str] = []
    seen: set[str] = set()
    duplicate_seen: set[str] = set()
    for raw_code in codes:
        code = str(raw_code).strip()
        if not code:
            continue
        if code in seen:
            if code not in duplicate_seen:
                duplicates.append(code)
                duplicate_seen.add(code)
            continue
        seen.add(code)
        unique.append(code)
    return tuple(unique), tuple(duplicates)


def _themes_by_code(
    ranked_codes: Sequence[str],
    stock_all_boards: Mapping[str, Sequence[str]],
    canonical_theme_map: Mapping[str, str] | None,
) -> dict[str, frozenset[str]]:
    canonical = {
        str(board).strip(): str(theme).strip()
        for board, theme in (canonical_theme_map or {}).items()
        if str(board).strip() and str(theme).strip()
    }
    result: dict[str, frozenset[str]] = {}
    for code in ranked_codes:
        themes: set[str] = set()
        for raw_board in stock_all_boards.get(code, ()):
            board = str(raw_board).strip()
            if not board:
                continue
            themes.add(canonical.get(board, board))
        result[code] = frozenset(themes)
    return result


def _connected_components(
    ranked_codes: Sequence[str],
    themes_by_code: Mapping[str, frozenset[str]],
) -> tuple[tuple[str, ...], ...]:
    if not ranked_codes:
        return ()

    parent = {code: code for code in ranked_codes}

    def find(code: str) -> str:
        root = code
        while parent[root] != root:
            root = parent[root]
        while parent[code] != code:
            next_code = parent[code]
            parent[code] = root
            code = next_code
        return root

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    stocks_by_theme: dict[str, list[str]] = defaultdict(list)
    for code in ranked_codes:
        for theme in sorted(themes_by_code[code]):
            stocks_by_theme[theme].append(code)

    for stocks in stocks_by_theme.values():
        anchor = stocks[0]
        for code in stocks[1:]:
            union(anchor, code)

    grouped: dict[str, list[str]] = defaultdict(list)
    for code in ranked_codes:
        grouped[find(code)].append(code)

    rank_index = {code: index for index, code in enumerate(ranked_codes)}
    components = [tuple(codes) for codes in grouped.values()]
    components.sort(key=lambda codes: (-len(codes), rank_index[codes[0]]))
    return tuple(components)


def _compute_metrics(
    ranked_codes: Sequence[str],
    components: Sequence[tuple[str, ...]],
    themes_by_code: Mapping[str, frozenset[str]],
    stock_is_driver: Mapping[str, bool],
) -> V16DayGateMetrics:
    ranked_count = len(ranked_codes)
    if ranked_count == 0:
        return V16DayGateMetrics(
            ranked_count=0,
            themed_stock_count=0,
            theme_coverage=0.0,
            component_count=0,
            largest_cluster_size=0,
            largest_cluster_share=0.0,
            largest_cluster_codes=(),
            largest_cluster_themes=(),
            effective_cluster_count=0.0,
            top3_main_cluster_coverage=0.0,
            driver_count=0,
            driver_breadth=0.0,
        )

    largest_cluster = components[0]
    largest_set = set(largest_cluster)
    component_shares = [len(component) / ranked_count for component in components]
    concentration = sum(share * share for share in component_shares)
    effective_cluster_count = 1.0 / concentration

    top3 = ranked_codes[:3]
    top3_main_count = sum(code in largest_set for code in top3)
    driver_count = sum(bool(stock_is_driver.get(code, False)) for code in ranked_codes)
    themed_stock_count = sum(bool(themes_by_code[code]) for code in ranked_codes)
    largest_themes = sorted({theme for code in largest_cluster for theme in themes_by_code[code]})

    return V16DayGateMetrics(
        ranked_count=ranked_count,
        themed_stock_count=themed_stock_count,
        theme_coverage=themed_stock_count / ranked_count,
        component_count=len(components),
        largest_cluster_size=len(largest_cluster),
        largest_cluster_share=len(largest_cluster) / ranked_count,
        largest_cluster_codes=largest_cluster,
        largest_cluster_themes=tuple(largest_themes),
        effective_cluster_count=effective_cluster_count,
        top3_main_cluster_coverage=top3_main_count / len(top3),
        driver_count=driver_count,
        driver_breadth=driver_count / ranked_count,
    )


def _quality_issues(
    gate_input: V16DayGateInput,
    duplicate_codes: Sequence[str],
    missing_boards: Sequence[str],
    missing_drivers: Sequence[str],
) -> tuple[str, ...]:
    issues = list(gate_input.data_quality_issues)
    if not gate_input.upstream_data_complete and not gate_input.data_quality_issues:
        issues.append("upstream_data_complete=false")
    if duplicate_codes:
        issues.append(f"duplicate_ranked_codes={','.join(duplicate_codes)}")
    if missing_boards:
        issues.append(f"missing_board_membership={','.join(missing_boards)}")
    if missing_drivers:
        issues.append(f"missing_driver_flag={','.join(missing_drivers)}")
    return tuple(issues)


def _deduplicate(reasons: Sequence[GateReason]) -> tuple[GateReason, ...]:
    return tuple(dict.fromkeys(reasons))
