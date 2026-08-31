"""Deterministic, offline protocol for V16 board-theme normalization.

This module deliberately contains no LLM client and no trading decision API.  Codex,
Kimi, and a human reviewer may produce *proposals* using the same strict JSON schema,
but a proposal cannot directly allow, block, size, or otherwise control an order.

The protocol is designed around four reproducibility rules:

* every input board is covered exactly once;
* every evidence claim points back to structured request data;
* protocol, taxonomy, request, prompt, response, and audit documents are hashed;
* two independently produced responses can be compared without free-text parsing.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Final, Literal, NoReturn

PROTOCOL_VERSION: Final = "v16-theme-semantics/1.0"
TAXONOMY_APPROVAL_SCHEMA_VERSION: Final = "v16-theme-taxonomy-approval/1.0"
PROVIDERS: Final = ("codex", "human", "kimi")
THEME_LABELS: Final = ("noise", "theme", "umbrella")
EVIDENCE_KINDS: Final = (
    "cooccurrence",
    "manual_review",
    "name_similarity",
    "stock_overlap",
    "taxonomy_alias",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_ID_RE = re.compile(r"^(noise|theme|umbrella):[a-z0-9][a-z0-9._-]{0,79}$")
_MODEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_STOCK_CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")


class ThemeSemanticsValidationError(ValueError):
    """Raised when a protocol document is not strict, canonical, or self-consistent."""


class UnapprovedThemeSemanticsError(RuntimeError):
    """Raised when a candidate Agent response is used as an approved gate taxonomy."""


@dataclass(frozen=True)
class ThemeSemanticIndex:
    """Immutable alias index with an explicit approval boundary.

    ``raw_to_canonical_theme_id`` retains all labels for audit and display.
    ``excluded_aliases`` contains every ``umbrella`` or ``noise`` alias.  Gate code
    must call :meth:`bridge_theme_id`, which only works for an approved taxonomy
    and returns IDs for ``label=theme`` aliases only.
    """

    approval_status: Literal["approved", "candidate"]
    source_hash: str
    raw_to_canonical_theme_id: Mapping[str, str]
    excluded_aliases: frozenset[str]

    def bridge_theme_id(self, raw_board: str) -> str | None:
        """Return an approved leaf-theme ID, or ``None`` for excluded/unknown aliases."""

        if self.approval_status != "approved":
            raise UnapprovedThemeSemanticsError(
                "candidate Agent output cannot be used by V16 gate logic; "
                "promote it through a separately reviewed taxonomy artifact"
            )
        if raw_board in self.excluded_aliases:
            return None
        return self.raw_to_canonical_theme_id.get(raw_board)


def canonical_json(value: Any) -> str:
    """Return the only JSON serialization used for protocol hashes."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ThemeSemanticsValidationError(f"value is not canonical JSON: {exc}") from exc


def sha256_json(value: Any) -> str:
    """Hash a JSON-compatible value using :func:`canonical_json`."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_text(value: str) -> str:
    """Hash UTF-8 text without newline or platform normalization."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_json_strict(text: str) -> Any:
    """Parse JSON while rejecting duplicate keys and non-finite number constants."""

    def reject_constant(token: str) -> None:
        raise ThemeSemanticsValidationError(f"non-finite JSON number is forbidden: {token}")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ThemeSemanticsValidationError(f"duplicate JSON object key: {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ThemeSemanticsValidationError:
        raise
    except json.JSONDecodeError as exc:
        raise ThemeSemanticsValidationError(f"invalid JSON: {exc}") from exc


NORMALIZATION_REQUEST_SCHEMA: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "urn:v16-theme-semantics:request:1.0",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "schema_version",
        "protocol_hash",
        "request_hash",
        "taxonomy_hash",
        "allow_new_themes",
        "raw_boards",
        "stocks",
        "cooccurrences",
        "taxonomy",
    ],
    "properties": {
        "schema_version": {"const": PROTOCOL_VERSION},
        "protocol_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "request_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "taxonomy_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "allow_new_themes": {"type": "boolean"},
        "raw_boards": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {"type": "string", "minLength": 1, "maxLength": 120},
        },
        "stocks": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["stock_code", "board_names"],
                "properties": {
                    "stock_code": {"type": "string", "pattern": _STOCK_CODE_RE.pattern},
                    "board_names": {
                        "type": "array",
                        "minItems": 1,
                        "uniqueItems": True,
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "cooccurrences": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["board_a", "board_b", "count"],
                "properties": {
                    "board_a": {"type": "string"},
                    "board_b": {"type": "string"},
                    "count": {"type": "integer", "minimum": 1},
                },
            },
        },
        "taxonomy": {
            "type": "object",
            "additionalProperties": False,
            "required": ["taxonomy_version", "themes"],
            "properties": {
                "taxonomy_version": {"type": "string", "minLength": 1, "maxLength": 120},
                "themes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "canonical_theme_id",
                            "canonical_name",
                            "label",
                            "aliases",
                        ],
                        "properties": {
                            "canonical_theme_id": {
                                "type": "string",
                                "pattern": _CANONICAL_ID_RE.pattern,
                            },
                            "canonical_name": {"type": "string", "minLength": 1},
                            "label": {"enum": list(THEME_LABELS)},
                            "aliases": {
                                "type": "array",
                                "minItems": 1,
                                "uniqueItems": True,
                                "items": {"type": "string", "minLength": 1},
                            },
                        },
                    },
                },
            },
        },
    },
}

_EVIDENCE_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["kind", "board_names", "stock_codes"],
    "properties": {
        "kind": {"enum": list(EVIDENCE_KINDS)},
        "board_names": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {"type": "string"},
        },
        "stock_codes": {
            "type": "array",
            "uniqueItems": True,
            "items": {"type": "string", "pattern": _STOCK_CODE_RE.pattern},
        },
    },
}

_THEME_PROPOSAL_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "canonical_theme_id",
        "canonical_name",
        "label",
        "aliases",
        "confidence",
        "evidence",
    ],
    "properties": {
        "canonical_theme_id": {"type": "string", "pattern": _CANONICAL_ID_RE.pattern},
        "canonical_name": {"type": "string", "minLength": 1, "maxLength": 120},
        "label": {"enum": list(THEME_LABELS)},
        "aliases": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {"type": "string"},
        },
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "evidence": {"type": "array", "minItems": 1, "items": _EVIDENCE_SCHEMA},
    },
}

NORMALIZATION_RESPONSE_SCHEMA: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "urn:v16-theme-semantics:response:1.0",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "schema_version",
        "protocol_hash",
        "request_hash",
        "taxonomy_hash",
        "provider",
        "model",
        "prompt_hash",
        "themes",
    ],
    "properties": {
        "schema_version": {"const": PROTOCOL_VERSION},
        "protocol_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "request_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "taxonomy_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "provider": {"enum": list(PROVIDERS)},
        "model": {"type": "string", "pattern": _MODEL_RE.pattern},
        "prompt_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "themes": {"type": "array", "minItems": 1, "items": _THEME_PROPOSAL_SCHEMA},
    },
}

_AGENT_IDENTITY_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["provider", "model", "prompt_hash", "response_hash"],
    "properties": {
        "provider": {"enum": list(PROVIDERS)},
        "model": {"type": "string", "pattern": _MODEL_RE.pattern},
        "prompt_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "response_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
    },
}

_DISAGREEMENT_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "raw_board",
        "fields",
        "semantic_difference",
        "left",
        "right",
    ],
    "properties": {
        "raw_board": {"type": "string"},
        "fields": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "enum": [
                    "canonical_theme_id",
                    "canonical_name",
                    "label",
                    "aliases",
                    "confidence",
                    "evidence",
                ]
            },
        },
        "semantic_difference": {"type": "boolean"},
        "left": _THEME_PROPOSAL_SCHEMA,
        "right": _THEME_PROPOSAL_SCHEMA,
    },
}

DISAGREEMENT_AUDIT_SCHEMA: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "urn:v16-theme-semantics:audit:1.0",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "schema_version",
        "protocol_hash",
        "request_hash",
        "left_agent",
        "right_agent",
        "independent_agents",
        "semantic_agreement",
        "exact_agreement",
        "disagreements",
        "audit_hash",
    ],
    "properties": {
        "schema_version": {"const": PROTOCOL_VERSION},
        "protocol_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "request_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
        "left_agent": _AGENT_IDENTITY_SCHEMA,
        "right_agent": _AGENT_IDENTITY_SCHEMA,
        "independent_agents": {"type": "boolean"},
        "semantic_agreement": {"type": "boolean"},
        "exact_agreement": {"type": "boolean"},
        "disagreements": {"type": "array", "items": _DISAGREEMENT_SCHEMA},
        "audit_hash": {"type": "string", "pattern": _SHA256_RE.pattern},
    },
}

_PROMPT_RULES: Final[tuple[str, ...]] = (
    "Return exactly one JSON object and no markdown or explanatory text.",
    "Use only keys allowed by the response schema; never emit a trade action or position advice.",
    "Cover every raw board exactly once across all aliases and invent no raw board aliases.",
    "Sort themes by canonical_theme_id; board-name arrays must preserve their relative "
    "order from request.raw_boards.",
    "Sort evidence by kind/board_names/stock_codes and sort every stock_codes array.",
    "Use label theme, umbrella, or noise; the canonical ID prefix must equal the label.",
    "Use only structured evidence supported by the request; do not write a free-text rationale.",
    "Every evidence board_names array must be a subset of that same theme's aliases.",
    "Every alias must appear in at least one evidence board_names array for its theme.",
    "Do not cite cross-theme cooccurrence as evidence for either theme.",
    "Confidence must be between 0 and 1 with at most four decimal places.",
    "Echo run_context provider/model plus protocol_hash, request_hash, taxonomy_hash, "
    "and prompt_hash exactly.",
)

PROTOCOL_HASH: Final = sha256_json(
    {
        "protocol_version": PROTOCOL_VERSION,
        "request_schema": NORMALIZATION_REQUEST_SCHEMA,
        "response_schema": NORMALIZATION_RESPONSE_SCHEMA,
        "audit_schema": DISAGREEMENT_AUDIT_SCHEMA,
        "prompt_rules": list(_PROMPT_RULES),
    }
)


def _fail(path: str, message: str) -> NoReturn:
    raise ThemeSemanticsValidationError(f"{path}: {message}")


def _object(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail(path, "must be an object")
    return value


def _array(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(path, "must be an array")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], path: str) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        _fail(path, f"keys mismatch; missing={missing}, extra={extra}")


def _canonical_text(value: Any, path: str, *, max_length: int = 120) -> str:
    if not isinstance(value, str):
        _fail(path, "must be a string")
    normalized = unicodedata.normalize("NFC", value.strip())
    if not normalized:
        _fail(path, "must not be empty")
    if normalized != value:
        _fail(path, "must already be NFC-normalized with no surrounding whitespace")
    if len(value) > max_length:
        _fail(path, f"must be at most {max_length} characters")
    if any(unicodedata.category(character) == "Cc" for character in value):
        _fail(path, "must not contain control characters")
    return value


def _canonical_string_array(
    value: Any,
    path: str,
    *,
    min_items: int = 0,
    max_length: int = 120,
) -> list[str]:
    items = _array(value, path)
    if len(items) < min_items:
        _fail(path, f"must contain at least {min_items} item(s)")
    parsed = [
        _canonical_text(item, f"{path}[{index}]", max_length=max_length)
        for index, item in enumerate(items)
    ]
    if len(parsed) != len(set(parsed)):
        _fail(path, "must not contain duplicates")
    if parsed != sorted(parsed):
        _fail(path, "must be sorted by Unicode code point")
    return parsed


def _hash(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        _fail(path, "must be a lowercase 64-character SHA-256 hex digest")
    return value


def _canonical_id(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _CANONICAL_ID_RE.fullmatch(value):
        _fail(path, f"must match {_CANONICAL_ID_RE.pattern}")
    return value


def _confidence(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(path, "must be a JSON number")
    number = float(value)
    if not math.isfinite(number) or not 0 <= number <= 1:
        _fail(path, "must be finite and between 0 and 1")
    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation as exc:
        raise ThemeSemanticsValidationError(f"{path}: invalid decimal confidence") from exc
    if decimal_value != decimal_value.quantize(Decimal("0.0001")):
        _fail(path, "must have at most four decimal places")
    return number


def validate_taxonomy(taxonomy: Any) -> None:
    """Validate a taxonomy document and require canonical ordering."""

    obj = _object(taxonomy, "taxonomy")
    _exact_keys(obj, {"taxonomy_version", "themes"}, "taxonomy")
    _canonical_text(obj["taxonomy_version"], "taxonomy.taxonomy_version")
    themes = _array(obj["themes"], "taxonomy.themes")
    ids: list[str] = []
    aliases_seen: dict[str, str] = {}
    for index, raw_theme in enumerate(themes):
        path = f"taxonomy.themes[{index}]"
        theme = _object(raw_theme, path)
        _exact_keys(
            theme,
            {"canonical_theme_id", "canonical_name", "label", "aliases"},
            path,
        )
        canonical_id = _canonical_id(theme["canonical_theme_id"], f"{path}.canonical_theme_id")
        label = theme["label"]
        if label not in THEME_LABELS:
            _fail(f"{path}.label", f"must be one of {list(THEME_LABELS)}")
        if canonical_id.split(":", 1)[0] != label:
            _fail(path, "canonical_theme_id prefix must match label")
        _canonical_text(theme["canonical_name"], f"{path}.canonical_name")
        aliases = _canonical_string_array(theme["aliases"], f"{path}.aliases", min_items=1)
        for alias in aliases:
            if alias in aliases_seen:
                _fail(
                    f"{path}.aliases",
                    f"alias {alias!r} already belongs to {aliases_seen[alias]!r}",
                )
            aliases_seen[alias] = canonical_id
        ids.append(canonical_id)
    if ids != sorted(ids):
        _fail("taxonomy.themes", "must be sorted by canonical_theme_id")
    if len(ids) != len(set(ids)):
        _fail("taxonomy.themes", "canonical_theme_id values must be unique")


def taxonomy_sha256(taxonomy: Any) -> str:
    """Validate and hash a taxonomy."""

    validate_taxonomy(taxonomy)
    return sha256_json(taxonomy)


def validate_approved_taxonomy_artifact(artifact: Any) -> None:
    """Validate the explicit human-approval envelope required by gate runtime.

    A candidate taxonomy or Agent response does not have this envelope and is
    therefore structurally ineligible for runtime loading.  The separate
    configured artifact hash protects the complete approval record from edits.
    """

    obj = _object(artifact, "approved_taxonomy_artifact")
    _exact_keys(
        obj,
        {"schema_version", "artifact_type", "approval", "taxonomy"},
        "approved_taxonomy_artifact",
    )
    if obj["schema_version"] != TAXONOMY_APPROVAL_SCHEMA_VERSION:
        _fail(
            "approved_taxonomy_artifact.schema_version",
            f"must be {TAXONOMY_APPROVAL_SCHEMA_VERSION!r}",
        )
    if obj["artifact_type"] != "v16_theme_taxonomy_approval":
        _fail(
            "approved_taxonomy_artifact.artifact_type",
            "must be 'v16_theme_taxonomy_approval'",
        )
    approval = _object(obj["approval"], "approved_taxonomy_artifact.approval")
    _exact_keys(
        approval,
        {
            "status",
            "reviewed_by",
            "reviewed_at",
            "review_ref",
            "source_candidate_manifest_sha256",
            "taxonomy_sha256",
        },
        "approved_taxonomy_artifact.approval",
    )
    if approval["status"] != "human_approved":
        _fail(
            "approved_taxonomy_artifact.approval.status",
            "must be 'human_approved'",
        )
    _canonical_text(
        approval["reviewed_by"],
        "approved_taxonomy_artifact.approval.reviewed_by",
    )
    _canonical_text(
        approval["review_ref"],
        "approved_taxonomy_artifact.approval.review_ref",
    )
    source_candidate_hash = approval["source_candidate_manifest_sha256"]
    if source_candidate_hash is not None:
        _hash(
            source_candidate_hash,
            "approved_taxonomy_artifact.approval.source_candidate_manifest_sha256",
        )
    reviewed_at = _canonical_text(
        approval["reviewed_at"],
        "approved_taxonomy_artifact.approval.reviewed_at",
    )
    try:
        parsed_reviewed_at = datetime.fromisoformat(reviewed_at)
    except ValueError as exc:
        raise ThemeSemanticsValidationError(
            "approved_taxonomy_artifact.approval.reviewed_at: must be ISO-8601"
        ) from exc
    if parsed_reviewed_at.tzinfo is None or parsed_reviewed_at.utcoffset() is None:
        _fail(
            "approved_taxonomy_artifact.approval.reviewed_at",
            "must include a timezone offset",
        )
    expected_taxonomy_hash = _hash(
        approval["taxonomy_sha256"],
        "approved_taxonomy_artifact.approval.taxonomy_sha256",
    )
    actual_taxonomy_hash = taxonomy_sha256(obj["taxonomy"])
    if actual_taxonomy_hash != expected_taxonomy_hash:
        _fail(
            "approved_taxonomy_artifact.approval.taxonomy_sha256",
            "does not match the enclosed taxonomy",
        )
    taxonomy_obj = _object(obj["taxonomy"], "approved_taxonomy_artifact.taxonomy")
    taxonomy_version = str(taxonomy_obj["taxonomy_version"])
    if taxonomy_version.startswith("candidate-"):
        _fail(
            "approved_taxonomy_artifact.taxonomy.taxonomy_version",
            "candidate-* versions cannot be approved runtime taxonomies",
        )


def approved_taxonomy_artifact_sha256(artifact: Any) -> str:
    """Validate and hash the complete human-approval artifact."""

    validate_approved_taxonomy_artifact(artifact)
    return sha256_json(artifact)


def build_approved_theme_index(
    artifact: Any,
    *,
    approved_artifact_hash: str,
) -> ThemeSemanticIndex:
    """Compile a human-approved artifact into the only gate-consumable index.

    Agent responses and consensus-candidate taxonomy files fail the envelope
    validation.  The caller must also provide the exact canonical hash of the
    complete approval artifact from separate runtime configuration.
    """

    try:
        validate_approved_taxonomy_artifact(artifact)
    except ThemeSemanticsValidationError as exc:
        raise UnapprovedThemeSemanticsError(
            "taxonomy lacks a valid human-approval artifact"
        ) from exc
    artifact_obj = _object(artifact, "approved_taxonomy_artifact")
    _hash(approved_artifact_hash, "approved_artifact_hash")
    actual_artifact_hash = sha256_json(artifact_obj)
    if actual_artifact_hash != approved_artifact_hash:
        raise UnapprovedThemeSemanticsError(
            "approval artifact hash is not the externally approved artifact hash"
        )
    taxonomy_obj = _object(artifact_obj["taxonomy"], "approved_taxonomy_artifact.taxonomy")
    actual_taxonomy_hash = taxonomy_sha256(taxonomy_obj)
    raw_to_canonical: dict[str, str] = {}
    excluded: set[str] = set()
    for theme in taxonomy_obj["themes"]:
        canonical_id = theme["canonical_theme_id"]
        for alias in theme["aliases"]:
            raw_to_canonical[alias] = canonical_id
            if theme["label"] != "theme":
                excluded.add(alias)
    return ThemeSemanticIndex(
        approval_status="approved",
        source_hash=actual_taxonomy_hash,
        raw_to_canonical_theme_id=MappingProxyType(dict(sorted(raw_to_canonical.items()))),
        excluded_aliases=frozenset(excluded),
    )


def build_normalization_request(
    raw_boards: Sequence[str],
    *,
    stock_boards: Mapping[str, Sequence[str]] | None = None,
    cooccurrences: Sequence[tuple[str, str, int]] | None = None,
    taxonomy: Mapping[str, Any] | None = None,
    allow_new_themes: bool = True,
) -> dict[str, Any]:
    """Build a canonical, hashed request from raw names and optional evidence.

    ``stock_boards`` maps a stable stock code to the raw boards observed for that
    stock.  ``cooccurrences`` contains ``(board_a, board_b, count)`` tuples.  All
    evidence must refer only to names present in ``raw_boards``.
    """

    if isinstance(raw_boards, (str, bytes)):
        _fail("raw_boards", "must be a sequence of strings, not one string")
    normalized_boards: list[str] = []
    for index, board in enumerate(raw_boards):
        if not isinstance(board, str):
            _fail(f"raw_boards[{index}]", "must be a string")
        normalized_boards.append(unicodedata.normalize("NFC", board.strip()))
    if any(not board for board in normalized_boards):
        _fail("raw_boards", "must not contain empty names")
    if len(normalized_boards) != len(set(normalized_boards)):
        _fail("raw_boards", "contains duplicates after whitespace/NFC normalization")
    boards = sorted(normalized_boards)
    if not boards:
        _fail("raw_boards", "must contain at least one board")
    board_set = set(boards)

    stock_rows: list[dict[str, Any]] = []
    for raw_code, raw_names in (stock_boards or {}).items():
        if not isinstance(raw_code, str):
            _fail("stock_boards", "stock codes must be strings")
        code = raw_code.strip()
        if not _STOCK_CODE_RE.fullmatch(code):
            _fail(f"stock_boards[{raw_code!r}]", "invalid stable stock code")
        if isinstance(raw_names, (str, bytes)):
            _fail(f"stock_boards[{code!r}]", "must be a sequence of board names")
        normalized_names: list[str] = []
        for name_index, name in enumerate(raw_names):
            if not isinstance(name, str):
                _fail(
                    f"stock_boards[{code!r}][{name_index}]",
                    "must be a string",
                )
            normalized_names.append(unicodedata.normalize("NFC", name.strip()))
        if len(normalized_names) != len(set(normalized_names)):
            _fail(f"stock_boards[{code!r}]", "contains duplicate board names")
        names = sorted(normalized_names)
        if not names or any(not name for name in names):
            _fail(f"stock_boards[{code!r}]", "must contain non-empty board names")
        unknown = sorted(set(names) - board_set)
        if unknown:
            _fail(f"stock_boards[{code!r}]", f"unknown raw boards: {unknown}")
        stock_rows.append({"stock_code": code, "board_names": names})
    stock_rows.sort(key=lambda row: row["stock_code"])
    if len({row["stock_code"] for row in stock_rows}) != len(stock_rows):
        _fail("stock_boards", "stock codes must be unique")

    cooccurrence_rows: list[dict[str, Any]] = []
    for index, value in enumerate(cooccurrences or ()):
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != 3:
            _fail(f"cooccurrences[{index}]", "must be a (board_a, board_b, count) tuple")
        raw_a, raw_b, count = value
        if not isinstance(raw_a, str) or not isinstance(raw_b, str):
            _fail(f"cooccurrences[{index}]", "board names must be strings")
        a = unicodedata.normalize("NFC", raw_a.strip())
        b = unicodedata.normalize("NFC", raw_b.strip())
        if a not in board_set or b not in board_set:
            _fail(f"cooccurrences[{index}]", "both boards must occur in raw_boards")
        if a == b:
            _fail(f"cooccurrences[{index}]", "boards must differ")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            _fail(f"cooccurrences[{index}].count", "must be a positive integer")
        a, b = sorted((a, b))
        cooccurrence_rows.append({"board_a": a, "board_b": b, "count": count})
    cooccurrence_rows.sort(key=lambda row: (row["board_a"], row["board_b"]))
    pairs = [(row["board_a"], row["board_b"]) for row in cooccurrence_rows]
    if len(pairs) != len(set(pairs)):
        _fail("cooccurrences", "board pairs must be unique")

    taxonomy_source: Mapping[str, Any] = taxonomy or {
        "taxonomy_version": "empty",
        "themes": [],
    }
    validate_taxonomy(taxonomy_source)
    taxonomy_value = parse_json_strict(canonical_json(taxonomy_source))
    if not isinstance(allow_new_themes, bool):
        _fail("allow_new_themes", "must be a boolean")

    payload: dict[str, Any] = {
        "schema_version": PROTOCOL_VERSION,
        "protocol_hash": PROTOCOL_HASH,
        "taxonomy_hash": taxonomy_sha256(taxonomy_value),
        "allow_new_themes": allow_new_themes,
        "raw_boards": boards,
        "stocks": stock_rows,
        "cooccurrences": cooccurrence_rows,
        "taxonomy": taxonomy_value,
    }
    request = {**payload, "request_hash": sha256_json(payload)}
    validate_normalization_request(request)
    return request


def validate_normalization_request(request: Any) -> None:
    """Validate request structure, ordering, references, and all embedded hashes."""

    obj = _object(request, "request")
    expected_keys = {
        "schema_version",
        "protocol_hash",
        "request_hash",
        "taxonomy_hash",
        "allow_new_themes",
        "raw_boards",
        "stocks",
        "cooccurrences",
        "taxonomy",
    }
    _exact_keys(obj, expected_keys, "request")
    if obj["schema_version"] != PROTOCOL_VERSION:
        _fail("request.schema_version", f"must equal {PROTOCOL_VERSION!r}")
    if obj["protocol_hash"] != PROTOCOL_HASH:
        _fail("request.protocol_hash", "does not match this implementation")
    _hash(obj["request_hash"], "request.request_hash")
    _hash(obj["taxonomy_hash"], "request.taxonomy_hash")
    if not isinstance(obj["allow_new_themes"], bool):
        _fail("request.allow_new_themes", "must be a boolean")

    boards = _canonical_string_array(obj["raw_boards"], "request.raw_boards", min_items=1)
    board_set = set(boards)

    stocks = _array(obj["stocks"], "request.stocks")
    stock_codes: list[str] = []
    for index, raw_stock in enumerate(stocks):
        path = f"request.stocks[{index}]"
        stock = _object(raw_stock, path)
        _exact_keys(stock, {"stock_code", "board_names"}, path)
        code = _canonical_text(stock["stock_code"], f"{path}.stock_code", max_length=32)
        if not _STOCK_CODE_RE.fullmatch(code):
            _fail(f"{path}.stock_code", f"must match {_STOCK_CODE_RE.pattern}")
        names = _canonical_string_array(stock["board_names"], f"{path}.board_names", min_items=1)
        unknown = sorted(set(names) - board_set)
        if unknown:
            _fail(f"{path}.board_names", f"unknown raw boards: {unknown}")
        stock_codes.append(code)
    if stock_codes != sorted(stock_codes) or len(stock_codes) != len(set(stock_codes)):
        _fail("request.stocks", "must have unique rows sorted by stock_code")

    cooccurrences_value = _array(obj["cooccurrences"], "request.cooccurrences")
    occurrence_keys: list[tuple[str, str]] = []
    for index, raw_pair in enumerate(cooccurrences_value):
        path = f"request.cooccurrences[{index}]"
        pair = _object(raw_pair, path)
        _exact_keys(pair, {"board_a", "board_b", "count"}, path)
        a = _canonical_text(pair["board_a"], f"{path}.board_a")
        b = _canonical_text(pair["board_b"], f"{path}.board_b")
        if a not in board_set or b not in board_set:
            _fail(path, "both boards must occur in request.raw_boards")
        if not a < b:
            _fail(path, "board_a must sort before board_b")
        count = pair["count"]
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            _fail(f"{path}.count", "must be a positive integer")
        occurrence_keys.append((a, b))
    if occurrence_keys != sorted(occurrence_keys) or len(occurrence_keys) != len(
        set(occurrence_keys)
    ):
        _fail("request.cooccurrences", "must have unique rows sorted by board pair")

    validate_taxonomy(obj["taxonomy"])
    expected_taxonomy_hash = sha256_json(obj["taxonomy"])
    if obj["taxonomy_hash"] != expected_taxonomy_hash:
        _fail("request.taxonomy_hash", "does not match the embedded taxonomy")

    payload = {key: obj[key] for key in obj if key != "request_hash"}
    expected_request_hash = sha256_json(payload)
    if obj["request_hash"] != expected_request_hash:
        _fail("request.request_hash", "does not match canonical request content")


def _provider_model(provider: Any, model: Any, path: str) -> tuple[str, str]:
    if provider not in PROVIDERS:
        _fail(f"{path}.provider", f"must be one of {list(PROVIDERS)}")
    model_value = _canonical_text(model, f"{path}.model", max_length=128)
    if not _MODEL_RE.fullmatch(model_value):
        _fail(f"{path}.model", f"must match {_MODEL_RE.pattern}")
    return provider, model_value


def _prompt_payload(request: Mapping[str, Any], *, provider: str, model: str) -> dict[str, Any]:
    return {
        "rules": list(_PROMPT_RULES),
        "run_context": {"provider": provider, "model": model},
        "request": request,
        "response_schema": NORMALIZATION_RESPONSE_SCHEMA,
    }


def prompt_sha256(request: Any, *, provider: str, model: str) -> str:
    """Return the exact hash an identified provider run must echo."""

    validate_normalization_request(request)
    provider_value, model_value = _provider_model(provider, model, "run_context")
    return sha256_json(_prompt_payload(request, provider=provider_value, model=model_value))


def build_normalization_prompt(request: Any, *, provider: str, model: str) -> str:
    """Render the same protocol with an explicit, hash-bound provider identity."""

    validate_normalization_request(request)
    provider_value, model_value = _provider_model(provider, model, "run_context")
    payload = _prompt_payload(request, provider=provider_value, model=model_value)
    prompt_hash = sha256_json(payload)
    return (
        "V16 THEME SEMANTICS OFFLINE PROPOSAL\n"
        f"PROMPT_PAYLOAD_SHA256={prompt_hash}\n"
        "The following canonical JSON object is the complete prompt payload. "
        "Follow its rules and response schema exactly.\n"
        f"{canonical_json(payload)}\n"
    )


def _taxonomy_by_id(request: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {theme["canonical_theme_id"]: theme for theme in request["taxonomy"]["themes"]}


def _request_stock_boards(request: Mapping[str, Any]) -> dict[str, set[str]]:
    return {stock["stock_code"]: set(stock["board_names"]) for stock in request["stocks"]}


def _request_cooccurrences(request: Mapping[str, Any]) -> set[tuple[str, str]]:
    return {(row["board_a"], row["board_b"]) for row in request["cooccurrences"]}


def _validate_evidence(
    raw_evidence: Any,
    *,
    path: str,
    aliases: list[str],
    canonical_theme_id: str,
    request: Mapping[str, Any],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    evidence = _object(raw_evidence, path)
    _exact_keys(evidence, {"kind", "board_names", "stock_codes"}, path)
    kind = evidence["kind"]
    if kind not in EVIDENCE_KINDS:
        _fail(f"{path}.kind", f"must be one of {list(EVIDENCE_KINDS)}")
    board_names = _canonical_string_array(
        evidence["board_names"], f"{path}.board_names", min_items=1
    )
    stock_codes = _canonical_string_array(
        evidence["stock_codes"],
        f"{path}.stock_codes",
        max_length=32,
    )
    if not set(board_names).issubset(aliases):
        _fail(f"{path}.board_names", "must be a subset of this theme's aliases")
    stock_board_map = _request_stock_boards(request)
    unknown_codes = sorted(set(stock_codes) - set(stock_board_map))
    if unknown_codes:
        _fail(f"{path}.stock_codes", f"unknown request stock codes: {unknown_codes}")

    if kind in {"name_similarity", "manual_review"}:
        if stock_codes:
            _fail(f"{path}.stock_codes", f"must be empty for {kind}")
    elif kind == "stock_overlap":
        if len(board_names) < 2 or not stock_codes:
            _fail(path, "stock_overlap requires at least two boards and one stock")
        for code in stock_codes:
            if not set(board_names).issubset(stock_board_map[code]):
                _fail(path, f"stock {code!r} does not support all referenced boards")
    elif kind == "cooccurrence":
        if len(board_names) != 2 or stock_codes:
            _fail(path, "cooccurrence requires exactly two boards and no stock codes")
        pair = tuple(board_names)
        if pair not in _request_cooccurrences(request):
            _fail(path, "referenced cooccurrence pair does not exist in the request")
    elif kind == "taxonomy_alias":
        if stock_codes:
            _fail(f"{path}.stock_codes", "must be empty for taxonomy_alias")
        taxonomy_theme = _taxonomy_by_id(request).get(canonical_theme_id)
        if taxonomy_theme is None:
            _fail(path, "taxonomy_alias requires an existing taxonomy theme")
        unsupported = sorted(set(board_names) - set(taxonomy_theme["aliases"]))
        if unsupported:
            _fail(path, f"aliases are not approved by the taxonomy: {unsupported}")
    return kind, tuple(board_names), tuple(stock_codes)


def validate_normalization_response(response: Any, request: Any) -> None:
    """Validate a provider response against the exact request and prompt.

    Unknown keys are rejected at every level.  In particular, fields such as
    ``trade_action``, ``allow_trade``, ``position``, or a free-text rationale are
    outside the schema and cannot pass this validator.
    """

    validate_normalization_request(request)
    request_obj = _object(request, "request")
    obj = _object(response, "response")
    expected_keys = {
        "schema_version",
        "protocol_hash",
        "request_hash",
        "taxonomy_hash",
        "provider",
        "model",
        "prompt_hash",
        "themes",
    }
    _exact_keys(obj, expected_keys, "response")
    if obj["schema_version"] != PROTOCOL_VERSION:
        _fail("response.schema_version", f"must equal {PROTOCOL_VERSION!r}")
    if obj["protocol_hash"] != PROTOCOL_HASH:
        _fail("response.protocol_hash", "does not match this implementation")
    if obj["request_hash"] != request_obj["request_hash"]:
        _fail("response.request_hash", "does not match the request")
    if obj["taxonomy_hash"] != request_obj["taxonomy_hash"]:
        _fail("response.taxonomy_hash", "does not match the request")
    provider = obj["provider"]
    if provider not in PROVIDERS:
        _fail("response.provider", f"must be one of {list(PROVIDERS)}")
    _, model = _provider_model(provider, obj["model"], "response")
    _hash(obj["prompt_hash"], "response.prompt_hash")
    if obj["prompt_hash"] != prompt_sha256(request_obj, provider=provider, model=model):
        _fail("response.prompt_hash", "does not match this provider/model prompt payload")

    raw_boards = set(request_obj["raw_boards"])
    taxonomy_by_id = _taxonomy_by_id(request_obj)
    themes = _array(obj["themes"], "response.themes")
    if not themes:
        _fail("response.themes", "must contain at least one theme")
    canonical_ids: list[str] = []
    aliases_seen: dict[str, str] = {}

    for index, raw_theme in enumerate(themes):
        path = f"response.themes[{index}]"
        theme = _object(raw_theme, path)
        _exact_keys(
            theme,
            {
                "canonical_theme_id",
                "canonical_name",
                "label",
                "aliases",
                "confidence",
                "evidence",
            },
            path,
        )
        canonical_id = _canonical_id(theme["canonical_theme_id"], f"{path}.canonical_theme_id")
        canonical_name = _canonical_text(theme["canonical_name"], f"{path}.canonical_name")
        label = theme["label"]
        if label not in THEME_LABELS:
            _fail(f"{path}.label", f"must be one of {list(THEME_LABELS)}")
        if canonical_id.split(":", 1)[0] != label:
            _fail(path, "canonical_theme_id prefix must match label")
        aliases = _canonical_string_array(theme["aliases"], f"{path}.aliases", min_items=1)
        unknown_aliases = sorted(set(aliases) - raw_boards)
        if unknown_aliases:
            _fail(f"{path}.aliases", f"not present in request.raw_boards: {unknown_aliases}")
        for alias in aliases:
            if alias in aliases_seen:
                _fail(
                    f"{path}.aliases",
                    f"raw board {alias!r} already mapped to {aliases_seen[alias]!r}",
                )
            aliases_seen[alias] = canonical_id
        _confidence(theme["confidence"], f"{path}.confidence")

        taxonomy_theme = taxonomy_by_id.get(canonical_id)
        if taxonomy_theme is None and not request_obj["allow_new_themes"]:
            _fail(path, f"new canonical theme {canonical_id!r} is not allowed")
        if taxonomy_theme is not None:
            if canonical_name != taxonomy_theme["canonical_name"]:
                _fail(path, "canonical_name differs from the taxonomy")
            if label != taxonomy_theme["label"]:
                _fail(path, "label differs from the taxonomy")

        evidence_values = _array(theme["evidence"], f"{path}.evidence")
        if not evidence_values:
            _fail(f"{path}.evidence", "must contain at least one structured item")
        evidence_keys: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = []
        evidence_boards: set[str] = set()
        for evidence_index, evidence in enumerate(evidence_values):
            evidence_key = _validate_evidence(
                evidence,
                path=f"{path}.evidence[{evidence_index}]",
                aliases=aliases,
                canonical_theme_id=canonical_id,
                request=request_obj,
            )
            evidence_keys.append(evidence_key)
            evidence_boards.update(evidence_key[1])
        if evidence_keys != sorted(evidence_keys):
            _fail(f"{path}.evidence", "must be sorted by kind, board_names, stock_codes")
        if len(evidence_keys) != len(set(evidence_keys)):
            _fail(f"{path}.evidence", "must not contain duplicate items")
        if evidence_boards != set(aliases):
            missing = sorted(set(aliases) - evidence_boards)
            _fail(f"{path}.evidence", f"every alias needs structured evidence; missing={missing}")
        canonical_ids.append(canonical_id)

    if canonical_ids != sorted(canonical_ids):
        _fail("response.themes", "must be sorted by canonical_theme_id")
    if len(canonical_ids) != len(set(canonical_ids)):
        _fail("response.themes", "canonical_theme_id values must be unique")
    missing_boards = sorted(raw_boards - set(aliases_seen))
    if missing_boards:
        _fail("response.themes", f"raw boards are not covered: {missing_boards}")


def canonicalize_normalization_response(response: Any, request: Any) -> dict[str, Any]:
    """Mechanically sort a proposal, then apply the full strict validator.

    This function never changes a scalar value, adds/removes evidence, merges/splits a
    theme, or promotes a candidate.  It exists because ordering is mechanical and
    should not depend on an Agent's ability to reproduce Unicode collation.  Keep the
    raw provider document alongside the returned canonical candidate for provenance.
    """

    validate_normalization_request(request)
    if not isinstance(response, Mapping):
        _fail("response", "must be an object")
    candidate = deepcopy(dict(response))
    raw_themes = candidate.get("themes")
    if isinstance(raw_themes, list):
        for raw_theme in raw_themes:
            if not isinstance(raw_theme, dict):
                continue
            aliases = raw_theme.get("aliases")
            if isinstance(aliases, list) and all(isinstance(alias, str) for alias in aliases):
                aliases.sort()
            evidence_values = raw_theme.get("evidence")
            if isinstance(evidence_values, list):
                for evidence in evidence_values:
                    if not isinstance(evidence, dict):
                        continue
                    board_names = evidence.get("board_names")
                    if isinstance(board_names, list) and all(
                        isinstance(board, str) for board in board_names
                    ):
                        board_names.sort()
                    stock_codes = evidence.get("stock_codes")
                    if isinstance(stock_codes, list) and all(
                        isinstance(code, str) for code in stock_codes
                    ):
                        stock_codes.sort()
                if all(
                    isinstance(evidence, Mapping)
                    and isinstance(evidence.get("kind"), str)
                    and isinstance(evidence.get("board_names"), list)
                    and all(isinstance(board, str) for board in evidence["board_names"])
                    and isinstance(evidence.get("stock_codes"), list)
                    and all(isinstance(code, str) for code in evidence["stock_codes"])
                    for evidence in evidence_values
                ):
                    evidence_values.sort(
                        key=lambda evidence: (
                            evidence["kind"],
                            tuple(evidence["board_names"]),
                            tuple(evidence["stock_codes"]),
                        )
                    )
        if all(
            isinstance(theme, Mapping) and isinstance(theme.get("canonical_theme_id"), str)
            for theme in raw_themes
        ):
            raw_themes.sort(key=lambda theme: theme["canonical_theme_id"])
    validate_normalization_response(candidate, request)
    return candidate


def normalization_response_sha256(response: Any, request: Any) -> str:
    """Validate and hash a normalization response."""

    validate_normalization_response(response, request)
    return sha256_json(response)


def build_candidate_theme_index(response: Any, request: Any) -> ThemeSemanticIndex:
    """Compile a validated Agent proposal for audit, never for automatic gating."""

    validate_normalization_response(response, request)
    response_obj = _object(response, "response")
    raw_to_canonical: dict[str, str] = {}
    excluded: set[str] = set()
    for theme in response_obj["themes"]:
        canonical_id = theme["canonical_theme_id"]
        for alias in theme["aliases"]:
            raw_to_canonical[alias] = canonical_id
            if theme["label"] != "theme":
                excluded.add(alias)
    return ThemeSemanticIndex(
        approval_status="candidate",
        source_hash=sha256_json(response_obj),
        raw_to_canonical_theme_id=MappingProxyType(dict(sorted(raw_to_canonical.items()))),
        excluded_aliases=frozenset(excluded),
    )


def _response_board_map(response: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for theme in response["themes"]:
        for alias in theme["aliases"]:
            result[alias] = theme
    return result


def _agent_identity(response: Mapping[str, Any], response_hash: str) -> dict[str, str]:
    return {
        "provider": response["provider"],
        "model": response["model"],
        "prompt_hash": response["prompt_hash"],
        "response_hash": response_hash,
    }


def audit_normalization_responses(left: Any, right: Any, request: Any) -> dict[str, Any]:
    """Create a deterministic, hash-addressed audit of two provider responses."""

    validate_normalization_response(left, request)
    validate_normalization_response(right, request)
    left_obj = _object(left, "left")
    right_obj = _object(right, "right")
    request_obj = _object(request, "request")
    left_hash = sha256_json(left_obj)
    right_hash = sha256_json(right_obj)
    left_map = _response_board_map(left_obj)
    right_map = _response_board_map(right_obj)
    fields = (
        "canonical_theme_id",
        "canonical_name",
        "label",
        "aliases",
        "confidence",
        "evidence",
    )
    semantic_fields = {"canonical_theme_id", "canonical_name", "label", "aliases"}
    disagreements: list[dict[str, Any]] = []
    for board in request_obj["raw_boards"]:
        left_theme = left_map[board]
        right_theme = right_map[board]
        changed = [field for field in fields if left_theme[field] != right_theme[field]]
        if changed:
            disagreements.append(
                {
                    "raw_board": board,
                    "fields": changed,
                    "semantic_difference": bool(set(changed) & semantic_fields),
                    "left": {field: left_theme[field] for field in fields},
                    "right": {field: right_theme[field] for field in fields},
                }
            )

    left_agent = _agent_identity(left_obj, left_hash)
    right_agent = _agent_identity(right_obj, right_hash)
    payload: dict[str, Any] = {
        "schema_version": PROTOCOL_VERSION,
        "protocol_hash": PROTOCOL_HASH,
        "request_hash": request_obj["request_hash"],
        "left_agent": left_agent,
        "right_agent": right_agent,
        "independent_agents": (left_agent["provider"], left_agent["model"])
        != (right_agent["provider"], right_agent["model"]),
        "semantic_agreement": not any(row["semantic_difference"] for row in disagreements),
        "exact_agreement": not disagreements,
        "disagreements": disagreements,
    }
    return {**payload, "audit_hash": sha256_json(payload)}


def validate_disagreement_audit(audit: Any, left: Any, right: Any, request: Any) -> None:
    """Recompute an audit and require a byte-equivalent canonical JSON document."""

    audit_obj = _object(audit, "audit")
    expected = audit_normalization_responses(left, right, request)
    if canonical_json(audit_obj) != canonical_json(expected):
        _fail("audit", "does not match the deterministic audit of these responses")


def build_consensus_candidate_taxonomy(
    left: Any, right: Any, request: Any
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a non-approved taxonomy containing only exact semantic agreement.

    Confidence/evidence differences do not change the canonical mapping and are
    recorded separately.  Any difference in canonical ID, canonical name, label,
    or complete alias group excludes that raw board from the candidate taxonomy and
    places it in ``manual_review``.  The returned manifest explicitly forbids runtime
    loading; human review must create a separate approved taxonomy artifact/hash.
    """

    validate_normalization_response(left, request)
    validate_normalization_response(right, request)
    left_obj = _object(left, "left")
    right_obj = _object(right, "right")
    request_obj = _object(request, "request")
    audit = audit_normalization_responses(left_obj, right_obj, request_obj)
    left_map = _response_board_map(left_obj)
    right_map = _response_board_map(right_obj)
    semantic_fields = ("canonical_theme_id", "canonical_name", "label", "aliases")
    metadata_fields = ("confidence", "evidence")
    consensus_ids: set[str] = set()
    consensus_aliases: list[str] = []
    manual_review: list[dict[str, Any]] = []
    metadata_disagreements: list[dict[str, Any]] = []

    for board in request_obj["raw_boards"]:
        left_theme = left_map[board]
        right_theme = right_map[board]
        semantic_changed = [
            field for field in semantic_fields if left_theme[field] != right_theme[field]
        ]
        if semantic_changed:
            manual_review.append(
                {
                    "raw_board": board,
                    "fields": semantic_changed,
                    "left": {field: left_theme[field] for field in semantic_fields},
                    "right": {field: right_theme[field] for field in semantic_fields},
                }
            )
            continue
        consensus_ids.add(left_theme["canonical_theme_id"])
        consensus_aliases.append(board)
        metadata_changed = [
            field for field in metadata_fields if left_theme[field] != right_theme[field]
        ]
        if metadata_changed:
            metadata_disagreements.append({"raw_board": board, "fields": metadata_changed})

    left_by_id = {theme["canonical_theme_id"]: theme for theme in left_obj["themes"]}
    consensus_themes = [
        {
            "canonical_theme_id": left_by_id[canonical_id]["canonical_theme_id"],
            "canonical_name": left_by_id[canonical_id]["canonical_name"],
            "label": left_by_id[canonical_id]["label"],
            "aliases": left_by_id[canonical_id]["aliases"],
        }
        for canonical_id in sorted(consensus_ids)
    ]
    taxonomy = {
        "taxonomy_version": f"candidate-{audit['audit_hash'][:16]}",
        "themes": consensus_themes,
    }
    validate_taxonomy(taxonomy)
    manifest_payload: dict[str, Any] = {
        "artifact_type": "v16_theme_taxonomy_candidate",
        "artifact_status": "candidate/not_approved",
        "runtime_load_allowed": False,
        "requires_human_approval": True,
        "protocol_hash": PROTOCOL_HASH,
        "request_hash": request_obj["request_hash"],
        "audit_hash": audit["audit_hash"],
        "left_response_hash": sha256_json(left_obj),
        "right_response_hash": sha256_json(right_obj),
        "taxonomy_hash": sha256_json(taxonomy),
        "consensus_rule": "exact canonical_theme_id/canonical_name/label/aliases",
        "consensus_aliases": sorted(consensus_aliases),
        "manual_review": manual_review,
        "metadata_disagreements": metadata_disagreements,
    }
    manifest = {**manifest_payload, "manifest_hash": sha256_json(manifest_payload)}
    return taxonomy, manifest


__all__ = [
    "DISAGREEMENT_AUDIT_SCHEMA",
    "EVIDENCE_KINDS",
    "NORMALIZATION_REQUEST_SCHEMA",
    "NORMALIZATION_RESPONSE_SCHEMA",
    "PROTOCOL_HASH",
    "PROTOCOL_VERSION",
    "PROVIDERS",
    "TAXONOMY_APPROVAL_SCHEMA_VERSION",
    "THEME_LABELS",
    "ThemeSemanticIndex",
    "ThemeSemanticsValidationError",
    "UnapprovedThemeSemanticsError",
    "audit_normalization_responses",
    "approved_taxonomy_artifact_sha256",
    "build_approved_theme_index",
    "build_candidate_theme_index",
    "build_consensus_candidate_taxonomy",
    "build_normalization_prompt",
    "build_normalization_request",
    "canonicalize_normalization_response",
    "canonical_json",
    "normalization_response_sha256",
    "parse_json_strict",
    "prompt_sha256",
    "sha256_json",
    "sha256_text",
    "taxonomy_sha256",
    "validate_approved_taxonomy_artifact",
    "validate_normalization_request",
    "validate_normalization_response",
    "validate_disagreement_audit",
    "validate_taxonomy",
]
