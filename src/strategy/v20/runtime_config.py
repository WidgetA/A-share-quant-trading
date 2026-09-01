"""Strict runtime configuration and activation guard for V20."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass
from datetime import time
from pathlib import Path
from typing import Any, Mapping, Protocol, cast
from urllib.parse import urlsplit

import yaml

from src.data.database.tls import sha256_file
from src.data.sources.local_concept_mapper import resolve_concept_data_path


class V20ConfigError(RuntimeError):
    pass


class _DatabaseConsumerConfig(Protocol):
    @property
    def host(self) -> str: ...

    @property
    def port(self) -> int: ...

    @property
    def database(self) -> str: ...

    @property
    def user(self) -> str: ...

    @property
    def password(self) -> str: ...

    @property
    def ssl_mode(self) -> str: ...

    @property
    def ssl_root_cert(self) -> str: ...

    @property
    def ssl_root_cert_sha256(self) -> str: ...

    @property
    def connect_timeout_seconds(self) -> float: ...

    @property
    def command_timeout_seconds(self) -> float: ...


_TOP_LEVEL_KEYS = {
    "schema_version",
    "strategy_version",
    "official_stream_id",
    "enabled",
    "deployment_mode",
    "production_activation_guard",
    "timezone",
    "return_profile_id",
    "reference_profile_id",
    "state_lineage_id",
    "clock",
    "market_data",
    "policy",
    "artifacts",
    "database",
    "routes",
    "bootstrap",
}
_CLOCK_KEYS = {
    "prewarm",
    "minute_collection_start",
    "decision_bar_label",
    "publish_deadline",
    "decision_finalization_deadline",
    "reference_bar_label",
    "reference_lock_deadline_next_day",
    "mews_cutoff_d1",
    "plan_exit",
    "reminder_check",
}
_MARKET_KEYS = {
    "minimum_quote_coverage",
    "minimum_breadth_universe",
    "minute_poll_seconds",
    "exit_poll_seconds",
}
_POLICY_VALUES = {
    "health_window": 3,
    "health_recovery_confirmations": 3,
    "breadth_wilson_z": 1.645,
    "breadth_full_upper": 0.50,
    "breadth_half_upper": 0.60,
    "rolling_window": 7,
    "rolling_bad_min_losses": 5,
    "g_max_component_size": 3,
    "g_weak_amount_min_metrics": 2,
    "d1_stop_factor": 0.92,
    "d2_stop_factor": 0.88,
    "d2_mews_danger_stop_factor": 0.95,
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SCHEMA_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UNCONFIGURED = "UNCONFIGURED"
_ROUTE_BINDING_KEYS = {
    "route_id",
    "expected_bot_origin",
    "expected_app_id_sha256",
    "expected_chat_id_sha256",
}

# These files jointly define the stock-selection and state-evolution semantics.
# Hashing the deployed bytes is deliberately stricter than recording a git
# branch name: two hosts cannot claim the same V20 config while running a
# different V16 scanner, model, feature list, board universe, or V20 rule.
_STRATEGY_DEPENDENCY_FILES = (
    "src/strategy/strategies/v16_scanner.py",
    "src/strategy/strategies/momentum_sector_scanner.py",
    "src/strategy/lgbrank_scorer.py",
    "src/strategy/filters/stock_filter.py",
    "src/strategy/filters/board_filter.py",
    "src/strategy/filters/reversal_factor_filter.py",
    "src/strategy/filters/momentum_quality_filter.py",
    "src/data/clients/ifind_http_client.py",
    "src/common/config.py",
    "src/data/sources/local_concept_mapper.py",
    "src/data/clients/tushare_realtime.py",
    "src/data/clients/iquant_historical_adapter.py",
    "src/data/clients/v20_market_data.py",
    "src/data/database/fundamentals_db.py",
    "src/data/database/tls.py",
    "src/data/database/v20_repository.py",
    "src/common/feishu_bot.py",
    "src/common/v20_feishu.py",
    "src/web/v15_scan_service.py",
    "src/web/v20_scan_pipeline.py",
    "src/web/v20_service.py",
    "src/web/v20_routes.py",
    "src/strategy/v20/artifacts.py",
    "src/strategy/v20/decision_engine.py",
    "src/strategy/v20/exit_policy.py",
    "src/strategy/v20/identity.py",
    "src/strategy/v20/models.py",
    "src/strategy/v20/policy.py",
    "src/strategy/v20/rolling7_history.py",
    "src/strategy/v20/runtime_config.py",
    "src/strategy/v20/shadow_evaluator.py",
    "models/lgbrank_latest.txt",
    "models/feature_list.json",
    "data/sectors.json",
    "data/board_constituents.json",
    "docs/strategy-v20-artifacts/rolling7-v16-market-history-v1.json",
    "pyproject.toml",
    "uv.lock",
)

# Only these deployed bytes can change a V20 selection, BASE/R7/G input, or
# official state transition.  Operational wrappers remain covered by the full
# config hash above, but no longer fork a state lineage merely because a
# formatter, route, replay, database adapter, or service lifecycle changed.
#
# ``V20_STATE_INPUT_ORCHESTRATION_V2`` explicitly versions the state-sensitive
# orchestration that still lives in ``v20_service.py`` (receipt selection,
# cutoff handling, policy input assembly, gap maturity, and invalid-state
# transition ordering).  Any semantic edit there must bump this profile.
_STATE_SEMANTICS_DEPENDENCY_FILES = (
    "src/strategy/strategies/v16_scanner.py",
    "src/strategy/strategies/momentum_sector_scanner.py",
    "src/strategy/lgbrank_scorer.py",
    "src/strategy/filters/stock_filter.py",
    "src/strategy/filters/board_filter.py",
    "src/strategy/filters/reversal_factor_filter.py",
    "src/strategy/filters/momentum_quality_filter.py",
    "src/data/clients/ifind_http_client.py",
    "src/common/config.py",
    "src/data/sources/local_concept_mapper.py",
    "src/data/clients/tushare_realtime.py",
    "src/data/clients/iquant_historical_adapter.py",
    "src/data/clients/v20_market_data.py",
    "src/data/database/fundamentals_db.py",
    "src/web/v15_scan_service.py",
    "src/web/v20_scan_pipeline.py",
    "src/strategy/v20/artifacts.py",
    "src/strategy/v20/decision_engine.py",
    "src/strategy/v20/exit_policy.py",
    "src/strategy/v20/identity.py",
    "src/strategy/v20/models.py",
    "src/strategy/v20/policy.py",
    "src/strategy/v20/rolling7_history.py",
    "src/strategy/v20/shadow_evaluator.py",
    "models/lgbrank_latest.txt",
    "models/feature_list.json",
    "data/sectors.json",
    "data/board_constituents.json",
    "docs/strategy-v20-artifacts/rolling7-v16-market-history-v1.json",
    "pyproject.toml",
    "uv.lock",
)
_STATE_SEMANTICS_SCHEMA = "v20-state-semantics/v2"
_STATE_INPUT_ORCHESTRATION_PROFILE = "V20_STATE_INPUT_ORCHESTRATION_V2"
_LEGACY_STATE_SEMANTICS_SCHEMA = "v20-state-semantics/v1"
_AUDITED_LEGACY_STATE_SEMANTICS_HASHES = frozenset(
    {
        # main@4211cd0: the only legacy lineage admitted to the v2 core model.
        # Its only deployment-byte changes before this migration were the
        # reviewed V20 late-replay service and Feishu formatter changes.
        "b2ba54f990cfe6b0e4b8f38c97e096a72205d78e34e484593eacaf5243ac2ce0",
    }
)
_MIXED_STATE_SOURCE_CLASSES = {
    "src/web/v20_service.py": {
        "07ac09b4e61f376ec896c893ea5e88ee89562ae573402bccd9d63a0694d52e6e": (
            "V20_SERVICE_STATE_ORCHESTRATION_V1"
        ),
        "533534faf87d7f1b45bff3af9624d12365f91275f9609f549ea9b3c91d7d2bbb": (
            "V20_SERVICE_STATE_ORCHESTRATION_V1"
        ),
        "12fd28d7abdcdafca6932cf2b08d2c870a971d2f4c3e38a06e97bdd29921d24e": (
            "V20_SERVICE_STATE_ORCHESTRATION_V1"
        ),
        "95900ffebbdac8f08615c35049cf4d76499059e15ea5a114549045a82dbbcece": (
            "V20_SERVICE_STATE_ORCHESTRATION_V1"
        ),
        "1c71c2a8ff33f52d0d4f2366a3c7d1316ce24e6a38c9ab75ebbfb7dd982bf0bd": (
            "V20_SERVICE_STATE_ORCHESTRATION_V1"
        ),
        "41e8f48f95b39e93869b076a8e6acb29673371a2e6b7b2e8a0460cd093719a4a": (
            "V20_SERVICE_STATE_ORCHESTRATION_V2"
        ),
        "b7c1f8244f34ba6a5a6e518358cc7f40b52d33e487bc7b7ad2577c54b130203c": (
            "V20_SERVICE_STATE_ORCHESTRATION_V2"
        ),
        "2b7e90e82da0ab65d4db3885648bac3b9ae18edd8c5455c2bf8ec5deb7fe1371": (
            "V20_SERVICE_STATE_ORCHESTRATION_V2"
        ),
        # Reviewed operational-only change: bound the 11:30 feed-health
        # frontier to vendor publication timing without changing which legal
        # minute bar can create an exit intent or any official state input.
        "d45331761ac952e6e279eb9e851b13c183491bf38db5dab5d40934eddd13e30d": (
            "V20_SERVICE_STATE_ORCHESTRATION_V2"
        ),
    },
    "src/data/database/v20_repository.py": {
        "4e1afb37e369340891f2d5c9e807de2c7636391168877f91932a2152471c2902": (
            "V20_LEDGER_STATE_CONTRACT_V1"
        ),
        "7167e475f3a6dd857673540a8ac0bf812c4a72b9bff4a871e61ea677c22f1209": (
            "V20_LEDGER_STATE_CONTRACT_V1"
        ),
        "6f74da33d9b8d632b1357e432117600d73356975d59ea96242a7f2c38d815f9f": (
            "V20_LEDGER_STATE_CONTRACT_V1"
        ),
    },
}


def _canonical_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _state_semantics_source(payload: Mapping[str, Any]) -> dict[str, Any]:
    dependencies = payload.get("strategy_dependency_hashes")
    if not isinstance(dependencies, Mapping):
        raise V20ConfigError("frozen config lacks strategy dependency hashes")
    state_dependencies: dict[str, str] = {}
    for relative in _STATE_SEMANTICS_DEPENDENCY_FILES:
        digest = dependencies.get(relative)
        if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
            raise V20ConfigError(f"frozen config lacks valid state dependency: {relative}")
        state_dependencies[relative] = digest
    mixed_source_classes: dict[str, str] = {}
    for relative, reviewed in _MIXED_STATE_SOURCE_CLASSES.items():
        digest = dependencies.get(relative)
        if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
            raise V20ConfigError(f"frozen config lacks valid mixed dependency: {relative}")
        semantic_class = reviewed.get(digest)
        if semantic_class is None:
            raise V20ConfigError(
                f"unreviewed state-sensitive mixed source bytes: {relative}={digest}"
            )
        mixed_source_classes[relative] = semantic_class
    required_mappings = ("clock", "market_data", "policy")
    for field in required_mappings:
        if not isinstance(payload.get(field), Mapping):
            raise V20ConfigError(f"frozen config lacks valid {field}")
    for field in (
        "strategy_version",
        "timezone",
        "return_profile_id",
        "reference_profile_id",
    ):
        if not isinstance(payload.get(field), str) or not str(payload[field]):
            raise V20ConfigError(f"frozen config lacks valid {field}")
    manifest_hash = payload.get("g_manifest_sha256")
    if not isinstance(manifest_hash, str) or not _SHA256.fullmatch(manifest_hash):
        raise V20ConfigError("frozen config lacks valid g_manifest_sha256")
    return {
        "schema_version": _STATE_SEMANTICS_SCHEMA,
        "strategy_version": payload["strategy_version"],
        "timezone": payload["timezone"],
        "return_profile_id": payload["return_profile_id"],
        "reference_profile_id": payload["reference_profile_id"],
        "clock": dict(cast(Mapping[str, Any], payload["clock"])),
        "market_data": dict(cast(Mapping[str, Any], payload["market_data"])),
        "policy": dict(cast(Mapping[str, Any], payload["policy"])),
        "g_manifest_sha256": manifest_hash,
        "state_input_orchestration_profile": _STATE_INPUT_ORCHESTRATION_PROFILE,
        "mixed_state_source_classes": mixed_source_classes,
        "state_dependency_hashes": state_dependencies,
    }


def state_semantics_hash_from_frozen_payload(payload: Mapping[str, Any]) -> str:
    """Derive the stable state/core hash from a frozen runtime-config payload."""

    return _canonical_hash(_state_semantics_source(payload))


def state_semantics_payload_from_frozen_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return _state_semantics_source(payload)


def declared_state_semantics_is_authentic(payload: Mapping[str, Any]) -> bool:
    """Authenticate either the legacy full-byte hash or the current core hash."""

    declared = payload.get("state_semantics_hash")
    if not isinstance(declared, str) or not _SHA256.fullmatch(declared):
        return False
    embedded = payload.get("state_semantics_payload")
    if embedded is not None:
        return (
            isinstance(embedded, Mapping)
            and dict(embedded) == _state_semantics_source(payload)
            and _canonical_hash(embedded) == declared
        )
    dependencies = payload.get("strategy_dependency_hashes")
    if not isinstance(dependencies, Mapping):
        return False
    legacy = {
        "schema_version": _LEGACY_STATE_SEMANTICS_SCHEMA,
        "strategy_version": payload.get("strategy_version"),
        "timezone": payload.get("timezone"),
        "return_profile_id": payload.get("return_profile_id"),
        "reference_profile_id": payload.get("reference_profile_id"),
        "clock": payload.get("clock"),
        "market_data": payload.get("market_data"),
        "policy": payload.get("policy"),
        "g_manifest_sha256": payload.get("g_manifest_sha256"),
        "strategy_dependency_hashes": dict(dependencies),
    }
    return _canonical_hash(legacy) == declared


def legacy_state_semantics_is_compatible_with_current(
    legacy_payload: Mapping[str, Any],
    current_payload: Mapping[str, Any],
) -> bool:
    """Prove the one reviewed legacy-full-hash to current-core bridge.

    This is intentionally not a generic legacy migration.  Unknown v1 service
    bytes are rejected even when their obvious policy files happen to match.
    Once a config carries the v2 payload, ordinary compatibility is exact core
    hash equality.
    """

    declared = legacy_payload.get("state_semantics_hash")
    if declared not in _AUDITED_LEGACY_STATE_SEMANTICS_HASHES:
        return False
    if not declared_state_semantics_is_authentic(legacy_payload):
        return False
    if not declared_state_semantics_is_authentic(current_payload):
        return False
    if current_payload.get("state_semantics_hash") != state_semantics_hash_from_frozen_payload(
        current_payload
    ):
        return False
    identity_fields = (
        "strategy_version",
        "official_stream_id",
        "state_lineage_id",
        "timezone",
        "return_profile_id",
        "reference_profile_id",
        "clock",
        "market_data",
        "policy",
        "g_manifest_sha256",
        "bootstrap_mode",
        "bootstrap_checkpoint_sha256",
    )
    if any(legacy_payload.get(field) != current_payload.get(field) for field in identity_fields):
        return False
    legacy_dependencies = legacy_payload.get("strategy_dependency_hashes")
    current_dependencies = current_payload.get("strategy_dependency_hashes")
    if not isinstance(legacy_dependencies, Mapping) or not isinstance(
        current_dependencies, Mapping
    ):
        return False
    if not all(
        legacy_dependencies.get(relative) == current_dependencies.get(relative)
        for relative in _STATE_SEMANTICS_DEPENDENCY_FILES
    ):
        return False
    for relative, reviewed in _MIXED_STATE_SOURCE_CLASSES.items():
        legacy_digest = legacy_dependencies.get(relative)
        current_digest = current_dependencies.get(relative)
        if not isinstance(legacy_digest, str) or not isinstance(current_digest, str):
            return False
        legacy_class = reviewed.get(legacy_digest)
        current_class = reviewed.get(current_digest)
        if legacy_class is None or legacy_class != current_class:
            return False
    return True


def is_audited_legacy_state_semantics_hash(value: object) -> bool:
    return isinstance(value, str) and value in _AUDITED_LEGACY_STATE_SEMANTICS_HASHES


def _exact_keys(value: dict[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        missing = sorted(expected - set(value))
        extra = sorted(set(value) - expected)
        raise V20ConfigError(f"{field} field set mismatch; missing={missing}, extra={extra}")


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise V20ConfigError(f"duplicate YAML key: {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _dependency_hashes(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in _STRATEGY_DEPENDENCY_FILES:
        path = (
            resolve_concept_data_path(root, Path(relative).name)
            if relative in {"data/sectors.json", "data/board_constituents.json"}
            else root / relative
        )
        if not path.is_file():
            raise V20ConfigError(f"missing frozen strategy dependency: {relative}")
        result[relative] = _sha256_file(path)
    return result


def _time(value: object, field: str) -> time:
    if not isinstance(value, str):
        raise V20ConfigError(f"{field} must be HH:MM")
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise V20ConfigError(f"{field} must be HH:MM") from exc


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise V20ConfigError(f"{name} must be a boolean")


def _canonical_https_origin(value: str, field: str) -> str:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise V20ConfigError(f"{field} must be a strict HTTPS origin")
    try:
        port = parsed.port
    except ValueError as exc:
        raise V20ConfigError(f"{field} has an invalid port") from exc
    hostname = parsed.hostname.lower()
    if ":" in hostname:
        hostname = f"[{hostname}]"
    return f"https://{hostname}{f':{port}' if port is not None else ''}"


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _explicit_env(name: str, *, minimum_length: int = 1) -> str:
    value = os.environ.get(name)
    if value is None or len(value.strip()) < minimum_length:
        raise V20ConfigError(f"enabled V20 requires explicit {name}")
    normalized = value.strip()
    lowered = normalized.lower()
    placeholder_markers = (
        "unconfigured",
        "replace-with",
        "change-me",
        "changeme",
        "placeholder",
        "dummy",
        "todo",
        "example.",
    )
    if any(marker in lowered for marker in placeholder_markers):
        raise V20ConfigError(f"enabled V20 rejects placeholder {name}")
    return normalized


def _parse_route_binding(name: str, value: Mapping[str, Any]) -> V20RouteBinding:
    _exact_keys(dict(value), _ROUTE_BINDING_KEYS, f"routes.{name}")
    route_id = str(value["route_id"]).strip()
    origin = str(value["expected_bot_origin"]).strip()
    app_hash = str(value["expected_app_id_sha256"]).strip()
    chat_hash = str(value["expected_chat_id_sha256"]).strip()
    fields = (origin, app_hash, chat_hash)
    if all(item == _UNCONFIGURED for item in fields):
        return V20RouteBinding(route_id, *fields)
    if any(item == _UNCONFIGURED for item in fields):
        raise V20ConfigError(f"routes.{name} reviewed binding is only partially configured")
    canonical_origin = _canonical_https_origin(origin, f"routes.{name}.expected_bot_origin")
    if not _SHA256.fullmatch(app_hash) or not _SHA256.fullmatch(chat_hash):
        raise V20ConfigError(f"routes.{name} reviewed identifiers must be SHA-256 digests")
    return V20RouteBinding(route_id, canonical_origin, app_hash, chat_hash)


def validate_v20_api_keys() -> None:
    """Require independent evidence-write and status-read runtime keys."""

    ingest_key = _explicit_env("V20_INGEST_API_KEY", minimum_length=32)
    status_key = _explicit_env("V20_STATUS_API_KEY", minimum_length=32)
    if hmac.compare_digest(ingest_key, status_key):
        raise V20ConfigError("V20 ingest and status API keys must be different")


def validated_v20_tushare_token() -> str:
    """Return only the explicit V20 environment token, never legacy fallbacks."""

    return _explicit_env("TUSHARE_TOKEN")


def validate_v20_database_consumers(
    writer: _DatabaseConsumerConfig,
    fundamentals: _DatabaseConsumerConfig,
) -> None:
    """Bind the configs actually consumed by asyncpg to the reviewed environment."""

    def validate(prefix: str, actual: _DatabaseConsumerConfig) -> None:
        try:
            expected: dict[str, object] = {
                "host": _explicit_env(f"{prefix}_HOST"),
                "port": int(_explicit_env(f"{prefix}_PORT")),
                "database": _explicit_env(f"{prefix}_NAME"),
                "user": _explicit_env(f"{prefix}_USER"),
                "ssl_mode": _explicit_env(f"{prefix}_SSLMODE"),
                "ssl_root_cert": _explicit_env(f"{prefix}_SSLROOTCERT"),
                "ssl_root_cert_sha256": _explicit_env(f"{prefix}_SSLROOTCERT_SHA256"),
                "connect_timeout_seconds": float(
                    _explicit_env(f"{prefix}_CONNECT_TIMEOUT_SECONDS")
                ),
                "command_timeout_seconds": float(
                    _explicit_env(f"{prefix}_COMMAND_TIMEOUT_SECONDS")
                ),
            }
        except ValueError as exc:
            raise V20ConfigError(f"enabled V20 has invalid numeric {prefix} setting") from exc
        for field, expected_value in expected.items():
            if getattr(actual, field) != expected_value:
                raise V20ConfigError(f"actual {prefix} {field} differs from explicit environment")
        expected_password = _explicit_env(f"{prefix}_PASSWORD")
        actual_password = actual.password
        if not isinstance(actual_password, str) or not hmac.compare_digest(
            actual_password,
            expected_password,
        ):
            raise V20ConfigError(f"actual {prefix} password differs from explicit environment")

    validate("V20_DB", writer)
    validate("DB", fundamentals)


def _validate_enabled_environment(
    *,
    active_binding: V20RouteBinding,
    route_prefix: str,
    v20_ca_sha256: str,
    fundamentals_ca_sha256: str,
) -> None:
    if not active_binding.configured:
        raise V20ConfigError("enabled V20 requires a reviewed destination binding")

    bot_url = _explicit_env(f"{route_prefix}_BOT_URL")
    app_id = _explicit_env(f"{route_prefix}_APP_ID")
    _explicit_env(f"{route_prefix}_APP_SECRET")
    chat_id = _explicit_env(f"{route_prefix}_CHAT_ID")
    actual_origin = _canonical_https_origin(bot_url, f"{route_prefix}_BOT_URL")
    if actual_origin != active_binding.expected_bot_origin:
        raise V20ConfigError("active V20 relay origin differs from reviewed binding")
    if not hmac.compare_digest(_sha256_text(app_id), active_binding.expected_app_id_sha256):
        raise V20ConfigError("active V20 app_id differs from reviewed binding")
    if not hmac.compare_digest(_sha256_text(chat_id), active_binding.expected_chat_id_sha256):
        raise V20ConfigError("active V20 chat_id differs from reviewed binding")

    validate_v20_api_keys()

    db_names = (
        "V20_DB_HOST",
        "V20_DB_PORT",
        "V20_DB_NAME",
        "V20_DB_USER",
        "V20_DB_PASSWORD",
        "DB_HOST",
        "DB_PORT",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "V20_DB_CONNECT_TIMEOUT_SECONDS",
        "V20_DB_COMMAND_TIMEOUT_SECONDS",
        "DB_CONNECT_TIMEOUT_SECONDS",
        "DB_COMMAND_TIMEOUT_SECONDS",
    )
    values = {name: _explicit_env(name) for name in db_names}
    validated_v20_tushare_token()
    for prefix, expected_hash in (
        ("V20_DB", v20_ca_sha256),
        ("DB", fundamentals_ca_sha256),
    ):
        if not _SHA256.fullmatch(expected_hash):
            raise V20ConfigError(f"enabled V20 requires reviewed {prefix} CA SHA-256")
        if _explicit_env(f"{prefix}_SSLMODE") != "verify-full":
            raise V20ConfigError(f"{prefix}_SSLMODE must be verify-full")
        ca_path = _explicit_env(f"{prefix}_SSLROOTCERT")
        env_hash = _explicit_env(f"{prefix}_SSLROOTCERT_SHA256")
        if env_hash != expected_hash:
            raise V20ConfigError(f"{prefix} CA digest differs from reviewed configuration")
        try:
            actual_hash = sha256_file(ca_path)
        except ValueError as exc:
            raise V20ConfigError(str(exc)) from exc
        if not hmac.compare_digest(actual_hash, expected_hash):
            raise V20ConfigError(f"{prefix} CA file differs from reviewed digest")

    if values["V20_DB_USER"] == values["DB_USER"]:
        raise V20ConfigError("V20 writer and fundamentals reader must use different principals")


@dataclass(frozen=True)
class V20ClockConfig:
    prewarm: time
    minute_collection_start: time
    decision_bar_label: str
    publish_deadline: time
    decision_finalization_deadline: time
    reference_bar_label: str
    reference_lock_deadline_next_day: time
    mews_cutoff_d1: time
    plan_exit: time
    reminder_check: time


@dataclass(frozen=True)
class V20MarketConfig:
    minimum_quote_coverage: float
    minimum_breadth_universe: int
    minute_poll_seconds: int
    exit_poll_seconds: int


@dataclass(frozen=True)
class V20RouteBinding:
    route_id: str
    expected_bot_origin: str
    expected_app_id_sha256: str
    expected_chat_id_sha256: str

    @property
    def configured(self) -> bool:
        return all(
            value != _UNCONFIGURED
            for value in (
                self.expected_bot_origin,
                self.expected_app_id_sha256,
                self.expected_chat_id_sha256,
            )
        )

    def as_payload(self) -> dict[str, str]:
        return {
            "route_id": self.route_id,
            "expected_bot_origin": self.expected_bot_origin,
            "expected_app_id_sha256": self.expected_app_id_sha256,
            "expected_chat_id_sha256": self.expected_chat_id_sha256,
        }

    @property
    def destination_fingerprint(self) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "schema_version": "v20-destination-binding/v1",
                    **self.as_payload(),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class V20RuntimeConfig:
    project_root: Path
    config_path: Path
    schema_version: str
    strategy_version: str
    official_stream_id: str
    enabled: bool
    deployment_mode: str
    production_activation_guard: bool
    timezone: str
    return_profile_id: str
    reference_profile_id: str
    state_lineage_id: str
    clock: V20ClockConfig
    market: V20MarketConfig
    policy: dict[str, Any]
    artifact_manifest_path: Path
    artifact_manifest_sha256: str
    database_schema: str
    database_pool_min_size: int
    database_pool_max_size: int
    route_id: str
    route_bindings: dict[str, V20RouteBinding]
    route_binding: V20RouteBinding
    v20_db_ca_sha256: str
    fundamentals_db_ca_sha256: str
    bootstrap_mode: str
    bootstrap_checkpoint_path: Path | None
    bootstrap_checkpoint_sha256: str | None
    strategy_dependency_hashes: dict[str, str]
    database_config_sha256: str
    state_semantics_payload: dict[str, Any]
    state_semantics_hash: str
    frozen_payload: dict[str, Any]
    config_hash: str

    @property
    def is_production(self) -> bool:
        return self.deployment_mode == "production_push"


def load_v20_runtime_config(
    project_root: Path,
    config_path: Path | None = None,
) -> V20RuntimeConfig:
    root = project_root.resolve()
    path = (config_path or root / "config" / "v20.yaml").resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise V20ConfigError("V20 config must stay inside the project root") from exc
    try:
        raw = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except (OSError, yaml.YAMLError) as exc:
        raise V20ConfigError(f"cannot load V20 config: {exc}") from exc
    if not isinstance(raw, dict):
        raise V20ConfigError("V20 config root must be an object")
    _exact_keys(raw, _TOP_LEVEL_KEYS, "V20 config")
    if raw.get("schema_version") != "v20-runtime/v2":
        raise V20ConfigError("unsupported V20 config schema")
    if (
        type(raw.get("enabled")) is not bool
        or type(raw.get("production_activation_guard")) is not bool
    ):
        raise V20ConfigError("enabled and production_activation_guard must be booleans")
    if raw["enabled"] or raw["production_activation_guard"]:
        raise V20ConfigError(
            "checked-in enabled and production_activation_guard must remain false; "
            "activation is environment-only"
        )

    enabled = _bool_env("V20_ENABLED", False)
    mode = os.getenv("V20_MODE", str(raw.get("deployment_mode", "forward_shadow"))).strip()
    if mode not in {"forward_shadow", "production_push"}:
        raise V20ConfigError("deployment_mode must be forward_shadow or production_push")
    activation_guard = _bool_env(
        "V20_ALLOW_PRODUCTION_PUSH",
        False,
    )
    if (
        enabled
        and mode == "production_push"
        and (
            os.getenv("V20_ENABLED") is None
            or os.getenv("V20_MODE") is None
            or os.getenv("V20_ALLOW_PRODUCTION_PUSH") is None
            or not activation_guard
        )
    ):
        raise V20ConfigError(
            "production push requires explicit V20_ENABLED=true, "
            "V20_MODE=production_push, and V20_ALLOW_PRODUCTION_PUSH=true"
        )
    strategy_version = raw.get("strategy_version")
    stream_id = raw.get("official_stream_id")
    lineage_id = raw.get("state_lineage_id")
    if not isinstance(strategy_version, str) or not strategy_version.strip():
        raise V20ConfigError("strategy_version must be a non-empty string")
    if not isinstance(stream_id, str) or not isinstance(lineage_id, str):
        raise V20ConfigError("official_stream_id and state_lineage_id must be strings")
    stream_id = stream_id.strip()
    lineage_id = lineage_id.strip()
    if not stream_id or not lineage_id:
        raise V20ConfigError("official_stream_id and state_lineage_id cannot be empty")
    if mode == "forward_shadow" and (
        "SHADOW" not in stream_id.upper() or "SHADOW" not in lineage_id.upper()
    ):
        raise V20ConfigError("forward shadow requires dedicated SHADOW stream and lineage IDs")
    if mode == "production_push" and (
        "SHADOW" in stream_id.upper() or "SHADOW" in lineage_id.upper()
    ):
        raise V20ConfigError("production push cannot reuse a shadow stream or lineage")

    clock_raw = raw.get("clock")
    market_raw = raw.get("market_data")
    artifacts_raw = raw.get("artifacts")
    database_raw = raw.get("database")
    routes_raw = raw.get("routes")
    bootstrap_raw = raw.get("bootstrap")
    policy_raw = raw.get("policy")
    for name, value in (
        ("clock", clock_raw),
        ("market_data", market_raw),
        ("artifacts", artifacts_raw),
        ("database", database_raw),
        ("routes", routes_raw),
        ("bootstrap", bootstrap_raw),
        ("policy", policy_raw),
    ):
        if not isinstance(value, dict):
            raise V20ConfigError(f"{name} must be an object")
    # The loop above is the runtime schema guard.  Retain those proven types
    # explicitly so static checking cannot accidentally mask a nullable config
    # branch added during a future edit.
    clock_raw = cast(dict[str, Any], clock_raw)
    market_raw = cast(dict[str, Any], market_raw)
    artifacts_raw = cast(dict[str, Any], artifacts_raw)
    database_raw = cast(dict[str, Any], database_raw)
    routes_raw = cast(dict[str, Any], routes_raw)
    bootstrap_raw = cast(dict[str, Any], bootstrap_raw)
    policy_raw = cast(dict[str, Any], policy_raw)
    _exact_keys(clock_raw, _CLOCK_KEYS, "clock")
    _exact_keys(market_raw, _MARKET_KEYS, "market_data")
    _exact_keys(policy_raw, set(_POLICY_VALUES), "policy")
    _exact_keys(artifacts_raw, {"g_manifest", "g_manifest_sha256"}, "artifacts")
    _exact_keys(
        database_raw,
        {
            "schema",
            "pool_min_size",
            "pool_max_size",
            "v20_tls_ca_sha256",
            "fundamentals_tls_ca_sha256",
        },
        "database",
    )
    _exact_keys(routes_raw, {"forward_shadow", "production_push"}, "routes")
    _exact_keys(bootstrap_raw, {"mode", "checkpoint_path", "checkpoint_sha256"}, "bootstrap")

    if raw.get("timezone") != "Asia/Shanghai":
        raise V20ConfigError("V20 timezone is frozen to Asia/Shanghai")
    if raw.get("return_profile_id") != "ZERO_COST_GROSS_PRICE_RETURN_V1":
        raise V20ConfigError("unsupported V20 return profile")
    if raw.get("reference_profile_id") != "CALENDAR_0940_OPEN_END_LABEL_0941_V1":
        raise V20ConfigError("unsupported V20 reference profile")
    if policy_raw != _POLICY_VALUES:
        raise V20ConfigError("V20 policy values differ from the frozen implementation")
    frozen_clock_values = {
        "prewarm": "09:15",
        "minute_collection_start": "09:31",
        "decision_bar_label": "09:39",
        "publish_deadline": "09:40",
        "decision_finalization_deadline": "09:45",
        "reference_bar_label": "09:41",
        "reference_lock_deadline_next_day": "09:30",
        "mews_cutoff_d1": "09:40",
        "plan_exit": "14:57",
        "reminder_check": "09:35",
    }
    if clock_raw != frozen_clock_values:
        raise V20ConfigError("V20 clock values differ from the frozen implementation")
    if float(market_raw["minimum_quote_coverage"]) != 0.80:
        raise V20ConfigError("V20 minimum quote coverage is frozen at 0.80")
    if int(market_raw["minimum_breadth_universe"]) != 1_000:
        raise V20ConfigError("V20 minimum breadth universe is frozen at 1000")
    if int(market_raw["minute_poll_seconds"]) < 1:
        raise V20ConfigError("minute_poll_seconds must be positive")
    if int(market_raw["exit_poll_seconds"]) < 1:
        raise V20ConfigError("exit_poll_seconds must be positive")
    for route_name in ("forward_shadow", "production_push"):
        if not isinstance(routes_raw[route_name], Mapping):
            raise V20ConfigError(f"routes.{route_name} must be an object")
    route_bindings = {
        route_name: _parse_route_binding(
            route_name,
            cast(Mapping[str, Any], routes_raw[route_name]),
        )
        for route_name in ("forward_shadow", "production_push")
    }
    shadow_route_id = route_bindings["forward_shadow"].route_id
    formal_route_id = route_bindings["production_push"].route_id
    if not shadow_route_id or not formal_route_id:
        raise V20ConfigError("V20 route IDs cannot be empty")
    if shadow_route_id == formal_route_id:
        raise V20ConfigError("forward shadow and production must use different Feishu routes")
    if (shadow_route_id, formal_route_id) != (
        "V20_SHADOW_FEISHU",
        "V20_FORMAL_FEISHU",
    ):
        raise V20ConfigError("V20 route IDs do not match the isolated runtime route registry")
    configured_bindings = [binding for binding in route_bindings.values() if binding.configured]
    if len(configured_bindings) == 2:
        if (
            configured_bindings[0].expected_app_id_sha256
            == configured_bindings[1].expected_app_id_sha256
            or configured_bindings[0].expected_chat_id_sha256
            == configured_bindings[1].expected_chat_id_sha256
        ):
            raise V20ConfigError(
                "reviewed shadow and production destinations must use different app/chat IDs"
            )

    database_schema = database_raw["schema"]
    if not isinstance(database_schema, str) or not _SCHEMA_IDENTIFIER.fullmatch(database_schema):
        raise V20ConfigError("database.schema must be a valid PostgreSQL identifier")
    database_pool_min_size = int(database_raw["pool_min_size"])
    database_pool_max_size = int(database_raw["pool_max_size"])
    if (
        database_pool_min_size < 1
        or database_pool_max_size < database_pool_min_size
        or database_pool_max_size < 7
    ):
        raise V20ConfigError(
            "database pool sizes must satisfy 1 <= min <= max and max >= 7; "
            "the runtime leader holds one dedicated connection"
        )
    v20_db_ca_sha256 = str(database_raw["v20_tls_ca_sha256"]).strip()
    fundamentals_db_ca_sha256 = str(database_raw["fundamentals_tls_ca_sha256"]).strip()
    for name, value in (
        ("database.v20_tls_ca_sha256", v20_db_ca_sha256),
        ("database.fundamentals_tls_ca_sha256", fundamentals_db_ca_sha256),
    ):
        if value != _UNCONFIGURED and not _SHA256.fullmatch(value):
            raise V20ConfigError(f"{name} must be UNCONFIGURED or a lowercase SHA-256 digest")

    route_id = shadow_route_id if mode == "forward_shadow" else formal_route_id
    route_binding = route_bindings[mode]
    if enabled:
        _validate_enabled_environment(
            active_binding=route_binding,
            route_prefix=("V20_SHADOW_FEISHU" if mode == "forward_shadow" else "V20_FEISHU"),
            v20_ca_sha256=v20_db_ca_sha256,
            fundamentals_ca_sha256=fundamentals_db_ca_sha256,
        )

    manifest_path = (root / str(artifacts_raw["g_manifest"])).resolve()
    try:
        manifest_path.relative_to(root)
    except ValueError as exc:
        raise V20ConfigError("artifact manifest must stay inside project root") from exc
    expected_manifest_hash = artifacts_raw["g_manifest_sha256"]
    if not isinstance(expected_manifest_hash, str) or not _SHA256.fullmatch(expected_manifest_hash):
        raise V20ConfigError("g_manifest_sha256 must be a lowercase SHA-256 digest")
    if not manifest_path.is_file():
        raise V20ConfigError(f"missing G artifact manifest: {manifest_path}")
    actual_manifest_hash = _sha256_file(manifest_path)
    if actual_manifest_hash != expected_manifest_hash:
        raise V20ConfigError(f"G artifact manifest hash mismatch: {actual_manifest_hash}")

    checkpoint_value = bootstrap_raw.get("checkpoint_path")
    checkpoint_path = (root / str(checkpoint_value)).resolve() if checkpoint_value else None
    checkpoint_hash = bootstrap_raw.get("checkpoint_sha256")
    if checkpoint_path is not None:
        try:
            checkpoint_path.relative_to(root)
        except ValueError as exc:
            raise V20ConfigError("bootstrap checkpoint must stay inside the project root") from exc
    bootstrap_mode = str(bootstrap_raw.get("mode", ""))
    if mode == "production_push" and bootstrap_mode != "CHECKPOINT":
        raise V20ConfigError("production push requires bootstrap.mode=CHECKPOINT")
    if bootstrap_mode == "CHECKPOINT":
        if checkpoint_path is None or not checkpoint_path.is_file() or not checkpoint_hash:
            raise V20ConfigError("bootstrap checkpoint file/hash is required")
        if not isinstance(checkpoint_hash, str) or not _SHA256.fullmatch(checkpoint_hash):
            raise V20ConfigError("checkpoint_sha256 must be a lowercase SHA-256 digest")
        if _sha256_file(checkpoint_path) != checkpoint_hash:
            raise V20ConfigError("bootstrap checkpoint hash mismatch")
    elif bootstrap_mode != "EMPTY_FORWARD_SHADOW":
        raise V20ConfigError("unsupported bootstrap mode")

    dependency_hashes = _dependency_hashes(root)
    database_config_path = root / "config" / "database-config.yaml"
    if not database_config_path.is_file():
        raise V20ConfigError("missing database connection configuration")
    database_config_sha256 = _sha256_file(database_config_path)
    state_semantics_payload = _state_semantics_source(
        {
            "strategy_version": raw["strategy_version"],
            "timezone": raw["timezone"],
            "return_profile_id": raw["return_profile_id"],
            "reference_profile_id": raw["reference_profile_id"],
            "clock": clock_raw,
            "market_data": market_raw,
            "policy": policy_raw,
            "g_manifest_sha256": actual_manifest_hash,
            "strategy_dependency_hashes": dependency_hashes,
        }
    )
    state_semantics_hash = _canonical_hash(state_semantics_payload)
    frozen_payload = {
        "schema_version": raw["schema_version"],
        "strategy_version": raw["strategy_version"],
        "official_stream_id": raw["official_stream_id"],
        "deployment_mode": mode,
        "timezone": raw["timezone"],
        "return_profile_id": raw["return_profile_id"],
        "reference_profile_id": raw["reference_profile_id"],
        "state_lineage_id": raw["state_lineage_id"],
        "clock": clock_raw,
        "market_data": market_raw,
        "policy": policy_raw,
        "g_manifest_sha256": actual_manifest_hash,
        "strategy_dependency_hashes": dependency_hashes,
        # Database wiring is operational identity, not strategy/state semantics.
        "database_config_sha256": database_config_sha256,
        "state_semantics_hash": state_semantics_hash,
        "state_semantics_payload": state_semantics_payload,
        "database_schema": database_raw["schema"],
        "database_tls_ca_sha256": {
            "v20": v20_db_ca_sha256,
            "fundamentals": fundamentals_db_ca_sha256,
        },
        "route_id": route_id,
        "route_bindings": {name: binding.as_payload() for name, binding in route_bindings.items()},
        "bootstrap_mode": bootstrap_mode,
        "bootstrap_checkpoint_sha256": checkpoint_hash,
    }
    config_hash = hashlib.sha256(
        json.dumps(
            frozen_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()

    return V20RuntimeConfig(
        project_root=root,
        config_path=path,
        schema_version=str(raw["schema_version"]),
        strategy_version=strategy_version,
        official_stream_id=stream_id,
        enabled=enabled,
        deployment_mode=mode,
        production_activation_guard=activation_guard,
        timezone=str(raw["timezone"]),
        return_profile_id=str(raw["return_profile_id"]),
        reference_profile_id=str(raw["reference_profile_id"]),
        state_lineage_id=lineage_id,
        clock=V20ClockConfig(
            prewarm=_time(clock_raw["prewarm"], "clock.prewarm"),
            minute_collection_start=_time(
                clock_raw["minute_collection_start"],
                "clock.minute_collection_start",
            ),
            decision_bar_label=str(clock_raw["decision_bar_label"]),
            publish_deadline=_time(clock_raw["publish_deadline"], "clock.publish_deadline"),
            decision_finalization_deadline=_time(
                clock_raw["decision_finalization_deadline"],
                "clock.decision_finalization_deadline",
            ),
            reference_bar_label=str(clock_raw["reference_bar_label"]),
            reference_lock_deadline_next_day=_time(
                clock_raw["reference_lock_deadline_next_day"],
                "clock.reference_lock_deadline_next_day",
            ),
            mews_cutoff_d1=_time(clock_raw["mews_cutoff_d1"], "clock.mews_cutoff_d1"),
            plan_exit=_time(clock_raw["plan_exit"], "clock.plan_exit"),
            reminder_check=_time(clock_raw["reminder_check"], "clock.reminder_check"),
        ),
        market=V20MarketConfig(
            minimum_quote_coverage=float(market_raw["minimum_quote_coverage"]),
            minimum_breadth_universe=int(market_raw["minimum_breadth_universe"]),
            minute_poll_seconds=int(market_raw["minute_poll_seconds"]),
            exit_poll_seconds=int(market_raw["exit_poll_seconds"]),
        ),
        policy=dict(policy_raw),
        artifact_manifest_path=manifest_path,
        artifact_manifest_sha256=actual_manifest_hash,
        database_schema=database_schema,
        database_pool_min_size=database_pool_min_size,
        database_pool_max_size=database_pool_max_size,
        route_id=route_id,
        route_bindings=route_bindings,
        route_binding=route_binding,
        v20_db_ca_sha256=v20_db_ca_sha256,
        fundamentals_db_ca_sha256=fundamentals_db_ca_sha256,
        bootstrap_mode=bootstrap_mode,
        bootstrap_checkpoint_path=checkpoint_path,
        bootstrap_checkpoint_sha256=str(checkpoint_hash) if checkpoint_hash else None,
        strategy_dependency_hashes=dependency_hashes,
        database_config_sha256=database_config_sha256,
        state_semantics_payload=state_semantics_payload,
        state_semantics_hash=state_semantics_hash,
        frozen_payload=frozen_payload,
        config_hash=config_hash,
    )


__all__ = [
    "V20ClockConfig",
    "V20ConfigError",
    "V20MarketConfig",
    "V20RouteBinding",
    "V20RuntimeConfig",
    "declared_state_semantics_is_authentic",
    "is_audited_legacy_state_semantics_hash",
    "legacy_state_semantics_is_compatible_with_current",
    "load_v20_runtime_config",
    "state_semantics_hash_from_frozen_payload",
    "state_semantics_payload_from_frozen_payload",
    "validate_v20_api_keys",
    "validate_v20_database_consumers",
    "validated_v20_tushare_token",
]
