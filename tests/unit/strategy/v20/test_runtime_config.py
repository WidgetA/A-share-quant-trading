import hashlib
import shutil
from pathlib import Path

import pytest
import yaml

from src.strategy.v20.runtime_compatibility import is_audited_state_semantics_transition
from src.strategy.v20.runtime_config import (
    _MIXED_STATE_SOURCE_CLASSES,
    _STRATEGY_DEPENDENCY_FILES,
    V20ConfigError,
    load_v20_runtime_config,
    state_semantics_hash_from_frozen_payload,
    state_semantics_payload_from_frozen_payload,
)

PROJECT_ROOT = Path(__file__).resolve().parents[4]


_PRE_SELECTION_V2_CORE = "ca8670343e13251287e7016ed2af1d26101f567b40f70705020733350e56dbbc"
_SELECTION_V3_CORE = "94464f2a2c4a9c33c5041aeb640f0510947a438f4d5ddd305cdfc0e5f1cfba4b"
_SELECTION_V4_CORE = "0f5fbbd1e6cce372217373023f3681cf09100b870e7c4d187e2ebc7ebd1a8290"
_TYPE_CLEAN_CORE = "d402b32262be3f922a218c3fcd87c67c3943460b61103bdb9fae0e27104b8c41"
_SELECTION_V3_DEPENDENCIES = {
    "src/data/clients/tushare_realtime.py": (
        "5acbe08e3309d5db7d62cd2a6811eff07b212d665035894573ebe463ed61f6b9"
    ),
    "src/data/database/v20_repository.py": (
        "d34ba101ec95cd6a3a8c7b0933fbb3b6cee9c29dfca7af9fd316f13bf04b9601"
    ),
    "src/web/v15_scan_service.py": (
        "e7b786a06fee5c3d4d73af19a82137bb3bc4b1890ea0ef482a332286c630b4f3"
    ),
    "src/web/v20_scan_pipeline.py": (
        "526fa5aa2dae700c3824dc84576b166dcc1b9b1d3e6f48dffb7eb57efb61865c"
    ),
    "src/web/v20_service.py": ("985b11a06d4222fbb1ef42da6313bf155522400606e08c205d365ec901a3f7df"),
}

_SELECTION_V4_DEPENDENCIES = {
    "src/data/clients/tushare_realtime.py": (
        "ff1fc6d7e38c4b51f56a6d71f7abc1c7e3ef71034f365006c161fad0eceb381f"
    ),
    "src/data/database/v16_canonical_artifact_store.py": (
        "b5bbc0616384ebc07351d3af4946a3978f026d8145efadd5018fac0b5c3f9a51"
    ),
    "src/strategy/v20/runtime_compatibility.py": (
        "71a43defc9d6c07cff922d8b696dc27ebab57823aa2ad1bc095d4681d55d1f7d"
    ),
    "src/strategy/v20/runtime_config.py": (
        "e224cad5a81e065b42260b8463a34fd12dd1654a6f63f8d831880afe09a8bd4d"
    ),
    "src/web/v15_scan_service.py": (
        "e0c4835f87fc8962cf073277dbeebc27bc510623c854934960590975ea96efcd"
    ),
    "src/web/v20_service.py": ("ba7e69ab519186e8ac77441423ef67a034778cb770f71fe65028f22b69ceff62"),
    "src/web/v20_v16_daygate_attestation.py": (
        "cfeeb8bcef49bddb581df4a325325de32e3aee28ceb57ed4349b08c990adde77"
    ),
}


def _isolated_project(tmp_path: Path) -> tuple[Path, dict]:
    root = tmp_path / "project"
    (root / "config").mkdir(parents=True)
    artifact_dir = root / "docs" / "strategy-v20-artifacts"
    artifact_dir.mkdir(parents=True)
    shutil.copy2(
        PROJECT_ROOT / "docs" / "strategy-v20-artifacts" / "manifest-v1.json",
        artifact_dir / "manifest-v1.json",
    )
    for relative in _STRATEGY_DEPENDENCY_FILES:
        dependency = root / relative
        dependency.parent.mkdir(parents=True, exist_ok=True)
        if relative in {
            "src/web/v20_service.py",
            "src/data/database/v20_repository.py",
        }:
            shutil.copy2(PROJECT_ROOT / relative, dependency)
        else:
            dependency.write_text(f"test fixture for {relative}\n", encoding="utf-8")
    shutil.copy2(
        PROJECT_ROOT / "config" / "database-config.yaml",
        root / "config" / "database-config.yaml",
    )
    raw = yaml.safe_load((PROJECT_ROOT / "config" / "v20.yaml").read_text(encoding="utf-8"))
    return root, raw


def _write_runtime(root: Path, raw: dict) -> None:
    (root / "config" / "v20.yaml").write_text(
        yaml.safe_dump(raw, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _configure_reviewed_shadow(
    root: Path,
    raw: dict,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_id = "shadow-app"
    chat_id = "shadow-chat"
    raw["routes"]["forward_shadow"] = {
        "route_id": "V20_SHADOW_FEISHU",
        "expected_bot_origin": "https://relay.internal",
        "expected_app_id_sha256": hashlib.sha256(app_id.encode()).hexdigest(),
        "expected_chat_id_sha256": hashlib.sha256(chat_id.encode()).hexdigest(),
    }
    ca_path = tmp_path / "postgres-ca.pem"
    ca_path.write_text("reviewed test CA bytes\n", encoding="utf-8")
    ca_hash = hashlib.sha256(ca_path.read_bytes()).hexdigest()
    raw["database"]["v20_tls_ca_sha256"] = ca_hash
    raw["database"]["fundamentals_tls_ca_sha256"] = ca_hash
    _write_runtime(root, raw)
    environment = {
        "V20_ENABLED": "true",
        "V20_MODE": "forward_shadow",
        "V20_SHADOW_FEISHU_BOT_URL": "https://relay.internal",
        "V20_SHADOW_FEISHU_APP_ID": app_id,
        "V20_SHADOW_FEISHU_APP_SECRET": "shadow-secret",
        "V20_SHADOW_FEISHU_CHAT_ID": chat_id,
        "V20_INGEST_API_KEY": "i" * 32,
        "V20_STATUS_API_KEY": "s" * 32,
        "V20_DB_HOST": "postgres.internal",
        "V20_DB_PORT": "5432",
        "V20_DB_NAME": "strategy",
        "V20_DB_USER": "v20_writer",
        "V20_DB_PASSWORD": "writer-secret",
        "V20_DB_SSLMODE": "verify-full",
        "V20_DB_SSLROOTCERT": str(ca_path),
        "V20_DB_SSLROOTCERT_SHA256": ca_hash,
        "V20_DB_CONNECT_TIMEOUT_SECONDS": "5",
        "V20_DB_COMMAND_TIMEOUT_SECONDS": "15",
        "DB_HOST": "postgres.internal",
        "DB_PORT": "5432",
        "DB_NAME": "strategy",
        "DB_USER": "fundamentals_reader",
        "DB_PASSWORD": "reader-secret",
        "DB_SSLMODE": "verify-full",
        "DB_SSLROOTCERT": str(ca_path),
        "DB_SSLROOTCERT_SHA256": ca_hash,
        "DB_CONNECT_TIMEOUT_SECONDS": "5",
        "DB_COMMAND_TIMEOUT_SECONDS": "15",
        "TUSHARE_TOKEN": "tushare-secret",
    }
    for name, value in environment.items():
        monkeypatch.setenv(name, value)


def test_frozen_runtime_config_loads_with_safe_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    monkeypatch.delenv("V20_ALLOW_PRODUCTION_PUSH", raising=False)

    config = load_v20_runtime_config(PROJECT_ROOT)

    assert config.enabled is False
    assert config.deployment_mode == "forward_shadow"
    assert "SHADOW" in config.official_stream_id
    assert "SHADOW" in config.state_lineage_id
    assert config.clock.decision_bar_label == "09:39"
    assert config.clock.reference_bar_label == "09:41"
    assert config.return_profile_id == "ZERO_COST_GROSS_PRICE_RETURN_V1"
    assert len(config.strategy_dependency_hashes) == len(_STRATEGY_DEPENDENCY_FILES)
    assert config.state_semantics_hash == _TYPE_CLEAN_CORE
    assert (
        config.state_semantics_payload["state_input_orchestration_profile"]
        == "V20_STATE_INPUT_ORCHESTRATION_V3"
    )
    assert {
        "src/data/database/v16_canonical_artifact_store.py",
        "src/data/database/v20_mews_guard_store.py",
        "src/data/database/v20_mews_receipt_guard.py",
        "src/web/v20_v16_canonical_artifact.py",
    }.issubset(config.state_semantics_payload["state_dependency_hashes"])
    assert (
        "src/web/v20_v16_daygate_attestation.py"
        not in config.state_semantics_payload["state_dependency_hashes"]
    )
    assert config.frozen_payload["state_semantics_hash"] == config.state_semantics_hash
    assert len(config.config_hash) == 64
    assert config.schema_version == "v20-runtime/v2"
    assert config.route_binding.configured is False
    assert config.v20_db_ca_sha256 == "UNCONFIGURED"


def test_config_hash_binds_exact_v16_and_v20_strategy_bytes(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)

    before = load_v20_runtime_config(root)
    dependency = root / "src" / "strategy" / "strategies" / "v16_scanner.py"
    dependency.write_text("different deployed V16 bytes\n", encoding="utf-8")
    after = load_v20_runtime_config(root)

    assert before.state_semantics_hash != after.state_semantics_hash
    assert before.config_hash != after.config_hash


def test_config_hash_binds_database_wiring_without_changing_state_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)

    before = load_v20_runtime_config(root)
    database_config = root / "config" / "database-config.yaml"
    database_config.write_text(
        database_config.read_text(encoding="utf-8") + "\n# reviewed wiring changed\n",
        encoding="utf-8",
    )
    after = load_v20_runtime_config(root)

    assert before.database_config_sha256 != after.database_config_sha256
    assert before.config_hash != after.config_hash
    assert before.state_semantics_hash == after.state_semantics_hash


@pytest.mark.parametrize(
    "relative",
    [
        "src/data/database/v16_canonical_artifact_store.py",
        "src/data/database/v20_mews_guard_store.py",
        "src/data/database/v20_mews_receipt_guard.py",
        "src/web/v20_v16_canonical_artifact.py",
    ],
)
def test_v4_state_hash_binds_each_new_official_state_module(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: str,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)
    before = load_v20_runtime_config(root)
    (root / relative).write_text("changed new runtime module bytes\n", encoding="utf-8")
    after = load_v20_runtime_config(root)

    assert before.state_semantics_hash != after.state_semantics_hash
    assert before.config_hash != after.config_hash


def test_daygate_check_only_bytes_bind_full_config_without_forking_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)
    before = load_v20_runtime_config(root)

    (root / "src/web/v20_v16_daygate_attestation.py").write_text(
        "changed explicit check-only attestation bytes\n",
        encoding="utf-8",
    )
    after = load_v20_runtime_config(root)

    assert before.config_hash != after.config_hash
    assert before.state_semantics_hash == after.state_semantics_hash


@pytest.mark.parametrize(
    "relative",
    [
        "src/strategy/strategies/momentum_sector_scanner.py",
        "src/strategy/filters/momentum_quality_filter.py",
        "src/data/clients/ifind_http_client.py",
        "src/common/config.py",
    ],
)
def test_config_hash_binds_lazy_v16_import_closure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: str,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)
    before = load_v20_runtime_config(root)

    (root / relative).write_text("changed lazy V16 dependency\n", encoding="utf-8")
    after = load_v20_runtime_config(root)

    assert before.state_semantics_hash != after.state_semantics_hash
    assert before.config_hash != after.config_hash


@pytest.mark.parametrize(
    "relative",
    ["src/common/feishu_bot.py", "src/common/v20_feishu.py"],
)
def test_notification_only_bytes_do_not_fork_state_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: str,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)
    before = load_v20_runtime_config(root)

    (root / relative).write_text("changed notification-only bytes\n", encoding="utf-8")
    after = load_v20_runtime_config(root)

    assert before.config_hash != after.config_hash
    assert before.state_semantics_hash == after.state_semantics_hash


def test_unreviewed_mixed_service_bytes_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)

    (root / "src/web/v20_service.py").write_text(
        "unreviewed state-sensitive service bytes\n",
        encoding="utf-8",
    )

    with pytest.raises(V20ConfigError, match="unreviewed state-sensitive mixed source"):
        load_v20_runtime_config(root)


def test_selection_v4_service_lf_and_crlf_share_core_but_bind_full_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)
    service_path = root / "src/web/v20_service.py"
    source = (PROJECT_ROOT / "src/web/v20_service.py").read_bytes()
    lf_source = source.replace(b"\r\n", b"\n")
    crlf_source = lf_source.replace(b"\n", b"\r\n")

    assert hashlib.sha256(lf_source).hexdigest() == (
        "a7170343fdac66b177bb1ca50c4680308f2ce4e83d77ec00aabb69204b71b0ac"
    )
    assert hashlib.sha256(crlf_source).hexdigest() == (
        "149c36817be6665c12e2bbfeceb5527fd6382d2865cbbf95b16a86ee118b0d17"
    )

    service_path.write_bytes(lf_source)
    lf_config = load_v20_runtime_config(root)
    service_path.write_bytes(crlf_source)
    crlf_config = load_v20_runtime_config(root)

    assert lf_config.state_semantics_hash == crlf_config.state_semantics_hash
    assert (
        lf_config.state_semantics_payload["mixed_state_source_classes"]["src/web/v20_service.py"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert lf_config.config_hash != crlf_config.config_hash


def test_selection_v3_and_v4_source_bytes_and_upgrade_edges_are_exact() -> None:
    service_classes = _MIXED_STATE_SOURCE_CLASSES["src/web/v20_service.py"]
    repository_classes = _MIXED_STATE_SOURCE_CLASSES["src/data/database/v20_repository.py"]

    assert (
        service_classes["985b11a06d4222fbb1ef42da6313bf155522400606e08c205d365ec901a3f7df"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V3"
    )
    assert (
        service_classes["2aaf4957addc5f09d7eca3aa7e45ed525c87a7fb8e470ad68f0b5b2f94d9d78f"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V3"
    )
    assert (
        service_classes["aa5268d53a9337c84c1a4ef9f25e78b1e657dbe81e72de989a36ea370dcc4f24"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert (
        service_classes["a33c99d74fe3cdbc220b5806ddb071fd551eb6ae15505a0b0e57d02707ca445e"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert (
        service_classes["ba7e69ab519186e8ac77441423ef67a034778cb770f71fe65028f22b69ceff62"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert (
        service_classes["616059227bf2e79802ba17fb0936102143f9e3c73bb63fbe8215491d64d092a8"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert (
        service_classes["d1135bbf20c3beecaa114ad918fa49b6a6b279b62d3fb1d455a4d3e7122d97f1"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert (
        service_classes["e514292401bde5930b503fc640393cd90f7a241a2ff93059cff1bc230902e5e0"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert (
        service_classes["a7170343fdac66b177bb1ca50c4680308f2ce4e83d77ec00aabb69204b71b0ac"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert (
        service_classes["149c36817be6665c12e2bbfeceb5527fd6382d2865cbbf95b16a86ee118b0d17"]
        == "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )
    assert "56fe73c25e65309a3fc52cc1a47c0a102c99a07b8370f9d5788c8ef2222921e1" not in (
        service_classes
    )
    assert "568616622cc3fcd7cf21f60773a87db6a9bda9cdbb236eec6d5c960c31b2e998" not in (
        service_classes
    )
    assert (
        repository_classes["bfcb7d5881e2597bfbc46d3826e9cee45656e4419d2e6dc489fea3c81de4d35e"]
        == "V20_LEDGER_STATE_CONTRACT_V2"
    )
    assert (
        repository_classes["d34ba101ec95cd6a3a8c7b0933fbb3b6cee9c29dfca7af9fd316f13bf04b9601"]
        == "V20_LEDGER_STATE_CONTRACT_V2"
    )
    assert (
        repository_classes["535fd459ac0867e7373d3a398f8d4425f5e388cf6a331c65fbd61faefc5255e9"]
        == "V20_LEDGER_STATE_CONTRACT_V2"
    )
    assert "e0d8cedccc7a69b1d47addd4948a02c5f7f80647d9f67d1c9bd2fa74aa8e6040" not in (
        repository_classes
    )
    assert "948285f38293a07c07e27e4e77b54999506a7a3a1a926aa4a58c6e543544c094" not in (
        repository_classes
    )
    assert is_audited_state_semantics_transition(
        _PRE_SELECTION_V2_CORE,
        _SELECTION_V3_CORE,
    )
    assert is_audited_state_semantics_transition(
        _SELECTION_V3_CORE,
        _TYPE_CLEAN_CORE,
    )
    assert not is_audited_state_semantics_transition(
        _SELECTION_V3_CORE,
        _SELECTION_V4_CORE,
    )
    assert not is_audited_state_semantics_transition(
        _SELECTION_V4_CORE,
        _TYPE_CLEAN_CORE,
    )
    assert not is_audited_state_semantics_transition(
        _PRE_SELECTION_V2_CORE,
        _SELECTION_V4_CORE,
    )
    assert not is_audited_state_semantics_transition(
        _PRE_SELECTION_V2_CORE,
        _TYPE_CLEAN_CORE,
    )
    assert not is_audited_state_semantics_transition(
        "0" * 64,
        _SELECTION_V3_CORE,
    )
    assert not is_audited_state_semantics_transition(
        _PRE_SELECTION_V2_CORE,
        "f" * 64,
    )


def test_historical_v3_payload_keeps_its_original_profile_and_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    current = load_v20_runtime_config(PROJECT_ROOT)
    payload = dict(current.frozen_payload)
    dependencies = dict(payload["strategy_dependency_hashes"])
    dependencies.update(_SELECTION_V3_DEPENDENCIES)
    payload["strategy_dependency_hashes"] = dependencies

    historical = state_semantics_payload_from_frozen_payload(payload)

    assert state_semantics_hash_from_frozen_payload(payload) == _SELECTION_V3_CORE
    assert historical["state_input_orchestration_profile"] == ("V20_STATE_INPUT_ORCHESTRATION_V1")
    assert not {
        "src/data/database/v16_canonical_artifact_store.py",
        "src/data/database/v20_mews_guard_store.py",
        "src/data/database/v20_mews_receipt_guard.py",
        "src/web/v20_v16_canonical_artifact.py",
    }.intersection(historical["state_dependency_hashes"])


def test_historical_v4_payload_keeps_its_original_profile_and_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    current = load_v20_runtime_config(PROJECT_ROOT)
    payload = dict(current.frozen_payload)
    dependencies = dict(payload["strategy_dependency_hashes"])
    dependencies.update(_SELECTION_V4_DEPENDENCIES)
    payload["strategy_dependency_hashes"] = dependencies

    historical = state_semantics_payload_from_frozen_payload(payload)

    assert state_semantics_hash_from_frozen_payload(payload) == _SELECTION_V4_CORE
    assert historical["state_input_orchestration_profile"] == ("V20_STATE_INPUT_ORCHESTRATION_V3")
    assert historical["mixed_state_source_classes"]["src/web/v20_service.py"] == (
        "V20_SERVICE_STATE_ORCHESTRATION_V4"
    )


def test_v4_payload_fails_closed_when_new_state_dependency_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    current = load_v20_runtime_config(PROJECT_ROOT)
    payload = dict(current.frozen_payload)
    dependencies = dict(payload["strategy_dependency_hashes"])
    dependencies.pop("src/web/v20_v16_canonical_artifact.py")
    payload["strategy_dependency_hashes"] = dependencies

    with pytest.raises(V20ConfigError, match="lacks valid state dependency"):
        state_semantics_payload_from_frozen_payload(payload)


def test_container_bundled_data_paths_keep_same_logical_hash_keys(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    bundled = root / "bundled_data"
    bundled.mkdir()
    for filename in (
        "sectors.json",
        "board_constituents.json",
        "v20_mews_bootstrap.json.gz",
    ):
        shutil.move(str(root / "data" / filename), str(bundled / filename))
    _write_runtime(root, raw)

    config = load_v20_runtime_config(root)

    assert "data/sectors.json" in config.strategy_dependency_hashes
    assert "data/board_constituents.json" in config.strategy_dependency_hashes
    assert "data/v20_mews_bootstrap.json.gz" in config.strategy_dependency_hashes


def test_production_requires_explicit_activation_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("V20_ENABLED", "true")
    monkeypatch.setenv("V20_MODE", "production_push")
    monkeypatch.delenv("V20_ALLOW_PRODUCTION_PUSH", raising=False)

    with pytest.raises(V20ConfigError, match="explicit"):
        load_v20_runtime_config(PROJECT_ROOT)


def test_production_cannot_reuse_default_shadow_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("V20_ENABLED", "true")
    monkeypatch.setenv("V20_MODE", "production_push")
    monkeypatch.setenv("V20_ALLOW_PRODUCTION_PUSH", "true")

    with pytest.raises(V20ConfigError, match="cannot reuse a shadow"):
        load_v20_runtime_config(PROJECT_ROOT)


def test_checkpoint_cannot_escape_immutable_project_root(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    outside = tmp_path / "outside-checkpoint.json"
    outside.write_text("{}\n", encoding="utf-8")
    raw["bootstrap"] = {
        "mode": "CHECKPOINT",
        "checkpoint_path": str(outside),
        "checkpoint_sha256": hashlib.sha256(outside.read_bytes()).hexdigest(),
    }
    _write_runtime(root, raw)

    with pytest.raises(V20ConfigError, match="must stay inside"):
        load_v20_runtime_config(root)


def test_route_ids_must_match_the_only_isolated_runtime_registry(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    raw["routes"]["forward_shadow"]["route_id"] = "TYPO_SHADOW_ROUTE"
    _write_runtime(root, raw)

    with pytest.raises(V20ConfigError, match="isolated runtime route registry"):
        load_v20_runtime_config(root)


def test_database_schema_is_validated_before_it_enters_config_hash(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    raw["database"]["schema"] = "v20;drop"
    _write_runtime(root, raw)

    with pytest.raises(V20ConfigError, match="PostgreSQL identifier"):
        load_v20_runtime_config(root)


def test_database_pool_reserves_capacity_beyond_the_leader_connection(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    monkeypatch.delenv("V20_MODE", raising=False)
    root, raw = _isolated_project(tmp_path)
    raw["database"]["pool_min_size"] = 1
    raw["database"]["pool_max_size"] = 6
    _write_runtime(root, raw)

    with pytest.raises(V20ConfigError, match="max >= 7"):
        load_v20_runtime_config(root)


def test_yaml_guard_cannot_replace_explicit_production_env(tmp_path, monkeypatch) -> None:
    root, raw = _isolated_project(tmp_path)
    raw["production_activation_guard"] = True
    _write_runtime(root, raw)
    monkeypatch.setenv("V20_ENABLED", "true")
    monkeypatch.setenv("V20_MODE", "production_push")
    monkeypatch.delenv("V20_ALLOW_PRODUCTION_PUSH", raising=False)

    with pytest.raises(V20ConfigError, match="must remain false"):
        load_v20_runtime_config(root)


def test_yaml_enabled_true_cannot_bypass_host_environment_guard(tmp_path, monkeypatch) -> None:
    root, raw = _isolated_project(tmp_path)
    raw["enabled"] = True
    _write_runtime(root, raw)
    monkeypatch.delenv("V20_ENABLED", raising=False)

    with pytest.raises(V20ConfigError, match="activation is environment-only"):
        load_v20_runtime_config(root)


def test_boolean_like_yaml_strings_are_rejected(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    root, raw = _isolated_project(tmp_path)
    raw["enabled"] = "false"
    _write_runtime(root, raw)

    with pytest.raises(V20ConfigError, match="must be booleans"):
        load_v20_runtime_config(root)


def test_runtime_v1_is_rejected_instead_of_silently_changing_schema(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    root, raw = _isolated_project(tmp_path)
    raw["schema_version"] = "v20-runtime/v1"
    _write_runtime(root, raw)

    with pytest.raises(V20ConfigError, match="unsupported V20 config schema"):
        load_v20_runtime_config(root)


def test_reviewed_destination_binding_changes_config_hash(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    root, raw = _isolated_project(tmp_path)
    _write_runtime(root, raw)
    before = load_v20_runtime_config(root)
    raw["routes"]["forward_shadow"] = {
        "route_id": "V20_SHADOW_FEISHU",
        "expected_bot_origin": "https://relay.internal",
        "expected_app_id_sha256": "a" * 64,
        "expected_chat_id_sha256": "b" * 64,
    }
    _write_runtime(root, raw)

    after = load_v20_runtime_config(root)

    assert before.config_hash != after.config_hash
    assert (
        after.frozen_payload["route_bindings"]["forward_shadow"]["expected_chat_id_sha256"]
        == "b" * 64
    )


def test_reviewed_database_ca_hashes_are_bound_into_config_hash(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("V20_ENABLED", raising=False)
    root, raw = _isolated_project(tmp_path)
    raw["database"]["v20_tls_ca_sha256"] = "a" * 64
    raw["database"]["fundamentals_tls_ca_sha256"] = "b" * 64
    _write_runtime(root, raw)
    before = load_v20_runtime_config(root)
    raw["database"]["fundamentals_tls_ca_sha256"] = "c" * 64
    _write_runtime(root, raw)

    after = load_v20_runtime_config(root)

    assert before.config_hash != after.config_hash
    assert after.frozen_payload["database_tls_ca_sha256"] == {
        "v20": "a" * 64,
        "fundamentals": "c" * 64,
    }


def test_enabled_v20_accepts_only_complete_reviewed_environment(tmp_path, monkeypatch) -> None:
    root, raw = _isolated_project(tmp_path)
    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)

    config = load_v20_runtime_config(root)

    assert config.enabled is True
    assert config.route_binding.configured is True
    assert config.frozen_payload["database_tls_ca_sha256"]["v20"] == (config.v20_db_ca_sha256)


def test_enabled_v20_fails_before_database_factory_when_explicit_db_env_is_missing(
    tmp_path, monkeypatch
) -> None:
    root, raw = _isolated_project(tmp_path)
    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    monkeypatch.delenv("V20_DB_PASSWORD")

    with pytest.raises(V20ConfigError, match="explicit V20_DB_PASSWORD"):
        load_v20_runtime_config(root)


def test_enabled_v20_rejects_copied_example_and_dummy_values(tmp_path, monkeypatch) -> None:
    root, raw = _isolated_project(tmp_path)
    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    monkeypatch.setenv("V20_DB_HOST", "postgres.example.internal")

    with pytest.raises(V20ConfigError, match="placeholder V20_DB_HOST"):
        load_v20_runtime_config(root)

    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    monkeypatch.setenv("TUSHARE_TOKEN", "dummy-token")
    with pytest.raises(V20ConfigError, match="placeholder TUSHARE_TOKEN"):
        load_v20_runtime_config(root)


def test_enabled_v20_rejects_route_and_ca_drift(tmp_path, monkeypatch) -> None:
    root, raw = _isolated_project(tmp_path)
    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    monkeypatch.setenv("V20_SHADOW_FEISHU_CHAT_ID", "different-chat")

    with pytest.raises(V20ConfigError, match="chat_id differs"):
        load_v20_runtime_config(root)

    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    monkeypatch.setenv("V20_DB_SSLROOTCERT_SHA256", "f" * 64)
    with pytest.raises(V20ConfigError, match="CA digest differs"):
        load_v20_runtime_config(root)


def test_enabled_v20_rejects_shared_api_keys(
    tmp_path,
    monkeypatch,
) -> None:
    root, raw = _isolated_project(tmp_path)
    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    shared = "x" * 32
    monkeypatch.setenv("V20_INGEST_API_KEY", shared)
    monkeypatch.setenv("V20_STATUS_API_KEY", shared)

    with pytest.raises(V20ConfigError, match="must be different"):
        load_v20_runtime_config(root)


@pytest.mark.parametrize(
    "name",
    ("V20_INGEST_API_KEY", "V20_STATUS_API_KEY"),
)
def test_enabled_v20_rejects_short_api_keys(name: str, tmp_path, monkeypatch) -> None:
    root, raw = _isolated_project(tmp_path)
    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    monkeypatch.setenv(name, "short")

    with pytest.raises(V20ConfigError, match=f"explicit {name}"):
        load_v20_runtime_config(root)


def test_enabled_v20_rejects_require_sslmode_and_shared_database_principal(
    tmp_path, monkeypatch
) -> None:
    root, raw = _isolated_project(tmp_path)
    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    monkeypatch.setenv("DB_SSLMODE", "require")

    with pytest.raises(V20ConfigError, match="DB_SSLMODE must be verify-full"):
        load_v20_runtime_config(root)

    _configure_reviewed_shadow(root, raw, tmp_path, monkeypatch)
    monkeypatch.setenv("DB_USER", "v20_writer")
    with pytest.raises(V20ConfigError, match="different principals"):
        load_v20_runtime_config(root)
