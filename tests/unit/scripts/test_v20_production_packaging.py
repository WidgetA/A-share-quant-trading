from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripts.main import _require_v20_host_when_enabled

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_docker_build_is_lockfile_strict_and_contains_v20_runtime_inputs() -> None:
    dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")

    python_image = (
        "python:3.11-slim-trixie@sha256:"
        "1042b61448fef4ba92d16a8c7eb4996d027568ce64792a7877fd88511e0af7c6"
    )
    assert dockerfile.count(f"FROM {python_image}") == 2
    assert "ghcr.io/astral-sh/uv:0.9.27@sha256:" in dockerfile
    assert "COPY pyproject.toml uv.lock README.md ./" in dockerfile
    assert "RUN uv sync --frozen --no-dev" in dockerfile
    assert "|| uv sync" not in dockerfile
    assert dockerfile.count("COPY pyproject.toml uv.lock ./") >= 1
    assert (
        "COPY data/sectors.json data/board_constituents.json "
        "data/board_relevance_cache.json ./bundled_data/"
    ) in dockerfile
    for copy_instruction in (
        "COPY scripts/ ./scripts/",
        "COPY config/ ./config/",
        "COPY docs/strategy-v20-artifacts/ ./docs/strategy-v20-artifacts/",
        "COPY migrations/ ./migrations/",
        "COPY models/ ./models/",
    ):
        assert copy_instruction in dockerfile
    assert "FROM common-runtime AS v20" in dockerfile
    assert 'CMD ["python", "scripts/v20_main.py"]' in dockerfile
    assert "FROM common-runtime AS runtime" in dockerfile
    assert dockerfile.rstrip().endswith('CMD ["python", "scripts/main.py"]')


def test_docker_context_excludes_local_secrets_but_keeps_checkpoint_directory() -> None:
    patterns = (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()

    assert ".env" in patterns
    assert "config/secrets.yaml" in patterns
    assert "config/secrets*.yaml" in patterns
    assert "config/*.env" in patterns
    assert "!config/*.env.example" in patterns
    assert "*.pem" in patterns
    assert "*.key" in patterns
    assert not any(
        "v20-checkpoints" in pattern and not pattern.startswith("#") for pattern in patterns
    )


def test_checked_in_v20_defaults_are_disabled_and_shadow_isolated() -> None:
    config = yaml.safe_load((PROJECT_ROOT / "config" / "v20.yaml").read_text(encoding="utf-8"))
    env_example = (PROJECT_ROOT / "config" / "v20.env.example").read_text(encoding="utf-8")

    assert config["enabled"] is False
    assert config["deployment_mode"] == "forward_shadow"
    assert config["production_activation_guard"] is False
    assert "SHADOW" in config["official_stream_id"]
    assert "SHADOW" in config["state_lineage_id"]
    assert config["schema_version"] == "v20-runtime/v2"
    assert config["routes"]["forward_shadow"]["route_id"] == "V20_SHADOW_FEISHU"
    assert config["routes"]["production_push"]["route_id"] == "V20_FORMAL_FEISHU"
    assert config["routes"]["forward_shadow"]["expected_bot_origin"] == "UNCONFIGURED"
    assert "V20_ENABLED=false" in env_example
    assert "V20_ALLOW_PRODUCTION_PUSH=false" in env_example
    assert "V20_HOST=0.0.0.0" in env_example
    assert "V20_PORT=8000" in env_example
    assert "WEB_ENABLED=true" in env_example
    assert "V20_STATUS_API_KEY=" in env_example
    assert "DB_SSLMODE=verify-full" in env_example
    assert "V20_DB_SSLMODE=verify-full" in env_example


@pytest.mark.parametrize("enabled", ["1", "true", "YES", "on"])
def test_enabled_v20_requires_fastapi_host(monkeypatch, enabled: str) -> None:
    monkeypatch.setenv("V20_ENABLED", enabled)

    with pytest.raises(RuntimeError, match="WEB_ENABLED=true"):
        _require_v20_host_when_enabled({"enabled": False})

    _require_v20_host_when_enabled({"enabled": True})


def test_invalid_v20_enabled_flag_fails_before_startup(monkeypatch) -> None:
    monkeypatch.setenv("V20_ENABLED", "sometimes")

    with pytest.raises(ValueError, match="must be a boolean"):
        _require_v20_host_when_enabled({"enabled": True})


def test_platform_entry_rejects_formal_v20_even_when_web_is_enabled(monkeypatch) -> None:
    monkeypatch.setenv("V20_ENABLED", "true")
    monkeypatch.setenv("V20_MODE", "production_push")

    with pytest.raises(RuntimeError, match="scripts/v20_main.py"):
        _require_v20_host_when_enabled({"enabled": True})


def test_checkpoint_command_in_runbook_matches_exporter_cli() -> None:
    runbook = (PROJECT_ROOT / "docs" / "strategy-v20-runbook.md").read_text(encoding="utf-8")

    assert "python scripts/export_v20_checkpoint.py" in runbook
    for flag in (
        "--database-config",
        "--source-stream",
        "--source-lineage",
        "--target-stream",
        "--target-lineage",
        "--as-of",
        "--output",
    ):
        assert flag in runbook
    assert "V20_DATABASE_URL" not in runbook
    assert "docker build --target v20" in runbook
    assert "route_id + official_stream_id + lineage_id" in runbook
