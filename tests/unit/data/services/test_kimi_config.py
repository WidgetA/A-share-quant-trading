# === MODULE PURPOSE ===
# Tests for kimi_config.build_kimi_config_toml — the startup generator that
# writes kimi-cli's config from a static API key (no OAuth). The critical
# invariants: it's valid TOML, the provider has NO oauth block (so kimi uses
# the plaintext key), and the same key is wired into the search/fetch services
# (so native SearchWeb/FetchURL keep working).

from __future__ import annotations

import tomllib

import pytest

from src.data.services.kimi_config import (
    DEFAULT_BASE_URL,
    build_kimi_config_toml,
    ensure_kimi_config_from_env,
)


def test_generated_config_is_valid_toml_with_key_and_no_oauth():
    toml_str = build_kimi_config_toml("sk-kimi-TESTKEY123")
    cfg = tomllib.loads(toml_str)

    provider = cfg["providers"]["managed:kimi-code"]
    assert provider["type"] == "kimi"
    assert provider["base_url"] == DEFAULT_BASE_URL
    assert provider["api_key"] == "sk-kimi-TESTKEY123"
    # The whole point: NO oauth block → kimi uses the plaintext api_key.
    assert "oauth" not in provider

    # Default model is Kimi 3 (k3, 1M context); the older kimi-for-coding
    # block is still emitted so KIMI_MODEL can roll back without a rebuild.
    model = cfg["models"]["kimi-code/k3"]
    assert model["provider"] == "managed:kimi-code"
    assert model["model"] == "k3"
    assert model["max_context_size"] == 1048576
    assert cfg["default_model"] == "kimi-code/k3"
    assert cfg["models"]["kimi-code/kimi-for-coding"]["model"] == "kimi-for-coding"


def test_model_override_and_unknown_model_rejected():
    cfg = tomllib.loads(build_kimi_config_toml("sk-kimi-X", model="kimi-for-coding"))
    assert cfg["default_model"] == "kimi-code/kimi-for-coding"
    with pytest.raises(ValueError):
        build_kimi_config_toml("sk-kimi-X", model="k9-nonexistent")


def test_ensure_respects_kimi_model_env(monkeypatch, tmp_path):
    monkeypatch.setenv("KIMI_API_KEY", "sk-kimi-FROMENV")
    monkeypatch.setenv("KIMI_SHARE_DIR", str(tmp_path))
    monkeypatch.setenv("KIMI_MODEL", "kimi-for-coding")
    assert ensure_kimi_config_from_env() is True
    cfg = tomllib.loads((tmp_path / "config.toml").read_text(encoding="utf-8"))
    assert cfg["default_model"] == "kimi-code/kimi-for-coding"
    # An unknown model must refuse to write a config (path B then alerts).
    monkeypatch.setenv("KIMI_MODEL", "bogus-model")
    (tmp_path / "config.toml").unlink()
    assert ensure_kimi_config_from_env() is False
    assert not (tmp_path / "config.toml").exists()


def test_search_and_fetch_services_share_the_key():
    # Native SearchWeb/FetchURL must be wired with the same key (verified that
    # one key authorizes both chat and the coding search/fetch endpoints).
    cfg = tomllib.loads(build_kimi_config_toml("sk-kimi-ABC"))
    search = cfg["services"]["moonshot_search"]
    fetch = cfg["services"]["moonshot_fetch"]
    assert search["base_url"] == DEFAULT_BASE_URL + "/search"
    assert fetch["base_url"] == DEFAULT_BASE_URL + "/fetch"
    assert search["api_key"] == "sk-kimi-ABC"
    assert fetch["api_key"] == "sk-kimi-ABC"
    assert "oauth" not in search
    assert "oauth" not in fetch


def test_custom_base_url():
    cfg = tomllib.loads(build_kimi_config_toml("sk-kimi-X", "https://api.moonshot.cn/v1"))
    assert cfg["providers"]["managed:kimi-code"]["base_url"] == "https://api.moonshot.cn/v1"
    assert cfg["services"]["moonshot_search"]["base_url"] == "https://api.moonshot.cn/v1/search"


def test_rejects_key_with_toml_breaking_chars():
    # A key containing a quote/backslash/newline would break the TOML string —
    # that's a malformed value, raise rather than silently corrupt the config.
    for bad in ['sk-"injection', "sk-back\\slash", "sk-new\nline", ""]:
        with pytest.raises(ValueError):
            build_kimi_config_toml(bad)


def test_ensure_returns_false_when_no_key(monkeypatch, tmp_path):
    monkeypatch.delenv("KIMI_API_KEY", raising=False)
    monkeypatch.setenv("KIMI_SHARE_DIR", str(tmp_path))
    assert ensure_kimi_config_from_env() is False
    assert not (tmp_path / "config.toml").exists()


def test_ensure_writes_config_when_key_present(monkeypatch, tmp_path):
    monkeypatch.setenv("KIMI_API_KEY", "sk-kimi-FROMENV")
    monkeypatch.setenv("KIMI_SHARE_DIR", str(tmp_path))
    assert ensure_kimi_config_from_env() is True
    written = (tmp_path / "config.toml").read_text(encoding="utf-8")
    cfg = tomllib.loads(written)
    assert cfg["providers"]["managed:kimi-code"]["api_key"] == "sk-kimi-FROMENV"
