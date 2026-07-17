# === MODULE PURPOSE ===
# Generate kimi-cli's ~/.kimi/config.toml at container startup from a static
# Kimi-Code API key (env KIMI_API_KEY) — path B's authentication.
#
# WHY a static key instead of OAuth (2026-06):
#   The old deploy uploaded OAuth credentials. kimi's OAuth access_token lives
#   only ~15 minutes and refresh is device-bound; an unattended 4am job would
#   work until the refresh_token rotated/expired, then silently fail auth with
#   no way to re-login headlessly. A Kimi-Code API key (sk-kimi-..., authorizing
#   api.kimi.com/coding/v1) never expires and needs no interactive login.
#
# Source facts this relies on (kimi-cli, verified):
#   - A provider with type="kimi" and NO `oauth` field uses its plaintext
#     `api_key` directly (kimi_cli/llm.py create_llm / resolve_api_key).
#   - The SAME key also authorizes the coding search/fetch endpoints, so
#     [services.moonshot_search]/[services.moonshot_fetch] with that key keep
#     native SearchWeb/FetchURL working (verified end-to-end on 920039).
#
# The key is NEVER committed or baked into the image — it arrives only via the
# KIMI_API_KEY env var (set in the gitignored, user-managed docker-compose.yml).

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.kimi.com/coding/v1"

# Known models on the kimi-code coding endpoint (specs mirror kimi-cli's own
# managed config). default = k3 (Kimi 3, 1M context). Override with the
# KIMI_MODEL env var to roll back to an older model WITHOUT rebuilding the
# image (e.g. KIMI_MODEL=kimi-for-coding in docker-compose.yml).
DEFAULT_MODEL = "k3"
_MODEL_SPECS = {
    "k3": {"max_context_size": 1048576, "display_name": "K3"},
    "kimi-for-coding": {"max_context_size": 262144, "display_name": "Kimi-k2.6"},
    "kimi-for-coding-highspeed": {
        "max_context_size": 262144,
        "display_name": "Kimi-k2.6 Highspeed",
    },
}


def kimi_share_dir() -> Path:
    """Where kimi-cli reads config/credentials — mirrors kimi's own logic
    (``$KIMI_SHARE_DIR`` or ``~/.kimi``)."""
    override = os.environ.get("KIMI_SHARE_DIR")
    return Path(override) if override else Path.home() / ".kimi"


def build_kimi_config_toml(
    api_key: str, base_url: str = DEFAULT_BASE_URL, model: str = DEFAULT_MODEL
) -> str:
    """Build a kimi-cli config.toml that authenticates with a STATIC api_key
    (no ``oauth`` block) and wires native search/fetch with the same key.

    Pure function (no I/O) so it's unit-testable. Raises ValueError if the
    key/base_url contain characters that would break the TOML string (a sign
    of a malformed value, not something to silently embed), or if ``model``
    is not one of the known coding-endpoint models (fail fast rather than
    ship a config kimi can't run)."""
    for label, value in (("KIMI_API_KEY", api_key), ("base_url", base_url)):
        if not value or any(c in value for c in ('"', "\\", "\n", "\r")):
            raise ValueError(f"{label} 为空或含非法字符,拒绝写入 kimi 配置")
    if model not in _MODEL_SPECS:
        raise ValueError(
            f"未知 kimi 模型 {model!r},可选: {sorted(_MODEL_SPECS)} — 拒绝写入 kimi 配置"
        )
    search_url = f"{base_url}/search"
    fetch_url = f"{base_url}/fetch"
    model_blocks = "\n".join(
        f'''[models."kimi-code/{name}"]
provider = "managed:kimi-code"
model = "{name}"
max_context_size = {spec["max_context_size"]}
capabilities = ["video_in", "image_in", "thinking"]
display_name = "{spec["display_name"]}"
'''
        for name, spec in _MODEL_SPECS.items()
    )
    # NOTE: provider has NO [providers."managed:kimi-code".oauth] block — that
    # absence is what makes kimi use the plaintext api_key. Do not add one.
    return f'''# AUTO-GENERATED at startup by kimi_config.ensure_kimi_config_from_env().
# Do NOT edit by hand and do NOT commit a key here — the api_key comes from the
# KIMI_API_KEY env var at runtime. See src/data/services/kimi_config.py.
default_model = "kimi-code/{model}"
default_thinking = true
default_yolo = false
skip_afk_prompt_injection = false
theme = "dark"
merge_all_available_skills = true
telemetry = false

{model_blocks}
[providers."managed:kimi-code"]
type = "kimi"
base_url = "{base_url}"
api_key = "{api_key}"

[services.moonshot_search]
base_url = "{search_url}"
api_key = "{api_key}"

[services.moonshot_fetch]
base_url = "{fetch_url}"
api_key = "{api_key}"

[loop_control]
max_steps_per_turn = 100
max_retries_per_step = 3
reserved_context_size = 50000
compaction_trigger_ratio = 0.85
'''


def ensure_kimi_config_from_env() -> bool:
    """Write ``<share_dir>/config.toml`` from the KIMI_API_KEY env var.

    Returns True if a config was written (key present), False if KIMI_API_KEY
    is unset/blank (caller should then NOT start path B and should alert). Never
    logs the key. Overwrites on every startup so a key rotation takes effect on
    restart.
    """
    api_key = (os.environ.get("KIMI_API_KEY") or "").strip()
    if not api_key:
        logger.warning("KIMI_API_KEY 未设置 — 不生成 kimi 配置(path B 无法认证)")
        return False
    base_url = (os.environ.get("KIMI_CODE_BASE_URL") or DEFAULT_BASE_URL).strip()
    model = (os.environ.get("KIMI_MODEL") or DEFAULT_MODEL).strip()
    try:
        content = build_kimi_config_toml(api_key, base_url, model)
    except ValueError as e:
        logger.error("生成 kimi 配置失败: %s", e)
        return False
    dest = kimi_share_dir() / "config.toml"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(content, encoding="utf-8")
    logger.info(
        "已生成 kimi 配置 %s (认证用静态 API key,base_url=%s,模型=%s)", dest, base_url, model
    )
    return True
