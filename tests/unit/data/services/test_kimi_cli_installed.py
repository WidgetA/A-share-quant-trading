# === MODULE PURPOSE ===
# CI GATE: path B (server-side listing verification) depends on the `kimi`
# binary being present in the deploy image. kimi-cli requires Python >=3.13,
# which is why the whole service was migrated to 3.13. This test fails CI if
# kimi-cli is not installed/runnable — so a build where path B is
# dead-on-arrival can never pass CI (and never deploy).
#
# If you ever drop kimi-cli as a dependency, delete this test AND the
# server-side path-B scheduler in the same change — don't just silence it.

from __future__ import annotations

import subprocess

from src.data.services.kimi_listing_verifier import kimi_available


def test_kimi_cli_on_path():
    """kimi-cli must be installed (its console script on PATH)."""
    assert kimi_available(), (
        "kimi-cli not on PATH — path B would be dead-on-arrival in the image. "
        "It's a declared dependency (pyproject), so `uv sync` should install it; "
        "if this fails, the dependency or the Python version (needs >=3.13) is wrong."
    )


def test_kimi_cli_runs():
    """The kimi binary must actually execute (catches broken install / missing
    system lib), not merely exist on PATH."""
    proc = subprocess.run(
        ["kimi", "--version"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, (
        f"`kimi --version` failed (rc={proc.returncode}): "
        f"{proc.stderr.strip() or proc.stdout.strip()}"
    )
