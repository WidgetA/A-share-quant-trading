# === MODULE PURPOSE ===
# Shared kimi-cli oracle for fetching a stock's real first-trading day
# (IPO list_date). Used by BOTH the offline backfill script
# (scripts/verify_list_date_kimi.py) and the server-side auto-verify job
# (src/data/services/listing_verify_scheduler.py, "path B").
#
# kimi must SearchWeb to confirm the date — we NEVER let it guess. Parse
# failures return a "not found"-style result (or None); the caller writes
# a verified=false placeholder rather than inventing a list_date. This is
# a data-quality audit path, so an explicit "don't know" beats a wrong date.

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
from pathlib import Path

KIMI_PROMPT_TMPL = (
    "查 A 股股票代码 {code} 的真实首次挂牌交易日期 (IPO 首日)。"
    "必须用 SearchWeb 工具至少搜索一次实际网页 (新浪财经/东方财富/雪球/巨潮资讯/"
    "同花顺/钛媒体/交易所公告等),不要凭印象答。"
    "先简要列出你找到的信息,再在最后一行单独输出一行 JSON:\n"
    '{{"code":"{code}","name":"<公司中文名,如不知填 null>","list_date":"YYYY-MM-DD",'
    '"source":"<URL>"}}\n'
    "如果搜索后仍找不到该代码的实际挂牌日,输出 list_date 为 null:\n"
    '{{"code":"{code}","name":null,"list_date":null,"source":null,"error":"not found"}}'
)

_REAL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_kimi_output(text: str, code: str) -> dict | None:
    """Extract the answer JSON for ``code`` from kimi --print raw stdout.

    Pure function (no I/O) so it's unit-testable without spawning kimi.

    Strategy: scan for any JSON object containing ``"code":"<6 digits>"``.
    kimi's repr-style output wraps long Chinese text across the console
    width, injecting raw \\r\\n / \\t mid-JSON, so we strip those control
    chars before parsing. Prefer a candidate with a real YYYY-MM-DD
    list_date over a null / "not found" one (kimi sometimes echoes the
    prompt-template placeholder first, then the real answer).

    Returns the parsed dict, or None if nothing parseable for ``code``.
    """
    candidates: list[dict] = []
    for m in re.finditer(r'"code"\s*:\s*"(\d{6})"', text):
        start = text.rfind("{", 0, m.start())
        end = text.find("}", m.end())
        if start == -1 or end == -1:
            continue
        snippet = text[start : end + 1]
        cleaned = re.sub(r"[\r\n\t]+", "", snippet)
        unescaped = cleaned.replace("\\n", "\n").replace('\\"', '"')
        for cand in (cleaned, snippet, unescaped):
            try:
                obj = json.loads(cand)
            except Exception:
                continue
            if obj.get("code") == code:
                candidates.append(obj)
                break

    if not candidates:
        return None

    # Prefer a candidate with a *real* YYYY-MM-DD list_date.
    for obj in candidates:
        ld = obj.get("list_date")
        if isinstance(ld, str) and _REAL_DATE_RE.match(ld):
            return obj
    # No real date — return the last explicit "not found" candidate, else last.
    for obj in reversed(candidates):
        if obj.get("error") == "not found":
            return obj
    return candidates[-1]


def kimi_available() -> bool:
    """Whether the ``kimi`` CLI binary is on PATH (container deploy concern)."""
    return shutil.which("kimi") is not None


async def run_kimi_for_code(
    code: str,
    timeout_sec: int = 180,
    raw_dir: Path | None = None,
) -> dict | None:
    """Invoke kimi-cli print mode for one code, parse the JSON it returns.

    Args:
        code: 6-digit A-share code.
        timeout_sec: kill the subprocess after this many seconds.
        raw_dir: if given, save raw stdout to ``raw_dir/<code>.txt`` for
            offline diagnosis on parse failure.

    Returns the parsed dict (``{code, name, list_date, source}`` or a
    ``{code, ..., error: "not found"}`` shape), or None on timeout / parse
    failure (caller decides whether to retry or write a placeholder).
    """
    prompt = KIMI_PROMPT_TMPL.format(code=code)
    # Force UTF-8 stdio so kimi can print Chinese / replacement chars on
    # Windows without hitting the GBK console codec (which silently kills
    # the process mid-print and truncates --print output).
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    proc = await asyncio.create_subprocess_exec(
        "kimi",
        "--print",
        "--afk",
        "--no-thinking",  # faster + cheaper; SearchWeb still works
        "-p",
        prompt,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
    )
    try:
        out_bytes, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        return None

    text = out_bytes.decode("utf-8", errors="replace")

    if raw_dir is not None:
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / f"{code}.txt").write_text(text, encoding="utf-8")

    return parse_kimi_output(text, code)
