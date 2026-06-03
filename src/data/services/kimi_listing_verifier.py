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
    "查清楚 A 股股票代码 {code} 到底是什么、现在是什么情况。"
    "请实证查证、绝不凭印象,用你能用的**全部**工具:先 SearchWeb 搜索;"
    "**若搜索失败或无果,改用 FetchURL 抓取财经页面**(东方财富个股资料页、新浪财经、"
    "同花顺、雪球、巨潮资讯、北交所/交易所公告等);也可用 Shell(如 curl)取数据。"
    "**一个工具/来源失败就换下一个,多试几个再下结论,不要因为某个工具不可用就放弃。**\n"
    "需要查清这几点:\n"
    "① 公司中文名;\n"
    "② 首次上市日(IPO 首日);\n"
    "③ 现在的状态:仍在交易 / 已退市 / 老代码已迁到新代码"
    "(北交所 8、4 开头常迁到 92 开头)/ 更名换号;\n"
    "④ 若已退市或迁移,退市日 / 迁移日(知道就填);\n"
    "⑤ 若是迁号/更名换号,**新代码是多少**(6 位数字;只是更名但代码没变就填 null)。\n"
    "先简要列出你找到的信息,再在最后一行单独输出一行 JSON:\n"
    '{{"code":"{code}","name":"<公司中文名,如不知填 null>","list_date":"YYYY-MM-DD 或 null",'
    '"delist_date":"YYYY-MM-DD 或 null(仅已退市/迁移时填)",'
    '"status":"<在交易|已退市|迁移新代码|更名|查不到>",'
    '"new_code":"<迁号/更名换号后的新代码 6 位,否则 null>",'
    '"note":"<一句中文话说清这个代码现在到底怎么回事,给人看的,别写代号>",'
    '"source":"<URL>"}}\n'
    "只有确实多方查证都查不到时,status 填 查不到、其余可为 null(绝不编造日期):\n"
    '{{"code":"{code}","name":null,"list_date":null,"delist_date":null,"status":"查不到",'
    '"new_code":null,"note":"多方查证仍无结果","source":null,"error":"not found"}}'
)

_REAL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Markers that mean kimi-cli itself failed (not authenticated / not working),
# as opposed to "kimi searched and found nothing". If these appear we must
# raise loudly, never silently treat the stock as "查不到".
_TOOL_ERROR_MARKERS = (
    "login",
    "log in",
    "unauthorized",
    "未登录",
    "登录",
    "凭证",
    "credential",
    "access token",
    "refresh token",
    "401",
    "403",
    "authenticat",
    "api key",
    "quota",
    "rate limit",
)

# Output markers meaning kimi never ran the model in this (headless) invocation —
# no default model/provider configured (~/.kimi/config.toml missing). Distinct
# from auth errors; must raise (never a "查不到").
_NO_LLM_MARKERS = (
    "llm not set",
    "llm not configured",
    "no llm configured",
    "model not set",
    "no model configured",
    "未设置模型",
    "未配置模型",
)

# Auth/runtime failure phrases — SPECIFIC enough not to match a 6-digit code or
# a YYYY-MM-DD date (so safe to check before parsing). "Invalid Authentication"
# = the 401 seen when the 15-min OAuth token is expired and refresh failed.
_AUTH_FAIL_PHRASES = (
    "invalid authentication",
    "invalid_authentication",
    "unauthorized",
    "error code: 401",
    "error code: 403",
    "stepinterrupted",
)


class KimiToolError(RuntimeError):
    """kimi-cli itself failed (timeout / nonzero exit / no auth / unparseable
    output) — distinct from a valid 'searched but not found' answer. Callers
    MUST surface this as an error, never write a 'not found' placeholder."""


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


def finding_from_result(code: str, result: dict) -> dict:
    """Normalize kimi's parsed answer into a plain-Chinese "怎么回事" record.

    This is what flows into the report the user reads — so it carries the
    human one-liner (``note``) + status, not just the machine dates. Pure
    function (no I/O) so it's unit-testable.
    """
    status = result.get("status")
    if not isinstance(status, str) or not status.strip():
        # Old-style answers (no status field) — infer a coarse one from dates.
        ld = result.get("list_date")
        if result.get("error") == "not found" or not ld:
            status = "查不到"
        elif result.get("delist_date"):
            status = "已退市"
        else:
            status = "在交易"
    note = result.get("note")
    if not isinstance(note, str) or not note.strip():
        note = "(kimi 未给出说明)"
    # new_code only when it's a real 6-digit code change (迁号/更名换号).
    nc = result.get("new_code")
    new_code = (
        nc if (isinstance(nc, str) and len(nc) == 6 and nc.isdigit() and nc != code) else None
    )
    return {
        "code": code,
        "name": result.get("name"),
        "status": status.strip(),
        "note": note.strip(),
        "list_date": result.get("list_date"),
        "delist_date": result.get("delist_date"),
        "new_code": new_code,
        "source": result.get("source"),
    }


def kimi_available() -> bool:
    """Whether the ``kimi`` CLI binary is on PATH (container deploy concern)."""
    return shutil.which("kimi") is not None


async def run_kimi_for_code(
    code: str,
    timeout_sec: int = 180,
    raw_dir: Path | None = None,
) -> dict:
    """Invoke kimi-cli print mode for one code, parse the JSON it returns.

    Returns the parsed dict on a VALID answer — either a found date
    (``{code, name, list_date, source}``) or kimi's explicit "searched but
    not found" (``{code, ..., error: "not found"}``).

    Raises ``KimiToolError`` when kimi-cli itself failed to produce a usable
    answer (timeout / nonzero exit / empty output / auth-error markers /
    unparseable). This is NOT "查不到" — it means the tool is broken/
    unauthenticated, and the caller must surface it loudly, never write a
    "not found" placeholder.
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
        # NOTE: thinking left ON. With --no-thinking, when SearchWeb fails kimi
        # gives up ("查不到") instead of reasoning its way to a FetchURL/Shell
        # fallback (which works — verified manually for 920039). Slower but it
        # actually finds answers.
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
        raise KimiToolError(f"kimi 超时 (>{timeout_sec}s) — code={code}") from None

    text = out_bytes.decode("utf-8", errors="replace")

    if raw_dir is not None:
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / f"{code}.txt").write_text(text, encoding="utf-8")

    # "LLM not set": the headless --print run had no model configured (no
    # ~/.kimi/config.toml in the container — only credentials were uploaded), so
    # kimi never ran the model/tools and just echoed the prompt. The prompt
    # itself contains an example {"...","error":"not found"} JSON, which the
    # parser below would otherwise FALSELY return as a "查不到". This is a TOOL
    # ERROR (kimi couldn't run), never a real "not found" — raise loudly so we
    # never write a bogus placeholder.
    low_all = text.lower()
    if any(m in low_all for m in _NO_LLM_MARKERS):
        raise KimiToolError(
            f"kimi 未配置模型 (LLM not set) — code={code};"
            " 容器缺 ~/.kimi/config.toml(只上传了凭证,没带模型/provider 配置)"
        )
    # Auth/runtime failure: kimi started but the API rejected the token (expired
    # 15-min OAuth, refresh failed — e.g. missing device_id) or the step was
    # interrupted. Specific phrases (won't match a 6-digit code or a date).
    # Catch BEFORE parsing — else parse_kimi_output matches the prompt-echo's
    # example "not found" JSON and falsely reports 查不到.
    if any(m in low_all for m in _AUTH_FAIL_PHRASES):
        snippet = " ".join(text.split())[-200:]
        raise KimiToolError(f"kimi 认证/运行失败(API 拒绝)— code={code}; 输出尾: {snippet!r}")

    parsed = parse_kimi_output(text, code)
    if parsed is not None:
        # kimi gave a usable answer (found date OR explicit "not found").
        return parsed

    # No parseable answer → the tool itself failed. Distinguish auth/tool
    # failure (the common case when credentials are missing) so the operator
    # gets a real reason instead of a fake "查不到".
    rc = proc.returncode
    snippet = " ".join(text.split())[:200]
    low = text.lower()
    if any(mark in low for mark in _TOOL_ERROR_MARKERS):
        reason = "kimi 未授权/登录或凭证问题"
    elif not text.strip():
        reason = "kimi 无任何输出"
    elif rc not in (0, None):
        reason = f"kimi 退出码 {rc}"
    else:
        reason = "kimi 输出无法解析(非有效答案)"
    raise KimiToolError(f"{reason} — code={code}; 输出片段: {snippet!r}")
