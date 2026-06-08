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
import tempfile
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

# Result-file channel: kimi writes its FINAL answer JSON to a file via its Shell tool,
# and we read THAT file instead of scraping JSON out of the noisy --print trace. The trace
# also contains the prompt's own example JSON (with the code substituted in), which is what
# made the stdout parser misread 高特电子=未上市 as "查不到". The file holds only kimi's real
# answer → no echo to confuse. ``{path}`` is filled per-call (a unique temp path).
_RESULT_FILE_INSTRUCTION = (
    "\n\n**最后一步(必须做,不能省略)**:把你上面那一行最终答案 JSON,用 Shell **原样写入文件** "
    "`{path}` —— 只写这一个 JSON 对象、UTF-8 编码,**不要 markdown 代码块包裹、不要任何多余文字**。"
    "例如:\n"
    "cat > {path} <<'KIMI_JSON_EOF'\n"
    "{{你的最终答案 JSON}}\n"
    "KIMI_JSON_EOF\n"
    "写完用 `cat {path}` 确认文件里是一个合法 JSON 对象。"
)


def _read_result_file(path: str, code: str) -> dict | None:
    """Read kimi's answer from the file it was told to write. Returns the parsed dict,
    or None if the file is missing / empty / not parseable for ``code``. Reuses
    ``parse_kimi_output`` (the file should hold only the answer, so there is no
    prompt-echo to confuse it)."""
    try:
        content = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    if not content.strip():
        return None
    return parse_kimi_output(content, code)


_REAL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# NOTE: there is deliberately NO loose "auth-words anywhere in the trace" check.
# A successful kimi run is a 50–100K-char web-search trace that routinely *mentions*
# 登录 / 凭证 / 401 / 403 (page content + URLs like cfi.cn/p2026...000403.html). Substring-
# matching those over the whole trace cried "请检查 kimi 凭证" on runs where kimi actually
# authenticated and answered correctly. Real API auth rejections are detected by the
# precise response-format phrases in `_AUTH_FAIL_PHRASES` only. 报什么错就是什么错。

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

# kimi *API* auth-rejection phrases — RESPONSE-format strings specific enough not to match
# a 6-digit code, a date, a URL, or financial page content (so they won't false-fire on a
# successful search trace). Only THESE mean "the token was rejected → check KIMI_API_KEY".
# Deliberately NOT here: bare "401"/"403" (appear in URLs/numbers), "unauthorized" (appears
# in article text), "登录"/"凭证" (every Chinese finance site), "stepinterrupted" (a turn
# interruption, NOT auth — it falls through to an honest "解析失败"/"退出码" instead).
_AUTH_FAIL_PHRASES = (
    "invalid authentication",
    "invalid_authentication",
    "authenticationerror",
    "invalid_api_key",  # OpenAI-compatible error code (Moonshot/Kimi API is OpenAI-shaped)
    "incorrect api key",  # OpenAI message "Incorrect API key provided"
    "error code: 401",  # openai-python client format ("Error code: 401 - ...")
    "error code: 403",
)

# A successful kimi run is a tens-of-KB search trace. If kimi produced almost nothing, it
# barely ran — most likely a startup/auth/config failure that printed an error and exited,
# NOT a search-then-parse problem. We flag those for the operator to read the saved raw
# trace, instead of guessing more auth substrings (build observability, don't guess).
_SHORT_OUTPUT_CHARS = 2000


def _looks_like_api_auth_failure(text: str) -> bool:
    """True ONLY on a kimi API auth rejection (precise response-format phrases). A 100K
    search trace that merely mentions 登录/凭证/401/403 is NOT auth — that loose match is
    exactly what produced false "请检查 kimi 凭证" alarms on successful runs. Bare "401
    Unauthorized"/"403 Forbidden" are deliberately NOT here: they also appear when a *fetched
    page* returns 403, which is not a kimi-token problem — the short-output heuristic in
    _classify_unparseable covers a real auth death without that false-positive."""
    low = text.lower()
    return any(p in low for p in _AUTH_FAIL_PHRASES)


def _classify_unparseable(text: str, returncode: int | None) -> str:
    """kimi ran (not an API auth rejection, not 'no LLM') but we got no usable answer.
    Report what it ACTUALLY is — never guess 'credentials'. 是什么错就报什么错。"""
    stripped = text.strip()
    if not stripped:
        return "kimi 无任何输出"
    if returncode not in (0, None):
        return f"kimi 退出码 {returncode}"
    if len(stripped) < _SHORT_OUTPUT_CHARS:
        # barely ran → likely startup/auth/config failure, not a search-then-parse miss
        return "kimi 几乎没输出就结束(疑似启动/认证/配置类失败,请看原始 trace)"
    return "kimi 跑完但没给出可识别的答案(输出解析失败)"


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
    # No real date: kimi's ACTUAL answer is the LAST JSON it emits (in a ```json fence
    # at the end). Any earlier "code" JSON is the prompt's echoed example
    # (name=null / status=查不到 / error="not found"). A correct answer can legitimately
    # have list_date=null — 未上市 (new IPO not trading yet) / 迁移新代码 / 已退市 — so we
    # must NOT prefer the prompt's "error: not found" example over it. (That bug turned
    # 高特电子=未上市 into a false "查不到/名字未知".) Prefer the last INFORMATIVE answer
    # (real company name, or a concrete status other than 查不到); only if none is
    # informative does kimi genuinely not know → fall back to its last word.
    for obj in reversed(candidates):
        name = obj.get("name")
        status = obj.get("status")
        has_name = isinstance(name, str) and bool(name.strip())
        real_status = (
            isinstance(status, str) and bool(status.strip()) and status.strip() != "查不到"
        )
        if has_name or real_status:
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


# Deep web-search + thinking on a hard code (新股待挂牌 / IPO 叫停 / 北交所迁号) routinely
# runs many minutes and a 50–100K-char trace. Measured live: successful 北交所-migration verifies
# took ~6min; the slowest still exceeded 600s and got cut off MID-SEARCH (timed out right before
# writing its already-correct answer → false tool-error). 1200s gives the slow ones headroom so
# the answer lands and the code leaves the queue instead of re-burning every night. The nightly
# ② caps its batch to _STEP_TIMEOUT_KIMI*0.8//this (≈14 codes), so a backlog drains over nights.
KIMI_VERIFY_TIMEOUT_SEC = 1200


async def run_kimi_for_code(
    code: str,
    timeout_sec: int = KIMI_VERIFY_TIMEOUT_SEC,
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
    # Result-file channel (primary): tell kimi to write its FINAL answer JSON to this temp
    # file via its Shell tool; we read the FILE rather than scraping the --print trace
    # (which also carries the prompt's own example JSON — the cause of the 高特电子 misparse).
    fd, result_path = tempfile.mkstemp(prefix=f"kimi_{code}_", suffix=".json")
    os.close(fd)
    prompt += _RESULT_FILE_INSTRUCTION.format(path=result_path)
    # Force UTF-8 stdio so kimi can print Chinese / replacement chars on
    # Windows without hitting the GBK console codec (which silently kills
    # the process mid-print and truncates --print output).
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    try:
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
        # kimi never ran the model/tools and just echoed the prompt. This is a TOOL
        # ERROR (kimi couldn't run), never a real "not found" — raise loudly so we
        # never write a bogus placeholder.
        low_all = text.lower()
        if any(m in low_all for m in _NO_LLM_MARKERS):
            raise KimiToolError(
                f"kimi 未配置模型 (LLM not set) — code={code};"
                " 容器缺 ~/.kimi/config.toml(只上传了凭证,没带模型/provider 配置)"
            )
        # API auth rejection: ONLY the precise response-format phrases (not bare 401/登录/凭证
        # that appear in any search trace). This is the sole path that says "check the token".
        if _looks_like_api_auth_failure(text):
            snippet = " ".join(text.split())[-200:]
            raise KimiToolError(
                f"kimi API 认证被拒(token 无效/过期,查 KIMI_API_KEY)— code={code}; 尾: {snippet!r}"
            )

        # PRIMARY: read kimi's answer from the file it was told to write — unambiguous,
        # no prompt-echo to misparse. Fall back to scraping stdout only if no file.
        file_answer = _read_result_file(result_path, code)
        if file_answer is not None:
            return file_answer

        parsed = parse_kimi_output(text, code)
        if parsed is not None:
            # kimi answered in stdout but didn't write the file — still usable.
            return parsed

        # kimi ran but produced no usable answer and it's NOT an API auth rejection.
        # Report the REAL reason (timeout already raised above; here it's empty/exit/parse).
        # NEVER guess "凭证" — that loose全文匹配 was the false-alarm bug. 是什么错就报什么错。
        snippet = " ".join(text.split())[:200]
        reason = _classify_unparseable(text, proc.returncode)
        raise KimiToolError(f"{reason} — code={code}; 输出片段: {snippet!r}")
    finally:
        try:
            os.unlink(result_path)
        except OSError:
            pass
