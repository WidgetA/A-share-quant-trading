# === MODULE PURPOSE ===
# Use kimi-cli (Kimi K2.6) as an independent oracle to fetch the real
# list_date of every A-share code that appeared in (B - D - S) across
# 2023-01-03..today. The result is written one JSON per code under
# data/audit/list_dates/. Resume-safe: existing files are skipped.
#
# This is the "全量 kimi 验证" path: every code gets its own SearchWeb
# call, no reliance on Tushare for the list_date itself. Later we cross
# the result with each day's diff to decide whether (B - D - S) is
# always "stocks not yet listed on that day".

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path

OUT_DIR = Path("data/audit/list_dates")
CODES_FILE = Path("data/audit/diff_codes.txt")

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

# kimi prints `text='...'` inside a `TextPart(...)` block. We extract that.
TEXT_PART_RE = re.compile(r"TextPart\(\s*type='text',\s*text='(.+?)'(?:,|\s*\))", re.DOTALL)
# Fallback: find a JSON object containing our `code` field.
JSON_OBJ_RE = re.compile(r'\{[^{}]*?"code"\s*:\s*"\d{6}"[^{}]*?\}', re.DOTALL)


async def _run_kimi_for_code(code: str, timeout_sec: int = 180) -> dict | None:
    """Invoke kimi-cli print mode for one code, parse the JSON it returns.

    Returns None on timeout / parse failure (caller decides whether to retry).
    """
    import os

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

    # Save raw stdout for offline diagnosis on parse failure.
    raw_dir = OUT_DIR.parent / "list_dates_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / f"{code}.txt").write_text(text, encoding="utf-8")

    # Strategy: scan the raw output for any JSON object that contains
    # `"code":"<6 digits>"` and either a `"list_date"` or `"error"` key.
    # We DON'T try to extract via the TextPart regex first — kimi's repr-
    # style output can wrap the JSON across newlines or include single
    # quotes that confuse the outer extractor. Instead we look directly.
    candidates = []
    for m in re.finditer(r'"code"\s*:\s*"(\d{6})"', text):
        # Walk left to find `{`, walk right to find matching `}` (simple,
        # since the inner JSON has no nested objects).
        start = text.rfind("{", 0, m.start())
        end = text.find("}", m.end())
        if start == -1 or end == -1:
            continue
        snippet = text[start : end + 1]
        # On Windows the kimi --print TextPart wraps long Chinese text
        # across the console width, injecting raw \r\n into JSON string
        # values mid-character. Strip those raw control chars before
        # parsing — JSON spec requires \\n escape so legitimate newlines
        # in payload aren't lost.
        cleaned = re.sub(r"[\r\n\t]+", "", snippet)
        # Also try a python-repr unescape pass (rare path for some cases).
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

    # Prefer a candidate with a *real* YYYY-MM-DD list_date over one with
    # null OR a prompt-template placeholder (e.g. literal "YYYY-MM-DD").
    real_date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    for obj in candidates:
        ld = obj.get("list_date")
        if isinstance(ld, str) and real_date_re.match(ld):
            return obj
    # No real date — return the last "not found"-style candidate (skipping
    # prompt-template placeholder echoes if any).
    for obj in reversed(candidates):
        if obj.get("error") == "not found":
            return obj
    return candidates[-1]


async def process_one(code: str, sem: asyncio.Semaphore, retries: int = 2) -> tuple[str, str]:
    """Process one code with retry. Returns (status, reason) for the log."""
    out_file = OUT_DIR / f"{code}.json"
    if out_file.exists():
        return "skip", "已有结果"

    async with sem:
        for attempt in range(1, retries + 2):
            try:
                result = await _run_kimi_for_code(code)
            except Exception as e:
                result = None
                err = f"exception: {type(e).__name__}: {e}"
            else:
                err = "parse failed"

            if result is not None:
                out_file.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                return "ok", f"list_date={result.get('list_date')}"

            if attempt <= retries:
                await asyncio.sleep(3)  # brief backoff
                continue

        # final failure — record a placeholder so we don't re-run forever
        out_file.write_text(
            json.dumps(
                {"code": code, "error": err, "_unverified": True},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return "fail", err


async def main(concurrency: int) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    codes = [c.strip() for c in CODES_FILE.read_text(encoding="utf-8").splitlines() if c.strip()]
    print(f"待验证: {len(codes)} 只股票, 并发 = {concurrency}", file=sys.stderr, flush=True)

    sem = asyncio.Semaphore(concurrency)
    counters = {"ok": 0, "skip": 0, "fail": 0}
    start_ts = time.monotonic()

    async def _worker(idx: int, code: str) -> None:
        status, reason = await process_one(code, sem)
        counters[status] += 1
        elapsed = time.monotonic() - start_ts
        done = counters["ok"] + counters["skip"] + counters["fail"]
        rate = done / elapsed if elapsed > 0 else 0
        eta = (len(codes) - done) / rate if rate > 0 else 0
        tag = {"ok": "✓", "skip": "−", "fail": "✗"}[status]
        print(
            f"[{done}/{len(codes)}] {tag} {code}: {reason} "
            f"(elapsed {elapsed:.0f}s, eta {eta:.0f}s)",
            file=sys.stderr,
            flush=True,
        )

    await asyncio.gather(*[_worker(i, c) for i, c in enumerate(codes, 1)])

    print(
        f"\n完成: ok={counters['ok']} skip={counters['skip']} fail={counters['fail']}",
        file=sys.stderr,
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    asyncio.run(main(args.concurrency))
