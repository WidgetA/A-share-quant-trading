# === MODULE PURPOSE ===
# Offline backfill driver: use kimi-cli (model per kimi_config, Kimi 3) as an independent
# oracle to fetch the real list_date of every A-share code listed in
# data/audit/diff_codes.txt. The result is written one JSON per code
# under data/audit/list_dates/. Resume-safe: existing files are skipped.
#
# The actual kimi invocation + JSON parsing lives in the shared module
# src/data/services/kimi_listing_verifier.py (also used by the server-side
# auto-verify scheduler). This script is just the offline file-IO shell.

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from src.data.services.kimi_listing_verifier import KIMI_VERIFY_TIMEOUT_SEC, run_kimi_for_code

OUT_DIR = Path("data/audit/list_dates")
CODES_FILE = Path("data/audit/diff_codes.txt")
RAW_DIR = OUT_DIR.parent / "list_dates_raw"


async def _run_kimi_for_code(code: str, timeout_sec: int = KIMI_VERIFY_TIMEOUT_SEC) -> dict | None:
    """Thin wrapper: delegate to the shared verifier, saving raw stdout
    under data/audit/list_dates_raw/ for offline diagnosis."""
    return await run_kimi_for_code(code, timeout_sec=timeout_sec, raw_dir=RAW_DIR)


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
