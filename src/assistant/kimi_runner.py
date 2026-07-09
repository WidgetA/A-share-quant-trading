# === MODULE PURPOSE ===
# Spawn kimi-cli for one assistant task (AST-001), with the repo skill library
# attached via --skills-dir. Reuses the reliability pattern hardened in path B
# (kimi_listing_verifier) and NOTE-002 (ai_journal):
#   - result comes back through a temp FILE kimi writes, never scraped from the
#     --print trace
#   - the timeout is a hang-backstop ONLY, never a failure verdict — we wait for
#     kimi's own exit and judge from its explicit output
#   - errors are classified honestly (auth vs timeout vs empty vs unparseable);
#     KimiToolError means "the tool is broken, stop the batch", not "no answer"
#
# Safety posture (AST-001): the subprocess env carries ONLY the dedicated
# read-only key (ASSISTANT_READONLY_KEY) — the trading key is never injected.
# The working directory is confined to a per-run temp dir.

import asyncio
import logging
import os
import tempfile
from pathlib import Path

from src.common.config import PROJECT_ROOT
from src.common.kimi_lock import KIMI_GLOBAL_LOCK

logger = logging.getLogger(__name__)

# Hang-backstop, same philosophy as KIMI_VERIFY_TIMEOUT_SEC (see path B notes):
# generous, and hitting it reports "kimi never exited", keeping partial output.
KIMI_ASSISTANT_TIMEOUT_SEC = int(os.environ.get("KIMI_ASSISTANT_TIMEOUT_SEC", "900"))

SKILLS_DIR = (
    Path(os.environ.get("ASSISTANT_SKILLS_DIR", ""))
    if os.environ.get("ASSISTANT_SKILLS_DIR")
    else PROJECT_ROOT / "kimi-skills"
)

_RESULT_FILE_INSTRUCTION = (
    "\n\n最后一步(必须做):把给用户的最终回复(纯文本大白话中文,不是 JSON、不带 markdown"
    " 代码块)用 Shell 写入文件 {path} (UTF-8 编码)。写完这个文件任务才算完成。"
)


def thinking_args(enable_thinking: bool = False) -> list[str]:
    """Slash-command tasks run --no-thinking (latency): skills are scripted —
    read the skill, one curl, format — no open-ended research where thinking
    earns its cost. Free-form questions pass enable_thinking=True: improvising
    (which table, which tool, fallback on failure) is exactly where thinking
    pays — same lesson as path B (with it off, kimi gives up instead of
    reasoning to a tool fallback, 920039). KIMI_ASSISTANT_THINKING=1 forces
    thinking on for everything. Read at call time, not import."""
    if enable_thinking:
        return []
    if os.environ.get("KIMI_ASSISTANT_THINKING", "").strip().lower() in ("1", "true", "yes", "on"):
        return []
    return ["--no-thinking"]


async def run_kimi_assistant_task(
    task_prompt: str,
    readonly_key: str | None,
    api_base: str,
    timeout_sec: int = KIMI_ASSISTANT_TIMEOUT_SEC,
    enable_thinking: bool = False,
) -> str:
    """Run one assistant task through kimi + skills; return the reply text.

    Raises KimiToolError when kimi itself is broken (auth / hang / no output),
    RuntimeError when kimi ran but produced no usable reply file. Callers turn
    both into honest plain-Chinese replies — never a fabricated answer.
    """
    from src.data.services.kimi_listing_verifier import (
        KimiToolError,
        _classify_unparseable,
        _looks_like_api_auth_failure,
    )

    fd, result_path = tempfile.mkstemp(prefix="assistant_reply_", suffix=".txt")
    os.close(fd)
    # Confined working directory: kimi's Shell runs relative to a scratch dir,
    # not the app root. (It can still address absolute paths — the real trading
    # guard is the key scoping, this just keeps runs tidy and reviewable.)
    work_dir = tempfile.mkdtemp(prefix="assistant_work_")
    full_prompt = task_prompt + _RESULT_FILE_INSTRUCTION.format(path=result_path)

    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    env["ASSISTANT_API_BASE"] = api_base
    if readonly_key:
        env["ASSISTANT_READONLY_KEY"] = readonly_key
    # NEVER hand the trading key to kimi — read-only key or nothing.
    env.pop("TRADING_API_KEY", None)

    try:
        # ONE kimi at a time process-wide (shared with pipeline ② + AI journal).
        async with KIMI_GLOBAL_LOCK:
            proc = await asyncio.create_subprocess_exec(
                "kimi",
                "--print",
                "--afk",
                *thinking_args(enable_thinking),
                "--skills-dir",
                str(SKILLS_DIR),
                "--work-dir",
                work_dir,
                "-p",
                full_prompt,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )

            async def _drain() -> bytes:
                assert proc.stdout is not None
                return await proc.stdout.read()

            try:
                raw = (await asyncio.wait_for(_drain(), timeout=timeout_sec)).decode(
                    "utf-8", errors="replace"
                )
                await proc.wait()
            except TimeoutError:
                proc.kill()
                raise KimiToolError(f"kimi 超过 {timeout_sec}s 未退出(卡死兜底)") from None

        if _looks_like_api_auth_failure(raw):
            raise KimiToolError("kimi API 认证被拒(精确响应短语命中),请检查 KIMI_API_KEY")

        result_file = Path(result_path)
        if result_file.exists() and result_file.stat().st_size > 0:
            reply = result_file.read_text(encoding="utf-8-sig").strip()
            if reply:
                return reply
        raise RuntimeError(
            f"kimi 跑完但没写回复文件({_classify_unparseable(raw, proc.returncode)})"
        )
    finally:
        for p in (result_path,):
            try:
                os.unlink(p)
            except OSError:
                pass
