# === MODULE PURPOSE ===
# AST-001 Phase 2 — read-only SQL gate for the Feishu assistant's free-form
# questions. kimi queries GreptimeDB ONLY through this validator + the
# /api/trading/assistant-sql endpoint; the validation is server-side and
# mandatory, so a prompt-injected kimi cannot write/drop through this path
# (its trading isolation is the read-only key; this is the data equivalent).
#
# Rules enforced here (pure function, unit-tested):
#   - single statement, SELECT / SHOW / DESCRIBE / DESC / WITH only
#   - no write/DDL keywords anywhere (whole-word match)
#   - a LIMIT is forced on SELECT/WITH (appended when absent, capped when huge)
#   - bounded statement length

import re

MAX_SQL_LEN = 4000
DEFAULT_LIMIT = 200
MAX_LIMIT = 2000

_ALLOWED_FIRST = ("select", "show", "describe", "desc", "with")
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|create|alter|truncate|grant|revoke|set|copy|merge)\b",
    re.IGNORECASE,
)
_LIMIT_RE = re.compile(r"\blimit\s+(\d+)", re.IGNORECASE)


def validate_readonly_sql(sql: str) -> str:
    """Return a normalized, guaranteed-read-only statement or raise ValueError
    with a plain-Chinese reason (surfaced verbatim to kimi so it can adjust)."""
    text = (sql or "").strip()
    if not text:
        raise ValueError("SQL 为空")
    if len(text) > MAX_SQL_LEN:
        raise ValueError(f"SQL 太长(上限 {MAX_SQL_LEN} 字符)")

    text = text.rstrip(";").strip()
    if ";" in text:
        raise ValueError("只允许单条语句(不接受分号分隔的多条 SQL)")

    first = text.split(None, 1)[0].lower()
    if first not in _ALLOWED_FIRST:
        raise ValueError("只允许只读查询:SELECT / SHOW / DESCRIBE / WITH 开头")

    if _FORBIDDEN.search(text):
        raise ValueError("语句里出现了写操作关键词,只读端点拒绝执行;请改写查询")

    if first in ("select", "with"):
        m = _LIMIT_RE.search(text)
        if m is None:
            text = f"{text} LIMIT {DEFAULT_LIMIT}"
        elif int(m.group(1)) > MAX_LIMIT:
            raise ValueError(f"LIMIT 太大(上限 {MAX_LIMIT} 行),分批查")
    return text


def rows_to_jsonable(records: list) -> list[dict]:
    """asyncpg Records → JSON-safe dicts (non-primitive values stringified)."""
    out: list[dict] = []
    for r in records:
        d = dict(r)
        out.append(
            {
                k: (v if isinstance(v, (int, float, str, bool)) or v is None else str(v))
                for k, v in d.items()
            }
        )
    return out
