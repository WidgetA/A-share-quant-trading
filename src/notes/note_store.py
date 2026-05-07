# === MODULE PURPOSE ===
# Per-stock trade notes stored in GreptimeDB (`trade_notes` table).
#
# Two event sources mixed in one table:
#   - source='broker' — auto-INSERT on successful place_order, structured fields
#     (price/qty/side) filled, content empty
#   - source='user' / 'ai' — manual events (思考/复盘/AI总结), content is markdown
#
# All queries use raw inlined SQL because GreptimeDB does not support PREPARE
# (asyncpg statement_cache_size=0). String values are escaped via `_q()`.

from __future__ import annotations

import calendar
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


_CREATE_TRADE_NOTES_SQL = """
CREATE TABLE IF NOT EXISTS trade_notes (
    ts          TIMESTAMP TIME INDEX,
    code        STRING,
    event_id    STRING,
    event_type  STRING,
    event_source STRING,
    title       STRING,
    price       FLOAT64,
    qty         INT64,
    side        STRING,
    content     STRING,
    author      STRING,
    deleted     BOOLEAN,
    PRIMARY KEY (code, event_id)
)
"""


# Event types surfaced in the manual-add dropdown. Other thoughts (思考/复盘/
# AI总结) belong in the 正文 of an existing event, not as separate types.
DEFAULT_EVENT_TYPES = [
    "买入",
    "卖出",
]


@dataclass
class NoteEvent:
    ts: datetime
    code: str
    event_id: str
    event_type: str
    source: str  # 'broker' | 'user' | 'ai'
    title: str
    price: float | None
    qty: int | None
    side: str | None  # 'buy' | 'sell' | None
    content: str
    author: str
    deleted: bool


def _q(s: str | None) -> str:
    """SQL-escape a string for inlining: wrap in single quotes, double internal quotes."""
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _row_to_event(row) -> NoteEvent:
    ts_raw = row["ts"]
    if isinstance(ts_raw, datetime):
        ts = ts_raw if ts_raw.tzinfo else ts_raw.replace(tzinfo=timezone.utc)
    else:
        # epoch ms int fallback
        ts = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
    return NoteEvent(
        ts=ts,
        code=row["code"],
        event_id=row["event_id"],
        event_type=row["event_type"] or "",
        source=row["event_source"] or "",
        title=row["title"] or "",
        price=row["price"],
        qty=row["qty"],
        side=row["side"],
        content=row["content"] or "",
        author=row["author"] or "",
        deleted=bool(row["deleted"]) if row["deleted"] is not None else False,
    )


# `deleted = false OR deleted IS NULL` — NULL appears for rows written before
# the column existed (ALTER TABLE adds NULL to old rows). MEMORY has the bug
# story; reproduce the defensive predicate everywhere.
_NOT_DELETED = "(deleted = false OR deleted IS NULL)"


class TradeNoteStore:
    """CRUD for `trade_notes` table.

    Reuses the long-lived `GreptimeBacktestStorage.db` connection pool from
    `app.state.storage` — do NOT instantiate a separate pool.
    """

    def __init__(self, storage) -> None:
        # storage: GreptimeBacktestStorage (avoid import cycle by typing loosely)
        self._db = storage.db

    async def ensure_schema(self) -> None:
        await self._db.execute(_CREATE_TRADE_NOTES_SQL)

    # ---------- left pane: list of stocks with any event ----------

    async def list_stocks(self, beijing_date: str | None = None) -> list[dict]:
        """All stocks that have at least one event, ordered by most recent activity.

        If `beijing_date` (YYYY-MM-DD) is provided, only stocks with at least
        one event during that Beijing day are returned. Times in the table are
        stored as UTC epoch ms, so the filter window is converted to UTC ms
        via calendar.timegm() per CLAUDE.md asyncpg/GreptimeDB rules.
        """
        where = _NOT_DELETED
        if beijing_date:
            try:
                y, m, d = (int(p) for p in beijing_date.split("-"))
            except (ValueError, AttributeError) as e:
                raise ValueError(f"beijing_date must be YYYY-MM-DD, got {beijing_date!r}") from e
            # Beijing 00:00 = UTC (date - 8h)
            start_naive_utc = datetime(y, m, d) - timedelta(hours=8)
            end_naive_utc = start_naive_utc + timedelta(days=1)
            start_ms = calendar.timegm(start_naive_utc.timetuple()) * 1000
            end_ms = calendar.timegm(end_naive_utc.timetuple()) * 1000
            where = f"{_NOT_DELETED} AND ts >= {start_ms} AND ts < {end_ms}"
        sql = (
            f"SELECT code, MAX(ts) AS last_ts, COUNT(*) AS event_count "
            f"FROM trade_notes WHERE {where} "
            f"GROUP BY code ORDER BY last_ts DESC"
        )
        rows = await self._db.fetch(sql)
        out: list[dict] = []
        for r in rows:
            ts_raw = r["last_ts"]
            if isinstance(ts_raw, datetime):
                last_ts = ts_raw if ts_raw.tzinfo else ts_raw.replace(tzinfo=timezone.utc)
            else:
                last_ts = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
            out.append(
                {
                    "code": r["code"],
                    "last_ts": last_ts.isoformat(),
                    "event_count": int(r["event_count"]),
                }
            )
        return out

    # ---------- middle pane: events for one stock ----------

    async def list_events(self, code: str) -> list[NoteEvent]:
        sql = (
            f"SELECT ts, code, event_id, event_type, event_source, title, "
            f"       price, qty, side, content, author, deleted "
            f"FROM trade_notes "
            f"WHERE code = {_q(code)} AND {_NOT_DELETED} "
            f"ORDER BY ts ASC"
        )
        rows = await self._db.fetch(sql)
        return [_row_to_event(r) for r in rows]

    # ---------- right pane: single event ----------

    async def get_event(self, code: str, event_id: str) -> NoteEvent | None:
        # Filter deleted in WHERE: an edit that moves ts BACKWARDS leaves the
        # old row at a higher ts with deleted=true. ORDER BY ts DESC alone
        # would return that deleted row first. Using the _NOT_DELETED clause
        # ensures we always get the latest LIVE row.
        sql = (
            f"SELECT ts, code, event_id, event_type, event_source, title, "
            f"       price, qty, side, content, author, deleted "
            f"FROM trade_notes "
            f"WHERE code = {_q(code)} AND event_id = {_q(event_id)} "
            f"AND {_NOT_DELETED} "
            f"ORDER BY ts DESC LIMIT 1"
        )
        row = await self._db.fetchrow(sql)
        if row is None:
            return None
        return _row_to_event(row)

    # ---------- writes ----------

    async def upsert_broker_event_by_order_id(
        self,
        *,
        order_id: int | str,
        code: str,
        side: str,  # 'buy' | 'sell'
        qty: int,
        price: float | None,
        ts_ms: int | None = None,
    ) -> bool:
        """Idempotent insert keyed by broker order_id.

        Used by the manual backfill endpoint to import already-filled orders
        from `broker.get_orders()` into trade_notes without creating dupes if
        the operator triggers backfill twice. Returns True if newly written,
        False if a row with this order_id already exists.
        """
        event_id = f"broker_{order_id}"
        check_sql = (
            f"SELECT 1 FROM trade_notes "
            f"WHERE code = {_q(code)} AND event_id = {_q(event_id)} LIMIT 1"
        )
        existing = await self._db.fetchrow(check_sql)
        if existing is not None:
            return False
        side_lower = side.lower()
        event_type = "买入" if side_lower == "buy" else "卖出"
        if price is not None:
            title = f"{event_type} @{price:.2f} x {qty}"
        else:
            title = f"{event_type} 市价 x {qty}"
        await self._raw_insert(
            ts_ms=ts_ms if ts_ms is not None else _now_ms(),
            code=code,
            event_id=event_id,
            event_type=event_type,
            source="broker",
            title=title,
            price=price,
            qty=qty,
            side=side_lower,
            content="",
            author="system",
            deleted=False,
        )
        return True

    async def import_today_filled_orders(
        self,
        broker,
        code_filter: set[str] | None = None,
    ) -> tuple[int, int]:
        """Import today's FILLED orders from broker into trade_notes.

        Reads `broker.get_orders()`, keeps rows with status='FILLED', and
        upserts one event per order keyed by `broker_<order_id>`. Idempotent:
        re-running won't duplicate. Used by both /backfill-today and the
        post-batch-order hook in routes.py.

        `code_filter` (set of bare 6-digit codes): if given, only orders for
        these codes are imported — useful to scope work after a batch order.
        Pass None to import all of today's fills.

        Returns (written, skipped).
        """
        orders = await broker.get_orders()
        written = 0
        skipped = 0
        for o in orders:
            if str(o.get("status", "")).upper() != "FILLED":
                continue
            order_id = o.get("order_id")
            code = o.get("code") or ""
            if not code:
                continue
            bare_code = code.split(".")[0]
            if code_filter is not None and bare_code not in code_filter:
                continue
            side_raw = str(o.get("side", "")).lower()
            qty = int(o.get("qty") or 0)
            price = o.get("price")
            try:
                price_val = float(price) if price not in (None, 0, "0") else None
            except (TypeError, ValueError):
                price_val = None
            if order_id is None or side_raw not in ("buy", "sell") or qty <= 0:
                continue
            inserted = await self.upsert_broker_event_by_order_id(
                order_id=order_id,
                code=bare_code,
                side=side_raw,
                qty=qty,
                price=price_val,
            )
            if inserted:
                written += 1
            else:
                skipped += 1
        return written, skipped

    async def create_event(
        self,
        code: str,
        event_type: str,
        title: str,
        content: str,
        author: str = "user",
        source: str = "user",
        ts_ms: int | None = None,
        price: float | None = None,
        qty: int | None = None,
        side: str | None = None,
    ) -> str:
        return await self._insert(
            code=code,
            event_type=event_type,
            source=source,
            title=title,
            price=price,
            qty=qty,
            side=side,
            content=content,
            author=author,
            ts_ms=ts_ms if ts_ms is not None else _now_ms(),
        )

    async def update_event(
        self,
        code: str,
        event_id: str,
        title: str | None = None,
        content: str | None = None,
        ts_ms: int | None = None,
        event_type: str | None = None,
    ) -> bool:
        """Update title, content, ts, and/or event_type. Other fields immutable.

        Re-INSERT with same (code, event_id, ts) overwrites via mito dedup.
        Changing ts creates a row at the new ts; we soft-delete the row at
        the old ts to prevent both showing up in list_events (which scans
        all rows for the code, no GROUP BY).

        When event_type toggles between 买入/卖出, `side` flips with it so the
        two stay consistent (the events-list filter and dashboard meta both
        read `side`).
        """
        existing = await self.get_event(code, event_id)
        if existing is None:
            return False
        new_title = title if title is not None else existing.title
        new_content = content if content is not None else existing.content
        new_event_type = event_type if event_type is not None else existing.event_type
        new_side: str | None
        if event_type is not None and event_type in ("买入", "卖出"):
            new_side = "buy" if event_type == "买入" else "sell"
        else:
            new_side = existing.side
        old_ts_ms = int(existing.ts.timestamp() * 1000)
        new_ts_ms = ts_ms if ts_ms is not None else old_ts_ms
        if new_ts_ms != old_ts_ms:
            # Soft-delete the old-ts row first.
            await self._raw_insert(
                ts_ms=old_ts_ms,
                code=existing.code,
                event_id=existing.event_id,
                event_type=existing.event_type,
                source=existing.source,
                title=existing.title,
                price=existing.price,
                qty=existing.qty,
                side=existing.side,
                content=existing.content,
                author=existing.author,
                deleted=True,
            )
        await self._raw_insert(
            ts_ms=new_ts_ms,
            code=existing.code,
            event_id=existing.event_id,
            event_type=new_event_type,
            source=existing.source,
            title=new_title,
            price=existing.price,
            qty=existing.qty,
            side=new_side,
            content=new_content,
            author=existing.author,
            deleted=False,
        )
        return True

    async def rename_stock_code(self, old_code: str, new_code: str) -> int:
        """Move all LIVE events from old_code to new_code. Returns count moved.

        For each event under old_code, INSERT a copy under new_code (same
        event_id and ts, all other fields preserved), then soft-delete the
        old row. PRIMARY KEY (code, event_id) means the new (new_code,
        event_id) is a fresh row — no overwrite of any pre-existing data
        under new_code.

        If new_code already has events, the result is a merge — events
        interleave by ts in the events list. That's the desired semantic
        for "I logged trades under a typo'd code, fix them onto the real
        code".

        Soft-deleted rows under old_code stay where they are (already dead).
        """
        if old_code == new_code:
            return 0
        events = await self.list_events(old_code)  # live only
        moved = 0
        for ev in events:
            old_ts_ms = int(ev.ts.timestamp() * 1000)
            await self._raw_insert(
                ts_ms=old_ts_ms,
                code=new_code,
                event_id=ev.event_id,
                event_type=ev.event_type,
                source=ev.source,
                title=ev.title,
                price=ev.price,
                qty=ev.qty,
                side=ev.side,
                content=ev.content,
                author=ev.author,
                deleted=False,
            )
            await self._raw_insert(
                ts_ms=old_ts_ms,
                code=old_code,
                event_id=ev.event_id,
                event_type=ev.event_type,
                source=ev.source,
                title=ev.title,
                price=ev.price,
                qty=ev.qty,
                side=ev.side,
                content=ev.content,
                author=ev.author,
                deleted=True,
            )
            moved += 1
        return moved

    async def delete_event(self, code: str, event_id: str) -> bool:
        """Soft delete: re-INSERT same row with deleted=true."""
        existing = await self.get_event(code, event_id)
        if existing is None:
            return False
        ts_ms = int(existing.ts.timestamp() * 1000)
        await self._raw_insert(
            ts_ms=ts_ms,
            code=existing.code,
            event_id=existing.event_id,
            event_type=existing.event_type,
            source=existing.source,
            title=existing.title,
            price=existing.price,
            qty=existing.qty,
            side=existing.side,
            content=existing.content,
            author=existing.author,
            deleted=True,
        )
        return True

    # ---------- internal ----------

    async def _insert(
        self,
        *,
        code: str,
        event_type: str,
        source: str,
        title: str,
        price: float | None,
        qty: int | None,
        side: str | None,
        content: str,
        author: str,
        ts_ms: int,
    ) -> str:
        event_id = uuid.uuid4().hex
        await self._raw_insert(
            ts_ms=ts_ms,
            code=code,
            event_id=event_id,
            event_type=event_type,
            source=source,
            title=title,
            price=price,
            qty=qty,
            side=side,
            content=content,
            author=author,
            deleted=False,
        )
        return event_id

    async def _raw_insert(
        self,
        *,
        ts_ms: int,
        code: str,
        event_id: str,
        event_type: str,
        source: str,
        title: str,
        price: float | None,
        qty: int | None,
        side: str | None,
        content: str,
        author: str,
        deleted: bool,
    ) -> None:
        price_lit = "NULL" if price is None else f"{price}"
        qty_lit = "NULL" if qty is None else f"{int(qty)}"
        side_lit = _q(side) if side else "NULL"
        sql = (
            "INSERT INTO trade_notes "
            "(ts, code, event_id, event_type, event_source, title, "
            " price, qty, side, content, author, deleted) "
            f"VALUES ({ts_ms}, {_q(code)}, {_q(event_id)}, {_q(event_type)}, "
            f"{_q(source)}, {_q(title)}, {price_lit}, {qty_lit}, {side_lit}, "
            f"{_q(content)}, {_q(author)}, {'true' if deleted else 'false'})"
        )
        await self._db.execute(sql)
