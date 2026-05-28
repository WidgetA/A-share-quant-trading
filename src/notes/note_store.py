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
from typing import Any

logger = logging.getLogger(__name__)

# Sentinel for update_event params that distinguishes "caller omitted this field"
# (keep the existing value) from "caller explicitly passed None" (clear to None).
_UNSET: Any = object()


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
    content_external STRING,
    author      STRING,
    deleted     BOOLEAN,
    commission   FLOAT64,
    transfer_fee FLOAT64,
    stamp_tax    FLOAT64,
    dividend     FLOAT64,
    realized_pnl FLOAT64,
    PRIMARY KEY (code, event_id)
)
"""

# Idempotent ALTERs for existing deploys. GreptimeDB errors if the column
# already exists; ensure_schema() swallows that case only.
_ALTER_ADD_CONTENT_EXTERNAL_SQL = "ALTER TABLE trade_notes ADD COLUMN content_external STRING"
_ALTER_ADD_COMMISSION_SQL = "ALTER TABLE trade_notes ADD COLUMN commission FLOAT64"
_ALTER_ADD_TRANSFER_FEE_SQL = "ALTER TABLE trade_notes ADD COLUMN transfer_fee FLOAT64"
_ALTER_ADD_STAMP_TAX_SQL = "ALTER TABLE trade_notes ADD COLUMN stamp_tax FLOAT64"
_ALTER_ADD_DIVIDEND_SQL = "ALTER TABLE trade_notes ADD COLUMN dividend FLOAT64"
_ALTER_ADD_REALIZED_PNL_SQL = "ALTER TABLE trade_notes ADD COLUMN realized_pnl FLOAT64"


# Manually-inserted cards in 篇 view live in their own table — they aren't
# trade events and don't belong in the events list. The 篇 view fetches
# events + cards and interleaves them by ts.
_CREATE_NOTE_CARDS_SQL = """
CREATE TABLE IF NOT EXISTS note_cards (
    ts          TIMESTAMP TIME INDEX,
    code        STRING,
    card_id     STRING,
    content     STRING,
    deleted     BOOLEAN,
    PRIMARY KEY (code, card_id)
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
    content: str  # 对内 — private notes
    content_external: str  # 对外 — for sharing; only used for 买入/卖出
    author: str
    deleted: bool
    # Fee/dividend/P&L fields. All optional — NULL for legacy rows and for
    # broker-imported events (broker fill data doesn't include these).
    commission: float | None  # 手续费 (买/卖)
    transfer_fee: float | None  # 过户费 (买/卖)
    stamp_tax: float | None  # 印花税 (仅卖出)
    dividend: float | None  # 股息 (仅卖出事件登记)
    realized_pnl: float | None  # 平仓收益 (仅卖出)


@dataclass
class NoteCard:
    ts: datetime
    code: str
    card_id: str
    content: str
    deleted: bool


def _q(s: str | None) -> str:
    """SQL-escape a string for inlining: wrap in single quotes, double internal quotes."""
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _row_to_card(row) -> NoteCard:
    ts_raw = row["ts"]
    if isinstance(ts_raw, datetime):
        ts = ts_raw if ts_raw.tzinfo else ts_raw.replace(tzinfo=timezone.utc)
    else:
        ts = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
    return NoteCard(
        ts=ts,
        code=row["code"],
        card_id=row["card_id"],
        content=row["content"] or "",
        deleted=bool(row["deleted"]) if row["deleted"] is not None else False,
    )


def _row_to_event(row) -> NoteEvent:
    ts_raw = row["ts"]
    if isinstance(ts_raw, datetime):
        ts = ts_raw if ts_raw.tzinfo else ts_raw.replace(tzinfo=timezone.utc)
    else:
        # epoch ms int fallback
        ts = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
    # Defensive: legacy rows / pre-ALTER rows may not have these keys at all
    # if the result mapping is dict-backed; .get() returns None either way.
    def _opt(name: str) -> float | None:
        try:
            return row[name]
        except (KeyError, IndexError):
            return None

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
        content_external=row["content_external"] or "",
        author=row["author"] or "",
        deleted=bool(row["deleted"]) if row["deleted"] is not None else False,
        commission=_opt("commission"),
        transfer_fee=_opt("transfer_fee"),
        stamp_tax=_opt("stamp_tax"),
        dividend=_opt("dividend"),
        realized_pnl=_opt("realized_pnl"),
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
        await self._db.execute(_CREATE_NOTE_CARDS_SQL)
        # Idempotent ALTERs for deploys created before these columns existed.
        # GreptimeDB raises if the column is already there — swallow that case
        # only; surface anything else.
        for label, sql in (
            ("content_external", _ALTER_ADD_CONTENT_EXTERNAL_SQL),
            ("commission", _ALTER_ADD_COMMISSION_SQL),
            ("transfer_fee", _ALTER_ADD_TRANSFER_FEE_SQL),
            ("stamp_tax", _ALTER_ADD_STAMP_TAX_SQL),
            ("dividend", _ALTER_ADD_DIVIDEND_SQL),
            ("realized_pnl", _ALTER_ADD_REALIZED_PNL_SQL),
        ):
            try:
                await self._db.execute(sql)
            except Exception as e:
                msg = str(e).lower()
                if "exists" not in msg and "duplicate" not in msg:
                    logger.warning(f"trade_notes ALTER {label}: {e}")

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
            f"       price, qty, side, content, content_external, author, deleted, "
            f"       commission, transfer_fee, stamp_tax, dividend, realized_pnl "
            f"FROM trade_notes "
            f"WHERE code = {_q(code)} AND {_NOT_DELETED} "
            f"ORDER BY ts ASC"
        )
        rows = await self._db.fetch(sql)
        return [_row_to_event(r) for r in rows]

    # ---------- export: all live events across all stocks in a date range ----------

    async def list_events_in_range(
        self, start_beijing_date: str, end_beijing_date: str
    ) -> list[NoteEvent]:
        """All live events with ts falling in Beijing date [start, end] inclusive.

        Both bounds are YYYY-MM-DD Beijing days; the window is closed on the
        start side and the end day is included by extending to end+1 day 00:00
        Beijing time. UTC epoch ms conversion follows the same calendar.timegm()
        pattern as list_stocks() — see CLAUDE.md §7.
        """

        def _parse(d: str) -> tuple[int, int, int]:
            try:
                y, m, dd = (int(p) for p in d.split("-"))
                return y, m, dd
            except (ValueError, AttributeError) as e:
                raise ValueError(f"date must be YYYY-MM-DD, got {d!r}") from e

        sy, sm, sd = _parse(start_beijing_date)
        ey, em, ed = _parse(end_beijing_date)
        # Beijing 00:00 = UTC (date - 8h)
        start_naive_utc = datetime(sy, sm, sd) - timedelta(hours=8)
        end_naive_utc = datetime(ey, em, ed) - timedelta(hours=8) + timedelta(days=1)
        start_ms = calendar.timegm(start_naive_utc.timetuple()) * 1000
        end_ms = calendar.timegm(end_naive_utc.timetuple()) * 1000
        if end_ms <= start_ms:
            raise ValueError(
                f"end_beijing_date must be >= start_beijing_date "
                f"(got {start_beijing_date} → {end_beijing_date})"
            )
        sql = (
            f"SELECT ts, code, event_id, event_type, event_source, title, "
            f"       price, qty, side, content, content_external, author, deleted, "
            f"       commission, transfer_fee, stamp_tax, dividend, realized_pnl "
            f"FROM trade_notes "
            f"WHERE {_NOT_DELETED} AND ts >= {start_ms} AND ts < {end_ms} "
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
            f"       price, qty, side, content, content_external, author, deleted, "
            f"       commission, transfer_fee, stamp_tax, dividend, realized_pnl "
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
        """Upsert a broker fill event keyed by order_id.

        New row → INSERT. Existing row → re-INSERT to overwrite broker fields
        (price/qty/title), preserving the user's content/content_external/ts.
        Re-INSERT on same (code, event_id, ts) upserts in place via the PK.

        Always returns True (it always writes).
        """
        event_id = f"broker_{order_id}"
        # Filter to live rows only — without this, if `update_event` moved the
        # live row to a new ts (leaving a soft-deleted shadow at the old ts),
        # ORDER BY ts DESC would return the shadow and we'd re-insert at the
        # old ts with deleted=False, resurrecting it as a duplicate.
        existing_sql = (
            f"SELECT ts, event_type, event_source AS source, "
            f"       content, content_external, author, "
            f"       commission, transfer_fee, stamp_tax, dividend, realized_pnl "
            f"FROM trade_notes "
            f"WHERE code = {_q(code)} AND event_id = {_q(event_id)} "
            f"AND {_NOT_DELETED} "
            f"ORDER BY ts DESC LIMIT 1"
        )
        existing = await self._db.fetchrow(existing_sql)
        side_lower = side.lower()
        event_type = "买入" if side_lower == "buy" else "卖出"
        if price is not None:
            title = f"{event_type} @{price:.2f} x {qty}"
        else:
            title = f"{event_type} 市价 x {qty}"

        if existing is not None:
            old_ts = existing["ts"]
            if isinstance(old_ts, datetime):
                row_ts_ms = int(
                    (old_ts if old_ts.tzinfo else old_ts.replace(tzinfo=timezone.utc)).timestamp()
                    * 1000
                )
            else:
                row_ts_ms = int(old_ts)
            row_event_type = existing["event_type"] or event_type
            row_source = existing["source"] or "broker"
            row_content = existing["content"] or ""
            row_content_external = existing["content_external"] or ""
            row_author = existing["author"] or "system"

            def _existing_opt(name: str) -> float | None:
                try:
                    return existing[name]
                except (KeyError, IndexError):
                    return None

            row_commission = _existing_opt("commission")
            row_transfer_fee = _existing_opt("transfer_fee")
            row_stamp_tax = _existing_opt("stamp_tax")
            row_dividend = _existing_opt("dividend")
            row_realized_pnl = _existing_opt("realized_pnl")
        else:
            row_ts_ms = ts_ms if ts_ms is not None else _now_ms()
            row_event_type = event_type
            row_source = "broker"
            row_content = ""
            row_content_external = ""
            row_author = "system"
            row_commission = None
            row_transfer_fee = None
            row_stamp_tax = None
            row_dividend = None
            row_realized_pnl = None

        await self._raw_insert(
            ts_ms=row_ts_ms,
            code=code,
            event_id=event_id,
            event_type=row_event_type,
            source=row_source,
            title=title,
            price=price,
            qty=qty,
            side=side_lower,
            content=row_content,
            content_external=row_content_external,
            author=row_author,
            deleted=False,
            commission=row_commission,
            transfer_fee=row_transfer_fee,
            stamp_tax=row_stamp_tax,
            dividend=row_dividend,
            realized_pnl=row_realized_pnl,
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
        return await self.import_filled_orders_from_list(orders, code_filter)

    async def import_filled_orders_from_list(
        self,
        orders: list[dict],
        code_filter: set[str] | None = None,
    ) -> tuple[int, int]:
        """Same as import_today_filled_orders but takes already-fetched orders.

        Lets the /api/trading/orders endpoint reuse the orders it already
        fetched for the UI, instead of double-calling broker.get_orders() on
        every page poll.
        """
        written = 0
        skipped = 0
        rejected: list[tuple[str, dict]] = []
        for o in orders:
            status_raw = str(o.get("status", ""))
            order_id = o.get("order_id")
            code = o.get("code") or ""
            if not code:
                rejected.append(("no_code", o))
                continue
            bare_code = code.split(".")[0]
            if code_filter is not None and bare_code not in code_filter:
                rejected.append(("filtered_out", o))
                continue
            # Non-FILLED single-order (REJECTED / CANCELLED / etc.): if an
            # earlier place_order hook wrote a note for this order_id (back
            # when we wrote notes on submit rather than on fill), soft-delete
            # the orphan so it stops showing up. Today these statuses produce
            # no notes; this branch only cleans up historical pollution.
            if status_raw.upper() != "FILLED":
                if order_id is not None:
                    deleted = await self.delete_event(bare_code, f"broker_{order_id}")
                    if deleted:
                        logger.info(
                            "trade-notes: soft-deleted orphan broker_%s on %s (status=%s)",
                            order_id,
                            bare_code,
                            status_raw,
                        )
                rejected.append((f"status={status_raw!r}", o))
                continue
            side_raw = str(o.get("side", "")).lower()
            qty = int(o.get("qty") or 0)
            # FILLED 单的真实成交价在 avg_traded_price 字段；price 是委托价（市价单为 0）。
            # 笔记记录的是已成交事件，必须用成交均价。
            price = o.get("avg_traded_price") or o.get("price")
            price_val: float | None
            if price is None or price in (0, "0"):
                price_val = None
            else:
                try:
                    price_val = float(price)
                except (TypeError, ValueError):
                    price_val = None
            if side_raw not in ("buy", "sell"):
                rejected.append((f"bad_side={side_raw!r}", o))
                continue
            if qty <= 0:
                rejected.append((f"bad_qty={qty}", o))
                continue
            # Orders placed in the broker's own client app come back with
            # order_id=None — broker has no internal id for them. Synthesize
            # a stable dedupe key from (code, side, qty, submit_time) and use
            # submit_time as the note ts so the entry shows the actual fill
            # moment, not the import-poll moment.
            submit_time_raw = o.get("submit_time")
            submit_ts_ms: int | None = None
            if submit_time_raw:
                try:
                    submit_ts_ms = int(
                        datetime.fromisoformat(str(submit_time_raw)).timestamp() * 1000
                    )
                except (TypeError, ValueError):
                    submit_ts_ms = None
            if order_id is None:
                if submit_ts_ms is None:
                    rejected.append(("no_order_id_and_no_submit_time", o))
                    continue
                effective_order_id: int | str = (
                    f"manual_{bare_code}_{side_raw}_{qty}_{submit_ts_ms}"
                )
            else:
                effective_order_id = order_id
            inserted = await self.upsert_broker_event_by_order_id(
                order_id=effective_order_id,
                code=bare_code,
                side=side_raw,
                qty=qty,
                price=price_val,
                ts_ms=submit_ts_ms,
            )
            if inserted:
                written += 1
            else:
                skipped += 1
        if rejected and (written + skipped) == 0:
            logger.warning(
                "import_filled_orders_from_list: %d rows in, 0 imported. Rejections: %s",
                len(orders),
                [(reason, o) for reason, o in rejected[:10]],
            )
        return written, skipped

    async def create_event(
        self,
        code: str,
        event_type: str,
        title: str,
        content: str,
        content_external: str = "",
        author: str = "user",
        source: str = "user",
        ts_ms: int | None = None,
        price: float | None = None,
        qty: int | None = None,
        side: str | None = None,
        commission: float | None = None,
        transfer_fee: float | None = None,
        stamp_tax: float | None = None,
        dividend: float | None = None,
        realized_pnl: float | None = None,
    ) -> str:
        # For 买入/卖出 events, keep side in sync with event_type and auto-fill
        # the title in the same format the broker-import path uses, so manual
        # entries and broker-imported entries render identically in the篇 view.
        if event_type in ("买入", "卖出"):
            if side is None:
                side = "buy" if event_type == "买入" else "sell"
            if not title and qty is not None:
                if price is not None:
                    title = f"{event_type} @{price:.2f} x {qty}"
                else:
                    title = f"{event_type} 市价 x {qty}"
        return await self._insert(
            code=code,
            event_type=event_type,
            source=source,
            title=title,
            price=price,
            qty=qty,
            side=side,
            content=content,
            content_external=content_external,
            author=author,
            ts_ms=ts_ms if ts_ms is not None else _now_ms(),
            commission=commission,
            transfer_fee=transfer_fee,
            stamp_tax=stamp_tax,
            dividend=dividend,
            realized_pnl=realized_pnl,
        )

    async def update_event(
        self,
        code: str,
        event_id: str,
        title: str | None = None,
        content: str | None = None,
        content_external: str | None = None,
        ts_ms: int | None = None,
        event_type: str | None = None,
        price: Any = _UNSET,
        qty: Any = _UNSET,
        commission: Any = _UNSET,
        transfer_fee: Any = _UNSET,
        stamp_tax: Any = _UNSET,
        dividend: Any = _UNSET,
        realized_pnl: Any = _UNSET,
    ) -> bool:
        """Update title, content (对内/对外), ts, event_type, price/qty, and fees.

        Re-INSERT with same (code, event_id, ts) overwrites via mito dedup.
        Changing ts creates a row at the new ts; we soft-delete the row at
        the old ts to prevent both showing up in list_events (which scans
        all rows for the code, no GROUP BY).

        When event_type toggles between 买入/卖出, `side` flips with it so the
        two stay consistent (the events-list filter and dashboard meta both
        read `side`).

        Numeric fields use the _UNSET sentinel so the caller can clear them
        to None (market price / unknown qty / no fee) explicitly, distinct
        from "keep existing".
        """
        existing = await self.get_event(code, event_id)
        if existing is None:
            return False
        new_title = title if title is not None else existing.title
        new_content = content if content is not None else existing.content
        new_content_external = (
            content_external if content_external is not None else existing.content_external
        )
        new_event_type = event_type if event_type is not None else existing.event_type
        new_side: str | None
        if event_type is not None and event_type in ("买入", "卖出"):
            new_side = "buy" if event_type == "买入" else "sell"
        else:
            new_side = existing.side
        new_price = price if price is not _UNSET else existing.price
        new_qty = qty if qty is not _UNSET else existing.qty
        new_commission = commission if commission is not _UNSET else existing.commission
        new_transfer_fee = transfer_fee if transfer_fee is not _UNSET else existing.transfer_fee
        new_stamp_tax = stamp_tax if stamp_tax is not _UNSET else existing.stamp_tax
        new_dividend = dividend if dividend is not _UNSET else existing.dividend
        new_realized_pnl = realized_pnl if realized_pnl is not _UNSET else existing.realized_pnl
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
                content_external=existing.content_external,
                author=existing.author,
                deleted=True,
                commission=existing.commission,
                transfer_fee=existing.transfer_fee,
                stamp_tax=existing.stamp_tax,
                dividend=existing.dividend,
                realized_pnl=existing.realized_pnl,
            )
        await self._raw_insert(
            ts_ms=new_ts_ms,
            code=existing.code,
            event_id=existing.event_id,
            event_type=new_event_type,
            source=existing.source,
            title=new_title,
            price=new_price,
            qty=new_qty,
            side=new_side,
            content=new_content,
            content_external=new_content_external,
            author=existing.author,
            deleted=False,
            commission=new_commission,
            transfer_fee=new_transfer_fee,
            stamp_tax=new_stamp_tax,
            dividend=new_dividend,
            realized_pnl=new_realized_pnl,
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
                content_external=ev.content_external,
                author=ev.author,
                deleted=False,
                commission=ev.commission,
                transfer_fee=ev.transfer_fee,
                stamp_tax=ev.stamp_tax,
                dividend=ev.dividend,
                realized_pnl=ev.realized_pnl,
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
                content_external=ev.content_external,
                author=ev.author,
                deleted=True,
                commission=ev.commission,
                transfer_fee=ev.transfer_fee,
                stamp_tax=ev.stamp_tax,
                dividend=ev.dividend,
                realized_pnl=ev.realized_pnl,
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
            content_external=existing.content_external,
            author=existing.author,
            deleted=True,
            commission=existing.commission,
            transfer_fee=existing.transfer_fee,
            stamp_tax=existing.stamp_tax,
            dividend=existing.dividend,
            realized_pnl=existing.realized_pnl,
        )
        return True

    # ---------- cards (note_cards table) ----------

    async def list_cards(self, code: str) -> list[NoteCard]:
        sql = (
            f"SELECT ts, code, card_id, content, deleted "
            f"FROM note_cards "
            f"WHERE code = {_q(code)} AND {_NOT_DELETED} "
            f"ORDER BY ts ASC"
        )
        rows = await self._db.fetch(sql)
        return [_row_to_card(r) for r in rows]

    async def get_card(self, code: str, card_id: str) -> NoteCard | None:
        sql = (
            f"SELECT ts, code, card_id, content, deleted "
            f"FROM note_cards "
            f"WHERE code = {_q(code)} AND card_id = {_q(card_id)} "
            f"AND {_NOT_DELETED} "
            f"ORDER BY ts DESC LIMIT 1"
        )
        row = await self._db.fetchrow(sql)
        if row is None:
            return None
        return _row_to_card(row)

    async def create_card(self, code: str, content: str, ts_ms: int) -> str:
        card_id = uuid.uuid4().hex
        await self._raw_card_insert(
            ts_ms=ts_ms, code=code, card_id=card_id, content=content, deleted=False
        )
        return card_id

    async def update_card(
        self,
        code: str,
        card_id: str,
        content: str | None = None,
        ts_ms: int | None = None,
    ) -> bool:
        existing = await self.get_card(code, card_id)
        if existing is None:
            return False
        new_content = content if content is not None else existing.content
        old_ts_ms = int(existing.ts.timestamp() * 1000)
        new_ts_ms = ts_ms if ts_ms is not None else old_ts_ms
        if new_ts_ms != old_ts_ms:
            # Soft-delete the row at the old ts first; otherwise list_cards
            # would see both. Same pattern as update_event.
            await self._raw_card_insert(
                ts_ms=old_ts_ms,
                code=existing.code,
                card_id=existing.card_id,
                content=existing.content,
                deleted=True,
            )
        await self._raw_card_insert(
            ts_ms=new_ts_ms,
            code=existing.code,
            card_id=existing.card_id,
            content=new_content,
            deleted=False,
        )
        return True

    async def delete_card(self, code: str, card_id: str) -> bool:
        existing = await self.get_card(code, card_id)
        if existing is None:
            return False
        ts_ms = int(existing.ts.timestamp() * 1000)
        await self._raw_card_insert(
            ts_ms=ts_ms,
            code=existing.code,
            card_id=existing.card_id,
            content=existing.content,
            deleted=True,
        )
        return True

    async def _raw_card_insert(
        self,
        *,
        ts_ms: int,
        code: str,
        card_id: str,
        content: str,
        deleted: bool,
    ) -> None:
        sql = (
            "INSERT INTO note_cards "
            "(ts, code, card_id, content, deleted) "
            f"VALUES ({ts_ms}, {_q(code)}, {_q(card_id)}, "
            f"{_q(content)}, {'true' if deleted else 'false'})"
        )
        await self._db.execute(sql)

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
        content_external: str,
        author: str,
        ts_ms: int,
        commission: float | None = None,
        transfer_fee: float | None = None,
        stamp_tax: float | None = None,
        dividend: float | None = None,
        realized_pnl: float | None = None,
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
            content_external=content_external,
            author=author,
            deleted=False,
            commission=commission,
            transfer_fee=transfer_fee,
            stamp_tax=stamp_tax,
            dividend=dividend,
            realized_pnl=realized_pnl,
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
        content_external: str,
        author: str,
        deleted: bool,
        commission: float | None = None,
        transfer_fee: float | None = None,
        stamp_tax: float | None = None,
        dividend: float | None = None,
        realized_pnl: float | None = None,
    ) -> None:
        price_lit = "NULL" if price is None else f"{price}"
        qty_lit = "NULL" if qty is None else f"{int(qty)}"
        side_lit = _q(side) if side else "NULL"

        def _flit(v: float | None) -> str:
            return "NULL" if v is None else f"{float(v)}"

        sql = (
            "INSERT INTO trade_notes "
            "(ts, code, event_id, event_type, event_source, title, "
            " price, qty, side, content, content_external, author, deleted, "
            " commission, transfer_fee, stamp_tax, dividend, realized_pnl) "
            f"VALUES ({ts_ms}, {_q(code)}, {_q(event_id)}, {_q(event_type)}, "
            f"{_q(source)}, {_q(title)}, {price_lit}, {qty_lit}, {side_lit}, "
            f"{_q(content)}, {_q(content_external)}, {_q(author)}, "
            f"{'true' if deleted else 'false'}, "
            f"{_flit(commission)}, {_flit(transfer_fee)}, {_flit(stamp_tax)}, "
            f"{_flit(dividend)}, {_flit(realized_pnl)})"
        )
        await self._db.execute(sql)
