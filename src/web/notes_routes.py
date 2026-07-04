# === MODULE PURPOSE ===
# HTTP entry points for the trade-notes feature (NOTE-001).
#
# Three-pane master-detail UI backed by GreptimeDB `trade_notes` + `note_cards`:
#   GET  /trade-notes                              → HTML page (vanilla JS)
#   GET  /trade-notes/backfill                     → 补作业 page: date-ordered bulk backfill table
#   GET  /api/notes/events-range                   → all events in a Beijing date range (JSON)
#   GET  /api/notes/stock-name/{code}              → code → company name lookup
#   GET  /api/notes/stocks                         → left pane (stocks)
#   PATCH /api/notes/stocks/{code}                 → rename a stock's code
#   GET  /api/notes/{code}/events                  → middle pane (events list)
#   GET  /api/notes/{code}/events/{event_id}       → right pane (single event)
#   POST /api/notes/{code}/events                  → create manual event
#   PATCH /api/notes/{code}/events/{event_id}      → edit title/content/type/ts
#   DELETE /api/notes/{code}/events/{event_id}     → hard delete (physical, shadows too)
#   GET  /api/notes/{code}/cards                   → 篇 view: manual timestamped cards
#   POST /api/notes/{code}/cards                   → create card
#   PATCH /api/notes/{code}/cards/{card_id}        → edit card content/ts
#   DELETE /api/notes/{code}/cards/{card_id}       → hard-delete card (physical DELETE)
#
# Auto-write of broker events happens in routes.py at the place_order success
# point — not here.

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field

from src.notes.note_store import DEFAULT_EVENT_TYPES, TradeNoteStore

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_BEIJING_TZ = ZoneInfo("Asia/Shanghai")

logger = logging.getLogger(__name__)


# Pydantic models MUST be at module level — see ANA-001 docs about
# `from __future__ import annotations` interaction with FastAPI body parsing.
class CreateEventRequest(BaseModel):
    event_type: str = Field(..., min_length=1, max_length=32)
    title: str = Field("", max_length=200)
    content: str = Field("", max_length=100_000)
    content_external: str = Field("", max_length=100_000)
    author: str = Field("user", max_length=64)
    source: str = Field("user", pattern=r"^(user|ai)$")
    ts_ms: int | None = Field(None, description="Epoch ms; defaults to now if omitted")
    price: float | None = Field(None, ge=0)
    qty: int | None = Field(None, ge=0)
    side: str | None = Field(None, pattern=r"^(buy|sell)$")
    # Fees / dividend / realized P&L. All optional — 买入 leaves stamp_tax /
    # dividend / realized_pnl as None (UI hides them).
    commission: float | None = Field(None, ge=0)
    transfer_fee: float | None = Field(None, ge=0)
    stamp_tax: float | None = Field(None, ge=0)
    # dividend = 派息额 − 股息税净到手；扣税多于派息时可为负，所以不加 ge=0。
    dividend: float | None = Field(None)
    # realized_pnl can legitimately be negative (loss), so no ge=0.
    realized_pnl: float | None = Field(None)


class UpdateEventRequest(BaseModel):
    title: str | None = Field(None, max_length=200)
    content: str | None = Field(None, max_length=100_000)
    content_external: str | None = Field(None, max_length=100_000)
    ts_ms: int | None = Field(None, description="Epoch ms; omit to keep current ts")
    event_type: str | None = Field(None, min_length=1, max_length=32)
    # For numeric fields, distinguish "omitted" (keep existing) from "null"
    # (clear to market/none) via `model_fields_set` in the route handler.
    price: float | None = Field(None, ge=0)
    qty: int | None = Field(None, ge=0)
    commission: float | None = Field(None, ge=0)
    transfer_fee: float | None = Field(None, ge=0)
    stamp_tax: float | None = Field(None, ge=0)
    dividend: float | None = Field(None)  # 净到手，可负（见 CreateEventRequest）
    realized_pnl: float | None = Field(None)


class RenameStockRequest(BaseModel):
    new_code: str = Field(..., pattern=r"^\d{6}$")


class CreateCardRequest(BaseModel):
    content: str = Field("", max_length=100_000)
    ts_ms: int = Field(..., description="Epoch ms — required, the card's own ts")


class UpdateCardRequest(BaseModel):
    content: str | None = Field(None, max_length=100_000)
    ts_ms: int | None = Field(None, description="Epoch ms; omit to keep current ts")


def _get_store(request: Request) -> TradeNoteStore:
    storage = getattr(request.app.state, "storage", None)
    if storage is None:
        raise HTTPException(status_code=503, detail="GreptimeDB unavailable")
    return TradeNoteStore(storage)


def create_notes_router() -> APIRouter:
    router = APIRouter(tags=["notes"])

    @router.get("/trade-notes", response_class=HTMLResponse)
    async def trade_notes_page(request: Request):
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "trade_notes.html",
            {
                "request": request,
                "event_types": DEFAULT_EVENT_TYPES,
            },
        )

    @router.get("/trade-notes/backfill", response_class=HTMLResponse)
    async def trade_notes_backfill_page(request: Request):
        """补作业页面 — 按日期排列的表格式批量补录历史买卖/费用."""
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "trade_notes_backfill.html",
            {
                "request": request,
                "event_types": DEFAULT_EVENT_TYPES,
            },
        )

    @router.get("/api/notes/events-range")
    async def events_in_range(
        request: Request, start: str | None = None, end: str | None = None
    ) -> dict:
        """All live events in Beijing date [start, end], JSON (for the backfill table).

        Omit both params to get every live event ever recorded — the backfill
        page loads everything and filters "待补" client-side.

        Same query as /api/notes/export but returned as a plain JSON body
        (no attachment) and including the internal `content` field so rows
        can be edited in place. Timestamps are Beijing-local ISO strings.
        """
        from src.data.sources.local_concept_mapper import LocalConceptMapper

        if (start is None) != (end is None):
            raise HTTPException(status_code=400, detail="start and end must be provided together")
        if start is not None and end is not None:
            if not _DATE_RE.match(start) or not _DATE_RE.match(end):
                raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
            if start > end:
                raise HTTPException(status_code=400, detail="start must be <= end")
        store = _get_store(request)
        try:
            events = await store.list_events_in_range(start, end)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        mapper = LocalConceptMapper()
        return {
            "range": {"start": start, "end": end},
            "count": len(events),
            "events": [
                {
                    "ts": e.ts.astimezone(_BEIJING_TZ).isoformat(),
                    "code": e.code,
                    "name": mapper.get_stock_name(e.code),
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "source": e.source,
                    "title": e.title,
                    "price": e.price,
                    "qty": e.qty,
                    "side": e.side,
                    "content": e.content,
                    "content_external": e.content_external,
                    "author": e.author,
                    "commission": e.commission,
                    "transfer_fee": e.transfer_fee,
                    "stamp_tax": e.stamp_tax,
                    "dividend": e.dividend,
                    "realized_pnl": e.realized_pnl,
                }
                for e in events
            ],
        }

    @router.get("/api/notes/stock-name/{code}")
    async def stock_name(code: str) -> dict:
        """Code → company name (backfill table confirms the typed code)."""
        from src.data.sources.local_concept_mapper import LocalConceptMapper

        if not re.match(r"^\d{6}$", code):
            raise HTTPException(status_code=400, detail="code must be 6 digits")
        mapper = LocalConceptMapper()
        return {"code": code, "name": mapper.get_stock_name(code)}

    @router.get("/api/notes/stocks")
    async def list_stocks(request: Request, date: str | None = None) -> dict:
        from src.data.sources.local_concept_mapper import LocalConceptMapper

        store = _get_store(request)
        try:
            stocks = await store.list_stocks(beijing_date=date)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        mapper = LocalConceptMapper()
        for s in stocks:
            s["name"] = mapper.get_stock_name(s["code"])
        return {"stocks": stocks}

    @router.get("/api/notes/{code}/events")
    async def list_events(code: str, request: Request) -> dict:
        store = _get_store(request)
        events = await store.list_events(code)
        return {
            "code": code,
            "events": [
                {
                    "ts": e.ts.isoformat(),
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "source": e.source,
                    "title": e.title,
                    "price": e.price,
                    "qty": e.qty,
                    "side": e.side,
                    "content": e.content,
                    "content_external": e.content_external,
                    "author": e.author,
                    "has_content": bool(e.content or e.content_external),
                    "commission": e.commission,
                    "transfer_fee": e.transfer_fee,
                    "stamp_tax": e.stamp_tax,
                    "dividend": e.dividend,
                    "realized_pnl": e.realized_pnl,
                }
                for e in events
            ],
        }

    @router.get("/api/notes/{code}/events/{event_id}")
    async def get_event(code: str, event_id: str, request: Request) -> dict:
        store = _get_store(request)
        ev = await store.get_event(code, event_id)
        if ev is None:
            raise HTTPException(status_code=404, detail="event not found")
        return {
            "ts": ev.ts.isoformat(),
            "code": ev.code,
            "event_id": ev.event_id,
            "event_type": ev.event_type,
            "source": ev.source,
            "title": ev.title,
            "price": ev.price,
            "qty": ev.qty,
            "side": ev.side,
            "content": ev.content,
            "content_external": ev.content_external,
            "author": ev.author,
            "commission": ev.commission,
            "transfer_fee": ev.transfer_fee,
            "stamp_tax": ev.stamp_tax,
            "dividend": ev.dividend,
            "realized_pnl": ev.realized_pnl,
        }

    @router.post("/api/notes/{code}/events")
    async def create_event(code: str, body: CreateEventRequest, request: Request) -> dict:
        store = _get_store(request)
        event_id = await store.create_event(
            code=code,
            event_type=body.event_type,
            title=body.title,
            content=body.content,
            content_external=body.content_external,
            author=body.author,
            source=body.source,
            ts_ms=body.ts_ms,
            price=body.price,
            qty=body.qty,
            side=body.side,
            commission=body.commission,
            transfer_fee=body.transfer_fee,
            stamp_tax=body.stamp_tax,
            dividend=body.dividend,
            realized_pnl=body.realized_pnl,
        )
        logger.info(f"trade-notes: created {body.source} event for {code} ({body.event_type})")
        return {"event_id": event_id}

    @router.patch("/api/notes/{code}/events/{event_id}")
    async def update_event(
        code: str, event_id: str, body: UpdateEventRequest, request: Request
    ) -> dict:
        store = _get_store(request)
        extra: dict = {}
        fields = body.model_fields_set
        # Same "field present → forward; absent → keep" pattern as price/qty.
        # Lets the client explicitly clear a fee to NULL by sending `null`.
        for name in (
            "price",
            "qty",
            "commission",
            "transfer_fee",
            "stamp_tax",
            "dividend",
            "realized_pnl",
        ):
            if name in fields:
                extra[name] = getattr(body, name)
        ok = await store.update_event(
            code=code,
            event_id=event_id,
            title=body.title,
            content=body.content,
            content_external=body.content_external,
            ts_ms=body.ts_ms,
            event_type=body.event_type,
            **extra,
        )
        if not ok:
            raise HTTPException(status_code=404, detail="event not found")
        return {"updated": True}

    @router.patch("/api/notes/stocks/{code}")
    async def rename_stock(code: str, body: RenameStockRequest, request: Request) -> dict:
        """Rename a stock's code — moves all live events from {code} to {new_code}.

        Used to correct a typo'd code after the fact; if {new_code} already
        has events the two are merged.
        """
        store = _get_store(request)
        moved = await store.rename_stock_code(code, body.new_code)
        logger.info(f"trade-notes rename: {code} -> {body.new_code} ({moved} events moved)")
        return {"moved": moved, "new_code": body.new_code}

    @router.delete("/api/notes/{code}/events/{event_id}")
    async def delete_event(code: str, event_id: str, request: Request) -> dict:
        store = _get_store(request)
        ok = await store.delete_event(code, event_id)
        if not ok:
            raise HTTPException(status_code=404, detail="event not found")
        return {"deleted": True}

    @router.get("/api/notes/{code}/cards")
    async def list_cards(code: str, request: Request) -> dict:
        store = _get_store(request)
        cards = await store.list_cards(code)
        return {
            "code": code,
            "cards": [
                {
                    "ts": c.ts.isoformat(),
                    "card_id": c.card_id,
                    "content": c.content,
                }
                for c in cards
            ],
        }

    @router.post("/api/notes/{code}/cards")
    async def create_card(code: str, body: CreateCardRequest, request: Request) -> dict:
        store = _get_store(request)
        card_id = await store.create_card(code=code, content=body.content, ts_ms=body.ts_ms)
        return {"card_id": card_id}

    @router.patch("/api/notes/{code}/cards/{card_id}")
    async def update_card(
        code: str, card_id: str, body: UpdateCardRequest, request: Request
    ) -> dict:
        store = _get_store(request)
        ok = await store.update_card(
            code=code, card_id=card_id, content=body.content, ts_ms=body.ts_ms
        )
        if not ok:
            raise HTTPException(status_code=404, detail="card not found")
        return {"updated": True}

    @router.delete("/api/notes/{code}/cards/{card_id}")
    async def delete_card(code: str, card_id: str, request: Request) -> dict:
        store = _get_store(request)
        ok = await store.delete_card(code, card_id)
        if not ok:
            raise HTTPException(status_code=404, detail="card not found")
        return {"deleted": True}

    @router.post("/api/notes/backfill-today")
    async def backfill_today(request: Request) -> dict:
        """Import today's FILLED broker orders into trade_notes.

        Thin wrapper over `TradeNoteStore.import_today_filled_orders` — the
        same helper used by the post-batch-order hook, so a manual click
        gets the same idempotent semantics (events keyed by broker_<order_id>).
        """
        from src.trading.broker_client import BrokerClient

        store = _get_store(request)
        broker: BrokerClient | None = getattr(request.app.state, "broker", None)
        if broker is None:
            raise HTTPException(status_code=503, detail="Broker 未配置")
        try:
            written, skipped = await store.import_today_filled_orders(broker)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"backfill 失败: {e}") from e
        logger.info(f"trade-notes backfill-today: written={written} skipped={skipped}")
        return {"written": written, "skipped": skipped}

    @router.get("/api/notes/export")
    async def export_notes(request: Request, start: str, end: str) -> Response:
        """Export all live events in Beijing date [start, end] as a JSON file.

        Filters: deleted=false; all stocks; events only (no cards). Timestamps
        in the response are Beijing-local ISO strings to match how users read
        the data offline.
        """
        from src.data.sources.local_concept_mapper import LocalConceptMapper

        if not _DATE_RE.match(start) or not _DATE_RE.match(end):
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
        if start > end:
            raise HTTPException(status_code=400, detail="start must be <= end")
        store = _get_store(request)
        try:
            events = await store.list_events_in_range(start, end)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        mapper = LocalConceptMapper()
        payload = {
            "exported_at": datetime.now(_BEIJING_TZ).isoformat(),
            "range": {"start": start, "end": end},
            "count": len(events),
            "events": [
                {
                    "ts": e.ts.astimezone(_BEIJING_TZ).isoformat(),
                    "code": e.code,
                    "name": mapper.get_stock_name(e.code),
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "source": e.source,
                    "title": e.title,
                    "price": e.price,
                    "qty": e.qty,
                    "side": e.side,
                    "content_external": e.content_external,
                    "author": e.author,
                    "commission": e.commission,
                    "transfer_fee": e.transfer_fee,
                    "stamp_tax": e.stamp_tax,
                    "dividend": e.dividend,
                    "realized_pnl": e.realized_pnl,
                }
                for e in events
            ],
        }
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        filename = f"trade_notes_{start}_{end}.json"
        logger.info(f"trade-notes export: {start}..{end} count={len(events)}")
        return Response(
            content=body,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return router
