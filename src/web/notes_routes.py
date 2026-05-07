# === MODULE PURPOSE ===
# HTTP entry points for the trade-notes feature (NOTE-001).
#
# Three-pane master-detail UI backed by GreptimeDB `trade_notes`:
#   GET  /trade-notes                              → HTML page (vanilla JS)
#   GET  /api/notes/stocks                         → left pane (stocks)
#   GET  /api/notes/{code}/events                  → middle pane (events list)
#   GET  /api/notes/{code}/events/{event_id}       → right pane (single event)
#   POST /api/notes/{code}/events                  → create manual event
#   PATCH /api/notes/{code}/events/{event_id}      → edit title/content
#   DELETE /api/notes/{code}/events/{event_id}     → soft delete
#
# Auto-write of broker events happens in routes.py at the place_order success
# point — not here.

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from src.notes.note_store import DEFAULT_EVENT_TYPES, TradeNoteStore

logger = logging.getLogger(__name__)


# Pydantic models MUST be at module level — see ANA-001 docs about
# `from __future__ import annotations` interaction with FastAPI body parsing.
class CreateEventRequest(BaseModel):
    event_type: str = Field(..., min_length=1, max_length=32)
    title: str = Field("", max_length=200)
    content: str = Field("", max_length=100_000)
    author: str = Field("user", max_length=64)
    source: str = Field("user", pattern=r"^(user|ai)$")
    ts_ms: int | None = Field(None, description="Epoch ms; defaults to now if omitted")


class UpdateEventRequest(BaseModel):
    title: str | None = Field(None, max_length=200)
    content: str | None = Field(None, max_length=100_000)
    ts_ms: int | None = Field(None, description="Epoch ms; omit to keep current ts")
    event_type: str | None = Field(None, min_length=1, max_length=32)


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
                    "author": e.author,
                    "has_content": bool(e.content),
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
            "author": ev.author,
        }

    @router.post("/api/notes/{code}/events")
    async def create_event(code: str, body: CreateEventRequest, request: Request) -> dict:
        store = _get_store(request)
        event_id = await store.create_event(
            code=code,
            event_type=body.event_type,
            title=body.title,
            content=body.content,
            author=body.author,
            source=body.source,
            ts_ms=body.ts_ms,
        )
        logger.info(f"trade-notes: created {body.source} event for {code} ({body.event_type})")
        return {"event_id": event_id}

    @router.patch("/api/notes/{code}/events/{event_id}")
    async def update_event(
        code: str, event_id: str, body: UpdateEventRequest, request: Request
    ) -> dict:
        store = _get_store(request)
        ok = await store.update_event(
            code=code,
            event_id=event_id,
            title=body.title,
            content=body.content,
            ts_ms=body.ts_ms,
            event_type=body.event_type,
        )
        if not ok:
            raise HTTPException(status_code=404, detail="event not found")
        return {"updated": True}

    @router.delete("/api/notes/{code}/events/{event_id}")
    async def delete_event(code: str, event_id: str, request: Request) -> dict:
        store = _get_store(request)
        ok = await store.delete_event(code, event_id)
        if not ok:
            raise HTTPException(status_code=404, detail="event not found")
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

    return router
