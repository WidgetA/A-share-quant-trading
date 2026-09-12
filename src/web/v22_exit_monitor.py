"""V22 notification-only monitor using the service's independently owned resources."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timedelta
from typing import Any

from src.common.v20_feishu import seal_v20_payload
from src.data.database.v20_mews_guard_store import _STRICT_CANDIDATE_SQL, _snapshot_from_row
from src.data.database.v22_positions import V22PositionStore
from src.strategy.v22_slim.exits import Bar, evaluate_day


class V22ExitMonitor:
    def __init__(self, service: Any):
        self.service = service
        self.store = V22PositionStore(service._repository, service.config)
        self.cache: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        self.minute: str | None = None

    async def rows(
        self, api: str, params: dict[str, Any], *, cache_key: tuple[Any, ...]
    ) -> list[dict[str, Any]]:
        if cache_key not in self.cache:
            response = await self.service._scan_state.realtime_client._api_call(api, params)
            body = response["data"]
            rows = [dict(zip(body["fields"], row, strict=True)) for row in body["items"]]
            if not rows:
                return []
            self.cache[cache_key] = rows
        return self.cache[cache_key]

    async def bars(self, code: str, day: date, now: datetime) -> list[Bar]:
        symbol = code + (".SH" if code.startswith("6") else ".SZ")
        if day == now.date():
            rows = await self.rows(
                "rt_min_daily",
                {"ts_code": symbol, "freq": "1MIN"},
                cache_key=("rt", self.minute, code),
            )
        elif day < now.date():
            rows = await self.rows(
                "stk_mins",
                {
                    "ts_code": symbol,
                    "freq": "1min",
                    "start_date": day.isoformat() + " 09:30:00",
                    "end_date": day.isoformat() + " 15:00:00",
                },
                cache_key=("history", day, code),
            )
        else:
            raise ValueError("V22 monitor cannot request future minute bars")
        result = []
        for row in rows:
            if row.get("ts_code") != symbol:
                raise ValueError("V22 minute response belongs to another stock")
            stamp = datetime.fromisoformat(str(row.get("time") or row.get("trade_time")))
            stamp = (
                stamp.replace(tzinfo=now.tzinfo)
                if stamp.tzinfo is None
                else stamp.astimezone(now.tzinfo)
            )
            if stamp.date() != day or stamp > now.replace(second=0, microsecond=0):
                continue
            result.append(
                Bar(
                    stamp,
                    *(float(row[key]) for key in ("open", "high", "low", "close", "vol", "amount")),
                )
            )
        if len(result) >= 6000:
            raise ValueError("V22 minute response may be truncated")
        return sorted(result, key=lambda b: b.at)

    async def daily(self, code: str, entry: date, now: datetime) -> list[dict[str, Any]]:
        symbol = code + (".SH" if code.startswith("6") else ".SZ")
        rows = await self.rows(
            "daily",
            {
                "ts_code": symbol,
                "start_date": (entry - timedelta(days=100)).strftime("%Y%m%d"),
                "end_date": (now.date() - timedelta(days=1)).strftime("%Y%m%d"),
            },
            cache_key=("daily", now.date(), code, entry),
        )
        if any(
            row["ts_code"] != symbol or row["trade_date"] >= now.strftime("%Y%m%d") for row in rows
        ):
            raise ValueError("V22 daily history identity or date is invalid")
        return sorted((r for r in rows if float(r["vol"]) > 0), key=lambda r: r["trade_date"])

    async def limits(self, code: str, day: date) -> dict[str, Any] | None:
        symbol = code + (".SH" if code.startswith("6") else ".SZ")
        key = ("limit", day, code)
        if key not in self.cache:
            response = await self.service._scan_state.realtime_client._api_call(
                "stk_limit",
                {"ts_code": symbol, "trade_date": day.strftime("%Y%m%d")},
                fields="ts_code,trade_date,pre_close,up_limit,down_limit",
            )
            body = response["data"]
            rows = [dict(zip(body["fields"], row, strict=True)) for row in body["items"]]
            if not rows:
                return None
            if (
                len(rows) != 1
                or rows[0]["ts_code"] != symbol
                or rows[0]["trade_date"] != day.strftime("%Y%m%d")
            ):
                raise ValueError("V22 current price limits are misbound")
            self.cache[key] = rows
        return self.cache[key][0]

    async def danger(self, entry: date, d1: date, now: datetime) -> bool:
        # The frozen V22 rule requires a D1 observation already usable at D1
        # 09:40. Do not silently substitute the legacy V20's later D2 selection.
        async with self.service._repository.pool.acquire() as connection:
            row = await connection.fetchrow(
                _STRICT_CANDIDATE_SQL.format(schema=self.service._repository.schema)
                + " AND snapshot.generated_at < $3 AND snapshot.receipt_sealed_at < $3 "
                "ORDER BY snapshot.receipt_sealed_at DESC,snapshot.snapshot_id DESC LIMIT 1",
                entry,
                d1,
                datetime.combine(d1, time(9, 40), tzinfo=now.tzinfo),
            )
        if row is None:
            return False  # Frozen missing-MEWS compatibility uses the ordinary 12% rule.
        _, state, _ = _snapshot_from_row(
            row,
            cutoff=datetime.combine(d1, time(9, 40), tzinfo=now.tzinfo),
            source_trade_date=entry,
            availability_date=d1,
            source_must_precede=d1,
        )
        return state == "DANGER"

    async def position(
        self, position: dict[str, Any], calendar: tuple[date, ...], now: datetime
    ) -> None:
        entry_day = date.fromisoformat(position["entry_date"])
        if entry_day not in calendar or entry_day > now.date():
            raise ValueError("V22 buy date lacks an exchange calendar session")
        start = calendar.index(entry_day)
        if len(calendar) <= start + 3:
            raise ValueError("V22 exit calendar lacks the following three sessions")
        code = position["code"]
        price = position["entry_price"]
        reference = None
        if price is None:
            entry_bars = await self.bars(code, entry_day, now)
            reference_bars = [
                b for b in entry_bars if b.at.time().replace(tzinfo=None) == time(9, 41) and b.valid
            ]
            if len(reference_bars) != 1:
                return
            reference = price = reference_bars[0].open
        if now.date() == entry_day:
            if reference is not None:
                await self.store.apply(position, reference=reference)
            return
        daily = await self.daily(code, entry_day, now)
        by_day = {
            date.fromisoformat(
                f"{r['trade_date'][:4]}-{r['trade_date'][4:6]}-{r['trade_date'][6:]}"
            ): r
            for r in daily
        }
        extended = bool(position["extended"])
        for phase, day in enumerate(calendar[start + 1 :], start=1):
            if day > now.date():
                break
            if position["extended"] and phase <= 2:
                continue
            if phase > 2 and not extended:
                continue
            bars = await self.bars(code, day, now)
            as_of = now if day == now.date() else datetime.combine(day, time(15), tzinfo=now.tzinfo)
            limit = await self.limits(code, day) if bars else None
            prior = [r for r in daily if r["trade_date"] < day.strftime("%Y%m%d")]
            before = float("nan")
            if entry_day in by_day:
                before = float(by_day[entry_day]["close"]) / price
                for earlier in calendar[start + 1 : start + phase]:
                    if earlier not in by_day:
                        before = float("nan")
                        break
                    r = by_day[earlier]
                    before *= float(r["close"]) / float(r["pre_close"])
            signal, extended = evaluate_day(
                bars,
                phase=phase,
                now=as_of,
                entry=price,
                prior=prior,
                pre_close=float(limit["pre_close"]) if limit else float("nan"),
                before_factor=before,
                up_limit=float(limit["up_limit"]) if limit else float("nan"),
                danger=await self.danger(entry_day, calendar[start + 1], now)
                if phase == 2
                else False,
                extended=extended,
            )
            if signal is not None:
                event_id = await self.store.apply(
                    position, reference=reference, extended=extended, signal=signal
                )
                if event_id is not None:
                    await self.service._repository.seal_event(event_id, seal_v20_payload)
                return
        if reference is not None or extended != position["extended"]:
            await self.store.apply(position, reference=reference, extended=extended)

    async def run(self, now: datetime) -> None:
        key = now.strftime("%Y%m%d%H%M")
        if self.minute != key:
            self.cache = {k: v for k, v in self.cache.items() if k[0] != "rt"}
            self.minute = key
        positions = await self.store.list(active=True)
        if not positions:
            self.cache.clear()
            return
        calendar = tuple(await self.service._load_trade_calendar(now.date()))
        # Small held-stock requests, never a second full-market acquisition.
        # One bad stock must not prevent alerts for the remaining holdings.
        errors = []
        semaphore = asyncio.Semaphore(3)

        async def check(position: dict[str, Any]) -> None:
            try:
                async with semaphore:
                    await self.position(position, calendar, now)
            except Exception as exc:
                errors.append(f"{position['code']}: {type(exc).__name__}: {exc}")

        await asyncio.gather(*(check(position) for position in positions))
        if errors:
            raise ValueError("V22 exit monitoring input failed: " + "; ".join(errors))
