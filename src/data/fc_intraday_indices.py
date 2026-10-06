"""Signed native US FC minute fetches, parsed from the full original source domestically."""

import uuid
from typing import Any

from src.data.fc_yahoo_indices import FCYahooIndexClient
from src.data.yahoo_intraday_indices import INTERVAL_SECONDS, parse_intraday_chart


class FCYahooIntradayIndexClient(FCYahooIndexClient):
    """Share official SDK authentication and finite retries with the daily adapter."""

    def _parse_source(self, source: dict, payload: dict, fetched_at: int):
        return parse_intraday_chart(
            source,
            symbol=payload["symbol"],
            market=payload["market"],
            fetched_at=fetched_at,
            interval=payload["interval"],
            start=payload["start"],
            end=payload["end"],
        )

    async def fetch(
        self,
        symbol: str,
        market: str,
        *,
        start: int,
        end: int,
        interval: str = "1m",
    ) -> dict[str, Any]:
        if (
            not isinstance(symbol, str)
            or not symbol
            or market not in ("US", "KR")
            or type(start) is not int
            or start < 0
            or type(end) is not int
            or end <= start
            or interval not in INTERVAL_SECONDS
        ):
            raise ValueError("Invalid FC minute source window")
        payload = {
            "schema_version": 1,
            "request_id": uuid.uuid4().hex,
            "symbol": symbol,
            "market": market,
            "start": start,
            "end": end,
            "interval": interval,
            "capability": "hour_history" if interval == "1h" else "minute_history",
        }
        return await self._fetch_payload(payload)
