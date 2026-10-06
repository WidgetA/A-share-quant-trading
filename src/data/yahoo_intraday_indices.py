"""Preserve actual Yahoo intraday index observations without synthesizing bars."""

from __future__ import annotations

import struct
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from src.data.yahoo_indices import YahooChartTransport, YahooIndexError, _number

INTERVAL_SECONDS = {"1m": 60, "5m": 300}


def parse_intraday_chart(
    payload: dict,
    *,
    symbol: str,
    market: str,
    fetched_at: int,
    interval: str = "1m",
    start: int | None = None,
    end: int | None = None,
) -> tuple[dict, list[dict]]:
    """Validate source granularity/identity, keep every timestamp and explicit null.

    All-zero OHL represents missing prices. Its actual volume field is preserved,
    including zero; this is a provider observation, not a claim of traded quantity.
    Later source timestamps establish ended bars. Wall-clock close does not.
    """
    if interval not in INTERVAL_SECONDS:
        raise YahooIndexError("Unsupported intraday interval")
    if type(fetched_at) is not int or not 0 < fetched_at < 2**63:
        raise YahooIndexError("Invalid fetch timestamp milliseconds")
    if (start is not None or end is not None) and (
        type(start) is not int or start < 0 or type(end) is not int or end <= start
    ):
        raise YahooIndexError("Invalid requested intraday window")
    chart = payload.get("chart") if isinstance(payload, dict) else None
    if not isinstance(chart, dict) or chart.get("error"):
        raise YahooIndexError("Yahoo returned no usable intraday chart")
    results = chart.get("result")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        raise YahooIndexError("No unique intraday chart result")
    result = results[0]
    meta = result.get("meta")
    if (
        not isinstance(meta, dict)
        or meta.get("symbol") != symbol
        or meta.get("instrumentType") != "INDEX"
    ):
        raise YahooIndexError("Intraday identity does not match the requested index")
    expected = {"US": ("USD", "America/"), "KR": ("KRW", "Asia/Seoul")}
    if market not in expected:
        raise YahooIndexError("Unsupported market")
    currency, timezone_prefix = expected[market]
    timezone_name = meta.get("exchangeTimezoneName")
    if (
        meta.get("currency") != currency
        or not isinstance(timezone_name, str)
        or (
            timezone_name != timezone_prefix
            if market == "KR"
            else not timezone_name.startswith(timezone_prefix)
        )
    ):
        raise YahooIndexError("Intraday currency/timezone differs from the mapped market")
    timezone = ZoneInfo(timezone_name)
    if meta.get("dataGranularity") != interval:
        raise YahooIndexError("Returned granularity is not the requested intraday interval")
    indicators = result.get("indicators")
    quotes = indicators.get("quote") if isinstance(indicators, dict) else None
    if not isinstance(quotes, list) or len(quotes) != 1 or not isinstance(quotes[0], dict):
        raise YahooIndexError("No unique intraday quote arrays")
    series = quotes[0]
    timestamps = result.get("timestamp")
    if timestamps is None or timestamps == []:
        # Observed HTTP200 weekend response: verified INDEX metadata, no source
        # timestamps, quote=[{}]. Metadata's latest price is not a window bar.
        if series or indicators.get("adjclose") not in (None, []):
            raise YahooIndexError("Intraday arrays without source timestamps")
        return meta, []
    if (
        not isinstance(timestamps, list)
        or not timestamps
        or any(type(stamp) is not int or stamp <= 0 for stamp in timestamps)
    ):
        raise YahooIndexError("No valid source timestamps")
    if timestamps != sorted(timestamps) or len(set(timestamps)) != len(timestamps):
        raise YahooIndexError("Intraday timestamps must be chronological and unique")
    seconds = INTERVAL_SECONDS[interval]
    unaligned = [index for index, stamp in enumerate(timestamps) if stamp % seconds]
    appended_quote = bool(unaligned)
    if unaligned and not (
        unaligned == [len(timestamps) - 1]
        and type(meta.get("regularMarketTime")) is int
        and timestamps[-1] == meta["regularMarketTime"]
    ):
        raise YahooIndexError("Non-aligned intraday history is not a supported minute bar")
    for key in ("open", "high", "low", "close", "volume"):
        values = series.get(key)
        if values is not None and (not isinstance(values, list) or len(values) != len(timestamps)):
            raise YahooIndexError(f"Misaligned intraday {key} array")
    adjusted_sets = indicators.get("adjclose")
    if adjusted_sets is None or adjusted_sets == []:
        adjusted = None
    elif (
        isinstance(adjusted_sets, list)
        and len(adjusted_sets) == 1
        and isinstance(adjusted_sets[0], dict)
    ):
        adjusted = adjusted_sets[0].get("adjclose")
        if adjusted is not None and (
            not isinstance(adjusted, list) or len(adjusted) != len(timestamps)
        ):
            raise YahooIndexError("Misaligned intraday adjusted-close array")
    else:
        raise YahooIndexError("Invalid intraday adjusted-close arrays")

    def value(key: str, index: int) -> float | None:
        array = series.get(key)
        return _number(array[index]) if array is not None else None

    quote_time = meta.get("regularMarketTime")
    tail = timestamps[-1]
    published_ends = [
        session["end"]
        for group in (meta.get("tradingPeriods") or [])
        if isinstance(group, list)
        for session in group
        if isinstance(session, dict) and type(session.get("end")) is int
    ]
    published_close_clock = (
        type(quote_time) is int
        and bool(published_ends)
        and max(published_ends) == tail
        and quote_time >= tail
        and datetime.fromtimestamp(quote_time, timezone).date()
        == datetime.fromtimestamp(tail, timezone).date()
    )

    def latest_quote_shape() -> bool:
        latest_close = value("close", len(timestamps) - 1)
        latest_price = _number(meta.get("regularMarketPrice"))
        # Observed KR session-close quotes encode metadata's decimal latest
        # price as float32 OHLC. This is a source encoding, not an epsilon.
        price_matches = latest_price is not None and latest_close == latest_price
        if latest_price is not None and not price_matches:
            try:
                price_matches = (
                    latest_close == struct.unpack("!f", struct.pack("!f", latest_price))[0]
                )
            except (OverflowError, struct.error) as exc:
                raise YahooIndexError("Invalid latest intraday metadata price") from exc
        return (
            price_matches
            and latest_close is not None
            and all(
                value(field, len(timestamps) - 1) == latest_close
                for field in ("open", "high", "low")
            )
            and value("volume", len(timestamps) - 1) == 0
        )

    # The observed KR closing quote remains a quote when a wider request
    # contains it. This inside-window pattern has not been established for US.
    if market == "KR" and not appended_quote and published_close_clock and latest_quote_shape():
        appended_quote = True
    if start is not None:
        outside = [
            index
            for index, stamp in enumerate(timestamps)
            if not start - seconds <= stamp < end + seconds
        ]
        if outside:
            if outside != [len(timestamps) - 1]:
                raise YahooIndexError("Intraday history is outside the requested window")
            if not appended_quote:
                source_clock = type(quote_time) is int and (
                    quote_time == tail or published_close_clock
                )
                if not (source_clock and latest_quote_shape()):
                    raise YahooIndexError("Unproven latest point outside the requested window")
                appended_quote = True
    bar_times = timestamps[:-1] if appended_quote else timestamps
    periods = meta.get("currentTradingPeriod", {})
    regular = periods.get("regular", {}) if isinstance(periods, dict) else {}
    regular = regular if isinstance(regular, dict) else {}
    session_start, session_end = regular.get("start"), regular.get("end")
    points = []
    for index, stamp in enumerate(timestamps):
        snapshot = appended_quote and index == len(timestamps) - 1
        close = value("close", index)
        if close is not None and close <= 0:
            raise YahooIndexError("Invalid intraday index close")
        opening, high, low = (value(key, index) for key in ("open", "high", "low"))
        if all(item in (None, 0.0) for item in (opening, high, low)):
            opening = high = low = None
        elif any(item is not None and item <= 0 for item in (opening, high, low)):
            raise YahooIndexError("Invalid intraday OHLC")
        known = [item for item in (opening, close, high, low) if item is not None]
        if (high is not None and high < max(known)) or (low is not None and low > min(known)):
            raise YahooIndexError("Inconsistent intraday OHLC")
        volume = value("volume", index)
        if volume is not None and volume < 0:
            raise YahooIndexError("Negative intraday volume")
        adj = _number(adjusted[index]) if adjusted is not None else None
        if adj is not None and adj <= 0:
            raise YahooIndexError("Invalid intraday adjusted close")
        final = None
        if not snapshot and close is not None:
            if bar_times[-1] >= stamp + seconds:
                final = True
            elif (
                type(quote_time) is int
                and type(session_start) is int
                and type(session_end) is int
                and session_start <= quote_time < session_end
                and stamp <= quote_time < stamp + seconds
            ):
                final = False
        points.append(
            {
                "provider": "yahoo",
                "market": market,
                "symbol": symbol,
                "interval": interval,
                "data_kind": "minute_quote_snapshot" if snapshot else "minute_bar",
                "ts": stamp * 1000,
                "name": meta.get("longName") or meta.get("shortName") or symbol,
                "currency": currency,
                "exchange_timezone": timezone_name,
                "trade_date": datetime.fromtimestamp(stamp, timezone).date().isoformat(),
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "adjusted_close": adj,
                "volume": volume,
                "is_final": final,
                "fetched_at": fetched_at,
            }
        )
    return meta, points


class YahooIntradayIndexClient:
    """Explicit-window minute requests; an optional borrowed transport shares cooldown."""

    def __init__(
        self,
        *,
        proxy: str | None = None,
        client: httpx.AsyncClient | None = None,
        transport: YahooChartTransport | None = None,
    ):
        if transport is not None and (proxy is not None or client is not None):
            raise ValueError(
                "A borrowed transport cannot be combined with another HTTP client/proxy"
            )
        self._transport = transport or YahooChartTransport(proxy=proxy, client=client)
        self._owns_transport = transport is None

    async def aclose(self) -> None:
        if self._owns_transport:
            await self._transport.aclose()

    async def fetch(
        self,
        symbol: str,
        market: str,
        *,
        start: int,
        end: int | None = None,
        interval: str = "1m",
    ) -> dict[str, Any]:
        end = int(time.time()) if end is None else end
        if (
            not isinstance(symbol, str)
            or not symbol
            or market not in {"US", "KR"}
            or interval not in INTERVAL_SECONDS
            or type(start) is not int
            or start < 0
            or type(end) is not int
            or end <= start
        ):
            raise ValueError("Invalid intraday source window")
        source = await self._transport.fetch_chart(
            symbol,
            {
                "interval": interval,
                "period1": str(start),
                "period2": str(end),
            },
        )
        metadata, points = parse_intraday_chart(
            source.pop("payload"),
            symbol=symbol,
            market=market,
            fetched_at=source["fetched_at"],
            interval=interval,
            start=start,
            end=end,
        )
        return {
            "metadata": metadata,
            "points": points,
            "missing_close_timestamps": [p["ts"] for p in points if p["close"] is None],
            "source_empty": not points,
            "requested_range": {"start": start, "end": end, "interval": interval},
            **source,
        }
