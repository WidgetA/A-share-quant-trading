"""Point-in-time ordinary A-share universe rules shared by ingestion and backtest."""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

_ALLOWED_MARKETS = {"主板", "创业板", "科创板"}
_SSE_PREFIXES = ("600", "601", "603", "605", "688")
_SZSE_PREFIXES = ("000", "001", "002", "003", "300", "301")


def is_ordinary_a_stock(row: Mapping[str, Any]) -> bool:
    """Strictly admit SSE/SZSE main-board, ChiNext and STAR ordinary shares."""

    exchange = str(row.get("exchange") or "").upper()
    market = str(row.get("market") or "")
    symbol = str(row.get("symbol") or str(row.get("ts_code") or "").split(".")[0])
    name = str(row.get("name") or "").upper()
    ts_code = str(row.get("ts_code") or "").upper()
    if exchange not in {"SSE", "SZSE"} or market not in _ALLOWED_MARKETS:
        return False
    if ts_code.endswith(".BJ") or "CDR" in name or market.upper() == "CDR":
        return False
    if exchange == "SSE":
        return symbol.startswith(_SSE_PREFIXES)
    return symbol.startswith(_SZSE_PREFIXES)


def is_active_on(row: Mapping[str, Any], day: date) -> bool:
    if not bool(row.get("is_ordinary_a", is_ordinary_a_stock(row))):
        return False
    list_date = row.get("list_date")
    delist_date = row.get("delist_date")
    # Without a verified listing boundary we cannot safely include the stock
    # in a point-in-time universe (doing so could fill into its pre-list era).
    if not isinstance(list_date, date):
        return False
    if day < list_date:
        return False
    if isinstance(delist_date, date) and day > delist_date:
        return False
    return True


def is_shenzhen_or_shanghai_code(ts_code: str) -> bool:
    upper = ts_code.upper()
    return upper.endswith(".SH") or upper.endswith(".SZ")
