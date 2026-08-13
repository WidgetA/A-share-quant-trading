#!/usr/bin/env python3
"""Deterministic CLI used by the Kimi price-alert skill."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx

# Kimi runs from a scratch working directory. Resolve the real repository root
# from this bundled script so imports work in both local development and /app.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.assistant.price_alerts import (  # noqa: E402
    PriceAlertStore,
    normalize_stock_code,
    price_to_fen,
)


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def _fetch_holdings() -> list[dict[str, Any]]:
    api_base = os.environ.get("ASSISTANT_API_BASE", "").strip().rstrip("/")
    readonly_key = os.environ.get("ASSISTANT_READONLY_KEY", "").strip()
    if not api_base:
        raise RuntimeError("助手没有配置本机只读接口地址")
    if not readonly_key:
        raise RuntimeError("助手没有配置只读查询钥匙")
    try:
        response = httpx.get(
            f"{api_base}/api/trading/holdings",
            headers={"X-API-Key": readonly_key},
            timeout=15.0,
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
        raise RuntimeError(f"持仓接口返回 {exc.response.status_code}") from exc
    except (httpx.HTTPError, ValueError) as exc:
        raise RuntimeError(f"持仓接口没查通: {exc}") from exc
    holdings = payload.get("holdings") if isinstance(payload, dict) else None
    if not isinstance(holdings, list):
        raise RuntimeError("持仓接口返回格式不对")
    return [item for item in holdings if isinstance(item, dict)]


def _resolve_holding(stock: str, holdings: list[dict[str, Any]]) -> dict[str, Any]:
    query = (stock or "").strip()
    if not query:
        raise ValueError("没有说要监控哪只股票")
    bare_query = normalize_stock_code(query)
    if len(bare_query) == 6 and bare_query.isdigit():
        matched = [
            item
            for item in holdings
            if normalize_stock_code(str(item.get("code") or "")) == bare_query
        ]
    else:
        folded = query.casefold().replace(" ", "")
        exact = [
            item
            for item in holdings
            if str(item.get("name") or "").casefold().replace(" ", "") == folded
        ]
        matched = exact or [
            item
            for item in holdings
            if folded in str(item.get("name") or "").casefold().replace(" ", "")
        ]
    if not matched:
        names = "、".join(
            f"{normalize_stock_code(str(item.get('code') or ''))} {item.get('name') or ''}"
            for item in holdings[:12]
        )
        suffix = f"；当前持仓有: {names}" if names else "；当前没有持仓"
        raise ValueError(f"当前持仓里没找到“{query}”{suffix}")
    if len(matched) > 1:
        choices = "、".join(
            f"{normalize_stock_code(str(item.get('code') or ''))} {item.get('name') or ''}"
            for item in matched
        )
        raise ValueError(f"“{query}”匹配到多只持仓，请改用六位代码: {choices}")
    return matched[0]


def _resolve_alert_stock(stock: str, alerts: list[dict[str, Any]]) -> str:
    query = (stock or "").strip()
    bare_query = normalize_stock_code(query)
    if len(bare_query) == 6 and bare_query.isdigit():
        codes = {item["stock_code"] for item in alerts if item["stock_code"] == bare_query}
    else:
        folded = query.casefold().replace(" ", "")
        exact = {
            item["stock_code"]
            for item in alerts
            if str(item.get("stock_name") or "").casefold().replace(" ", "") == folded
        }
        codes = exact or {
            item["stock_code"]
            for item in alerts
            if folded in str(item.get("stock_name") or "").casefold().replace(" ", "")
        }
    if not codes:
        raise ValueError(f"没有找到“{query}”对应的生效中预警")
    if len(codes) > 1:
        raise ValueError(f"“{query}”对应多只股票，请改用六位代码")
    return next(iter(codes))


def _create(args: argparse.Namespace, store: PriceAlertStore) -> dict[str, Any]:
    # Validate before making the holdings request so obvious malformed input is cheap.
    price_to_fen(args.price)
    holding = _resolve_holding(args.stock, _fetch_holdings())
    code = normalize_stock_code(str(holding.get("code") or ""))
    name = str(holding.get("name") or code).strip() or code
    alert, created = store.create_alert(
        stock_code=code,
        stock_name=name,
        direction=args.direction,
        threshold=args.price,
    )
    current_price = holding.get("last_price")
    already_matches = False
    try:
        current_fen = price_to_fen(current_price)
        threshold_fen = price_to_fen(args.price)
        already_matches = (args.direction == "below" and current_fen <= threshold_fen) or (
            args.direction == "above" and current_fen >= threshold_fen
        )
    except ValueError:
        current_price = None
    return {
        "ok": True,
        "action": "created" if created else "already_exists",
        "created": created,
        "alert": alert,
        "current_price": current_price,
        "already_matches": already_matches,
        "note": (
            "当前价已经满足条件，将在下一次有效交易时段行情刷新时触发"
            if already_matches
            else "服务端已开始监控"
        ),
    }


def _list(args: argparse.Namespace, store: PriceAlertStore) -> dict[str, Any]:
    status = None if args.status == "all" else args.status
    alerts = store.list_alerts(status=status)
    return {"ok": True, "action": "list", "status": args.status, "alerts": alerts}


def _cancel(args: argparse.Namespace, store: PriceAlertStore) -> dict[str, Any]:
    if args.alert_id:
        count = store.cancel_alerts(alert_id=args.alert_id)
        target = args.alert_id
    else:
        active = store.list_alerts(status="active")
        code = _resolve_alert_stock(args.stock, active)
        count = store.cancel_alerts(stock_code=code)
        target = code
    return {"ok": True, "action": "cancel", "target": target, "cancelled": count}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="管理持仓价格飞书预警")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="创建一次性价格预警")
    create.add_argument("--stock", required=True, help="当前持仓的名称或六位代码")
    create.add_argument("--direction", required=True, choices=("below", "above"))
    create.add_argument("--price", required=True)

    listing = sub.add_parser("list", help="查看预警")
    listing.add_argument(
        "--status", choices=("active", "triggered", "cancelled", "all"), default="active"
    )

    cancel = sub.add_parser("cancel", help="取消生效中的预警")
    group = cancel.add_mutually_exclusive_group(required=True)
    group.add_argument("--id", dest="alert_id", help="预警编号")
    group.add_argument("--stock", help="股票名称或六位代码；取消该股票全部生效规则")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = PriceAlertStore()
    try:
        if args.command == "create":
            result = _create(args, store)
        elif args.command == "list":
            result = _list(args, store)
        else:
            result = _cancel(args, store)
    except Exception as exc:
        _emit({"ok": False, "error": str(exc)})
        return 2
    _emit(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
