#!/usr/bin/env python3
"""Cross-check today's KOSPI/KOSDAQ snapshot at a Beijing-time minute.

No third-party packages are required. Naver and Yahoo must agree on the Korean
session date, previous close, official open, and requested minute's opening
level. Any missing/stale/conflicting input produces ABSTAIN.
"""

from __future__ import annotations

import argparse
import json
import math
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from datetime import time as wall_time
from typing import Any
from zoneinfo import ZoneInfo

BEIJING = ZoneInfo("Asia/Shanghai")
SEOUL = ZoneInfo("Asia/Seoul")
INDEXES = {
    "kospi": {"label": "KOSPI", "naver": "KOSPI", "yahoo": "^KS11"},
    "kosdaq": {"label": "KOSDAQ", "naver": "KOSDAQ", "yahoo": "^KQ11"},
}
TOLERANCE_POINTS = 0.05


class DataError(RuntimeError):
    pass


def positive_number(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DataError(f"{field}不是有效数字: {value!r}") from exc
    if not math.isfinite(number) or number <= 0:
        raise DataError(f"{field}不是有效正数: {value!r}")
    return number


def request_json(url: str, timeout: float, referer: str | None = None) -> dict[str, Any]:
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "Mozilla/5.0 (compatible; korea-market-skill/1.0)",
    }
    if referer:
        headers["Referer"] = referer
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                payload = json.loads(response.read().decode(charset))
            if not isinstance(payload, dict):
                raise DataError("接口返回的不是JSON对象")
            return payload
        except Exception as exc:  # network boundary
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
    raise DataError(f"网络请求失败: {type(last_error).__name__}: {last_error}")


def fetch_naver(index_key: str, symbol: str, timeout: float) -> dict[str, Any]:
    url = (
        "https://api.stock.naver.com/chart/domestic/index/"
        f"{urllib.parse.quote(symbol, safe='')}?periodType=day"
    )
    return {
        "index_key": index_key,
        "provider": "naver",
        "url": url,
        "payload": request_json(url, timeout, "https://finance.naver.com/"),
    }


def fetch_yahoo(index_key: str, symbol: str, timeout: float) -> dict[str, Any]:
    encoded = urllib.parse.quote(symbol, safe="")
    minute_url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
        "?range=5d&interval=1m&includePrePost=false&events=history"
    )
    daily_url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
        "?range=1mo&interval=1d&includePrePost=false&events=history"
    )
    return {
        "index_key": index_key,
        "provider": "yahoo",
        "urls": [minute_url, daily_url],
        "payload": {
            "minute": request_json(minute_url, timeout),
            "daily": request_json(daily_url, timeout),
        },
    }


def fetch_sources(timeout: float) -> tuple[dict[str, dict[str, Any]], list[str]]:
    jobs = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for index_key, config in INDEXES.items():
            jobs.append(pool.submit(fetch_naver, index_key, config["naver"], timeout))
            jobs.append(pool.submit(fetch_yahoo, index_key, config["yahoo"], timeout))
        results: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        for future in as_completed(jobs):
            try:
                item = future.result()
                results[f"{item['provider']}:{item['index_key']}"] = item
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")
    return results, errors


def naver_values(payload: dict[str, Any], target_kst: datetime) -> dict[str, Any]:
    target_day = target_kst.strftime("%Y%m%d")
    target_minute = target_kst.strftime("%Y%m%d%H%M00")
    if str(payload.get("tradeBaseAt", "")) != target_day:
        raise DataError(f"Naver交易日陈旧: {payload.get('tradeBaseAt')} != {target_day}")
    rows = {str(row.get("localDateTime")): row for row in payload.get("priceInfos", [])}
    target_row = rows.get(target_minute)
    if target_row is None:
        raise DataError(f"Naver尚无{target_kst.strftime('%H:%M')} KST分钟数据")
    return {
        "target_date": target_kst.date().isoformat(),
        "previous_date": datetime.strptime(str(payload.get("lastTradeBaseAt")), "%Y%m%d")
        .date()
        .isoformat(),
        "previous_close": positive_number(payload.get("lastClosePrice"), "Naver前收"),
        "official_open": positive_number(payload.get("openPrice"), "Naver正式开盘"),
        "snapshot_open": positive_number(target_row.get("openPrice"), "Naver指定分钟开盘"),
        "snapshot_close": positive_number(target_row.get("currentPrice"), "Naver指定分钟收盘"),
        "market_status": str(payload.get("marketStatus", "")),
        "provider_time": str(payload.get("localDateTimeNow", "")),
    }


def yahoo_values(payload: dict[str, Any], target_kst: datetime) -> dict[str, Any]:
    minute_payload = payload.get("minute") or {}
    daily_payload = payload.get("daily") or {}
    chart = minute_payload.get("chart", {})
    if chart.get("error") is not None or not chart.get("result"):
        raise DataError(f"Yahoo行情响应无效: {chart.get('error')}")
    result = chart["result"][0]
    meta = result.get("meta", {})
    if meta.get("exchangeTimezoneName") != "Asia/Seoul":
        raise DataError(f"Yahoo时区异常: {meta.get('exchangeTimezoneName')}")
    timestamps = result.get("timestamp") or []
    quotes = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    opens = quotes.get("open") or []
    closes = quotes.get("close") or []

    target_date = target_kst.date()
    exact: tuple[float, float] | None = None
    official_open: float | None = None
    prior_dates: set[date] = set()
    for index, epoch in enumerate(timestamps):
        local = datetime.fromtimestamp(int(epoch), SEOUL)
        if local.date() < target_date:
            prior_dates.add(local.date())
        if local.date() != target_date or local.second != 0:
            continue
        if index >= len(opens) or opens[index] is None:
            continue
        open_value = positive_number(opens[index], "Yahoo分钟开盘")
        close_value = (
            positive_number(closes[index], "Yahoo分钟收盘")
            if index < len(closes) and closes[index] is not None
            else open_value
        )
        if (local.hour, local.minute) == (9, 0):
            official_open = open_value
        if local.replace(second=0, microsecond=0) == target_kst.replace(second=0, microsecond=0):
            exact = (open_value, close_value)

    if not prior_dates:
        raise DataError("Yahoo分钟线无法确认前一韩国交易日")
    if official_open is None:
        raise DataError("Yahoo缺少09:00 KST正式开盘bar")
    if exact is None:
        raise DataError(f"Yahoo尚无{target_kst.strftime('%H:%M')} KST分钟数据")

    previous_date = max(prior_dates)
    daily_chart = daily_payload.get("chart", {})
    daily_results = daily_chart.get("result") or []
    daily_previous_close: float | None = None
    if daily_chart.get("error") is None and daily_results:
        daily_result = daily_results[0]
        daily_timestamps = daily_result.get("timestamp") or []
        daily_quotes = ((daily_result.get("indicators") or {}).get("quote") or [{}])[0]
        daily_closes = daily_quotes.get("close") or []
        for index, epoch in enumerate(daily_timestamps):
            session_date = datetime.fromtimestamp(int(epoch), SEOUL).date()
            if (
                session_date == previous_date
                and index < len(daily_closes)
                and daily_closes[index] is not None
            ):
                daily_previous_close = positive_number(daily_closes[index], "Yahoo对应日期日线前收")
                break

    if daily_previous_close is not None:
        previous_close = daily_previous_close
        previous_close_source = "daily_exact_date"
    else:
        previous_close = positive_number(meta.get("previousClose"), "Yahoo分钟元数据前收")
        previous_close_source = "minute_metadata_fallback"
    return {
        "target_date": target_date.isoformat(),
        "previous_date": previous_date.isoformat(),
        "previous_close": previous_close,
        "previous_close_source": previous_close_source,
        "official_open": official_open,
        "snapshot_open": exact[0],
        "snapshot_close": exact[1],
        "provider_time": meta.get("regularMarketTime"),
    }


def evaluate_index(
    label: str,
    naver: dict[str, Any],
    yahoo: dict[str, Any],
    tolerance: float = TOLERANCE_POINTS,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    if naver["target_date"] != yahoo["target_date"]:
        errors.append(
            f"{label}双源目标日期不一致: {naver['target_date']} vs {yahoo['target_date']}"
        )
    if naver["previous_date"] != yahoo["previous_date"]:
        errors.append(
            f"{label}双源前一交易日不一致: {naver['previous_date']} vs {yahoo['previous_date']}"
        )

    fields = {
        "前收": "previous_close",
        "正式开盘": "official_open",
        "指定分钟": "snapshot_open",
    }
    differences: dict[str, float] = {}
    for chinese, field in fields.items():
        difference = abs(float(naver[field]) - float(yahoo[field]))
        differences[field] = difference
        if difference > tolerance:
            errors.append(f"{label}双源{chinese}差{difference:.4f}点，超过{tolerance:.4f}点")

    previous_close = float(naver["previous_close"])
    official_open = float(naver["official_open"])
    snapshot = float(naver["snapshot_open"])
    open_gap_pct = (official_open / previous_close - 1.0) * 100.0
    snapshot_change_pct = (snapshot / previous_close - 1.0) * 100.0
    yahoo_open_gap_pct = (
        float(yahoo["official_open"]) / float(yahoo["previous_close"]) - 1.0
    ) * 100.0
    yahoo_snapshot_pct = (
        float(yahoo["snapshot_open"]) / float(yahoo["previous_close"]) - 1.0
    ) * 100.0
    if (open_gap_pct > 0) != (yahoo_open_gap_pct > 0):
        errors.append(f"{label}双源正式开盘红绿方向不一致")
    if (snapshot_change_pct > 0) != (yahoo_snapshot_pct > 0):
        errors.append(f"{label}双源指定时刻红绿方向不一致")

    return {
        "label": label,
        "previous_date": naver["previous_date"],
        "previous_close": previous_close,
        "official_open": official_open,
        "open_gap_pct": open_gap_pct,
        "snapshot_level": snapshot,
        "snapshot_change_pct": snapshot_change_pct,
        "snapshot_red": snapshot_change_pct > 0,
        "open_red": open_gap_pct > 0,
        "cross_source_differences": differences,
    }, errors


def evaluate(
    sources: dict[str, dict[str, Any]], target_bjt: datetime, fetch_errors: list[str]
) -> dict[str, Any]:
    errors = list(fetch_errors)
    indices: dict[str, Any] = {}
    target_kst = target_bjt.astimezone(SEOUL)
    for index_key, config in INDEXES.items():
        naver_item = sources.get(f"naver:{index_key}")
        yahoo_item = sources.get(f"yahoo:{index_key}")
        if naver_item is None or yahoo_item is None:
            errors.append(f"{config['label']}缺少Naver或Yahoo来源")
            continue
        try:
            naver = naver_values(naver_item["payload"], target_kst)
            yahoo = yahoo_values(yahoo_item["payload"], target_kst)
            result, index_errors = evaluate_index(config["label"], naver, yahoo)
            indices[index_key] = result
            errors.extend(index_errors)
        except Exception as exc:
            errors.append(f"{config['label']}: {type(exc).__name__}: {exc}")

    ok = not errors and len(indices) == len(INDEXES)
    return {
        "status": "OK" if ok else "ABSTAIN",
        "requested_beijing_time": target_bjt.isoformat(timespec="minutes"),
        "corresponding_seoul_time": target_kst.isoformat(timespec="minutes"),
        "indices": indices,
        "snapshot_both_red": (all(row["snapshot_red"] for row in indices.values()) if ok else None),
        "opening_double_red": (all(row["open_red"] for row in indices.values()) if ok else None),
        "source_check": "PASS" if ok else "FAIL_CLOSED",
        "sources": ["Naver Finance intraday", "Yahoo Finance 1-minute chart"],
        "errors": errors,
    }


def latest_common_target(sources: dict[str, dict[str, Any]], target_date: date) -> datetime:
    common_by_index: list[set[str]] = []
    target_day = target_date.strftime("%Y%m%d")
    for index_key in INDEXES:
        naver_item = sources.get(f"naver:{index_key}")
        yahoo_item = sources.get(f"yahoo:{index_key}")
        if naver_item is None or yahoo_item is None:
            raise DataError("缺少Naver或Yahoo来源，无法确定最新共同分钟")
        naver_payload = naver_item["payload"]
        if str(naver_payload.get("tradeBaseAt", "")) != target_day:
            raise DataError(f"Naver尚无{target_date.isoformat()}交易日数据")
        naver_minutes = {
            str(row.get("localDateTime"))
            for row in naver_payload.get("priceInfos", [])
            if str(row.get("localDateTime", "")).startswith(target_day)
            and row.get("openPrice") is not None
        }

        minute_payload = (yahoo_item["payload"] or {}).get("minute") or {}
        results = (minute_payload.get("chart") or {}).get("result") or []
        if not results:
            raise DataError("Yahoo分钟响应为空，无法确定最新共同分钟")
        result = results[0]
        timestamps = result.get("timestamp") or []
        quotes = ((result.get("indicators") or {}).get("quote") or [{}])[0]
        opens = quotes.get("open") or []
        yahoo_minutes: set[str] = set()
        for index, epoch in enumerate(timestamps):
            local = datetime.fromtimestamp(int(epoch), SEOUL)
            if (
                local.date() == target_date
                and local.second == 0
                and index < len(opens)
                and opens[index] is not None
            ):
                yahoo_minutes.add(local.strftime("%Y%m%d%H%M00"))
        common_by_index.append(naver_minutes & yahoo_minutes)

    common = set.intersection(*common_by_index) if common_by_index else set()
    if not common:
        raise DataError("今天尚无KOSPI/KOSDAQ双源共同分钟")
    latest_kst = datetime.strptime(max(common), "%Y%m%d%H%M%S").replace(tzinfo=SEOUL)
    return latest_kst.astimezone(BEIJING)


def parse_target(value: str | None, now: datetime) -> tuple[datetime | None, str]:
    if value is None:
        chosen = wall_time(9, 0)
        mode = "default_09_00"
    elif value.lower() == "now":
        return None, "latest_common_minute"
    else:
        try:
            chosen = datetime.strptime(value, "%H:%M").time()
        except ValueError as exc:
            raise DataError("--time必须是HH:MM或now") from exc
        mode = "explicit_time"
    target = datetime.combine(now.date(), chosen, tzinfo=BEIJING)
    if not wall_time(8, 0) <= target.time() <= wall_time(14, 29):
        raise DataError("查询时间须在北京时间08:00至14:29之间")
    return target, mode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="双源查询当天韩国股市分钟截面")
    parser.add_argument("--time", help="北京时间HH:MM；省略时固定09:00；now取最近完整分钟")
    parser.add_argument("--wait-seconds", type=float, default=75.0)
    parser.add_argument("--request-timeout", type=float, default=25.0)
    args = parser.parse_args()
    if args.wait_seconds < 0 or args.request_timeout <= 0:
        parser.error("等待和超时参数无效")
    return args


def main() -> int:
    args = parse_args()
    now = datetime.now(BEIJING)
    try:
        target_bjt, mode = parse_target(args.time, now)
    except DataError as exc:
        print(
            json.dumps(
                {"status": "ABSTAIN", "errors": [str(exc)]},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2

    if target_bjt is not None and target_bjt > now + timedelta(minutes=2):
        result = {
            "status": "ABSTAIN",
            "requested_beijing_time": target_bjt.isoformat(timespec="minutes"),
            "errors": ["指定时间尚未到达，不能用未来数据作答"],
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    deadline = time.monotonic() + args.wait_seconds
    result: dict[str, Any]
    while True:
        sources, fetch_errors = fetch_sources(args.request_timeout)
        effective_target = target_bjt
        if effective_target is None:
            try:
                effective_target = latest_common_target(sources, now.date())
            except DataError as exc:
                result = {
                    "status": "ABSTAIN",
                    "source_check": "FAIL_CLOSED",
                    "errors": [*fetch_errors, str(exc)],
                }
                effective_target = None
        if effective_target is not None:
            result = evaluate(sources, effective_target, fetch_errors)
            if mode == "latest_common_minute":
                result["latest_common_delay_minutes"] = round(
                    max(
                        0.0,
                        (datetime.now(BEIJING) - effective_target).total_seconds() / 60.0,
                    ),
                    1,
                )
        result["request_mode"] = mode
        result["observed_at"] = datetime.now(BEIJING).isoformat(timespec="seconds")
        if result["status"] == "OK" or time.monotonic() >= deadline:
            break
        if target_bjt is not None and target_bjt < datetime.now(BEIJING) - timedelta(minutes=3):
            break
        time.sleep(min(5.0, max(0.0, deadline - time.monotonic())))

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
